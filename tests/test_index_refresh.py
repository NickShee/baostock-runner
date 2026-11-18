from collections import Counter
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock


class IndexRefreshTest(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.settings = Settings(db_path=str(Path(temp.name) / "test.sqlite3"),
                                 log_path=str(Path(temp.name) / "audit.jsonl"), offline=True,
                                 fetch_financial_start_year=2030)
        self.set_time("2026-11-16T16:01:00+00:00")  # Shanghai 11/17 00:01
        self.clock = Clock(now_fn=lambda: self.now)
        self.calls = Counter()
        self.restart()

    def set_time(self, iso):
        self.now = datetime.fromisoformat(iso)

    def restart(self):
        self.storage = Storage(self.settings.db_path, self.settings.log_path, clock=self.clock)
        gateway = Mock(storage=self.storage, clock=self.clock)
        gateway.call.side_effect = self.respond
        self.fetcher = Fetcher(gateway, self.settings)

    def respond(self, method, params, **kwargs):
        self.calls[self.clock.business_date().isoformat(), method] += 1
        return {"data": [{"code": "sh.600000", "code_name": "浦发银行"}]}

    def poll_index(self):
        self.fetcher.wake("index_constituents")
        return self.fetcher._step_once()

    def test_once_per_index_per_shanghai_day_across_restart(self):
        self.assertTrue(self.fetcher._step_once())  # Empty universe also downloads HS300.
        self.storage.put_trade_calendar([{"calendar_date": "2026-11-18", "is_trading_day": 0}])
        self.storage.set_meta("stock_industry")
        self.assertTrue(self.fetcher._step_once())  # Automatic refresh of the other two indices.
        self.restart()
        self.assertFalse(self.fetcher._step_once())
        self.assertFalse(self.poll_index())
        self.set_time("2026-11-17T00:00:00+00:00")  # UTC midnight, same Shanghai day.
        self.assertFalse(self.poll_index())
        self.set_time("2026-11-17T15:59:59+00:00")
        self.assertFalse(self.poll_index())
        self.set_time("2026-11-17T16:00:00+00:00")  # Next Shanghai midnight.
        self.assertTrue(self.poll_index())
        self.assertFalse(self.poll_index())
        expected = Counter({("2026-11-17", "query_stock_basic"): 1})
        for day in ("2026-11-17", "2026-11-18"):
            for index in ("hs300", "sz50", "zz500"):
                expected[day, f"query_{index}_stocks"] = 1
                self.assertEqual(self.storage.get_index_codes(index, day), ["sh.600000"])
        self.assertEqual(self.calls, expected)

    def test_partial_refresh_only_retries_missing_snapshot(self):
        def partial(method, params, **kwargs):
            result = self.respond(method, params, **kwargs)
            return {"data": []} if method == "query_zz500_stocks" else result

        self.fetcher.gateway.call.side_effect = partial
        with self.assertRaisesRegex(RuntimeError, "query_zz500_stocks"):
            self.poll_index()
        self.assertEqual(self.storage.get_index_codes("zz500", "2026-11-17"), [])
        self.restart()
        self.assertTrue(self.poll_index())
        self.assertFalse(self.poll_index())
        self.assertEqual(self.calls, {
            ("2026-11-17", "query_hs300_stocks"): 1,
            ("2026-11-17", "query_sz50_stocks"): 1,
            ("2026-11-17", "query_zz500_stocks"): 2,
        })

    def test_batch_crossing_midnight_uses_new_date_for_subsequent_calls(self):
        self.set_time("2026-11-17T15:59:59+00:00")

        def cross_midnight(method, params, **kwargs):
            result = self.respond(method, params, **kwargs)
            self.set_time("2026-11-17T16:00:01+00:00")
            return result

        self.fetcher.gateway.call.side_effect = cross_midnight
        self.poll_index()
        self.poll_index()
        self.assertFalse(self.poll_index())
        self.assertEqual(self.calls, {
            ("2026-11-17", "query_hs300_stocks"): 1,
            ("2026-11-18", "query_hs300_stocks"): 1,
            ("2026-11-18", "query_sz50_stocks"): 1,
            ("2026-11-18", "query_zz500_stocks"): 1,
        })
