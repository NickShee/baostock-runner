"""Regression coverage for the production pre-IPO request exhaustion incident."""
import datetime
import os
import tempfile
import unittest
from unittest.mock import Mock

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock


class DailyCollectionRegressionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = datetime.datetime.fromisoformat('2026-09-24T18:30:00+08:00')
        self.clock = Clock(now_fn=lambda: self.now)
        self.settings = Settings(
            db_path=os.path.join(self.tmp.name, 'data.sqlite3'),
            log_path=os.path.join(self.tmp.name, 'audit.jsonl'),
            offline=True, fetch_adjustflags='1,3', fetch_batch_size=1,
            fetch_daily_start_date='2026-09-21', daily_batch_window_days=1)
        self.storage = Storage(self.settings.db_path, self.settings.log_path, clock=self.clock)
        self.storage.put_trade_calendar([
            {'calendar_date': day, 'is_trading_day': 1}
            for day in ['2026-09-21', '2026-09-22', '2026-09-23', '2026-09-24']])
        self.gateway = Mock(storage=self.storage, settings=self.settings, clock=self.clock)
        self.gateway.daily_bars.side_effect = self.download
        self.fetcher = Fetcher(self.gateway, self.settings)

    def download(self, params, **kwargs):
        self.storage.put_daily_bars(params['code'], 'd', params['adjustflag'], [
            {'date': day, 'close': '10'} for day in self.fetcher._trade_dates_in_range(
                params['start_date'], params['end_date'])])

    def seed(self, code, days, flags=('1', '3'), **sec):
        self.storage.put_securities([{'code': code, 'status': '1', **sec}])
        for flag in flags:
            self.storage.put_daily_bars(code, 'd', flag, [
                {'date': day, 'close': '10'} for day in days])

    def coverage(self, code, **kwargs):
        return self.storage.get_daily_coverage(
            [code], '2026-09-21', '2026-09-24', '1', **kwargs)

    def test_partial_listing_lifetime_excludes_pre_ipo_and_post_delisting(self):
        self.seed('sh.600000', ['2026-09-22', '2026-09-23'],
                  ipo_date='2026-09-22', out_date='2026-09-24')
        result = self.coverage('sh.600000')
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['per_code']['sh.600000']['gap_windows'], [])
        self.assertFalse(self.fetcher._fetch_daily_batch())
        self.gateway.daily_bars.assert_not_called()

    def test_explicit_empty_calendar_does_not_invent_daily_gaps(self):
        self.seed('sh.600000', [])
        result = self.coverage('sh.600000', trade_dates=[])
        self.assertEqual(result['per_code']['sh.600000']['missing_count'], 0)

    def test_recent_pool_updates_precede_history_and_skip_pre_ipo(self):
        self.seed('sh.600000', [], ipo_date='2026-09-23')
        self.seed('sz.000001', [], ipo_date='2026-09-21')
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertTrue(self.fetcher._fetch_daily_batch())
        calls = self.gateway.daily_bars.call_args_list
        self.assertEqual({c.args[0]['code'] for c in calls}, {'sh.600000', 'sz.000001'})
        self.assertTrue(all(c.args[0]['start_date'] == '2026-09-24' for c in calls))
        self.assertTrue(self.fetcher._fetch_daily_batch())
        params = self.gateway.daily_bars.call_args.args[0]
        self.assertEqual(params['start_date'], '2026-09-23')
        self.assertEqual(params['end_date'], '2026-09-23')

    def test_missing_secondary_adjustment_is_collected(self):
        self.seed('sh.600000', ['2026-09-24'], flags=('1',))
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertEqual(self.gateway.daily_bars.call_count, 1)
        self.assertEqual(self.gateway.daily_bars.call_args.args[0]['adjustflag'], '3')

    def test_middle_gap_is_repaired_even_when_latest_bar_exists(self):
        self.seed('sh.600000', ['2026-09-21', '2026-09-23', '2026-09-24'])
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertTrue(all(c.kwargs['force_refresh'] for c in self.gateway.daily_bars.call_args_list))
        self.assertEqual(self.coverage('sh.600000')['status'], 'complete')
        self.assertFalse(self.fetcher._fetch_daily_batch())

    def test_empty_response_waits_then_retries_instead_of_success(self):
        self.seed('sh.600000', [])
        self.gateway.daily_bars.side_effect = lambda *a, **k: None
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertEqual(self.storage.job_status('daily_bars', 'sh.600000|1'), 'waiting_data')
        self.gateway.daily_bars.reset_mock()
        self.assertFalse(self.fetcher._fetch_daily_batch())
        self.gateway.daily_bars.assert_not_called()
        self.now += datetime.timedelta(minutes=6)
        self.assertTrue(self.fetcher._fetch_daily_batch())

    def test_morning_success_does_not_suppress_evening_target(self):
        self.now = datetime.datetime.fromisoformat('2026-09-24T10:00:00+08:00')
        self.seed('sh.600000', [], ipo_date='2026-09-23')
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertEqual(self.storage.job_status('daily_bars', 'sh.600000|1'), 'succeeded')
        self.now = datetime.datetime.fromisoformat('2026-09-24T18:30:00+08:00')
        self.gateway.daily_bars.reset_mock()
        self.assertTrue(self.fetcher._fetch_daily_batch())
        self.assertEqual(self.gateway.daily_bars.call_args.args[0]['end_date'], '2026-09-24')


if __name__ == '__main__':
    unittest.main()
