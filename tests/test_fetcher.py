import os
import tempfile
import unittest

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher, BudgetExhausted
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import Storage


def _settings(directory, **kw):
    base = dict(
        db_path=os.path.join(directory, "data.sqlite3"),
        log_path=os.path.join(directory, "audit.jsonl"),
    )
    base.update(kw)
    return Settings(**base)


class StorageFetchSchemaTest(unittest.TestCase):
    def test_fetch_schema_tables_created(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            tables = set(storage.list_tables())
            for t in ("securities", "trade_calendar", "index_constituents", "financials",
                      "dividends", "adjust_factors", "stock_industry", "download_jobs",
                      "download_usage", "dataset_meta"):
                self.assertIn(t, tables)

    def test_securities_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            storage.put_securities([
                {"code": "sh.600000", "name": "浦发银行", "status": "1"},
                {"code": "sz.000001", "name": "平安银行", "status": "1"},
            ])
            self.assertEqual(storage.count_securities(), 2)
            self.assertEqual(storage.get_securities_codes(), ["sh.600000", "sz.000001"])
            # 幂等覆盖：同主键更新不产生新行
            storage.put_securities([{"code": "sh.600000", "name": "浦发银行X", "status": "1"}])
            self.assertEqual(storage.count_securities(), 2)
            self.assertEqual(storage.get_securities()[0]["name"], "浦发银行X")

    def test_financials_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            storage.put_financials("profit", "sh.600000", 2025, 4, {"netProfit": "100.0"})
            self.assertTrue(storage.financial_exists("profit", "sh.600000", 2025, 4))
            self.assertFalse(storage.financial_exists("profit", "sh.600000", 2025, 3))
            self.assertEqual(storage.latest_financial_period("profit"), (2025, 4))
            rows = storage.get_financials("profit", "sh.600000")
            self.assertEqual(rows[0]["data"]["netProfit"], "100.0")

    def test_download_usage_counter(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            self.assertEqual(storage.download_usage_today(), 0)
            storage.increment_download_usage()
            storage.increment_download_usage()
            self.assertEqual(storage.download_usage_today(), 2)

    def test_daily_pending_codes_and_today_dedup(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            storage.put_securities([
                {"code": "sh.600000", "name": "浦发", "status": "1"},
                {"code": "sz.000001", "name": "平安", "status": "1"},
            ])
            storage.put_daily_bars("sh.600000", "d", "3",
                                   [{"date": "2026-09-15", "code": "sh.600000", "close": "10"}])
            pending = storage.get_daily_pending_codes(limit=10, end_date="2026-09-16", primary_adjustflag="3")
            # 600000 已有数据但未到 09-16；000001 无数据 → 都在 pending
            self.assertEqual(set(pending), {"sh.600000", "sz.000001"})
            # 模拟今天已 done → 当日去重，不再返回
            storage.job_upsert("daily_bars", "sh.600000|3", "done")
            storage.job_upsert("daily_bars", "sz.000001|3", "done")
            pending2 = storage.get_daily_pending_codes(limit=10, end_date="2026-09-16", primary_adjustflag="3")
            self.assertEqual(pending2, [])

    def test_job_stats(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            storage.job_upsert("daily_bars", "a|3", "done")
            storage.job_upsert("daily_bars", "b|3", "failed", error="boom")
            stats = storage.job_stats("daily_bars")
            self.assertEqual(stats["done"], 1)
            self.assertEqual(stats["failed"], 1)


class GatewayUseCacheTest(unittest.TestCase):
    def test_call_use_cache_false_skips_cache(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=100, offline=True)
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                first = gateway.call("query_history_k_data_plus", params, use_cache=False)
                second = gateway.call("query_history_k_data_plus", params, use_cache=False)
                self.assertFalse(first["cache_hit"])
                self.assertFalse(second["cache_hit"])  # 强制刷新：第二次仍不是缓存
                self.assertEqual(gateway.storage.usage_today(), 2)
            finally:
                gateway.close()

    def test_daily_bars_use_cache_false_updates_local(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=100, offline=True)
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sz.000001", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                first = gateway.daily_bars(params, use_cache=False)
                second = gateway.daily_bars(params, use_cache=False)
                self.assertFalse(first["cache_hit"])
                self.assertFalse(second["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), 2)
            finally:
                gateway.close()


class FetcherBudgetTest(unittest.TestCase):
    def test_fetch_budget_ratio(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, daily_hard_limit=100, fetch_budget_ratio=0.5, offline=True)
            gateway = BaoStockGateway(settings)
            fetcher = Fetcher(gateway, settings)
            self.assertEqual(fetcher.fetch_budget, 50)
            self.assertEqual(fetcher.budget_left(), 50)
            gateway.close()

    def test_budget_exhausted_raises(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, daily_hard_limit=10, fetch_budget_ratio=0.5, offline=True)
            gateway = BaoStockGateway(settings)
            fetcher = Fetcher(gateway, settings)
            for _ in range(5):
                gateway.storage.increment_download_usage()
            self.assertEqual(fetcher.budget_left(), 0)
            with self.assertRaises(BudgetExhausted):
                fetcher._bounded_call("query_trade_dates", {"start_date": "2026-09-15", "end_date": "2026-09-16"})
            gateway.close()

    def test_start_disabled_when_fetch_enabled_false(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, offline=True, fetch_enabled=False)
            gateway = BaoStockGateway(settings)
            fetcher = Fetcher(gateway, settings)
            fetcher.start()
            self.assertFalse(fetcher.thread.is_alive())
            gateway.close()

    def test_bounded_call_counts_download_usage(self):
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=100,
                                 fetch_budget_ratio=0.5, offline=True)
            gateway = BaoStockGateway(settings)
            fetcher = Fetcher(gateway, settings)
            try:
                fetcher._bounded_call("query_trade_dates", {"start_date": "2026-09-15", "end_date": "2026-09-16"})
                self.assertEqual(gateway.storage.download_usage_today(), 1)
                self.assertEqual(gateway.storage.usage_today(), 1)  # 同时计入总预算
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
