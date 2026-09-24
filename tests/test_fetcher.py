import datetime
import os
import tempfile
import threading
import time
import unittest

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher, BudgetExhausted
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock


def _settings(directory, **kw):
    base = dict(
        db_path=os.path.join(directory, "data.sqlite3"),
        log_path=os.path.join(directory, "audit.jsonl"),
    )
    base.update(kw)
    return Settings(**base)


def _fixed_clock() -> Clock:
    """A-01: 固定业务日（上海 2026-09-24 周四 18:30，已过当日检查时间 18:00）。
    避免测试依赖真实当前年份/日期（ENV-01 特别要求：可注入时钟）。"""
    fixed = datetime.datetime(2026, 9, 24, 18, 30,
                              tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
    return Clock(now_fn=lambda: fixed)


def _ready_storage(storage, fetcher):
    """A-01: 让调度前置条件就绪（写真实日历覆盖到今天），避免被日历补齐短路。

    同时写入今天与 2099 的日线 bar，使日线"已追平到最新"（缺口检查无活），
    从而让 _step_once 可以进入财务阶段测试。
    """
    storage.put_securities([{"code": "sh.600000", "name": "浦发", "status": "1"}])
    storage.set_meta("stock_industry", detail="ready")
    storage.set_meta("index_constituents", detail="ready")
    today = fetcher.business_today
    storage.put_trade_calendar([
        {"calendar_date": today, "is_trading_day": 1},
        {"calendar_date": "2099-01-01", "is_trading_day": 1},
    ])
    storage.put_daily_bars("sh.600000", "d", "3", [
        {"date": today, "code": "sh.600000", "close": "10"},
        {"date": "2099-01-01", "code": "sh.600000", "close": "10"},
    ])


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
            storage.job_upsert("daily_bars", "sh.600000|3", "succeeded")
            storage.job_upsert("daily_bars", "sz.000001|3", "succeeded")
            pending2 = storage.get_daily_pending_codes(limit=10, end_date="2026-09-16", primary_adjustflag="3")
            self.assertEqual(pending2, [])

    def test_job_stats(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(db_path=os.path.join(d, "data.sqlite3"), log_path=os.path.join(d, "audit.jsonl"))
            storage.job_upsert("daily_bars", "a|3", "succeeded")
            storage.job_upsert("daily_bars", "b|3", "retryable_failed", error="boom")
            stats = storage.job_stats("daily_bars")
            self.assertEqual(stats["succeeded"], 1)
            self.assertEqual(stats["done"], 1)  # 向后兼容别名
            self.assertEqual(stats["retryable_failed"], 1)
            self.assertEqual(stats["total"], 2)


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

    def test_step_once_proceeds_to_financials_after_daily_done(self):
        """回归：日线无待补后，_step_once 必须继续进入财务阶段（不可被短路截断）。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=100,
                                 fetch_budget_ratio=0.5, offline=True,
                                 fetch_financial_start_year=2025)
            gateway = BaoStockGateway(settings, clock=_fixed_clock())
            fetcher = Fetcher(gateway, settings)
            try:
                storage = gateway.storage
                _ready_storage(storage, fetcher)
                # 日线"已到最新"：写入未来日期 bar，确保 pending 为空
                storage.put_daily_bars("sh.600000", "d", "3",
                                       [{"date": "2099-01-01", "code": "sh.600000", "close": "10"}])
                worked = fetcher._step_once()
                self.assertTrue(worked)
                self.assertGreater(storage.count_rows("financials"), 0)
            finally:
                gateway.close()

    def test_financial_empty_period_skipped_after_done(self):
        """回归：空报告期标记 waiting_data 后未到期不得反复查询（防死循环）。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=100,
                                 fetch_budget_ratio=0.5, offline=True,
                                 fetch_financial_start_year=2026)
            gateway = BaoStockGateway(settings, clock=_fixed_clock())
            fetcher = Fetcher(gateway, settings)
            try:
                storage = gateway.storage
                _ready_storage(storage, fetcher)
                # 固定时钟 2026-09-24：已结束季度 = 2026Q2；
                # 未来未结束季度（Q3/Q4）不入队，首个入队任务为 2026Q2。
                worked = fetcher._step_once()
                self.assertTrue(worked)
                rows = storage.get_financials("profit", "sh.600000")
                self.assertEqual(rows[0]["quarter"], 2)
                # 空财报后续轮询：等待期未到不再重复查询（_financial_job_due 直接判定）
                storage.job_upsert("financials", "sh.600000|2026Q2|profit",
                                   storage.JOB_WAITING_DATA,
                                   next_retry_at="2099-01-01T00:00:00+00:00")
                due, _reason = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertFalse(due)  # waiting_data 未到期不重查（防死循环）
                # 空财报等待到期后重新查询
                now = storage.clock.now_utc().isoformat()
                storage.job_upsert("financials", "sh.600000|2026Q2|profit",
                                   storage.JOB_WAITING_DATA,
                                   next_retry_at="2020-01-01T00:00:00+00:00")
                due2, _r2 = fetcher._financial_job_due("profit", "sh.600000", 2026, 2)
                self.assertTrue(due2)
            finally:
                gateway.close()


class GatewayPriorityTest(unittest.TestCase):
    def test_mcp_request_preempts_fetcher_in_queue(self):
        """回归：MCP 查询（priority=0）插入 fetcher（priority=1）请求之间时，
        worker 必须先执行 MCP 请求，再继续 fetcher 请求。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, min_interval_seconds=0, daily_hard_limit=1000,
                                 fetch_budget_ratio=0.5, offline=True)
            gateway = BaoStockGateway(settings)
            order = []
            orig_execute = gateway._execute

            def tracked(bs, method, p, use_cache=True):
                order.append(method)
                return orig_execute(bs, method, p, use_cache)

            gateway._execute = tracked
            try:
                def fetcher_flow():
                    # fetcher：先入队一个低优先级请求，稍后再入队第二个
                    gateway.call("query_trade_dates",
                                 {"start_date": "2026-01-01", "end_date": "2026-01-31"},
                                 use_cache=False, priority=1)
                    time.sleep(0.15)  # 窗口：让 MCP 请求插队
                    gateway.call("query_all_stock", {"trade_date": "2026-01-05"},
                                 use_cache=False, priority=1)

                t = threading.Thread(target=fetcher_flow)
                t.start()
                time.sleep(0.03)
                # MCP 高优先级请求插入（此时队列中已有第二个 fetcher 请求在等）
                gateway.call("query_stock_basic", {"code": "", "code_name": ""}, priority=0)
                t.join()
                # MCP 必须在第二个 fetcher 请求之前执行
                self.assertLess(order.index("query_stock_basic"), order.index("query_all_stock"))
                self.assertEqual(order[0], "query_trade_dates")
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
