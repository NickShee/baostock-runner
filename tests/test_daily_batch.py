"""C-02: 每日主采集 daily_batch 模式测试。

验收点（隔离验证，不切换生产默认值）：
- 新增模式 per_stock（默认）/daily_batch；默认保留 per_stock。
- daily_batch 使用日期级任务和覆盖校验；历史回填/缺口继续走逐股路径。
- 批量响应先校验再提交：截断（行数不足）/重复键冲突/未解释缺失不得标记整日完成。
- 缺估值字段时追加补充任务（不把行情覆盖报告当作筛选就绪报告）。
- 接口不可用时退回逐股路径，并保留降级状态。
- 两条路径得到相同标准记录；重复执行幂等；部分响应/权限/超时/预算不足可恢复。
- 未通过真实实验时仅允许隔离模式验证，不切换生产默认值。
"""
import os
import tempfile
import unittest
from unittest import mock

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher, BudgetExhausted
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.timeutil import Clock
import datetime


def _settings(directory, **kw):
    base = dict(
        db_path=os.path.join(directory, "data.sqlite3"),
        log_path=os.path.join(directory, "audit.jsonl"),
        min_interval_seconds=0,
        daily_hard_limit=10000,
        fetch_budget_ratio=0.8,
        offline=True,
        fetch_mode="daily_batch",
        daily_batch_min_rows=5,
        daily_batch_window_days=3,
        fetch_daily_start_date="2026-09-01",
        fetch_adjustflags="3",
        fetch_batch_size=100,
    )
    base.update(kw)
    return Settings(**base)


def _fixed_clock() -> Clock:
    fixed = datetime.datetime(2026, 9, 24, 18, 30,
                              tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
    return Clock(now_fn=lambda: fixed)


def _ready_storage(storage, fetcher, days=("2026-09-23", "2026-09-24")):
    """让调度前置就绪：证券池/日历/行业/成分 + 覆盖到今天。"""
    saved_clock = storage.clock
    storage.clock = fetcher.clock
    try:
        storage.put_securities([{"code": "sh.600000", "name": "浦发", "status": "1"},
                                {"code": "sz.000001", "name": "平安", "status": "1"}])
        storage.set_meta("stock_industry", detail="ready")
        storage.set_meta("index_constituents", detail="ready")
        calendar = [{"calendar_date": "2099-01-01", "is_trading_day": 1}]
        for d in days:
            calendar.append({"calendar_date": d, "is_trading_day": 1})
        storage.put_trade_calendar(calendar)
        # 证券池逐股已覆盖到前一天，让 daily_batch 窗口主采集这些天
        for d in days:
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": d, "code": "sh.600000", "close": "10"}])
            storage.put_daily_bars("sz.000001", "d", "3", [
                {"date": d, "code": "sz.000001", "close": "11"}])
    finally:
        storage.clock = saved_clock


def _batch_rows(day, codes=None, with_valuation=True):
    """生成 >= daily_batch_min_rows 的批量行（默认 8 个代码，满足 5 行阈值）。"""
    if codes is None:
        codes = [f"sh.60{i:04d}" for i in range(1000, 1008)]
    rows = []
    for i, code in enumerate(codes):
        r = {"date": day, "code": code, "open": "10.0", "high": "10.5",
             "low": "9.9", "close": "10.2", "preclose": "10.0",
             "volume": "1000", "amount": "10200", "adjustflag": "3",
             "turn": "0.5", "tradestatus": "1", "pctChg": "2.0", "isST": "0"}
        if with_valuation:
            r.update({"peTTM": "5.5", "pbMRQ": "0.6", "psTTM": "2.1", "pcfNcfTTM": "1.2"})
        rows.append(r)
    return rows


class DailyBatchModeTest(unittest.TestCase):
    def test_default_mode_is_per_stock(self):
        """默认保留 per_stock；daily_batch 需要显式开启（未过真实实验不切换生产默认）。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d)
            self.assertEqual(settings.fetch_mode, "daily_batch")  # 测试显式开启
            default_settings = Settings(
                db_path=os.path.join(d, "data2.sqlite3"),
                log_path=os.path.join(d, "audit2.jsonl"))
            self.assertEqual(default_settings.fetch_mode, "per_stock")

    def test_daily_batch_mode_commits_standardized_rows(self):
        """批量接口响应标准化后提交；与逐股路径字段一致；重复执行幂等。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            # 替换 gateway.call 模拟批量接口（离线隔离）
            with mock.patch.object(gateway, "call") as call:
                def fake_call(method, params, **kw):
                    if method == "query_daily_history_k_AStock":
                        return {"data": _batch_rows(params["date"]), "cache_hit": False}
                    raise AssertionError(f"unexpected method: {method}")
                call.side_effect = fake_call
                worked = fetcher._fetch_daily_batch_mode()
                self.assertTrue(worked)
                self.assertEqual(call.call_count, 2)  # 窗口 2 个交易日（09-23/09-24）
                # 日线事实表已写入（批量行代码）
                rows = storage.get_daily_bars("sh.601000", "d", "3", "2026-09-22", "2026-09-24")
                self.assertTrue(rows)
                self.assertIn("2026-09-23", [r["date"] for r in rows])
                self.assertIn("2026-09-24", [r["date"] for r in rows])
                # 重复执行幂等：日期级任务已 succeeded → 无活
                worked2 = fetcher._fetch_daily_batch_mode()
                self.assertFalse(worked2)

    def test_truncated_response_not_marked_complete(self):
        """截断（行数不足阈值）不得标记整日完成 → retryable_failed。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            with mock.patch.object(gateway, "call") as call:
                call.return_value = {"data": _batch_rows("2026-09-23")[:1], "cache_hit": False}
                fetcher._fetch_daily_batch_mode()
                status = storage.job_status("daily_bars_day", "2026-09-23")
                self.assertEqual(status, storage.JOB_RETRYABLE_FAILED)
                # 未标整日完成：succeeded 的日期任务数应为 0
                self.assertNotEqual(status, storage.JOB_SUCCEEDED)

    def test_duplicate_keys_not_marked_complete(self):
        """重复键冲突不得标记整日完成。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            rows = _batch_rows("2026-09-23")
            rows.append(dict(rows[0]))  # 重复 code
            with mock.patch.object(gateway, "call") as call:
                call.return_value = {"data": rows, "cache_hit": False}
                fetcher._fetch_daily_batch_mode()
                status = storage.job_status("daily_bars_day", "2026-09-23")
                self.assertEqual(status, storage.JOB_RETRYABLE_FAILED)
                self.assertIn("duplicate", storage.job_detail("daily_bars_day", "2026-09-23") or "")

    def test_missing_valuation_appends_supplementary_note(self):
        """缺估值字段：仍完成行情覆盖，但记录补充任务说明（不当作筛选就绪）。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            with mock.patch.object(gateway, "call") as call:
                def fake_call(method, params, **kw):
                    return {"data": _batch_rows(params["date"], with_valuation=False),
                            "cache_hit": False}
                call.side_effect = fake_call
                fetcher._fetch_daily_batch_mode()
                status = storage.job_status("daily_bars_day", "2026-09-23")
                self.assertEqual(status, storage.JOB_SUCCEEDED)
                detail = storage.job_detail("daily_bars_day", "2026-09-23") or ""
                self.assertIn("missing valuation fields", detail)
                self.assertIn("peTTM", detail)

    def test_upstream_failure_falls_back_to_per_stock(self):
        """批量接口不可用（权限/超时）→ 降级逐股路径，保留降级状态。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, daily_hard_limit=100000)
            gateway = BaoStockGateway(settings, clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            with mock.patch.object(gateway, "call") as call:
                def fake_call(method, params, **kw):
                    if method == "query_daily_history_k_AStock":
                        raise PermissionError("interface not permitted")
                    return {"data": [], "cache_hit": False}
                call.side_effect = fake_call
                worked = fetcher._fetch_daily_batch_mode()
                self.assertTrue(worked)
                # 批量接口被尝试且失败 → 降级逐股完成，日期任务最终 succeeded
                batch_attempted = any(c[0][0] == "query_daily_history_k_AStock"
                                      for c in call.call_args_list)
                self.assertTrue(batch_attempted)
                self.assertEqual(storage.job_status("daily_bars_day", "2026-09-23"),
                                 storage.JOB_SUCCEEDED)

    def test_budget_exhaustion_recoverable(self):
        """预算不足：抛 BudgetExhausted（软暂停信号），不丢任务、下次可继续。

        通过极小 budget_left 注入：直接替换 fetcher.budget_left 模拟预算耗尽，
        验证任务未被标成功、恢复后仍可重试。
        """
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d)
            gateway = BaoStockGateway(settings, clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            with mock.patch.object(gateway, "call") as call:
                call.side_effect = lambda method, params, **kw: {"data": _batch_rows(params["date"]),
                                                                 "cache_hit": False}
                with mock.patch.object(fetcher, "budget_left", return_value=0):
                    with self.assertRaises(BudgetExhausted):
                        fetcher._fetch_daily_batch_mode()
                # 任务未标完成，恢复后可继续
                status = storage.job_status("daily_bars_day", "2026-09-23")
                self.assertNotEqual(status, storage.JOB_SUCCEEDED)


class PerStockStillWorksTest(unittest.TestCase):
    def test_per_stock_mode_ignores_daily_batch(self):
        """per_stock 模式下 _fetch_daily_batch_mode 无活（默认行为不变）。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, fetch_mode="per_stock")
            gateway = BaoStockGateway(settings, clock=_fixed_clock())
            fetcher = Fetcher(gateway, gateway.settings)
            storage = gateway.storage
            _ready_storage(storage, fetcher)
            worked = fetcher._fetch_daily_batch_mode()
            self.assertFalse(worked)


if __name__ == "__main__":
    unittest.main()
