"""C-01: 按日批量接口 Adapter 与实验工具离线测试。

测试原则：全部使用临时目录、固定响应样本与本地文件，不登录真实 BaoStock、
不访问生产数据库。真实实验仅能走现有唯一 worker（EXT-02 范畴），离线只验证：
- 类型化 Adapter 只允许 query_daily_history_k_AStock，不增加任意远端方法入口。
- 离线/未真实访问时实验结果状态必须标记 unverified。
- 固定响应样本可标准化为统一字段名；未知字段保留原名。
- 实验记录可持久化并回读（含版本、签名、字段映射、性能报告）。
- 白名单 SUPPORTED_METHODS 包含批量接口；未白名单方法被拒绝。
"""
import json
import os
import tempfile
import unittest

from baostock_runner.adapters import (BATCH_EXPECTED_FIELDS, BATCH_METHOD,
                                       DailyBatchAdapter, ExperimentRecorder,
                                       BatchExperiment)
from baostock_runner.config import Settings
from baostock_runner.gateway import SUPPORTED_METHODS, BaoStockGateway

# 固定响应样本：模拟批量接口返回的原始行（含估值/状态字段，与逐股日线对齐）
SAMPLE_RAW_ROWS = [
    {"date": "2026-09-23", "code": "sh.600000", "open": "10.00", "high": "10.50",
     "low": "9.90", "close": "10.20", "preclose": "10.00", "volume": "1000000",
     "amount": "10200000", "adjustflag": "3", "turn": "0.35", "tradestatus": "1",
     "pctChg": "2.00", "peTTM": "5.5", "pbMRQ": "0.6", "psTTM": "2.1",
     "pcfNcfTTM": "1.2", "isST": "0"},
    {"date": "2026-09-23", "code": "sz.000001", "open": "12.00", "high": "12.30",
     "low": "11.80", "close": "12.10", "preclose": "12.00", "volume": "2000000",
     "amount": "24000000", "adjustflag": "3", "turn": "0.50", "tradestatus": "1",
     "pctChg": "0.83", "peTTM": "7.0", "pbMRQ": "1.0", "psTTM": "3.0",
     "pcfNcfTTM": "1.5", "isST": "0"},
]


def _settings(directory, **kw):
    base = dict(db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0, daily_hard_limit=1000, offline=True)
    base.update(kw)
    return Settings(**base)


class AdapterContractTest(unittest.TestCase):
    def test_batch_method_in_whitelist(self):
        """批量接口在白名单内；未白名单方法不在 SUPPORTED_METHODS。"""
        self.assertIn(BATCH_METHOD, SUPPORTED_METHODS)
        self.assertNotIn("query_any_custom_method", SUPPORTED_METHODS)

    def test_expected_fields_defined(self):
        """批量接口期望字段与逐股日线对齐（含估值/状态字段）。"""
        self.assertIn("preclose", BATCH_EXPECTED_FIELDS)
        self.assertIn("tradestatus", BATCH_EXPECTED_FIELDS)
        self.assertIn("isST", BATCH_EXPECTED_FIELDS)
        self.assertIn("peTTM", BATCH_EXPECTED_FIELDS)
        self.assertIn("pcfNcfTTM", BATCH_EXPECTED_FIELDS)

    def test_adapter_offline_run_is_unverified(self):
        """离线/未真实访问：实验状态必须 unverified，不能误报 supported。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d))
            adapter = DailyBatchAdapter(gateway)
            try:
                exp = adapter.run("2026-09-23")
                self.assertEqual(exp.status, "unverified")
                # 离线/隔离验证注记：真实实验需 EXT-02（唯一 worker）
                self.assertIn("offline", exp.note)
                self.assertIn("EXT-02", exp.note)
                # 离线模拟仍返回行（用于链路测试），但状态明确 unverified
                self.assertIsInstance(exp.raw_rows, list)
            finally:
                gateway.close()

    def test_standardize_rows_maps_observed_fields(self):
        """固定响应样本标准化为统一字段名；未知字段保留原名。"""
        adapter = DailyBatchAdapter.__new__(DailyBatchAdapter)  # 不触达 gateway
        adapter.field_mapping = {f: f for f in BATCH_EXPECTED_FIELDS}
        std = adapter.standardize_rows(SAMPLE_RAW_ROWS)
        self.assertEqual(len(std), 2)
        self.assertEqual(std[0]["code"], "sh.600000")
        self.assertEqual(std[0]["close"], "10.20")
        self.assertEqual(std[0]["peTTM"], "5.5")
        self.assertEqual(std[0]["isST"], "0")
        # 未知字段保留原名（不猜值）
        rows_with_unknown = [{"date": "2026-09-23", "code": "sh.600000", "unknown_field": "x"}]
        std2 = adapter.standardize_rows(rows_with_unknown)
        self.assertIn("unknown_field", std2[0])
        self.assertEqual(std2[0]["unknown_field"], "x")


class ExperimentRecorderTest(unittest.TestCase):
    def test_save_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "experiments", "batch_20260923.json")
            recorder = ExperimentRecorder(path)
            exp = BatchExperiment(
                baostock_version="0.9.4", date="2026-09-23",
                status="unverified", raw_rows=SAMPLE_RAW_ROWS,
                fields_observed=list(BATCH_EXPECTED_FIELDS),
                field_mapping={f: f for f in BATCH_EXPECTED_FIELDS},
                performance={"elapsed_seconds": 0.1, "rows": 2},
                note="offline sample",
            )
            saved = recorder.save(exp)
            self.assertEqual(saved, path)
            loaded = ExperimentRecorder.load(path)
            self.assertEqual(loaded["method"], BATCH_METHOD)
            self.assertEqual(loaded["baostock_version"], "0.9.4")
            self.assertEqual(loaded["status"], "unverified")
            self.assertEqual(loaded["performance"]["rows"], 2)
            self.assertEqual(len(loaded["raw_rows"]), 2)
            self.assertIn("recorded_at", loaded)

    def test_invalid_status_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            recorder = ExperimentRecorder(os.path.join(d, "x.json"))
            exp = BatchExperiment(status="not-a-real-status")
            with self.assertRaises(ValueError):
                recorder.save(exp)

    def test_gateway_rejects_arbitrary_method_via_execute(self):
        """未白名单方法在执行路径被拒绝（不增加任意远端方法执行入口）。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d, offline=False))
            try:
                with self.assertRaises(ValueError):
                    gateway._execute(None, "query_any_custom_method", {})
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
