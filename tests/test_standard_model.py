"""D-01: 最小标准模型与版本留存测试。

验收点：
- 旧 MCP 字段兼容：get_securities 仍返回 code/name 等兼容键，新增 asset_type/source 不破坏读取。
- 相同响应不产生重复版本：同批/同内容写入幂等；不同批次/内容形成版本序列。
- 更新可追溯：security_versions 记录观察时间；financials 写 pub_date/stat_date。
- 旧版本可重建：security_versions 可按 observed_at 回溯；financial_history 保留修订。
- data_batches 批次记录可查询。
- 字段映射集中维护：standard 模块标准化证券/财务元信息；未知来源/时间标记 unknown，不补造。
"""
import datetime
import os
import tempfile
import unittest
from datetime import timezone

from baostock_runner.config import Settings
from baostock_runner.storage import SCHEMA_VERSION, Storage
from baostock_runner.standard import (SOURCE_BAOSTOCK, SOURCE_UNKNOWN,
                                       infer_asset_type, standardize_financial_meta,
                                       standardize_security)
from baostock_runner.timeutil import Clock


def _clock(now_iso="2026-09-24T10:00:00+00:00"):
    return Clock(now_fn=lambda: datetime.datetime.fromisoformat(now_iso))


def _mk(directory, **kw):
    base = dict(db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"))
    base.update(kw)
    return Storage(**base)


class SecurityStandardModelTest(unittest.TestCase):
    def test_legacy_fields_compatible(self):
        """旧 MCP 字段兼容：code/name/ipo_date 等仍可读取；新增 asset_type/source。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d, clock=_clock())
            self.assertEqual(storage.integrity_check()["user_version"], SCHEMA_VERSION)
            storage.put_securities([
                {"code": "sh.600000", "code_name": "浦发银行", "ipoDate": "1999-11-10",
                 "outDate": "", "type": "1", "status": "1"},
            ])
            rows = storage.get_securities()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["code"], "sh.600000")
            self.assertEqual(rows[0]["name"], "浦发银行")
            self.assertEqual(rows[0]["ipo_date"], "1999-11-10")
            self.assertEqual(rows[0]["asset_type"], "stock")
            self.assertEqual(rows[0]["source"], "baostock")
            # 兼容查询：旧字段名仍可用
            self.assertIn("code", rows[0])
            self.assertIn("status", rows[0])

    def test_same_response_no_duplicate_versions(self):
        """相同响应不产生重复版本：同内容重复写入幂等（版本数不增）。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d, clock=_clock())
            row = {"code": "sh.600000", "code_name": "浦发银行",
                   "ipoDate": "1999-11-10", "type": "1", "status": "1"}
            storage.put_securities([row])
            storage.put_securities([row])
            versions = storage.get_security_versions("sh.600000")
            # 同一 observed_at（同批次同内容）→ PRIMARY KEY 幂等，只 1 条版本
            self.assertEqual(len(versions), 1)

    def test_update_is_traceable_and_old_version_rebuildable(self):
        """更新可追溯：不同时间写入形成版本序列；旧版本可重建。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d, clock=_clock("2026-09-24T10:00:00+00:00"))
            storage.put_securities([{"code": "sh.600000", "code_name": "浦发银行", "status": "1"}])
            # 时间推进后更新名称
            storage.clock = _clock("2026-09-25T10:00:00+00:00")
            storage.put_securities([{"code": "sh.600000", "code_name": "浦发银行X", "status": "1"}])
            versions = storage.get_security_versions("sh.600000")
            self.assertEqual(len(versions), 2)
            # 旧版本可重建：最早的 observed_at 保留旧名
            self.assertEqual(versions[0]["name"], "浦发银行")
            self.assertEqual(versions[1]["name"], "浦发银行X")
            self.assertEqual(versions[0]["observed_at"], "2026-09-24T10:00:00+00:00")
            self.assertEqual(versions[1]["observed_at"], "2026-09-25T10:00:00+00:00")
            # 当前表保留最新值
            current = storage.get_securities()[0]
            self.assertEqual(current["name"], "浦发银行X")


class FinancialStandardMetaTest(unittest.TestCase):
    def test_financial_meta_extracted(self):
        """财务标准元信息：pubDate/statDate 提取为 pub_date/stat_date。"""
        payload = {"code": "sh.600000", "pubDate": "2026-04-30", "statDate": "2025-12-31",
                   "netProfit": "1.2"}
        meta = standardize_financial_meta(payload)
        self.assertEqual(meta["pub_date"], "2026-04-30")
        self.assertEqual(meta["stat_date"], "2025-12-31")

    def test_financial_meta_missing_not_guessed(self):
        """缺失公告日期/报告期不猜值。"""
        meta = standardize_financial_meta({"netProfit": "1.2"})
        self.assertEqual(meta, {})

    def test_put_financials_writes_standard_meta(self):
        """put_financials 同事务写 pub_date/stat_date 标准列。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d, clock=_clock())
            payload = {"code": "sh.600000", "pubDate": "2026-04-30", "statDate": "2025-12-31"}
            with storage._session() as db:
                storage._put_financials_in_txn(db, "profit", "sh.600000", 2025, 4, payload,
                                               fetched_at="2026-09-24T10:00:00+00:00")
            with storage._session() as db:
                row = db.execute(
                    "SELECT payload, pub_date, stat_date FROM financials WHERE dataset='profit' AND code='sh.600000' AND year=2025 AND quarter=4"
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[1], "2026-04-30")
            self.assertEqual(row[2], "2025-12-31")


class BatchRecordTest(unittest.TestCase):
    def test_batch_lifecycle(self):
        """采集批次记录：开始→完成→查询。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(d, clock=_clock())
            storage.batch_start("B-20260923", SOURCE_BAOSTOCK, "0.9.4", "daily_batch:2026-09-23")
            storage.batch_finish("B-20260923", "succeeded", 5000, "ok")
            status = storage.batch_status("B-20260923")
            self.assertEqual(status["status"], "succeeded")
            self.assertEqual(status["rows_written"], 5000)
            self.assertEqual(status["scope"], "daily_batch:2026-09-23")
            self.assertIsNotNone(status["finished_at"])


class StandardModuleTest(unittest.TestCase):
    def test_standardize_security(self):
        """证券标准化：上游字段映射；未知来源不补造。"""
        row = {"code": "sh.600000", "code_name": "浦发银行", "ipoDate": "1999-11-10"}
        std = standardize_security(row, source=SOURCE_BAOSTOCK, asset_type="stock")
        self.assertEqual(std["code"], "sh.600000")
        self.assertEqual(std["name"], "浦发银行")
        self.assertEqual(std["ipo_date"], "1999-11-10")
        self.assertEqual(std["source"], "baostock")
        self.assertEqual(std["asset_type"], "stock")

    def test_infer_asset_type(self):
        """资产类型推断：未知前缀返回 unknown。"""
        self.assertEqual(infer_asset_type("sh.600000"), "stock")
        self.assertEqual(infer_asset_type("sz.000001"), "stock")
        self.assertEqual(infer_asset_type(""), "unknown")
        self.assertEqual(infer_asset_type("xx.123456"), "unknown")

    def test_unknown_source_semantics(self):
        """历史未知来源标记 unknown，不补造精确时间。"""
        self.assertEqual(SOURCE_UNKNOWN, "unknown")


if __name__ == "__main__":
    unittest.main()
