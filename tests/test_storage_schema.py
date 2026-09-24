"""B-01: schema 版本与顺序迁移、SQLite 一致性备份与隔离恢复测试。

测试原则：全部使用临时目录与本地文件，不登录真实 BaoStock、不访问生产数据库。
验收点：
- 新库初始化后 user_version == SCHEMA_VERSION，全部事实表存在。
- 旧结构样本（daily_bars payload 旧表 + 无新表）迁移后数据保留；迁移两次结果一致（幂等）。
- 未知更高 schema 版本（user_version > SCHEMA_VERSION）启动时拒绝写入。
- 一致性备份 integrity_check 通过；隔离恢复到新路径后完整性、行数及关键字段对账通过。
"""
import os
import sqlite3
import tempfile
import unittest

from baostock_runner.storage import SCHEMA_VERSION, Storage

EXPECTED_TABLES = {
    "cache", "usage", "daily_bars", "download_usage", "securities",
    "trade_calendar", "index_constituents", "financials", "dividends",
    "adjust_factors", "stock_industry", "download_jobs", "dataset_meta",
}


def _mk(db_path: str, log_path: str) -> Storage:
    return Storage(db_path=db_path, log_path=log_path)


class SchemaVersionTest(unittest.TestCase):
    def test_new_db_has_current_schema_version(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite3")
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            self.assertEqual(storage.integrity_check()["user_version"], SCHEMA_VERSION)
            self.assertTrue(storage.integrity_check()["ok"])
            self.assertTrue(EXPECTED_TABLES.issubset(set(storage.list_tables())))

    def test_legacy_sample_migrates_twice_consistently(self):
        """旧结构样本（daily_bars payload 旧表 + 无新表）迁移两次结果一致。"""
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "legacy.sqlite3")
            # 构造旧结构库：只有 cache/usage + 旧 daily_bars(payload, 无 code 列)
            db = sqlite3.connect(db_path)
            db.execute("""CREATE TABLE cache (
                cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL)""")
            db.execute("""CREATE TABLE usage (
                usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE daily_bars (
                bar_date TEXT, frequency TEXT, adjustflag TEXT,
                payload TEXT, updated_at TEXT NOT NULL)""")
            db.execute("INSERT INTO cache VALUES ('k1', '{}', '2026-09-24T00:00:00+00:00')")
            db.execute("INSERT INTO usage VALUES ('2026-09-24', 3)")
            db.execute("INSERT INTO daily_bars VALUES ('2026-09-23', 'd', '3', '{\"close\":10}', '2026-09-24T00:00:00+00:00')")
            db.commit()
            db.close()

            # 第一次迁移
            storage1 = _mk(db_path, os.path.join(d, "audit1.jsonl"))
            v1 = storage1.integrity_check()
            self.assertEqual(v1["user_version"], SCHEMA_VERSION)
            self.assertTrue(v1["ok"])
            # 旧表被重命名保留，数据未丢失
            self.assertIn("daily_bars_legacy", v1["tables"])
            self.assertEqual(v1["rows"]["daily_bars_legacy"], 1)
            self.assertEqual(v1["rows"]["cache"], 1)
            self.assertEqual(v1["rows"]["usage"], 1)
            # 新表已建立
            self.assertEqual(v1["rows"]["securities"], 0)
            self.assertTrue(EXPECTED_TABLES.issubset(set(v1["tables"])))

            # 第二次迁移（重新打开同一库）：结果一致，不重复执行
            storage2 = _mk(db_path, os.path.join(d, "audit2.jsonl"))
            v2 = storage2.integrity_check()
            self.assertEqual(v1["rows"], v2["rows"])
            self.assertEqual(v1["tables"], v2["tables"])
            self.assertEqual(v2["user_version"], SCHEMA_VERSION)

    def test_newer_schema_version_refuses_write(self):
        """未知更高 schema 版本启动时拒绝写入。"""
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "future.sqlite3")
            db = sqlite3.connect(db_path)
            db.execute("PRAGMA user_version = %d" % (SCHEMA_VERSION + 1))
            db.commit()
            db.close()
            with self.assertRaisesRegex(RuntimeError, "newer than supported"):
                _mk(db_path, os.path.join(d, "audit.jsonl"))

    def test_daily_bars_legacy_data_preserved_after_migration(self):
        """旧 daily_bars payload 表迁移后数据可读（raw_json 保留原字段）。"""
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "legacy2.sqlite3")
            db = sqlite3.connect(db_path)
            db.execute("""CREATE TABLE daily_bars (
                bar_date TEXT, frequency TEXT, adjustflag TEXT,
                payload TEXT, updated_at TEXT NOT NULL)""")
            db.execute("INSERT INTO daily_bars VALUES ('2026-09-23', 'd', '3', '{\"close\":10}', '2026-09-24T00:00:00+00:00')")
            db.commit()
            db.close()
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            # legacy 表数据完整
            rows = storage.verify_backup(db_path)["rows"]
            self.assertEqual(rows.get("daily_bars_legacy", 0), 1)


class BackupRestoreTest(unittest.TestCase):
    def test_backup_consistent_and_restore_reconciled(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite3")
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            storage.put_securities([
                {"code": "sh.600000", "name": "浦发银行", "status": "1"},
                {"code": "sz.000001", "name": "平安银行", "status": "1"},
            ])
            storage.put_daily_bars("sh.600000", "d", "3",
                                   [{"date": "2026-09-23", "code": "sh.600000", "close": "10.5", "pctChg": "1.2"}])
            before = storage.integrity_check()

            backup_path = os.path.join(d, "backup.sqlite3")
            backup_info = storage.backup_to(backup_path)
            self.assertTrue(backup_info["ok"])
            self.assertEqual(backup_info["user_version"], SCHEMA_VERSION)
            # 备份行数与源库一致
            self.assertEqual(backup_info["rows"]["securities"], 2)
            self.assertEqual(backup_info["rows"]["daily_bars"], 1)

            # 污染源库：删除数据
            with storage._session() as db:
                db.execute("DELETE FROM securities")
                db.execute("DELETE FROM daily_bars")
            polluted = storage.integrity_check()
            self.assertEqual(polluted["rows"]["securities"], 0)
            self.assertEqual(polluted["rows"]["daily_bars"], 0)

            # 隔离恢复到新路径（不覆盖当前库）
            restored_path = os.path.join(d, "restored.sqlite3")
            restored_info = storage.restore_from(backup_path, target_path=restored_path)
            self.assertTrue(restored_info["ok"])
            self.assertEqual(restored_info["user_version"], SCHEMA_VERSION)
            # 行数对账：与备份一致
            self.assertEqual(restored_info["rows"]["securities"], 2)
            self.assertEqual(restored_info["rows"]["daily_bars"], 1)
            # 关键字段对账
            restored = _mk(restored_path, os.path.join(d, "audit-restored.jsonl"))
            bars = restored.get_daily_bars("sh.600000", "d", "3", "2026-09-23", "2026-09-23")
            self.assertEqual(len(bars), 1)
            self.assertEqual(bars[0]["close"], 10.5)
            self.assertEqual(bars[0]["pctChg"], 1.2)
            secs = restored.get_securities()
            self.assertEqual(len(secs), 2)
            # 源库保持污染状态未被恢复覆盖（隔离恢复）
            self.assertEqual(storage.count_securities(), 0)

    def test_restore_from_invalid_backup_fails(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite3")
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            bad = os.path.join(d, "bad.sqlite3")
            with open(bad, "w") as f:
                f.write("this is not a sqlite database")
            with self.assertRaises(Exception):
                storage.restore_from(bad, target_path=os.path.join(d, "out.sqlite3"))

    def test_integrity_check_reports_ok_on_healthy_db(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite3")
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            storage.put_securities([{"code": "sh.600000", "name": "浦发", "status": "1"}])
            info = storage.integrity_check()
            self.assertTrue(info["ok"])
            self.assertEqual(info["integrity"], "ok")
            self.assertEqual(info["rows"]["securities"], 1)


if __name__ == "__main__":
    unittest.main()
