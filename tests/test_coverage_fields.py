"""A-02: 日线字段提升、缺口覆盖检查、force_refresh、覆盖状态区分测试。

测试原则：全部使用临时目录与本地文件，不登录真实 BaoStock、不访问生产数据库。
验收点：
- schema v2 迁移：新库/旧库均有 7 个新字段列；旧数据保留。
- 窄字段请求不丢数据：字段级合并，缺失字段保留旧值。
- 覆盖检查识别头部/中间/尾部缺口，并合并为连续请求窗口。
- 覆盖状态区分 expected/effective/excluded/unknown；行情与估值字段分别统计。
- force_refresh 同时绕过参数缓存与事实表覆盖短路。
- 停牌合法记录（tradestatus 空/0）不被当作坏数据。
"""
import os
import sqlite3
import tempfile
import unittest

from baostock_runner.config import Settings
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import SCHEMA_VERSION, Storage


def _mk(db_path: str, log_path: str) -> Storage:
    return Storage(db_path=db_path, log_path=log_path)


NEW_COLUMNS = {"preclose", "tradestatus", "isST", "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM"}


def _table_columns(db_path: str, table: str) -> set[str]:
    db = sqlite3.connect(db_path)
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
    finally:
        db.close()


class SchemaV2MigrationTest(unittest.TestCase):
    def test_new_db_has_v2_columns(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "data.sqlite3")
            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            self.assertEqual(storage.integrity_check()["user_version"], SCHEMA_VERSION)
            cols = _table_columns(db_path, "daily_bars")
            self.assertTrue(NEW_COLUMNS.issubset(cols))

    def test_v1_db_migrates_to_v2_preserving_data(self):
        """v1 结构样本（无新列）迁移到 v2：列被添加、旧数据保留。"""
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "legacy.sqlite3")
            db = sqlite3.connect(db_path)
            db.execute("PRAGMA user_version = 1")
            db.execute("""CREATE TABLE daily_bars (
                code TEXT NOT NULL, bar_date TEXT NOT NULL,
                frequency TEXT NOT NULL, adjustflag TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, amount REAL, pct_chg REAL, turn REAL,
                raw_json TEXT, updated_at TEXT NOT NULL,
                PRIMARY KEY(code, bar_date, frequency, adjustflag))""")
            db.execute("""INSERT INTO daily_bars VALUES (
                'sh.600000', '2026-09-23', 'd', '3', 10, 11, 9, 10.5,
                1000, 5000, 1.2, 0.8, '{"close":10.5}', '2026-09-24T00:00:00+00:00')""")
            db.commit()
            db.close()

            storage = _mk(db_path, os.path.join(d, "audit.jsonl"))
            self.assertEqual(storage.integrity_check()["user_version"], SCHEMA_VERSION)
            cols = _table_columns(db_path, "daily_bars")
            self.assertTrue(NEW_COLUMNS.issubset(cols))
            bars = storage.get_daily_bars("sh.600000", "d", "3", "2026-09-23", "2026-09-23")
            self.assertEqual(len(bars), 1)
            self.assertEqual(bars[0]["close"], 10.5)
            self.assertIsNone(bars[0]["preclose"])


class FieldMergeTest(unittest.TestCase):
    def test_narrow_field_write_keeps_old_values(self):
        """窄字段请求不丢数据：先宽字段写，再窄字段写，旧字段保留。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"))
            storage.put_daily_bars("sh.600000", "d", "3", [{
                "date": "2026-09-23", "code": "sh.600000",
                "open": "10", "high": "11", "low": "9", "close": "10.5",
                "preclose": "10", "volume": "1000", "amount": "5000",
                "pctChg": "1.2", "turn": "0.8", "tradestatus": "1", "isST": "0",
                "peTTM": "12.3", "pbMRQ": "1.1", "psTTM": "2.2", "pcfNcfTTM": "3.3",
            }])
            storage.put_daily_bars("sh.600000", "d", "3", [{
                "date": "2026-09-23", "code": "sh.600000", "close": "10.8",
            }])
            bars = storage.get_daily_bars("sh.600000", "d", "3", "2026-09-23", "2026-09-23")
            self.assertEqual(len(bars), 1)
            row = bars[0]
            self.assertEqual(row["close"], 10.8)
            self.assertEqual(row["open"], 10)
            self.assertEqual(row["high"], 11)
            self.assertEqual(row["preclose"], 10)
            self.assertEqual(row["volume"], 1000)
            self.assertEqual(row["peTTM"], 12.3)
            self.assertEqual(row["tradestatus"], "1")

    def test_legal_empty_value_kept_as_none(self):
        """上游明确返回的合法空值（空串）按接口语义处理为 NULL，不保留旧值。"""
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"))
            storage.put_daily_bars("sh.600000", "d", "3", [{
                "date": "2026-09-23", "code": "sh.600000", "close": "10", "peTTM": "5",
            }])
            storage.put_daily_bars("sh.600000", "d", "3", [{
                "date": "2026-09-23", "code": "sh.600000", "close": "10", "peTTM": "",
            }])
            bars = storage.get_daily_bars("sh.600000", "d", "3", "2026-09-23", "2026-09-23")
            self.assertIsNone(bars[0]["peTTM"])


class CoverageGapTest(unittest.TestCase):
    TRADE_DATES = ["2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"]

    def _storage_with_sec(self, d, rows=None):
        storage = _mk(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"))
        storage.put_securities(rows or [
            {"code": "sh.600000", "name": "A", "status": "1"},
            {"code": "sz.000001", "name": "B", "status": "1"},
        ])
        return storage

    def test_head_middle_tail_gaps_merged(self):
        """头部/中间/尾部缺口识别并合并为连续窗口。"""
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage_with_sec(d)
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-22", "code": "sh.600000", "close": "10"},
                {"date": "2026-09-23", "code": "sh.600000", "close": "10.2"},
            ])
            cov = storage.get_daily_coverage(
                ["sh.600000"], "2026-09-21", "2026-09-24", "3",
                trade_dates=self.TRADE_DATES)
            pc = cov["per_code"]["sh.600000"]
            self.assertEqual(pc["state"], "effective")
            self.assertEqual(pc["missing_count"], 2)
            self.assertEqual(pc["gap_windows"],
                             [{"start": "2026-09-21", "end": "2026-09-21", "days": 1},
                              {"start": "2026-09-24", "end": "2026-09-24", "days": 1}])

    def test_middle_gap_only(self):
        """中间缺口识别。"""
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage_with_sec(d)
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-21", "code": "sh.600000", "close": "10"},
                {"date": "2026-09-23", "code": "sh.600000", "close": "10.2"},
                {"date": "2026-09-24", "code": "sh.600000", "close": "10.3"},
            ])
            cov = storage.get_daily_coverage(
                ["sh.600000"], "2026-09-21", "2026-09-24", "3",
                trade_dates=self.TRADE_DATES)
            pc = cov["per_code"]["sh.600000"]
            self.assertEqual(pc["gap_windows"],
                             [{"start": "2026-09-22", "end": "2026-09-22", "days": 1}])

    def test_status_complete_partial_unknown_excluded(self):
        with tempfile.TemporaryDirectory() as d:
            storage = _mk(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"))
            storage.put_securities([
                {"code": "sh.600000", "name": "A", "status": "1"},
                {"code": "sz.000001", "name": "B", "status": "D"},
            ])
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-21", "code": "sh.600000", "close": "10"},
                {"date": "2026-09-22", "code": "sh.600000", "close": "10.1"},
                {"date": "2026-09-23", "code": "sh.600000", "close": "10.2"},
                {"date": "2026-09-24", "code": "sh.600000", "close": "10.3"},
            ])
            cov = storage.get_daily_coverage(
                ["sh.600000", "sz.000001", "sz.000002"], "2026-09-21", "2026-09-24", "3",
                trade_dates=self.TRADE_DATES)
            self.assertEqual(cov["per_code"]["sh.600000"]["state"], "effective")
            self.assertEqual(cov["per_code"]["sz.000001"]["state"], "excluded")
            self.assertEqual(cov["per_code"]["sz.000002"]["state"], "unknown")
            self.assertEqual(cov["status"], "unknown")
            self.assertEqual(cov["effective"], 1)
            self.assertEqual(cov["excluded"], 1)
            self.assertEqual(cov["unknown"], 1)

    def test_market_vs_valuation_effective_separate(self):
        """行情与估值字段分别统计有效数。"""
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage_with_sec(d)
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-21", "code": "sh.600000", "close": "10"},
            ])
            storage.put_daily_bars("sz.000001", "d", "3", [
                {"date": "2026-09-21", "code": "sz.000001", "close": "20", "peTTM": "8", "pbMRQ": "1.5"},
            ])
            cov = storage.get_daily_coverage(
                ["sh.600000", "sz.000001"], "2026-09-21", "2026-09-24", "3",
                trade_dates=self.TRADE_DATES)
            self.assertEqual(cov["market_effective"], 2)
            self.assertEqual(cov["valuation_effective"], 1)

    def test_suspension_not_treated_as_bad_data(self):
        """停牌合法记录（tradestatus 空/0）不被当作坏数据：仍计入 effective。"""
        with tempfile.TemporaryDirectory() as d:
            storage = self._storage_with_sec(d)
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-21", "code": "sh.600000", "close": "10",
                 "volume": "0", "tradestatus": "0"},
            ])
            cov = storage.get_daily_coverage(
                ["sh.600000"], "2026-09-21", "2026-09-24", "3",
                trade_dates=self.TRADE_DATES)
            self.assertEqual(cov["per_code"]["sh.600000"]["state"], "effective")
            self.assertEqual(cov["status"], "partial")


class ForceRefreshTest(unittest.TestCase):
    def test_force_refresh_bypasses_fact_table_shortcircuit(self):
        """force_refresh 绕过事实表"已覆盖"短路：本地已到 end_date 仍全区间重查。"""
        with tempfile.TemporaryDirectory() as d:
            settings = Settings(
                db_path=os.path.join(d, "data.sqlite3"),
                log_path=os.path.join(d, "audit.jsonl"),
                min_interval_seconds=0, daily_hard_limit=1000, offline=True)
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-21", "end_date": "2026-09-24",
                          "frequency": "d", "adjustflag": "3"}
                r1 = gateway.daily_bars(params)
                self.assertFalse(r1["cache_hit"])
                usage_after_first = gateway.storage.usage_today()
                r2 = gateway.daily_bars(params)
                self.assertTrue(r2["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), usage_after_first)
                r3 = gateway.daily_bars(params, force_refresh=True)
                self.assertFalse(r3["cache_hit"])
                self.assertTrue(r3["force_refresh"])
                self.assertEqual(gateway.storage.usage_today(), usage_after_first + 1)
            finally:
                gateway.close()

    def test_force_refresh_keeps_old_fields(self):
        """force_refresh 使用字段级合并：重查窄字段不丢已有估值字段。"""
        with tempfile.TemporaryDirectory() as d:
            settings = Settings(
                db_path=os.path.join(d, "data.sqlite3"),
                log_path=os.path.join(d, "audit.jsonl"),
                min_interval_seconds=0, daily_hard_limit=1000, offline=True)
            gateway = BaoStockGateway(settings)
            try:
                # 首次宽字段请求（offline 模式返回模拟行，只含 fields 所列字段）
                wide_params = {"code": "sh.600000",
                               "fields": "date,code,close,peTTM,pbMRQ",
                               "start_date": "2026-09-21", "end_date": "2026-09-21",
                               "frequency": "d", "adjustflag": "3"}
                r1 = gateway.daily_bars(wide_params)
                self.assertFalse(r1["cache_hit"])
                # 窄字段强制刷新：只查 close
                narrow_params = {"code": "sh.600000",
                                 "fields": "date,code,close",
                                 "start_date": "2026-09-21", "end_date": "2026-09-21",
                                 "frequency": "d", "adjustflag": "3"}
                r2 = gateway.daily_bars(narrow_params, force_refresh=True)
                self.assertFalse(r2["cache_hit"])
                # 本地仍保留估值字段（字段级合并未清空）
                bars = gateway.storage.get_daily_bars(
                    "sh.600000", "d", "3", "2026-09-21", "2026-09-21")
                self.assertEqual(len(bars), 1)
                self.assertEqual(bars[0]["close"], 10.0)
                self.assertIsNotNone(bars[0]["peTTM"])
                self.assertIsNotNone(bars[0]["pbMRQ"])
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
