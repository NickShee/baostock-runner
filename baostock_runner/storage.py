import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any


class Storage:
    def __init__(self, db_path: str, log_path: str):
        self.db_path, self.log_path = db_path, log_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        log_parent = os.path.dirname(log_path)
        if log_parent:
            os.makedirs(log_parent, exist_ok=True)
        with self._session() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA busy_timeout=30000")
            db.execute("""CREATE TABLE IF NOT EXISTS cache (
                cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                created_at TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS usage (
                usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""")
            self._ensure_daily_bars_schema(db)
            self._ensure_fetch_schema(db)

    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=30)
        db.execute("PRAGMA busy_timeout=30000")
        return db

    @contextmanager
    def _session(self):
        db = self._connect()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _ensure_daily_bars_schema(self, db):
        columns = {row[1] for row in db.execute("PRAGMA table_info(daily_bars)")}
        if columns and "payload" in columns and "code" not in columns:
            db.execute("ALTER TABLE daily_bars RENAME TO daily_bars_legacy")
        db.execute("""CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT NOT NULL,
            bar_date TEXT NOT NULL,
            frequency TEXT NOT NULL,
            adjustflag TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            pct_chg REAL,
            turn REAL,
            raw_json TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(code, bar_date, frequency, adjustflag))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_daily_bars_code_date ON daily_bars(code, bar_date)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_daily_bars_date ON daily_bars(bar_date)")

    def _ensure_fetch_schema(self, db):
        """P1 background fetcher schema: fact tables + job/usage/metadata bookkeeping."""
        db.execute("""CREATE TABLE IF NOT EXISTS download_usage (
            usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""")
        db.execute("""CREATE TABLE IF NOT EXISTS securities (
            code TEXT PRIMARY KEY,
            name TEXT, trade_status TEXT,
            ipo_date TEXT, out_date TEXT,
            type TEXT, status TEXT,
            updated_at TEXT NOT NULL)""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_securities_status ON securities(status)")
        db.execute("""CREATE TABLE IF NOT EXISTS trade_calendar (
            calendar_date TEXT PRIMARY KEY, is_trading_day INTEGER NOT NULL)""")
        db.execute("""CREATE TABLE IF NOT EXISTS index_constituents (
            index_code TEXT NOT NULL, asof_date TEXT NOT NULL,
            code TEXT NOT NULL, name TEXT, fetched_at TEXT NOT NULL,
            PRIMARY KEY(index_code, asof_date, code))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_index_constituents_code ON index_constituents(index_code, asof_date)")
        db.execute("""CREATE TABLE IF NOT EXISTS financials (
            dataset TEXT NOT NULL, code TEXT NOT NULL,
            year INTEGER NOT NULL, quarter INTEGER NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(dataset, code, year, quarter))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_financials_code ON financials(code)")
        db.execute("""CREATE TABLE IF NOT EXISTS dividends (
            code TEXT NOT NULL, year INTEGER NOT NULL, year_type TEXT NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, year, year_type))""")
        db.execute("""CREATE TABLE IF NOT EXISTS adjust_factors (
            code TEXT NOT NULL, factor_date TEXT NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, factor_date))""")
        db.execute("""CREATE TABLE IF NOT EXISTS stock_industry (
            code TEXT NOT NULL, industry TEXT NOT NULL,
            classification TEXT, update_date TEXT, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, industry, classification))""")
        db.execute("""CREATE TABLE IF NOT EXISTS download_jobs (
            dataset TEXT NOT NULL, batch_id TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT, updated_at TEXT NOT NULL,
            PRIMARY KEY(dataset, batch_id))""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs(dataset, status)")
        db.execute("""CREATE TABLE IF NOT EXISTS dataset_meta (
            dataset TEXT PRIMARY KEY, last_updated TEXT, detail TEXT)""")

    def get_cache(self, key: str):
        with self._session() as db:
            row = db.execute("SELECT payload FROM cache WHERE cache_key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put_cache(self, key: str, value: Any):
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as db:
            db.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, ?)", (key, json.dumps(value, ensure_ascii=False), now))

    def increment_usage(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._session() as db:
            db.execute("INSERT INTO usage(usage_date, request_count) VALUES (?, 1) ON CONFLICT(usage_date) DO UPDATE SET request_count=request_count+1", (today,))
            return db.execute("SELECT request_count FROM usage WHERE usage_date=?", (today,)).fetchone()[0]

    def usage_today(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._session() as db:
            row = db.execute("SELECT request_count FROM usage WHERE usage_date=?", (today,)).fetchone()
        return row[0] if row else 0

    def audit(self, event: dict[str, Any]):
        event = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def get_daily_bars(self, code: str, frequency: str, adjustflag: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                """SELECT code, bar_date, frequency, adjustflag, open, high, low,
                   close, volume, amount, pct_chg, turn
                   FROM daily_bars
                   WHERE code=? AND frequency=? AND adjustflag=?
                     AND bar_date BETWEEN ? AND ? ORDER BY bar_date""",
                (code, frequency, adjustflag, start_date, end_date),
            ).fetchall()
        keys = ("code", "date", "frequency", "adjustflag", "open", "high", "low", "close", "volume", "amount", "pctChg", "turn")
        return [dict(zip(keys, row)) for row in rows]

    def latest_daily_bar(self, code: str, frequency: str, adjustflag: str) -> str | None:
        with self._session() as db:
            row = db.execute(
                "SELECT MAX(bar_date) FROM daily_bars WHERE code=? AND frequency=? AND adjustflag=?",
                (code, frequency, adjustflag),
            ).fetchone()
        return row[0] if row and row[0] else None

    def put_daily_bars(self, code: str, frequency: str, adjustflag: str, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = []
        for row in rows:
            if not row.get("date"):
                continue
            values.append((
                code, row["date"], frequency, adjustflag,
                self._number(row.get("open")), self._number(row.get("high")),
                self._number(row.get("low")), self._number(row.get("close")),
                self._number(row.get("volume")), self._number(row.get("amount")),
                self._number(row.get("pctChg")), self._number(row.get("turn")),
                json.dumps(row, ensure_ascii=False), now,
            ))
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO daily_bars
                (code, bar_date, frequency, adjustflag, open, high, low, close,
                 volume, amount, pct_chg, turn, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", values)

    @staticmethod
    def _number(value):
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # ---------- generic helpers ----------

    def list_tables(self) -> list[str]:
        with self._session() as db:
            rows = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def count_rows(self, table: str) -> int:
        with self._session() as db:
            row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return row[0] if row else 0

    def set_meta(self, dataset: str, detail: str | None = None):
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as db:
            db.execute("""INSERT INTO dataset_meta(dataset, last_updated, detail)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset) DO UPDATE SET last_updated=excluded.last_updated, detail=excluded.detail""",
                (dataset, now, detail or ""))

    def get_meta(self, dataset: str) -> dict[str, str] | None:
        with self._session() as db:
            row = db.execute("SELECT dataset, last_updated, detail FROM dataset_meta WHERE dataset=?", (dataset,)).fetchone()
        return {"dataset": row[0], "last_updated": row[1], "detail": row[2]} if row else None

    # ---------- download budget (fetcher only; MCP keeps using usage table) ----------

    def increment_download_usage(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._session() as db:
            db.execute("""INSERT INTO download_usage(usage_date, request_count) VALUES (?, 1)
                ON CONFLICT(usage_date) DO UPDATE SET request_count=request_count+1""", (today,))
            return db.execute("SELECT request_count FROM download_usage WHERE usage_date=?", (today,)).fetchone()[0]

    def download_usage_today(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._session() as db:
            row = db.execute("SELECT request_count FROM download_usage WHERE usage_date=?", (today,)).fetchone()
        return row[0] if row else 0

    # ---------- download jobs (断点续传) ----------

    def job_upsert(self, dataset: str, batch_id: str, status: str, error: str | None = None):
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as db:
            db.execute("""INSERT INTO download_jobs(dataset, batch_id, status, attempts, error, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(dataset, batch_id) DO UPDATE SET
                    status=excluded.status,
                    attempts=download_jobs.attempts+1,
                    error=excluded.error,
                    updated_at=excluded.updated_at""",
                (dataset, batch_id, status, error, now))

    def job_status(self, dataset: str, batch_id: str) -> str | None:
        with self._session() as db:
            row = db.execute("SELECT status FROM download_jobs WHERE dataset=? AND batch_id=?", (dataset, batch_id)).fetchone()
        return row[0] if row else None

    def job_stats(self, dataset: str | None = None) -> dict[str, int]:
        with self._session() as db:
            if dataset:
                rows = db.execute("SELECT status, COUNT(*) FROM download_jobs WHERE dataset=? GROUP BY status", (dataset,)).fetchall()
            else:
                rows = db.execute("SELECT status, COUNT(*) FROM download_jobs GROUP BY status").fetchall()
        stats = {"done": 0, "pending": 0, "failed": 0, "total": 0}
        for status, count in rows:
            stats[status] = count
            stats["total"] += count
        return stats

    # ---------- securities ----------

    def put_securities(self, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = [(
            r.get("code"), r.get("name", ""), r.get("trade_status", ""),
            r.get("ipo_date", ""), r.get("out_date", ""), r.get("type", ""),
            r.get("status", ""), now,
        ) for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO securities
                (code, name, trade_status, ipo_date, out_date, type, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", values)

    def get_securities(self) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                """SELECT code, name, trade_status, ipo_date, out_date, type, status
                   FROM securities ORDER BY code""").fetchall()
        keys = ("code", "name", "trade_status", "ipo_date", "out_date", "type", "status")
        return [dict(zip(keys, row)) for row in rows]

    def get_securities_codes(self) -> list[str]:
        with self._session() as db:
            rows = db.execute("SELECT code FROM securities WHERE status IN ('', '1') ORDER BY code").fetchall()
        return [r[0] for r in rows]

    def count_securities(self) -> int:
        return self.count_rows("securities")

    # ---------- trade calendar ----------

    def put_trade_calendar(self, rows: list[dict[str, Any]]):
        values = [(r.get("calendar_date"), int(r.get("is_trading_day", 0))) for r in rows if r.get("calendar_date")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO trade_calendar(calendar_date, is_trading_day)
                VALUES (?, ?)""", values)

    def latest_trade_date(self, on_or_before: str) -> str | None:
        with self._session() as db:
            row = db.execute(
                "SELECT MAX(calendar_date) FROM trade_calendar WHERE is_trading_day=1 AND calendar_date <= ?",
                (on_or_before,)).fetchone()
        return row[0] if row and row[0] else None

    # ---------- index constituents ----------

    def put_index_constituents(self, index_code: str, asof_date: str, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = [(index_code, asof_date, r.get("code"), r.get("name", ""), now) for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO index_constituents
                (index_code, asof_date, code, name, fetched_at) VALUES (?, ?, ?, ?, ?)""", values)

    def get_index_codes(self, index_code: str, asof_date: str | None = None) -> list[str]:
        with self._session() as db:
            if asof_date:
                rows = db.execute(
                    "SELECT code FROM index_constituents WHERE index_code=? AND asof_date=?",
                    (index_code, asof_date)).fetchall()
            else:
                rows = db.execute(
                    "SELECT code FROM index_constituents WHERE index_code=? ORDER BY asof_date DESC LIMIT 500",
                    (index_code,)).fetchall()
        return [r[0] for r in rows]

    # ---------- financials ----------

    def put_financials(self, dataset: str, code: str, year: int, quarter: int, payload: dict[str, Any]):
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as db:
            db.execute("""INSERT OR REPLACE INTO financials(dataset, code, year, quarter, payload, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)""", (dataset, code, year, quarter, json.dumps(payload, ensure_ascii=False), now))

    def financial_exists(self, dataset: str, code: str, year: int, quarter: int) -> bool:
        with self._session() as db:
            row = db.execute("SELECT 1 FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
                (dataset, code, year, quarter)).fetchone()
        return row is not None

    def get_financials(self, dataset: str, code: str) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                "SELECT year, quarter, payload FROM financials WHERE dataset=? AND code=? ORDER BY year DESC, quarter DESC",
                (dataset, code)).fetchall()
        return [{"year": r[0], "quarter": r[1], "data": json.loads(r[2])} for r in rows]

    def latest_financial_period(self, dataset: str) -> tuple[int, int] | None:
        with self._session() as db:
            row = db.execute(
                "SELECT year, quarter FROM financials WHERE dataset=? ORDER BY year DESC, quarter DESC LIMIT 1",
                (dataset,)).fetchone()
        return (row[0], row[1]) if row else None

    # ---------- dividends / adjust factors / industry ----------

    def put_dividend(self, code: str, year: int, year_type: str, payload: Any):
        now = datetime.now(timezone.utc).isoformat()
        with self._session() as db:
            db.execute("""INSERT OR REPLACE INTO dividends(code, year, year_type, payload, fetched_at)
                VALUES (?, ?, ?, ?, ?)""", (code, year, year_type, json.dumps(payload, ensure_ascii=False), now))

    def dividend_exists(self, code: str, year: int, year_type: str) -> bool:
        with self._session() as db:
            row = db.execute("SELECT 1 FROM dividends WHERE code=? AND year=? AND year_type=?",
                (code, year, year_type)).fetchone()
        return row is not None

    def put_adjust_factors(self, code: str, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = [(code, r.get("date", ""), json.dumps(r, ensure_ascii=False), now) for r in rows if r.get("date")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO adjust_factors(code, factor_date, payload, fetched_at)
                VALUES (?, ?, ?, ?)""", values)

    def put_stock_industry(self, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = [(r.get("code"), r.get("industry", ""), r.get("classification", ""), r.get("update_date", ""), now)
                  for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO stock_industry
                (code, industry, classification, update_date, fetched_at) VALUES (?, ?, ?, ?, ?)""", values)

    # ---------- daily backfill pending selection (断点续传 + 当日去重) ----------

    def get_daily_pending_codes(self, limit: int, end_date: str, primary_adjustflag: str) -> list[str]:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._session() as db:
            rows = db.execute("""
                SELECT s.code FROM securities s
                LEFT JOIN (SELECT code, MAX(bar_date) AS m FROM daily_bars
                           WHERE frequency='d' AND adjustflag=? GROUP BY code) b
                  ON s.code = b.code
                LEFT JOIN download_jobs j
                  ON j.dataset='daily_bars' AND j.batch_id = s.code || '|' || ?
                WHERE s.status IN ('', '1')
                  AND (b.m IS NULL OR b.m < ?)
                  -- SQL 三值逻辑：LEFT JOIN 无匹配时 j.* 为 NULL，必须显式放行
                  AND (j.batch_id IS NULL OR NOT (j.status='done' AND substr(j.updated_at, 1, 10) = ?))
                ORDER BY s.code
                LIMIT ?""", (primary_adjustflag, primary_adjustflag, end_date, today, limit)).fetchall()
        return [r[0] for r in rows]
