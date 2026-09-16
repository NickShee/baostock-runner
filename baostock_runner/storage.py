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
