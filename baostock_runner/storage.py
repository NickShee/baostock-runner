import json
import os
import sqlite3
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
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS cache (
                cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                created_at TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS usage (
                usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS daily_bars (
                series_key TEXT NOT NULL, bar_date TEXT NOT NULL, payload TEXT NOT NULL,
                updated_at TEXT NOT NULL, PRIMARY KEY(series_key, bar_date))""")

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def get_cache(self, key: str):
        with self._connect() as db:
            row = db.execute("SELECT payload FROM cache WHERE cache_key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put_cache(self, key: str, value: Any):
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO cache VALUES (?, ?, ?)", (key, json.dumps(value, ensure_ascii=False), now))

    def increment_usage(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as db:
            db.execute("INSERT INTO usage(usage_date, request_count) VALUES (?, 1) ON CONFLICT(usage_date) DO UPDATE SET request_count=request_count+1", (today,))
            return db.execute("SELECT request_count FROM usage WHERE usage_date=?", (today,)).fetchone()[0]

    def usage_today(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        with self._connect() as db:
            row = db.execute("SELECT request_count FROM usage WHERE usage_date=?", (today,)).fetchone()
        return row[0] if row else 0

    def audit(self, event: dict[str, Any]):
        event = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def get_daily_bars(self, series_key: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT payload FROM daily_bars WHERE series_key=? AND bar_date BETWEEN ? AND ? ORDER BY bar_date",
                (series_key, start_date, end_date),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def latest_daily_bar(self, series_key: str) -> str | None:
        with self._connect() as db:
            row = db.execute("SELECT MAX(bar_date) FROM daily_bars WHERE series_key=?", (series_key,)).fetchone()
        return row[0] if row and row[0] else None

    def put_daily_bars(self, series_key: str, rows: list[dict[str, Any]]):
        now = datetime.now(timezone.utc).isoformat()
        values = [(series_key, row["date"], json.dumps(row, ensure_ascii=False), now) for row in rows if row.get("date")]
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO daily_bars VALUES (?, ?, ?, ?)", values)
