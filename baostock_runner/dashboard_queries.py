"""Read-only query helpers used by the dashboard HTTP API.

The dashboard deliberately reads SQLite through :class:`Storage` and never
opens a BaoStock connection of its own.  Remote refreshes remain a separate,
explicit gateway operation in ``dashboard_api.py``.
"""

from typing import Any

from .storage import Storage


class DashboardQueries:
    def __init__(self, storage: Storage):
        self.storage = storage

    def search_securities(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        query = query.strip()
        if not query:
            return []
        limit = max(1, min(limit, 50))
        pattern = f"%{query}%"
        with self.storage._session() as db:
            rows = db.execute(
                """SELECT code, name, trade_status, ipo_date, out_date, type, status
                   FROM securities
                   WHERE code LIKE ? OR name LIKE ?
                   ORDER BY CASE WHEN code=? THEN 0 ELSE 1 END, code
                   LIMIT ?""",
                (pattern, pattern, query, limit),
            ).fetchall()
        keys = ("code", "name", "trade_status", "ipo_date", "out_date", "type", "status")
        return [dict(zip(keys, row)) for row in rows]

    def daily_coverage(self, code: str, frequency: str, adjustflag: str) -> dict[str, Any]:
        with self.storage._session() as db:
            row = db.execute(
                """SELECT COUNT(*), MIN(bar_date), MAX(bar_date), MAX(updated_at)
                   FROM daily_bars WHERE code=? AND frequency=? AND adjustflag=?""",
                (code, frequency, adjustflag),
            ).fetchone()
        return {
            "rows": row[0] or 0,
            "start_date": row[1],
            "end_date": row[2],
            "updated_at": row[3],
        }

    def coverage(self) -> dict[str, Any]:
        with self.storage._session() as db:
            daily = db.execute(
                """SELECT COUNT(*), COUNT(DISTINCT code), MIN(bar_date), MAX(bar_date),
                          MAX(updated_at) FROM daily_bars"""
            ).fetchone()
            financial = db.execute(
                """SELECT COUNT(*), COUNT(DISTINCT code), MIN(year), MAX(year),
                          MAX(fetched_at) FROM financials"""
            ).fetchone()
            daily_adjust = db.execute(
                """SELECT adjustflag, COUNT(*), COUNT(DISTINCT code), MIN(bar_date), MAX(bar_date)
                   FROM daily_bars GROUP BY adjustflag ORDER BY adjustflag"""
            ).fetchall()
            financial_datasets = db.execute(
                """SELECT dataset, COUNT(*), COUNT(DISTINCT code), MIN(year), MAX(year)
                   FROM financials GROUP BY dataset ORDER BY dataset"""
            ).fetchall()

        latest_profit = self.storage.latest_financial_period("profit")
        return {
            "securities": self.storage.count_securities(),
            "trade_calendar_days": self.storage.count_rows("trade_calendar"),
            "daily_bars": {
                "rows": daily[0] or 0,
                "codes": daily[1] or 0,
                "start_date": daily[2],
                "end_date": daily[3],
                "updated_at": daily[4],
                "by_adjustflag": [
                    {"adjustflag": r[0], "rows": r[1], "codes": r[2], "start_date": r[3], "end_date": r[4]}
                    for r in daily_adjust
                ],
            },
            "financials": {
                "rows": financial[0] or 0,
                "codes": financial[1] or 0,
                "start_year": financial[2],
                "end_year": financial[3],
                "updated_at": financial[4],
                "latest_profit_period": (
                    {"year": latest_profit[0], "quarter": latest_profit[1]}
                    if latest_profit else None
                ),
                "by_dataset": [
                    {"dataset": r[0], "rows": r[1], "codes": r[2], "start_year": r[3], "end_year": r[4]}
                    for r in financial_datasets
                ],
            },
            "other": {
                "industry_rows": self.storage.count_rows("stock_industry"),
                "dividends_rows": self.storage.count_rows("dividends"),
                "adjust_factors_rows": self.storage.count_rows("adjust_factors"),
                "index_constituents_rows": self.storage.count_rows("index_constituents"),
            },
        }

    def job_errors(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 100))
        with self.storage._session() as db:
            rows = db.execute(
                """SELECT dataset, batch_id, status, attempts, error, updated_at
                   FROM download_jobs WHERE status='failed' OR error IS NOT NULL
                   ORDER BY updated_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        keys = ("dataset", "batch_id", "status", "attempts", "error", "updated_at")
        return [dict(zip(keys, row)) for row in rows]
