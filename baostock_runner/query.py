"""Shared, local-first query contracts for MCP and HTTP."""
from __future__ import annotations

from datetime import date
import base64
import binascii
import re
from typing import Any

CODE = re.compile(r"(?:sh|sz|bj)\.\d{6}\Z")
DAILY_FIELDS = frozenset("date code open high low close preclose volume amount pctChg turn tradestatus isST peTTM pbMRQ psTTM pcfNcfTTM".split())
FINANCIAL_DATASETS = frozenset({"profit", "growth", "balance", "cash_flow", "operation", "dupont"})


def valid_code(code: str) -> str:
    if not CODE.fullmatch(code):
        raise ValueError("invalid stock code")
    return code


def valid_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("date must use a real YYYY-MM-DD date") from exc
    if parsed.isoformat() != value:
        raise ValueError("date must use YYYY-MM-DD")
    return value


def page(rows: list[dict], limit: int | None, cursor: str | None = None) -> tuple[list[dict], str | None]:
    if limit is None:
        return rows, None
    if not 1 <= limit <= 5000:
        raise ValueError("limit must be between 1 and 5000")
    try:
        offset = int(base64.urlsafe_b64decode(cursor.encode()).decode()) if cursor else 0
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise ValueError("invalid cursor") from exc
    if offset < 0:
        raise ValueError("invalid cursor")
    next_offset = offset + limit
    return rows[offset:next_offset], (base64.urlsafe_b64encode(str(next_offset).encode()).decode() if next_offset < len(rows) else None)


class QueryService:
    def __init__(self, gateway):
        self.gateway = gateway
        self.storage = gateway.storage

    def daily(self, code: str, start_date: str, end_date: str, adjust: str = "none",
              fields: str = "date,code,open,high,low,close,volume,amount,pctChg,turn",
              limit: int | None = None, cursor: str | None = None) -> dict[str, Any]:
        valid_code(code)
        valid_date(start_date); valid_date(end_date)
        if start_date > end_date:
            raise ValueError("start_date must not be later than end_date")
        flags = {"none": "3", "forward": "2", "backward": "1"}
        if adjust not in flags:
            raise ValueError("adjust must be none, forward, or backward")
        selected = [f.strip() for f in fields.split(",") if f.strip()]
        if not selected or set(selected) - DAILY_FIELDS:
            raise ValueError("invalid daily fields")
        rows = self.storage.get_daily_bars(code, "d", flags[adjust], start_date, end_date)
        coverage = self.storage.get_daily_coverage([code], start_date, end_date, flags[adjust])
        rows, next_cursor = page(rows, limit, cursor)
        meta = {"source": "local_sqlite", "coverage": coverage, "stale": False,
                "date_range": {"start": start_date, "end": end_date},
                "adjust": adjust, "next_cursor": next_cursor}
        return {"rows": rows, "data": rows, "count": len(rows), "coverage": coverage,
                "next_cursor": next_cursor, "metadata": meta}

    def financials(self, code: str, dataset: str, limit: int | None = None,
                   cursor: str | None = None) -> dict[str, Any]:
        valid_code(code)
        if dataset not in FINANCIAL_DATASETS:
            raise ValueError("invalid financial dataset")
        rows, next_cursor = page(self.storage.get_financials(dataset, code), limit, cursor)
        return {"rows": rows, "data": rows, "count": len(rows), "next_cursor": next_cursor,
                "metadata": {"source": "local_sqlite", "dataset": dataset, "next_cursor": next_cursor}}

    def stocks(self, query: str = "", limit: int = 500, cursor: str | None = None) -> dict[str, Any]:
        if len(query) > 80:
            raise ValueError("query too long")
        rows = [r for r in self.storage.get_securities()
                if query.lower() in (r.get("code") or "").lower() or query.lower() in (r.get("name") or "").lower()]
        rows, next_cursor = page(rows, limit, cursor)
        return {"data": rows, "count": len(rows), "next_cursor": next_cursor,
                "metadata": {"source": "local_sqlite", "next_cursor": next_cursor}}

    def latest(self, codes: list[str], adjust: str = "none") -> dict[str, Any]:
        if not codes or len(codes) > 50:
            raise ValueError("codes must contain between 1 and 50 stocks")
        today = self.gateway.clock.business_date().isoformat()
        rows = []
        columns = ["date", "code", "open", "high", "low", "close", "volume", "amount", "pctChg", "turn"]
        for code in codes:
            valid_code(code)
            flag = {"none": "3", "forward": "2", "backward": "1"}.get(adjust)
            if flag is None:
                raise ValueError("adjust must be none, forward, or backward")
            with self.storage._session() as db:
                row = db.execute("SELECT MAX(bar_date) FROM daily_bars WHERE code=? AND frequency='d' AND adjustflag=? AND bar_date<=?",
                                 (code, flag, today)).fetchone()
            latest_date = row[0] if row and row[0] else today
            result = self.daily(code, latest_date, latest_date, adjust)
            latest = result["rows"][-1:] if result["rows"] else []
            rows.append({"code": code, "frequency": "d", "adjustflag": flag,
                         "columns": columns, "rows": [[row.get(key) for key in columns] for row in latest],
                         "data": latest, "cache_hit": True, "incremental": True,
                         "metadata": result["metadata"]})
        return {"data": rows, "count": len(rows), "metadata": {"source": "local_sqlite"}}
