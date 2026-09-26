"""Local, versioned research runs. No BaoStock calls are made here."""
from __future__ import annotations

from datetime import date, timedelta
import json
import math
import statistics
import uuid
from typing import Any

from .query import valid_code, valid_date

RULE_VERSION = "baseline_v1"


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _number(value: Any) -> float | None:
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


class ResearchService:
    def __init__(self, storage):
        self.storage = storage

    def watchlist(self, name: str = "default", codes: list[str] | None = None) -> dict:
        if not name or len(name) > 80:
            raise ValueError("invalid watchlist name")
        with self.storage._session() as db:
            if codes is not None:
                for code in codes:
                    valid_code(code)
                now = self.storage.clock.now_utc().isoformat()
                db.execute("DELETE FROM watchlists WHERE name=?", (name,))
                db.executemany("INSERT INTO watchlists VALUES (?,?,?)", [(name, c, now) for c in sorted(set(codes))])
            rows = db.execute("SELECT code FROM watchlists WHERE name=? ORDER BY code", (name,)).fetchall()
        return {"name": name, "codes": [r[0] for r in rows]}

    def universe(self, asof_date: str, codes: list[str] | None = None, source: str = "hs300") -> dict:
        valid_date(asof_date)
        observed = self.storage.clock.now_utc().isoformat()
        if codes is not None:
            if not codes or len(codes) > 5000:
                raise ValueError("fixed universe must contain 1 to 5000 codes")
            selected = sorted({valid_code(c) for c in codes})
            quality = "survivorship_bias"
            source = "fixed"
        elif source == "hs300":
            with self.storage._session() as db:
                previous = db.execute("""SELECT snapshot_id FROM universe_snapshots
                    WHERE asof_date=? AND source='hs300' AND quality='point_in_time'
                    ORDER BY observed_at LIMIT 1""", (asof_date,)).fetchone()
                if previous:
                    return self.get_universe(previous[0])
                row = db.execute("SELECT MAX(asof_date) FROM index_constituents WHERE index_code='hs300' AND asof_date<=?", (asof_date,)).fetchone()
                snapshot_date = row[0] if row else None
                rows = db.execute("SELECT code, fetched_at FROM index_constituents WHERE index_code='hs300' AND asof_date=? ORDER BY code", (snapshot_date,)).fetchall() if snapshot_date else []
            if not rows:
                raise ValueError("historical hs300 snapshot unavailable; provide an explicit fixed universe")
            selected = [r[0] for r in rows]
            observed = max(r[1] for r in rows)
            quality = "point_in_time" if snapshot_date == asof_date and observed[:10] <= asof_date else "revised_history"
        else:
            raise ValueError("unsupported universe source")
        snapshot_id = uuid.uuid4().hex
        with self.storage._session() as db:
            db.execute("INSERT INTO universe_snapshots VALUES (?,?,?,?,?,?)",
                       (snapshot_id, asof_date, source, observed, _dump(selected), quality))
        return {"snapshot_id": snapshot_id, "asof_date": asof_date, "source": source,
                "observed_at": observed, "codes": selected, "quality": quality}

    def get_universe(self, snapshot_id: str) -> dict:
        with self.storage._session() as db:
            row = db.execute("SELECT asof_date,source,observed_at,codes_json,quality FROM universe_snapshots WHERE snapshot_id=?", (snapshot_id,)).fetchone()
        if not row:
            raise ValueError("universe snapshot not found")
        return {"snapshot_id": snapshot_id, "asof_date": row[0], "source": row[1],
                "observed_at": row[2], "codes": json.loads(row[3]), "quality": row[4]}

    def _finance(self, db, code: str, dataset: str, field: str, asof_date: str, mode: str) -> tuple[dict | None, str | None]:
        if mode == "point_in_time":
            rows = db.execute("""SELECT year,quarter,payload,fetched_at,pub_date FROM financials WHERE code=? AND dataset=?
                UNION ALL SELECT year,quarter,payload,fetched_at,NULL FROM financial_history WHERE code=? AND dataset=?
                UNION ALL SELECT year,quarter,payload,first_observed_at,NULL FROM financial_versions WHERE code=? AND dataset=?
                ORDER BY year DESC,quarter DESC,fetched_at DESC""", (code, dataset, code, dataset, code, dataset)).fetchall()
        else:
            rows = db.execute("SELECT year,quarter,payload,fetched_at,pub_date FROM financials WHERE code=? AND dataset=? ORDER BY year DESC, quarter DESC", (code, dataset)).fetchall()
        for year, quarter, raw, fetched, pub in rows:
            data = json.loads(raw)
            if _number(data.get(field)) is None:
                continue
            announced = pub or data.get("pubDate") or data.get("pub_date")
            if announced:
                try:
                    # Date-only announcement becomes usable on the next trading day.
                    next_row = db.execute("SELECT MIN(calendar_date) FROM trade_calendar WHERE is_trading_day=1 AND calendar_date>?", (announced,)).fetchone()
                    available = next_row[0] if next_row else None
                except (ValueError, TypeError):
                    available = None
            else:
                available = fetched[:10]
            if not available or available > asof_date:
                continue
            if mode == "point_in_time" and fetched[:10] > asof_date:
                continue
            return {"year": year, "quarter": quarter, "data": data, "fetched_at": fetched,
                    "announced": announced, "available": available}, None
        return None, f"{dataset}_unavailable"

    def screen(self, asof_date: str, mode: str = "revised_history", codes: list[str] | None = None,
               universe_snapshot_id: str | None = None, max_selected: int = 10) -> dict:
        valid_date(asof_date)
        if not self.storage.is_trading_day(asof_date):
            raise ValueError("asof_date must be a recorded trading day")
        if mode not in {"point_in_time", "revised_history"}:
            raise ValueError("invalid research mode")
        if not 1 <= max_selected <= 10:
            raise ValueError("max_selected must be between 1 and 10")
        if universe_snapshot_id:
            universe = self.get_universe(universe_snapshot_id)
            if universe["asof_date"] != asof_date:
                raise ValueError("universe snapshot does not match asof_date")
        else:
            universe = self.universe(asof_date, codes)
        if mode == "point_in_time" and universe["quality"] != "point_in_time":
            raise ValueError("insufficient historical universe evidence for point_in_time")
        selected_codes = universe["codes"]
        results = []
        inputs = {}
        start = (date.fromisoformat(asof_date) - timedelta(days=100)).isoformat()
        with self.storage._session() as db:
            for code in selected_codes:
                bars = (self.storage.get_daily_bars_asof(code, "d", "1", start, asof_date, asof_date)
                        if mode == "point_in_time" else
                        self.storage.get_daily_bars(code, "d", "1", start, asof_date))
                profit, profit_error = self._finance(db, code, "profit", "roeAvg", asof_date, mode)
                cash, cash_error = self._finance(db, code, "cash_flow", "CFOToNP", asof_date, mode)
                inputs[code] = {"bars": bars, "profit": profit, "cash_flow": cash}
                reasons = []
                factors = {"momentum_20": None, "volatility_20": None, "turn_20": None,
                           "roeAvg": None, "CFOToNP": None}
                eligible = [b for b in bars if _number(b.get("close")) is not None and _number(b.get("close")) > 0]
                if len(eligible) < 21:
                    reasons.append("insufficient_price_window")
                else:
                    close = [_number(b["close"]) for b in eligible[-21:]]
                    returns = [close[i] / close[i-1] - 1 for i in range(1, 21)]
                    factors["momentum_20"] = close[-1] / close[0] - 1
                    factors["volatility_20"] = statistics.stdev(returns) * math.sqrt(252)
                    turns = [_number(b.get("turn")) for b in eligible[-20:]]
                    if all(v is not None for v in turns):
                        factors["turn_20"] = sum(turns) / 20
                    else:
                        reasons.append("insufficient_turn_window")
                factors["roeAvg"] = _number(profit["data"].get("roeAvg")) if profit else None
                factors["CFOToNP"] = _number(cash["data"].get("CFOToNP")) if cash else None
                if factors["roeAvg"] is None:
                    reasons.append(profit_error or "roeAvg_missing")
                if factors["CFOToNP"] is None:
                    reasons.append(cash_error or "CFOToNP_missing")
                last = bars[-1] if bars else None
                if not last or last["date"] != asof_date or last.get("tradestatus") != "1":
                    reasons.append("not_tradable_on_signal_day")
                if last and str(last.get("isST", "")).lower() in {"1", "true"}:
                    reasons.append("st")
                status = "insufficient_data" if any(r.startswith("insufficient") or r.endswith("_unavailable") or r.endswith("_missing") for r in reasons) else "excluded" if reasons else "eligible"
                if status == "eligible" and not (factors["momentum_20"] > 0 and factors["roeAvg"] > 0 and factors["CFOToNP"] > 0):
                    status = "not_matched"; reasons.append("baseline_v1_threshold")
                results.append({"code": code, "status": status, "reasons": reasons, "factors": factors, "rank": None})
        eligible = sorted((r for r in results if r["status"] == "eligible"), key=lambda r: (-r["factors"]["momentum_20"], r["code"]))
        for rank, row in enumerate(eligible, 1):
            row["rank"] = rank
            row["status"] = "selected" if rank <= max_selected else "not_matched"
            if rank > max_selected:
                row["reasons"].append("rank_limit")
        quality = ("point_in_time" if mode == "point_in_time" else
                   "survivorship_bias" if universe["quality"] == "survivorship_bias" else "revised_history")
        run_id = uuid.uuid4().hex
        params = {"max_selected": max_selected, "window": 20, "price_adjust": "backward", "rule_version": RULE_VERSION}
        with self.storage._session() as db:
            db.execute("INSERT INTO screen_run VALUES (?,?,?,?,?,?,?,?,?)",
                       (run_id, asof_date, mode, RULE_VERSION, quality, _dump(universe), _dump(inputs), _dump(params), self.storage.clock.now_utc().isoformat()))
            db.executemany("INSERT INTO screen_result VALUES (?,?,?,?,?,?)",
                           [(run_id, r["code"], r["rank"], r["status"], _dump(r["reasons"]), _dump(r["factors"])) for r in results])
        return self.get_screen(run_id)

    def get_screen(self, run_id: str) -> dict:
        with self.storage._session() as db:
            run = db.execute("SELECT asof_date,mode,rule_version,quality,universe_json,inputs_json,parameters_json,created_at FROM screen_run WHERE run_id=?", (run_id,)).fetchone()
            if not run:
                raise ValueError("screen run not found")
            rows = db.execute("SELECT code,rank,status,reasons_json,factors_json FROM screen_result WHERE run_id=? ORDER BY rank IS NULL,rank,code", (run_id,)).fetchall()
        return {"run_id": run_id, "asof_date": run[0], "mode": run[1], "rule_version": run[2], "quality": run[3],
                "universe": json.loads(run[4]), "inputs": json.loads(run[5]), "parameters": json.loads(run[6]),
                "created_at": run[7], "results": [{"code": r[0], "rank": r[1], "status": r[2], "reasons": json.loads(r[3]), "factors": json.loads(r[4])} for r in rows]}
