"""Daily long-only adjusted-price approximation using frozen screen signals."""
from __future__ import annotations

from datetime import date
import json
import math
import uuid

from .query import valid_date


def _finite(value):
    try:
        n = float(value)
        return n if math.isfinite(n) and n > 0 else None
    except (TypeError, ValueError):
        return None


class BacktestService:
    def __init__(self, storage, research):
        self.storage, self.research = storage, research

    def run(self, screen_run_ids: list[str], end_date: str, initial_cash: float = 1_000_000,
            fee_bps: float = 10, slippage_bps: float = 5, max_pending_days: int = 3,
            boundaries: dict[str, dict[str, dict[str, float]]] | None = None,
            price_snapshot: dict[str, dict[str, dict]] | None = None) -> dict:
        valid_date(end_date)
        if not screen_run_ids or len(screen_run_ids) > 520:
            raise ValueError("1 to 520 screen runs required")
        if not 0 < initial_cash <= 1e12 or not 0 <= fee_bps <= 100 or not 0 <= slippage_bps <= 100 or not 1 <= max_pending_days <= 3:
            raise ValueError("invalid backtest assumptions")
        screens = [self.research.get_screen(i) for i in screen_run_ids]
        screens.sort(key=lambda s: s["asof_date"])
        if len({s["asof_date"] for s in screens}) != len(screens):
            raise ValueError("duplicate signal date")
        if any(s["asof_date"] >= end_date for s in screens):
            raise ValueError("end_date must follow all signals")
        if any(s["asof_date"] != s["universe"]["asof_date"] for s in screens):
            raise ValueError("signal universe date mismatch")
        if any(s["rule_version"] != screens[0]["rule_version"] or s["mode"] != screens[0]["mode"] for s in screens):
            raise ValueError("incompatible signal versions")
        start_date = screens[0]["asof_date"]
        with self.storage._session() as db:
            calendar = [r[0] for r in db.execute("SELECT calendar_date FROM trade_calendar WHERE is_trading_day=1 AND calendar_date BETWEEN ? AND ? ORDER BY calendar_date", (start_date, end_date))]
        if not calendar or calendar[-1] < end_date and not self.storage.is_trading_day(end_date):
            # A non-trading end date is allowed if the calendar covers it.
            if self.storage.calendar_max_date() is None or self.storage.calendar_max_date() < end_date:
                raise ValueError("trade calendar does not cover end_date")
        if not all(s["asof_date"] in calendar for s in screens):
            raise ValueError("signal date missing from trade calendar")
        # A signal must be the final exchange trading day of its ISO week.
        for s in screens:
            d = date.fromisoformat(s["asof_date"])
            if any(date.fromisoformat(c).isocalendar()[:2] == d.isocalendar()[:2] and c > s["asof_date"] for c in calendar):
                raise ValueError("signal must be after the last trading close of its week")
        codes = sorted({r["code"] for s in screens for r in s["results"] if r["status"] == "selected"})
        bars = price_snapshot if price_snapshot is not None else {
            code: {r["date"]: r for r in self.storage.get_daily_bars(code, "d", "1", start_date, end_date)}
            for code in codes}
        signal = {s["asof_date"]: [r["code"] for r in s["results"] if r["status"] == "selected"] for s in screens}
        boundaries = boundaries or {}
        for per_day in boundaries.values():
            for bounds in per_day.values():
                if _finite(bounds.get("up")) is None or _finite(bounds.get("down")) is None or bounds["down"] >= bounds["up"]:
                    raise ValueError("invalid price-limit boundary")
        cash = float(initial_cash)
        holdings: dict[str, float] = {}
        pending: dict[str, dict] = {}
        last_prices: dict[str, tuple[float, int]] = {}
        orders, fills, daily = [], [], []
        total_fees = 0.0
        traded_notional = 0.0
        incomplete = False
        peak = initial_cash
        prior_nav = initial_cash
        for day_index, day in enumerate(calendar):
            day_reasons = []
            # Close signal creates orders for the following trading day. Never execute here.
            if day in signal:
                target = signal[day][:10]
                pending = {code: {"code": code, "side": "sell" if code not in target else "target",
                                  "created_day": day_index, "signal_date": day, "target_count": len(target)}
                           for code in set(holdings) | set(target)}
                for order in pending.values():
                    orders.append({**order, "status": "pending"})
            def opening(code):
                row = bars.get(code, {}).get(day)
                return _finite(row.get("open")) if row else None
            nav_open = cash + sum(qty * (opening(code) or (last_prices[code][0] if code in last_prices else 0))
                                  for code, qty in holdings.items())
            target_count = max((o["target_count"] for o in pending.values()), default=0)
            target_value = nav_open / target_count if target_count else 0
            def direction(code, order):
                if order["side"] == "sell":
                    return "sell"
                op = opening(code)
                if op is None:
                    return "buy"
                return "sell" if holdings.get(code, 0) * op > target_value else "buy"
            ordered = sorted(pending.items(), key=lambda item: (direction(*item) != "sell", item[0]))
            for code, order in ordered:
                if day_index <= order["created_day"]:
                    continue
                if day_index - order["created_day"] > max_pending_days:
                    orders.append({**order, "date": day, "status": "expired"}); del pending[code]; incomplete = True; continue
                bar = bars.get(code, {}).get(day)
                op = _finite(bar.get("open")) if bar else None
                if not bar or bar.get("tradestatus") != "1" or op is None:
                    day_reasons.append({"code": code, "reason": "suspended_or_open_missing"}); continue
                bound = boundaries.get(day, {}).get(code)
                if not bound or _finite(bound.get("up")) is None or _finite(bound.get("down")) is None:
                    day_reasons.append({"code": code, "reason": "price_limit_unknown"}); continue
                side = direction(code, order)
                if side == "buy" and op >= bound["up"] or side == "sell" and op <= bound["down"]:
                    day_reasons.append({"code": code, "reason": "price_limit_blocked"}); continue
                price = op * (1 + slippage_bps / 10000 * (1 if side == "buy" else -1))
                fee_rate = fee_bps / 10000
                if side == "sell":
                    current = holdings.get(code, 0.0)
                    qty = current if order["side"] == "sell" else max(0.0, current - target_value / op)
                    gross = qty * price
                    fee = gross * fee_rate
                    cash += gross - fee
                    remaining = current - qty
                    if remaining > 1e-9:
                        holdings[code] = remaining
                    else:
                        holdings.pop(code, None)
                else:
                    # Divide available cash over still-pending buys. No leverage.
                    buy_count = max(1, sum(direction(c, v) == "buy" for c, v in pending.items()))
                    budget = cash / buy_count
                    needed = max(0.0, target_value / op - holdings.get(code, 0.0))
                    qty = min(needed, budget / (price * (1 + fee_rate)))
                    gross = qty * price
                    fee = gross * fee_rate
                    cash -= gross + fee
                    holdings[code] = holdings.get(code, 0.0) + qty
                if qty <= 1e-9:
                    orders.append({**order, "date": day, "status": "satisfied"})
                    del pending[code]
                    continue
                total_fees += fee
                traded_notional += gross
                fills.append({"date": day, "code": code, "side": side, "quantity": qty,
                              "price": price, "gross": gross, "fee": fee, "signal_date": order["signal_date"]})
                orders.append({**order, "date": day, "status": "filled"})
                del pending[code]
            positions = []
            nav = cash
            for code, qty in sorted(holdings.items()):
                bar = bars.get(code, {}).get(day)
                close = _finite(bar.get("close")) if bar else None
                if close is not None:
                    last_prices[code] = (close, day_index)
                prior = last_prices.get(code)
                stale_days = day_index - prior[1] if prior else None
                if prior is None or stale_days > 3 and (not bar or bar.get("tradestatus") != "0"):
                    incomplete = True
                    day_reasons.append({"code": code, "reason": "valuation_missing_over_3_days"})
                valuation = close or (prior[0] if prior else None)
                if valuation is not None:
                    nav += qty * valuation
                positions.append({"code": code, "quantity": qty, "price": valuation,
                                  "value": qty * valuation if valuation else None, "stale_days": stale_days})
            peak = max(peak, nav)
            daily.append({"date": day, "cash": cash, "holdings": positions, "nav": nav,
                          "return": nav / prior_nav - 1, "cumulative_return": nav / initial_cash - 1,
                          "drawdown": nav / peak - 1, "reasons": day_reasons})
            prior_nav = nav
        if pending:
            incomplete = True
        if not daily:
            raise ValueError("no trading days in backtest")
        signal_quality = ("survivorship_bias" if any(s["quality"] == "survivorship_bias" for s in screens) else
                          "revised_history" if any(s["quality"] != "point_in_time" for s in screens) else "point_in_time")
        result_quality = "incomplete" if incomplete else "revised_history" if signal_quality == "point_in_time" else signal_quality
        result = {"model": "adjusted_price_approximation", "screen_run_ids": screen_run_ids,
                  "price_snapshot": bars, "boundaries": boundaries,
                  "orders": orders, "fills": fills, "daily": daily,
                  "metrics": {"initial_cash": initial_cash, "ending_nav": daily[-1]["nav"],
                              "cumulative_return": None if incomplete else daily[-1]["nav"] / initial_cash - 1,
                              "annualized_return": None if incomplete else (daily[-1]["nav"] / initial_cash) ** (252 / len(daily)) - 1,
                              "max_drawdown": None if incomplete else min(r["drawdown"] for r in daily),
                              "turnover": traded_notional / initial_cash, "fees": total_fees},
                  "quality": result_quality, "signal_quality": signal_quality,
                  "quality_reasons": (["valuation_or_order_incomplete"] if incomplete else []) +
                                     (["user_supplied_price_boundaries_unverified"] if signal_quality == "point_in_time" else []),
                  "assumptions": {"initial_cash": initial_cash, "fee_bps": fee_bps,
                                  "slippage_bps": slippage_bps, "max_pending_days": max_pending_days,
                                  "fractional_shares": True, "price_adjust": "backward",
                                  "signal": "last_trading_close_of_week", "execution": "next_trading_open"}}
        run_id = uuid.uuid4().hex
        with self.storage._session() as db:
            db.execute("INSERT INTO backtest_run VALUES (?,?,?,?,?,?,?)",
                       (run_id, screens[0]["run_id"], "incomplete" if incomplete else "succeeded",
                        result["quality"], json.dumps(result["assumptions"]), json.dumps(result),
                        self.storage.clock.now_utc().isoformat()))
        return {"run_id": run_id, **result}

    def get(self, run_id: str) -> dict:
        with self.storage._session() as db:
            row = db.execute("SELECT result_json FROM backtest_run WHERE run_id=?", (run_id,)).fetchone()
        if not row:
            raise ValueError("backtest run not found")
        return {"run_id": run_id, **json.loads(row[0])}
