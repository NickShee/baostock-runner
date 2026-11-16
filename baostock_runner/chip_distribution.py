"""Local, turnover-weighted estimate of the distribution of holding costs."""
from math import isfinite

HISTORY_START = "2018-01-01"
BIN_COUNT = 100
MIN_HISTORY = 60


def _number(value, *, positive=False):
    return isinstance(value, (int, float)) and isfinite(value) and (value > 0 if positive else value >= 0)


def _day(row):
    day, trade, low, high, close, volume, amount, turn, adjusted_close = row
    if (trade == "0" or not _number(low, positive=True) or not _number(high, positive=True)
            or high < low or not _number(close, positive=True) or not _number(adjusted_close, positive=True)
            or not _number(volume, positive=True) or not _number(amount, positive=True) or not _number(turn)):
        return None
    factor = adjusted_close / close
    low, high = low * factor, high * factor
    mean = min(high, max(low, amount / volume * factor))
    return day, low, high, mean, min(1.0, turn / 100.0), factor


def _daily_weights(low, high, mode, start, step):
    def bin_index(price):
        return min(BIN_COUNT - 1, max(0, int((price - start) / step)))

    if low == high:
        return [(bin_index(low), 1.0)]

    def cdf(price):
        if price <= low:
            return 0.0
        if price >= high:
            return 1.0
        if price < mode:
            return (price - low) ** 2 / ((high - low) * (mode - low))
        return 1 - (high - price) ** 2 / ((high - low) * (high - mode))

    return [(i, cdf(start + (i + 1) * step) - cdf(start + i * step))
            for i in range(bin_index(low), bin_index(high) + 1)]


def estimate_chip_distribution(code, observation_date, calendar_status, rows):
    result = {"code": code, "date": observation_date, "method": "turnover-triangular-v1",
              "price_basis": "unadjusted_on_date"}
    if calendar_status == 0:
        return {**result, "status": "non_trading_day"}
    if not rows or rows[-1][0] != observation_date:
        return {**result, "status": "no_daily_bar"}
    if rows[-1][1] == "0":
        return {**result, "status": "suspended"}
    anchor = _day(rows[-1])
    if anchor is None:
        return {**result, "status": "incomplete_data"}

    days = [usable for row in rows if (usable := _day(row)) is not None]
    history = {"start_date": days[0][0], "end_date": observation_date,
               "bars_used": len(days), "bars_skipped": len(rows) - len(days)}
    if len(days) < MIN_HISTORY:
        return {**result, "status": "insufficient_history", "history": history}

    anchor_factor = anchor[-1]
    start = min(row[1] for row in days)
    end = max(row[2] for row in days)
    step = max((end - start) / BIN_COUNT, 0.01 * anchor_factor)
    weights = [0.0] * BIN_COUNT
    for index, (_, low, high, mean, turn, _) in enumerate(days):
        # Seed with the first eligible day's shape, then replace a share each day.
        rate = turn if index else 1.0
        if rate == 0:
            continue
        weights = [weight * (1 - rate) for weight in weights]
        for bin_index, weight in _daily_weights(low, high, mean, start, step):
            weights[bin_index] += weight * rate

    total = sum(weights)
    weights = [weight / total for weight in weights]

    def percentile(target):
        accumulated = 0.0
        for index, weight in enumerate(weights):
            if weight and accumulated + weight >= target:
                fraction = (target - accumulated) / weight
                return (start + (index + fraction) * step) / anchor_factor
            accumulated += weight
        return (start + BIN_COUNT * step) / anchor_factor

    peak_index = max(range(BIN_COUNT), key=weights.__getitem__)
    return {**result, "status": "ok", "history": history,
            "min_price": start / anchor_factor, "price_step": step / anchor_factor,
            "weights": weights, "peak": (start + (peak_index + 0.5) * step) / anchor_factor,
            "range70": [percentile(0.15), percentile(0.85)]}
