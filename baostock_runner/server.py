import atexit
import re
from mcp.server.fastmcp import FastMCP

from .config import Settings
from .fetcher import Fetcher
from .gateway import BaoStockGateway


settings = Settings()
mcp = FastMCP(
    "baostock-runner",
    host=settings.mcp_host,
    port=settings.mcp_port,
    streamable_http_path=settings.mcp_path,
    stateless_http=settings.mcp_stateless,
)
gateway = BaoStockGateway(settings)
fetcher = Fetcher(gateway, settings)
atexit.register(gateway.close)
atexit.register(fetcher.stop)


@mcp.tool()
def get_stock_daily_bars(code: str, start_date: str, end_date: str,
                         adjust: str = "none", fields: str = "date,code,open,high,low,close,volume,amount,pctChg,turn") -> dict:
    """Get daily A-share bars through the single queued BaoStock connection.

    code examples: sh.600000, sz.000001, bj.430047. adjust is none, forward, or backward.
    Dates use YYYY-MM-DD.
    """
    if not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_date) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_date):
        raise ValueError("start_date and end_date must use YYYY-MM-DD")
    if start_date > end_date:
        raise ValueError("start_date must not be later than end_date")
    if adjust not in {"none", "forward", "backward"}:
        raise ValueError("adjust must be none, forward, or backward")
    adjustflag = {"none": "3", "forward": "2", "backward": "1"}[adjust]
    return gateway.daily_bars({
        "code": code, "fields": fields, "start_date": start_date, "end_date": end_date,
        "frequency": "d", "adjustflag": adjustflag,
    })


def _date(value: str, name: str):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{name} must use YYYY-MM-DD")


@mcp.tool()
def get_trade_calendar(start_date: str, end_date: str) -> dict:
    """Get the A-share trading calendar for a date range."""
    _date(start_date, "start_date")
    _date(end_date, "end_date")
    if start_date > end_date:
        raise ValueError("start_date must not be later than end_date")
    return gateway.call("query_trade_dates", {"start_date": start_date, "end_date": end_date})


@mcp.tool()
def get_all_stocks(trade_date: str) -> dict:
    """Get the security list available on a trading date."""
    _date(trade_date, "trade_date")
    return gateway.call("query_all_stock", {"day": trade_date})


@mcp.tool()
def get_stock_basic(code: str = "", status: str = "L", fields: str = "code,code_name,ipoDate,outDate,type,status") -> dict:
    """Get stock master data. Leave code empty to query the market list."""
    if code and not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    if status not in {"L", "D", "P", ""}:
        raise ValueError("status must be L, D, P, or empty")
    # ``fields`` is an output projection for the MCP tool.  It is not a
    # parameter accepted by baostock.query_stock_basic().
    requested_fields = [field.strip() for field in fields.split(",") if field.strip()]
    if not requested_fields:
        raise ValueError("fields must contain at least one field")
    # baostock.query_stock_basic(code='', code_name='') -- 无 status 参数；
    # status 仅作为 MCP 层的投影过滤条件，透传会触发 TypeError。
    result = gateway.call("query_stock_basic", {"code": code, "code_name": ""})
    result["data"] = [
        {field: row.get(field) for field in requested_fields}
        for row in result.get("data", [])
    ]
    return result


@mcp.tool()
def get_stock_industry(code: str = "") -> dict:
    """Get industry classification, optionally for one stock."""
    if code and not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    return gateway.call("query_stock_industry", {"code": code})


@mcp.tool()
def get_financial_data(code: str, year: int, quarter: int, dataset: str = "profit") -> dict:
    """Get quarterly financial data: profit, growth, balance, or cash_flow."""
    if not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    if year < 1990 or year > 2100 or quarter not in {1, 2, 3, 4}:
        raise ValueError("year or quarter is invalid")
    methods = {"profit": "query_profit_data", "growth": "query_growth_data", "balance": "query_balance_data", "cash_flow": "query_cash_flow_data"}
    if dataset not in methods:
        raise ValueError("dataset must be profit, growth, balance, or cash_flow")
    return gateway.call(methods[dataset], {"code": code, "year": year, "quarter": quarter})


@mcp.tool()
def get_dividend_data(code: str, year: int = 0) -> dict:
    """Get dividend data for a stock, optionally filtered by year."""
    if not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    if year and (year < 1990 or year > 2100):
        raise ValueError("year is invalid")
    return gateway.call("query_dividend_data", {"code": code, "year": year})


@mcp.tool()
def get_adjust_factor(code: str, start_date: str = "", end_date: str = "") -> dict:
    """Get historical adjustment factors for a stock."""
    if not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    if start_date:
        _date(start_date, "start_date")
    if end_date:
        _date(end_date, "end_date")
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must not be later than end_date")
    return gateway.call("query_adjust_factor", {"code": code, "start_date": start_date, "end_date": end_date})


@mcp.tool()
def get_index_constituents(index: str, date: str = "") -> dict:
    """Get constituents for hs300, sz50, or zz500."""
    methods = {"hs300": "query_hs300_stocks", "sz50": "query_sz50_stocks", "zz500": "query_zz500_stocks"}
    if index not in methods:
        raise ValueError("index must be hs300, sz50, or zz500")
    if date:
        _date(date, "date")
    params = {"date": date} if date else {}
    return gateway.call(methods[index], params)


@mcp.tool()
def get_latest_stock_snapshot(codes: list[str], adjust: str = "none") -> dict:
    """Fetch the latest available daily bar for up to 50 stocks, serially."""
    if not codes or len(codes) > 50:
        raise ValueError("codes must contain between 1 and 50 stocks")
    for code in codes:
        if not re.fullmatch(r"(?:sh|sz|bj)\.\d{6}", code):
            raise ValueError(f"invalid stock code: {code}")
    # The date range is deliberately bounded; the Gateway still serializes every call.
    today = gateway.clock.business_date().isoformat()
    results = []
    for code in codes:
        result = get_stock_daily_bars(code, today, today, adjust)
        results.append({"code": code, **result})
    return {"data": results, "count": len(results)}


@mcp.tool()
def gateway_status() -> dict:
    """Return cache/request status and whether the circuit breaker is open."""
    count = gateway.storage.usage_today()
    return {
        "request_count_today": count,
        "warning_limit": gateway.settings.daily_warning_limit,
        "hard_limit": gateway.settings.daily_hard_limit,
        "warning_limit_reached": count >= gateway.settings.daily_warning_limit,
        "circuit_breaker_open": gateway.breaker.is_set(),
        "queue_length": gateway.jobs.qsize(),
        "offline": gateway.settings.offline,
        "fetcher_budget_left": fetcher.budget_left(),
    }


@mcp.tool()
def start_backfill(dataset: str = "auto") -> dict:
    """Trigger the background fetcher (wake it up) and return its state.

    dataset: auto | daily_bars | financials | dividends | adjust_factors |
             securities | calendar | industry | index_constituents
    The fetcher runs continuously; this tool only nudges it to poll immediately.
    """
    if settings.offline:
        return {"ok": False, "reason": "offline mode", "state": fetcher.state}
    fetcher.wake(dataset)
    return {
        "ok": True,
        "state": fetcher.state,
        "budget": {
            "total_hard_limit": settings.daily_hard_limit,
            "fetcher_budget": fetcher.fetch_budget,
            "fetcher_used_today": gateway.storage.download_usage_today(),
            "fetcher_left": fetcher.budget_left(),
        },
    }


@mcp.tool()
def get_backfill_status() -> dict:
    """Return background fetcher status, budget split, and per-dataset job stats."""
    storage = gateway.storage
    return {
        "state": fetcher.state,
        "budget": {
            "ratio": settings.fetch_budget_ratio,
            "total_hard_limit": settings.daily_hard_limit,
            "fetcher_budget": fetcher.fetch_budget,
            "fetcher_used_today": storage.download_usage_today(),
            "fetcher_left": fetcher.budget_left(),
            "mcp_used_today": storage.usage_today(),
            "mcp_reserved": settings.daily_hard_limit - fetcher.fetch_budget,
        },
        "jobs": {
            ds: storage.job_stats(ds)
            for ds in ("daily_bars", "financials", "dividends", "adjust_factors")
        },
        "settings": {
            "fetch_enabled": settings.fetch_enabled,
            "fetch_universe": settings.fetch_universe,
            "fetch_daily_start_date": settings.fetch_daily_start_date,
            "fetch_financial_start_year": settings.fetch_financial_start_year,
            "fetch_financial_datasets": settings.fetch_financial_datasets,
            "fetch_adjustflags": settings.fetch_adjustflags,
            "fetch_batch_size": settings.fetch_batch_size,
        },
    }


@mcp.tool()
def get_market_coverage() -> dict:
    """Return local SQLite coverage: row counts and freshness per dataset."""
    storage = gateway.storage
    latest_fin = storage.latest_financial_period("profit")
    return {
        "securities": storage.count_securities(),
        "trade_calendar_days": storage.count_rows("trade_calendar"),
        "daily_bars_rows": storage.count_rows("daily_bars"),
        "industry_rows": storage.count_rows("stock_industry"),
        "financials_rows": storage.count_rows("financials"),
        "financials_latest_period": latest_fin,
        "dividends_rows": storage.count_rows("dividends"),
        "adjust_factors_rows": storage.count_rows("adjust_factors"),
        "index_constituents_rows": storage.count_rows("index_constituents"),
        "db_path": storage.db_path,
    }


def main():
    settings = gateway.settings
    if settings.mcp_transport == "stdio":
        # stdio 模式仅用于调试/单客户端，不启动后台 fetcher。
        mcp.run(transport="stdio")
        return
    if settings.mcp_transport != "streamable-http":
        raise ValueError("BAOSTOCK_MCP_TRANSPORT must be streamable-http or stdio")
    fetcher.start()
    mcp.run(transport="streamable-http")
