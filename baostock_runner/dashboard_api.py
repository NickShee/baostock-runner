"""Dashboard routes mounted on the existing FastMCP ASGI application."""

import datetime
import re
from pathlib import Path
from typing import Any

import anyio
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response

from .config import Settings
from .dashboard_queries import DashboardQueries
from .fetcher import Fetcher
from .gateway import BaoStockGateway


CODE_RE = re.compile(r"(?:sh|sz|bj)\.\d{6}")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
FRONTEND_DIR = Path(__file__).with_name("dashboard_dist")


def _json(data: Any, status_code: int = 200) -> JSONResponse:
    return JSONResponse(data, status_code=status_code)


def _query_date(value: str | None, default: str, name: str) -> str:
    value = value or default
    if not DATE_RE.fullmatch(value):
        raise ValueError(f"{name} must use YYYY-MM-DD")
    try:
        datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} is invalid") from exc
    return value


def _code(value: str) -> str:
    if not CODE_RE.fullmatch(value):
        raise ValueError("code must look like sh.600000, sz.000001, or bj.430047")
    return value


def register_dashboard_routes(mcp, gateway: BaoStockGateway, fetcher: Fetcher, settings: Settings):
    queries = DashboardQueries(gateway.storage)

    @mcp.custom_route("/dashboard", methods=["GET"], include_in_schema=False)
    async def dashboard_root(request: Request) -> Response:
        index = FRONTEND_DIR / "index.html"
        if not index.exists():
            return Response("dashboard frontend has not been built", status_code=503, media_type="text/plain")
        return FileResponse(index)

    @mcp.custom_route("/dashboard/", methods=["GET"], include_in_schema=False)
    async def dashboard_root_slash(request: Request) -> Response:
        return await dashboard_root(request)

    @mcp.custom_route("/dashboard/assets/{path:path}", methods=["GET"], include_in_schema=False)
    async def dashboard_asset(request: Request) -> Response:
        requested = request.path_params.get("path", "")
        asset = (FRONTEND_DIR / "assets" / requested).resolve()
        assets_root = (FRONTEND_DIR / "assets").resolve()
        if assets_root not in asset.parents or not asset.is_file():
            return Response("not found", status_code=404, media_type="text/plain")
        return FileResponse(asset)

    @mcp.custom_route("/api/dashboard/status", methods=["GET"])
    async def dashboard_status(request: Request) -> JSONResponse:
        storage = gateway.storage
        return _json({
            "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "fetcher": fetcher.state,
            "budget": {
                "ratio": settings.fetch_budget_ratio,
                "total_hard_limit": settings.daily_hard_limit,
                "fetcher_budget": fetcher.fetch_budget,
                "fetcher_used_today": storage.download_usage_today(),
                "fetcher_left": fetcher.budget_left(),
                "mcp_used_today": storage.usage_today(),
                "mcp_reserved": settings.daily_hard_limit - fetcher.fetch_budget,
            },
            "gateway": {
                "queue_length": gateway.jobs.qsize(),
                "circuit_breaker_open": gateway.breaker.is_set(),
                "offline": settings.offline,
                "request_count_today": storage.usage_today(),
                "warning_limit": settings.daily_warning_limit,
                "hard_limit": settings.daily_hard_limit,
            },
            "jobs": {
                dataset: storage.job_stats(dataset)
                for dataset in ("daily_bars", "financials", "dividends", "adjust_factors")
            },
        })

    @mcp.custom_route("/api/dashboard/coverage", methods=["GET"])
    async def dashboard_coverage(request: Request) -> JSONResponse:
        return _json(queries.coverage())

    @mcp.custom_route("/api/dashboard/errors", methods=["GET"])
    async def dashboard_errors(request: Request) -> JSONResponse:
        return _json({"errors": queries.job_errors()})

    @mcp.custom_route("/api/stocks", methods=["GET"])
    async def stocks_search(request: Request) -> JSONResponse:
        query = request.query_params.get("q", "")
        return _json({"query": query, "data": queries.search_securities(query)})

    @mcp.custom_route("/api/stocks/{code}/daily", methods=["GET"])
    async def stock_daily(request: Request) -> JSONResponse:
        try:
            code = _code(request.path_params["code"])
            adjustflag = request.query_params.get("adjustflag", "3")
            if adjustflag not in {"1", "2", "3"}:
                raise ValueError("adjustflag must be 1, 2, or 3")
            today = datetime.date.today().isoformat()
            start_date = _query_date(request.query_params.get("start_date"),
                                     (datetime.date.today() - datetime.timedelta(days=365)).isoformat(), "start_date")
            end_date = _query_date(request.query_params.get("end_date"), today, "end_date")
            if start_date > end_date:
                raise ValueError("start_date must not be later than end_date")
            rows = gateway.storage.get_daily_bars(code, "d", adjustflag, start_date, end_date)
            return _json({
                "code": code, "frequency": "d", "adjustflag": adjustflag,
                "start_date": start_date, "end_date": end_date,
                "coverage": queries.daily_coverage(code, "d", adjustflag),
                "rows": rows,
                "source": "local",
            })
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)

    @mcp.custom_route("/api/stocks/{code}/financials", methods=["GET"])
    async def stock_financials(request: Request) -> JSONResponse:
        try:
            code = _code(request.path_params["code"])
            dataset = request.query_params.get("dataset", "profit")
            if dataset not in {"profit", "growth", "balance", "cash_flow", "operation", "dupont"}:
                raise ValueError("unsupported financial dataset")
            return _json({"code": code, "dataset": dataset,
                          "rows": gateway.storage.get_financials(dataset, code), "source": "local"})
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)

    @mcp.custom_route("/api/stocks/{code}/daily/refresh", methods=["POST"])
    async def stock_daily_refresh(request: Request) -> JSONResponse:
        try:
            code = _code(request.path_params["code"])
            today = datetime.date.today().isoformat()
            start_date = _query_date(request.query_params.get("start_date"), settings.fetch_daily_start_date, "start_date")
            end_date = _query_date(request.query_params.get("end_date"), today, "end_date")
            if start_date > end_date:
                raise ValueError("start_date must not be later than end_date")
            adjustflag = request.query_params.get("adjustflag", "3")
            if adjustflag not in {"1", "2", "3"}:
                raise ValueError("adjustflag must be 1, 2, or 3")
            params = {
                "code": code,
                "fields": "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                "start_date": start_date,
                "end_date": end_date,
                "frequency": "d",
                "adjustflag": adjustflag,
            }
            before = gateway.storage.latest_daily_bar(code, "d", adjustflag)
            result = await anyio.to_thread.run_sync(
                lambda: gateway.daily_bars(params, use_cache=False, priority=0)
            )
            return _json({
                "ok": True,
                "code": code,
                "remote_requested": before is None or before < end_date,
                "cache_hit": result["cache_hit"],
                "coverage": queries.daily_coverage(code, "d", adjustflag),
                "rows": result["rows"],
                "request_count_today": result["request_count_today"],
            })
        except ValueError as exc:
            return _json({"error": str(exc)}, 400)
        except Exception as exc:
            return _json({"error": str(exc), "type": type(exc).__name__}, 502)

    return queries
