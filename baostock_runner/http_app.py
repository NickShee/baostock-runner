"""Same-process dashboard/API routes; MCP keeps its existing transport path."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import hmac
import inspect
import json
import os
from pathlib import Path
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, FileResponse, RedirectResponse, PlainTextResponse
from starlette.routing import Route

from .query import QueryService, valid_code, valid_date
from .research import ResearchService
from .backtest import BacktestService


class ApiAuth(BaseHTTPMiddleware):
    def __init__(self, app, token: str, dev_mode: bool):
        super().__init__(app)
        self.token, self.dev_mode = token, dev_mode

    async def dispatch(self, request, call_next):
        if request.url.path.startswith("/api/"):
            if self.dev_mode and request.client and request.client.host in {"127.0.0.1", "::1"}:
                return await call_next(request)
            provided = request.headers.get("authorization", "")
            if not self.token:
                return JSONResponse({"error": "BAOSTOCK_HTTP_TOKEN is not configured"}, status_code=503)
            if not hmac.compare_digest(provided, "Bearer " + self.token):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


class LocalJobs:
    def __init__(self, storage):
        self.storage = storage
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="local-app")
        with self.storage._session() as db:
            db.execute("UPDATE app_jobs SET status='failed', error='service restarted', updated_at=? WHERE status IN ('queued','running')",
                       (self.storage.clock.now_utc().isoformat(),))

    def submit(self, kind, request, fn):
        job_id = uuid.uuid4().hex
        now = self.storage.clock.now_utc().isoformat()
        with self.storage._session() as db:
            db.execute("INSERT INTO app_jobs VALUES (?,?,?,?,?,?,?,?)",
                       (job_id, kind, "queued", json.dumps(request), None, None, now, now))
        def run():
            self._update(job_id, "running")
            try:
                result = fn()
                self._update(job_id, "succeeded", result=result)
            except Exception as exc:
                self._update(job_id, "failed", error=str(exc))
        self.executor.submit(run)
        return {"job_id": job_id, "status": "queued"}

    def _update(self, job_id, status, result=None, error=None):
        with self.storage._session() as db:
            db.execute("UPDATE app_jobs SET status=?,result_json=?,error=?,updated_at=? WHERE job_id=?",
                       (status, json.dumps(result, ensure_ascii=False) if result is not None else None,
                        error, self.storage.clock.now_utc().isoformat(), job_id))

    def get(self, job_id):
        with self.storage._session() as db:
            row = db.execute("SELECT kind,status,request_json,result_json,error,created_at,updated_at FROM app_jobs WHERE job_id=?", (job_id,)).fetchone()
        if not row:
            raise ValueError("job not found")
        return {"job_id": job_id, "kind": row[0], "status": row[1], "request": json.loads(row[2]),
                "result": json.loads(row[3]) if row[3] else None, "error": row[4],
                "created_at": row[5], "updated_at": row[6]}


def build_http_app(mcp, gateway, fetcher, settings):
    query = QueryService(gateway)
    research = ResearchService(gateway.storage)
    backtest = BacktestService(gateway.storage, research)
    jobs = LocalJobs(gateway.storage)
    dist = Path(__file__).parent / "dashboard_dist"

    def respond(fn):
        async def endpoint(request):
            try:
                result = fn(request)
                return JSONResponse(await result if inspect.isawaitable(result) else result)
            except (ValueError, TypeError, KeyError) as exc:
                return JSONResponse({"error": str(exc)}, status_code=400)
        return endpoint

    def respond_accepted(fn):
        async def endpoint(request):
            try:
                return await fn(request)
            except (ValueError, TypeError, KeyError) as exc:
                return JSONResponse({"error": str(exc)}, status_code=400)
        return endpoint

    def n(request, key, default):
        return int(request.query_params.get(key, default))

    async def health(request):
        try:
            with gateway.storage._session() as db:
                db.execute("SELECT 1").fetchone()
            return JSONResponse({"status": "ok", "database": "ok", "jobs": "ok", "upstream": gateway.health_status()})
        except Exception as exc:
            return JSONResponse({"status": "failed", "error": str(exc)}, status_code=503)

    async def ready(request):
        try:
            with gateway.storage._session() as db:
                db.execute("SELECT 1 FROM securities LIMIT 1").fetchone()
            return JSONResponse({"status": "ready", "local_queries": True, "upstream": gateway.health_status()})
        except Exception as exc:
            return JSONResponse({"status": "not_ready", "error": str(exc)}, status_code=503)

    async def status(request):
        storage = gateway.storage
        return {"server_time": storage.clock.now_utc().isoformat(),
                "latest_trade_day": storage.latest_trade_date(gateway.clock.business_date().isoformat()),
                "fetcher": fetcher.state,
                "gateway": {"queue_length": gateway.jobs.qsize(), "circuit_breaker_open": gateway.breaker.is_set(),
                            "health": gateway.health_status()},
                "budget": {"fetcher_used_today": storage.download_usage_today_conservative(),
                           "fetcher_left": fetcher.budget_left(), "total_used_today": storage.usage_today_conservative()},
                "jobs": {ds: storage.job_stats(ds) for ds in ("daily_bars", "financials", "dividends", "adjust_factors")},
                "last_success": {ds: storage.get_meta(ds) for ds in ("daily_bars", "financials", "dividends", "adjust_factors")}}

    async def coverage(request):
        s = gateway.storage
        with s._session() as db:
            daily = db.execute("SELECT COUNT(*),COUNT(DISTINCT code),MIN(bar_date),MAX(bar_date) FROM daily_bars").fetchone()
            fin = db.execute("SELECT COUNT(*),COUNT(DISTINCT code),MIN(year),MAX(year),MAX(fetched_at) FROM financials").fetchone()
        return {"securities": s.count_securities(),
                "daily_bars": dict(zip(("rows", "codes", "start_date", "end_date"), daily)),
                "financials": dict(zip(("rows", "codes", "start_year", "end_year", "updated_at"), fin))}

    async def errors(request):
        with gateway.storage._session() as db:
            rows = db.execute("SELECT dataset,batch_id,error,updated_at,next_retry_at FROM download_jobs WHERE status IN ('retryable_failed','permanent_failed') ORDER BY updated_at DESC LIMIT 20").fetchall()
        return {"errors": [dict(zip(("dataset", "batch_id", "error", "updated_at", "next_retry_at"), r)) for r in rows]}

    async def stocks(request):
        return query.stocks(request.query_params.get("q", ""), n(request, "limit", 500), request.query_params.get("cursor"))

    async def daily(request):
        code = valid_code(request.path_params["code"])
        today = gateway.clock.business_date().isoformat()
        start = request.query_params.get("start_date", (date.fromisoformat(today) - timedelta(days=365)).isoformat())
        end = request.query_params.get("end_date", today)
        adjust = {"3": "none", "2": "forward", "1": "backward"}.get(request.query_params.get("adjustflag", "3"))
        return query.daily(code, start, end, adjust, limit=n(request, "limit", 500), cursor=request.query_params.get("cursor"))

    async def financials(request):
        return query.financials(request.path_params["code"], request.query_params.get("dataset", "profit"),
                                n(request, "limit", 500), request.query_params.get("cursor"))

    async def refresh(request):
        code = valid_code(request.path_params["code"])
        payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
        today = gateway.clock.business_date().isoformat()
        start = valid_date(payload.get("start_date", (date.fromisoformat(today) - timedelta(days=365)).isoformat()))
        end = valid_date(payload.get("end_date", today))
        flag = request.query_params.get("adjustflag", "3")
        if flag not in {"1", "2", "3"} or start > end:
            raise ValueError("invalid refresh range or adjustflag")
        fields = "date,code,open,high,low,close,preclose,volume,amount,pctChg,turn,tradestatus,isST,peTTM,pbMRQ,psTTM,pcfNcfTTM"
        job = jobs.submit("refresh", {"code": code, "start_date": start, "end_date": end, "adjustflag": flag},
                          lambda: gateway.daily_bars({"code": code, "fields": fields, "start_date": start,
                                                      "end_date": end, "frequency": "d", "adjustflag": flag}, force_refresh=True))
        return JSONResponse(job, status_code=202)

    async def create_screen(request):
        body = await request.json()
        job = jobs.submit("screen", body, lambda: research.screen(**body))
        return JSONResponse(job, status_code=202)

    async def create_backtest(request):
        body = await request.json()
        job = jobs.submit("backtest", body, lambda: backtest.run(**body))
        return JSONResponse(job, status_code=202)

    async def set_watchlist(request):
        body = await request.json()
        return research.watchlist(body.get("name", "default"), body.get("codes"))

    async def create_universe(request):
        body = await request.json()
        return research.universe(**body)

    async def get_watchlist(request):
        return research.watchlist(request.query_params.get("name", "default"))

    async def export(request):
        kind, run_id = request.path_params["kind"], request.path_params["run_id"]
        result = research.get_screen(run_id) if kind == "screen" else backtest.get(run_id) if kind == "backtest" else None
        if result is None:
            raise ValueError("invalid export kind")
        return JSONResponse(result, headers={"Content-Disposition": f'attachment; filename="{kind}-{run_id}.json"'})

    async def dashboard(request):
        path = dist / "index.html"
        return FileResponse(path) if path.exists() else PlainTextResponse("dashboard build missing", status_code=503)

    async def asset(request):
        name = request.path_params["name"]
        if "/" in name or ".." in name:
            return PlainTextResponse("not found", status_code=404)
        path = dist / "assets" / name
        return FileResponse(path) if path.is_file() else PlainTextResponse("not found", status_code=404)

    async def job(request):
        return jobs.get(request.path_params["job_id"])

    routes = [
        Route("/healthz", health), Route("/readyz", ready),
        Route("/dashboard", lambda r: RedirectResponse("/dashboard/")),
        Route("/dashboard/", dashboard), Route("/dashboard/assets/{name}", asset),
        Route("/api/dashboard/status", respond(status)), Route("/api/dashboard/coverage", respond(coverage)),
        Route("/api/dashboard/errors", respond(errors)),
        Route("/api/stocks", respond(stocks)),
        Route("/api/stocks/{code}/daily", respond(daily)),
        Route("/api/stocks/{code}/financials", respond(financials)),
        Route("/api/stocks/{code}/daily/refresh", respond_accepted(refresh), methods=["POST"]),
        Route("/api/jobs/{job_id}", respond(job)),
        Route("/api/research/watchlist", respond(get_watchlist), methods=["GET"]),
        Route("/api/research/watchlist", respond(set_watchlist), methods=["PUT"]),
        Route("/api/research/universes", respond(create_universe), methods=["POST"]),
        Route("/api/research/universes/{snapshot_id}", respond(lambda r: research.get_universe(r.path_params["snapshot_id"]))),
        Route("/api/research/screens", respond_accepted(create_screen), methods=["POST"]),
        Route("/api/research/screens/{run_id}", respond(lambda r: research.get_screen(r.path_params["run_id"]))),
        Route("/api/research/backtests", respond_accepted(create_backtest), methods=["POST"]),
        Route("/api/research/backtests/{run_id}", respond(lambda r: backtest.get(r.path_params["run_id"]))),
        Route("/api/research/export/{kind}/{run_id}", respond_accepted(export)),
    ]
    app = mcp.streamable_http_app()
    app.router.routes.extend(routes)
    app.add_middleware(ApiAuth, token=os.getenv("BAOSTOCK_HTTP_TOKEN", ""),
                       dev_mode=os.getenv("BAOSTOCK_HTTP_DEV_MODE", "false").lower() == "true")
    return app
