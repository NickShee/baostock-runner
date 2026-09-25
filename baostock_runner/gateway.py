from dataclasses import dataclass
import hashlib
import heapq
import itertools
import json
import os
import queue
import socket
import threading
import time
import uuid
from typing import Any

from .config import Settings
from .storage import Storage
from .timeutil import Clock


BLACKLIST_CODE = "10001011"
# 网络层异常：连接被重置/超时/拒绝。出现这类异常时应重连而非盲目重试原会话。
RECONNECTABLE_NETWORK_EXC = (ConnectionError, socket.timeout, BrokenPipeError, OSError)


class ReconnectableFailure(RuntimeError):
    """BaoStock 查询在重试耗尽后仍失败，可能通过重连恢复（非黑名单、非纯参数错误）。"""


SUPPORTED_METHODS = {
    "query_history_k_data_plus", "query_trade_dates", "query_all_stock",
    "query_stock_basic", "query_stock_industry", "query_profit_data",
    "query_growth_data", "query_balance_data", "query_cash_flow_data",
    "query_operation_data", "query_dupont_data",
    "query_dividend_data", "query_adjust_factor",
    "query_hs300_stocks", "query_sz50_stocks", "query_zz500_stocks",
}


@dataclass
class Job:
    method: str
    params: dict[str, Any]
    result: queue.Queue
    # False = 绕过参数缓存（后台 fetcher 使用，确保拿到远端最新数据且不污染 cache）。
    use_cache: bool = True
    # 队列优先级：0 = MCP 查询（最高，插队）；1 = 后台 fetcher（让位）。
    # worker 始终优先取优先级最小的 Job。
    priority: int = 0
    origin: str = "mcp"
    request_id: str = ""
    deadline: float = 0.0
    enqueued_at: float = 0.0


class BaoStockGateway:
    """All BaoStock calls happen in this one worker thread and one session."""
    def __init__(self, settings: Settings | None = None, clock: Clock | None = None):
        self.settings = settings or Settings()
        # A-01: 可注入时钟（业务日 Asia/Shanghai、审计 UTC），传给 Storage/Fetcher。
        # 测试可传假时钟推进日期/预算日；默认使用真实时钟。
        self.clock: Clock = clock or Clock()
        self.storage = Storage(self.settings.db_path, self.settings.log_path, clock=self.clock)
        # 优先级队列：MCP 查询（priority=0）总排到 fetcher（priority=1）前面；
        # 同优先级用递增序号保证 FIFO，不比较 Job 对象本身。
        self.jobs: queue.PriorityQueue = queue.PriorityQueue(maxsize=max(1, self.settings.request_queue_capacity))
        self._seq = itertools.count()
        self._closed = False
        self._queue_lock = threading.Lock()
        self._foreground_since_background = 0
        self._active_job: Job | None = None
        self.stop_event = threading.Event()
        self.breaker = threading.Event()
        self.ready = threading.Event()
        self.startup_error: Exception | None = None
        # 会话保活：记录最后一次成功活动的时间（monotonic），超时则主动重连。
        self.last_activity = time.monotonic()
        self.session_lock = threading.Lock()
        # worker 心跳：worker 每处理一个 Job（以及大数据量遍历中定期）刷新。
        # watchdog 线程发现心跳停滞超过阈值即终止进程，由容器 restart 自动拉起。
        self.last_heartbeat = time.monotonic()
        # 忙碌标志：仅当 worker 正在执行 Job 时才认为"应当在刷新心跳"。
        # 空闲（队列为空）是正常状态而非卡死，否则 watchdog 会周期性误杀空闲进程。
        self._busy = False
        self.worker = threading.Thread(target=self._worker, name="baostock-single-worker", daemon=True)
        self.worker.start()
        self.watchdog = None
        if self.settings.watchdog_enabled:
            self.watchdog = threading.Thread(
                target=self._watchdog_loop, name="baostock-watchdog", daemon=True
            )
            self.watchdog.start()

    def close(self):
        with self._queue_lock:
            if self._closed:
                return
            self._closed = True
            self.stop_event.set()
            while True:
                try:
                    _, _, job = self.jobs.get_nowait()
                except queue.Empty:
                    break
                if job is not None:
                    self._finish(job, False, RuntimeError("BaoStock gateway is shutting down"), "shutdown")
            self.jobs.put_nowait((-1, next(self._seq), None))
        self.worker.join(timeout=10)
        if self.watchdog is not None:
            self.watchdog.join(timeout=2)

    def call(self, method: str, params: dict[str, Any], use_cache: bool = True, priority: int = 0,
             origin: str | None = None, request_id: str | None = None,
             deadline_seconds: float | None = None) -> dict[str, Any]:
        origin = origin or ("fetcher" if priority > 0 else "mcp")
        if origin not in {"mcp", "dashboard", "fetcher", "maintenance"}:
            raise ValueError("origin must be mcp, dashboard, fetcher, or maintenance")
        priority = 1 if origin == "fetcher" else 0
        request_id = request_id or uuid.uuid4().hex
        default_deadline = (self.settings.background_request_deadline_seconds if origin == "fetcher"
                            else self.settings.foreground_request_deadline_seconds)
        request_deadline_seconds = max(0.001, deadline_seconds if deadline_seconds is not None else default_deadline)
        deadline = time.monotonic() + request_deadline_seconds
        if self._closed:
            raise RuntimeError("BaoStock gateway is shutting down")
        if not self.ready.wait(timeout=min(15, max(0.001, deadline - time.monotonic()))):
            raise TimeoutError("BaoStock worker did not become ready before the request deadline")
        if self.startup_error is not None:
            raise self.startup_error
        if self.breaker.is_set():
            raise RuntimeError("BaoStock circuit breaker is open; manual intervention required")
        key = hashlib.sha256(json.dumps({"method": method, "params": params}, sort_keys=True).encode()).hexdigest()
        if use_cache:
            cached = self.storage.get_cache(key)
            if cached is not None:
                self.storage.audit({"interface": method, **params, "cache_hit": True, "request_count_today": self.storage.usage_today_conservative(), "error_code": "0", "origin": origin, "request_id": request_id, "status": "cache_hit"}, self.settings.audit_log_max_bytes)
                return {"data": cached, "cache_hit": True, "request_count_today": self.storage.usage_today_conservative(), "request_id": request_id}
        result: queue.Queue = queue.Queue(maxsize=1)
        job = Job(method, params, result, use_cache, priority, origin, request_id, deadline, time.monotonic())
        try:
            with self._queue_lock:
                if self._closed:
                    raise RuntimeError("BaoStock gateway is shutting down")
                self.jobs.put_nowait((priority, next(self._seq), job))
        except queue.Full as exc:
            self._finish(job, False, RuntimeError("BaoStock request queue is full"), "queue_full")
            raise RuntimeError("BaoStock request queue is full") from exc
        try:
            ok, value = result.get(timeout=max(0.001, deadline - time.monotonic()))
        except queue.Empty as exc:
            self.storage.audit({"interface": method, **params, "origin": origin, "request_id": request_id,
                                "status": "caller_timeout", "deadline_seconds": request_deadline_seconds},
                               self.settings.audit_log_max_bytes)
            raise TimeoutError(f"BaoStock request {request_id} exceeded its deadline; running SDK call may continue") from exc
        if not ok:
            raise value
        value["request_id"] = request_id
        return value

    def _finish(self, job: Job, ok: bool, value: Any, status: str):
        event = {"interface": job.method, **job.params, "origin": job.origin, "request_id": job.request_id,
                 "status": status, "error_type": None if ok else type(value).__name__,
                 "error": None if ok else str(value)[:1000]}
        self.storage.audit(event, self.settings.audit_log_max_bytes)
        try:
            job.result.put_nowait((ok, value))
        except queue.Full:
            pass

    def _take_job(self):
        """Priority dequeue with bounded starvation for long-waiting fetcher jobs."""
        item = self.jobs.get(timeout=0.2)
        _, _, first = item
        if first is None or first.priority > 0:
            if first is not None:
                self._foreground_since_background = 0
            return first
        if self._foreground_since_background < max(1, self.settings.background_fairness_foreground_burst):
            self._foreground_since_background += 1
            return first
        cutoff = time.monotonic() - self.settings.background_fairness_wait_seconds
        with self.jobs.mutex:
            eligible = [(entry[2].enqueued_at, index, entry) for index, entry in enumerate(self.jobs.queue)
                        if entry[2] is not None and entry[2].origin == "fetcher" and entry[2].enqueued_at <= cutoff]
            if not eligible:
                return first
            _, index, selected = min(eligible)
            self.jobs.queue[index] = self.jobs.queue[-1]
            self.jobs.queue.pop()
            self.jobs.queue.append(item)
            heapq.heapify(self.jobs.queue)
            self._foreground_since_background = 0
            return selected[2]

    def daily_bars(self, params: dict[str, Any], use_cache: bool = True, priority: int = 0,
                   force_refresh: bool = False, origin: str | None = None) -> dict[str, Any]:
        """Read local daily bars and fetch missing ranges from BaoStock.

        A-02：force_refresh=True 时同时绕过参数缓存（use_cache 视为 False）与
        事实表"已覆盖"短路（即使本地已到 end_date 也全区间重查）。
        写入采用字段级合并（storage.put_daily_bars merge=True），窄字段刷新不丢旧字段。
        """
        code, frequency, adjustflag = params["code"], params["frequency"], params["adjustflag"]
        start_date, end_date = params["start_date"], params["end_date"]
        local = self.storage.get_daily_bars(code, frequency, adjustflag, start_date, end_date)
        latest = self.storage.latest_daily_bar(code, frequency, adjustflag)
        fetched = False
        if force_refresh:
            # 强制刷新：绕过事实表覆盖判断，全区间重查（含已覆盖的头部/中间区间）。
            query_params = dict(params)
            use_cache = False
        elif latest is None or not local:
            query_params = dict(params)
        elif latest < end_date:
            query_params = dict(params, start_date=latest, end_date=end_date)
        else:
            query_params = None
        if query_params is not None:
            result = self.call("query_history_k_data_plus", query_params, use_cache=use_cache, priority=priority, origin=origin)
            new_rows = result["data"]
            # merge=True：窄字段/强制刷新不覆盖旧字段值（A-02 验收）。
            self.storage.put_daily_bars(code, frequency, adjustflag, new_rows, merge=True)
            fetched = not result["cache_hit"]
            local = self.storage.get_daily_bars(code, frequency, adjustflag, start_date, end_date)
        columns = [field.strip() for field in params["fields"].split(",")]
        rows = [[row.get(field) for field in columns] for row in local]
        return {
            "code": code,
            "frequency": frequency,
            "adjustflag": adjustflag,
            "columns": columns,
            "rows": rows,
            "cache_hit": not fetched,
            "incremental": True,
            "force_refresh": force_refresh,
            "request_count_today": self.storage.usage_today_conservative(),
        }

    def _worker(self):
        bs = None
        logged_in = False
        try:
            if not self.settings.offline:
                if bool(self.settings.user_id) != bool(self.settings.password):
                    raise RuntimeError("BAOSTOCK_USER_ID and BAOSTOCK_PASSWORD must be provided together")
                import baostock as bs_module
                bs = bs_module
                self._record_attempt("session_login", {}, "login")
                login = (
                    bs.login(user_id=self.settings.user_id, password=self.settings.password)
                    if self.settings.user_id
                    else bs.login()
                )
                if login.error_code != "0":
                    if login.error_code == BLACKLIST_CODE:
                        self.breaker.set()
                    raise RuntimeError(f"BaoStock login failed: {login.error_code} {login.error_msg}")
                logged_in = True
                self.last_activity = time.monotonic()
                if self.settings.verify_after_login:
                    self._record_attempt("query_stock_basic", {"code": "sh.000001"}, "login_verification")
                    verification = bs.query_stock_basic(code="sh.000001")
                    if verification.error_code != "0":
                        raise RuntimeError(f"BaoStock post-login verification failed: {verification.error_code} {verification.error_msg}")
            self.ready.set()
            while True:
                try:
                    job = self._take_job()
                except queue.Empty:
                    if self.stop_event.is_set():
                        break
                    continue
                if job is None:
                    break
                if time.monotonic() >= job.deadline:
                    self._finish(job, False, TimeoutError("BaoStock request expired while queued"), "expired")
                    continue
                self._busy = True
                self._active_job = job
                self._beat()
                try:
                    value = self._execute_robust(bs, job.method, job.params, job.use_cache)
                    self._finish(job, True, value, "succeeded")
                except Exception as exc:
                    self._finish(job, False, exc, "failed")
                finally:
                    self._active_job = None
                    self._busy = False
        except Exception as exc:
            self.startup_error = exc
            self.ready.set()
            self.storage.audit({"interface": "gateway_startup", "origin": "maintenance",
                                "request_id": "gateway-session", "status": "failed",
                                "error_type": type(exc).__name__, "error": str(exc)[:1000]},
                               self.settings.audit_log_max_bytes)
            while True:
                try:
                    _, _, job = self.jobs.get_nowait()
                except queue.Empty:
                    break
                if job is not None:
                    self._finish(job, False, exc, "startup_failed")
        finally:
            if logged_in and bs is not None:
                bs.logout()

    def _beat(self):
        """刷新 worker 心跳（watchdog 据此判断 worker 是否卡死）。"""
        self.last_heartbeat = time.monotonic()

    def _watchdog_tick(self) -> bool:
        """单次看门狗判定：worker 心跳停滞是否超过阈值（True = 应终止进程）。"""
        timeout = max(30, self.settings.watchdog_timeout_seconds)
        try:
            if not self._busy:
                # worker 空闲等待 Job，心跳本来就不刷新，属正常状态。
                return False
            idle = time.monotonic() - self.last_heartbeat
            if idle > timeout:
                print(
                    f"[watchdog] CRITICAL: gateway worker heartbeat stalled for {idle:.0f}s "
                    f"(> {timeout}s). Killing process to trigger container restart.",
                    flush=True,
                )
                return True
        except Exception:
            pass
        return False

    def _watchdog_loop(self):
        """worker 心跳看门狗线程：见 _watchdog_tick；停滞时 os._exit(1) 触发容器重启。

        背景：baostock 免费服务在半开连接下可能让 rs.next() 进入用户态死循环，
        worker 线程被占死，queue 中的 Job 全部排队无人处理（fetcher/MCP 同时阻塞），
        而进程表面仍存活（uvicorn 正常响应健康检查），实际数据零进展、CPU 空转。
        此时唯一可靠的自愈路径是终止进程，让 restart: unless-stopped 拉起重来。
        """
        interval = max(5, self.settings.watchdog_check_interval_seconds)
        while not self.stop_event.is_set():
            if self._watchdog_tick():
                os._exit(1)
            self.stop_event.wait(interval)

    def _session_fresh(self) -> bool:
        """距上一次成功活动是否仍在会话空闲超时阈值内。"""
        return (time.monotonic() - self.last_activity) < self.settings.session_idle_timeout_seconds

    def _reconnect(self, bs) -> None:
        """主动重建 BaoStock 会话：logout -> login -> 轻量查询验证。"""
        with self.session_lock:
            try:
                if bs is not None and bs.is_login():
                    bs.logout()
            except Exception:
                pass
            time.sleep(1)
            self._record_attempt("session_login", {}, "relogin")
            login = (
                bs.login(user_id=self.settings.user_id, password=self.settings.password)
                if self.settings.user_id
                else bs.login()
            )
            if login.error_code != "0":
                if login.error_code == BLACKLIST_CODE:
                    self.breaker.set()
                raise RuntimeError(f"BaoStock re-login failed: {login.error_code} {login.error_msg}")
            if self.settings.verify_after_login:
                self._record_attempt("query_stock_basic", {"code": "sh.000001"}, "login_verification")
                rs = bs.query_stock_basic(code="sh.000001")
                if rs.error_code != "0":
                    raise RuntimeError(f"BaoStock post-login verification failed: {rs.error_code} {rs.error_msg}")
            self.last_activity = time.monotonic()

    def _record_attempt(self, method: str, params: dict[str, Any], operation: str = "query"):
        job = self._active_job
        origin = job.origin if job else "maintenance"
        request_id = job.request_id if job else "gateway-session"
        total_limit = self.settings.daily_hard_limit
        background_limit = max(0, min(total_limit, int(total_limit * self.settings.fetch_budget_ratio)))
        try:
            total, background = self.storage.reserve_request(origin, total_limit, background_limit)
        except Exception as exc:
            self.storage.audit({"interface": method, **params, "origin": origin, "request_id": request_id,
                                "operation": operation, "status": "attempt_rejected", "error_type": type(exc).__name__,
                                "error": str(exc)[:1000]}, self.settings.audit_log_max_bytes)
            raise
        self.storage.audit({"interface": method, **params, "origin": origin, "request_id": request_id,
                            "operation": operation, "status": "attempt", "request_count_today": total,
                            "background_request_count_today": background}, self.settings.audit_log_max_bytes)

    def health_status(self) -> dict[str, Any]:
        return {
            "healthy": self.ready.is_set() and self.startup_error is None and self.worker.is_alive(),
            "ready": self.ready.is_set() and self.startup_error is None and not self.breaker.is_set(),
            "worker_alive": self.worker.is_alive(),
            "circuit_breaker_open": self.breaker.is_set(),
            "queue_length": self.jobs.qsize(),
            "queue_capacity": self.jobs.maxsize,
            "busy": self._busy,
            "closed": self._closed,
        }

    def _execute_robust(self, bs, method: str, p: dict[str, Any], use_cache: bool = True):
        """健壮执行路径：先保证会话新鲜，失败后视类型重连一轮再试。"""
        if self.storage.usage_today_conservative() >= self.settings.daily_hard_limit:
            raise RuntimeError("Daily BaoStock request hard limit reached")
        if self.settings.offline:
            return self._execute(bs, method, p, use_cache)
        if not self._session_fresh():
            self._reconnect(bs)
        try:
            return self._execute(bs, method, p, use_cache)
        except ReconnectableFailure:
            if not (self.settings.reconnect_on_failure and not self.breaker.is_set()):
                raise
            self._reconnect(bs)
            return self._execute(bs, method, p, use_cache)
        except RECONNECTABLE_NETWORK_EXC:
            if not (self.settings.reconnect_on_failure and not self.breaker.is_set()):
                raise
            self._reconnect(bs)
            return self._execute(bs, method, p, use_cache)

    def _execute(self, bs, method: str, p: dict[str, Any], use_cache: bool = True):
        if self.settings.offline:
            self._record_attempt(method, p, "offline_simulation")
            count = self.storage.usage_today_conservative()
            fields = [field.strip() for field in p.get("fields", "date,code").split(",")]
            # 离线模拟行：code 用代码、date 用 start_date、其余字段用合法数值，
            # 避免日期字符串被 _number 转浮点失败（A-02 字段提升测试依赖）。
            row = {}
            for field in fields:
                if field == "code":
                    row[field] = p.get("code", "")
                elif field == "date":
                    row[field] = p.get("start_date", "")
                else:
                    row[field] = "10.0"
            rows = [row]
        else:
            time.sleep(self.settings.min_interval_seconds)
            fn = getattr(bs, method, None)
            if fn is None or method not in SUPPORTED_METHODS:
                raise ValueError(f"Unsupported BaoStock method: {method}")
            rs = None
            attempts = self.settings.max_retries + 1
            for attempt in range(attempts):
                self._record_attempt(method, p, f"query_attempt_{attempt + 1}")
                try:
                    rs = fn(**p)
                except Exception as exc:
                    self.storage.audit({"interface": method, **p,
                                        "origin": self._active_job.origin if self._active_job else "maintenance",
                                        "request_id": self._active_job.request_id if self._active_job else "gateway-session",
                                        "operation": f"query_attempt_{attempt + 1}", "status": "exception",
                                        "error_type": type(exc).__name__, "error": str(exc)[:1000]}, self.settings.audit_log_max_bytes)
                    if not isinstance(exc, RECONNECTABLE_NETWORK_EXC) or attempt + 1 == attempts:
                        raise
                    delay = self.settings.retry_delays[min(attempt, len(self.settings.retry_delays) - 1)] if self.settings.retry_delays else 5
                    time.sleep(delay)
                    continue
                if rs.error_code == "0":
                    break
                if rs.error_code == BLACKLIST_CODE:
                    self.breaker.set()
                    raise RuntimeError(f"BaoStock query failed: {rs.error_code} {rs.error_msg}")
                self.storage.audit({"interface": method, **p,
                                    "origin": self._active_job.origin if self._active_job else "maintenance",
                                    "request_id": self._active_job.request_id if self._active_job else "gateway-session",
                                    "operation": f"query_attempt_{attempt + 1}", "status": "upstream_error",
                                    "error_code": rs.error_code, "error": str(rs.error_msg)[:1000]}, self.settings.audit_log_max_bytes)
                if attempt + 1 == attempts:
                    raise ReconnectableFailure(f"BaoStock query failed after {attempts} attempts: {rs.error_code} {rs.error_msg}")
                delay = self.settings.retry_delays[min(attempt, len(self.settings.retry_delays) - 1)] if self.settings.retry_delays else 5
                time.sleep(delay)
            self.last_activity = time.monotonic()
            rows = []
            loop_started = time.monotonic()
            while rs.next():
                rows.append(dict(zip(rs.fields, rs.get_row_data())))
                # 死循环防护：baostock 半开连接下 rs.next() 可能永远返回 True。
                # 行数上限 + 遍历超时双保险，避免 worker 被占死（CPU 空转、Job 排队）。
                if len(rows) > self.settings.max_result_rows:
                    raise RuntimeError(
                        f"BaoStock result set exceeded {self.settings.max_result_rows} rows; "
                        "aborting to prevent rs.next() dead-loop"
                    )
                if time.monotonic() - loop_started > self.settings.watchdog_timeout_seconds:
                    raise ReconnectableFailure(
                        "BaoStock rs.next() stalled (result set read timed out); "
                        "connection likely half-open"
                    )
                if len(rows) % 100 == 0:
                    self._beat()
            count = self.storage.usage_today_conservative()
        payload = {"data": rows, "cache_hit": False, "request_count_today": count}
        if use_cache:
            self.storage.put_cache(self._key(method, p), rows)
        job = self._active_job
        self.storage.audit({"interface": method, **p, "cache_hit": False, "request_count_today": count,
                            "error_code": "0", "origin": job.origin if job else "maintenance",
                            "request_id": job.request_id if job else "gateway-session", "status": "query_succeeded"},
                           self.settings.audit_log_max_bytes)
        return payload

    def _key(self, method, params):
        return hashlib.sha256(json.dumps({"method": method, "params": params}, sort_keys=True).encode()).hexdigest()
