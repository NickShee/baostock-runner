from dataclasses import dataclass
import hashlib
import json
import queue
import socket
import threading
import time
from typing import Any

from .config import Settings
from .storage import Storage


BLACKLIST_CODE = "10001011"
# 网络层异常：连接被重置/超时/拒绝。出现这类异常时应重连而非盲目重试原会话。
RECONNECTABLE_NETWORK_EXC = (ConnectionError, socket.timeout, BrokenPipeError, OSError)


class ReconnectableFailure(RuntimeError):
    """BaoStock 查询在重试耗尽后仍失败，可能通过重连恢复（非黑名单、非纯参数错误）。"""


SUPPORTED_METHODS = {
    "query_history_k_data_plus", "query_trade_dates", "query_all_stock",
    "query_stock_basic", "query_stock_industry", "query_profit_data",
    "query_growth_data", "query_balance_data", "query_cash_flow_data",
    "query_dividend_data", "query_adjust_factor",
    "query_hs300_stocks", "query_sz50_stocks", "query_zz500_stocks",
}


@dataclass
class Job:
    method: str
    params: dict[str, Any]
    result: queue.Queue


class BaoStockGateway:
    """All BaoStock calls happen in this one worker thread and one session."""
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self.storage = Storage(self.settings.db_path, self.settings.log_path)
        self.jobs: queue.Queue[Job | None] = queue.Queue()
        self.stop_event = threading.Event()
        self.breaker = threading.Event()
        self.ready = threading.Event()
        self.startup_error: Exception | None = None
        # 会话保活：记录最后一次成功活动的时间（monotonic），超时则主动重连。
        self.last_activity = time.monotonic()
        self.session_lock = threading.Lock()
        self.worker = threading.Thread(target=self._worker, name="baostock-single-worker", daemon=True)
        self.worker.start()

    def close(self):
        self.stop_event.set()
        self.jobs.put(None)
        self.worker.join(timeout=10)

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.ready.wait(timeout=15):
            raise RuntimeError("BaoStock worker did not become ready")
        if self.startup_error is not None:
            raise self.startup_error
        if self.breaker.is_set():
            raise RuntimeError("BaoStock circuit breaker is open; manual intervention required")
        key = hashlib.sha256(json.dumps({"method": method, "params": params}, sort_keys=True).encode()).hexdigest()
        cached = self.storage.get_cache(key)
        if cached is not None:
            self.storage.audit({"interface": method, **params, "cache_hit": True, "request_count_today": self.storage.usage_today(), "error_code": "0"})
            return {"data": cached, "cache_hit": True, "request_count_today": self.storage.usage_today()}
        result: queue.Queue = queue.Queue(maxsize=1)
        self.jobs.put(Job(method, params, result))
        ok, value = result.get()
        if not ok:
            raise value
        return value

    def daily_bars(self, params: dict[str, Any]) -> dict[str, Any]:
        """Read local daily bars and only fetch the missing tail from BaoStock."""
        code, frequency, adjustflag = params["code"], params["frequency"], params["adjustflag"]
        start_date, end_date = params["start_date"], params["end_date"]
        local = self.storage.get_daily_bars(code, frequency, adjustflag, start_date, end_date)
        latest = self.storage.latest_daily_bar(code, frequency, adjustflag)
        fetched = False
        if latest is None or not local:
            query_params = dict(params)
        elif latest < end_date:
            query_params = dict(params, start_date=latest, end_date=end_date)
        else:
            query_params = None
        if query_params is not None:
            result = self.call("query_history_k_data_plus", query_params)
            new_rows = result["data"]
            self.storage.put_daily_bars(code, frequency, adjustflag, new_rows)
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
            "request_count_today": self.storage.usage_today(),
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
            self.ready.set()
            while not self.stop_event.is_set():
                job = self.jobs.get()
                if job is None:
                    break
                try:
                    value = self._execute_robust(bs, job.method, job.params)
                    job.result.put((True, value))
                except Exception as exc:
                    job.result.put((False, exc))
        except Exception as exc:
            self.startup_error = exc
            self.ready.set()
            while True:
                try:
                    job = self.jobs.get_nowait()
                except queue.Empty:
                    break
                if job is not None:
                    job.result.put((False, exc))
        finally:
            if logged_in and bs is not None:
                bs.logout()

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
                rs = bs.query_stock_basic(code="sh.000001")
                if rs.error_code != "0":
                    raise RuntimeError(f"BaoStock post-login verification failed: {rs.error_code} {rs.error_msg}")
            self.last_activity = time.monotonic()

    def _execute_robust(self, bs, method: str, p: dict[str, Any]):
        """健壮执行路径：先保证会话新鲜，失败后视类型重连一轮再试。"""
        if self.storage.usage_today() >= self.settings.daily_hard_limit:
            raise RuntimeError("Daily BaoStock request hard limit reached")
        if self.settings.offline:
            return self._execute(bs, method, p)
        if not self._session_fresh():
            self._reconnect(bs)
        try:
            return self._execute(bs, method, p)
        except ReconnectableFailure:
            if not (self.settings.reconnect_on_failure and not self.breaker.is_set()):
                raise
            self._reconnect(bs)
            return self._execute(bs, method, p)
        except RECONNECTABLE_NETWORK_EXC:
            if not (self.settings.reconnect_on_failure and not self.breaker.is_set()):
                raise
            self._reconnect(bs)
            return self._execute(bs, method, p)

    def _execute(self, bs, method: str, p: dict[str, Any]):
        if self.storage.usage_today() >= self.settings.daily_hard_limit:
            raise RuntimeError("Daily BaoStock request hard limit reached")
        if self.settings.offline:
            fields = [field.strip() for field in p.get("fields", "date,code").split(",")]
            row = {field: (p.get("code") if field == "code" else p.get("start_date", "")) for field in fields}
            rows = [row]
            count = self.storage.increment_usage()
        else:
            time.sleep(self.settings.min_interval_seconds)
            fn = getattr(bs, method, None)
            if fn is None or method not in SUPPORTED_METHODS:
                raise ValueError(f"Unsupported BaoStock method: {method}")
            rs = None
            attempts = self.settings.max_retries + 1
            for attempt in range(attempts):
                rs = fn(**p)
                if rs.error_code == "0":
                    break
                if rs.error_code == BLACKLIST_CODE:
                    self.breaker.set()
                    raise RuntimeError(f"BaoStock query failed: {rs.error_code} {rs.error_msg}")
                if attempt + 1 == attempts:
                    raise ReconnectableFailure(f"BaoStock query failed after {attempts} attempts: {rs.error_code} {rs.error_msg}")
                delay = self.settings.retry_delays[min(attempt, len(self.settings.retry_delays) - 1)] if self.settings.retry_delays else 5
                time.sleep(delay)
            self.last_activity = time.monotonic()
            rows = []
            while rs.next():
                rows.append(dict(zip(rs.fields, rs.get_row_data())))
            count = self.storage.increment_usage()
        payload = {"data": rows, "cache_hit": False, "request_count_today": count}
        self.storage.put_cache(self._key(method, p), rows)
        self.storage.audit({"interface": method, **p, "cache_hit": False, "request_count_today": count, "error_code": "0"})
        return payload

    def _key(self, method, params):
        return hashlib.sha256(json.dumps({"method": method, "params": params}, sort_keys=True).encode()).hexdigest()
