"""后台分批次下载器（P1）。

设计约束（BaoStock 官方：非线程安全，并行需用多进程）：
- fetcher 不直连 BaoStock，所有请求都通过 gateway 的唯一 worker 队列执行
  （gateway.call / gateway.daily_bars），天然串行，与 MCP 请求共享同一条连接。
- 预算隔离：fetcher 独立使用 download_usage 计数，硬顶 =
  daily_hard_limit * fetch_budget_ratio（默认 2/3 = 0.67）。达到即软暂停
  （不抛错、不抢占），次日按天计数自动恢复。MCP 至少保留 1/3 额度。
- 断点续传：download_jobs 表记录每个 (dataset, batch_id) 的状态；
  已 done 的批次重启后自动跳过。
- 幂等：所有事实表使用复合主键 + INSERT OR REPLACE，可重复执行。
- 日线"当日去重"：今天已 done 的股票当天不再重复拉取，跨天自动重新检查
  （实现每日增量）。
"""

import datetime
import threading
import time
from typing import Any

from .config import Settings
from .gateway import BaoStockGateway
from .timeutil import Clock, parse_check_time


class BudgetExhausted(Exception):
    """fetcher 当日下载预算已用完（软暂停信号，次日自动恢复）。"""


FINANCIAL_METHODS = {
    "profit": "query_profit_data",
    "growth": "query_growth_data",
    "balance": "query_balance_data",
    "cash_flow": "query_cash_flow_data",
    "operation": "query_operation_data",
    "dupont": "query_dupont_data",
}

# 日线回填字段（与 MCP get_stock_daily_bars 默认字段一致，多带估值/ST 字段）
DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,"
    "turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
)


class Fetcher:
    def __init__(self, gateway: BaoStockGateway, settings: Settings | None = None):
        self.gateway = gateway
        self.settings = settings or gateway.settings
        self.storage = gateway.storage
        # A-01: 统一时间。业务日/预算按 Asia/Shanghai；审计时间 UTC。
        self.clock: Clock = gateway.clock
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()
        self._state_lock = threading.Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "current": "not started",
            "dataset": "",
            "done": 0,
            "total": 0,
            "last_error": "",
            "updated_at": "",
        }
        self.thread = threading.Thread(target=self._run, name="baostock-fetcher", daemon=True)

    # ---------- A-01: 业务日/时间辅助 ----------

    @property
    def business_today(self) -> str:
        """当前上海业务日（YYYY-MM-DD）。"""
        return self.clock.business_date().isoformat()

    # ---------- lifecycle ----------

    def start(self):
        if not self.settings.fetch_enabled:
            return
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.wake_event.set()

    def wake(self, dataset: str = ""):
        """立即唤醒一次调度循环（供 start_backfill MCP 工具使用）。"""
        if dataset:
            self._set_state(dataset=dataset)
        self.wake_event.set()

    @property
    def state(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self._state)

    def _set_state(self, **kw):
        kw.setdefault("updated_at", self.clock.now_utc().isoformat())
        with self._state_lock:
            self._state.update(kw)

    # ---------- budget ----------

    @property
    def fetch_budget(self) -> int:
        return int(self.settings.daily_hard_limit * self.settings.fetch_budget_ratio)

    def budget_left(self) -> int:
        return max(0, self.fetch_budget - self.storage.download_usage_today())

    # ---------- main loop ----------

    def _sleep(self, seconds: float):
        self.wake_event.wait(timeout=seconds)
        self.wake_event.clear()

    def _run(self):
        self._set_state(current="fetcher started")
        # A-03: 重启回收——进程重启后回收过期 running 任务，避免任务永久卡死。
        try:
            recovered = self.storage.job_recover_stale_running()
            if recovered:
                self._set_state(current=f"recovered {recovered} stale running job(s)")
        except Exception as exc:
            self._set_state(last_error=f"startup recovery failed: {exc}")
        while not self.stop_event.is_set():
            if self.settings.offline:
                self._set_state(running=False, current="offline mode; fetcher idle")
                self._sleep(60)
                continue
            try:
                if self.budget_left() <= 0:
                    self._set_state(running=False, current="download budget exhausted; paused until next day")
                    self._sleep(self.settings.fetch_pause_sleep_seconds)
                    continue
                self._set_state(running=True)
                did_work = self._step_once()
                if did_work:
                    continue
                self._set_state(running=False, current="all fetch tasks caught up; idle")
                self._sleep(self.settings.fetch_idle_sleep_seconds)
            except BudgetExhausted:
                self._set_state(running=False, current="download budget exhausted; paused until next day")
                self._sleep(self.settings.fetch_pause_sleep_seconds)
            except Exception as exc:
                self._set_state(running=False, current=f"error: {exc}", last_error=str(exc))
                self._sleep(60)

    def _step_once(self) -> bool:
        """执行一个批次的工作；返回是否发生了真实的 BaoStock 请求。

        初始化类任务（证券池/日历/行业/成分）必须短路完成；
        轮询类任务（日线/财务/分红/复权）逐个尝试，任何一个做了活就返回 True，
        避免因某类任务暂时无活而短路掉后续阶段（如日线完成后必须继续财务）。
        """
        if self._need_universe_init():
            return self._fetch_universe_init()
        if self._need_calendar():
            return self._fetch_calendar()
        if self._need_industry():
            return self._fetch_industry()
        if self._need_index_refresh():
            return self._fetch_index()
        if self._fetch_daily_batch():
            return True
        if self._fetch_financial_batch():
            return True
        if self.settings.fetch_include_dividends and self._fetch_dividends_batch():
            return True
        if self.settings.fetch_include_adjust_factors and self._fetch_adjust_factors_batch():
            return True
        return False

    # ---------- init: securities pool + base data ----------

    def _need_universe_init(self) -> bool:
        return self.storage.count_securities() == 0

    def _fetch_universe_init(self) -> bool:
        if self.settings.fetch_universe != "hs300":
            self._set_state(last_error=f"universe '{self.settings.fetch_universe}' not supported in P1")
            return False
        self._set_state(current="init: fetching hs300 constituents")
        res = self._bounded_call("query_hs300_stocks", {})
        constituents = res["data"]
        if not constituents:
            raise RuntimeError("query_hs300_stocks returned no constituents")
        today = self.business_today
        self.storage.put_index_constituents("hs300", today, constituents)
        codes = [r["code"] for r in constituents]
        basic_res = self._bounded_call("query_stock_basic", {"code": "", "code_name": ""})
        basic_by_code = {r["code"]: r for r in basic_res["data"]}
        rows = []
        for code in codes:
            b = basic_by_code.get(code, {})
            rows.append({
                "code": code,
                "name": b.get("code_name", ""),
                "trade_status": "",
                "ipo_date": b.get("ipoDate", ""),
                "out_date": b.get("outDate", ""),
                "type": b.get("type", ""),
                "status": b.get("status", ""),
            })
        self.storage.put_securities(rows)
        self.storage.set_meta("securities", detail=f"hs300 count={len(rows)} asof={today}")
        self._set_state(current=f"init done: securities={len(rows)}", done=len(rows), total=len(rows))
        return True

    def _need_calendar(self) -> bool:
        """A-01: 按覆盖终点触发补齐。

        目标终点 = 上海业务日 + 可配置预拉窗口（默认 0 = 不假设未来）。
        本地日历最大日期 < 目标终点即触发补齐；不依赖 meta 月份判断。
        """
        max_date = self.storage.calendar_max_date()
        if max_date is None:
            return True
        target = self._calendar_target_end()
        return max_date < target

    def _calendar_target_end(self) -> str:
        """日历补齐目标终点：上海业务日 + prelook 窗口。"""
        end = datetime.date.fromisoformat(self.business_today)
        if self.settings.calendar_prelook_days > 0:
            end += datetime.timedelta(days=self.settings.calendar_prelook_days)
        return end.isoformat()

    def _fetch_calendar(self) -> bool:
        start = self.settings.fetch_daily_start_date
        end = self._calendar_target_end()
        self._set_state(current="init: fetching trade calendar")
        res = self._bounded_call("query_trade_dates", {"start_date": start, "end_date": end})
        rows = [
            {"calendar_date": r["calendar_date"], "is_trading_day": int(r["is_trading_day"])}
            for r in res["data"] if r.get("calendar_date")
        ]
        self.storage.put_trade_calendar(rows)
        self.storage.set_meta("trade_calendar", detail=f"rows={len(rows)} range={start}..{end}")
        self._set_state(current=f"trade calendar refreshed: {len(rows)} days")
        return True

    def _need_industry(self) -> bool:
        meta = self.storage.get_meta("stock_industry")
        if meta is None:
            return True
        last = meta.get("last_updated", "")
        try:
            d = datetime.date.fromisoformat(last[:10])
            return (datetime.date.fromisoformat(self.business_today) - d).days >= 7
        except ValueError:
            return True

    def _fetch_industry(self) -> bool:
        self._set_state(current="init: fetching industry classification")
        res = self._bounded_call("query_stock_industry", {"code": ""})
        codes = set(self.storage.get_securities_codes())
        rows = [
            {
                "code": r["code"],
                "industry": r.get("industry", ""),
                "classification": r.get("industryClassification", ""),
                "update_date": r.get("updateDate", ""),
            }
            for r in res["data"] if r.get("code") in codes
        ]
        self.storage.put_stock_industry(rows)
        self.storage.set_meta("stock_industry", detail=f"rows={len(rows)}")
        self._set_state(current=f"industry refreshed: {len(rows)} rows")
        return True

    def _need_index_refresh(self) -> bool:
        meta = self.storage.get_meta("index_constituents")
        if meta is None:
            return True
        return not meta.get("last_updated", "").startswith(self.business_today)

    def _fetch_index(self) -> bool:
        self._set_state(current="refreshing index constituents")
        today = self.business_today
        for index_code, method in (
            ("hs300", "query_hs300_stocks"),
            ("sz50", "query_sz50_stocks"),
            ("zz500", "query_zz500_stocks"),
        ):
            res = self._bounded_call(method, {})
            rows = [{"code": r["code"], "name": r.get("code_name", "")} for r in res["data"] if r.get("code")]
            self.storage.put_index_constituents(index_code, today, rows)
        self.storage.set_meta("index_constituents", detail=f"asof={today}")
        self._set_state(current=f"index constituents refreshed asof={today}")
        return True

    # ---------- daily bars backfill (分批) ----------

    def _latest_trade_date(self) -> str:
        """A-01: 目标交易日。

        - 当日检查默认从上海时间 daily_check_time（默认 18:00）开始，仅代表"开始尝试"。
        - 盘前/盘中（早于检查时间）以最近已结束交易日为目标，不把今天当作已完成。
        - 空结果不代表当天完成（由覆盖检查/任务状态在 A-02/A-03 负责）。
        """
        now_sh = self.clock.now_shanghai()
        today = now_sh.date()
        check = parse_check_time(self.settings.daily_check_time)
        if now_sh.time() >= check and self.storage.is_trading_day(today.isoformat()):
            # 已过检查时间且今天是交易日：目标 = 今天（开始尝试）
            return today.isoformat()
        # 否则以最近已结束交易日为目标：严格早于今天（今天尚未结束/不是交易日）
        yesterday = (today - datetime.timedelta(days=1)).isoformat()
        return self.storage.latest_trade_date(yesterday) or yesterday

    def _need_daily_backfill(self) -> bool:
        return self.storage.count_securities() > 0

    def _adjustflags(self) -> list[str]:
        return [x.strip() for x in self.settings.fetch_adjustflags.split(",") if x.strip()] or ["3"]

    def _fetch_daily_batch(self) -> bool:
        latest = self._latest_trade_date()
        adjustflags = self._adjustflags()
        pending = self.storage.get_daily_pending_codes(
            limit=self.settings.fetch_batch_size,
            end_date=latest,
            primary_adjustflag=adjustflags[0],
        )
        if not pending:
            return False
        self._set_state(current=f"daily backfill: {len(pending)} codes", dataset="daily_bars",
                        total=len(pending), done=0)
        done = 0
        for code in pending:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            for af in adjustflags:
                # A-02：按缺口窗口补齐（头部/中间/尾部），不预生成全市场历史笛卡尔积。
                # 用本地交易日历判断应覆盖日期，把缺口合并为连续请求窗口逐段请求。
                trade_dates = self._trade_dates_in_range(
                    self.settings.fetch_daily_start_date, latest)
                coverage = self.storage.get_daily_coverage(
                    [code], self.settings.fetch_daily_start_date, latest, af,
                    trade_dates=trade_dates)
                gaps = coverage["per_code"][code]["gap_windows"]
                # 回退：本地交易日历缺失时（异常），按整个请求区间作为一个窗口补齐。
                if not gaps and not trade_dates:
                    gaps = [{"start": self.settings.fetch_daily_start_date,
                             "end": latest, "days": 0}]
                if gaps:
                    for win in gaps:
                        if self.budget_left() <= 0:
                            raise BudgetExhausted()
                        self.gateway.daily_bars({
                            "code": code,
                            "fields": DAILY_FIELDS,
                            "start_date": win["start"],
                            "end_date": win["end"],
                            "frequency": "d",
                            "adjustflag": af,
                        }, use_cache=False, priority=1)
                        self.storage.increment_download_usage()
                # 无缺口时无需请求；仍记录 job（用于当日去重/断点续传）。
                self.storage.job_upsert("daily_bars", f"{code}|{af}", self.storage.JOB_SUCCEEDED)
            done += 1
            self._set_state(done=done)
        self._set_state(current=f"daily backfill batch done: {done} codes")
        return True

    def _trade_dates_in_range(self, start_date: str, end_date: str) -> list[str]:
        """读取本地交易日历在 [start, end] 内的交易日列表（用于覆盖检查）。"""
        with self.storage._session() as db:
            rows = db.execute(
                "SELECT calendar_date FROM trade_calendar "
                "WHERE is_trading_day=1 AND calendar_date BETWEEN ? AND ? ORDER BY calendar_date",
                (start_date, end_date)).fetchall()
        return [r[0] for r in rows]

    # ---------- financials backfill (A-03: 状态机 + 修订检查 + 退避) ----------

    def _need_financial_backfill(self) -> bool:
        return self.storage.count_securities() > 0

    def _current_ended_quarter(self) -> tuple[int, int]:
        """当前已结束季度：上海业务日所在季度往前推（季度结束后才视为已结束）。

        例：10月（Q4 进行中）→ 已结束季度为 Q3；1月 → 上一年 Q4。
        """
        now = self.clock.business_date()
        quarter = (now.month - 1) // 3 + 1
        if quarter == 1:
            return now.year - 1, 4
        return now.year, quarter - 1

    def _is_future_quarter(self, year: int, quarter: int) -> bool:
        """未来未结束季度不入队。"""
        ended = self._current_ended_quarter()
        return (year, quarter) > ended

    def _retry_delay(self, attempts: int) -> str:
        """指数退避：60s 起、翻倍、最长 1h；返回 UTC ISO 时间（可注入时钟）。"""
        delay = min(self.settings.task_retry_base_seconds * (2 ** max(0, attempts - 1)),
                    self.settings.task_retry_max_seconds)
        return self.storage.clock.now_utc().timestamp() + delay

    def _iso_from_ts(self, ts: float) -> str:
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()

    def _financial_job_due(self, ds: str, code: str, year: int, quarter: int) -> tuple[bool, str]:
        """判断财务任务是否到期执行；返回 (是否执行, 原因)。

        - succeeded：按修订检查周期（近两季度每天/其余 30 天）判断是否到期重查。
        - waiting_data：next_retry_at 到期（空财报 24h 后重查）。
        - retryable_failed/pending/无记录：执行。
        - permanent_failed：不再执行（需人工处置）。
        """
        job_id = f"{code}|{year}Q{quarter}|{ds}"
        status = self.storage.job_status("financials", job_id)
        now_iso = self.storage.clock.now_utc().isoformat()
        if status == self.storage.JOB_PERMANENT_FAILED:
            return False, "permanent_failed"
        if status == self.storage.JOB_SUCCEEDED:
            fetched = self.storage.financial_fetched_at(ds, code, year, quarter)
            if not fetched:
                return True, "succeeded but no fact row (recheck)"
            ended_year, ended_q = self._current_ended_quarter()
            recent = (year, quarter) >= (ended_year - 0, ended_q - 1) and \
                     (year, quarter) <= (ended_year, ended_q)
            # 近两个已结束季度每天检查，其余 30 天检查
            days = self.settings.financial_revision_check_days if recent else \
                self.settings.financial_revision_check_days_full
            from datetime import datetime as _dt
            fetched_dt = _dt.fromisoformat(fetched)
            threshold = self.storage.clock.now_utc().timestamp() - days * 86400
            if fetched_dt.timestamp() < threshold:
                return True, f"revision check due (>{days}d)"
            return False, "revision check not due"
        if status == self.storage.JOB_WAITING_DATA:
            row = self.storage._job_row("financials", job_id)
            if row and row["next_retry_at"] and row["next_retry_at"] > now_iso:
                return False, "waiting_data not due"
            return True, "waiting_data retry due"
        return True, f"status={status or 'none'}"

    def _fetch_financial_batch(self) -> bool:
        codes = self.storage.get_securities_codes()
        if not codes:
            return False
        datasets = [x.strip() for x in self.settings.fetch_financial_datasets.split(",") if x.strip()] or ["profit"]
        current_year = self.clock.business_date().year
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            for year in range(current_year, self.settings.fetch_financial_start_year - 1, -1):
                for quarter in (4, 3, 2, 1):
                    if self._is_future_quarter(year, quarter):
                        continue  # 未来未结束季度不入队
                    for ds in datasets:
                        if ds not in FINANCIAL_METHODS:
                            continue
                        due, reason = self._financial_job_due(ds, code, year, quarter)
                        if not due:
                            continue
                        job_id = f"{code}|{year}Q{quarter}|{ds}"
                        self._set_state(current=f"financials: {code} {year}Q{quarter} {ds} ({reason})",
                                        dataset="financials", done=1, total=1)
                        try:
                            res = self._bounded_call(
                                FINANCIAL_METHODS[ds],
                                {"code": code, "year": year, "quarter": quarter})
                            # 同事务提交：财务数据 + 任务成功状态（成功数据与作业完成状态同事务）。
                            with self.storage._session() as db:
                                if res["data"]:
                                    for row in res["data"]:
                                        self.storage._put_financials_in_txn(
                                            db, ds, code, year, quarter, row,
                                            fetched_at=self.storage.clock.now_utc().isoformat())
                                    self.storage._job_upsert_in_txn(
                                        db, "financials", job_id, self.storage.JOB_SUCCEEDED,
                                        rows_written=len(res["data"]))
                                else:
                                    # 空财报：不写事实行，标记 waiting_data 等待披露窗口重查。
                                    self.storage._job_upsert_in_txn(
                                        db, "financials", job_id, self.storage.JOB_WAITING_DATA,
                                        next_retry_at=self._iso_from_ts(
                                            self.storage.clock.now_utc().timestamp()
                                            + self.settings.financial_waiting_retry_hours * 3600),
                                        error="empty report period (waiting for release)")
                            return True
                        except BudgetExhausted:
                            raise
                        except Exception as exc:
                            # 网络失败指数退避；失败股票不阻塞后续任务（捕获后继续）。
                            attempts = self.storage.job_attempts("financials", job_id) + 1
                            self.storage.job_upsert(
                                "financials", job_id, self.storage.JOB_RETRYABLE_FAILED,
                                error=str(exc), error_class=type(exc).__name__,
                                next_retry_at=self._iso_from_ts(self._retry_delay(attempts)))
                            self._set_state(last_error=f"financials {job_id}: {exc}")
        return False

    # ---------- dividends / adjust factors (A-03: 检查窗口增量更新) ----------

    def _need_dividends(self) -> bool:
        return True

    def _fetch_dividends_batch(self) -> bool:
        """分红按检查窗口增量更新：不再按股票永久完成。

        succeeded 且未到检查窗口 → 跳过；否则重新查询并刷新事实行。
        """
        codes = self.storage.get_securities_codes()
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            job_id = f"{code}|all"
            status = self.storage.job_status("dividends", job_id)
            if status == self.storage.JOB_SUCCEEDED:
                # 检查窗口：succeeded 后 30 天内不重复（分红低频，按窗口刷新）
                row = self.storage._job_row("dividends", job_id)
                if row and row["updated_at"]:
                    from datetime import datetime as _dt
                    try:
                        fetched_dt = _dt.fromisoformat(row["updated_at"])
                        if self.storage.clock.now_utc().timestamp() - fetched_dt.timestamp() < 30 * 86400:
                            continue
                    except ValueError:
                        pass
            self._set_state(current=f"dividends: {code}", dataset="dividends")
            try:
                res = self._bounded_call("query_dividend_data", {"code": code, "year": 0})
                self.storage.put_dividend(code, 0, "all", res["data"])
                self.storage.job_upsert("dividends", job_id, self.storage.JOB_SUCCEEDED,
                                        rows_written=len(res["data"]))
            except BudgetExhausted:
                raise
            except Exception as exc:
                attempts = self.storage.job_attempts("dividends", job_id) + 1
                self.storage.job_upsert("dividends", job_id, self.storage.JOB_RETRYABLE_FAILED,
                                        error=str(exc), error_class=type(exc).__name__,
                                        next_retry_at=self._iso_from_ts(self._retry_delay(attempts)))
            return True
        return False

    def _fetch_adjust_factors_batch(self) -> bool:
        """复权因子按检查窗口增量更新：不再按股票永久完成。"""
        codes = self.storage.get_securities_codes()
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            status = self.storage.job_status("adjust_factors", code)
            if status == self.storage.JOB_SUCCEEDED:
                row = self.storage._job_row("adjust_factors", code)
                if row and row["updated_at"]:
                    from datetime import datetime as _dt
                    try:
                        fetched_dt = _dt.fromisoformat(row["updated_at"])
                        if self.storage.clock.now_utc().timestamp() - fetched_dt.timestamp() < 7 * 86400:
                            continue
                    except ValueError:
                        pass
            self._set_state(current=f"adjust_factors: {code}", dataset="adjust_factors")
            try:
                res = self._bounded_call("query_adjust_factor", {
                    "code": code,
                    "start_date": self.settings.fetch_daily_start_date,
                    "end_date": self.business_today,
                })
                rows = [r for r in res["data"] if r.get("date")]
                self.storage.put_adjust_factors(code, rows)
                self.storage.job_upsert("adjust_factors", code, self.storage.JOB_SUCCEEDED,
                                        rows_written=len(rows))
            except BudgetExhausted:
                raise
            except Exception as exc:
                attempts = self.storage.job_attempts("adjust_factors", code) + 1
                self.storage.job_upsert("adjust_factors", code, self.storage.JOB_RETRYABLE_FAILED,
                                        error=str(exc), error_class=type(exc).__name__,
                                        next_retry_at=self._iso_from_ts(self._retry_delay(attempts)))
            return True
        return False

    # ---------- helper ----------

    def _bounded_call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """带预算闸门的 gateway 调用：强制绕过参数缓存并计入 download_usage。

        priority=1：fetcher 请求为低优先级，MCP 查询（priority=0）可插队。
        """
        if self.budget_left() <= 0:
            raise BudgetExhausted()
        result = self.gateway.call(method, params, use_cache=False, priority=1)
        # 真实请求成功后才计入下载预算（gateway 侧只计总 usage）。
        self.storage.increment_download_usage()
        return result
