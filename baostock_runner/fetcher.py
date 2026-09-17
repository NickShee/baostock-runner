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
        kw.setdefault("updated_at", datetime.datetime.now(datetime.timezone.utc).isoformat())
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
        """执行一个批次的工作；返回是否发生了真实的 BaoStock 请求。"""
        if self._need_universe_init():
            return self._fetch_universe_init()
        if self._need_calendar():
            return self._fetch_calendar()
        if self._need_industry():
            return self._fetch_industry()
        if self._need_index_refresh():
            return self._fetch_index()
        if self._need_daily_backfill():
            return self._fetch_daily_batch()
        if self._need_financial_backfill():
            return self._fetch_financial_batch()
        if self.settings.fetch_include_dividends and self._need_dividends():
            return self._fetch_dividends_batch()
        if self.settings.fetch_include_adjust_factors and self._need_adjust_factors():
            return self._fetch_adjust_factors_batch()
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
        today = datetime.date.today().isoformat()
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
        meta = self.storage.get_meta("trade_calendar")
        if meta is None:
            return True
        # 每月刷新一次（拉到今天）
        last = meta.get("last_updated", "")
        return not last.startswith(datetime.date.today().strftime("%Y-%m"))

    def _fetch_calendar(self) -> bool:
        start = self.settings.fetch_daily_start_date
        end = datetime.date.today().isoformat()
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
            return (datetime.date.today() - d).days >= 7
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
        return not meta.get("last_updated", "").startswith(datetime.date.today().isoformat())

    def _fetch_index(self) -> bool:
        self._set_state(current="refreshing index constituents")
        today = datetime.date.today().isoformat()
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
        today = datetime.date.today().isoformat()
        return self.storage.latest_trade_date(today) or today

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
                self.gateway.daily_bars({
                    "code": code,
                    "fields": DAILY_FIELDS,
                    "start_date": self.settings.fetch_daily_start_date,
                    "end_date": latest,
                    "frequency": "d",
                    "adjustflag": af,
                }, use_cache=False)
                # 真实请求成功后才计入下载预算（gateway 侧只计总 usage）。
                self.storage.increment_download_usage()
                self.storage.job_upsert("daily_bars", f"{code}|{af}", "done")
            done += 1
            self._set_state(done=done)
        self._set_state(current=f"daily backfill batch done: {done} codes")
        return True

    # ---------- financials backfill (按 code × year × quarter，倒序优先最新) ----------

    def _need_financial_backfill(self) -> bool:
        return self.storage.count_securities() > 0

    def _fetch_financial_batch(self) -> bool:
        codes = self.storage.get_securities_codes()
        if not codes:
            return False
        datasets = [x.strip() for x in self.settings.fetch_financial_datasets.split(",") if x.strip()] or ["profit"]
        current_year = datetime.date.today().year
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            for year in range(current_year, self.settings.fetch_financial_start_year - 1, -1):
                for quarter in (4, 3, 2, 1):
                    for ds in datasets:
                        if ds not in FINANCIAL_METHODS:
                            continue
                        if self.storage.financial_exists(ds, code, year, quarter):
                            continue
                        self._set_state(current=f"financials: {code} {year}Q{quarter} {ds}",
                                        dataset="financials", done=1, total=1)
                        res = self._bounded_call(FINANCIAL_METHODS[ds], {"code": code, "year": year, "quarter": quarter})
                        for row in res["data"]:
                            self.storage.put_financials(ds, code, year, quarter, row)
                        self.storage.job_upsert("financials", f"{code}|{year}Q{quarter}|{ds}", "done")
                        return True  # 一次只处理一个请求，回到主循环继续（便于预算/心跳控制）
        return False

    # ---------- dividends / adjust factors (默认关闭) ----------

    def _need_dividends(self) -> bool:
        return True

    def _fetch_dividends_batch(self) -> bool:
        codes = self.storage.get_securities_codes()
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            if self.storage.dividend_exists(code, 0, "all"):
                continue
            self._set_state(current=f"dividends: {code}", dataset="dividends")
            res = self._bounded_call("query_dividend_data", {"code": code, "year": 0})
            self.storage.put_dividend(code, 0, "all", res["data"])
            self.storage.job_upsert("dividends", f"{code}|all", "done")
            return True
        return False

    def _fetch_adjust_factors_batch(self) -> bool:
        codes = self.storage.get_securities_codes()
        for code in codes:
            if self.budget_left() <= 0:
                raise BudgetExhausted()
            if self.storage.job_status("adjust_factors", code) == "done":
                continue
            self._set_state(current=f"adjust_factors: {code}", dataset="adjust_factors")
            res = self._bounded_call("query_adjust_factor", {
                "code": code,
                "start_date": self.settings.fetch_daily_start_date,
                "end_date": datetime.date.today().isoformat(),
            })
            rows = [r for r in res["data"] if r.get("date")]
            self.storage.put_adjust_factors(code, rows)
            self.storage.job_upsert("adjust_factors", code, "done")
            return True
        return False

    # ---------- helper ----------

    def _bounded_call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """带预算闸门的 gateway 调用：强制绕过参数缓存并计入 download_usage。"""
        if self.budget_left() <= 0:
            raise BudgetExhausted()
        result = self.gateway.call(method, params, use_cache=False)
        # 真实请求成功后才计入下载预算（gateway 侧只计总 usage）。
        self.storage.increment_download_usage()
        return result
