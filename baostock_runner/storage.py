import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Callable

from .timeutil import Clock

# B-01: schema 版本。当前最新版本为 4。
# 迁移采用顺序执行：旧库（user_version < SCHEMA_VERSION）逐版本升级；
# 未知更高版本（user_version > SCHEMA_VERSION）启动时拒绝写入，防止旧代码破坏新库。
SCHEMA_VERSION = 4

# A-03: 任务状态机。
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_SUCCEEDED = "succeeded"
JOB_WAITING_DATA = "waiting_data"
JOB_RETRYABLE_FAILED = "retryable_failed"
JOB_PERMANENT_FAILED = "permanent_failed"
JOB_STATES = {JOB_PENDING, JOB_RUNNING, JOB_SUCCEEDED, JOB_WAITING_DATA,
              JOB_RETRYABLE_FAILED, JOB_PERMANENT_FAILED}

# 日线字段映射：上游字段名（baostock 返回键）→ daily_bars 存储列名。
DAILY_FIELD_MAP = {
    "open": "open", "high": "high", "low": "low", "close": "close",
    "preclose": "preclose", "volume": "volume", "amount": "amount",
    "pctChg": "pct_chg", "turn": "turn",
    "tradestatus": "tradestatus", "isST": "isST",
    "peTTM": "peTTM", "pbMRQ": "pbMRQ", "psTTM": "psTTM", "pcfNcfTTM": "pcfNcfTTM",
}
# 行情核心字段（不含估值/状态）：用于行情 vs 估值分别统计覆盖。
DAILY_MARKET_FIELDS = ("open", "high", "low", "close", "preclose", "volume", "amount", "pctChg", "turn")
# 估值字段：A-02 提升为可查询列。
DAILY_VALUATION_FIELDS = ("peTTM", "pbMRQ", "psTTM", "pcfNcfTTM")

# 迁移定义：version -> 有序步骤列表。每步为 SQL 字符串或 callable(db)。
# 步骤全部幂等（CREATE TABLE IF NOT EXISTS / ALTER TABLE ... IF 已存在检测），
# 旧结构（如 daily_bars 的 payload 旧表）在迁移中被识别并重命名保留。
MIGRATIONS: dict[int, list[Any]] = {
    1: [
        """CREATE TABLE IF NOT EXISTS cache (
            cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
            created_at TEXT NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS usage (
            usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""",
        # daily_bars 旧结构（只有 payload 无 code）迁移：重命名为 legacy 保留数据。
        # 幂等：仅当旧结构存在时才执行；新库首次建表时无旧表，不触发。
        lambda db: _migrate_daily_bars_legacy(db),
        """CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT NOT NULL,
            bar_date TEXT NOT NULL,
            frequency TEXT NOT NULL,
            adjustflag TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            pct_chg REAL,
            turn REAL,
            raw_json TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(code, bar_date, frequency, adjustflag))""",
        "CREATE INDEX IF NOT EXISTS idx_daily_bars_code_date ON daily_bars(code, bar_date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_bars_date ON daily_bars(bar_date)",
        """CREATE TABLE IF NOT EXISTS download_usage (
            usage_date TEXT PRIMARY KEY, request_count INTEGER NOT NULL DEFAULT 0)""",
        """CREATE TABLE IF NOT EXISTS securities (
            code TEXT PRIMARY KEY,
            name TEXT, trade_status TEXT,
            ipo_date TEXT, out_date TEXT,
            type TEXT, status TEXT,
            updated_at TEXT NOT NULL)""",
        "CREATE INDEX IF NOT EXISTS idx_securities_status ON securities(status)",
        """CREATE TABLE IF NOT EXISTS trade_calendar (
            calendar_date TEXT PRIMARY KEY, is_trading_day INTEGER NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS index_constituents (
            index_code TEXT NOT NULL, asof_date TEXT NOT NULL,
            code TEXT NOT NULL, name TEXT, fetched_at TEXT NOT NULL,
            PRIMARY KEY(index_code, asof_date, code))""",
        "CREATE INDEX IF NOT EXISTS idx_index_constituents_code ON index_constituents(index_code, asof_date)",
        """CREATE TABLE IF NOT EXISTS financials (
            dataset TEXT NOT NULL, code TEXT NOT NULL,
            year INTEGER NOT NULL, quarter INTEGER NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(dataset, code, year, quarter))""",
        "CREATE INDEX IF NOT EXISTS idx_financials_code ON financials(code)",
        """CREATE TABLE IF NOT EXISTS dividends (
            code TEXT NOT NULL, year INTEGER NOT NULL, year_type TEXT NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, year, year_type))""",
        """CREATE TABLE IF NOT EXISTS adjust_factors (
            code TEXT NOT NULL, factor_date TEXT NOT NULL,
            payload TEXT NOT NULL, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, factor_date))""",
        """CREATE TABLE IF NOT EXISTS stock_industry (
            code TEXT NOT NULL, industry TEXT NOT NULL,
            classification TEXT, update_date TEXT, fetched_at TEXT NOT NULL,
            PRIMARY KEY(code, industry, classification))""",
        """CREATE TABLE IF NOT EXISTS download_jobs (
            dataset TEXT NOT NULL, batch_id TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT, updated_at TEXT NOT NULL,
            PRIMARY KEY(dataset, batch_id))""",
        "CREATE INDEX IF NOT EXISTS idx_download_jobs_status ON download_jobs(dataset, status)",
        """CREATE TABLE IF NOT EXISTS dataset_meta (
            dataset TEXT PRIMARY KEY, last_updated TEXT, detail TEXT)""",
    ],
    # A-02: 提升日线字段为可查询列（preclose/tradestatus/isST/估值字段）。
    # 通过 ALTER TABLE ADD COLUMN 顺序迁移；列已存在时幂等跳过。
    2: [
        lambda db: _add_column_if_missing(db, "daily_bars", "preclose", "REAL"),
        lambda db: _add_column_if_missing(db, "daily_bars", "tradestatus", "TEXT"),
        lambda db: _add_column_if_missing(db, "daily_bars", "isST", "TEXT"),
        lambda db: _add_column_if_missing(db, "daily_bars", "peTTM", "REAL"),
        lambda db: _add_column_if_missing(db, "daily_bars", "pbMRQ", "REAL"),
        lambda db: _add_column_if_missing(db, "daily_bars", "psTTM", "REAL"),
        lambda db: _add_column_if_missing(db, "daily_bars", "pcfNcfTTM", "REAL"),
    ],
    # A-03: 任务状态机字段 + 财务修订历史表 + 旧 done 迁移。
    3: [
        lambda db: _add_column_if_missing(db, "download_jobs", "window_start", "TEXT"),
        lambda db: _add_column_if_missing(db, "download_jobs", "window_end", "TEXT"),
        lambda db: _add_column_if_missing(db, "download_jobs", "next_retry_at", "TEXT"),
        lambda db: _add_column_if_missing(db, "download_jobs", "lease_expires_at", "TEXT"),
        lambda db: _add_column_if_missing(db, "download_jobs", "rows_written", "INTEGER"),
        lambda db: _add_column_if_missing(db, "download_jobs", "error_class", "TEXT"),
        """CREATE TABLE IF NOT EXISTS financial_history (
            dataset TEXT NOT NULL, code TEXT NOT NULL,
            year INTEGER NOT NULL, quarter INTEGER NOT NULL,
            revision INTEGER NOT NULL, payload TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY(dataset, code, year, quarter, revision))""",
        # 旧 'done' 状态迁移为 'succeeded'（A-03：保留完成语义但允许后续按规则刷新）。
        lambda db: _migrate_legacy_done(db),
    ],
    # A-04: cache 表增加 method/expires_at/is_empty 列，支持分类型 TTL 与 stale 判定。
    4: [
        lambda db: _add_column_if_missing(db, "cache", "method", "TEXT"),
        lambda db: _add_column_if_missing(db, "cache", "expires_at", "TEXT"),
        lambda db: _add_column_if_missing(db, "cache", "is_empty", "INTEGER"),
        # 旧无分类缓存到期失效：迁移前 cache 行无 expires_at（NULL），
        # 读缓存时视为已过期（A-04：不清空事实表，仅停止命中）。
    ],
}


def _migrate_legacy_done(db: sqlite3.Connection) -> None:
    """A-03: 旧 download_jobs.status='done' 迁移为 'succeeded'。

    - 有事实记录：succeeded，允许按修订检查规则刷新（不无条件全量重抓）。
    - 空财报/分红/复权等按新规则在调度时重新评估，这里只做状态归一。
    """
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "download_jobs" not in tables:
        return
    db.execute("UPDATE download_jobs SET status='succeeded' WHERE status='done'")


def _add_column_if_missing(db: sqlite3.Connection, table: str, column: str, coltype: str) -> None:
    """幂等 ADD COLUMN：表或列不存在时跳过（迁移可重复执行、可跨版本安全）。"""
    tables = {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if table not in tables:
        return
    columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def _date_range(start_date: str, end_date: str) -> list[str]:
    """生成 [start, end] 逐日 ISO 日期（含边界）；仅作为交易日历缺失时的兜底。"""
    from datetime import date, timedelta
    out: list[str] = []
    cur = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _merge_date_windows(missing_dates: list[str]) -> list[dict[str, Any]]:
    """将缺失日期合并为连续窗口 [start,end]，用于生成请求批次而非笛卡尔积。

    输入为任意顺序的 ISO 日期，输出按时间排序的窗口列表。
    """
    from datetime import date, timedelta
    if not missing_dates:
        return []
    windows: list[dict[str, Any]] = []
    cur: list[date] = []
    for d in sorted(set(missing_dates)):
        day = date.fromisoformat(d)
        if cur and (day - cur[-1]).days == 1:
            cur.append(day)
        else:
            if cur:
                windows.append({"start": cur[0].isoformat(), "end": cur[-1].isoformat(),
                                "days": len(cur)})
            cur = [day]
    if cur:
        windows.append({"start": cur[0].isoformat(), "end": cur[-1].isoformat(),
                        "days": len(cur)})
    return windows


def _migrate_daily_bars_legacy(db: sqlite3.Connection) -> None:
    """daily_bars 旧结构（payload 列、无 code 列）迁移：重命名为 legacy 保留历史数据。"""
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "daily_bars" not in tables:
        return
    columns = {row[1] for row in db.execute("PRAGMA table_info(daily_bars)")}
    if columns and "payload" in columns and "code" not in columns:
        db.execute("ALTER TABLE daily_bars RENAME TO daily_bars_legacy")


class Storage:
    # A-03: 任务状态机常量（类属性别名，便于 storage 实例直接访问）。
    JOB_PENDING = JOB_PENDING
    JOB_RUNNING = JOB_RUNNING
    JOB_SUCCEEDED = JOB_SUCCEEDED
    JOB_WAITING_DATA = JOB_WAITING_DATA
    JOB_RETRYABLE_FAILED = JOB_RETRYABLE_FAILED
    JOB_PERMANENT_FAILED = JOB_PERMANENT_FAILED

    def __init__(self, db_path: str, log_path: str, clock: Clock | None = None,
                 settings: Any | None = None):
        self.db_path, self.log_path = db_path, log_path
        # A-04: 配置（缓存 TTL 等）。默认 Settings() 保持向后兼容。
        from .config import Settings
        self.settings = settings or Settings()
        # A-01: 可注入时钟。业务日/预算计数按 Asia/Shanghai 划日；审计时间 UTC。
        self.clock = clock or Clock()
        self._audit_lock = threading.Lock()
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        log_parent = os.path.dirname(log_path)
        if log_parent:
            os.makedirs(log_parent, exist_ok=True)
        with self._session() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA busy_timeout=30000")
            self._migrate(db)

    # ---------- A-01: 业务日（Asia/Shanghai）预算键与保守读取 ----------

    def _business_day(self) -> str:
        """当前业务日（Asia/Shanghai 日历日）作为预算键。"""
        return self.clock.business_date().isoformat()

    def _utc_day(self) -> str:
        """当前 UTC 日历日（用于兼容旧预算日记录）。"""
        return self.clock.utc_date().isoformat()

    def _count_on(self, table: str, day: str) -> int:
        with self._session() as db:
            row = db.execute(
                f"SELECT request_count FROM {table} WHERE usage_date=?", (day,)
            ).fetchone()
        return row[0] if row else 0

    def increment_usage(self) -> int:
        """A-01: 总请求预算按上海业务日计数。"""
        today = self._business_day()
        with self._session() as db:
            db.execute("INSERT INTO usage(usage_date, request_count) VALUES (?, 1) ON CONFLICT(usage_date) DO UPDATE SET request_count=request_count+1", (today,))
            return db.execute("SELECT request_count FROM usage WHERE usage_date=?", (today,)).fetchone()[0]

    def reserve_request(self, origin: str, total_limit: int, background_limit: int) -> tuple[int, int]:
        """Atomically reserve one upstream attempt before calling BaoStock.

        Failed responses and retries consume budget too. The background quota is
        a ceiling over the total daily budget and is checked in the same write
        transaction as the total counter.
        """
        today, legacy_day = self._business_day(), self._utc_day()
        with self._session() as db:
            db.execute("BEGIN IMMEDIATE")
            def count(table: str) -> int:
                vals = [db.execute(f"SELECT request_count FROM {table} WHERE usage_date=?", (d,)).fetchone() for d in {today, legacy_day}]
                return max((row[0] for row in vals if row), default=0)
            total = count("usage")
            if total >= total_limit:
                raise RuntimeError("Daily BaoStock request hard limit reached")
            background = count("download_usage")
            if origin == "fetcher" and background >= background_limit:
                raise RuntimeError("Daily background BaoStock request budget reached")
            db.execute("INSERT INTO usage(usage_date, request_count) VALUES (?, ?) ON CONFLICT(usage_date) DO UPDATE SET request_count=excluded.request_count", (today, total + 1))
            total += 1
            if origin == "fetcher":
                db.execute("INSERT INTO download_usage(usage_date, request_count) VALUES (?, ?) ON CONFLICT(usage_date) DO UPDATE SET request_count=excluded.request_count", (today, background + 1))
                background += 1
            return total, background

    def usage_today(self) -> int:
        """当前上海业务日已用量。"""
        return self._count_on("usage", self._business_day())

    def usage_today_conservative(self) -> int:
        """A-01: 切换预算日口径期间按新旧计数中较保守的已用量限制请求。

        旧版本以 UTC 日期为预算键；切换后新键为上海业务日。迁移当天新旧键可能
        并存（UTC 与上海相差 8 小时），取两者较大值，避免"重置额度"导致超限。
        不删除旧记录，保留原语义。
        """
        return max(self._count_on("usage", self._business_day()),
                   self._count_on("usage", self._utc_day()))

    def usage_today_legacy_utc(self) -> int:
        """旧口径（UTC 日）已用量，仅用于迁移期对账，不用于新请求限制。"""
        return self._count_on("usage", self._utc_day())

    def increment_download_usage(self) -> int:
        """A-01: fetcher 下载预算按上海业务日计数。"""
        today = self._business_day()
        with self._session() as db:
            db.execute("""INSERT INTO download_usage(usage_date, request_count) VALUES (?, 1)
                ON CONFLICT(usage_date) DO UPDATE SET request_count=request_count+1""", (today,))
            return db.execute("SELECT request_count FROM download_usage WHERE usage_date=?", (today,)).fetchone()[0]

    def download_usage_today(self) -> int:
        """当前上海业务日 fetcher 下载已用量。"""
        return self._count_on("download_usage", self._business_day())

    def download_usage_today_conservative(self) -> int:
        """切换期间较保守的下载已用量（新旧键取大）。"""
        return max(self._count_on("download_usage", self._business_day()),
                   self._count_on("download_usage", self._utc_day()))

    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=30)
        db.execute("PRAGMA busy_timeout=30000")
        # WAL 模式官方推荐组合：synchronous=NORMAL（每次 commit 不强制 fsync）。
        # __init__ 只在初始化连接设置过；若这里遗漏，每次 _session 新连接会回落到
        # 默认 FULL，导致每次 commit 都 fsync（慢存储上每次 1s+，B-02 高频预算计数受影响）。
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    @contextmanager
    def _session(self):
        db = self._connect()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    # ---------- B-01: schema 版本与顺序迁移 ----------

    def _schema_version(self, db: sqlite3.Connection) -> int:
        return db.execute("PRAGMA user_version").fetchone()[0]

    def _migrate(self, db: sqlite3.Connection) -> None:
        """顺序迁移：从当前 user_version 逐版本升级到 SCHEMA_VERSION。

        未知更高版本（user_version > SCHEMA_VERSION）时拒绝写入：
        该库由更新版本创建，旧代码写入可能破坏新结构。
        """
        current = self._schema_version(db)
        if current > SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema version {current} is newer than supported "
                f"version {SCHEMA_VERSION}; refusing to write"
            )
        for version in range(current + 1, SCHEMA_VERSION + 1):
            steps = MIGRATIONS.get(version)
            if not steps:
                continue
            for step in steps:
                if isinstance(step, str):
                    db.execute(step)
                else:
                    step(db)
            db.execute(f"PRAGMA user_version = {version}")

    # ---------- B-01: SQLite 一致性备份与隔离恢复 ----------

    def backup_to(self, path: str) -> dict[str, Any]:
        """创建一致性备份（SQLite Online Backup API）。

        在线备份不锁库、不受 WAL 影响，产生与当前提交状态一致的快照。
        返回备份文件的完整性校验与统计。
        """
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._connect() as src:
            dst = sqlite3.connect(path)
            try:
                src.backup(dst)
            finally:
                dst.close()
        return self.verify_backup(path)

    def restore_from(self, backup_path: str, target_path: str | None = None) -> dict[str, Any]:
        """从备份隔离恢复到目标路径（默认当前 db_path）。

        恢复前先验证备份完整性；恢复后对目标库做完整性检查、行数对账。
        target_path 用于隔离恢复演练（不覆盖当前库）；生产恢复应在停止写入后执行。
        """
        verified = self.verify_backup(backup_path)
        if not verified["ok"]:
            raise RuntimeError(f"backup {backup_path} failed integrity check: {verified['integrity']}")
        target = target_path or self.db_path
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        src = sqlite3.connect(backup_path)
        try:
            dst = sqlite3.connect(target)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
        return self.verify_backup(target)

    def verify_backup(self, path: str) -> dict[str, Any]:
        """对指定库文件做完整性检查、行数统计与 schema 版本读取（只读，不修改）。"""
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = [r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()]
            rows = {t: db.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
            return {
                "ok": integrity == "ok",
                "integrity": integrity,
                "user_version": version,
                "tables": tables,
                "rows": rows,
            }
        finally:
            db.close()

    def integrity_check(self) -> dict[str, Any]:
        """当前库完整性检查 + 行数统计（用于备份前后对账）。"""
        return self.verify_backup(self.db_path)

    # ---------- A-04: 参数缓存（分类型 TTL + 过期/stale 判定） ----------

    FINANCIAL_METHODS = {
        "query_profit_data", "query_growth_data", "query_balance_data",
        "query_cash_flow_data", "query_operation_data", "query_dupont_data",
    }
    BASIC_METHODS = {"query_stock_basic", "query_stock_industry"}
    QUOTE_METHODS = {"query_history_k_data_plus"}

    def cache_ttl_for(self, method: str, is_empty: bool = False,
                      recent: bool = False) -> int:
        """按方法类型与结果性质返回 TTL（秒）。"""
        if is_empty:
            return self.settings.cache_ttl_empty_seconds
        if method in self.FINANCIAL_METHODS or method in {
                "query_hs300_stocks", "query_sz50_stocks", "query_zz500_stocks"}:
            return self.settings.cache_ttl_financial_seconds
        if method in self.BASIC_METHODS:
            return self.settings.cache_ttl_basic_seconds
        if method in self.QUOTE_METHODS:
            return self.settings.cache_ttl_recent_quotes_seconds if recent \
                else self.settings.cache_ttl_historical_quotes_seconds
        return self.settings.cache_ttl_default_seconds

    def put_cache(self, key: str, value: Any, method: str = "",
                  is_empty: bool = False, ttl_seconds: int | None = None,
                  recent: bool = False):
        """写入缓存并记录 method/过期时间；ttl_seconds 为空时按 method 分类判定。"""
        now = self.clock.now_utc()
        ttl = self.cache_ttl_for(method, is_empty, recent) if ttl_seconds is None else ttl_seconds
        expires = (now.timestamp() + ttl)
        from datetime import datetime as _dt
        expires_iso = _dt.fromtimestamp(expires, tz=timezone.utc).isoformat()
        with self._session() as db:
            db.execute(
                "INSERT OR REPLACE INTO cache(cache_key, payload, created_at, method, expires_at, is_empty)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (key, json.dumps(value, ensure_ascii=False), now.isoformat(),
                 method, expires_iso, 1 if is_empty else 0))

    def get_cache(self, key: str):
        """读取缓存。

        返回 dict：{payload, method, expires_at, is_empty, stale}。
        - 不存在 → None。
        - 已过期（expires_at < now）或旧无分类缓存（expires_at NULL）→ stale=True
          （A-04：旧缓存到期失效，不清空事实表；上层可决定用 stale 还是重查）。
        """
        with self._session() as db:
            row = db.execute(
                "SELECT payload, created_at, method, expires_at, is_empty FROM cache WHERE cache_key=?",
                (key,)).fetchone()
        if row is None:
            return None
        payload, created_at, method, expires_at, is_empty = row
        now_ts = self.clock.now_utc().timestamp()
        stale = False
        if expires_at is None:
            # 旧无分类缓存：无过期时间，视为已过期（A-04 迁移前遗留）。
            stale = True
        else:
            try:
                from datetime import datetime as _dt
                stale = _dt.fromisoformat(expires_at).timestamp() < now_ts
            except ValueError:
                stale = True
        return {
            "payload": json.loads(payload),
            "method": method or "",
            "expires_at": expires_at,
            "is_empty": bool(is_empty),
            "stale": stale,
        }

    def get_cache_meta(self, key: str) -> dict | None:
        """仅返回缓存元信息（不解析 payload），用于本地优先判定。"""
        info = self.get_cache(key)
        if info is None:
            return None
        return {k: v for k, v in info.items() if k != "payload"}

    def audit(self, event: dict[str, Any], max_bytes: int = 20 * 1024 * 1024):
        event = {"timestamp": self.clock.now_utc().isoformat(), **event}
        line = json.dumps(event, ensure_ascii=False) + "\n"
        with self._audit_lock:
            try:
                if max_bytes > 0 and os.path.exists(self.log_path) and os.path.getsize(self.log_path) + len(line.encode("utf-8")) > max_bytes:
                    stamp = self.clock.now_utc().strftime("%Y%m%dT%H%M%S%fZ")
                    rotated_path = f"{self.log_path}.{stamp}"
                    suffix = 1
                    while os.path.exists(rotated_path):
                        rotated_path = f"{self.log_path}.{stamp}.{suffix}"
                        suffix += 1
                    os.replace(self.log_path, rotated_path)
            except FileNotFoundError:
                pass
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line)

    def get_daily_bars(self, code: str, frequency: str, adjustflag: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                """SELECT code, bar_date, frequency, adjustflag, open, high, low,
                   close, preclose, volume, amount, pct_chg, turn,
                   tradestatus, isST, peTTM, pbMRQ, psTTM, pcfNcfTTM
                   FROM daily_bars
                   WHERE code=? AND frequency=? AND adjustflag=?
                     AND bar_date BETWEEN ? AND ? ORDER BY bar_date""",
                (code, frequency, adjustflag, start_date, end_date),
            ).fetchall()
        keys = ("code", "date", "frequency", "adjustflag", "open", "high", "low",
                "close", "preclose", "volume", "amount", "pctChg", "turn",
                "tradestatus", "isST", "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM")
        return [dict(zip(keys, row)) for row in rows]

    def latest_daily_bar(self, code: str, frequency: str, adjustflag: str) -> str | None:
        with self._session() as db:
            row = db.execute(
                "SELECT MAX(bar_date) FROM daily_bars WHERE code=? AND frequency=? AND adjustflag=?",
                (code, frequency, adjustflag),
            ).fetchone()
        return row[0] if row and row[0] else None

    def put_daily_bars(self, code: str, frequency: str, adjustflag: str,
                       rows: list[dict[str, Any]], merge: bool = True):
        """写入日线行。A-02：字段级合并，窄字段请求不覆盖旧值。

        - merge=True（默认）：按 (code, bar_date, frequency, adjustflag) 读取旧行，
          新行中出现的字段覆盖旧值，缺失字段保留旧值；避免"窄字段请求清空完整记录"。
        - 上游明确返回的合法空值（空串/None）按接口语义保留为 NULL，不视为缺失。
        """
        now = self.clock.now_utc().isoformat()
        # 预读旧行（仅当需要合并时）：只读本次写入涉及的日期窗口，避免全历史扫描。
        old_by_date: dict[str, dict[str, Any]] = {}
        if merge:
            dates = sorted({r["date"] for r in rows if r.get("date")})
            if dates:
                for r in self.get_daily_bars(code, frequency, adjustflag, dates[0], dates[-1]):
                    old_by_date[r["date"]] = r
        values = []
        for row in rows:
            d = row.get("date")
            if not d:
                continue
            merged = dict(old_by_date.get(d, {})) if merge else {}
            # 仅覆盖新行中明确出现的字段；缺失字段（窄请求未包含）保留旧值。
            for key, col in DAILY_FIELD_MAP.items():
                if key in row:
                    merged[key] = row[key]
            values.append((
                code, d, frequency, adjustflag,
                self._number(merged.get("open")), self._number(merged.get("high")),
                self._number(merged.get("low")), self._number(merged.get("close")),
                self._number(merged.get("preclose")), self._number(merged.get("volume")),
                self._number(merged.get("amount")), self._number(merged.get("pctChg")),
                self._number(merged.get("turn")), merged.get("tradestatus"),
                merged.get("isST"), self._number(merged.get("peTTM")),
                self._number(merged.get("pbMRQ")), self._number(merged.get("psTTM")),
                self._number(merged.get("pcfNcfTTM")),
                json.dumps(merged, ensure_ascii=False), now,
            ))
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO daily_bars
                (code, bar_date, frequency, adjustflag, open, high, low, close,
                 preclose, volume, amount, pct_chg, turn, tradestatus, isST,
                 peTTM, pbMRQ, psTTM, pcfNcfTTM, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", values)

    @staticmethod
    def _number(value):
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # ---------- A-02: 日线覆盖检查（头部/中间/尾部缺口 + 状态区分） ----------

    def get_daily_coverage(self, codes: list[str], start_date: str, end_date: str,
                           adjustflag: str, frequency: str = "d",
                           trade_dates: list[str] | None = None) -> dict[str, Any]:
        """检查日线在指定区间内的覆盖情况，不再以 MAX(bar_date) 代表整个区间完整。

        - 对每个证券，用交易日历（trade_dates，可显式传入）确定应覆盖的日期集。
        - 将缺口按连续日期合并为请求窗口（头部/中间/尾部），不生成全市场笛卡尔积任务。
        - 区分：expected（预期应覆盖）/ effective（有有效记录）/ excluded（合法排除，
          如未上市/已退市/停牌）/ unknown（历史状态证据不足，不推断为完整）。
        - 行情字段与估值字段分别统计有效数。
        返回 dict，包含 per-code 覆盖、缺口窗口、各状态计数及总体状态。
        """
        result: dict[str, Any] = {
            "scope": {"codes": len(codes), "start_date": start_date, "end_date": end_date,
                      "frequency": frequency, "adjustflag": adjustflag},
            "per_code": {},
            "expected": 0, "effective": 0, "excluded": 0, "unknown": 0,
            "market_effective": 0, "valuation_effective": 0,
            "status": "complete",
        }
        if not codes:
            return result

        with self._session() as db:
            # 每个 code 在区间内的实际行
            actual: dict[str, dict[str, dict[str, Any]]] = {}
            rows = db.execute(
                """SELECT code, bar_date, open, high, low, close, volume, amount,
                          preclose, peTTM, pbMRQ, psTTM, pcfNcfTTM, tradestatus
                   FROM daily_bars
                   WHERE frequency=? AND adjustflag=?
                     AND bar_date BETWEEN ? AND ?""",
                (frequency, adjustflag, start_date, end_date)).fetchall()
            for r in rows:
                actual.setdefault(r[0], {})[r[1]] = {
                    "market": any(self._number(x) is not None for x in
                                  (r[2], r[3], r[4], r[5], r[6], r[7], r[8])),
                    "valuation": any(self._number(x) is not None for x in (r[9], r[10], r[11], r[12])),
                    "tradestatus": r[13],
                }
            # 证券主数据：用于判断合法排除（未上市/已退市）与未知
            sec_rows = {r[0]: r for r in db.execute(
                "SELECT code, status, ipo_date, out_date FROM securities").fetchall()}

        # 应覆盖日期集：优先显式交易日历，否则退化为请求区间逐日（仅作兜底）
        expected_dates = trade_dates or [
            d for d in _date_range(start_date, end_date)
        ]

        for code in codes:
            sec = sec_rows.get(code)
            dates = set(actual.get(code, {}))
            missing = [d for d in expected_dates if d not in dates]
            gaps = _merge_date_windows(missing) if missing else []

            # 状态判定：合法排除优先；无证券记录/无法判断历史状态 → unknown
            if sec is not None:
                status_field = sec[1] or ""
                ipo, out = sec[2] or "", sec[3] or ""
                if out and out < start_date:
                    state, reason = "excluded", f"delisted before {start_date}"
                elif ipo and ipo > end_date:
                    state, reason = "excluded", f"not listed until {ipo}"
                elif status_field == "D":
                    state, reason = "excluded", "delisted (status D)"
                elif dates:
                    state, reason = "effective", ""
                elif gaps:
                    state, reason = "unknown", "expected but no record; listing history insufficient"
                else:
                    state, reason = "effective", "no missing within scope"
            else:
                state, reason = "unknown", "no securities master record"

            if state == "effective":
                result["effective"] += 1
                if dates:
                    market_ok = any(actual[code][d]["market"] for d in dates)
                    valuation_ok = any(actual[code][d]["valuation"] for d in dates)
                    if market_ok:
                        result["market_effective"] += 1
                    if valuation_ok:
                        result["valuation_effective"] += 1
            elif state == "excluded":
                result["excluded"] += 1
            elif state == "unknown":
                result["unknown"] += 1

            result["per_code"][code] = {
                "state": state,
                "reason": reason,
                "bars_in_range": len(dates),
                "missing_count": len(missing),
                "gap_windows": gaps,  # 已合并的连续缺口窗口
            }

        result["expected"] = len(codes) - result["excluded"] - result["unknown"]
        if result["unknown"] > 0:
            result["status"] = "unknown"  # 历史状态证据不足，不推断为完整
        elif result["expected"] > 0 and result["effective"] >= result["expected"] and \
                all(not c["gap_windows"] for c in result["per_code"].values()
                    if c["state"] == "effective"):
            result["status"] = "complete"
        else:
            result["status"] = "partial"
        return result

    # ---------- generic helpers ----------

    def list_tables(self) -> list[str]:
        with self._session() as db:
            rows = db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def count_rows(self, table: str) -> int:
        with self._session() as db:
            row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return row[0] if row else 0

    def set_meta(self, dataset: str, detail: str | None = None):
        now = self.clock.now_utc().isoformat()
        with self._session() as db:
            db.execute("""INSERT INTO dataset_meta(dataset, last_updated, detail)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset) DO UPDATE SET last_updated=excluded.last_updated, detail=excluded.detail""",
                (dataset, now, detail or ""))

    def get_meta(self, dataset: str) -> dict[str, str] | None:
        with self._session() as db:
            row = db.execute("SELECT dataset, last_updated, detail FROM dataset_meta WHERE dataset=?", (dataset,)).fetchone()
        return {"dataset": row[0], "last_updated": row[1], "detail": row[2]} if row else None

    # ---------- download budget (fetcher only; MCP keeps using usage table) ----------
    # A-01: 预算计数实现已上移至类首部（按 Asia/Shanghai 业务日，含保守旧键读取）。

    # ---------- download jobs (A-03: 可重查任务状态机) ----------

    def job_upsert(self, dataset: str, batch_id: str, status: str, error: str | None = None,
                   window_start: str | None = None, window_end: str | None = None,
                   next_retry_at: str | None = None, lease_expires_at: str | None = None,
                   rows_written: int | None = None, error_class: str | None = None):
        """写入/更新任务状态。attempts 在每次更新时 +1（用于退避重试计数）。

        A-03：记录请求窗口、下次重试时间、租约到期、行数与错误分类，供状态机调度。
        """
        if status not in JOB_STATES:
            raise ValueError(f"invalid job status: {status}")
        now = self.clock.now_utc().isoformat()
        with self._session() as db:
            db.execute("""INSERT INTO download_jobs
                (dataset, batch_id, status, attempts, error, updated_at,
                 window_start, window_end, next_retry_at, lease_expires_at,
                 rows_written, error_class)
                VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset, batch_id) DO UPDATE SET
                    status=excluded.status,
                    attempts=download_jobs.attempts+1,
                    error=excluded.error,
                    updated_at=excluded.updated_at,
                    window_start=excluded.window_start,
                    window_end=excluded.window_end,
                    next_retry_at=excluded.next_retry_at,
                    lease_expires_at=excluded.lease_expires_at,
                    rows_written=excluded.rows_written,
                    error_class=excluded.error_class""",
                (dataset, batch_id, status, error, now,
                 window_start, window_end, next_retry_at, lease_expires_at,
                 rows_written, error_class))

    def job_status(self, dataset: str, batch_id: str) -> str | None:
        with self._session() as db:
            row = db.execute("SELECT status FROM download_jobs WHERE dataset=? AND batch_id=?", (dataset, batch_id)).fetchone()
        if not row:
            return None
        # 读时归一化：兼容迁移前遗留的旧 'done' 状态（A-03）。
        return JOB_SUCCEEDED if row[0] == "done" else row[0]

    def job_stats(self, dataset: str | None = None) -> dict[str, int]:
        with self._session() as db:
            if dataset:
                rows = db.execute("SELECT status, COUNT(*) FROM download_jobs WHERE dataset=? GROUP BY status", (dataset,)).fetchall()
            else:
                rows = db.execute("SELECT status, COUNT(*) FROM download_jobs GROUP BY status").fetchall()
        stats = {state: 0 for state in JOB_STATES}
        stats["total"] = 0
        for status, count in rows:
            stats[status] = count
            stats["total"] += count
        # 向后兼容：旧客户端依赖 'done' 键（= succeeded 数量）。
        stats["done"] = stats[JOB_SUCCEEDED]
        return stats

    # ---------- A-03: 状态机辅助（租约 / 到期 / 重启回收） ----------

    def job_claim(self, dataset: str, batch_id: str, lease_seconds: int) -> bool:
        """将 pending/waiting_data/retryable_failed 且已到期任务置为 running（领租约）。

        返回是否成功领取；running/succeeded/permanent_failed 或未到期不领取。
        """
        now_iso = self.clock.now_utc().isoformat()
        lease_expires = (self.clock.now_utc().timestamp() + lease_seconds)
        from datetime import datetime
        lease_iso = datetime.fromtimestamp(lease_expires, tz=timezone.utc).isoformat()
        with self._session() as db:
            row = db.execute(
                """SELECT 1 FROM download_jobs WHERE dataset=? AND batch_id=?
                   AND status IN (?,?,?)
                   AND (next_retry_at IS NULL OR next_retry_at <= ?)""",
                (dataset, batch_id, JOB_PENDING, JOB_WAITING_DATA, JOB_RETRYABLE_FAILED, now_iso)
            ).fetchone()
            if row is None:
                return False
            db.execute(
                """UPDATE download_jobs SET status=?, lease_expires_at=?, updated_at=?
                   WHERE dataset=? AND batch_id=?""",
                (JOB_RUNNING, lease_iso, now_iso, dataset, batch_id))
            return True

    def job_release(self, dataset: str, batch_id: str, status: str,
                    next_retry_at: str | None = None, rows_written: int | None = None,
                    error: str | None = None, error_class: str | None = None):
        """释放租约并置为终态/等待态。succeeded 清除租约；waiting_data/retryable_failed 记录重试时间。"""
        if status not in JOB_STATES:
            raise ValueError(f"invalid job status: {status}")
        now = self.clock.now_utc().isoformat()
        with self._session() as db:
            db.execute(
                """UPDATE download_jobs SET status=?, lease_expires_at=NULL,
                   next_retry_at=?, rows_written=COALESCE(?, rows_written),
                   error=?, error_class=?, updated_at=?
                   WHERE dataset=? AND batch_id=?""",
                (status, next_retry_at, rows_written, error, error_class, now, dataset, batch_id))

    def job_recover_stale_running(self) -> int:
        """A-03: 重启回收——回收租约过期的 running 任务为 retryable_failed。

        进程重启后旧租约（lease_expires_at < now 或为 NULL）无法续期，
        视为僵死任务，转 retryable_failed 以便后续按退避重试；返回回收数量。
        """
        now_iso = self.clock.now_utc().isoformat()
        with self._session() as db:
            cur = db.execute(
                """UPDATE download_jobs SET status=?, lease_expires_at=NULL,
                   error='recovered stale running task after restart',
                   error_class='restart_recovery', updated_at=?
                   WHERE status=? AND (lease_expires_at IS NULL OR lease_expires_at < ?)""",
                (JOB_RETRYABLE_FAILED, now_iso, JOB_RUNNING, now_iso))
            return cur.rowcount

    def job_due(self, dataset: str, limit: int = 100) -> list[tuple[str, str]]:
        """返回 dataset 下已到期可领取的任务 (batch_id, next_retry_at)。"""
        now_iso = self.clock.now_utc().isoformat()
        with self._session() as db:
            rows = db.execute(
                """SELECT batch_id, next_retry_at FROM download_jobs
                   WHERE dataset=? AND status IN (?,?,?)
                     AND (next_retry_at IS NULL OR next_retry_at <= ?)
                   ORDER BY COALESCE(next_retry_at, '0000-01-01') LIMIT ?""",
                (dataset, JOB_PENDING, JOB_WAITING_DATA, JOB_RETRYABLE_FAILED, now_iso, limit)).fetchall()
        return [(r[0], r[1]) for r in rows]

    def job_attempts(self, dataset: str, batch_id: str) -> int:
        """任务尝试次数（退避计算用）。"""
        with self._session() as db:
            row = db.execute(
                "SELECT attempts FROM download_jobs WHERE dataset=? AND batch_id=?",
                (dataset, batch_id)).fetchone()
        return row[0] if row else 0

    def _job_row(self, dataset: str, batch_id: str) -> dict[str, Any] | None:
        """任务完整行（状态机调度只读）。"""
        with self._session() as db:
            row = db.execute(
                """SELECT dataset, batch_id, status, attempts, error, updated_at,
                          window_start, window_end, next_retry_at, lease_expires_at,
                          rows_written, error_class
                   FROM download_jobs WHERE dataset=? AND batch_id=?""",
                (dataset, batch_id)).fetchone()
        if not row:
            return None
        keys = ("dataset", "batch_id", "status", "attempts", "error", "updated_at",
                "window_start", "window_end", "next_retry_at", "lease_expires_at",
                "rows_written", "error_class")
        return dict(zip(keys, row))

    # 事务内版本：供 fetcher 在同一个事务里提交事实数据与作业状态（A-03 同事务提交）。

    @staticmethod
    def _job_upsert_in_txn(db: sqlite3.Connection, dataset: str, batch_id: str, status: str,
                           error: str | None = None, next_retry_at: str | None = None,
                           rows_written: int | None = None, error_class: str | None = None):
        if status not in JOB_STATES:
            raise ValueError(f"invalid job status: {status}")
        # staticmethod：无 self，使用模块级 UTC 时间（事务内由调用方控制节奏）。
        now = datetime.now(timezone.utc).isoformat()
        db.execute("""INSERT INTO download_jobs
            (dataset, batch_id, status, attempts, error, updated_at,
             next_retry_at, rows_written, error_class)
            VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset, batch_id) DO UPDATE SET
                status=excluded.status,
                attempts=download_jobs.attempts+1,
                error=excluded.error,
                updated_at=excluded.updated_at,
                next_retry_at=excluded.next_retry_at,
                rows_written=excluded.rows_written,
                error_class=excluded.error_class""",
            (dataset, batch_id, status, error, now, next_retry_at, rows_written, error_class))

    @staticmethod
    def _put_financials_in_txn(db: sqlite3.Connection, dataset: str, code: str,
                               year: int, quarter: int, payload: dict[str, Any],
                               fetched_at: str | None = None):
        """事务内写财务数据（含修订归档），与 job 状态同事务提交。"""
        now = fetched_at or datetime.now(timezone.utc).isoformat()
        old = db.execute(
            "SELECT payload, fetched_at FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
            (dataset, code, year, quarter)).fetchone()
        if old is not None:
            old_payload = old[0]
            if old_payload != json.dumps(payload, ensure_ascii=False):
                rev = db.execute(
                    "SELECT COALESCE(MAX(revision), 0) FROM financial_history WHERE dataset=? AND code=? AND year=? AND quarter=?",
                    (dataset, code, year, quarter)).fetchone()[0]
                db.execute("""INSERT INTO financial_history
                    (dataset, code, year, quarter, revision, payload, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (dataset, code, year, quarter, rev + 1, old_payload, old[1]))
        db.execute("""INSERT OR REPLACE INTO financials(dataset, code, year, quarter, payload, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)""", (dataset, code, year, quarter, json.dumps(payload, ensure_ascii=False), now))

    # ---------- securities ----------

    def put_securities(self, rows: list[dict[str, Any]]):
        now = self.clock.now_utc().isoformat()
        values = [(
            r.get("code"), r.get("name", ""), r.get("trade_status", ""),
            r.get("ipo_date", ""), r.get("out_date", ""), r.get("type", ""),
            r.get("status", ""), now,
        ) for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO securities
                (code, name, trade_status, ipo_date, out_date, type, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", values)

    def get_securities(self) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                """SELECT code, name, trade_status, ipo_date, out_date, type, status
                   FROM securities ORDER BY code""").fetchall()
        keys = ("code", "name", "trade_status", "ipo_date", "out_date", "type", "status")
        return [dict(zip(keys, row)) for row in rows]

    def get_securities_codes(self) -> list[str]:
        with self._session() as db:
            rows = db.execute("SELECT code FROM securities WHERE status IN ('', '1') ORDER BY code").fetchall()
        return [r[0] for r in rows]

    def count_securities(self) -> int:
        return self.count_rows("securities")

    # ---------- trade calendar ----------

    def put_trade_calendar(self, rows: list[dict[str, Any]]):
        values = [(r.get("calendar_date"), int(r.get("is_trading_day", 0))) for r in rows if r.get("calendar_date")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO trade_calendar(calendar_date, is_trading_day)
                VALUES (?, ?)""", values)

    def latest_trade_date(self, on_or_before: str) -> str | None:
        with self._session() as db:
            row = db.execute(
                "SELECT MAX(calendar_date) FROM trade_calendar WHERE is_trading_day=1 AND calendar_date <= ?",
                (on_or_before,)).fetchone()
        return row[0] if row and row[0] else None

    def calendar_max_date(self) -> str | None:
        """A-01: 交易日历覆盖的最大日期（无论是否交易日）。"""
        with self._session() as db:
            row = db.execute("SELECT MAX(calendar_date) FROM trade_calendar").fetchone()
        return row[0] if row and row[0] else None

    def is_trading_day(self, day: str) -> bool:
        """A-01: 指定日期是否为已记录的交易日。"""
        with self._session() as db:
            row = db.execute(
                "SELECT is_trading_day FROM trade_calendar WHERE calendar_date=?", (day,)
            ).fetchone()
        return bool(row and row[0])

    # ---------- index constituents ----------

    def put_index_constituents(self, index_code: str, asof_date: str, rows: list[dict[str, Any]]):
        now = self.clock.now_utc().isoformat()
        values = [(index_code, asof_date, r.get("code"), r.get("name", ""), now) for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO index_constituents
                (index_code, asof_date, code, name, fetched_at) VALUES (?, ?, ?, ?, ?)""", values)

    def get_index_codes(self, index_code: str, asof_date: str | None = None) -> list[str]:
        with self._session() as db:
            if asof_date:
                rows = db.execute(
                    "SELECT code FROM index_constituents WHERE index_code=? AND asof_date=?",
                    (index_code, asof_date)).fetchall()
            else:
                rows = db.execute(
                    "SELECT code FROM index_constituents WHERE index_code=? ORDER BY asof_date DESC LIMIT 500",
                    (index_code,)).fetchall()
        return [r[0] for r in rows]

    # ---------- financials (A-03: 修订版本留存 + 修订检查) ----------

    def put_financials(self, dataset: str, code: str, year: int, quarter: int, payload: dict[str, Any]):
        """写入财务数据；若与已有版本不同，先归档旧版本到 financial_history。"""
        now = self.clock.now_utc().isoformat()
        with self._session() as db:
            old = db.execute(
                "SELECT payload, fetched_at FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
                (dataset, code, year, quarter)).fetchone()
            if old is not None:
                old_payload = old[0]
                if old_payload != json.dumps(payload, ensure_ascii=False):
                    rev = db.execute(
                        "SELECT COALESCE(MAX(revision), 0) FROM financial_history WHERE dataset=? AND code=? AND year=? AND quarter=?",
                        (dataset, code, year, quarter)).fetchone()[0]
                    db.execute("""INSERT INTO financial_history
                        (dataset, code, year, quarter, revision, payload, fetched_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (dataset, code, year, quarter, rev + 1, old_payload, old[1]))
            db.execute("""INSERT OR REPLACE INTO financials(dataset, code, year, quarter, payload, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)""", (dataset, code, year, quarter, json.dumps(payload, ensure_ascii=False), now))

    def financial_exists(self, dataset: str, code: str, year: int, quarter: int) -> bool:
        with self._session() as db:
            row = db.execute("SELECT 1 FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
                (dataset, code, year, quarter)).fetchone()
        return row is not None

    def financial_fetched_at(self, dataset: str, code: str, year: int, quarter: int) -> str | None:
        """财务数据最近采集时间（修订检查用）。"""
        with self._session() as db:
            row = db.execute(
                "SELECT fetched_at FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
                (dataset, code, year, quarter)).fetchone()
        return row[0] if row else None

    def financial_history(self, dataset: str, code: str, year: int, quarter: int) -> list[dict[str, Any]]:
        """财务修订历史（含当前版本，降序）。"""
        with self._session() as db:
            cur = db.execute(
                "SELECT payload, fetched_at FROM financials WHERE dataset=? AND code=? AND year=? AND quarter=?",
                (dataset, code, year, quarter)).fetchone()
            hist = db.execute(
                "SELECT revision, payload, fetched_at FROM financial_history WHERE dataset=? AND code=? AND year=? AND quarter=? ORDER BY revision DESC",
                (dataset, code, year, quarter)).fetchall()
        out = []
        if cur:
            out.append({"revision": len(hist) + 1, "data": json.loads(cur[0]), "fetched_at": cur[1]})
        for rev, payload, fetched_at in hist:
            out.append({"revision": rev, "data": json.loads(payload), "fetched_at": fetched_at})
        return out

    def get_financials(self, dataset: str, code: str) -> list[dict[str, Any]]:
        with self._session() as db:
            rows = db.execute(
                "SELECT year, quarter, payload FROM financials WHERE dataset=? AND code=? ORDER BY year DESC, quarter DESC",
                (dataset, code)).fetchall()
        return [{"year": r[0], "quarter": r[1], "data": json.loads(r[2])} for r in rows]

    def latest_financial_period(self, dataset: str) -> tuple[int, int] | None:
        with self._session() as db:
            row = db.execute(
                "SELECT year, quarter FROM financials WHERE dataset=? ORDER BY year DESC, quarter DESC LIMIT 1",
                (dataset,)).fetchone()
        return (row[0], row[1]) if row else None

    # ---------- dividends / adjust factors / industry ----------

    def put_dividend(self, code: str, year: int, year_type: str, payload: Any):
        now = self.clock.now_utc().isoformat()
        with self._session() as db:
            db.execute("""INSERT OR REPLACE INTO dividends(code, year, year_type, payload, fetched_at)
                VALUES (?, ?, ?, ?, ?)""", (code, year, year_type, json.dumps(payload, ensure_ascii=False), now))

    def dividend_exists(self, code: str, year: int, year_type: str) -> bool:
        with self._session() as db:
            row = db.execute("SELECT 1 FROM dividends WHERE code=? AND year=? AND year_type=?",
                (code, year, year_type)).fetchone()
        return row is not None

    def put_adjust_factors(self, code: str, rows: list[dict[str, Any]]):
        now = self.clock.now_utc().isoformat()
        values = [(code, r.get("date", ""), json.dumps(r, ensure_ascii=False), now) for r in rows if r.get("date")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO adjust_factors(code, factor_date, payload, fetched_at)
                VALUES (?, ?, ?, ?)""", values)

    def put_stock_industry(self, rows: list[dict[str, Any]]):
        now = self.clock.now_utc().isoformat()
        values = [(r.get("code"), r.get("industry", ""), r.get("classification", ""), r.get("update_date", ""), now)
                  for r in rows if r.get("code")]
        if not values:
            return
        with self._session() as db:
            db.executemany("""INSERT OR REPLACE INTO stock_industry
                (code, industry, classification, update_date, fetched_at) VALUES (?, ?, ?, ?, ?)""", values)

    # ---------- daily backfill pending selection (断点续传 + 当日去重) ----------

    def get_daily_pending_codes(self, limit: int, end_date: str, primary_adjustflag: str) -> list[str]:
        today = self._business_day()
        with self._session() as db:
            rows = db.execute("""
                SELECT s.code FROM securities s
                LEFT JOIN (SELECT code, MAX(bar_date) AS m FROM daily_bars
                           WHERE frequency='d' AND adjustflag=? GROUP BY code) b
                  ON s.code = b.code
                LEFT JOIN download_jobs j
                  ON j.dataset='daily_bars' AND j.batch_id = s.code || '|' || ?
                WHERE s.status IN ('', '1')
                  AND (b.m IS NULL OR b.m < ?)
                  -- SQL 三值逻辑：LEFT JOIN 无匹配时 j.* 为 NULL，必须显式放行
                  AND (j.batch_id IS NULL OR NOT (j.status='succeeded' AND substr(j.updated_at, 1, 10) = ?))
                ORDER BY s.code
                LIMIT ?""", (primary_adjustflag, primary_adjustflag, end_date, today, limit)).fetchall()
        return [r[0] for r in rows]
