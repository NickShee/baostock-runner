from dataclasses import dataclass
import os


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("BAOSTOCK_DB_PATH", "/data/baostock.sqlite3")
    log_path: str = os.getenv("BAOSTOCK_LOG_PATH", "/data/baostock-audit.jsonl")
    user_id: str = os.getenv("BAOSTOCK_USER_ID", "")
    password: str = os.getenv("BAOSTOCK_PASSWORD", "")
    min_interval_seconds: float = _float("BAOSTOCK_MIN_INTERVAL_SECONDS", 1.0)
    daily_warning_limit: int = _int("BAOSTOCK_DAILY_WARNING_LIMIT", 10_000)
    daily_hard_limit: int = _int("BAOSTOCK_DAILY_HARD_LIMIT", 20_000)
    max_retries: int = _int("BAOSTOCK_MAX_RETRIES", 2)
    retry_delays: tuple[float, ...] = tuple(
        float(x) for x in os.getenv("BAOSTOCK_RETRY_DELAYS_SECONDS", "5,30,120").split(",") if x
    )
    # 会话保活：距上一次 BaoStock 查询超过该秒数后，执行下一次查询前主动 logout+login。
    # 应对服务端空闲超时静默断开长连接（baostock 免费服务的常见行为）。
    session_idle_timeout_seconds: float = _float("BAOSTOCK_SESSION_IDLE_TIMEOUT_SECONDS", 1800)
    # 查询失败（非黑名单/非参数错误）且重试耗尽后，重连一次再整轮重试。
    reconnect_on_failure: bool = os.getenv("BAOSTOCK_RECONNECT_ON_FAILURE", "true").lower() == "true"
    # 登录后执行一次轻量查询（query_stock_basic）验证通道真实可用，避免"假登录"。
    verify_after_login: bool = os.getenv("BAOSTOCK_VERIFY_AFTER_LOGIN", "true").lower() == "true"
    offline: bool = os.getenv("BAOSTOCK_OFFLINE", "false").lower() == "true"
    mcp_transport: str = os.getenv("BAOSTOCK_MCP_TRANSPORT", "streamable-http")
    mcp_host: str = os.getenv("BAOSTOCK_MCP_HOST", "0.0.0.0")
    mcp_port: int = _int("BAOSTOCK_MCP_PORT", 8000)
    mcp_path: str = os.getenv("BAOSTOCK_MCP_PATH", "/mcp")
    # True = stateless Streamable HTTP (no session tracking, no 404 "Session not found"
    # after idle timeout). Fixes disconnects through reverse proxies / long idle gaps.
    mcp_stateless: bool = os.getenv("BAOSTOCK_MCP_STATELESS", "true").lower() == "true"
    # --- background fetcher (P1) ---
    # 后台分批次下载器开关。关闭后仅保留 MCP 按需拉取行为（原有行为不变）。
    fetch_enabled: bool = os.getenv("BAOSTOCK_FETCH_ENABLED", "true").lower() == "true"
    # 股票池：hs300 = 沪深300 成分股（P1 实现）；all = 全 A 股（预留扩展位）。
    fetch_universe: str = os.getenv("BAOSTOCK_FETCH_UNIVERSE", "hs300")
    # fetcher 占每日总预算（daily_hard_limit）的比例。默认 2/3，保证 MCP 至少保留 1/3。
    fetch_budget_ratio: float = _float("BAOSTOCK_FETCH_BUDGET_RATIO", 0.67)
    # 每批处理的股票数量（分批次下载的粒度）。
    fetch_batch_size: int = _int("BAOSTOCK_FETCH_BATCH_SIZE", 50)
    # 日线回填起点（首次全量回填的开始日期）。
    fetch_daily_start_date: str = os.getenv("BAOSTOCK_FETCH_DAILY_START_DATE", "2018-01-01")
    # 财务回填起点年份（含），倒序补齐到当前年。
    fetch_financial_start_year: int = _int("BAOSTOCK_FETCH_FINANCIAL_START_YEAR", 2022)
    # 财务回填 dataset 列表，逗号分隔：profit,growth,balance,cash_flow,operation,dupont
    fetch_financial_datasets: str = os.getenv("BAOSTOCK_FETCH_FINANCIAL_DATASETS", "profit")
    # 日线缓存复权方式，逗号分隔：3=不复权, 1=后复权, 2=前复权
    fetch_adjustflags: str = os.getenv("BAOSTOCK_FETCH_ADJUSTFLAGS", "3")
    # 是否回填分红数据
    fetch_include_dividends: bool = os.getenv("BAOSTOCK_FETCH_INCLUDE_DIVIDENDS", "false").lower() == "true"
    # 是否回填复权因子
    fetch_include_adjust_factors: bool = os.getenv("BAOSTOCK_FETCH_INCLUDE_ADJUST_FACTORS", "false").lower() == "true"
    # 全部任务追上最新后，空闲轮询间隔（秒）。
    fetch_idle_sleep_seconds: int = _int("BAOSTOCK_FETCH_IDLE_SLEEP_SECONDS", 300)
    # 预算耗尽后的暂停检查间隔（秒）；按天计数，跨天自动恢复。
    fetch_pause_sleep_seconds: int = _int("BAOSTOCK_FETCH_PAUSE_SLEEP_SECONDS", 600)
    # --- A-01: 统一时间与日历推进 ---
    # 业务日/预算计数统一按 Asia/Shanghai 划日；审计时间统一保存 UTC（见 timeutil.Clock）。
    # 日线当日检查默认从上海时间 18:00 开始（仅表示"开始尝试"），HH:MM 格式，可配置。
    daily_check_time: str = os.getenv("BAOSTOCK_DAILY_CHECK_TIME", "18:00")
    # 交易日历预拉窗口（天）：上游若支持未来日历可预拉一个窗口；0 = 不假设未来日期必得。
    calendar_prelook_days: int = _int("BAOSTOCK_CALENDAR_PRELOOK_DAYS", 0)
    # --- A-03: 可重查任务与财务修订 ---
    # 空财报（waiting_data）到期重查间隔（小时）。
    financial_waiting_retry_hours: int = _int("BAOSTOCK_FINANCIAL_WAITING_RETRY_HOURS", 24)
    # 最近两个已结束季度每日检查一次修订（天）。
    financial_revision_check_days: int = _int("BAOSTOCK_FINANCIAL_REVISION_CHECK_DAYS", 1)
    # 其余已完成报告每 30 天检查一次修订（天）。
    financial_revision_check_days_full: int = _int("BAOSTOCK_FINANCIAL_REVISION_CHECK_DAYS_FULL", 30)
    # 任务租约到期时间（秒）：running 任务超时未完成视为僵死，重启/调度回收。
    task_lease_seconds: int = _int("BAOSTOCK_TASK_LEASE_SECONDS", 600)
    # 网络失败指数退避：起始秒、最大秒。
    task_retry_base_seconds: int = _int("BAOSTOCK_TASK_RETRY_BASE_SECONDS", 60)
    task_retry_max_seconds: int = _int("BAOSTOCK_TASK_RETRY_MAX_SECONDS", 3600)
    # --- worker heartbeat / watchdog ---
    # gateway worker 心跳看门狗：worker 卡死（如 baostock rs.next() 在半开连接下死循环）
    # 且心跳停滞超过阈值时，主动终止进程，由容器 restart 策略自动拉起。
    # 正常查询（含大数据量遍历）会持续刷新心跳，不会误杀。
    watchdog_enabled: bool = os.getenv("BAOSTOCK_WATCHDOG_ENABLED", "true").lower() == "true"
    watchdog_timeout_seconds: int = _int("BAOSTOCK_WATCHDOG_TIMEOUT_SECONDS", 300)
    watchdog_check_interval_seconds: int = _int("BAOSTOCK_WATCHDOG_CHECK_INTERVAL_SECONDS", 15)
    # 单次 baostock 查询结果集行数上限：rs.next() 死循环时强制中止，防止无限累积。
    max_result_rows: int = _int("BAOSTOCK_MAX_RESULT_ROWS", 50000)
