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
