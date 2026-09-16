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
    offline: bool = os.getenv("BAOSTOCK_OFFLINE", "false").lower() == "true"
    mcp_transport: str = os.getenv("BAOSTOCK_MCP_TRANSPORT", "streamable-http")
    mcp_host: str = os.getenv("BAOSTOCK_MCP_HOST", "0.0.0.0")
    mcp_port: int = _int("BAOSTOCK_MCP_PORT", 8000)
    mcp_path: str = os.getenv("BAOSTOCK_MCP_PATH", "/mcp")
    # True = stateless Streamable HTTP (no session tracking, no 404 "Session not found"
    # after idle timeout). Fixes disconnects through reverse proxies / long idle gaps.
    mcp_stateless: bool = os.getenv("BAOSTOCK_MCP_STATELESS", "true").lower() == "true"
