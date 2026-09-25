"""D-01: 最小标准模型——字段映射与标准化规则集中维护。

约定：
- 保留现有证券代码（如 sh.600000）为兼容标识；asset_type/source 附加标注。
- 标准化日期、数值、金额/数量单位和空值语义。
- 字段映射集中于此，避免各处散落；上游字段未验证时记录 unsupported/unknown，
  不得凭猜测填值或改单位。
"""
from typing import Any

# 资产类型（默认 stock；指数/ETF 等后续按需扩展）
ASSET_TYPE_STOCK = "stock"
ASSET_TYPE_INDEX = "index"
ASSET_TYPE_ETF = "etf"
ASSET_TYPE_UNKNOWN = "unknown"

# 数据来源
SOURCE_BAOSTOCK = "baostock"
SOURCE_UNKNOWN = "unknown"

# 证券主数据：上游字段 → 标准字段（保留 code 兼容标识）
SECURITY_FIELD_MAP = {
    "code": "code",
    "code_name": "name",
    "ipoDate": "ipo_date",
    "outDate": "out_date",
    "type": "type",
    "status": "status",
}

# 日线字段：上游返回键 → 标准存储列（与 storage.DAILY_FIELD_MAP 对齐）
DAILY_FIELD_MAP = {
    "date": "bar_date",
    "code": "code",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "preclose": "preclose",
    "volume": "volume",
    "amount": "amount",
    "adjustflag": "adjustflag",
    "turn": "turn",
    "tradestatus": "tradestatus",
    "pctChg": "pct_chg",
    "peTTM": "peTTM",
    "pbMRQ": "pbMRQ",
    "psTTM": "psTTM",
    "pcfNcfTTM": "pcfNcfTTM",
    "isST": "isST",
}

# 财务字段：上游常用键 → 标准键（payload 内保留原始字段）
FINANCIAL_STANDARD_KEYS = {
    "pubDate": "pub_date",
    "statDate": "stat_date",
    "code": "code",
}


def standardize_security(row: dict[str, Any], source: str = SOURCE_BAOSTOCK,
                         asset_type: str = ASSET_TYPE_STOCK) -> dict[str, Any]:
    """证券主数据标准化：仅映射已存在字段，缺失字段不猜值。"""
    out: dict[str, Any] = {"source": source, "asset_type": asset_type}
    for upstream, std in SECURITY_FIELD_MAP.items():
        if upstream in row:
            out[std] = row[upstream]
    # 兼容：已有标准键直接透传
    for std in ("name", "ipo_date", "out_date", "type", "status", "trade_status"):
        if std in row and std not in out:
            out[std] = row[std]
    return out


def standardize_financial_meta(payload: dict[str, Any]) -> dict[str, str]:
    """从财务 payload 提取标准元信息（公告日期/报告期），缺失返回空串不猜值。"""
    meta: dict[str, str] = {}
    for upstream, std in FINANCIAL_STANDARD_KEYS.items():
        if upstream in payload and payload[upstream] is not None:
            meta[std] = str(payload[upstream])
    return meta


def infer_asset_type(code: str) -> str:
    """按代码前缀推断资产类型；未知前缀返回 unknown（不以交易所前缀强制判别）。"""
    if not code or "." not in code:
        return ASSET_TYPE_UNKNOWN
    prefix = code.split(".", 1)[0].lower()
    if prefix in {"sh", "sz", "bj"}:
        # 股票与指数需要更多证据，默认按 stock；ETF/指数由调用方显式指定。
        return ASSET_TYPE_STOCK
    return ASSET_TYPE_UNKNOWN
