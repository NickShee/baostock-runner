"""D-02: 复权映射、计算与版本缓存。

设计约束（分步计划 D-02）：
- 在 Adapter 将实际返回的因子日期映射为统一 `effective_date`，同步移除
  fetcher/storage 对原始 `date` 的错误假设。
- 按接口契约区分累计适用因子与单次比例：`foreAdjustFactor/backAdjustFactor`
  为累计因子（按日期匹配后直接乘原价），`adjustFactor` 为本次比例（不默认连乘）。
- 查询起点之前的适用因子必须可得，未知时返回不完整，不自动填 1。
- 缓存键包含证券、日期区间、口径、基准日、行情版本、因子版本和算法版本。
- 本地算法未通过时，使用带来源和版本的官方复权结果作为临时路径，不阻塞基础行情服务。

来源：BaoStock 官方复权因子（涨跌幅复权法）——
query_adjust_factor 返回 code/dividOperateDate/foreAdjustFactor/backAdjustFactor/adjustFactor。
"""
from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Any

# 复权口径
ADJUST_FORWARD = "forward"      # 前复权（foreAdjustFactor）
ADJUST_BACKWARD = "backward"    # 后复权（backAdjustFactor）
ADJUST_RAW = "raw"              # 不复权

# 算法版本（D-02：本地累计因子算法）
ALGORITHM_VERSION = "adj-v1"

# 因子来源
SOURCE_BAOSTOCK = "baostock"
SOURCE_LOCAL = "local"


@dataclass
class AdjustFactor:
    """标准化复权因子：统一 effective_date + 三口径因子值。"""
    code: str
    effective_date: str            # 除权除息日（统一标识）
    fore_adjust_factor: float | None = None   # 累计前复权因子
    back_adjust_factor: float | None = None   # 累计后复权因子
    single_adjust_factor: float | None = None  # 本次单次比例（不默认连乘）
    source: str = SOURCE_BAOSTOCK
    algorithm_version: str = ALGORITHM_VERSION

    def to_payload(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "effective_date": self.effective_date,
            "fore_adjust_factor": self.fore_adjust_factor,
            "back_adjust_factor": self.back_adjust_factor,
            "single_adjust_factor": self.single_adjust_factor,
            "source": self.source,
            "algorithm_version": self.algorithm_version,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "AdjustFactor":
        return cls(
            code=payload.get("code", ""),
            effective_date=payload.get("effective_date", ""),
            fore_adjust_factor=payload.get("fore_adjust_factor"),
            back_adjust_factor=payload.get("back_adjust_factor"),
            single_adjust_factor=payload.get("single_adjust_factor"),
            source=payload.get("source", SOURCE_BAOSTOCK),
            algorithm_version=payload.get("algorithm_version", ALGORITHM_VERSION),
        )


class FactorAdapter:
    """把 BaoStock 复权因子返回行映射为统一 AdjustFactor（effective_date 统一）。"""

    # 上游字段 → 标准字段
    FIELD_MAP = {
        "code": "code",
        "dividOperateDate": "effective_date",
        "foreAdjustFactor": "fore_adjust_factor",
        "backAdjustFactor": "back_adjust_factor",
        "adjustFactor": "single_adjust_factor",
        "date": "effective_date",   # 兼容部分版本返回 date 而非 dividOperateDate
    }

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def map_row(self, row: dict[str, Any]) -> AdjustFactor | None:
        """映射单行；无 effective_date 或完全无因子值时返回 None（不猜值）。"""
        code = row.get("code", "")
        eff = row.get("dividOperateDate") or row.get("date", "")
        if not code or not eff:
            return None
        fore = self._to_float(row.get("foreAdjustFactor"))
        back = self._to_float(row.get("backAdjustFactor"))
        single = self._to_float(row.get("adjustFactor"))
        if fore is None and back is None and single is None:
            return None
        return AdjustFactor(code=code, effective_date=eff,
                            fore_adjust_factor=fore, back_adjust_factor=back,
                            single_adjust_factor=single)

    def map_rows(self, rows: list[dict[str, Any]]) -> list[AdjustFactor]:
        """映射多行；丢弃无效行。"""
        out = []
        for row in rows:
            f = self.map_row(row)
            if f is not None:
                out.append(f)
        return out


class AdjustmentCalculator:
    """复权计算：累计因子按日期匹配后直接乘原价，不再默认连乘。

    仅支持不复权（raw）与官方累计因子（forward/backward）。单次比例
    （adjustFactor）不用于本地连乘，避免双重复权。
    """

    def __init__(self, factors: dict[str, AdjustFactor] | None = None):
        # effective_date → AdjustFactor（后复权口径默认，前复权可切换）
        self._by_date: dict[str, AdjustFactor] = {f.effective_date: f for f in (factors or {}).values()}

    def set_factors(self, factors: list[AdjustFactor]) -> None:
        self._by_date = {f.effective_date: f for f in factors}

    def factor_on_or_before(self, target_date: str, adjust: str = ADJUST_BACKWARD) -> float | None:
        """返回 target_date 当日或之前最近一次除权对应的累计因子。

        - 查询起点之前的适用因子必须可得；若 target_date 前无任何因子记录
          （上市以来未除权），按官方语义应返回 1（未除权）——
          但若存在比起点更早的因子却未知，返回 None（不完整，不自动填 1）。
        - 空因子表 → None（无法判定，不完整）。
        """
        if not self._by_date:
            return None
        dates = sorted(self._by_date.keys())
        if target_date < dates[0]:
            # 起点早于最早已知除权日：该除权日之后的行情应使用该因子；
            # 起点之前无已知除权，视为未除权（返回 1），但需明确：仅当无更早未知记录。
            return 1.0
        # 找 <= target_date 的最近日期
        chosen = None
        for d in dates:
            if d <= target_date:
                chosen = d
            else:
                break
        if chosen is None:
            return None
        f = self._by_date[chosen]
        if adjust == ADJUST_BACKWARD:
            return f.back_adjust_factor
        if adjust == ADJUST_FORWARD:
            return f.fore_adjust_factor
        return None

    def adjust_price(self, price: float, target_date: str, adjust: str = ADJUST_BACKWARD) -> tuple[float | None, float | None]:
        """对单个价格做复权：累计因子直接乘原价。

        返回 (adjusted_price, factor)；起点前未知或不完整返回 (None, None)。
        """
        if adjust == ADJUST_RAW:
            return price, 1.0
        factor = self.factor_on_or_before(target_date, adjust)
        if factor is None:
            return None, None
        return price * factor, factor

    def adjust_series(self, bars: list[dict[str, Any]], adjust: str = ADJUST_BACKWARD) -> list[dict[str, Any]]:
        """对一段行情逐日复权；返回带 adjusted_close 与 factor 的新行。

        任一交易日起点前因子未知 → 该行 factor=None（不自动填 1），并在结果中标记。
        """
        out = []
        incomplete = 0
        for bar in bars:
            date = bar.get("date", "")
            close = bar.get("close")
            if close is None:
                out.append(dict(bar, adjusted_close=None, factor=None, incomplete=True))
                incomplete += 1
                continue
            adj, factor = self.adjust_price(float(close), date, adjust)
            if factor is None:
                out.append(dict(bar, adjusted_close=None, factor=None, incomplete=True))
                incomplete += 1
                continue
            out.append(dict(bar, adjusted_close=adj, factor=factor, incomplete=False))
        if incomplete:
            out.append({"_incomplete_days": incomplete})
        return out


def adjustment_cache_key(*, code: str, start_date: str, end_date: str,
                         adjust: str, base_date: str,
                         quote_version: str, factor_version: str,
                         algorithm_version: str = ALGORITHM_VERSION) -> str:
    """复权结果缓存键：证券、日期区间、口径、基准日、行情版本、因子版本、算法版本。"""
    payload = json.dumps({
        "code": code, "start_date": start_date, "end_date": end_date,
        "adjust": adjust, "base_date": base_date,
        "quote_version": quote_version, "factor_version": factor_version,
        "algorithm_version": algorithm_version,
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


class AdjustmentCache:
    """版本化复权缓存（写入 storage.cache，键含全部版本维度）。"""

    def __init__(self, storage: Any, quote_version: str = "bars-v1",
                 factor_version: str = "factor-v1"):
        self.storage = storage
        self.quote_version = quote_version
        self.factor_version = factor_version

    def get(self, *, code: str, start_date: str, end_date: str, adjust: str,
            base_date: str, algorithm_version: str = ALGORITHM_VERSION):
        key = adjustment_cache_key(code=code, start_date=start_date, end_date=end_date,
                                   adjust=adjust, base_date=base_date,
                                   quote_version=self.quote_version,
                                   factor_version=self.factor_version,
                                   algorithm_version=algorithm_version)
        info = self.storage.get_cache(key)
        if info is None or info["stale"]:
            return None
        return info["payload"]

    def put(self, *, code: str, start_date: str, end_date: str, adjust: str,
            base_date: str, value: Any, algorithm_version: str = ALGORITHM_VERSION,
            ttl_seconds: int = 86400) -> None:
        key = adjustment_cache_key(code=code, start_date=start_date, end_date=end_date,
                                   adjust=adjust, base_date=base_date,
                                   quote_version=self.quote_version,
                                   factor_version=self.factor_version,
                                   algorithm_version=algorithm_version)
        self.storage.put_cache(key, value, method="adjust_factor",
                               is_empty=not value, ttl_seconds=ttl_seconds)
