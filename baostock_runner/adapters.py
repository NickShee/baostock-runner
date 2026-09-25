"""C-01: 按日批量接口类型化 Adapter 与实验工具。

设计约束（分步计划 C-01）：
- 只为允许的方法建立类型化 Adapter，不增加任意远端方法执行入口。
- 实验工具使用注入的 Gateway，不得通过导入服务模块意外创建第二个连接。
- 保存固定版本、签名、原始响应、字段映射、权限、日期范围、证券差集和性能报告。
- 离线测试使用固定响应样本；真实实验只能走现有唯一 worker。
- 实验结果状态固定为 supported/partial/unsupported/unverified；
  没有真实访问时必须标记 unverified（本环境无法登录真实 BaoStock → unverified）。
"""
from dataclasses import dataclass, field
import datetime
import json
import os
import time
from typing import Any

# 目标接口签名（BaoStock 官方：query_daily_history_k_AStock(date='') → 全 A 股某日行情）
BATCH_METHOD = "query_daily_history_k_AStock"

# 批量接口返回字段（与逐股日线对齐；以实际部署版本返回为准，未真实访问 → unverified）
BATCH_EXPECTED_FIELDS = (
    "date", "code", "open", "high", "low", "close", "preclose",
    "volume", "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
)

# 实验结果状态枚举
EXPERIMENT_STATUS = ("supported", "partial", "unsupported", "unverified")


@dataclass
class BatchExperiment:
    """一次批量接口实验的记录（含固定版本、签名、原始响应与对账信息）。"""
    baostock_version: str = ""            # 固定部署版本（如 '0.9.4'；未真实访问时按包版本记录）
    method: str = BATCH_METHOD
    signature: str = "query_daily_history_k_AStock(date='')"
    date: str = ""
    status: str = "unverified"            # supported/partial/unsupported/unverified
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    fields_observed: list[str] = field(default_factory=list)
    field_mapping: dict[str, str] = field(default_factory=dict)
    permission: str = ""                  # 权限/错误码观察（真实实验）
    date_range_ok: bool | None = None
    security_diff: dict[str, Any] = field(default_factory=dict)  # 与同日证券集合差集
    performance: dict[str, Any] = field(default_factory=dict)    # 行数/耗时/内存等
    note: str = ""

    def validate(self) -> None:
        if self.status not in EXPERIMENT_STATUS:
            raise ValueError(f"invalid experiment status: {self.status}")


class DailyBatchAdapter:
    """类型化 Adapter：封装 query_daily_history_k_AStock 的调用与结果标准化。

    不暴露任意远端方法执行入口；只允许本接口。
    """

    def __init__(self, gateway: Any, baostock_version: str = ""):
        self.gateway = gateway
        self.method = BATCH_METHOD
        self.baostock_version = baostock_version or self._detect_version()

    @staticmethod
    def _detect_version() -> str:
        try:
            import baostock
            return getattr(baostock, "__version__", "") or "unknown"
        except Exception:
            return "unverified"

    def run(self, trade_date: str) -> BatchExperiment:
        """通过注入的 Gateway（唯一 worker）执行单日批量查询（离线/真实均走此路径）。

        真实实验仅允许走现有唯一 worker；本环境未登录真实 BaoStock → 状态 unverified。
        """
        exp = BatchExperiment(baostock_version=self.baostock_version, date=trade_date)
        t0 = time.monotonic()
        try:
            result = self.gateway.call(self.method, {"date": trade_date}, use_cache=False)
        except Exception as exc:
            exp.status = "unverified"
            exp.note = f"real upstream access unavailable in this environment: {type(exc).__name__}: {exc}"
            exp.performance = {"elapsed_seconds": round(time.monotonic() - t0, 3)}
            return exp
        rows = result.get("data", [])
        exp.raw_rows = rows
        exp.performance = {
            "elapsed_seconds": round(time.monotonic() - t0, 3),
            "rows": len(rows),
            "cache_hit": result.get("cache_hit", False),
        }
        if rows:
            exp.fields_observed = sorted({k for r in rows for k in r.keys()})
            exp.field_mapping = {f: f for f in BATCH_EXPECTED_FIELDS if f in exp.fields_observed}
        # 未真实访问（离线/登录失败）→ unverified；真实成功且字段完整才可判定 supported/partial。
        exp.status = "unverified"
        exp.note = "offline/isolated validation only; real upstream experiment requires EXT-02"
        return exp

    def standardize_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """把批量接口原始行标准化为统一字段名（与逐股 daily_bars 存储对齐）。

        仅映射已观察到的字段；未知字段保留原名并记录。不猜值、不改单位。
        """
        out = []
        for r in rows:
            std: dict[str, Any] = {}
            for k, v in r.items():
                key = self.field_mapping.get(k, k)
                std[key] = v
            out.append(std)
        return out


class ExperimentRecorder:
    """实验记录持久化：保存固定版本、签名、原始响应、字段映射与性能报告。"""

    def __init__(self, path: str):
        self.path = path

    def save(self, exp: BatchExperiment) -> str:
        exp.validate()
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "method": exp.method,
            "baostock_version": exp.baostock_version,
            "signature": exp.signature,
            "date": exp.date,
            "status": exp.status,
            "fields_observed": exp.fields_observed,
            "field_mapping": exp.field_mapping,
            "permission": exp.permission,
            "date_range_ok": exp.date_range_ok,
            "security_diff": exp.security_diff,
            "performance": exp.performance,
            "note": exp.note,
            "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "raw_rows": exp.raw_rows,
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return self.path

    @staticmethod
    def load(path: str) -> dict[str, Any]:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
