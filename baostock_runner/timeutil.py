"""A-01: 统一时间模型。

约定：
- 业务日（交易日历、每日预算计数）统一按 Asia/Shanghai 划日。
  容器系统时区可能是 UTC，直接用 date.today() 会导致业务日偏移最多 8 小时。
- 审计时间戳统一保存为带时区的 UTC 时间（既有的 audit 事件已如此）。
- 支持可注入时钟（Clock.now_fn），测试可用固定时间/手动推进，
  不依赖真实当前日期、未来年份数据或长时间等待。
"""
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass
class Clock:
    """可注入时钟；默认返回真实 UTC now。

    now_fn 返回带时区的 datetime（aware）。测试可传固定时间或可控推进函数。
    """
    now_fn: Optional[Callable[[], datetime]] = None

    def now(self) -> datetime:
        """当前时刻（带时区）。"""
        return self.now_fn() if self.now_fn is not None else datetime.now(timezone.utc)

    def now_utc(self) -> datetime:
        """审计时间：统一转为 UTC。"""
        return self.now().astimezone(timezone.utc)

    def now_shanghai(self) -> datetime:
        """当前上海时刻（带 Asia/Shanghai 时区）。"""
        return self.now().astimezone(SHANGHAI_TZ)

    def business_date(self) -> date:
        """业务日：Asia/Shanghai 的日历日（用于预算计数、目标交易日）。"""
        return self.now_shanghai().date()

    def utc_date(self) -> date:
        """UTC 日历日（用于读取/兼容旧预算日记录）。"""
        return self.now_utc().date()


def parse_check_time(value: str) -> time:
    """解析 'HH:MM' 配置为 time（上海时区），非法时抛 ValueError。"""
    hour, _, minute = value.partition(":")
    return time(int(hour), int(minute))


def is_before_check_time(now_shanghai: datetime, check: time) -> bool:
    """当前上海时刻是否早于当日检查时间。"""
    return now_shanghai.time() < check


def previous_trading_day(calendar_days: list[date], on_or_before: date) -> Optional[date]:
    """在已记录的交易日列表中找 <= on_or_before 的最近交易日；找不到返回 None。"""
    for d in sorted(calendar_days, reverse=True):
        if d <= on_or_before:
            return d
    return None
