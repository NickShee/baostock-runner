"""A-01: 统一时间（Asia/Shanghai 业务日 + UTC 审计）与日历推进测试。

测试原则：全部使用可注入时钟（Clock.now_fn）与临时数据库，不登录真实 BaoStock、
不访问生产数据库、不依赖真实当前日期/未来年份数据。
验收点：
- 预算计数按上海业务日划日（UTC 深夜时上海已跨天）。
- 切换期间新旧预算键并存时按较保守的已用量限制请求，不重置额度。
- 日历按覆盖终点触发补齐，不依赖 meta 月份判断。
- 日线目标交易日：盘前（早于检查时间）以最近已结束交易日为目标；
  盘后（不早于检查时间）且今天是交易日时目标为今天；检查时间可配置。
- 审计时间保存为 UTC。
"""
import datetime
import os
import tempfile
import unittest
from datetime import timezone

from baostock_runner.config import Settings
from baostock_runner.fetcher import Fetcher
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock, SHANGHAI_TZ


def _shanghai_now(iso_with_tz: str) -> datetime.datetime:
    """解析带时区 ISO 时间并转为 Shanghai 时刻的 aware datetime。"""
    dt = datetime.datetime.fromisoformat(iso_with_tz)
    return dt.astimezone(timezone.utc)


def _settings(directory, **kw):
    base = dict(
        db_path=os.path.join(directory, "data.sqlite3"),
        log_path=os.path.join(directory, "audit.jsonl"),
        daily_check_time="18:00",
        calendar_prelook_days=0,
    )
    base.update(kw)
    return Settings(**base)


class BusinessDayBudgetTest(unittest.TestCase):
    def test_budget_keyed_by_shanghai_business_day(self):
        """UTC 深夜（上海已跨天）时预算键为上海日期，而非 UTC 日期。"""
        # 2026-09-24 16:30 UTC = 2026-09-25 00:30 Asia/Shanghai
        now = _shanghai_now("2026-09-24T16:30:00+00:00")
        clock = Clock(now_fn=lambda: now)
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"), clock=clock)
            storage.increment_usage()
            storage.increment_usage()
            # 上海业务日 = 2026-09-25
            self.assertEqual(storage.usage_today(), 2)
            # UTC 日历日 = 2026-09-24，旧键不应有计数
            self.assertEqual(storage.usage_today_legacy_utc(), 0)
            # 审计时间保存 UTC
            self.assertEqual(storage.clock.now_utc().isoformat(), "2026-09-24T16:30:00+00:00")

    def test_conservative_usage_takes_max_of_old_and_new_keys(self):
        """切换期间旧 UTC 键与新上海键并存时，保守计数取较大值，不重置额度。"""
        # UTC 2026-09-24 16:30 = 上海 2026-09-25 00:30（两键不同日，切换期并存）
        now = _shanghai_now("2026-09-24T16:30:00+00:00")
        clock = Clock(now_fn=lambda: now)
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"), os.path.join(d, "audit.jsonl"), clock=clock)
            # 模拟旧版本按 UTC 日期写入旧键（2026-09-24）
            with storage._session() as db:
                db.execute("INSERT INTO usage(usage_date, request_count) VALUES (?, ?)", ("2026-09-24", 7))
                db.execute("INSERT INTO download_usage(usage_date, request_count) VALUES (?, ?)", ("2026-09-24", 5))
            # 新版本按上海业务日写入（2026-09-25）
            storage.increment_usage()
            storage.increment_usage()
            storage.increment_download_usage()
            # 上海键 = 2（usage）/ 1（download_usage）；UTC 旧键 = 7 / 5 → 保守取大
            self.assertEqual(storage.usage_today(), 2)
            self.assertEqual(storage.usage_today_conservative(), 7)
            self.assertEqual(storage.download_usage_today_conservative(), 5)


class CalendarAdvanceTest(unittest.TestCase):
    def _gateway(self, d, **kw):
        settings = _settings(d, offline=True, **kw)
        return BaoStockGateway(settings)

    def test_calendar_need_refresh_when_below_target(self):
        with tempfile.TemporaryDirectory() as d:
            gateway = self._gateway(d)
            fetcher = Fetcher(gateway)
            storage = gateway.storage
            try:
                # 初始无日历 → 需要补齐
                self.assertTrue(fetcher._need_calendar())
                # 日历只覆盖到昨天 → 仍需要补齐（终点 = 上海业务日）
                yesterday = (fetcher.clock.business_date() - datetime.timedelta(days=1)).isoformat()
                storage.put_trade_calendar([{"calendar_date": yesterday, "is_trading_day": 1}])
                self.assertTrue(fetcher._need_calendar())
                # 日历覆盖到今天 → 不需要补齐
                storage.put_trade_calendar([{"calendar_date": fetcher.business_today, "is_trading_day": 1}])
                self.assertFalse(fetcher._need_calendar())
            finally:
                gateway.close()

    def test_calendar_prelook_window(self):
        """预拉窗口配置：目标终点 = 上海业务日 + prelook 天。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d, offline=True, calendar_prelook_days=5)
            gateway = BaoStockGateway(settings)
            fetcher = Fetcher(gateway)
            try:
                expected = (fetcher.clock.business_date() + datetime.timedelta(days=5)).isoformat()
                self.assertEqual(fetcher._calendar_target_end(), expected)
            finally:
                gateway.close()


class TargetTradeDateTest(unittest.TestCase):
    def _fetcher(self, d, now_iso, **kw):
        now = _shanghai_now(now_iso)
        clock = Clock(now_fn=lambda: now)
        settings = _settings(d, offline=True, **kw)
        gateway = BaoStockGateway(settings, clock=clock)
        return Fetcher(gateway), gateway, clock

    def _seed_calendar(self, storage, dates):
        storage.put_trade_calendar([{"calendar_date": d, "is_trading_day": 1} for d in dates])

    def test_before_check_time_targets_last_completed_trade_date(self):
        """盘前（17:00 < 18:00）：目标 = 最近已结束交易日（昨天），不是今天。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway, clock = self._fetcher(d, "2026-09-24T17:00:00+08:00")
            try:
                self._seed_calendar(gateway.storage, ["2026-09-23", "2026-09-24"])
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-23")
            finally:
                gateway.close()

    def test_after_check_time_targets_today_if_trading_day(self):
        """盘后（18:30 >= 18:00）且今天是交易日：目标 = 今天。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway, clock = self._fetcher(d, "2026-09-24T18:30:00+08:00")
            try:
                self._seed_calendar(gateway.storage, ["2026-09-23", "2026-09-24"])
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-24")
            finally:
                gateway.close()

    def test_weekend_targets_previous_friday(self):
        """周六（非交易日）：目标 = 最近已结束交易日（周五），不把今天当目标。"""
        with tempfile.TemporaryDirectory() as d:
            # 2026-09-26 是周六
            fetcher, gateway, clock = self._fetcher(d, "2026-09-26T12:00:00+08:00")
            try:
                self._seed_calendar(gateway.storage, ["2026-09-24", "2026-09-25"])
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-25")
            finally:
                gateway.close()

    def test_after_check_time_weekend_targets_previous_friday(self):
        """周六即使过了检查时间，目标仍是最近已结束交易日。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway, clock = self._fetcher(d, "2026-09-26T20:00:00+08:00")
            try:
                self._seed_calendar(gateway.storage, ["2026-09-24", "2026-09-25"])
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-25")
            finally:
                gateway.close()

    def test_custom_check_time(self):
        """检查时间可配置：09:00 时，09:30 已过检查 → 目标是今天（若为交易日）。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway, clock = self._fetcher(d, "2026-09-24T09:30:00+08:00", daily_check_time="09:00")
            try:
                self._seed_calendar(gateway.storage, ["2026-09-23", "2026-09-24"])
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-24")
            finally:
                gateway.close()

    def test_missing_calendar_falls_back_to_yesterday(self):
        """日历缺失时（无记录）目标回退到昨天，不假设未来可得。"""
        with tempfile.TemporaryDirectory() as d:
            fetcher, gateway, clock = self._fetcher(d, "2026-09-24T18:30:00+08:00")
            try:
                self.assertEqual(fetcher._latest_trade_date(), "2026-09-23")
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
