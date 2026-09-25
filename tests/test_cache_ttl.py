"""A-04: 缓存 TTL 分类型、过期 stale 判定、上游故障本地读取测试。

测试原则：全部使用临时目录、可注入时钟与本地文件，不登录真实 BaoStock、
不访问生产数据库、不依赖真实当前日期。
验收点：
- 分类型 TTL：空结果/近期行情/历史行情/财务/基本资料/默认 TTL 各自不同。
- 过期缓存 stale=True：不再命中，重查后刷新；空结果不会永久命中。
- 上游故障时返回本地 stale 数据并标记远端错误；无本地数据时抛出原错误。
- 旧无分类缓存（expires_at NULL）到期失效，不清空事实表。
- 强制刷新（use_cache=False）确实触发远端。
"""
import datetime
import os
import tempfile
import unittest
from datetime import timezone

from baostock_runner.config import Settings
from baostock_runner.gateway import BaoStockGateway
from baostock_runner.storage import SCHEMA_VERSION, Storage
from baostock_runner.timeutil import Clock


def _clock(now_iso="2026-09-24T10:00:00+00:00"):
    now = datetime.datetime.fromisoformat(now_iso)
    return Clock(now_fn=lambda: now)


def _settings(directory, **kw):
    base = dict(
        db_path=os.path.join(directory, "data.sqlite3"),
        log_path=os.path.join(directory, "audit.jsonl"),
        min_interval_seconds=0,
        daily_hard_limit=1000,
        cache_ttl_empty_seconds=900,          # 15min
        cache_ttl_recent_quotes_seconds=900,  # 15min
        cache_ttl_historical_quotes_seconds=7 * 86400,
        cache_ttl_financial_seconds=86400,
        cache_ttl_basic_seconds=7 * 86400,
        cache_ttl_default_seconds=3600,
    )
    base.update(kw)
    return Settings(**base)


class CacheTtlClassificationTest(unittest.TestCase):
    def test_ttl_by_method_type(self):
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"),
                              os.path.join(d, "audit.jsonl"),
                              clock=_clock(), settings=_settings(d))
            s = storage.settings
            # 空结果最短
            self.assertEqual(storage.cache_ttl_for("query_profit_data", is_empty=True), s.cache_ttl_empty_seconds)
            # 财务
            self.assertEqual(storage.cache_ttl_for("query_profit_data"), s.cache_ttl_financial_seconds)
            self.assertEqual(storage.cache_ttl_for("query_hs300_stocks"), s.cache_ttl_financial_seconds)
            # 基本资料/行业
            self.assertEqual(storage.cache_ttl_for("query_stock_basic"), s.cache_ttl_basic_seconds)
            self.assertEqual(storage.cache_ttl_for("query_stock_industry"), s.cache_ttl_basic_seconds)
            # 历史行情 vs 近期行情
            self.assertEqual(storage.cache_ttl_for("query_history_k_data_plus", recent=False),
                             s.cache_ttl_historical_quotes_seconds)
            self.assertEqual(storage.cache_ttl_for("query_history_k_data_plus", recent=True),
                             s.cache_ttl_recent_quotes_seconds)
            # 默认
            self.assertEqual(storage.cache_ttl_for("query_all_stock"), s.cache_ttl_default_seconds)

    def test_recent_quote_detection(self):
        """日线区间终点距业务日 <= 窗口视为近期。"""
        with tempfile.TemporaryDirectory() as d:
            settings = _settings(d)
            gateway = BaoStockGateway(settings, clock=_clock("2026-09-24T10:00:00+00:00"))
            try:
                # 业务日 2026-09-24；窗口 5 天 → end=09-20 及以上为近期
                self.assertTrue(gateway._is_recent_quote(
                    "query_history_k_data_plus", {"end_date": "2026-09-20"}))
                self.assertTrue(gateway._is_recent_quote(
                    "query_history_k_data_plus", {"end_date": "2026-09-24"}))
                self.assertFalse(gateway._is_recent_quote(
                    "query_history_k_data_plus", {"end_date": "2026-09-15"}))
                self.assertFalse(gateway._is_recent_quote("query_profit_data", {"end_date": "2026-09-24"}))
            finally:
                gateway.close()


class CacheExpiryTest(unittest.TestCase):
    def test_expired_cache_is_stale_and_not_hit(self):
        """过期缓存 stale=True；call 不再命中，重查后刷新。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_clock())
            try:
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                r1 = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(r1["cache_hit"])
                usage_after_first = gateway.storage.usage_today()
                # 未过期命中
                r2 = gateway.call("query_history_k_data_plus", params)
                self.assertTrue(r2["cache_hit"])
                self.assertFalse(r2.get("stale", False))
                self.assertEqual(gateway.storage.usage_today(), usage_after_first)
                # 推进时间超过历史行情 TTL（7 天）→ stale，重查
                # 注意：推进到 2026-10-05 已跨业务日，usage_today() 查新业务日键，
                # 应等于 1（新业务日仅重查 1 次远端请求）。
                gateway.storage.clock = _clock("2026-10-05T10:00:00+00:00")
                r3 = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(r3["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), 1)
            finally:
                gateway.close()

    def test_empty_result_does_not_hit_forever(self):
        """空结果 15min TTL：过期后重查，不永久命中。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_clock())
            try:
                # 构造空结果：offline 模式下 mock _execute 返回空行并计一次远端
                original = gateway._execute

                def empty_execute(bs, method, p, use_cache=True):
                    count = gateway.storage.increment_usage()
                    payload = {"data": [], "cache_hit": False,
                               "request_count_today": count}
                    gateway.storage.put_cache(gateway._key(method, p), [], method=method, is_empty=True)
                    return payload
                gateway._execute = empty_execute
                params = {"code": "sh.600000", "fields": "date,code",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                r1 = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(r1["cache_hit"])
                usage_after = gateway.storage.usage_today()
                # 未过期 → 命中（空结果也命中，但 TTL 短）
                r2 = gateway.call("query_history_k_data_plus", params)
                self.assertTrue(r2["cache_hit"])
                # 超过 15min → stale，重查
                gateway.storage.clock = _clock("2026-09-24T10:16:00+00:00")
                r3 = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(r3["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), usage_after + 1)
                gateway._execute = original
            finally:
                gateway.close()

    def test_legacy_unclassified_cache_expires(self):
        """旧无分类缓存（expires_at NULL）视为过期，不清空事实表。"""
        with tempfile.TemporaryDirectory() as d:
            storage = Storage(os.path.join(d, "data.sqlite3"),
                              os.path.join(d, "audit.jsonl"), clock=_clock(),
                              settings=_settings(d))
            self.assertEqual(storage.integrity_check()["user_version"], SCHEMA_VERSION)
            # 模拟旧缓存行（无 method/expires_at）
            key = "legacy-key"
            with storage._session() as db:
                db.execute("INSERT INTO cache(cache_key, payload, created_at) VALUES (?, ?, ?)",
                           (key, "[]", "2026-09-01T00:00:00+00:00"))
            info = storage.get_cache(key)
            self.assertIsNotNone(info)
            self.assertTrue(info["stale"])  # 旧缓存到期失效
            # 事实表未受影响
            self.assertEqual(storage.count_rows("daily_bars"), 0)


class UpstreamFailureFallbackTest(unittest.TestCase):
    def test_upstream_failure_returns_stale_local(self):
        """上游故障时返回本地 stale 数据并标记远端错误。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_clock())
            try:
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                r1 = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(r1["cache_hit"])
                # 让缓存过期
                gateway.storage.clock = _clock("2026-10-05T10:00:00+00:00")
                # 上游故障：mock _execute 抛错
                original = gateway._execute

                def failing(bs, method, p, use_cache=True):
                    raise ConnectionError("simulated upstream failure")
                gateway._execute = failing
                r2 = gateway.call("query_history_k_data_plus", params)
                self.assertTrue(r2["stale"])
                self.assertTrue(r2["cache_hit"])
                self.assertIn("simulated upstream failure", r2["upstream_error"])
                self.assertEqual(r2["upstream_error_type"], "ConnectionError")
                # 有本地 stale 数据
                self.assertEqual(r2["data"], r1["data"])
                gateway._execute = original
            finally:
                gateway.close()

    def test_upstream_failure_without_local_raises(self):
        """无本地数据时上游故障抛出原错误。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_clock())
            try:
                original = gateway._execute

                def failing(bs, method, p, use_cache=True):
                    raise ConnectionError("no local data either")
                gateway._execute = failing
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                with self.assertRaises(ConnectionError):
                    gateway.call("query_history_k_data_plus", params)
                gateway._execute = original
            finally:
                gateway.close()

    def test_force_refresh_bypasses_cache(self):
        """强制刷新（use_cache=False）绕过缓存，确实触发远端。"""
        with tempfile.TemporaryDirectory() as d:
            gateway = BaoStockGateway(_settings(d), clock=_clock())
            try:
                params = {"code": "sh.600000", "fields": "date,code,close",
                          "start_date": "2026-09-15", "end_date": "2026-09-16",
                          "frequency": "d", "adjustflag": "3"}
                r1 = gateway.call("query_history_k_data_plus", params)
                usage_after = gateway.storage.usage_today()
                # use_cache=True 命中
                r2 = gateway.call("query_history_k_data_plus", params)
                self.assertTrue(r2["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), usage_after)
                # use_cache=False 强制远端
                r3 = gateway.call("query_history_k_data_plus", params, use_cache=False)
                self.assertFalse(r3["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), usage_after + 1)
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
