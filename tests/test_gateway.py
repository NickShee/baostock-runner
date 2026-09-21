import os
import sys
import tempfile
import time
import types
import unittest

from baostock_runner.config import Settings
from baostock_runner.gateway import BaoStockGateway, ReconnectableFailure


class _FakeResult:
    def __init__(self, error_code="0", error_msg="", rows=None, fields=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self._rows = rows or []
        self.fields = fields or ["date", "code", "close"]
        self._idx = 0

    def next(self):
        if self._idx < len(self._rows):
            self._idx += 1
            return True
        return False

    def get_row_data(self):
        return [self._rows[self._idx - 1].get(field) for field in self.fields]


class _FakeBS:
    """Minimal stand-in for the real baostock module to test reconnect paths."""

    def __init__(self):
        self.login_count = 0
        self.logout_count = 0
        self.fail_queries = 0

    def login(self, user_id="", password=""):
        self.login_count += 1
        return _FakeResult()

    def logout(self):
        self.logout_count += 1

    def is_login(self):
        return True

    def query_stock_basic(self, code=""):
        return _FakeResult()

    def query_history_k_data_plus(self, **params):
        if self.fail_queries > 0:
            self.fail_queries -= 1
            return _FakeResult(error_code="999999", error_msg="simulated failure")
        return _FakeResult(rows=[{"date": params["start_date"], "code": params["code"], "close": "10.0"}])


class GatewayTest(unittest.TestCase):
    def test_offline_queue_and_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=10,
                offline=True,
            )
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sh.600000", "fields": "date,code,close", "start_date": "2026-09-15", "end_date": "2026-09-16", "frequency": "d", "adjustflag": "3"}
                first = gateway.call("query_history_k_data_plus", params)
                second = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(first["cache_hit"])
                self.assertTrue(second["cache_hit"])
                self.assertEqual(gateway.storage.usage_today(), 1)
            finally:
                gateway.close()

    def test_incremental_daily_bars_reuses_local_series(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=10,
                offline=True,
            )
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sz.000001", "fields": "date,code,close", "start_date": "2026-09-15", "end_date": "2026-09-16", "frequency": "d", "adjustflag": "3"}
                first = gateway.daily_bars(params)
                second = gateway.daily_bars(params)
                self.assertFalse(first["cache_hit"])
                self.assertTrue(second["cache_hit"])
                self.assertTrue(second["incremental"])
                self.assertEqual(second["columns"], ["date", "code", "close"])
                self.assertEqual(len(second["rows"]), 1)
                self.assertEqual(gateway.storage.usage_today(), 1)
            finally:
                gateway.close()

    def test_generic_methods_are_available_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=20,
                offline=True,
            )
            gateway = BaoStockGateway(settings)
            try:
                result = gateway.call("query_trade_dates", {"start_date": "2026-09-15", "end_date": "2026-09-16"})
                self.assertIn("data", result)
                result = gateway.call("query_profit_data", {"code": "sh.600000", "year": 2025, "quarter": 4})
                self.assertIn("data", result)
            finally:
                gateway.close()

    def test_credentials_must_be_a_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                user_id="demo-user",
                password="",
                offline=False,
            )
            gateway = BaoStockGateway(settings)
            try:
                with self.assertRaisesRegex(RuntimeError, "must be provided together"):
                    gateway.call("query_trade_dates", {"start_date": "2026-09-15", "end_date": "2026-09-16"})
            finally:
                gateway.close()

    def _install_fake_baostock(self, fake: _FakeBS):
        module = types.ModuleType("baostock")
        module.login = fake.login
        module.logout = fake.logout
        module.is_login = fake.is_login
        module.query_stock_basic = fake.query_stock_basic
        module.query_history_k_data_plus = fake.query_history_k_data_plus
        self._saved_bs = sys.modules.get("baostock")
        sys.modules["baostock"] = module

    def tearDown(self):
        saved = getattr(self, "_saved_bs", None)
        if saved is None:
            sys.modules.pop("baostock", None)
        else:
            sys.modules["baostock"] = saved

    def test_stale_session_reconnects_before_query(self):
        fake = _FakeBS()
        self._install_fake_baostock(fake)
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=100,
                max_retries=0,
                retry_delays=(),
                session_idle_timeout_seconds=0.001,  # 必然判定为过期
                verify_after_login=True,
                offline=False,
            )
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sh.600000", "fields": "date,code,close", "start_date": "2026-09-15", "end_date": "2026-09-16", "frequency": "d", "adjustflag": "3"}
                result = gateway.call("query_history_k_data_plus", params)
                self.assertEqual(result["data"][0]["close"], "10.0")
                # 启动登录 1 次 + 会话过期主动重连 1 次
                self.assertEqual(fake.login_count, 2)
                self.assertGreaterEqual(fake.logout_count, 1)
            finally:
                gateway.close()

    def test_query_failure_reconnects_then_succeeds(self):
        fake = _FakeBS()
        fake.fail_queries = 1  # 第一次查询失败，重连后成功
        self._install_fake_baostock(fake)
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=100,
                max_retries=0,  # 单次尝试即失败，立即进入重连路径
                retry_delays=(),
                session_idle_timeout_seconds=3600,  # 会话新鲜，仅验证失败重连
                verify_after_login=True,
                offline=False,
            )
            gateway = BaoStockGateway(settings)
            try:
                params = {"code": "sz.000001", "fields": "date,code,close", "start_date": "2026-09-15", "end_date": "2026-09-16", "frequency": "d", "adjustflag": "3"}
                result = gateway.call("query_history_k_data_plus", params)
                self.assertFalse(result["cache_hit"])
                self.assertEqual(fake.login_count, 2)  # 启动 1 + 失败重连 1
                self.assertGreaterEqual(fake.logout_count, 1)
            finally:
                gateway.close()


    # ---------- dead-loop guards & worker watchdog ----------

    def test_result_set_row_cap_breaks_dead_loop(self):
        class _InfiniteResult:
            error_code = "0"
            error_msg = ""
            fields = ["date", "code", "close"]

            def next(self):
                return True  # never ends -> simulated rs.next() dead loop

            def get_row_data(self):
                return ["2026-09-15", "sh.600000", "10.0"]

        class _InfiniteBS:
            def login(self, user_id="", password=""):
                return _FakeResult()

            def logout(self):
                pass

            def is_login(self):
                return True

            def query_stock_basic(self, code=""):
                return _FakeResult()

            def query_history_k_data_plus(self, **params):
                return _InfiniteResult()

        self._install_fake_baostock(_InfiniteBS())
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=1000,
                max_result_rows=200,
                watchdog_timeout_seconds=60,
                watchdog_enabled=False,
                offline=False,
            )
            gateway = BaoStockGateway(settings)
            try:
                with self.assertRaisesRegex(RuntimeError, "result set exceeded"):
                    gateway.call("query_history_k_data_plus", {"code": "sh.600000", "start_date": "2026-09-15", "end_date": "2026-09-16"})
                # 死在 increment_usage 之前：usage 不应被计入
                self.assertEqual(gateway.storage.usage_today(), 0)
            finally:
                gateway.close()

    def test_result_set_timeout_breaks_dead_loop(self):
        class _SlowInfiniteResult:
            error_code = "0"
            error_msg = ""
            fields = ["date", "code", "close"]

            def next(self):
                time.sleep(0.02)  # 每行消耗一点真实时间，触发遍历超时
                return True

            def get_row_data(self):
                return ["2026-09-15", "sh.600000", "10.0"]

        class _SlowBS:
            def login(self, user_id="", password=""):
                return _FakeResult()

            def logout(self):
                pass

            def is_login(self):
                return True

            def query_stock_basic(self, code=""):
                return _FakeResult()

            def query_history_k_data_plus(self, **params):
                return _SlowInfiniteResult()

        self._install_fake_baostock(_SlowBS())
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                daily_hard_limit=1000,
                max_result_rows=100000,  # 行数上限放宽，由遍历超时兜底
                watchdog_timeout_seconds=1,
                max_retries=0,
                retry_delays=(),
                reconnect_on_failure=False,
                watchdog_enabled=False,
                offline=False,
            )
            gateway = BaoStockGateway(settings)
            try:
                with self.assertRaises(ReconnectableFailure):
                    gateway.call("query_history_k_data_plus", {"code": "sh.600000", "start_date": "2026-09-15", "end_date": "2026-09-16"})
            finally:
                gateway.close()

    def test_watchdog_tick_detects_stall(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = Settings(
                db_path=os.path.join(directory, "data.sqlite3"),
                log_path=os.path.join(directory, "audit.jsonl"),
                min_interval_seconds=0,
                watchdog_timeout_seconds=30,
                watchdog_enabled=False,
                offline=True,
            )
            gateway = BaoStockGateway(settings)
            try:
                self.assertFalse(gateway._watchdog_tick())
                gateway.last_heartbeat = time.monotonic() - 999
                self.assertTrue(gateway._watchdog_tick())
            finally:
                gateway.close()


if __name__ == "__main__":
    unittest.main()
