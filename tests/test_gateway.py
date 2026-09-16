import os
import tempfile
import unittest

from baostock_runner.config import Settings
from baostock_runner.gateway import BaoStockGateway


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
                self.assertEqual(len(second["data"]), 1)
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


if __name__ == "__main__":
    unittest.main()
