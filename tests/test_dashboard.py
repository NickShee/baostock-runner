import os
import queue
import tempfile
import unittest

from baostock_runner.config import Settings
from baostock_runner.dashboard_api import register_dashboard_routes
from baostock_runner.dashboard_queries import DashboardQueries
from baostock_runner.storage import Storage


class DashboardQueriesTest(unittest.TestCase):
    def test_search_coverage_and_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = Storage(os.path.join(directory, "data.sqlite3"), os.path.join(directory, "audit.jsonl"))
            storage.put_securities([
                {"code": "sh.600000", "name": "浦发银行", "status": "1"},
                {"code": "sz.000001", "name": "平安银行", "status": "1"},
            ])
            storage.put_daily_bars("sh.600000", "d", "3", [
                {"date": "2026-09-17", "code": "sh.600000", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100"},
            ])
            storage.put_financials("profit", "sh.600000", 2025, 4, {"netProfit": "100"})
            storage.job_upsert("financials", "sh.600000|2026Q1|profit", "failed", error="temporary")

            queries = DashboardQueries(storage)
            self.assertEqual(queries.search_securities("浦发")[0]["code"], "sh.600000")
            coverage = queries.coverage()
            self.assertEqual(coverage["daily_bars"]["rows"], 1)
            self.assertEqual(coverage["financials"]["rows"], 1)
            self.assertEqual(queries.job_errors()[0]["error"], "temporary")


class DashboardRouteRegistrationTest(unittest.TestCase):
    def test_expected_routes_are_registered(self):
        class FakeMCP:
            def __init__(self):
                self.routes = []

            def custom_route(self, path, methods, **kwargs):
                def decorator(func):
                    self.routes.append((path, tuple(methods), func))
                    return func
                return decorator

        class FakeGateway:
            def __init__(self):
                self.storage = Storage(":memory:", os.devnull)
                self.jobs = queue.Queue()
                self.breaker = type("Breaker", (), {"is_set": lambda self: False})()

        class FakeFetcher:
            state = {"running": False}

        mcp = FakeMCP()
        gateway = FakeGateway()
        settings = Settings(db_path=":memory:", log_path=os.devnull, offline=True)
        register_dashboard_routes(mcp, gateway, FakeFetcher(), settings)
        paths = {path for path, _, _ in mcp.routes}
        for path in (
            "/dashboard", "/dashboard/", "/dashboard/assets/{path:path}",
            "/api/dashboard/status", "/api/dashboard/coverage", "/api/dashboard/errors",
            "/api/stocks", "/api/stocks/{code}/daily", "/api/stocks/{code}/financials",
            "/api/stocks/{code}/daily/refresh",
        ):
            self.assertIn(path, paths)


if __name__ == "__main__":
    unittest.main()
