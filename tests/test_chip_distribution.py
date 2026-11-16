from datetime import date, timedelta
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mcp.server.fastmcp import FastMCP
from starlette.testclient import TestClient

from baostock_runner.chip_distribution import estimate_chip_distribution
from baostock_runner.http_app import build_http_app
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock

CODE = "sh.600519"


def history(count=65):
    dates = [(date(2024, 1, 1) + timedelta(days=i)).isoformat() for i in range(count)]
    # Before the split, 20 yuan maps to the same 20 adjusted units as 10 yuan after.
    return [(d, "1", p, p, p, 100, p * 100, 2, 20)
            for i, d in enumerate(dates) for p in [20 if i < 60 else 10]]


class ChipDistributionTests(unittest.TestCase):
    def test_weights_and_price_basis_survive_a_split(self):
        rows = history()
        result = estimate_chip_distribution(CODE, rows[-1][0], 1, rows)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["price_basis"], "unadjusted_on_date")
        self.assertEqual(result["history"]["bars_used"], 65)
        self.assertEqual(len(result["weights"]), 100)
        self.assertAlmostEqual(sum(result["weights"]), 1)
        self.assertAlmostEqual(result["peak"], 10.005)
        self.assertTrue(all(10 <= p <= 10.01 for p in result["range70"]))

    def test_missing_and_suspended_anchor_and_insufficient_history(self):
        rows = history(10)
        end = rows[-1][0]
        self.assertEqual(estimate_chip_distribution(CODE, end, 0, rows)["status"], "non_trading_day")
        self.assertEqual(estimate_chip_distribution(CODE, end, None, rows[:-1])["status"], "no_daily_bar")
        suspended = (end, "0", *rows[-1][2:])
        self.assertEqual(estimate_chip_distribution(CODE, end, 1, rows[:-1] + [suspended])["status"], "suspended")
        missing = (*rows[-1][:-1], None)
        self.assertEqual(estimate_chip_distribution(CODE, end, 1, rows[:-1] + [missing])["status"], "incomplete_data")
        self.assertEqual(estimate_chip_distribution(CODE, end, 1, rows)["history"]["bars_used"], 10)

    def test_api_is_authenticated_and_excludes_future_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Storage(tmp + "/data.sqlite3", tmp + "/audit.jsonl")
            rows = history()
            raw = [{"date": d, "code": CODE, "low": low, "high": high, "close": close,
                    "volume": volume, "amount": amount, "turn": turn, "tradestatus": trade}
                   for d, trade, low, high, close, volume, amount, turn, _ in rows]
            store.put_daily_bars(CODE, "d", "3", raw)
            store.put_daily_bars(CODE, "d", "1", [dict(bar, close=20) for bar in raw])
            gateway = SimpleNamespace(storage=store, clock=Clock())
            mcp = FastMCP("test", streamable_http_path="/mcp", stateless_http=True)
            with patch.dict("os.environ", {"BAOSTOCK_HTTP_TOKEN": "test-token", "BAOSTOCK_HTTP_DEV_MODE": "false"}):
                app = build_http_app(mcp, gateway, SimpleNamespace(), SimpleNamespace())
            with TestClient(app, headers={"Authorization": "Bearer test-token"}) as client:
                path = f"/api/stocks/{CODE}/chip-distribution"
                selected = rows[59][0]
                self.assertEqual(client.get(path, params={"date": selected},
                                            headers={"Authorization": "Bearer invalid"}).status_code, 401)
                self.assertEqual(client.get(path, params={"date": "2024-02-30"}).status_code, 400)
                response = client.get(path, params={"date": selected})
                self.assertEqual(response.status_code, 200)
                data = response.json()
                self.assertEqual(data["history"]["end_date"], selected)
                self.assertEqual(data["history"]["bars_used"], 60)
                self.assertAlmostEqual(data["peak"], 20.005)
                closes = client.get(f"/api/stocks/{CODE}/macd-input").json()
                self.assertEqual(closes["adjustflag"], "1")
                self.assertEqual(closes["closes"][-1], [rows[-1][0], 20.0])
