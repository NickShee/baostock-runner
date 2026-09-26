import tempfile
import time
import unittest
import logging
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from mcp.server.fastmcp import FastMCP
from starlette.testclient import TestClient

from baostock_runner.http_app import build_http_app
from baostock_runner.storage import Storage
from baostock_runner.timeutil import Clock


class HttpContractTests(unittest.TestCase):
    def test_auth_health_local_query_and_refresh_job(self):
        logging.getLogger('httpx').setLevel(logging.WARNING)
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(tmp + '/data.sqlite3', tmp + '/audit.jsonl')
            days = []
            d = date(2026, 8, 24)
            while d <= date(2026, 9, 29):
                if d.weekday() < 5:
                    days.append(d.isoformat())
                d += timedelta(days=1)
            storage.put_trade_calendar([{'calendar_date': day, 'is_trading_day': 1} for day in days])
            code = 'sh.600000'
            storage.put_daily_bars(code, 'd', '1', [
                {'date': day, 'code': code, 'open': 10 + i*.1, 'high': 11 + i*.1,
                 'low': 9 + i*.1, 'close': 10 + i*.1, 'turn': 2,
                 'tradestatus': '1', 'isST': '0'} for i, day in enumerate(days)])
            storage.put_financials('profit', code, 2026, 2, {'roeAvg': 12, 'pubDate': '2026-08-01'})
            storage.put_financials('cash_flow', code, 2026, 2, {'CFOToNP': 1.5, 'pubDate': '2026-08-01'})
            gateway = SimpleNamespace(storage=storage, clock=Clock(), jobs=SimpleNamespace(qsize=lambda: 0),
                                      breaker=SimpleNamespace(is_set=lambda: False),
                                      health_status=lambda: {'upstream': 'offline'},
                                      daily_bars=lambda params, force_refresh=False: {'data': [], 'remote_requested': False})
            fetcher = SimpleNamespace(state={'running': False}, budget_left=lambda: 100)
            mcp = FastMCP('test', streamable_http_path='/mcp', stateless_http=True)
            with patch.dict('os.environ', {'BAOSTOCK_HTTP_TOKEN': 'secret', 'BAOSTOCK_HTTP_DEV_MODE': 'false'}):
                app = build_http_app(mcp, gateway, fetcher, SimpleNamespace())
            with TestClient(app) as client:
                self.assertEqual(client.get('/healthz').status_code, 200)
                self.assertEqual(client.get('/readyz').status_code, 200)
                self.assertEqual(client.get('/api/stocks').status_code, 401)
                headers = {'Authorization': 'Bearer secret'}
                response = client.get('/api/stocks', headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['data'], [])
                response = client.post('/api/stocks/sh.600000/daily/refresh', headers=headers)
                self.assertEqual(response.status_code, 202)
                job_id = response.json()['job_id']
                self.assertEqual(client.get('/api/jobs/' + job_id, headers=headers).status_code, 200)
                self.assertEqual(client.get('/api/research/screens/missing', headers=headers).status_code, 400)
                queued = client.post('/api/research/screens', headers=headers,
                                     json={'asof_date': '2026-09-25', 'codes': [code]})
                self.assertEqual(queued.status_code, 202)
                def completed(job_id):
                    for _ in range(200):
                        value = client.get('/api/jobs/' + job_id, headers=headers).json()
                        if value['status'] in {'succeeded', 'failed'}:
                            return value
                        time.sleep(.05)
                    self.fail('job did not complete')
                screened = completed(queued.json()['job_id'])
                self.assertEqual(screened['status'], 'succeeded', screened.get('error'))
                run_id = screened['result']['run_id']
                self.assertEqual(client.get('/api/research/screens/' + run_id, headers=headers).json()['results'][0]['status'], 'selected')
                queued = client.post('/api/research/backtests', headers=headers,
                                     json={'screen_run_ids': [run_id], 'end_date': '2026-09-29',
                                           'boundaries': {'2026-09-28': {code: {'up': 100, 'down': 1}}}})
                self.assertEqual(queued.status_code, 202)
                tested = completed(queued.json()['job_id'])
                self.assertEqual(tested['status'], 'succeeded', tested.get('error'))
                self.assertEqual(tested['result']['fills'][0]['date'], '2026-09-28')
