import os
import tempfile
import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from baostock_runner.storage import Storage, SCHEMA_VERSION
from baostock_runner.research import ResearchService
from baostock_runner.backtest import BacktestService
from baostock_runner.query import QueryService, valid_date, page


class ResearchFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = Storage(self.tmp.name + '/test.sqlite3', self.tmp.name + '/audit.jsonl')
        self.research = ResearchService(self.storage)
        self.backtest = BacktestService(self.storage, self.research)
        days = []
        d = date(2026, 8, 24)
        while d <= date(2026, 9, 29):
            if d.weekday() < 5:
                days.append(d.isoformat())
            d += timedelta(days=1)
        self.storage.put_trade_calendar([{'calendar_date': d, 'is_trading_day': 1} for d in days])
        self.days = days
        self.code = 'sh.600000'
        bars = [{'date': d, 'code': self.code, 'open': str(10 + i * .1), 'high': str(11 + i * .1),
                 'low': str(9 + i * .1), 'close': str(10 + i * .1), 'turn': '2',
                 'tradestatus': '1', 'isST': '0'} for i, d in enumerate(days)]
        self.storage.put_daily_bars(self.code, 'd', '1', bars)
        self.storage.put_financials('profit', self.code, 2026, 2, {'roeAvg': '12', 'pubDate': '2026-08-01'})
        self.storage.put_financials('cash_flow', self.code, 2026, 2, {'CFOToNP': '1.5', 'pubDate': '2026-08-01'})

    def test_schema_and_screen_to_backtest(self):
        with self.storage._session() as db:
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], SCHEMA_VERSION)
        screen = self.research.screen('2026-09-25', codes=[self.code])
        self.assertEqual(screen['quality'], 'survivorship_bias')
        self.assertEqual(screen['results'][0]['status'], 'selected')
        self.assertEqual(len(screen['inputs'][self.code]['bars']), len(self.days) - 2)
        monday = '2026-09-28'
        result = self.backtest.run([screen['run_id']], '2026-09-29',
                                   boundaries={monday: {self.code: {'up': 100, 'down': 1}}})
        self.assertEqual(result['model'], 'adjusted_price_approximation')
        self.assertEqual(result['fills'][0]['date'], monday)
        self.assertEqual(result['fills'][0]['signal_date'], '2026-09-25')
        self.assertAlmostEqual(result['fills'][0]['fee'], result['fills'][0]['gross'] * .001)
        self.assertEqual(self.backtest.get(result['run_id'])['fills'], result['fills'])
        replay = self.backtest.run([screen['run_id']], '2026-09-29',
                                   boundaries=result['boundaries'], price_snapshot=result['price_snapshot'])
        self.assertEqual(replay['fills'], result['fills'])

    def test_strict_history_rejects_fixed_universe(self):
        with self.assertRaisesRegex(ValueError, 'insufficient historical universe'):
            self.research.screen('2026-09-25', mode='point_in_time', codes=[self.code])

    def test_future_financial_revision_does_not_change_strict_screen(self):
        with self.storage._session() as db:
            db.execute("INSERT INTO index_constituents VALUES ('hs300','2026-09-25',?,'sample','2026-09-20T00:00:00+00:00')", (self.code,))
            db.execute("UPDATE financials SET fetched_at='2026-09-20T00:00:00+00:00' WHERE code=?", (self.code,))
            db.execute("UPDATE daily_bars SET updated_at='2026-09-20T00:00:00+00:00' WHERE code=?", (self.code,))
        first = self.research.screen('2026-09-25', mode='point_in_time')
        self.assertEqual(first['results'][0]['factors']['roeAvg'], 12)
        self.assertEqual(first['results'][0]['status'], 'selected')
        self.storage.put_financials('profit', self.code, 2026, 2, {'roeAvg': '12', 'pubDate': '2026-08-01'})
        self.assertEqual(self.research.screen('2026-09-25', mode='point_in_time')['results'][0]['factors'],
                         first['results'][0]['factors'])
        self.storage.put_financials('profit', self.code, 2026, 2, {'roeAvg': '99', 'pubDate': '2026-08-01'})
        self.storage.put_daily_bars(self.code, 'd', '1', [{'date': '2026-09-25', 'close': '99'}])
        self.storage.put_index_constituents('hs300', '2026-09-25', [{'code': 'sz.000001'}])
        second = self.research.screen('2026-09-25', mode='point_in_time')
        self.assertEqual(second['results'][0]['factors'], first['results'][0]['factors'])
        self.assertEqual(len(second['results']), 1)

    def test_unknown_price_boundary_blocks_trade(self):
        screen = self.research.screen('2026-09-25', codes=[self.code])
        result = self.backtest.run([screen['run_id']], '2026-09-29')
        self.assertEqual(result['fills'], [])
        self.assertTrue(any(x['reason'] == 'price_limit_unknown' for x in result['daily'][-1]['reasons']))

    def test_weekly_rebalance_sells_overweight_holding(self):
        second = 'sz.000001'
        extra = ['2026-09-30', '2026-10-01', '2026-10-02', '2026-10-05', '2026-10-06']
        self.storage.put_trade_calendar([{'calendar_date': d, 'is_trading_day': 1} for d in extra])
        days = self.days + extra
        self.storage.put_daily_bars(self.code, 'd', '1', [
            {'date': d, 'code': self.code, 'open': 10+i*.3, 'high': 11+i*.3,
             'low': 9+i*.3, 'close': 10+i*.3, 'turn': 2, 'tradestatus': '1', 'isST': '0'}
            for i, d in enumerate(days)])
        self.storage.put_daily_bars(second, 'd', '1', [
            {'date': d, 'code': second, 'open': 10+i*.05, 'high': 11+i*.05,
             'low': 9+i*.05, 'close': 10+i*.05, 'turn': 2, 'tradestatus': '1', 'isST': '0'}
            for i, d in enumerate(days)])
        self.storage.put_financials('profit', second, 2026, 2, {'roeAvg': 12, 'pubDate': '2026-08-01'})
        self.storage.put_financials('cash_flow', second, 2026, 2, {'CFOToNP': 1.5, 'pubDate': '2026-08-01'})
        first = self.research.screen('2026-09-25', codes=[self.code, second])
        second_screen = self.research.screen('2026-10-02', codes=[self.code, second])
        self.assertEqual(sum(r['status'] == 'selected' for r in first['results']), 2)
        boundaries = {d: {c: {'up': 100, 'down': 1} for c in (self.code, second)}
                      for d in ('2026-09-28', '2026-10-05')}
        run = self.backtest.run([first['run_id'], second_screen['run_id']], '2026-10-06', boundaries=boundaries)
        self.assertTrue(any(f['date'] == '2026-10-05' and f['side'] == 'sell' for f in run['fills']))
        self.assertTrue(all(day['cash'] >= -1e-6 for day in run['daily']))

    def test_query_validation_and_pagination(self):
        with self.assertRaises(ValueError):
            valid_date('2026-02-30')
        rows, cursor = page([{'i': i} for i in range(3)], 2)
        self.assertEqual([r['i'] for r in rows], [0, 1])
        self.assertEqual(page([{'i': i} for i in range(3)], 2, cursor)[0][0]['i'], 2)
        gateway = SimpleNamespace(storage=self.storage, clock=SimpleNamespace(business_date=lambda: date(2026, 9, 26)))
        result = QueryService(gateway).daily(self.code, '2026-09-01', '2026-09-25', 'backward', limit=5)
        self.assertEqual(result['count'], 5)
        self.assertIn('coverage', result['metadata'])
        latest = QueryService(gateway).latest([self.code], 'backward')
        self.assertEqual(latest['data'][0]['data'][0]['date'], '2026-09-25')
        self.assertEqual(len(latest['data'][0]['rows']), 1)
