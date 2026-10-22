import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import server


class OptionsPaperAccountsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name).resolve()
        self.now = datetime.fromisoformat('2026-10-21T15:00:00+00:00')
        for index, (book_key, title, signal, exit_policy, filter_policy, relative) in enumerate(server.OPTIONS_ARM_BOOKS):
            path = self.base / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            position = {
                'position_id': f'{book_key}-open', 'symbol': 'SPY', 'expiry': '2026-10-23',
                'strike': 600, 'type': 'call', 'quantity': 2, 'multiplier': 100,
                'entry_debit': 300.0, 'last_bid': 1.75,
                'last_quote_time': '2026-10-21T14:59:00+00:00',
            } if index == 0 else None
            trades = []
            if index == 0:
                trades = [
                    {'position_id': 'partial1', 'entry_time': '2026-10-21T13:00:00Z',
                     'exit_time': '2026-10-21T14:00:00Z', 'quantity': 1, 'net_pnl': 12.35},
                    {'position_id': 'overnight', 'entry_time': '2026-10-20T13:00:00Z',
                     'exit_time': '2026-10-20T14:00:00Z', 'quantity': 1, 'net_pnl': -50.0},
                    {'position_id': 'ny-boundary', 'entry_time': '2026-10-20T19:00:00Z',
                     'exit_time': '2026-10-21T03:59:59Z', 'quantity': 1, 'net_pnl': 100.0},
                ]
            data = {
                'strategy_id': f'strategy-{book_key}', 'initial_cash': 1000,
                'cash': [1155, 942, 1072.6, 1000, 1000, 1000, 1000][index],
                'realized_pnl': [155, -58, 72.6, 0, 0, 0, 0][index],
                'positions': {position['position_id']: position} if position else {},
                'trades': trades,
            }
            path.write_text(json.dumps(data))

    def tearDown(self):
        self.temp.cleanup()

    def test_all_seven_books_are_projected_including_empty_initial_books(self):
        result = server.options_paper_accounts(self.base, self.now)
        self.assertEqual(result['coverage'], {
            'expected': 7, 'observed': 7, 'complete': True,
            'sources': [{'source': row['source'], 'status': 'ok', 'error': None} for row in result['accounts']],
            'errors': [],
        })
        self.assertEqual(len(result['accounts']), 7)
        self.assertEqual(result['accounts'][6]['initial_cash_cents'], 100000)
        self.assertEqual(result['accounts'][6]['cash_cents'], 100000)
        self.assertEqual(result['accounts'][6]['today_realized_pnl_cents'], 0)
        self.assertEqual(result['accounts'][6]['open_positions'], 0)
        self.assertEqual(result['accounts'][0]['today_realized_pnl_cents'], 1235)
        self.assertEqual(result['accounts'][0]['total_realized_pnl_cents'], 15500)
        self.assertEqual(result['accounts'][0]['positions'][0]['quantity'], 2)
        self.assertEqual(result['accounts'][0]['positions'][0]['multiplier'], 100)

    def test_missing_and_malformed_books_remain_unknown(self):
        path = self.base / server.OPTIONS_ARM_BOOKS[1][-1]
        path.unlink()
        malformed = self.base / server.OPTIONS_ARM_BOOKS[2][-1]
        malformed.write_text('{broken json')
        result = server.options_paper_accounts(self.base, self.now)
        self.assertFalse(result['coverage']['complete'])
        self.assertEqual(result['coverage']['observed'], 5)
        for index in (1, 2):
            row = result['accounts'][index]
            self.assertEqual(row['source_status'], 'unavailable')
            self.assertIsNone(row['cash_cents'])
            self.assertIsNone(row['today_realized_pnl_cents'])
            self.assertIsNone(row['open_positions'])

    def test_old_open_mark_is_last_observed_and_stale(self):
        path = self.base / server.OPTIONS_ARM_BOOKS[0][-1]
        data = json.loads(path.read_text())
        position = data['positions']['original_a-open']
        position['last_quote_time'] = '2026-10-21T14:00:00+00:00'
        path.write_text(json.dumps(data))
        result = server.options_paper_accounts(self.base, self.now)
        projected = result['accounts'][0]['positions'][0]
        self.assertFalse(projected['mark_fresh'])
        self.assertEqual(projected['last_observed_mark_cents'], 35000)
        self.assertEqual(projected['last_observed_at'], '2026-10-21T14:00:00+00:00')

    def test_registered_source_paths_are_unique_and_exactly_seven(self):
        paths = [row[-1] for row in server.OPTIONS_ARM_BOOKS]
        self.assertEqual(len(paths), 7)
        self.assertEqual(len(set(paths)), 7)
        self.assertEqual(paths, [
            'neorome-stochastic-paper/v2/arms/A.json',
            'neorome-stochastic-paper/v2/arms/B.json',
            'neorome-stochastic-paper/f15-10x10/arms/A.json',
            'neorome-stochastic-paper/f15-10x10/arms/B.json',
            'neorome-stochastic-paper/intraday-comparison-v1/arms/A20.json',
            'neorome-stochastic-paper/intraday-comparison-v1/arms/B20.json',
            'neorome-stochastic-paper/intraday-comparison-v1/arms/SMA.json',
        ])

    def test_projection_api_shape_and_dashboard_table(self):
        result = server.options_paper_accounts(self.base, self.now)
        self.assertEqual(set(result), {'as_of', 'accounts', 'coverage', 'errors'})
        self.assertEqual(len(result['accounts']), 7)
        page = Path(server.__file__).with_name('index.html').read_text()
        self.assertIn('Options paper accounts', page)
        self.assertIn('options-paper-body', page)
        self.assertIn("fetch('/api/options-paper'", page)


if __name__ == '__main__':
    unittest.main()
