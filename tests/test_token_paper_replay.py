import copy
import unittest
from neorome.token_paper_replay import replay
from neorome.jupiter_quotes import SOL, USDC


def fixture():
    cost = {'version': 'stress-v1', 'additional_usd': '0.01', 'excludes_embedded_route_fees': True}
    spec = {'mint': SOL, 'decision_at': '2026-09-23T00:00:00Z',
            'exit_decision_at': '2026-09-23T00:15:00Z', 'latency_seconds': '1',
            'input_usdc_raw': '4900000', 'available_cash_usdc_raw': '100000000',
            'entry_cost': cost, 'exit_cost': cost}
    def observation(start, end, incoming, outgoing, amount, output, minimum):
        return {'requested_at': start, 'received_at': end, 'response_sha256': 'a'*64,
                'quote': {'inputMint': incoming, 'outputMint': outgoing, 'inAmount': amount,
                          'outAmount': output, 'otherAmountThreshold': minimum,
                          'priceImpact': '0.1', 'priceImpactUnit': 'percentage_points'}}
    entry = observation('2026-09-23T00:00:01Z', '2026-09-23T00:00:02Z', USDC, SOL, '4900000', '1000', '990')
    exit_quote = observation('2026-09-23T00:15:01Z', '2026-09-23T00:15:02Z', SOL, USDC, '990', '5100000', '5000000')
    return spec, entry, exit_quote


class TokenReplayTests(unittest.TestCase):
    def test_conservative_quantity_costs_and_no_admission(self):
        result = replay(*fixture())
        self.assertEqual(result['status'], 'modeled_closed')
        self.assertEqual(result['realized_pnl_usdc_raw'], '80000')
        self.assertFalse(result['paper_admitted'])

    def test_missing_exit_keeps_unresolved_position(self):
        spec, entry, _ = fixture()
        result = replay(spec, entry)
        self.assertEqual(result['status'], 'unresolved')
        self.assertIsNone(result['realized_pnl_usdc_raw'])

    def test_signed_provider_impact_uses_magnitude(self):
        spec, entry, sell = fixture()
        entry['quote']['priceImpact'] = '-0.025'
        self.assertEqual(replay(spec, entry, sell)['status'], 'modeled_closed')
        entry['quote']['priceImpact'] = '-3'
        self.assertEqual(replay(spec, entry, sell)['status'], 'entry_rejected')

    def test_optimistic_quantity_cannot_be_sold(self):
        spec, entry, sell = fixture()
        sell['quote']['inAmount'] = '1000'
        self.assertEqual(replay(spec, entry, sell)['status'], 'unresolved')

    def test_late_stale_and_predecision_quotes_rejected(self):
        for field, value in [('requested_at','2026-09-23T00:00:00Z'),
                             ('received_at','2026-09-23T00:00:04Z'),
                             ('received_at','2026-09-23T00:00:10Z')]:
            spec, entry, sell = fixture()
            entry[field] = value
            self.assertEqual(replay(spec, entry, sell)['status'], 'entry_rejected')

    def test_fees_count_toward_cap(self):
        spec, entry, sell = fixture()
        spec['input_usdc_raw'] = entry['quote']['inAmount'] = '5000000'
        self.assertEqual(replay(spec, entry, sell)['status'], 'entry_rejected')

    def test_invalid_impact_threshold_and_cost(self):
        for field, value in [('priceImpact','NaN'), ('priceImpact','3'), ('otherAmountThreshold','1001')]:
            spec, entry, sell = fixture()
            entry['quote'][field] = value
            self.assertEqual(replay(spec, entry, sell)['status'], 'entry_rejected')
        spec, entry, sell = fixture()
        spec['entry_cost'] = copy.deepcopy(spec['entry_cost'])
        spec['entry_cost']['excludes_embedded_route_fees'] = False
        self.assertEqual(replay(spec, entry, sell)['status'], 'entry_rejected')

if __name__ == '__main__': unittest.main()
