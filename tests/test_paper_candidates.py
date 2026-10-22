import unittest

from desks.paper_candidates import (
    screen_crypto,
    screen_equity,
    screen_options,
    screen_prediction,
)


AS_OF = "2026-01-01T12:00:00Z"
QUOTE = "2026-01-01T11:59:00Z"


class PaperCandidateTests(unittest.TestCase):
    def test_exact_fee_units_and_non_execution_scope(self):
        base = {'ask':'10','fee_cents_per_unit':1,'quote_timestamp':QUOTE,
                'signal_known_at':'2026-01-01T11:58:00Z','side':'buy',
                'fractional_supported':False,'units':1}
        result = screen_equity(base, as_of=AS_OF)
        self.assertEqual(result['cost_cents'], 1001)
        self.assertEqual(result['scope'], 'offline_sizing_only')
        self.assertFalse(result['eligible_for_execution'])
        del base['fee_cents_per_unit']
        self.assertFalse(screen_equity(base, as_of=AS_OF)['accepted'])

    def test_equity_fractional_size_is_fully_funded_and_integer_cash(self):
        result = screen_equity(
            {
                "ask": "10.00",
                "fee_cents_per_unit": 1,
                "quote_timestamp": QUOTE,
                "signal_known_at": "2026-01-01T11:58:00Z",
                "side": "buy",
                "fractional_supported": True,
            },
            as_of=AS_OF,
        )
        self.assertTrue(result["accepted"])
        self.assertGreater(result["units"], 0)
        self.assertEqual(result["cost_cents"], 1500)
        self.assertEqual(result["loss_cents"], 1500)

    def test_equity_rejects_requested_oversize_and_future_quote(self):
        base = {
            "ask": 10,
            "fee_cents_per_unit": 0,
            "quote_timestamp": QUOTE,
            "signal_known_at": "2026-01-01T11:58:00Z",
            "side": "buy",
            "fractional_supported": False,
            "units": 2,
        }
        oversize = screen_equity(base, as_of=AS_OF)
        self.assertFalse(oversize["accepted"])
        self.assertTrue(any("exceeds" in reason for reason in oversize["reasons"]))

        future = dict(base, units=1, quote_timestamp="2026-01-01T12:01:00Z")
        future_result = screen_equity(future, as_of=AS_OF)
        self.assertFalse(future_result["accepted"])
        self.assertTrue(any("future" in reason for reason in future_result["reasons"]))

    def test_options_debit_is_whole_contract_and_rejects_no_affordable_contract(self):
        accepted = screen_options(
            {
                "long_ask": "0.20",
                "short_bid": "0.10",
                "fees_cents_per_contract": 100,
                "multiplier": 100,
                "quote_timestamp": QUOTE,
            },
            as_of=AS_OF,
        )
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["units"], 1)
        self.assertEqual(accepted["cost_cents"], 1100)

        too_expensive = screen_options(
            {
                "long_ask": "0.30",
                "fees_cents_per_contract": 0,
                "short_bid": "0.10",
                "multiplier": 100,
                "quote_timestamp": QUOTE,
            },
            as_of=AS_OF,
        )
        self.assertFalse(too_expensive["accepted"])
        self.assertTrue(any("contract" in reason for reason in too_expensive["reasons"]))

    def test_prediction_requires_terms_and_net_edge(self):
        accepted = screen_prediction(
            {
                "price": "0.40",
                "probability": "0.50",
                "payout": 1,
                "fee_cents_per_contract": 1,
                "quote_timestamp": QUOTE,
                "settlement_terms": "Official station observation, UTC date.",
                "category": "weather",
                "contracts": 10,
            },
            as_of=AS_OF,
        )
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["cost_cents"], 410)
        self.assertLessEqual(accepted["cost_cents"], 500)

        missing_terms = screen_prediction(
            {
                "price": "0.40",
                "probability": "0.50",
                "payout": 1,
                "quote_timestamp": QUOTE,
                "settlement_terms": "",
                "category": "weather",
            },
            as_of=AS_OF,
        )
        self.assertFalse(missing_terms["accepted"])
        self.assertTrue(any("settlement" in reason for reason in missing_terms["reasons"]))

    def test_prediction_rejects_stale_and_btc_spot_substitute(self):
        stale = screen_prediction(
            {
                "price": "0.40",
                "probability": "0.50",
                "payout": 1,
                "quote_timestamp": "2026-01-01T11:50:00Z",
                "settlement_terms": "Terms",
                "category": "weather",
            },
            as_of=AS_OF,
        )
        self.assertFalse(stale["accepted"])
        self.assertTrue(any("stale" in reason for reason in stale["reasons"]))

        spot = screen_prediction(
            {
                "price": "0.40",
                "probability": "0.50",
                "payout": 1,
                "quote_timestamp": QUOTE,
                "settlement_terms": "Terms",
                "category": "crypto_event",
                "underlying": "BTC",
                "instrument_type": "spot",
            },
            as_of=AS_OF,
        )
        self.assertFalse(spot["accepted"])
        self.assertTrue(any("spot" in reason for reason in spot["reasons"]))

    def test_crypto_requires_explicit_passed_controls_and_size_matched_exit(self):
        base = {
            "buy_price": "0.01",
            "buy_size": 500,
            "buy_quote_timestamp": QUOTE,
            "exit_price": "0.011",
            "exit_size": 500,
            "exit_quote_timestamp": QUOTE,
            "fee_bps": 100,
            "authority_passed": True,
            "sellability_passed": True,
            "owner_concentration_passed": True,
            "liquidity_passed": True,
            "fractional_supported": False,
            "units": 400,
        }
        accepted = screen_crypto(base, as_of=AS_OF)
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["units"], 400)
        self.assertEqual(accepted["cost_cents"], 404)
        self.assertEqual(accepted["quoted_exit_net_cents"], 435)
        self.assertEqual(accepted["fill_status"], "unfilled_quote")

        unknown = screen_crypto(dict(base, liquidity_passed=None), as_of=AS_OF)
        self.assertFalse(unknown["accepted"])
        self.assertTrue(any("liquidity" in reason for reason in unknown["reasons"]))

        unmatched = screen_crypto(dict(base, exit_size=399), as_of=AS_OF)
        self.assertFalse(unmatched["accepted"])
        self.assertTrue(any("size" in reason for reason in unmatched["reasons"]))

    def test_crypto_rejects_future_or_stale_exit_quote(self):
        future = {
            "buy_price": "0.01", "buy_size": 500, "buy_quote_timestamp": QUOTE,
            "exit_price": "0.011", "exit_size": 500,
            "exit_quote_timestamp": "2026-01-01T12:01:00Z",
            "authority_passed": True, "sellability_passed": True,
            "owner_concentration_passed": True, "liquidity_passed": True,
        }
        result = screen_crypto(future, as_of=AS_OF)
        self.assertFalse(result["accepted"])
        self.assertTrue(any("future" in reason for reason in result["reasons"]))


if __name__ == "__main__":
    unittest.main()
