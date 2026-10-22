from datetime import datetime, timedelta, timezone
import unittest

from neorome.equity_intraday_provider import admit_quote, provider_capability


UTC = timezone.utc
AS_OF = datetime(2026, 9, 25, 14, 0, 1, tzinfo=UTC)


def quote(**overrides):
    value = {
        "provider": "alpaca_sip",
        "source_version": "sip-v2",
        "symbol": "MU",
        "bid": "100.00",
        "ask": "100.05",
        "quote_at": "2026-09-25T14:00:00.500000Z",
        "received_at": "2026-09-25T14:00:00.600000Z",
        "provenance_sha256": "a" * 64,
        "feed": "sip",
    }
    value.update(overrides)
    return value


class EquityIntradayProviderTests(unittest.TestCase):
    def test_valid_sip_bid_ask_is_admitted_as_evidence_only(self):
        result = admit_quote("alpaca_sip", quote(), expected_symbol="MU", as_of=AS_OF)
        self.assertTrue(result.admitted)
        self.assertIsNotNone(result.quote)
        self.assertEqual(result.quote.ask, result.quote.ask.__class__("100.05"))
        self.assertIsNone(result.reason_code)

    def test_wrong_feed_is_rejected_without_fallback(self):
        result = admit_quote("alpaca_sip", quote(feed="iex"), expected_symbol="MU", as_of=AS_OF)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_code, "wrong_feed")

    def test_stale_quote_is_rejected(self):
        stale = (AS_OF - timedelta(seconds=2)).isoformat()
        result = admit_quote("alpaca_sip", quote(quote_at=stale, received_at=stale), expected_symbol="MU", as_of=AS_OF)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_code, "invalid_quote_evidence")
        self.assertEqual(result.reason, "quote is stale")

    def test_missing_bid_ask_and_provenance_fail_closed(self):
        missing_quote = admit_quote("alpaca_sip", quote(bid=None), expected_symbol="MU", as_of=AS_OF)
        missing_provenance = admit_quote("alpaca_sip", quote(provenance_sha256=None), expected_symbol="MU", as_of=AS_OF)
        self.assertEqual(missing_quote.reason_code, "invalid_quote_evidence")
        self.assertEqual(missing_provenance.reason_code, "invalid_quote_evidence")

    def test_symbol_identity_and_crossed_quotes_fail_closed(self):
        wrong_symbol = admit_quote("alpaca_sip", quote(symbol="NVDA"), expected_symbol="MU", as_of=AS_OF)
        crossed = admit_quote("alpaca_sip", quote(bid="101", ask="100"), expected_symbol="MU", as_of=AS_OF)
        self.assertEqual(wrong_symbol.reason, "quote symbol does not match requested symbol")
        self.assertEqual(crossed.reason, "quote is crossed")

    def test_robinhood_is_a_precise_external_capability_blocker(self):
        result = admit_quote("robinhood", quote(provider="robinhood", feed="equity"), expected_symbol="MU", as_of=AS_OF)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_code, "provider_capability_insufficient")
        self.assertIn("no callable US equity quote", result.reason)

    def test_tradingview_last_price_cannot_be_promoted_to_fill_evidence(self):
        result = admit_quote("tradingview", {
            "provider": "tradingview", "source_version": "mcp-0.9.0", "symbol": "MU",
            "price": 100.0, "timestamp": AS_OF.isoformat(), "feed": "chart",
            "provenance_sha256": "b" * 64,
        }, expected_symbol="MU", as_of=AS_OF)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_code, "provider_capability_insufficient")
        self.assertIn("bid/ask", result.reason)

    def test_unknown_provider_is_non_failing_blocker(self):
        result = admit_quote("some-feed", {}, expected_symbol="MU", as_of=AS_OF)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason_code, "unknown_provider")

    def test_capability_records_are_read_only_descriptions(self):
        robinhood = provider_capability("Robinhood")
        tradingview = provider_capability("TRADINGVIEW")
        self.assertFalse(robinhood.current_bid_ask)
        self.assertFalse(tradingview.provider_event_timestamp)


if __name__ == "__main__":
    unittest.main()
