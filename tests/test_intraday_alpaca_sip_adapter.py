from datetime import datetime, timezone
import unittest

from desks.equities.intraday_alpaca_sip_adapter import AlpacaSIPCaptureError, provider_from_captures


UTC = timezone.utc


def captures():
    return {
        "calendar": {"calendar": [{"date": "2026-09-21", "open": "2026-09-21T09:30:00", "close": "2026-09-21T16:00:00"}]},
        "bar_prefix": {"request": {"feed": "sip", "timeframe": "1Min"}, "adjustment": "raw", "bars": {"SYNTH": [{"timestamp": "2026-09-21T13:44:00Z", "close": 10}]}},
        "quotes": {"request": {"feed": "sip"}, "quotes": {"SYNTH": [{"timestamp": "2026-09-21T13:45:00.25Z", "bid_price": 9.9, "ask_price": 10, "bid_size": 10, "ask_size": 10}]}},
    }


def universe():
    return {"source": "pit-vendor", "source_version": "v2026", "selection_at": "2026-09-21T13:44:00Z",
            "members": [{"permanent_id": "id-1", "symbol": "SYNTH"}],
            "historical_membership": {"source": "pit-vendor", "includes_delisted": True}}


class AlpacaSIPAdapterTests(unittest.TestCase):
    def test_normalizes_sip_captures_and_preserves_raw_envelopes(self):
        provider = provider_from_captures(captures(), universe(), quote_size_documentation="alpaca-doc-v1#sizes")
        quote = provider("quotes", {"symbol": "SYNTH"})
        self.assertEqual(quote["payload"][0]["ask"], 10)
        self.assertIn("raw_response", quote)
        calendar = provider("calendar", {})
        self.assertEqual(calendar["payload"]["session_close"], "2026-09-21T20:00:00Z")

    def test_refuses_missing_sip_or_historical_provenance(self):
        broken = captures(); broken["quotes"]["request"]["feed"] = "iex"
        provider = provider_from_captures(broken, universe(), quote_size_documentation="doc")
        with self.assertRaisesRegex(AlpacaSIPCaptureError, "SIP"):
            provider("quotes", {"symbol": "SYNTH"})
        bad_universe = universe(); bad_universe["historical_membership"]["includes_delisted"] = False
        with self.assertRaisesRegex(AlpacaSIPCaptureError, "delisted"):
            provider_from_captures(captures(), bad_universe, quote_size_documentation="doc")


if __name__ == "__main__": unittest.main()
