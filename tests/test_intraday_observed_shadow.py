from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from desks.equities.intraday_observed_shadow import ObservedShadowRefusal, collect_once


UTC = timezone.utc
SIGNAL = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)


def iso(seconds=0):
    return (SIGNAL + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def provider(kind, _request):
    common = {"source": "documented-sip-provider", "source_version": "2026-09-schema"}
    values = {
        "universe": {**common, "selection_at": iso(-2), "payload": [{"permanent_id": "id-1", "symbol": "SYNTH"}]},
        "calendar": {**common, "payload": {"session_close": iso(6 * 60 * 60 + 15 * 60)}},
        "bar_prefix": {**common, "timeframe": "1Min", "adjustment": "raw", "payload": [{"timestamp": iso(-60)}]},
        "quotes": {**common, "size_units": "shares", "size_units_documentation": "provider-spec-v2026.09#quote-size", "payload": [{"symbol": "SYNTH", "timestamp": iso(.25), "bid": 9.9, "ask": 10.0, "bid_size": 10, "ask_size": 10}]},
    }
    return values[kind]


class ObservedShadowTests(unittest.TestCase):
    def test_collects_receipt_timed_provider_bundle_without_execution(self):
        receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL, SIGNAL + timedelta(seconds=.5)])
        with TemporaryDirectory() as temp:
            result = collect_once(temp, provider, signal_at=SIGNAL, symbol="SYNTH", order_lifetime=timedelta(seconds=1), clock=lambda: next(receipts))
            self.assertEqual(result["status"], "collected_unfilled_shadow_input")
            self.assertFalse(result["eligible_for_execution"])
            self.assertEqual(result["broker_order_count"], 0)
            quote_path = Path(result["archived"]["quotes"]["path"])
            self.assertIn('"received_at":"2026-09-21T13:45:00.500000Z"', quote_path.read_text())
            self.assertTrue(result["evidence"]["size_units_verified"])

    def test_refuses_ambiguous_size_units_and_late_evidence(self):
        def ambiguous(kind, request):
            item = dict(provider(kind, request))
            if kind == "quotes":
                item.pop("size_units_documentation")
            return item
        receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL, SIGNAL + timedelta(seconds=.5)])
        with TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ObservedShadowRefusal, "size units"):
                collect_once(temp, ambiguous, signal_at=SIGNAL, symbol="SYNTH", order_lifetime=timedelta(seconds=1), clock=lambda: next(receipts))
        receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL + timedelta(seconds=.1), SIGNAL + timedelta(seconds=.5)])
        with TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ObservedShadowRefusal, "bar_prefix.*after signal"):
                collect_once(temp, provider, signal_at=SIGNAL, symbol="SYNTH", order_lifetime=timedelta(seconds=1), clock=lambda: next(receipts))

    def test_refuses_quote_receipt_after_bounded_deadline(self):
        receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL, SIGNAL + timedelta(seconds=2)])
        with TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ObservedShadowRefusal, "order deadline"):
                collect_once(temp, provider, signal_at=SIGNAL, symbol="SYNTH", order_lifetime=timedelta(seconds=1), clock=lambda: next(receipts))


if __name__ == "__main__": unittest.main()
