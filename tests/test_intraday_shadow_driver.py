from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from desks.equities.intraday_shadow_driver import run_once


UTC = timezone.utc
SIGNAL = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)


def iso(seconds=0): return (SIGNAL + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def provider(kind, _request):
    common = {"source": "documented-sip-provider", "source_version": "2026-09-schema"}
    return {
        "universe": {**common, "selection_at": iso(-2), "payload": [{"permanent_id": "id-1", "symbol": "SYNTH"}]},
        "calendar": {**common, "payload": {"session_close": iso(6 * 60 * 60 + 15 * 60)}},
        "bar_prefix": {**common, "timeframe": "1Min", "adjustment": "raw", "payload": [{"timestamp": iso(-60), "close": 10.0}]},
        "quotes": {**common, "size_units": "shares", "size_units_documentation": "provider-spec-v2026.09#quote-size", "payload": [{"symbol": "SYNTH", "timestamp": iso(.25), "bid": 9.9, "ask": 10.0, "bid_size": 10, "ask_size": 10}]},
    }[kind]


def clock():
    return next(clock.receipts)


def candidate_decision(_inputs):
    return {"action": "candidate", "candidate": {
        "candidate_id": "obs-orb15-synth", "strategy_id": "orb15", "symbol": "SYNTH",
        "signal_timestamp": iso(), "exit_decision_timestamp": iso(6 * 60 * 60 + 10 * 60),
        "session_close": iso(6 * 60 * 60 + 15 * 60), "universe_eligible": True,
        "prefix_complete_through_signal": True, "bar_quality_clear": True,
        "fractional_supported": False, "fee_cents_per_unit": 0, "units": 1,
    }}


class ShadowDriverTests(unittest.TestCase):
    def setUp(self):
        clock.receipts = iter([SIGNAL - timedelta(seconds=1), SIGNAL - timedelta(seconds=.5), SIGNAL, SIGNAL + timedelta(seconds=.5)])

    def call(self, root, decision=candidate_decision):
        return run_once(root, provider, decision, signal_at=SIGNAL, symbol="SYNTH", order_lifetime=timedelta(seconds=1), clock=clock)

    def test_retains_unfilled_candidate_with_realistic_fill_fields(self):
        with TemporaryDirectory() as temp:
            result = self.call(temp)
            self.assertEqual(result["status"], "eligible_unfilled_candidate")
            self.assertFalse(result["eligible_for_execution"])
            self.assertEqual(result["broker_order_count"], 0)
            self.assertEqual(result["prepared"]["entry_quote"]["ask"], 10.0)
            self.assertEqual(result["prepared"]["entry_quote"]["displayed_size"], 10.0)
            retained = json.loads(Path(result["decision_path"]).read_text())
            self.assertEqual(retained["prepared"]["status"], "prepared_unfilled_reservation")

    def test_retains_explicit_abstention_and_never_passes_quotes_to_signal(self):
        seen = {}
        def abstain(inputs):
            seen.update(inputs)
            return {"action": "abstain", "reason": "opening range not broken"}
        with TemporaryDirectory() as temp:
            result = self.call(temp, abstain)
            self.assertEqual(result["status"], "abstained")
            self.assertNotIn("quotes", seen["raw"])
            self.assertTrue(Path(result["decision_path"]).exists())

    def test_rejects_candidate_that_does_not_bind_collected_signal(self):
        def wrong(candidate):
            result = candidate_decision(candidate)
            result["candidate"]["signal_timestamp"] = iso(-60)
            return result
        with TemporaryDirectory() as temp:
            result = self.call(temp, wrong)
            self.assertEqual(result["status"], "rejected")
            self.assertIn("bind collected", result["reason"])


if __name__ == "__main__": unittest.main()
