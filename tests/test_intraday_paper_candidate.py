from datetime import datetime, timedelta, timezone
import unittest

from desks.equities.intraday_paper_candidate import prepare_entry


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)


def iso(seconds):
    return (BASE + timedelta(seconds=seconds)).isoformat()


def candidate(**overrides):
    value = {
        "candidate_id": "orb15-MU-2026-09-21",
        "strategy_id": "orb15",
        "symbol": "MU",
        "signal_timestamp": iso(0),
        "exit_decision_timestamp": iso(6 * 60 * 60 + 10 * 60),
        "session_close": iso(6 * 60 * 60 + 15 * 60),
        "universe_eligible": True,
        "prefix_complete_through_signal": True,
        "bar_quality_clear": True,
        "size_units_verified": True,
        "universe_evidence_sha256": "a" * 64,
        "bar_prefix_sha256": "b" * 64,
        "quote_evidence_sha256": "c" * 64,
        "universe_known_at": iso(-1),
        "bar_prefix_known_at": iso(0),
        "calendar_known_at": iso(-1),
        "fractional_supported": False,
        "fee_cents_per_unit": 0,
        "units": 1,
    }
    value.update(overrides)
    return value


def quote(seconds, *, bid=9.9, ask=10.0, size=10, receipt=None):
    return {"symbol": "MU", "timestamp": iso(seconds), "received_at": iso(seconds if receipt is None else receipt), "bid": bid, "ask": ask, "bid_size": size, "ask_size": size}


class IntradayPaperCandidateTests(unittest.TestCase):
    def test_future_evidence_and_late_receipt_rejected(self):
        result = prepare_entry(candidate(universe_known_at=iso(1)), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(result['accepted'])
        result = prepare_entry(candidate(), [quote(0.25, receipt=1)], order_lifetime=timedelta(seconds=0.25))
        self.assertFalse(result['accepted'])

    def test_prepare_is_unfilled_and_uses_strict_entry_evidence(self):
        result = prepare_entry(candidate(), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertTrue(result["accepted"])
        self.assertFalse(result["eligible_for_execution"])
        self.assertEqual(result["status"], "prepared_unfilled_reservation")
        self.assertEqual(result["funded_cost_cents"], 1000)
        self.assertEqual(result["exit_status"], "not_yet_observable_requires_separate_quote_replay")
        self.assertEqual(result["order_expires_at"], iso(1.25))
        self.assertEqual(result["planned_exit_at"], candidate()["exit_decision_timestamp"])

    def test_missing_control_or_quote_rejects_without_reservation(self):
        missing = prepare_entry(candidate(bar_quality_clear=False), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(missing["accepted"])
        self.assertIn("bar_quality_clear must be explicitly true", missing["reasons"])
        unavailable = prepare_entry(candidate(), [quote(2)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(unavailable["accepted"])
        self.assertIn("entry quote replay is unresolved", unavailable["reasons"])

    def test_depth_and_receipt_evidence_cannot_be_bypassed(self):
        shallow = prepare_entry(candidate(units=1), [quote(0.25, size=0.5)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(shallow["accepted"])
        self.assertIn("entry displayed depth does not cover sized quantity", shallow["reasons"])
        no_receipt = quote(0.25)
        no_receipt.pop("received_at")
        absent = prepare_entry(candidate(), [no_receipt], order_lifetime=timedelta(seconds=1))
        self.assertFalse(absent["accepted"])

    def test_frozen_signal_window_calendar_exit_and_prefix_evidence_are_enforced(self):
        early = prepare_entry(candidate(signal_timestamp="2026-09-21T13:30:00Z"), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(early["accepted"])
        self.assertIn("orb15 signal is before its frozen New York window", early["reasons"])
        wrong_exit = prepare_entry(candidate(exit_decision_timestamp=iso(60)), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(wrong_exit["accepted"])
        self.assertIn("exit_decision_timestamp must equal session_close minus five minutes", wrong_exit["reasons"])
        incomplete = prepare_entry(candidate(prefix_complete_through_signal=False), [quote(0.25)], order_lifetime=timedelta(seconds=1))
        self.assertFalse(incomplete["accepted"])
        self.assertIn("prefix_complete_through_signal must be explicitly true", incomplete["reasons"])


if __name__ == "__main__":
    unittest.main()
