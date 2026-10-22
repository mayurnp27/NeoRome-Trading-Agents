from tempfile import TemporaryDirectory
import unittest

from desks.equities.intraday_chronological import ChronologicalEvaluationError, evaluate_holdout, evaluate_sessions, retain_trial


HASHES = {"universe_evidence_sha256": "a" * 64, "bar_prefix_sha256": "b" * 64, "quote_evidence_sha256": "c" * 64}


class ChronologicalTests(unittest.TestCase):
    def test_retains_unavailable_sessions_without_merit_claim(self):
        sessions = [
            {"session_date": "2026-09-21", "evidence": HASHES, "prefix_complete_through_signal": True, "bar_quality_clear": True, "size_units_verified": True},
            {"session_date": "2026-09-22", "evidence": HASHES, "prefix_complete_through_signal": True, "bar_quality_clear": True, "size_units_verified": False},
        ]
        result = evaluate_sessions(sessions)
        self.assertEqual(result["eligible_sessions"], 1)
        self.assertEqual(result["sessions"][1]["reason"], "quote_size_units_unverified")
        with TemporaryDirectory() as temp:
            saved = retain_trial(temp, {"strategy": "EQ-INTRADAY-001", "sessions": sessions})
            self.assertEqual(saved["outcome"]["eligible_sessions"], 1)

    def test_predeclared_holdout_preserves_embargo_and_never_claims_merit(self):
        sessions = [
            {"session_date": "2026-09-21", "evidence": HASHES, "prefix_complete_through_signal": True, "bar_quality_clear": True, "size_units_verified": True},
            {"session_date": "2026-09-22", "evidence": HASHES, "prefix_complete_through_signal": True, "bar_quality_clear": False, "size_units_verified": True},
            {"session_date": "2026-09-23", "evidence": HASHES, "prefix_complete_through_signal": True, "bar_quality_clear": True, "size_units_verified": True},
        ]
        result = evaluate_holdout(sessions, development_end="2026-09-21", holdout_start="2026-09-23")
        self.assertEqual(len(result["embargoed"]), 1)
        self.assertEqual(result["holdout_eligible_sessions"], 1)
        self.assertEqual(result["merit_status"], "unavailable_pending_outcomes")
        with self.assertRaisesRegex(ChronologicalEvaluationError, "after development_end"):
            evaluate_holdout(sessions, development_end="2026-09-23", holdout_start="2026-09-23")

    def test_requires_strict_chronology(self):
        with self.assertRaisesRegex(ChronologicalEvaluationError, "strictly chronological"):
            evaluate_sessions([{"session_date": "2026-09-22"}, {"session_date": "2026-09-21"}])


if __name__ == "__main__": unittest.main()
