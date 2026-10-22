import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from neorome.prediction_internal_paper_runtime import MAX_FILES, RuntimeEvidenceError, evaluate_weather, run_status


EVIDENCE = Path("coordination/verification/desk-admission/prediction-evidence-20260923T13.json.gz")
SHA256 = "2395f531618cf15c414d23e09593b32df4cf2ada65a75fabd0e5f1d0afcc2fa8"


class PredictionInternalPaperRuntimeTests(unittest.TestCase):
    def test_current_complete_weather_evidence_abstains_without_calibration(self):
        result = evaluate_weather(EVIDENCE, expected_sha256=SHA256)
        self.assertEqual(result["status"], "abstained")
        self.assertIn("weather_calibration_eligible_false", result["abstention_reasons"])
        self.assertEqual(result["ledger_mutations"], 0)
        self.assertEqual(result["broker_orders_sent"], 0)

    def test_expected_digest_is_enforced(self):
        with self.assertRaisesRegex(RuntimeEvidenceError, "SHA-256"):
            evaluate_weather(EVIDENCE, expected_sha256="0" * 64)

    def test_status_selects_newest_complete_immutable_archive_and_writes_atomically(self):
        def archive(collected_at, complete=True):
            return {"source_kind": "observed", "venue": "kalshi_public", "series_ticker": "KXHIGHNY",
                    "collected_at": collected_at, "complete_response": complete,
                    "calibration_eligible": False, "probabilities": None, "orders_sent": 0,
                    "responses": [{"kind": "open"}, {"kind": "settled"}]}
        with tempfile.TemporaryDirectory() as root:
            evidence_dir = Path(root) / "evidence"; state_dir = Path(root) / "status"
            evidence_dir.mkdir()
            (evidence_dir / "older.json").write_text(json.dumps(archive("2026-09-23T12:00:00Z")))
            (evidence_dir / "newer-incomplete.json").write_text(json.dumps(archive("2026-09-23T14:00:00Z", False)))
            newest = evidence_dir / "newest.json"
            newest.write_text(json.dumps(archive("2026-09-23T13:00:00Z")))
            result = run_status(evidence_dir, state_dir)
            self.assertEqual(result["status"], "abstained")
            self.assertEqual(result["selected_evidence"], str(newest))
            stored = json.loads((state_dir / "status.json").read_text())
            self.assertEqual(stored, result)
            self.assertFalse(list(state_dir.glob(".status.*.tmp")))
            self.assertEqual((state_dir / "status.json").stat().st_mode & 0o777, 0o600)

    def test_status_keeps_selecting_newest_complete_archive_after_directory_grows(self):
        def archive(collected_at):
            return {"source_kind": "observed", "venue": "kalshi_public", "series_ticker": "KXHIGHNY",
                    "collected_at": collected_at, "complete_response": True,
                    "calibration_eligible": False, "probabilities": None, "orders_sent": 0,
                    "responses": [{"kind": "open"}, {"kind": "settled"}]}

        with tempfile.TemporaryDirectory() as root:
            evidence_dir = Path(root) / "evidence"
            evidence_dir.mkdir()
            for index in range(MAX_FILES + 5):
                name = f"2026-09-{index:04d}.json"
                stamp = (datetime(2026, 9, 23, tzinfo=timezone.utc) + timedelta(minutes=index)).isoformat()
                (evidence_dir / name).write_text(json.dumps(archive(stamp)))
            newest = sorted(evidence_dir.iterdir())[-1]

            result = run_status(evidence_dir, Path(root) / "status")

            self.assertEqual(result["selected_evidence"], str(newest))
            self.assertEqual(result["status"], "abstained")


if __name__ == "__main__":
    unittest.main()
