import copy
import json
from pathlib import Path
import unittest

from neorome.crypto_interval_observed_evidence import ObservedEvidenceError, accumulate, normalize


CAPTURE = Path("coordination/verification/crypto-cross-lane/interval-observed.json")


class CryptoIntervalObservedEvidenceTests(unittest.TestCase):
    def capture(self):
        return json.loads(CAPTURE.read_text(encoding="utf-8"))

    def test_current_public_capture_is_normalized_as_incomplete_without_inference(self):
        result = normalize(self.capture())
        self.assertEqual(result["status"], "incomplete_observed_evidence")
        self.assertFalse(result["eligible_for_shadow"])
        self.assertIn("missing_exchange_event_timestamp", result["blockers"])
        self.assertIn("ambiguous_orderbook_size_units", result["blockers"])
        self.assertIn("quadratic_fee_formula_unverified", result["blockers"])
        self.assertEqual(result["fields"]["settlement"]["status"], "terms_bound_only")
        self.assertEqual(result["fields"]["calibration"]["status"], "unavailable")
        self.assertEqual(result["capture_provenance"]["books"]["records"][0]["sha256"], self.capture()["books"][0]["sha256"])

    def test_raw_provider_hash_is_verified_before_normalization(self):
        capture = self.capture()
        capture["books"][0]["raw_b64"] = capture["books"][0]["raw_b64"][:-4] + "AAAA"
        with self.assertRaisesRegex(ObservedEvidenceError, "raw hash mismatch"):
            normalize(capture)

    def test_accumulator_is_idempotent_and_rejects_conflicting_identity(self):
        record = normalize(self.capture())
        first = accumulate(None, record)
        second = accumulate(first, record)
        self.assertEqual(second, first)
        self.assertEqual(first["observation_count"], 1)
        conflict = copy.deepcopy(record)
        conflict["record_sha256"] = "f" * 64
        with self.assertRaisesRegex(ObservedEvidenceError, "identity conflicts"):
            accumulate(first, conflict)

    def test_synthetic_capture_is_refused(self):
        capture = self.capture()
        capture["paper_admitted"] = True
        with self.assertRaisesRegex(ObservedEvidenceError, "execution-disabled"):
            normalize(capture)


if __name__ == "__main__":
    unittest.main()
