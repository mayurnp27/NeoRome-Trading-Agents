import copy
from contextlib import redirect_stdout
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from neorome.paper_ledger import PaperLedger
from neorome.prediction_full_port import initialize
from neorome.prediction_weather_coordinator import coordinate, main, run_status
from tests.test_prediction_observed_shadow import valid_envelope
from tests.test_weather_probability import contracts, forecast, mapping, pair
from neorome.weather_validation import digest


UTC = timezone.utc
NOW = datetime(2026, 9, 23, 2, 21, tzinfo=UTC)


def inputs():
    identity = mapping(); pairs = [pair(index, identity) for index in range(50)]
    source = valid_envelope(); contract = copy.deepcopy(source["adapter"]["contract"])
    weather_contract = {"ticker": contract["market_id"], "terms_sha256": contract["terms"]["terms_sha256"],
                        "cutoff_at": contract["cutoff_at"], "outcome_rule": "inclusive_range", "floor_f": "70", "cap_f": "70"}
    current = forecast(identity, weather_contract)
    current["known_at"] = "2026-09-23T02:20:00Z"
    payload = {"predictor_max_f": current["predictor_max_f"], "known_at": current["known_at"]}
    current["forecast_sha256"] = digest(payload)
    execution = {key: source[key] for key in ("adapter", "market_identity", "timing", "quote", "fee_schema", "settlement_definition")}
    return pairs, identity, current, execution


class PredictionWeatherCoordinatorTests(unittest.TestCase):
    def test_realistic_missing_mapping_or_samples_abstain_without_mutation(self):
        result = coordinate([], None)
        self.assertEqual(result["status"], "abstained")
        self.assertEqual(result["ledger_mutations"], 0)
        self.assertEqual(result["broker_orders_sent"], 0)

    def test_calibrated_candidate_reserves_isolated_full_port_prediction_sleeve(self):
        pairs, identity, current, execution = inputs()
        with tempfile.TemporaryDirectory() as root:
            book = initialize(root + "/paper.sqlite3", now=NOW)
            result = coordinate(pairs, identity, forecast=current, execution=execution, now=NOW,
                                book=book, state_dir=root + "/state", mark_provider=lambda *_: [])
            self.assertEqual(result["status"], "paper_lifecycle")
            self.assertEqual(result["lifecycle"]["actions"][0]["type"], "reserve")
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertEqual(result["ledger_mutations"], 1)

    def test_terms_or_cutoff_mismatch_abstain_before_ledger(self):
        pairs, identity, current, execution = inputs()
        current["contract"]["cutoff_at"] = "2026-09-23T02:31:00Z"
        result = coordinate(pairs, identity, forecast=current, execution=execution, now=NOW)
        self.assertEqual(result["status"], "abstained")
        self.assertIn("cutoff differ", result["abstention_reasons"][0])

    def test_stop_or_recovery_gate_reaches_lifecycle_without_reservation(self):
        pairs, identity, current, execution = inputs()
        with tempfile.TemporaryDirectory() as root:
            book = initialize(root + "/paper.sqlite3", now=NOW)
            result = coordinate(pairs, identity, forecast=current, execution=execution, now=NOW,
                                book=book, state_dir=root + "/state", mark_provider=lambda *_: [], recovery_ok=False)
            self.assertEqual(result["status"], "paper_lifecycle")
            self.assertEqual(result["lifecycle"]["blocker"], "entries_disabled")
            self.assertEqual(result["ledger_mutations"], 0)

    def test_shared_multi_desk_ledger_is_rejected_before_any_reservation(self):
        pairs, identity, current, execution = inputs()
        with tempfile.TemporaryDirectory() as root:
            book = PaperLedger(root + "/paper.sqlite3", enforce_market_risk=True, clock=lambda: NOW)
            result = coordinate(pairs, identity, forecast=current, execution=execution, now=NOW,
                                book=book, state_dir=root + "/state", mark_provider=lambda *_: [])
            self.assertEqual(result["status"], "abstained")
            self.assertIn("full-port", result["abstention_reasons"][0])
            self.assertEqual(result["ledger_mutations"], 0)

    def test_nested_hash_addressed_pairs_and_no_mapping_write_status_only(self):
        identity = mapping()
        record = pair(0, identity)
        with tempfile.TemporaryDirectory() as root:
            pairs_dir = Path(root) / "dataset" / "pairs" / "2026" / "09"
            state_dir = Path(root) / "status"
            pairs_dir.mkdir(parents=True)
            (pairs_dir / "paired-event.json").write_text(json.dumps(record))
            # Arbitrary weather JSON is ignored because it has no valid pair hash.
            (pairs_dir / "raw-capture.json").write_text(json.dumps({"source_kind": "observed"}))
            result = run_status(Path(root) / "dataset", state_dir)
            self.assertEqual(result["status"], "abstained")
            self.assertIn("mapping_missing", result["abstention_reasons"][0])
            self.assertEqual(result["ledger_mutations"], 0)
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertEqual(json.loads((state_dir / "status.json").read_text()), result)
            self.assertFalse(list(state_dir.glob(".status.*.tmp")))
            self.assertEqual((state_dir / "status.json").stat().st_mode & 0o777, 0o600)

    def test_cli_omits_mapping_argument_without_parse_failure(self):
        with tempfile.TemporaryDirectory() as root:
            with redirect_stdout(io.StringIO()):
                code = main(["--pairs-dir", root, "--state", root + "/status"])
            self.assertEqual(code, 0)
            stored = json.loads((Path(root) / "status" / "status.json").read_text())
            self.assertEqual(stored["status"], "abstained")
            self.assertEqual(stored["ledger_mutations"], 0)


if __name__ == "__main__":
    unittest.main()
