import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from neorome.paper_ledger import PaperLedger
from neorome.prediction_full_port import initialize
from neorome.prediction_weather_candidate_pipeline import run_once as candidate_run
from neorome.prediction_weather_execution_snapshot import build
from neorome.prediction_weather_inbox import PredictionWeatherInboxError, publish_snapshot
from neorome.prediction_weather_lifecycle import run_once as lifecycle_run
from tests.test_weather_probability import contracts, forecast, mapping, pair
from neorome.weather_validation import digest

UTC = timezone.utc
EVENT = datetime(2026, 2, 20, 1, tzinfo=UTC)
CUTOFF = datetime(2026, 2, 20, 4, tzinfo=UTC)


def source(version, payload, known_at=EVENT):
    return {"source_kind": "observed", "source_version": version, "known_at": known_at.isoformat().replace("+00:00", "Z"), "payload": payload, "sha256": digest(payload)}


def market_evidence():
    row = {"ticker": "KXHIGHNY-26FEB20-T70", "event_ticker": "KXHIGHNY-26FEB20", "status": "active", "market_type": "binary", "price_level_structure": "linear_cent",
           "rules_primary": "Official temperature range rule", "rules_secondary": "Official Weather Company final publication", "strike_type": "range", "floor_strike": 70,
           "notional_value_dollars": "1.0000", "open_time": "2026-02-19T14:00:00Z", "close_time": CUTOFF.isoformat().replace("+00:00", "Z"), "expected_expiration_time": "2026-02-20T19:00:00Z"}
    return {"source_kind": "observed", "venue": "kalshi_public", "series_ticker": "KXHIGHNY", "complete_response": True,
            "responses": [{"status": "open", "complete_response": True, "markets": [row]}, {"status": "settled", "complete_response": True, "markets": []}], "orders_sent": 0}


def fee():
    return {"verification": "official", "version": "kalshi-fee-reviewed-v1", "entry_fee_cents_per_contract": "1", "exit_fee_cents_per_contract": "1", "settlement_fee_cents_per_contract": "0",
            "provenance": source("official-fee-v1", {"version": "kalshi-fee-reviewed-v1"})}


def settlement():
    return {"verification": "official", "version": "kalshi-weather-reviewed-v1", "rule": "Weather Company final maximum", "source_identity": {"name": "The Weather Company", "station": "CLINYC"},
            "cutoff_at": CUTOFF.isoformat().replace("+00:00", "Z"), "payout_cents": 100, "provenance": source("official-terms-v1", {"rule": "Weather Company final maximum"})}


def books(at, tag):
    payload = {"tag": tag, "at": at.isoformat()}
    return {"provenance": source("orderbook-v1", payload, at), "books": [{"ticker": "KXHIGHNY-26FEB20-T70", "event_at": at.isoformat().replace("+00:00", "Z"),
             "received_at": (at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"), "size_units": "contracts", "contract_size": 1,
             "asks": [{"price": "0.40", "size": 2}], "bids": [{"price": "0.39", "size": 2}]}]}


class PredictionWeatherFullPaperTests(unittest.TestCase):
    def test_current_style_snapshot_keeps_missing_economic_terms_as_blockers(self):
        value = build(market_evidence())
        self.assertEqual(value["status"], "abstained")
        self.assertIn("official_fee_metadata_missing", value["abstention_reasons"])
        self.assertIn("official_settlement_metadata_missing", value["abstention_reasons"])
        self.assertEqual(value["orders_sent"], 0)
        bad = copy.deepcopy(value); bad["raw"] = "provider payload"; body = dict(bad); body.pop("snapshot_sha256"); bad["snapshot_sha256"] = digest(body)
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(PredictionWeatherInboxError, "unsanitized"):
                publish_snapshot(bad, Path(root) / "inbox")

    def test_unenabled_units_separate_read_only_builder_from_sole_ledger_worker(self):
        snapshot_unit = Path("deploy/recorder/neorome-prediction-weather-execution-snapshot.service").read_text()
        candidate_unit = Path("deploy/recorder/neorome-prediction-weather-candidate.service").read_text()
        paper_unit = Path("deploy/recorder/neorome-prediction-weather-paper.service").read_text()
        self.assertIn("User=neorome", snapshot_unit)
        self.assertIn("--market-evidence-dir /var/lib/neorome-prediction-evidence/evidence", snapshot_unit)
        self.assertIn("User=neorome", candidate_unit)
        self.assertIn("--publish-inbox /var/lib/neorome-prediction-weather-inbox", candidate_unit)
        self.assertIn("User=neorome", paper_unit)
        self.assertIn("--ledger /var/lib/neorome-prediction-paper/paper.sqlite3", paper_unit)
        self.assertIn("ReadOnlyPaths=/var/lib/neorome-prediction-weather-inbox", paper_unit)
        blocked = "InaccessiblePaths=/var/lib/neorome-paper /var/lib/neorome-paper-recovery /etc/neorome"
        self.assertIn(blocked, snapshot_unit)
        self.assertIn(blocked, candidate_unit)
        self.assertNotIn("/var/lib/neorome-paper", snapshot_unit.split("ReadWritePaths=")[1].split("\n")[0])
        self.assertNotIn("/var/lib/neorome-paper", candidate_unit.split("ReadWritePaths=")[1].split("\n")[0])
        self.assertIn("ReadWritePaths=/var/lib/neorome-prediction-weather-paper /var/lib/neorome-prediction-paper", paper_unit)
        self.assertIn("InaccessiblePaths=/var/lib/neorome-paper /etc/neorome", paper_unit)
        init_unit = Path("deploy/recorder/neorome-prediction-paper-ledger-init.service").read_text()
        self.assertIn("--initialize --ledger /var/lib/neorome-prediction-paper/paper.sqlite3", init_unit)
        self.assertIn("InaccessiblePaths=/var/lib/neorome-paper /var/lib/neorome-paper-recovery /etc/neorome", init_unit)

    def test_shared_ledger_lock_is_an_honest_zero_mutation_abstention(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root); ledger = path / "paper.sqlite3"; initialize(ledger, now=EVENT)
            with patch("neorome.prediction_weather_lifecycle.paper_run", side_effect=__import__("sqlite3").OperationalError("database is locked")):
                result = lifecycle_run(inbox_root=path / "missing-inbox", ledger_path=ledger, state_dir=path / "state", recovery_status=None, now=EVENT)
            self.assertEqual(result["status"], "abstained")
            self.assertEqual(result["ledger_mutations"], 0)
            self.assertEqual(result["orders_sent"], 0)

    def test_sanitized_service_path_reserves_fills_marks_settles_and_restarts(self):
        identity = mapping(); pairs = [pair(index, identity) for index in range(50)]
        initial = build(market_evidence(), books(EVENT, "one"), fee=fee(), settlement=settlement())
        self.assertEqual(initial["status"], "eligible_execution_snapshot")
        execution = initial["executions"][0]
        weather_contract = {"ticker": execution["market_identity"]["market_id"], "terms_sha256": execution["market_identity"]["terms_sha256"],
                            "cutoff_at": execution["adapter"]["contract"]["cutoff_at"], "outcome_rule": "inclusive_range", "floor_f": "70", "cap_f": "70"}
        current = forecast(identity, weather_contract)
        current["known_at"] = "2026-02-20T01:00:00Z"
        current["forecast_sha256"] = digest({"predictor_max_f": current["predictor_max_f"], "known_at": current["known_at"]})
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root); pairs_dir = root_path / "pairs"; pairs_dir.mkdir()
            for index, record in enumerate(pairs): (pairs_dir / f"{index}.json").write_text(json.dumps(record))
            inbox = root_path / "inbox"; snapshot_path = root_path / "initial.json"; snapshot_path.write_text(json.dumps(initial))
            publish_snapshot(initial, inbox)
            candidate = candidate_run(pairs_dir=pairs_dir, mapping=identity, forecast=current, snapshot=initial, inbox_root=inbox, now=EVENT + timedelta(seconds=2))
            self.assertEqual(candidate["status"], "eligible_shadow_candidate")
            ledger_path = root_path / "paper.sqlite3"; initialize(ledger_path, now=EVENT)
            state = root_path / "state"
            reserved = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=EVENT + timedelta(seconds=3))
            self.assertEqual(reserved["lifecycle"]["actions"][0]["type"], "reserve")
            # The entry snapshot cannot fill its own reservation even after the delay.
            same = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=EVENT + timedelta(seconds=14))
            self.assertEqual(same["lifecycle"]["blocker"], "awaiting_independent_fill_quote")
            later = build(market_evidence(), books(EVENT + timedelta(seconds=20), "two"), fee=fee(), settlement=settlement())
            publish_snapshot(later, inbox)
            filled = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=EVENT + timedelta(seconds=22))
            self.assertEqual(filled["lifecycle"]["actions"][0]["type"], "simulated_fill")
            marked_snapshot = build(market_evidence(), books(EVENT + timedelta(seconds=24), "mark"), fee=fee(), settlement=settlement())
            publish_snapshot(marked_snapshot, inbox)
            marked = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=EVENT + timedelta(seconds=26))
            self.assertEqual(marked["lifecycle"]["actions"][0]["type"], "mark")
            terms = later["executions"][0]["market_identity"]["terms_sha256"]
            final = build(market_evidence(), books(EVENT + timedelta(seconds=30), "three"), fee=fee(), settlement=settlement(), settlement_results={"results": [{"market_id": "KXHIGHNY-26FEB20-T70", "terms_sha256": terms, "finalized": True,
                         "settlement_at": (CUTOFF + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"), "received_at": (CUTOFF + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"), "source_hash": "a" * 64, "result": "yes", "yes_payout_cents": 100}]})
            publish_snapshot(final, inbox)
            settled = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=CUTOFF + timedelta(minutes=3))
            self.assertEqual(settled["lifecycle"]["actions"][0]["type"], "settlement")
            # Simulate a crash after the durable ledger close but before local
            # completed-candidate state is retained: audit recovery still blocks replay.
            (state / "completed-candidates.json").unlink()
            restarted = lifecycle_run(inbox_root=inbox, ledger_path=ledger_path, state_dir=state, recovery_status=None, now=CUTOFF + timedelta(minutes=4))
            self.assertEqual(restarted["lifecycle"]["blocker"], "candidate_missing")
            self.assertEqual(settled["orders_sent"], 0)


if __name__ == "__main__":
    unittest.main()
