import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import unittest

from neorome.crypto_interval_adapter import adapt_interval_observed
from neorome.prediction_observed_shadow import SHADOW_VERSION, run_once


UTC = timezone.utc
IDENTITY = {
    "market_id": "KXBTC15M-TEST-30",
    "outcome_id": "KXBTC15M-TEST-30:YES",
    "outcome": "YES",
    "terms": {
        "terms_sha256": "terms-placeholder",
        "rules_primary": "CF Benchmarks BRTI final average",
    },
    "source_identity": {"venue": "Kalshi", "benchmark": "BRTI"},
    "open_at": "2026-09-23T02:15:00Z",
    "cutoff_at": "2026-09-23T02:30:00Z",
    "expiry_at": "2026-09-23T02:35:00Z",
}
FEE = {
    "version": "fixed-stress-v1",
    "entry_fee_cents_per_contract": "1",
    "exit_fee_cents_per_contract": "1",
    "settlement_fee_cents_per_contract": "1",
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def source(version, payload, known_at="2026-09-23T02:20:00Z"):
    return {"source_kind": "observed", "source_version": version, "known_at": known_at, "payload": payload, "sha256": digest(payload)}


def valid_envelope():
    contract = copy.deepcopy(IDENTITY)
    contract["terms"]["terms_sha256"] = digest({"rules": "CF Benchmarks BRTI final average"})
    event = {
        **contract,
        "event_type": "book",
        "event_at": "2026-09-23T02:20:00Z",
        "received_at": "2026-09-23T02:20:01Z",
        "asks": [{"price": "0.42", "size": 2}],
        "bids": [{"price": "0.40", "size": 2}],
    }
    adapter = {
        "scope": "offline_research_only",
        "execution_enabled": False,
        "paper_admitted": False,
        "admissible_for_replay": True,
        "admission_reasons": [],
        "contract": contract,
        "events": [event],
        "fee": {"status": "caller_supplied_fixed_stress", **FEE},
    }
    market_body = {
        "venue": "Kalshi", "market_id": contract["market_id"], "outcome_id": contract["outcome_id"],
        "outcome": contract["outcome"], "terms_sha256": contract["terms"]["terms_sha256"],
    }
    market = {**market_body, "identity_sha256": digest(market_body)}
    settlement_body = {
        "version": "kalshi-btci-settlement-v1",
        "rule": "CF Benchmarks BRTI final average at cutoff",
        "source_identity": {"name": "CF Benchmarks", "benchmark": "BRTI"},
        "cutoff_at": contract["cutoff_at"],
        "payout_cents": 100,
    }
    settlement = {
        **settlement_body,
        "definition_sha256": digest(settlement_body),
        "provenance": source("terms-v1", {"rule": settlement_body["rule"]}),
    }
    return {
        "schema_version": 1,
        "shadow_version": SHADOW_VERSION,
        "source_kind": "observed",
        "execution_enabled": False,
        "adapter": adapter,
        "market_identity": market,
        "timing": {
            "event_at": event["event_at"], "received_at": event["received_at"],
            "decision_at": "2026-09-23T02:20:02Z", "event_at_source": "venue_event", "received_at_source": "local_receipt",
        },
        "quote": {"size_units": "contracts", "contract_size": 2, "asks": event["asks"], "size_provenance": source("book-schema-v1", {"units": "contracts"})},
        "fee_schema": {**FEE, "provenance": source("fee-map-v1", {"formula": "fixed stress"})},
        "settlement_definition": settlement,
        "probability": {
            "value": "0.61", "model_version": "btc-interval-calibrated-v1",
            "calibration": {"eligible": True, "out_of_sample": True, "protocol_version": "calibration-30dev-20holdout-v1", "development_events": 30, "holdout_events": 20},
            "provenance": source("calibration-evidence-v1", {"holdout_brier": "0.21"}),
        },
    }


class PredictionObservedShadowTests(unittest.TestCase):
    def test_eligible_candidate_is_content_addressed_without_side_effects(self):
        evidence = valid_envelope()
        first = run_once(evidence, now="2026-09-23T02:21:00Z")
        second = run_once(evidence, now="2026-09-23T02:21:00Z")
        self.assertEqual(first["status"], "eligible_shadow_candidate")
        self.assertEqual(first["candidate_sha256"], second["candidate_sha256"])
        self.assertFalse(first["eligible_for_execution"])
        self.assertEqual(first["ledger_mutations"], 0)
        self.assertEqual(first["broker_order_count"], 0)
        self.assertEqual(first["candidate"]["size"]["contract_size"], 2)

    def test_current_real_capture_abstains_on_missing_admission_gates(self):
        capture = json.loads(Path("coordination/verification/crypto-cross-lane/interval-observed.json").read_text())
        adapter = adapt_interval_observed(capture)
        result = run_once({
            "schema_version": 1,
            "shadow_version": SHADOW_VERSION,
            "source_kind": "observed",
            "execution_enabled": False,
            "adapter": adapter,
        })
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(any("missing_exchange_event_timestamp" in reason for reason in result["abstention_reasons"]))
        self.assertEqual(result["ledger_mutations"], 0)
        self.assertEqual(result["network_calls"], 0)

    def test_missing_probability_timestamp_or_fee_never_becomes_candidate(self):
        evidence = valid_envelope()
        evidence["probability"]["provenance"]["known_at"] = "2026-09-23T02:20:03Z"
        result = run_once(evidence, now="2026-09-23T02:21:00Z")
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(any("after decision time" in reason for reason in result["abstention_reasons"]))
        evidence = valid_envelope()
        evidence["quote"]["size_units"] = "unknown"
        result = run_once(evidence)
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(any("size units" in reason for reason in result["abstention_reasons"]))

    def test_causal_timing_rejects_decision_before_receipt(self):
        evidence = valid_envelope()
        evidence["timing"]["decision_at"] = "2026-09-23T02:20:00.500000Z"
        result = run_once(evidence, now="2026-09-23T02:21:00Z")
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(any("chronology" in reason for reason in result["abstention_reasons"]))

    def test_timing_rejects_decision_after_cutoff(self):
        evidence = valid_envelope()
        evidence["timing"]["decision_at"] = "2026-09-23T02:30:01Z"
        evidence["probability"]["provenance"]["known_at"] = "2026-09-23T02:29:00Z"
        result = run_once(evidence, now="2026-09-23T02:31:00Z")
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(any("cutoff or expiry" in reason for reason in result["abstention_reasons"]))


if __name__ == "__main__":
    unittest.main()
