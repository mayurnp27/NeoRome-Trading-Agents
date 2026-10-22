from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from desks.options.selection import payload_sha256
from neorome.options_observed_shadow import ObservedShadowArtifactConflict, run_observed_shadow

from tests.test_options_shadow_rehearsal import _evidence_fixture

UTC = timezone.utc
NOW = datetime(2026, 9, 22, 14, 0, tzinfo=UTC)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _evidence():
    observation, context, candidates = _evidence_fixture()
    holdings = {
        "complete": True, "local_receipt_at": NOW.isoformat(), "timestamp": NOW.isoformat(),
        "source": "read-only-account-snapshot", "option_holdings": {},
        "underlying_quantity": 0, "open_orders_reconciled": True,
    }
    holdings["sha256"] = _hash(holdings)
    return {
        "source": "robinhood-trading", "local_receipt_at": NOW.isoformat(),
        "chain_complete": True, "size_units": "contracts",
        "size_units_evidence": "provider-contract-size-v1",
        "normalized_quotes": candidates, "selector": {"observation": observation, "context": context},
        "holdings": holdings,
    }


class OptionsObservedShadowTests(unittest.TestCase):
    @staticmethod
    def _set_candidate_rows(evidence, rows):
        evidence["normalized_quotes"] = rows
        context = evidence["selector"]["context"]
        context["candidate_rows_sha256"] = payload_sha256(rows)
        context["normalized_payload_sha256"] = payload_sha256(context, hash_field="normalized_payload_sha256")
        return evidence

    def test_complete_bounded_100_row_universe_reaches_deterministic_selector(self):
        evidence = _evidence()
        rows = deepcopy(evidence["normalized_quotes"])
        for strike in range(771, 869):
            row = deepcopy(rows[1])
            row["strike"] = str(strike)
            row["symbol"] = f"SPY261030C{strike * 1000:08d}"
            row["instrument_id"] = f"{row['symbol']}-id"
            rows.append(row)
        self.assertEqual(len(rows), 100)
        result = run_observed_shadow(self._set_candidate_rows(evidence, rows), as_of=NOW)
        self.assertEqual(result["status"], "eligible_shadow_candidate")
        self.assertEqual(result["shadow_candidate"]["long"]["symbol"], "SPY261030C00765000")
        self.assertEqual(result["shadow_candidate"]["short"]["symbol"], "SPY261030C00770000")

    def test_explicit_full_port_profile_uses_only_an_affordable_registered_pair(self):
        evidence = _evidence()
        rows = deepcopy(evidence["normalized_quotes"])
        # Delta-perfect pair is deliberately unaffordable.  The second long
        # remains in the registered band and forms an independently valid,
        # conservative $110 debit vertical with the same short leg.
        rows[0].update(ask="10.00", bid="9.90")
        affordable = deepcopy(rows[0])
        affordable.update(symbol="SPY261030C00766000", instrument_id="full-port-long",
                          strike="766", delta="0.45", ask="1.20", bid="1.10")
        rows.append(affordable)
        result = run_observed_shadow(self._set_candidate_rows(evidence, rows), as_of=NOW,
                                     max_entry_cost="300", paper_profile="options_full_port_v1")
        self.assertEqual(result["status"], "eligible_shadow_candidate")
        self.assertEqual(result["paper_profile"], "options_full_port_v1")
        self.assertEqual(result["shadow_candidate"]["long"]["symbol"], "SPY261030C00766000")
        self.assertEqual(result["shadow_candidate"]["cost_model"]["total_entry_cost"], "116.00")

    def test_candidate_universe_over_100_or_under_2_is_refused(self):
        base = _evidence()
        for rows in (base["normalized_quotes"][:1], base["normalized_quotes"] * 51):
            evidence = deepcopy(base)
            result = run_observed_shadow(self._set_candidate_rows(evidence, rows), as_of=NOW)
            self.assertEqual(result["status"], "abstain")
            self.assertIn("2-100", result["reason"])

    def test_archived_snapshot_route_uses_existing_robinhood_normalizer(self):
        evidence = _evidence()
        rows = evidence.pop("normalized_quotes")
        evidence["snapshot"] = {"source": "robinhood-trading", "received_at": NOW.isoformat(), "responses": []}
        with patch("neorome.options_observed_shadow.normalize", return_value=rows) as normalizer:
            result = run_observed_shadow(evidence, as_of=NOW)
        self.assertEqual(result["status"], "eligible_shadow_candidate")
        normalizer.assert_called_once()

    def test_eligible_result_is_content_addressed_and_read_only(self):
        evidence = _evidence()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decision.json"
            result = run_observed_shadow(evidence, as_of=NOW, output_path=path)
            self.assertEqual(result["status"], "eligible_shadow_candidate")
            self.assertEqual(result["shadow_candidate"]["fill_status"], "not_a_fill")
            self.assertEqual(result["orders_sent"], 0)
            self.assertEqual(result["reservations_created"], 0)
            self.assertFalse(result["ledger_written"])
            self.assertFalse(result["broker_called"])
            self.assertEqual(len(result["artifact_sha256"]), 64)
            stored = json.loads(path.read_text())
            self.assertEqual(stored["artifact_sha256"], result["artifact_sha256"])
            self.assertEqual(stored["evidence"]["size_units"], "contracts")

    def test_artifact_replay_is_idempotent_and_conflicts_fail_closed(self):
        evidence = _evidence()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "decision.json"
            first = run_observed_shadow(evidence, as_of=NOW, output_path=path)
            replay = run_observed_shadow(deepcopy(evidence), as_of=NOW, output_path=path)
            self.assertEqual(first, replay)
            self.assertEqual(first["artifact_sha256"], replay["artifact_sha256"])
            changed = deepcopy(evidence)
            changed["chain_complete"] = False
            with self.assertRaises(ObservedShadowArtifactConflict):
                run_observed_shadow(changed, as_of=NOW, output_path=path)

    def test_missing_receipt_chain_or_size_units_abstains_precisely(self):
        cases = [
            ("local_receipt_at", "local_receipt_invalid"),
            ("chain_complete", "chain_incomplete"),
            ("size_units", "size_units_unverified"),
        ]
        for field, expected in cases:
            with self.subTest(field=field):
                evidence = _evidence()
                if field == "local_receipt_at":
                    del evidence[field]
                elif field == "chain_complete":
                    evidence[field] = False
                else:
                    evidence[field] = "shares"
                result = run_observed_shadow(evidence, as_of=NOW)
                self.assertEqual(result["status"], "abstain")
                self.assertEqual(result["reason_code"], expected)

    def test_stale_quote_holdings_identity_and_hash_fail_closed(self):
        cases = []
        stale = _evidence()
        stale["normalized_quotes"] = deepcopy(stale["normalized_quotes"])
        stale["normalized_quotes"][0]["timestamp"] = "2026-09-22T13:00:00+00:00"
        cases.append(stale)
        incomplete = _evidence()
        incomplete["holdings"] = deepcopy(incomplete["holdings"])
        incomplete["holdings"]["complete"] = False
        cases.append(incomplete)
        outside = _evidence()
        outside["holdings"] = deepcopy(outside["holdings"])
        outside["holdings"]["option_holdings"] = {"SPY261030C00900000": 0}
        outside["holdings"]["sha256"] = _hash({k: v for k, v in outside["holdings"].items() if k != "sha256"})
        cases.append(outside)
        bad_hash = _evidence()
        bad_hash["holdings"] = deepcopy(bad_hash["holdings"])
        bad_hash["holdings"]["sha256"] = "f" * 64
        cases.append(bad_hash)
        for evidence in cases:
            with self.subTest(evidence=evidence):
                result = run_observed_shadow(evidence, as_of=NOW)
                self.assertEqual(result["status"], "abstain")
                self.assertEqual(result["broker_called"], False)

    def test_selector_no_trade_is_an_abstention_without_side_effects(self):
        evidence = _evidence()
        evidence["selector"]["observation"] = deepcopy(evidence["selector"]["observation"])
        for row in evidence["selector"]["observation"]["bars"]:
            row["c"] = "100"
            row["o"] = row["h"] = row["l"] = "100"
        evidence["selector"]["observation"]["normalized_payload_sha256"] = payload_sha256(
            evidence["selector"]["observation"], hash_field="normalized_payload_sha256")
        result = run_observed_shadow(evidence, as_of=NOW)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["reason_code"], "trend_equal")
        self.assertFalse(result["ledger_written"])


if __name__ == "__main__":
    unittest.main()
