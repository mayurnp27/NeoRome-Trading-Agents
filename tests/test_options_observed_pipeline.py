from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import json
import tempfile
import unittest

from neorome.options_cloud_evidence import _sha, collect_cloud_evidence
from neorome.options_dated_artifacts import resolve_current
from neorome.options_internal_paper import SHARED_BALANCES, run_once
from neorome.options_observed_pipeline import produce_observed_candidate, save_candidate_attempt
from neorome.paper_ledger import PaperLedger
from tests.test_options_cloud_evidence import FakeProvider, LargeCatalogProvider, NOW, _quote
from tests.test_options_shadow_rehearsal import _evidence_fixture


class OptionsObservedPipelineTests(unittest.TestCase):
    def _capture(self):
        class ExecutableProvider(FakeProvider):
            def __call__(self, tool, args):
                if tool == "get_option_quotes":
                    self.calls.append((tool, dict(args)))
                    results = []
                    for identifier in args["instrument_ids"]:
                        quote = _quote(identifier, "0.50" if identifier.endswith("1") else "0.25")
                        if identifier.endswith("1"):
                            quote.update(bid_price="0.30", ask_price="0.35")
                        if identifier.endswith("2"):
                            quote.update(bid_price="0.25", ask_price="0.30")
                        results.append({"quote": quote})
                    return {"results": results}
                return super().__call__(tool, args)
        return collect_cloud_evidence(ExecutableProvider(), symbol="SPY", right="call", as_of=NOW, clock=lambda: NOW)

    @staticmethod
    def _rehash(capture):
        capture["holdings"]["sha256"] = _sha({key: value for key, value in capture["holdings"].items() if key != "sha256"})
        capture["artifact_sha256"] = _sha({key: value for key, value in capture.items() if key != "artifact_sha256"})
        return capture

    def test_complete_capture_flows_to_observed_candidate_and_shared_reservation(self):
        capture = self._capture()
        observation, context, _rows = _evidence_fixture()
        # The capture is the source of option identities, quotes, receipt, and
        # directional catalog; the supplied context contains only calendar and
        # market-session evidence.
        decision = produce_observed_candidate(capture, observation, context)
        self.assertEqual(decision["status"], "eligible_shadow_candidate")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            PaperLedger(root / "paper.sqlite3", balances=SHARED_BALANCES, enforce_market_risk=True,
                        clock=lambda: NOW)
            result = run_once(candidate=decision, state_dir=root / "state", ledger_path=root / "paper.sqlite3", now=NOW)
            self.assertEqual(result["status"], "reserved")
            self.assertEqual(result["orders_sent"], 0)

    def test_100_row_cloud_capture_reaches_the_frozen_selector(self):
        class ExecutableLargeProvider(LargeCatalogProvider):
            def __call__(self, tool, args):
                if tool == "get_option_quotes":
                    self.calls.append((tool, dict(args)))
                    rows = []
                    for identifier in args["instrument_ids"]:
                        quote = _quote(identifier, "0.50" if identifier == "opt-0" else "0.25")
                        if identifier == "opt-0":
                            quote.update(bid_price="0.30", ask_price="0.35")
                        else:
                            quote.update(bid_price="0.25", ask_price="0.30")
                        rows.append({"quote": quote})
                    return {"results": rows}
                return super().__call__(tool, args)
        capture = collect_cloud_evidence(ExecutableLargeProvider(), symbol="SPY", right="call", as_of=NOW,
                                         clock=lambda: NOW)
        self.assertEqual(len(capture["normalized_quotes"]), 100)
        observation, context, _rows = _evidence_fixture()
        decision = produce_observed_candidate(capture, observation, context)
        self.assertEqual(decision["status"], "eligible_shadow_candidate")

    def test_unrelated_account_positions_do_not_enter_paper_risk(self):
        capture = self._capture()
        capture["holdings"]["option_holdings"] = {"PERSONAL-UNRELATED-OPTION": 7}
        self._rehash(capture)
        observation, context, _rows = _evidence_fixture()
        decision = produce_observed_candidate(capture, observation, context)
        self.assertEqual(decision["status"], "eligible_shadow_candidate")
        self.assertEqual(decision["evidence"]["unrelated_holding_count"], 1)

    def test_selected_leg_position_and_ambiguous_open_order_abstain(self):
        observation, context, _rows = _evidence_fixture()
        base = self._capture()
        eligible = produce_observed_candidate(base, observation, context)
        selected = eligible["shadow_candidate"]["long"]["symbol"]
        conflicted = deepcopy(base)
        conflicted["holdings"]["option_holdings"] = {selected: 1}
        decision = produce_observed_candidate(self._rehash(conflicted), observation, context)
        self.assertEqual(decision["status"], "abstain")
        self.assertEqual(decision["reason_code"], "selected_leg_holding_conflict")
        ambiguous = deepcopy(base)
        ambiguous["holdings"]["open_orders"] = [{"id": "provider-order", "state": "queued"}]
        decision = produce_observed_candidate(self._rehash(ambiguous), observation, context)
        self.assertEqual(decision["status"], "abstain")
        self.assertEqual(decision["reason_code"], "ambiguous_open_order")

    def test_same_day_candidate_attempts_are_immutable_and_latest_becomes_current(self):
        observation, context, _rows = _evidence_fixture()
        eligible_capture = self._capture()
        eligible = produce_observed_candidate(eligible_capture, observation, context)
        self.assertEqual(eligible["status"], "eligible_shadow_candidate")
        abstain_capture = deepcopy(eligible_capture)
        abstain_capture["as_of"] = "2026-09-22T14:00:01Z"
        abstain_capture["artifact_sha256"] = "a" * 64
        abstain = {"status": "abstain", "reason_code": "no_viable_pair", "orders_sent": 0}
        eligible_capture["as_of"] = "2026-09-22T14:00:02Z"
        eligible_capture["artifact_sha256"] = "b" * 64
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = save_candidate_attempt(root, "2026-09-22", abstain_capture, abstain)
            second = save_candidate_attempt(root, "2026-09-22", eligible_capture, eligible)
            self.assertNotEqual(first, second)
            self.assertTrue(first.name.startswith("candidate-20260922T140001"))
            self.assertTrue(second.name.startswith("candidate-20260922T140002"))
            self.assertEqual(json.loads(resolve_current(root, "candidate.json")["candidate.json"].read_text())["status"],
                             "eligible_shadow_candidate")

    def test_real_fixed_two_strike_probe_is_retained_as_an_abstention(self):
        probe = json.loads(Path("coordination/verification/desk-admission/options-fresh-20260923.json").read_text())
        observation, context, _rows = _evidence_fixture()
        result = produce_observed_candidate(probe, observation, context)
        self.assertEqual(result["status"], "abstain")
        self.assertEqual(result["orders_sent"], 0)
        self.assertFalse(result.get("ledger_written", False))
