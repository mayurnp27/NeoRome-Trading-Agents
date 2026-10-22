from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest

from neorome.options_cloud_evidence import collect_cloud_evidence
from neorome.options_dated_artifacts import read_current, resolve_current, save_stage
from neorome.options_internal_paper import SHARED_BALANCES, run_once
from neorome.options_observed_pipeline import produce_observed_candidate
from neorome.options_trend_inputs import TrendInputError, build_trend_inputs, current_direction_hint
from neorome.paper_ledger import PaperLedger
from tests.test_options_cloud_evidence import FakeProvider, _quote

UTC = timezone.utc
AS_OF = datetime(2026, 9, 23, 14, 0, tzinfo=UTC)
ARCHIVE = Path("coordination/verification/equity-intraday/cloud-equity-daily-20260923.json")


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _qqq_archive() -> dict:
    value = json.loads(ARCHIVE.read_text())
    # A retained-fixture twin gives the builder a complete independent archive
    # shape while preserving the source's raw-evidence chronology checks.
    value = json.loads(json.dumps(value).replace("SPY", "QQQ"))
    bundle = value["bundle"]
    import hashlib
    value["hashes"]["context_sha256"] = hashlib.sha256(_canonical(bundle["context"]).encode()).hexdigest()
    value["hashes"]["observation_sha256"] = hashlib.sha256(_canonical(bundle["observation"]).encode()).hexdigest()
    value["hashes"]["bundle_sha256"] = hashlib.sha256(_canonical(bundle).encode()).hexdigest()
    return value


def _bearish_spy_archive() -> dict:
    value = json.loads(ARCHIVE.read_text())
    bar = value["bundle"]["observation"]["bars"][-1]
    bar.update(o="1", h="1", l="1", c="1")
    import hashlib
    bundle = value["bundle"]
    value["hashes"]["observation_sha256"] = hashlib.sha256(_canonical(bundle["observation"]).encode()).hexdigest()
    value["hashes"]["bundle_sha256"] = hashlib.sha256(_canonical(bundle).encode()).hexdigest()
    return value


class OptionsTrendInputsTests(unittest.TestCase):
    def test_real_spy_only_archive_abstains_when_qqq_is_missing(self):
        spy = json.loads(ARCHIVE.read_text())
        with self.assertRaisesRegex(TrendInputError, "QQQ"):
            build_trend_inputs(spy, spy)

    def test_paired_archives_freeze_prior_200_sessions_and_date_pointer(self):
        observation, context = build_trend_inputs(json.loads(ARCHIVE.read_text()), _qqq_archive(), now=AS_OF)
        self.assertEqual(observation["symbol"], "SPY")
        self.assertEqual(len(observation["bars"]), 200)
        self.assertTrue(all(row["session_date"] < context["current_session"]["date"] for row in observation["bars"]))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            date = context["current_session"]["date"]
            save_stage(root, date, "trend-observation.json", observation)
            save_stage(root, date, "trend-context.json", context)
            # Exact replay is safe; a pointer is a regular JSON value, never a link.
            save_stage(root, date, "trend-observation.json", observation)
            self.assertFalse((root / "current.json").is_symlink())
            self.assertEqual(read_current(root)["date"], date)
            self.assertIn("trend-context.json", resolve_current(root, "trend-context.json"))

    def test_stale_archive_and_symlink_pointer_are_refused(self):
        spy, qqq = json.loads(ARCHIVE.read_text()), _qqq_archive()
        with self.assertRaisesRegex(TrendInputError, "stale"):
            build_trend_inputs(spy, qqq, now=AS_OF + timedelta(days=1))
        observation, context = build_trend_inputs(spy, qqq, now=AS_OF)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.symlink(root / "missing", root / "current.json")
            with self.assertRaisesRegex(Exception, "pointer"):
                save_stage(root, context["current_session"]["date"], "trend-observation.json", observation)

    def test_frozen_bullish_and_bearish_hints_select_cloud_right_without_relaxing_selector(self):
        cases = ((json.loads(ARCHIVE.read_text()), "call"), (_bearish_spy_archive(), "put"))
        for spy, expected in cases:
            observation, context = build_trend_inputs(spy, _qqq_archive(), now=AS_OF)
            self.assertEqual(context["direction_hint"]["right"], expected)
            with tempfile.TemporaryDirectory() as directory:
                root, date = Path(directory), context["current_session"]["date"]
                save_stage(root, date, "trend-observation.json", observation)
                save_stage(root, date, "trend-context.json", context)
                self.assertEqual(current_direction_hint(str(root))["right"], expected)
        unit = Path("deploy/recorder/neorome-options-cloud-evidence.service").read_text()
        self.assertIn("--trend-state-dir /var/lib/neorome-options-data/internal-paper", unit)
        self.assertNotIn("--right call", unit)

    def test_same_day_capture_attempts_are_immutable_and_later_success_becomes_current(self):
        from neorome.options_dated_artifacts import save_stage
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); date = "2026-09-23"
            abstain = {"status": "abstain", "as_of": "2026-09-23T14:00:00Z"}
            captured = {"status": "captured", "as_of": "2026-09-23T14:01:00Z"}
            save_stage(root, date, "capture-20260923T140000000000Z.json", abstain)
            save_stage(root, date, "capture-20260923T140100000000Z.json", captured, current_name="capture.json")
            self.assertTrue((root / date / "capture-20260923T140000000000Z.json").exists())
            self.assertEqual(json.loads(resolve_current(root, "capture.json")["capture.json"].read_text())["status"], "captured")

    def test_capture_through_trend_candidate_to_shared_reservation(self):
        class FreshProvider(FakeProvider):
            def __call__(self, tool, args):
                if tool == "get_option_quotes":
                    self.calls.append((tool, dict(args)))
                    rows = []
                    for identifier in args["instrument_ids"]:
                        quote = _quote(identifier, "0.50" if identifier.endswith("1") else "0.25")
                        quote["updated_at"] = (AS_OF - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
                        quote.update(bid_price="0.30" if identifier.endswith("1") else "0.25",
                                     ask_price="0.35" if identifier.endswith("1") else "0.30")
                        rows.append({"quote": quote})
                    return {"results": rows}
                return super().__call__(tool, args)
        capture = collect_cloud_evidence(FreshProvider(), symbol="SPY", right="call", as_of=AS_OF, clock=lambda: AS_OF)
        observation, context = build_trend_inputs(json.loads(ARCHIVE.read_text()), _qqq_archive(), now=AS_OF)
        decision = produce_observed_candidate(capture, observation, context)
        self.assertEqual(decision["status"], "eligible_shadow_candidate")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts, date = root / "artifacts", AS_OF.date().isoformat()
            save_stage(artifacts, date, "capture.json", capture)
            save_stage(artifacts, date, "trend-observation.json", observation)
            save_stage(artifacts, date, "trend-context.json", context)
            save_stage(artifacts, date, "candidate.json", decision)
            candidate_path = resolve_current(artifacts, "candidate.json")["candidate.json"]
            PaperLedger(root / "paper.sqlite3", balances=SHARED_BALANCES, enforce_market_risk=True, clock=lambda: AS_OF)
            result = run_once(candidate=json.loads(candidate_path.read_text()), state_dir=root / "state", ledger_path=root / "paper.sqlite3", now=AS_OF)
            self.assertEqual(result["status"], "reserved")
            self.assertEqual(result["orders_sent"], 0)
