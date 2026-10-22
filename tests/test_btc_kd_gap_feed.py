from __future__ import annotations

import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from neorome import btc_fast_stochastic_feed as fast_feed
from neorome import btc_fast_stochastic_paper as fast_paper
from neorome import btc_kd_gap_feed as gap_feed
from neorome import btc_stochastic_feed as slow_feed
from neorome import btc_stochastic_paper as slow_paper


class GapDecisionTests(unittest.TestCase):
    def signal(self, direction="bullish", k=25.0, d=20.0):
        return {"direction": direction, "k": k, "d": d}

    def test_gap_is_directional_and_inclusive_at_five(self):
        self.assertEqual(gap_feed.gap_decision(self.signal(k=25, d=20)), {
            "decision": "accept", "reason": None, "gap": 5, "max_gap": 5.0})
        self.assertEqual(gap_feed.gap_decision(self.signal("bearish", k=20, d=25))["gap"], 5)
        self.assertEqual(gap_feed.gap_decision(self.signal(k=25.0001, d=20))["reason"], "kd_gap_exceeded")
        self.assertEqual(gap_feed.gap_decision(self.signal(k=20, d=20))["reason"], "kd_gap_exceeded")

    def test_nonfinite_boolean_and_out_of_range_values_fail_closed(self):
        for bad in (float("nan"), float("inf"), True, -0.1, 100.1, "25"):
            with self.subTest(value=bad):
                decision = gap_feed.gap_decision(self.signal(k=bad, d=20))
                self.assertEqual(decision["decision"], "invalid")
                self.assertEqual(decision["reason"], "kd_gap_exceeded")
        self.assertTrue(math.isnan(float("nan")))

    def test_roots_and_experiment_identities_are_distinct(self):
        self.assertNotEqual(gap_feed.EXPERIMENTS["5m"]["strategy_id"], gap_feed.EXPERIMENTS["30m"]["strategy_id"])
        self.assertNotEqual(gap_feed.PRODUCTION_ROOTS["5m"], gap_feed.PRODUCTION_ROOTS["30m"])
        self.assertEqual(gap_feed.EXPERIMENTS["5m"]["starting_cash_cents"], 50_000)
        self.assertEqual(gap_feed.EXPERIMENTS["30m"]["exit_policy"], "hold_to_official_settlement")
        for timeframe, paper in (("5m", fast_paper), ("30m", slow_paper)):
            self.assertNotEqual(gap_feed.EXPERIMENTS[timeframe]["strategy_id"], paper.STRATEGY_ID)
            self.assertNotEqual(gap_feed.PRODUCTION_ROOTS[timeframe], paper.DEFAULT_STATE_ROOT)


class GapFeedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = (Path(self.tmp.name) / "paper").resolve()

    def tearDown(self):
        self.tmp.cleanup()

    def test_config_is_immutable_idempotent_and_blocks_unconfigured_state(self):
        gap_feed._configure_root(self.root, "5m")
        path = self.root / gap_feed.CONFIG_NAME
        first = path.read_text()
        gap_feed._configure_root(self.root, "5m")
        self.assertEqual(path.read_text(), first)
        with self.assertRaisesRegex(gap_feed.GapFeedError, "config_conflict"):
            gap_feed._configure_root(self.root, "30m")
        other = Path(self.tmp.name) / "unconfigured"
        other.mkdir()
        (other / "btc_fast_stochastic_paper.sqlite3").touch()
        with self.assertRaisesRegex(gap_feed.GapFeedError, "requires_experiment_config"):
            gap_feed._configure_root(other, "5m")

    def test_roots_reject_original_aliases_cross_timeframe_and_symlinks(self):
        with self.assertRaisesRegex(gap_feed.GapFeedError, "alias_or_protected"):
            gap_feed._root_for("5m", fast_paper.DEFAULT_STATE_ROOT)
        with self.assertRaisesRegex(gap_feed.GapFeedError, "production_state_root_must_match_timeframe"):
            gap_feed._root_for("5m", gap_feed.PRODUCTION_ROOTS["30m"])
        with self.assertRaisesRegex(gap_feed.GapFeedError, "other_experiment_or_descendant"):
            gap_feed._root_for("5m", gap_feed.PRODUCTION_ROOTS["30m"] / "nested")
        link = Path(self.tmp.name) / "root-link"
        link.symlink_to(self.root)
        with self.assertRaisesRegex(gap_feed.GapFeedError, "symlink_refused"):
            gap_feed._root_for("5m", link)

    def test_default_root_symlink_ancestor_symlink_and_protected_descendants_fail_closed(self):
        original = (Path(self.tmp.name) / "original").resolve()
        original.mkdir()
        production_link = Path(self.tmp.name) / "production-link"
        production_link.symlink_to(original)
        with patch.dict(gap_feed.PRODUCTION_ROOTS, {"5m": production_link}):
            with self.assertRaisesRegex(gap_feed.GapFeedError, "symlink_refused"):
                gap_feed._root_for("5m")

        real_parent = (Path(self.tmp.name) / "real-parent").resolve()
        real_parent.mkdir()
        parent_link = Path(self.tmp.name) / "parent-link"
        parent_link.symlink_to(real_parent)
        with self.assertRaisesRegex(gap_feed.GapFeedError, "symlink_refused"):
            gap_feed._root_for("5m", parent_link / "paper")

        for protected in (fast_paper.DEFAULT_STATE_ROOT, slow_paper.DEFAULT_STATE_ROOT,
                          Path("/var/lib/neorome-btc-brackets-v1")):
            with self.subTest(protected=protected):
                with self.assertRaisesRegex(gap_feed.GapFeedError, "protected_descendant|other_experiment"):
                    gap_feed._root_for("5m", protected / "nested")

    def test_identity_and_patches_restore_after_exception(self):
        before = (fast_paper.STRATEGY_ID, fast_paper.PROFILE_VERSION, fast_paper.STRATEGY_VERSION,
                  fast_feed.ROOT, fast_feed.tv_signal.evaluate, fast_paper.run_once, fast_paper._signal,
                  fast_feed.archive_cycle)
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with gap_feed._isolated_adapter("5m", self.root):
                self.assertEqual(fast_paper.STRATEGY_ID, gap_feed.EXPERIMENTS["5m"]["strategy_id"])
                raise RuntimeError("boom")
        after = (fast_paper.STRATEGY_ID, fast_paper.PROFILE_VERSION, fast_paper.STRATEGY_VERSION,
                 fast_feed.ROOT, fast_feed.tv_signal.evaluate, fast_paper.run_once, fast_paper._signal,
                 fast_feed.archive_cycle)
        self.assertEqual(after, before)

    def test_final_entry_gate_blocks_direct_bypass_and_allows_other_lifecycle_events(self):
        seen = []
        with patch.object(fast_paper, "run_once", side_effect=lambda value, *a, **k: seen.append(value) or {"status": "accepted"}):
            with gap_feed._isolated_adapter("5m", self.root) as (_feed, paper):
                from datetime import datetime

                now = datetime(2027, 1, 15, 8, 0, 30, tzinfo=gap_feed.UTC)
                signal = {"valid": True, "instrument": "BTC-USD", "timeframe_minutes": 5,
                          "stochastic_settings": [5, 3, 3], "method": "fresh_cross", "direction": "bullish",
                          "contract_side": "yes", "previous_k": 19, "previous_d": 20, "k": 25, "d": 20,
                          "oversold": 20, "bar_close_timestamp": int(now.timestamp()) // 300 * 300,
                          "observed_receipt_at": "2027-01-15T08:00:10+00:00", "source_hash": "a" * 64,
                          "explicit_source_bar_timestamp": True, "signal_id": "fresh-cross"}
                market = {"open_time": "2027-01-15T08:00:00+00:00", "close_time": "2027-01-15T08:15:00+00:00"}
                entry = {"signal": signal, "market": market}
                self.assertEqual(paper.run_once(entry, self.root, now)["status"], "accepted")
                with self.assertRaisesRegex(gap_feed.GapFeedError, "kd_gap_exceeded"):
                    paper.run_once({"signal": signal | {"k": 26}, "market": market}, self.root, now)
                with self.assertRaisesRegex(gap_feed.GapFeedError, "signal_strategy_identity_differs"):
                    paper.run_once({"signal": signal | {"strategy": "FOREIGN-STRATEGY"}, "market": market}, self.root, now)
                settlement = {"settlement": {"ticker": "KXBTC15M-X", "result": "yes"}}
                self.assertEqual(paper.run_once(settlement, self.root)["status"], "accepted")
        self.assertEqual(seen[0]["signal"]["strategy"], gap_feed.EXPERIMENTS["5m"]["strategy_id"])
        self.assertIn("settlement", seen[1])

    def test_evaluator_abstains_over_gap_and_keeps_gate_metadata_for_entry(self):
        selected = {"direction": "bullish", "k": 26, "d": 20}
        evaluator = gap_feed._gate_evaluator(lambda *_a, **_k: {"selected_signal": selected, "status": "checked"})
        rejected = evaluator(None, None, None)
        self.assertIsNone(rejected["selected_signal"])
        self.assertEqual(rejected["status"], "abstain")
        self.assertEqual(rejected["reason"], "kd_gap_exceeded")
        accepted = gap_feed._gate_evaluator(lambda *_a, **_k: {
            "selected_signal": selected | {"k": 25}, "status": "checked"})(None, None, None)
        self.assertEqual(accepted["selected_signal"]["kd_gap_gate"]["gap"], 5)

    def test_initialize_makes_snapshot_without_provider_calls_and_restart_is_safe(self):
        with patch.object(fast_feed.PublicData, "capture", side_effect=AssertionError("provider call")):
            first = gap_feed.initialize("5m", self.root)
            second = gap_feed.initialize("5m", self.root)
        self.assertEqual(first["strategy_id"], gap_feed.EXPERIMENTS["5m"]["strategy_id"])
        self.assertEqual(second["experiment"]["k_d_gap_max"], 5.0)
        self.assertEqual(json.loads((self.root / gap_feed.CONFIG_NAME).read_text()), gap_feed.EXPERIMENTS["5m"])

    def test_real_entry_mark_settlement_restart_persists_identity_and_gate(self):
        from tests.test_btc_fast_stochastic_paper import at as fast_at, evidence as fast_evidence, make_book as fast_book
        from tests.test_btc_stochastic_paper import at as slow_at, make_book as slow_book, make_input as slow_input

        fixtures = {
            "5m": (fast_evidence(0), fast_at, fast_book),
            "30m": (slow_input(), slow_at, slow_book),
        }
        original_ids = {"5m": fast_paper.STRATEGY_ID, "30m": slow_paper.STRATEGY_ID}
        for timeframe, (entry, at, make_book) in fixtures.items():
            with self.subTest(timeframe=timeframe):
                root = (Path(self.tmp.name) / timeframe).resolve()
                gap_feed.initialize(timeframe, root)
                feed, paper = gap_feed._adapter(timeframe)
                with gap_feed._isolated_adapter(timeframe, root):
                    filled = paper.run_once(entry, root, at(10))
                    self.assertEqual(filled["status"], "filled")
                    self.assertEqual(paper.snapshot(root, at(10))["strategy_id"],
                                     gap_feed.EXPERIMENTS[timeframe]["strategy_id"])
                    mark = {"mark_book": make_book(12, source="lifecycle-mark") | {"ticker": entry["market"]["ticker"]}}
                    self.assertEqual(paper.run_once(mark, root, at(12))["status"], "open")
                    settlement = {"settlement": {"ticker": entry["market"]["ticker"], "status": "settled",
                                  "result": "yes", "observed_at": at(902), "source_hash": "b" * 64}}
                    self.assertEqual(paper.run_once(settlement, root, at(905))["status"], "settled")
                    intent = next(event for event in paper.snapshot(root, at(906))["audit_events"]
                                  if event["event_type"] == "intent")
                    self.assertEqual(intent["payload"]["signal"]["kd_gap_gate"], {
                        "decision": "accept", "reason": None, "gap": 0.5, "max_gap": 5.0})
                    reserve = next(event for event in paper.initialize(root).audit()
                                   if event.event_type == "reserve")
                    proposal = reserve.payload["payload"]
                    self.assertEqual(proposal["strategy"], gap_feed.EXPERIMENTS[timeframe]["strategy_id"])
                    self.assertEqual(proposal["strategy_version"], gap_feed.EXPERIMENTS[timeframe]["strategy_version"])
                    marker = next(iter(proposal["metadata"].values()))
                    self.assertEqual(marker["signal"]["kd_gap_gate"]["max_gap"], 5.0)
                reopened = gap_feed.initialize(timeframe, root)
                self.assertEqual(reopened["strategy_id"], gap_feed.EXPERIMENTS[timeframe]["strategy_id"])
                self.assertIsNone(reopened["active_position"])
                self.assertEqual(paper.STRATEGY_ID, original_ids[timeframe])

    def test_fresh_crossover_validation_remains_in_original_admission(self):
        from datetime import datetime

        close = int(datetime(2027, 1, 15, 8, 0, tzinfo=gap_feed.UTC).timestamp())
        signal = {"valid": True, "instrument": "BTC-USD", "timeframe_minutes": 5,
                  "stochastic_settings": [5, 3, 3], "method": "fresh_cross",
                  "direction": "bullish", "contract_side": "yes", "previous_k": 19, "previous_d": 20,
                  "k": 21, "d": 20, "oversold": 20, "bar_close_timestamp": close,
                  "observed_receipt_at": "2027-01-15T08:00:10+00:00", "source_hash": "a" * 64,
                  "explicit_source_bar_timestamp": True, "signal_id": "fresh-cross"}
        market = {"open_time": "2027-01-15T08:00:00+00:00", "close_time": "2027-01-15T08:15:00+00:00"}
        now = datetime(2027, 1, 15, 8, 0, 30, tzinfo=gap_feed.UTC)
        with gap_feed._isolated_adapter("5m", self.root):
            parsed = fast_paper._signal(signal, market, now)
            self.assertEqual(parsed["k"] - parsed["d"], 1)
            invalid_cross = signal | {"previous_d": 18}
            with self.assertRaisesRegex(fast_paper.BTCStochasticPaperError, "fresh K/D crossover"):
                fast_paper._signal(invalid_cross, market, now)


if __name__ == "__main__":
    unittest.main()
