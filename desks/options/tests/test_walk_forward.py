import json
import tempfile
import unittest
from pathlib import Path

from desks.options.fixtures.walk_forward_fixture import dataset
from desks.options.walk_forward import (
    CostModel,
    ReplayError,
    register_trial,
    run_walk_forward,
    sha256_json,
    write_acceptance_artifact,
)


class OptionsWalkForwardTests(unittest.TestCase):
    def test_synthetic_fixture_requires_explicit_opt_in_and_reports_blocker(self):
        result = run_walk_forward(dataset())
        self.assertEqual(result["status"], "data_blocked")
        self.assertFalse(result["admissible_for_paper"])
        self.assertTrue(any("synthetic fixture" in item for item in result["data_admission"]["blockers"]))

    def test_indicative_nested_quote_blocks_historical_admission(self):
        data = dataset()
        data["data_admission"] = {"source_kind": "historical_executable", "provider": "example", "entitlement_confirmed": True, "quotes_complete": True, "provider_timestamps": True}
        data["rows"][200]["entry_quotes"]["long"]["source_kind"] = "indicative"
        result = run_walk_forward(data)
        self.assertEqual(result["status"], "data_blocked")
        self.assertTrue(any("not historical executable" in item for item in result["data_admission"]["blockers"]))

    def test_fixture_replay_uses_train_validation_holdout_and_executable_sides(self):
        result = run_walk_forward(dataset(), allow_synthetic=True)
        self.assertEqual(result["status"], "research_only")
        self.assertFalse(result["admissible_for_paper"])
        self.assertEqual(len(result["windows"]), 1)
        validation = result["windows"][0]["splits"]["validation"]["metrics"]
        self.assertEqual(validation["filled_trades"], 1)
        self.assertEqual(validation["no_fill_count"], 0)
        self.assertEqual(validation["net_pnl"], "1.00")
        self.assertEqual(validation["benchmark_pnl"], "-0.59940040")
        holdout = result["windows"][0]["splits"]["holdout"]["metrics"]
        self.assertTrue(result["final_untouched_holdout"]["locked"])
        self.assertEqual(holdout["filled_trades"], 0)
        self.assertIn("full_walk_forward", result["revalidation"])

    def test_each_leg_capacity_classifies_partial_fill_and_no_fill(self):
        data = dataset()
        row = data["rows"][200]
        row["entry_quotes"]["short"]["bid_size"] = "0.5"
        result = run_walk_forward(data, allow_synthetic=True)
        validation = result["windows"][0]["splits"]["validation"]["metrics"]
        self.assertEqual(validation["partial_fill_count"], 1)
        self.assertEqual(validation["filled_trades"], 0)

        data = dataset()
        row = data["rows"][200]
        row["entry_quotes"]["long"]["ask_size"] = "0"
        # A zero displayed size is invalid evidence and is recorded as no fill.
        result = run_walk_forward(data, allow_synthetic=True)
        validation = result["windows"][0]["splits"]["validation"]["metrics"]
        self.assertGreaterEqual(validation["no_fill_count"], 1)

    def test_stress_is_deterministic_and_artifact_is_immutable(self):
        data = dataset()
        first = run_walk_forward(data, allow_synthetic=True, cost_model=CostModel(slippage_bps="25"))
        second = run_walk_forward(data, allow_synthetic=True, cost_model=CostModel(slippage_bps="25"))
        self.assertEqual(first, second)
        with tempfile.TemporaryDirectory() as directory:
            artifact = write_acceptance_artifact(Path(directory) / "acceptance.json", first)
            self.assertEqual(artifact["artifact_sha256"], sha256_json({key: value for key, value in artifact.items() if key != "artifact_sha256"}))
            with self.assertRaises(ReplayError):
                write_acceptance_artifact(Path(directory) / "acceptance.json", first)

    def test_trial_registry_is_idempotent_and_rejects_conflicts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trials.json"
            trial = {"trial_id": "OPT-001-fixture-1", "strategy_version": "OPT-001-v1", "data_manifest_sha256": "a" * 64, "cost_model": {"slippage_bps": "0"}, "split_config": {"holdout": 30}}
            register_trial(path, trial)
            register_trial(path, trial)
            self.assertEqual(len(json.loads(path.read_text())["trials"]), 1)
            conflict = dict(trial, cost_model={"slippage_bps": "50"})
            with self.assertRaises(ReplayError):
                register_trial(path, conflict)


if __name__ == "__main__":
    unittest.main()
