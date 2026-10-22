import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from neorome.jupiter_quotes import SOL, USDC
from neorome.token_outcomes import OutcomeError, evaluate


T0 = "2026-09-23T00:00:00Z"


def route(start, end, incoming, outgoing, amount, output, minimum, *, impact="0.1", digest="a" * 64):
    return {
        "requested_at": start,
        "received_at": end,
        "response_sha256": digest,
        "quote": {
            "inputMint": incoming,
            "outputMint": outgoing,
            "inAmount": str(amount),
            "outAmount": str(output),
            "otherAmountThreshold": str(minimum),
            "priceImpact": impact,
            "priceImpactUnit": "percentage_points",
        },
    }


def discovery(*, rejected=False, disappeared=False, entry=True):
    row = {
        "chain": "solana",
        "mint": SOL,
        "known_at": T0,
        "received_at": T0,
        "research_status": "observed_pending_risk_review",
        "screen_rejections": ["pool_too_young"] if rejected else [],
    }
    if disappeared:
        row["research_status"] = "disappeared"
    if entry:
        row["quotes"] = {"observations": [route(
            "2026-09-23T00:00:01Z", "2026-09-23T00:00:02Z", USDC, SOL,
            5_000_000, 1000, 900)]}
    return {"schema_version": 1, "observed_at": T0, "candidates": [row]}


def sell(at_minutes=15, *, amount=900, output=5_500_000, minimum=5_400_000,
         received_extra=2, **changes):
    target = datetime(2026, 9, 23, tzinfo=timezone.utc) + timedelta(minutes=at_minutes)
    requested = target.isoformat().replace("+00:00", "Z")
    received = (target + timedelta(seconds=received_extra)).isoformat().replace("+00:00", "Z")
    value = route(requested, received,
                  SOL, USDC, amount, output, minimum)
    value.update(changes)
    return value


class TokenOutcomeTests(unittest.TestCase):
    def test_minimum_output_costs_and_full_loss_are_scenarios(self):
        result = evaluate(discovery(), [sell()], costs={
            "version": "fees-v1", "excludes_embedded_route_fees": True,
            "entry_extra_usdc_raw": "100", "exit_extra_usdc_raw": "200"})
        row = result["cohort"][0]
        outcome = row["horizons"][0]
        self.assertEqual(outcome["status"], "modeled_outcome")
        self.assertEqual(outcome["minimum_output_usdc_raw"], "5400000")
        self.assertEqual(outcome["net_pnl_usdc_raw"], "399700")
        self.assertEqual(outcome["full_loss_stress_usdc_raw"], "-5000300")
        self.assertTrue(outcome["full_loss_stress_is_scenario"])
        self.assertFalse(result["execution_enabled"])
        self.assertEqual(result["trade_rows"], [])

    def test_no_lookahead_and_first_route_within_bounded_window(self):
        before = sell(received_extra=1)
        before["received_at"] = "2026-09-23T00:14:59Z"
        first = sell(received_extra=3, minimum=5_100_000)
        later = sell(received_extra=4, minimum=5_000_000)
        result = evaluate(discovery(), [before, later, first], horizon_tolerance_seconds=10)
        outcome = result["cohort"][0]["horizons"][0]
        self.assertEqual(outcome["status"], "modeled_outcome")
        self.assertEqual(outcome["minimum_output_usdc_raw"], "5100000")
        self.assertEqual(outcome["route_observation"]["received_at"], "2026-09-23T00:15:03Z")

    def test_rejections_and_disappeared_candidates_are_retained_no_trade(self):
        payload = discovery(rejected=True)
        payload["candidates"].append(discovery(disappeared=True)["candidates"][0])
        result = evaluate(payload, [sell()])
        self.assertEqual([row["status"] for row in result["cohort"]],
                         ["screen_rejected", "disappeared"])
        self.assertEqual(result["cohort"][0]["horizons"][0]["status"],
                         "not_evaluated_screen_rejected")
        self.assertFalse(any(row["trade_eligible"] for row in result["cohort"]))

    def test_entry_and_sell_quantity_mismatch_are_not_silently_repaired(self):
        bad_entry = discovery()
        bad_entry["candidates"][0]["quotes"]["observations"][0]["quote"].pop("otherAmountThreshold")
        row = evaluate(bad_entry, [sell()])["cohort"][0]
        self.assertEqual(row["status"], "entry_unresolved")

        wrong = sell(amount=901)
        row = evaluate(discovery(), [wrong])["cohort"][0]
        self.assertEqual(row["horizons"][0]["status"], "route_failure")
        self.assertIn("quantity", row["horizons"][0]["reasons"][0])

    def test_recorded_at_or_observed_at_cannot_supply_discovery_vintage(self):
        payload = discovery()
        candidate = payload["candidates"][0]
        candidate.pop("known_at")
        candidate.pop("received_at")
        candidate["observed_at"] = T0
        candidate["recorded_at"] = T0
        with self.assertRaises(OutcomeError):
            evaluate(payload, [])

    def test_missing_route_is_separate_from_route_failure(self):
        row = evaluate(discovery(), [sell(60)])["cohort"][0]
        self.assertEqual(row["horizons"][0]["status"], "unresolved_missing_route")
        self.assertEqual(row["horizons"][1]["status"], "modeled_outcome")

    def test_hash_impact_units_and_cost_fee_double_counting_fail_closed(self):
        for changes in ({"response_sha256": "bad"},
                        {"quote": {**sell()["quote"], "priceImpactUnit": "ratio"}},
                        {"quote": {**sell()["quote"], "priceImpact": "3"}}):
            observation = sell()
            observation.update(changes)
            row = evaluate(discovery(), [observation])["cohort"][0]
            self.assertEqual(row["horizons"][0]["status"], "route_failure")
        with self.assertRaises(OutcomeError):
            evaluate(discovery(), [sell()], costs={
                "version": "bad", "excludes_embedded_route_fees": True,
                "route_fee_usdc_raw": "1"})

    def test_immutable_cli_output_and_raw_input_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "input.json"
            output = Path(directory) / "outcomes.json"
            source.write_text(json.dumps({"discovery": discovery(), "sell_observations": [sell()]}))
            command = [sys.executable, "-m", "neorome.token_outcomes",
                       "--input", str(source), "--output", str(output)]
            subprocess.run(command, check=True, capture_output=True, text=True)
            result = json.loads(output.read_text())
            self.assertEqual(len(result["input_sha256"]), 64)
            with self.assertRaises(subprocess.CalledProcessError):
                subprocess.run(command, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
