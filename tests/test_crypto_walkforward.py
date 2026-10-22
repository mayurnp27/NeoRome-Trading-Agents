import unittest
from datetime import date, timedelta
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from desks.crypto.walkforward import build_report, walk_forward_liquid, walk_forward_tokens
from neorome.jupiter_quotes import SOL, USDC


def bars(count=180):
    start = date(2026, 1, 1)
    output = []
    for index in range(count):
        close = 100
        if index == 100:
            close = 130
        elif index == 101:
            close = 131
        elif index == 102:
            close = 90
        output.append({
            "timestamp": f"{(start + timedelta(days=index)).isoformat()}T00:00:00Z",
            "complete": True, "open": str(close - 1), "high": str(close + 1),
            "low": str(close - 2), "close": str(close),
        })
    output[101]["open"] = "77"
    output[101]["low"] = "76"
    output[103]["open"] = "88"
    output[103]["low"] = "87"
    return output


def route(start, end, incoming, outgoing, amount, output, minimum):
    return {"requested_at": start, "received_at": end, "response_sha256": "a" * 64,
            "quote": {"inputMint": incoming, "outputMint": outgoing,
                      "inAmount": str(amount), "outAmount": str(output),
                      "otherAmountThreshold": str(minimum), "priceImpact": "0.1",
                      "priceImpactUnit": "percentage_points"}}


class CryptoWalkForwardTests(unittest.TestCase):
    def test_liquid_report_is_versioned_causal_and_cost_stressed(self):
        first = walk_forward_liquid(bars())
        second = walk_forward_liquid(bars())
        self.assertEqual(first, second)
        self.assertEqual(first["trial_version"], "cr-001-walkforward-v1")
        self.assertEqual(first["cost_stress_bps_round_trip"], [10, 25, 50])
        self.assertEqual(first["split"]["phases"]["development"]["scenarios"]["round_trip_25bps"]["closed_trade_count"], 1)
        self.assertIn("venue_execution_evidence_missing", first["acceptance"]["blockers"])
        self.assertEqual(first["execution_enabled"], False)

    def test_token_report_retains_rejected_cohort_and_compares_x(self):
        discovery = {"schema_version": 1, "observed_at": "2026-09-23T00:00:00Z", "candidates": [{
            "chain": "solana", "mint": SOL, "known_at": "2026-09-23T00:00:00Z",
            "received_at": "2026-09-23T00:00:00Z",
            "screen_rejections": [], "x_features": {"mentions": 3,
            "unique_credible_authors": 2, "bot_post_fraction": "0", "rug_warning_post_ids": []},
            "quotes": {"observations": [route("2026-09-23T00:00:01Z", "2026-09-23T00:00:02Z",
                                                   USDC, SOL, 5_000_000, 1000, 900)]},
        }, {"chain": "solana", "mint": "9xQeWvG816bUx9EPf7B4wX4Jf4n9zQn7Yp8v8eW6x6Y",
            "known_at": "2026-09-23T00:01:00Z", "received_at": "2026-09-23T00:01:00Z",
            "screen_rejections": [],
            "quotes": {"observations": [route("2026-09-23T00:01:01Z", "2026-09-23T00:01:02Z",
                                                   USDC, "9xQeWvG816bUx9EPf7B4wX4Jf4n9zQn7Yp8v8eW6x6Y", 5_000_000, 1000, 900)]},
        }, {"chain": "solana", "mint": SOL, "known_at": "2026-09-23T00:02:00Z",
            "received_at": "2026-09-23T00:02:00Z",
            "screen_rejections": ["pool_too_young"]} ]}
        # Keep this test focused on the evaluator's immutable retention and X
        # grouping; duplicate SOL is intentionally rejected from the report's
        # eligible map by its key collision.
        result = walk_forward_tokens(discovery, [], minimum_candidates=1)
        self.assertEqual(result["trial_version"], "solana-memecoin-walkforward-v1")
        self.assertEqual(result["execution_enabled"], False)
        self.assertGreaterEqual(len(result["retained_outcomes"]), 2)
        self.assertIn("x_features_or_eligibility_missing", result["acceptance"]["blockers"])
        self.assertEqual(result["policy"]["horizons_minutes"], [15, 60, 240])

    def test_combined_report_and_cli_envelope_are_research_only(self):
        payload = {"bars": bars()}
        result = build_report(payload)
        self.assertIn("liquid", result)
        self.assertTrue(result["research_only"])
        self.assertFalse(result["execution_enabled"])
        self.assertEqual(len(result["liquid"]["trial_id"]), 64)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "trial.json"
            output = Path(directory) / "report.json"
            source.write_text(json.dumps(payload))
            subprocess.run([sys.executable, "-m", "desks.crypto.walkforward",
                            "--input", str(source), "--output", str(output)],
                           check=True, capture_output=True, text=True)
            cli_result = json.loads(output.read_text())
            self.assertEqual(cli_result["liquid"]["trial_id"], result["liquid"]["trial_id"])


if __name__ == "__main__":
    unittest.main()
