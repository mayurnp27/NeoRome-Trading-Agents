from datetime import datetime, timedelta, timezone
import unittest

from neorome.prediction_market_baseline import BaselineError, build_dataset


UTC = timezone.utc


def records(count=50):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [{"event_id": f"e-{index}", "family": "KXBTC15M", "market_id": f"m-{index}", "terms_sha256": "a" * 64,
             "decision_at": (start + timedelta(days=index)).isoformat().replace("+00:00", "Z"),
             "known_at": (start + timedelta(days=index) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
             "settlement_at": (start + timedelta(days=index, hours=1)).isoformat().replace("+00:00", "Z"),
             "settlement_finalized": True, "settlement_source_hash": f"{index + 100:064x}",
             "yes_bid_cents": 40, "yes_ask_cents": 42, "result": "YES" if index % 2 == 0 else "NO", "source_hash": f"{index:064x}"}
            for index in range(count)]


class PredictionMarketBaselineTests(unittest.TestCase):
    def test_midpoint_dataset_is_calibration_reference_not_alpha(self):
        result = build_dataset(records(), family="KXBTC15M", source_identity={"venue": "Kalshi", "source": "CF Benchmarks"})
        self.assertTrue(result["calibration_eligible"])
        self.assertFalse(result["alpha_validated"])
        self.assertTrue(result["registration"]["not_alpha"])
        self.assertEqual(result["metrics"]["holdout"]["events"], 20)

    def test_known_after_decision_and_mixed_terms_refuse_dataset(self):
        sample = records()
        sample[0]["settlement_finalized"] = False
        with self.assertRaisesRegex(BaselineError, "not finalized"):
            build_dataset(sample, family="KXBTC15M", source_identity={"venue": "Kalshi"})
        sample = records()
        sample[0]["known_at"] = "2026-01-01T00:01:00Z"
        with self.assertRaisesRegex(BaselineError, "known after decision"):
            build_dataset(sample, family="KXBTC15M", source_identity={"venue": "Kalshi"})
        sample = records()
        sample[1]["terms_sha256"] = "b" * 64
        with self.assertRaisesRegex(BaselineError, "one exact terms"):
            build_dataset(sample, family="KXBTC15M", source_identity={"venue": "Kalshi"})


if __name__ == "__main__":
    unittest.main()
