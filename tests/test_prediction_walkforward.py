from datetime import datetime, timedelta, timezone
import unittest

from neorome.prediction_walkforward import WalkForwardError, replay


UTC = timezone.utc
FEES = {"version": "fixed-stress-v1", "entry_fee_cents_per_contract": 1, "exit_fee_cents_per_contract": 1, "settlement_fee_cents_per_contract": 1}


def events(count=60):
    first = datetime(2026, 1, 1, tzinfo=UTC)
    return [{"event_id": f"event-{index}", "decision_at": (first + timedelta(days=index)).isoformat().replace("+00:00", "Z"),
             "probability_known_at": (first + timedelta(days=index) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
             "probability_lower_bound": "0.50", "probability": "0.50", "ask_cents": 40,
             "outcome": "YES" if index % 2 == 0 else "NO",
             "settlement_at": (first + timedelta(days=index, hours=1)).isoformat().replace("+00:00", "Z")} for index in range(count)]


class PredictionWalkForwardTests(unittest.TestCase):
    def test_chronological_splits_include_fees_and_holdout(self):
        result = replay(events(), fee_schedule=FEES)
        self.assertEqual(result["events"], 60)
        self.assertEqual(result["splits"]["development"]["events"], 30)
        self.assertEqual(result["splits"]["validation"]["events"], 10)
        self.assertEqual(result["splits"]["holdout"]["events"], 20)
        self.assertEqual(result["splits"]["holdout"]["net_pnl_cents"], 160)
        self.assertEqual(result["fee_schedule"]["version"], "fixed-stress-v1")
        self.assertFalse(result["profitability_validated"])

    def test_probability_known_after_decision_is_refused(self):
        sample = events()
        sample[0]["probability_known_at"] = "2026-01-01T00:01:00Z"
        with self.assertRaisesRegex(WalkForwardError, "known after decision"):
            replay(sample, fee_schedule=FEES)

    def test_out_of_order_events_are_refused(self):
        sample = events()
        sample[1]["decision_at"] = sample[0]["decision_at"]
        sample[1]["probability_known_at"] = "2025-12-31T23:59:00Z"
        with self.assertRaisesRegex(WalkForwardError, "strictly chronological"):
            replay(sample, fee_schedule=FEES)


if __name__ == "__main__":
    unittest.main()
