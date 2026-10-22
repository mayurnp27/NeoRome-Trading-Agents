import json
import math
import random
import unittest

from neorome.btc_distance_signal import evaluate


def candles(decision_epoch=1_800_000_900, count=500, constant=False):
    rows = []
    previous = 20_000.0
    rng = random.Random(0)
    for offset in range(count, 0, -1):
        epoch = decision_epoch - offset * 300
        close = 20_000.0 if constant else previous + rng.choice((-3, -2, -1, 1, 2, 3))
        rows.append([epoch, min(previous, close) - 1, max(previous, close) + 1, previous, close, 1.0])
        previous = close
    return rows


class BtcDistanceSignalTests(unittest.TestCase):
    def test_returns_json_serializable_signal_and_uses_no_future_rows(self):
        decision = 1_800_000_900
        rows = candles(decision)
        result = evaluate(rows, decision, 1.0)
        with_future = evaluate(rows + [[decision, 1, 1_000_000, 500_000, 1_000_000, 100]], decision, 1.0)
        self.assertTrue(result["valid"])
        self.assertGreaterEqual(result["distance"], 1)
        self.assertTrue(result["qualifies"])
        self.assertIn(result["signal"], (-1, 1))
        self.assertEqual(result, with_future)
        self.assertTrue(result["decision_time"].endswith("Z"))
        json.dumps(result, allow_nan=False)

    def test_distance_threshold_abstains_without_losing_valid_features(self):
        decision = 1_800_000_900
        rows = candles(decision)
        spot = rows[-1][4]
        result = evaluate(rows, decision, spot)
        self.assertTrue(result["valid"])
        self.assertFalse(result["qualifies"])
        self.assertEqual(result["signal"], 0)
        self.assertEqual(result["distance"], 0)
        self.assertEqual(result["reason"], "distance_below_threshold")

    def test_missing_five_minute_close_invalidates_sigma_history(self):
        decision = 1_800_000_900
        rows = candles(decision)
        del rows[-30]
        result = evaluate(rows, decision, 10_000)
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "insufficient_or_invalid_sigma_history")
        self.assertEqual(result["signal"], 0)

    def test_zero_return_volatility_is_invalid(self):
        decision = 1_800_000_900
        result = evaluate(candles(decision, constant=True), decision, 10_000)
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "insufficient_or_invalid_sigma_history")

    def test_duplicate_and_unaligned_rows_fail_closed(self):
        decision = 1_800_000_900
        rows = candles(decision)
        duplicate = evaluate(rows + [rows[0]], decision, 10_000)
        unaligned = evaluate(rows + [[decision - 301, 1, 2, 1, 2, 1]], decision, 10_000)
        self.assertEqual(duplicate["reason"], "duplicate_candle")
        self.assertEqual(unaligned["reason"], "unaligned_candle")

    def test_invalid_strike_and_decision_are_serializable(self):
        invalid_strike = evaluate([], 1_800_000_900, math.nan)
        invalid_decision = evaluate([], "bad", 20_000)
        self.assertEqual(invalid_strike["reason"], "invalid_strike")
        self.assertEqual(invalid_decision["reason"], "invalid_decision_epoch")
        json.dumps([invalid_strike, invalid_decision], allow_nan=False)


if __name__ == "__main__":
    unittest.main()
