import unittest
from unittest.mock import patch

from neorome.btc_sma1m_signal import _zone_exit_direction, evaluate, stochastic_5_3_3


def bars(closes, *, lows=None, highs=None, start=1_800_000_000):
    lows = lows or [max(0.01, close - 1) for close in closes]
    highs = highs or [close + 1 for close in closes]
    return [
        {
            "timestamp": start + i * 60,
            "bar_close_timestamp": start + (i + 1) * 60,
            "open": close,
            "high": max(highs[i], close),
            "low": min(lows[i], close),
            "close": close,
        }
        for i, close in enumerate(closes)
    ]


class BtcSma1mSignalTests(unittest.TestCase):
    def evaluate_at(self, values, **kwargs):
        candles = bars(values)
        target = candles[-1]["bar_close_timestamp"]
        return evaluate(candles, observed_at=target + 10, target_close=target, **kwargs)

    def test_strict_up_and_down_crosses(self):
        up = self.evaluate_at([5, 5, 5, 5, 5, 6])
        self.assertEqual(up["direction"], "yes")
        self.assertTrue(up["pure_qualifies"])
        down = self.evaluate_at([6, 6, 6, 6, 6, 5])
        self.assertEqual(down["direction"], "no")
        self.assertTrue(down["pure_qualifies"])

    def test_equality_is_only_a_setup_and_held_side_does_not_repeat(self):
        equality = self.evaluate_at([5, 5, 5, 5, 5, 5])
        self.assertFalse(equality["pure_qualifies"])
        self.assertIsNone(equality["direction"])
        held = self.evaluate_at([5, 5, 5, 5, 6, 7])
        self.assertFalse(held["pure_qualifies"])

    def test_forming_bar_is_ignored(self):
        candles = bars([5, 5, 5, 5, 5, 6])
        target = candles[-1]["bar_close_timestamp"]
        forming = bars([99], start=target)[0]
        result = evaluate(candles + [forming], observed_at=target + 10, target_close=target)
        self.assertEqual(result["direction"], "yes")

    def test_stale_gap_duplicate_nan_and_invalid_ohlc_are_rejected(self):
        candles = bars([5, 5, 5, 5, 5, 6])
        target = candles[-1]["bar_close_timestamp"]
        stale = evaluate(candles, observed_at=target + 31, target_close=target)
        self.assertIn("stale_observation", stale["reasons"])
        gap_bars = [*candles[:3], *candles[4:]]
        gap = evaluate(gap_bars, observed_at=target + 1, target_close=target)
        self.assertIn("bar_gap", gap["reasons"])
        duplicate = evaluate(candles + [dict(candles[-1])], observed_at=target + 1, target_close=target)
        self.assertIn("duplicate_timestamp", duplicate["reasons"])
        oversized = bars([5] * 300 + [6])
        oversized_target = oversized[-1]["bar_close_timestamp"]
        too_many = evaluate(oversized, observed_at=oversized_target + 1, target_close=oversized_target)
        self.assertIn("too_many_bars", too_many["reasons"])
        invalid = [dict(row) for row in candles]
        invalid[-1]["close"] = float("nan")
        self.assertIn("invalid_close", evaluate(invalid, observed_at=target + 1, target_close=target)["reasons"])
        invalid = [dict(row) for row in candles]
        invalid[-1]["low"] = invalid[-1]["high"] + 1
        self.assertIn("invalid_ohlc_range", evaluate(invalid, observed_at=target + 1, target_close=target)["reasons"])

    def test_stochastic_hand_calculation_and_flat_undefined(self):
        candles = bars([20] * 9, lows=[1] * 9, highs=[96] * 9)
        self.assertEqual(stochastic_5_3_3(candles), (20.0, 20.0))
        high = bars([80] * 9, lows=[1] * 9, highs=[99.75] * 9)
        self.assertEqual(stochastic_5_3_3(high), (80.0, 80.0))
        flat = bars([5] * 9, lows=[5] * 9, highs=[5] * 9)
        self.assertEqual(stochastic_5_3_3(flat), (None, None))
        # Undefined stochastic never removes the independent SMA signal.
        cross = bars([5, 5, 5, 5, 5, 6], lows=[5] * 6, highs=[5] * 6)
        target = cross[-1]["bar_close_timestamp"]
        result = evaluate(cross, observed_at=target + 1, target_close=target)
        self.assertTrue(result["pure_qualifies"])
        self.assertFalse(result["confirmed_qualifies"])
        self.assertIn("stochastic_undefined", result["reasons"])

    def test_zone_exit_uses_kd_cross_since_arm_and_confirms_only_on_latest_bar(self):
        # K crosses D while both are still oversold, then both leave the zone.
        k = [12, 10, 15, 21]
        d = [13, 12, 14, 20.5]
        self.assertEqual(_zone_exit_direction(k, d), "yes")
        # The first both-above-20 bar consumes the setup. A later cross outside
        # the zone cannot enter retroactively.
        self.assertIsNone(_zone_exit_direction([10, 12, 25, 27], [11, 13, 26, 25]))
        # Leaving without a K/D cross is not an entry, and it consumes the arm.
        no_cross = _zone_exit_direction([10, 12, 21, 23], [11, 13, 22, 24])
        self.assertIsNone(no_cross)
        # The prior bar has both values out of the zone; this confirms once,
        # and adding a later bar makes the latest-only output empty again.
        confirmed_k = [12, 10, 15, 21]
        confirmed_d = [13, 12, 14, 20.5]
        self.assertEqual(_zone_exit_direction(confirmed_k, confirmed_d), "yes")
        self.assertIsNone(_zone_exit_direction(confirmed_k + [22], confirmed_d + [21]))

    def test_bear_zone_exit_and_undefined_values_reset_pending_state(self):
        self.assertEqual(_zone_exit_direction([90, 90, 84, 78], [85, 91, 85, 79]), "no")
        # An undefined candle clears an armed/crossed setup.
        reset = _zone_exit_direction([10, 15, None, 21], [12, 14, None, 20])
        self.assertIsNone(reset)

    def test_sma_output_is_independent_and_stochastic_fields_are_explicit(self):
        candles = bars([5] * 8 + [6], lows=[5] * 9, highs=[100] * 9)
        target = candles[-1]["bar_close_timestamp"]
        result = evaluate(candles, observed_at=target + 1, target_close=target)
        self.assertEqual(result["direction"], "yes")
        self.assertEqual(result["sma_bias_direction"], "yes")
        self.assertIsNone(result["zone_exit_direction"])
        self.assertTrue(result["pure_qualifies"])
        self.assertFalse(result["confirmed_qualifies"])

    def test_zone_exit_confirmation_matches_sma_bias_and_supports_fresh_option(self):
        # Current SMA bias is up, but the crossover happened before this bar.
        candles = bars([5, 5, 5, 5, 6, 7, 8, 9, 10], lows=[1] * 9, highs=[100] * 9)
        target = candles[-1]["bar_close_timestamp"]
        with patch("neorome.btc_sma1m_signal._zone_exit_direction", return_value="yes"):
            result = evaluate(candles, observed_at=target + 1, target_close=target)
            strict = evaluate(
                candles,
                observed_at=target + 1,
                target_close=target,
                fresh_sma_confirmation=True,
            )
        self.assertEqual(result["sma_bias_direction"], "yes")
        self.assertIsNone(result["direction"])
        self.assertTrue(result["confirmed_qualifies"])
        self.assertFalse(strict["confirmed_qualifies"])


if __name__ == "__main__":
    unittest.main()
