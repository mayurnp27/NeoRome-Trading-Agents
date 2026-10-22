import unittest

from neorome.btc_hourly98_signal import (
    _zone_exit_direction,
    evaluate,
    stochastic_98_3_3,
)


def bars(count, *, timeframe=15, start=1_800_000_000):
    seconds = timeframe * 60
    result = []
    for i in range(count):
        close = 100 + ((i * 17) % 43) - ((i * 7) % 19)
        result.append({
            "timestamp": start + i * seconds,
            "bar_close_timestamp": start + (i + 1) * seconds,
            "open": close,
            "high": close + 2 + (i % 4),
            "low": close - 3 - (i % 5),
            "close": close + (i % 3) - 1,
        })
    return result


def reference_stoch9833(candles):
    raw = []
    for i, row in enumerate(candles):
        if i < 97:
            raw.append(None)
            continue
        window = candles[i - 97 : i + 1]
        lo = min(item["low"] for item in window)
        hi = max(item["high"] for item in window)
        raw.append(None if hi == lo else 100 * (row["close"] - lo) / (hi - lo))
    slow_k = [None if i < 2 or any(v is None for v in raw[i - 2 : i + 1]) else sum(raw[i - 2 : i + 1]) / 3 for i in range(len(raw))]
    slow_d = [None if i < 2 or any(v is None for v in slow_k[i - 2 : i + 1]) else sum(slow_k[i - 2 : i + 1]) / 3 for i in range(len(slow_k))]
    return slow_k[-1], slow_d[-1]


class BtcHourly98SignalTests(unittest.TestCase):
    def test_exact_98_3_3_math_matches_independent_reference(self):
        candles = bars(120)
        actual = stochastic_98_3_3(candles)
        expected = reference_stoch9833(candles)
        self.assertAlmostEqual(actual[0], expected[0], places=12)
        self.assertAlmostEqual(actual[1], expected[1], places=12)

    def test_fresh_bullish_and_bearish_zone_exits(self):
        self.assertEqual(
            _zone_exit_direction([12, 10, 15, 21], [13, 12, 14, 20.5]),
            ("yes", "bullish_zone_exit_confirmed"),
        )
        self.assertEqual(
            _zone_exit_direction([90, 90, 84, 78], [85, 91, 85, 79]),
            ("no", "bearish_zone_exit_confirmed"),
        )

    def test_equality_consumption_and_cross_must_occur_during_setup(self):
        # Equality remains in the armed zone and is not a strict K/D cross.
        self.assertEqual(_zone_exit_direction([10, 12, 20, 21], [11, 12, 20, 22])[0], None)
        # A K/D cross after both lines leave the zone cannot revive a consumed setup.
        self.assertEqual(_zone_exit_direction([10, 12, 25, 27], [11, 13, 26, 25])[0], None)
        # The first exit consumes an uncrossed setup even if a later cross occurs.
        self.assertEqual(_zone_exit_direction([10, 12, 21, 25, 27], [11, 13, 22, 26, 25])[0], None)

    def test_undefined_resets_state_and_flat_stochastics_are_undefined(self):
        self.assertEqual(
            _zone_exit_direction([12, 10, None, 21], [13, 12, None, 20.5]),
            (None, "no_zone_exit"),
        )
        flat = bars(120)
        for row in flat:
            row["low"] = row["high"] = row["close"] = row["open"] = 100
        self.assertEqual(stochastic_98_3_3(flat), (None, None))

    def test_native_timeframes_forming_bars_and_validation(self):
        for timeframe in (15, 30):
            candles = bars(110, timeframe=timeframe)
            target = candles[-1]["bar_close_timestamp"]
            forming = bars(1, timeframe=timeframe, start=target)[0]
            result = evaluate(candles + [forming], observed_at=target + 30, target_close=target, timeframe_minutes=timeframe)
            self.assertEqual(result["source_bar_timestamp"], candles[-1]["timestamp"])
            self.assertEqual(result["source_bar_close_timestamp"], target)
            self.assertEqual(result["parameters"], {"k_period": 98, "smooth_k": 3, "smooth_d": 3})

            gap = candles[:30] + candles[31:]
            self.assertIn("bar_gap", evaluate(gap, observed_at=target + 1, target_close=target, timeframe_minutes=timeframe)["reasons"])
            self.assertIn("future_timestamp", evaluate(candles + [bars(1, timeframe=timeframe, start=target + 2 * timeframe * 60)[0]], observed_at=target + 1, target_close=target, timeframe_minutes=timeframe)["reasons"])
            self.assertIn("stale_observation", evaluate(candles, observed_at=target + 91, target_close=target, timeframe_minutes=timeframe)["reasons"])

    def test_warmup_and_output_reason(self):
        candles = bars(102)
        target = candles[-1]["bar_close_timestamp"]
        result = evaluate(candles, observed_at=target, target_close=target)
        self.assertIn("insufficient_bars", result["reasons"])
        self.assertIn(result["zone_exit_reason"], {"no_zone_exit", "stochastic_undefined"})


if __name__ == "__main__":
    unittest.main()
