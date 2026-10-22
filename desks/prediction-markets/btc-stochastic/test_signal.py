import importlib.util
from pathlib import Path
import unittest

MODULE_PATH = Path(__file__).with_name("signal.py")
SPEC = importlib.util.spec_from_file_location("btc_stochastic_signal", MODULE_PATH)
signal = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(signal)


def make_candles(closes, start=0, step=900, low=1.0, high=101.0):
    return [
        {"start": start + i * step, "open": close, "high": high,
         "low": low, "close": close, "volume": 1.0}
        for i, close in enumerate(closes)
    ]


class SignalTests(unittest.TestCase):
    def test_hand_calculated_stochastic_turn_and_crossover(self):
        candles = make_candles([21.0] * 17 + [31.0])
        points = signal.stochastic(candles, 900)
        self.assertEqual(len(points), 1)
        self.assertAlmostEqual(points[0]["k"], 70.0 / 3.0)
        self.assertAlmostEqual(points[0]["d"], 190.0 / 9.0)

        # Build one preceding closed value so the last point emits both fixed
        # events: previous K is exactly 20, current K rises, and K crosses D.
        closes = [21.0] * 18 + [31.0]
        raw = make_candles(closes)
        # The 15m variant is sufficient to exercise event comparators directly.
        turn = signal.signals_for_timeframe(raw, now=19 * 900, timeframe_minutes=15, method="turn")
        cross = signal.signals_for_timeframe(raw, now=19 * 900, timeframe_minutes=15, method="cross")
        self.assertEqual(len(turn), 1)
        self.assertEqual(len(cross), 1)
        self.assertEqual(turn[0]["bar_close_timestamp"], 19 * 900)
        self.assertEqual(turn[0]["signal_id"], cross[0]["signal_id"].replace("cross", "turn"))

    def test_mirrored_bearish_turn_and_cross_emit_no_with_unique_ids(self):
        # Reflect the bullish fixture around 51 to produce K=80 then a decline.
        raw = make_candles([81.0] * 18 + [71.0])
        turn = signal.signals_for_timeframe(raw, now=19 * 900, timeframe_minutes=15,
                                            method="turn", direction="bearish")
        cross = signal.signals_for_timeframe(raw, now=19 * 900, timeframe_minutes=15,
                                             method="cross", direction="bearish")
        self.assertEqual(len(turn), 1)
        self.assertEqual(len(cross), 1)
        self.assertEqual(turn[0]["previous_k"], 80)
        self.assertLess(turn[0]["k"], turn[0]["previous_k"])
        self.assertEqual(turn[0]["direction"], "bearish")
        self.assertEqual(turn[0]["contract_side"], "no")
        self.assertEqual(turn[0]["overbought"], 80)
        self.assertIn("bearish", turn[0]["signal_id"])

        bullish = signal.signals_for_timeframe(
            make_candles([21.0] * 18 + [31.0]), now=19 * 900,
            timeframe_minutes=15, method="turn")
        signal.ensure_unique_signal_ids([bullish[0], turn[0]])

    def test_rejects_missing_duplicate_unsorted_and_invalid_values(self):
        valid = make_candles([10.0, 11.0, 12.0])
        missing = [valid[0], valid[2]]
        duplicate = [valid[0], valid[0]]
        unsorted = [valid[1], valid[0]]
        for rows in (missing, duplicate, unsorted):
            with self.subTest(rows=rows), self.assertRaises(signal.CandleValidationError):
                signal.validate_candles(rows, now=100_000)
        nan_rows = make_candles([10.0, 11.0])
        nan_rows[1]["close"] = float("nan")
        zero_rows = make_candles([10.0, 11.0])
        zero_rows[1]["low"] = 0.0
        for rows in (nan_rows, zero_rows):
            with self.subTest(rows=rows), self.assertRaises(signal.CandleValidationError):
                signal.validate_candles(rows, now=100_000)

    def test_flat_stochastic_range_abstains(self):
        rows = make_candles([10.0] * 24, low=10.0, high=10.0)
        self.assertEqual(signal.stochastic(rows), [])
        scan = signal.scan_signals(rows, now=24 * 900)
        self.assertTrue(all(not item["signals"] for item in scan["evaluations"]))

    def test_undefined_oscillator_gap_does_not_create_a_stale_turn(self):
        # The initial 19 aggregate bars yield K=20. Enough flat bars flush
        # every oscillator window; the first recovered K=30 is separated
        # from the previous valid point and cannot emit a stale turn.
        bars = make_candles([21.0] * 166 + [31.0] * 64, step=900)
        for index in range(38, 166):
            bars[index]["low"] = bars[index]["close"]
            bars[index]["high"] = bars[index]["close"]
        self.assertEqual(signal.signals_for_timeframe(bars, now=len(bars) * 900, timeframe_minutes=30), [])

    def test_open_bar_is_discarded_and_signal_timestamp_is_closed_bar(self):
        rows = make_candles([21.0] * 18 + [31.0])
        closed = signal.validate_candles(rows, now=19 * 900 - 1)
        self.assertEqual(len(closed), 18)
        self.assertEqual(signal.signals_for_timeframe(rows, now=19 * 900 - 1, timeframe_minutes=15), [])
        emitted = signal.signals_for_timeframe(rows, now=19 * 900, timeframe_minutes=15)
        self.assertEqual(emitted[0]["bar_close_timestamp"], 19 * 900)

    def test_aggregation_only_returns_complete_utc_buckets(self):
        rows = make_candles([10.0] * 5)
        self.assertEqual(len(signal.aggregate_candles(rows, 30)), 2)
        self.assertEqual(signal.aggregate_candles(rows[:1], 30), [])
        self.assertEqual(len(signal.aggregate_candles(rows, 60)), 1)

    def test_duplicate_signal_ids_are_rejected(self):
        signal_row = {"signal_id": "same"}
        with self.assertRaisesRegex(ValueError, "duplicate signal_id"):
            signal.ensure_unique_signal_ids([signal_row, signal_row])


if __name__ == "__main__":
    unittest.main()
