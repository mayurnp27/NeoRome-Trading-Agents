import importlib.util
from pathlib import Path
import unittest

MODULE_PATH = Path(__file__).with_name("research.py")
SPEC = importlib.util.spec_from_file_location("btc_stochastic_research", MODULE_PATH)
research = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(research)


def source_with_last_turn(target_close=41.0):
    """38 complete 30m bars, a final turn, then one target 15m bar."""
    rows = []
    for i in range(76):
        close = 31.0 if i == 75 else 21.0
        rows.append({"start": i * 900, "open": close, "high": 101.0,
                     "low": 1.0, "close": close, "volume": 1.0})
    target_start = 76 * 900
    rows.append({"start": target_start, "open": 31.0, "high": 101.0,
                 "low": 1.0, "close": target_close, "volume": 1.0})
    return rows


class ResearchTests(unittest.TestCase):
    def test_next_candle_target_is_aligned_to_signal_close(self):
        rows = source_with_last_turn()
        result = research.run_research(rows, now=len(rows) * 900)
        variant = result["variants"]["30m_turn"]
        self.assertEqual(variant["labeled_signals"], 1)
        label = variant["signals"][0]
        self.assertEqual(label["bar_close_timestamp"], label["target_start_timestamp"])
        self.assertEqual(label["target_close_timestamp"], label["target_start_timestamp"] + 900)
        self.assertAlmostEqual(label["target_return"], 10.0 / 31.0)
        self.assertTrue(label["direction_correct"])
        self.assertFalse(result["target_is_venue_pnl"])
        self.assertFalse(result["winner_selected_from_holdout"])

    def test_flat_target_is_conservative_loss_and_holdout_can_be_insufficient(self):
        rows = source_with_last_turn(target_close=31.0)
        result = research.run_research(rows, now=len(rows) * 900, minimum_holdout_signals=10)
        variant = result["variants"]["30m_turn"]
        self.assertEqual(variant["signals"][0]["target_direction"], "flat")
        self.assertFalse(variant["signals"][0]["direction_correct"])
        self.assertEqual(variant["holdout"]["flat_targets_counted_as_losses"], 1)
        self.assertEqual(variant["holdout"]["status"], "insufficient_holdout")

    def test_future_target_value_does_not_change_signal_features(self):
        upward = research.run_research(source_with_last_turn(target_close=41.0), now=77 * 900)
        downward = research.run_research(source_with_last_turn(target_close=20.0), now=77 * 900)
        up_signal = upward["variants"]["30m_turn"]["signals"][0]
        down_signal = downward["variants"]["30m_turn"]["signals"][0]
        for key in ("signal_id", "bar_close_timestamp", "previous_k", "previous_d", "k", "d"):
            self.assertEqual(up_signal[key], down_signal[key])
        self.assertNotEqual(up_signal["target_direction"], down_signal["target_direction"])

    def test_chronological_split_and_train_holdout_summaries_exist_for_every_variant(self):
        rows = source_with_last_turn()
        result = research.run_research(rows, now=len(rows) * 900)
        self.assertEqual(result["split"]["train_fraction"], 0.70)
        self.assertEqual(len(result["variants"]), 6)
        for variant in result["variants"].values():
            self.assertIn("train", variant)
            self.assertIn("holdout", variant)
            self.assertIn("wilson_95", variant["holdout"])

    def test_wilson_interval_handles_zero_and_valid_counts(self):
        self.assertIsNone(research.wilson_interval(0, 0))
        interval = research.wilson_interval(8, 10)
        self.assertLess(interval["lower"], 0.8)
        self.assertGreater(interval["upper"], 0.8)


if __name__ == "__main__":
    unittest.main()
