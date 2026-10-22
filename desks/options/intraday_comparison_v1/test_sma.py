"""SMA-only signal and book isolation checks for the three-book cohort."""
from datetime import timedelta
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import core

ROOT = Path(__file__).parent


class SmaSignalTests(unittest.TestCase):
    def setUp(self):
        self.config = core.load_config(ROOT / "config-SMA.json")
        self.asof = core.parse_time("2026-10-21T10:00:05-04:00")

    def bars(self, closes, *, skip=None, future=None):
        starts = [core.parse_time("2026-10-21T09:30:00-04:00") + timedelta(minutes=5 * i)
                  for i in range(len(closes))]
        result = [{"begins_at": core.stamp(start), "open_price": close, "high_price": close + 0.5,
                   "low_price": close - 0.5, "close_price": close, "volume": 1, "session": "reg"}
                  for i, (start, close) in enumerate(zip(starts, closes)) if i != skip]
        if future is not None:
            start = starts[-1] + timedelta(minutes=15)
            result.append({"begins_at": core.stamp(start), "open_price": future, "high_price": future + 1,
                           "low_price": future - 1, "close_price": future, "volume": 1, "session": "reg"})
        return result

    def report(self, bars, asof=None):
        return core.signals({"results": [{"symbol": "SPY", "interval": "5minute", "bars": bars}]}, asof or self.asof, self.config)

    def test_up_and_down_crosses_use_completed_close_evidence_and_never_call_stochastic(self):
        with patch.object(core, "stochastic_series", side_effect=AssertionError("stochastic used")), \
             patch.object(core, "indicator_snapshot", side_effect=AssertionError("stochastic snapshot used")), \
             patch.object(core, "directional_filter_decision", side_effect=AssertionError("stochastic veto used")), \
             patch.object(core, "aggregate_bars", side_effect=AssertionError("higher timeframe used")):
            for closes, direction, label in (([10, 10, 10, 10, 10, 11], "call", "above"),
                                              ([10, 10, 10, 10, 10, 9], "put", "below")):
                with self.subTest(direction=direction):
                    candidate = self.report(self.bars(closes))["candidates"][0]
                    self.assertEqual(candidate["direction"], direction)
                    evidence = candidate["sma_evidence"]
                    self.assertEqual(evidence["cross"], label)
                    self.assertEqual(evidence["sma_fast_period"], 2)
                    self.assertEqual(evidence["sma_slow_period"], 5)
                    self.assertEqual(evidence["timeframe_minutes"], 5)
                    self.assertEqual(candidate["signal_time"], "2026-10-21T14:00:00Z")

    def test_equality_persistent_trend_and_repeated_bar_are_deduplicated_by_id(self):
        equal = self.report(self.bars([10] * 6))
        self.assertEqual(equal["candidates"], [])
        self.assertEqual(equal["evaluations"][0]["reason"], "no_sma_crossover")
        first = self.report(self.bars([10, 10, 10, 10, 10, 11]))["candidates"][0]
        second = self.report(self.bars([10, 10, 10, 10, 10, 11]))["candidates"][0]
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["sma_evidence"], second["sma_evidence"])
        persistent = self.report(self.bars([10, 10, 10, 10, 11, 12]))
        self.assertEqual(persistent["candidates"], [])

    def test_six_bar_warmup_missing_bar_and_future_bar_fail_closed(self):
        short = self.report(self.bars([10] * 5))
        self.assertEqual(short["evaluations"][0]["reason"], "insufficient_sma_warmup")
        missing = self.report(self.bars([10, 10, 10, 10, 10, 11, 12], skip=2), core.parse_time("2026-10-21T10:05:05-04:00"))
        self.assertEqual(missing["evaluations"][0]["reason"], "missing_bar_in_sma_window")
        future = self.report(self.bars([10, 10, 10, 10, 10], future=11))
        self.assertEqual(future["evaluations"][0]["reason"], "insufficient_sma_warmup")

    def test_sma_config_has_no_stochastic_fields_and_malformed_stochastic_config_rejected(self):
        self.assertNotIn("stochastic", self.config)
        self.assertEqual(self.config["directional_filter_timeframes"], [])
        with self.assertRaisesRegex(ValueError, "cannot configure stochastic"):
            core.load_config({"book_key": "SMA", "signal_type": "sma_cross", "stochastic": {"length": 5}})

    def test_extreme_stochastic_value_does_not_veto_sma_book_entry(self):
        signal = {"id": self.config["strategy_id"] + ":manual", "strategy_id": self.config["strategy_id"],
                  "symbol": "SPY", "direction": "call", "signal_time": "2026-10-21T14:00:00Z",
                  "expiry": "2026-10-23", "indicators": {"15": {"k": 80}}}
        updated = "2026-10-21T14:00:04Z"
        snapshot = {"asof": "2026-10-21T14:00:05Z", "signals": [signal],
                    "catalog": [{"option_id": "SPY-C101", "symbol": "SPY", "expiry": "2026-10-23",
                                 "type": "call", "strike": 101, "multiplier": 100}],
                    "underlying_quotes": [{"symbol": "SPY", "price": 100, "updated_at": updated}],
                    "quotes": [{"option_id": "SPY-C101", "symbol": "SPY", "expiry": "2026-10-23",
                                "type": "call", "strike": 101, "multiplier": 100, "bid": 1, "ask": 1.02,
                                "bid_size": 10, "ask_size": 10, "updated_at": updated}]}
        result = core.apply_snapshot(snapshot, core.new_state(self.config), self.config)
        self.assertEqual(len(result["positions"]), 1)
        self.assertFalse(any(event.get("reason") == "directional_stochastic_veto" for event in result["events"]))


if __name__ == "__main__":
    unittest.main()
