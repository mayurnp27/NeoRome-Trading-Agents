"""Focused tests for the optional 15-minute directional stochastic veto."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import core

ROOT = Path(__file__).parent


class DirectionalFilterTests(unittest.TestCase):
    def setUp(self):
        self.config = core.load_config(json.loads((ROOT / "config-A.json").read_text()))
        self.config["symbols"] = ["SPY"]

    def indicators(self, k15):
        return {"5": {"k": 99, "d": 1, "prior_k": 1, "prior_d": 2},
                "15": {"k": k15, "d": 50, "prior_k": 50, "prior_d": 50},
                "30": {"k": 1, "d": 99, "prior_k": 99, "prior_d": 99}}

    def signal(self, direction, k15, sid="variant:s1"):
        return {"id": sid, "strategy_id": self.config["strategy_id"], "symbol": "SPY",
                "direction": direction, "signal_time": "2026-10-19T09:35:00-04:00",
                "expiry": "2026-10-23", "indicators": self.indicators(k15)}

    def observation(self, signal):
        asof, updated = "2026-10-19T09:35:05-04:00", "2026-10-19T09:35:04-04:00"
        catalog, quotes = [], []
        for right in ("call", "put"):
            for strike in (99.0, 100.0, 101.0):
                oid = f"SPY-2026-10-23-{right}-{int(strike)}"
                catalog.append({"option_id": oid, "symbol": "SPY", "expiry": "2026-10-23", "type": right, "strike": strike, "multiplier": 100})
                quotes.append({"option_id": oid, "symbol": "SPY", "expiry": "2026-10-23", "type": right, "strike": strike, "multiplier": 100, "bid": 1.0, "ask": 1.02, "bid_size": 10, "ask_size": 10, "updated_at": updated})
        return {"asof": asof, "signals": [signal], "catalog": catalog, "quotes": quotes,
                "underlying_quotes": [{"symbol": "SPY", "price": 100, "updated_at": updated}]}

    def test_inclusive_threshold_vetoes_manual_signals_with_evidence(self):
        for direction, k15 in (("call", 80), ("put", 20)):
            with self.subTest(direction=direction):
                result = core.apply_snapshot(self.observation(self.signal(direction, k15)), core.new_state(self.config), self.config)
                rejected = [event for event in result["events"] if event["kind"] == "entry_rejected"][-1]
                self.assertEqual(rejected["reason"], "directional_stochastic_veto")
                self.assertEqual(rejected["directional_filter_evidence"], [{
                    "timeframe_minutes": 15, "current_k": k15,
                    "comparison": ">=" if direction == "call" else "<=",
                    "threshold": 80 if direction == "call" else 20, "veto": True}])
                self.assertEqual(result["positions"], [])

    def test_malformed_or_missing_current_k_fails_closed(self):
        for value in (None, "80", float("nan"), float("inf"), -1, 101):
            with self.subTest(value=value):
                signal = self.signal("call", 50)
                signal["indicators"]["15"]["k"] = value
                result = core.apply_snapshot(self.observation(signal), core.new_state(self.config), self.config)
                rejected = [event for event in result["events"] if event["kind"] == "entry_rejected"][-1]
                self.assertEqual(rejected["reason"], "invalid_directional_filter_indicator")
                self.assertEqual(result["positions"], [])
        signal = self.signal("put", 50)
        del signal["indicators"]["15"]
        result = core.apply_snapshot(self.observation(signal), core.new_state(self.config), self.config)
        self.assertEqual(result["events"][-1]["reason"], "invalid_directional_filter_indicator")

    def test_opposite_extremes_and_unfiltered_timeframes_are_allowed(self):
        for direction, k15, prior_k, current_d in (("call", 0, 100, 100), ("put", 100, 0, 0),
                                                    ("call", 79.999, 100, 100), ("put", 20.001, 0, 0)):
            with self.subTest(direction=direction, k15=k15):
                signal = self.signal(direction, k15)
                signal["indicators"]["15"]["prior_k"] = prior_k
                signal["indicators"]["15"]["d"] = current_d
                result = core.apply_snapshot(self.observation(signal), core.new_state(self.config), self.config)
                self.assertEqual(len(result["positions"]), 1)

    def test_filter_config_is_strict_and_default_preserves_baseline(self):
        self.assertEqual(core.load_config({"strategy_id": "A"})["directional_filter_timeframes"], [])
        for invalid in ([True], ["15"], [10], [15, 15], "15", None):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "directional_filter_timeframes"):
                core.load_config({"directional_filter_timeframes": invalid})
        baseline = core.load_config({"strategy_id": "A", "symbols": ["SPY"], "session_dates": ["2026-10-19"]})
        signal = {"id": "A:baseline", "strategy_id": "A", "symbol": "SPY", "direction": "call", "signal_time": "2026-10-19T09:35:00-04:00", "expiry": "2026-10-23"}
        result = core.apply_snapshot(self.observation(signal), core.new_state(baseline), baseline)
        self.assertEqual(len(result["positions"]), 1)

    def test_signal_generation_applies_same_inclusive_current_k_veto(self):
        asof = core.parse_time("2026-10-19T09:35:10-04:00")
        bar = {"start": core.parse_time("2026-10-19T09:30:00-04:00"), "end": core.parse_time("2026-10-19T09:35:00-04:00"), "developing": False}
        frames = [{"k": 60, "d": 50, "prior_k": 40, "prior_d": 50},
                  {"k": 80, "d": 70, "prior_k": 65, "prior_d": 60},
                  {"k": 60, "d": 50, "prior_k": 55, "prior_d": 50}]
        with patch.object(core, "normalized_bars", return_value=([bar], [])), \
             patch.object(core, "aggregate_bars", side_effect=lambda bars, minutes, now: [bar]), \
             patch.object(core, "continuous_indicator_window", return_value=True), \
             patch.object(core, "indicator_snapshot", side_effect=frames):
            report = core.signals({"results": [{"symbol": "SPY", "bars": [{}]}]}, asof, self.config)
        self.assertEqual(report["candidates"], [])
        self.assertEqual(report["evaluations"][0]["reason"], "directional_stochastic_veto")
        self.assertEqual(report["evaluations"][0]["directional_filter"]["evidence"][0]["current_k"], 80)

    def test_variants_activate_october_19_and_preserve_end_date(self):
        for filename in ("config-A.json", "config-B.json"):
            config = core.load_config(json.loads((ROOT / filename).read_text()))
            self.assertFalse(core.entry_window(core.parse_time("2026-10-16T09:40:00-04:00"), config))
            self.assertTrue(core.entry_window(core.parse_time("2026-10-19T09:40:00-04:00"), config))
            self.assertTrue(core.allowed_day(core.date(2026, 11, 11), config))
            self.assertFalse(core.allowed_day(core.date(2026, 11, 12), config))
            self.assertEqual(config["directional_filter_timeframes"], [15])
            self.assertEqual(config["take_profit_fraction"], 0.10)
            self.assertEqual(config["stop_loss_fraction"], 0.10)
            self.assertEqual(config["initial_cash"], 1000)
            self.assertEqual(config["max_position_equity_fraction"], 1.0)
            self.assertEqual(config["sizing_policy"], "equal_cash_executable")


if __name__ == "__main__":
    unittest.main()
