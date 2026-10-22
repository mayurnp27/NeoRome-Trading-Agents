"""Behavioral tests for the isolated stochastic paper account."""
import json
from pathlib import Path
import tempfile
import unittest
from datetime import timedelta

import core


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_path = Path(self.tmp.name) / "A.json"
        self.config = core.load_config({"strategy_id": "A", "symbols": ["SPY"], "session_dates": ["2026-10-15"]})

    def snapshot(self, asof="2026-10-15T09:35:05-04:00", signals=None, quote_updates=None, spot=100.0, expiry="2026-10-16"):
        at = core.parse_time(asof)
        updated = core.stamp(at - timedelta(seconds=1))
        catalog = []
        quotes = []
        for typ in ("call", "put"):
            strikes = (99.0, 100.0, 101.0)
            for strike in strikes:
                oid = f"SPY-{expiry}-{typ}-{int(strike)}"
                catalog.append({"option_id": oid, "symbol": "SPY", "expiry": expiry, "type": typ, "strike": strike, "multiplier": 100})
                bid, ask, bid_size, ask_size = (quote_updates or {}).get(oid, (1.00, 1.02, 10, 10))
                quotes.append({"option_id": oid, "symbol": "SPY", "expiry": expiry, "type": typ, "strike": strike, "multiplier": 100, "bid": bid, "ask": ask, "bid_size": bid_size, "ask_size": ask_size, "updated_at": updated})
        return {"asof": asof, "signals": signals or [], "catalog": catalog, "underlying_quotes": [{"symbol": "SPY", "price": spot, "updated_at": updated}], "quotes": quotes}

    def signal(self, sid="A:s1", direction="call", signal_time="2026-10-15T09:35:00-04:00", expiry="2026-10-16", strategy="A"):
        return {"id": sid, "strategy_id": strategy, "symbol": "SPY", "direction": direction, "signal_time": signal_time, "expiry": expiry}

    def process(self, observation, path=None, config=None):
        return core.process_snapshot(observation, path or self.state_path, config or self.config)

    def test_multiple_positions_size_by_equity_and_depth(self):
        obs = self.snapshot(signals=[self.signal(), self.signal("A:s2", "put")])
        # Calls select 101, puts select 99. Both are OTM and the ask sizes bind the total.
        obs["quotes"] = [dict(q, ask_size=2) if q["strike"] in (99, 101) else q for q in obs["quotes"]]
        result = self.process(obs)
        self.assertEqual(len(result["positions"]), 2)
        self.assertEqual([p["quantity"] for p in result["positions"]], [2, 2])
        self.assertAlmostEqual(result["cash"], 591.6)
        self.assertAlmostEqual(result["equity"], 991.6)

    def test_same_quote_depth_is_not_double_counted_between_signals(self):
        signals = [self.signal("A:one"), self.signal("A:two")]
        obs = self.snapshot(signals=signals)
        selected = next(q for q in obs["quotes"] if q["option_id"].endswith("call-101"))
        selected["ask_size"] = 2
        result = core.apply_snapshot(obs, core.new_state(self.config), self.config)
        self.assertEqual(len(result["positions"]), 1)
        self.assertEqual(result["positions"][0]["quantity"], 2)
        self.assertEqual(result["events"][-1]["reason"], "insufficient_cash_or_ask_depth")

    def test_ask_depth_is_consumed_once_per_quote_timestamp(self):
        first = self.snapshot(signals=[self.signal("A:call-first")])
        first_call = next(q for q in first["quotes"] if q["option_id"].endswith("call-101"))
        first_call["ask_size"] = 2
        result = self.process(first)
        self.assertEqual(result["positions"][0]["quantity"], 2)

        close = self.snapshot("2026-10-15T09:35:07-04:00", quote_updates={"SPY-2026-10-16-call-101": (1.24, 1.26, 2, 10)})
        close_quote = next(q for q in close["quotes"] if q["option_id"].endswith("call-101"))
        close_quote["updated_at"] = "2026-10-15T09:35:06-04:00"
        result = self.process(close)
        self.assertEqual(result["positions"], [])

        repeated = self.snapshot("2026-10-15T09:35:08-04:00", [self.signal("A:call-repeat", signal_time="2026-10-15T09:35:01-04:00")])
        repeated_call = next(q for q in repeated["quotes"] if q["option_id"].endswith("call-101"))
        repeated_call.update(ask_size=2, updated_at="2026-10-15T09:35:04-04:00")
        result = self.process(repeated)
        self.assertEqual(result["events"][-1]["reason"], "insufficient_cash_or_ask_depth")
        self.assertEqual(result["positions"], [])

        refreshed = self.snapshot("2026-10-15T09:35:09-04:00", [self.signal("A:call-new", signal_time="2026-10-15T09:35:01-04:00")])
        refreshed_call = next(q for q in refreshed["quotes"] if q["option_id"].endswith("call-101"))
        refreshed_call.update(ask_size=2, updated_at="2026-10-15T09:35:08-04:00")
        result = self.process(refreshed)
        self.assertEqual(len(result["positions"]), 1)
        self.assertEqual(result["positions"][0]["quantity"], 2)

    def test_bid_depth_and_exit_latch_survive_repeated_quote(self):
        self.process(self.snapshot(signals=[self.signal()]))
        trigger = self.snapshot("2026-10-15T09:40:05-04:00", quote_updates={"SPY-2026-10-16-call-101": (1.24, 1.26, 1, 10)})
        trigger_quote = next(q for q in trigger["quotes"] if q["option_id"].endswith("call-101"))
        trigger_quote["updated_at"] = "2026-10-15T09:40:04-04:00"
        result = self.process(trigger)
        self.assertEqual(result["positions"][0]["quantity"], 2)
        self.assertEqual(result["positions"][0]["exit_latched_reason"], "take_profit")

        repeated = self.snapshot("2026-10-15T09:40:06-04:00", quote_updates={"SPY-2026-10-16-call-101": (1.00, 1.02, 1, 10)})
        repeated_quote = next(q for q in repeated["quotes"] if q["option_id"].endswith("call-101"))
        repeated_quote["updated_at"] = "2026-10-15T09:40:04-04:00"
        result = self.process(repeated)
        self.assertEqual(result["positions"][0]["quantity"], 2)
        self.assertTrue(any(e["kind"] == "unresolved_exit_depth" for e in result["events"]))

        refreshed = self.snapshot("2026-10-15T09:40:07-04:00", quote_updates={"SPY-2026-10-16-call-101": (1.00, 1.02, 1, 10)})
        result = self.process(refreshed)
        self.assertEqual(result["positions"][0]["quantity"], 1)
        self.assertEqual(json.loads(self.state_path.read_text())["trades"][-1]["reason"], "take_profit")
        closing = self.snapshot("2026-10-15T11:01:00-04:00", quote_updates={"SPY-2026-10-16-call-101": (1.00, 1.02, 10, 10)})
        result = self.process(closing)
        self.assertEqual(result["positions"], [])
        final_trade = json.loads(self.state_path.read_text())["trades"][-1]
        self.assertEqual(final_trade["reason"], "time_exit")
        self.assertTrue(final_trade["late_exit"])

    def test_equity_budget_limits_whole_contract_quantity(self):
        obs = self.snapshot(signals=[self.signal()])
        result = self.process(obs)
        pos = result["positions"][0]
        self.assertEqual(pos["quantity"], 3)  # floor(40% * $1,000 / ($102 + $0.10))
        self.assertLessEqual(pos["entry_debit"], 400)

    def multi_symbol_observation(self, signals, second_ask=1.02, second_bid=1.00, second_depth=10):
        obs = self.snapshot(signals=signals)
        obs["catalog"] += [dict(row, symbol="QQQ", option_id=row["option_id"].replace("SPY", "QQQ")) for row in self.snapshot()["catalog"]]
        obs["quotes"] += [dict(q, symbol="QQQ", option_id=q["option_id"].replace("SPY", "QQQ"), ask=second_ask, bid=second_bid, ask_size=second_depth) for q in self.snapshot()["quotes"]]
        obs["underlying_quotes"].append({"symbol": "QQQ", "price": 100.0, "updated_at": obs["underlying_quotes"][0]["updated_at"]})
        return obs

    def test_equal_cash_allows_one_setup_to_use_more_than_legacy_cap(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY"], "session_dates": ["2026-10-15"], "sizing_policy": "equal_cash_executable", "max_position_equity_fraction": 1})
        result = core.apply_snapshot(self.snapshot(signals=[self.signal()]), core.new_state(config), config)
        self.assertEqual(result["positions"][0]["quantity"], 9)
        self.assertGreater(result["positions"][0]["entry_debit"], 400)
        self.assertLessEqual(result["positions"][0]["entry_debit"], 1000)
        self.assertEqual(result["positions"][0]["sizing_policy"], "equal_cash_executable")

    def test_equal_cash_splits_across_qualified_symbols_and_respects_depth(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY", "QQQ"], "session_dates": ["2026-10-15"], "sizing_policy": "equal_cash_executable", "max_position_equity_fraction": 1})
        qqq = dict(self.signal("A:qqq"), symbol="QQQ")
        obs = self.multi_symbol_observation([self.signal(), qqq], second_depth=2)
        for quote in obs["quotes"]:
            if quote["symbol"] == "SPY" and quote["option_id"].endswith("call-101"):
                quote["ask_size"] = 2
        result = core.apply_snapshot(obs, core.new_state(config), config)
        self.assertEqual([position["quantity"] for position in result["positions"]], [2, 2])
        self.assertEqual([position["cash_budget"] for position in result["positions"]], [500, 500])
        self.assertGreaterEqual(result["cash"], 0)
        self.assertLessEqual(sum(position["quantity"] for position in result["positions"]), 4)

    def test_consumed_quote_depth_does_not_dilute_an_executable_setup(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY", "QQQ"], "session_dates": ["2026-10-15"], "sizing_policy": "equal_cash_executable", "max_position_equity_fraction": 1})
        signals = [self.signal(), dict(self.signal("A:qqq"), symbol="QQQ")]
        obs = self.multi_symbol_observation(signals)
        state = core.new_state(config)
        consumed = next(q for q in obs["quotes"] if q["symbol"] == "SPY" and q["option_id"].endswith("call-101"))
        core.consume_quote_depth(state, "ask", consumed, consumed["ask_size"])
        result = core.apply_snapshot(obs, state, config)
        self.assertEqual([position["signal_id"] for position in result["positions"]], ["A:qqq"])
        self.assertEqual(result["positions"][0]["cash_budget"], 1000)
        self.assertGreater(result["positions"][0]["entry_debit"], 400)
        rejected = next(event for event in result["events"] if event.get("signal_id") == "A:s1")
        self.assertEqual(rejected["reason"], "insufficient_cash_or_ask_depth")

    def test_invalid_and_unaffordable_setups_do_not_dilute_equal_cash(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY", "QQQ", "IWM"], "session_dates": ["2026-10-15"], "sizing_policy": "equal_cash_executable", "max_position_equity_fraction": 1})
        qqq_expensive = dict(self.signal("A:expensive"), symbol="QQQ")
        iwm_wide = dict(self.signal("A:wide"), symbol="IWM")
        obs = self.multi_symbol_observation([self.signal(), qqq_expensive, iwm_wide], second_ask=8.0, second_bid=7.98)
        extra = self.snapshot()
        obs["catalog"] += [dict(row, symbol="IWM", option_id=row["option_id"].replace("SPY", "IWM")) for row in extra["catalog"]]
        obs["quotes"] += [dict(q, symbol="IWM", option_id=q["option_id"].replace("SPY", "IWM"), bid=0.90, ask=1.02) for q in extra["quotes"]]
        obs["underlying_quotes"].append({"symbol": "IWM", "price": 100.0, "updated_at": obs["underlying_quotes"][0]["updated_at"]})
        obs["signals"].append(self.signal("B:other-arm", strategy="B"))
        result = core.apply_snapshot(obs, core.new_state(config), config)
        self.assertEqual([position["signal_id"] for position in result["positions"]], ["A:s1"])
        self.assertGreater(result["positions"][0]["entry_debit"], 400)
        self.assertEqual(result["positions"][0]["cash_budget"], 1000)
        self.assertIn("B:other-arm", [event.get("signal_id") for event in result["events"] if event["kind"] == "entry_rejected"])
        wide_rejection = next(event for event in result["events"] if event.get("signal_id") == "A:wide")
        self.assertEqual(wide_rejection["reason"], "no_executable_exact_otm_quote")

    def test_equal_share_fallback_selects_one_when_each_contract_costs_over_half_cash(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY", "QQQ"], "session_dates": ["2026-10-15"], "sizing_policy": "equal_cash_executable", "max_position_equity_fraction": 1})
        signals = [self.signal("A:one"), dict(self.signal("A:two"), symbol="QQQ")]
        obs = self.multi_symbol_observation(signals, second_ask=6.0, second_bid=5.98)
        for quote in obs["quotes"]:
            if quote["symbol"] == "SPY" and quote["option_id"].endswith("call-101"):
                quote.update(ask=6.0, bid=5.98)
        result = core.apply_snapshot(obs, core.new_state(config), config)
        self.assertEqual([position["signal_id"] for position in result["positions"]], ["A:one"])
        self.assertEqual(result["positions"][0]["allocation_reason"], "single_candidate_fallback_tightest_relative_spread")
        self.assertEqual(result["positions"][0]["quantity"], 1)
        self.assertLessEqual(result["positions"][0]["entry_debit"], 1000)

    def test_core_expiry_stays_in_calendar_week_and_rejects_explicit_next_week(self):
        config = core.load_config({"strategy_id": "A", "symbols": ["SPY"], "session_dates": ["2026-10-20", "2026-10-22"]})
        snapshot = {"catalog": [{"symbol": "SPY", "expiry": "2026-10-23"}, {"symbol": "SPY", "expiry": "2026-10-26"}]}
        signal = self.signal(signal_time="2026-10-20T09:35:00-04:00", expiry=None)
        asof = core.parse_time("2026-10-20T09:35:05-04:00")
        self.assertEqual(core.resolve_expiry(signal, snapshot, asof, config), "2026-10-23")
        signal["expiry"] = "2026-10-26"
        self.assertIsNone(core.resolve_expiry(signal, snapshot, asof, config))
        eligible = ["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-23", "2026-10-26"]
        snapshot["catalog"] = [{"symbol": "SPY", "expiry": expiry} for expiry in eligible]
        signal.pop("expiry")
        self.assertEqual(core.resolve_expiry(signal, snapshot, asof, config), "2026-10-21")
        for dates, expected in ((["2026-10-19", "2026-10-20", "2026-10-23"], "2026-10-23"),
                                (["2026-10-19", "2026-10-23"], "2026-10-23"),
                                (["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23"], "2026-10-23")):
            snapshot["catalog"] = [{"symbol": "SPY", "expiry": expiry} for expiry in dates]
            asof_day = "2026-10-22" if dates[-2] == "2026-10-22" else "2026-10-20"
            signal["signal_time"] = asof_day + "T09:35:00-04:00"
            self.assertEqual(core.resolve_expiry(signal, snapshot, core.parse_time(asof_day + "T09:35:05-04:00"), config), expected)

    def test_fresh_existing_marks_enter_new_risk_but_stale_mark_blocks_it(self):
        self.process(self.snapshot(signals=[self.signal()]))
        second = self.signal("A:s2", "put", "2026-10-15T09:36:00-04:00")
        fresh = self.snapshot("2026-10-15T09:36:05-04:00", [second])
        result = self.process(fresh)
        self.assertEqual(len(result["positions"]), 2)
        third = self.signal("A:s3", "call", "2026-10-15T09:37:00-04:00")
        stale = self.snapshot("2026-10-15T09:37:05-04:00", [third])
        stale["quotes"] = [q for q in stale["quotes"] if q["option_id"] != "SPY-2026-10-16-call-101" and q["option_id"] != "SPY-2026-10-16-put-99"]
        stale["underlying_quotes"][0]["price"] = 99.5  # candidate call uses strike 100, distinct from held call 101
        stale["quotes"] = [q for q in stale["quotes"] if q["option_id"] not in ("SPY-2026-10-16-call-101", "SPY-2026-10-16-put-99")]
        # In the real account the two held contracts have no current marks, so no sizing is allowed.
        result = self.process(stale)
        self.assertEqual(result["events"][-1]["reason"], "unknown_or_stale_position_mark")
        self.assertEqual(len(result["positions"]), 2)

    def test_exact_otm_selection_and_atm_only_is_rejected(self):
        for direction, wanted_strike in (("call", 101), ("put", 99)):
            with self.subTest(direction=direction):
                obs = self.snapshot(signals=[self.signal("A:" + direction, direction)])
                result = core.apply_snapshot(obs, core.new_state(self.config), self.config)
                self.assertEqual(result["positions"][0]["strike"], wanted_strike)
        atm = self.snapshot(signals=[self.signal()], spot=100)
        atm["catalog"] = [r for r in atm["catalog"] if r["strike"] == 100]
        result = core.apply_snapshot(atm, core.new_state(self.config), self.config)
        self.assertEqual(result["events"][-1]["reason"], "no_exact_one_strike_otm_contract")

    def test_partial_exit_and_contract_fees(self):
        entered = self.process(self.snapshot(signals=[self.signal()]))
        self.assertEqual(entered["positions"][0]["quantity"], 3)
        updates = {"SPY-2026-10-16-call-101": (1.24, 1.26, 1, 10)}
        result = self.process(self.snapshot("2026-10-15T09:40:05-04:00", quote_updates=updates))
        self.assertEqual(result["positions"][0]["quantity"], 2)
        trade = json.loads(self.state_path.read_text())["trades"][0]
        self.assertEqual(trade["quantity"], 1)
        self.assertAlmostEqual(trade["exit_fee"], 0.10)
        self.assertAlmostEqual(trade["entry_fee"], 0.10)
        self.assertAlmostEqual(trade["net_pnl"], 21.8)
        self.assertAlmostEqual(result["cash"], 817.6)

    def test_stale_bid_keeps_position_unresolved(self):
        self.process(self.snapshot(signals=[self.signal()]))
        stale = self.snapshot("2026-10-15T09:40:05-04:00")
        for q in stale["quotes"]:
            q["updated_at"] = "2026-10-15T09:39:00-04:00"
        result = self.process(stale)
        self.assertEqual(len(result["positions"]), 1)
        self.assertTrue(any(e["kind"] == "unresolved_mark" for e in result["events"]))

    def test_no_daily_trade_or_loss_latches(self):
        self.process(self.snapshot(signals=[self.signal()]))
        stop = self.snapshot("2026-10-15T09:40:05-04:00", quote_updates={"SPY-2026-10-16-call-101": (0.80, 0.82, 10, 10)})
        self.process(stop)
        next_signal = self.signal("A:s2", "put", "2026-10-15T09:41:00-04:00")
        result = self.process(self.snapshot("2026-10-15T09:41:05-04:00", [next_signal]))
        self.assertEqual(len(result["positions"]), 1)
        self.assertEqual(result["positions"][0]["signal_id"], "A:s2")

    def test_multiple_signals_independent_and_config_fingerprint(self):
        a = self.snapshot(signals=[self.signal(), self.signal("B:s1", "put", strategy="B")])
        result_a = self.process(a)
        self.assertEqual([p["signal_id"] for p in result_a["positions"]], ["A:s1"])
        config_b = core.load_config({"strategy_id": "B", "stochastic": {"length": 5, "smooth_k": 3, "smooth_d": 3}, "symbols": ["SPY"], "session_dates": ["2026-10-15"]})
        result_b = self.process(a, Path(self.tmp.name) / "B.json", config_b)
        self.assertEqual([p["signal_id"] for p in result_b["positions"]], ["B:s1"])
        with self.assertRaisesRegex(ValueError, "Config differs"):
            core.process_snapshot(a, self.state_path, config_b)

    def test_quote_must_be_after_signal_and_spread_is_capped(self):
        obs = self.snapshot(signals=[self.signal()])
        next(q for q in obs["quotes"] if q["option_id"].endswith("call-101"))["updated_at"] = "2026-10-15T09:35:00-04:00"
        result = core.apply_snapshot(obs, core.new_state(self.config), self.config)
        self.assertEqual(result["events"][-1]["reason"], "no_executable_exact_otm_quote")
        obs = self.snapshot(signals=[self.signal()])
        q = next(q for q in obs["quotes"] if q["option_id"].endswith("call-101"))
        q.update(bid=0.9, ask=1.02)
        result = core.apply_snapshot(obs, core.new_state(self.config), self.config)
        self.assertEqual(result["events"][-1]["quote_rejections"][0]["reason"], "spread_too_wide")

    def test_status_reports_positions_quantity_and_equity(self):
        self.process(self.snapshot(signals=[self.signal()]))
        report = core.status(self.state_path, self.config)
        self.assertEqual(report["positions"][0]["quantity"], 3)
        self.assertAlmostEqual(report["equity"], 993.7)
        self.assertEqual(report["closed_contracts"], 0)

    def test_idempotency_and_out_of_order_guard(self):
        obs = self.snapshot(signals=[self.signal()])
        first = self.process(obs)
        repeated = self.process(obs)
        self.assertTrue(repeated["duplicate"])
        self.assertEqual(first["cash"], repeated["cash"])
        with self.assertRaisesRegex(ValueError, "precedes"):
            self.process(self.snapshot("2026-10-15T09:35:04-04:00"))


if __name__ == "__main__":
    unittest.main()
