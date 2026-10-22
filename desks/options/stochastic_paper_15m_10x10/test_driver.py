"""Offline integration checks for the bounded one-shot market-data driver."""
from datetime import date, datetime, timedelta, timezone
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import core
import driver


class DriverTests(unittest.TestCase):
    UNDERLYING_IDS = {
        "SPY": "a64d5a2f-738b-43a2-a4b8-2a1bb0bfb6d1",
        "IWM": "b649c88a-7abb-4e77-97a6-e71a821849e0",
        "QQQ": "59f3a0a1-9d31-4a16-8a68-9abdf24134e8",
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config_dir = self.root / "configs"
        self.config_dir.mkdir()
        for arm, strategy, length in (("A", "A-14-3-3-OTM40-v2", 14), ("B", "B-5-3-3-OTM40-v2", 5)):
            config = core.load_config({"strategy_id": strategy, "stochastic": {"length": length, "smooth_k": 3, "smooth_d": 3},
                                       "symbols": ["SPY"], "session_dates": ["2026-10-15"]})
            (self.config_dir / ("config-" + arm + ".json")).write_text(json.dumps(config))

    def at(self, value):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def test_unrelated_release_metadata_is_not_a_strategy_config(self):
        (self.config_dir / "manifest.json").write_text(json.dumps({"release": "v2"}))
        desk = driver.OneShotDriver(self.root / "state", self.config_dir, lambda *_: {})
        self.assertEqual(set(desk._configs()), {"A", "B"})

    def test_read_only_preflight_can_verify_sources_before_pilot_begins(self):
        desk = driver.OneShotDriver(self.root / "state", self.config_dir, lambda *_: {},
                                    lambda: self.at("2026-10-14T20:30:00Z"))
        self.assertEqual(desk.scheduled_mode(), "calendar_closed")
        self.assertEqual(desk.scheduled_mode(preflight=True), "preflight")

    def test_transport_root_named_neorome_can_contain_the_python_package(self):
        root = self.root / "neorome"
        package = root / "neorome"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        (package / "robinhood_cloud_data.py").write_text(
            "READ_ONLY_TOOLS=frozenset()\n"
            "class CloudDataSession:\n"
            " def __init__(self,tokens,max_calls):self.tokens=tokens\n"
            " def __enter__(self):return self\n")
        old_path = list(sys.path)
        old_modules = {k: v for k, v in sys.modules.items() if k == "neorome" or k.startswith("neorome.")}
        try:
            for key in old_modules:sys.modules.pop(key)
            session = driver._cloud_session(SimpleNamespace(transport_root=str(root), tokens="test-only"))
            self.assertEqual(session.tokens, "test-only")
            self.assertEqual(sys.path[0], str(root))
        finally:
            sys.path[:] = old_path
            for key in list(sys.modules):
                if key == "neorome" or key.startswith("neorome."):sys.modules.pop(key)
            sys.modules.update(old_modules)

    def chain_response(self, symbol):
        return {"chains": [{"id": "chain-" + symbol, "symbol": symbol,
                            "expiration_dates": ["2026-10-15", "2026-10-16"],
                            "trade_value_multiplier": 100, "cash_component": 0,
                            "can_open_position": True, "settle_on_open": False,
                            "underlying_instruments": [{"symbol": "", "instrument":
                                "http://edge-internal.brokeback-shard-router.region.rh/instruments/" + self.UNDERLYING_IDS[symbol] + "/"}]}]}

    def search_response(self, symbol):
        return {"results": [{"instrument_id": self.UNDERLYING_IDS[symbol], "symbol": symbol}]}

    def instrument_response(self, symbol):
        return {"instruments": [
            {"id": symbol + "-C101", "chain_symbol": symbol, "expiration_date": "2026-10-16", "strike_price": "101",
             "type": "call", "trade_value_multiplier": 100, "underlying_type": "equity", "state": "active", "tradability": "tradable"},
            {"id": symbol + "-P99", "chain_symbol": symbol, "expiration_date": "2026-10-16", "strike_price": "99",
             "type": "put", "trade_value_multiplier": 100, "underlying_type": "equity", "state": "active", "tradability": "tradable"},
        ], "next": None}

    def test_expiry_selection_and_friday_rule(self):
        thursday = date(2026, 10, 15)
        self.assertEqual(driver.choose_expiry(["2026-10-15", "2026-10-16", "2026-10-19"], thursday), "2026-10-16")
        self.assertEqual(driver.choose_expiry(["2026-10-16"], thursday), "2026-10-16")
        self.assertEqual(driver.choose_expiry(["2026-10-16", "2026-10-19"], date(2026, 10, 16)), "2026-10-16")
        self.assertIsNone(driver.choose_expiry(["2026-10-19"], date(2026, 10, 16)))
        tuesday = date(2026, 10, 20)
        self.assertEqual(driver.choose_expiry(["2026-10-23", "2026-10-26"], tuesday), "2026-10-23")
        self.assertEqual(driver.choose_expiry(["2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23"], tuesday), "2026-10-21")
        self.assertEqual(driver.choose_expiry(["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-23", "2026-10-26"], tuesday), "2026-10-21")
        self.assertEqual(driver.choose_expiry(["2026-10-19", "2026-10-20", "2026-10-23"], tuesday), "2026-10-23")
        self.assertEqual(driver.choose_expiry(["2026-10-19", "2026-10-23"], tuesday), "2026-10-23")
        self.assertEqual(driver.choose_expiry(["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23"], date(2026, 10, 22)), "2026-10-23")
        self.assertIsNone(driver.choose_expiry(["2026-10-26"], tuesday))
        self.assertIsNone(driver.choose_expiry(["2026-10-23", "2026-10-26"], date(2026, 10, 24)))

    def test_blank_underlying_symbol_is_verified_against_search_and_cached_for_day(self):
        calls = []

        def provider(tool, args):
            calls.append((tool, args))
            if tool == "get_option_chains":
                return self.chain_response("IWM")
            if tool == "search":
                self.assertEqual(args, {"query": "IWM", "asset_type": "instrument", "limit": 5})
                return self.search_response("IWM")
            if tool == "get_option_instruments":
                return self.instrument_response("IWM")
            self.fail("unexpected provider tool " + tool)

        now = self.at("2026-10-15T13:35:05Z")
        state_dir = self.root / "identity-cache"
        first = driver.OneShotDriver(state_dir, self.config_dir, provider, lambda: now)
        catalog, expiry = first._chain_and_catalog("IWM")
        second = driver.OneShotDriver(state_dir, self.config_dir, provider, lambda: now)
        second._chain_and_catalog("IWM")
        self.assertEqual(expiry, "2026-10-16")
        self.assertEqual(len(catalog), 2)
        self.assertEqual(sum(tool == "search" for tool, _ in calls), 1)
        self.assertFalse(any("edge-internal" in json.dumps(args) for _, args in calls))

    def test_underlying_identity_id_mismatch_is_rejected(self):
        def provider(tool, args):
            if tool == "get_option_chains":
                return self.chain_response("IWM")
            if tool == "search":
                return {"results": [{"instrument_id": "00000000-0000-4000-8000-000000000000", "symbol": "IWM"}]}
            self.fail("catalog fetch must not follow an unverified underlying")

        desk = driver.OneShotDriver(self.root / "identity-mismatch", self.config_dir, provider,
                                    lambda: self.at("2026-10-15T13:35:05Z"))
        with self.assertRaisesRegex(driver.DriverError, "underlying_identity_search_mismatch"):
            desk._chain_and_catalog("IWM")

    def test_arm_state_is_loaded_from_book_label_path(self):
        configs = driver.OneShotDriver(self.root / "state", self.config_dir, lambda *_: {})._configs()
        state_dir = self.root / "state" / "arms"
        state_dir.mkdir(parents=True)
        state = core.new_state(configs["A"])
        state["cash"] = 777
        (state_dir / "A.json").write_text(json.dumps(state))
        desk = driver.OneShotDriver(self.root / "state", self.config_dir, lambda *_: {})
        self.assertEqual(desk._read_state("A", configs["A"])["cash"], 777)
        self.assertFalse((state_dir / (configs["A"]["strategy_id"] + ".json")).exists())

    def test_open_arm_position_is_managed_from_book_path_on_next_exit_cycle(self):
        configs = driver.OneShotDriver(self.root / "state", self.config_dir, lambda *_: {})._configs()
        state_dir = self.root / "state" / "arms"
        state_dir.mkdir(parents=True)
        opening = {"asof": "2026-10-15T14:00:00Z",
            "signals": [{"id": "A:held", "strategy_id": configs["A"]["strategy_id"], "symbol": "SPY",
                         "direction": "call", "signal_time": "2026-10-15T13:59:00Z", "expiry": "2026-10-16"}],
            "catalog": [{"option_id": "SPY-C101", "symbol": "SPY", "expiry": "2026-10-16", "type": "call", "strike": 101, "multiplier": 100}],
            "underlying_quotes": [{"symbol": "SPY", "price": 100, "updated_at": "2026-10-15T13:59:59Z"}],
            "quotes": [{"option_id": "SPY-C101", "symbol": "SPY", "expiry": "2026-10-16", "type": "call", "strike": 101,
                        "multiplier": 100, "bid": 0.49, "ask": 0.50, "bid_size": 2, "ask_size": 2, "updated_at": "2026-10-15T13:59:59Z"}]}
        arm_state = core.new_state(configs["A"])
        core.apply_snapshot(opening, arm_state, configs["A"])
        (state_dir / "A.json").write_text(json.dumps(arm_state))
        (state_dir / "B.json").write_text(json.dumps(core.new_state(configs["B"])))
        calls = []

        def call(tool, args):
            calls.append((tool, args))
            self.assertEqual(tool, "get_option_quotes")
            return {"results": [{"quote": {"instrument_id": "SPY-C101", "bid_price": 0.62, "ask_price": 0.63,
                "bid_size": 2, "ask_size": 1, "updated_at": "2026-10-15T15:06:00Z"}}]}

        now = self.at("2026-10-15T15:06:00Z")
        result = driver.OneShotDriver(self.root / "state", self.config_dir, call, lambda: now).run_once()
        saved = json.loads((state_dir / "A.json").read_text())
        self.assertEqual(result["status"], "exit_only")
        self.assertEqual(saved["positions"], {}, result)
        self.assertEqual(len(saved["trades"]), 1)
        self.assertEqual(calls, [("get_option_quotes", {"instrument_ids": ["SPY-C101"]})])

    def test_opaque_cursor_is_forwarded_verbatim_and_budget_reserved_before_call(self):
        opaque = "https://market.invalid/instruments?cursor=a%2Fb%3D%3D&limit=20"
        self.assertEqual(driver._cursor({"next": opaque}), opaque)
        now = self.at("2026-10-15T13:35:05Z")
        observed = []

        def call(tool, args):
            budget = json.loads((self.root / "state" / "call-budget" / "2026-10-15.json").read_text())
            observed.append((tool, args, budget["count"], budget["calls"][0]["outcome"]))
            return {"results": []}

        desk = driver.OneShotDriver(self.root / "state", self.config_dir, call, lambda: now)
        desk.data_call("get_equity_quotes", {"symbols": ["SPY"]})
        self.assertEqual(observed[0][2:], (1, "reserved"))
        self.assertEqual(desk.call_count, 1)
        with self.assertRaises(driver.DriverError):
            desk.data_call("place_option_order", {})

    def test_missing_entry_quote_never_fabricates_a_paper_fill(self):
        now = self.at("2026-10-15T13:35:05Z")
        calls = []

        def call(tool, args):
            calls.append((tool, args))
            if tool == "get_equity_historicals":
                return {"results": [{"symbol": symbol, "bars": [{"begins_at": "2026-10-15T13:30:00Z",
                    "open_price": 100, "high_price": 101, "low_price": 99, "close_price": 100,
                    "volume": 100, "session": "reg"}]} for symbol in args["symbols"]]}
            if tool == "get_equity_quotes":
                return {"results": [{"quote": {"symbol": symbol, "last_trade_price": 100,
                    "venue_last_trade_time": "2026-10-15T13:35:04Z"}} for symbol in args["symbols"]]}
            if tool == "get_option_chains":
                return self.chain_response(args["underlying_symbol"])
            if tool == "search":
                return self.search_response(args["query"])
            if tool == "get_option_instruments":
                return self.instrument_response("SPY")
            if tool == "get_option_quotes":
                return {"results": []}
            self.fail("unexpected provider tool " + tool)

        def signal_output(_document, _asof, config):
            return {"candidates": [{"id": config["strategy_id"] + ":s1", "strategy_id": config["strategy_id"],
                "symbol": "SPY", "direction": "call", "signal_time": "2026-10-15T13:35:00Z"}], "evaluations": []}

        with patch.object(driver.core, "signals", side_effect=signal_output):
            result = driver.OneShotDriver(self.root / "runtime", self.config_dir, call, lambda: now).run_once()
        self.assertEqual(result["status"], "processed")
        self.assertEqual(result["candidate_count"], 2)
        for arm in ("A", "B"):
            state = json.loads((self.root / "runtime" / "arms" / (arm + ".json")).read_text())
            self.assertEqual(state["positions"], {})
            self.assertEqual(state["cash"], 1000)
        self.assertEqual(sum(tool == "get_option_quotes" for tool, _ in calls), 1)

    def test_provider_clock_advances_before_fresh_entry_quote_is_applied(self):
        current = {"value": self.at("2026-10-15T13:35:05Z")}

        def call(tool, args):
            current["value"] += timedelta(seconds=1)
            if tool == "get_equity_historicals":
                return {"results": [{"symbol": symbol, "bars": [{"begins_at": "2026-10-15T13:30:00Z",
                    "open_price": 100, "high_price": 101, "low_price": 99, "close_price": 100,
                    "volume": 100, "session": "reg"}]} for symbol in args["symbols"]]}
            if tool == "get_equity_quotes":
                return {"results": [{"quote": {"symbol": symbol, "last_trade_price": 100,
                    "venue_last_trade_time": current["value"].isoformat().replace("+00:00", "Z")}} for symbol in args["symbols"]]}
            if tool == "get_option_chains":
                return self.chain_response(args["underlying_symbol"])
            if tool == "search":
                return self.search_response(args["query"])
            if tool == "get_option_instruments":
                return self.instrument_response("SPY")
            if tool == "get_option_quotes":
                return {"results": [{"quote": {"instrument_id": "SPY-C101", "bid_price": 0.49,
                    "ask_price": 0.50, "bid_size": 2, "ask_size": 2,
                    "updated_at": current["value"].isoformat().replace("+00:00", "Z")}}]}
            self.fail("unexpected provider tool " + tool)

        def signal_output(_document, _asof, config):
            return {"candidates": [{"id": config["strategy_id"] + ":fresh", "strategy_id": config["strategy_id"],
                "symbol": "SPY", "direction": "call", "signal_time": "2026-10-15T13:35:00Z"}], "evaluations": []}

        with patch.object(driver.core, "signals", side_effect=signal_output):
            result = driver.OneShotDriver(self.root / "clocked", self.config_dir, call,
                lambda: current["value"]).run_once()
        self.assertEqual(result["status"], "processed")
        for arm in ("A", "B"):
            state = json.loads((self.root / "clocked" / "arms" / (arm + ".json")).read_text())
            self.assertEqual(len(state["positions"]), 1)

    def test_developing_history_bar_is_not_cached_and_final_bar_is_fetched(self):
        state_dir = self.root / "bars"
        now = {"value": self.at("2026-10-15T13:35:05Z")}

        def first_call(tool, args):
            self.assertEqual(tool, "get_equity_historicals")
            return {"results": [{"symbol": "SPY", "bars": [
                {"begins_at": "2026-10-15T13:30:00Z", "open_price": 100, "high_price": 101, "low_price": 99, "close_price": 100, "volume": 1, "session": "reg"},
                {"begins_at": "2026-10-15T13:35:00Z", "open_price": 100, "high_price": 102, "low_price": 99, "close_price": 101, "volume": 1, "session": "reg"},
            ]}]}

        first = driver.OneShotDriver(state_dir, self.config_dir, first_call, lambda: now["value"])
        cached = first._load_bars(["SPY"])["SPY"]
        self.assertEqual([row["begins_at"] for row in cached], ["2026-10-15T13:30:00Z"])

        now["value"] = self.at("2026-10-15T13:40:05Z")
        requests = []

        def second_call(tool, args):
            requests.append(args)
            return {"results": [{"symbol": "SPY", "bars": [
                {"begins_at": "2026-10-15T13:35:00Z", "open_price": 100, "high_price": 102, "low_price": 99, "close_price": 101, "volume": 2, "session": "reg"},
                {"begins_at": "2026-10-15T13:40:00Z", "open_price": 101, "high_price": 103, "low_price": 100, "close_price": 102, "volume": 1, "session": "reg"},
            ]}]}

        second = driver.OneShotDriver(state_dir, self.config_dir, second_call, lambda: now["value"])
        cached = second._load_bars(["SPY"])["SPY"]
        self.assertEqual(requests[0]["start_time"], "2026-10-15T13:35:00Z")
        self.assertEqual([row["begins_at"] for row in cached], ["2026-10-15T13:30:00Z", "2026-10-15T13:35:00Z"])

    def test_flat_midday_run_skips_all_provider_calls(self):
        calls = []
        now = self.at("2026-10-15T15:59:00Z")
        desk = driver.OneShotDriver(self.root / "midday", self.config_dir,
            lambda tool, args: calls.append((tool, args)) or {}, lambda: now)
        desk.clock = lambda: self.at("2026-10-15T16:00:00Z")
        desk._refresh_clock()
        result = desk.run_once()
        self.assertEqual(result["status"], "outside_active_window")
        self.assertEqual(calls, [])
        self.assertFalse((self.root / "midday").exists())

    def test_main_skips_transport_initialization_when_schedule_is_closed(self):
        state_dir = self.root / "closed-main"
        output = io.StringIO()
        with patch.object(driver.OneShotDriver, "scheduled_mode", return_value="calendar_closed"), \
             patch.object(driver, "_cloud_session", side_effect=AssertionError("transport must not initialize")):
            with redirect_stdout(output):
                code = driver.main(["--state-dir", str(state_dir), "--config-dir", str(self.config_dir)])
        self.assertEqual(code, 0)
        self.assertIn('"status": "calendar_closed"', output.getvalue())
        self.assertFalse(state_dir.exists())

    def test_closed_market_preflight_reports_stale_and_does_not_transition_arms(self):
        now = self.at("2026-10-15T20:00:00Z")

        def call(tool, args):
            if tool == "get_equity_historicals":
                return {"results": [{"symbol": symbol, "bars": []} for symbol in args["symbols"]]}
            if tool == "get_equity_quotes":
                return {"results": [{"quote": {"symbol": symbol, "last_trade_price": 100,
                    "venue_last_trade_time": "2026-10-15T19:59:00Z"}} for symbol in args["symbols"]]}
            if tool == "get_option_chains":
                return self.chain_response("IWM")
            if tool == "search":
                return self.search_response(args["query"])
            if tool == "get_option_instruments":
                return self.instrument_response("IWM")
            if tool == "get_option_quotes":
                return {"results": [{"quote": {"instrument_id": ident, "bid_price": 1,
                    "ask_price": 1.02, "bid_size": 2, "ask_size": 2,
                    "updated_at": "2026-10-15T19:00:00Z"}} for ident in args["instrument_ids"]]}
            self.fail("unexpected provider tool " + tool)

        state = self.root / "preflight"
        result = driver.OneShotDriver(state, self.config_dir, call, lambda: now).run_once(preflight=True)
        self.assertEqual(result["status"], "stale_market_data")
        self.assertFalse(result["ready"])
        self.assertEqual(result["paper_transitions"], 0)
        self.assertFalse((state / "arms").exists())
        self.assertGreaterEqual(result["option_quote_count"], 1)


class ArchivedTSLAExpiryTests(unittest.TestCase):
    def test_actual_october_20_tsla_chain_selects_friday(self):
        # Archived read-only Robinhood chain: 20261020-0049-get_option_chains.json.
        listed = ["2026-10-23", "2026-10-26", "2026-10-28", "2026-10-30"]
        self.assertEqual(driver.choose_expiry(listed, date(2026, 10, 20)), "2026-10-23")


if __name__ == "__main__":
    unittest.main()
