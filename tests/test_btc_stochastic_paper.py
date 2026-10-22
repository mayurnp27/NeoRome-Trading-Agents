from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

WORKTREE_ROOT = Path(__file__).resolve().parents[1]
MAIN_ROOT = Path("/home/neorome/Documents/ChatGPT/NeoRome - Agentic Trading Firm")
for path in (str(MAIN_ROOT), str(WORKTREE_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import neorome
neorome.__path__.insert(0, str(WORKTREE_ROOT / "neorome"))
from neorome.paper_ledger import MarketRiskPolicy, PaperLedger, Proposal, RiskLimits
from neorome.btc_stochastic_paper import (
    BTCStochasticPaperError,
    fee_cents,
    initialize,
    run_once,
    set_manual_stop,
    snapshot,
    _book,
)

UTC = timezone.utc
OPEN = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
CLOSE = OPEN + timedelta(minutes=15)
TICKER = "KXBTC15M-26OCT141200"
RULES = ("YES if the average of CF Benchmarks Bitcoin Real Time Index (BRTI) values observed "
         "at the start of each of the sixty seconds before expiration is at least the floor strike.")


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def at(seconds):
    return (OPEN + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def make_book(receipt, *, no_bid="0.41", no_quantity="10", yes_bid="0.58", yes_quantity="10", source="a"):
    return {
        "rest_date": at(receipt - 1),
        "received_at": at(receipt),
        "source_hash": digest(source + str(receipt) + str(no_bid) + str(yes_bid)),
        "orderbook_fp": {
            "yes_dollars": [[yes_bid, yes_quantity]] if yes_bid is not None else [],
            "no_dollars": [[no_bid, no_quantity]] if no_bid is not None else [],
        },
    }


def make_input(*, method="fresh_cross", timeframe=30, direction="bullish", ticker=TICKER, book2=True, book1=None,
               second=None, signal_id="btc-stoch-30m-turn-1791979200", status="active"):
    bullish = direction == "bullish"
    return {
        "signal": {
            "signal_id": signal_id,
            "instrument": "BTC-USD",
            "timeframe_minutes": timeframe,
            "method": method,
            "bar_close_timestamp": int(OPEN.timestamp()),
            "observed_receipt_at": at(1),
            "source_hash": digest("coinbase-closed-bar"),
            "valid": True,
            "previous_k": 19.5 if bullish else 80.5,
            "previous_d": 22.0 if bullish else 80.0,
            "k": 21.0 if bullish else 79.0,
            "d": 20.5 if bullish else 80.0,
            "direction": direction,
            "contract_side": "yes" if bullish else "no",
            ("oversold" if bullish else "overbought"): 20 if bullish else 80,
        },
        "market": {
            "ticker": ticker,
            "exchange_index": 2,
            "status": status,
            "open_time": at(0),
            "close_time": CLOSE.isoformat().replace("+00:00", "Z"),
            "floor_strike": "65000.00",
            "rules_primary": RULES,
            "source_hash": digest("market-metadata"),
            "venue": "kalshi",
        },
        "fee": {"fee_type": "quadratic", "fee_multiplier": "1", "source_hash": digest("fee-metadata")},
        "book1": book1 or make_book(2, source="book1"),
        "book2": second if second is not None else (make_book(4, source="book2") if book2 else None),
    }


class BTCStochasticPaperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "paper"

    def tearDown(self):
        self.temp.cleanup()

    def test_reopen_preserves_funded_cash_and_history(self):
        book = initialize(self.root)
        self.assertEqual(next(x for x in book.balances() if x.desk == "prediction").available_cash_cents, 50_000)
        run_once(make_input(), self.root, at(10))
        before = book.balances()
        self.assertEqual(initialize(self.root).balances(), before)
        self.assertEqual(initialize(self.root).audit(), book.audit())

    def test_legacy_position_without_side_defaults_to_yes_for_official_settlement(self):
        book = initialize(self.root)
        legacy_marker = {"profile_version": "btc-stochastic-paper-v1",
                         "market": {"ticker": TICKER, "close_time": CLOSE.isoformat().replace("+00:00", "Z")},
                         "fee": {"fee_multiplier": "1"}}
        proposal = Proposal(
            proposal_id="legacy-yes-proposal", desk="prediction", strategy="STOCH-BTC30-TURN-v1",
            strategy_version="v1", source_hash=digest("legacy-source"), source_timestamp=OPEN,
            instrument=TICKER, exposure_group="prediction:" + TICKER, idea_id="legacy-yes-proposal",
            expiry_at=CLOSE, quantity=2, unit_cost_cents=40, fee_cents=2, provenance="observed",
            metadata={"neorome.btc_stochastic_paper": legacy_marker})
        reservation = book.reserve(proposal, now=OPEN)
        position = book.simulate_fill(reservation.reservation_id, fill_id="legacy-yes-fill", quantity=2,
                                      unit_cost_cents=40, fee_cents=2, provenance="observed",
                                      filled_at=OPEN + timedelta(seconds=1))
        self.assertEqual(position.quantity, 2)
        settled = run_once({"settlement": {"ticker": TICKER, "status": "settled", "result": "yes",
                                            "observed_at": CLOSE.isoformat().replace("+00:00", "Z"),
                                            "source_hash": digest("legacy-yes-settlement")}},
                           self.root, CLOSE + timedelta(seconds=5))
        self.assertEqual(settled["contract_side"], "yes")
        self.assertEqual(settled["close"]["proceeds_cents"], 200)

    def test_fee_uses_quadratic_formula_and_rounds_up_to_cents(self):
        self.assertEqual(fee_cents([50], "1"), 2)
        self.assertEqual(fee_cents([50], "2"), 4)
        self.assertEqual(fee_cents([61] * 8, "1"), 14)

    def test_captured_kalshi_book_supports_http_date_four_decimals_and_fractional_depth(self):
        captured = {
            "rest_date": "Wed, 14 Oct 2026 05:14:53 GMT",
            "received_at": "2026-10-14T05:14:53.600310+00:00",
            "source_hash": digest("captured-kalshi-book-probe"),
            "orderbook_fp": {
                "no_dollars": [["0.9930", "250.00"], ["0.9950", "206.00"],
                               ["0.9970", "4254.41"], ["0.9980", "800.00"],
                               ["0.9990", "147599.53"]],
                "yes_dollars": [],
            },
        }
        parsed = _book(captured, datetime.fromisoformat("2026-10-14T05:14:54+00:00"), "captured")
        self.assertEqual(parsed["yes_bids"], [])
        self.assertEqual([row["price_cents"] for row in parsed["yes_asks"]], [1] * 5)
        self.assertEqual([row["price_cents"] for row in parsed["no_bids"]], [99] * 5)
        self.assertEqual(parsed["no_asks"], [])
        self.assertEqual([row["quantity"] for row in parsed["yes_asks"]], [147599, 800, 4254, 206, 250])

    def test_book_normalizes_both_contract_sides_with_conservative_complements(self):
        value = make_book(2, yes_bid="0.581", no_bid="0.413")
        parsed = _book(value, datetime.fromisoformat(at(10).replace("Z", "+00:00")), "sides")
        self.assertEqual(parsed["yes_bids"][0]["price_cents"], 58)
        self.assertEqual(parsed["yes_asks"][0]["price_cents"], 59)
        self.assertEqual(parsed["no_bids"][0]["price_cents"], 41)
        self.assertEqual(parsed["no_asks"][0]["price_cents"], 42)

    def test_book_ladders_accept_either_monotonic_order_and_reject_duplicates_or_crossed_subcents(self):
        now = datetime.fromisoformat(at(10).replace("Z", "+00:00"))
        for ladder in (["0.40", "0.45"], ["0.45", "0.40"]):
            with self.subTest(ladder=ladder):
                value = make_book(2)
                value["orderbook_fp"]["yes_dollars"] = []
                value["orderbook_fp"]["no_dollars"] = [[price, "2"] for price in ladder]
                parsed = _book(value, now, "ladder")
                self.assertEqual([row["price_cents"] for row in parsed["yes_asks"]], [55, 60])
        duplicate = make_book(2)
        duplicate["orderbook_fp"]["no_dollars"] = [["0.40", "1"], ["0.40", "1"]]
        with self.assertRaisesRegex(BTCStochasticPaperError, "duplicate"):
            _book(duplicate, now, "duplicate")
        unordered = make_book(2)
        unordered["orderbook_fp"]["no_dollars"] = [["0.40", "1"], ["0.45", "1"], ["0.42", "1"]]
        with self.assertRaisesRegex(BTCStochasticPaperError, "monotonic"):
            _book(unordered, now, "unordered")
        overprecision = make_book(2)
        overprecision["orderbook_fp"]["no_dollars"] = [["0.40001", "1"]]
        with self.assertRaises(BTCStochasticPaperError):
            _book(overprecision, now, "overprecision")
        crossed = make_book(2)
        crossed["orderbook_fp"] = {"yes_dollars": [["0.5001", "1"]], "no_dollars": [["0.5000", "1"]]}
        with self.assertRaisesRegex(BTCStochasticPaperError, "crossed"):
            _book(crossed, now, "subcent-cross")

    def test_primary_turn_uses_twenty_percent_available_cash_subject_to_depth_and_fills_once(self):
        result = run_once(make_input(), self.root, at(10))
        self.assertEqual(result["status"], "filled")
        fill = result["fill"]
        self.assertEqual(fill["quantity"], 10)
        self.assertGreater(fill["total_cost_cents"], 500)
        self.assertLessEqual(fill["total_cost_cents"], 10_000)
        state = snapshot(self.root, at(10))
        self.assertEqual(state["allocation_bps"], 2_000)
        self.assertEqual(state["allocation_policy_version"], "btc-cash20-v1")
        self.assertEqual(state["available_cash_before_cents"], 50_000)
        intent = next(event for event in state["audit_events"] if event["event_type"] == "intent")
        self.assertEqual(intent["payload"]["available_cash_before_cents"], 50_000)
        self.assertEqual(fill["provenance"], "observed")
        self.assertEqual(fill["execution_mode"], "internal_simulation")
        self.assertEqual(result["venue_orders_sent"], 0)
        self.assertEqual(snapshot(self.root, at(10))["active_position"]["instrument"], TICKER)
        replay = run_once(make_input(), self.root, at(10))
        self.assertEqual(replay["status"], "open")
        fills = [event for event in initialize(self.root).audit() if event.event_type == "fill"]
        self.assertEqual(len(fills), 1)

    def test_only_primary_30m_turn_and_oversold_20_are_accepted(self):
        for kwargs in (
            {"timeframe": 15}, {"timeframe": 60}, {"method": "cross"},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(BTCStochasticPaperError):
                run_once(make_input(**kwargs), self.root / str(kwargs), at(10))
        wrong_threshold = make_input()
        wrong_threshold["signal"]["oversold"] = 25
        with self.assertRaises(BTCStochasticPaperError):
            run_once(wrong_threshold, self.root / "threshold", at(10))
        wrong_direction = make_input()
        wrong_direction["signal"]["k"] = 18
        with self.assertRaises(BTCStochasticPaperError):
            run_once(wrong_direction, self.root / "direction", at(10))
        wrong_side = make_input(direction="bearish")
        wrong_side["signal"]["contract_side"] = "yes"
        with self.assertRaisesRegex(BTCStochasticPaperError, "direction and contract_side"):
            run_once(wrong_side, self.root / "side-mismatch", at(10))
        wrong_bearish_threshold = make_input(direction="bearish")
        wrong_bearish_threshold["signal"]["overbought"] = 79
        with self.assertRaisesRegex(BTCStochasticPaperError, "overbought-80"):
            run_once(wrong_bearish_threshold, self.root / "overbought", at(10))
        for index, changes in enumerate((
            {"previous_k": -5.0, "k": 1000.0},
            {"previous_k": -5.0, "k": 10.0},
            {"previous_k": 19.5, "k": 1000.0},
            {"previous_k": True, "k": 21.0},
            {"previous_k": "19.5", "k": 21.0},
        )):
            invalid = make_input()
            invalid["signal"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(BTCStochasticPaperError):
                run_once(invalid, self.root / f"invalid-k-{index}", at(10))

    def test_market_timing_identity_and_settlement_terms_are_required(self):
        bad_cases = []
        bad = make_input(ticker="KXBTC15M-TEST000000")
        bad_cases.append(bad)
        bad = make_input(); bad["market"]["exchange_index"] = 1; bad_cases.append(bad)
        bad = make_input(); bad["market"]["rules_primary"] = "BRTI price at expiration"; bad_cases.append(bad)
        bad = make_input(); bad["market"]["rules_primary"] = "NO if the 60-second CF Benchmarks BRTI average is at least the strike."; bad_cases.append(bad)
        bad = make_input(); bad["market"]["open_time"] = at(60); bad_cases.append(bad)
        bad = make_input(); bad["signal"]["observed_receipt_at"] = at(-1); bad_cases.append(bad)
        for index, item in enumerate(bad_cases):
            with self.subTest(index=index), self.assertRaises(BTCStochasticPaperError):
                run_once(item, self.root / str(index), at(10))
        with self.assertRaises(BTCStochasticPaperError):
            run_once(make_input(), self.root / "late", at(91))

    def test_fractional_display_depth_is_floored_to_whole_contracts(self):
        item = make_input()
        item["book1"]["orderbook_fp"]["no_dollars"] = [["0.41", "8.9"]]
        item["book2"]["orderbook_fp"]["no_dollars"] = [["0.41", "8.9"]]
        result = run_once(item, self.root, at(10))
        self.assertEqual(result["status"], "filled")
        self.assertEqual(result["fill"]["quantity"], 8)

    def test_book1_size_requires_full_displayed_depth_and_cost_budget(self):
        shallow = make_input()
        shallow["book1"]["orderbook_fp"]["no_dollars"] = [["0.41", "2"]]
        result = run_once(shallow, self.root, at(10))
        self.assertEqual(result["status"], "filled")
        self.assertEqual(result["fill"]["quantity"], 2)

        too_expensive = make_input(book1=make_book(2, no_bid="0.00", source="too-expensive"))
        abstain = run_once(too_expensive, self.root / "expensive", at(10))
        self.assertEqual(abstain["status"], "abstain")

    def test_two_observed_books_reserve_for_the_worse_still_eligible_price(self):
        second = make_book(4, no_bid="0.40", source="worse-book2")
        result = run_once(make_input(second=second), self.root, at(10))
        self.assertEqual(result["status"], "filled")
        self.assertLessEqual(result["fill"]["total_cost_cents"], 10_000)
        self.assertEqual(initialize(self.root).active_exposure_count("prediction"), 1)

    def test_pending_reservation_survives_restart_and_resumes_with_fresh_book2(self):
        pending_input = make_input(book2=False)
        pending = run_once(pending_input, self.root, at(3))
        self.assertEqual(pending["status"], "pending")
        self.assertEqual(initialize(self.root).active_exposure_count("prediction"), 1)
        restarted = initialize(self.root)
        self.assertEqual(restarted.active_exposure_count("prediction"), 1)
        resumed_input = make_input(book2=False)
        resumed_input["book2"] = make_book(5, source="resume-book2")
        result = run_once(resumed_input, self.root, at(10))
        self.assertEqual(result["status"], "filled")
        self.assertEqual(len([event for event in initialize(self.root).audit() if event.event_type == "fill"]), 1)

    def test_global_stop_or_recovery_blocker_cancels_pending_and_releases_cash(self):
        blockers = ("global_manual_stop", "recovery_not_fresh_and_verified",
                    "recovery_status_missing_or_invalid")
        for index, blocker in enumerate(blockers):
            root = self.root / str(index)
            with self.subTest(blocker=blocker):
                pending = run_once(make_input(book2=False), root, at(3))
                self.assertEqual(pending["status"], "pending")
                before = [row for row in initialize(root).balances() if row.desk == "prediction"][0]
                self.assertLess(before.available_cash_cents, 50_000)
                result = run_once({"entry_blocker_reason": blocker}, root, at(5))
                self.assertEqual(result["status"], "cancelled")
                after = [row for row in initialize(root).balances() if row.desk == "prediction"][0]
                self.assertEqual(after.available_cash_cents, 50_000)
                self.assertFalse(snapshot(root, at(5))["manual_stop_active"])

    def test_stale_or_too_early_book2_cancels_reservation(self):
        too_early = make_input(second=make_book(3, source="early"))
        result = run_once(too_early, self.root, at(10))
        self.assertEqual(result["status"], "cancelled")
        stale = make_input(book2=False)
        run_once(stale, self.root / "stale", at(3))
        stale["book2"] = make_book(4, source="stale")
        result = run_once(stale, self.root / "stale", at(30))
        self.assertEqual(result["status"], "cancelled")

        same_capture = make_input()
        same_capture["book2"]["source_hash"] = same_capture["book1"]["source_hash"]
        result = run_once(same_capture, self.root / "same-capture", at(10))
        self.assertEqual(result["status"], "cancelled")

    def test_pending_fill_expires_after_frozen_signal_window(self):
        pending = make_input(book2=False)
        result = run_once(pending, self.root, at(2))
        self.assertEqual(result["status"], "pending")
        pending["book2"] = make_book(100, source="late-book2")
        result = run_once(pending, self.root, at(102))
        self.assertEqual(result["status"], "cancelled")
        self.assertIn("entry window expired", result["reason"])
        self.assertEqual(len([event for event in initialize(self.root).audit() if event.event_type == "fill"]), 0)
        self.assertEqual(initialize(self.root).active_exposure_count("prediction"), 0)
        self.assertFalse(snapshot(self.root, at(102))["manual_stop_active"])

    def test_settled_yes_and_no_close_by_official_result_only_and_are_idempotent(self):
        for result in ("yes", "no"):
            root = self.root / result
            filled = run_once(make_input(), root, at(10))
            quantity = filled["fill"]["quantity"]
            settlement = {"ticker": TICKER, "status": "finalized", "result": result,
                          "observed_at": CLOSE.isoformat().replace("+00:00", "Z"),
                          "source_hash": digest("settlement-" + result)}
            close_input = make_input()
            close_input["settlement"] = settlement
            closed = run_once(close_input, root, CLOSE + timedelta(seconds=5))
            self.assertEqual(closed["status"], "settled")
            self.assertEqual(closed["close"]["proceeds_cents"], quantity * (100 if result == "yes" else 0))
            self.assertEqual(closed["close"]["fee_cents"], 0)
            replay = run_once(close_input, root, CLOSE + timedelta(seconds=6))
            self.assertEqual(replay["status"], "completed_replay")
            self.assertEqual(len([event for event in initialize(root).audit() if event.event_type == "close"]), 1)

    def test_bearish_signal_buys_no_marks_no_bid_and_settles_by_held_side(self):
        for official_result, expected in (("no", 100), ("yes", 0)):
            root = self.root / official_result
            item = make_input(direction="bearish", signal_id="btc-stoch-30m-bearish-turn-1791979200")
            filled = run_once(item, root, at(10))
            self.assertEqual(filled["status"], "filled")
            self.assertEqual(filled["fill"]["quantity"], 10)
            self.assertEqual(filled["mark"]["liquidation_value_cents"], 40 * filled["fill"]["quantity"] - filled["mark"]["exit_fee_cents"])
            settlement = {"ticker": TICKER, "status": "settled", "result": official_result,
                          "observed_at": CLOSE.isoformat().replace("+00:00", "Z"),
                          "source_hash": digest("no-settlement-" + official_result)}
            settled = run_once({"settlement": settlement}, root, CLOSE + timedelta(seconds=5))
            self.assertEqual(settled["contract_side"], "no")
            self.assertEqual(settled["close"]["proceeds_cents"], expected * filled["fill"]["quantity"])

    def test_missing_result_and_missing_bid_leave_open_position_unvalued(self):
        filled = run_once(make_input(), self.root, at(10))
        self.assertEqual(filled["status"], "filled")
        self.assertEqual(filled["mark"]["status"], "valued")
        open_state = run_once({}, self.root, at(12))
        self.assertEqual(open_state["status"], "open")
        self.assertEqual(open_state["mark"]["liquidation_value_cents"], None)

        no_bid = make_book(13, yes_bid=None, source="missing-bid")
        no_bid_input = {"mark_book": {**no_bid, "ticker": TICKER}}
        marked = run_once(no_bid_input, self.root, at(15))
        self.assertEqual(marked["mark"]["status"], "unvalued_missing_bid_depth")
        self.assertEqual(marked["mark"]["liquidation_value_cents"], None)
        self.assertEqual(initialize(self.root).active_exposure_count("prediction"), 1)

    def test_manual_stop_blocks_entry_and_cancels_pending_reservation(self):
        stopped_root = self.root / "stop-before-entry"
        set_manual_stop(stopped_root, True, now=at(1))
        blocked = run_once(make_input(), stopped_root, at(10))
        self.assertEqual(blocked["status"], "abstain")
        self.assertTrue(snapshot(stopped_root, at(10))["manual_stop_active"])

        pending_root = self.root / "stop-pending"
        run_once(make_input(book2=False), pending_root, at(3))
        set_manual_stop(pending_root, True, now=at(4))
        canceled = run_once(make_input(book2=False), pending_root, at(5))
        self.assertEqual(canceled["status"], "cancelled")
        self.assertEqual(initialize(pending_root).active_exposure_count("prediction"), 0)

    def test_stale_book1_and_invalid_fee_metadata_cannot_reserve(self):
        stale = make_input(book1=make_book(-20, source="stale-book1"))
        with self.assertRaises(BTCStochasticPaperError):
            run_once(stale, self.root / "stale-book1", at(10))
        fee = make_input(); fee["fee"]["fee_type"] = "linear"
        with self.assertRaises(BTCStochasticPaperError):
            run_once(fee, self.root / "fee", at(10))

    def test_audit_events_are_immutable_and_expose_only_simulated_execution(self):
        run_once(make_input(), self.root, at(10))
        same_quote = {"mark_book": {**make_book(4, source="book2"), "ticker": TICKER}}
        repeated_mark = run_once(same_quote, self.root, at(11))
        self.assertEqual(repeated_mark["mark"]["status"], "valued")
        state = snapshot(self.root, at(11))
        self.assertEqual(state["venue_orders_sent"], 0)
        self.assertEqual(state["execution_mode"], "internal_simulation")
        self.assertIsNotNone(state["market_snapshot"])
        self.assertIsNotNone(state["market_snapshot"]["market_nav_cents"])
        self.assertEqual(state["ledger_snapshot"]["nav_basis"], "cost")
        event_ids = [row["event_id"] for row in state["audit_events"]]
        self.assertEqual(len(event_ids), len(set(event_ids)))
        self.assertTrue(any(row["event_type"] == "intent" for row in state["audit_events"]))

    def test_isolated_risk_policy_does_not_latch_at_the_shared_default_loss_threshold(self):
        policy = initialize(self.root).market_risk_policy
        self.assertEqual(policy.daily_loss_bps, 10_000)
        self.assertEqual(policy.max_drawdown_bps, 10_000)
        input_map = make_input(
            book1=make_book(2, no_bid="0.76", no_quantity="19", yes_bid="0.20", yes_quantity="19"),
            second=make_book(4, no_bid="0.76", no_quantity="19", yes_bid="0.20", yes_quantity="19"),
        )
        filled = run_once(input_map, self.root, at(10))
        self.assertEqual(filled["fill"]["total_cost_cents"], 500)
        zero_bid = make_book(11, no_bid="0.76", no_quantity="19", yes_bid="0.00", yes_quantity="19")
        marked = run_once({"mark_book": {**zero_bid, "ticker": TICKER}}, self.root, at(12))
        self.assertEqual(marked["mark"]["liquidation_value_cents"], 0)
        self.assertFalse(initialize(self.root).risk_stop_active())
        market_state = snapshot(self.root, at(12))["market_snapshot"]
        self.assertEqual(market_state["market_nav_cents"], 49_500)
        self.assertFalse(market_state["automatic_risk_latched"])


if __name__ == "__main__":
    unittest.main()
