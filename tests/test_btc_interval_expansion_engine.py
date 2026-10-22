import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from neorome.btc_bracket_engine import BracketError, Engine as BracketEngine
from neorome.btc_interval_expansion_engine import ExpansionEngine
from neorome.btc_sma1m_engine import SMAEngine


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def quote(at, label, *, bid=20, ask=20, bid_qty=10000, ask_qty=10000):
    return {
        "received_at": at,
        "source_hash": digest(label),
        "yes_bids": [{"price_cents": bid, "quantity": bid_qty}],
        "yes_asks": [{"price_cents": ask, "quantity": ask_qty}],
        "no_bids": [{"price_cents": bid, "quantity": bid_qty}],
        "no_asks": [{"price_cents": ask, "quantity": ask_qty}],
    }


class IntervalExpansionEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "expansion.sqlite3"
        self.engine = ExpansionEngine(self.path, started_at=0)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def cohort(self, group, *, ticker=None, at=1, close=100, side="yes", cid=None):
        return {
            "cohort_id": cid or f"{group}-{ticker or 'BTC'}-{at}",
            "timeframe": group,
            "ticker": ticker or f"BTC-{group}",
            "side": side,
            "observed_at": at,
            "signal_time": at,
            "close_time": close,
            "fee_multiplier": "1",
            "source_hash": digest(f"signal-{cid or group}-{ticker}-{at}"),
        }

    def account(self, group, suffix="hold"):
        return next(a for a in self.engine.status(10)["arms"]
                    if a["timeframe"] == group and a["arm_id"].endswith(suffix))

    def enter(self, group, *, ticker=None, side="yes", bid=20, ask=20, cid=None):
        ticker = ticker or f"BTC-{group}"
        self.engine.register_cohort(self.cohort(group, ticker=ticker, side=side, cid=cid))
        self.engine.on_quote(ticker, quote(2, f"{group}-first-{cid}", bid=bid, ask=ask))
        self.engine.on_quote(ticker, quote(4, f"{group}-second-{cid}", bid=bid, ask=ask))
        return ticker

    def test_frozen_34_and_optional_54_accounts_are_independent_of_existing_engines(self):
        status = self.engine.status(0)
        self.assertEqual(len(status["arms"]), 34)
        self.assertEqual({a["initial_cash_cents"] for a in status["arms"]}, {50000})
        self.assertFalse(status["config"]["include_reversal_brackets"])
        self.assertEqual(len({a["arm_id"] for a in status["arms"]}), 34)
        optional_path = Path(self.tmp.name) / "optional.sqlite3"
        optional = ExpansionEngine(optional_path, started_at=0, include_reversal_brackets=True)
        try:
            self.assertEqual(len(optional.status(0)["arms"]), 54)
            self.assertTrue(optional.status(0)["config"]["include_reversal_brackets"])
            optional.close()
            with self.assertRaises(BracketError):
                ExpansionEngine(optional_path, started_at=0)
        finally:
            try:
                optional.close()
            except sqlite3.Error:
                pass
        base_path = Path(self.tmp.name) / "base.sqlite3"
        base_engine = BracketEngine(base_path, started_at=0)
        self.assertEqual(len(base_engine.status(0)["arms"]), 22)
        base_engine.close()
        sma_path = Path(self.tmp.name) / "sma.sqlite3"
        sma_engine = SMAEngine(sma_path, started_at=0)
        self.assertEqual(len(sma_engine.status(0)["arms"]), 4)
        sma_engine.close()

    def test_each_account_spends_twenty_percent_and_busy_arm_does_not_block_flat_peer(self):
        ticker = "BTC-MULTI"
        self.enter("15m-original", ticker=ticker, cid="original")
        self.engine.register_cohort(self.cohort("15m-comparison", ticker=ticker, at=5, cid="comp"))
        self.engine.on_quote(ticker, quote(6, "comp-first"))
        self.engine.on_quote(ticker, quote(8, "comp-second"))
        original = self.account("15m-original")
        comp = self.account("15m-comparison", "tp10-sl10")
        self.assertIsNotNone(original["position"])
        self.assertIsNotNone(comp["position"])
        for arm in (original, comp):
            pos = arm["position"]
            self.assertEqual(pos["allocation_bps"], 2000)
            self.assertLessEqual(pos["cost_cents"], 10000)
        self.assertEqual(original["position"]["available_cash_before_cents"], 50000)
        self.assertEqual(comp["position"]["available_cash_before_cents"], 50000)

    def test_distinct_group_cohort_processes_every_arm_without_shared_position_state(self):
        ticker = "BTC-BRACKETS"
        self.engine.register_cohort(self.cohort("1m-sma-brackets", ticker=ticker, cid="sma"))
        self.engine.on_quote(ticker, quote(2, "one"))
        self.engine.on_quote(ticker, quote(4, "two"))
        arms = [a for a in self.engine.status(4)["arms"]
                if a["timeframe"] == "1m-sma-brackets"]
        self.assertEqual(len(arms), 10)
        self.assertTrue(all(a["position"] is not None for a in arms))
        self.assertTrue(all(a["position"]["allocation_bps"] == 2000 for a in arms))
        self.assertTrue(all(a["position"]["cost_cents"] <= 10000 for a in arms))
        self.assertTrue(all(a["position"] is None for a in self.engine.status(4)["arms"]
                            if a["timeframe"] != "1m-sma-brackets"))
        flat_arm_id = "1m-sma-tp10-sl20"
        flat = next(a for a in arms if a["arm_id"] == flat_arm_id)
        self.engine.db.execute(
            "UPDATE arms SET position=NULL,open_cost=0,available_cash=50000 WHERE arm_id=?",
            (flat_arm_id,),
        )
        self.engine.register_cohort(self.cohort("1m-sma-brackets", ticker=ticker, at=5, cid="sma-next"))
        self.engine.on_quote(ticker, quote(6, "next-one"))
        self.engine.on_quote(ticker, quote(8, "next-two"))
        updated = next(a for a in self.engine.status(8)["arms"] if a["arm_id"] == flat_arm_id)
        self.assertIsNotNone(updated["position"], "a flat peer may enter while other arms are busy")
        events = [json.loads(row[0]) for row in self.engine.db.execute(
            "SELECT payload FROM events WHERE kind='skip' AND arm_id=?", (flat_arm_id,))]
        self.assertFalse(any(item.get("reason") == "position_already_open" for item in events))
        busy_events = [json.loads(row[0]) for row in self.engine.db.execute(
            "SELECT payload FROM events WHERE kind='skip' AND arm_id=?", (arms[0]["arm_id"],))]
        self.assertTrue(any(item.get("reason") == "position_already_open" for item in busy_events))

    def test_one_minute_deadline_is_30_seconds_and_fifteen_minute_is_90(self):
        self.engine.register_cohort(self.cohort("1m-sma-brackets", ticker="BTC-1", at=10, cid="m1"))
        self.engine.register_cohort(self.cohort("15m-original", ticker="BTC-15", at=10, cid="m15"))
        self.engine.on_quote("BTC-1", quote(11, "m1-first"))
        self.engine.on_quote("BTC-15", quote(11, "m15-first"))
        self.engine.expire(40)
        counts = self.engine.status(40)["counts"]
        self.assertEqual(counts["skipped"], 1)
        self.assertEqual(counts["waiting"], 1)
        self.engine.on_quote("BTC-15", quote(13, "m15-second"))
        original = self.account("15m-original")
        self.assertIsNotNone(original["position"])
        with self.assertRaises(BracketError):
            self.engine.register_cohort({**self.cohort("1m-sma-brackets", at=41, cid="stale"),
                                         "signal_time": 10})

    def test_bracket_trigger_waits_for_later_quote_and_partial_residual_settles_after_restart(self):
        ticker = "BTC-EXIT"
        self.enter("1m-sma-brackets", ticker=ticker, cid="exit")
        arm_id = "1m-sma-tp10-sl10"
        arm = next(a for a in self.engine.status(4)["arms"] if a["arm_id"] == arm_id)
        quantity = arm["position"]["quantity"]
        self.engine.on_quote(ticker, quote(5, "tp-trigger", bid=40))
        arm = next(a for a in self.engine.status(5)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["trigger"]["reason"], "take_profit")
        self.engine.on_quote(ticker, quote(5.9, "too-soon", bid=40, bid_qty=1))
        arm = next(a for a in self.engine.status(5.9)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], quantity)
        self.engine.on_quote(ticker, quote(6, "partial", bid=40, bid_qty=1))
        arm = next(a for a in self.engine.status(6)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], quantity - 1)
        position = arm["position"]
        self.engine.close()
        self.engine = ExpansionEngine(self.path, started_at=0)
        self.engine.on_quote(ticker, quote(7, "partial", bid=40, bid_qty=10000))
        arm = next(a for a in self.engine.status(7)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], quantity - 1)
        self.assertEqual(arm["position"]["accrued_net_proceeds_cents"],
                         position["accrued_net_proceeds_cents"])
        self.engine.on_settlement(ticker, "yes", 100, digest("settlement"))
        arm = next(a for a in self.engine.status(100)["arms"] if a["arm_id"] == arm_id)
        self.assertIsNone(arm["position"])
        self.assertEqual(arm["trades"], 1)

    def test_entry_quote_may_trigger_bracket_but_cannot_execute_until_later_quote(self):
        ticker = "BTC-ENTRY-TRIGGER"
        self.enter("1m-sma-brackets", ticker=ticker, bid=40, cid="entry-trigger")
        arm_id = "1m-sma-tp10-sl10"
        arm = next(a for a in self.engine.status(4)["arms"] if a["arm_id"] == arm_id)
        quantity = arm["position"]["quantity"]
        self.assertEqual(arm["position"]["trigger"]["at"], 4)
        self.assertEqual(arm["position"]["trigger"]["reason"], "take_profit")
        self.engine.on_quote(ticker, quote(4.9, "entry-too-soon", bid=40, bid_qty=1))
        arm = next(a for a in self.engine.status(4.9)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], quantity)
        self.engine.on_quote(ticker, quote(5, "entry-delayed-fill", bid=40, bid_qty=1))
        arm = next(a for a in self.engine.status(5)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], quantity - 1)

    def test_trailing_behavior_is_limited_to_its_own_account_and_hold_never_exits(self):
        ticker = "BTC-TRAIL"
        self.enter("15m-trailing", ticker=ticker, cid="trail")
        self.engine.register_cohort(self.cohort("15m-original", ticker=ticker, cid="hold"))
        self.engine.on_quote(ticker, quote(5, "hold-first"))
        self.engine.on_quote(ticker, quote(7, "hold-second"))
        self.engine.on_quote(ticker, quote(8, "activate", bid=40))
        status = self.engine.status(8)
        trail = next(a for a in status["arms"] if a["timeframe"] == "15m-trailing")
        hold = next(a for a in status["arms"] if a["timeframe"] == "15m-original")
        self.assertIsNotNone(trail["position"].get("trailing_activated_at"))
        self.assertNotIn("trailing_activated_at", hold["position"])
        self.engine.on_quote(ticker, quote(9, "trail-peak", bid=60))
        self.engine.on_quote(ticker, quote(10, "trail-drop", bid=20))
        trail = next(a for a in self.engine.status(10)["arms"] if a["timeframe"] == "15m-trailing")
        self.assertIsNotNone(trail["position"].get("trailing_trigger"))
        self.engine.on_quote(ticker, quote(12, "drop-exit", bid=20))
        hold = next(a for a in self.engine.status(12)["arms"] if a["timeframe"] == "15m-original")
        self.assertIsNotNone(hold["position"])
        self.assertIsNone(hold["position"]["trigger"])

    def test_optional_reversal_is_sticky_delayed_partial_and_hash_persists(self):
        self.engine.close()
        self.path = Path(self.tmp.name) / "reversal.sqlite3"
        self.engine = ExpansionEngine(self.path, started_at=0, include_reversal_brackets=True)
        ticker = "BTC-REV"
        self.enter("1m-sma-reversal-brackets", ticker=ticker, cid="rev")
        arm_id = "1m-sma-reversal-tp10-sl10"
        self.engine.on_reversal("no", 5, digest("cross"), 5)
        initial = next(a for a in self.engine.status(5)["arms"] if a["arm_id"] == arm_id)
        start_quantity = initial["position"]["quantity"]
        self.engine.on_quote(ticker, quote(5.5, "early", bid=40, bid_qty=1))
        arm = next(a for a in self.engine.status(5.5)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], start_quantity)
        self.assertEqual(arm["position"]["trigger"]["reason"], "opposite_sma_cross",
                         "a later TP observation cannot replace the first reversal intent")
        self.engine.on_quote(ticker, quote(6, "rev-partial", bid=30, bid_qty=2))
        arm = next(a for a in self.engine.status(6)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["trigger"]["reason"], "opposite_sma_cross")
        used = arm["position"]["reversal_used_exit_hashes"]
        self.assertEqual(used, [digest("rev-partial")])
        left = arm["position"]["quantity"]
        self.engine.close()
        self.engine = ExpansionEngine(self.path, started_at=0, include_reversal_brackets=True)
        self.engine.on_quote(ticker, quote(7, "rev-partial", bid=30, bid_qty=10000))
        arm = next(a for a in self.engine.status(7)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["quantity"], left)
        with self.assertRaises(BracketError):
            ExpansionEngine(self.path, started_at=1, include_reversal_brackets=True)

    def test_busy_reversal_arm_does_not_block_flat_peer_from_opposite_cohort(self):
        self.engine.close()
        self.path = Path(self.tmp.name) / "reversal-peer.sqlite3"
        self.engine = ExpansionEngine(self.path, started_at=0, include_reversal_brackets=True)
        ticker = "BTC-REV-PEER"
        self.enter("1m-sma-reversal-brackets", ticker=ticker, cid="rev-peer-original")
        group_arms = [a for a in self.engine.status(4)["arms"]
                      if a["timeframe"] == "1m-sma-reversal-brackets"]
        flat_arm_id = "1m-sma-reversal-tp10-sl20"
        self.engine.db.execute(
            "UPDATE arms SET position=NULL,open_cost=0,available_cash=50000 WHERE arm_id=?",
            (flat_arm_id,),
        )
        self.engine.on_reversal("no", 5, digest("peer-reversal"), 5)
        self.engine.register_cohort(self.cohort("1m-sma-reversal-brackets", ticker=ticker,
                                                at=5, side="no", cid="rev-peer-opposite"))
        self.engine.on_quote(ticker, quote(5.5, "peer-first"))
        self.engine.on_quote(ticker, quote(7.5, "peer-second"))
        flat = next(a for a in self.engine.status(7.5)["arms"] if a["arm_id"] == flat_arm_id)
        self.assertIsNotNone(flat["position"])
        self.assertEqual(flat["position"]["side"], "no")
        busy_arm_id = next(a["arm_id"] for a in group_arms if a["arm_id"] != flat_arm_id)
        busy_skips = [json.loads(r[0]) for r in self.engine.db.execute(
            "SELECT payload FROM events WHERE kind='skip' AND arm_id=?", (busy_arm_id,))]
        self.assertTrue(any(payload.get("reason") == "position_already_open"
                            for payload in busy_skips))

    def test_first_take_profit_intent_is_not_replaced_by_later_reversal(self):
        self.engine.close()
        self.path = Path(self.tmp.name) / "tp-first.sqlite3"
        self.engine = ExpansionEngine(self.path, started_at=0, include_reversal_brackets=True)
        ticker = "BTC-TP-FIRST"
        self.enter("1m-sma-reversal-brackets", ticker=ticker, cid="tp-first")
        arm_id = "1m-sma-reversal-tp10-sl10"
        self.engine.on_quote(ticker, quote(5, "tp-first-trigger", bid=40))
        self.engine.on_reversal("no", 5.5, digest("later-cross"), 5.5)
        arm = next(a for a in self.engine.status(5.5)["arms"] if a["arm_id"] == arm_id)
        self.assertEqual(arm["position"]["trigger"]["reason"], "take_profit")
        self.engine.on_quote(ticker, quote(6, "tp-first-exit", bid=40))
        arm = next(a for a in self.engine.status(6)["arms"] if a["arm_id"] == arm_id)
        self.assertIsNone(arm["position"])

    def test_invalid_group_and_conflicting_cohort_are_rejected(self):
        cohort = self.cohort("15m-original", cid="conflict")
        self.engine.register_cohort(cohort)
        with self.assertRaises(BracketError):
            self.engine.register_cohort({**cohort, "side": "no"})
        with self.assertRaises(BracketError):
            self.engine.register_cohort({**cohort, "cohort_id": "bad", "timeframe": "30m"})


if __name__ == "__main__":
    unittest.main()
