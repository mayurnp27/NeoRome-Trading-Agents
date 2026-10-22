import hashlib
import tempfile
import unittest
from pathlib import Path

from neorome.btc_bracket_engine import BracketError
from neorome.btc_trailing_engine import TrailingEngine


def digest(label):
    return hashlib.sha256(label.encode()).hexdigest()


def quote(at, label, *, bid=10, bid_qty=10000, ask=20, ask_qty=10000):
    return {
        "received_at": at,
        "source_hash": digest(label),
        "yes_bids": [{"price_cents": bid, "quantity": bid_qty}],
        "yes_asks": [{"price_cents": ask, "quantity": ask_qty}],
        "no_bids": [{"price_cents": 10, "quantity": 10000}],
        "no_asks": [{"price_cents": 20, "quantity": 10000}],
    }


class TrailingEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "trailing.sqlite"
        self.engine = TrailingEngine(self.path, started_at=0)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def enter(self, timeframe="5m"):
        ticker = f"BTC-{timeframe}"
        self.engine.register_cohort({
            "cohort_id": f"cohort-{timeframe}", "timeframe": timeframe,
            "ticker": ticker, "side": "yes", "observed_at": 1,
            "signal_time": 0, "close_time": 100,
            "fee_multiplier": "1", "source_hash": digest("cohort" + timeframe),
        })
        self.engine.on_quote(ticker, quote(2, "first" + timeframe))
        self.engine.on_quote(ticker, quote(4, "entry" + timeframe))
        arm = next(a for a in self.engine.status(4)["arms"] if a["timeframe"] == timeframe)
        self.assertIsNotNone(arm["position"])
        return ticker, arm["position"]["quantity"], arm["position"]["cost_cents"]

    def send_bid(self, ticker, at, label, bid, qty=10000):
        self.engine.on_quote(ticker, quote(at, label, bid=bid, bid_qty=qty))

    def test_two_independent_accounts_budget_fee_inclusive_twenty_percent(self):
        arms = self.engine.status(0)["arms"]
        self.assertEqual({a["arm_id"] for a in arms}, {
            "5m-trailing-a50-d50", "30m-trailing-a50-d50"})
        self.assertEqual([a["initial_cash_cents"] for a in arms], [50000, 50000])
        self.enter("5m")
        self.enter("30m")
        for arm in self.engine.status(4)["arms"]:
            self.assertIsNotNone(arm["position"])
            pos = arm["position"]
            self.assertEqual(pos["allocation_bps"], 2000)
            self.assertLessEqual(pos["cost_cents"], pos["allocation_budget_cents"])
            self.assertEqual(pos["available_cash_before_cents"], 50000)
        self.assertEqual({a["timeframe"] for a in self.engine.status(4)["arms"]}, {"5m", "30m"})

    def test_activation_uses_fee_aware_net_value_and_trails_peak(self):
        ticker, _, cost = self.enter()
        self.send_bid(ticker, 5, "below-activation", 32)
        arm = next(a for a in self.engine.status(5)["arms"] if a["position"])
        self.assertIsNone(arm["position"].get("trailing_activated_at"))
        self.send_bid(ticker, 6, "activate", 36)
        arm = next(a for a in self.engine.status(6)["arms"] if a["position"])
        pos = arm["position"]
        self.assertGreaterEqual(pos["trailing_highwater_net_cents"] * 100, cost * 150)
        self.assertEqual(pos["trailing_activation_source_hash"], digest("activate"))
        self.send_bid(ticker, 7, "new-peak", 50)
        arm = next(a for a in self.engine.status(7)["arms"] if a["position"])
        self.assertGreater(arm["position"]["trailing_highwater_net_cents"], pos["trailing_highwater_net_cents"])
        peak = arm["position"]["trailing_highwater_net_cents"]
        self.send_bid(ticker, 8, "halfway", 25)
        arm = next(a for a in self.engine.status(8)["arms"] if a["position"])
        trigger = arm["position"]["trailing_trigger"]
        self.assertEqual(trigger["source_hash"], digest("halfway"))
        self.assertEqual(trigger["highwater_net_cents"], peak)

    def test_delayed_distinct_exit_hash_partial_fill_is_sticky_and_settles_residual(self):
        ticker, quantity, _ = self.enter()
        self.send_bid(ticker, 5, "activation", 40)
        self.send_bid(ticker, 6, "peak", 55)
        self.send_bid(ticker, 7, "trigger", 25)
        self.send_bid(ticker, 7.5, "trigger", 0)  # Same hash cannot execute the trigger quote.
        # Partial visible depth is sufficient after the sticky trigger.
        self.send_bid(ticker, 8, "partial", 20, qty=5)
        arm = next(a for a in self.engine.status(8)["arms"] if a["position"])
        self.assertEqual(arm["position"]["quantity"], quantity - 5)
        self.assertEqual(arm["position"]["trailing_used_exit_hashes"], [digest("partial")])
        # The fill hash is permanently excluded; a repeated hash does not fill again.
        self.send_bid(ticker, 9, "partial", 20, qty=10000)
        arm = next(a for a in self.engine.status(9)["arms"] if a["position"])
        self.assertEqual(arm["position"]["quantity"], quantity - 5)
        # Settlement remains authoritative for the residual and accounts accrued fees.
        self.engine.on_settlement(ticker, "no", 100, digest("settlement"))
        final = next(a for a in self.engine.status(100)["arms"] if a["timeframe"] == "5m")
        self.assertIsNone(final["position"])
        self.assertEqual(final["trades"], 1)
        self.assertEqual(final["losses"], 1)
        self.engine.on_settlement(ticker, "no", 100, digest("settlement-replay"))

    def test_one_second_minimum_and_trigger_quote_exclusion(self):
        ticker, quantity, _ = self.enter()
        self.send_bid(ticker, 5, "activation", 40)
        self.send_bid(ticker, 6, "peak", 55)
        self.send_bid(ticker, 7, "trigger", 25)
        self.send_bid(ticker, 7.999, "too-soon", 10)
        arm = next(a for a in self.engine.status(7.999)["arms"] if a["position"])
        self.assertEqual(arm["position"]["quantity"], quantity)
        self.send_bid(ticker, 8, "trigger", 10)
        arm = next(a for a in self.engine.status(8)["arms"] if a["position"])
        self.assertEqual(arm["position"]["quantity"], quantity)
        self.send_bid(ticker, 8.01, "valid", 10, qty=1)
        arm = next(a for a in self.engine.status(8.01)["arms"] if a["position"])
        self.assertLess(arm["position"]["quantity"], quantity)

    def test_restart_persists_highwater_trigger_and_hash_exclusions(self):
        ticker, quantity, _ = self.enter()
        self.send_bid(ticker, 5, "activation", 40)
        self.send_bid(ticker, 6, "peak", 55)
        self.send_bid(ticker, 7, "trigger", 25)
        self.engine.close()
        self.engine = TrailingEngine(self.path, started_at=0)
        arm = next(a for a in self.engine.status(7)["arms"] if a["position"])
        self.assertEqual(arm["position"]["trailing_trigger"]["source_hash"], digest("trigger"))
        self.send_bid(ticker, 8, "partial", 20, qty=3)
        self.engine.close()
        self.engine = TrailingEngine(self.path, started_at=0)
        arm = next(a for a in self.engine.status(8)["arms"] if a["position"])
        self.assertEqual(arm["position"]["quantity"], quantity - 3)
        self.assertEqual(arm["position"]["trailing_used_exit_hashes"], [digest("partial")])
        with self.assertRaises(BracketError):
            TrailingEngine(self.path, started_at=1)

    def test_configuration_mismatch_and_base_database_are_rejected(self):
        with self.assertRaises(BracketError):
            TrailingEngine(self.path, started_at=1)
        base_path = Path(self.tmp.name) / "base.sqlite"
        from neorome.btc_bracket_engine import Engine
        base_engine = Engine(base_path, started_at=0)
        base_engine.close()
        with self.assertRaises(BracketError):
            TrailingEngine(base_path, started_at=0)

    def test_no_full_depth_cannot_activate_and_quotes_reject_duplicate_time(self):
        ticker, quantity, _ = self.enter()
        self.send_bid(ticker, 5, "shallow", 60, qty=quantity - 1)
        arm = next(a for a in self.engine.status(5)["arms"] if a["position"])
        self.assertIsNone(arm["position"].get("trailing_activated_at"))
        with self.assertRaises(BracketError):
            self.send_bid(ticker, 5, "same-time", 60)
        with self.assertRaises(BracketError):
            self.send_bid(ticker, 4.9, "out-of-order", 60)
        self.engine.on_quote(ticker, quote(100, "close", bid=60))
        arm = next(a for a in self.engine.status(100)["arms"] if a["position"])
        self.assertIsNone(arm["position"].get("trailing_activated_at"))
        self.engine.on_settlement(ticker, "no", 100, digest("settle"))
        self.engine.on_settlement(ticker, "no", 100, digest("settle"))


if __name__ == "__main__":
    unittest.main()
