"""Focused policy tests for the authoritative cloud paper-ledger snapshot."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from neorome import paper_ledger as ledger_module


NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
CRYPTO_POLICY = ledger_module.MarketRiskPolicy(daily_loss_bps=5000, max_drawdown_bps=6000)


def make_ledger(path, *, policy=None):
    return ledger_module.PaperLedger(
        path,
        balances={"prediction": 0, "equities": 0, "options": 0, "crypto": 30_000},
        limits=ledger_module.RiskLimits(
            aggregate_cap_cents=30_000,
            group_cap_cents=30_000,
            per_idea_cents={"prediction": 1, "equities": 1, "options": 1, "crypto": 30_000},
        ),
        market_risk_policy=policy,
        clock=lambda: NOW,
    )


def proposal(key, cost=20_000):
    return ledger_module.Proposal(
        key, "crypto", "policy-fixture", "1", "a" * 64, NOW, "SYNTH",
        key, key, NOW + timedelta(hours=1), 1, cost, provenance="synthetic",
    )


def mark(position_id, value, at=NOW):
    return ledger_module.MarketMark(position_id, value, at, "b" * 64, "synthetic")


class MarketRiskPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "ledger.db"

    def tearDown(self):
        self.temp.cleanup()

    def latch_flat_ledger(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("empty-start", [], NOW)
        reservation = ledger.reserve(proposal("loss"))
        fill = ledger.simulate_fill(reservation.reservation_id)
        ledger.record_marks("loss-mark", [mark(fill.position_id, 0)], NOW)
        ledger.close(fill.position_id, close_id="loss-close", proceeds_cents=19_000)
        ledger.record_marks("flat-after-loss", [], NOW)
        self.assertTrue(ledger.market_snapshot(NOW).automatic_risk_latched)
        self.assertFalse(ledger.market_snapshot(NOW).entry_allowed)
        return ledger

    def rearm(self, ledger, rearm_id="crypto-expansion"):
        return ledger.rearm_automatic_risk(
            rearm_id=rearm_id,
            reason="bounded high-volatility paper experiment",
            policy=CRYPTO_POLICY,
            at=NOW,
        )

    def test_defaults_remain_one_and_five_percent(self):
        defaults = ledger_module.MarketRiskPolicy()
        self.assertEqual((defaults.daily_loss_bps, defaults.max_drawdown_bps), (100, 500))
        ledger = make_ledger(self.path)
        self.assertEqual(ledger.market_risk_policy, defaults)

    def test_missing_day_flat_cash_anchor_preserves_realized_loss(self):
        ledger = self.latch_flat_ledger()
        later = NOW + timedelta(days=2)
        ledger.record_marks("flat-gap", [], later)
        snapshot = ledger.market_snapshot(later)
        self.assertEqual(snapshot.daily_anchor_cents, 29_000)
        self.assertEqual(snapshot.high_water_cents, 30_000)
        self.assertTrue(snapshot.automatic_risk_latched)
        self.assertEqual(ledger.snapshot().paper_nav_cents, 29_000)
        reopened = make_ledger(self.path)
        self.assertEqual(reopened.market_snapshot(later).daily_anchor_cents, 29_000)

    def test_missing_day_with_historical_open_position_cannot_use_cash_anchor(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("start", [], NOW)
        fill = ledger.simulate_fill(ledger.reserve(proposal("held")).reservation_id)
        later = NOW + timedelta(days=2)
        ledger.record_marks("held-gap", [mark(fill.position_id, 20_000, later)], later)
        self.assertIsNone(ledger.market_snapshot(later).daily_anchor_cents)
        self.assertEqual(ledger.market_snapshot(later).block_reason, "missing_prior_utc_day_anchor")

    def test_audited_rearm_preserves_cash_highwater_and_historical_marks(self):
        ledger = self.latch_flat_ledger()
        before_cash = ledger.snapshot().paper_nav_cents
        before_snapshot = ledger.market_snapshot(NOW)
        with sqlite3.connect(self.path) as conn:
            before_batches = conn.execute(
                "SELECT batch_id,as_of,market_nav_cents,high_water_cents,automatic_latched "
                "FROM mark_batches ORDER BY rowid"
            ).fetchall()
        self.assertEqual(before_cash, 29_000)
        self.assertEqual(before_snapshot.high_water_cents, 30_000)

        self.assertTrue(self.rearm(ledger))
        self.assertFalse(ledger.market_snapshot(NOW).automatic_risk_latched)
        self.assertFalse(ledger.market_snapshot(NOW).entry_allowed)
        # The prior batch remains an immutable latch record. A new complete batch
        # establishes the post-rearm active state for subsequent paper entries.
        ledger.record_marks("post-rearm-flat", [], NOW)
        self.assertFalse(ledger.market_snapshot(NOW).automatic_risk_latched)
        self.assertEqual(ledger.snapshot().paper_nav_cents, before_cash)
        self.assertEqual(ledger.market_snapshot(NOW).high_water_cents, before_snapshot.high_water_cents)
        with sqlite3.connect(self.path) as conn:
            after_batches = conn.execute(
                "SELECT batch_id,as_of,market_nav_cents,high_water_cents,automatic_latched "
                "FROM mark_batches ORDER BY rowid"
            ).fetchall()
        self.assertEqual(after_batches[:len(before_batches)], before_batches)
        self.assertEqual(len(after_batches), len(before_batches) + 1)
        rearm_events = [event for event in ledger.audit() if event.event_type == "automatic_risk_rearm"]
        self.assertEqual(len(rearm_events), 1)
        self.assertFalse(rearm_events[0].payload["cash_changed"])
        self.assertFalse(rearm_events[0].payload["historical_marks_changed"])

    def test_rearmed_policy_persists_and_retrips_at_new_threshold(self):
        ledger = self.latch_flat_ledger()
        self.rearm(ledger)
        resumed = make_ledger(self.path)
        self.assertEqual(resumed.market_risk_policy, CRYPTO_POLICY)
        resumed.record_marks("post-rearm-flat", [], NOW)
        reservation = resumed.reserve(proposal("new-loss"))
        fill = resumed.simulate_fill(reservation.reservation_id)
        snapshot = resumed.record_marks("new-threshold-loss", [mark(fill.position_id, 0)], NOW)
        self.assertGreaterEqual(snapshot.daily_loss_bps, CRYPTO_POLICY.daily_loss_bps)
        self.assertTrue(snapshot.automatic_risk_latched)

    def test_rearm_rejects_open_position_and_outstanding_reservation(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("empty-start", [], NOW)
        first = ledger.reserve(proposal("open-loss"))
        fill = ledger.simulate_fill(first.reservation_id)
        ledger.record_marks("loss-mark", [mark(fill.position_id, 0)], NOW)
        with self.assertRaises(ledger_module.InvalidState):
            self.rearm(ledger, "open-position")

        ledger.close(fill.position_id, close_id="close-loss", proceeds_cents=19_000)
        ledger.record_marks("flat-latched", [], NOW)
        self.assertTrue(ledger.market_snapshot(NOW).automatic_risk_latched)
        # A reservation made before the latch remains accounted for and blocks rearm.
        # Reserve is normally blocked after latching, so create it before the loss in
        # a separate ledger fixture below.

    def test_rearm_rejects_latched_ledger_with_reservation(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("empty-start", [], NOW)
        open_reservation = ledger.reserve(proposal("open"))
        fill = ledger.simulate_fill(open_reservation.reservation_id)
        outstanding = ledger.reserve(proposal("reserved", cost=1_000))
        self.assertEqual(outstanding.state, "reserved")
        ledger.record_marks("loss-mark", [mark(fill.position_id, 0)], NOW)
        self.assertTrue(ledger.market_snapshot(NOW).automatic_risk_latched)
        ledger.close(fill.position_id, close_id="close-loss", proceeds_cents=19_000)
        ledger.record_marks("flat-latched", [], NOW)
        with self.assertRaises(ledger_module.InvalidState):
            self.rearm(ledger, "open-reservation")

    def test_unlatched_flat_policy_configuration_is_audited_and_idempotent(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("empty-start", [], NOW)
        before_cash = ledger.snapshot().paper_nav_cents
        before_highwater = ledger.market_snapshot(NOW).high_water_cents
        self.assertTrue(self.rearm(ledger, "crypto-configure"))
        ledger.dispose()
        reopened = make_ledger(self.path)
        self.assertEqual(reopened.market_risk_policy, CRYPTO_POLICY)
        self.assertFalse(self.rearm(reopened, "crypto-configure"))
        self.assertEqual(reopened.snapshot().paper_nav_cents, before_cash)
        self.assertEqual(reopened.market_snapshot(NOW).high_water_cents, before_highwater)
        events = [event for event in reopened.audit() if event.event_type == "automatic_risk_rearm"]
        self.assertEqual(len(events), 1)
        self.assertFalse(events[0].payload["previous_latched"])


    def open_ledger(self):
        ledger = make_ledger(self.path)
        ledger.record_marks("initial", [], NOW)
        fill = ledger.simulate_fill(ledger.reserve(proposal("inflight", cost=1500)).reservation_id)
        ledger.record_marks("position-mark", [mark(fill.position_id, 1300)], NOW)
        return ledger, fill

    def test_in_position_expansion_preserves_every_financial_row(self):
        ledger, fill = self.open_ledger()
        with sqlite3.connect(self.path) as conn:
            before = {t: conn.execute("SELECT * FROM " + t).fetchall()
                      for t in ["accounts", "positions", "fills", "closes", "mark_batches", "market_marks"]}
        self.assertTrue(ledger.rearm_automatic_risk(rearm_id="open-migrate", reason="approved paper expansion",
            policy=CRYPTO_POLICY, allow_open_positions=True, at=NOW))
        with sqlite3.connect(self.path) as conn:
            after = {t: conn.execute("SELECT * FROM " + t).fetchall() for t in before}
        self.assertEqual(before, after)
        reopened = make_ledger(self.path)
        self.assertEqual(reopened.get_position(fill.position_id).state, "open")
        self.assertEqual(reopened.market_risk_policy, CRYPTO_POLICY)
        self.assertFalse(reopened.rearm_automatic_risk(rearm_id="open-migrate", reason="approved paper expansion",
            policy=CRYPTO_POLICY, allow_open_positions=True, at=NOW))
        reopened.record_marks("post-open-migrate", [mark(fill.position_id, 1250)], NOW)
        self.assertTrue(reopened.market_snapshot(NOW).entry_allowed)
        reopened.close(fill.position_id, close_id="normal-exit", proceeds_cents=1250)
        self.assertEqual(reopened.get_position(fill.position_id).state, "closed")

    def test_in_position_migration_rejects_stale_marks(self):
        ledger, _ = self.open_ledger()
        with self.assertRaises(ledger_module.InvalidState):
            ledger.rearm_automatic_risk(rearm_id="stale-open", reason="approved",
                policy=CRYPTO_POLICY, allow_open_positions=True, at=NOW + timedelta(hours=1))
        self.assertEqual(ledger.market_risk_policy, ledger_module.MarketRiskPolicy())

    def test_in_position_migration_rejects_reservations_and_lower_limits(self):
        ledger, _ = self.open_ledger()
        with self.assertRaises(ledger_module.ValidationError):
            ledger.rearm_automatic_risk(rearm_id="lower-open", reason="approved",
                policy=ledger_module.MarketRiskPolicy(50, 500), allow_open_positions=True, at=NOW)
        ledger.reserve(proposal("pending", cost=1000))
        with self.assertRaises(ledger_module.InvalidState):
            ledger.rearm_automatic_risk(rearm_id="reserved-open", reason="approved",
                policy=CRYPTO_POLICY, allow_open_positions=True, at=NOW)


if __name__ == "__main__":
    unittest.main()
