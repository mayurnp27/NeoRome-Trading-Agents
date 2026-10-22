from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest

from neorome.paper_ledger import (
    IdempotencyConflict, InvalidState, MarketMark, PaperLedger, Proposal,
    RiskStopActive, ValidationError,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 17, 12, tzinfo=UTC)
HASH = "b" * 64


def proposal(key, at=T0, desk="equities", cost=400, provenance="synthetic", group=None):
    return Proposal(key, desk, "fixture", "1", "a" * 64, at, key.upper(),
                    group or key, key, at + timedelta(minutes=5), 1, cost,
                    provenance=provenance)


def mark(position_id, value, at=T0, provenance="synthetic", source_hash=HASH):
    return MarketMark(position_id, value, at, source_hash, provenance)


class MarketMarkTests(unittest.TestCase):
    def test_missing_market_settings_are_not_reseeded(self):
        self.initial_coverage()
        with sqlite3.connect(self.path) as conn:
            conn.execute("DELETE FROM settings WHERE key='market_first_session_date'")
        with self.assertRaises(InvalidState):
            PaperLedger(self.path, enforce_market_risk=True, clock=lambda: T0)

    def test_generation_tamper_is_detected(self):
        self.initial_coverage()
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE settings SET value='9' WHERE key='position_generation'")
        with self.assertRaises(InvalidState):
            self.ledger.market_snapshot()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "ledger.db"
        self.now = T0
        self.ledger = PaperLedger(self.path, enforce_market_risk=True,
                                  clock=lambda: self.now)

    def tearDown(self):
        self.tmp.cleanup()

    def initial_coverage(self, at=T0, batch="empty"):
        return self.ledger.record_marks(batch, [], at)

    def open_position(self, key="one", at=T0, cost=400, provenance="synthetic"):
        if not self.ledger.audit():
            self.initial_coverage(at)
        reservation = self.ledger.reserve(proposal(key, at, cost=cost,
                                                   provenance=provenance), now=at)
        return self.ledger.simulate_fill(reservation.reservation_id,
                                         provenance=provenance, filled_at=at)

    def test_empty_snapshot_enables_initial_reserve_and_config_is_persisted(self):
        snap = self.initial_coverage()
        self.assertEqual(snap.market_nav_cents, 80_000)
        self.assertTrue(snap.entry_allowed)
        reservation = self.ledger.reserve(proposal("first"))
        snap = self.ledger.market_snapshot()
        self.assertEqual((snap.available_cash_cents, snap.reserved_cash_cents),
                         (79_600, 400))
        with self.assertRaises(ValidationError):
            PaperLedger(self.path, enforce_market_risk=False, clock=lambda: self.now)
        PaperLedger(self.path, enforce_market_risk=True, clock=lambda: self.now)

    def test_missing_stale_and_future_marks_block_new_risk(self):
        with self.assertRaises(InvalidState):
            self.ledger.market_snapshot()
        with self.assertRaises(RiskStopActive):
            self.ledger.reserve(proposal("missing"))
        self.initial_coverage()
        self.now = T0 + timedelta(seconds=301)
        self.assertEqual(self.ledger.market_snapshot().block_reason, "market_marks_stale")
        self.ledger.set_risk_stop(True, reason="manual too", at=self.now)
        stopped = self.ledger.market_snapshot()
        self.assertFalse(stopped.coverage_current)
        self.assertIsNone(stopped.market_nav_cents)
        with self.assertRaises(RiskStopActive):
            self.ledger.reserve(proposal("stale", self.now), now=self.now)
        with self.assertRaises(ValidationError):
            self.ledger.record_marks("future", [mark("unknown", 1, self.now)], self.now - timedelta(seconds=1))

    def test_fill_invalidates_coverage_but_is_allowed_after_expiry(self):
        self.initial_coverage()
        reservation = self.ledger.reserve(proposal("paid"))
        self.now = T0 + timedelta(minutes=10)
        fill = self.ledger.simulate_fill(reservation.reservation_id, filled_at=T0 + timedelta(minutes=4))
        self.assertEqual(self.ledger.market_snapshot().block_reason, "position_coverage_changed")
        with self.assertRaises(RiskStopActive):
            self.ledger.reserve(proposal("blocked", self.now), now=self.now)
        self.ledger.record_marks("covered", [mark(fill.position_id, 400, self.now)], self.now)
        self.ledger.reserve(proposal("next", self.now), now=self.now)

    def test_rejects_partial_duplicate_wrong_position_and_provenance_atomically(self):
        fill = self.open_position()
        bad_batches = [
            [],
            [mark("not-a-position", 400)],
            [mark(fill.position_id, 400), mark(fill.position_id, 400)],
            [mark(fill.position_id, 400, provenance="observed")],
            [mark(fill.position_id, 400, source_hash="bad")],
            [mark(fill.position_id, True)],
            [mark(fill.position_id, 400, T0 - timedelta(seconds=301))],
            [mark(fill.position_id, 400, T0 + timedelta(seconds=1))],
        ]
        for index, marks in enumerate(bad_batches):
            with self.assertRaises(ValidationError):
                self.ledger.record_marks(f"bad-{index}", marks, T0)
        with sqlite3.connect(self.path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM mark_batches").fetchone()[0], 1)

    def test_batch_idempotency_and_replay_does_not_restore_coverage(self):
        fill = self.open_position()
        expected = self.ledger.record_marks("marks", [mark(fill.position_id, 350)], T0)
        self.assertEqual(self.ledger.record_marks("marks", [mark(fill.position_id, 350)], T0), expected)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.record_marks("marks", [mark(fill.position_id, 351)], T0)
        self.ledger.close(fill.position_id, proceeds_cents=350, closed_at=T0)
        replay = self.ledger.record_marks("marks", [mark(fill.position_id, 350)], T0)
        self.assertFalse(replay.coverage_current)
        self.assertIsNone(replay.market_nav_cents)
        self.assertEqual(replay.marked_liquidation_value_cents, 0)
        reconciled = self.ledger.record_marks("after-close", [], T0)
        self.assertEqual(reconciled.market_nav_cents, 79_950)

    def test_first_session_anchor_reused_and_source_cannot_precede_open(self):
        fill = self.open_position()
        later = T0 + timedelta(minutes=2)
        with self.assertRaises(ValidationError):
            self.ledger.record_marks("pre-open-source", [mark(fill.position_id, 400, T0 - timedelta(seconds=1))], T0)
        snap = self.ledger.record_marks("same-day", [mark(fill.position_id, 390, later)], later)
        self.assertEqual(snap.daily_anchor_cents, 80_000)

    def test_prior_day_snapshot_cannot_cover_new_utc_day_even_while_fresh(self):
        late = T0.replace(hour=23, minute=59, second=58)
        self.ledger.record_marks("late-empty", [], late)
        after_midnight = late + timedelta(seconds=3)
        snap = self.ledger.market_snapshot(after_midnight)
        self.assertFalse(snap.coverage_current)
        self.assertIsNone(snap.market_nav_cents)
        self.assertEqual(snap.block_reason, "current_utc_day_snapshot_missing")
        with self.assertRaises(RiskStopActive):
            self.ledger.reserve(proposal("rollover", after_midnight), now=after_midnight)

    def test_fractional_timestamp_ordering_uses_actual_time(self):
        first = T0.replace(microsecond=900_000)
        second = first + timedelta(microseconds=200_000)
        self.ledger.record_marks("fractional-first", [], first)
        self.ledger.record_marks("next-second", [], second)
        self.ledger.record_marks("fractional-first", [], first)
        self.assertEqual(self.ledger.market_snapshot(second).batch_id, "next-second")

    def test_price_fall_is_visible_before_close_while_cost_snapshot_is_unchanged(self):
        fill = self.open_position(cost=400)
        snap = self.ledger.record_marks("down", [mark(fill.position_id, 200)], T0)
        self.assertEqual(snap.market_nav_cents, 79_800)
        self.assertEqual(self.ledger.snapshot().paper_nav_cents, 80_000)

    def test_prior_utc_day_last_mark_is_anchor_and_overnight_loss_latches(self):
        fill = self.open_position(cost=1500)
        morning = T0
        evening = T0.replace(hour=23, minute=59)
        self.ledger.record_marks("morning", [mark(fill.position_id, 1400, morning)], morning)
        self.ledger.record_marks("close", [mark(fill.position_id, 1500, evening)], evening)
        next_day = (T0 + timedelta(days=1)).replace(hour=0, minute=1)
        snap = self.ledger.record_marks("overnight", [mark(fill.position_id, 700, next_day)], next_day)
        self.assertEqual(snap.daily_anchor_cents, 80_000)
        self.assertEqual(snap.daily_loss_bps, 100)
        self.assertTrue(snap.automatic_risk_latched)

    def test_missing_prior_calendar_day_blocks_without_fabricating_anchor(self):
        fill = self.open_position()
        self.ledger.record_marks("day-one", [mark(fill.position_id, 400)], T0)
        day_three = T0 + timedelta(days=2)
        snap = self.ledger.record_marks("day-three", [mark(fill.position_id, 400, day_three)], day_three)
        self.assertIsNone(snap.daily_anchor_cents)
        self.assertEqual(snap.block_reason, "missing_prior_utc_day_anchor")
        with self.assertRaises(RiskStopActive):
            self.ledger.reserve(proposal("gap", day_three), now=day_three)

    def test_exact_daily_threshold_latches_survives_restart_and_manual_clear(self):
        fill = self.open_position(cost=1500)
        snap = self.ledger.record_marks("threshold", [mark(fill.position_id, 700)], T0)
        self.assertEqual(snap.daily_loss_bps, 100)
        self.assertTrue(snap.automatic_risk_latched)
        self.ledger.set_risk_stop(True, reason="manual")
        self.ledger.set_risk_stop(False, reason="manual clear")
        restarted = PaperLedger(self.path, enforce_market_risk=True, clock=lambda: T0)
        self.assertTrue(restarted.risk_stop_active())
        self.assertEqual(restarted.market_snapshot().block_reason, "automatic_loss_latch")
        with self.assertRaises(RiskStopActive):
            restarted.reserve(proposal("cannot-bypass"))

    def test_exact_drawdown_threshold_latches(self):
        positions = []
        specifications = [
            ("e1", "equities", 1500), ("o1", "options", 1500),
            ("e2", "equities", 1500), ("o2", "options", 1500),
            ("c1", "crypto", 500), ("p1", "prediction", 500),
            ("c2", "crypto", 500), ("p2", "prediction", 500),
        ]
        self.initial_coverage()
        for index, (key, desk, cost) in enumerate(specifications):
            reservation = self.ledger.reserve(proposal(key, desk=desk, cost=cost, group=key))
            positions.append(self.ledger.simulate_fill(reservation.reservation_id))
            if index != len(specifications) - 1:
                marks = [mark(p.position_id, specifications[i][2]) for i, p in enumerate(positions)]
                self.ledger.record_marks(f"step-{index}", marks, T0)
        high_at = T0 + timedelta(minutes=1)
        high_values = [spec[2] + 250 for spec in specifications]
        high = self.ledger.record_marks("high", [mark(p.position_id, value, high_at) for p, value in zip(positions, high_values)], high_at)
        self.assertEqual(high.market_nav_cents, 82_000)
        def values_for_nav(nav):
            quotient, remainder = divmod(nav - 72_000, len(positions))
            return [quotient + int(i < remainder) for i in range(len(positions))]
        observations = [(0, 81_600), (1, 80_800), (2, 80_000),
                        (3, 79_201), (4, 78_410), (5, 77_900)]
        low = None
        for day, nav in observations:
            at = (T0 + timedelta(days=day)).replace(hour=23, minute=59)
            low = self.ledger.record_marks(
                f"descent-{day}",
                [mark(p.position_id, value, at) for p, value in zip(positions, values_for_nav(nav))],
                at,
            )
            if nav != 77_900:
                self.assertFalse(low.automatic_risk_latched)
        self.assertIsNotNone(low)
        self.assertEqual(low.market_nav_cents, 77_900)
        self.assertLess(low.daily_loss_bps, 100)
        self.assertEqual(low.drawdown_bps, 500)
        self.assertTrue(low.automatic_risk_latched)

    def test_rejected_batch_rolls_back_all_rows_and_highwater(self):
        fill = self.open_position()
        with sqlite3.connect(self.path) as conn:
            before = conn.execute("SELECT value FROM settings WHERE key='market_high_water'").fetchone()[0]
        original = self.ledger._audit
        self.ledger._audit = lambda *args: (_ for _ in ()).throw(RuntimeError("injected"))
        with self.assertRaises(RuntimeError):
            self.ledger.record_marks("rollback", [mark(fill.position_id, 500)], T0)
        self.ledger._audit = original
        with sqlite3.connect(self.path) as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM mark_batches WHERE batch_id='rollback'").fetchone())
            self.assertEqual(conn.execute("SELECT value FROM settings WHERE key='market_high_water'").fetchone()[0], before)

    def test_tampered_mark_payload_blocks_restart(self):
        fill = self.open_position()
        self.ledger.record_marks("valid", [mark(fill.position_id, 400)], T0)
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE market_marks SET source_hash=? WHERE batch_id='valid'", ("c" * 64,))
        with self.assertRaises(InvalidState):
            PaperLedger(self.path, enforce_market_risk=True, clock=lambda: T0)

    def test_tampered_market_setting_blocks_restart(self):
        self.initial_coverage()
        with sqlite3.connect(self.path) as conn:
            conn.execute("UPDATE settings SET value='999999' WHERE key='market_high_water'")
        with self.assertRaises(InvalidState):
            PaperLedger(self.path, enforce_market_risk=True, clock=lambda: T0)

    def test_legacy_cost_database_migrates_only_as_default_false(self):
        legacy_path = Path(self.tmp.name) / "legacy.db"
        legacy = PaperLedger(legacy_path, clock=lambda: T0)
        with sqlite3.connect(legacy_path) as conn:
            config = json.loads(conn.execute("SELECT value FROM settings WHERE key='ledger_config'").fetchone()[0])
            config.pop("enforce_market_risk")
            conn.execute("UPDATE settings SET value=? WHERE key='ledger_config'", (json.dumps(config),))
            conn.execute("DROP TABLE market_marks")
            conn.execute("DROP TABLE mark_batches")
            conn.execute("DELETE FROM settings WHERE key IN ('position_generation','market_high_water','automatic_risk_latched')")
        reopened = PaperLedger(legacy_path, clock=lambda: T0)
        self.assertEqual(reopened.snapshot().paper_nav_cents, 80_000)
        with self.assertRaises(ValidationError):
            PaperLedger(legacy_path, enforce_market_risk=True, clock=lambda: T0)


if __name__ == "__main__":
    unittest.main()
