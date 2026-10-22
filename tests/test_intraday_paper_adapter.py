from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from desks.equities.intraday_paper_adapter import (
    EntryGateBlocked,
    IntradayAdapterError,
    IntradayPaperAdapter,
)
from desks.equities.intraday_paper_candidate import prepare_entry
from neorome.paper_ledger import (
    MarketMark,
    PaperLedger,
    RiskLimitExceeded,
    RiskStopActive,
)


UTC = timezone.utc
BASE = datetime(2026, 9, 21, 13, 45, tzinfo=UTC)
HASH = "a" * 64


def iso(seconds: float) -> str:
    return (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def candidate(**overrides):
    value = {
        "candidate_id": "orb15-MU-2026-09-21",
        "strategy_id": "orb15",
        "symbol": "MU",
        "signal_timestamp": iso(0),
        "exit_decision_timestamp": iso(6 * 60 * 60 + 10 * 60),
        "session_close": iso(6 * 60 * 60 + 15 * 60),
        "universe_eligible": True,
        "prefix_complete_through_signal": True,
        "bar_quality_clear": True,
        "size_units_verified": True,
        "universe_evidence_sha256": HASH,
        "bar_prefix_sha256": "b" * 64,
        "quote_evidence_sha256": "c" * 64,
        "universe_known_at": iso(-1),
        "bar_prefix_known_at": iso(0),
        "calendar_known_at": iso(-1),
        "fractional_supported": False,
        "fee_cents_per_unit": 2,
        "units": 1,
    }
    value.update(overrides)
    return value


def quote(seconds: float, *, ask=10.0, bid=9.9, size=10):
    return {
        "symbol": "MU",
        "timestamp": iso(seconds),
        "received_at": iso(seconds),
        "bid": bid,
        "ask": ask,
        "bid_size": size,
        "ask_size": size,
    }


class IntradayPaperAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "paper.sqlite3"
        self.ledger = PaperLedger(self.path, clock=lambda: BASE)
        self.adapter = IntradayPaperAdapter(self.ledger)

    def tearDown(self):
        self.tmp.cleanup()

    def reserve(self, *, now=None, recovery_ok=True, allow_entry=True, **changes):
        return self.adapter.reserve(
            candidate(**changes), [quote(0.25)], order_lifetime=timedelta(seconds=1),
            now=now, recovery_ok=recovery_ok, allow_entry=allow_entry,
        )

    def test_restart_replay_is_idempotent_and_reserves_fees_without_fill(self):
        first = self.reserve(now=BASE + timedelta(seconds=0.25))
        restarted = IntradayPaperAdapter(PaperLedger(self.path, clock=lambda: BASE))
        replay = restarted.reserve(
            candidate(), [quote(0.25)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25)
        )
        self.assertEqual(first, replay)
        self.assertEqual(first.reserved_cents, 1002)
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, 1002)
        self.assertEqual(self.ledger.snapshot().open_position_cost_cents, 0)

    def test_entry_mark_exit_are_idempotent_and_fees_realize(self):
        reservation = self.reserve(now=BASE + timedelta(seconds=0.25))
        fill = self.adapter.entry(
            reservation.reservation_id,
            quote_timestamp=BASE + timedelta(seconds=0.5),
            filled_at=BASE + timedelta(seconds=0.6),
        )
        replay = self.adapter.entry(
            reservation.reservation_id,
            quote_timestamp=BASE + timedelta(seconds=0.5),
            filled_at=BASE + timedelta(seconds=0.6),
        )
        self.assertEqual(fill, replay)
        mark = MarketMark(fill.position_id, 990, BASE + timedelta(seconds=1), "d" * 64, "synthetic")
        self.assertEqual(self.adapter.mark("mark-one", [mark], as_of=BASE + timedelta(seconds=1)).batch_id, "mark-one")
        closed = self.adapter.exit(fill.position_id, proceeds_cents=990, fee_cents=2, closed_at=BASE + timedelta(seconds=2))
        self.assertEqual(self.adapter.exit(fill.position_id, proceeds_cents=990, fee_cents=2, closed_at=BASE + timedelta(seconds=2)), closed)
        self.assertEqual(closed.realized_pnl_cents, -14)

    def test_cancel_and_expiry_release_reservation_without_fill(self):
        reservation = self.reserve(now=BASE + timedelta(seconds=0.25))
        with self.assertRaisesRegex(Exception, "not expired"):
            self.adapter.expire(reservation.reservation_id, now=BASE + timedelta(seconds=0.5))
        expired = self.adapter.expire(reservation.reservation_id, now=BASE + timedelta(seconds=2))
        self.assertEqual(expired.state, "cancelled")
        self.assertEqual(self.adapter.cancel(reservation.reservation_id, reason="entry_expired"), expired)
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, 0)
        self.assertEqual(self.ledger.snapshot().open_position_cost_cents, 0)

    def test_stop_and_recovery_gates_block_reserve_and_entry_but_exit_remains_allowed(self):
        with self.assertRaises(EntryGateBlocked):
            self.reserve(now=BASE + timedelta(seconds=0.25), recovery_ok=False)
        reservation = self.reserve(now=BASE + timedelta(seconds=0.25), candidate_id="second")
        self.ledger.set_risk_stop(True, reason="rehearsal stop", at=BASE + timedelta(seconds=0.3))
        with self.assertRaises(RiskStopActive):
            self.adapter.reserve(candidate(candidate_id="third"), [quote(0.25)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25))
        with self.assertRaises(RiskStopActive):
            self.adapter.entry(reservation.reservation_id, quote_timestamp=BASE + timedelta(seconds=0.5), filled_at=BASE + timedelta(seconds=0.6))

    def test_cap_group_and_fractional_rejections(self):
        with self.assertRaises(IntradayAdapterError):
            self.reserve(now=BASE + timedelta(seconds=0.25), units=2, fee_cents_per_unit=2, **{"candidate_id": "too-many"})
        first = self.adapter.reserve(candidate(candidate_id="one"), [quote(0.25, ask=14.9)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25))
        second = self.adapter.reserve(candidate(candidate_id="two"), [quote(0.25, ask=14.9)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25))
        self.assertEqual(first.reserved_cents, 1492)
        self.assertEqual(second.reserved_cents, 1492)
        with self.assertRaises(RiskLimitExceeded):
            self.adapter.reserve(candidate(candidate_id="three"), [quote(0.25, ask=1.0, bid=0.9)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25))
        with self.assertRaises(IntradayAdapterError):
            self.adapter.reserve(candidate(candidate_id="fraction", units=0.5, fractional_supported=True), [quote(0.25)], order_lifetime=timedelta(seconds=1), now=BASE + timedelta(seconds=0.25))


if __name__ == "__main__":
    unittest.main()
