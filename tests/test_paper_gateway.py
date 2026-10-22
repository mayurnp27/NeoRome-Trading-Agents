from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from neorome.paper_gateway import GatewayValidationError, PaperProposalGateway
from neorome.paper_ledger import IdempotencyConflict, PaperLedger, RiskLimitExceeded


NOW = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)
HASH = "a" * 64
FILL_HASH = "b" * 64


def observed_source(timestamp: datetime, source_hash: str = HASH) -> dict[str, object]:
    stamp = timestamp.isoformat().replace("+00:00", "Z")
    return {"kind": "observed", "hash": source_hash, "observed_at": stamp, "received_at": stamp}


def proposal(desk: str, *, proposal_id: str | None = None, ask: int = 100, depth: int = 1,
             source_time: datetime = NOW - timedelta(seconds=60), delay: int = 10) -> dict[str, object]:
    result: dict[str, object] = {
        "proposal_id": proposal_id or f"{desk}-one",
        "desk": desk,
        "strategy": "fixture",
        "strategy_version": "1",
        "source_kind": "observed",
        "source": observed_source(source_time),
        "instrument": {"prediction": "WEATHER-YES", "options": "SPY-vertical"}.get(desk, desk.upper()),
        "instrument_type": {"prediction": "prediction", "options": "debit_spread"}.get(desk, "cash_asset"),
        "side": "long",
        "exposure_group": f"{desk}-group",
        "idea_id": proposal_id or f"{desk}-idea",
        "quantity": 1,
        "expiry_at": (NOW + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "quote": {
            "timestamp": source_time.isoformat().replace("+00:00", "Z"),
            "bid_cents": max(1, ask - 1), "ask_cents": ask, "depth": depth,
        },
        "cost": {"unit_cost_cents": ask, "fee_cents": 0},
        "execution": {"delay_seconds": delay},
        "metadata": {"fixture": True},
    }
    if desk == "options":
        result["legs"] = [
            {"symbol": "SPY-261218C650", "side": "buy", "ratio": 1},
            {"symbol": "SPY-261218C655", "side": "sell", "ratio": 1},
        ]
    return result


def fill_event(reservation: dict[str, object], *, desk: str, ask: int = 100,
               quote_time: datetime = NOW - timedelta(seconds=20), depth: int = 1,
               filled_at: datetime = NOW, fill_id: str = "fill-one") -> dict[str, object]:
    stamp = quote_time.isoformat().replace("+00:00", "Z")
    return {
        "reservation_id": reservation["reservation_id"], "fill_id": fill_id,
        "source_kind": "observed", "source": observed_source(quote_time, FILL_HASH),
        "quote": {"timestamp": stamp, "bid_cents": max(1, ask - 1), "ask_cents": ask, "depth": depth},
        "quantity": 1, "cost": {"unit_cost_cents": ask, "fee_cents": 0},
        "filled_at": filled_at.isoformat().replace("+00:00", "Z"),
    }


class PaperGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = PaperLedger(Path(self.tmp.name) / "paper.sqlite3", clock=lambda: NOW)
        self.gateway = PaperProposalGateway(self.ledger, clock=lambda: NOW)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_fill_freshness_cannot_exceed_sixty_seconds(self):
        gateway = PaperProposalGateway(self.ledger, max_source_age_seconds=300, clock=lambda: NOW)
        reservation = gateway.reserve(proposal('options'))['reservation']
        later = NOW+timedelta(seconds=80)
        event = fill_event(reservation, desk='options', quote_time=NOW+timedelta(seconds=11), filled_at=later)
        with self.assertRaises(GatewayValidationError):
            gateway.record_fill(event, now=later)

    def test_common_four_desk_shape_reserves_through_existing_ledger(self) -> None:
        asks = {"prediction": 100, "equities": 250, "options": 500, "crypto": 200}
        for desk, ask in asks.items():
            result = self.gateway.reserve(proposal(desk, ask=ask, proposal_id=f"{desk}-id"))
            self.assertEqual(result["status"], "reserved")
            self.assertEqual(result["source_kind"], "observed")
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, sum(asks.values()))
        self.assertEqual(self.ledger.snapshot().open_position_cost_cents, 0)

    def test_proposal_alone_never_creates_fill_and_exact_replay_is_idempotent(self) -> None:
        payload = proposal("equities", ask=300)
        first = self.gateway.reserve(payload, now=NOW - timedelta(seconds=40))
        replay = self.gateway.reserve(deepcopy(payload))
        self.assertEqual(first["reservation"], replay["reservation"])
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, 300)

        event = fill_event(first["reservation"], desk="equities", ask=300)
        restarted_ledger = PaperLedger(Path(self.tmp.name) / "paper.sqlite3", clock=lambda: NOW)
        restarted_gateway = PaperProposalGateway(restarted_ledger, clock=lambda: NOW)
        filled = restarted_gateway.record_fill(event)
        replayed = restarted_gateway.record_fill(deepcopy(event))
        self.assertEqual(filled["fill"], replayed["fill"])
        self.assertEqual(restarted_ledger.snapshot().open_position_cost_cents, 300)

    def test_stale_exact_replay_returns_prior_reservation_and_changed_replay_fails(self) -> None:
        payload = proposal("equities", ask=300)
        first = self.gateway.reserve(payload, now=NOW)
        replay = self.gateway.reserve(deepcopy(payload), now=NOW + timedelta(seconds=301))
        self.assertEqual(first["reservation"], replay["reservation"])
        changed = deepcopy(payload)
        changed["cost"] = {"unit_cost_cents": 299, "fee_cents": 0}
        changed["quote"] = {**changed["quote"], "ask_cents": 299}
        with self.assertRaises(IdempotencyConflict):
            self.gateway.reserve(changed, now=NOW + timedelta(seconds=301))

    def test_stale_exact_fill_replay_returns_prior_fill_and_new_past_expiry_fill_rejects(self) -> None:
        reserved = self.gateway.reserve(
            proposal("crypto", ask=200, delay=20), now=NOW - timedelta(seconds=40)
        )["reservation"]
        event = fill_event(
            reserved, desk="crypto", ask=200,
            quote_time=NOW - timedelta(seconds=15), filled_at=NOW,
        )
        filled = self.gateway.record_fill(event, now=NOW)
        replayed = self.gateway.record_fill(deepcopy(event), now=NOW + timedelta(seconds=301))
        self.assertEqual(filled["fill"], replayed["fill"])

        expiring = proposal("equities", ask=300, delay=10)
        expiring["expiry_at"] = (NOW + timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
        reservation = self.gateway.reserve(expiring, now=NOW)["reservation"]
        late_event = fill_event(
            reservation, desk="equities", ask=300,
            quote_time=NOW + timedelta(seconds=11), filled_at=NOW + timedelta(seconds=20),
        )
        with self.assertRaisesRegex(GatewayValidationError, "fill submission"):
            self.gateway.record_fill(late_event, now=NOW + timedelta(seconds=120))

    def test_reservation_delay_must_fit_from_current_time_to_expiry(self) -> None:
        expiring = proposal("prediction", ask=100, delay=10)
        expiring["expiry_at"] = (NOW + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        with self.assertRaisesRegex(GatewayValidationError, "execution delay"):
            self.gateway.reserve(expiring, now=NOW)

    def test_duplicate_proposal_conflict_and_cross_source_are_rejected(self) -> None:
        payload = proposal("crypto", ask=200)
        self.gateway.reserve(payload)
        conflicting = deepcopy(payload)
        conflicting["cost"] = {"unit_cost_cents": 201, "fee_cents": 0}
        conflicting["quote"] = {**conflicting["quote"], "ask_cents": 201}
        with self.assertRaises(IdempotencyConflict):
            self.gateway.reserve(conflicting)
        synthetic = deepcopy(payload)
        synthetic["source_kind"] = "synthetic"
        synthetic["source"] = {**synthetic["source"], "kind": "synthetic"}
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(synthetic)
        synthetic_gateway = PaperProposalGateway(self.ledger, source_kind="synthetic", clock=lambda: NOW)
        with self.assertRaises(GatewayValidationError):
            synthetic_gateway.reserve(synthetic)

    def test_stale_future_missing_provenance_and_depth_fail_closed(self) -> None:
        stale = proposal("prediction", source_time=NOW - timedelta(minutes=6))
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(stale)
        future = proposal("prediction", source_time=NOW + timedelta(seconds=1))
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(future)
        missing = proposal("prediction")
        del missing["source"]
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(missing)
        shallow = proposal("prediction", ask=100, depth=0)
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(shallow)

    def test_fill_requires_observed_sufficient_depth_and_declared_delay(self) -> None:
        reserved = self.gateway.reserve(
            proposal("prediction", ask=100, delay=20), now=NOW - timedelta(seconds=40)
        )["reservation"]
        shallow = fill_event(reserved, desk="prediction", ask=100, depth=0)
        with self.assertRaises(GatewayValidationError):
            self.gateway.record_fill(shallow)
        early = fill_event(reserved, desk="prediction", ask=100,
                           quote_time=NOW - timedelta(seconds=25), filled_at=NOW)
        with self.assertRaises(GatewayValidationError):
            self.gateway.record_fill(early)
        partial = fill_event(reserved, desk="prediction", ask=100)
        partial["quantity"] = 2
        with self.assertRaises(GatewayValidationError):
            self.gateway.record_fill(partial)
        self.assertEqual(self.ledger.snapshot().reserved_cash_cents, 100)

    def test_cash_budget_risk_caps_and_unresolved_lifecycle_are_enforced(self) -> None:
        too_large = proposal("prediction", ask=501)
        with self.assertRaises(RiskLimitExceeded):
            self.gateway.reserve(too_large)
        reserved = self.gateway.reserve(
            proposal("equities", ask=300), now=NOW - timedelta(seconds=40)
        )["reservation"]
        event = fill_event(reserved, desk="equities", ask=300)
        self.gateway.record_fill(event)
        changed = deepcopy(event)
        changed["cost"] = {"unit_cost_cents": 299, "fee_cents": 0}
        changed["quote"] = {**changed["quote"], "ask_cents": 299}
        with self.assertRaises(GatewayValidationError):
            self.gateway.record_fill(changed)
        with self.assertRaises(GatewayValidationError):
            self.gateway.cancel(reserved["reservation_id"], reason="too late")

    def test_pre_decision_quote_reuse_is_rejected_but_post_arrival_quote_fills(self) -> None:
        reserved = self.gateway.reserve(
            proposal("crypto", ask=200, delay=20), now=NOW - timedelta(seconds=40)
        )["reservation"]
        old_quote = fill_event(
            reserved, desk="crypto", ask=200,
            quote_time=NOW - timedelta(seconds=25), filled_at=NOW,
        )
        with self.assertRaisesRegex(GatewayValidationError, "post-reservation delay"):
            self.gateway.record_fill(old_quote)
        new_quote = fill_event(
            reserved, desk="crypto", ask=200,
            quote_time=NOW - timedelta(seconds=15), filled_at=NOW,
        )
        self.assertEqual(self.gateway.record_fill(new_quote)["status"], "filled")

    def test_explicit_observed_price_improvement_reduces_ledger_basis(self) -> None:
        reserved = self.gateway.reserve(
            proposal("equities", ask=300, delay=10), now=NOW - timedelta(seconds=40)
        )["reservation"]
        improved = fill_event(
            reserved, desk="equities", ask=299,
            quote_time=NOW - timedelta(seconds=15), filled_at=NOW,
        )
        result = self.gateway.record_fill(improved)
        self.assertEqual(result["fill"]["total_cost_cents"], 299)
        self.assertEqual(self.ledger.snapshot().open_position_cost_cents, 299)

    def test_debit_spread_and_long_only_shape(self) -> None:
        valid = proposal("options", ask=500)
        self.gateway.reserve(valid)
        invalid = proposal("options", ask=500, proposal_id="options-credit")
        invalid["legs"] = [
            {"symbol": "SPY-261218C650", "side": "buy", "ratio": 2},
            {"symbol": "SPY-261218C655", "side": "sell", "ratio": 1},
        ]
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(invalid)
        short = proposal("crypto", ask=100, proposal_id="short")
        short["side"] = "short"
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(short)
        multi_asset = proposal("crypto", ask=100, proposal_id="multi-asset")
        multi_asset["quantity"] = 2
        multi_asset["quote"] = {**multi_asset["quote"], "depth": 2}
        with self.assertRaises(GatewayValidationError):
            self.gateway.reserve(multi_asset)


if __name__ == "__main__":
    unittest.main()
