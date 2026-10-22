from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from neorome.options_gateway import convert_options_proposal, reserve_options_proposal
from neorome.options_lifecycle import OptionsLifecycle
from neorome.paper_gateway import PaperProposalGateway
from neorome.paper_ledger import PaperLedger


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
HASH = "a" * 64
FILL_HASH = "b" * 64


def occ(strike: int) -> str:
    return f"{'SPY':<6}261218C{strike * 1000:08d}"


def leg(strike: int, side: str, *, bid: str, ask: str, timestamp: datetime, quantity: int = 2) -> dict[str, object]:
    return {
        "symbol": occ(strike), "underlying": "SPY", "right": "C", "expiry": "2026-12-18",
        "strike": str(strike), "side": side, "quantity": quantity, "bid": bid, "ask": ask,
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"), "multiplier": 100, "adjusted": False,
        "depth": quantity + 2, "ask_size": quantity + 2, "bid_size": quantity + 2,
    }


def proposal(*, timestamp: datetime = NOW - timedelta(seconds=60), quantity: int = 2) -> dict[str, object]:
    return {
        "proposal_id": "opt-life-001", "strategy": "trend-vertical", "strategy_version": "v1",
        "source_kind": "synthetic", "source": {"kind": "synthetic", "hash": HASH,
            "received_at": (NOW - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")},
        "idea_id": "spy-idea-001", "expiry_at": (NOW + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "execution": {"delay_seconds": 10},
        "legs": [leg(650, "buy", bid="0.02", ask="0.03", timestamp=timestamp, quantity=quantity),
                 leg(655, "sell", bid="0.01", ask="0.02", timestamp=timestamp, quantity=quantity)],
        "metadata": {"caller": "lifecycle-test"},
    }


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class OptionsLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.ledger = PaperLedger(self.root / "paper.sqlite3", clock=lambda: NOW, enforce_market_risk=True)
        self.ledger.record_marks("initial-cash", [], NOW - timedelta(seconds=60))
        self.gateway = PaperProposalGateway(self.ledger, source_kind="synthetic", clock=lambda: NOW)
        raw = proposal()
        raw["legs"][1]["timestamp"] = (NOW - timedelta(seconds=59)).isoformat().replace("+00:00", "Z")
        self.converted = convert_options_proposal(raw, as_of=NOW - timedelta(seconds=30))
        self.reservation = reserve_options_proposal(raw, self.gateway, as_of=NOW - timedelta(seconds=30),
                                                    now=NOW - timedelta(seconds=30))["reservation"]
        event = {
            "reservation_id": self.reservation["reservation_id"], "fill_id": "opt-life-fill",
            "source_kind": "synthetic", "source": {"kind": "synthetic", "hash": FILL_HASH,
                "observed_at": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z"),
                "received_at": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z")},
            "quote": {"timestamp": (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z"),
                      "bid_cents": 0, "ask_cents": 200, "depth": 2},
            "quantity": 2, "cost": {"unit_cost_cents": 200, "fee_cents": 0}, "filled_at": NOW.isoformat().replace("+00:00", "Z"),
        }
        fill_result = self.gateway.record_fill(event)
        self.fill = fill_result["fill"]
        self.position_id = self.fill["position_id"]
        self.entry = {
            "position_id": self.position_id, "proposal_id": self.converted["proposal_id"],
            "strategy": self.converted["strategy"], "strategy_version": self.converted["strategy_version"],
            "instrument": self.converted["instrument"], "exposure_group": self.converted["exposure_group"],
            "idea_id": self.converted["idea_id"], "quantity": 2, "ledger_cost_cents": self.fill["total_cost_cents"],
            "ledger_fee_cents": self.fill["fee_cents"], "entry_as_of": NOW,
            "legs": deepcopy(raw["legs"]), "raw_input": raw, "raw_input_digest": digest(raw),
            "fill_input": fill_result["fill_event"],
        }
        self.entry["legs"][0]["timestamp"] = (NOW - timedelta(seconds=15)).isoformat().replace("+00:00", "Z")
        self.entry["legs"][1]["timestamp"] = (NOW - timedelta(seconds=14)).isoformat().replace("+00:00", "Z")
        self.entry["fill_input_digest"] = digest(self.entry["fill_input"])
        self.reports = [
            {"report_id": "r-buy", "order_id": "open-buy", "symbol": occ(650), "intent": "open",
             "cumulative_quantity": 2, "timestamp": (NOW + timedelta(seconds=1)).isoformat()},
            {"report_id": "r-sell", "order_id": "open-sell", "symbol": occ(655), "intent": "open",
             "cumulative_quantity": 2, "timestamp": (NOW + timedelta(seconds=2)).isoformat()},
        ]
        self.snapshot = {"complete": True, "timestamp": (NOW + timedelta(seconds=3)).isoformat(),
                         "option_holdings": {occ(650): 2, occ(655): -2}, "underlying_quantity": 0,
                         "open_orders_reconciled": True}

    def tearDown(self) -> None:
        self.temp.cleanup()

    def quotes(self, at: datetime, *, profitable: bool = True, hashes=("c" * 64, "d" * 64)):
        if profitable:
            values = [("4.00", "4.10"), ("0.01", "0.02")]
        else:
            values = [("0.00", "0.01"), ("3.99", "4.00")]
        quotes = {occ(650): {"bid": values[0][0], "ask": values[0][1], "timestamp": at.isoformat(), "bid_size": 4, "ask_size": 4},
                  occ(655): {"bid": values[1][0], "ask": values[1][1], "timestamp": at.isoformat(), "bid_size": 4, "ask_size": 4}}
        sources = {}
        for symbol, hash_hint, quote in zip((occ(650), occ(655)), hashes, (quotes[occ(650)], quotes[occ(655)])):
            source_quote = dict(quote)
            source_quote["timestamp"] = at.isoformat().replace("+00:00", "Z")
            provider = "synthetic-feed-" + hash_hint[0]
            source_material = {"symbol": symbol, "provenance": "synthetic", "provider": provider,
                               "quote": source_quote, "observed_at": at.isoformat().replace("+00:00", "Z"),
                               "received_at": at.isoformat().replace("+00:00", "Z")}
            sources[symbol] = {**source_material, "hash": digest(source_material),
                               "quote_digest": digest({"symbol": symbol, "quote": source_quote})}
        return quotes, sources

    def manager(self) -> OptionsLifecycle:
        return OptionsLifecycle(self.ledger, self.root / "options", clock=lambda: NOW)

    def test_trigger_requires_later_distinct_quote_and_persists_restart(self):
        manager = self.manager()
        q1, s1 = self.quotes(NOW + timedelta(seconds=5))
        mark = manager.position_mark(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                                     quotes=q1, quote_sources=s1, now=NOW + timedelta(seconds=5))
        self.assertEqual(mark.position_id, self.position_id)
        self.assertEqual(mark.liquidation_value_cents, 79600)
        first = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q1, quote_sources=s1, now=NOW + timedelta(seconds=5))
        self.assertEqual(first["status"], "exit_intent")
        same = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q1, quote_sources=s1, now=NOW + timedelta(seconds=16))
        self.assertEqual(same["status"], "exit_intent")
        self.assertIn("same quote", same["blockers"][0])
        q2, s2 = self.quotes(NOW + timedelta(seconds=18), hashes=("e" * 64, "f" * 64))
        closed = OptionsLifecycle(self.ledger, self.root / "options", clock=lambda: NOW).run(
            self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q2, quote_sources=s2, now=NOW + timedelta(seconds=18))
        self.assertEqual(closed["status"], "closed")
        self.assertEqual(self.ledger.get_position(self.position_id).state, "closed")
        self.assertEqual(OptionsLifecycle(self.ledger, self.root / "options").run()["status"], "closed")

    def test_missing_holdings_identity_and_expiry_fail_closed(self):
        manager = self.manager()
        q, s = self.quotes(NOW + timedelta(seconds=5))
        missing = manager.run(self.entry, quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(missing["status"], "reconciliation_required")
        altered = deepcopy(self.entry)
        altered["quantity"] = 1
        altered["position_id"] = self.position_id
        blocked = manager.run(altered, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(blocked["status"], "reconciliation_required")
        first = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(first["status"], "exit_intent")
        expired = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q, quote_sources=s, now=NOW + timedelta(seconds=66))
        self.assertEqual(expired["status"], "intent_expired")

    def test_close_fee_rounds_up_and_restart_recovers_after_ledger_mutation(self):
        manager = self.manager()
        q1, s1 = self.quotes(NOW + timedelta(seconds=5))
        self.assertEqual(manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                                     quotes=q1, quote_sources=s1, now=NOW + timedelta(seconds=5))["status"], "exit_intent")
        original_close = self.ledger.close

        def close_then_raise(*args, **kwargs):
            result = original_close(*args, **kwargs)
            raise RuntimeError("simulated crash after ledger commit")

        self.ledger.close = close_then_raise  # type: ignore[method-assign]
        q2, s2 = self.quotes(NOW + timedelta(seconds=18), hashes=("e" * 64, "f" * 64))
        crashed = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                              quotes=q2, quote_sources=s2, exit_fee_per_spread="0.001",
                              now=NOW + timedelta(seconds=18))
        self.assertEqual(crashed["status"], "reconciliation_required")
        self.assertEqual(self.ledger.get_position(self.position_id).state, "closed")
        recovered = OptionsLifecycle(self.ledger, self.root / "options").run(self.entry, now=NOW + timedelta(seconds=19))
        self.assertEqual(recovered["status"], "closed")

    def test_negative_liability_never_clipped_and_fee_guard_is_persistent(self):
        manager = self.manager()
        q, s = self.quotes(NOW + timedelta(seconds=5), profitable=False)
        result = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot, quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(result["status"], "reconciliation_required")
        self.assertTrue(result["risk_retained"])
        self.assertEqual(self.ledger.get_position(self.position_id).state, "open")
        with self.assertRaises(ValueError):
            manager.position_mark(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                                  quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))

    def test_source_hash_must_cover_normalized_leg_payload(self):
        manager = self.manager()
        q, s = self.quotes(NOW + timedelta(seconds=5))
        bad = deepcopy(s)
        bad[occ(650)]["hash"] = "a" * 64
        result = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                             quotes=q, quote_sources=bad, now=NOW + timedelta(seconds=5))
        self.assertEqual(result["status"], "reconciliation_required")
        self.assertIn("source hash", result["blockers"][0])

    def test_fill_time_is_entry_boundary_and_async_filled_legs_are_valid(self):
        manager = self.manager()
        self.assertNotEqual(self.entry["legs"][0]["timestamp"], self.entry["legs"][1]["timestamp"])
        q, s = self.quotes(NOW + timedelta(seconds=5))
        accepted = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                               quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(accepted["status"], "exit_intent")
        prefill_quotes, prefill_sources = self.quotes(NOW - timedelta(seconds=1))
        with self.assertRaises(ValueError):
            manager.position_mark(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                                  quotes=prefill_quotes, quote_sources=prefill_sources,
                                  now=NOW - timedelta(seconds=1))
        prefill_reports = deepcopy(self.reports)
        prefill_reports[0]["timestamp"] = (NOW - timedelta(seconds=1)).isoformat()
        blocked = OptionsLifecycle(self.ledger, self.root / "prefill-reports").run(
            self.entry, reports=prefill_reports, holdings_snapshot=self.snapshot,
            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(blocked["status"], "reconciliation_required")

    def test_gateway_fill_identity_and_entry_fee_are_not_self_asserted(self):
        q, s = self.quotes(NOW + timedelta(seconds=5))
        arbitrary = deepcopy(self.entry)
        arbitrary["fill_input"]["fill_id"] = "invented-fill"
        arbitrary["fill_input_digest"] = digest(arbitrary["fill_input"])
        result = OptionsLifecycle(self.ledger, self.root / "arbitrary-fill").run(
            arbitrary, reports=self.reports, holdings_snapshot=self.snapshot,
            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(result["status"], "reconciliation_required")
        self.assertIn("does not identify", result["blockers"][0])

        subset = deepcopy(self.entry)
        subset["fill_input"].pop("source_kind")
        subset["fill_input_digest"] = digest(subset["fill_input"])
        result = OptionsLifecycle(self.ledger, self.root / "subset-fill").run(
            subset, reports=self.reports, holdings_snapshot=self.snapshot,
            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertIn("exact normalized", result["blockers"][0])

        altered_fee = deepcopy(self.entry)
        altered_fee["entry_fee_per_spread"] = "0.01"
        result = OptionsLifecycle(self.ledger, self.root / "altered-fee").run(
            altered_fee, reports=self.reports, holdings_snapshot=self.snapshot,
            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertIn("modeled entry fees", result["blockers"][0])

    def test_raw_proposal_prices_may_differ_from_actual_filled_leg_record(self):
        q, s = self.quotes(NOW + timedelta(seconds=5))
        entry = deepcopy(self.entry)
        entry["raw_input"]["legs"][0]["ask"] = "0.04"
        entry["raw_input_digest"] = digest(entry["raw_input"])
        result = OptionsLifecycle(self.ledger, self.root / "changed-proposal-price").run(
            entry, reports=self.reports, holdings_snapshot=self.snapshot,
            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(result["status"], "exit_intent")

    def test_wall_clock_delay_does_not_accept_early_provider_quotes(self):
        manager = self.manager()
        q1, s1 = self.quotes(NOW + timedelta(seconds=5))
        self.assertEqual(manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                                     quotes=q1, quote_sources=s1, now=NOW + timedelta(seconds=5))["status"], "exit_intent")
        early, early_sources = self.quotes(NOW + timedelta(seconds=6), hashes=("e" * 64, "f" * 64))
        result = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                             quotes=early, quote_sources=early_sources, now=NOW + timedelta(seconds=18))
        self.assertEqual(result["status"], "exit_intent")
        self.assertIn("at least ten seconds", result["blockers"][0])

    def test_assignment_or_unknown_activity_latches_accounting_reconciliation(self):
        manager = self.manager(); q, s = self.quotes(NOW + timedelta(seconds=5))
        assigned = deepcopy(self.snapshot); assigned["underlying_quantity"] = 100
        first = manager.run(self.entry, reports=self.reports, holdings_snapshot=assigned,
                            quotes=q, quote_sources=s, now=NOW + timedelta(seconds=5))
        self.assertEqual(first["status"], "accounting_reconciliation_required")
        self.assertTrue(first["accounting_reconciliation_latched"])
        fresh_q, fresh_s = self.quotes(NOW + timedelta(seconds=6))
        still = manager.run(self.entry, reports=self.reports, holdings_snapshot=self.snapshot,
                            quotes=fresh_q, quote_sources=fresh_s, now=NOW + timedelta(seconds=6))
        self.assertEqual(still["status"], "accounting_reconciliation_required")
        self.assertTrue(still["risk_retained"])
        self.assertEqual(self.ledger.get_position(self.position_id).state, "open")


if __name__ == "__main__":
    unittest.main()
