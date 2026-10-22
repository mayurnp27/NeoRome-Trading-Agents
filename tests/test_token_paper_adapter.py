from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import neorome.token_paper_adapter as token_adapter_module
from neorome.crypto_research import TOKEN_PROGRAM
from neorome.jupiter_quotes import SOL, USDC
from neorome.token_paper_adapter import TokenPaperAdapter, TokenPaperError

UTC = timezone.utc
T0 = datetime(2026, 9, 23, tzinfo=UTC)


def iso(value): return value.isoformat().replace("+00:00", "Z")
def digest(value): return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def route(requested, received, incoming, outgoing, amount, output, minimum, source="a" * 64):
    return {"chain": "solana", "requested_at": iso(requested), "received_at": iso(received),
            "response_sha256": source, "quote": {"inputMint": incoming, "outputMint": outgoing,
            "inAmount": str(amount), "outAmount": str(output), "otherAmountThreshold": str(minimum),
            "priceImpact": "0.1", "priceImpactUnit": "percentage_points"}}


ENTRY = route(T0 + timedelta(seconds=1), T0 + timedelta(seconds=2), USDC, SOL,
              4_800_000, 1_000, 900, "a" * 64)
PREFLIGHT = route(T0 + timedelta(seconds=3), T0 + timedelta(seconds=4), SOL, USDC,
                  900, 4_600_000, 4_500_000, "b" * 64)
SELL15 = route(T0 + timedelta(minutes=15, seconds=2), T0 + timedelta(minutes=15, seconds=3),
               SOL, USDC, 900, 4_800_000, 4_700_000, "c" * 64)
SELL60 = route(T0 + timedelta(minutes=60, seconds=2), T0 + timedelta(minutes=60, seconds=3),
               SOL, USDC, 900, 5_200_000, 5_100_000, "2" * 64)
COSTS = {"version": "token-paper-costs-v1", "excludes_embedded_route_fees": True,
         "entry_extra_usdc_raw": "100000", "exit_extra_usdc_raw": "50000"}


def discovery():
    return {"schema_version": 1, "observed_at": iso(T0), "execution_enabled": False,
            "candidates": [{"chain": "solana", "mint": SOL, "known_at": iso(T0),
                            "received_at": iso(T0), "research_status": "observed_pending_risk_review",
                            "screen_rejections": [], "entry_observation": deepcopy(ENTRY)}]}


def review():
    value = {"version": "token-risk-review-v1", "source_kind": "synthetic", "chain": "solana",
             "mint": SOL, "reviewed_at": iso(T0 + timedelta(seconds=5)),
             "token_program": TOKEN_PROGRAM, "mint_authority": None, "freeze_authority": None,
             "transfer_restrictions": "none_reviewed",
             "sellability": "jupiter_size_matched_minimum_output",
             "entry_source_hash": "a" * 64, "sell_source_hash": "b" * 64,
             "owner_concentration_reviewed": True, "liquidity_reviewed": True,
             "evidence_hashes": {"mint": "d" * 64, "accounts": "e" * 64,
                                 "pool": "f" * 64, "transfer": "1" * 64}}
    value["review_sha256"] = digest(value)
    return value


def full_loss(at):
    value = {"version": "token-full-loss-review-v1", "chain": "solana", "mint": SOL,
             "decision": "realize_full_loss", "reason": "explicit synthetic stress",
             "reviewed_at": iso(at)}
    value["review_sha256"] = digest(value)
    return value


class TokenPaperAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)

    def tearDown(self): self.temp.cleanup()

    def enter_open(self):
        adapter = TokenPaperAdapter(self.root)
        result = adapter.run(discovery=discovery(), preflight_observation=PREFLIGHT,
                             risk_review=review(), costs=COSTS, sell_observations=[],
                             now=T0 + timedelta(seconds=6))
        self.assertEqual(result["status"], "unresolved_sell_route")
        self.assertTrue(result["risk_retained"])
        return adapter, result

    def enter_mark(self):
        self.enter_open()
        adapter = TokenPaperAdapter(self.root)
        result = adapter.run(sell_observations=[SELL15],
                             now=T0 + timedelta(minutes=15, seconds=5))
        self.assertEqual(result["status"], "marked_open")
        return adapter, result

    def test_entry_precedes_horizon_evidence_and_uses_review_complete_time(self):
        adapter, _ = self.enter_open()
        position = next(p for p in (adapter.ledger.get_position(e.payload["position_id"])
                                    for e in adapter.ledger.audit() if e.event_type == "fill"))
        self.assertEqual(position.opened_at, iso(T0 + timedelta(seconds=6)))
        self.assertLess(datetime.fromisoformat(position.opened_at.replace("Z", "+00:00")),
                        datetime.fromisoformat(SELL15["received_at"].replace("Z", "+00:00")))
        self.assertFalse(any(e.event_type == "mark" for e in adapter.ledger.audit()))

    def test_future_horizon_evidence_is_ignored_during_entry(self):
        adapter = TokenPaperAdapter(self.root)
        result = adapter.run(discovery=discovery(), preflight_observation=PREFLIGHT,
                             risk_review=review(), costs=COSTS, sell_observations=[SELL15],
                             now=T0 + timedelta(seconds=6))
        self.assertEqual(result["status"], "unresolved_sell_route")
        fill = next(e for e in adapter.ledger.audit() if e.event_type == "fill")
        self.assertEqual(fill.occurred_at, iso(T0 + timedelta(seconds=6)))
        self.assertFalse(any(item["event"] == "synthetic_mark" for item in result["history"]))

    def test_reserve_fill_mark_and_restart_exit_are_idempotent(self):
        adapter, result = self.enter_mark()
        self.assertTrue(result["synthetic"]); self.assertTrue(result["research_only"])
        self.assertFalse(result["execution_enabled"]); self.assertFalse(result["paper_admission_evidence"])
        snapshot = adapter.ledger.snapshot()
        self.assertEqual(snapshot.open_position_cost_cents, 490)
        self.assertLessEqual(snapshot.open_position_cost_cents, 500)
        restarted = TokenPaperAdapter(self.root)
        closed = restarted.run(sell_observations=[SELL60], horizon_minutes=60, action="exit",
                               now=T0 + timedelta(minutes=60, seconds=5))
        self.assertEqual(closed["status"], "closed_route")
        audit_counts = {kind: sum(e.event_type == kind for e in restarted.ledger.audit()) for kind in ("reserve", "fill", "close")}
        self.assertEqual(audit_counts, {"reserve": 1, "fill": 1, "close": 1})
        again = TokenPaperAdapter(self.root).run(action="exit", now=T0 + timedelta(minutes=61))
        self.assertEqual(again["status"], "closed_route")

    def test_unresolved_sell_route_retains_open_risk_without_invented_mark(self):
        adapter, _ = self.enter_mark()
        result = TokenPaperAdapter(self.root).run(sell_observations=[], horizon_minutes=60,
                                                   action="exit", now=T0 + timedelta(minutes=60, seconds=5))
        self.assertEqual(result["status"], "unresolved_sell_route")
        self.assertTrue(result["risk_retained"])
        self.assertGreater(adapter.ledger.snapshot().open_position_cost_cents, 0)

    def test_future_sell_route_cannot_mark_or_exit_early(self):
        self.enter_mark()
        result = TokenPaperAdapter(self.root).run(sell_observations=[SELL60], horizon_minutes=60,
                                                   action="exit", now=T0 + timedelta(minutes=30))
        self.assertEqual(result["status"], "unresolved_sell_route")
        self.assertFalse(any(e.event_type == "close" for e in TokenPaperAdapter(self.root).ledger.audit()))
        self.assertGreater(TokenPaperAdapter(self.root).ledger.snapshot().open_position_cost_cents, 0)

    def test_explicit_full_loss_closes_at_zero_and_is_not_route_failure(self):
        adapter, _ = self.enter_mark(); at = T0 + timedelta(minutes=20)
        result = TokenPaperAdapter(self.root).run(action="full_loss", full_loss_review=full_loss(at), now=at)
        self.assertEqual(result["status"], "closed_full_loss")
        close = next(e for e in adapter.ledger.audit() if e.event_type == "close")
        self.assertEqual(close.payload["proceeds_cents"], 0)

    def test_stop_and_recovery_block_entry_without_reservation(self):
        for kwargs in ({"allow_entry": False}, {"recovery_ok": False}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as root:
                adapter = TokenPaperAdapter(root)
                result = adapter.run(discovery=discovery(), preflight_observation=PREFLIGHT,
                                     risk_review=review(), costs=COSTS, sell_observations=[],
                                     now=T0 + timedelta(seconds=6), **kwargs)
                self.assertEqual(result["status"], "entries_disabled")
                self.assertFalse(any(e.event_type == "reserve" for e in adapter.ledger.audit()))

    def test_restart_cancels_crashed_pending_entry_during_stop(self):
        adapter = TokenPaperAdapter(self.root)
        with patch.object(adapter.ledger, "simulate_fill", side_effect=RuntimeError("synthetic crash")):
            with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
                adapter.run(discovery=discovery(), risk_review=review(), costs=COSTS,
                            preflight_observation=PREFLIGHT, sell_observations=[],
                            now=T0 + timedelta(seconds=6))
        restarted = TokenPaperAdapter(self.root)
        result = restarted.run(discovery=discovery(), preflight_observation=PREFLIGHT,
                               risk_review=review(), costs=COSTS, sell_observations=[],
                               allow_entry=False, now=T0 + timedelta(seconds=7))
        self.assertEqual(result["status"], "entries_disabled")
        reservation = next(e.entity_id for e in restarted.ledger.audit() if e.event_type == "reserve")
        self.assertEqual(restarted.ledger.get_reservation(reservation).state, "cancelled")

    def test_restart_reconciles_close_committed_before_state_write(self):
        self.enter_mark()
        real_write = token_adapter_module._write
        calls = 0

        def crash_after_close(path, value):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("synthetic post-close crash")
            real_write(path, value)

        with patch.object(token_adapter_module, "_write", side_effect=crash_after_close):
            with self.assertRaisesRegex(RuntimeError, "synthetic post-close crash"):
                TokenPaperAdapter(self.root).run(sell_observations=[SELL60], horizon_minutes=60,
                                                  action="exit",
                                                  now=T0 + timedelta(minutes=60, seconds=5))
        recovered = TokenPaperAdapter(self.root).run(now=T0 + timedelta(minutes=61))
        self.assertEqual(recovered["status"], "closed_route")
        self.assertEqual(sum(e.event_type == "close" for e in TokenPaperAdapter(self.root).ledger.audit()), 1)

    def test_identity_controls_costs_and_timing_fail_closed(self):
        cases = []
        bad_review = review(); bad_review["mint"] = USDC; bad_review["review_sha256"] = digest({k:v for k,v in bad_review.items() if k != "review_sha256"}); cases.append((discovery(), bad_review, COSTS, [SELL15]))
        active = review(); active["mint_authority"] = SOL; active["review_sha256"] = digest({k:v for k,v in active.items() if k != "review_sha256"}); cases.append((discovery(), active, COSTS, [SELL15]))
        expensive = dict(COSTS); expensive["entry_extra_usdc_raw"] = "200000"; cases.append((discovery(), review(), expensive, [SELL15]))
        lookahead = discovery(); lookahead["candidates"][0]["received_at"] = iso(T0 + timedelta(seconds=2)); cases.append((lookahead, review(), COSTS, [SELL15]))
        wrong_quantity = deepcopy(PREFLIGHT); wrong_quantity["quote"]["inAmount"] = "901"; cases.append((discovery(), review(), COSTS, wrong_quantity))
        cases = [(disc, controls, costs, PREFLIGHT) for disc, controls, costs, _ in cases[:-1]] + [cases[-1]]
        for disc, controls, costs, preflight in cases:
            with self.subTest(case=len(cases)), tempfile.TemporaryDirectory() as root:
                with self.assertRaises(TokenPaperError):
                    TokenPaperAdapter(root).run(discovery=disc, preflight_observation=preflight,
                                                risk_review=controls, costs=costs,
                                                sell_observations=[], now=T0 + timedelta(seconds=6))

    def test_research_only_outcomes_never_claim_admission_or_profitability(self):
        _, result = self.enter_mark()
        self.assertNotIn("profitable", result)
        self.assertFalse(result["paper_admission_evidence"])
        self.assertTrue(all(item["event"].startswith("synthetic_") for item in result["history"]))


if __name__ == "__main__": unittest.main()
