from datetime import datetime, timedelta, timezone
import json
import sqlite3
from pathlib import Path
import tempfile
from unittest.mock import patch

from neorome.jupiter_quotes import SOL, USDC
from neorome.paper_ledger import PaperLedger, RiskLimits
from neorome.token_memecoin_runtime import run_once
from neorome.token_social import SocialError


UTC = timezone.utc
NOW = datetime(2026, 9, 25, 14, 0, tzinfo=UTC)


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def route(received, incoming, outgoing, amount, output, minimum, digest):
    return {
        "received_at": iso(received), "response_sha256": digest,
        "quote": {"inputMint": incoming, "outputMint": outgoing, "inAmount": str(amount),
                   "outAmount": str(output), "otherAmountThreshold": str(minimum),
                   "priceImpact": "0.1", "priceImpactUnit": "percentage_points"},
    }


ENTRY = route(NOW, USDC, SOL, 4_800_000, 1_000, 900, "a" * 64)
SELL_PREFLIGHT = route(NOW + timedelta(seconds=1), SOL, USDC, 900, 4_700_000, 4_600_000, "b" * 64)
SOCIAL = {
    "received_at": iso(NOW), "response_sha256": "c" * 64,
    "signals": {"mentions": 3, "unique_credible_authors": 2,
                 "bot_post_fraction": "0", "rug_warning_post_ids": []},
}
MINT = SOL


def candidate_bundle():
    return {"candidates": [{
        "mint": MINT,
        "pool": {"buys_h1": 5, "sells_h1": 2, "volume_h1_usd": 1_000},
        "account_concentration": {"largest_account_supply_fraction": "0.10",
                                   "listed_accounts_supply_fraction": "0.50"},
        "quotes": {"observations": [ENTRY, SELL_PREFLIGHT]},
        "evidence": [{"received_at": iso(NOW), "response_sha256": "d" * 64}],
    }]}


def invoke(root, ledger, *, research, reserve_patch=None, fill_patch=None, now=NOW):
    patches = [patch("neorome.token_memecoin_runtime.research", research)]
    if reserve_patch is not None:
        patches.append(patch.object(PaperLedger, "reserve", reserve_patch))
    if fill_patch is not None:
        patches.append(patch.object(PaperLedger, "simulate_fill", fill_patch))
    for item in patches:
        item.start()
    try:
        return run_once(root / "state", ledger, now=now, sources=object(), quotes=object(),
                        social_client=type("Social", (), {"search_contract": lambda self, mint: SOCIAL})())
    finally:
        for item in reversed(patches):
            item.stop()


def audit_types(ledger):
    return [event.event_type for event in PaperLedger(ledger, enforce_market_risk=True, clock=lambda: NOW).audit()]


def test_locked_reserve_is_audited_and_retried_without_duplicate_fill():
    with tempfile.TemporaryDirectory() as directory:
        root, ledger = Path(directory), Path(directory) / "paper.sqlite3"
        locked = lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked"))
        first = invoke(root, ledger, research=lambda *args: candidate_bundle(), reserve_patch=locked)
        assert first == {"status": "abstain", "reason": "shared_ledger_busy", "operation": "reserve_or_fill",
                         "retry": "next_timer", "pending_reconciliation": True, "orders_sent": 0}
        state = json.loads((root / "state" / "state.json").read_text())
        assert state["last_ledger_contention"]["reason"] == "shared_ledger_busy"
        assert audit_types(ledger) == ["market_marks"]

        second = invoke(root, ledger, research=lambda *args: (_ for _ in ()).throw(AssertionError("rediscovered")),
                        now=NOW + timedelta(minutes=1))
        assert second["status"] == "simulated_fill"
        assert second["fill_recovered"] is True
        assert audit_types(ledger).count("reserve") == 1
        assert audit_types(ledger).count("fill") == 1


def test_locked_fill_recovers_committed_reservation_once():
    with tempfile.TemporaryDirectory() as directory:
        root, ledger = Path(directory), Path(directory) / "paper.sqlite3"
        original = PaperLedger.simulate_fill
        calls = {"count": 0}

        def locked_once(self, *args, **kwargs):
            if calls["count"] == 0:
                calls["count"] += 1
                raise sqlite3.OperationalError("database is locked")
            return original(self, *args, **kwargs)

        first = invoke(root, ledger, research=lambda *args: candidate_bundle(), fill_patch=locked_once)
        assert first["reason"] == "shared_ledger_busy"
        assert first["operation"] == "fill_after_reserve"
        assert audit_types(ledger).count("reserve") == 1
        assert audit_types(ledger).count("fill") == 0

        second = invoke(root, ledger, research=lambda *args: (_ for _ in ()).throw(AssertionError("rediscovered")),
                        now=NOW + timedelta(minutes=1))
        assert second["status"] == "simulated_fill"
        assert second["fill_recovered"] is True
        assert audit_types(ledger).count("reserve") == 1
        assert audit_types(ledger).count("fill") == 1


def test_eligible_candidate_still_admits_internal_paper_fill():
    with tempfile.TemporaryDirectory() as directory:
        root, ledger = Path(directory), Path(directory) / "paper.sqlite3"
        result = invoke(root, ledger, research=lambda *args: candidate_bundle())
        assert result["status"] == "simulated_fill"
        book = PaperLedger(ledger, enforce_market_risk=True, clock=lambda: NOW)
        assert len([event for event in book.audit() if event.event_type == "fill"]) == 1
        assert book.get_position(result["fill"]["position_id"]).state == "open"


def test_x_signal_gap_persists_candidate_and_cohort_evidence_without_a_fill():
    with tempfile.TemporaryDirectory() as directory:
        root, ledger = Path(directory), Path(directory) / "paper.sqlite3"
        social = type("Social", (), {"search_contract": lambda self, mint: (_ for _ in ()).throw(SocialError("X search HTTP 402"))})()
        with patch("neorome.token_memecoin_runtime.research", lambda *args: candidate_bundle()):
            result = run_once(root / "state", ledger, now=NOW, sources=object(), quotes=object(), social_client=social)
        assert result["status"] == "abstain"
        files = list((root / "state" / "candidates").glob("*.json"))
        assert len(files) == 1
        persisted = json.loads(files[0].read_text())
        assert persisted["social"]["channel_status"] == "unavailable"
        assert persisted["eligible_for_internal_paper"] is False
        assert {item["cohort_id"] for item in persisted["experiments"]} == {"M0", "M1", "M2", "M3"}
        assert [event for event in PaperLedger(ledger, enforce_market_risk=True, clock=lambda: NOW).audit() if event.event_type == "fill"] == []


def test_x_cooldown_avoids_repeating_a_refused_social_request():
    with tempfile.TemporaryDirectory() as directory:
        root, ledger = Path(directory), Path(directory) / "paper.sqlite3"
        state = root / "state" / "state.json"; state.parent.mkdir(parents=True)
        state.write_text(json.dumps({"schema_version": 1, "policy_version": "memecoin-paper-v1", "seen": [], "active": None,
                                     "x_retry_not_before": iso(NOW + timedelta(minutes=10))}))
        social = type("Social", (), {"search_contract": lambda self, mint: (_ for _ in ()).throw(AssertionError("should not call X"))})()
        with patch("neorome.token_memecoin_runtime.research", lambda *args: candidate_bundle()):
            result = run_once(root / "state", ledger, now=NOW, sources=object(), quotes=object(), social_client=social)
        assert result["reason"] == "x_signal_unavailable: X retry cooldown active"
        persisted = json.loads(next((root / "state" / "candidates").glob("*.json")).read_text())
        assert persisted["social"]["error"] == "X retry cooldown active"


def test_isolated_program_sleeve_uses_300_crypto_dollars_without_mutating_shared_ledger():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        program = root / "program.sqlite3"
        shared = root / "shared.sqlite3"
        with patch("neorome.token_memecoin_runtime.research", lambda *args: candidate_bundle()):
            result = run_once(root / "program-state", program, now=NOW, sources=object(), quotes=object(),
                              social_client=type("Social", (), {"search_contract": lambda self, mint: SOCIAL})(),
                              crypto_sleeve_cents=30_000)
        assert result["status"] == "simulated_fill"
        balances = {item.desk: item.initial_cash_cents for item in PaperLedger(
            program, balances={"prediction": 1, "equities": 1, "options": 1, "crypto": 30_000},
            limits=RiskLimits(
                aggregate_cap_cents=30_000, group_cap_cents=30_000,
                per_idea_cents={"prediction": 1, "equities": 1, "options": 1, "crypto": 500}),
            enforce_market_risk=True, clock=lambda: NOW).balances()}
        assert balances["crypto"] == 30_000
        assert not shared.exists()


def test_active_route_marks_the_ledger_before_reporting_an_open_position():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        ledger = root / "program.sqlite3"
        with patch("neorome.token_memecoin_runtime.research", lambda *args: candidate_bundle()):
            opened = run_once(root / "state", ledger, now=NOW, sources=object(), quotes=object(),
                              social_client=type("Social", (), {"search_contract": lambda self, mint: SOCIAL})(),
                              crypto_sleeve_cents=30_000)
        assert opened["status"] == "simulated_fill"
        sell = route(NOW + timedelta(seconds=1), SOL, USDC, 900, 4_700_000, 4_600_000, "e" * 64)
        quotes = type("Quotes", (), {"quote": lambda self, mint, output, amount: sell})()
        marked = run_once(root / "state", ledger, now=NOW + timedelta(seconds=1), quotes=quotes,
                          crypto_sleeve_cents=30_000)
        assert marked["status"] == "marked_open"
        assert marked["mark"]["gross_proceeds_cents"] == 460
        assert marked["mark"]["exit_fee_cents"] == 5
        assert marked["mark"]["liquidation_value_cents"] == 455
        snapshot = PaperLedger(ledger, balances={"prediction": 1, "equities": 1, "options": 1, "crypto": 30_000},
                               limits=RiskLimits(aggregate_cap_cents=30_000, group_cap_cents=30_000,
                               per_idea_cents={"prediction": 1, "equities": 1, "options": 1, "crypto": 500}),
                               enforce_market_risk=True, clock=lambda: NOW + timedelta(seconds=1)).market_snapshot()
        assert snapshot.coverage_current is True
        assert snapshot.market_nav_cents == 30_000 - 485 + 455 + 3


def test_stop_close_records_gross_proceeds_and_a_capped_exit_fee():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        ledger = root / "program.sqlite3"
        with patch("neorome.token_memecoin_runtime.research", lambda *args: candidate_bundle()):
            opened = run_once(root / "state", ledger, now=NOW, sources=object(), quotes=object(),
                              social_client=type("Social", (), {"search_contract": lambda self, mint: SOCIAL})(),
                              crypto_sleeve_cents=30_000)
        sell = route(NOW + timedelta(seconds=1), SOL, USDC, 900, 30_000, 30_000, "f" * 64)
        quotes = type("Quotes", (), {"quote": lambda self, mint, output, amount: sell})()
        closed = run_once(root / "state", ledger, now=NOW + timedelta(seconds=1), quotes=quotes,
                          crypto_sleeve_cents=30_000)
        assert closed["status"] == "simulated_close"
        assert closed["reason"] == "stop_50pct"
        assert closed["close"]["proceeds_cents"] == 3
        assert closed["close"]["fee_cents"] == 3
        fill = opened["fill"]
        assert fill["fee_cents"] == 5
