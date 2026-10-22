import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome.memecoin_program_report import build_report
from neorome.paper_ledger import MarketMark, PaperLedger, Proposal, RiskLimits


UTC = timezone.utc
NOW = datetime(2026, 9, 25, 14, 0, tzinfo=UTC)


def _isolated_ledger(path):
    limits = RiskLimits(aggregate_cap_cents=30_000, group_cap_cents=30_000,
                        per_idea_cents={"prediction": 1, "equities": 1, "options": 1, "crypto": 500})
    return PaperLedger(path, balances={"prediction": 1, "equities": 1, "options": 1, "crypto": 30_000},
                       limits=limits, enforce_market_risk=True, clock=lambda: NOW)


def test_report_counts_candidate_cohorts_and_isolated_crypto_cash():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        candidates = root / "state" / "candidates"
        candidates.mkdir(parents=True)
        (candidates / "a.json").write_text(json.dumps({
            "eligible_for_internal_paper": False, "entry_quote": {"quote": {}}, "sell_preflight": {"quote": {}},
            "social": {"channel_status": "unavailable"}, "rejection_reasons": ["x_contract_mentions_below_3"],
            "experiments": [{"cohort_id": "M0", "status": "rejected"}, {"cohort_id": "M1", "status": "unresolved"}],
        }))
        _isolated_ledger(root / "program.sqlite3")
        report = build_report(root / "state", root / "program.sqlite3")
        assert report["mode"] == "internal_paper_only"
        assert report["discovery"]["retained_candidates"] == 1
        assert report["discovery"]["cohort_status"] == {"M0:rejected": 1, "M1:unresolved": 1}
        assert report["execution"]["initial_cash_cents"] == 30_000
        assert report["execution"]["net_pnl_cents"] == 0
        assert report["execution"]["target_nav_cents"] == 70_000
        assert report["execution"]["target_gap_cents"] == 40_000
        assert report["execution"]["target_reached"] is False


def test_report_withholds_nav_when_an_open_mark_has_expired():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        book = _isolated_ledger(root / "program.sqlite3")
        book.record_marks("initial", [], NOW)
        proposal = Proposal("meme", "crypto", "test", "v1", "a" * 64, NOW, "solana:test", "token:test",
                            "token:test", NOW + timedelta(minutes=1), 1, 480, 5, "observed")
        reservation = book.reserve(proposal, now=NOW)
        fill = book.simulate_fill(reservation.reservation_id, fill_id="fill-meme", provenance="observed", filled_at=NOW)
        book.record_marks("open", [MarketMark(fill.position_id, 455, NOW, "b" * 64, "observed")], NOW)
        fresh = build_report(root / "state", root / "program.sqlite3", now=NOW)
        assert fresh["execution"]["mark_coverage_status"] == "complete"
        assert fresh["execution"]["sleeve_nav_cents"] == 30_000 - 485 + 455
        stale = build_report(root / "state", root / "program.sqlite3", now=NOW + timedelta(seconds=91))
        assert stale["execution"]["mark_coverage_status"] == "stale"
        assert stale["execution"]["sleeve_nav_cents"] is None
        assert stale["execution"]["net_pnl_cents"] is None
        assert stale["execution"]["target_gap_cents"] is None
