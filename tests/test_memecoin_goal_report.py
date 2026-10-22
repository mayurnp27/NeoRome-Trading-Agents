import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from neorome.paper_ledger import MarketMark, PaperLedger, Proposal, RiskLimits
from neorome.memecoin_goal_report import GOAL_END, GOAL_START, build_report, publish_report


UTC = timezone.utc
NOW = GOAL_START + timedelta(hours=12)


def make_ledger(path, *, cash=30_000, clock=NOW):
    limits = RiskLimits(aggregate_cap_cents=30_000, group_cap_cents=30_000,
                        per_idea_cents={"prediction": 1, "equities": 1,
                                       "options": 1, "crypto": 5_000})
    return PaperLedger(path, balances={"prediction": 0, "equities": 0,
                                       "options": 0, "crypto": cash}, limits=limits,
                       clock=lambda: clock)


def make_fill(book, when, *, fee=0, value=400, provenance="observed", name="one"):
    proposal = Proposal(
        f"proposal-{name}", "crypto", "test", "v1", "a" * 64,
        when, f"solana:{name}", f"token:{name}", f"token:{name}",
        when + timedelta(hours=1), 1, value, fee, provenance,
        {"token_memecoin_v1": {"cohort_id": "E4"}},
    )
    reservation = book.reserve(proposal, now=when)
    return book.simulate_fill(reservation.reservation_id, fill_id=f"fill-{name}",
                              provenance=provenance, filled_at=when)


def sleeve(report, cohort="E4"):
    return next(item for item in report["sleeves"] if item["cohort_id"] == cohort)


class GoalReportTests(unittest.TestCase):
    def test_future_fill_and_close_are_excluded_from_cutoff(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = make_ledger(root / "paper-e4.sqlite3")
            future = NOW + timedelta(minutes=10)
            fill = make_fill(ledger, future, name="future")
            ledger.close(fill.position_id, proceeds_cents=500, closed_at=future + timedelta(minutes=1),
                         close_id="close-future", provenance="observed")
            report = build_report(root, now=NOW)
            execution = sleeve(report)["ledger"]["execution"]
            self.assertEqual(execution["fills"], 0)
            self.assertEqual(execution["closed_positions"], 0)
            self.assertEqual(sleeve(report)["ledger"]["initial_cash_cents"], 30_000)
            self.assertEqual(sleeve(report)["ledger"]["lifecycle_counts"]["future_fills_excluded"], 1)

    def test_asof_open_position_survives_current_close(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = make_ledger(root / "paper-e4.sqlite3")
            opened = NOW - timedelta(minutes=1)
            fill = make_fill(ledger, opened, name="asof")
            ledger.record_marks("mark-before-close", [MarketMark(fill.position_id, 450, NOW, "b" * 64, "observed")], NOW)
            ledger.close(fill.position_id, proceeds_cents=500, closed_at=NOW + timedelta(minutes=1),
                         close_id="close-later", provenance="observed")
            report = build_report(root, now=NOW)
            execution = sleeve(report)["ledger"]["execution"]
            self.assertEqual(execution["open_positions"], 1)
            self.assertEqual(execution["closed_positions"], 0)
            self.assertEqual(execution["nav_cents"], 30_050)

    def test_stale_mark_withholds_nav_instead_of_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = make_ledger(root / "paper-e4.sqlite3")
            fill = make_fill(ledger, NOW - timedelta(minutes=2), name="stale")
            ledger.record_marks("mark-stale", [MarketMark(fill.position_id, 450,
                                                            NOW - timedelta(seconds=91), "c" * 64,
                                                            "observed")], NOW)
            result = sleeve(build_report(root, now=NOW))["ledger"]
            self.assertEqual(result["mark_coverage"]["status"], "stale")
            self.assertIsNone(result["execution"]["nav_cents"])

    def test_missing_ledger_is_distinct_from_zero_cash_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            make_ledger(root / "paper-e4.sqlite3", cash=0)
            report = build_report(root, now=NOW)
            zero = sleeve(report)["ledger"]
            missing = sleeve(report, "E5")["ledger"]
            self.assertEqual(zero["status"], "ok")
            self.assertEqual(zero["initial_cash_cents"], 0)
            self.assertEqual(zero["execution"]["nav_cents"], 0)
            self.assertEqual(missing["status"], "ledger_missing")
            self.assertIsNone(missing["initial_cash_cents"])

    def test_sleeves_are_isolated_and_fees_flow_through_pnl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = make_ledger(root / "paper-e4.sqlite3")
            fill = make_fill(ledger, NOW - timedelta(minutes=1), fee=5, value=400, name="fees")
            ledger.close(fill.position_id, proceeds_cents=500, fee_cents=7, close_id="close-fees",
                         closed_at=NOW, provenance="observed")
            make_ledger(root / "paper-e5.sqlite3")
            report = build_report(root, now=NOW)
            e4 = sleeve(report, "E4")["ledger"]
            self.assertEqual(e4["execution"]["total_fees_cents"], 12)
            self.assertEqual(e4["execution"]["realized_pnl_cents"], 88)
            self.assertEqual(e4["available_cash_cents"], 30_088)
            self.assertIsNone(report["combined"]["aggregate_nav_cents"])
            self.assertEqual(report["combined"]["required_sleeves_present"], 2)

    def test_candidate_scope_uses_current_cohort_and_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "e4" / "candidates"
            candidates.mkdir(parents=True)
            def write(name, recorded_at, experiments):
                (candidates / name).write_text(json.dumps({"recorded_at": recorded_at,
                                                            "experiments": experiments}))
            write("in.json", _iso(NOW), [{"cohort_id": "E4", "status": "eligible"},
                                          {"cohort_id": "E5", "status": "rejected", "reasons": ["other"]}])
            write("reject.json", _iso(NOW), [{"cohort_id": "E4", "status": "rejected",
                                               "reasons": ["no_route", "no_route"]}])
            write("future.json", _iso(GOAL_END + timedelta(hours=1)),
                  [{"cohort_id": "E4", "status": "eligible"}])
            summary = sleeve(build_report(root, now=NOW))["candidates"]
            self.assertEqual(summary["candidates"], 2)
            self.assertEqual(summary["status_counts"], {"eligible": 1, "rejected": 1})
            self.assertEqual(summary["rejection_reasons"], {"no_route": 1})

    def test_publish_writes_json_markdown_and_history_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "reports" / "goal-comparison-latest.json"
            markdown = root / "reports" / "goal-comparison-latest.md"
            publish_report(root, output, markdown, now=NOW)
            publish_report(root, output, markdown, now=NOW + timedelta(minutes=1))
            self.assertTrue(output.is_file())
            self.assertTrue(markdown.is_file())
            self.assertTrue(list((root / "reports" / "history").glob("*.json")))
            parsed = json.loads(output.read_text())
            self.assertEqual(parsed["as_of"], _iso(NOW + timedelta(minutes=1)))


def _iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    unittest.main()
