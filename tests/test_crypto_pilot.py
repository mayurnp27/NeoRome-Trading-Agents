import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome.crypto_pilot import run
from neorome.paper_ledger import PaperLedger, RiskLimits


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)


def bars(now=NOW, rising=True):
    first = now.date() - timedelta(days=102)
    result = []
    for index in range(102):
        close = 100 + index if rising else 300 - index
        result.append(
            {
                "timestamp": f"{first + timedelta(days=index)}T00:00:00Z",
                "complete": True,
                "open": str(close),
                "high": str(close),
                "low": str(close - 1),
                "close": str(close),
            }
        )
    return result


def evidence(now=NOW, rising=True, source_kind="observed"):
    return {
        "schema_version": 1,
        "attempted_at": now.isoformat(),
        "completed_at": now.isoformat(),
        "mode": "flat_research_only",
        "policy_id": "CR-001",
        "policy_version": "cr-001-v1",
        "source_kind": source_kind,
        "execution_enabled": False,
        "orders_sent": 0,
        "ledger_mutations": 0,
        "profit_claim": False,
        "bars_source": {"source_kind": "observed", "provider": "test"},
        "bars": bars(now, rising),
    }


def quote(now, *, price="100", depth="1", source_hash="a" * 64):
    return (
        {"symbol": "BTC/USD", "timestamp": now.isoformat(), "bid": price, "ask": price, "depth_units": depth},
        {"source_kind": "observed", "sha256": source_hash},
    )


class CryptoPilotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state = self.root / "pilot"
        self.ledger_path = self.root / "paper" / "paper.sqlite3"

    def tearDown(self):
        self.temp.cleanup()

    def call(self, at, *, rising=True, q=None, source=None, daily=None, allow_entry=True, mark_provider=None):
        q, source = q or quote(at)
        return run(
            state_dir=self.state,
            now=at,
            ledger_path=self.ledger_path,
            daily_evidence=daily or evidence(at, rising),
            quote=q,
            quote_source=source,
            allow_entry=allow_entry,
            mark_provider=mark_provider,
        )

    def test_shared_mark_failure_blocks_arrival_fill(self):
        self.call(NOW)
        def unavailable(book, now):
            raise ValueError("foreign quote unavailable")
        result = self.call(NOW + timedelta(seconds=20), mark_provider=unavailable)
        self.assertFalse(result["actions"])
        self.assertIn("foreign quote unavailable", result["blocker"])
        book = PaperLedger(self.ledger_path, enforce_market_risk=True)
        self.assertFalse(any(e.event_type == "fill" for e in book.audit()))

    def test_foreign_mark_failure_does_not_strand_crypto_exit(self):
        self.call(NOW)
        self.call(NOW + timedelta(seconds=11))
        def unavailable(book, now):
            raise ValueError("foreign quote unavailable")
        target = self.call(NOW + timedelta(seconds=25), rising=False,
                           mark_provider=unavailable)
        self.assertEqual(target["blocker"], "exit_target_pending")
        closed = self.call(NOW + timedelta(seconds=40), rising=False,
                           mark_provider=unavailable)
        self.assertEqual([a["type"] for a in closed["actions"]], ["simulated_close"])
        self.assertIn("foreign quote unavailable", closed["blocker"])

    def test_maintenance_blocks_entries_but_marks_existing_position(self):
        blocked = self.call(NOW, allow_entry=False)
        self.assertEqual(blocked["blocker"], "entries_disabled")
        self.assertEqual(blocked["actions"], [])
        self.call(NOW)
        self.call(NOW + timedelta(seconds=20))
        marked = self.call(NOW + timedelta(seconds=40), allow_entry=False)
        self.assertTrue(marked["market_snapshot"]["coverage_current"])
        self.assertIsNone(marked["blocker"])

    def test_maintenance_cancels_pending(self):
        self.call(NOW)
        result = self.call(NOW + timedelta(seconds=20), allow_entry=False)
        self.assertEqual(result["actions"][0]["type"], "cancel_risk_stop")
        self.assertFalse(any(e.event_type == "fill" for e in PaperLedger(self.ledger_path, enforce_market_risk=True).audit()))

    def test_risk_stop_after_reservation_cancels_before_fill(self):
        first = self.call(NOW)
        reservation_id = first["actions"][0]["result"]["reservation_id"]
        book = PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW)
        book.set_risk_stop(True, reason="test", at=NOW + timedelta(seconds=1))
        result = self.call(NOW + timedelta(seconds=20))
        self.assertEqual([item["type"] for item in result["actions"]], ["cancel_risk_stop"])
        self.assertEqual(book.get_reservation(reservation_id).state, "cancelled")
        self.assertFalse(any(event.event_type == "fill" for event in book.audit()))

    def test_reserve_requires_later_quote_then_fill_mark_exit_and_restart(self):
        first = self.call(NOW)
        self.assertEqual([action["type"] for action in first["actions"]], ["reserve"])
        reservation_id = first["actions"][0]["result"]["reservation_id"]

        same = self.call(NOW + timedelta(seconds=5), q=quote(NOW))
        self.assertEqual(same["actions"], [])
        self.assertEqual(same["blocker"], "awaiting_independent_post_reservation_quote")

        later_quote = quote(NOW + timedelta(seconds=11), source_hash="b" * 64)
        filled = self.call(NOW + timedelta(seconds=11), q=later_quote)
        self.assertEqual([action["type"] for action in filled["actions"]], ["simulated_fill"])
        book = PaperLedger(self.ledger_path, enforce_market_risk=True, clock=lambda: NOW + timedelta(seconds=11))
        self.assertEqual(len([event for event in book.audit() if event.event_type == "fill"]), 1)
        self.assertEqual(filled["cash_reconciliation"]["reconciled"], True)

        marked = self.call(NOW + timedelta(seconds=20), q=quote(NOW + timedelta(seconds=20), source_hash="c" * 64))
        self.assertEqual(marked["actions"], [])
        self.assertIsNone(marked["blocker"])

        target = self.call(
            NOW + timedelta(seconds=31),
            rising=False,
            q=quote(NOW + timedelta(seconds=31), price="99", source_hash="d" * 64),
        )
        self.assertEqual(target["actions"], [])
        self.assertEqual(target["blocker"], "exit_target_pending")
        exited = self.call(
            NOW + timedelta(seconds=42),
            rising=False,
            q=quote(NOW + timedelta(seconds=42), price="99", source_hash="e" * 64),
        )
        self.assertEqual([action["type"] for action in exited["actions"]], ["simulated_close"])
        self.assertEqual(exited["blocker"], None)
        lifecycle = json.loads((self.state / "state.json").read_text())
        self.assertEqual(lifecycle["fill_evidence"][0]["source_hash"], "b" * 64)
        self.assertEqual(lifecycle["fill_evidence"][0]["depth_units"], "1")
        self.assertEqual(lifecycle["close_evidence"][0]["source_hash"], "e" * 64)
        self.assertEqual(lifecycle["close_evidence"][0]["bid"], "99")
        restarted = self.call(NOW + timedelta(seconds=50), rising=False, q=quote(NOW + timedelta(seconds=50), source_hash="f" * 64))
        self.assertEqual(restarted["actions"], [])
        self.assertEqual(restarted["cash_reconciliation"]["reconciled"], True)
        self.assertEqual(json.loads((self.state / "state.json").read_text())["last_action"], "close")

    def test_wrong_source_stale_quote_and_depth_fail_closed(self):
        bad = evidence(NOW, source_kind="synthetic")
        wrong = self.call(NOW, daily=bad)
        self.assertIn("observed", wrong["blocker"] or "")

        stale = quote(NOW - timedelta(seconds=61))
        result = self.call(NOW, q=stale)
        self.assertEqual(result["actions"], [])
        self.assertEqual(result["blocker"], "quote_stale")

        shallow = self.call(NOW + timedelta(days=1), q=quote(NOW + timedelta(days=1), depth="0.0000001"))
        self.assertEqual(shallow["actions"], [])
        self.assertIn("depth", shallow["blocker"] or "")

    def test_pending_reservation_expires_and_fixed_idea_does_not_reset(self):
        first = self.call(NOW)
        self.assertEqual(first["actions"][0]["type"], "reserve")
        missing_daily = self.root / "does-not-exist.json"
        expired = run(
            daily_evidence_path=missing_daily,
            state_dir=self.state,
            now=NOW + timedelta(seconds=61),
            ledger_path=self.ledger_path,
            quote=quote(NOW + timedelta(seconds=61), source_hash="f" * 64)[0],
            quote_source=quote(NOW + timedelta(seconds=61), source_hash="f" * 64)[1],
        )
        self.assertEqual(expired["actions"][0]["type"], "cancel_expired_reservation")
        # A new date can reserve again only after the prior reservation was
        # cancelled; active paper exposure remains under the fixed idea ID.
        next_day = self.call(NOW + timedelta(days=1), q=quote(NOW + timedelta(days=1), source_hash="1" * 64))
        self.assertEqual(next_day["actions"][0]["type"], "reserve")
        proposal = next(event for event in PaperLedger(self.ledger_path, enforce_market_risk=True).audit() if event.event_type == "reserve")
        self.assertEqual(proposal.payload["payload"]["idea_id"], "CR-001:BTC/USD")

    def test_current_daily_evidence_file_round_trip(self):
        path = self.root / "decision.json"
        path.write_text(json.dumps(evidence(NOW)))
        q, source = quote(NOW)
        result = run(path, self.state, NOW, ledger_path=self.ledger_path, quote=q, quote_source=source)
        self.assertEqual(result["actions"][0]["type"], "reserve")
        self.assertEqual(result["status"], "ok")

    def test_open_position_is_marked_before_invalid_new_daily_evidence_blocks(self):
        self.call(NOW)
        self.call(NOW + timedelta(seconds=11), q=quote(NOW + timedelta(seconds=11), source_hash="4" * 64))
        invalid = {"source_kind": "observed", "execution_enabled": False, "completed_at": NOW.isoformat()}
        result = self.call(NOW + timedelta(seconds=20), q=quote(NOW + timedelta(seconds=20), source_hash="5" * 64), daily=invalid)
        self.assertIn("daily evidence", result["blocker"] or "")
        self.assertTrue(result["market_snapshot"]["coverage_current"])

    def test_exit_same_quote_is_durable_pending_close(self):
        self.call(NOW)
        fill_quote = quote(NOW + timedelta(seconds=11), source_hash="2" * 64)
        self.call(NOW + timedelta(seconds=11), q=fill_quote)
        same_exit = self.call(NOW + timedelta(seconds=12), rising=False, q=fill_quote)
        self.assertEqual(same_exit["actions"], [])
        self.assertEqual(same_exit["blocker"], "exit_target_pending")
        state = json.loads((self.state / "state.json").read_text())
        self.assertEqual(state["pending_close"]["target"], "exit")
        later = self.call(NOW + timedelta(seconds=23), rising=False, q=quote(NOW + timedelta(seconds=23), source_hash="3" * 64))
        self.assertEqual([action["type"] for action in later["actions"]], ["simulated_close"])

    def test_firm_cap_mismatch_is_refused(self):
        self.ledger_path.parent.mkdir(parents=True)
        PaperLedger(
            self.ledger_path,
            enforce_market_risk=True,
            limits=RiskLimits(per_idea_cents={"prediction": 500, "equities": 1500, "options": 1500, "crypto": 600}),
        )
        with self.assertRaises(ValueError):
            self.call(NOW)


if __name__ == "__main__":
    unittest.main()
