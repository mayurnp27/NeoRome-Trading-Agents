from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from desks.equities.intraday_shadow_rehearsal import run_synthetic_rehearsal


class ShadowRehearsalTests(unittest.TestCase):
    def test_synthetic_exit_lifecycle_is_isolated_and_idempotent(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            result = run_synthetic_rehearsal(root, root / "rehearsal.sqlite3")
            self.assertEqual(result["scope"], "synthetic_engineering_rehearsal_only")
            self.assertFalse(result["eligible_for_execution"])
            self.assertEqual(result["status"], "closed_synthetic")
            self.assertEqual(result["reservation_id"], result["restart_reservation_id"])
            self.assertTrue(result["recovery_refused"])
            self.assertTrue(result["stop_refused"])
            self.assertEqual(result["realized_pnl_cents"], -14)
            self.assertTrue((root / "raw" / "quotes").exists())
            bar_record = next((root / "raw" / "bar_prefix").glob("*.json"))
            self.assertEqual(json.loads(bar_record.read_text())["payload"][-1]["timestamp"],
                             "2026-09-21T13:44:00Z")

    def test_synthetic_pending_entry_can_only_cancel_after_expiry(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            result = run_synthetic_rehearsal(root, root / "rehearsal.sqlite3", outcome="cancel")
            self.assertEqual(result["status"], "cancelled_before_fill")
            self.assertEqual(result["cancelled_state"], "cancelled")
            self.assertTrue(result["recovery_refused"])
            self.assertTrue(result["stop_refused"])

    def test_rejects_a_ledger_outside_its_isolated_evidence_root(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(ValueError, "isolated beneath"):
                run_synthetic_rehearsal(root, root.parent / "not-isolated.sqlite3")


if __name__ == "__main__":
    unittest.main()
