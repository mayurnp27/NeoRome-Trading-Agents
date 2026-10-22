from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from neorome.equity_intraday_opportunity import run_once


UTC = timezone.utc
OPEN = datetime(2026, 9, 23, 13, 46, tzinfo=UTC)
CLOSED = datetime(2026, 9, 23, 17, 55, tzinfo=UTC)


class ForwardOpportunityTests(unittest.TestCase):
    def test_closed_window_records_no_backdating_without_provider_or_ledger_access(self):
        calls = []

        def unexpected(*_args, **_kwargs):
            calls.append(True)
            raise AssertionError("closed-window scanner must not access provider or ledger")

        with TemporaryDirectory() as root:
            result = run_once(root, "unused-universe", "MU", state_dir="unused-state", ledger_path="unused-ledger",
                              now=CLOSED, decision_capture=unexpected, lifecycle=unexpected)
            self.assertEqual(result["status"], "abstained")
            self.assertEqual(result["blocker"], "entry_window_closed_no_backdating")
            self.assertEqual(result["ledger_mutations"], 0)
            self.assertEqual(result["broker_orders_sent"], 0)
            self.assertFalse(calls)
            self.assertTrue((Path(root) / "opportunities" / f"{result['opportunity_sha256']}.json").is_file())

    def test_open_window_reserves_then_requires_a_separate_later_quote_before_fill(self):
        calls = []
        decision = {"status": "eligible_unfilled_candidate", "decision_sha256": "d" * 64}
        quote = {"status": "captured", "capture_sha256": "e" * 64,
                 "quote": {"symbol": "MU", "source_sha256": "f" * 64}}

        def capture_decision(*_args, **kwargs):
            calls.append(("decision", kwargs["signal_at"])); return decision

        def capture_quote(*args, **_kwargs):
            calls.append(("quote", args[2])); return quote

        def lifecycle(*_args, **kwargs):
            calls.append(("lifecycle", "entry_fill_quote" in kwargs))
            return ({"status": "advanced", "actions": [{"type": "reserve_observed_internal_paper"}], "blocker": None}
                    if len([item for item in calls if item[0] == "lifecycle"]) == 1
                    else {"status": "advanced", "actions": [{"type": "simulated_observed_entry"}], "blocker": None})

        with TemporaryDirectory() as root:
            result = run_once(root, "universe", "MU", state_dir="state", ledger_path="ledger", now=OPEN,
                              client=object(), decision_capture=capture_decision, quote_capture=capture_quote,
                              lifecycle=lifecycle)
        self.assertEqual(result["status"], "advanced")
        self.assertEqual(result["ledger_mutations"], 2)
        self.assertEqual([item[0] for item in calls], ["decision", "lifecycle", "quote", "lifecycle"])
        self.assertFalse(calls[1][1])
        self.assertTrue(calls[3][1])
        self.assertEqual(result["broker_orders_sent"], 0)


if __name__ == "__main__":
    unittest.main()
