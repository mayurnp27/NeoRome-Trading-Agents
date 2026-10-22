from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import stat
import tempfile
import unittest

from neorome.options_lifecycle_inbox import publish, read_events
from neorome.options_internal_paper import run_once
from neorome.options_observed_shadow import run_observed_shadow
from tests.test_options_internal_paper import cloud_for, initialized_ledger
from tests.test_options_observed_shadow import NOW, _evidence


class OptionsLifecycleInboxTests(unittest.TestCase):
    def test_immutable_entry_mark_exit_events_drive_restartable_paper_lifecycle(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        symbols = {decision["shadow_candidate"]["long"]["symbol"], decision["shadow_candidate"]["short"]["symbol"]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); inbox = root / "inbox"; state = root / "state"; ledger = root / "paper.sqlite3"
            initialized_ledger(ledger)
            self.assertEqual(run_once(candidate=decision, state_dir=state, ledger_path=ledger, now=NOW)["status"], "reserved")
            captures = []
            for phase, seconds in (("entry", 12), ("mark", 30), ("exit", 50), ("exit", 70)):
                capture = cloud_for(decision, NOW + timedelta(seconds=seconds))
                publish(phase=phase, event_id=f"{phase}-{seconds}", date=NOW.date().isoformat(), capture=capture,
                        expected_symbols=symbols, root=inbox)
                captures.append(capture)
            events = read_events(inbox)
            self.assertEqual([event["phase"] for event in events], ["entry", "mark", "exit", "exit"])
            entry = events[0]["capture"]; mark = events[1]["capture"]
            filled = run_once(candidate=None, entry_cloud=entry, state_dir=state, ledger_path=ledger, now=NOW + timedelta(seconds=14))
            self.assertEqual(filled["status"], "filled_internal_paper")
            marked = run_once(candidate=None, mark_cloud=mark, state_dir=state, ledger_path=ledger, now=NOW + timedelta(seconds=32))
            self.assertEqual(marked["mark_status"], "persisted")
            first_exit = run_once(candidate=None, exit_cloud=events[2]["capture"], state_dir=state, ledger_path=ledger, now=NOW + timedelta(seconds=52))
            restarted = run_once(candidate=None, exit_cloud=events[3]["capture"], state_dir=state, ledger_path=ledger, now=NOW + timedelta(seconds=72))
            self.assertEqual(first_exit["orders_sent"], 0)
            self.assertEqual(restarted["orders_sent"], 0)
            self.assertTrue((state / "state.json").exists())

    def test_lifecycle_units_keep_the_neorome_group_read_boundary(self):
        for name, phase in (("entry", "entry"), ("mark", "mark"), ("exit", "exit")):
            unit = Path(f"deploy/recorder/neorome-options-{name}-capture.service").read_text()
            self.assertIn("User=neorome-options", unit)
            self.assertIn("Group=neorome", unit)
            self.assertIn(f"--phase {phase}", unit)
            self.assertIn("--lifecycle-inbox-root /var/lib/neorome-options-inbox/lifecycle", unit)

    def test_full_port_lifecycle_units_and_timers_stay_with_the_isolated_route(self):
        for name, phase in (("entry", "entry"), ("mark", "mark"), ("exit", "exit")):
            unit = Path(f"deploy/recorder/neorome-options-full-port-{name}-capture.service").read_text()
            timer = Path(f"deploy/recorder/neorome-options-full-port-{name}-capture.timer").read_text()
            self.assertIn("User=neorome-options", unit)
            self.assertIn("Group=neorome", unit)
            self.assertIn(f"--phase {phase}", unit)
            self.assertIn("--candidate-inbox-root /var/lib/neorome-options-full-port-inbox", unit)
            self.assertIn("--lifecycle-inbox-root /var/lib/neorome-options-full-port-inbox/lifecycle", unit)
            self.assertIn(f"Unit=neorome-options-full-port-{name}-capture.service", timer)
            self.assertIn("Persistent=false", timer)
        for phase in ("reserve", "entry", "mark", "exit"):
            timer = Path(f"deploy/recorder/neorome-options-full-port-internal-paper-{phase}.timer").read_text()
            self.assertIn("Unit=neorome-options-full-port-internal-paper.service", timer)
            self.assertIn("Persistent=false", timer)
        worker = Path("deploy/recorder/neorome-options-full-port-internal-paper.service").read_text()
        self.assertIn("User=neorome", worker)
        self.assertIn("--full-port", worker)
        self.assertIn("--ledger /var/lib/neorome-options-full-port/paper.sqlite3", worker)
        self.assertIn("--lifecycle-inbox-root /var/lib/neorome-options-full-port-inbox/lifecycle", worker)

    def test_lifecycle_event_stays_group_readable_under_service_umask(self):
        decision = run_observed_shadow(_evidence(), as_of=NOW)
        symbols = {decision["shadow_candidate"]["long"]["symbol"], decision["shadow_candidate"]["short"]["symbol"]}
        with tempfile.TemporaryDirectory() as directory:
            previous = os.umask(0o077)
            try:
                artifact = publish(phase="entry", event_id="entry-umask", date=NOW.date().isoformat(),
                                   capture=cloud_for(decision, NOW + timedelta(seconds=12)),
                                   expected_symbols=symbols, root=Path(directory) / "inbox")
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o640)
