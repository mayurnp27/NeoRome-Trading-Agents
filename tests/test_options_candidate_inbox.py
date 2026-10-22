from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

from neorome.options_candidate_inbox import CandidateInboxError, publish, read_current_candidate
from neorome.options_dated_artifacts import sha256


def _candidate():
    value = {"schema_version": 1, "artifact_kind": "options-observed-shadow-decision-v1", "status": "eligible_shadow_candidate",
             "reason_code": "eligible", "reason": "retained read-only decision", "shadow_candidate": {"status": "candidate"},
             "orders_sent": 0, "reservations_created": 0, "ledger_written": False, "broker_called": False,
             "profitability_status": "not_evaluated", "evidence": {"provider": "robinhood-trading", "quote_evidence_sha256": "a" * 64}}
    value["artifact_sha256"] = sha256(value)
    return value


class OptionsCandidateInboxTests(unittest.TestCase):
    def test_hash_bound_sanitized_inbox_is_group_readable_not_world_readable_and_rolls_forward(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "inbox"
            first = publish(_candidate(), root, date="2026-09-23")
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o750)
            self.assertEqual(stat.S_IMODE(first.stat().st_mode), 0o640)
            self.assertFalse(first.is_symlink())
            self.assertEqual(read_current_candidate(root)["artifact_sha256"], _candidate()["artifact_sha256"])
            second = publish(_candidate(), root, date="2026-09-24")
            self.assertEqual(second.parent.name, "2026-09-24")
            self.assertEqual(json.loads((root / "current.json").read_text())["date"], "2026-09-24")
            self.assertEqual(stat.S_IMODE((root / "current.json").stat().st_mode), 0o640)

    def test_raw_provider_material_and_bad_hash_are_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            raw = _candidate(); raw["evidence"]["raw"] = "no"
            raw["artifact_sha256"] = sha256({key: value for key, value in raw.items() if key != "artifact_sha256"})
            with self.assertRaisesRegex(CandidateInboxError, "sanitized"):
                publish(raw, Path(directory) / "inbox", date="2026-09-23")
            bad = _candidate(); bad["reason"] = "tampered"
            with self.assertRaisesRegex(CandidateInboxError, "hash"):
                publish(bad, Path(directory) / "inbox", date="2026-09-23")

    def test_artifact_stays_group_readable_under_service_umask(self):
        with tempfile.TemporaryDirectory() as directory:
            previous = os.umask(0o077)
            try:
                artifact = publish(_candidate(), Path(directory) / "inbox", date="2026-09-23")
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o640)

    def test_same_day_retry_keeps_both_candidates_and_advances_current(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "inbox"
            first_candidate = _candidate()
            first = publish(first_candidate, root, date="2026-09-23")
            second_candidate = _candidate()
            second_candidate["reason"] = "later independently retained decision"
            second_candidate["artifact_sha256"] = sha256({key: value for key, value in second_candidate.items() if key != "artifact_sha256"})
            second = publish(second_candidate, root, date="2026-09-23")
            self.assertNotEqual(first, second)
            self.assertTrue(first.is_file())
            self.assertTrue(second.is_file())
            self.assertEqual(read_current_candidate(root)["artifact_sha256"], second_candidate["artifact_sha256"])

    def test_sanitized_full_port_profile_reaches_the_sole_ledger_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "inbox"
            candidate = _candidate()
            candidate["paper_profile"] = "options_full_port_v1"
            candidate["artifact_sha256"] = sha256({key: value for key, value in candidate.items() if key != "artifact_sha256"})
            publish(candidate, root, date="2026-09-23")
            self.assertEqual(read_current_candidate(root)["paper_profile"], "options_full_port_v1")

    def test_units_establish_writer_reader_boundary(self):
        writer = Path("deploy/recorder/neorome-options-observed-candidate.service").read_text()
        reader = Path("deploy/recorder/neorome-options-internal-paper.service").read_text()
        self.assertIn("User=neorome-options", writer)
        self.assertIn("Group=neorome", writer)
        self.assertIn("StateDirectory=neorome-options-inbox", writer)
        self.assertIn("--publish-inbox /var/lib/neorome-options-inbox", writer)
        self.assertIn("User=neorome", reader)
        self.assertIn("--candidate-inbox-root /var/lib/neorome-options-inbox", reader)
        self.assertIn("ReadOnlyPaths=/var/lib/neorome-options-inbox", reader)
