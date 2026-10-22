import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome.repair_memory import FIELDS, digest, export, read_database, retrieve


class RepairMemoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.vault = Path(self.tmp.name) / "vault"
        self.snapshot = {"schema": 1, "exported_at": datetime.now(timezone.utc).isoformat(),
                         "incidents": [{"incident_id": "../../escape", "status": "resolved",
                                        "summary": "backup sqlite lock token=DO_NOT_EXPORT",
                                        "updated_at": "2026-09-22T12:00:00Z"}]}

    def test_export_is_bounded_redacted_and_ids_cannot_escape(self):
        manifest = export(self.snapshot, self.vault, "test-fixture")
        incident = next(item for item in manifest["documents"] if item["kind"] == "incident")
        text = (self.vault / incident["path"]).read_text()
        self.assertNotIn("DO_NOT_EXPORT", text)
        self.assertIn("[REDACTED]", text)
        self.assertTrue(incident["path"].startswith("Cloud/inc-"))
        result = retrieve("sqlite lock", self.vault, max_chars=500)
        self.assertLessEqual(result["characters"], 500)
        self.assertTrue(result["selected"])

    def test_modified_exports_and_unindexed_drafts_do_not_enter_brief(self):
        manifest = export(self.snapshot, self.vault)
        incident = next(item for item in manifest["documents"] if item["kind"] == "incident")
        (self.vault / incident["path"]).write_text("unique_pilot_error pretend verified")
        (self.vault / "Draft.md").write_text("unique_pilot_error restart everything")
        result = retrieve("unique_pilot_error", self.vault)
        self.assertFalse(result["selected"])
        self.assertTrue(result["excluded"])

    def test_stale_incidents_excluded_but_historical_runbooks_available(self):
        self.snapshot["exported_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        export(self.snapshot, self.vault)
        result = retrieve("sqlite lock", self.vault)
        self.assertTrue(result["selected"])
        self.assertTrue(all(path.startswith("Runbooks/") for path in result["selected"]))
        self.assertTrue(any("stale" in reason for reason in result["excluded"]))

    def test_manifest_excludes_incident_no_longer_in_export(self):
        export(self.snapshot, self.vault)
        self.snapshot["incidents"] = []
        manifest = export(self.snapshot, self.vault)
        self.assertFalse(any(item["kind"] == "incident" for item in manifest["documents"]))
        self.assertFalse(retrieve("escape", self.vault)["selected"])

    def test_source_changes_require_refresh(self):
        manifest = export(self.snapshot, self.vault)
        runbook = next(item for item in manifest["documents"] if item["kind"] == "runbook")
        runbook["source_sha256"] = "changed"
        (self.vault / "manifest.json").write_text(json.dumps(manifest))
        result = retrieve("readiness recovery", self.vault)
        self.assertIn(runbook["path"] + ": source changed; refresh required", result["excluded"])

    def test_read_database_does_not_mutate_or_create_missing_database(self):
        path = Path(self.tmp.name) / "copy.sqlite3"
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE incidents (" + ",".join(key + " TEXT" for key in FIELDS) + ")")
            values = ["one", "resolved", "warning", "recovery lock", "2026-09-22", None,
                      "2026-09-22", "fixture", "corr", "event-one"]
            conn.execute("INSERT INTO incidents VALUES (" + ",".join("?" for _ in values) + ")", values)
        before = digest(path.read_bytes().hex())
        self.assertEqual(read_database(path)["incidents"][0]["incident_id"], "one")
        self.assertEqual(before, digest(path.read_bytes().hex()))
        missing = path.parent / "missing.sqlite3"
        with self.assertRaises(sqlite3.OperationalError):
            read_database(missing)
        self.assertFalse(missing.exists())

    def test_rejects_invalid_schema_future_timestamp_and_over_budget(self):
        with self.assertRaises(ValueError):
            export({"schema": 2, "incidents": []}, self.vault)
        self.snapshot["exported_at"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        with self.assertRaises(ValueError):
            export(self.snapshot, self.vault)
        with self.assertRaises(ValueError):
            retrieve("lock", self.vault, max_chars=12001)


if __name__ == "__main__":
    unittest.main()
