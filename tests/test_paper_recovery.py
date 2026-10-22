import fcntl
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from neorome import paper_recovery
from neorome.paper_ledger import PaperLedger, Proposal
from neorome.paper_recovery import (
    MAX_COMBINED_BYTES,
    health_check,
    restore_bundle,
    run_once,
    verify_bundle,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)


class RecoveryFixture:
    def __init__(self, root: Path):
        self.root = root
        self.recovery = root / "recovery"
        self.runtime = root / "runtime"
        self.pilot = root / "pilot" / "state.json"
        self.daily = root / "daily"
        self.recorder = root / "recorder" / "recorder.sqlite3"
        self.paper = root / "paper" / "paper.sqlite3"
        self.runtime.mkdir(parents=True)
        self.pilot.parent.mkdir(parents=True)
        self.daily.mkdir(parents=True)
        self.recorder.parent.mkdir(parents=True)
        self.paper.parent.mkdir(parents=True)
        ledger = PaperLedger(self.paper, enforce_market_risk=False, clock=lambda: NOW)
        proposal = Proposal(
            proposal_id="proposal-recovery-1",
            desk="crypto",
            strategy="recovery-test",
            strategy_version="v1",
            source_hash="a" * 64,
            source_timestamp=NOW,
            instrument="BTC/USD",
            exposure_group="crypto:btc",
            idea_id="recovery-idea",
            expiry_at=NOW + timedelta(minutes=5),
            quantity=1,
            unit_cost_cents=100,
            provenance="observed",
            metadata={"quote": "test"},
        )
        reservation = ledger.reserve(proposal, now=NOW)
        ledger.simulate_fill(reservation.reservation_id, fill_id="fill-recovery-1", provenance="observed", filled_at=NOW)
        self.fill_id = "fill-recovery-1"
        with self.pilot.open("w", encoding="utf-8") as stream:
            json.dump({"schema_version": 1, "fill_evidence": {"fill_id": self.fill_id, "source_hash": "a" * 64}}, stream)
        (self.daily / "decision-2026-09-20.json").write_text('{"date":"2026-09-20","decision":"hold"}\n', encoding="utf-8")
        (self.daily / "decision-2026-09-21.json").write_text('{"date":"2026-09-21","decision":"hold"}\n', encoding="utf-8")
        with sqlite3.connect(self.recorder) as db:
            db.execute("CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT, status TEXT, observation TEXT, sha256 TEXT)")
            db.execute("INSERT INTO attempts(started_at,status,observation,sha256) VALUES (?,?,?,?)", (NOW.isoformat(), "fresh", "{}", hashlib.sha256(b"{}").hexdigest()))
        for name, value in (("status.json", {"operational_healthy": True}), ("state.json", {"runner": "idle"}), ("claim.json", {"claim": "none"})):
            (self.runtime / name).write_text(json.dumps(value), encoding="utf-8")
        (self.runtime / "STOP").write_bytes(b"")

    def sources(self):
        return {
            "paper_ledger": self.paper,
            "pilot_state": self.pilot,
            "daily_root": self.daily,
            "recorder_database": self.recorder,
            "runtime_root": self.runtime,
            "runtime_lock": self.runtime / "runtime.lock",
            "pilot_lock": self.root / "pilot.lock",
        }


class PaperRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.fixture = RecoveryFixture(Path(self.temp.name))

    def tearDown(self):
        self.temp.cleanup()

    def test_backup_verify_restore_preserves_fill_and_history(self):
        result = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        self.assertEqual(result["status"], "backup_created")
        bundle = self.fixture.recovery / "2026-09-21"
        self.assertEqual(verify_bundle(bundle)["status"], "valid")
        restored = Path(self.temp.name) / "restored"
        result = restore_bundle(bundle, restored)
        self.assertEqual(result["status"], "restore_verified")
        self.assertEqual(verify_bundle(bundle)["status"], "valid")
        self.assertFalse(list(bundle.rglob("*-wal")))
        self.assertFalse(list(bundle.rglob("*-shm")))
        with sqlite3.connect(restored / "paper/paper.sqlite3") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM fills WHERE fill_id=?", (self.fixture.fill_id,)).fetchone()[0], 1)
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        reopened = PaperLedger(restored / "paper/paper.sqlite3", enforce_market_risk=False)
        self.assertTrue(any(record.entity_id == self.fixture.fill_id for record in reopened.audit()))
        self.assertTrue((restored / "daily/decision-2026-09-20.json").exists())
        self.assertTrue((restored / "runtime/STOP").exists())

    def test_snapshot_is_delete_journal_for_readonly_validators(self):
        result = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        self.assertEqual(result["status"], "backup_created")
        bundle = self.fixture.recovery / "2026-09-21"
        snapshot = bundle / "paper/paper.sqlite3"
        with sqlite3.connect(snapshot.resolve().as_uri() + "?mode=ro", uri=True) as database:
            self.assertEqual(database.execute("PRAGMA journal_mode").fetchone()[0].lower(), "delete")
            self.assertEqual(database.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertFalse(Path(str(snapshot) + "-wal").exists())
        self.assertFalse(Path(str(snapshot) + "-shm").exists())
        self.assertEqual(verify_bundle(bundle)["status"], "valid")

    def test_alternate_recovery_root_preserves_invalid_original_bundle(self):
        self.assertEqual(run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())["status"], "backup_created")
        original = self.fixture.recovery / "2026-09-21"
        manifest_before = (original / "manifest.json").read_bytes()
        snapshot = original / "paper/paper.sqlite3"
        snapshot_hash = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        Path(str(snapshot) + "-wal").write_bytes(b"")
        Path(str(snapshot) + "-shm").write_bytes(b"x" * 32768)
        self.assertEqual(verify_bundle(original)["errors"], ["bundle payload set mismatch"])

        alternate = self.fixture.root / "recovery-alternate"
        result = run_once(alternate, now=NOW, sources=self.fixture.sources())
        self.assertEqual(result["status"], "backup_created")
        alternate_bundle = alternate / "2026-09-21"
        self.assertEqual(verify_bundle(alternate_bundle)["status"], "valid")
        self.assertTrue(health_check(alternate, now=NOW)["operational_healthy"])
        self.assertEqual((original / "manifest.json").read_bytes(), manifest_before)
        self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(), snapshot_hash)
        self.assertTrue(Path(str(snapshot) + "-wal").exists())
        self.assertTrue(Path(str(snapshot) + "-shm").exists())

    def test_tamper_and_empty_manifest_are_rejected(self):
        run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        bundle = self.fixture.recovery / "2026-09-21"
        (bundle / "tamper.txt").write_text("tampered", encoding="utf-8")
        self.assertEqual(verify_bundle(bundle)["status"], "invalid")
        forged = self.fixture.recovery / "2026-09-22"
        forged.mkdir()
        manifest = {"schema_version": 1, "owner": "neorome.paper_recovery", "files": [], "sqlite_files": [], "components": {}}
        payload = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
        (forged / "manifest.json").write_bytes(payload)
        (forged / "manifest.sha256").write_text(hashlib.sha256(payload).hexdigest() + "\n", encoding="ascii")
        self.assertEqual(verify_bundle(forged)["status"], "invalid")

    def test_restart_replay_and_clock_rollback(self):
        first = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        replay = run_once(self.fixture.recovery, now=NOW + timedelta(minutes=1), sources=self.fixture.sources())
        self.assertEqual(first["status"], "backup_created")
        self.assertEqual(replay["status"], "already_verified")
        rollback = run_once(self.fixture.recovery, now=NOW - timedelta(minutes=1), sources=self.fixture.sources())
        self.assertEqual(rollback["status"], "clock_rollback")
        self.assertFalse(health_check(self.fixture.recovery, now=NOW)["operational_healthy"])

    def test_incomplete_claim_without_output_can_retry(self):
        self.fixture.recovery.mkdir(parents=True, exist_ok=True)
        (self.fixture.recovery / "recovery-state.json").write_text(
            json.dumps({"schema_version": 1, "bundle_date": "2026-09-21", "started_at": NOW.isoformat(), "status": "incomplete"}),
            encoding="utf-8",
        )
        result = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        self.assertEqual(result["status"], "backup_created")

    def test_running_lock_does_not_overwrite_status(self):
        lock_path = self.fixture.runtime / "runtime.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            result = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
            self.assertEqual(result["status"], "already_running")
            self.assertFalse((self.fixture.recovery / "status.json").exists())
        finally:
            stream.close()

    def test_existing_verified_bundle_refreshes_health_despite_runtime_lock(self):
        run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        lock_path = self.fixture.runtime / "runtime.lock"
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            refreshed = run_once(self.fixture.recovery, now=NOW + timedelta(hours=3),
                                 sources=self.fixture.sources())
            self.assertEqual(refreshed["status"], "already_verified")
            self.assertTrue(refreshed["operational_healthy"])
            self.assertTrue(health_check(self.fixture.recovery, now=NOW + timedelta(hours=3))["operational_healthy"])
        finally:
            stream.close()

    def test_sqlite_backup_opens_source_read_only(self):
        source = Path(self.temp.name) / "read-only-source" / "source.sqlite3"
        source.parent.mkdir()
        with sqlite3.connect(source) as database:
            database.execute("CREATE TABLE evidence (value TEXT NOT NULL)")
            database.execute("INSERT INTO evidence VALUES ('preserved')")
        source.chmod(0o400)
        source.parent.chmod(0o500)
        destination = Path(self.temp.name) / "backup" / "copied.sqlite3"
        try:
            paper_recovery._sqlite_backup(source, destination)
        finally:
            source.parent.chmod(0o700)
        with sqlite3.connect(destination) as database:
            self.assertEqual(database.execute("SELECT value FROM evidence").fetchone(), ("preserved",))

    def test_new_bundle_includes_optional_control_plane_database(self):
        control = Path(self.temp.name) / "control" / "control-plane.sqlite3"
        control.parent.mkdir()
        with sqlite3.connect(control) as database:
            database.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY)")
            database.execute("INSERT INTO events VALUES ('event-1')")
        sources = self.fixture.sources()
        sources["control_database"] = control
        result = run_once(self.fixture.recovery, now=NOW, sources=sources)
        self.assertEqual(result["status"], "backup_created")
        bundle = self.fixture.recovery / "2026-09-21"
        manifest = json.loads((bundle / "manifest.json").read_text())
        self.assertTrue(manifest["components"]["control"]["present"])
        self.assertIn("control/control-plane.sqlite3", manifest["sqlite_files"])
        restored = Path(self.temp.name) / "restored-control"
        restore_bundle(bundle, restored)
        with sqlite3.connect(restored / "control/control-plane.sqlite3") as database:
            self.assertEqual(database.execute("SELECT event_id FROM events").fetchone()[0], "event-1")

    def test_retention_keeps_seven_verified_bundles(self):
        for offset in range(8):
            result = run_once(self.fixture.recovery, now=NOW + timedelta(days=offset), sources=self.fixture.sources())
            self.assertIn(result["status"], {"backup_created", "already_verified"})
        dates = sorted(path for path in self.fixture.recovery.iterdir() if path.is_dir() and path.name[:4].isdigit())
        self.assertEqual(len(dates), 7)
        self.assertTrue(all(verify_bundle(path)["status"] == "valid" for path in dates))

    def test_storage_cap_preflight(self):
        filler = self.fixture.recovery / "filler"
        self.fixture.recovery.mkdir(parents=True, exist_ok=True)
        with filler.open("wb") as stream:
            stream.truncate(MAX_COMBINED_BYTES)
        result = run_once(self.fixture.recovery, now=NOW, sources=self.fixture.sources())
        self.assertEqual(result["status"], "storage_limit")
        self.assertFalse((self.fixture.recovery / "2026-09-21").exists())


if __name__ == "__main__":
    unittest.main()
