import contextlib
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from neorome.recorder_ops import (
    RecorderOpsError,
    backup_recorder,
    health_summary,
    main,
    restore_backup,
    verify_backup,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)


def observation(timestamp: datetime = NOW, symbol: str = "SPY") -> str:
    return json.dumps(
        {
            "raw_response": {
                "quotes": {
                    symbol: {
                        "t": timestamp.isoformat(),
                        "bp": 100,
                        "ap": 101,
                        "bs": 10,
                        "as": 10,
                    }
                }
            },
            "evidence": {"feed": "iex"},
        },
        separators=(",", ":"),
    )


def make_state(
    path: Path,
    rows: list[tuple[str, str, str | None, str | None]] | None = None,
    database: str = "recorder.sqlite3",
) -> None:
    path.mkdir()
    with sqlite3.connect(path / database) as db:
        db.execute(
            "CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "status TEXT NOT NULL, detail TEXT, observation TEXT, sha256 TEXT)"
        )
        for started_at, status, detail, payload in rows or []:
            db.execute(
                "INSERT INTO attempts(started_at,status,detail,observation) VALUES (?,?,?,?)",
                (started_at, status, detail, payload),
            )


class RecorderOpsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_backup_has_manifest_checksum_and_is_immutable(self):
        state = self.root / "state"
        make_state(state, [(NOW.isoformat(), "fresh", None, observation())])
        output = self.root / "backups" / "first"
        result = backup_recorder(state, output, now=NOW)
        self.assertEqual(result["status"], "backup_created")
        verified = verify_backup(output)
        self.assertEqual(verified["status"], "valid")
        self.assertTrue((output / "manifest.sha256").read_text().strip())
        with self.assertRaises(RecorderOpsError):
            backup_recorder(state, output, now=NOW)

    def test_corrupted_database_fails_checksum_and_integrity_verification(self):
        state = self.root / "state"
        make_state(state, [(NOW.isoformat(), "fresh", None, observation())])
        output = self.root / "backup"
        backup_recorder(state, output, now=NOW)
        os.chmod(output / "recorder.sqlite3", 0o600)
        with (output / "recorder.sqlite3").open("ab") as stream:
            stream.write(b"corruption")
        self.assertEqual(verify_backup(output)["status"], "invalid")

    def test_active_writer_backup_contains_only_committed_snapshot(self):
        state = self.root / "state"
        state.mkdir()
        database = state / "recorder.sqlite3"
        writer = sqlite3.connect(database)
        writer.execute(
            "CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "status TEXT NOT NULL, detail TEXT, observation TEXT, sha256 TEXT)"
        )
        writer.execute(
            "INSERT INTO attempts(started_at,status) VALUES (?,?)", (NOW.isoformat(), "fresh")
        )
        writer.commit()
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO attempts(started_at,status) VALUES (?,?)",
            ((NOW + timedelta(seconds=1)).isoformat(), "incomplete"),
        )
        output = self.root / "backup"
        try:
            backup_recorder(state, output, now=NOW)
        finally:
            writer.rollback()
            writer.close()
        self.assertEqual(verify_backup(output)["status"], "valid")
        with sqlite3.connect(output / "recorder.sqlite3") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM attempts").fetchone()[0], 1)

    def test_restore_requires_new_directory_and_does_not_overwrite(self):
        state = self.root / "state"
        make_state(state, [(NOW.isoformat(), "fresh", None, observation())])
        output = self.root / "backup"
        backup_recorder(state, output, now=NOW)
        restored = self.root / "restored"
        result = restore_backup(output, restored)
        self.assertEqual(result["status"], "restore_verified")
        with sqlite3.connect(restored / "recorder.sqlite3") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM attempts").fetchone()[0], 1)
        with self.assertRaises(RecorderOpsError):
            restore_backup(output, restored)
        self.assertEqual(verify_backup(output)["status"], "valid")

    def test_health_separates_heartbeat_data_and_failure_history(self):
        state = self.root / "state"
        rows = [
            (NOW.isoformat(), "fresh", None, observation()),
            ((NOW + timedelta(seconds=30)).isoformat(), "fetch_or_validation_failed", "RuntimeError", None),
            ((NOW + timedelta(seconds=60)).isoformat(), "incomplete", None, None),
        ]
        make_state(state, rows)
        summary = health_summary(state, NOW + timedelta(seconds=120))
        self.assertEqual(summary["status"], "healthy")
        self.assertEqual(summary["process"]["status"], "healthy")
        self.assertEqual(summary["data"]["status"], "fresh")
        self.assertEqual(summary["last_failure"]["status"], "fetch_or_validation_failed")
        self.assertEqual(summary["last_incomplete"]["status"], "incomplete")
        self.assertEqual(summary["attempts"]["incomplete"], 1)

        stale = health_summary(state, NOW + timedelta(seconds=300))
        self.assertEqual(stale["process"]["status"], "heartbeat_stale")
        self.assertEqual(stale["data"]["status"], "stale")
        self.assertEqual(stale["status"], "unhealthy")

    def test_health_accepts_other_recorder_instrument_without_schema_assumptions(self):
        state = self.root / "btc-state"
        make_state(state, [(NOW.isoformat(), "fresh", None, observation(symbol="BTC/USD"))])
        summary = health_summary(state, NOW + timedelta(seconds=30))
        self.assertEqual(summary["status"], "healthy")
        self.assertEqual(summary["data"]["status"], "fresh")

    def test_backup_and_health_can_target_a_separate_feed_database(self):
        state = self.root / "btc-state"
        database = "recorder-btc.sqlite3"
        make_state(state, [(NOW.isoformat(), "fresh", None, observation(symbol="BTC/USD"))], database)
        output = self.root / "btc-backup"
        self.assertEqual(
            backup_recorder(state, output, now=NOW, database=database)["status"],
            "backup_created",
        )
        self.assertEqual(verify_backup(output)["status"], "valid")
        restored = self.root / "btc-restored"
        self.assertEqual(restore_backup(output, restored)["status"], "restore_verified")
        summary = health_summary(state, NOW + timedelta(seconds=30), database=database)
        self.assertEqual(summary["status"], "healthy")

    def test_health_cli_is_json_and_has_distinct_stale_data_exit(self):
        state = self.root / "state"
        current = datetime.now(UTC)
        make_state(state, [(current.isoformat(), "fresh", None, observation(current))])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = main(["health", "--state", str(state)])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "healthy")
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = main(["health", "--state", str(state), "--data-stale-seconds", "0"])
        self.assertEqual(code, 3)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "stale_data")


if __name__ == "__main__":
    unittest.main()
