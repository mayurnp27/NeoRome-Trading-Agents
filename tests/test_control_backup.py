import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from neorome.control_backup import ControlBackupError, publish_snapshot


class ControlBackupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_publishes_committed_wal_snapshot_for_read_only_recovery(self):
        source = self.root / "control.sqlite3"
        writer = sqlite3.connect(source)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE events (value TEXT NOT NULL)")
        writer.execute("INSERT INTO events VALUES ('committed')")
        writer.commit()
        destination = self.root / "recovery-source.sqlite3"
        result = publish_snapshot(source, destination)
        self.assertEqual(result["status"], "published")
        self.assertEqual(destination.stat().st_mode & 0o777, 0o400)
        writer.execute("INSERT INTO events VALUES ('later')")
        writer.commit()
        writer.close()
        destination.parent.chmod(0o500)
        try:
            recovered = sqlite3.connect(
                f"{destination.resolve().as_uri()}?mode=ro", uri=True
            )
            self.assertEqual(
                recovered.execute("SELECT value FROM events ORDER BY rowid").fetchall(),
                [("committed",)],
            )
            self.assertEqual(recovered.execute("PRAGMA integrity_check").fetchone(), ("ok",))
            recovered.close()
        finally:
            destination.parent.chmod(0o700)

    def test_refuses_symlink_destination(self):
        source = self.root / "control.sqlite3"
        with sqlite3.connect(source) as database:
            database.execute("CREATE TABLE events (value TEXT)")
        target = self.root / "target.sqlite3"
        target.write_bytes(b"preserve")
        destination = self.root / "recovery-source.sqlite3"
        os.symlink(target, destination)
        with self.assertRaises(ControlBackupError):
            publish_snapshot(source, destination)
        self.assertEqual(target.read_bytes(), b"preserve")


if __name__ == "__main__":
    unittest.main()
