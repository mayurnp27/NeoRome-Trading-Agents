from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from neorome.recorder_ops import backup_recorder
from neorome.paper_ledger import PaperLedger
from neorome.recorder_supervisor import (
    DEFAULT_BTC_STATE,
    DEFAULT_OPERATIONS_STATE,
    DEFAULT_SPY_STATE,
    run_once, SupervisorError,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)


def observation(timestamp: datetime, symbol: str) -> str:
    return json.dumps({
        "raw_response": {"quotes": {symbol: {
            "t": timestamp.isoformat(), "bp": 100, "ap": 101, "bs": 10, "as": 10,
        }}},
        "evidence": {"feed": "fixture"},
    }, separators=(",", ":"))


def make_state(path: Path, *, timestamp: datetime = NOW, symbol: str = "SPY") -> None:
    path.mkdir()
    with sqlite3.connect(path / "recorder.sqlite3") as db:
        db.execute(
            "CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, "
            "status TEXT NOT NULL, detail TEXT, observation TEXT, sha256 TEXT)"
        )
        db.execute(
            "INSERT INTO attempts(started_at,status,observation) VALUES (?,?,?)",
            (timestamp.isoformat(), "fresh", observation(timestamp, symbol)),
        )


class RecorderSupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.spy = self.root / "spy"
        self.btc = self.root / "btc"
        self.operations = self.root / "ops"
        make_state(self.spy, symbol="SPY")
        make_state(self.btc, symbol="BTC/USD")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def run_supervisor(self, now: datetime = NOW, **kwargs: object) -> dict[str, object]:
        return run_once(self.spy, self.btc, self.operations, now=now, **kwargs)

    def test_paper_database_backup_is_verified_and_missing_book_degrades(self):
        paper = self.root / 'paper'
        PaperLedger(paper / 'paper.sqlite3', enforce_market_risk=True)
        result = self.run_supervisor(paper_state=paper)
        self.assertTrue(result['operational_healthy'])
        self.assertEqual(result['backups']['paper']['verification']['database']['filename'], 'paper.sqlite3')
        (paper / 'paper.sqlite3').unlink()
        later = self.run_supervisor(paper_state=paper)
        self.assertEqual(later['backups']['paper']['status'], 'error')

    def test_sqlite_failure_publishes_degraded_status(self):
        with patch('neorome.recorder_supervisor.backup_recorder', side_effect=sqlite3.OperationalError('test')):
            result = self.run_supervisor()
        self.assertFalse(result['operational_healthy'])
        self.assertEqual(result['backups']['spy']['status'], 'error')
        self.assertFalse(json.loads((self.operations/'status.json').read_text())['operational_healthy'])

    def test_defaults_match_host_service_and_status_is_atomic_json(self) -> None:
        self.assertEqual(DEFAULT_SPY_STATE, "/var/lib/neorome-recorder")
        self.assertEqual(DEFAULT_BTC_STATE, "/var/lib/neorome-crypto-recorder")
        self.assertEqual(DEFAULT_OPERATIONS_STATE, "/var/lib/neorome-ops")
        result = self.run_supervisor()
        self.assertEqual(result["status"], "healthy")
        status_path = self.operations / "status.json"
        self.assertEqual(json.loads(status_path.read_text())["status"], "healthy")
        self.assertFalse(list(self.operations.glob(".status.json.*.tmp")))
        self.assertEqual(result["backups"]["spy"]["status"], "backup_created")
        self.assertEqual(result["backups"]["btc"]["status"], "backup_created")

    def test_same_utc_day_is_idempotent(self) -> None:
        first = self.run_supervisor()
        second = self.run_supervisor(NOW + timedelta(minutes=1))
        self.assertEqual(first["backups"]["spy"]["status"], "backup_created")
        self.assertEqual(second["backups"]["spy"]["status"], "already_verified")
        self.assertEqual(second["backups"]["btc"]["status"], "already_verified")
        self.assertEqual(sorted(p.name for p in (self.operations / "backups" / "spy").iterdir()), ["2026-09-21"])
        self.assertEqual(sorted(p.name for p in (self.operations / "backups" / "btc").iterdir()), ["2026-09-21"])

    def test_retention_keeps_seven_verified_dated_backups_and_preserves_invalid(self) -> None:
        for days_ago in range(7, 0, -1):
            day = NOW - timedelta(days=days_ago)
            backup_recorder(self.spy, self.operations / "backups" / "spy" / day.date().isoformat(), now=day)
            backup_recorder(self.btc, self.operations / "backups" / "btc" / day.date().isoformat(), now=day)
        invalid = self.operations / "backups" / "spy" / "2026-09-01"
        invalid.mkdir(parents=True)
        (invalid / "manifest.json").write_text("corrupt")

        result = self.run_supervisor(
            heartbeat_seconds=10**9,
            data_stale_seconds=10**9,
        )
        self.assertEqual(result["backups"]["spy"]["status"], "backup_created")
        spy_dates = sorted(
            p.name for p in (self.operations / "backups" / "spy").iterdir()
            if p.is_dir() and p.name != "2026-09-01"
        )
        self.assertEqual(spy_dates, [
            "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
            "2026-09-19", "2026-09-20", "2026-09-21",
        ])
        self.assertTrue(invalid.exists())

    def test_missing_btc_state_reports_degraded_but_still_checks_and_backups_spy(self) -> None:
        shutil.rmtree(self.btc)
        result = self.run_supervisor()
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["health"]["spy"]["status"], "healthy")
        self.assertEqual(result["health"]["btc"]["status"], "not_started")
        self.assertEqual(result["backups"]["spy"]["status"], "backup_created")
        self.assertEqual(result["backups"]["btc"]["status"], "error")
        self.assertEqual(json.loads((self.operations / "status.json").read_text())["status"], "degraded")

    def test_storage_cap_blocks_new_backup_before_copy(self) -> None:
        result = self.run_supervisor(storage_cap_bytes=1024)
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["backups"]["spy"]["status"], "storage_cap")
        self.assertEqual(result["backups"]["btc"]["status"], "storage_cap")
        self.assertFalse((self.operations / "backups" / "spy" / "2026-09-21").exists())

    def test_symlink_live_state_is_refused(self) -> None:
        shutil.rmtree(self.btc)
        self.btc.symlink_to(self.spy, target_is_directory=True)
        result = self.run_supervisor()
        self.assertEqual(result["health"]["btc"]["status"], "refused_symlink")
        self.assertEqual(result["backups"]["btc"]["status"], "error")
        self.assertEqual(result["status"], "degraded")

    def fleet_policy(self):
        path = self.root / 'fleet.json'
        value = {'schema': 'neorome-operational-fleet-v1', 'active_recorders': ['btc'],
                 'required_units': ['neorome-crypto-recorder.timer', 'neorome-equity-intraday-iex-fullfunded.timer'],
                 'archived_recorders': {'spy': {'reason': 'Replaced by IEX full-funded paper engine',
                     'replacement_units': ['neorome-equity-intraday-iex-fullfunded.timer']}}}
        path.write_text(json.dumps(value))
        return path

    def test_retired_recorder_stays_visible_and_backed_up_with_verified_replacement(self):
        policy = self.fleet_policy()
        health = {'status': 'stale', 'process': {'status': 'stale'}, 'data': {'status': 'stale'}}
        with patch('neorome.recorder_supervisor._health', side_effect=[health, {'process': {'status': 'healthy'}, 'data': {'status': 'fresh'}}]), patch('neorome.unit_health.check_units', return_value={'operational_healthy': True, 'issues': []}):
            result = self.run_supervisor(fleet_policy=policy, check_service_units=True)
        self.assertTrue(result['operational_healthy'])
        self.assertFalse(result['health']['spy']['operational_healthy'])
        self.assertTrue(result['health']['spy']['replacement_verified'])
        self.assertEqual(result['backups']['spy']['status'], 'backup_created')

    def test_retirement_does_not_hide_replacement_failure_or_missing_backup(self):
        policy = self.fleet_policy()
        with patch('neorome.unit_health.check_units', return_value={'operational_healthy': False, 'issues': ['replacement inactive']}):
            self.assertFalse(self.run_supervisor(fleet_policy=policy, check_service_units=True)['operational_healthy'])
        shutil.rmtree(self.spy)
        with patch('neorome.unit_health.check_units', return_value={'operational_healthy': True, 'issues': []}):
            result = self.run_supervisor(fleet_policy=policy, check_service_units=True)
        self.assertFalse(result['operational_healthy'])
        self.assertEqual(result['backups']['spy']['status'], 'error')

    def test_unrelated_fleet_failure_does_not_invalidate_spy_replacement(self):
        policy = self.fleet_policy()
        with patch('neorome.unit_health.check_units', side_effect=[
            {'operational_healthy': False, 'issues': ['unrelated collector failed']},
            {'operational_healthy': True, 'issues': []},
        ]) as check:
            result = self.run_supervisor(fleet_policy=policy, check_service_units=True)
        self.assertTrue(result['health']['spy']['replacement_verified'])
        self.assertFalse(result['operational_healthy'])
        self.assertEqual(result['status'], 'degraded')
        self.assertEqual(check.call_args.kwargs['required_units'],
                         ['neorome-equity-intraday-iex-fullfunded.timer'])

    def test_invalid_retirement_or_unverified_policy_refused(self):
        policy = self.fleet_policy()
        with self.assertRaises(SupervisorError):
            self.run_supervisor(fleet_policy=policy)
        value = json.loads(policy.read_text())
        value['archived_recorders']['spy']['replacement_units'] = ['neorome-unmonitored.timer']
        policy.write_text(json.dumps(value))
        with self.assertRaises(SupervisorError):
            self.run_supervisor(fleet_policy=policy, check_service_units=True)


if __name__ == "__main__":
    unittest.main()
