from datetime import datetime, timedelta, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parents[1] / "operations/maintenance_agent/operator_v1"))
import desk_readiness


NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


class DeskReadinessTests(unittest.TestCase):
    def _systemd(self, *, inactive_timer=None):
        def run(args, **kwargs):
            units = args[-len(desk_readiness.SYSTEMD_UNITS):]
            blocks = []
            for unit in units:
                timer = unit.endswith(".timer")
                active = "inactive" if unit == inactive_timer else ("active" if timer else "inactive")
                next_trigger = "n/a" if not timer or active != "active" else "Wed 2026-10-14 12:15:00 UTC"
                blocks.append(f"Id={unit}\nLoadState=loaded\nActiveState={active}\nResult=success\nNextElapseUSecRealtime={next_trigger}")
            return SimpleNamespace(returncode=0, stdout="\n\n".join(blocks))
        return run

    def _inputs(self, root, *, prediction_age=0):
        root = Path(root)
        paths = {}

        def write_json(name, value):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value))
            paths[name] = str(path)
            return path

        pkg = root / "equity-package"
        pkg.mkdir()
        (pkg / "neorome").mkdir()
        (pkg / "neorome/equity_intraday_iex_fullfunded.py").write_text("# fixture\n")
        paths["equity_package"] = str(pkg)
        write_json("equity/profile.json", {"profile_id": "EQ-INTRADAY-IEX-FULLFUNDED-v1", "broker_orders_enabled": False})
        db = root / "equity/paper.sqlite3"
        connection = sqlite3.connect(db)
        connection.execute("create table positions (id integer)")
        connection.commit()
        connection.close()
        paths["equity_database"] = str(db)
        write_json("equity/watchlist.json", {"symbols": ["SPY"]})
        for arm, strategy in (("A", "strategy-A"), ("B", "strategy-B")):
            config = {"strategy_id": strategy}
            write_json(f"options/config-{arm}.json", config)
            write_json(f"options/arms/{arm}.json", {
                "schema_version": 2, "strategy_id": strategy,
                "config_fingerprint": hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest(), "cash": 1000.0,
                "initial_cash": 1000.0, "realized_pnl": 0.0,
                "positions": {}, "trades": [],
            })
        stamp = lambda age=0: (NOW - timedelta(seconds=age)).isoformat()
        write_json("prediction/feed.json", {"finished_at": stamp(prediction_age), "mode": "internal-paper"})
        write_json("crypto/status.json", {"updated_at": stamp(), "operational_healthy": True})
        recorder = root / "crypto/recorder.sqlite3"
        with sqlite3.connect(recorder) as db:
            db.execute("CREATE TABLE attempts (id INTEGER PRIMARY KEY, started_at TEXT, status TEXT, observation TEXT)")
            db.execute("INSERT INTO attempts VALUES (1,?,?,?)", (stamp(), 'fresh', json.dumps({'raw_response': {'quotes': {'BTC/USD': {'t': stamp()}}}})))
        aliases = {
            "option_config_a": "options/config-A.json", "option_config_b": "options/config-B.json",
            "option_arm_a": "options/arms/A.json", "option_arm_b": "options/arms/B.json",
            "prediction_feed": "prediction/feed.json", "crypto_status": "crypto/status.json",
            "btc_recorder_database": "crypto/recorder.sqlite3", "equity_profile": "equity/profile.json",
            "equity_watchlist": "equity/watchlist.json",
        }
        paths.update({key: str(root / value) for key, value in aliases.items()})
        return paths

    def test_complete_read_only_evidence_marks_operational_profiles_armed_only(self):
        with tempfile.TemporaryDirectory() as root:
            projection = desk_readiness.build_projection(paths=self._inputs(root), now=NOW, run=self._systemd())
        self.assertEqual({desk: value["status"] for desk, value in projection["desks"].items()},
                         {"equities": "active", "options": "active", "prediction": "active", "crypto": "active"})
        for desk in projection["desks"].values():
            self.assertEqual(desk["entry_eligibility"], "unknown")
        self.assertEqual(projection["desks"]["options"]["profiles"]["A"]["operational_state"], "armed_for_session")
        self.assertEqual(projection["desks"]["options"]["profiles"]["A"]["entry_eligibility"], "unknown")

    def test_missing_or_stale_evidence_never_arms_desk(self):
        with tempfile.TemporaryDirectory() as root:
            missing = desk_readiness.build_projection(paths={"equity_package": root}, now=NOW, run=self._systemd())
            stale = desk_readiness.build_projection(paths=self._inputs(root, prediction_age=181), now=NOW, run=self._systemd())
        self.assertEqual(missing["desks"]["equities"]["status"], "unknown")
        self.assertEqual(stale["desks"]["prediction"]["status"], "unknown")
        self.assertFalse(stale["desks"]["prediction"]["profiles"]["btc_stochastic"]["checks"]["structural_checks_passed"])

    def test_inactive_timer_is_not_reported_as_armed(self):
        with tempfile.TemporaryDirectory() as root:
            projection = desk_readiness.build_projection(paths=self._inputs(root), now=NOW,
                run=self._systemd(inactive_timer="neorome-stochastic-paper-v2.timer"))
        self.assertEqual(projection["desks"]["options"]["status"], "unknown")

    def test_symlink_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            target = Path(root) / "target.json"
            target.write_text('{"updated_at":"2026-10-14T12:00:00Z"}')
            alias = Path(root) / "alias.json"
            alias.symlink_to(target)
            value, status = desk_readiness._read_json(alias)
        self.assertIsNone(value)
        self.assertEqual(status, "missing")

    def test_stale_collector_and_config_drift_are_not_armed(self):
        with tempfile.TemporaryDirectory() as root:
            paths = self._inputs(root)
            config = Path(paths['option_config_a'])
            config.write_text('{"strategy_id":"strategy-A","changed":true}')
            with sqlite3.connect(paths['btc_recorder_database']) as db:
                db.execute("UPDATE attempts SET started_at=?", ((NOW-timedelta(seconds=181)).isoformat(),))
            projection = desk_readiness.build_projection(paths=paths, now=NOW, run=self._systemd())
        self.assertEqual(projection['desks']['options']['status'], 'unknown')
        self.assertEqual(projection['desks']['crypto']['status'], 'unknown')

    def test_active_timer_without_next_trigger_needs_running_worker(self):
        rows={'neorome-test.timer': {'LoadState':'loaded', 'ActiveState':'active',
              'NextElapseUSecRealtime':'', 'NextElapseUSecMonotonic':'infinity'}}
        self.assertFalse(desk_readiness._timer_armed(rows, 'neorome-test.timer'))
        rows['neorome-test.service']={'ActiveState':'activating','MainPID':'123'}
        self.assertTrue(desk_readiness._timer_armed(rows, 'neorome-test.timer'))
        rows['neorome-test.service']['MainPID']='0'
        self.assertFalse(desk_readiness._timer_armed(rows, 'neorome-test.timer'))

    def test_failed_option_service_is_not_armed(self):
        base = self._systemd()
        def failed(*a, **kw):
            result = base(*a, **kw)
            result.stdout = result.stdout.replace('Id=neorome-stochastic-paper-v2.service\nLoadState=loaded\nActiveState=inactive\nResult=success', 'Id=neorome-stochastic-paper-v2.service\nLoadState=loaded\nActiveState=failed\nResult=exit-code')
            return result
        with tempfile.TemporaryDirectory() as root:
            projection = desk_readiness.build_projection(paths=self._inputs(root), now=NOW, run=failed)
        self.assertEqual(projection['desks']['options']['status'], 'unknown')


if __name__ == "__main__":
    unittest.main()
