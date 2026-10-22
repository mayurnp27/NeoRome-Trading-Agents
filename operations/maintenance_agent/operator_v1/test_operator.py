import json
import stat
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

import paper_feedback
import remote_supervisor

UTC = timezone.utc


class PaperFeedbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.paper = self.root / "paper"
        self.arms = self.paper / "arms"
        self.configs = self.root / "configs"
        self.output_dir = self.root / "inbox"
        self.arms.mkdir(parents=True)
        self.configs.mkdir()
        self.output_dir.mkdir()
        self.output = self.output_dir / "paper-feedback.json"
        self.now = datetime(2026, 10, 14, 14, 0, tzinfo=UTC)
        self._write_fixtures(self.now)

    def tearDown(self):
        self.temp.cleanup()

    def _write(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))

    def _write_fixtures(self, stamp):
        iso = stamp.isoformat().replace("+00:00", "Z")
        current = {
            "schema_version": 1, "mode": "runtime", "asof": iso,
            "private_token": "must not leak", "arms": {
                "A": {"cash": 812.3, "realized_pnl": 12.3, "equity": 985.0, "status": "managed",
                      "positions": [{"symbol": "IWM", "quantity": 1, "mark_status": "fresh",
                                     "last_bid": 1.72, "last_quote_time": iso, "secret": "omit"}]},
                "B": {"cash": 1000, "realized_pnl": 0, "equity": 1000, "status": "idle", "positions": []},
            },
        }
        self._write(self.paper / "current.json", current)
        for arm in ("A", "B"):
            positions = ({"p1": {"symbol": "IWM", "expiry": "2026-10-16", "type": "call",
                                  "strike": 271, "quantity": 1, "entry_time": iso, "entry_ask": 1.6,
                                  "entry_debit": 160.1, "last_bid": 1.72, "last_quote_time": iso,
                                  "mark_status": "fresh", "signal": {"secret": "omit"}}}
                         if arm == "A" else {})
            trades = [{"symbol": "IWM", "expiry": "2026-10-16", "type": "call", "strike": 271,
                       "quantity": 1, "entry_time": iso, "exit_time": iso, "entry_ask": 1.2,
                       "exit_bid": 1.5, "net_pnl": 29.8, "reason": "take_profit", "late_exit": False,
                       "credential": "omit"} for _ in range(22)]
            self._write(self.arms / f"{arm}.json", {"last_observed_at": iso, "cash": 812.3,
                                                       "positions": positions, "trades": trades,
                                                       "secret": "omit"})
            self._write(self.configs / f"config-{arm}.json", {"strategy_id": f"arm-{arm}", "initial_cash": 1000,
                                                                 "broker_token": "omit"})

    def _build(self, now=None):
        return paper_feedback.build_feedback(
            current_path=self.paper / "current.json",
            state_paths={arm: self.arms / f"{arm}.json" for arm in ("A", "B")},
            config_paths={arm: self.configs / f"config-{arm}.json" for arm in ("A", "B")},
            now=now or self.now,
        )

    def test_curated_projection_keeps_only_observed_public_fields(self):
        projection = self._build()
        self.assertEqual(projection["mode"], "paper_only")
        self.assertEqual(projection["readiness"], "unknown")
        self.assertEqual(projection["arms"]["A"]["initial_cash"], 1000)
        self.assertEqual(projection["arms"]["A"]["position_count"], 1)
        self.assertEqual(projection["arms"]["A"]["positions"][0]["last_observed_bid"], 1.72)
        trades = projection["arms"]["A"]["closed_trades"]
        self.assertEqual(len(trades), 20)
        encoded = json.dumps(projection)
        for secret in ("private_token", "broker_token", "credential", "signal", "must not leak"):
            self.assertNotIn(secret, encoded)

    def test_stale_sources_do_not_publish_current_marks_or_equity(self):
        old = self.now - timedelta(seconds=181)
        self._write_fixtures(old)
        projection = self._build()
        self.assertEqual(projection["sources"]["current"]["status"], "stale")
        self.assertEqual(projection["arms"]["A"]["equity_status"], "not_observed")
        self.assertNotIn("last_observed_bid", projection["arms"]["A"]["positions"][0])

    def test_missing_and_oversize_inputs_are_marked_without_exposing_content(self):
        (self.paper / "current.json").unlink()
        (self.arms / "A.json").write_text("x" * (2 * 1024 * 1024 + 1))
        projection = self._build()
        self.assertEqual(projection["sources"]["current"]["status"], "missing")
        self.assertEqual(projection["sources"]["arm_A"]["status"], "oversized")
        self.assertEqual(projection["readiness"], "unknown")
        self.assertNotIn("equity", projection["arms"]["A"])

    def test_large_arm_history_remains_available_without_weakening_freshness(self):
        path = self.arms / "A.json"
        state = json.loads(path.read_text())
        state["trades"] *= 220
        self._write(path, state)
        self.assertGreater(path.stat().st_size, 1316417)
        self.assertLess(path.stat().st_size, 2 * 1024 * 1024)
        projection = self._build()
        self.assertEqual(projection["sources"]["arm_A"]["status"], "fresh")
        self.assertEqual(projection["arms"]["A"]["state_status"], "available")
        self.assertEqual(len(projection["arms"]["A"]["closed_trades"]), 20)
        self.assertEqual(projection["arms"]["A"]["positions"][0]["last_observed_bid"], 1.72)
        self.assertNotIn("credential", json.dumps(projection))

        state["positions"]["p1"]["last_quote_time"] = (self.now - timedelta(seconds=16)).isoformat()
        self._write(path, state)
        arm = self._build()["arms"]["A"]
        self.assertNotIn("last_observed_bid", arm["positions"][0])
        self.assertNotIn("equity", arm)

        state["last_observed_at"] = (self.now - timedelta(seconds=181)).isoformat()
        self._write(path, state)
        projection = self._build()
        self.assertEqual(projection["sources"]["arm_A"]["status"], "stale")
        self.assertNotIn("equity", projection["arms"]["A"])

    def test_larger_arm_budget_does_not_expand_other_source_limits(self):
        for path, key in ((self.paper / "current.json", "current"),
                          (self.configs / "config-A.json", "config_A")):
            with self.subTest(source=key):
                original = path.read_bytes()
                self._write(path, {"padding": "x" * (1024 * 1024)})
                self.assertEqual(self._build()["sources"][key]["status"], "oversized")
                path.write_bytes(original)

    def test_symlink_input_is_refused(self):
        target = self.root / "outside.json"
        target.write_text(json.dumps({"asof": self.now.isoformat(), "arms": {}}))
        (self.paper / "current.json").unlink()
        (self.paper / "current.json").symlink_to(target)
        projection = self._build()
        self.assertEqual(projection["sources"]["current"]["status"], "symlink")

    def test_missing_arm_observation_never_uses_old_state_mark_as_current(self):
        self._write(self.paper / "current.json", {"asof": self.now.isoformat(), "arms": {}})
        projection = self._build()
        arm = projection["arms"]["A"]
        self.assertEqual(arm["position_count"], 1)
        self.assertNotIn("equity", arm)
        self.assertNotIn("last_observed_bid", arm["positions"][0])

    def test_once_publishes_atomically_and_never_mutates_sources(self):
        source_paths = [self.paper / "current.json", *self.arms.glob("*.json"), *self.configs.glob("*.json")]
        before = {path: path.read_bytes() for path in source_paths}
        value = paper_feedback.run_once(root=self.paper, config_root=self.configs,
                                        output=self.output, now=self.now)
        self.assertEqual(json.loads(self.output.read_text()), value)
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o644)
        self.assertEqual(before, {path: path.read_bytes() for path in source_paths})


class RemoteSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.home = self.root / "home"
        self.workspace = self.root / "workspace"
        self.state = self.root / "state"
        self.now = datetime(2026, 10, 14, 14, 0, tzinfo=UTC)

    def tearDown(self):
        self.temp.cleanup()

    def test_exact_remote_control_command_and_environment_stripping(self):
        script = self.root / "codex-fake"
        script.write_text("#!/usr/bin/python3\nimport json,sys\n"
                          "if sys.argv[1:] != ['remote-control','start','--json']: sys.exit(4)\n"
                          "print(json.dumps({'status':'connected','serverName':'Rome-01','environmentId':'env-7','token':'omit'}))\n")
        script.chmod(0o755)
        report = remote_supervisor.run_once(codex_binary=str(script), home=self.home,
            workspace=self.workspace, state=self.state, now=self.now)
        self.assertEqual(report["status"], "connected")
        self.assertEqual(report["server_name"], "Rome-01")
        self.assertEqual(report["environment_id"], "env-7")
        self.assertNotIn("token", json.dumps(report))
        self.assertNotIn("OPENAI_API_KEY", remote_supervisor._minimal_env(self.home, self.workspace))

    def test_failure_reports_only_safe_error_class(self):
        def fail(_binary, _env):
            return {"status": "failed", "error_class": "cli_failed", "output": b"secret token"}
        report = remote_supervisor.run_once(codex_binary="unused", home=self.home,
            workspace=self.workspace, state=self.state, now=self.now, execute=fail)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_class"], "cli_failed")
        self.assertNotIn("secret", json.dumps(report))
        self.assertNotIn("token", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
