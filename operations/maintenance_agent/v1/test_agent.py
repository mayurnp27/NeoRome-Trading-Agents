import hashlib
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

import agent

UTC = timezone.utc


class FakeSystemd:
    def __init__(self, units):
        self.units = units
        self.calls = []
        self.fail_start = False
        self.on_start = None
        self.fail_reset = False

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))
        if argv[1] == "list-units":
            lines = [f"{name} loaded {v['ActiveState']} {v['SubState']} {v['Result']}" for name, v in self.units.items()]
            return SimpleNamespace(returncode=0, stdout="\n".join(lines), stderr="")
        if argv[1] == "show":
            props = argv[3].split("=", 1)[1].split(",")
            names = argv[4:]
            chunks = []
            for name in names:
                row = self.units.get(name)
                if row is None:
                    chunks.append(f"Id={name}\nLoadState=not-found")
                    continue
                chunks.append("\n".join(f"{key}={row.get(key, '')}" for key in props))
            return SimpleNamespace(returncode=0, stdout="\n\n".join(chunks), stderr="")
        if argv[1] == "--no-block" and argv[2] in {"start", "restart"} and len(argv) == 4:
            if self.fail_start:
                raise agent.subprocess.TimeoutExpired(argv, 5)
            if self.on_start:
                self.on_start(argv[3])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if argv[1] == 'reset-failed' and len(argv) == 3:
            return SimpleNamespace(returncode=int(self.fail_reset), stdout='', stderr='')
        raise AssertionError(f"unexpected command {argv}")


def row(name, *, active="active", kind="simple", pid="100", job="", fragment="", dropins=""):
    return {
        "Id": name, "LoadState": "loaded", "ActiveState": active,
        "SubState": "running" if active == "active" else "dead", "Result": "success",
        "Type": kind, "MainPID": pid, "Job": job, "UnitFileState": "enabled",
        "Unit": name[:-6] + ".service" if name.endswith(".timer") else "",
        "LastTriggerUSec": "", "NextElapseUSecRealtime": "", "NextElapseUSecMonotonic": "",
        "FragmentPath": fragment, "DropInPaths": dropins,
    }


class AgentTests(unittest.TestCase):
    def test_outcome_failure_detected_after_hours_despite_healthy_process(self):
        source = self.root / "options.json"
        source.write_text(json.dumps({"entry_record": {"position_id": "paper"},
                                     "updated_at": "2026-10-14T19:51:00Z",
                                     "last_result": {"status": "holding_internal_paper", "mark_status": "blocked_evidence"}}))
        self.write_policy(guards=False, source_statuses=[{"name": "options_full_port", "path": str(source),
                         "max_age_seconds": 1200, "market_window": "equity_rth", "outcome_kind": "options_lifecycle"}])
        report = agent.run_once(policy_path=self.policy_path, state_dir=self.state, apply=True,
                                now=datetime(2026, 10, 15, 1, tzinfo=UTC), run=self.run)
        self.assertIn({"key": "outcome:options_full_port", "reason": "position_valuation_blocked"},
                      [{k:v for k,v in i.items() if k != "episode_id"} for i in report["issues"]])
        self.assertFalse(report["actions"])

    def test_mark_success_and_closed_position_clear_outcome(self):
        for result in ({"mark_status": "persisted"}, {"status": "closed_internal_paper"}):
            self.assertEqual(agent._outcome_issues("options_full_port", "options_lifecycle",
                             {"entry_record": {}, "last_result": result}), [])

    def test_stochastic_nested_failure(self):
        issues = agent._outcome_issues("stochastic", "stochastic_options",
                                       {"status": "processed", "arms": {"A": {"status": "failed"}}})
        self.assertEqual(issues[0]["reason"], "strategy_cycle_failed")

    def test_recovery_runs_for_outcome_and_cooldown_prevents_repeat(self):
        source = self.root / "options.json"
        source.write_text(json.dumps({"entry_record": {}, "updated_at": agent._utc(self.now),
                                     "last_result": {"mark_status": "blocked_evidence"}}))
        self.write_policy(guards=False, units=[agent.OUTCOME_REPAIR], source_statuses=[{
            "name": "options_full_port", "path": str(source), "max_age_seconds": 1200,
            "outcome_kind": "options_lifecycle", "market_window": "equity_rth"}])
        policy = json.loads(self.policy_path.read_text())
        policy["repair_guards"][agent.OUTCOME_REPAIR] = {"unit_sha256": "a"*64,
            "source_path": "/opt/neorome/neorome/options_internal_paper.py", "source_sha256": "a"*64,
            "dropin_sha256": {}, "capture_unit_sha256": "a"*64,
            "capture_source_sha256": "a"*64, "capture_dropin_sha256": {}}
        self.policy_path.write_text(json.dumps(policy))
        with patch.object(agent, "_repair_target", return_value=True), patch.object(agent, "_recheck_target", return_value=True):
            first = agent.run_once(policy_path=self.policy_path, state_dir=self.state, apply=True, now=self.now, run=self.run)
            second = agent.run_once(policy_path=self.policy_path, state_dir=self.state, apply=True, now=self.now + timedelta(seconds=5), run=self.run)
        self.assertEqual(first["actions"], [{"unit": agent.OUTCOME_REPAIR, "status": "start_queued"}])
        self.assertFalse(second["actions"])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state = self.root / "state"
        self.policy_path = self.root / "policy.json"
        self.unit_path = self.root / "health.service"
        self.source_path = self.root / "health_endpoint.py"
        self.original_dropin_roots = agent.SYSTEMD_DROPIN_ROOTS
        self.dropin_root = self.root / "systemd" / "system"
        agent.SYSTEMD_DROPIN_ROOTS = (self.dropin_root,)
        dropin_dir = self.dropin_root / "neorome-health.service.d"
        dropin_dir.mkdir(parents=True)
        self.dropin_path = dropin_dir / "10-onfailure.conf"
        self.unit_path.write_text("[Service]\nType=simple\n")
        self.source_path.write_text("health endpoint fixture\n")
        self.dropin_path.write_text("[Unit]\nOnFailure=neorome-maintenance-agent.service\n")
        self.units = {
            "neorome-health.service": row("neorome-health.service", active="failed", pid="0", fragment=str(self.unit_path), dropins=str(self.dropin_path)),
            "neorome-health.timer": row("neorome-health.timer", kind="oneshot", active="inactive", pid="0"),
        }
        self.write_policy()
        self.now = datetime(2026, 10, 14, 14, 0, tzinfo=UTC)
        self.run = FakeSystemd(self.units)

    def tearDown(self):
        agent.SYSTEMD_DROPIN_ROOTS = self.original_dropin_roots
        self.tmp.cleanup()

    def write_policy(self, *, units=None, guards=True, source_statuses=None):
        guards_value = {}
        if guards:
            guards_value["neorome-health.service"] = {
                "unit_sha256": hashlib.sha256(self.unit_path.read_bytes()).hexdigest(),
                "source_path": str(self.source_path),
                "source_sha256": hashlib.sha256(self.source_path.read_bytes()).hexdigest(),
                "dropin_sha256": {str(self.dropin_path): hashlib.sha256(self.dropin_path.read_bytes()).hexdigest()},
            }
        value = {
            "schema_version": 1,
            "expected_units": units or ["neorome-health.service"],
            "source_statuses": source_statuses or [],
            "repair_guards": guards_value,
        }
        self.policy_path.write_text(json.dumps(value))

    def call(self, *, apply=False, now=None):
        return agent.run_once(policy_path=self.policy_path, state_dir=self.state, apply=apply,
                              now=now or self.now, run=self.run)

    def test_dry_run_never_starts_any_unit(self):
        report = self.call()
        self.assertEqual(report["status"], "degraded")
        self.assertEqual(report["actions"], [])
        self.assertFalse(any(cmd[-2:] == ["start", "neorome-health.service"] for cmd in self.run.calls))

    def parent_repair_fixture(self):
        name=agent.PARENT_REPAIR
        self.write_policy(guards=False,units=[name])
        policy=json.loads(self.policy_path.read_text())
        policy['repair_guards'][name]={'unit_sha256':'c'*64,'source_path':agent.PARENT_SOURCE,
            'source_sha256':'c'*64,'startup_source_sha256':'c'*64,'dropin_sha256':{}}
        self.policy_path.write_text(json.dumps(policy))
        self.run.units[name]=row(name,active='failed',kind='exec',pid='0',fragment='/parent.service')

    def test_parent_reset_start_is_audited_bounded_and_only_after_ready_proof(self):
        self.parent_repair_fixture()
        with patch.object(agent,'_hash_file',return_value='c'*64), \
             patch.object(agent,'_dropin_hashes_match',return_value=True), \
             patch.object(agent,'_parent_retry_evidence',return_value=False):
            self.assertEqual(self.call(apply=True)['actions'],[])
        self.assertFalse(any(c[1] in {'reset-failed','--no-block'} for c in self.run.calls))
        with patch.object(agent,'_hash_file',return_value='c'*64), \
             patch.object(agent,'_dropin_hashes_match',return_value=True), \
             patch.object(agent,'_parent_retry_evidence',return_value=True):
            report=self.call(apply=True)
            again=self.call(apply=True)
        mutations=[c for c in self.run.calls if c[1] in {'reset-failed','--no-block'}]
        self.assertEqual(mutations,[['systemctl','reset-failed',agent.PARENT_REPAIR],
                                   ['systemctl','--no-block','start',agent.PARENT_REPAIR]])
        self.assertEqual(report['actions'][0]['status'],'start_queued')
        self.assertEqual(again['actions'],[])
        events=[json.loads(line) for line in (self.state/'repair-events.jsonl').read_text().splitlines()]
        self.assertTrue(any(e.get('start_limit_reset_before_start') is True for e in events))

    def test_parent_failed_reset_never_starts(self):
        self.parent_repair_fixture();self.run.fail_reset=True
        with patch.object(agent,'_hash_file',return_value='c'*64), \
             patch.object(agent,'_dropin_hashes_match',return_value=True), \
             patch.object(agent,'_parent_retry_evidence',return_value=True):
            report=self.call(apply=True)
        self.assertEqual(report['actions'][0]['status'],'start_unknown')
        self.assertFalse(any(c[1]=='--no-block' for c in self.run.calls))

    def test_incident_episode_is_stable_while_active_and_fresh_after_resolution(self):
        state = {}
        first = [{"key": "unit:health", "reason": "unit_failed"}]
        agent._attach_incident_episodes(first, state)
        episode = first[0]["episode_id"]
        self.assertRegex(episode, r"^[0-9a-f]{64}$")

        # Polling/source metadata does not participate in an active episode.
        second = [{"key": "unit:health", "reason": "unit_failed", "source_version": "b" * 64}]
        agent._attach_incident_episodes(second, state)
        self.assertEqual(second[0]["episode_id"], episode)

        agent._attach_incident_episodes([], state)
        self.assertEqual(state["incident_episodes"], {})
        reopened = [{"key": "unit:health", "reason": "unit_failed"}]
        agent._attach_incident_episodes(reopened, state)
        self.assertRegex(reopened[0]["episode_id"], r"^[0-9a-f]{64}$")
        self.assertNotEqual(reopened[0]["episode_id"], episode)

    def test_apply_starts_only_allowlisted_inactive_service_and_never_timer(self):
        self.run.units["neorome-health.service"]["ActiveState"] = "inactive"
        self.run.units["neorome-health.service"]["SubState"] = "dead"
        self.run.units["neorome-health.service"]["Result"] = "success"
        report = self.call(apply=True)
        starts = [cmd for cmd in self.run.calls if "start" in cmd]
        self.assertEqual(starts, [["systemctl", "--no-block", "start", "neorome-health.service"]])
        self.assertEqual(report["actions"][0]["status"], "start_queued")

    def test_zero_job_identifier_is_no_queued_job(self):
        item = self.run.units["neorome-health.service"]
        item.update(ActiveState="inactive", MainPID="0", Job="0")
        guard = json.loads(self.policy_path.read_text())["repair_guards"]["neorome-health.service"]
        self.assertTrue(agent._repair_target("neorome-health.service", item, guard, self.run.units))

    def test_active_timer_prevents_service_start(self):
        self.run.units["neorome-health.timer"]["ActiveState"] = "active"
        self.call(apply=True)
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))

    def test_infinity_timer_deadline_is_unarmed_and_inactive_expected_timer_is_reported(self):
        timer = row("neorome-paper-runtime.timer", kind="oneshot", active="active", pid="0")
        timer["NextElapseUSecMonotonic"] = "infinity"
        timer["NextElapseUSecRealtime"] = "infinity"
        target = row("neorome-paper-runtime.service", active="inactive", kind="oneshot", pid="0")
        issues = agent._unit_issues({timer["Id"]: timer, target["Id"]: target}, [timer["Id"]])
        self.assertEqual(issues, [{"key": "unit:neorome-paper-runtime.timer", "reason": "timer_unarmed"}])
        timer["ActiveState"] = "inactive"
        issues = agent._unit_issues({timer["Id"]: timer}, [timer["Id"]])
        self.assertEqual(issues[0]["reason"], "timer_inactive")
        # An intentionally disabled timer outside expected_units is not a fault.
        self.assertEqual(agent._unit_issues({timer["Id"]: timer}, []), [])

    def test_timer_uses_shared_unit_property_for_running_service(self):
        timer = row("neorome-equity-shared-marks.timer", kind="oneshot", active="active", pid="0")
        timer["Unit"] = "neorome-equity-shared-marks.service"
        timer["NextElapseUSecMonotonic"] = "infinity"
        timer["NextElapseUSecRealtime"] = "infinity"
        shared = row("neorome-equity-shared-marks.service", kind="oneshot", active="active", pid="123")
        derived = row("neorome-equity-shared-marks.timer.service", kind="oneshot", active="inactive", pid="0")
        issues = agent._unit_issues(
            {timer["Id"]: timer, shared["Id"]: shared, derived["Id"]: derived}, [timer["Id"]]
        )
        self.assertEqual(issues, [])

    def test_timer_waiting_for_queued_job_or_cleanup_is_not_unarmed(self):
        timer = row("neorome-control-sync.timer", kind="oneshot", active="active", pid="0")
        timer["Unit"] = "neorome-control-sync.service"
        timer["NextElapseUSecMonotonic"] = "infinity"
        timer["NextElapseUSecRealtime"] = "infinity"
        for updates in ({"ActiveState": "deactivating"}, {"Job": "12345"}, {"MainPID": "123"}):
            with self.subTest(updates=updates):
                service = row("neorome-control-sync.service", kind="oneshot", active="inactive", pid="0")
                service.update(updates)
                self.assertEqual(agent._unit_issues({timer["Id"]: timer, service["Id"]: service},
                                                   [timer["Id"], service["Id"]]), [])

    def test_queued_cleanup_does_not_hide_failed_paired_service(self):
        timer = row("neorome-control-sync.timer", kind="oneshot", active="active", pid="0")
        timer["Unit"] = "neorome-control-sync.service"
        timer["NextElapseUSecMonotonic"] = "infinity"
        service = row("neorome-control-sync.service", kind="oneshot", active="failed", pid="0")
        service["Job"] = "12345"
        self.assertEqual(agent._unit_issues({timer["Id"]: timer, service["Id"]: service},
                                           [timer["Id"], service["Id"]]),
                         [{"key": "unit:" + service["Id"], "reason": "unit_failed"}])

    def test_timer_unarmed_requires_continuous_twenty_seconds_and_clears_when_healthy(self):
        timer_name = "neorome-paper-runtime.timer"
        timer = row(timer_name, kind="oneshot", active="active", pid="0")
        timer["NextElapseUSecMonotonic"] = "infinity"
        timer["NextElapseUSecRealtime"] = "infinity"
        service = row("neorome-paper-runtime.service", kind="oneshot", active="inactive", pid="0")
        self.units.clear()
        self.units.update({timer_name: timer, service["Id"]: service})
        self.write_policy(units=[timer_name], guards=False)

        first = self.call(now=self.now)
        self.assertEqual(first["issues"], [])
        self.assertEqual(json.loads((self.state / "state.json").read_text())["timer_unarmed_since"]["unit:" + timer_name], agent._utc(self.now))
        before_threshold = self.call(now=self.now + timedelta(seconds=19))
        self.assertEqual(before_threshold["issues"], [])
        sustained = self.call(now=self.now + timedelta(seconds=20))
        self.assertEqual([(issue["key"], issue["reason"]) for issue in sustained["issues"]],
                         [("unit:" + timer_name, "timer_unarmed")])
        self.assertRegex(sustained["issues"][0]["episode_id"], r"^[0-9a-f]{64}$")

        timer["NextElapseUSecRealtime"] = "2026-10-14 14:05:00 UTC"
        healthy = self.call(now=self.now + timedelta(seconds=21))
        self.assertEqual(healthy["issues"], [])
        saved = json.loads((self.state / "state.json").read_text())
        self.assertEqual(saved["timer_unarmed_since"], {})

        timer["NextElapseUSecRealtime"] = "infinity"
        restarted_episode = self.call(now=self.now + timedelta(seconds=30))
        self.assertEqual(restarted_episode["issues"], [])
        saved = json.loads((self.state / "state.json").read_text())
        self.assertEqual(saved["timer_unarmed_since"]["unit:" + timer_name], agent._utc(self.now + timedelta(seconds=30)))

    def test_other_unit_faults_remain_immediate_during_timer_grace(self):
        timer_name = "neorome-paper-runtime.timer"
        timer = row(timer_name, kind="oneshot", active="active", pid="0")
        timer["NextElapseUSecMonotonic"] = "infinity"
        timer["NextElapseUSecRealtime"] = "infinity"
        service = row("neorome-paper-runtime.service", kind="oneshot", active="inactive", pid="0")
        failed = row("neorome-health.service", active="failed", kind="simple", pid="0")
        self.units.clear()
        self.units.update({timer_name: timer, service["Id"]: service, failed["Id"]: failed})
        self.write_policy(units=[timer_name, failed["Id"]], guards=False)
        report = self.call(now=self.now)
        self.assertEqual([(issue["key"], issue["reason"]) for issue in report["issues"]],
                         [("unit:neorome-health.service", "unit_failed")])

    def test_invalid_or_future_timer_unarmed_time_restarts_grace(self):
        key = "unit:neorome-paper-runtime.timer"
        issue = {"key": key, "reason": "timer_unarmed"}
        for saved_time in ("invalid", agent._utc(self.now + timedelta(seconds=1))):
            state = {"timer_unarmed_since": {key: saved_time}}
            visible = agent._defer_timer_unarmed([issue], state, self.now)
            self.assertEqual(visible, [])
            self.assertEqual(state["timer_unarmed_since"], {key: agent._utc(self.now)})

    def test_newly_discovered_failed_units_are_reported_but_disabled_timers_are_quiet(self):
        failed = row("neorome-options-probe.service", active="failed", pid="0")
        disabled = row("neorome-optional.timer", active="inactive", kind="oneshot", pid="0")
        issues = agent._unit_issues({failed["Id"]: failed, disabled["Id"]: disabled}, [])
        self.assertEqual(issues, [{"key": "unit:neorome-options-probe.service", "reason": "unit_failed"}])

    def test_active_job_oneshot_and_timer_are_never_repaired(self):
        for item in self.run.units.values():
            if item["Id"].endswith(".service"):
                item.update(ActiveState="active", MainPID="0", Job="start")
        self.run.units["neorome-health.service"]["Type"] = "oneshot"
        self.call(apply=True)
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))
        self.assertEqual(agent._repair_target("neorome-health.timer", self.run.units["neorome-health.timer"], {}, self.run.units), False)
        oneshot = row("neorome-job.service", active="inactive", kind="oneshot", pid="0")
        self.assertEqual(agent._unit_issues({"neorome-job.service": oneshot}, ["neorome-job.service"]), [])

    def test_hash_mismatch_blocks_start(self):
        self.source_path.write_text("changed source\n")
        self.call(apply=True)
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))

    def test_unit_hash_mismatch_blocks_start(self):
        value = json.loads(self.policy_path.read_text())
        value["repair_guards"]["neorome-health.service"]["unit_sha256"] = "0" * 64
        self.policy_path.write_text(json.dumps(value))
        self.call(apply=True)
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))

    def test_dropin_hash_mismatch_blocks_start(self):
        self.dropin_path.write_text("[Unit]\nOnFailure=unexpected.service\n")
        self.call(apply=True)
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))

    def test_missing_or_unpinned_dropin_blocks_start(self):
        item = self.run.units["neorome-health.service"]
        guard = json.loads(self.policy_path.read_text())["repair_guards"]["neorome-health.service"]
        item["DropInPaths"] = ""
        self.assertFalse(agent._repair_target("neorome-health.service", item, guard, self.run.units))
        item["DropInPaths"] = f"{self.dropin_path} {self.root / 'extra.conf'}"
        self.assertFalse(agent._repair_target("neorome-health.service", item, guard, self.run.units))

    def test_policy_requires_explicit_dropin_hash_map(self):
        value = json.loads(self.policy_path.read_text())
        del value["repair_guards"]["neorome-health.service"]["dropin_sha256"]
        self.policy_path.write_text(json.dumps(value))
        with self.assertRaises(agent.MaintenanceError):
            self.call(apply=True)

    def test_policy_cannot_extend_static_allowlist(self):
        self.write_policy(units=["neorome-health.service"], guards=False)
        value = json.loads(self.policy_path.read_text())
        value["repair_guards"]["neorome-paper-runtime.service"] = {
            "unit_sha256": "a" * 64, "source_path": str(self.source_path), "source_sha256": "b" * 64,
        }
        self.policy_path.write_text(json.dumps(value))
        with self.assertRaises(agent.MaintenanceError):
            self.call(apply=True)

    def test_attempt_is_durable_before_interrupted_start_and_respects_cooldown(self):
        self.run.fail_start = True
        report = self.call(apply=True)
        state = json.loads((self.state / "state.json").read_text())
        self.assertEqual(state["attempts"]["neorome-health.service"]["count"], 1)
        self.assertEqual(report["actions"][0]["status"], "start_unknown")
        self.run.fail_start = False
        self.run.calls.clear()
        self.call(apply=True, now=self.now + timedelta(seconds=30))
        self.assertFalse(any("start" in cmd for cmd in self.run.calls))
        self.call(apply=True, now=self.now + timedelta(seconds=601))
        self.assertEqual(sum("start" in cmd for cmd in self.run.calls), 1)

    def test_incidents_deduplicate_and_resolve(self):
        first = self.call()
        self.assertEqual(len((self.state / "repair-events.jsonl").read_text().splitlines()), 1)
        self.call(now=self.now + timedelta(seconds=60))
        self.assertEqual(len((self.state / "repair-events.jsonl").read_text().splitlines()), 1)
        self.run.units["neorome-health.service"].update(ActiveState="active", MainPID="88", SubState="running")
        self.call(now=self.now + timedelta(seconds=120))
        events = [json.loads(line) for line in (self.state / "repair-events.jsonl").read_text().splitlines()]
        self.assertEqual([e["event"] for e in events], ["opened", "resolved"])
        self.assertFalse(json.loads((self.state / "incidents.json").read_text())["active"])

    def test_stale_status_is_suppressed_outside_equity_hours(self):
        source = self.root / "desk.json"
        source.write_text(json.dumps({"updated_at": "2026-10-14T12:00:00Z"}))
        self.write_policy(source_statuses=[{"name": "equity", "path": str(source), "max_age_seconds": 60, "market_window": "equity_rth"}])
        closed = agent._source_issues(agent._load_policy(self.policy_path), datetime(2026, 10, 14, 22, 0, tzinfo=UTC))
        opened = agent._source_issues(agent._load_policy(self.policy_path), datetime(2026, 10, 14, 14, 0, tzinfo=UTC))
        self.assertEqual(closed, [])
        self.assertEqual(opened[0]["reason"], "status_stale")

    def test_fresh_status_with_false_configured_health_field_is_reported(self):
        source = self.root / "status.json"
        source.write_text(json.dumps({"updated_at": self.now.isoformat(), "operational_healthy": False}))
        self.write_policy(source_statuses=[{
            "name": "paper", "path": str(source), "max_age_seconds": 180,
            "market_window": "always", "health_field": "operational_healthy",
        }])
        issues = agent._source_issues(agent._load_policy(self.policy_path), self.now)
        self.assertEqual(issues, [{"key": "source:paper:health", "reason": "status_unhealthy"}])

    def test_health_field_policy_is_strictly_validated(self):
        source = self.root / "status.json"
        source.write_text("{}")
        self.write_policy(source_statuses=[{
            "name": "paper", "path": str(source), "max_age_seconds": 180,
            "health_field": "",
        }])
        with self.assertRaises(agent.MaintenanceError):
            agent._load_policy(self.policy_path)

    def test_fresh_file_mtime_health_flag_stays_unhealthy(self):
        source = self.root / "completion.json"
        source.write_text(json.dumps({"updated_at": "2020-01-01T00:00:00Z", "operational_healthy": False}))
        os.utime(source, (self.now.timestamp(), self.now.timestamp()))
        self.write_policy(source_statuses=[{
            "name": "completion", "path": str(source), "max_age_seconds": 180,
            "timestamp_source": "file_mtime", "health_field": "operational_healthy",
        }])
        issues = agent._source_issues(agent._load_policy(self.policy_path), self.now)
        self.assertEqual(issues, [{"key": "source:completion:health", "reason": "status_unhealthy"}])

    def test_health_and_freshness_remain_distinct_incidents(self):
        source = self.root / "stale-unhealthy.json"
        source.write_text(json.dumps({"updated_at": "2026-10-14T12:00:00Z", "operational_healthy": False}))
        self.write_policy(source_statuses=[{"name": "desk", "path": str(source), "max_age_seconds": 60,
                                           "health_field": "operational_healthy"}])
        issues = agent._source_issues(agent._load_policy(self.policy_path), self.now)
        self.assertEqual({item["key"] for item in issues}, {"source:desk:health", "source:desk"})

    def test_stale_file_mtime_reports_completion_staleness(self):
        source = self.root / "completion.json"
        source.write_text(json.dumps({"updated_at": self.now.isoformat()}))
        stale = self.now.timestamp() - 300
        os.utime(source, (stale, stale))
        self.write_policy(source_statuses=[{
            "name": "completion", "path": str(source), "max_age_seconds": 60,
            "timestamp_source": "file_mtime",
        }])
        issues = agent._source_issues(agent._load_policy(self.policy_path), self.now)
        self.assertEqual(issues, [{"key": "source:completion", "reason": "status_file_stale"}])

    def test_timestamp_source_enum_is_strictly_validated(self):
        source = self.root / "completion.json"
        source.write_text("{}")
        self.write_policy(source_statuses=[{
            "name": "completion", "path": str(source), "max_age_seconds": 60,
            "timestamp_source": "quote_timestamp",
        }])
        with self.assertRaises(agent.MaintenanceError):
            agent._load_policy(self.policy_path)

    def test_no_provider_or_finance_commands(self):
        self.call(apply=True)
        self.assertTrue(all(cmd[0] == "systemctl" for cmd in self.run.calls))
        self.assertFalse(any(any(token in " ".join(cmd).lower() for token in ("robinhood", "alpaca", "provider", "trade", "ledger")) for cmd in self.run.calls))

    def test_event_capacity_blocks_recovery_without_truncating_history(self):
        original = agent.MAX_EVENTS_BYTES
        try:
            agent.MAX_EVENTS_BYTES = 1
            report = self.call(apply=True)
            self.assertTrue(report["event_history_capacity_exhausted"])
            self.assertFalse(any("start" in cmd for cmd in self.run.calls))
            event_file = self.state / "repair-events.jsonl"
            self.assertFalse(event_file.exists())
        finally:
            agent.MAX_EVENTS_BYTES = original

    def _write_recipe(self, target):
        unit_dir = self.root / "recipes"
        unit_dir.mkdir(exist_ok=True)
        unit_path = unit_dir / target
        unit_path.write_text("[Unit]\nDescription=reviewed fixture\n")
        source_path = "/opt/neorome/fixture_worker.py"
        guard = {"unit_sha256": hashlib.sha256(unit_path.read_bytes()).hexdigest(),
                 "source_path": source_path, "source_sha256": hashlib.sha256(b"source fixture").hexdigest(),
                 "dropin_sha256": {}}
        return unit_path, source_path, guard

    def _write_oneshot_recipe(self, name):
        timer_name = agent.ONESHOT_RETRY_RECIPES[name]
        service_path, source_path, guard = self._write_recipe(name)
        timer_path, _, _ = self._write_recipe(timer_name)
        item = row(name, active="failed", kind="oneshot", pid="0", fragment=str(service_path))
        timer = row(timer_name, active="active", kind="oneshot", pid="0", fragment=str(timer_path))
        self.units.clear()
        self.units[name] = item
        self.units[timer_name] = timer
        return source_path, guard

    def test_only_fixed_idempotent_oneshots_can_retry_while_paired_timer_active(self):
        name = "neorome-memecoin-e2-report.service"
        source_path, guard = self._write_oneshot_recipe(name)
        self.write_policy(units=[name], guards=False)
        policy = json.loads(self.policy_path.read_text())
        policy["repair_guards"][name] = guard
        self.policy_path.write_text(json.dumps(policy))
        hashes = {source_path: guard["source_sha256"],
                  self.units[name]["FragmentPath"]: guard["unit_sha256"]}
        self.run.on_start = lambda unit: self.assertEqual(
            json.loads((self.state / "state.json").read_text())["attempts"][name]["count"], 1)
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path))):
            report = self.call(apply=True)
        self.assertEqual(report["actions"], [{"unit": name, "status": "start_queued"}])
        self.assertEqual([call[-2:] for call in self.run.calls if "start" in call], [["start", name]])

    def test_l7t4_report_retry_requires_the_pinned_readonly_publisher(self):
        name = "neorome-l7t4-report.service"
        _, guard = self._write_oneshot_recipe(name)
        source = "/opt/neorome-l7t4-reporting-20261007-v2/publish.py"
        guard["source_path"] = source
        self.write_policy(units=[name], guards=False)
        policy = json.loads(self.policy_path.read_text())
        policy["repair_guards"][name] = guard
        self.policy_path.write_text(json.dumps(policy))
        hashes = {source: guard["source_sha256"],
                  self.units[name]["FragmentPath"]: guard["unit_sha256"]}
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path))):
            report = self.call(apply=True)
            drifted = dict(guard, source_sha256="0" * 64)
            self.assertFalse(agent._repair_target(name, self.units[name], drifted, self.units))
        self.assertEqual(report["actions"], [{"unit": name, "status": "start_queued"}])

    def test_report_oneshot_inactive_running_pid_job_and_direct_trading_target_are_rejected(self):
        name = "neorome-memecoin-e4-report.service"
        source_path, guard = self._write_oneshot_recipe(name)
        timer = agent.ONESHOT_RETRY_RECIPES[name]
        for updates in ({"ActiveState": "inactive"}, {"ActiveState": "active"},
                        {"ActiveState": "failed", "MainPID": "52"},
                        {"ActiveState": "failed", "Job": "start"}):
            item = dict(self.units[name])
            item.update(updates)
            with patch.object(agent, "_hash_file", return_value="0" * 64):
                self.assertFalse(agent._repair_target(name, item, guard,
                    {name: item, timer: self.units[timer]}))
        policy = {"schema_version": 1, "expected_units": ["neorome-memecoin-e4.service"],
                  "source_statuses": [], "repair_guards": {"neorome-memecoin-e4.service": guard}}
        self.policy_path.write_text(json.dumps(policy))
        with self.assertRaises(agent.MaintenanceError):
            agent._load_policy(self.policy_path)

    def test_timer_recipe_validates_fixed_pair_and_rejects_active_or_busy_worker(self):
        timer_name = "neorome-stochastic-paper-v2.timer"
        service_name = agent.TIMER_REPAIR_RECIPES[timer_name]
        timer_path, _, timer_guard = self._write_recipe(timer_name)
        service_path, source_path, worker_guard = self._write_recipe(service_name)
        timer_dropin_dir = self.dropin_root / (timer_name + ".d")
        timer_dropin_dir.mkdir()
        timer_dropin = timer_dropin_dir / "reviewed.conf"
        timer_dropin.write_text("[Timer]\nPersistent=false\n")
        worker_dropin_dir = self.dropin_root / (service_name + ".d")
        worker_dropin_dir.mkdir()
        worker_dropin = worker_dropin_dir / "reviewed.conf"
        worker_dropin.write_text("[Service]\nNoNewPrivileges=true\n")
        timer_guard.update({
            "source_path": source_path, "source_sha256": worker_guard["source_sha256"],
            "dropin_sha256": {str(timer_dropin): hashlib.sha256(timer_dropin.read_bytes()).hexdigest()},
            "paired_service": service_name, "paired_unit_sha256": worker_guard["unit_sha256"],
            "paired_dropin_sha256": {str(worker_dropin): hashlib.sha256(worker_dropin.read_bytes()).hexdigest()},
        })
        timer = row(timer_name, active="inactive", kind="oneshot", pid="0", fragment=str(timer_path), dropins=str(timer_dropin))
        worker = row(service_name, active="inactive", kind="oneshot", pid="0", fragment=str(service_path), dropins=str(worker_dropin))
        hashes = {source_path: timer_guard["source_sha256"]}
        fixture_units = {timer_name: timer, service_name: worker}
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path),
                hashlib.sha256(Path(path).read_bytes()).hexdigest() if Path(path).is_file() else None)):
            self.assertTrue(agent._repair_target(timer_name, timer, timer_guard, fixture_units))
            active = dict(timer, ActiveState="active")
            self.assertFalse(agent._repair_target(timer_name, active, timer_guard, fixture_units))
            worker_busy = dict(worker, ActiveState="active", MainPID="99")
            self.assertFalse(agent._repair_target(timer_name, timer, timer_guard,
                {timer_name: timer, service_name: worker_busy}))
            timer_path.write_text("drifted timer source\n")
            self.assertFalse(agent._repair_target(timer_name, timer, timer_guard, fixture_units))

        wrong_pair = dict(timer_guard, paired_service="neorome-e4.service")
        policy = {"schema_version": 1, "expected_units": [timer_name], "source_statuses": [],
                  "repair_guards": {timer_name: wrong_pair}}
        self.policy_path.write_text(json.dumps(policy))
        with self.assertRaises(agent.MaintenanceError):
            agent._load_policy(self.policy_path)

    def test_timer_recovery_starts_only_after_durable_attempt_and_exact_recheck(self):
        timer_name = "neorome-paper-runtime.timer"
        service_name = agent.TIMER_REPAIR_RECIPES[timer_name]
        timer_path, _, guard = self._write_recipe(timer_name)
        service_path, source_path, worker_guard = self._write_recipe(service_name)
        guard.update({"source_path": source_path, "source_sha256": worker_guard["source_sha256"],
                      "paired_service": service_name, "paired_unit_sha256": worker_guard["unit_sha256"],
                      "paired_dropin_sha256": {}})
        timer = row(timer_name, active="inactive", kind="oneshot", pid="0", fragment=str(timer_path))
        worker = row(service_name, active="inactive", kind="oneshot", pid="0", fragment=str(service_path))
        self.units.clear()
        self.units.update({timer_name: timer, service_name: worker})
        self.write_policy(units=[timer_name], guards=False)
        policy = json.loads(self.policy_path.read_text())
        policy["repair_guards"][timer_name] = guard
        self.policy_path.write_text(json.dumps(policy))
        hashes = {source_path: guard["source_sha256"]}
        self.run.on_start = lambda unit: self.assertEqual(
            json.loads((self.state / "state.json").read_text())["attempts"][timer_name]["count"], 1)
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path),
                hashlib.sha256(Path(path).read_bytes()).hexdigest() if Path(path).is_file() else None)):
            report = self.call(apply=True)
        self.assertEqual(report["actions"], [{"unit": timer_name, "status": "start_queued"}])
        self.assertEqual([call[-2:] for call in self.run.calls if "start" in call], [["start", timer_name]])

    def _control_sync_timer_fixture(self, *, timer_state="active", next_time="infinity", worker_state="inactive"):
        timer_name = "neorome-control-sync.timer"
        service_name = "neorome-control-sync.service"
        timer_path, _, guard = self._write_recipe(timer_name)
        service_path, source_path, worker_guard = self._write_recipe(service_name)
        guard.update({"source_path": source_path, "source_sha256": worker_guard["source_sha256"],
                      "paired_service": service_name, "paired_unit_sha256": worker_guard["unit_sha256"],
                      "paired_dropin_sha256": {}})
        timer = row(timer_name, active=timer_state, kind="oneshot", pid="0", fragment=str(timer_path))
        timer["NextElapseUSecRealtime"] = next_time
        timer["NextElapseUSecMonotonic"] = "infinity" if next_time == "infinity" else ""
        timer["Unit"] = service_name
        worker = row(service_name, active=worker_state, kind="oneshot", pid="0", fragment=str(service_path))
        self.units.clear()
        self.units.update({timer_name: timer, service_name: worker})
        self.write_policy(units=[timer_name], guards=False)
        policy = json.loads(self.policy_path.read_text())
        policy["repair_guards"][timer_name] = guard
        self.policy_path.write_text(json.dumps(policy))
        hashes = {source_path: guard["source_sha256"]}
        hashes[str(timer_path)] = guard["unit_sha256"]
        hashes[str(service_path)] = guard["paired_unit_sha256"]
        return timer_name, service_name, timer, worker, guard, hashes

    def test_control_sync_active_unarmed_timer_restarts_after_debounce_and_durable_claim(self):
        timer_name, _service_name, timer, _worker, _guard, hashes = self._control_sync_timer_fixture()
        self.run.on_start = lambda unit: self.assertEqual(
            json.loads((self.state / "state.json").read_text())["attempts"][unit]["count"], 1)
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path))):
            early = self.call(apply=True, now=self.now)
            self.assertEqual(early["actions"], [])
            report = self.call(apply=True, now=self.now + timedelta(seconds=20))
        self.assertEqual(report["actions"], [{"unit": timer_name, "status": "restart_queued"}])
        self.assertEqual([call[-2:] for call in self.run.calls if len(call) >= 3 and call[2] in {"start", "restart"}],
                         [["restart", timer_name]])
        saved = json.loads((self.state / "state.json").read_text())
        self.assertEqual(saved["attempts"][timer_name]["count"], 1)
        self.assertEqual(saved["attempts"][timer_name]["status"], "restart_queued")

    def test_control_sync_inactive_timer_keeps_start_behavior(self):
        timer_name, _service_name, timer, _worker, guard, hashes = self._control_sync_timer_fixture(
            timer_state="inactive")
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path))):
            report = self.call(apply=True)
        self.assertEqual(report["actions"], [{"unit": timer_name, "status": "start_queued"}])
        self.assertEqual([call[-2:] for call in self.run.calls if len(call) >= 3 and call[2] in {"start", "restart"}],
                         [["start", timer_name]])

    def test_control_sync_restart_rejects_busy_or_armed_pair_and_guard_drift(self):
        timer_name, service_name, timer, worker, guard, hashes = self._control_sync_timer_fixture(worker_state="active")
        timer["Job"] = "0"
        with patch.object(agent, "_hash_file", side_effect=lambda path: hashes.get(str(path))):
            self.assertFalse(agent._repair_target(timer_name, timer, guard, self.units))
            idle_worker = dict(worker, ActiveState="inactive")
            armed_timer = dict(timer, NextElapseUSecRealtime="2026-10-15 14:05:00 UTC")
            self.assertFalse(agent._repair_target(timer_name, armed_timer, guard,
                                                  {timer_name: armed_timer, service_name: idle_worker}))
            drift = dict(guard, paired_unit_sha256="0" * 64)
            self.assertFalse(agent._repair_target(timer_name, timer, drift,
                                                  {timer_name: timer, service_name: idle_worker}))

    def test_trading_timer_active_unarmed_restart_is_forbidden(self):
        timer_name = "neorome-stochastic-paper-v2.timer"
        service_name = agent.TIMER_REPAIR_RECIPES[timer_name]
        timer_path, _, guard = self._write_recipe(timer_name)
        service_path, source_path, paired_guard = self._write_recipe(service_name)
        guard.update({"source_path": source_path, "source_sha256": paired_guard["source_sha256"],
                      "paired_service": service_name, "paired_unit_sha256": paired_guard["unit_sha256"],
                      "paired_dropin_sha256": {}})
        timer = row(timer_name, active="active", kind="oneshot", pid="0", fragment=str(timer_path))
        timer.update(NextElapseUSecRealtime="infinity", NextElapseUSecMonotonic="infinity")
        worker = row(service_name, active="inactive", kind="oneshot", pid="0", fragment=str(service_path))
        with patch.object(agent, "_hash_file", side_effect=lambda path: "0" * 64):
            self.assertFalse(agent._repair_target(timer_name, timer, guard,
                                                  {timer_name: timer, service_name: worker}))


if __name__ == "__main__":
    unittest.main()
