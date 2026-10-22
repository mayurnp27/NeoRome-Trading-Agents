import json
import hashlib
import os
import io
import tarfile
import tempfile
import unittest
from unittest import mock
from datetime import datetime, timezone, timedelta
from pathlib import Path

import worker

UTC = timezone.utc
PACKAGE_SHA = "a" * 64


class WorkerTests(unittest.TestCase):
    def test_bound_context_batch_is_copied_once_and_new_evidence_dispatches(self):
        import os
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from operations.maintenance_agent.context_v1 import context
        entries = [{'key': f'unit:neorome-bcp3-child-freshness-r2@{n}-978750-997.service',
                    'reason': 'unit_failed', 'episode_id': f'{n:064x}', 'cause_code': 'stdin_closed',
                    'parent_failure_code':'shared_budget_busy','parent_pid':'978750',
                    'parent_failure_status':'cycle_error','parent_failure_reason':'chain_candidate_preflight_due'} for n in (11, 13)]

        def publish(version='b'*64):
            self.write_inbox(incidents=entries)
            value=json.loads(self.inbox.read_text())
            value.update(units={context.PARENT: {'MainPID': '1410080'}}, runtime_version=version)
            groups=context.groups(value)
            for group in groups.values():
                group['repair_memory']={'status': 'available', 'brief': 'Reference data only', 'selected': []}
            document={'schema_version': 1, 'as_of': value['as_of'],
                'observer_package_sha256': PACKAGE_SHA, 'memory_status': 'available', 'groups': groups}
            value['repair_context_sha256']=context.atomic(self.inbox.with_name('repair-context.json'),document,0o644)
            self.inbox.write_text(json.dumps(value))

        with mock.patch.object(context, 'TRUSTED_UID', os.getuid()):
            publish()
            self.assertEqual(self.invoke()['status'], 'completed')
            workspace=self.launch_calls[0][3]
            self.assertTrue((workspace/'repair-context.json').is_file())
            self.assertEqual(len(json.loads((workspace/'repair-context.json').read_text())['members']),2)
            self.assertEqual(self.invoke()['status'], 'awaiting_remediation')
            self.assertEqual(len(self.launch_calls),1)
            publish(version='c'*64)
            self.assertEqual(self.invoke()['status'], 'completed')
            self.assertEqual(len(self.launch_calls),2)
        claims=json.loads((self.state/'claims.json').read_text())
        self.assertTrue(all(len(c['source_fingerprints'])==2 for c in claims['claims'].values()))

    def test_corrupt_bound_context_does_not_duplicate_or_claim_an_incident(self):
        value=json.loads(self.inbox.read_text());value['repair_context_sha256']='f'*64
        self.inbox.write_text(json.dumps(value));self.inbox.with_name('repair-context.json').write_text('{}')
        self.assertEqual(self.invoke()['status'], 'repair_context_unavailable')
        state=json.loads((self.state/'claims.json').read_text())
        self.assertEqual(state['calls_used'],0)
        self.assertEqual(state['claims'],{})
        self.assertEqual(self.launch_calls,[])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.inbox = self.root / "inbox" / "status.json"
        self.state = self.root / "state"
        self.source = self.root / "source"
        (self.source / "neorome").mkdir(parents=True)
        (self.source / "neorome" / "module.py").write_text("def example():\n    return 1\n")
        self.now = datetime(2026, 10, 14, 14, 0, tzinfo=UTC)
        self.write_inbox()
        self.auth_calls = 0
        self.launch_calls = []
        self.preflight_calls = []
        worker._SANDBOX_PREFLIGHT_CACHE.clear()

    def tearDown(self):
        self.temp.cleanup()

    def write_inbox(self, incidents=None, *, package_sha=PACKAGE_SHA, as_of=None):
        self.inbox.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": 1,
            "as_of": (as_of or self.now).isoformat().replace("+00:00", "Z"),
            "agent_package_sha256": package_sha,
            "incidents": incidents if incidents is not None else [{"key": "unit:neorome-health.service", "reason": "unit_failed"}],
        }
        self.inbox.write_text(json.dumps(payload))

    def auth(self, status="authenticated"):
        def check(_binary, _env):
            self.auth_calls += 1
            return status
        return check

    def launcher(self, status="completed", *, fail=False):
        def call(binary, model, effort, state_dir, home_dir, workspace, schema, **kwargs):
            self.launch_calls.append((binary, model, effort, workspace, kwargs))
            claims = json.loads((Path(state_dir) / "claims.json").read_text())
            self.assertEqual(claims["calls_used"], len(self.launch_calls))
            self.assertEqual(claims["claims"][Path(workspace).name]["status"], "claimed_before_exec")
            self.assertTrue((workspace / "incident.json").is_file())
            if fail:
                raise RuntimeError("simulated worker interruption")
            result = {"diagnosis": "cause", "change_paths": ["neorome/module.py"],
                      "test_results": ["unit tests pass"], "escalation": "human review required"}
            return {"status": status, "result": result}
        return call

    def preflight(self, status="ready"):
        def check(binary, env, cwd):
            self.preflight_calls.append((binary, dict(env), cwd))
            return {"status": status}
        return check

    def invoke(self, *, auth=None, preflight=None, launcher=None, now=None, model="gpt-6-luna"):
        return worker.run_once(
            inbox_path=self.inbox, state_dir=self.state, source_root=self.source,
            now=now or self.now, model=model, auth_check=auth or self.auth(),
            preflight_check=preflight or self.preflight(),
            launcher=launcher or self.launcher(),
        )

    def test_claim_is_durable_before_launcher_and_exception_does_not_repeat(self):
        report = self.invoke(launcher=self.launcher(fail=True))
        self.assertEqual(report["status"], "launcher_error")
        state = json.loads((self.state / "claims.json").read_text())
        self.assertEqual(state["calls_used"], 1)
        self.assertEqual(len(state["claims"]), 1)
        again = self.invoke(now=self.now + timedelta(seconds=2), launcher=self.launcher())
        self.assertEqual(again["status"], "awaiting_remediation")
        self.assertEqual(len(self.launch_calls), 1)

    def test_root_snapshot_priority_and_real_candidate_submission(self):
        from operations.maintenance_agent.auto_v1 import bridge
        import base64
        self.inbox=self.inbox.resolve();self.state=self.state.resolve();self.source=self.source.resolve()
        content=b'VALUE=1\n';entry={'profile_id':'probe','fingerprint':'a'*64,'snapshot_hash':'b'*64,'attempt_key':'c'*64,'sources':{'probe.py':{'sha256':hashlib.sha256(content).hexdigest(),'content':base64.b64encode(content).decode()}},'tests':['test_probe']}
        p=self.inbox.with_name('auto-repair-catalog.json');p.write_text(json.dumps({'schema_version':1,'as_of':self.now.isoformat(),'incidents':[entry]}));p.chmod(0o644)
        def launch(binary,model,effort,state,home,workspace,schema,**kwargs):
            self.assertEqual(workspace.name,'a'*64)
            self.assertEqual((workspace/'repair_target/probe.py').read_bytes(),content)
            (workspace/'repair_target/probe.py').write_text('VALUE=2\n')
            return {'status':'completed','result':{'diagnosis':'real cause','change_paths':['repair_target/probe.py'],'test_results':['regression passes'],'escalation':''}}
        with mock.patch.object(bridge,'TRUSTED_UID',os.getuid()):self.assertEqual(self.invoke(launcher=launch)['status'],'completed')
        proposal=json.loads((self.state/'runs'/('a'*64)/'auto-proposal.json').read_text())
        self.assertEqual(set(proposal['files']),{'probe.py'})
        self.assertEqual(proposal['attempt_key'],'c'*64)

    def test_idle_requires_verified_incident_clearance_without_new_diagnosis(self):
        self.invoke()
        pending = self.invoke(now=self.now + timedelta(seconds=2))
        self.assertEqual(pending['status'], 'awaiting_remediation')
        self.assertIn('1 unresolved incident', pending['detail'])
        self.write_inbox(incidents=[], as_of=self.now + timedelta(seconds=3))
        cleared = self.invoke(now=self.now + timedelta(seconds=3))
        self.assertEqual(cleared['status'], 'idle')
        self.assertNotIn('detail', cleared)
        self.assertEqual(cleared['calls_used_today'], 1)
        self.assertEqual(len(self.launch_calls), 1)

    def test_diagnosing_status_and_call_count_are_published_before_launcher(self):
        observed = {}

        def launcher(*args, **kwargs):
            observed.update(json.loads((self.state / "status.json").read_text()))
            claims = json.loads((self.state / "claims.json").read_text())
            observed["durable_calls"] = claims["calls_used"]
            observed["durable_claim_status"] = claims["claims"][args[5].name]["status"]
            observed["entered_at"] = datetime.now(UTC)
            return {"status": "completed", "result": {"diagnosis": "cause", "change_paths": [],
                    "test_results": [], "escalation": "review"}}

        report = self.invoke(launcher=launcher)
        self.assertEqual(observed["status"], "diagnosing")
        self.assertEqual(observed["calls_used_today"], 1)
        self.assertEqual(observed["durable_calls"], 1)
        self.assertEqual(observed["durable_claim_status"], "claimed_before_exec")
        self.assertEqual(report["status"], "completed")
        self.assertGreaterEqual(datetime.fromisoformat(report["updated_at"].replace("Z", "+00:00")),
                                observed["entered_at"])

    def test_missing_auth_does_not_claim_and_waits_before_rechecking(self):
        report = self.invoke(auth=self.auth("authentication_required"))
        self.assertEqual(report["status"], "authentication_required")
        self.assertFalse((self.state / "claims.json").exists() and json.loads((self.state / "claims.json").read_text())["claims"])
        cached = self.invoke(now=self.now + timedelta(seconds=10), auth=self.auth("authenticated"))
        self.assertEqual(cached["status"], "authentication_required")
        self.assertEqual(len(self.launch_calls), 0)
        resumed = self.invoke(now=self.now + timedelta(seconds=61), auth=self.auth("authenticated"))
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(len(self.launch_calls), 1)

    def test_sandbox_failure_does_not_claim_or_consume_daily_budget(self):
        report = self.invoke(preflight=self.preflight("command_runner_unavailable"))
        self.assertEqual(report["status"], "command_runner_unavailable")
        self.assertEqual(report["error_class"], "command_runner_unavailable")
        self.assertEqual(report["calls_used_today"], 0)
        state = json.loads((self.state / "claims.json").read_text())
        self.assertEqual(state["calls_used"], 0)
        self.assertEqual(state["claims"], {})
        self.assertEqual(self.launch_calls, [])
        self.assertEqual(len(self.preflight_calls), 1)

    def test_sandbox_preflight_runs_before_incident_claim_with_minimal_env(self):
        def check(binary, env, cwd):
            self.preflight_calls.append((binary, dict(env), cwd))
            claims_path = self.state / "claims.json"
            if claims_path.exists():
                claims = json.loads(claims_path.read_text())
                self.assertEqual(claims["claims"], {})
                self.assertEqual(claims["calls_used"], 0)
            self.assertNotIn("OPENAI_API_KEY", env)
            self.assertNotIn("ANTHROPIC_API_KEY", env)
            self.assertEqual(cwd, self.state / "sandbox-preflight")
            return {"status": "ready"}
        report = self.invoke(preflight=check)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(len(self.preflight_calls), 1)

    def test_sandbox_preflight_cache_expires_after_at_most_sixty_seconds(self):
        calls = []
        def check(*_args):
            calls.append(1)
            return {"status": "ready"}
        env = {"PATH": "/usr/bin:/bin", "HOME": str(self.root), "CODEX_HOME": str(self.root / ".codex")}
        cwd = self.root / "private"
        cwd.mkdir()
        first = worker._cached_sandbox_preflight("codex", env, cwd, check=check, now_monotonic=100)
        cached = worker._cached_sandbox_preflight("codex", env, cwd, check=check, now_monotonic=159.9)
        expired = worker._cached_sandbox_preflight("codex", env, cwd, check=check, now_monotonic=160)
        self.assertEqual((first, cached, expired), ({"status": "ready"},) * 3)
        self.assertEqual(len(calls), 2)

    def test_sandbox_preflight_uses_supported_linux_smoke_command_and_discards_output(self):
        process = mock.Mock()
        process.wait.return_value = 0
        process.poll.return_value = 0
        env = {"PATH": "/usr/bin:/bin", "HOME": str(self.root), "CODEX_HOME": str(self.root / ".codex")}
        executable = self.root / "codex"
        executable.write_text("fixture")
        executable.chmod(0o700)
        with mock.patch.object(worker.subprocess, "Popen", return_value=process) as popen:
            report = worker._sandbox_preflight(str(executable), env, self.root)
        self.assertEqual(report, {"status": "ready"})
        args, kwargs = popen.call_args
        self.assertEqual(args[0], [str(executable), "sandbox", *worker.permission_flags(write=True), "--", "/bin/true"])
        self.assertEqual(kwargs["cwd"], self.root)
        self.assertEqual(kwargs["env"], env)
        self.assertEqual(kwargs["stdout"], worker.subprocess.DEVNULL)
        self.assertEqual(kwargs["stderr"], worker.subprocess.DEVNULL)
        process.wait.assert_called_once_with(timeout=worker.SANDBOX_PREFLIGHT_TIMEOUT_SECONDS)

    def test_actual_exec_forces_safe_fixed_shell_path(self):
        argv = worker._codex_exec_argv("/opt/neorome-codex/codex", "gpt-6-luna", "high",
                                       self.root / "result.json", self.root, self.root / "schema.json")
        self.assertIn("shell_environment_policy.inherit=none", argv)
        self.assertIn('shell_environment_policy.set.PATH="/usr/bin:/bin"', argv)
        self.assertNotIn("OPENAI_API_KEY", argv)

    def test_stale_input_never_launches(self):
        self.write_inbox(as_of=self.now - timedelta(seconds=181))
        report = self.invoke()
        self.assertEqual(report["status"], "stale_or_invalid_input")
        self.assertEqual(self.launch_calls, [])

    def test_malformed_input_never_launches(self):
        self.inbox.write_text('{"schema_version":1,"incidents":[{"key":"x","reason":"y"}]}')
        report = self.invoke()
        self.assertEqual(report["status"], "stale_or_invalid_input")
        self.assertEqual(self.launch_calls, [])

    def test_systemd_instance_key_does_not_block_other_incidents(self):
        entries = [
            {"key": "unit:neorome-bcp3-child-freshness-r2@11-978750-997.service", "reason": "unit_failed"},
            {"key": "outcome:options_stochastic_completion", "reason": "strategy_cycle_failed"},
        ]
        self.write_inbox(incidents=entries)
        _value, projected = worker._incident_projection(self.inbox, self.now, max_age_seconds=180)
        self.assertEqual({row['key'] for row in projected}, {row['key'] for row in entries})
        for key in ("unit:bad/name.service", "unit:bad;command", "unit:bad$(command)", "unit:bad\nname"):
            self.write_inbox(incidents=[{"key": key, "reason": "unit_failed"}])
            with self.assertRaises(worker.WorkerError):
                worker._incident_projection(self.inbox, self.now, max_age_seconds=180)

    def test_diagnosis_has_no_daily_cap_and_preserves_accounting_and_dedup(self):
        for initial_count in (64, 10000):
            with self.subTest(initial_count=initial_count):
                self.state.mkdir(exist_ok=True)
                (self.state / "claims.json").write_text(json.dumps({
                    "schema_version": 1, "utc_day": self.now.date().isoformat(),
                    "calls_used": initial_count, "claims": {},
                }))
                launches = []

                def launcher(*args, **kwargs):
                    saved = json.loads((self.state / "claims.json").read_text())
                    self.assertEqual(saved["calls_used"], initial_count + 1)
                    self.assertEqual(len(saved["claims"]), 1)
                    launches.append(True)
                    return {"status": "completed"}

                report = self.invoke(launcher=launcher)
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["calls_used_today"], initial_count + 1)
                self.assertIsNone(report["daily_call_limit"])
                repeat = self.invoke(launcher=launcher)
                self.assertEqual(repeat["status"], "awaiting_remediation")
                self.assertEqual(repeat["calls_used_today"], initial_count + 1)
                self.assertEqual(len(launches), 1)

    def test_timeout_claim_is_not_retried(self):
        report = self.invoke(launcher=self.launcher("timeout"))
        self.assertEqual(report["status"], "timeout")
        repeat = self.invoke(now=self.now + timedelta(seconds=3))
        self.assertEqual(repeat["status"], "awaiting_remediation")
        self.assertEqual(len(self.launch_calls), 1)

    def test_new_source_package_version_is_a_new_fingerprint(self):
        self.invoke()
        self.write_inbox(package_sha="b" * 64)
        report = self.invoke(now=self.now + timedelta(seconds=2))
        self.assertEqual(report["status"], "completed")
        state = json.loads((self.state / "claims.json").read_text())
        self.assertEqual(len(state["claims"]), 2)
        self.assertEqual(state["calls_used"], 2)

    def test_same_episode_ignores_source_version_but_reopened_episode_is_new(self):
        first_episode = "1" * 64
        second_episode = "2" * 64
        self.write_inbox(incidents=[{"key": "unit:neorome-health.service",
                                    "reason": "unit_failed", "episode_id": first_episode}])
        self.assertEqual(self.invoke()["status"], "completed")
        self.write_inbox(package_sha="b" * 64, incidents=[{"key": "unit:neorome-health.service",
                                    "reason": "unit_failed", "episode_id": first_episode}],
                         as_of=self.now + timedelta(seconds=2))
        self.assertEqual(self.invoke(now=self.now + timedelta(seconds=2))["status"], "awaiting_remediation")
        self.assertEqual(len(self.launch_calls), 1)

        self.write_inbox(package_sha="c" * 64, incidents=[{"key": "unit:neorome-health.service",
                                    "reason": "unit_failed", "episode_id": second_episode}],
                         as_of=self.now + timedelta(seconds=4))
        self.assertEqual(self.invoke(now=self.now + timedelta(seconds=4))["status"], "completed")
        self.assertEqual(len(self.launch_calls), 2)
        claims = json.loads((self.state / "claims.json").read_text())["claims"]
        self.assertEqual(len(claims), 2)
        self.assertEqual({claim["episode_id"] for claim in claims.values()}, {first_episode, second_episode})

    def test_malformed_episode_is_rejected_without_launch(self):
        for episode_id in ("not-a-sha", None, "G" * 64):
            with self.subTest(episode_id=episode_id):
                self.write_inbox(incidents=[{"key": "unit:neorome-health.service",
                                            "reason": "unit_failed", "episode_id": episode_id}])
                report = self.invoke()
                self.assertEqual(report["status"], "stale_or_invalid_input")
        self.assertEqual(self.launch_calls, [])

    def test_source_copy_excludes_credentials_and_runtime_databases(self):
        (self.source / ".env.production").write_text("SECRET=ignored")
        (self.source / "private.key").write_text("ignored")
        (self.source / "runtime.sqlite3").write_text("ignored")
        (self.source / "vendor").mkdir()
        (self.source / "vendor" / "library.py").write_text("third party")
        (self.source / "runtime.log").write_text("runtime output")
        (self.source / "docs").mkdir()
        (self.source / "docs" / "CLOUD_VALIDATION_SCHEDULER.md").write_text("approved public doc")
        (self.source / "docs" / "unreviewed.md").write_text("not approved")
        (self.source / "arbitrary-tools").mkdir()
        (self.source / "arbitrary-tools" / "script.py").write_text("not copied")
        report = self.invoke()
        self.assertEqual(report["status"], "completed")
        workspace = self.launch_calls[0][3]
        self.assertTrue((workspace / "neorome" / "module.py").exists())
        self.assertFalse((workspace / ".env.production").exists())
        self.assertFalse((workspace / "private.key").exists())
        self.assertFalse((workspace / "runtime.sqlite3").exists())
        self.assertFalse((workspace / "runtime.log").exists())
        self.assertFalse((workspace / "vendor").exists())
        self.assertFalse((workspace / "arbitrary-tools").exists())
        self.assertTrue((workspace / "docs" / "CLOUD_VALIDATION_SCHEDULER.md").exists())
        self.assertFalse((workspace / "docs" / "unreviewed.md").exists())

    def test_nonincident_workspace_children_do_not_consume_hot_slots(self):
        workspace_root = self.state / "workspaces"
        workspace_root.mkdir(parents=True)
        active_fingerprint = "c" * 64
        (workspace_root / active_fingerprint).mkdir()
        (workspace_root / "codex-remote").mkdir()
        (workspace_root / "probe.tmp").write_text("probe")
        (workspace_root / ("d" * 64)).write_text("not a directory")
        (workspace_root / ("e" * 64)).symlink_to(self.source, target_is_directory=True)
        self.assertEqual(
            worker._hot_workspace_entries(workspace_root, workspace_root / ("f" * 64)),
            [workspace_root / active_fingerprint],
        )
        with mock.patch.object(worker, "MAX_WORKSPACES", 2):
            report = self.invoke()
        self.assertEqual(report["status"], "completed")
        self.assertEqual(len(self.launch_calls), 1)
        self.assertTrue((workspace_root / "codex-remote").is_dir())
        self.assertEqual((workspace_root / "probe.tmp").read_text(), "probe")

    def test_closed_workspace_is_archived_with_integrity_before_hot_capacity_reuse(self):
        workspace_root = self.state / "workspaces"
        workspace_root.mkdir(parents=True)
        fingerprints = ["1" * 64, "2" * 64]
        for index, fingerprint in enumerate(fingerprints):
            folder = workspace_root / fingerprint
            (folder / "neorome").mkdir(parents=True)
            (folder / "neorome" / "fix.py").write_text(f"value = {index}\n")
            (folder / "review.json").write_text(json.dumps({"diagnosis": f"kept-{index}"}))
        foreign = workspace_root / "codex-remote"
        foreign.mkdir()
        (foreign / "keep.txt").write_text("untouched")
        claims = {
            "schema_version": 1, "utc_day": self.now.date().isoformat(), "calls_used": 0,
            "claims": {
                fingerprints[0]: {"status": "completed", "finished_at": "2026-10-14T13:00:00Z"},
                fingerprints[1]: {"status": "codex_failed", "finished_at": "2026-10-14T13:30:00Z"},
            },
        }
        self.state.mkdir(exist_ok=True)
        (self.state / "claims.json").write_text(json.dumps(claims))

        with mock.patch.object(worker, "MAX_WORKSPACES", 2):
            report = self.invoke()
        self.assertEqual(report["status"], "completed")
        archive_root = self.state / "runs" / "archive"
        archive_path = archive_root / f"{fingerprints[0]}.tar.gz"
        receipt_path = archive_root / f"{fingerprints[0]}.sha256.json"
        receipt = json.loads(receipt_path.read_text())
        archive_bytes = archive_path.read_bytes()
        self.assertEqual(receipt["sha256"], hashlib.sha256(archive_bytes).hexdigest())
        self.assertEqual(receipt["size_bytes"], len(archive_bytes))
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
            members = set(archive.getnames())
            self.assertIn(f"{fingerprints[0]}/neorome/fix.py", members)
            self.assertIn(f"{fingerprints[0]}/review.json", members)
            self.assertEqual(archive.extractfile(f"{fingerprints[0]}/review.json").read(),
                             b'{"diagnosis": "kept-0"}')
        self.assertFalse((workspace_root / fingerprints[0]).exists())
        self.assertTrue((workspace_root / fingerprints[1]).is_dir())
        self.assertTrue((foreign / "keep.txt").is_file())
        saved = json.loads((self.state / "claims.json").read_text())
        self.assertEqual(saved["calls_used"], 1)
        self.assertEqual(set(saved["claims"]), set(fingerprints + [self.launch_calls[0][3].name]))

    def test_byte_pressure_archives_closed_workspaces_and_preserves_foreign_children(self):
        workspace_root = self.state / "workspaces"
        workspace_root.mkdir(parents=True)
        fingerprint = "5" * 64
        folder = workspace_root / fingerprint
        folder.mkdir()
        (folder / "large-review.txt").write_text("r" * 900)
        foreign = workspace_root / "operator-project"
        foreign.mkdir()
        (foreign / "probe.txt").write_text("keep this project")
        claims = {"schema_version": 1, "utc_day": self.now.date().isoformat(), "calls_used": 0,
                  "claims": {fingerprint: {"status": "completed", "finished_at": "2026-10-14T13:00:00Z"}}}
        self.state.mkdir(exist_ok=True)
        (self.state / "claims.json").write_text(json.dumps(claims))

        before = worker._workspace_children_bytes(workspace_root, workspace_root / ("f" * 64))
        # Directory inode sizes differ across Linux and macOS; reserve actual fixture metadata.
        headroom = 64 * 1024
        with (mock.patch.object(worker, "MAX_WORKSPACE_TOTAL_BYTES", before + headroom - 512),
              mock.patch.object(worker, "MAX_WORKSPACE_PREP_HEADROOM", headroom)):
            before = worker._workspace_children_bytes(workspace_root, workspace_root / ("f" * 64))
            self.assertGreater(before + worker.MAX_WORKSPACE_PREP_HEADROOM,
                               worker.MAX_WORKSPACE_TOTAL_BYTES)
            report = self.invoke()

        self.assertEqual(report["status"], "completed")
        self.assertFalse(folder.exists())
        archive_path = self.state / "runs" / "archive" / f"{fingerprint}.tar.gz"
        self.assertTrue(archive_path.is_file())
        with tarfile.open(archive_path, "r:gz") as archive:
            self.assertEqual(archive.extractfile(f"{fingerprint}/large-review.txt").read(), b"r" * 900)
        self.assertEqual((foreign / "probe.txt").read_text(), "keep this project")
        saved = json.loads((self.state / "claims.json").read_text())
        self.assertIn(fingerprint, saved["claims"])
        self.assertEqual(saved["calls_used"], 1)

    def test_archive_count_does_not_limit_diagnosis_but_byte_budget_still_applies(self):
        runs = self.root / "runs"
        archive_root = runs / "archive"
        archive_root.mkdir(parents=True)
        for index in range(128):
            (archive_root / f"{index:064x}.tar.gz").write_bytes(b"existing-history")
        fingerprint = "f" * 64
        folder = self.root / "workspaces" / fingerprint
        folder.mkdir(parents=True)
        (folder / "review.json").write_text('{"diagnosis":"retain"}')
        claim = {"status": "completed", "finished_at": "2026-10-14T13:00:00Z"}
        with mock.patch.object(worker, "MAX_COLD_ARCHIVE_BYTES", 1):
            self.assertFalse(worker._archive_closed_workspace(fingerprint, folder, claim, runs))
        self.assertTrue(folder.is_dir())
        self.assertTrue(worker._archive_closed_workspace(fingerprint, folder, claim, runs))
        self.assertEqual(len(list(archive_root.glob("*.tar.gz"))), 129)
        self.assertEqual((archive_root / f"{0:064x}.tar.gz").read_bytes(), b"existing-history")
        receipt = json.loads((archive_root / f"{fingerprint}.sha256.json").read_text())
        self.assertEqual(receipt["sha256"], hashlib.sha256(
            (archive_root / f"{fingerprint}.tar.gz").read_bytes()).hexdigest())
        self.assertFalse(folder.exists())

    def test_symlinked_closed_workspace_is_not_archived_or_removed(self):
        workspace_root = self.state / "workspaces"
        workspace_root.mkdir(parents=True)
        fingerprint = "3" * 64
        folder = workspace_root / fingerprint
        folder.mkdir()
        (folder / "outside").symlink_to(self.source, target_is_directory=True)
        claims = {"schema_version": 1, "utc_day": self.now.date().isoformat(), "calls_used": 1,
                  "claims": {fingerprint: {"status": "completed", "finished_at": "2026-10-14T13:00:00Z"}}}
        self.state.mkdir(exist_ok=True)
        (self.state / "claims.json").write_text(json.dumps(claims))
        with mock.patch.object(worker, "MAX_WORKSPACES", 1):
            report = self.invoke()
        self.assertEqual(report["status"], "workspace_capacity_exhausted")
        self.assertTrue(folder.is_dir())
        self.assertTrue((folder / "outside").is_symlink())
        self.assertFalse((self.state / "runs" / "archive" / f"{fingerprint}.tar.gz").exists())
        self.assertEqual(self.launch_calls, [])

    def test_unclaimed_or_active_workspace_is_never_archived(self):
        workspace_root = self.state / "workspaces"
        workspace_root.mkdir(parents=True)
        fingerprint = "4" * 64
        folder = workspace_root / fingerprint
        folder.mkdir()
        (folder / "keep.py").write_text("still active\n")
        claims = {"schema_version": 1, "utc_day": self.now.date().isoformat(), "calls_used": 0,
                  "claims": {fingerprint: {"status": "claimed_before_exec"}}}
        self.state.mkdir(exist_ok=True)
        (self.state / "claims.json").write_text(json.dumps(claims))
        with mock.patch.object(worker, "MAX_WORKSPACES", 1):
            report = self.invoke()
        self.assertEqual(report["status"], "workspace_capacity_exhausted")
        self.assertEqual((folder / "keep.py").read_text(), "still active\n")
        self.assertFalse((self.state / "runs" / "archive" / f"{fingerprint}.tar.gz").exists())
        self.assertEqual(self.launch_calls, [])

    def test_journal_emits_transitions_and_one_minute_heartbeat_only(self):
        same = {"status": "authentication_required", "detail": None,
                "updated_at": "first", "calls_used_today": 0}
        self.assertTrue(worker._should_emit_report(same, None, 0, 1))
        signature = worker._report_signature(same)
        refreshed = dict(same, updated_at="second")
        self.assertFalse(worker._should_emit_report(refreshed, signature, 1, 30))
        self.assertTrue(worker._should_emit_report(refreshed, signature, 1, 61))
        self.assertTrue(worker._should_emit_report(dict(same, status="completed"), signature, 1, 2))


if __name__ == "__main__":
    unittest.main()
