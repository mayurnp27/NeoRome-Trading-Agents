from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from neorome.control_plane_store import ControlPlaneStore
from neorome.control_plane_sync import run, sync_desk_readiness, sync_status


NOW = datetime(2026, 9, 22, 20, 0, tzinfo=timezone.utc)


class ControlPlaneSyncTests(unittest.TestCase):
    def _root_owned_stat(self):
        original = Path.stat

        def stat(path, *args, **kwargs):
            value = original(path, *args, **kwargs)
            return SimpleNamespace(st_mode=value.st_mode, st_uid=0)
        return patch("neorome.control_plane_sync.Path.stat", autospec=True, side_effect=stat)

    def _projection(self, *, as_of=None, status="active"):
        stamp = as_of or NOW.isoformat().replace("+00:00", "Z")
        return {"schema_version": 1, "as_of": stamp,
                "desks": {desk: {"status": status, "entry_eligibility": "unknown",
                                  "profiles": {"paper": {"operational_state": "armed_for_session",
                                                             "entry_eligibility": "unknown"}}}
                          for desk in ("equities", "options", "prediction", "crypto")}}

    def test_readiness_overlay_preserves_legacy_block_as_detail_and_unknown_eligibility(self):
        class Store:
            events = []
            def record_desk_state(self, event):
                self.events.append(event)

        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "desk-readiness.json"
            source.write_text(json.dumps(self._projection()))
            store = Store()
            with self._root_owned_stat():
                result = sync_desk_readiness(store, path=source, now=NOW,
                    legacy_statuses={"options": "blocked_freshness"},
                    legacy_details={"weather_calibration": "blocked", "options_full_port_enabled": False})
            self.assertEqual(result["status"], "applied")
            options = next(event for event in store.events if event.desk == "options")
            self.assertEqual(options.data["status"], "active")
            self.assertEqual(options.data["details"]["legacy_status"], "blocked_freshness")
            self.assertEqual(options.data["details"]["entry_eligibility"], "unknown")
            self.assertEqual(options.data["details"]["legacy_gates"],
                             {"weather_calibration": "blocked", "options_full_port_enabled": False})

    def test_stale_malformed_or_non_root_projection_is_ignored(self):
        class Store:
            events = []
            def record_desk_state(self, event):
                self.events.append(event)

        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "desk-readiness.json"
            store = Store()
            for payload, expected in ((self._projection(as_of=(NOW - timedelta(seconds=46)).isoformat()), "stale"),
                                      ({"schema_version": 1, "as_of": NOW.isoformat(), "desks": {}}, "invalid")):
                source.write_text(json.dumps(payload))
                with self._root_owned_stat():
                    result = sync_desk_readiness(store, path=source, now=NOW)
                self.assertEqual(result["reason"], expected)
            source.write_text(json.dumps(self._projection()))
            with patch("neorome.control_plane_sync.Path.stat", autospec=True,
                       side_effect=lambda path, *args, **kwargs: SimpleNamespace(st_mode=0o100600, st_uid=501)):
                result = sync_desk_readiness(store, path=source, now=NOW)
            self.assertEqual(result["reason"], "wrong_owner")
            self.assertEqual(store.events, [])

    def test_missing_and_malformed_projection_are_ignored(self):
        class Store:
            events = []
            def record_desk_state(self, event):
                self.events.append(event)

        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "desk-readiness.json"
            store = Store()
            self.assertEqual(sync_desk_readiness(store, path=source, now=NOW)["reason"], "missing")
            source.write_text("not-json")
            with self._root_owned_stat():
                self.assertEqual(sync_desk_readiness(store, path=source, now=NOW)["reason"], "unreadable")
            self.assertEqual(store.events, [])

    def test_unknown_projection_does_not_replace_legacy_block(self):
        class Store:
            events = []
            def record_desk_state(self, event):
                self.events.append(event)

        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "desk-readiness.json"
            source.write_text(json.dumps(self._projection(status="unknown")))
            store = Store()
            with self._root_owned_stat():
                result = sync_desk_readiness(store, path=source, now=NOW,
                    legacy_statuses={"options": "blocked_freshness"})
            self.assertEqual(result["desks"], [])
            self.assertEqual(store.events, [])

    def test_actual_store_overlay_falls_back_on_stale_projection_and_restores(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "desk-readiness.json"
            database = root / "control.sqlite3"
            source.write_text(json.dumps(self._projection()))
            with ControlPlaneStore(database) as store:
                with self._root_owned_stat():
                    first = sync_desk_readiness(store, path=source, now=NOW)
                    self.assertEqual(first["desks"], ["crypto", "equities", "options", "prediction"])
                    self.assertEqual(store.get_desk("options")["status"], "active")

                    source.write_text(json.dumps(self._projection(
                        as_of=(NOW - timedelta(seconds=46)).isoformat().replace("+00:00", "Z"))))
                    stale = sync_desk_readiness(store, path=source, now=NOW,
                        legacy_statuses={"options": "blocked_freshness", "equities": "active"})
                    self.assertEqual(stale["reason"], "stale")
                    options = store.get_desk("options")
                    self.assertEqual(options["status"], "blocked_freshness")
                    self.assertEqual(options["details"]["entry_eligibility"], "unknown")
                    self.assertEqual(options["details"]["readiness_projection"], "unavailable")
                    self.assertEqual(options["details"]["read_status"], "stale")
                    self.assertEqual(options["details"]["legacy_status"], "blocked_freshness")
                    self.assertEqual(store.get_desk("equities")["status"], "unknown")

                    source.write_text(json.dumps(self._projection(
                        as_of=(NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"))))
                    restored = sync_desk_readiness(store, path=source, now=NOW + timedelta(seconds=1))
                    self.assertEqual(restored["desks"], ["crypto", "equities", "options", "prediction"])
                    self.assertEqual(store.get_desk("options")["status"], "active")
                    self.assertEqual(store.get_desk("options")["details"]["readiness_projection"], "validated")

                    source.write_text(json.dumps(self._projection(status="unknown",
                        as_of=(NOW + timedelta(seconds=2)).isoformat().replace("+00:00", "Z"))))
                    unknown = sync_desk_readiness(store, path=source, now=NOW + timedelta(seconds=2),
                        legacy_statuses={"options": "blocked_freshness"})
                    self.assertIn("options", unknown["fallback_desks"])
                    self.assertEqual(store.get_desk("options")["status"], "blocked_freshness")
                    self.assertEqual(store.get_desk("options")["details"]["read_status"], "desk_unknown")
                    source.write_text(json.dumps(self._projection(
                        as_of=(NOW + timedelta(seconds=3)).isoformat().replace("+00:00", "Z"))))
                    restored_again = sync_desk_readiness(store, path=source, now=NOW + timedelta(seconds=3))
                    self.assertEqual(restored_again["desks"], ["crypto", "equities", "options", "prediction"])
                    self.assertEqual(store.get_desk("options")["status"], "active")

    def test_legacy_heartbeat_cannot_erase_marker_before_stale_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database = root / 'control.sqlite3'
            source = root / 'readiness.json'
            runtime = root / 'runtime.json'
            source.write_text(json.dumps(self._projection()))
            with ControlPlaneStore(database) as store, self._root_owned_stat():
                sync_desk_readiness(store, path=source, now=NOW)
            later = NOW + timedelta(seconds=301)
            runtime.write_text(json.dumps({'updated_at': later.isoformat(), 'operational_healthy': True,
                                          'desk_status': {'crypto':'active','options':'blocked_freshness'}}))
            with self._root_owned_stat():
                run(database=str(database), statuses={'paper_runtime':str(runtime)}, now=later, desk_readiness=source)
            with ControlPlaneStore(database) as store:
                self.assertEqual(store.get_desk('crypto')['status'], 'unknown')
                self.assertEqual(store.get_desk('options')['status'], 'blocked_freshness')
                self.assertEqual(store.get_desk('crypto')['details']['readiness_projection'], 'unavailable')

    def test_invalid_projection_does_not_overwrite_unrelated_legacy_desk(self):
        from neorome.control_plane_store import NeoRomeEvent

        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / "desk-readiness.json"
            source.write_text("not-json")
            with ControlPlaneStore(root / "control.sqlite3") as store:
                store.record_desk_state(NeoRomeEvent.create(
                    event_id="00000000-0000-4000-8000-000000000001", timestamp=NOW.isoformat().replace("+00:00", "Z"),
                    desk="options", event_type="DESK_HEARTBEAT", severity="info",
                    summary="legacy options state", source="legacy.runtime", correlation_id="desk:options",
                    idempotency_key="legacy-options", data={"status": "blocked_freshness", "details": {"runtime_status": "ready"}},
                ))
                with self._root_owned_stat():
                    result = sync_desk_readiness(store, path=source, now=NOW,
                        legacy_statuses={"options": "active"})
                self.assertEqual(result["status"], "ignored")
                options = store.get_desk("options")
                self.assertEqual(options["status"], "blocked_freshness")
                self.assertEqual(options["details"], {"runtime_status": "ready"})

    def test_failure_and_restore_are_durable_and_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            database = root / "control.sqlite3"
            status = root / "recovery.json"
            status.write_text(json.dumps({"updated_at": NOW.isoformat(), "operational_healthy": False}))
            with ControlPlaneStore(database) as store:
                first = sync_status(store, component="recovery_verification", path=status, now=NOW)
                again = sync_status(store, component="recovery_verification", path=status, now=NOW)
                self.assertEqual(first["event_id"], again["event_id"])
                self.assertEqual(len(store.list_events()), 2)  # readiness plus incident event
                self.assertEqual(store.get_incident("readiness:recovery_verification")["status"], "open")
            restored = datetime(2026, 9, 22, 20, 5, tzinfo=timezone.utc)
            status.write_text(json.dumps({"updated_at": restored.isoformat(), "operational_healthy": True}))
            with ControlPlaneStore(database) as store:
                result = sync_status(store, component="recovery_verification", path=status, now=restored)
                self.assertTrue(result["changed"])
                self.assertEqual(store.get_incident("readiness:recovery_verification")["status"], "resolved")
                self.assertEqual(len(store.list_events()), 4)

    def test_run_keeps_components_independent(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            good = root / "good.json"
            good.write_text(json.dumps({"updated_at": NOW.isoformat(), "operational_healthy": True}))
            result = run(database=str(root / "control.sqlite3"), statuses={
                "operations": str(good), "paper_runtime": str(root / "missing.json")
            }, now=NOW)
            self.assertFalse(result["healthy"])
            self.assertEqual([item["status"] for item in result["components"]], ["ready", "degraded"])

    def test_runtime_projects_all_desk_states(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            status = root / "runtime.json"
            status.write_text(json.dumps({
                "updated_at": NOW.isoformat(), "operational_healthy": True,
                "desk_status": {"equities": "active", "options": "blocked_freshness"},
            }))
            with ControlPlaneStore(root / "control.sqlite3") as store:
                sync_status(store, component="paper_runtime", path=status,
                            max_age_seconds=180, now=NOW)
                self.assertEqual({item["desk"]: item["status"] for item in store.list_desks()},
                                 {"equities": "active", "options": "blocked_freshness"})

    def test_unchanged_health_is_bounded_to_five_minute_heartbeats(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            status = root / "ops.json"
            status.write_text(json.dumps({"updated_at": NOW.isoformat(), "operational_healthy": True}))
            with ControlPlaneStore(root / "control.sqlite3") as store:
                first = sync_status(store, component="operations", path=status, now=NOW)
                one_minute = sync_status(store, component="operations", path=status,
                                         now=NOW + timedelta(minutes=1))
                self.assertEqual(first["event_id"], one_minute["event_id"])
                self.assertEqual(len(store.list_events()), 1)


if __name__ == "__main__":
    unittest.main()
