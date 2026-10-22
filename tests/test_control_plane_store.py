import datetime as dt
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path

from neorome.control_plane_store import (
    ControlPlaneStore,
    IdempotencyConflict,
    NeoRomeEvent,
    ValidationError,
)


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 22, 12, 0, tzinfo=UTC)


def event(event_type="DESK_HEARTBEAT", *, desk="equities", data=None, when=NOW,
          event_id=None, idempotency_key=None, severity="info"):
    return NeoRomeEvent.create(
        event_id=event_id or str(uuid.uuid4()), timestamp=when, desk=desk,
        event_type=event_type, severity=severity, summary=f"test {event_type}",
        source="test-suite", data=data or {}, idempotency_key=idempotency_key,
        correlation_id="decision-1",
    )


class ControlPlaneStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "control.sqlite3"
        self.store = ControlPlaneStore(self.path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_event_is_durable_filterable_append_only_and_replay_safe(self):
        item = event("SIGNAL_GENERATED", data={"symbol": "SPY"}, idempotency_key="signal-1")
        first = self.store.emit_event(item)
        second = self.store.emit_event(item)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(len(self.store.list_events(event_type="SIGNAL_GENERATED")), 1)
        self.store.close()
        self.store = ControlPlaneStore(self.path)
        self.assertEqual(self.store.get_event(item.event_id)["data"], {"symbol": "SPY"})
        with sqlite3.connect(self.path) as conn:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                conn.execute("DELETE FROM events")

    def test_event_idempotency_conflict_and_validation(self):
        identifier = str(uuid.uuid4())
        self.store.emit_event(event(data={"value": 1}, event_id=identifier))
        with self.assertRaises(IdempotencyConflict):
            self.store.emit_event(event(data={"value": 2}, event_id=identifier))
        with self.assertRaises(ValidationError):
            self.store.emit_event(event(severity="debug"))
        with self.assertRaises(ValidationError):
            self.store.emit_event(event(when="2026-09-22T12:00:00"))

    def test_current_state_projections_do_not_regress_on_late_event(self):
        latest = event(data={"status": "ready", "details": {"feed": "fresh"}},
                       when=NOW + dt.timedelta(minutes=2))
        late = event(data={"status": "degraded", "details": {"feed": "stale"}}, when=NOW)
        self.store.upsert_desk(latest)
        self.store.upsert_desk(late)
        self.assertEqual(self.store.get_desk("equities")["status"], "ready")
        self.assertEqual(len(self.store.list_events()), 2)

        health = event("READINESS_DEGRADED", desk="system",
                       data={"component": "market-data", "status": "degraded",
                             "details": {"reason": "stale"}}, severity="warning")
        self.store.upsert_system_health(health)
        self.assertEqual(self.store.list_system_health()[0]["details"]["reason"], "stale")

    def test_incident_history_and_resolution(self):
        opened = event("SYSTEM_ERROR", desk="system", severity="critical",
                       data={"incident_id": "INC-1", "status": "open", "details": {"service": "api"}})
        self.store.record_incident(opened)
        resolved = event("READINESS_RESTORED", desk="system", when=NOW + dt.timedelta(minutes=4),
                         data={"incident_id": "INC-1", "status": "resolved",
                               "opened_at": NOW.isoformat(), "details": {"service": "api"}})
        self.store.record_incident(resolved)
        incident = self.store.get_incident("INC-1")
        self.assertEqual(incident["status"], "resolved")
        self.assertIsNotNone(incident["resolved_at"])
        self.assertEqual(len(self.store.list_events(correlation_id="decision-1")), 2)

    def _draft(self, key="create"):
        return self.store.create_task(
            task_id="NR-201", title="Build queue", description="Persist the queue",
            acceptance_criteria=["survives restart"], priority="high", created_by="owner",
            source="test-suite", idempotency_key=key, now=NOW,
        )

    def test_task_approval_claim_heartbeat_completion_and_restart(self):
        self._draft()
        self.store.update_task(
            "NR-201", changes={"description": "Persist it safely"}, actor="owner",
            source="test-suite", idempotency_key="edit", now=NOW,
        )
        self.store.approve_task(
            "NR-201", approved_by="owner", source="test-suite",
            idempotency_key="approve", now=NOW,
        )
        claimed = self.store.claim_task(
            "NR-201", worker_id="MAC_MAIN_DESK", source="worker",
            idempotency_key="claim", lease_seconds=60, now=NOW,
        )
        token = claimed["lease_token"]
        active = self.store.heartbeat_task(
            "NR-201", lease_token=token, worker_id="MAC_MAIN_DESK", source="worker",
            idempotency_key="heartbeat", lease_seconds=60, now=NOW + dt.timedelta(seconds=10),
        )
        self.assertEqual(active["status"], "IN_PROGRESS")
        complete = self.store.complete_task(
            "NR-201", lease_token=token, worker_id="MAC_MAIN_DESK", source="worker",
            idempotency_key="complete", results={"outcome": "passed"}, artifacts=["report.json"],
            test_results={"passed": 4}, now=NOW + dt.timedelta(seconds=20),
        )
        self.assertEqual(complete["status"], "COMPLETED")
        self.assertIsNone(complete["lease_token"])
        self.store.close()
        self.store = ControlPlaneStore(self.path)
        self.assertEqual(self.store.get_task("NR-201")["results"]["outcome"], "passed")
        self.assertEqual(len(self.store.list_tasks(status="COMPLETED")), 1)

    def test_task_retries_and_expired_lease_recovery(self):
        first = self._draft()
        replay = self._draft()
        self.assertEqual(first, replay)
        with self.assertRaises(IdempotencyConflict):
            self.store.create_task(
                task_id="NR-201", title="Different", description="Persist the queue",
                acceptance_criteria=["survives restart"], priority="high", created_by="owner",
                source="test-suite", idempotency_key="create", now=NOW,
            )
        self.store.approve_task("NR-201", approved_by="owner", source="test-suite",
                                idempotency_key="approve", now=NOW)
        claim = self.store.claim_task("NR-201", worker_id="worker-a", source="worker",
                                      idempotency_key="claim-a", lease_seconds=30, now=NOW)
        with self.assertRaises(ValidationError):
            self.store.claim_task("NR-201", worker_id="worker-b", source="worker",
                                  idempotency_key="claim-too-soon", now=NOW + dt.timedelta(seconds=10))
        recovered = self.store.claim_task("NR-201", worker_id="worker-b", source="worker",
                                          idempotency_key="claim-b", now=NOW + dt.timedelta(seconds=31))
        self.assertNotEqual(claim["lease_token"], recovered["lease_token"])
        with self.assertRaises(ValidationError):
            self.store.complete_task(
                "NR-201", lease_token=claim["lease_token"], worker_id="worker-a", source="worker",
                idempotency_key="old-complete", results={}, now=NOW + dt.timedelta(seconds=32),
            )

    def test_worker_heartbeat_is_durable_replay_safe_and_task_linked(self):
        self._draft()
        worker = self.store.heartbeat_worker(
            worker_id="MAC_MAIN_DESK", hostname="neo-mac", environment="development",
            status="online", current_task="NR-201", capabilities=["codex", "tests"],
            source="worker", idempotency_key="worker-heartbeat-1", now=NOW,
        )
        replay = self.store.heartbeat_worker(
            worker_id="MAC_MAIN_DESK", hostname="neo-mac", environment="development",
            status="online", current_task="NR-201", capabilities=["codex", "tests"],
            source="worker", idempotency_key="worker-heartbeat-1", now=NOW + dt.timedelta(seconds=5),
        )
        self.assertEqual(worker, replay)
        self.assertEqual(self.store.get_worker("MAC_MAIN_DESK")["capabilities"], ["codex", "tests"])
        self.assertEqual(len(self.store.list_workers(status="online")), 1)
        with self.assertRaises(ValidationError):
            self.store.heartbeat_worker(
                worker_id="OTHER", hostname="host", environment="development", status="online",
                current_task="NR-missing", capabilities=[], source="worker",
                idempotency_key="worker-missing", now=NOW,
            )

    def test_report_survives_failed_delivery_and_replays(self):
        kwargs = dict(
            report_id="R-20260922-AM", report_type="morning", period_start=NOW,
            period_end=NOW + dt.timedelta(hours=1), structured={"firm": {"status": "ready"}},
            human_readable="NeoRome morning report", source="reporter", idempotency_key="report-am",
        )
        first = self.store.store_report(**kwargs, generated_at=NOW + dt.timedelta(hours=1))
        replay = self.store.store_report(**kwargs, generated_at=NOW + dt.timedelta(hours=2))
        self.assertEqual(first["report_id"], replay["report_id"])
        delivery = self.store.record_report_delivery(
            first["report_id"], attempt_id="attempt-1", channel="email", status="failed",
            source="delivery", idempotency_key="delivery-1", error_summary="provider unavailable",
            next_retry_at=NOW + dt.timedelta(minutes=5), attempted_at=NOW,
        )
        self.assertEqual(delivery["status"], "failed")
        self.assertEqual(self.store.get_report(first["report_id"])["human_readable"],
                         "NeoRome morning report")
        self.assertEqual(len(self.store.list_report_deliveries(first["report_id"])), 1)
        self.assertTrue(self.store.integrity_check())


if __name__ == "__main__":
    unittest.main()
