"""Durable event ledger and current-state projections for NeoRome.

This module is intentionally independent from trading and network code.  Events are
the authoritative record; the desks, system_health, and incidents tables are
query-friendly projections updated in the same SQLite transaction as their source
event.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


UTC = dt.timezone.utc
DATABASE_SCHEMA_VERSION = 1
EVENT_SCHEMA_VERSION = 1
MAX_JSON_BYTES = 1_000_000

EVENT_TYPES = frozenset(
    {
        "DESK_HEARTBEAT", "SIGNAL_GENERATED", "SIGNAL_REJECTED",
        "TRADE_PROPOSED", "TRADE_APPROVED", "TRADE_REJECTED",
        "ORDER_SUBMITTED", "ORDER_FILLED", "ORDER_CANCELLED",
        "POSITION_OPENED", "POSITION_UPDATED", "POSITION_CLOSED", "PNL_UPDATE",
        "RISK_ALERT", "MARKET_OBSERVATION", "RESEARCH_COMPLETED", "AGENT_STARTED",
        "AGENT_COMPLETED", "AGENT_FAILED", "TASK_CREATED", "TASK_UPDATED",
        "TASK_STARTED", "TASK_COMPLETED", "SYSTEM_ERROR", "SERVICE_RESTART",
        "DATA_FEED_STALE", "REPORT_GENERATED", "REPORT_DELIVERY_FAILED",
        "RECOVERY_VERIFICATION_FAILED", "RECOVERY_VERIFICATION_RESTORED",
        "TRADING_ENTRY_GATE_CLOSED", "TRADING_ENTRY_GATE_RESTORED",
        "READINESS_DEGRADED", "READINESS_RESTORED", "DEPLOYMENT_STARTED",
        "DEPLOYMENT_COMPLETED",
    }
)
SEVERITIES = frozenset({"info", "warning", "critical"})
TASK_STATUSES = frozenset({
    "DRAFT", "APPROVED", "QUEUED", "CLAIMED", "IN_PROGRESS", "BLOCKED",
    "REVIEW", "COMPLETED", "FAILED", "CANCELLED",
})
_SLUG = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_EVENT_TYPE = re.compile(r"^[A-Z][A-Z0-9_]{0,95}$")


class ControlPlaneStoreError(Exception):
    """Base class for expected store failures."""


class ValidationError(ControlPlaneStoreError, ValueError):
    """A record violates the canonical control-plane contract."""


class IdempotencyConflict(ControlPlaneStoreError):
    """An event identifier or idempotency key was reused for other content."""


class SchemaVersionError(ControlPlaneStoreError):
    """The database or record schema is not supported by this implementation."""


@dataclass(frozen=True)
class NeoRomeEvent:
    event_id: str
    timestamp: dt.datetime | str
    desk: str
    event_type: str
    severity: str
    summary: str
    source: str
    data: Mapping[str, Any] = field(default_factory=dict)
    agent_id: str | None = None
    correlation_id: str | None = None
    idempotency_key: str | None = None
    schema_version: int = EVENT_SCHEMA_VERSION

    @classmethod
    def create(
        cls,
        *,
        desk: str,
        event_type: str,
        severity: str,
        summary: str,
        source: str,
        data: Mapping[str, Any] | None = None,
        agent_id: str | None = None,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
        timestamp: dt.datetime | str | None = None,
        event_id: str | None = None,
    ) -> "NeoRomeEvent":
        return cls(
            event_id=event_id or str(uuid.uuid4()),
            timestamp=timestamp or dt.datetime.now(UTC),
            desk=desk,
            event_type=event_type,
            severity=severity,
            summary=summary,
            source=source,
            data=data or {},
            agent_id=agent_id,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
        )


def _text(value: Any, name: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    clean = value.strip()
    if len(clean) > maximum:
        raise ValidationError(f"{name} exceeds {maximum} characters")
    return clean


def _optional_text(value: Any, name: str, *, maximum: int = 4096) -> str | None:
    if value is None:
        return None
    return _text(value, name, maximum=maximum)


def _slug(value: Any, name: str) -> str:
    clean = _text(value, name, maximum=64).lower()
    if not _SLUG.fullmatch(clean):
        raise ValidationError(f"{name} must be a lowercase slug")
    return clean


def _iso_utc(value: dt.datetime | str, name: str) -> str:
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"{name} must be an ISO-8601 timestamp") from exc
    elif isinstance(value, dt.datetime):
        parsed = value
    else:
        raise ValidationError(f"{name} must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{name} must include a timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _stable_uuid(namespace: str, idempotency_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"https://neorome.local/{namespace}/{idempotency_key}"))


def _json(value: Any, name: str) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{name} must be finite JSON data") from exc
    if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
        raise ValidationError(f"{name} exceeds {MAX_JSON_BYTES} bytes")
    return encoded


def _normalize_event(event: NeoRomeEvent) -> dict[str, Any]:
    if not isinstance(event, NeoRomeEvent):
        raise ValidationError("event must be a NeoRomeEvent")
    event_id = _text(event.event_id, "event_id", maximum=128)
    try:
        uuid.UUID(event_id)
    except ValueError as exc:
        raise ValidationError("event_id must be a UUID") from exc
    if event.schema_version != EVENT_SCHEMA_VERSION:
        raise SchemaVersionError(f"unsupported event schema version {event.schema_version}")
    event_type = _text(event.event_type, "event_type", maximum=96)
    if not _EVENT_TYPE.fullmatch(event_type):
        raise ValidationError("event_type must be an uppercase identifier")
    severity = _text(event.severity, "severity", maximum=16).lower()
    if severity not in SEVERITIES:
        raise ValidationError(f"severity must be one of {sorted(SEVERITIES)}")
    data_json = _json(dict(event.data), "data")
    normalized = {
        "event_id": event_id,
        "timestamp": _iso_utc(event.timestamp, "timestamp"),
        "desk": _slug(event.desk, "desk"),
        "agent_id": _optional_text(event.agent_id, "agent_id", maximum=128),
        "event_type": event_type,
        "severity": severity,
        "summary": _text(event.summary, "summary", maximum=2000),
        "data_json": data_json,
        "correlation_id": _optional_text(event.correlation_id, "correlation_id", maximum=128),
        "source": _text(event.source, "source", maximum=256),
        "schema_version": event.schema_version,
        "idempotency_key": _optional_text(event.idempotency_key, "idempotency_key", maximum=256),
    }
    fingerprint_body = {key: value for key, value in normalized.items() if key != "idempotency_key"}
    normalized["fingerprint"] = hashlib.sha256(_json(fingerprint_body, "event").encode()).hexdigest()
    return normalized


class ControlPlaneStore:
    """SQLite-backed institutional-memory boundary.

    ``append_event`` stores arbitrary canonical events. Projection methods store the
    same event and update one current-state table atomically. Replaying the same
    event is a no-op; reusing its ID/key with different content is rejected.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._memory: sqlite3.Connection | None = None
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._transaction() as conn:
            self._create_schema(conn)
            self._verify_schema(conn)
        if str(path) != ":memory:" and self.path.exists():
            os.chmod(self.path, 0o600)

    def close(self) -> None:
        if self._memory is not None:
            self._memory.close()
            self._memory = None

    def __enter__(self) -> "ControlPlaneStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _connect(self) -> sqlite3.Connection:
        if str(self.path) == ":memory:":
            if self._memory is None:
                self._memory = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
                self._configure(self._memory)
            return self._memory
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
        self._configure(conn)
        return conn

    @staticmethod
    def _configure(conn: sqlite3.Connection) -> None:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=FULL")
        if conn.execute("PRAGMA database_list").fetchone()[2]:
            conn.execute("PRAGMA journal_mode=WAL")

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connect()
            close = conn is not self._memory
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                if close:
                    conn.close()

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS control_plane_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO control_plane_metadata(key,value)
                VALUES ('database_schema_version','1');

            CREATE TABLE IF NOT EXISTS events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                desk TEXT NOT NULL,
                agent_id TEXT,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL CHECK(severity IN ('info','warning','critical')),
                summary TEXT NOT NULL,
                data_json TEXT NOT NULL,
                correlation_id TEXT,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                idempotency_key TEXT UNIQUE,
                fingerprint TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_time_idx ON events(timestamp, sequence);
            CREATE INDEX IF NOT EXISTS events_desk_idx ON events(desk, timestamp);
            CREATE INDEX IF NOT EXISTS events_type_idx ON events(event_type, timestamp);
            CREATE INDEX IF NOT EXISTS events_correlation_idx ON events(correlation_id);

            CREATE TRIGGER IF NOT EXISTS events_no_update
            BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
            CREATE TRIGGER IF NOT EXISTS events_no_delete
            BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

            CREATE TABLE IF NOT EXISTS desks (
                desk TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                last_event_id TEXT NOT NULL REFERENCES events(event_id),
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS system_health (
                component TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                last_event_id TEXT NOT NULL REFERENCES events(event_id),
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                severity TEXT NOT NULL CHECK(severity IN ('info','warning','critical')),
                summary TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                resolved_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                correlation_id TEXT,
                last_event_id TEXT NOT NULL REFERENCES events(event_id),
                details_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS incidents_status_idx ON incidents(status, updated_at);

            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                acceptance_criteria_json TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                created_by TEXT NOT NULL,
                approved_by TEXT,
                assigned_to TEXT,
                dependencies_json TEXT NOT NULL,
                blocked_by_json TEXT NOT NULL,
                execution_environment TEXT,
                results_json TEXT,
                artifacts_json TEXT,
                git_commit TEXT,
                test_results_json TEXT,
                lease_token TEXT,
                lease_expires_at TEXT,
                last_heartbeat_at TEXT,
                last_event_id TEXT NOT NULL REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS tasks_queue_idx ON tasks(status, priority, created_at);
            CREATE TABLE IF NOT EXISTS task_updates (
                update_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id),
                operation TEXT NOT NULL,
                status TEXT NOT NULL,
                actor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                event_id TEXT NOT NULL REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS task_updates_task_idx ON task_updates(task_id, created_at);

            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                hostname TEXT NOT NULL,
                environment TEXT NOT NULL,
                status TEXT NOT NULL,
                current_task TEXT REFERENCES tasks(task_id),
                capabilities_json TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                last_event_id TEXT NOT NULL REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS workers_status_idx ON workers(status, last_seen);

            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                report_type TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                structured_json TEXT NOT NULL,
                human_readable TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                fingerprint TEXT NOT NULL,
                last_event_id TEXT NOT NULL REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS reports_generated_idx ON reports(generated_at, report_type);
            CREATE TABLE IF NOT EXISTS report_delivery_attempts (
                attempt_id TEXT PRIMARY KEY,
                report_id TEXT NOT NULL REFERENCES reports(report_id),
                channel TEXT NOT NULL,
                status TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                next_retry_at TEXT,
                error_summary TEXT,
                source TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                fingerprint TEXT NOT NULL,
                event_id TEXT NOT NULL REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS deliveries_report_idx
                ON report_delivery_attempts(report_id, attempted_at);
            """
        )

    @staticmethod
    def _verify_schema(conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT value FROM control_plane_metadata WHERE key='database_schema_version'"
        ).fetchone()
        if row is None or int(row[0]) != DATABASE_SCHEMA_VERSION:
            found = None if row is None else row[0]
            raise SchemaVersionError(f"unsupported database schema version {found}")

    def append_event(self, event: NeoRomeEvent) -> dict[str, Any]:
        normalized = _normalize_event(event)
        with self._transaction() as conn:
            stored, _ = self._append(conn, normalized)
        return stored

    emit_event = append_event

    def record_desk_state(self, event: NeoRomeEvent) -> dict[str, Any]:
        normalized = _normalize_event(event)
        data = json.loads(normalized["data_json"])
        status = _slug(data.get("status"), "data.status")
        details_json = _json(data.get("details", {}), "data.details")
        with self._transaction() as conn:
            _, inserted = self._append(conn, normalized)
            if inserted:
                conn.execute(
                    """INSERT INTO desks(desk,status,created_at,updated_at,source,schema_version,last_event_id,details_json)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT(desk) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at,
                         source=excluded.source, schema_version=excluded.schema_version,
                         last_event_id=excluded.last_event_id, details_json=excluded.details_json
                       WHERE excluded.updated_at >= desks.updated_at""",
                    (normalized["desk"], status, normalized["timestamp"], normalized["timestamp"],
                     normalized["source"], EVENT_SCHEMA_VERSION, normalized["event_id"], details_json),
                )
            return self._row_dict(conn.execute(
                "SELECT * FROM desks WHERE desk=?", (normalized["desk"],)
            ).fetchone())

    def record_system_health(self, event: NeoRomeEvent) -> dict[str, Any]:
        normalized = _normalize_event(event)
        data = json.loads(normalized["data_json"])
        component = _slug(data.get("component"), "data.component")
        status = _slug(data.get("status"), "data.status")
        details_json = _json(data.get("details", {}), "data.details")
        with self._transaction() as conn:
            _, inserted = self._append(conn, normalized)
            if inserted:
                conn.execute(
                    """INSERT INTO system_health(component,status,created_at,updated_at,source,schema_version,last_event_id,details_json)
                       VALUES (?,?,?,?,?,?,?,?)
                       ON CONFLICT(component) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at,
                         source=excluded.source, schema_version=excluded.schema_version,
                         last_event_id=excluded.last_event_id, details_json=excluded.details_json
                       WHERE excluded.updated_at >= system_health.updated_at""",
                    (component, status, normalized["timestamp"], normalized["timestamp"],
                     normalized["source"], EVENT_SCHEMA_VERSION, normalized["event_id"], details_json),
                )
            return self._row_dict(conn.execute(
                "SELECT * FROM system_health WHERE component=?", (component,)
            ).fetchone())

    upsert_desk = record_desk_state
    upsert_system_health = record_system_health

    def record_incident(self, event: NeoRomeEvent) -> dict[str, Any]:
        normalized = _normalize_event(event)
        data = json.loads(normalized["data_json"])
        incident_id = _text(data.get("incident_id"), "data.incident_id", maximum=128)
        status = _slug(data.get("status"), "data.status")
        opened_at = _iso_utc(data.get("opened_at", normalized["timestamp"]), "data.opened_at")
        resolved_raw = data.get("resolved_at")
        resolved_at = None if resolved_raw is None else _iso_utc(resolved_raw, "data.resolved_at")
        if status == "resolved" and resolved_at is None:
            resolved_at = normalized["timestamp"]
        if resolved_at is not None and resolved_at < opened_at:
            raise ValidationError("resolved_at cannot precede opened_at")
        details_json = _json(data.get("details", {}), "data.details")
        with self._transaction() as conn:
            _, inserted = self._append(conn, normalized)
            if inserted:
                conn.execute(
                    """INSERT INTO incidents(incident_id,status,severity,summary,opened_at,resolved_at,
                         created_at,updated_at,source,schema_version,correlation_id,last_event_id,details_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(incident_id) DO UPDATE SET status=excluded.status,
                         severity=excluded.severity, summary=excluded.summary,
                         resolved_at=excluded.resolved_at, updated_at=excluded.updated_at,
                         source=excluded.source, schema_version=excluded.schema_version,
                         correlation_id=excluded.correlation_id, last_event_id=excluded.last_event_id,
                         details_json=excluded.details_json
                       WHERE excluded.updated_at >= incidents.updated_at""",
                    (incident_id, status, normalized["severity"], normalized["summary"], opened_at,
                     resolved_at, normalized["timestamp"], normalized["timestamp"], normalized["source"],
                     EVENT_SCHEMA_VERSION, normalized["correlation_id"], normalized["event_id"], details_json),
                )
            return self._row_dict(conn.execute(
                "SELECT * FROM incidents WHERE incident_id=?", (incident_id,)
            ).fetchone())

    def create_task(
        self, *, task_id: str, title: str, description: str,
        acceptance_criteria: Sequence[str], priority: str, created_by: str,
        source: str, idempotency_key: str, dependencies: Sequence[str] = (),
        blocked_by: Sequence[str] = (), execution_environment: str | None = None,
        now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        timestamp = _iso_utc(now or dt.datetime.now(UTC), "now")
        clean_id = _text(task_id, "task_id", maximum=128)
        payload = {
            "task_id": clean_id,
            "title": _text(title, "title", maximum=500),
            "description": _text(description, "description", maximum=50_000),
            "acceptance_criteria": list(acceptance_criteria),
            "priority": _slug(priority, "priority"),
            "status": "DRAFT",
            "created_by": _text(created_by, "created_by", maximum=128),
            "dependencies": list(dependencies),
            "blocked_by": list(blocked_by),
            "execution_environment": _optional_text(execution_environment, "execution_environment", maximum=128),
        }
        for name in ("acceptance_criteria", "dependencies", "blocked_by"):
            if not all(isinstance(item, str) and item.strip() for item in payload[name]):
                raise ValidationError(f"{name} must contain non-empty strings")
        return self._task_operation(
            task_id=clean_id, operation="create", actor=payload["created_by"], source=source,
            idempotency_key=idempotency_key, timestamp=timestamp, payload=payload,
        )

    def update_task(
        self, task_id: str, *, changes: Mapping[str, Any], actor: str, source: str,
        idempotency_key: str, now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        allowed = {"title", "description", "acceptance_criteria", "priority", "assigned_to",
                   "dependencies", "blocked_by", "execution_environment"}
        unknown = set(changes) - allowed
        if unknown or not changes:
            raise ValidationError(f"unsupported or empty task changes: {sorted(unknown)}")
        clean: dict[str, Any] = {}
        for key, value in changes.items():
            if key in {"acceptance_criteria", "dependencies", "blocked_by"}:
                if not isinstance(value, (list, tuple)) or not all(isinstance(v, str) and v.strip() for v in value):
                    raise ValidationError(f"{key} must contain non-empty strings")
                clean[key] = list(value)
            elif key == "priority":
                clean[key] = _slug(value, key)
            elif key == "title":
                clean[key] = _text(value, key, maximum=500)
            elif key == "description":
                clean[key] = _text(value, key, maximum=50_000)
            else:
                clean[key] = _optional_text(value, key, maximum=128)
        return self._task_operation(
            task_id=_text(task_id, "task_id", maximum=128), operation="update",
            actor=_text(actor, "actor", maximum=128), source=source,
            idempotency_key=idempotency_key,
            timestamp=_iso_utc(now or dt.datetime.now(UTC), "now"), payload={"changes": clean},
        )

    def approve_task(
        self, task_id: str, *, approved_by: str, source: str, idempotency_key: str,
        now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        return self._task_operation(
            task_id=_text(task_id, "task_id", maximum=128), operation="approve",
            actor=_text(approved_by, "approved_by", maximum=128), source=source,
            idempotency_key=idempotency_key,
            timestamp=_iso_utc(now or dt.datetime.now(UTC), "now"), payload={},
        )

    def claim_task(
        self, task_id: str, *, worker_id: str, source: str, idempotency_key: str,
        lease_seconds: int = 900, now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or not 30 <= lease_seconds <= 86400:
            raise ValidationError("lease_seconds must be between 30 and 86400")
        current = _iso_utc(now or dt.datetime.now(UTC), "now")
        expires = (_parse_utc(current) + dt.timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds").replace("+00:00", "Z")
        return self._task_operation(
            task_id=_text(task_id, "task_id", maximum=128), operation="claim",
            actor=_text(worker_id, "worker_id", maximum=128), source=source,
            idempotency_key=idempotency_key, timestamp=current,
            payload={"worker_id": worker_id, "lease_token": _stable_uuid("lease", idempotency_key),
                     "lease_expires_at": expires},
        )

    def heartbeat_task(
        self, task_id: str, *, lease_token: str, worker_id: str, source: str,
        idempotency_key: str, lease_seconds: int = 900,
        now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or not 30 <= lease_seconds <= 86400:
            raise ValidationError("lease_seconds must be between 30 and 86400")
        current = _iso_utc(now or dt.datetime.now(UTC), "now")
        expires = (_parse_utc(current) + dt.timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds").replace("+00:00", "Z")
        return self._task_operation(
            task_id=_text(task_id, "task_id", maximum=128), operation="heartbeat",
            actor=_text(worker_id, "worker_id", maximum=128), source=source,
            idempotency_key=idempotency_key, timestamp=current,
            payload={"worker_id": worker_id, "lease_token": _text(lease_token, "lease_token", maximum=128),
                     "lease_expires_at": expires},
        )

    def complete_task(
        self, task_id: str, *, lease_token: str, worker_id: str, source: str,
        idempotency_key: str, results: Mapping[str, Any], artifacts: Sequence[str] = (),
        git_commit: str | None = None, test_results: Mapping[str, Any] | None = None,
        status: str = "COMPLETED", now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        if status not in {"REVIEW", "COMPLETED", "FAILED"}:
            raise ValidationError("completion status must be REVIEW, COMPLETED, or FAILED")
        return self._task_operation(
            task_id=_text(task_id, "task_id", maximum=128), operation="complete",
            actor=_text(worker_id, "worker_id", maximum=128), source=source,
            idempotency_key=idempotency_key,
            timestamp=_iso_utc(now or dt.datetime.now(UTC), "now"),
            payload={"worker_id": worker_id, "lease_token": _text(lease_token, "lease_token", maximum=128),
                     "status": status, "results": dict(results), "artifacts": list(artifacts),
                     "git_commit": git_commit, "test_results": dict(test_results or {})},
        )

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return None if row is None else self._task_dict(row)

    def list_tasks(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        query, values = "SELECT * FROM tasks", []
        if status is not None:
            if status not in TASK_STATUSES:
                raise ValidationError("unknown task status")
            query += " WHERE status=?"
            values.append(status)
        query += " ORDER BY created_at, task_id LIMIT ?"
        values.append(limit)
        with self._read() as conn:
            rows = conn.execute(query, values).fetchall()
        return [self._task_dict(row) for row in rows]

    def heartbeat_worker(
        self, *, worker_id: str, hostname: str, environment: str, status: str,
        capabilities: Sequence[str], source: str, idempotency_key: str,
        current_task: str | None = None, now: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        timestamp = _iso_utc(now or dt.datetime.now(UTC), "now")
        payload = {
            "worker_id": _text(worker_id, "worker_id", maximum=128),
            "hostname": _text(hostname, "hostname", maximum=256),
            "environment": _slug(environment, "environment"),
            "status": _slug(status, "status"),
            "current_task": _optional_text(current_task, "current_task", maximum=128),
            "capabilities": list(capabilities),
        }
        if not all(isinstance(item, str) and item.strip() for item in payload["capabilities"]):
            raise ValidationError("capabilities must contain non-empty strings")
        idem = _text(idempotency_key, "idempotency_key", maximum=256)
        event = _normalize_event(NeoRomeEvent.create(
            event_id=_stable_uuid("worker", idem), timestamp=timestamp, desk="main",
            event_type="WORKER_HEARTBEAT", severity="info", source=source,
            agent_id=payload["worker_id"], summary=f"Worker {payload['worker_id']} is {payload['status']}",
            data=payload, idempotency_key=f"worker:{idem}",
        ))
        with self._transaction() as conn:
            prior = conn.execute("SELECT data_json FROM events WHERE idempotency_key=?", (f"worker:{idem}",)).fetchone()
            if prior is not None:
                if json.loads(prior["data_json"]) != payload:
                    raise IdempotencyConflict("worker heartbeat idempotency key was reused with different content")
                return self._worker_dict(conn.execute(
                    "SELECT * FROM workers WHERE worker_id=?", (payload["worker_id"],)
                ).fetchone())
            if payload["current_task"] is not None and conn.execute(
                    "SELECT 1 FROM tasks WHERE task_id=?", (payload["current_task"],)).fetchone() is None:
                raise ValidationError("current_task does not exist")
            _, inserted = self._append(conn, event)
            if inserted:
                conn.execute(
                    """INSERT INTO workers(worker_id,hostname,environment,status,current_task,capabilities_json,
                       last_seen,created_at,updated_at,source,schema_version,last_event_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(worker_id) DO UPDATE SET hostname=excluded.hostname,
                         environment=excluded.environment,status=excluded.status,current_task=excluded.current_task,
                         capabilities_json=excluded.capabilities_json,last_seen=excluded.last_seen,
                         updated_at=excluded.updated_at,source=excluded.source,
                         schema_version=excluded.schema_version,last_event_id=excluded.last_event_id
                       WHERE excluded.last_seen >= workers.last_seen""",
                    (payload["worker_id"], payload["hostname"], payload["environment"], payload["status"],
                     payload["current_task"], _json(payload["capabilities"], "capabilities"), timestamp,
                     timestamp, timestamp, _text(source, "source", maximum=256), EVENT_SCHEMA_VERSION,
                     event["event_id"]),
                )
            return self._worker_dict(conn.execute(
                "SELECT * FROM workers WHERE worker_id=?", (payload["worker_id"],)
            ).fetchone())

    def get_worker(self, worker_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
        return None if row is None else self._worker_dict(row)

    def list_workers(self, *, status: str | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT * FROM workers", ()
        if status is not None:
            query += " WHERE status=?"
            values = (_slug(status, "status"),)
        query += " ORDER BY worker_id"
        with self._read() as conn:
            return [self._worker_dict(row) for row in conn.execute(query, values).fetchall()]

    def store_report(
        self, *, report_id: str, report_type: str, period_start: dt.datetime | str,
        period_end: dt.datetime | str, structured: Mapping[str, Any], human_readable: str,
        source: str, idempotency_key: str, generated_at: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        generated = _iso_utc(generated_at or dt.datetime.now(UTC), "generated_at")
        payload = {
            "report_id": _text(report_id, "report_id", maximum=128),
            "report_type": _slug(report_type, "report_type"),
            "period_start": _iso_utc(period_start, "period_start"),
            "period_end": _iso_utc(period_end, "period_end"),
            "generated_at": generated, "structured": dict(structured),
            "human_readable": _text(human_readable, "human_readable", maximum=500_000),
        }
        if payload["period_end"] < payload["period_start"]:
            raise ValidationError("period_end cannot precede period_start")
        fingerprint_payload = {key: value for key, value in payload.items() if key != "generated_at"}
        fingerprint = hashlib.sha256(_json(fingerprint_payload, "report").encode()).hexdigest()
        idem = _text(idempotency_key, "idempotency_key", maximum=256)
        event = _normalize_event(NeoRomeEvent.create(
            event_id=_stable_uuid("report", idem), timestamp=generated, desk="main",
            event_type="REPORT_GENERATED", severity="info", source=source,
            summary=f"Generated {payload['report_type']} report {payload['report_id']}",
            data={"report_id": payload["report_id"], "report_type": payload["report_type"]},
            idempotency_key=f"report:{idem}",
        ))
        with self._transaction() as conn:
            existing = conn.execute("SELECT * FROM reports WHERE idempotency_key=?", (idem,)).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("report idempotency key was reused with different content")
                return self._report_dict(existing)
            if conn.execute("SELECT 1 FROM reports WHERE report_id=?", (payload["report_id"],)).fetchone():
                raise IdempotencyConflict("report_id already exists")
            self._append(conn, event)
            conn.execute(
                """INSERT INTO reports(report_id,report_type,period_start,period_end,generated_at,
                   created_at,updated_at,source,status,schema_version,structured_json,human_readable,
                   idempotency_key,fingerprint,last_event_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (payload["report_id"], payload["report_type"], payload["period_start"], payload["period_end"],
                 generated, generated, generated, _text(source, "source", maximum=256), "generated",
                 EVENT_SCHEMA_VERSION, _json(payload["structured"], "structured"), payload["human_readable"],
                 idem, fingerprint, event["event_id"]),
            )
            return self._report_dict(conn.execute("SELECT * FROM reports WHERE report_id=?", (payload["report_id"],)).fetchone())

    def record_report_delivery(
        self, report_id: str, *, attempt_id: str, channel: str, status: str, source: str,
        idempotency_key: str, error_summary: str | None = None,
        next_retry_at: dt.datetime | str | None = None, attempted_at: dt.datetime | str | None = None,
    ) -> dict[str, Any]:
        attempted = _iso_utc(attempted_at or dt.datetime.now(UTC), "attempted_at")
        status_clean = _slug(status, "status")
        if status_clean not in {"pending", "succeeded", "failed"}:
            raise ValidationError("delivery status must be pending, succeeded, or failed")
        payload = {"attempt_id": _text(attempt_id, "attempt_id", maximum=128),
                   "report_id": _text(report_id, "report_id", maximum=128),
                   "channel": _slug(channel, "channel"), "status": status_clean,
                   "attempted_at": attempted,
                   "next_retry_at": None if next_retry_at is None else _iso_utc(next_retry_at, "next_retry_at"),
                   "error_summary": _optional_text(error_summary, "error_summary", maximum=2000)}
        fingerprint_payload = {key: value for key, value in payload.items() if key != "attempted_at"}
        fingerprint = hashlib.sha256(_json(fingerprint_payload, "delivery").encode()).hexdigest()
        idem = _text(idempotency_key, "idempotency_key", maximum=256)
        event_type = "REPORT_DELIVERY_FAILED" if status_clean == "failed" else "REPORT_DELIVERY_ATTEMPTED"
        event = _normalize_event(NeoRomeEvent.create(
            event_id=_stable_uuid("delivery", idem), timestamp=attempted, desk="main",
            event_type=event_type, severity="warning" if status_clean == "failed" else "info",
            source=source, summary=f"Report {report_id} delivery {status_clean}", data=payload,
            idempotency_key=f"delivery:{idem}",
        ))
        with self._transaction() as conn:
            existing = conn.execute("SELECT * FROM report_delivery_attempts WHERE idempotency_key=?", (idem,)).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("delivery idempotency key was reused with different content")
                return dict(existing)
            if conn.execute("SELECT 1 FROM report_delivery_attempts WHERE attempt_id=?", (attempt_id,)).fetchone():
                raise IdempotencyConflict("delivery attempt_id already exists")
            if conn.execute("SELECT 1 FROM reports WHERE report_id=?", (report_id,)).fetchone() is None:
                raise ValidationError("report does not exist")
            self._append(conn, event)
            conn.execute(
                """INSERT INTO report_delivery_attempts(attempt_id,report_id,channel,status,attempted_at,
                   next_retry_at,error_summary,source,schema_version,idempotency_key,fingerprint,event_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (payload["attempt_id"], report_id, payload["channel"], status_clean, attempted,
                 payload["next_retry_at"], payload["error_summary"], _text(source, "source", maximum=256),
                 EVENT_SCHEMA_VERSION, idem, fingerprint, event["event_id"]),
            )
            return dict(conn.execute("SELECT * FROM report_delivery_attempts WHERE attempt_id=?", (attempt_id,)).fetchone())

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM reports WHERE report_id=?", (report_id,)).fetchone()
        return None if row is None else self._report_dict(row)

    def list_reports(self, *, report_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        query, values = "SELECT * FROM reports", []
        if report_type is not None:
            query += " WHERE report_type=?"
            values.append(_slug(report_type, "report_type"))
        query += " ORDER BY generated_at DESC, report_id LIMIT ?"
        values.append(limit)
        with self._read() as conn:
            return [self._report_dict(row) for row in conn.execute(query, values).fetchall()]

    def list_report_deliveries(self, report_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM report_delivery_attempts WHERE report_id=? ORDER BY attempted_at,attempt_id",
                (report_id,),
            ).fetchall()]

    def _task_operation(
        self, *, task_id: str, operation: str, actor: str, source: str,
        idempotency_key: str, timestamp: str, payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        idem = _text(idempotency_key, "idempotency_key", maximum=256)
        source_clean = _text(source, "source", maximum=256)
        semantic_payload = dict(payload)
        semantic_payload.pop("lease_expires_at", None)
        operation_body = {"task_id": task_id, "operation": operation, "actor": actor,
                          "payload": semantic_payload}
        fingerprint = hashlib.sha256(_json(operation_body, "task operation").encode()).hexdigest()
        event_types = {"create": "TASK_CREATED", "update": "TASK_UPDATED", "approve": "TASK_UPDATED",
                       "claim": "TASK_STARTED", "heartbeat": "TASK_UPDATED", "complete": "TASK_COMPLETED"}
        with self._transaction() as conn:
            prior = conn.execute("SELECT * FROM task_updates WHERE idempotency_key=?", (idem,)).fetchone()
            if prior is not None:
                if prior["fingerprint"] != fingerprint:
                    raise IdempotencyConflict("task idempotency key was reused with different content")
                return self._task_dict(conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone())
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if operation == "create":
                if row is not None:
                    raise IdempotencyConflict("task_id already exists")
                status = "DRAFT"
            elif row is None:
                raise ValidationError("task does not exist")
            else:
                status = row["status"]

            event_payload = {"task_id": task_id, "operation": operation, **dict(payload)}
            event = _normalize_event(NeoRomeEvent.create(
                event_id=_stable_uuid("task", idem), timestamp=timestamp, desk="main",
                event_type=event_types[operation],
                severity="critical" if operation == "complete" and payload.get("status") == "FAILED" else "info",
                source=source_clean, agent_id=actor, correlation_id=task_id,
                summary=f"Task {task_id} {operation}", data=event_payload,
                idempotency_key=f"task:{idem}",
            ))
            self._append(conn, event)

            if operation == "create":
                conn.execute(
                    """INSERT INTO tasks(task_id,title,description,acceptance_criteria_json,priority,status,
                       created_at,updated_at,source,schema_version,created_by,approved_by,assigned_to,
                       dependencies_json,blocked_by_json,execution_environment,results_json,artifacts_json,
                       git_commit,test_results_json,lease_token,lease_expires_at,last_heartbeat_at,last_event_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (task_id, payload["title"], payload["description"], _json(payload["acceptance_criteria"], "acceptance_criteria"),
                     payload["priority"], "DRAFT", timestamp, timestamp, source_clean, EVENT_SCHEMA_VERSION,
                     payload["created_by"], None, None, _json(payload["dependencies"], "dependencies"),
                     _json(payload["blocked_by"], "blocked_by"), payload["execution_environment"], None,
                     None, None, None, None, None, None, event["event_id"]),
                )
            elif operation == "update":
                if status not in {"DRAFT", "APPROVED", "QUEUED", "BLOCKED"}:
                    raise ValidationError(f"cannot edit task in {status} status")
                column_map = {
                    "title": "title", "description": "description", "acceptance_criteria": "acceptance_criteria_json",
                    "priority": "priority", "assigned_to": "assigned_to", "dependencies": "dependencies_json",
                    "blocked_by": "blocked_by_json", "execution_environment": "execution_environment",
                }
                assignments, values = [], []
                for key, value in payload["changes"].items():
                    assignments.append(f"{column_map[key]}=?")
                    values.append(_json(value, key) if key in {"acceptance_criteria", "dependencies", "blocked_by"} else value)
                assignments.extend(["updated_at=?", "source=?", "last_event_id=?"])
                values.extend([timestamp, source_clean, event["event_id"], task_id])
                conn.execute(f"UPDATE tasks SET {','.join(assignments)} WHERE task_id=?", values)
            elif operation == "approve":
                if status not in {"DRAFT", "BLOCKED"}:
                    raise ValidationError(f"cannot approve task in {status} status")
                conn.execute(
                    "UPDATE tasks SET status='APPROVED',approved_by=?,updated_at=?,source=?,last_event_id=? WHERE task_id=?",
                    (actor, timestamp, source_clean, event["event_id"], task_id),
                )
                status = "APPROVED"
            elif operation == "claim":
                lease_expired = row["lease_expires_at"] is not None and row["lease_expires_at"] < timestamp
                if status not in {"APPROVED", "QUEUED"} and not (status in {"CLAIMED", "IN_PROGRESS"} and lease_expired):
                    raise ValidationError(f"task in {status} status is not claimable")
                conn.execute(
                    """UPDATE tasks SET status='CLAIMED',assigned_to=?,lease_token=?,lease_expires_at=?,
                       last_heartbeat_at=?,updated_at=?,source=?,last_event_id=? WHERE task_id=?""",
                    (actor, payload["lease_token"], payload["lease_expires_at"], timestamp,
                     timestamp, source_clean, event["event_id"], task_id),
                )
                status = "CLAIMED"
            elif operation == "heartbeat":
                self._require_active_lease(row, payload["lease_token"], actor, timestamp)
                conn.execute(
                    """UPDATE tasks SET status='IN_PROGRESS',lease_expires_at=?,last_heartbeat_at=?,
                       updated_at=?,source=?,last_event_id=? WHERE task_id=?""",
                    (payload["lease_expires_at"], timestamp, timestamp, source_clean, event["event_id"], task_id),
                )
                status = "IN_PROGRESS"
            elif operation == "complete":
                self._require_active_lease(row, payload["lease_token"], actor, timestamp)
                conn.execute(
                    """UPDATE tasks SET status=?,results_json=?,artifacts_json=?,git_commit=?,test_results_json=?,
                       lease_token=NULL,lease_expires_at=NULL,updated_at=?,source=?,last_event_id=? WHERE task_id=?""",
                    (payload["status"], _json(payload["results"], "results"), _json(payload["artifacts"], "artifacts"),
                     payload["git_commit"], _json(payload["test_results"], "test_results"), timestamp,
                     source_clean, event["event_id"], task_id),
                )
                status = payload["status"]

            conn.execute(
                """INSERT INTO task_updates(update_id,task_id,operation,status,actor,created_at,source,
                   schema_version,idempotency_key,payload_json,fingerprint,event_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (_stable_uuid("task-update", idem), task_id, operation, status, actor, timestamp, source_clean,
                 EVENT_SCHEMA_VERSION, idem, _json(payload, "payload"), fingerprint, event["event_id"]),
            )
            return self._task_dict(conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone())

    @staticmethod
    def _require_active_lease(row: sqlite3.Row, token: str, worker: str, timestamp: str) -> None:
        if row["status"] not in {"CLAIMED", "IN_PROGRESS"}:
            raise ValidationError(f"task in {row['status']} status has no active lease")
        if row["lease_token"] != token or row["assigned_to"] != worker:
            raise ValidationError("lease token or worker does not match")
        if row["lease_expires_at"] is None or row["lease_expires_at"] < timestamp:
            raise ValidationError("task lease has expired")

    @staticmethod
    def _append(conn: sqlite3.Connection, normalized: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        existing = conn.execute(
            "SELECT * FROM events WHERE event_id=? OR (? IS NOT NULL AND idempotency_key=?)",
            (normalized["event_id"], normalized["idempotency_key"], normalized["idempotency_key"]),
        ).fetchone()
        if existing is not None:
            if existing["fingerprint"] != normalized["fingerprint"]:
                raise IdempotencyConflict("event_id or idempotency_key was reused with different content")
            return ControlPlaneStore._event_dict(existing), False
        ingested_at = dt.datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        conn.execute(
            """INSERT INTO events(event_id,timestamp,ingested_at,desk,agent_id,event_type,severity,
                 summary,data_json,correlation_id,source,schema_version,idempotency_key,fingerprint)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            tuple(normalized[key] for key in (
                "event_id", "timestamp"
            )) + (ingested_at,) + tuple(normalized[key] for key in (
                "desk", "agent_id", "event_type", "severity", "summary", "data_json",
                "correlation_id", "source", "schema_version", "idempotency_key", "fingerprint"
            )),
        )
        row = conn.execute("SELECT * FROM events WHERE event_id=?", (normalized["event_id"],)).fetchone()
        return ControlPlaneStore._event_dict(row), True

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()
        return None if row is None else self._event_dict(row)

    def list_events(
        self,
        *,
        desk: str | None = None,
        event_type: str | None = None,
        severity: str | None = None,
        since: dt.datetime | str | None = None,
        until: dt.datetime | str | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValidationError("limit must be between 1 and 1000")
        clauses, values = [], []
        for column, value in (("desk", desk), ("event_type", event_type),
                              ("severity", severity), ("correlation_id", correlation_id)):
            if value is not None:
                clauses.append(f"{column}=?")
                if column == "desk":
                    values.append(_slug(value, column))
                elif column == "severity":
                    clean = _text(value, column, maximum=16).lower()
                    if clean not in SEVERITIES:
                        raise ValidationError("unknown severity")
                    values.append(clean)
                else:
                    values.append(_text(value, column, maximum=128))
        if since is not None:
            clauses.append("timestamp>=?")
            values.append(_iso_utc(since, "since"))
        if until is not None:
            clauses.append("timestamp<=?")
            values.append(_iso_utc(until, "until"))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self._read() as conn:
            rows = conn.execute(
                f"SELECT * FROM events{where} ORDER BY timestamp DESC, sequence DESC LIMIT ?",  # noqa: S608
                (*values, limit),
            ).fetchall()
        return [self._event_dict(row) for row in rows]

    def get_desk(self, desk: str) -> dict[str, Any] | None:
        return self._projection("desks", "desk", _slug(desk, "desk"))

    def list_desks(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [self._row_dict(row) for row in conn.execute(
                "SELECT * FROM desks ORDER BY desk"
            ).fetchall()]

    def get_system_health(self, component: str) -> dict[str, Any] | None:
        return self._projection("system_health", "component", _slug(component, "component"))

    def list_system_health(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [self._row_dict(row) for row in conn.execute(
                "SELECT * FROM system_health ORDER BY component"
            ).fetchall()]

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        return self._projection("incidents", "incident_id", _text(incident_id, "incident_id", maximum=128))

    def list_incidents(self, *, status: str | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT * FROM incidents", ()
        if status is not None:
            query += " WHERE status=?"
            values = (_slug(status, "status"),)
        query += " ORDER BY updated_at DESC, incident_id"
        with self._read() as conn:
            rows = conn.execute(query, values).fetchall()
        return [self._row_dict(row) for row in rows]

    def integrity_check(self) -> bool:
        with self._read() as conn:
            return conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    @contextlib.contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connect()
            close = conn is not self._memory
            try:
                yield conn
            finally:
                if close:
                    conn.close()

    def _projection(self, table: str, key: str, value: str) -> dict[str, Any] | None:
        if table not in {"desks", "system_health", "incidents"}:
            raise ValueError("unknown projection")
        with self._read() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE {key}=?", (value,)).fetchone()
        return None if row is None else self._row_dict(row)

    @staticmethod
    def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        if "details_json" in result:
            result["details"] = json.loads(result.pop("details_json"))
        return result

    @staticmethod
    def _task_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for column in ("acceptance_criteria_json", "dependencies_json", "blocked_by_json",
                       "results_json", "artifacts_json", "test_results_json"):
            value = result.pop(column)
            result[column[:-5]] = None if value is None else json.loads(value)
        return result

    @staticmethod
    def _report_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["structured"] = json.loads(result.pop("structured_json"))
        result.pop("fingerprint", None)
        return result

    @staticmethod
    def _worker_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["capabilities"] = json.loads(result.pop("capabilities_json"))
        return result

    @staticmethod
    def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["data"] = json.loads(result.pop("data_json"))
        result.pop("fingerprint", None)
        return result


__all__: Sequence[str] = (
    "ControlPlaneStore", "ControlPlaneStoreError", "IdempotencyConflict", "NeoRomeEvent",
    "SchemaVersionError", "ValidationError", "EVENT_TYPES", "EVENT_SCHEMA_VERSION", "TASK_STATUSES",
)
