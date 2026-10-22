"""Idempotently project existing cloud status files into institutional memory."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid
from typing import Any, Mapping

from .control_plane_store import ControlPlaneStore, NeoRomeEvent


UTC = timezone.utc
NAMESPACE = uuid.UUID("698b318f-c0fc-4cf4-9631-c8ba7d5143f7")
HEARTBEAT_INTERVAL_SECONDS = 300
DESK_READINESS_MAX_AGE_SECONDS = 45
READINESS_DESKS = {"equities", "options", "prediction", "crypto"}


def _safe_legacy_gate_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 2:
        return None
    if isinstance(value, str):
        return value[:256]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_safe_legacy_gate_value(item, depth=depth + 1) for item in value[:32]
                if not isinstance(item, (Mapping, list))]
    if isinstance(value, Mapping):
        result = {}
        for key, item in list(value.items())[:32]:
            name = str(key)
            if any(token in name.lower() for token in ("secret", "token", "credential", "password", "api_key", "auth")):
                continue
            if isinstance(item, (str, int, float, bool)) or item is None:
                result[name] = _safe_legacy_gate_value(item, depth=depth + 1)
        return result
    return None


def _read_status(path: str | os.PathLike[str]) -> tuple[Mapping[str, Any] | None, str]:
    source = Path(path)
    try:
        if source.is_symlink() or not source.is_file():
            return None, "missing"
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            raw = stream.read(1_000_001)
        if len(raw) > 1_000_000:
            return None, "oversize"
        value = json.loads(raw)
        if not isinstance(value, Mapping):
            return None, "invalid"
        return value, "ok"
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None, "unreadable"


def _timestamp(value: Mapping[str, Any] | None, now: datetime) -> str:
    raw = None if value is None else value.get("updated_at")
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
    return now.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _fresh(value: Mapping[str, Any] | None, now: datetime, max_age_seconds: int) -> bool:
    if value is None or not isinstance(value.get("updated_at"), str):
        return False
    try:
        parsed = datetime.fromisoformat(value["updated_at"].replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return False
        age = (now - parsed.astimezone(UTC)).total_seconds()
        return 0 <= age <= max_age_seconds
    except ValueError:
        return False


def _detail(value: Mapping[str, Any] | None, read_status: str) -> dict[str, Any]:
    """Retain decision-useful health evidence without copying secrets or paths."""
    result: dict[str, Any] = {"read_status": read_status}
    if value is None:
        return result
    for key in ("operational_healthy", "status", "blocker", "operational_failure",
                "price_blocked", "last_success", "last_cycle"):
        item = value.get(key)
        if isinstance(item, (str, int, float, bool)) or item is None:
            result[key] = item
    return result


def _read_desk_readiness(path: str | os.PathLike[str], now: datetime) -> tuple[Mapping[str, Any] | None, str]:
    """Accept only a recent root-owned projection with the fixed desk schema."""
    source = Path(path)
    try:
        if source.is_symlink() or not source.is_file():
            return None, "missing"
        metadata = source.stat()
        if metadata.st_uid != 0 or metadata.st_mode & 0o022:
            return None, "wrong_owner"
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            raw = stream.read(65_537)
        if len(raw) > 65_536:
            return None, "oversize"
        value = json.loads(raw)
        if not isinstance(value, Mapping) or value.get("schema_version") != 1:
            return None, "invalid"
        timestamp = value.get("as_of")
        if not isinstance(timestamp, str):
            return None, "invalid"
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None, "invalid"
        age = (now.astimezone(UTC) - parsed.astimezone(UTC)).total_seconds()
        if not 0 <= age <= DESK_READINESS_MAX_AGE_SECONDS:
            return None, "stale"
        desks = value.get("desks")
        if not isinstance(desks, Mapping) or set(desks) != READINESS_DESKS:
            return None, "invalid"
        for desk, entry in desks.items():
            if not isinstance(entry, Mapping) or entry.get("status") not in {"active", "blocked", "unknown"}:
                return None, "invalid"
            if entry.get("entry_eligibility") != "unknown" or not isinstance(entry.get("profiles"), Mapping):
                return None, "invalid"
            if entry.get("status") == "active":
                profiles = entry["profiles"]
                if not profiles:
                    return None, "invalid"
                for profile in profiles.values():
                    if not isinstance(profile, Mapping) or profile.get("entry_eligibility") != "unknown":
                        return None, "invalid"
                    if profile.get("operational_state") not in {"armed_for_session", "healthy", "active"}:
                        return None, "invalid"
        return value, "ok"
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError):
        return None, "unreadable"


def sync_desk_readiness(store: ControlPlaneStore, *, path: str | os.PathLike[str],
                        now: datetime, legacy_statuses: Mapping[str, Any] | None = None,
                        legacy_details: Mapping[str, Any] | None = None,
                        prior_desks: Mapping[str, Any] | None = None,
                        ) -> dict[str, Any]:
    projection, read_status = _read_desk_readiness(path, now)
    projected: list[str] = []
    fallbacks: list[str] = []
    current_stamp = now.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def record_fallback(desk: str, reason: str) -> None:
        getter = getattr(store, "get_desk", None)
        prior = getter(desk) if callable(getter) else None
        prior_details = prior.get("details") if isinstance(prior, Mapping) else None
        if not isinstance(prior_details, Mapping) or prior_details.get("readiness_projection") != "validated":
            prior = (prior_desks or {}).get(desk)
            prior_details = prior.get('details') if isinstance(prior, Mapping) else None
        if not isinstance(prior_details, Mapping) or prior_details.get("readiness_projection") != "validated":
            return
        legacy = (legacy_statuses or {}).get(desk)
        if (not isinstance(legacy, str) or not legacy or len(legacy) > 256 or
                legacy.strip().lower() == "active"):
            fallback_status = "unknown"
        else:
            fallback_status = legacy
        key = f"desk-readiness:v1:{desk}:{current_stamp}:unavailable:{reason}:{fallback_status}"
        store.record_desk_state(NeoRomeEvent.create(
            event_id=str(uuid.uuid5(NAMESPACE, key)), timestamp=current_stamp,
            desk=desk, event_type="DESK_HEARTBEAT", severity="warning",
            summary=f"{desk} desk readiness projection is unavailable",
            source="neorome.control_plane_sync", correlation_id=f"desk:{desk}",
            idempotency_key=key,
            data={"status": fallback_status, "details": {
                "entry_eligibility": "unknown", "readiness_projection": "unavailable",
                "read_status": reason, "legacy_status": fallback_status,
            }},
        ))
        fallbacks.append(desk)

    if projection is None:
        for desk in sorted(READINESS_DESKS):
            record_fallback(desk, read_status)
        return {"status": "fallback" if fallbacks else "ignored", "reason": read_status,
                "desks": fallbacks}
    for desk in sorted(READINESS_DESKS):
        item = projection["desks"][desk]
        # Only operationally active desks may replace an older observational
        # label. Unknown and blocked evidence never turns legacy state green.
        if item["status"] != "active":
            record_fallback(desk, f"desk_{item['status']}")
            continue
        as_of = projection["as_of"]
        key = f"desk-readiness:v1:{desk}:{as_of}:active"
        legacy = (legacy_statuses or {}).get(desk)
        store.record_desk_state(NeoRomeEvent.create(
            event_id=str(uuid.uuid5(NAMESPACE, key)), timestamp=as_of,
            desk=desk, event_type="DESK_HEARTBEAT", severity="info",
            summary=f"{desk} desk has an armed or healthy operational profile",
            source="neorome.control_plane_sync", correlation_id=f"desk:{desk}",
            idempotency_key=key,
            data={"status": "active", "details": {
                "readiness_projection": "validated", "entry_eligibility": "unknown",
                "profiles": item["profiles"], "legacy_status": legacy,
                "legacy_gates": dict(legacy_details or {}),
                "projection_as_of": as_of,
            }},
        ))
        projected.append(desk)
    return {"status": "applied", "desks": projected, "fallback_desks": fallbacks,
            "as_of": projection["as_of"]}


def sync_status(
    store: ControlPlaneStore,
    *,
    component: str,
    path: str | os.PathLike[str],
    max_age_seconds: int = 660,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    value, read_status = _read_status(path)
    fresh = _fresh(value, current, max_age_seconds)
    healthy = (read_status == "ok" and value is not None
               and value.get("operational_healthy") is True and fresh)
    status = "ready" if healthy else "degraded"
    occurred_at = _timestamp(value, current)
    source_key = f"status:v2:{component}:{occurred_at}:{status}:{read_status}"
    prior = store.get_system_health(component)
    changed = prior is None or prior.get("status") != status
    if not changed and prior is not None:
        try:
            previous = datetime.fromisoformat(str(prior["updated_at"]).replace("Z", "+00:00"))
            if 0 <= (current - previous.astimezone(UTC)).total_seconds() < HEARTBEAT_INTERVAL_SECONDS:
                return {"component": component, "status": status, "changed": False,
                        "event_id": prior["last_event_id"]}
        except (KeyError, TypeError, ValueError):
            pass
    event_type = "READINESS_RESTORED" if healthy else "READINESS_DEGRADED"
    event = NeoRomeEvent.create(
        event_id=str(uuid.uuid5(NAMESPACE, source_key)),
        timestamp=occurred_at,
        desk="system",
        event_type=event_type,
        severity="info" if healthy else "critical",
        summary=f"{component} readiness is {status}",
        source="neorome.control_plane_sync",
        correlation_id=f"readiness:{component}",
        idempotency_key=source_key,
        data={"component": component, "status": status,
              "details": {**_detail(value, read_status), "fresh": fresh,
                          "max_age_seconds": max_age_seconds}},
    )
    stored = store.record_system_health(event)
    incident_key = f"readiness:{component}"
    incident_event_key = f"incident:v2:{component}:{occurred_at}:{status}:{read_status}"
    incident = NeoRomeEvent.create(
        event_id=str(uuid.uuid5(NAMESPACE, incident_event_key)),
        timestamp=occurred_at,
        desk="system",
        event_type=event_type,
        severity="info" if healthy else "critical",
        summary=f"{component} readiness {'restored' if healthy else 'degraded'}",
        source="neorome.control_plane_sync",
        correlation_id=incident_key,
        idempotency_key=incident_event_key,
        data={
            "incident_id": incident_key,
            "status": "resolved" if healthy else "open",
            "opened_at": occurred_at if not healthy else (
                (store.get_incident(incident_key) or {}).get("opened_at", occurred_at)
            ),
            "resolved_at": occurred_at if healthy else None,
            "details": {**_detail(value, read_status), "fresh": fresh,
                        "max_age_seconds": max_age_seconds},
        },
    )
    existing_incident = store.get_incident(incident_key)
    if not healthy or existing_incident is not None:
        store.record_incident(incident)
    desk_states = value.get("desk_status") if value is not None else None
    if component == "paper_runtime" and isinstance(desk_states, Mapping):
        for desk, desk_status in sorted(desk_states.items()):
            if not isinstance(desk, str) or not isinstance(desk_status, str):
                continue
            desk_data = {
                "status": desk_status,
                "details": {"runtime_status": status, "mode": "internal_paper"},
            }
            identity_payload = json.dumps(
                {"desk": desk, "source_updated_at": occurred_at, "data": desk_data},
                sort_keys=True, separators=(",", ":"),
            )
            desk_key = f"desk:v2:{uuid.uuid5(NAMESPACE, identity_payload)}"
            store.record_desk_state(NeoRomeEvent.create(
                event_id=str(uuid.uuid5(NAMESPACE, desk_key)), timestamp=occurred_at,
                desk=desk, event_type="DESK_HEARTBEAT", severity="info",
                summary=f"{desk} desk is {desk_status}", source="neorome.control_plane_sync",
                correlation_id=f"desk:{desk}", idempotency_key=desk_key, data=desk_data,
            ))
    return {"component": component, "status": status, "changed": changed,
            "event_id": stored["last_event_id"]}


def run(*, database: str, statuses: Mapping[str, str], now: datetime | None = None,
        desk_readiness: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    with ControlPlaneStore(database) as store:
        # Retain the overlay marker before a new legacy heartbeat replaces it.
        prior_desks = {desk: store.get_desk(desk) for desk in READINESS_DESKS} if desk_readiness is not None else {}
        ages = {"operations": 660, "paper_runtime": 180, "recovery_verification": 7200}
        results = [sync_status(store, component=name, path=path,
                               max_age_seconds=ages.get(name, 660), now=now)
                   for name, path in statuses.items()]
        result = {"schema_version": 1, "healthy": all(item["status"] == "ready" for item in results), "components": results}
        if desk_readiness is not None:
            runtime, _ = _read_status(statuses.get("paper_runtime", ""))
            legacy = runtime.get("desk_status") if runtime is not None else None
            gate_details = {}
            if runtime is not None:
                for key, value in runtime.items():
                    lowered = str(key).lower()
                    if not any(token in lowered for token in ("weather", "calibration", "full_port", "fullport")):
                        continue
                    if not any(token in lowered for token in ("secret", "token", "credential", "password", "api_key", "auth")):
                        gate_details[str(key)] = _safe_legacy_gate_value(value)
            result["desk_readiness"] = sync_desk_readiness(
                store, path=desk_readiness, now=(now or datetime.now(UTC)),
                legacy_statuses=legacy if isinstance(legacy, Mapping) else None,
                legacy_details=gate_details, prior_desks=prior_desks)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="/var/lib/neorome-control/control-plane.sqlite3")
    parser.add_argument("--operations", default="/var/lib/neorome-ops/status.json")
    parser.add_argument("--runtime", default="/var/lib/neorome-paper-runtime/status.json")
    parser.add_argument("--recovery", default="/var/lib/neorome-paper-recovery/status.json")
    parser.add_argument("--desk-readiness", nargs="?", const="/var/lib/neorome-maintenance-ai/inbox/desk-readiness.json")
    args = parser.parse_args()
    result = run(database=args.database, statuses={
        "operations": args.operations,
        "paper_runtime": args.runtime,
        "recovery_verification": args.recovery,
    }, desk_readiness=args.desk_readiness)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
