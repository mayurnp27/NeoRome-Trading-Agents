"""Generate durable NeoRome reports from cloud state without model involvement."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .control_plane_store import ControlPlaneStore, MAX_JSON_BYTES


UTC = timezone.utc
REPORT_JSON_BUDGET = MAX_JSON_BYTES
_SUMMARY_CHARS = 240


def _bounded_text(value: str, maximum: int = _SUMMARY_CHARS) -> str:
    if len(value) <= maximum:
        return value
    return value[: maximum - 1] + "…"


def _json_bytes(value: Any) -> int:
    # Match the store's _json serializer, including its Unicode escaping.
    return len(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _store_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _source_snapshot(
    store: ControlPlaneStore, period_start: datetime, until: datetime,
) -> tuple[dict[str, int], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read bounded metadata and exact counts from one read-only database snapshot."""
    start = period_start.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    end = until.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    terminal = ("COMPLETED", "FAILED", "CANCELLED")
    with store._read() as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        try:
            activity_total = connection.execute(
                "SELECT COUNT(*) FROM events WHERE timestamp>=? AND timestamp<=?", (start, end)
            ).fetchone()[0]
            degraded_total = connection.execute(
                "SELECT COUNT(*) FROM events WHERE timestamp>=? AND timestamp<=? AND event_type='READINESS_DEGRADED'",
                (start, end),
            ).fetchone()[0]
            incident_total = connection.execute(
                "SELECT COUNT(*) FROM incidents WHERE status='open'"
            ).fetchone()[0]
            task_total = connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE status NOT IN (?,?,?)", terminal
            ).fetchone()[0]
            events = [dict(row) for row in connection.execute(
                """SELECT event_id,timestamp,desk,event_type,severity,summary,source,
                          length(data_json)>2 AS details_available
                   FROM events WHERE timestamp>=? AND timestamp<=?
                   ORDER BY timestamp DESC,sequence DESC LIMIT 1000""", (start, end),
            )]
            incidents = [dict(row) for row in connection.execute(
                """SELECT incident_id,status,severity,summary,opened_at,updated_at,last_event_id,
                          length(details_json)>2 AS details_available
                   FROM incidents WHERE status='open'
                   ORDER BY updated_at DESC,incident_id LIMIT 1000"""
            )]
            tasks = [dict(row) for row in connection.execute(
                """SELECT task_id,title,status,priority,created_at,updated_at,last_event_id,
                          (length(description)>0 OR length(acceptance_criteria_json)>2
                           OR length(results_json)>2) AS details_available
                   FROM tasks WHERE status NOT IN (?,?,?)
                   ORDER BY created_at,task_id LIMIT 1000""", terminal,
            )]
            connection.execute("COMMIT")
        finally:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            connection.execute("PRAGMA query_only=OFF")
    counts = {"activity": activity_total, "degraded_activity": degraded_total,
              "incidents": incident_total, "outstanding_tasks": task_total}
    return counts, events, incidents, tasks


def _event_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": item["event_id"], "timestamp": item["timestamp"],
        "desk": item["desk"], "event_type": item["event_type"],
        "severity": item["severity"], "summary": _bounded_text(item["summary"]),
        "source": item["source"], "reference": f"control_plane.events/{item['event_id']}",
        "details_available": bool(item["details_available"]),
    }


def _incident_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": item["incident_id"], "status": item["status"],
        "severity": item["severity"], "summary": _bounded_text(item["summary"]),
        "opened_at": item["opened_at"], "updated_at": item["updated_at"],
        "last_event_id": item["last_event_id"],
        "reference": f"control_plane.incidents/{item['incident_id']}",
        "details_available": bool(item["details_available"]),
    }


def _task_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": item["task_id"], "title": _bounded_text(item["title"]),
        "status": item["status"], "priority": item["priority"],
        "created_at": item["created_at"], "updated_at": item["updated_at"],
        "last_event_id": item["last_event_id"],
        "reference": f"control_plane.tasks/{item['task_id']}",
        "details_available": bool(item["details_available"]),
    }


def _coverage(total: int, source_count: int, shown: int) -> dict[str, Any]:
    reasons = []
    if source_count < total:
        reasons.append("source_listing_limit")
    if shown < source_count:
        reasons.append("report_byte_budget")
    return {"total": total, "shown": shown, "omitted": total - shown,
            "complete": shown == total, "reasons": reasons}


def _within_store_budget(report: dict[str, Any], human_readable: str) -> bool:
    # This mirrors store_report's fingerprint envelope, the largest serialized
    # report object that it validates before persistence.
    envelope = {
        "report_id": report["report_id"], "report_type": report["report_type"],
        "period_start": _store_timestamp(report["period_start"]),
        "period_end": _store_timestamp(report["period_end"]),
        "structured": report, "human_readable": human_readable,
    }
    return _json_bytes(envelope) <= REPORT_JSON_BUDGET


def _nth_sunday(year: int, month: int, n: int) -> date:
    first = date(year, month, 1)
    return date(year, month, 1 + (6 - first.weekday()) % 7 + 7 * (n - 1))


def _new_york_zone(day: date):
    """US Eastern offset for report hours; avoids host timezone configuration."""
    dst_start = _nth_sunday(day.year, 3, 2)
    dst_end = _nth_sunday(day.year, 11, 1)
    offset = -4 if dst_start <= day < dst_end else -5
    return timezone(timedelta(hours=offset), "America/New_York")


def _new_york_now(now: datetime) -> datetime:
    # Determine the local calendar date conservatively, then apply that date's
    # report-hour offset. Both scheduled report hours occur after DST transitions.
    estimate = now.astimezone(timezone(timedelta(hours=-5)))
    zone = _new_york_zone(estimate.date())
    local = now.astimezone(zone)
    if local.date() != estimate.date():
        zone = _new_york_zone(local.date())
        local = now.astimezone(zone)
    return local


def report_slot(now: datetime) -> tuple[str, datetime, datetime]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    local = _new_york_now(now)
    report_type = "close" if local.time() >= time(16, 15) else "morning"
    slot_time = time(16, 15) if report_type == "close" else time(10, 0)
    zone = _new_york_zone(local.date())
    slot_local = datetime.combine(local.date(), slot_time, tzinfo=zone)
    start_local = datetime.combine(local.date(), time.min, tzinfo=zone)
    return report_type, start_local.astimezone(UTC), slot_local.astimezone(UTC)


def _paper_state(path: str | None) -> dict[str, Any]:
    if path is None:
        return {"available": False, "reason": "not_configured"}
    source = Path(path)
    if source.is_symlink() or not source.is_file():
        return {"available": False, "reason": "missing"}
    try:
        uri = f"file:{source.resolve()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=5) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            marker = connection.execute("SELECT value FROM settings WHERE key='ledger_marker'").fetchone()
            if marker is None or marker[0] != "neorome-paper-ledger-v1":
                return {"available": False, "reason": "invalid_marker"}
            balances = [dict(row) for row in connection.execute(
                "SELECT desk,initial_cash_cents,available_cash_cents,reserved_cash_cents FROM accounts ORDER BY desk"
            )]
            position_count = connection.execute(
                "SELECT COUNT(*) FROM positions WHERE state='open'"
            ).fetchone()[0]
            positions = [dict(row) for row in connection.execute(
                "SELECT position_id,desk,instrument,exposure_group,idea_id,quantity,basis_cents,state,opened_at FROM positions WHERE state='open' ORDER BY opened_at,position_id LIMIT 1000"
            )]
            realized = connection.execute("SELECT COALESCE(SUM(realized_pnl_cents),0) FROM closes").fetchone()[0]
            audit_count = connection.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
            connection.execute("COMMIT")
        return {"available": True, "balances": balances, "positions": positions,
                "position_count": position_count, "realized_pnl_cents": realized,
                "audit_event_count": audit_count}
    except sqlite3.Error as exc:
        return {"available": False, "reason": type(exc).__name__}


def _human(report: dict[str, Any]) -> str:
    paper = report["paper"]
    coverage = report["coverage"]
    lines = [
        f"NeoRome {report['report_type'].title()} Report — {report['period_end']}",
        f"Firm readiness: {report['firm_status']}",
        f"Open incidents: {coverage['incidents']['total']} ({coverage['incidents']['shown']} shown)",
        f"Outstanding tasks: {coverage['outstanding_tasks']['total']} ({coverage['outstanding_tasks']['shown']} shown)",
        f"Activity events: {coverage['activity']['total']} ({coverage['activity']['shown']} shown)",
    ]
    if paper.get("available"):
        lines.extend([
            f"Open positions: {paper['position_coverage']['total']} ({paper['position_coverage']['shown']} shown)",
            f"Realized P&L: ${paper['realized_pnl_cents'] / 100:.2f}",
            "Desk cash: " + ", ".join(
                f"{item['desk']} ${item['available_cash_cents'] / 100:.2f} available"
                for item in paper["balances"]
            ),
        ])
    else:
        lines.append(f"Paper ledger unavailable: {paper.get('reason', 'unknown')}")
    if report["incidents"]:
        lines.append("Incidents: " + "; ".join(item["summary"] for item in report["incidents"][:10]))
    if any(not item["complete"] for item in coverage.values()) or (
            paper.get("available") and not paper["position_coverage"]["complete"]):
        lines.append("Coverage is partial; omitted canonical details remain available through the listed references.")
    return "\n".join(lines) + "\n"


def generate_report(
    store: ControlPlaneStore,
    *,
    now: datetime,
    paper_ledger: str | None = None,
    forced_type: str | None = None,
) -> dict[str, Any]:
    inferred, period_start, period_end = report_slot(now)
    report_type = forced_type or inferred
    if report_type not in {"morning", "close"}:
        raise ValueError("report_type must be morning or close")
    report_id = f"{_new_york_now(period_end).date().isoformat()}-{report_type}"
    existing = store.get_report(report_id)
    if existing is not None:
        # A repair/manual snapshot taken before the scheduled cutoff must not
        # occupy that day's only immutable ID and suppress the scheduled report.
        # Preserve the early record and give the scheduled result a stable ID.
        generated = datetime.fromisoformat(existing["generated_at"].replace("Z", "+00:00"))
        if now.astimezone(UTC) >= period_end and generated < period_end:
            report_id += "-scheduled"
            existing = store.get_report(report_id)
        if existing is not None:
            return existing
    counts, raw_activity, raw_incidents, raw_tasks = _source_snapshot(store, period_start, now)
    activity = [_event_projection(item) for item in raw_activity]
    incidents = [_incident_projection(item) for item in raw_incidents]
    outstanding = [_task_projection(item) for item in raw_tasks]
    paper = _paper_state(paper_ledger)
    if paper.get("available"):
        position_total = paper.pop("position_count")
        position_source_count = min(position_total, 1000)
        paper["positions"] = [
            {**{key: item[key] for key in (
                "position_id", "desk", "instrument", "exposure_group", "idea_id",
                "quantity", "basis_cents", "state", "opened_at",
            )}, "reference": f"paper_ledger.positions/{item['position_id']}"}
            for item in paper["positions"]
        ]
        paper["position_coverage"] = _coverage(
            position_total, position_source_count, len(paper["positions"])
        )
    else:
        paper.pop("positions", None)
    source_counts = {key: min(counts[key], 1000) for key in (
        "activity", "incidents", "outstanding_tasks",
    )}
    report = {
        "schema_version": 1,
        "report_id": report_id,
        "report_type": report_type,
        "period_start": period_start.isoformat().replace("+00:00", "Z"),
        "period_end": period_end.isoformat().replace("+00:00", "Z"),
        "generated_at": now.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "firm_status": "degraded" if counts["incidents"] or counts["degraded_activity"] else "ready",
        "paper": paper,
        "incidents": incidents,
        "outstanding_tasks": outstanding,
        "activity": activity,
        "coverage": {
            "activity": _coverage(counts["activity"], source_counts["activity"], len(activity)),
            "incidents": _coverage(counts["incidents"], source_counts["incidents"], len(incidents)),
            "outstanding_tasks": _coverage(counts["outstanding_tasks"], source_counts["outstanding_tasks"], len(outstanding)),
        },
        "references": {
            "events": "control_plane.events/{event_id}",
            "incidents": "control_plane.incidents/{incident_id}",
            "tasks": "control_plane.tasks/{task_id}",
            "paper_positions": "paper_ledger.positions/{position_id}",
            "detail_policy": "Compact report projections retain canonical IDs; omitted row details remain in their source records.",
        },
    }
    human_readable = _human(report)
    # Keep the report's useful newest records while reducing the largest sections
    # until the exact serialized store envelope fits its unchanged 1 MB limit.
    sections = ("activity", "incidents", "outstanding_tasks")
    while not _within_store_budget(report, human_readable):
        candidates = [key for key in sections if report[key]]
        if paper.get("available") and paper["positions"]:
            candidates.append("paper.positions")
        if not candidates:
            raise ValueError("report metadata exceeds the durable report byte budget")
        # Binary-search the largest prefix of the largest section that fits.
        # This avoids serializing the full report once per omitted record.
        selected = max(candidates, key=lambda key: len(
            paper["positions"] if key == "paper.positions" else report[key]
        ))
        rows = paper["positions"] if selected == "paper.positions" else report[selected]
        original = rows
        low, high, best = 0, len(original) - 1, 0
        while low <= high:
            keep = (low + high) // 2
            if selected == "paper.positions":
                paper["positions"] = original[:keep]
                paper["position_coverage"] = _coverage(
                    position_total, position_source_count, keep
                )
            else:
                report[selected] = original[:keep]
                report["coverage"][selected] = _coverage(
                    counts[selected], source_counts[selected], keep
                )
            candidate_human = _human(report)
            if _within_store_budget(report, candidate_human):
                best = keep
                low = keep + 1
            else:
                high = keep - 1
        if selected == "paper.positions":
            paper["positions"] = original[:best]
            paper["position_coverage"] = _coverage(
                position_total, position_source_count, best
            )
        else:
            report[selected] = original[:best]
            report["coverage"][selected] = _coverage(
                counts[selected], source_counts[selected], best
            )
        human_readable = _human(report)
    return store.store_report(
        report_id=report_id,
        report_type=report_type,
        period_start=period_start,
        period_end=period_end,
        generated_at=now,
        structured=report,
        human_readable=human_readable,
        source="neorome.control_plane_reporting",
        idempotency_key=report_id,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="/var/lib/neorome-control/control-plane.sqlite3")
    parser.add_argument("--paper-ledger", default="/var/lib/neorome-paper/paper.sqlite3")
    parser.add_argument("--report-type", choices=("morning", "close"))
    args = parser.parse_args()
    with ControlPlaneStore(args.database) as store:
        report = generate_report(store, now=datetime.now(UTC), paper_ledger=args.paper_ledger,
                                 forced_type=args.report_type)
    print(json.dumps({"report_id": report["report_id"], "status": report["status"]}, sort_keys=True))


if __name__ == "__main__":
    main()
