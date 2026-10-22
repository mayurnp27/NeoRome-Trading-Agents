"""Chronological, retained-trial evaluation gate for EQ-INTRADAY.

It records whether a session is eligible for a prospective shadow decision;
it does not calculate P&L, fill an order, or turn unavailable data into a
negative/positive strategy result.
"""

from __future__ import annotations

from datetime import date
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence


class ChronologicalEvaluationError(ValueError):
    pass


REQUIRED_EVIDENCE = {"universe_evidence_sha256", "bar_prefix_sha256", "quote_evidence_sha256"}


def _day(value: Any) -> date:
    if not isinstance(value, str):
        raise ChronologicalEvaluationError("session_date must be ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ChronologicalEvaluationError("session_date must be ISO date") from exc


def evaluate_sessions(sessions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Retain every session and explicitly gate incomplete evidence as unavailable."""

    previous: date | None = None
    retained: list[dict[str, Any]] = []
    for item in sessions:
        if not isinstance(item, Mapping):
            raise ChronologicalEvaluationError("session must be an object")
        day = _day(item.get("session_date"))
        if previous is not None and day <= previous:
            raise ChronologicalEvaluationError("sessions must be strictly chronological and unique")
        previous = day
        evidence = item.get("evidence")
        reason: str | None = None
        if not isinstance(evidence, Mapping):
            reason = "missing_evidence"
        elif not REQUIRED_EVIDENCE.issubset(evidence) or any(not isinstance(evidence.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", evidence[key]) is None for key in REQUIRED_EVIDENCE):
            reason = "incomplete_evidence_hashes"
        elif item.get("prefix_complete_through_signal") is not True:
            reason = "incomplete_bar_prefix"
        elif item.get("bar_quality_clear") is not True:
            reason = "bar_quality_quarantine_or_unknown"
        elif item.get("size_units_verified") is not True:
            reason = "quote_size_units_unverified"
        retained.append({"session_date": day.isoformat(), "status": "shadow_eligible" if reason is None else "unavailable", "reason": reason, "candidate_id": item.get("candidate_id")})
    return {"scope": "chronological_shadow_gate", "sessions": retained,
            "eligible_sessions": sum(row["status"] == "shadow_eligible" for row in retained),
            "unavailable_sessions": sum(row["status"] == "unavailable" for row in retained)}


def evaluate_holdout(
    sessions: Sequence[Mapping[str, Any]],
    *,
    development_end: str,
    holdout_start: str,
) -> dict[str, Any]:
    """Gate a pre-declared chronological holdout without calculating merit.

    The caller supplies raw session evidence, never prior gate output.  The
    two dates divide the retained sequence into development and holdout; a
    session in the gap is retained but explicitly unavailable for either
    result.  This preserves an embargo rather than silently reusing it.
    """

    development_boundary = _day(development_end)
    holdout_boundary = _day(holdout_start)
    if holdout_boundary <= development_boundary:
        raise ChronologicalEvaluationError("holdout_start must be after development_end")
    gated = evaluate_sessions(sessions)
    development: list[dict[str, Any]] = []
    holdout: list[dict[str, Any]] = []
    embargoed: list[dict[str, Any]] = []
    for row in gated["sessions"]:
        day = _day(row["session_date"])
        if day <= development_boundary:
            development.append(row)
        elif day >= holdout_boundary:
            holdout.append(row)
        else:
            embargoed.append(row)
    return {
        "scope": "chronological_holdout_gate",
        "development_end": development_boundary.isoformat(),
        "holdout_start": holdout_boundary.isoformat(),
        "development": development,
        "holdout": holdout,
        "embargoed": embargoed,
        "holdout_eligible_sessions": sum(row["status"] == "shadow_eligible" for row in holdout),
        "holdout_unavailable_sessions": sum(row["status"] == "unavailable" for row in holdout),
        "merit_status": "unavailable_pending_outcomes",
    }


def retain_trial(path: str | Path, trial: Mapping[str, Any]) -> dict[str, Any]:
    """Create one immutable content-addressed trial record; no overwrites."""

    outcome = evaluate_sessions(trial.get("sessions", [])) if isinstance(trial, Mapping) else None
    if outcome is None:
        raise ChronologicalEvaluationError("trial must be an object")
    payload = {"trial": dict(trial), "outcome": outcome}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    digest = hashlib.sha256(raw).hexdigest()
    target = Path(path) / f"{digest}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.read_bytes() != raw:
            raise ChronologicalEvaluationError("trial digest collision")
    else:
        target.write_bytes(raw)
    return {"digest": digest, "path": str(target), "outcome": outcome}


__all__ = ["ChronologicalEvaluationError", "evaluate_holdout", "evaluate_sessions", "retain_trial"]
