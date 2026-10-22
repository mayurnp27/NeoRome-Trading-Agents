"""Finite observed-shadow decision driver for EQ-INTRADAY.

The driver composes the one-shot evidence collector with a caller-supplied,
deterministic signal decision. It never makes a network request itself, starts
no schedule, opens no ledger, and emits no broker order. Every outcome—an
eligible *unfilled* candidate, abstention, rejection, or collection refusal—
is content-addressed beneath the caller-selected archive root.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

from .intraday_evidence_archive import canonical_bytes, sha256
from .intraday_observed_shadow import ObservedShadowRefusal, Provider, collect_once
from .intraday_paper_candidate import prepare_entry


Decision = Callable[[Mapping[str, Any]], Mapping[str, Any]]


class ShadowDecisionError(ValueError):
    """A supplied deterministic decision cannot be retained safely."""


def _json_safe(value: Any) -> Any:
    """Normalize sizing Decimals before retaining the immutable decision."""

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _retain(root: str | Path, value: Mapping[str, Any]) -> dict[str, str]:
    """Write a content-addressed decision record without overwriting it."""

    raw = canonical_bytes(dict(value))
    digest = sha256(dict(value))
    directory = Path(root) / "decisions"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ShadowDecisionError("decision digest path has inconsistent bytes")
    else:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    return {"decision_sha256": digest, "decision_path": str(path)}


def _abstention(root: str | Path, *, status: str, reason: str, context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    record = {
        "scope": "observed_shadow_decision_only",
        "status": status,
        "eligible_for_execution": False,
        "broker_order_count": 0,
        "reason": reason,
        "context": dict(context or {}),
    }
    return {**record, **_retain(root, record)}


def run_once(
    archive_root: str | Path,
    provider: Provider,
    decision: Decision,
    **collector_kwargs: Any,
) -> dict[str, Any]:
    """Collect once and retain one candidate or explicit no-trade outcome.

    ``decision`` receives only pre-signal raw universe/calendar/bar-prefix
    inputs and their hashes. It must return either ``{"action": "abstain",
    "reason": ...}`` or ``{"action": "candidate", "candidate": ...}``.
    Post-signal quotes are intentionally excluded from the decision input and
    used only by the existing conservative entry replay/sizing boundary.
    """

    if not callable(decision):
        raise ShadowDecisionError("decision must be callable")
    try:
        collected = collect_once(archive_root, provider, **collector_kwargs)
    except ObservedShadowRefusal as exc:
        return _abstention(archive_root, status="collection_refused", reason=str(exc))
    raw = {kind: Path(meta["path"]).read_bytes() for kind, meta in collected["archived"].items()}
    # Load fresh immutable bytes and decode through the canonical archive
    # representation; never use a provider object retained outside the hash.
    import json
    artifacts = {kind: json.loads(value) for kind, value in raw.items()}
    decision_input = {
        "signal_at": collected["signal_at"],
        "symbol": collected["symbol"],
        "evidence": collected["evidence"],
        "archive": {kind: collected["archived"][kind] for kind in ("universe", "calendar", "bar_prefix")},
        "raw": {kind: artifacts[kind]["payload"] for kind in ("universe", "calendar", "bar_prefix")},
    }
    supplied = decision(decision_input)
    if not isinstance(supplied, Mapping):
        return _abstention(archive_root, status="decision_refused", reason="decision must return an object")
    action = supplied.get("action")
    if action == "abstain":
        reason = supplied.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return _abstention(archive_root, status="decision_refused", reason="abstention reason is required")
        return _abstention(archive_root, status="abstained", reason=reason.strip(), context={"collected": collected})
    if action != "candidate" or not isinstance(supplied.get("candidate"), Mapping):
        return _abstention(archive_root, status="decision_refused", reason="decision action must be abstain or candidate")
    candidate = dict(supplied["candidate"])
    if candidate.get("symbol") != collected["symbol"] or candidate.get("signal_timestamp") != collected["signal_at"]:
        return _abstention(archive_root, status="rejected", reason="candidate must bind collected symbol and signal timestamp", context={"collected": collected})
    # Collected evidence is authoritative; a decision callback cannot swap
    # hashes/known-at fields after seeing pre-signal data.
    candidate.update(collected["evidence"])
    prepared = prepare_entry(candidate, artifacts["quotes"]["payload"], order_lifetime=collector_kwargs["order_lifetime"])
    record = {
        "scope": "observed_shadow_decision_only",
        "status": "eligible_unfilled_candidate" if prepared.get("accepted") else "rejected",
        "eligible_for_execution": False,
        "broker_order_count": 0,
        "collected": collected,
        # Retain the exact deterministic candidate alongside its prepared
        # reservation.  A later internal-paper runtime must reconstruct from
        # immutable decision evidence, not from a hand-written copy of these
        # controls after a process restart.
        "candidate": candidate,
        "prepared": prepared,
    }
    record = _json_safe(record)
    return {**record, **_retain(archive_root, record)}


__all__ = ["ShadowDecisionError", "run_once"]
