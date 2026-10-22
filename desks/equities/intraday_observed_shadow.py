"""One-shot, fail-closed intake for prospective EQ-INTRADAY shadow evidence.

This module deliberately does not know how to authenticate, make HTTP calls,
or schedule itself.  An approved caller supplies a bounded provider callback;
the collector records its *own* receipt timestamps immediately after each
callback returns, archives exact supplied payloads, and produces only an
unfilled shadow-input record.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .intraday_evidence_archive import EvidenceValidationError, archive_artifact, shadow_evidence
from .intraday_quote_replay import DEFAULT_LATENCY


UTC = timezone.utc
Provider = Callable[[str, Mapping[str, Any]], Mapping[str, Any]]


class ObservedShadowRefusal(ValueError):
    """A provider response cannot support an observed-shadow record."""


def _utc(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ObservedShadowRefusal(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _response(kind: str, value: Any, received_at: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ObservedShadowRefusal(f"{kind} provider response must be an object")
    for field in ("source", "source_version", "payload"):
        if field not in value:
            raise ObservedShadowRefusal(f"{kind} provider response is missing {field}")
    artifact: dict[str, Any] = {
        "kind": kind,
        "source": value["source"],
        "source_version": value["source_version"],
        "known_at": _iso(received_at),
        "payload": value["payload"],
    }
    # Keep the bounded raw provider envelope alongside normalized rows. It is
    # included in the content hash but never interpreted by strategy logic.
    if "raw_response" in value:
        artifact["raw_response"] = value["raw_response"]
    if kind == "universe":
        artifact["selection_at"] = value.get("selection_at")
    elif kind == "bar_prefix":
        artifact["timeframe"] = value.get("timeframe")
        artifact["adjustment"] = value.get("adjustment")
    elif kind == "quotes":
        # The collector, not an upstream response, supplies receipt time. A
        # bounded batch gets the same local receipt instant for every row.
        rows = value["payload"]
        if not isinstance(rows, (list, tuple)):
            raise ObservedShadowRefusal("quotes provider payload must be a sequence")
        artifact["payload"] = [{**dict(row), "received_at": _iso(received_at)} if isinstance(row, Mapping) else row for row in rows]
        artifact["size_units"] = value.get("size_units")
        # This must be a provider document/version reference, not a boolean
        # assertion from a caller. Archive validation enforces its presence.
        artifact["size_units_evidence"] = value.get("size_units_documentation")
    return artifact


def collect_once(
    archive_root: str | Path,
    provider: Provider,
    *,
    signal_at: datetime,
    symbol: str,
    order_lifetime: timedelta,
    clock: Callable[[], datetime],
) -> dict[str, Any]:
    """Collect one bounded prospective input set and return no-trade evidence.

    The callback receives ``universe``, ``calendar``, ``bar_prefix`` and
    ``quotes`` in that order along with a small request mapping. It must return
    raw payload/source metadata only. The collector never assumes that a
    response is broker-executable or that a successful archive is a strategy
    signal; a later deterministic screener may decide either question.
    """

    if not callable(provider) or not callable(clock):
        raise ObservedShadowRefusal("provider and clock must be callable")
    signal = _utc(signal_at, "signal_at")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ObservedShadowRefusal("symbol is required")
    if not isinstance(order_lifetime, timedelta) or order_lifetime <= timedelta(0):
        raise ObservedShadowRefusal("order_lifetime must be positive")
    deadline = signal + DEFAULT_LATENCY + order_lifetime
    requests = {
        "universe": {"as_of": _iso(signal), "symbol": symbol.strip()},
        "calendar": {"session_date": signal.date().isoformat()},
        "bar_prefix": {"symbol": symbol.strip(), "end": _iso(signal), "timeframe": "1Min"},
        "quotes": {"symbol": symbol.strip(), "after": _iso(signal), "before": _iso(deadline)},
    }
    artifacts: dict[str, dict[str, Any]] = {}
    archived: dict[str, dict[str, Any]] = {}
    try:
        for kind in ("universe", "calendar", "bar_prefix", "quotes"):
            raw = provider(kind, requests[kind])
            received_at = _utc(clock(), f"{kind} local receipt")
            if kind != "quotes" and received_at > signal:
                raise ObservedShadowRefusal(f"{kind} was locally received after signal_at")
            if kind == "quotes" and received_at > deadline:
                raise ObservedShadowRefusal("quotes were locally received after bounded order deadline")
            artifact = _response(kind, raw, received_at)
            artifacts[kind] = artifact
            archived[kind] = archive_artifact(
                archive_root, artifact, signal_at=_iso(signal) if kind != "quotes" else None
            )
        evidence = shadow_evidence(
            artifacts["universe"], artifacts["bar_prefix"], artifacts["calendar"], artifacts["quotes"],
            signal_at=_iso(signal),
        )
    except EvidenceValidationError as exc:
        raise ObservedShadowRefusal(str(exc)) from exc
    return {
        "status": "collected_unfilled_shadow_input",
        "scope": "prospective_observed_shadow_evidence_only",
        "eligible_for_execution": False,
        "broker_order_count": 0,
        "signal_at": _iso(signal),
        "order_deadline": _iso(deadline),
        "symbol": symbol.strip(),
        "archived": archived,
        "evidence": evidence,
        "admission_status": "unavailable_pending_deterministic_signal_and_outcome_evidence",
    }


__all__ = ["ObservedShadowRefusal", "collect_once"]
