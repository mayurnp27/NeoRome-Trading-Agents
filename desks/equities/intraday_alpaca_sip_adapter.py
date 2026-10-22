"""Offline normalizer for caller-supplied Alpaca SIP captures.

No credentials, URLs, or HTTP client live here.  It turns four already saved
responses into the callback protocol consumed by ``intraday_observed_shadow``
and retains each original response envelope in the resulting archive hash.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")


class AlpacaSIPCaptureError(ValueError):
    pass


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AlpacaSIPCaptureError(f"{name} must be an object")
    return value


def _sip(capture: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    request = _mapping(capture.get("request"), f"{kind} request")
    if request.get("feed") != "sip":
        raise AlpacaSIPCaptureError(f"{kind} capture must explicitly use SIP")
    return request


def _calendar_close(row: Mapping[str, Any]) -> str:
    close = row.get("close")
    if not isinstance(close, str):
        raise AlpacaSIPCaptureError("calendar close is required")
    parsed = datetime.fromisoformat(close.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NY)
    return parsed.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")


def provider_from_captures(
    captures: Mapping[str, Any],
    universe: Mapping[str, Any],
    *,
    quote_size_documentation: str,
) -> callable:
    """Return a bounded callback over immutable caller-supplied captures.

    ``universe`` must contain source/version/selection timestamp/members and a
    historical-membership provenance object with explicit delisted coverage.
    This prevents the adapter from relabeling a current-survivor list as a
    point-in-time historical universe.
    """

    captures = _mapping(captures, "captures")
    universe = _mapping(universe, "universe")
    history = _mapping(universe.get("historical_membership"), "historical_membership")
    if history.get("includes_delisted") is not True:
        raise AlpacaSIPCaptureError("historical membership must explicitly include delisted securities")
    if not isinstance(quote_size_documentation, str) or not quote_size_documentation.strip():
        raise AlpacaSIPCaptureError("quote size documentation reference is required")

    def provider(kind: str, request: Mapping[str, Any]) -> Mapping[str, Any]:
        symbol = request.get("symbol")
        if kind == "universe":
            return {"source": universe.get("source"), "source_version": universe.get("source_version"),
                    "selection_at": universe.get("selection_at"), "payload": universe.get("members"),
                    "raw_response": {"universe": dict(universe)}}
        capture = _mapping(captures.get(kind), f"{kind} capture")
        if kind == "calendar":
            rows = capture.get("calendar")
            if not isinstance(rows, list) or len(rows) != 1:
                raise AlpacaSIPCaptureError("calendar capture must contain exactly one requested session")
            return {"source": "alpaca-sip", "source_version": "alpaca-capture-v1",
                    "payload": {"session_close": _calendar_close(_mapping(rows[0], "calendar row"))},
                    "raw_response": dict(capture)}
        request_meta = _sip(capture, kind)
        if kind == "bar_prefix":
            if request_meta.get("timeframe") != "1Min":
                raise AlpacaSIPCaptureError("bar capture must explicitly use 1Min")
            rows = _mapping(capture.get("bars"), "bars").get(symbol)
            if not isinstance(rows, list):
                raise AlpacaSIPCaptureError("bar capture is missing requested symbol")
            return {"source": "alpaca-sip", "source_version": "alpaca-capture-v1", "timeframe": "1Min",
                    "adjustment": capture.get("adjustment", "unknown"), "payload": rows, "raw_response": dict(capture)}
        if kind == "quotes":
            rows = _mapping(capture.get("quotes"), "quotes").get(symbol)
            if not isinstance(rows, list):
                raise AlpacaSIPCaptureError("quote capture is missing requested symbol")
            normalized = [{**dict(row), "bid": row.get("bid_price"), "ask": row.get("ask_price"),
                           "bid_size": row.get("bid_size"), "ask_size": row.get("ask_size")} for row in rows]
            return {"source": "alpaca-sip", "source_version": "alpaca-capture-v1", "payload": normalized,
                    "size_units": "shares", "size_units_documentation": quote_size_documentation,
                    "raw_response": dict(capture)}
        raise AlpacaSIPCaptureError(f"unsupported capture kind {kind}")
    return provider


__all__ = ["AlpacaSIPCaptureError", "provider_from_captures"]
