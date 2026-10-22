"""Finite read-only Alpaca SIP intake for EQ-INTRADAY.

This coordinator is deliberately separate from the paper lifecycle.  It can
only call Alpaca calendar/bars/quotes GET endpoints, archives evidence, and
returns a retained unfilled candidate or an abstention.  It never calls an
account, order, position, or transfer endpoint and never opens a ledger.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone, time
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping
from decimal import Decimal
from zoneinfo import ZoneInfo

from desks.equities.intraday_deterministic_decision import deterministic_decision
from desks.equities.intraday_evidence_archive import EvidenceValidationError, archive_artifact, canonical_bytes, sha256, shadow_evidence
from desks.equities.intraday_paper_candidate import prepare_entry
from neorome.alpaca_paper import KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE
from neorome.recorder import credentials


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
DATA_ORIGIN = "https://data.alpaca.markets"
PAPER_ORIGIN = "https://paper-api.alpaca.markets"
MAX_REQUESTS = 4
MAX_RESPONSE_BYTES = 2_000_000
MAX_ARCHIVE_BYTES = 16_000_000
ORDER_LIFETIME = timedelta(seconds=1)


class SIPCaptureError(ValueError):
    """A bounded capture refusal with safe, structured diagnostic evidence."""

    def __init__(self, message: str, *, diagnostic: Mapping[str, Any] | None = None):
        super().__init__(message)
        # Diagnostics are deliberately metadata-only: never retain URLs with
        # query strings, response bodies, or credential-bearing exception text.
        self.diagnostic = dict(diagnostic or {})


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise SIPCaptureError("redirect refused")


def _time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise SIPCaptureError(f"{label} must be an ISO timestamp")
    try:
        value = re.sub(r"(\.\d{6})\d+(?=Z|[+-]\d\d:\d\d$)", r"\1", value)
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SIPCaptureError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise SIPCaptureError(f"{label} must include timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SIPCaptureError(f"{label} must be an object")
    return value


def _save_once(path: Path, value: Mapping[str, Any]) -> None:
    raw = canonical_bytes(dict(value))
    if path.exists():
        if path.is_symlink() or path.read_bytes() != raw:
            raise SIPCaptureError("output already exists with different bytes")
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(raw)
    path.chmod(0o600)


def _read_regular_json(path: Path, label: str) -> Mapping[str, Any]:
    """Read a regular final component with O_NOFOLLOW before parsing JSON."""

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise SIPCaptureError(f"{label} unavailable") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SIPCaptureError(f"{label} must be a regular file")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read()
    finally:
        os.close(fd)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SIPCaptureError(f"{label} unreadable") from exc
    return _object(value, label)


def _resolve_current_watchlist(path: Path, manifest: Mapping[str, Any], signal_at: datetime) -> Mapping[str, Any]:
    """Resolve a date-addressed pilot artifact from a regular current manifest."""

    local_signal = signal_at.astimezone(NY)
    expected_artifact = f"frozen-universe-{local_signal.date().isoformat()}.json"
    if (manifest.get("schema_version") != 1 or manifest.get("kind") != "prospective_fixed_watchlist_current"
            or manifest.get("session_date") != local_signal.date().isoformat()
            or manifest.get("artifact") != expected_artifact):
        raise SIPCaptureError("current watchlist manifest does not bind signal session")
    digest = manifest.get("artifact_sha256")
    artifact_path = path.parent / expected_artifact
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(artifact_path, flags)
    except OSError as exc:
        raise SIPCaptureError("current watchlist artifact unavailable") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SIPCaptureError("current watchlist artifact must be a regular file")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read()
    finally:
        os.close(fd)
    if not isinstance(digest, str) or hashlib.sha256(raw).hexdigest() != digest:
        raise SIPCaptureError("current watchlist artifact hash mismatch")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SIPCaptureError("current watchlist artifact unreadable") from exc
    return _object(value, "current watchlist artifact")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


class SIPClient:
    """Four-request, no-redirect read-only client using recorder credentials."""

    def __init__(self, *, opener=None, credential_loader: Callable[[str], str] = credentials,
                 clock: Callable[[], datetime] = lambda: datetime.now(UTC)):
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.credential_loader, self.clock, self.requests = credential_loader, clock, 0
        self._keys: tuple[str, str] | None = None

    def get(self, origin: str, path: str, params: Mapping[str, str]) -> tuple[Any, dict[str, Any]]:
        if self.requests >= MAX_REQUESTS:
            raise SIPCaptureError("read-only request budget exhausted")
        allowed = {
            (PAPER_ORIGIN, "/v2/calendar"),
            (DATA_ORIGIN, "/v2/stocks/bars"),
            (DATA_ORIGIN, "/v2/stocks/quotes/latest"),
        }
        if (origin, path) not in allowed or params.get("feed") not in {None, "sip"}:
            raise SIPCaptureError("unsupported non-SIP or non-read-only endpoint")
        if self._keys is None:
            try:
                keys = (self.credential_loader(KEYCHAIN_KEY_ID_SERVICE), self.credential_loader(KEYCHAIN_SECRET_SERVICE))
            except Exception as exc:
                raise SIPCaptureError("secure Alpaca credential unavailable") from exc
            if any(not isinstance(key, str) or not key for key in keys):
                raise SIPCaptureError("secure Alpaca credential unavailable")
            self._keys = keys
        requested = self.clock()
        request = urllib.request.Request(origin + path + "?" + urllib.parse.urlencode(params), headers={
            "APCA-API-KEY-ID": self._keys[0], "APCA-API-SECRET-KEY": self._keys[1], "Accept": "application/json"}, method="GET")
        self.requests += 1
        diagnostic = {
            "provider": "alpaca",
            "endpoint": path,
            "feed": params.get("feed", "calendar"),
            "request_count": self.requests,
            "requested_at": _iso(requested),
            "execution_enabled": False,
            "fallback_used": False,
        }
        try:
            with self.opener.open(request, timeout=20) as response:
                status, raw = response.status, response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            # Preserve the access-denied class without retaining URL,
            # headers, response body, or credential-bearing error text.
            status = int(exc.code) if isinstance(exc.code, int) else 0
            diagnostic.update({"http_status": status, "failure_class": "provider_access_denied" if status == 403 else "provider_http_error"})
            raise SIPCaptureError(f"provider_http_{status}", diagnostic=diagnostic) from None
        except Exception as exc:
            raise SIPCaptureError("provider_transport_failure") from exc
        if status != 200:
            diagnostic.update({"http_status": status, "failure_class": "provider_http_error"})
            raise SIPCaptureError(f"provider_http_{status}", diagnostic=diagnostic)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise SIPCaptureError("provider_response_too_large")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SIPCaptureError("provider_response_invalid_json") from exc
        if not isinstance(payload, (dict, list)):
            raise SIPCaptureError("provider_response_not_object")
        return payload, {"provider": "alpaca", "feed": params.get("feed", "calendar"), "path": path,
                         "requested_at": _iso(requested), "received_at": _iso(self.clock()),
                         "response_sha256": hashlib.sha256(raw).hexdigest(), "request_count": self.requests}


def load_frozen_universe(path: str | Path, *, signal_at: datetime, symbol: str) -> dict[str, Any]:
    target = Path(path)
    source = _read_regular_json(target, "PIT universe file")
    if source.get("kind") == "prospective_fixed_watchlist_current":
        source = _resolve_current_watchlist(target, source, signal_at)
    selection = _time(source.get("selection_at"), "universe selection_at")
    if selection >= signal_at:
        raise SIPCaptureError("PIT universe was selected after signal")
    members = source.get("members")
    method = source.get("method", "historical_dynamic_pit")
    if method == "prospective_fixed_watchlist":
        local_selection, local_signal = selection.astimezone(NY), signal_at.astimezone(NY)
        if source.get("frozen") is not True or source.get("historical_claim") is not False:
            raise SIPCaptureError("prospective fixed watchlist must be frozen and non-historical")
        if source.get("session_date") != local_signal.date().isoformat() or local_selection.date() > local_signal.date():
            raise SIPCaptureError("prospective fixed watchlist does not bind signal session")
        if (local_selection.time() >= time(9, 30)
                and local_selection.date() == local_signal.date()):
            raise SIPCaptureError("prospective fixed watchlist was not frozen before session")
        if not isinstance(members, list) or source.get("membership_sha256") != sha256(members):
            raise SIPCaptureError("prospective fixed watchlist membership is not content-addressed")
        member = lambda row: isinstance(row, Mapping) and row.get("symbol") == symbol and row.get("watchlist_member") is True
    else:
        history = _object(source.get("historical_membership"), "historical membership")
        if history.get("includes_delisted") is not True:
            raise SIPCaptureError("PIT universe lacks explicit delisted coverage")
        member = lambda row: isinstance(row, Mapping) and row.get("symbol") == symbol and row.get("eligible") is True
    if not isinstance(members, list) or not any(member(row) for row in members):
        raise SIPCaptureError("PIT universe does not explicitly admit symbol")
    return dict(source)


def _calendar_payload(raw: Any, signal: datetime) -> dict[str, Any]:
    rows = raw.get("calendar", raw) if isinstance(raw, Mapping) else raw
    if not isinstance(rows, list) or len(rows) != 1:
        raise SIPCaptureError("calendar must contain exactly one session")
    row = _object(rows[0], "calendar row")
    close = row.get("close")
    if not isinstance(close, str):
        raise SIPCaptureError("calendar close missing")
    parsed = _time(close if "T" in close else signal.date().isoformat() + "T" + close + "-04:00", "calendar close")
    return {"session_close": _iso(parsed)}


def _bar_payload(raw: Mapping[str, Any], symbol: str, signal: datetime) -> list[dict[str, Any]]:
    rows = _object(raw.get("bars"), "bars").get(symbol)
    if not isinstance(rows, list) or not rows:
        raise SIPCaptureError("SIP bar prefix missing requested symbol")
    result = []
    for row in rows:
        item = _object(row, "bar")
        stamp = _time(item.get("t"), "bar timestamp")
        if stamp >= signal:
            raise SIPCaptureError("bar prefix includes post-signal bar")
        result.append({"symbol": symbol, "timestamp": _iso(stamp), "open": item.get("o"), "high": item.get("h"),
                       "low": item.get("l"), "close": item.get("c"), "volume": item.get("v"),
                       "vwap": item.get("vw"), "quality_clear": True})
    result.sort(key=lambda row: row["timestamp"])
    return result


def _quote_payload(raw: Mapping[str, Any], symbol: str) -> list[dict[str, Any]]:
    rows = _object(raw.get("quotes"), "quotes").get(symbol)
    if isinstance(rows, Mapping):
        rows = [rows]
    if not isinstance(rows, list) or not rows:
        raise SIPCaptureError("SIP quote response missing requested symbol")
    return [{"symbol": symbol, "timestamp": row.get("t"), "bid": row.get("bp"), "ask": row.get("ap"),
             "bid_size": row.get("bs"), "ask_size": row.get("as")} for row in rows if isinstance(row, Mapping)]


def capture_decision(
    archive_root: str | Path, universe_path: str | Path, symbol: str, *, client: SIPClient,
    signal_at: datetime | None = None, fee_cents_per_unit: int = 0, units: int = 1,
) -> dict[str, Any]:
    """Capture one bounded prospective bundle and retain a decision/abstention."""

    initial = (signal_at or client.clock()).astimezone(UTC)
    try:
        universe = load_frozen_universe(universe_path, signal_at=initial, symbol=symbol)
        calendar_raw, calendar_meta = client.get(PAPER_ORIGIN, "/v2/calendar", {"start": initial.date().isoformat(), "end": initial.date().isoformat()})
        bars_raw, bars_meta = client.get(DATA_ORIGIN, "/v2/stocks/bars", {"symbols": symbol, "timeframe": "1Min", "start": initial.replace(hour=13, minute=30, second=0, microsecond=0).isoformat(), "end": _iso(initial), "adjustment": "raw", "feed": "sip", "limit": "500", "sort": "asc"})
        # The actual local decision time is after the complete prefix is read;
        # this is deliberately conservative versus the provider bar timestamp.
        signal = (signal_at or client.clock()).astimezone(UTC)
        bars = _bar_payload(bars_raw, symbol, signal)
        calendar = _calendar_payload(calendar_raw, signal)
        quote_raw, quote_meta = client.get(DATA_ORIGIN, "/v2/stocks/quotes/latest", {"symbols": symbol, "feed": "sip"})
        quotes = _quote_payload(quote_raw, symbol)
        artifacts = {
            "universe": {"kind": "universe", "source": universe.get("source"), "source_version": universe.get("source_version"), "selection_at": universe.get("selection_at"), "known_at": universe.get("selection_at"), "payload": universe.get("members"), "raw_response": universe},
            "calendar": {"kind": "calendar", "source": "alpaca", "source_version": "sip-v2", "known_at": calendar_meta["received_at"], "payload": calendar, "raw_response": calendar_raw},
            "bar_prefix": {"kind": "bar_prefix", "source": "alpaca", "source_version": "sip-v2", "known_at": bars_meta["received_at"], "timeframe": "1Min", "adjustment": "raw", "payload": bars, "raw_response": bars_raw},
            "quotes": {"kind": "quotes", "source": "alpaca", "source_version": "sip-v2", "known_at": quote_meta["received_at"], "size_units": "shares", "size_units_evidence": "Alpaca market-data SIP quote size fields, v2", "payload": [{**row, "received_at": quote_meta["received_at"]} for row in quotes], "raw_response": quote_raw},
        }
        archived = {kind: archive_artifact(archive_root, item, signal_at=_iso(signal) if kind != "quotes" else None) for kind, item in artifacts.items()}
        evidence = shadow_evidence(artifacts["universe"], artifacts["bar_prefix"], artifacts["calendar"], artifacts["quotes"], signal_at=_iso(signal))
        raw = {"universe": artifacts["universe"]["payload"], "universe_method": universe.get("method"),
               "calendar": calendar, "bar_prefix": bars, "bar_prefix_quality_clear": True}
        decision = deterministic_decision({"signal_at": _iso(signal), "symbol": symbol, "raw": raw, "evidence": evidence}, fee_cents_per_unit=fee_cents_per_unit, units=units)
        if decision.get("action") != "candidate":
            result = {"scope": "observed_shadow_decision_only", "status": "abstained", "eligible_for_execution": False, "broker_order_count": 0, "reason": decision.get("reason"), "collected": {"archived": archived, "evidence": evidence}}
        else:
            candidate = decision["candidate"]
            prepared = prepare_entry(candidate, artifacts["quotes"]["payload"], order_lifetime=ORDER_LIFETIME)
            result = {"scope": "observed_shadow_decision_only", "status": "eligible_unfilled_candidate" if prepared.get("accepted") else "rejected", "eligible_for_execution": False, "broker_order_count": 0, "candidate": candidate, "prepared": prepared, "collected": {"archived": archived, "evidence": evidence}}
    except (SIPCaptureError, EvidenceValidationError) as exc:
        result = {"scope": "observed_shadow_decision_only", "status": "abstained", "eligible_for_execution": False, "broker_order_count": 0, "reason": str(exc), "gate": "sip_or_pit_unavailable"}
        if isinstance(exc, SIPCaptureError) and exc.diagnostic:
            result["diagnostic"] = exc.diagnostic
    result = _json_safe(result)
    result["broker_orders_sent"] = 0
    digest = sha256(result)
    _save_once(Path(archive_root) / "decisions" / f"{digest}.json", result)
    return {**result, "decision_sha256": digest}


def capture_later_quote(archive_root: str | Path, symbol: str, phase: str, *, client: SIPClient) -> dict[str, Any]:
    """Capture one independent SIP quote for a pending entry or planned exit.

    The caller passes the returned ``quote`` to ``equity_intraday_paper``;
    no ledger is opened here.  The response hash is retained beside the raw
    envelope and bound into the quote as ``source_sha256``.
    """

    if phase not in {"entry", "exit"}:
        raise SIPCaptureError("quote phase must be entry or exit")
    try:
        raw, meta = client.get(DATA_ORIGIN, "/v2/stocks/quotes/latest", {"symbols": symbol, "feed": "sip"})
        rows = _quote_payload(raw, symbol)
        if len(rows) != 1:
            raise SIPCaptureError("independent SIP quote must contain exactly one quote")
        quote = {**rows[0], "received_at": meta["received_at"], "source_sha256": meta["response_sha256"]}
        artifact = {"kind": "quotes", "source": "alpaca", "source_version": "sip-v2", "known_at": meta["received_at"],
                    "size_units": "shares", "size_units_evidence": "Alpaca market-data SIP quote size fields, v2",
                    "payload": [quote], "raw_response": raw}
        archived = archive_artifact(archive_root, artifact)
        result = {"status": "captured", "phase": phase, "quote": quote, "archived": archived,
                  "broker_orders_sent": 0, "execution_enabled": False}
    except (SIPCaptureError, EvidenceValidationError) as exc:
        result = {"status": "abstained", "phase": phase, "reason": str(exc), "broker_orders_sent": 0,
                  "execution_enabled": False}
        if isinstance(exc, SIPCaptureError) and exc.diagnostic:
            result["diagnostic"] = exc.diagnostic
    result = _json_safe(result)
    digest = sha256(result)
    _save_once(Path(archive_root) / "later-quotes" / f"{digest}.json", result)
    return {**result, "capture_sha256": digest}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", required=True); parser.add_argument("--universe", required=True)
    parser.add_argument("--symbol", required=True); parser.add_argument("--signal-at")
    parser.add_argument("--phase", choices=["decision", "entry", "exit"], default="decision")
    parser.add_argument("--fee-cents-per-unit", type=int, default=0); parser.add_argument("--units", type=int, default=1)
    args = parser.parse_args(argv)
    signal = _time(args.signal_at, "signal_at") if args.signal_at else None
    client = SIPClient()
    result = (capture_decision(args.archive_root, args.universe, args.symbol, client=client, signal_at=signal,
                               fee_cents_per_unit=args.fee_cents_per_unit, units=args.units)
              if args.phase == "decision" else capture_later_quote(args.archive_root, args.symbol, args.phase, client=client))
    print(json.dumps({"status": result["status"], "reason": result.get("reason"),
                      "diagnostic": result.get("diagnostic"), "broker_orders_sent": 0,
                      "decision_sha256": result.get("decision_sha256"), "capture_sha256": result.get("capture_sha256")}, sort_keys=True))
    return 0 if result["status"] in {"eligible_unfilled_candidate", "abstained", "rejected"} else 1


if __name__ == "__main__": raise SystemExit(main())
