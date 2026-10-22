"""Bounded, read-only SPY corporate-action evidence collection.

The collector makes one GET against Alpaca's market-data corporate-actions
endpoint for a process-date window.  It consumes one New York calendar day
claim before provider I/O, never follows pagination, never retries, and never
places or prepares an order.  The evidence is a statement about the provider
response only; an empty response does not establish that no announcement
exists elsewhere or that the vendor is complete.
"""

from __future__ import annotations

import argparse
import base64
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from . import recorder
from .alpaca_paper import KEYCHAIN_KEY_ID_SERVICE, KEYCHAIN_SECRET_SERVICE
from .equity_data import NoRedirect


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
ORIGIN = "https://data.alpaca.markets"
PATH = "/v1/corporate-actions"
SYMBOL = "SPY"
LOOKBACK_DAYS = 365
FUTURE_DAYS = 365
MAX_RESPONSE_BYTES = 2_000_000
MAX_STATE_BYTES = 64 * 1024 * 1024
MAX_ACTIONS = 1000

ACTION_DATE_FIELDS: dict[str, tuple[str, ...]] = {
    "cash_dividends": ("ex_date",),
    "stock_dividends": ("ex_date",),
    "forward_splits": ("ex_date",),
    "reverse_splits": ("ex_date",),
    "unit_splits": ("ex_date",),
    "spinoffs": ("ex_date",),
    "mergers": ("effective_date", "ex_date"),
    "symbol_changes": ("effective_date",),
    "name_changes": ("effective_date",),
    "rights_offerings": ("ex_date",),
    "tender_offers": ("effective_date",),
    "liquidations": ("effective_date",),
    "delistings": ("effective_date",),
}


class CorporateActionError(Exception):
    """Safe collector error; provider details are not persisted."""


class StorageLimitError(CorporateActionError):
    pass


def _aware(value: datetime | None, label: str = "time") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CorporateActionError(f"{label} must be timezone aware")
    return value.astimezone(UTC)


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} is not a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"{label} is not a timestamp") from None
    try:
        return _aware(parsed, label)
    except CorporateActionError:
        raise ValueError(f"{label} is not a timestamp") from None


def _date_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label} is not a date")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        raise ValueError(f"{label} is not a date") from None


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError):
        raise CorporateActionError("Response is not valid JSON") from None


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _credential(service: str) -> str:
    """Use the recorder's configured environment/keychain provider."""
    return recorder.credentials(service)


def _query_window(now: datetime) -> tuple[str, str]:
    local_day = _aware(now).astimezone(NY).date()
    return (
        (local_day - timedelta(days=LOOKBACK_DAYS)).isoformat(),
        (local_day + timedelta(days=FUTURE_DAYS)).isoformat(),
    )


def _action_type(row: Mapping[str, Any], group: str | None = None) -> Any:
    if group is not None:
        return group
    for key in ("corporate_action_type", "action_type", "type"):
        if key in row:
            return row[key]
    return None


def _normalize_action(row: Any, group: str | None = None) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise CorporateActionError("Invalid corporate action row")
    # Keep provider fields intact while adding stable aliases used by the
    # shared equity contract.  Unknown or missing fields remain visible.
    result = dict(row)
    action_type = _action_type(row, group)
    result["action_type"] = action_type
    result["corporate_action_type"] = action_type
    result["ex_date"] = _date_text(row.get("ex_date"), "ex_date")
    result["effective_date"] = _date_text(
        row.get("effective_date", row.get("effectiveDate")), "effective_date"
    )
    result["process_date"] = _date_text(row.get("process_date"), "process_date")
    date_fields = ACTION_DATE_FIELDS.get(action_type, ())
    result["economic_date_fields"] = list(date_fields)
    result["economic_dates"] = {
        field: result.get(field) for field in date_fields if result.get(field) is not None
    }
    return result


def _extract_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    groups = payload.get("corporate_actions")
    if not isinstance(groups, Mapping):
        raise CorporateActionError("Invalid corporate action response")
    rows: list[dict[str, Any]] = []
    for group, group_rows in groups.items():
        if not isinstance(group, str) or not isinstance(group_rows, list):
            raise CorporateActionError("Invalid corporate action group")
        for row in group_rows:
            rows.append(_normalize_action(row, group))
            if len(rows) > MAX_ACTIONS:
                raise CorporateActionError("Too many corporate actions")
    return rows


class CorporateActionsClient:
    """One-request Alpaca corporate-action client with redirects disabled."""

    def __init__(
        self,
        opener: Any = None,
        credentials: Callable[[str], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.credentials = credentials or _credential
        self.clock = clock or (lambda: datetime.now(UTC))
        self.requests = 0

    def fetch(
        self,
        *,
        symbol: str = SYMBOL,
        start: str | None = None,
        end: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        if symbol != SYMBOL:
            raise CorporateActionError("Only SPY is supported")
        observed_at = _aware(now or self.clock(), "collection time")
        query_start, query_end = _query_window(observed_at)
        if start is not None:
            query_start = _date_text(start, "query start") or query_start
        if end is not None:
            query_end = _date_text(end, "query end") or query_end
        if date.fromisoformat(query_start) > date.fromisoformat(query_end):
            raise CorporateActionError("Invalid process-date window")
        try:
            key = self.credentials(KEYCHAIN_KEY_ID_SERVICE)
            secret = self.credentials(KEYCHAIN_SECRET_SERVICE)
        except Exception:
            raise CorporateActionError("Credentials unavailable") from None
        if not all(isinstance(value, str) and value for value in (key, secret)):
            raise CorporateActionError("Credentials unavailable")
        params = {
            "symbols": SYMBOL,
            "start": query_start,
            "end": query_end,
            "data_quality": "all",
            "limit": "1000",
        }
        url = ORIGIN + PATH + "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(
            url,
            headers={
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "Accept": "application/json",
            },
            method="GET",
        )
        requested_at = _aware(self.clock(), "request time")
        self.requests += 1
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 200:
                    raise CorporateActionError("Corporate-actions HTTP status not successful")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise CorporateActionError(f"Corporate-actions HTTP {exc.code}") from None
        except CorporateActionError:
            raise
        except Exception:
            raise CorporateActionError("Corporate-actions transport failure") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise CorporateActionError("Corporate-actions response too large")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError):
            raise CorporateActionError("Invalid corporate-actions JSON") from None
        if not isinstance(payload, dict):
            raise CorporateActionError("Invalid corporate-actions object")
        received_at = _aware(self.clock(), "receipt time")
        rows = _extract_rows(payload)
        actions = [_normalize_action(row) for row in rows]
        token = payload.get("next_page_token")
        if token is not None and not isinstance(token, str):
            raise CorporateActionError("Invalid pagination token")
        return {
            "payload": payload,
            "raw_response": raw,
            "raw_response_b64": base64.b64encode(raw).decode("ascii"),
            "actions": actions,
            "next_page_token": token,
            "query_start": query_start,
            "query_end": query_end,
            "requested_at": requested_at.isoformat(),
            "received_at": received_at.isoformat(),
            "source_hash": _sha256_bytes(raw),
        }


def normalize_evidence(result: Mapping[str, Any], *, collected_at: datetime | None = None) -> dict[str, Any]:
    """Build the cross-desk observed corporate-action evidence contract."""
    if not isinstance(result, Mapping):
        raise CorporateActionError("Invalid client result")
    actions = result.get("actions")
    if not isinstance(actions, list):
        raise CorporateActionError("Invalid normalized actions")
    received = _parse_time(result.get("received_at"), "receipt time")
    collection = _aware(collected_at or received, "collection time")
    token = result.get("next_page_token")
    return {
        "schema_version": 1,
        "source_kind": "observed",
        "symbol": SYMBOL,
        "collected_at": collection.isoformat(),
        "query_start": _date_text(result.get("query_start"), "query start"),
        "query_end": _date_text(result.get("query_end"), "query end"),
        "source_hash": result.get("source_hash"),
        "complete_response": not bool(token),
        "actions": actions,
        # Parsed provider payload is retained for audit alongside the exact
        # wire-byte hash.  It is bounded by MAX_RESPONSE_BYTES at fetch time.
        "raw_response": result.get("payload"),
        "raw_response_b64": result.get("raw_response_b64"),
        "provenance": {
            "provider": "alpaca",
            "endpoint": ORIGIN + PATH,
            "query_window_semantics": "process_date (provider default)",
            "data_quality": "all",
            "limit": 1000,
            "requested_at": result.get("requested_at"),
            "received_at": received.isoformat(),
            "pending_next_page": bool(token),
        },
        "known_empty_only": len(actions) == 0,
    }


def _state_size(root: Path) -> int:
    total = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise CorporateActionError("Symlink in state directory refused")
            try:
                total += path.stat().st_size
            except OSError:
                raise CorporateActionError("Unable to inspect state directory") from None
    return total


def _ensure_private_directory(state: str | os.PathLike[str]) -> Path:
    root = Path(state)
    if root.exists() and root.is_symlink():
        raise CorporateActionError("State directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise CorporateActionError("State directory is not private")
    try:
        os.chmod(root, 0o700)
    except OSError:
        raise CorporateActionError("State directory permissions unavailable") from None
    return root


def _atomic_json(path: Path, value: Any, root: Path, *, replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise CorporateActionError("State file symlink refused")
        if not replace:
            raise CorporateActionError("State file already exists")
    payload = _canonical(value) + b"\n"
    existing = path.stat().st_size if path.exists() else 0
    if len(payload) > MAX_STATE_BYTES or _state_size(root) - existing + len(payload) > MAX_STATE_BYTES:
        raise StorageLimitError("State storage limit")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".equity-actions-", dir=str(path.parent))
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            raise CorporateActionError("Unable to persist private state") from None
    except OSError:
        raise CorporateActionError("Unable to persist private state") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _read_json(path: Path) -> Any:
    if path.is_symlink():
        raise CorporateActionError("State file symlink refused")
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError, UnicodeError):
        raise CorporateActionError("Invalid private state") from None


def _lock(root: Path):
    try:
        descriptor = os.open(root / "actions.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BlockingIOError:
        raise CorporateActionError("Corporate-actions collection already running") from None
    except OSError:
        raise CorporateActionError("Unable to acquire collection lock") from None


def _compact(status: str, run_date: str, *, evidence_path: Path | None = None, detail: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "run_date": run_date,
        "source_kind": "observed" if evidence_path is not None else None,
        "symbol": SYMBOL,
        "execution_enabled": False,
        "orders_sent": 0,
    }
    if evidence_path is not None:
        result["evidence_path"] = str(evidence_path)
    if detail is not None:
        result["detail"] = detail
    return result


def collect_once(
    state: str | os.PathLike[str],
    *,
    client: CorporateActionsClient | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Claim one NY day and perform at most one read-only provider GET."""
    clock = clock or (lambda: datetime.now(UTC))
    before = _aware(clock(), "clock")
    run_date = before.astimezone(NY).date().isoformat()
    root = _ensure_private_directory(state)
    lock = _lock(root)
    try:
        if _state_size(root) >= MAX_STATE_BYTES:
            return _compact("storage_limit", run_date)
        claim_path = root / "claim.json"
        claim = _read_json(claim_path)
        if claim is not None:
            if not isinstance(claim, dict):
                return _compact("invalid_claim", run_date)
            claimed_date = claim.get("run_date")
            try:
                claimed_at = _parse_time(claim.get("claimed_at"), "claim time")
            except ValueError:
                return _compact("invalid_claim", run_date)
            if before < claimed_at:
                return _compact("clock_rollback", run_date)
            if claimed_date == run_date:
                evidence_path = root / "evidence" / f"{run_date}.json"
                if evidence_path.exists() and not evidence_path.is_symlink():
                    return _compact("same_day_replay", run_date, evidence_path=evidence_path)
                return _compact("same_day_replay", run_date)
        claim = {"schema_version": 1, "run_date": run_date, "claimed_at": before.isoformat(), "research_only": True}
        try:
            _atomic_json(claim_path, claim, root, replace=True)
        except StorageLimitError:
            return _compact("storage_limit", run_date)
        evidence_path = root / "evidence" / f"{run_date}.json"
        status_path = root / "status.json"
        if evidence_path.exists() or evidence_path.is_symlink():
            return _compact("same_day_replay", run_date, evidence_path=evidence_path)
        try:
            result = (client or CorporateActionsClient(clock=clock)).fetch(now=before)
            # Receipt time is the evidence collection time; the pre-I/O claim
            # time is only the daily idempotency boundary.
            evidence = normalize_evidence(result)
            _atomic_json(evidence_path, evidence, root)
            status = "blocked_pagination" if not evidence["complete_response"] else "observed"
            status_value = _compact(status, run_date, evidence_path=evidence_path)
            status_value.update({"complete_response": evidence["complete_response"], "known_actions": len(evidence["actions"])})
            _atomic_json(status_path, status_value, root, replace=True)
            return status_value
        except StorageLimitError:
            status_value = _compact("storage_limit", run_date)
            try:
                _atomic_json(status_path, status_value, root, replace=True)
            except CorporateActionError:
                pass
            return status_value
        except Exception as exc:
            # The claim remains consumed after any provider or persistence
            # failure; only a safe exception class is retained in status.
            status_value = _compact("fetch_or_validation_failed", run_date, detail=type(exc).__name__)
            try:
                _atomic_json(status_path, status_value, root, replace=True)
            except CorporateActionError:
                pass
            return status_value
    finally:
        lock.close()


def _date_field(action: Mapping[str, Any], field: str) -> str | None:
    value = action.get(field)
    if value is None and field == "effective_date":
        value = action.get("effectiveDate")
    return _date_text(value, field)


def _decode_evidence_payload(evidence: Mapping[str, Any]) -> tuple[bytes, Mapping[str, Any], list[dict[str, Any]], bool]:
    encoded = evidence.get("raw_response_b64")
    if not isinstance(encoded, str):
        raise CorporateActionError("raw response is missing")
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
        payload = json.loads(raw)
    except (ValueError, UnicodeError, json.JSONDecodeError):
        raise CorporateActionError("raw response is invalid") from None
    if len(raw) > MAX_RESPONSE_BYTES or not isinstance(payload, Mapping):
        raise CorporateActionError("raw response is invalid")
    actions = _extract_rows(payload)
    token = payload.get("next_page_token")
    if token is not None and not isinstance(token, str):
        raise CorporateActionError("pagination token is invalid")
    return raw, payload, actions, not bool(token)


def validate_evidence(evidence: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """Validate provenance and same-NY-day freshness without asserting absence."""
    reasons: list[str] = []
    current = _aware(now, "now")
    if not isinstance(evidence, Mapping):
        return {"status": "blocked", "reasons": ["evidence is not an object"]}
    if evidence.get("source_kind") != "observed": reasons.append("source_kind is not observed")
    if evidence.get("symbol") != SYMBOL: reasons.append("symbol is not SPY")
    try:
        collected = _parse_time(evidence.get("collected_at"), "collected_at")
        if collected.astimezone(NY).date() != current.astimezone(NY).date():
            reasons.append("evidence is not fresh for the current New York day")
        if collected > current:
            reasons.append("evidence is from the future")
    except ValueError:
        reasons.append("collected_at is invalid")
        collected = None
    source_hash = evidence.get("source_hash")
    if not isinstance(source_hash, str) or len(source_hash) != 64 or any(c not in "0123456789abcdef" for c in source_hash.lower()):
        reasons.append("source_hash is invalid")
    raw_actions: list[dict[str, Any]] | None = None
    raw_payload: Mapping[str, Any] | None = None
    raw_complete: bool | None = None
    try:
        raw, raw_payload, raw_actions, raw_complete = _decode_evidence_payload(evidence)
        if isinstance(source_hash, str) and source_hash != _sha256_bytes(raw):
            reasons.append("source_hash does not match raw response")
        if evidence.get("actions") != raw_actions:
            reasons.append("normalized actions do not match raw response")
        if evidence.get("complete_response") is not raw_complete:
            reasons.append("complete_response does not match raw response")
        if evidence.get("raw_response") is not None and evidence.get("raw_response") != raw_payload:
            reasons.append("parsed raw response does not match raw bytes")
    except CorporateActionError as exc:
        reasons.append(str(exc))
    for field in ("query_start", "query_end"):
        try:
            if _date_text(evidence.get(field), field) is None:
                reasons.append(f"{field} is missing")
        except ValueError:
            reasons.append(f"{field} is invalid")
    try:
        query_start = date.fromisoformat(str(evidence.get("query_start")))
        query_end = date.fromisoformat(str(evidence.get("query_end")))
        if collected is not None:
            collected_day = collected.astimezone(NY).date()
            expected_start = collected_day - timedelta(days=LOOKBACK_DAYS)
            expected_end = collected_day + timedelta(days=FUTURE_DAYS)
            if query_start != expected_start or query_end != expected_end:
                reasons.append("query window does not match the fixed NY 365-day bounds")
        if query_start > query_end:
            reasons.append("query window is reversed")
    except (TypeError, ValueError):
        pass
    provenance = evidence.get("provenance")
    if not isinstance(provenance, Mapping):
        reasons.append("provenance is missing")
        provenance = {}
    if provenance.get("provider") != "alpaca":
        reasons.append("provenance provider is invalid")
    if provenance.get("endpoint") != ORIGIN + PATH:
        reasons.append("provenance endpoint is invalid")
    if provenance.get("query_window_semantics") != "process_date (provider default)":
        reasons.append("provenance process-date semantics are invalid")
    if provenance.get("data_quality") != "all" or provenance.get("limit") != 1000:
        reasons.append("provenance request bounds are invalid")
    try:
        requested = _parse_time(provenance.get("requested_at"), "requested_at")
        received = _parse_time(provenance.get("received_at"), "received_at")
        if requested > received or received > current:
            reasons.append("request and receipt chronology is invalid")
        if collected is not None and received != collected:
            reasons.append("collected_at does not match receipt time")
    except ValueError:
        reasons.append("request or receipt timestamp is invalid")
    if evidence.get("complete_response") is not True:
        reasons.append("response has pending pagination or is incomplete")
    actions = raw_actions if raw_actions is not None else evidence.get("actions")
    if not isinstance(actions, list):
        reasons.append("actions is not a list")
    else:
        for index, action in enumerate(actions):
            if not isinstance(action, Mapping):
                reasons.append(f"action {index} is not an object")
                continue
            if action.get("symbol") != SYMBOL:
                reasons.append(f"action {index} has the wrong symbol")
            for field in ("ex_date", "effective_date", "process_date"):
                try:
                    _date_field(action, field)
                except ValueError:
                    reasons.append(f"action {index} has invalid {field}")
            kind = action.get("action_type")
            required = ACTION_DATE_FIELDS.get(kind, ())
            if not required:
                reasons.append(f"action {index} has unknown action type")
            elif any(action.get(field) is None for field in required):
                reasons.append(f"action {index} has incomplete economic dates")
    return {"status": "clear" if not reasons else "blocked", "reasons": reasons}


def action_clearance(
    evidence: Mapping[str, Any],
    *,
    open_date: date | str,
    now: datetime,
    lookahead_days: int = 1,
) -> dict[str, Any]:
    """Return ``clear`` only when known dated actions do not cross the hold.

    The interval includes the open date through now and a bounded New York
    calendar lookahead.  Any unknown action date or action type blocks.  A clear
    result means no known intersecting action in this provider response; it is
    not a completeness or corporate-ledger assertion.
    """
    validation = validate_evidence(evidence, now=now)
    reasons = list(validation["reasons"])
    intersections: list[dict[str, Any]] = []
    if type(lookahead_days) is not int or not 0 <= lookahead_days <= 7:
        reasons.append("lookahead_days must be an integer from 0 through 7")
        lookahead_days = 0
    try:
        start = date.fromisoformat(open_date) if isinstance(open_date, str) else open_date
        if not isinstance(start, date):
            raise ValueError
    except (TypeError, ValueError):
        reasons.append("open_date is invalid")
        start = None
    current = _aware(now, "now")
    current_day = current.astimezone(NY).date()
    end = current_day + timedelta(days=lookahead_days)
    if start is not None and start > current_day:
        reasons.append("open_date is in the future")
    if start is not None:
        try:
            query_start = date.fromisoformat(str(evidence.get("query_start")))
            query_end = date.fromisoformat(str(evidence.get("query_end")))
            if start < query_start or start > query_end:
                reasons.append("open_date is outside the evidence query window")
        except (TypeError, ValueError):
            pass
    actions = evidence.get("actions") if isinstance(evidence, Mapping) else None
    if isinstance(actions, list):
        for index, action in enumerate(actions):
            if not isinstance(action, Mapping):
                continue
            kind = action.get("action_type", action.get("corporate_action_type", action.get("type")))
            try:
                economic_fields = ACTION_DATE_FIELDS.get(kind, ())
                economic_dates = {
                    field: _date_field(action, field) for field in economic_fields
                }
            except ValueError:
                reasons.append(f"action {index} has invalid date")
                continue
            if not kind or not economic_fields or any(value is None for value in economic_dates.values()):
                reasons.append(f"action {index} has unknown type or dates")
                continue
            if start is None:
                continue
            dates = list(economic_dates.values())
            if any(start <= date.fromisoformat(value) <= end for value in dates):
                intersections.append({"index": index, "action_type": kind, "dates": dates})
    if intersections:
        reasons.append("known corporate action intersects open-through-tomorrow window")
    if reasons:
        result: dict[str, Any] = {"status": "blocked", "reasons": reasons}
    else:
        result = {"status": "clear", "reasons": []}
    result["intersections"] = intersections
    result["window"] = {"start": start.isoformat() if start else None, "end": end.isoformat(), "lookahead_days": lookahead_days}
    return result


# Explicit aliases make the contract discoverable to the shared equity worker.
collect = collect_once
collect_daily = collect_once
validate_fresh_evidence = validate_evidence
determine_action_clearance = action_clearance
clearance = action_clearance


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    args = parser.parse_args()
    result = collect_once(args.state)
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] in {"fetch_or_validation_failed", "storage_limit", "blocked_pagination"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
