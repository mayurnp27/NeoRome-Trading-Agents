"""Bounded, replay-safe public prediction-market observation.

The collector runs continuously through a short systemd timer, but it never
places a venue order and never writes the shared paper ledger.  It records
immutable market and order-book responses, evaluates each market against the
same causal admission requirements used by the observed-shadow route, and
returns exact blocker codes when the public evidence is insufficient.

The public API intentionally has no retry or pagination loop.  A later run
may capture the next bounded snapshot; an incomplete response is retained as
such rather than silently treated as a complete market universe.
"""

from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping, Sequence

from .equity_data import NoRedirect


UTC = timezone.utc
VERSION = "prediction-24x7-v1"
ORIGIN = "https://api.elections.kalshi.com"
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_STORAGE_BYTES = 256 * 1024 * 1024
MAX_REQUESTS = 8
# Enrichment spends one bounded metadata request on the series and event for
# each selected family.  One market per family keeps the complete run within
# the eight-request ceiling (listing, series, event, and book x two).
MAX_MARKETS_PER_SERIES = 1
SLOT = timedelta(minutes=5)
DEFAULT_SERIES = ("KXBTC15M", "KXHIGHNY")
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9_-]{1,127}$")


class Prediction24x7Error(ValueError):
    """A bounded public observation could not be safely recorded."""


def _time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise Prediction24x7Error(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise Prediction24x7Error(f"{field} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Prediction24x7Error(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise Prediction24x7Error("value is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _root(value: str | os.PathLike[str]) -> Path:
    root = Path(value)
    if root.exists() and root.is_symlink():
        raise Prediction24x7Error("state directory symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    return root


def _size(root: Path) -> int:
    total = 0
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(directory) / name
            if path.is_symlink():
                raise Prediction24x7Error("symlink in state directory refused")
            total += path.stat().st_size
    return total


@contextmanager
def _lock(root: Path):
    try:
        descriptor = os.open(root / "prediction-24x7.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stream = os.fdopen(descriptor, "a+")
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise Prediction24x7Error("already_running") from exc
    except OSError as exc:
        raise Prediction24x7Error("unable to acquire scanner lock") from exc
    try:
        yield
    finally:
        stream.close()


def _write_once(path: Path, value: Any, root: Path, *, replace: bool = False) -> None:
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise Prediction24x7Error("state file symlink refused")
        if not replace:
            raise Prediction24x7Error("immutable state file already exists")
    payload = gzip.compress(_canonical(value) + b"\n", compresslevel=6, mtime=0) if path.suffix == ".gz" else _canonical(value) + b"\n"
    existing = path.stat().st_size if path.exists() else 0
    if len(payload) > MAX_STORAGE_BYTES or _size(root) - existing + len(payload) > MAX_STORAGE_BYTES:
        raise Prediction24x7Error("storage_limit")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".prediction-24x7-", dir=str(path.parent))
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = ""
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _slot_start(value: datetime) -> datetime:
    current = _time(value, "clock")
    seconds = int(current.timestamp())
    slot_seconds = int(SLOT.total_seconds())
    return datetime.fromtimestamp(seconds - seconds % slot_seconds, UTC)


def _json_body(raw: bytes) -> Mapping[str, Any]:
    if len(raw) > MAX_RESPONSE_BYTES:
        raise Prediction24x7Error("response_too_large")
    try:
        value = json.loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise Prediction24x7Error("invalid_json") from exc
    if not isinstance(value, Mapping):
        raise Prediction24x7Error("response_object_required")
    return value


class PublicClient:
    """Unauthenticated client with an explicit per-run request ceiling."""

    def __init__(self, opener: Any = None, clock: Callable[[], datetime] | None = None, *, max_requests: int = MAX_REQUESTS):
        self.opener = opener or urllib.request.build_opener(NoRedirect)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.max_requests = max_requests
        self.requests = 0

    def get(self, path: str, params: Mapping[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
        allowed = (path in {"/trade-api/v2/markets"} or
                   bool(re.fullmatch(r"/trade-api/v2/(?:series|events|markets)/[A-Z0-9][A-Z0-9_-]{1,127}(?:/orderbook)?", path)))
        if not allowed:
            raise Prediction24x7Error("endpoint_not_allowlisted")
        if self.requests >= self.max_requests:
            raise Prediction24x7Error("request_budget_exhausted")
        query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
        url = ORIGIN + path + (("?" + query) if query else "")
        requested_at = _time(now or self.clock(), "requested_at")
        request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        self.requests += 1
        try:
            with self.opener.open(request, timeout=20) as response:
                if response.status != 200:
                    raise Prediction24x7Error("public_api_status")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            raise Prediction24x7Error(f"public_api_http_{exc.code}") from None
        except Prediction24x7Error:
            raise
        except Exception as exc:
            raise Prediction24x7Error("public_api_transport") from exc
        received_at = _time(self.clock(), "received_at")
        payload = _json_body(raw)
        return {
            "path": path,
            "params": dict(params),
            "requested_at": _iso(requested_at),
            "received_at": _iso(received_at),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "raw_response_b64": base64.b64encode(raw).decode("ascii"),
            "payload": dict(payload),
        }


def _markets(payload: Mapping[str, Any], series: str) -> list[dict[str, Any]]:
    rows = payload.get("markets")
    if not isinstance(rows, list):
        raise Prediction24x7Error("markets_list_missing")
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        ticker = row.get("ticker")
        if not isinstance(ticker, str) or not ticker.startswith(series + "-") or not _TICKER.fullmatch(ticker):
            continue
        if row.get("status") not in {"active", "open"}:
            continue
        selected.append(dict(row))
    def whole_depth_score(row: Mapping[str, Any]) -> int:
        try:
            bid = int(Decimal(str(row.get("yes_bid_size_fp", "0"))).to_integral_value(rounding=ROUND_FLOOR))
            ask = int(Decimal(str(row.get("yes_ask_size_fp", "0"))).to_integral_value(rounding=ROUND_FLOOR))
            return min(bid, ask)
        except (InvalidOperation, TypeError, ValueError):
            return 0
    # Prefer a market whose published top-of-book sizes can support at least
    # one whole contract. The orderbook response remains authoritative and is
    # still revalidated after this bounded selection step.
    selected.sort(key=lambda row: (-whole_depth_score(row), str(row.get("close_time", "")), str(row.get("ticker", ""))))
    return selected[:MAX_MARKETS_PER_SERIES]


def _book_for(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    book = payload.get("orderbook", payload)
    if not isinstance(book, Mapping):
        raise Prediction24x7Error("orderbook_object_missing")
    return book


def _fixedpoint_book(payload: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Normalize Kalshi ``orderbook_fp`` into conservative whole contracts.

    Kalshi's public fixed-point schema defines ``*_fp`` quantities as contract
    counts with 0.01-contract granularity.  The shared internal-paper ledger
    currently trades whole contracts, so fractional remainders are discarded
    rather than rounded up.  A future fractional-ledger change must use a new
    strategy version.
    """
    raw = payload.get("orderbook_fp")
    if not isinstance(raw, Mapping):
        raise Prediction24x7Error("fixed_point_orderbook_missing")
    result: dict[str, list[dict[str, Any]]] = {}
    for source, target, reverse in (("yes_dollars", "yes_bids", True), ("no_dollars", "no_bids", True)):
        levels = raw.get(source)
        if not isinstance(levels, list):
            raise Prediction24x7Error("fixed_point_depth_missing")
        normalized: list[dict[str, Any]] = []
        for level in levels:
            if not isinstance(level, Sequence) or isinstance(level, (str, bytes)) or len(level) != 2:
                raise Prediction24x7Error("fixed_point_depth_invalid")
            try:
                price = Decimal(str(level[0])); quantity = Decimal(str(level[1]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise Prediction24x7Error("fixed_point_depth_invalid") from exc
            if not price.is_finite() or not quantity.is_finite() or not Decimal("0") <= price <= Decimal("1") or quantity <= 0:
                raise Prediction24x7Error("fixed_point_depth_invalid")
            whole = int(quantity.to_integral_value(rounding=ROUND_FLOOR))
            if whole > 0:
                normalized.append({"price": str(price), "size": whole})
        if not normalized:
            raise Prediction24x7Error("no_whole_contract_depth")
        result[target] = normalized
    # These are bids on each binary side.  Complementing the opposite side
    # yields the executable YES/NO ask while retaining the raw source above.
    result["yes_asks"] = [{"price": str(Decimal("1") - Decimal(item["price"])), "size": item["size"]} for item in result["no_bids"]]
    result["no_asks"] = [{"price": str(Decimal("1") - Decimal(item["price"])), "size": item["size"]} for item in result["yes_bids"]]
    return result


def _fee_schedule(series: Mapping[str, Any], event_response: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return a conservative whole-contract taker fee from official metadata."""
    fee_type = series.get("fee_type")
    multiplier = series.get("fee_multiplier")
    event_payload = event_response.get("payload") if isinstance(event_response, Mapping) else None
    event = event_payload.get("event") if isinstance(event_payload, Mapping) else None
    if isinstance(event, Mapping):
        override_type = event.get("fee_type_override")
        override_multiplier = event.get("fee_multiplier_override")
        if override_type:
            fee_type = override_type
        if override_multiplier is not None:
            multiplier = override_multiplier
    if fee_type not in {"quadratic", "quadratic_with_maker_fees"}:
        return None
    try:
        factor = Decimal(str(multiplier))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not factor.is_finite() or factor < 0:
        return None
    # Official schedule: taker fee = M * 0.07 * C * P * (1-P).  The maximum
    # at P=.50 is 1.75c*M per whole contract; ceiling to cents is conservative
    # for the existing integer-cent ledger. Settlement is explicitly free.
    upper_cents = int((factor * Decimal("1.75")).to_integral_value(rounding=ROUND_CEILING))
    received = event_response.get("received_at") if isinstance(event_response, Mapping) else series.get("last_updated_ts")
    provenance_payload = {"series_fee_type": fee_type, "series_fee_multiplier": str(factor),
                          "event_response_sha256": event_response.get("sha256") if isinstance(event_response, Mapping) else None,
                          "official_schedule_url": "https://kalshi.com/docs/kalshi-fee-schedule.pdf",
                          "schedule_effective_at": "2026-07-07T00:00:00Z",
                          "formula": "ceil_centicent(M * 0.07 * C * P * (1-P))", "settlement_fee": "0"}
    return {"version": "kalshi-public-fee-schedule-2026-07-07-upper-bound-v1",
            "entry_fee_cents_per_contract": str(upper_cents), "exit_fee_cents_per_contract": str(upper_cents),
            "settlement_fee_cents_per_contract": "0", "model": "whole_contract_taker_upper_bound",
            "provenance": {"source_kind": "observed", "source_version": "kalshi-fee-schedule-2026-07-07",
                           "known_at": received, "payload": provenance_payload, "sha256": _digest(provenance_payload)}}


def _integer_depth(book: Mapping[str, Any]) -> bool:
    present = False
    for side in ("yes", "no", "asks", "bids"):
        levels = book.get(side)
        if levels is None:
            continue
        present = True
        if not isinstance(levels, list):
            return False
        for level in levels:
            if not isinstance(level, Sequence) or isinstance(level, (str, bytes)) or len(level) < 2:
                return False
            if isinstance(level[1], bool) or not isinstance(level[1], int) or level[1] <= 0:
                return False
    return present


def _official_settlement(market: Mapping[str, Any], event_response: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Build a definition only from exact public market/event terms."""
    payload = event_response.get("payload") if isinstance(event_response, Mapping) else None
    event = payload.get("event") if isinstance(payload, Mapping) else None
    if not isinstance(event, Mapping):
        return None
    sources = event.get("settlement_sources")
    rule = market.get("rules_primary")
    if not isinstance(sources, list) or not sources or not isinstance(rule, str) or not rule.strip():
        return None
    close_at = market.get("close_time")
    try:
        _time(close_at, "market.close_time")
    except Prediction24x7Error:
        return None
    source_identity = {"venue": "Kalshi", "sources": sources, "event_ticker": event.get("event_ticker"),
                       "series_ticker": event.get("series_ticker")}
    body = {"version": "kalshi-public-terms-v1", "rule": rule, "rules_secondary": market.get("rules_secondary", ""),
            "source_identity": source_identity, "cutoff_at": close_at, "payout_cents": 100}
    payload_hash = _digest({"event": event, "market_terms": {key: market.get(key) for key in ("ticker", "rules_primary", "rules_secondary", "close_time")}})
    provenance_payload = {"event_response_sha256": event_response.get("sha256"), "terms_payload_sha256": payload_hash}
    return {**body, "definition_sha256": _digest(body),
            "provenance": {"source_kind": "observed", "source_version": "kalshi-public-event-terms-v1",
                           "known_at": event_response.get("received_at"), "payload": provenance_payload,
                           "sha256": _digest(provenance_payload)}}


def evaluate_market(record: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    """Classify one captured market without deriving probability or terms."""
    market = record.get("market")
    response = record.get("orderbook")
    reasons: list[str] = []
    if not isinstance(market, Mapping):
        reasons.append("market_metadata_missing")
    if not isinstance(response, Mapping):
        reasons.append("orderbook_missing")
    if reasons:
        return {"status": "abstained", "market_id": None, "abstention_reasons": reasons, "candidate": None}
    market_id = market.get("ticker")
    if not isinstance(market_id, str) or not _TICKER.fullmatch(market_id):
        reasons.append("market_identity_missing")
    # The public orderbook endpoint gives local receipt time, not a venue event
    # timestamp.  It must never be promoted into the causal event timestamp.
    if not record.get("venue_event_at"):
        reasons.append("venue_event_time_missing")
    if not market.get("rules_primary") and not record.get("settlement_definition"):
        reasons.append("settlement_terms_missing")
    if not record.get("fee_schedule"):
        reasons.append("fee_schedule_formula_missing" if record.get("fee_metadata") else "fee_schedule_unverified")
    if not record.get("probability"):
        reasons.append("calibrated_probability_missing")
    if not record.get("settlement_definition"):
        reasons.append("settlement_definition_missing")
    if record.get("listing_complete") is False:
        reasons.append("incomplete_market_pagination")
    try:
        depth = record.get("normalized_book") or _book_for(response.get("payload", response))
        normalized_ok = isinstance(depth, Mapping) and all(isinstance(depth.get(key), list) and bool(depth.get(key)) for key in ("yes_bids", "no_bids", "yes_asks", "no_asks"))
        if not (_integer_depth(depth) or normalized_ok):
            reasons.append("contract_units_or_depth_invalid")
    except Prediction24x7Error:
        reasons.append("orderbook_schema_invalid")
    received = response.get("received_at")
    if now is not None and received:
        try:
            if _time(received, "orderbook.received_at") > _time(now, "now"):
                reasons.append("receipt_in_future")
        except Prediction24x7Error:
            reasons.append("receipt_timestamp_invalid")
    status = "eligible_shadow_candidate" if not reasons and isinstance(record.get("candidate"), Mapping) else "abstained"
    if status != "eligible_shadow_candidate" and not reasons:
        reasons.append("candidate_evidence_not_materialized")
    return {"status": status, "market_id": market_id, "abstention_reasons": sorted(set(reasons)), "candidate": record.get("candidate") if status.startswith("eligible") else None}


def capture_once(
    state: str | os.PathLike[str],
    *,
    series: Sequence[str] = DEFAULT_SERIES,
    client: PublicClient | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Capture at most one bounded snapshot for the current five-minute slot."""
    clock = clock or (lambda: datetime.now(UTC))
    before = _time(clock(), "clock")
    root = _root(state)
    normalized_series = tuple(dict.fromkeys(series))
    if not normalized_series or any(not isinstance(item, str) or not _TICKER.fullmatch(item) for item in normalized_series):
        raise Prediction24x7Error("series allowlist is invalid")
    slot = _slot_start(before)
    slot_key = slot.strftime("%Y%m%dT%H%MZ")
    evidence_path = root / "evidence" / f"{slot_key}.json.gz"
    with _lock(root):
        if evidence_path.exists():
            return {"status": "same_slot_replay", "slot": _iso(slot), "evidence_path": str(evidence_path), "requests": 0,
                    "orders_sent": 0, "ledger_mutations": 0}
        if _size(root) >= MAX_STORAGE_BYTES:
            return {"status": "storage_limit", "slot": _iso(slot), "requests": 0, "orders_sent": 0, "ledger_mutations": 0}
        active_client = client or PublicClient(clock=clock)
        responses: list[dict[str, Any]] = []
        records: list[dict[str, Any]] = []
        blockers: dict[str, int] = {}
        try:
            for series_ticker in normalized_series:
                listing = active_client.get("/trade-api/v2/markets", {"series_ticker": series_ticker, "status": "open", "limit": 100}, now=before)
                responses.append(listing)
                listing_complete = listing["payload"].get("cursor") == ""
                series_response = active_client.get(f"/trade-api/v2/series/{series_ticker}", {}, now=before)
                responses.append(series_response)
                series_payload = series_response.get("payload", {}).get("series", {})
                for market in _markets(listing["payload"], series_ticker):
                    ticker = str(market["ticker"])
                    if active_client.requests >= active_client.max_requests:
                        break
                    event_ticker = market.get("event_ticker")
                    event_response = None
                    if isinstance(event_ticker, str) and _TICKER.fullmatch(event_ticker):
                        event_response = active_client.get(f"/trade-api/v2/events/{event_ticker}", {}, now=before)
                        responses.append(event_response)
                    book = active_client.get(f"/trade-api/v2/markets/{ticker}/orderbook", {"depth": 10}, now=before)
                    responses.append(book)
                    terms = _official_settlement(market, event_response)
                    raw_book = book.get("payload", {})
                    normalized_book = None
                    try:
                        normalized_book = _fixedpoint_book(raw_book)
                    except Prediction24x7Error:
                        pass
                    record = {"market": market, "orderbook": book, "listing_complete": listing_complete,
                              "series": series_payload if isinstance(series_payload, Mapping) else {},
                              "event": (event_response or {}).get("payload", {}).get("event", {}) if isinstance((event_response or {}).get("payload", {}), Mapping) else {},
                              "fee_metadata": {key: series_payload.get(key) for key in ("fee_type", "fee_multiplier", "last_updated_ts") if key in series_payload},
                              "fee_schedule": _fee_schedule(series_payload, event_response),
                              "settlement_definition": terms, "normalized_book": normalized_book}
                    result = evaluate_market(record)
                    for reason in result["abstention_reasons"]:
                        blockers[reason] = blockers.get(reason, 0) + 1
                    record["evaluation"] = result
                    records.append(record)
            evidence = {"schema_version": 1, "version": VERSION, "slot": _iso(slot), "collected_at": _iso(_time(active_client.clock(), "collected_at")),
                        "source_kind": "observed", "execution_enabled": False, "series": list(normalized_series),
                        "requests": active_client.requests, "responses": responses, "markets": records,
                        "candidate_count": sum(item["evaluation"]["status"] == "eligible_shadow_candidate" for item in records),
                        "blockers": blockers, "orders_sent": 0, "ledger_mutations": 0,
                        "research_notice": "Public observation only; HTTP receipt is not a venue event timestamp."}
            _write_once(evidence_path, evidence, root)
            status = "observed" if responses else "no_markets"
            result = {"status": status, "slot": _iso(slot), "evidence_path": str(evidence_path), "requests": active_client.requests,
                      "candidate_count": evidence["candidate_count"], "blockers": blockers, "orders_sent": 0, "ledger_mutations": 0}
        except Exception as exc:
            result = {"status": "failed_closed", "slot": _iso(slot), "requests": active_client.requests, "error": type(exc).__name__,
                      "orders_sent": 0, "ledger_mutations": 0}
        _write_once(root / "status.json", result, root, replace=True)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--series", action="append", dest="series")
    args = parser.parse_args(argv)
    try:
        result = capture_once(args.state, series=tuple(args.series or DEFAULT_SERIES))
    except Prediction24x7Error as exc:
        result = {"status": "failed_closed", "error": str(exc), "orders_sent": 0, "ledger_mutations": 0}
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] == "failed_closed" else 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Prediction24x7Error", "PublicClient", "capture_once", "evaluate_market"]
