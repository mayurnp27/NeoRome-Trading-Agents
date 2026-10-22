"""Build a bounded, read-only executable-weather snapshot.

The builder consumes archived public market evidence and optional separately
reviewed order-book, fee, and settlement records.  It makes no HTTP call and
never opens a ledger.  Missing official economic terms are retained as explicit
abstentions instead of being inferred from venue labels or displayed prices.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping

from .prediction_evidence import read_evidence
from .prediction_internal_paper_runtime import newest_complete_evidence

UTC = timezone.utc
MAX_INPUT_BYTES = 8 * 1024 * 1024
VERSION = "prediction-weather-execution-snapshot-v1"


class WeatherExecutionSnapshotError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise WeatherExecutionSnapshotError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise WeatherExecutionSnapshotError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WeatherExecutionSnapshotError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _read(path: str | os.PathLike[str]) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_INPUT_BYTES:
        raise WeatherExecutionSnapshotError("snapshot input is unsafe")
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WeatherExecutionSnapshotError("snapshot input is not JSON") from exc
    if not isinstance(value, dict):
        raise WeatherExecutionSnapshotError("snapshot input must be an object")
    return value


def _source(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("source_kind") != "observed":
        raise WeatherExecutionSnapshotError(f"{field} observed provenance is required")
    if not isinstance(value.get("source_version"), str) or not value["source_version"].strip():
        raise WeatherExecutionSnapshotError(f"{field} source version is required")
    _time(value.get("known_at"), f"{field}.known_at")
    payload = value.get("payload")
    digest = _hash(value.get("sha256"), f"{field}.sha256")
    if payload is not None and _digest(payload) != digest:
        raise WeatherExecutionSnapshotError(f"{field} provenance hash mismatch")
    return dict(value)


def _levels(value: Any, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise WeatherExecutionSnapshotError(f"{field} must contain executable depth")
    output = []
    for item in value:
        if not isinstance(item, Mapping) or not isinstance(item.get("price"), (str, int, float)) or isinstance(item.get("size"), bool) or not isinstance(item.get("size"), int) or item["size"] <= 0:
            raise WeatherExecutionSnapshotError(f"{field} level is invalid")
        output.append({"price": str(item["price"]), "size": item["size"]})
    return output


def _market_rows(evidence: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    if evidence.get("source_kind") != "observed" or evidence.get("venue") != "kalshi_public" or evidence.get("series_ticker") != "KXHIGHNY" or evidence.get("complete_response") is not True:
        raise WeatherExecutionSnapshotError("complete observed KXHIGHNY market evidence is required")
    responses = evidence.get("responses")
    if not isinstance(responses, list) or len(responses) != 2:
        raise WeatherExecutionSnapshotError("market evidence response shape is invalid")
    open_rows = []
    for response in responses:
        if not isinstance(response, Mapping):
            raise WeatherExecutionSnapshotError("market evidence response is invalid")
        if response.get("status") == "open":
            if response.get("complete_response") is not True or not isinstance(response.get("markets"), list):
                raise WeatherExecutionSnapshotError("open market response is incomplete")
            open_rows = [dict(row) for row in response["markets"] if isinstance(row, Mapping) and row.get("status") == "active"]
    if not open_rows:
        raise WeatherExecutionSnapshotError("active KXHIGHNY markets are absent")
    return open_rows, _digest(evidence)


def _contract(row: Mapping[str, Any], settlement: Mapping[str, Any] | None) -> dict[str, Any]:
    required = ("ticker", "event_ticker", "rules_primary", "rules_secondary", "open_time", "close_time", "expected_expiration_time", "market_type", "price_level_structure")
    if any(not isinstance(row.get(key), str) or not row[key] for key in required):
        raise WeatherExecutionSnapshotError("active market terms are incomplete")
    if row["market_type"] != "binary" or row["price_level_structure"] != "linear_cent":
        raise WeatherExecutionSnapshotError("active market has unsupported units")
    open_at, cutoff_at, expiry_at = (_time(row["open_time"], "market.open_time"), _time(row["close_time"], "market.close_time"), _time(row["expected_expiration_time"], "market.expected_expiration_time"))
    if not open_at < cutoff_at <= expiry_at:
        raise WeatherExecutionSnapshotError("active market lifetime is invalid")
    terms = {key: row.get(key) for key in ("ticker", "event_ticker", "rules_primary", "rules_secondary", "strike_type", "floor_strike", "price_level_structure", "notional_value_dollars", "close_time", "expected_expiration_time")}
    terms["terms_sha256"] = _digest(terms)
    source_identity = settlement.get("source_identity") if isinstance(settlement, Mapping) else None
    return {"market_id": row["ticker"], "outcome_id": row["ticker"] + ":YES", "outcome": "YES", "terms": terms,
            "source_identity": dict(source_identity) if isinstance(source_identity, Mapping) else {"status": "unverified"},
            "open_at": _iso(open_at), "cutoff_at": _iso(cutoff_at), "expiry_at": _iso(expiry_at)}


def _fee(value: Mapping[str, Any] | None) -> tuple[dict[str, Any] | None, str | None]:
    if value is None:
        return None, "official_fee_metadata_missing"
    try:
        if value.get("verification") != "official":
            raise WeatherExecutionSnapshotError("fee verification is not official")
        if not isinstance(value.get("version"), str) or not value["version"].strip():
            raise WeatherExecutionSnapshotError("fee version is required")
        for key in ("entry_fee_cents_per_contract", "exit_fee_cents_per_contract", "settlement_fee_cents_per_contract"):
            if str(value.get(key, "")).startswith("-") or not str(value.get(key, "")).isdigit():
                raise WeatherExecutionSnapshotError("fee cents must be nonnegative integers")
        result = {key: value[key] for key in ("version", "entry_fee_cents_per_contract", "exit_fee_cents_per_contract", "settlement_fee_cents_per_contract")}
        result["provenance"] = _source(value.get("provenance"), "fee")
        return result, None
    except WeatherExecutionSnapshotError:
        return None, "official_fee_metadata_unavailable"


def _settlement(value: Mapping[str, Any] | None, contract: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    if value is None:
        return None, "official_settlement_metadata_missing"
    try:
        if value.get("verification") != "official" or value.get("cutoff_at") != contract["cutoff_at"] or value.get("payout_cents") != 100:
            raise WeatherExecutionSnapshotError("settlement metadata is not exact and official")
        required = {"version", "rule", "source_identity"}
        if not required.issubset(value) or not isinstance(value["version"], str) or not isinstance(value["rule"], str) or not isinstance(value["source_identity"], Mapping):
            raise WeatherExecutionSnapshotError("settlement metadata is incomplete")
        body = {key: value[key] for key in ("version", "rule", "source_identity", "cutoff_at", "payout_cents")}
        result = {**body, "definition_sha256": _digest(body), "provenance": _source(value.get("provenance"), "settlement")}
        return result, None
    except WeatherExecutionSnapshotError:
        return None, "official_settlement_metadata_unavailable"


def _settlement_results(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if value is None: return []
    rows = value.get("results") if isinstance(value, Mapping) else None
    if not isinstance(rows, list): return []
    output = []
    for row in rows:
        if not isinstance(row, Mapping) or row.get("finalized") is not True or row.get("result") not in {"yes", "no"} or row.get("yes_payout_cents") not in {0, 100}:
            continue
        if not isinstance(row.get("market_id"), str) or not isinstance(row.get("terms_sha256"), str) or not isinstance(row.get("source_hash"), str): continue
        try:
            _time(row.get("settlement_at"), "settlement.settlement_at"); _time(row.get("received_at"), "settlement.received_at"); _hash(row.get("source_hash"), "settlement.source_hash")
        except WeatherExecutionSnapshotError: continue
        output.append({key: row[key] for key in ("market_id", "terms_sha256", "finalized", "settlement_at", "received_at", "source_hash", "result", "yes_payout_cents")})
    return output


def build(market_evidence: Mapping[str, Any], orderbooks: Mapping[str, Any] | None = None, *, fee: Mapping[str, Any] | None = None, settlement: Mapping[str, Any] | None = None, settlement_results: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Create one immutable status/snapshot record from caller-supplied evidence."""
    rows, market_hash = _market_rows(market_evidence)
    fee_schema, fee_reason = _fee(fee)
    executions: list[dict[str, Any]] = []
    books: dict[str, Mapping[str, Any]] = {}
    orderbook_reason = None
    if orderbooks is None:
        orderbook_reason = "executable_orderbook_snapshot_missing"
    else:
        try:
            _source(orderbooks.get("provenance"), "orderbook")
            if not isinstance(orderbooks.get("books"), list):
                raise WeatherExecutionSnapshotError("orderbook books are required")
            books = {str(item.get("ticker")): item for item in orderbooks["books"] if isinstance(item, Mapping)}
        except WeatherExecutionSnapshotError:
            orderbook_reason = "executable_orderbook_snapshot_unverified"
    contracts = []
    for row in rows:
        contract = _contract(row, settlement)
        contracts.append({"market_id": contract["market_id"], "terms_sha256": contract["terms"]["terms_sha256"], "cutoff_at": contract["cutoff_at"]})
        definition, settlement_reason = _settlement(settlement, contract)
        book = books.get(contract["market_id"])
        if orderbook_reason or fee_schema is None or definition is None or not isinstance(book, Mapping):
            continue
        try:
            event_at, received_at = _time(book.get("event_at"), "orderbook.event_at"), _time(book.get("received_at"), "orderbook.received_at")
            if event_at > received_at or event_at < _time(contract["open_at"], "contract.open_at") or received_at > _time(contract["cutoff_at"], "contract.cutoff_at"):
                raise WeatherExecutionSnapshotError("orderbook chronology is invalid")
            if book.get("size_units") != "contracts" or book.get("contract_size") != 1:
                raise WeatherExecutionSnapshotError("orderbook contract units are not explicit")
            asks, bids = _levels(book.get("asks"), "orderbook.asks"), _levels(book.get("bids"), "orderbook.bids")
            adapter = {"scope": "offline_research_only", "execution_enabled": False, "paper_admitted": False, "admissible_for_replay": True,
                       "admission_reasons": [], "contract": contract,
                       "events": [{**contract, "event_type": "book", "event_at": _iso(event_at), "received_at": _iso(received_at), "asks": asks, "bids": bids}],
                       "fee": {"status": "official_verified", **fee_schema}}
            body = {"venue": "Kalshi", "market_id": contract["market_id"], "outcome_id": contract["outcome_id"], "outcome": contract["outcome"], "terms_sha256": contract["terms"]["terms_sha256"]}
            executions.append({"adapter": adapter, "market_identity": {**body, "identity_sha256": _digest(body)},
                               "snapshot_timing": {"event_at": _iso(event_at), "received_at": _iso(received_at), "event_at_source": "venue_event", "received_at_source": "local_receipt"},
                               "quote": {"size_units": "contracts", "contract_size": 1, "asks": asks, "bids": bids, "size_provenance": _source(orderbooks["provenance"], "orderbook")},
                               "fee_schema": fee_schema, "settlement_definition": definition})
        except WeatherExecutionSnapshotError:
            continue
    reasons = []
    if orderbook_reason: reasons.append(orderbook_reason)
    if fee_reason: reasons.append(fee_reason)
    # The rule/cutoff fields alone do not prove a settlement source.
    if settlement is None: reasons.append("official_settlement_metadata_missing")
    elif not executions: reasons.append("no_complete_executable_weather_contract")
    body = {"schema_version": 1, "artifact_kind": VERSION, "source_kind": "observed", "market_evidence_sha256": market_hash,
            "contracts": contracts, "executions": executions, "settlements": _settlement_results(settlement_results), "abstention_reasons": sorted(set(reasons)), "orders_sent": 0, "ledger_mutations": 0, "venue_requests": 0}
    body["status"] = "eligible_execution_snapshot" if executions and not reasons else "abstained"
    body["snapshot_sha256"] = _digest(body)
    return body


def _write(path: str | os.PathLike[str], value: Mapping[str, Any]) -> None:
    target = Path(path); target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if target.is_symlink(): raise WeatherExecutionSnapshotError("snapshot output is unsafe")
    descriptor, temporary = tempfile.mkstemp(prefix=".weather-execution-", dir=target.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1; stream.write(_canonical(value) + b"\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: os.unlink(temporary)
        except FileNotFoundError: pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-evidence"); group.add_argument("--market-evidence-dir")
    parser.add_argument("--orderbooks")
    parser.add_argument("--fee"); parser.add_argument("--settlement"); parser.add_argument("--settlement-results"); parser.add_argument("--output", required=True); parser.add_argument("--publish-inbox")
    args = parser.parse_args(argv)
    try:
        evidence_path = args.market_evidence or newest_complete_evidence(args.market_evidence_dir)
        result = build(read_evidence(evidence_path), _read(args.orderbooks) if args.orderbooks else None,
                       fee=_read(args.fee) if args.fee else None, settlement=_read(args.settlement) if args.settlement else None,
                       settlement_results=_read(args.settlement_results) if args.settlement_results else None)
        _write(args.output, result)
        if args.publish_inbox:
            from .prediction_weather_inbox import publish_snapshot
            publish_snapshot(result, args.publish_inbox)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result["status"] in {"abstained", "eligible_execution_snapshot"} else 1
    except (WeatherExecutionSnapshotError, ValueError, OSError) as exc:
        print(json.dumps({"status": "failed_closed", "error": str(exc), "orders_sent": 0, "ledger_mutations": 0, "venue_requests": 0}, sort_keys=True)); return 1


if __name__ == "__main__":
    raise SystemExit(main())
