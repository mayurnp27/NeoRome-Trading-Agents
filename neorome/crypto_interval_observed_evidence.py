"""Read-only normalizer and accumulator for observed BTC interval evidence.

The public capture is useful only when each execution-relevant field is bound
to exact bytes and provenance.  This module preserves that evidence and marks
missing fields unavailable.  It never supplies an exchange timestamp from a
local receipt, turns ambiguous depth into contracts, derives venue fees, or
creates probabilities.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Mapping

from .crypto_interval_adapter import IntervalAdapterError, adapt_interval_observed


UTC = timezone.utc
SCHEMA_VERSION = 1
NORMALIZER_VERSION = "crypto-interval-observed-evidence-v1"


class ObservedEvidenceError(ValueError):
    """Observed evidence is malformed or cannot be safely normalized."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ObservedEvidenceError("evidence is not canonical JSON") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise ObservedEvidenceError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _time(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ObservedEvidenceError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ObservedEvidenceError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ObservedEvidenceError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ObservedEvidenceError(f"{field} is required")
    return value.strip()


def _captured_record(capture: Mapping[str, Any], field: str) -> dict[str, Any]:
    value = capture.get(field)
    if field == "books":
        if isinstance(value, Mapping):
            value = [value]
        if not isinstance(value, list) or not value:
            raise ObservedEvidenceError("capture books is not a successful record list")
        records = [_captured_one(item, "books") for item in value]
        return {
            "records": records,
            "received_at": max(record["received_at"] for record in records),
            "sha256": digest(records),
        }
    return _captured_one(value, field)


def _captured_one(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("status") != "captured":
        raise ObservedEvidenceError(f"capture {field} is not a successful record")
    for key in ("received_at", "sha256", "raw_b64", "payload"):
        if key not in value:
            raise ObservedEvidenceError(f"capture {field}.{key} is required")
    supplied = _hash(value["sha256"], f"capture {field}.sha256")
    try:
        raw = base64.b64decode(value["raw_b64"], validate=True)
    except (ValueError, TypeError) as exc:
        raise ObservedEvidenceError(f"capture {field}.raw_b64 is invalid") from exc
    if hashlib.sha256(raw).hexdigest() != supplied:
        raise ObservedEvidenceError(f"capture {field} raw hash mismatch")
    received = _time(value["received_at"], f"capture {field}.received_at")
    requested_raw = value.get("requested_at")
    if requested_raw is not None and _time(requested_raw, f"capture {field}.requested_at") > received:
        raise ObservedEvidenceError(f"capture {field} receipt precedes request")
    return {"received_at": _iso(received), "sha256": supplied, "payload": value["payload"]}


def _provenance(value: Any, field: str, *, payload_required: bool = True) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ObservedEvidenceError(f"{field} provenance is required")
    if value.get("source_kind") != "observed":
        raise ObservedEvidenceError(f"{field} must be observed evidence")
    if not isinstance(value.get("source_version"), str) or not value["source_version"].strip():
        raise ObservedEvidenceError(f"{field}.source_version is required")
    supplied = _hash(value.get("sha256"), f"{field}.sha256")
    if payload_required and "payload" not in value:
        raise ObservedEvidenceError(f"{field}.payload is required")
    if "payload" in value and digest(value["payload"]) != supplied:
        raise ObservedEvidenceError(f"{field} payload hash mismatch")
    return {**dict(value), "sha256": supplied}


def _unavailable(reason: str) -> dict[str, Any]:
    return {"status": "unavailable", "eligible": False, "reason": reason}


def _field_with_provenance(value: Any, field: str, *, expected: Mapping[str, Any] | None = None) -> dict[str, Any]:
    try:
        source = _provenance(value, field)
        if expected:
            for key, expected_value in expected.items():
                if source.get(key) != expected_value:
                    raise ObservedEvidenceError(f"{field}.{key} differs from adapter evidence")
        return {"status": "available", "eligible": True, "provenance": source}
    except ObservedEvidenceError as exc:
        return _unavailable(str(exc))


def _event_field(adapter: Mapping[str, Any], supplied: Any) -> dict[str, Any]:
    event = adapter["events"][0]
    event_at = event.get("event_at")
    received_at = event.get("received_at")
    if event_at is None:
        if supplied is None:
            return _unavailable("exchange event timestamp is absent; local receipt cannot substitute")
        try:
            source = _provenance(supplied, "event_timestamp")
            supplied_event = _time(source.get("event_at"), "event_timestamp.event_at")
            supplied_receipt = _time(source.get("received_at"), "event_timestamp.received_at")
            if supplied_event > supplied_receipt:
                raise ObservedEvidenceError("event timestamp follows receipt")
            return {"status": "available", "eligible": True, "event_at": _iso(supplied_event),
                    "received_at": _iso(supplied_receipt), "provenance": source}
        except ObservedEvidenceError as exc:
            return _unavailable(str(exc))
    try:
        event_dt = _time(event_at, "adapter.events[0].event_at")
        receipt_dt = _time(received_at, "adapter.events[0].received_at")
        if event_dt > receipt_dt:
            raise ObservedEvidenceError("adapter event timestamp follows receipt")
        if supplied is not None:
            source = _provenance(supplied, "event_timestamp")
            if _time(source.get("event_at"), "event_timestamp.event_at") != event_dt or _time(source.get("received_at"), "event_timestamp.received_at") != receipt_dt:
                raise ObservedEvidenceError("supplied event timestamps differ from adapter evidence")
            return {"status": "available", "eligible": True, "event_at": _iso(event_dt), "received_at": _iso(receipt_dt), "provenance": source}
        raw_hash = _hash(event.get("raw_book_sha256"), "adapter.events[0].raw_book_sha256")
        return {"status": "available", "eligible": True, "event_at": _iso(event_dt), "received_at": _iso(receipt_dt),
                "provenance": {"source_kind": "observed", "source_version": "adapter-event-v1", "sha256": raw_hash}}
    except ObservedEvidenceError as exc:
        return _unavailable(str(exc))


def _units_field(adapter: Mapping[str, Any], supplied: Any) -> dict[str, Any]:
    event = adapter["events"][0]
    if supplied is None:
        return _unavailable("orderbook size units are not established by the public capture")
    try:
        source = _provenance(supplied, "contract_units")
        if source.get("units") != "contracts":
            raise ObservedEvidenceError("contract units must be explicitly identified as contracts")
        for side in ("asks", "bids"):
            for level in event.get(side, []):
                if level.get("size") is None or not isinstance(level.get("size"), int) or level["size"] <= 0:
                    raise ObservedEvidenceError("normalized depth is not integer contract depth")
        return {"status": "available", "eligible": True, "units": "contracts", "provenance": source}
    except ObservedEvidenceError as exc:
        return _unavailable(str(exc))


def _fees_field(adapter: Mapping[str, Any], supplied: Any) -> dict[str, Any]:
    if supplied is None:
        fee = adapter.get("fee")
        reason = fee.get("reason", "fee mapping is unavailable") if isinstance(fee, Mapping) else "fee mapping is unavailable"
        return _unavailable(str(reason))
    try:
        source = _provenance(supplied, "fee_schema")
        version = _text(source.get("version"), "fee_schema.version")
        _text(source.get("rounding"), "fee_schema.rounding")
        _text(source.get("formula"), "fee_schema.formula")
        values = {}
        for action in ("entry", "exit", "settlement"):
            key = f"{action}_fee_cents_per_contract"
            value = Decimal(str(source.get(key)))
            if not value.is_finite() or value < 0:
                raise ObservedEvidenceError(f"fee_schema.{key} is invalid")
            values[key] = str(value)
        return {"status": "available", "eligible": True, "version": version, "rounding": source["rounding"],
                "formula": source["formula"], **values, "provenance": source}
    except (ObservedEvidenceError, InvalidOperation, TypeError, ValueError) as exc:
        return _unavailable(str(exc))


def _settlement_field(adapter: Mapping[str, Any], supplied: Any) -> dict[str, Any]:
    contract = adapter["contract"]
    identity = {
        "market_id": contract.get("market_id"),
        "outcome_id": contract.get("outcome_id"),
        "outcome": contract.get("outcome"),
        "terms_sha256": contract.get("terms", {}).get("terms_sha256") if isinstance(contract.get("terms"), Mapping) else None,
        "source_identity": contract.get("source_identity"),
    }
    if supplied is None:
        return {"status": "terms_bound_only", "eligible": False, "identity": identity,
                "reason": "finalized settlement evidence is absent"}
    try:
        source = _provenance(supplied, "settlement")
        if source.get("market_id") != identity["market_id"] or source.get("outcome_id") != identity["outcome_id"]:
            raise ObservedEvidenceError("settlement identity differs from admitted contract")
        _text(source.get("definition"), "settlement.definition")
        _time(source.get("settlement_ts"), "settlement.settlement_ts")
        if source.get("status") != "finalized" or source.get("result") not in ("yes", "no"):
            raise ObservedEvidenceError("settlement is not finalized ordinary binary evidence")
        return {"status": "available", "eligible": True, "identity": identity, "result": source["result"],
                "settlement_ts": source["settlement_ts"], "provenance": source}
    except ObservedEvidenceError as exc:
        return {"status": "unavailable", "eligible": False, "identity": identity, "reason": str(exc)}


def _calibration_field(supplied: Any) -> dict[str, Any]:
    if supplied is None:
        return _unavailable("no prospective calibrated probability observation supplied")
    try:
        source = _provenance(supplied, "calibration_observation")
        probability = Decimal(str(source.get("probability")))
        if not probability.is_finite() or probability < 0 or probability > 1:
            raise ObservedEvidenceError("calibration probability must be between zero and one")
        known_at = _time(source.get("known_at"), "calibration_observation.known_at")
        decision_at = _time(source.get("decision_at"), "calibration_observation.decision_at")
        if known_at > decision_at:
            raise ObservedEvidenceError("calibration probability was known after decision")
        _text(source.get("model_version"), "calibration_observation.model_version")
        _text(source.get("protocol_version"), "calibration_observation.protocol_version")
        if source.get("out_of_sample") is not True:
            raise ObservedEvidenceError("calibration observation is not out-of-sample")
        return {"status": "available", "eligible": True, "probability": str(probability), "known_at": _iso(known_at),
                "decision_at": _iso(decision_at), "provenance": source}
    except (ObservedEvidenceError, InvalidOperation, TypeError, ValueError) as exc:
        return _unavailable(str(exc))


def normalize(
    capture: Mapping[str, Any],
    *,
    event_timestamp: Mapping[str, Any] | None = None,
    contract_units: Mapping[str, Any] | None = None,
    fee_schema: Mapping[str, Any] | None = None,
    settlement: Mapping[str, Any] | None = None,
    calibration_observation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize one archived capture without filling missing evidence."""
    if not isinstance(capture, Mapping):
        raise ObservedEvidenceError("capture must be an object")
    if capture.get("kind") != "crypto_interval_evidence" or capture.get("execution_enabled") is not False or capture.get("paper_admitted") is not False:
        raise ObservedEvidenceError("only execution-disabled observed interval captures are accepted")
    captured = {field: _captured_record(capture, field) for field in ("series", "markets", "books")}
    try:
        adapter = adapt_interval_observed(capture, fee_schedule=fee_schema)
    except IntervalAdapterError as exc:
        raise ObservedEvidenceError(str(exc)) from exc
    fields = {
        "event_timestamp": _event_field(adapter, event_timestamp),
        "contract_units": _units_field(adapter, contract_units),
        "fee_schema": _fees_field(adapter, fee_schema),
        "settlement": _settlement_field(adapter, settlement),
        "calibration": _calibration_field(calibration_observation),
    }
    blockers = list(adapter.get("admission_reasons", []))
    blockers.extend(field["reason"] for field in fields.values() if not field.get("eligible") and field.get("reason"))
    body = {
        "schema_version": SCHEMA_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "source_kind": "observed",
        "execution_enabled": False,
        "paper_admitted": False,
        "capture_provenance": captured,
        "adapter_contract": adapter.get("contract"),
        "adapter_admission_reasons": sorted(set(adapter.get("admission_reasons", []))),
        "fields": fields,
        "blockers": sorted(set(str(reason) for reason in blockers)),
    }
    record_hash = digest(body)
    return {
        **body,
        "status": "complete_observed_evidence" if not blockers else "incomplete_observed_evidence",
        "eligible_for_shadow": not blockers,
        "record_sha256": record_hash,
        "observation_id": f"{adapter.get('contract', {}).get('market_id', 'unknown')}:{captured['books']['received_at']}",
        "notice": "Evidence normalization only; no probability, fill, settlement, ledger, or profitability inference is performed.",
    }


def accumulate(existing: Mapping[str, Any] | None, record: Mapping[str, Any]) -> dict[str, Any]:
    """Append one normalized observed record idempotently in memory."""
    if not isinstance(record, Mapping) or record.get("source_kind") != "observed" or record.get("normalizer_version") != NORMALIZER_VERSION:
        raise ObservedEvidenceError("only normalized observed records may be accumulated")
    _hash(record.get("record_sha256"), "record_sha256")
    if existing is None:
        rows: list[dict[str, Any]] = []
    else:
        if existing.get("schema_version") != SCHEMA_VERSION or existing.get("normalizer_version") != NORMALIZER_VERSION:
            raise ObservedEvidenceError("accumulator schema is unsupported")
        rows = [dict(row) for row in existing.get("observations", [])]
    for row in rows:
        if row.get("observation_id") == record.get("observation_id"):
            if row.get("record_sha256") != record.get("record_sha256"):
                raise ObservedEvidenceError("observation identity conflicts with prior record")
            return dict(existing) if existing is not None else {"schema_version": SCHEMA_VERSION, "normalizer_version": NORMALIZER_VERSION, "observations": rows}
    rows.append(dict(record))
    rows.sort(key=lambda row: (str(row.get("observation_id")), str(row.get("record_sha256"))))
    body = {"schema_version": SCHEMA_VERSION, "normalizer_version": NORMALIZER_VERSION, "observations": rows}
    return {**body, "observation_count": len(rows), "accumulator_sha256": digest(body)}


__all__ = ["ObservedEvidenceError", "accumulate", "digest", "normalize"]
