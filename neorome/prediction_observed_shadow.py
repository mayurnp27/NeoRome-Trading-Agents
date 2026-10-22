"""One-shot, fail-closed observed-shadow admission for prediction evidence.

The runner consumes a caller-supplied envelope containing an already-admitted
venue adapter record, exact timestamps, a contract-size declaration, a
versioned fee schema, settlement terms, and a calibrated probability record.
It never contacts a venue, mutates a ledger, schedules work, authenticates, or
places an order.  Its eligible output is a content-addressed shadow candidate
only; it is not an execution or profitability decision.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Mapping


UTC = timezone.utc
SCHEMA_VERSION = 1
SHADOW_VERSION = "prediction-observed-shadow-v1"


class ObservedShadowError(ValueError):
    """Evidence cannot support an observed-shadow candidate."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ObservedShadowError("evidence is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise ObservedShadowError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ObservedShadowError(f"{field} must be an ISO timestamp") from exc
    else:
        raise ObservedShadowError(f"{field} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ObservedShadowError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ObservedShadowError(f"{field} is required")
    return value.strip()


def _decimal(value: Any, field: str, *, low: Decimal | None = None, high: Decimal | None = None) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ObservedShadowError(f"{field} must be numeric")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ObservedShadowError(f"{field} must be numeric") from exc
    if not parsed.is_finite() or (low is not None and parsed < low) or (high is not None and parsed > high):
        raise ObservedShadowError(f"{field} is outside its allowed range")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ObservedShadowError(f"{field} must be a positive integer")
    return value


def _source(value: Any, field: str, *, require_version: bool = True) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ObservedShadowError(f"{field} provenance is required")
    if value.get("source_kind") != "observed":
        raise ObservedShadowError(f"{field} must be observed evidence")
    if require_version:
        _text(value.get("source_version"), f"{field}.source_version")
    _hash(value.get("sha256"), f"{field}.sha256")
    payload = value.get("payload")
    if payload is not None and _digest(payload) != value.get("sha256"):
        raise ObservedShadowError(f"{field} payload hash mismatch")
    return dict(value)


def _adapter_and_contract(evidence: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    adapter = evidence.get("adapter")
    if not isinstance(adapter, Mapping):
        raise ObservedShadowError("admitted venue adapter output is required")
    if adapter.get("scope") != "offline_research_only" or adapter.get("execution_enabled") is not False:
        raise ObservedShadowError("adapter must be research-only and execution-disabled")
    if adapter.get("paper_admitted") is not False:
        raise ObservedShadowError("adapter paper admission must remain disabled")
    reasons = adapter.get("admission_reasons")
    if not isinstance(reasons, list) or reasons:
        joined = ", ".join(str(reason) for reason in reasons or []) or "missing adapter admission record"
        raise ObservedShadowError(f"adapter admission failed: {joined}")
    if adapter.get("admissible_for_replay") is not True:
        raise ObservedShadowError("adapter output is not admitted for replay")
    contract = adapter.get("contract")
    if not isinstance(contract, Mapping):
        raise ObservedShadowError("adapter contract identity is required")
    for field in ("market_id", "outcome_id", "outcome", "terms", "source_identity", "cutoff_at", "expiry_at"):
        if field not in contract:
            raise ObservedShadowError(f"contract.{field} is required")
    _text(contract.get("market_id"), "contract.market_id")
    _text(contract.get("outcome_id"), "contract.outcome_id")
    _text(contract.get("outcome"), "contract.outcome")
    if not isinstance(contract.get("terms"), Mapping) or not isinstance(contract.get("source_identity"), Mapping):
        raise ObservedShadowError("contract terms and source identity are required")
    _time(contract.get("open_at"), "contract.open_at")
    _time(contract.get("cutoff_at"), "contract.cutoff_at")
    _time(contract.get("expiry_at"), "contract.expiry_at")
    return dict(adapter), dict(contract)


def _timing(evidence: Mapping[str, Any], adapter: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    timing = evidence.get("timing")
    if not isinstance(timing, Mapping):
        raise ObservedShadowError("exact event and receipt timestamps are required")
    event_at = _time(timing.get("event_at"), "timing.event_at")
    received_at = _time(timing.get("received_at"), "timing.received_at")
    decision_at = _time(timing.get("decision_at"), "timing.decision_at")
    if timing.get("event_at_source") != "venue_event" or timing.get("received_at_source") != "local_receipt":
        raise ObservedShadowError("event and receipt timestamp provenance is required")
    if event_at > received_at or received_at > decision_at:
        raise ObservedShadowError("event, decision, and receipt chronology is invalid")
    cutoff = _time(contract["cutoff_at"], "contract.cutoff_at")
    expiry = _time(contract["expiry_at"], "contract.expiry_at")
    if event_at < _time(contract["open_at"], "contract.open_at") or event_at > cutoff:
        raise ObservedShadowError("event timestamp is outside contract lifetime")
    if decision_at > cutoff or decision_at > expiry:
        raise ObservedShadowError("decision timestamp is after contract cutoff or expiry")
    events = adapter.get("events")
    if not isinstance(events, list) or len(events) != 1 or not isinstance(events[0], Mapping):
        raise ObservedShadowError("one admitted adapter event is required")
    event = events[0]
    if event.get("event_at") is None or _time(event.get("event_at"), "adapter.events[0].event_at") != event_at:
        raise ObservedShadowError("event timestamp does not match adapter evidence")
    if event.get("received_at") is None or _time(event.get("received_at"), "adapter.events[0].received_at") != received_at:
        raise ObservedShadowError("receipt timestamp does not match adapter evidence")
    return {"event_at": _iso(event_at), "received_at": _iso(received_at), "decision_at": _iso(decision_at),
            "event_at_source": "venue_event", "received_at_source": "local_receipt"}


def _market(evidence: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    market = evidence.get("market_identity")
    if not isinstance(market, Mapping):
        raise ObservedShadowError("market identity is required")
    required = {"venue", "market_id", "outcome_id", "outcome", "terms_sha256", "identity_sha256"}
    if not required.issubset(market):
        raise ObservedShadowError("market identity is incomplete")
    if (market["market_id"], market["outcome_id"], market["outcome"]) != (contract["market_id"], contract["outcome_id"], contract["outcome"]):
        raise ObservedShadowError("market identity differs from admitted contract")
    _text(market.get("venue"), "market_identity.venue")
    _hash(market.get("terms_sha256"), "market_identity.terms_sha256")
    if market["terms_sha256"] != contract["terms"].get("terms_sha256"):
        raise ObservedShadowError("market terms hash differs from admitted contract")
    body = {key: market[key] for key in required if key != "identity_sha256"}
    if _digest(body) != _hash(market.get("identity_sha256"), "market_identity.identity_sha256"):
        raise ObservedShadowError("market identity hash mismatch")
    return dict(market)


def _size(evidence: Mapping[str, Any], adapter: Mapping[str, Any]) -> dict[str, Any]:
    quote = evidence.get("quote")
    if not isinstance(quote, Mapping):
        raise ObservedShadowError("quote evidence is required")
    units = quote.get("size_units")
    if units != "contracts":
        raise ObservedShadowError("contract size units must be explicit")
    quantity = _positive_int(quote.get("contract_size"), "quote.contract_size")
    _source(quote.get("size_provenance"), "quote.size_provenance")
    asks = quote.get("asks")
    if not isinstance(asks, list) or not asks:
        raise ObservedShadowError("executable ask depth is required")
    depth = 0
    for level in asks:
        if not isinstance(level, Mapping):
            raise ObservedShadowError("ask depth level is malformed")
        depth += _positive_int(level.get("size"), "quote.ask.size")
    if depth < quantity:
        raise ObservedShadowError("contract size exceeds admitted ask depth")
    event = adapter["events"][0]
    if event.get("asks") != asks:
        raise ObservedShadowError("quote depth differs from admitted adapter evidence")
    bids = event.get("bids")
    if not isinstance(bids, list) or not bids:
        raise ObservedShadowError("executable bid depth is required")
    return {"contract_size": quantity, "size_units": units, "size_provenance": dict(quote["size_provenance"]),
            "ask": str(asks[0].get("price")), "asks": [dict(level) for level in asks],
            "bids": [dict(level) for level in bids]}


def _fees(evidence: Mapping[str, Any], adapter: Mapping[str, Any]) -> dict[str, Any]:
    fee = evidence.get("fee_schema")
    if not isinstance(fee, Mapping):
        raise ObservedShadowError("versioned fee schema is required")
    _text(fee.get("version"), "fee_schema.version")
    for action in ("entry", "exit", "settlement"):
        _decimal(fee.get(f"{action}_fee_cents_per_contract"), f"fee_schema.{action}_fee_cents_per_contract", low=Decimal("0"))
    _source(fee.get("provenance"), "fee_schema.provenance")
    adapter_fee = adapter.get("fee")
    if not isinstance(adapter_fee, Mapping) or adapter_fee.get("version") != fee.get("version"):
        raise ObservedShadowError("fee schema differs from admitted adapter evidence")
    for action in ("entry", "exit", "settlement"):
        key = f"{action}_fee_cents_per_contract"
        if Decimal(str(adapter_fee.get(key))) != Decimal(str(fee.get(key))):
            raise ObservedShadowError(f"fee schema differs for {key}")
    return dict(fee)


def _settlement(evidence: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    settlement = evidence.get("settlement_definition")
    if not isinstance(settlement, Mapping):
        raise ObservedShadowError("settlement definition is required")
    for field in ("version", "rule", "source_identity", "cutoff_at", "payout_cents", "definition_sha256"):
        if field not in settlement:
            raise ObservedShadowError(f"settlement_definition.{field} is required")
    _text(settlement.get("version"), "settlement_definition.version")
    _text(settlement.get("rule"), "settlement_definition.rule")
    if not isinstance(settlement.get("source_identity"), Mapping):
        raise ObservedShadowError("settlement source identity is required")
    cutoff = _time(settlement.get("cutoff_at"), "settlement_definition.cutoff_at")
    if cutoff != _time(contract["cutoff_at"], "contract.cutoff_at"):
        raise ObservedShadowError("settlement cutoff differs from contract")
    if settlement.get("payout_cents") != 100:
        raise ObservedShadowError("only one-dollar binary payout is supported")
    body = {key: settlement[key] for key in ("version", "rule", "source_identity", "cutoff_at", "payout_cents")}
    if _digest(body) != _hash(settlement.get("definition_sha256"), "settlement_definition.definition_sha256"):
        raise ObservedShadowError("settlement definition hash mismatch")
    _source(settlement.get("provenance"), "settlement_definition.provenance")
    return dict(settlement)


def _probability(evidence: Mapping[str, Any], *, decision_at: datetime) -> dict[str, Any]:
    probability = evidence.get("probability")
    if not isinstance(probability, Mapping):
        raise ObservedShadowError("calibrated probability evidence is required")
    value = _decimal(probability.get("value"), "probability.value", low=Decimal("0"), high=Decimal("1"))
    _text(probability.get("model_version"), "probability.model_version")
    calibration = probability.get("calibration")
    if not isinstance(calibration, Mapping) or calibration.get("eligible") is not True or calibration.get("out_of_sample") is not True:
        raise ObservedShadowError("probability calibration is not eligible")
    _text(calibration.get("protocol_version"), "probability.calibration.protocol_version")
    if _positive_int(calibration.get("development_events"), "probability.calibration.development_events") < 1:
        raise ObservedShadowError("probability calibration development evidence is missing")
    if _positive_int(calibration.get("holdout_events"), "probability.calibration.holdout_events") < 1:
        raise ObservedShadowError("probability calibration holdout evidence is missing")
    provenance = _source(probability.get("provenance"), "probability.provenance")
    known_at = _time(provenance.get("known_at"), "probability.provenance.known_at")
    if known_at > decision_at:
        raise ObservedShadowError("probability evidence was known after decision time")
    result = {"value": str(value), "model_version": probability["model_version"], "calibration": dict(calibration), "provenance": provenance}
    # Bounds are optional at the shadow stage so incomplete research remains a
    # useful, explicit shadow record.  The internal-paper driver requires them
    # before it can calculate a conservative economic edge.
    for field in ("lower_bound", "upper_bound", "confidence_level"):
        if field in probability:
            result[field] = str(_decimal(probability[field], f"probability.{field}", low=Decimal("0"), high=Decimal("1")))
    if "lower_bound" in result and "upper_bound" in result:
        if Decimal(result["lower_bound"]) > value or value > Decimal(result["upper_bound"]):
            raise ObservedShadowError("probability bounds do not contain probability.value")
    return result


def _abstention(reasons: list[str]) -> dict[str, Any]:
    return {
        "status": "abstained",
        "scope": "prospective_observed_shadow_evidence_only",
        "eligible_for_execution": False,
        "candidate": None,
        "candidate_sha256": None,
        "abstention_reasons": sorted(set(reasons)),
        "ledger_mutations": 0,
        "broker_order_count": 0,
        "network_calls": 0,
        "profitability_validated": False,
    }


def run_once(evidence: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    """Evaluate one caller-supplied observed-shadow evidence envelope."""
    try:
        if not isinstance(evidence, Mapping):
            raise ObservedShadowError("evidence envelope must be an object")
        if evidence.get("schema_version") != SCHEMA_VERSION or evidence.get("shadow_version") != SHADOW_VERSION:
            raise ObservedShadowError("shadow evidence schema is unsupported")
        if evidence.get("source_kind") != "observed" or evidence.get("execution_enabled") is not False:
            raise ObservedShadowError("observed, execution-disabled evidence is required")
        current = _time(now, "now") if now is not None else None
        adapter, contract = _adapter_and_contract(evidence)
        timing = _timing(evidence, adapter, contract)
        if current is not None and _time(timing["received_at"], "timing.received_at") > current:
            raise ObservedShadowError("receipt timestamp is in the future")
        market = _market(evidence, contract)
        size = _size(evidence, adapter)
        fees = _fees(evidence, adapter)
        settlement = _settlement(evidence, contract)
        probability = _probability(evidence, decision_at=_time(timing["decision_at"], "timing.decision_at"))
        body = {
            "schema_version": SCHEMA_VERSION,
            "shadow_version": SHADOW_VERSION,
            "source_kind": "observed",
            "contract": contract,
            "market_identity": market,
            "timing": timing,
            "size": size,
            "fee_schema": {key: fees[key] for key in fees if key != "provenance"},
            "settlement_definition": settlement,
            "probability": probability,
            "evidence_hashes": {"adapter": _digest(adapter), "envelope": _digest(evidence)},
        }
        candidate_hash = _digest(body)
        candidate = {**body, "candidate_sha256": candidate_hash}
        return {
            "status": "eligible_shadow_candidate",
            "scope": "prospective_observed_shadow_evidence_only",
            "eligible_for_execution": False,
            "candidate": candidate,
            "candidate_sha256": candidate_hash,
            "ledger_mutations": 0,
            "broker_order_count": 0,
            "network_calls": 0,
            "profitability_validated": False,
            "research_notice": "Observed shadow candidate only; no order, fill, ledger reservation, or profitability claim.",
        }
    except ObservedShadowError as exc:
        return _abstention([str(exc)])
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        return _abstention([f"malformed evidence: {exc}"])


run = run_once


__all__ = ["ObservedShadowError", "run_once", "run"]
