"""Adapt an archived public Kalshi BTC interval snapshot for research replay.

This module is deliberately stricter than a venue convenience parser.  It
binds one market and outcome to its exact terms/source identity, preserves raw
book-level provenance, and derives complementary binary asks with Decimal
arithmetic.  An HTTP receipt is never promoted to an exchange event time, so
the archived public snapshot is engineering evidence and cannot become a fill
input without an explicit event timestamp from a separate recorder.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


UTC = "UTC"
SOURCE_NAME = "CF Benchmarks"
SOURCE_INDEX = "BRTI"
EXPECTED_SERIES = "KXBTC15M"


class IntervalAdapterError(ValueError):
    """The archived capture is malformed for the bounded adapter."""


def _load(value: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(Path(value).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        raise IntervalAdapterError("capture input is not readable JSON") from exc
    if not isinstance(parsed, Mapping):
        raise IntervalAdapterError("capture input must be a JSON object")
    return dict(parsed)


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise IntervalAdapterError("terms are not canonical JSON") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise IntervalAdapterError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise IntervalAdapterError(f"{field} must be numeric") from exc
    if not result.is_finite() or result < 0 or result > 1:
        raise IntervalAdapterError(f"{field} must be between 0 and 1")
    return result


def _nonnegative_decimal(value: Any, field: str) -> Decimal:
    """Parse a finite nonnegative amount without applying probability bounds."""
    if isinstance(value, bool) or value is None:
        raise IntervalAdapterError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise IntervalAdapterError(f"{field} must be numeric") from exc
    if not result.is_finite() or result < 0:
        raise IntervalAdapterError(f"{field} must be nonnegative")
    return result


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntervalAdapterError(f"{field} is required")
    # Preserve the source spelling in the contract identity while rejecting
    # dates that cannot be ordered.  The replay layer performs final UTC
    # normalization when an explicit event timestamp is supplied.
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntervalAdapterError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntervalAdapterError(f"{field} must include a timezone")
    return value


def _capture_record(data: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    record = data.get(field)
    if not isinstance(record, Mapping):
        raise IntervalAdapterError(f"capture missing {field} record")
    if not isinstance(record.get("payload"), Mapping):
        raise IntervalAdapterError(f"capture {field} payload is missing")
    return record


def _first_market(data: Mapping[str, Any]) -> Mapping[str, Any]:
    markets = _capture_record(data, "markets")["payload"].get("markets")
    if not isinstance(markets, Sequence) or isinstance(markets, (str, bytes)) or len(markets) != 1:
        raise IntervalAdapterError("adapter requires exactly one captured market")
    market = markets[0]
    if not isinstance(market, Mapping):
        raise IntervalAdapterError("captured market is malformed")
    return market


def _terms(market: Mapping[str, Any], series: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "event_ticker", "ticker", "rules_primary", "rules_secondary", "strike_type",
        "custom_strike", "occurrence_datetime", "open_time", "close_time",
        "expected_expiration_time", "expiration_time", "price_level_structure",
    )
    value = {field: market.get(field) for field in fields}
    value["series_ticker"] = series.get("ticker")
    value["series_title"] = series.get("title")
    if not value["rules_primary"] or not value["rules_secondary"]:
        raise IntervalAdapterError("market settlement terms are incomplete")
    terms_text = f"{value['rules_primary']} {value['rules_secondary']}"
    if SOURCE_NAME not in terms_text or SOURCE_INDEX not in terms_text:
        raise IntervalAdapterError("terms do not bind CF Benchmarks BRTI")
    value["terms_sha256"] = _hash(value)
    return value


def _raw_levels(raw: Any, side: str) -> list[dict[str, Any]]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise IntervalAdapterError(f"orderbook_fp.{side}_dollars must be a list")
    levels: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)) or len(item) != 2:
            raise IntervalAdapterError(f"orderbook_fp.{side}_dollars[{index}] is malformed")
        price = _decimal(item[0], f"{side}[{index}].price")
        try:
            size = Decimal(str(item[1]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise IntervalAdapterError(f"{side}[{index}].size must be numeric") from exc
        if not size.is_finite() or size <= 0:
            raise IntervalAdapterError(f"{side}[{index}].size must be positive")
        levels.append({"price": price, "raw_size": str(item[1]), "raw_index": index})
    if len({level["price"] for level in levels}) != len(levels):
        raise IntervalAdapterError(f"{side} contains duplicate prices")
    return levels


def _normalized_levels(
    raw: Sequence[Mapping[str, Any]], *, complement: bool, raw_side: str, size_unit: str | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    levels: list[dict[str, Any]] = []
    reasons: list[str] = []
    for level in raw:
        price = (Decimal("1") - level["price"]) if complement else level["price"]
        size = Decimal(level["raw_size"])
        if size_unit != "contracts":
            reasons.append("ambiguous_orderbook_size_units")
        if size != size.to_integral_value():
            reasons.append("non_integer_contract_depth")
            normalized_size: int | None = None
        else:
            normalized_size = int(size)
        levels.append(
            {
                "price": str(price),
                "size": normalized_size,
                "raw_price": str(level["price"]),
                "raw_size": level["raw_size"],
                "raw_side": raw_side,
                "raw_index": level["raw_index"],
                "complemented": complement,
            }
        )
    levels.sort(key=lambda item: Decimal(item["price"]))
    return levels, [dict(level) for level in levels], sorted(set(reasons))


def _normalize_book(book: Mapping[str, Any], *, outcome: str, size_unit: str | None) -> tuple[dict[str, Any], list[str]]:
    payload = book.get("payload")
    if not isinstance(payload, Mapping):
        return {}, ["book_payload_missing"]
    orderbook = payload.get("orderbook_fp")
    if not isinstance(orderbook, Mapping):
        return {}, ["orderbook_fp_missing"]
    try:
        yes = _raw_levels(orderbook.get("yes_dollars"), "yes")
        no = _raw_levels(orderbook.get("no_dollars"), "no")
    except IntervalAdapterError as exc:
        return {}, [str(exc)]
    if outcome == "YES":
        bid_raw, ask_raw, bid_side, ask_side = yes, no, "yes", "no"
    else:
        bid_raw, ask_raw, bid_side, ask_side = no, yes, "no", "yes"
    bids, _bid_provenance, bid_reasons = _normalized_levels(bid_raw, complement=False, raw_side=bid_side, size_unit=size_unit)
    asks, _ask_provenance, ask_reasons = _normalized_levels(ask_raw, complement=True, raw_side=ask_side, size_unit=size_unit)
    bids.sort(key=lambda item: Decimal(item["price"]), reverse=True)
    reasons = bid_reasons + ask_reasons
    if bids and asks and Decimal(bids[0]["price"]) >= Decimal(asks[0]["price"]):
        reasons.append("crossed_normalized_book")
    return {"bids": bids, "asks": asks}, sorted(set(reasons))


def adapt_interval_observed(
    value: Mapping[str, Any] | str | Path,
    *,
    fee_schedule: Mapping[str, Any] | None = None,
    outcome: str = "YES",
) -> dict[str, Any]:
    """Return a replay-shaped, provenance-preserving engineering record.

    ``fee_schedule`` is accepted only as a versioned mapping.  The captured
    series advertises quadratic fees, but this adapter intentionally does not
    implement a formula from that label.  Without a caller-supplied schedule,
    the fee status remains unresolved.
    """
    data = _load(value)
    reasons: list[str] = []
    series_record = _capture_record(data, "series")
    series = series_record["payload"].get("series")
    if not isinstance(series, Mapping):
        raise IntervalAdapterError("series payload is malformed")
    market = _first_market(data)
    if outcome not in ("YES", "NO"):
        raise IntervalAdapterError("outcome must be YES or NO")
    if series.get("ticker") != EXPECTED_SERIES:
        reasons.append("unexpected_series_ticker")
    terms = _terms(market, series)
    market_ticker = market.get("ticker")
    engine = series.get("exchange_index")
    if not isinstance(engine, int) or isinstance(engine, bool) or engine < 0:
        reasons.append("missing_engine_identity")
    if market.get("exchange_index") != engine:
        reasons.append("engine_identity_mismatch")
    source_identity = {
        "venue": "Kalshi",
        "engine_id": engine,
        "series_ticker": series.get("ticker"),
        "settlement_source": SOURCE_NAME,
        "benchmark": SOURCE_INDEX,
    }
    try:
        open_at = _timestamp(market.get("open_time"), "market.open_time")
        cutoff_at = _timestamp(market.get("close_time"), "market.close_time")
        expiry_at = _timestamp(market.get("expected_expiration_time"), "market.expected_expiration_time")
    except IntervalAdapterError as exc:
        reasons.append(str(exc))
        open_at = cutoff_at = expiry_at = None
    book_records = data.get("books")
    if not isinstance(book_records, Sequence) or isinstance(book_records, (str, bytes)) or len(book_records) != 1:
        reasons.append("exactly_one_book_required")
        book_records = []
    book = book_records[0] if book_records and isinstance(book_records[0], Mapping) else {}
    receipt_at = book.get("received_at") if isinstance(book, Mapping) else None
    if not isinstance(receipt_at, str):
        reasons.append("missing_local_receipt_timestamp")
    # The capture stores no exchange timestamp.  In particular, requested_at
    # and received_at are never copied to event_at.
    event_at = None
    if isinstance(book, Mapping) and isinstance(book.get("event_at"), str):
        try:
            event_at = _timestamp(book["event_at"], "book.event_at")
        except IntervalAdapterError as exc:
            reasons.append(str(exc))
    else:
        reasons.append("missing_exchange_event_timestamp")
    normalized_book, book_reasons = _normalize_book(book, outcome=outcome, size_unit=book.get("size_unit") if isinstance(book, Mapping) else None)
    reasons.extend(book_reasons)
    required_fee_fields = (
        "entry_fee_cents_per_contract", "exit_fee_cents_per_contract", "settlement_fee_cents_per_contract"
    )
    if fee_schedule is None:
        fee = {"status": "unresolved", "reason": "quadratic_fee_formula_unverified", "required": True}
    elif not isinstance(fee_schedule, Mapping) or not isinstance(fee_schedule.get("version"), str) or not fee_schedule.get("version").strip():
        reasons.append("fee_schedule_version_required")
        fee = {"status": "unresolved", "reason": "invalid_caller_fee_schedule", "required": True}
    elif any(field not in fee_schedule for field in required_fee_fields):
        reasons.append("fee_schedule_fields_required")
        fee = {"status": "unresolved", "reason": "invalid_caller_fee_schedule", "required": True, **dict(fee_schedule)}
    else:
        fee_values_valid = True
        for field in required_fee_fields:
            try:
                if isinstance(fee_schedule[field], bool) or _nonnegative_decimal(fee_schedule[field], field) < 0:
                    fee_values_valid = False
            except IntervalAdapterError:
                fee_values_valid = False
        if not fee_values_valid:
            reasons.append("fee_schedule_values_invalid")
            fee = {"status": "unresolved", "reason": "invalid_caller_fee_schedule", "required": True, **dict(fee_schedule)}
        else:
            fee = {"status": "caller_supplied_fixed_stress", "required": False, **dict(fee_schedule)}
    if fee["status"] == "unresolved":
        reasons.append("fee_model_unresolved")
    event = {
        "event_type": "book",
        "market_id": market_ticker,
        "outcome_id": f"{market_ticker}:{outcome}",
        "outcome": outcome,
        "terms": terms,
        "source_identity": source_identity,
        "open_at": open_at,
        "cutoff_at": cutoff_at,
        "expiry_at": expiry_at,
        "event_at": event_at,
        "received_at": receipt_at,
        **normalized_book,
        "raw_book_sha256": book.get("sha256") if isinstance(book, Mapping) else None,
        "raw_level_provenance": {
            "bids": [
                {key: level[key] for key in ("raw_side", "raw_index", "raw_price", "raw_size", "complemented")}
                for level in normalized_book.get("bids", [])
            ],
            "asks": [
                {key: level[key] for key in ("raw_side", "raw_index", "raw_price", "raw_size", "complemented")}
                for level in normalized_book.get("asks", [])
            ],
        },
    }
    contract = {key: event[key] for key in ("market_id", "outcome_id", "outcome", "terms", "source_identity", "open_at", "cutoff_at", "expiry_at")}
    admissible = not reasons
    return {
        "scope": "offline_research_only",
        "execution_enabled": False,
        "paper_admitted": False,
        "admissible_for_replay": admissible,
        "admission_reasons": sorted(set(reasons)),
        "contract": contract,
        "events": [event],
        "fee": fee,
        "provenance": {
            "series_sha256": series_record.get("sha256"),
            "market_sha256": _capture_record(data, "markets").get("sha256"),
            "book_sha256": book.get("sha256") if isinstance(book, Mapping) else None,
            "local_receipt_at": receipt_at,
            "engine_id": engine,
            "terms_sha256": terms.get("terms_sha256"),
        },
    }


adapt = adapt_interval_observed


__all__ = ["IntervalAdapterError", "adapt", "adapt_interval_observed"]
