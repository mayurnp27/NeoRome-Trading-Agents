"""Offline adapter from validated debit verticals to the shared paper gateway.

This module validates the existing two-leg options contract, converts its
complete-spread quote into integer cents, and optionally reserves it through
``PaperProposalGateway``.  It never creates a client, loads credentials, or
submits an order.  A later independently observed complete-spread event must
be sent to the common gateway's ``record_fill`` method.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import re
from typing import Any, Mapping

from desks.options.verticals import (
    STANDARD_MULTIPLIER,
    OptionLeg,
    VerticalValidationError,
    validate_vertical,
)
from .paper_gateway import PaperProposalGateway

UTC = timezone.utc
_HASH = re.compile(r"^[0-9a-fA-F]{64}$")
_FORBIDDEN_LABELS = {"label", "leg_label", "role", "leg_role", "name"}


class OptionsGatewayValidationError(ValueError):
    """The options-to-paper-gateway contract cannot be admitted."""


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OptionsGatewayValidationError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OptionsGatewayValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OptionsGatewayValidationError(f"{field} must be an integer >= {minimum}")
    return value


def _decimal(value: Any, field: str, *, minimum: Decimal | None = None) -> Decimal:
    if value is None or isinstance(value, bool):
        raise OptionsGatewayValidationError(f"{field} must be a finite number")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OptionsGatewayValidationError(f"{field} must be a finite number") from exc
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise OptionsGatewayValidationError(f"{field} must be a finite number >= {minimum}")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OptionsGatewayValidationError(f"{field} must be timezone-aware") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsGatewayValidationError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _cents(value: Decimal, field: str, *, rounding: str) -> int:
    cents = (value * Decimal("100")).to_integral_value(rounding=rounding)
    if cents < 0:
        raise OptionsGatewayValidationError(f"{field} cannot be negative")
    return int(cents)


def _raw_quote_timestamp(leg: Mapping[str, Any], index: int) -> datetime:
    quote = leg.get("quote")
    quote = quote if isinstance(quote, Mapping) else {}
    value = leg.get("timestamp", leg.get("quote_timestamp", quote.get("timestamp")))
    return _timestamp(value, f"legs[{index}].quote_timestamp")


def _raw_depth(leg: Mapping[str, Any], index: int) -> int:
    """Require explicit per-leg executable depth; never infer it from size."""
    side = str(leg.get('side', '')).lower()
    field = 'ask_size' if side == 'buy' else 'bid_size'
    quote = leg.get('quote') if isinstance(leg.get('quote'), Mapping) else {}
    value = _integer(leg.get(field, quote.get(field)), f'legs[{index}].{field}', minimum=1)
    if 'depth' in leg and _integer(leg['depth'], 'depth', minimum=1) != value:
        raise OptionsGatewayValidationError('depth conflicts with executable side size')
    return value


def _validate_source(
    payload: Mapping[str, Any], *, observed_at: datetime,
    leg_quote_times: list[datetime], as_of: datetime,
) -> dict[str, Any]:
    source = dict(_mapping(payload.get("source"), "source"))
    source_kind = _text(payload.get("source_kind"), "source_kind")
    if source_kind not in {"observed", "synthetic"} or _text(source.get("kind"), "source.kind") != source_kind:
        raise OptionsGatewayValidationError("source kind must be observed or synthetic and match source.kind")
    source_hash = _text(source.get("hash"), "source.hash")
    if not _HASH.fullmatch(source_hash):
        raise OptionsGatewayValidationError("source.hash must be a 64-character hexadecimal digest")
    if 'observed_at' in source and _timestamp(source['observed_at'], 'source.observed_at') != observed_at:
        raise OptionsGatewayValidationError('source timestamp conflicts with option quotes')
    received = _timestamp(source.get("received_at"), "source.received_at")
    if any(quote_time > received for quote_time in leg_quote_times):
        raise OptionsGatewayValidationError("source.received_at cannot precede either leg quote time")
    if received > as_of:
        raise OptionsGatewayValidationError("source.received_at cannot be in the future")
    valid_until = source.get("valid_until")
    result = {"kind": source_kind, "hash": source_hash.lower(),
              "observed_at": _iso(observed_at), "received_at": _iso(received)}
    if valid_until is not None:
        until = _timestamp(valid_until, "source.valid_until")
        if until <= as_of:
            raise OptionsGatewayValidationError("source.valid_until has expired")
        result["valid_until"] = _iso(until)
    return result


def _leg_mapping(value: Any, index: int) -> Mapping[str, Any]:
    leg = _mapping(value, f"legs[{index}]")
    forbidden = sorted(_FORBIDDEN_LABELS.intersection(leg))
    if forbidden:
        raise OptionsGatewayValidationError(
            f"arbitrary option leg labels are unsupported: {', '.join(forbidden)}"
        )
    return leg


def convert_options_proposal(
    payload: Mapping[str, Any],
    *,
    as_of: datetime,
    max_quote_age: timedelta = timedelta(seconds=60),
    fee_cents_per_contract: int = 0,
    slippage_cents_per_contract: int = 0,
    max_loss_budget: Any = None,
    existing_reserved_risk: Any = "0",
    max_quantity: int | None = None,
) -> dict[str, Any]:
    """Convert one validated debit vertical to common gateway JSON.

    ``payload`` must contain exactly two ordinary option legs with explicit
    depth, a source hash/receipt, strategy identity, exposure/idea identity,
    execution delay, and an absolute proposal expiry.  The adapter does not
    infer missing lifecycle or provenance values. Gateway costs are per leg
    contract; the low-level vertical validator expects combined spread costs.
    """
    raw = _mapping(payload, "proposal")
    as_of_utc = _timestamp(as_of, "as_of")
    proposal_id = _text(raw.get("proposal_id"), "proposal_id")
    strategy = _text(raw.get("strategy"), "strategy")
    strategy_version = _text(raw.get("strategy_version"), "strategy_version")
    idea_id = _text(raw.get("idea_id"), "idea_id")
    execution = _mapping(raw.get("execution"), "execution")
    delay = _integer(execution.get("delay_seconds"), "execution.delay_seconds")
    expiry = _timestamp(raw.get("expiry_at"), "expiry_at")
    if expiry <= as_of_utc:
        raise OptionsGatewayValidationError("expiry_at must be after as_of")
    if isinstance(max_quote_age, timedelta) is False or not 0 <= max_quote_age.total_seconds() <= 60:
        raise OptionsGatewayValidationError("max_quote_age must be between zero and sixty seconds")
    fee_cents = _integer(fee_cents_per_contract, "fee_cents_per_contract")
    slippage_cents = _integer(slippage_cents_per_contract, "slippage_cents_per_contract")
    legs_raw = raw.get("legs")
    if not isinstance(legs_raw, list) or len(legs_raw) != 2:
        raise OptionsGatewayValidationError("options gateway requires exactly two legs")
    legs = [_leg_mapping(value, index) for index, value in enumerate(legs_raw)]
    depths = [_raw_depth(leg, index) for index, leg in enumerate(legs)]
    quote_times = [_raw_quote_timestamp(leg, index) for index, leg in enumerate(legs)]
    if any(quote_time > as_of_utc for quote_time in quote_times):
        raise OptionsGatewayValidationError("option leg quote timestamp cannot be in the future")
    if any(as_of_utc - quote_time > max_quote_age for quote_time in quote_times):
        raise OptionsGatewayValidationError("option leg quote is stale")
    # A complete spread is only as current as its oldest component. Preserve
    # both provider timestamps, and use the older one for shared freshness and
    # post-reservation delay enforcement rather than fabricating simultaneity.
    observed_at = min(quote_times)
    source = _validate_source(
        raw, observed_at=observed_at, leg_quote_times=quote_times, as_of=as_of_utc
    )
    try:
        validated = validate_vertical(
            legs,
            as_of=as_of_utc,
            max_quote_age=max_quote_age,
            max_loss_budget=max_loss_budget,
            existing_reserved_risk=existing_reserved_risk,
            fee_per_contract=Decimal(fee_cents * len(legs)) / Decimal("100"),
            slippage_per_contract=Decimal(slippage_cents * len(legs)) / Decimal("100"),
            max_quantity=max_quantity,
        )
    except VerticalValidationError as exc:
        raise OptionsGatewayValidationError(str(exc)) from exc
    if not validated.kind.endswith("debit"):
        raise OptionsGatewayValidationError("only defined-risk debit verticals are admitted")
    if validated.legs[0].quantity != validated.legs[1].quantity:
        raise OptionsGatewayValidationError("both option legs must have equal complete-spread quantity")
    quantity = validated.quantity
    complete_depth = min(depths)
    if complete_depth < quantity:
        raise OptionsGatewayValidationError("complete-spread depth is insufficient for all contracts")
    buy = next(leg for leg in validated.legs if leg.side.lower() == "buy")
    sell = next(leg for leg in validated.legs if leg.side.lower() == "sell")
    buy_bid = _decimal(buy.bid, "buy bid", minimum=Decimal("0"))
    buy_ask = _decimal(buy.ask, "buy ask", minimum=Decimal("0"))
    sell_bid = _decimal(sell.bid, "sell bid", minimum=Decimal("0"))
    sell_ask = _decimal(sell.ask, "sell ask", minimum=Decimal("0"))
    spread_ask_dollars = (buy_ask - sell_bid) * STANDARD_MULTIPLIER
    spread_bid_dollars = (buy_bid - sell_ask) * STANDARD_MULTIPLIER
    if spread_bid_dollars < 0:
        raise OptionsGatewayValidationError('negative liquidation quote cannot be represented by the shared gateway')
    ask_cents = _cents(spread_ask_dollars, "complete-spread ask", rounding=ROUND_CEILING)
    bid_cents = _cents(spread_bid_dollars, "complete-spread bid", rounding=ROUND_FLOOR)
    if ask_cents <= 0 or bid_cents > ask_cents:
        raise OptionsGatewayValidationError("complete-spread cents are invalid")
    total_fee_cents = (fee_cents + slippage_cents) * quantity * len(validated.legs)
    underlying = validated.legs[0].underlying.strip().upper()
    right = validated.legs[0].right.upper()[0]
    expiry_date = validated.legs[0].expiry
    if isinstance(expiry_date, datetime):
        expiry_date = expiry_date.date()
    elif isinstance(expiry_date, str):
        expiry_date = date.fromisoformat(expiry_date)
    exposure_group = 'us-broad-equity' if underlying in {'SPY', 'QQQ', 'IWM'} else f'us-equity:{underlying}'
    supplied_group = raw.get("exposure_group")
    if supplied_group is not None and _text(supplied_group, "exposure_group") != exposure_group:
        raise OptionsGatewayValidationError("exposure_group must use the canonical underlying group")
    instrument = "vertical:" + ":".join(sorted(leg.symbol.replace(" ", "") for leg in validated.legs))
    metadata = raw.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise OptionsGatewayValidationError("metadata must be an object")
    adapter_metadata = {
        "validated_kind": validated.kind,
        "underlying": underlying,
        "right": right,
        "expiry": expiry_date.isoformat(),
        "multiplier": STANDARD_MULTIPLIER,
        "complete_spread_unit": "one complete 1:1 two-leg spread",
        "leg_depths": depths,
        "complete_spread_depth": complete_depth,
        "leg_quote_timestamps": [
            {"symbol": leg.symbol.replace(" ", ""), "timestamp": _iso(timestamp)}
            for leg, timestamp in zip(validated.legs, quote_times)
        ],
        "quantity_contracts": quantity,
        "signed_net_debit_dollars_per_share": str(validated.signed_net_debit),
        "max_loss_dollars": str(validated.max_loss),
    }
    combined_metadata = dict(metadata)
    combined_metadata["options_gateway_v1"] = adapter_metadata
    return {
        "proposal_id": proposal_id,
        "desk": "options",
        "strategy": strategy,
        "strategy_version": strategy_version,
        "source_kind": source["kind"],
        "source": source,
        "instrument": instrument,
        "instrument_type": "debit_spread",
        "side": "long",
        "exposure_group": exposure_group,
        "idea_id": idea_id,
        "quantity": quantity,
        "expiry_at": _iso(expiry),
        "quote": {
            "timestamp": _iso(observed_at),
            "bid_cents": bid_cents,
            "ask_cents": ask_cents,
            "depth": complete_depth,
        },
        "cost": {"unit_cost_cents": ask_cents, "fee_cents": total_fee_cents},
        "execution": {"delay_seconds": delay},
        "legs": [
            {"symbol": leg.symbol.replace(" ", ""), "side": leg.side.lower(), "ratio": 1}
            for leg in validated.legs
        ],
        "metadata": combined_metadata,
    }


def reserve_options_proposal(
    payload: Mapping[str, Any],
    gateway: PaperProposalGateway,
    *,
    as_of: datetime,
    now: datetime | str | None = None,
    max_quote_age: timedelta = timedelta(seconds=60),
    fee_cents_per_contract: int = 0,
    slippage_cents_per_contract: int = 0,
    max_loss_budget: Any = None,
    existing_reserved_risk: Any = "0",
    max_quantity: int | None = None,
    full_port: bool = False,
) -> dict[str, Any]:
    """Convert and reserve through the existing shared ledger gateway."""
    if not isinstance(gateway, PaperProposalGateway):
        raise OptionsGatewayValidationError("gateway must be a PaperProposalGateway")
    limits = gateway.ledger.limits
    if not isinstance(full_port, bool):
        raise OptionsGatewayValidationError("full_port must be a bool")
    expected = (30_000, 30_000, 30_000) if full_port else (1_500, 3_000, 8_000)
    if (not gateway.ledger.enforce_market_risk or limits.per_idea_cents['options'] != expected[0]
            or limits.group_cap_cents != expected[1] or limits.aggregate_cap_cents != expected[2]):
        raise OptionsGatewayValidationError('firm market-risk enforcement and budget ceilings are required')
    proposal = convert_options_proposal(
        payload,
        as_of=as_of,
        max_quote_age=max_quote_age,
        fee_cents_per_contract=fee_cents_per_contract,
        slippage_cents_per_contract=slippage_cents_per_contract,
        max_loss_budget=max_loss_budget,
        existing_reserved_risk=existing_reserved_risk,
        max_quantity=max_quantity,
    )
    return gateway.reserve(proposal, now=now if now is not None else as_of)


prepare_options_proposal = convert_options_proposal

__all__ = [
    "OptionsGatewayValidationError",
    "convert_options_proposal",
    "prepare_options_proposal",
    "reserve_options_proposal",
]
