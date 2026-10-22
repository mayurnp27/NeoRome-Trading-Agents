"""Pure deterministic selector for the registered OPT-001 option experiment.

The selector consumes caller-supplied evidence only.  It does not fetch data,
construct orders, reserve risk, mark a ledger, or claim a fill or profit.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
STRATEGY_VERSION = "OPT-001-v1"
MAX_ENTRY_COST = Decimal("15")
TARGET_DTE = 45
MIN_DTE = 30
MAX_DTE = 60
QUOTE_MAX_AGE_SECONDS = 60
MAX_DELTA_ERROR = Decimal("0.10")
STANDARD_MULTIPLIER = 100
_OCC_RE = re.compile(r"^(?P<root>[A-Z]{1,6})(?P<date>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")
_HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class SelectionInputError(ValueError):
    """Raised internally for an invalid caller-supplied evidence set."""


def _decimal(value: Any, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise SelectionInputError(f"{field} must be a finite number")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise SelectionInputError(f"{field} must be a finite number") from None
    if not parsed.is_finite() or (positive and parsed <= 0) or (nonnegative and parsed < 0):
        raise SelectionInputError(f"{field} must be a finite number")
    return parsed


def _datetime(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise SelectionInputError(f"{field} must be timezone-aware") from None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SelectionInputError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _date(value: Any, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise SelectionInputError(f"{field} must be an ISO date")


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _HASH_RE.fullmatch(value):
        raise SelectionInputError(f"{field} must be a SHA-256 hex digest")
    return value.lower()


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError):
        raise SelectionInputError("normalized payload is not canonical JSON") from None


def payload_sha256(value: Any, *, hash_field: str | None = None) -> str:
    """Hash a normalized JSON payload, optionally excluding its hash field."""
    if hash_field is not None:
        if not isinstance(value, Mapping):
            raise SelectionInputError("hashed payload must be a mapping")
        value = {key: item for key, item in value.items() if key != hash_field}
    return hashlib.sha256(_canonical(value)).hexdigest()


def _verify_payload_hash(value: Mapping[str, Any], field: str) -> None:
    expected = _hash(value.get(field), field)
    if payload_sha256(value, hash_field=field) != expected:
        raise SelectionInputError(f"{field} does not match normalized payload")


def _evidence_ref(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise SelectionInputError(f"{field} is missing")
    identifier = value.get("evidence_id")
    if not isinstance(identifier, str) or not identifier.strip():
        raise SelectionInputError(f"{field} evidence_id is missing")
    source = value.get("source")
    if not isinstance(source, str) or not source.strip():
        raise SelectionInputError(f"{field} source is missing")
    return {
        "evidence_id": identifier,
        "sha256": _hash(value.get("sha256"), f"{field} sha256"),
        "source": source,
    }


def _no_trade(reason_code: str, reason: str, *, evidence: Sequence[Mapping[str, str]] = ()) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "strategy_version": STRATEGY_VERSION,
        "status": "no_trade",
        "reason_code": reason_code,
        "reason": reason,
        "candidate": None,
        "evidence": [dict(item) for item in evidence],
        "evidence_ids": [item["evidence_id"] for item in evidence],
        "fill_status": "not_a_fill",
        "profit_approved": False,
        "approved_profit": False,
        "hash_scope": "caller_attested_normalized_payload",
    }


def _parse_occ(symbol: Any) -> tuple[str, date, str, Decimal]:
    if not isinstance(symbol, str):
        raise SelectionInputError("option symbol is malformed")
    match = _OCC_RE.fullmatch(symbol)
    if not match:
        raise SelectionInputError("option symbol is malformed")
    encoded = match.group("date")
    try:
        expiry = date(2000 + int(encoded[:2]), int(encoded[2:4]), int(encoded[4:6]))
    except ValueError:
        raise SelectionInputError("option symbol expiration is malformed") from None
    strike = _decimal(Decimal(match.group("strike")) / Decimal("1000"), "option symbol strike", positive=True)
    return match.group("root"), expiry, match.group("right"), strike


def _validate_evidence(observation: Mapping[str, Any], context: Mapping[str, Any], symbol: str) -> list[dict[str, str]]:
    _verify_payload_hash(observation, "normalized_payload_sha256")
    _verify_payload_hash(context, "normalized_payload_sha256")
    raw = observation.get("evidence")
    if not isinstance(raw, list) or not raw:
        raise SelectionInputError("observation evidence provenance is missing")
    evidence = [_evidence_ref(item, f"observation evidence {index}") for index, item in enumerate(raw)]
    calendar = _evidence_ref(context.get("session_calendar_evidence"), "session calendar evidence")
    option = _evidence_ref(context.get("option_evidence"), "option evidence")
    evidence.extend((calendar, option))
    ids = [item["evidence_id"] for item in evidence]
    if len(ids) != len(set(ids)):
        raise SelectionInputError("evidence identifiers must be unique")
    if observation.get("symbol") != symbol:
        raise SelectionInputError("observation symbol does not match context symbol")
    if observation.get("source_kind") != "observed":
        raise SelectionInputError("observation source_kind must be observed")
    if observation.get("bars_complete") is not True:
        raise SelectionInputError("daily bars must be explicitly completed")
    if context.get("session_verified") is not True:
        raise SelectionInputError("current market session is not explicitly verified")
    chain = context.get("chain_metadata")
    if not isinstance(chain, Mapping) or chain.get("complete") is not True:
        raise SelectionInputError("complete chain metadata is required")
    universe = context.get("candidate_universe")
    if (
        not isinstance(universe, Mapping)
        or universe.get("complete") is not True
        or universe.get("limit") != 100
        or universe.get("next_page_token") not in (None, "")
    ):
        raise SelectionInputError("candidate universe is incomplete")
    return evidence


def _validate_sessions(context: Mapping[str, Any], now: datetime) -> tuple[list[date], date]:
    sessions = context.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        raise SelectionInputError("session calendar is missing")
    records: dict[date, tuple[datetime, datetime]] = {}
    for index, item in enumerate(sessions):
        if not isinstance(item, Mapping):
            raise SelectionInputError(f"session calendar row {index} is invalid")
        session_date = _date(item.get("date"), f"session {index} date")
        if session_date in records:
            raise SelectionInputError("session calendar contains duplicates")
        opened = _datetime(item.get("open_at"), f"session {index} open_at")
        closed = _datetime(item.get("close_at"), f"session {index} close_at")
        if opened.date() != session_date or closed.date() != session_date or not opened < closed:
            raise SelectionInputError("session calendar hours are malformed")
        records[session_date] = (opened, closed)
    today = now.astimezone(NY).date()
    current = records.get(today)
    current_session = context.get("current_session")
    if (
        current is None
        or not isinstance(current_session, Mapping)
        or current_session.get("is_open") is not True
        or _date(current_session.get("date"), "current session date") != today
    ):
        raise SelectionInputError("current market session is not open and verified")
    if not current[0] <= now < current[1]:
        raise SelectionInputError("current time is outside the verified market session")
    ordered = sorted(records)
    prior = [item for item in ordered if item < today]
    if len(prior) < 200:
        raise SelectionInputError("session calendar has fewer than 200 prior sessions")
    return prior, today


def _validate_bars(observation: Mapping[str, Any], prior_sessions: Sequence[date], today: date, now: datetime, observation_received: datetime) -> Decimal:
    bars = observation.get("bars")
    if not isinstance(bars, list) or len(bars) < 200:
        raise SelectionInputError("fewer than 200 completed daily bars")
    parsed: list[tuple[date, datetime, Decimal]] = []
    seen: set[date] = set()
    for index, row in enumerate(bars):
        if not isinstance(row, Mapping):
            raise SelectionInputError(f"daily bar {index} is malformed")
        timestamp = _datetime(row.get("timestamp"), f"daily bar {index} timestamp")
        if timestamp > now or timestamp > observation_received:
            raise SelectionInputError("daily bar is in the future of its observation")
        if row.get("complete", True) is not True:
            raise SelectionInputError("daily bar is incomplete")
        if "adjusted" in row and row.get("adjusted") is not False:
            raise SelectionInputError("daily bar adjustment metadata is unsupported")
        session_date = _date(row.get("session_date"), f"daily bar {index} session_date")
        if session_date != timestamp.astimezone(NY).date():
            raise SelectionInputError("daily bar session_date does not match its timestamp")
        for field in ("published_at", "known_at"):
            if field in row:
                known = _datetime(row.get(field), f"daily bar {index} {field}")
                if known > now or known > observation_received:
                    raise SelectionInputError("daily bar known time is in the future of observation")
        if session_date in seen:
            raise SelectionInputError("daily bars contain duplicate sessions")
        seen.add(session_date)
        opening = _decimal(row.get("o"), f"daily bar {index} open", positive=True)
        high = _decimal(row.get("h"), f"daily bar {index} high", positive=True)
        low = _decimal(row.get("l"), f"daily bar {index} low", positive=True)
        close = _decimal(row.get("c"), f"daily bar {index} close", positive=True)
        if low > min(opening, close) or high < max(opening, close) or low > high:
            raise SelectionInputError(f"daily bar {index} OHLC is inconsistent")
        parsed.append((session_date, timestamp, close))
    dates = [item[0] for item in parsed]
    if dates != sorted(dates):
        raise SelectionInputError("daily bars are unordered")
    expected = list(prior_sessions[-200:])
    if [item[0] for item in parsed[-200:]] != expected:
        raise SelectionInputError("daily bars do not match the last 200 prior sessions")
    if any(item[0] >= today for item in parsed):
        raise SelectionInputError("daily bars include a current or future session")
    if parsed[-1][0] != expected[-1]:
        raise SelectionInputError("latest daily bar is not the prior completed session")
    closes = [item[2] for item in parsed[-200:]]
    return closes[-1] - sum(closes, Decimal("0")) / Decimal(200)


def _validate_candidate(row: Mapping[str, Any], symbol: str, now: datetime, option_received: datetime, index: int) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise SelectionInputError(f"candidate {index} is malformed")
    if row.get("underlying") != symbol or row.get("adjusted") is not False:
        raise SelectionInputError(f"candidate {index} has mismatched or adjusted contract metadata")
    if row.get("multiplier") != STANDARD_MULTIPLIER:
        raise SelectionInputError(f"candidate {index} has unsupported multiplier")
    root, expiry_from_symbol, right_from_symbol, strike_from_symbol = _parse_occ(row.get("symbol"))
    expiry = _date(row.get("expiry"), f"candidate {index} expiry")
    right = row.get("right")
    if root != symbol or expiry != expiry_from_symbol or right not in {"C", "P"} or right != right_from_symbol:
        raise SelectionInputError(f"candidate {index} identity does not match its OCC symbol")
    strike = _decimal(row.get("strike"), f"candidate {index} strike", positive=True)
    if strike != strike_from_symbol:
        raise SelectionInputError(f"candidate {index} strike does not match its OCC symbol")
    identifier = row.get("instrument_id")
    if not isinstance(identifier, str) or not identifier:
        raise SelectionInputError(f"candidate {index} instrument_id is missing")
    timestamp = _datetime(row.get("timestamp"), f"candidate {index} timestamp")
    age = (now - timestamp).total_seconds()
    if timestamp > option_received or age < 0 or age > QUOTE_MAX_AGE_SECONDS:
        raise SelectionInputError(f"candidate {index} quote is stale or in the future")
    bid = _decimal(row.get("bid"), f"candidate {index} bid", positive=True)
    ask = _decimal(row.get("ask"), f"candidate {index} ask", positive=True)
    if bid > ask:
        raise SelectionInputError(f"candidate {index} quote is crossed")
    for field in ("bid_size", "ask_size"):
        value = row.get(field)
        if type(value) is not int or value < 1:
            raise SelectionInputError(f"candidate {index} has invalid executable depth")
    delta = _decimal(row.get("delta"), f"candidate {index} delta")
    if not -1 <= delta <= 1:
        raise SelectionInputError(f"candidate {index} delta is out of range")
    _decimal(row.get("implied_volatility"), f"candidate {index} implied volatility", positive=True)
    return {
        "instrument_id": identifier,
        "symbol": row["symbol"],
        "underlying": symbol,
        "expiry": expiry,
        "right": right,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "bid_size": row["bid_size"],
        "ask_size": row["ask_size"],
        "delta": delta,
        "timestamp": timestamp,
    }


def _ranked_pairs(rows: Sequence[dict[str, Any]], *, right: str, expiry: date, direction: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    group = [row for row in rows if row["right"] == right and row["expiry"] == expiry]
    if right == "C" and direction == "up":
        long_rows = [row for row in group if row["delta"] > 0]
        short_rows = [row for row in group if row["delta"] > 0]
        pair_filter = lambda long, short: long["strike"] < short["strike"]
    elif right == "P" and direction == "down":
        long_rows = [row for row in group if row["delta"] < 0]
        short_rows = [row for row in group if row["delta"] < 0]
        pair_filter = lambda long, short: long["strike"] > short["strike"]
    else:
        return []
    long_rows = [row for row in long_rows if abs(abs(row["delta"]) - Decimal("0.50")) <= MAX_DELTA_ERROR]
    short_rows = [row for row in short_rows if abs(abs(row["delta"]) - Decimal("0.25")) <= MAX_DELTA_ERROR]
    pairs = [(long, short) for long in long_rows for short in short_rows if pair_filter(long, short)]
    if not pairs:
        return []
    pairs.sort(key=lambda pair: (
        abs(abs(pair[0]["delta"]) - Decimal("0.50")) + abs(abs(pair[1]["delta"]) - Decimal("0.25")),
        abs(abs(pair[0]["delta"]) - Decimal("0.50")),
        abs(abs(pair[1]["delta"]) - Decimal("0.25")),
        pair[0]["strike"],
        pair[1]["strike"],
        pair[0]["instrument_id"],
        pair[1]["instrument_id"],
    ))
    return pairs


def _choose(rows: Sequence[dict[str, Any]], *, right: str, expiry: date, direction: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    pairs = _ranked_pairs(rows, right=right, expiry=expiry, direction=direction)
    return pairs[0] if pairs else None


def _catalog_expiries(context: Mapping[str, Any], symbol: str, right: str) -> set[date]:
    chain = context.get("chain_metadata")
    if not isinstance(chain, Mapping) or chain.get("symbol") != symbol:
        raise SelectionInputError("chain metadata symbol does not match")
    available = chain.get("available_expirations")
    if not isinstance(available, Mapping) or not isinstance(available.get(right), list) or not available[right]:
        raise SelectionInputError("directional expiration catalog is missing")
    result = {_date(item, f"{right} expiration catalog entry") for item in available[right]}
    if len(result) != len(available[right]):
        raise SelectionInputError("directional expiration catalog contains duplicates")
    return result


def select(
    observation: Mapping[str, Any],
    context: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    fee_per_leg: Any = None,
    slippage_per_leg: Any = None,
    max_entry_cost: Any = None,
) -> dict[str, Any]:
    """Return one deterministic OPT-001 candidate or a conservative abstention.

    ``fee_per_leg`` and ``slippage_per_leg`` are required dollar assumptions;
    they are charged twice for one whole two-leg spread.
    """
    evidence: list[dict[str, str]] = []
    try:
        if not isinstance(observation, Mapping) or not isinstance(context, Mapping):
            raise SelectionInputError("observation and context are required mappings")
        symbol = context.get("symbol")
        if symbol not in {"SPY", "QQQ"}:
            raise SelectionInputError("only SPY and QQQ are eligible")
        evidence = _validate_evidence(observation, context, symbol)
        now = _datetime(context.get("now"), "context now")
        observation_received = _datetime(observation.get("observed_at"), "observation observed_at")
        if observation_received > now:
            raise SelectionInputError("observation receipt is in the future")
        option_received = _datetime(context.get("option_received_at"), "option receipt")
        if option_received > now:
            raise SelectionInputError("option receipt is in the future")
        prior_sessions, today = _validate_sessions(context, now)
        difference = _validate_bars(observation, prior_sessions, today, now, observation_received)
        fee = _decimal(fee_per_leg, "fee_per_leg", nonnegative=True)
        slippage = _decimal(slippage_per_leg, "slippage_per_leg", nonnegative=True)
        entry_cap = MAX_ENTRY_COST if max_entry_cost is None else _decimal(max_entry_cost, "max_entry_cost", positive=True)
        if difference == 0:
            return _no_trade("trend_equal", "Prior close equals the 200-session mean", evidence=evidence)
        direction = "up" if difference > 0 else "down"
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)) or not candidates or len(candidates) > 100:
            raise SelectionInputError("normalized option candidates must contain 1–100 rows")
        expected_rows_hash = _hash(context.get("candidate_rows_sha256"), "candidate_rows_sha256")
        if payload_sha256(candidates) != expected_rows_hash:
            raise SelectionInputError("candidate_rows_sha256 does not match normalized candidates")
        right = "C" if direction == "up" else "P"
        catalog_expiries = _catalog_expiries(context, symbol, right)
        expiries = sorted(item for item in catalog_expiries if MIN_DTE <= (item - today).days <= MAX_DTE)
        if not expiries:
            return _no_trade("no_expiry_in_window", "No directional expiry is between 30 and 60 calendar days", evidence=evidence)
        expiry = min(expiries, key=lambda item: (abs((item - today).days - TARGET_DTE), item))
        universe = context["candidate_universe"]
        try:
            declared_expiry = _date(universe.get("expiry"), "candidate universe expiry")
        except SelectionInputError:
            raise SelectionInputError("candidate universe right/expiry declaration is missing") from None
        if universe.get("right") != right or declared_expiry != expiry:
            return _no_trade("missing_chosen_expiry", "Complete candidate universe does not cover the nearest directional expiry", evidence=evidence)
        rows = [_validate_candidate(row, symbol, now, option_received, index) for index, row in enumerate(candidates)]
        identifiers = [row["instrument_id"] for row in rows]
        if len(identifiers) != len(set(identifiers)):
            raise SelectionInputError("candidate instrument identifiers are duplicated")
        symbols = [row["symbol"] for row in rows]
        if len(symbols) != len(set(symbols)):
            raise SelectionInputError("candidate OCC symbols are duplicated")
        if any(row["right"] != right or row["expiry"] != expiry for row in rows):
            raise SelectionInputError("candidate rows do not match the declared directional expiry")
        pairs = _ranked_pairs(rows, right=right, expiry=expiry, direction=direction)
        if not pairs:
            return _no_trade("no_directional_pair", "No debit-oriented pair reaches the registered delta targets", evidence=evidence)
        # The legacy selector preserves its exact delta-first behaviour.  An
        # explicitly configured full-port paper profile instead ranks only
        # registered pairs that independently fit its bounded all-in budget.
        if max_entry_cost is None:
            long, short = pairs[0]
            debit_per_share = long["ask"] - short["bid"]
            if debit_per_share <= 0:
                return _no_trade("non_positive_debit", "Conservative ask-minus-bid debit is not positive", evidence=evidence)
            width = abs(long["strike"] - short["strike"])
            if debit_per_share >= width:
                return _no_trade("debit_exceeds_width", "Conservative debit is not below spread width", evidence=evidence)
        else:
            chosen = None
            for long, short in pairs:
                debit_per_share = long["ask"] - short["bid"]
                width = abs(long["strike"] - short["strike"])
                total = debit_per_share * STANDARD_MULTIPLIER + (fee + slippage) * 2
                if debit_per_share > 0 and debit_per_share < width and total <= entry_cap:
                    chosen = (long, short, debit_per_share)
                    break
            if chosen is None:
                return _no_trade("entry_cost_cap", f"No registered directional debit vertical fits the {entry_cap} dollar all-in cap", evidence=evidence)
            long, short, debit_per_share = chosen
        total_cost = debit_per_share * STANDARD_MULTIPLIER + (fee + slippage) * 2
        if total_cost > entry_cap:
            return _no_trade("entry_cost_cap", f"One whole spread costs more than {entry_cap} dollars including costs", evidence=evidence)
        result = {
            "schema_version": 1,
            "strategy_version": STRATEGY_VERSION,
            "status": "candidate",
            "reason_code": "eligible",
            "reason": "OPT-001 directional debit vertical passed deterministic evidence and cost checks",
            "symbol": symbol,
            "direction": direction,
            "dte": (expiry - today).days,
            "expiry": expiry.isoformat(),
            "long": _public_row(long),
            "short": _public_row(short),
            "selection": {"long_abs_delta_target": "0.50", "short_abs_delta_target": "0.25", "max_abs_delta_error": str(MAX_DELTA_ERROR), "expiry_target_dte": TARGET_DTE},
            "cost_model": {
                "long_ask": str(long["ask"]),
                "short_bid": str(short["bid"]),
                "debit_per_share": str(debit_per_share),
                "gross_spread_cost": str(debit_per_share * STANDARD_MULTIPLIER),
                "fee_per_leg": str(fee),
                "slippage_per_leg": str(slippage),
                "fees_and_slippage": str((fee + slippage) * 2),
                "total_entry_cost": str(total_cost),
                "max_entry_cost": str(entry_cap),
                "quantity": 1,
            },
            "evidence": evidence,
            "evidence_ids": [item["evidence_id"] for item in evidence],
            "fill_status": "not_a_fill",
            "profit_approved": False,
            "approved_profit": False,
        }
        result["candidate"] = {
            "symbol": symbol,
            "direction": direction,
            "expiry": expiry.isoformat(),
            "dte": (expiry - today).days,
            "long": result["long"],
            "short": result["short"],
            "cost_model": result["cost_model"],
        }
        return result
    except SelectionInputError as exc:
        return _no_trade("invalid_evidence", str(exc), evidence=evidence)


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "instrument_id": row["instrument_id"],
        "symbol": row["symbol"],
        "underlying": row["underlying"],
        "expiry": row["expiry"].isoformat(),
        "right": row["right"],
        "strike": str(row["strike"]),
        "delta": str(row["delta"]),
        "bid": str(row["bid"]),
        "ask": str(row["ask"]),
        "bid_size": row["bid_size"],
        "ask_size": row["ask_size"],
        "timestamp": row["timestamp"].isoformat(),
    }


select_candidate = select


__all__ = ["STRATEGY_VERSION", "SelectionInputError", "select", "select_candidate"]
