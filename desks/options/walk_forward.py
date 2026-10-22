"""Deterministic walk-forward replay for the registered OPT-001 strategy.

The replay consumes a retained JSON dataset.  It never fetches quotes, reads a
broker account, or creates orders.  A valid spread uses the long ask and short
bid at entry, then the long bid and short ask at exit.  Each leg's displayed
capacity is checked independently; an incomplete pair is reported as a
partial/no-fill outcome and is never turned into a completed spread trade.

The input format is intentionally small and explicit.  A dataset is an object
with ``data_admission`` and ``rows``.  Each row contains a completed
``session_date``/``close``, optional ``entry_quotes`` (``long`` and
``short``), optional ``exit_quotes`` of the same shape, and optional ``shares``
bid/ask evidence for the exposure-matched benchmark.  Every quote must carry
the complete contract identity and a provider timestamp.  See
``WALK_FORWARD.md`` and the retained fixture for an example.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Sequence


UTC = timezone.utc
POLICY_ID = "OPT-001"
POLICY_VERSION = "OPT-001-v1"
REPLAY_VERSION = "options-walk-forward-v1"
STANDARD_MULTIPLIER = 100
LOOKBACK_SESSIONS = 200
TARGET_DTE = 45
MIN_DTE = 30
MAX_DTE = 60
MAX_ENTRY_COST = Decimal("15")
MAX_DELTA_ERROR = Decimal("0.10")
DEFAULT_TRAIN = 200
DEFAULT_VALIDATION = 30
DEFAULT_HOLDOUT = 30
DEFAULT_STEP = 30
_OCC_RE = re.compile(r"^(?P<root>[A-Z]{1,6})(?P<date>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")


class ReplayError(ValueError):
    """The supplied dataset or replay configuration is not admissible."""


class DataAdmissionError(ReplayError):
    """Historical executable quote evidence is unavailable or unsuitable."""


def _decimal(value: Any, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ReplayError(f"{field} must be a finite number")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ReplayError(f"{field} must be a finite number") from exc
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise ReplayError(f"{field} is outside its allowed range")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ReplayError(f"{field} must be ISO-8601") from exc
    else:
        raise ReplayError(f"{field} must be ISO-8601")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


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
    raise ReplayError(f"{field} must be an ISO date")


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReplayError("value is not canonical JSON") from exc


def sha256_json(value: Any) -> str:
    """Return the canonical SHA-256 used for manifests and artifacts."""
    return hashlib.sha256(_canonical(value)).hexdigest()


def _occ_identity(symbol: Any, field: str) -> tuple[str, date, str, Decimal]:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ReplayError(f"{field} must be a non-empty OCC symbol")
    compact = symbol.replace(" ", "").upper()
    match = _OCC_RE.fullmatch(compact)
    if match is None:
        raise ReplayError(f"{field} is not a canonical OCC symbol")
    try:
        expiry = date(2000 + int(match.group("date")[:2]), int(match.group("date")[2:4]), int(match.group("date")[4:6]))
    except ValueError as exc:
        raise ReplayError(f"{field} has an invalid expiration") from exc
    return match.group("root"), expiry, match.group("right"), Decimal(match.group("strike")) / Decimal(1000)


def _source_kind(value: Any, *, synthetic: bool) -> str:
    if not isinstance(value, str) or not value:
        raise ReplayError("quote source_kind is required")
    accepted = {"historical_executable"}
    if synthetic:
        accepted.update({"synthetic_executable_fixture", "retained_executable_fixture"})
    if value not in accepted:
        raise DataAdmissionError("indicative or unspecified quotes cannot be used as executable backtest evidence")
    return value


@dataclass(frozen=True)
class CostModel:
    """Per-leg fees and conservative premium slippage."""

    fee_per_contract: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")
    share_fee_per_share: Decimal = Decimal("0")
    benchmark_capital: Decimal = Decimal("300")

    def __post_init__(self) -> None:
        object.__setattr__(self, "fee_per_contract", _decimal(self.fee_per_contract, "fee_per_contract", nonnegative=True))
        object.__setattr__(self, "slippage_bps", _decimal(self.slippage_bps, "slippage_bps", nonnegative=True))
        object.__setattr__(self, "share_fee_per_share", _decimal(self.share_fee_per_share, "share_fee_per_share", nonnegative=True))
        object.__setattr__(self, "benchmark_capital", _decimal(self.benchmark_capital, "benchmark_capital", positive=True))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "CostModel":
        mapping = value or {}
        if not isinstance(mapping, Mapping):
            raise ReplayError("cost_model must be an object")
        return cls(
            fee_per_contract=_decimal(mapping.get("fee_per_contract", 0), "fee_per_contract", nonnegative=True),
            slippage_bps=_decimal(mapping.get("slippage_bps", 0), "slippage_bps", nonnegative=True),
            share_fee_per_share=_decimal(mapping.get("share_fee_per_share", 0), "share_fee_per_share", nonnegative=True),
            benchmark_capital=_decimal(mapping.get("benchmark_capital", 300), "benchmark_capital", positive=True),
        )

    @property
    def slippage_rate(self) -> Decimal:
        return self.slippage_bps / Decimal(10000)

    def as_dict(self) -> dict[str, str]:
        return {
            "fee_per_contract": str(self.fee_per_contract),
            "slippage_bps": str(self.slippage_bps),
            "share_fee_per_share": str(self.share_fee_per_share),
            "benchmark_capital": str(self.benchmark_capital),
        }


@dataclass(frozen=True)
class Quote:
    symbol: str
    underlying: str
    expiry: date
    right: str
    strike: Decimal
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal
    delta: Decimal
    timestamp: datetime
    source_kind: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, leg: str, synthetic: bool) -> "Quote":
        if not isinstance(value, Mapping):
            raise ReplayError(f"{leg} quote must be an object")
        root, parsed_expiry, parsed_right, parsed_strike = _occ_identity(value.get("symbol"), f"{leg}.symbol")
        underlying = str(value.get("underlying", "")).strip().upper()
        expiry = _date(value.get("expiry"), f"{leg}.expiry")
        right = str(value.get("right", "")).strip().upper()
        strike = _decimal(value.get("strike"), f"{leg}.strike", positive=True)
        if (root, parsed_expiry, parsed_right, parsed_strike) != (underlying, expiry, right, strike):
            raise ReplayError(f"{leg} quote contract identity does not match its OCC symbol")
        if right not in {"C", "P"}:
            raise ReplayError(f"{leg}.right must be C or P")
        bid = _decimal(value.get("bid"), f"{leg}.bid", nonnegative=True)
        ask = _decimal(value.get("ask"), f"{leg}.ask", positive=True)
        if bid > ask:
            raise ReplayError(f"{leg} quote is crossed")
        bid_size = _decimal(value.get("bid_size"), f"{leg}.bid_size", positive=True)
        ask_size = _decimal(value.get("ask_size"), f"{leg}.ask_size", positive=True)
        delta = _decimal(value.get("delta"), f"{leg}.delta")
        if delta < -1 or delta > 1:
            raise ReplayError(f"{leg}.delta is outside [-1, 1]")
        timestamp = _timestamp(value.get("timestamp"), f"{leg}.timestamp")
        source_kind = _source_kind(value.get("source_kind"), synthetic=synthetic)
        if value.get("executable") is not True:
            raise DataAdmissionError(f"{leg} quote is not explicitly marked executable")
        return cls(
            symbol=str(value["symbol"]), underlying=underlying, expiry=expiry, right=right,
            strike=strike, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size,
            delta=delta, timestamp=timestamp, source_kind=source_kind,
        )

    def identity(self) -> tuple[str, str, date, str, Decimal]:
        # Keep the canonical OCC symbol in the identity.  The metadata checks
        # above prevent a padded/compact spelling from hiding a leg mismatch.
        return self.symbol.replace(" ", "").upper(), self.underlying, self.expiry, self.right, self.strike


@dataclass(frozen=True)
class Position:
    direction: str
    underlying: str
    expiry: date
    long: Quote
    short: Quote
    entry_session: date
    entry_timestamp: datetime
    entry_spot: Decimal
    quantity: Decimal
    entry_debit_per_share: Decimal
    entry_total_cost: Decimal
    entry_fee: Decimal
    width: Decimal


def _validate_admission(data: Mapping[str, Any]) -> dict[str, Any]:
    admission = data.get("data_admission")
    blockers: list[str] = []
    if not isinstance(admission, Mapping):
        blockers.append("historical executable bid/ask entitlement is not declared")
        admission = {}
    if admission.get("source_kind") != "historical_executable":
        blockers.append("retained options data is not declared as historical executable bid/ask evidence")
    if admission.get("entitlement_confirmed") is not True:
        blockers.append("historical options bid/ask entitlement is unconfirmed")
    if admission.get("quotes_complete") is not True:
        blockers.append("historical option quote coverage is not complete for the candidate universe")
    if admission.get("provider_timestamps") is not True:
        blockers.append("provider event timestamps are not verified")
    return {
        "status": "admitted" if not blockers else "blocked",
        "admissible": not blockers,
        "source_kind": admission.get("source_kind"),
        "provider": admission.get("provider"),
        "blockers": blockers,
    }


def _normalise_rows(data: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    rows = data.get("rows")
    if not isinstance(rows, list) or not rows:
        raise ReplayError("dataset.rows must be a non-empty list")
    result: list[dict[str, Any]] = []
    seen: set[date] = set()
    previous: date | None = None
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise ReplayError(f"rows[{index}] must be an object")
        session = _date(raw.get("session_date"), f"rows[{index}].session_date")
        if session in seen or (previous is not None and session <= previous):
            raise ReplayError("rows must be strictly chronological with unique session dates")
        timestamp = _timestamp(raw.get("signal_timestamp", f"{session.isoformat()}T20:00:00Z"), f"rows[{index}].signal_timestamp")
        close = _decimal(raw.get("close"), f"rows[{index}].close", positive=True)
        direction = raw.get("direction")
        if direction is not None and direction not in {"up", "down", "flat"}:
            raise ReplayError(f"rows[{index}].direction must be up, down or flat")
        result.append({**raw, "session_date": session, "signal_timestamp": timestamp, "close": close})
        seen.add(session)
        previous = session
    if len(result) < LOOKBACK_SESSIONS + 1:
        raise ReplayError(f"dataset needs at least {LOOKBACK_SESSIONS + 1} completed sessions")
    return tuple(result)


def _quote_provenance_blockers(rows: Sequence[Mapping[str, Any]], *, synthetic: bool) -> list[str]:
    """Reject indicative/unlabelled nested quotes before reporting admission."""
    blockers: list[str] = []
    allowed = {"synthetic_executable_fixture", "retained_executable_fixture"} if synthetic else {"historical_executable"}
    for index, row in enumerate(rows):
        for phase in ("entry_quotes", "exit_quotes"):
            pair = row.get(phase)
            if not isinstance(pair, Mapping):
                continue
            for leg in ("long", "short"):
                quote = pair.get(leg)
                if not isinstance(quote, Mapping):
                    continue
                if quote.get("source_kind") not in allowed:
                    blockers.append(f"rows[{index}].{phase}.{leg} is not historical executable quote evidence")
                if quote.get("executable") is not True:
                    blockers.append(f"rows[{index}].{phase}.{leg} is not explicitly executable")
    return blockers


def _signal(rows: Sequence[Mapping[str, Any]], index: int) -> tuple[str, Decimal]:
    mean = sum((row["close"] for row in rows[index - LOOKBACK_SESSIONS:index]), Decimal(0)) / Decimal(LOOKBACK_SESSIONS)
    close = rows[index]["close"]
    direction = "up" if close > mean else "down" if close < mean else "flat"
    return direction, mean


def _chain_expiry(row: Mapping[str, Any], *, session: date, long: Quote) -> None:
    chain = row.get("chain_metadata")
    if not isinstance(chain, Mapping) or chain.get("complete") is not True:
        raise ReplayError("complete option chain metadata is required")
    expirations = chain.get("available_expirations")
    if not isinstance(expirations, list) or not expirations:
        raise ReplayError("complete advertised expiration catalog is required")
    parsed = sorted({_date(item, "chain expiration") for item in expirations})
    if len(parsed) != len(expirations):
        raise ReplayError("chain expiration catalog contains duplicates")
    dte = (long.expiry - session).days
    expected = min(parsed, key=lambda item: (abs((item - session).days - TARGET_DTE), item))
    if long.expiry != expected or not MIN_DTE <= dte <= MAX_DTE:
        raise ReplayError("candidate expiry is not the nearest registered 45-day expiry")


def _candidate(row: Mapping[str, Any], *, direction: str, synthetic: bool) -> tuple[Quote, Quote] | str | None:
    raw = row.get("entry_quotes")
    if raw is None:
        return "missing_entry_quotes"
    if not isinstance(raw, Mapping):
        return "malformed_entry_quotes"
    try:
        long = Quote.from_mapping(raw.get("long"), leg="entry.long", synthetic=synthetic)
        short = Quote.from_mapping(raw.get("short"), leg="entry.short", synthetic=synthetic)
        if long.timestamp < row["signal_timestamp"] or short.timestamp < row["signal_timestamp"]:
            raise ReplayError("entry quote precedes the signal timestamp")
        if long.underlying != short.underlying or long.expiry != short.expiry or long.right != short.right:
            raise ReplayError("vertical legs do not share exact underlying, expiry and right")
        if direction == "up" and (long.right != "C" or not long.strike < short.strike):
            raise ReplayError("up signal requires a lower-strike call debit vertical")
        if direction == "down" and (long.right != "P" or not long.strike > short.strike):
            raise ReplayError("down signal requires a higher-strike put debit vertical")
        if abs(abs(long.delta) - Decimal("0.50")) > MAX_DELTA_ERROR or abs(abs(short.delta) - Decimal("0.25")) > MAX_DELTA_ERROR:
            raise ReplayError("leg deltas do not meet the registered targets")
        _chain_expiry(row, session=row["session_date"], long=long)
        width = abs(long.strike - short.strike)
        if long.ask <= short.bid or long.ask - short.bid >= width:
            raise ReplayError("conservative debit is not positive and below spread width")
        return long, short
    except (ReplayError, DataAdmissionError) as exc:
        return str(exc)


def _entry_fill(long: Quote, short: Quote, *, quantity: Decimal, costs: CostModel) -> tuple[str, Decimal, Decimal, dict[str, Any]]:
    capacity = min(long.ask_size, short.bid_size)
    if capacity <= 0:
        return "no_fill", Decimal(0), Decimal(0), {"long_ask_size": str(long.ask_size), "short_bid_size": str(short.bid_size)}
    if capacity < quantity:
        return "partial_fill", capacity, Decimal(0), {
            "long_ask_size": str(long.ask_size), "short_bid_size": str(short.bid_size), "matched_capacity": str(capacity),
            "unmatched_long_quantity": str(max(quantity - long.ask_size, Decimal(0))),
            "unmatched_short_quantity": str(max(quantity - short.bid_size, Decimal(0))),
        }
    rate = costs.slippage_rate
    long_price = long.ask * (Decimal(1) + rate)
    short_price = short.bid * (Decimal(1) - rate)
    debit = long_price - short_price
    fee = costs.fee_per_contract * Decimal(2) * quantity
    total = debit * Decimal(STANDARD_MULTIPLIER) * quantity + fee
    return "filled", quantity, total, {"long_entry_price": str(long_price), "short_entry_price": str(short_price), "debit_per_share": str(debit), "fee": str(fee)}


def _exit_fill(long: Quote, short: Quote, *, quantity: Decimal, costs: CostModel) -> tuple[str, Decimal, Decimal, dict[str, Any]]:
    capacity = min(long.bid_size, short.ask_size)
    if capacity <= 0:
        return "no_fill", Decimal(0), Decimal(0), {"long_bid_size": str(long.bid_size), "short_ask_size": str(short.ask_size)}
    if capacity < quantity:
        return "partial_fill", capacity, Decimal(0), {
            "long_bid_size": str(long.bid_size), "short_ask_size": str(short.ask_size), "matched_capacity": str(capacity),
            "unmatched_long_quantity": str(max(quantity - long.bid_size, Decimal(0))),
            "unmatched_short_quantity": str(max(quantity - short.ask_size, Decimal(0))),
        }
    rate = costs.slippage_rate
    long_price = long.bid * (Decimal(1) - rate)
    short_price = short.ask * (Decimal(1) + rate)
    credit = long_price - short_price
    fee = costs.fee_per_contract * Decimal(2) * quantity
    return "filled", quantity, credit * Decimal(STANDARD_MULTIPLIER) * quantity - fee, {
        "long_exit_price": str(long_price), "short_exit_price": str(short_price), "credit_per_share": str(credit), "fee": str(fee),
    }


def _quote_pair(value: Any, *, phase: str, synthetic: bool) -> tuple[Quote, Quote] | None:
    if not isinstance(value, Mapping):
        return None
    try:
        return (
            Quote.from_mapping(value.get("long"), leg=f"{phase}.long", synthetic=synthetic),
            Quote.from_mapping(value.get("short"), leg=f"{phase}.short", synthetic=synthetic),
        )
    except (ReplayError, DataAdmissionError):
        return None


def _share_entry(row: Mapping[str, Any], *, costs: CostModel) -> tuple[Decimal, Decimal] | None:
    shares = row.get("shares")
    if not isinstance(shares, Mapping):
        return None
    ask = _decimal(shares.get("ask"), "shares.ask", positive=True)
    ask_size = _decimal(shares.get("ask_size"), "shares.ask_size", positive=True)
    units = (costs.benchmark_capital / (ask * (Decimal(1) + costs.slippage_rate))).quantize(Decimal("0.000001"), rounding=ROUND_FLOOR)
    if units <= 0 or ask_size < units:
        return None
    return units, ask * (Decimal(1) + costs.slippage_rate)


def _share_exit(row: Mapping[str, Any], *, units: Decimal, costs: CostModel, entry_price: Decimal) -> Decimal | None:
    shares = row.get("shares")
    if not isinstance(shares, Mapping):
        return None
    bid = _decimal(shares.get("bid"), "shares.bid", positive=True)
    bid_size = _decimal(shares.get("bid_size"), "shares.bid_size", positive=True)
    if bid_size < units:
        return None
    exit_price = bid * (Decimal(1) - costs.slippage_rate)
    return (exit_price - entry_price) * units - costs.share_fee_per_share * units * Decimal(2)


def _metrics(*, opportunities: int, statuses: Mapping[str, int], trades: Sequence[Mapping[str, Any]], terminal_pnl: Decimal, capital: Decimal, benchmark_pnl: Decimal, costs: Decimal, unresolved: int) -> dict[str, Any]:
    net = sum((Decimal(item["net_pnl"]) for item in trades), Decimal(0)) + terminal_pnl
    equity = capital
    peak = capital
    max_dd = Decimal(0)
    for trade in trades:
        equity += Decimal(trade["net_pnl"])
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - Decimal(1))
    if terminal_pnl:
        equity += terminal_pnl
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - Decimal(1))
    fills = statuses.get("filled", 0)
    no_fills = statuses.get("no_fill", 0)
    partial = statuses.get("partial_fill", 0)
    return {
        "opportunities": opportunities,
        "filled_trades": fills,
        "no_fill_count": no_fills,
        "partial_fill_count": partial,
        "fill_rate": str(Decimal(fills) / Decimal(opportunities) if opportunities else Decimal(0)),
        "no_fill_rate": str(Decimal(no_fills) / Decimal(opportunities) if opportunities else Decimal(0)),
        "partial_fill_rate": str(Decimal(partial) / Decimal(opportunities) if opportunities else Decimal(0)),
        "net_pnl": str(net),
        "benchmark_pnl": str(benchmark_pnl),
        "max_drawdown": str(max_dd),
        "total_costs": str(costs),
        "unresolved_positions": unresolved,
        "trades": list(trades),
    }


def replay_segment(rows: Sequence[Mapping[str, Any]], *, start: int, end: int, costs: CostModel, max_entry_cost: Decimal = MAX_ENTRY_COST, synthetic: bool = False) -> dict[str, Any]:
    """Replay one chronological slice without carrying positions across its boundary."""
    if not 0 <= start < end <= len(rows):
        raise ReplayError("segment bounds are invalid")
    position: Position | None = None
    statuses = {"filled": 0, "no_fill": 0, "partial_fill": 0}
    opportunities = 0
    trades: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    benchmark_pnl = Decimal(0)
    benchmark_open: tuple[Decimal, Decimal] | None = None
    total_costs = Decimal(0)
    terminal_pnl = Decimal(0)
    unresolved = 0

    for index in range(start, end):
        row = rows[index]
        closed_this_row = False
        if index < LOOKBACK_SESSIONS:
            direction, mean = "flat", Decimal(0)
        else:
            direction, mean = _signal(rows, index)
        if row.get("direction") is not None and row["direction"] != direction:
            raise ReplayError(f"row {index} supplied direction does not match the 200-session trend signal")
        row_events: list[str] = []
        if position is not None:
            mark_pair = _quote_pair(row.get("exit_quotes"), phase="exit", synthetic=synthetic)
            if mark_pair is not None and any(quote.timestamp < row["signal_timestamp"] or quote.timestamp <= position.entry_timestamp for quote in mark_pair):
                mark_pair = None
            mark_pnl: Decimal | None = None
            if mark_pair is not None and mark_pair[0].identity() == position.long.identity() and mark_pair[1].identity() == position.short.identity():
                mark_status, _, mark_pnl, _ = _exit_fill(mark_pair[0], mark_pair[1], quantity=position.quantity, costs=costs)
                if mark_status != "filled":
                    mark_pnl = None
            dte = (position.expiry - row["session_date"]).days
            trigger: list[str] = []
            if direction != "flat" and direction != position.direction:
                trigger.append("signal_reversal")
            if dte <= 21:
                trigger.append("21_dte")
            if mark_pnl is not None:
                target = (position.width - position.entry_debit_per_share) * Decimal(STANDARD_MULTIPLIER) * position.quantity * Decimal("0.50")
                stop = position.entry_debit_per_share * Decimal(STANDARD_MULTIPLIER) * position.quantity * Decimal("0.50")
                if mark_pnl >= target:
                    trigger.append("50pct_max_profit")
                if mark_pnl <= -stop:
                    trigger.append("50pct_debit_loss")
            if trigger:
                if mark_pair is None or mark_pair[0].identity() != position.long.identity() or mark_pair[1].identity() != position.short.identity():
                    events.append({"session_date": row["session_date"].isoformat(), "status": "no_fill", "phase": "exit", "reasons": ["missing_or_mismatched_exit_quotes", *trigger]})
                    row_events.append("exit_no_fill")
                else:
                    exit_status, _, exit_pnl, evidence = _exit_fill(mark_pair[0], mark_pair[1], quantity=position.quantity, costs=costs)
                    if exit_status != "filled":
                        events.append({"session_date": row["session_date"].isoformat(), "status": exit_status, "phase": "exit", "reasons": trigger, "evidence": evidence})
                        row_events.append(f"exit_{exit_status}")
                    else:
                        trades.append({
                            "entry_session": position.entry_session.isoformat(), "exit_session": row["session_date"].isoformat(),
                            "direction": position.direction, "underlying": position.underlying,
                            "long_symbol": position.long.symbol, "short_symbol": position.short.symbol,
                            "entry_total_cost": str(position.entry_total_cost), "exit_pnl_after_exit_fee": str(exit_pnl),
                            "net_pnl": str(exit_pnl - position.entry_total_cost), "exit_trigger": trigger,
                            "exit_evidence": evidence,
                        })
                        total_costs += position.entry_fee + costs.fee_per_contract * Decimal(2) * position.quantity
                        benchmark_exit = _share_exit(row, units=benchmark_open[0], costs=costs, entry_price=benchmark_open[1]) if benchmark_open else None
                        if benchmark_exit is not None:
                            benchmark_pnl += benchmark_exit
                        benchmark_open = None
                        position = None
                        closed_this_row = True
                        row_events.append("exit_filled")

        # No signal exists until the full prior-session lookback is present.
        # Training rows before that point remain context only.
        if position is None and not closed_this_row and index >= max(start, LOOKBACK_SESSIONS):
            direction, _ = _signal(rows, index)
            if direction != "flat":
                opportunities += 1
                candidate = _candidate(row, direction=direction, synthetic=synthetic)
                if not isinstance(candidate, tuple):
                    statuses["no_fill"] += 1
                    events.append({"session_date": row["session_date"].isoformat(), "status": "no_fill", "phase": "entry", "reason": candidate})
                    row_events.append("entry_no_fill")
                else:
                    long, short = candidate
                    status, quantity, total, evidence = _entry_fill(long, short, quantity=Decimal(1), costs=costs)
                    if status != "filled":
                        statuses[status] += 1
                        events.append({"session_date": row["session_date"].isoformat(), "status": status, "phase": "entry", "evidence": evidence})
                        row_events.append(f"entry_{status}")
                    elif total > max_entry_cost:
                        statuses["no_fill"] += 1
                        events.append({"session_date": row["session_date"].isoformat(), "status": "no_fill", "phase": "entry", "reason": "entry_cost_cap", "total_entry_cost": str(total)})
                        row_events.append("entry_no_fill")
                    else:
                        spot = _decimal(row.get("spot", row["close"]), f"rows[{index}].spot", positive=True)
                        debit = (long.ask * (Decimal(1) + costs.slippage_rate)) - (short.bid * (Decimal(1) - costs.slippage_rate))
                        entry_fee = costs.fee_per_contract * Decimal(2)
                        position = Position(direction=direction, underlying=long.underlying, expiry=long.expiry, long=long, short=short,
                                            entry_session=row["session_date"], entry_timestamp=max(long.timestamp, short.timestamp), entry_spot=spot,
                                            quantity=quantity, entry_debit_per_share=debit, entry_total_cost=total, entry_fee=entry_fee,
                                            width=abs(long.strike - short.strike))
                        statuses["filled"] += 1
                        total_costs += entry_fee
                        benchmark_open = _share_entry(row, costs=costs)
                        events.append({"session_date": row["session_date"].isoformat(), "status": "filled", "phase": "entry", "entry_total_cost": str(total), "evidence": evidence})
                        row_events.append("entry_filled")
        if row_events:
            events[-1]["signal_direction"] = direction
            events[-1]["prior_200_mean"] = str(mean)

    if position is not None:
        unresolved += 1
        final_pair = _quote_pair(rows[end - 1].get("exit_quotes"), phase="terminal", synthetic=synthetic)
        if final_pair is not None and any(quote.timestamp < rows[end - 1]["signal_timestamp"] or quote.timestamp <= position.entry_timestamp for quote in final_pair):
            final_pair = None
        if final_pair is not None and final_pair[0].identity() == position.long.identity() and final_pair[1].identity() == position.short.identity():
            status, _, mark, _ = _exit_fill(final_pair[0], final_pair[1], quantity=position.quantity, costs=costs)
            if status == "filled":
                terminal_pnl = mark - position.entry_total_cost
    return {
        "start_index": start, "end_index": end, "start_session": rows[start]["session_date"].isoformat(), "end_session": rows[end - 1]["session_date"].isoformat(),
        "metrics": _metrics(opportunities=opportunities, statuses=statuses, trades=trades, terminal_pnl=terminal_pnl,
                             capital=costs.benchmark_capital, benchmark_pnl=benchmark_pnl, costs=total_costs, unresolved=unresolved),
        "events": events, "terminal_position": position is not None,
    }


def _split_windows(count: int, *, train: int, validation: int, holdout: int, step: int) -> list[dict[str, tuple[int, int]]]:
    if min(train, validation, holdout, step) <= 0:
        raise ReplayError("walk-forward split sizes must be positive")
    windows: list[dict[str, tuple[int, int]]] = []
    start = 0
    while start + train + validation + holdout <= count:
        windows.append({"train": (start, start + train), "validation": (start + train, start + train + validation), "holdout": (start + train + validation, start + train + validation + holdout)})
        start += step
    if not windows:
        raise ReplayError("dataset is shorter than train + validation + holdout")
    return windows


def recommend_revalidation_cadence() -> dict[str, Any]:
    return {
        "quote_feed": "each market session before any replay or paper admission",
        "strategy_review": "every 20 completed market sessions or 10 completed modeled trades",
        "full_walk_forward": "every 60 completed market sessions and whenever strategy, fee, provider, or contract rules change",
        "holdout_policy": "keep the final holdout untouched until an explicit acceptance review; inspecting it starts a new trial",
    }


def run_walk_forward(
    data: Mapping[str, Any],
    *,
    train_sessions: int = DEFAULT_TRAIN,
    validation_sessions: int = DEFAULT_VALIDATION,
    holdout_sessions: int = DEFAULT_HOLDOUT,
    step_sessions: int = DEFAULT_STEP,
    cost_model: CostModel | None = None,
    cost_stress: Mapping[str, CostModel] | None = None,
    allow_synthetic: bool = False,
) -> dict[str, Any]:
    """Replay registered OPT-001 chronologically and return an artifact-ready report."""
    if not isinstance(data, Mapping):
        raise ReplayError("dataset must be an object")
    admission = _validate_admission(data)
    rows = _normalise_rows(data)
    source = data.get("data_admission", {})
    synthetic = isinstance(source, Mapping) and source.get("source_kind") in {"synthetic_executable_fixture", "retained_executable_fixture"}
    admission["blockers"].extend(_quote_provenance_blockers(rows, synthetic=synthetic))
    if admission["blockers"]:
        admission["status"] = "blocked"
        admission["admissible"] = False
    if synthetic and not allow_synthetic:
        admission["status"] = "blocked"
        admission["admissible"] = False
        admission["blockers"].append("synthetic fixture replay requires explicit allow_synthetic and is never paper-admissible")
    if (synthetic and not allow_synthetic) or (not synthetic and not admission["admissible"]):
        return {
            "schema_version": 1, "artifact_kind": "options-walk-forward-acceptance-v1", "status": "data_blocked",
            "research_only": True, "execution_enabled": False, "admissible_for_paper": False,
            "replay_version": REPLAY_VERSION, "policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
            "data_admission": admission, "data_manifest_sha256": sha256_json(data.get("rows")), "windows": [],
            "revalidation": recommend_revalidation_cadence(),
        }
    costs = cost_model or CostModel.from_mapping(data.get("cost_model"))
    windows = _split_windows(len(rows), train=train_sessions, validation=validation_sessions, holdout=holdout_sessions, step=step_sessions)
    reports: list[dict[str, Any]] = []
    for window_index, bounds in enumerate(windows):
        reports.append({"window": window_index, "splits": {name: replay_segment(rows, start=start, end=end, costs=costs, synthetic=synthetic) for name, (start, end) in bounds.items()}})
    stresses: dict[str, Any] = {}
    scenarios = cost_stress or {"base": costs, "stress_25bps": CostModel(fee_per_contract=costs.fee_per_contract, slippage_bps=costs.slippage_bps + Decimal("25"), share_fee_per_share=costs.share_fee_per_share, benchmark_capital=costs.benchmark_capital), "stress_50bps": CostModel(fee_per_contract=costs.fee_per_contract, slippage_bps=costs.slippage_bps + Decimal("50"), share_fee_per_share=costs.share_fee_per_share, benchmark_capital=costs.benchmark_capital)}
    final = windows[-1]["holdout"]
    for name, scenario in scenarios.items():
        if not isinstance(name, str) or not isinstance(scenario, CostModel):
            raise ReplayError("cost_stress must map names to CostModel instances")
        stresses[name] = replay_segment(rows, start=final[0], end=final[1], costs=scenario, synthetic=synthetic)["metrics"]
    return {
        "schema_version": 1, "artifact_kind": "options-walk-forward-acceptance-v1", "status": "research_only",
        "research_only": True, "execution_enabled": False, "admissible_for_paper": bool(admission["admissible"] and not synthetic),
        "replay_version": REPLAY_VERSION, "policy_id": POLICY_ID, "policy_version": POLICY_VERSION,
        "data_admission": admission, "data_manifest_sha256": sha256_json(data.get("rows")), "cost_model": costs.as_dict(),
        "split_config": {"train_sessions": train_sessions, "validation_sessions": validation_sessions, "holdout_sessions": holdout_sessions, "step_sessions": step_sessions},
        "windows": reports, "final_untouched_holdout": {"window": len(windows) - 1, "bounds": {"start": final[0], "end": final[1]}, "locked": True},
        "holdout_cost_stress": stresses, "revalidation": recommend_revalidation_cadence(),
    }


def write_acceptance_artifact(path: str | os.PathLike[str], report: Mapping[str, Any]) -> dict[str, Any]:
    """Write one content-addressed immutable JSON artifact; never overwrite."""
    destination = Path(path)
    if destination.exists() or destination.is_symlink():
        raise ReplayError(f"acceptance artifact already exists: {destination}")
    body = dict(report)
    body.pop("artifact_sha256", None)
    body["artifact_sha256"] = sha256_json(body)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(body, handle, sort_keys=True, separators=(",", ":"), indent=2)
            handle.write("\n")
        os.replace(temporary, destination)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return body


def register_trial(path: str | os.PathLike[str], trial: Mapping[str, Any]) -> dict[str, Any]:
    """Append an immutable trial specification, or replay an exact registration."""
    if not isinstance(trial, Mapping):
        raise ReplayError("trial must be an object")
    trial_id = trial.get("trial_id")
    if not isinstance(trial_id, str) or not trial_id:
        raise ReplayError("trial_id is required")
    for field in ("strategy_version", "data_manifest_sha256", "cost_model", "split_config"):
        if field not in trial:
            raise ReplayError(f"trial.{field} is required")
    destination = Path(path)
    if destination.exists() and destination.is_symlink():
        raise ReplayError("trial registry cannot be a symlink")
    if destination.exists():
        try:
            registry = json.loads(destination.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReplayError("trial registry is unreadable") from exc
    else:
        registry = {"schema_version": 1, "trials": []}
    if not isinstance(registry, Mapping) or registry.get("schema_version") != 1 or not isinstance(registry.get("trials"), list):
        raise ReplayError("trial registry schema is invalid")
    existing = next((item for item in registry["trials"] if isinstance(item, Mapping) and item.get("trial_id") == trial_id), None)
    if existing is not None:
        if sha256_json(existing) != sha256_json(dict(trial)):
            raise ReplayError("trial_id is already registered with different content")
        return dict(registry)
    updated = {"schema_version": 1, "trials": [*registry["trials"], dict(trial)]}
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=str(destination.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(updated, handle, sort_keys=True, separators=(",", ":"), indent=2)
            handle.write("\n")
        os.replace(temporary, destination)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return updated


def load_dataset(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayError("dataset JSON is unreadable") from exc
    if not isinstance(value, Mapping):
        raise ReplayError("dataset JSON must be an object")
    return dict(value)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Replay registered OPT-001 options strategy with historical bid/ask evidence")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay", help="run a walk-forward replay")
    replay.add_argument("dataset")
    replay.add_argument("--output", required=True)
    replay.add_argument("--allow-synthetic", action="store_true", help="run a retained fixture while keeping paper admission blocked")
    replay.add_argument("--registry")
    replay.add_argument("--trial-id")
    register = sub.add_parser("register", help="append a pre-registered trial")
    register.add_argument("--registry", required=True)
    register.add_argument("--trial-id", required=True)
    register.add_argument("--strategy-version", default=POLICY_VERSION)
    register.add_argument("--data-manifest-sha256", required=True)
    register.add_argument("--cost-model", required=True, help="canonical JSON object")
    register.add_argument("--split-config", required=True, help="canonical JSON object")
    args = parser.parse_args()
    if args.command == "register":
        register_trial(args.registry, {"trial_id": args.trial_id, "strategy_version": args.strategy_version, "data_manifest_sha256": args.data_manifest_sha256, "cost_model": json.loads(args.cost_model), "split_config": json.loads(args.split_config)})
        return 0
    dataset = load_dataset(args.dataset)
    report = run_walk_forward(dataset, allow_synthetic=args.allow_synthetic)
    if args.registry and args.trial_id:
        register_trial(args.registry, {"trial_id": args.trial_id, "strategy_version": POLICY_VERSION, "data_manifest_sha256": report["data_manifest_sha256"], "cost_model": report.get("cost_model", dataset.get("cost_model", {})), "split_config": report.get("split_config", {})})
    artifact = write_acceptance_artifact(args.output, report)
    print(json.dumps({"status": artifact["status"], "artifact_sha256": artifact["artifact_sha256"], "admissible_for_paper": artifact["admissible_for_paper"]}, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())


__all__ = [
    "CostModel", "DataAdmissionError", "Quote", "ReplayError", "REPLAY_VERSION", "POLICY_VERSION",
    "load_dataset", "recommend_revalidation_cadence", "register_trial", "replay_segment", "run_walk_forward",
    "sha256_json", "write_acceptance_artifact",
]
