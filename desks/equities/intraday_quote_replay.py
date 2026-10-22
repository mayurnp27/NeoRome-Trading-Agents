"""Offline, conservative bid/ask replay for EQ-INTRADAY v1.

This module deliberately has no provider, broker, clock, or persistence
dependencies.  It consumes already materialised quote rows and frozen signal
timestamps.  Entry and exit fills are only returned when a chronologically
first quote satisfies the v1 validity rules:

* a long entry uses the first valid ask after signal time plus latency;
* an exit uses the first valid bid after the exit decision plus latency;
* crossed, non-positive, zero-sized, stale, malformed, or pre-cutoff quotes
  cannot be used as fills.

Quote sizes are carried through as supplied evidence.  They are intentionally
not interpreted as verified capacity or participation limits.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from numbers import Real
from typing import Any, Iterable, Mapping, Sequence


UTC = timezone.utc
DEFAULT_LATENCY = timedelta(milliseconds=250)
DEFAULT_MAX_QUOTE_AGE = timedelta(seconds=1)


class ReplayValidationError(ValueError):
    """Raised when a candidate or replay configuration is not well formed."""


def _timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif hasattr(value, "to_pydatetime"):
        parsed = value.to_pydatetime()
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ReplayValidationError(f"{label} is not an ISO timestamp") from exc
    else:
        raise ReplayValidationError(f"{label} is not a timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReplayValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        try:
            value = float(value)
        except (TypeError, ValueError) as exc:
            raise ReplayValidationError(f"{label} is not numeric") from exc
    result = float(value)
    if not math.isfinite(result):
        raise ReplayValidationError(f"{label} is not finite")
    return result


def _first(row: Mapping[str, Any], names: Sequence[str], *, default: Any = None) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return default


@dataclass(frozen=True, slots=True)
class Quote:
    """A normalized historical quote row.

    ``timestamp`` is the quote event timestamp.  If ``received_at`` is
    supplied, it is used to reject evidence that was already more than the
    permitted age when observed.  ``source_index`` is only a deterministic
    tie-breaker and is never market data.
    """

    timestamp: datetime
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    symbol: str | None = None
    sequence: int | float | str | None = None
    received_at: datetime | None = None
    source_index: int = 0

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, source_index: int = 0) -> "Quote":
        if not isinstance(row, Mapping):
            raise ReplayValidationError("quote row must be a mapping")
        timestamp = _timestamp(
            _first(row, ("timestamp", "quote_timestamp", "t")),
            "quote timestamp",
        )
        received_raw = _first(row, ("received_at", "observed_at", "as_of", "receipt_timestamp"))
        received_at = None if received_raw is None else _timestamp(received_raw, "quote received_at")
        if received_at is not None and received_at < timestamp:
            raise ReplayValidationError("quote received_at precedes quote timestamp")
        symbol_raw = _first(row, ("symbol", "ticker"))
        symbol = None if symbol_raw is None else str(symbol_raw).strip()
        if symbol == "":
            raise ReplayValidationError("quote symbol cannot be empty")
        sequence = _first(row, ("sequence", "sequence_number", "seq"))
        return cls(
            timestamp=timestamp,
            bid=_number(_first(row, ("bid", "bid_price", "bp")), "quote bid"),
            ask=_number(_first(row, ("ask", "ask_price", "ap")), "quote ask"),
            bid_size=_number(_first(row, ("bid_size", "size_bid", "bs")), "quote bid_size"),
            ask_size=_number(_first(row, ("ask_size", "size_ask", "as")), "quote ask_size"),
            symbol=symbol,
            sequence=sequence,
            received_at=received_at,
            source_index=source_index,
        )


@dataclass(frozen=True, slots=True)
class ReplayFill:
    """A conservative fill backed by one valid quote row."""

    phase: str
    timestamp: datetime
    price: float
    size: float
    quote_index: int
    quote_timestamp: datetime
    received_at: datetime | None


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Result for one frozen candidate.

    ``complete`` is true only when both entry and exit are evidenced.  An
    absent exit is explicitly unresolved; it is never converted to a mark,
    last price, or successful close.
    """

    symbol: str | None
    signal_timestamp: datetime
    exit_decision_timestamp: datetime
    entry: ReplayFill | None
    exit: ReplayFill | None
    rejection_counts: Mapping[str, Mapping[str, int]]
    reasons: tuple[str, ...]
    admission: bool = False

    @property
    def complete(self) -> bool:
        return self.entry is not None and self.exit is not None

    @property
    def unresolved(self) -> bool:
        return not self.complete


@dataclass(frozen=True, slots=True)
class _PreparedQuote:
    quote: Quote | None
    error: str | None
    source_index: int


def _prepare_quotes(rows: Iterable[Mapping[str, Any] | Quote]) -> list[_PreparedQuote]:
    prepared: list[_PreparedQuote] = []
    for index, row in enumerate(rows):
        if isinstance(row, Quote):
            prepared.append(_PreparedQuote(row, None, index))
            continue
        try:
            prepared.append(_PreparedQuote(Quote.from_mapping(row, source_index=index), None, index))
        except ReplayValidationError as exc:
            text = str(exc).lower()
            if "timestamp" in text:
                reason = "invalid_timestamp"
            elif "received_at" in text or "receipt" in text:
                reason = "invalid_receipt"
            elif "symbol" in text:
                reason = "invalid_symbol"
            else:
                reason = "invalid_quote"
            prepared.append(_PreparedQuote(None, reason, index))
    # Stable chronological ordering is the no-lookahead guarantee.  Explicit
    # sequence values break ties when providers supply them; source index is
    # the deterministic fallback for equal timestamps.
    def sort_key(item: _PreparedQuote) -> tuple[Any, ...]:
        if item.quote is None:
            return (datetime.max.replace(tzinfo=UTC), 2, item.source_index)
        sequence = item.quote.sequence
        if isinstance(sequence, (int, float)) and not isinstance(sequence, bool):
            sequence_key: tuple[int, Any] = (0, sequence)
        elif sequence is None:
            sequence_key = (2, "")
        else:
            sequence_key = (1, str(sequence))
        return (item.quote.timestamp, sequence_key, item.source_index)

    return sorted(prepared, key=sort_key)


def _validate_duration(value: timedelta, label: str, *, allow_zero: bool = False) -> timedelta:
    if not isinstance(value, timedelta):
        raise ReplayValidationError(f"{label} must be a timedelta")
    if value < timedelta(0) or (not allow_zero and value == timedelta(0)):
        raise ReplayValidationError(f"{label} must be positive")
    return value


def _phase_select(
    prepared: Sequence[_PreparedQuote],
    *,
    phase: str,
    cutoff: datetime,
    symbol: str | None,
    max_age: timedelta,
    require_received_at: bool,
    order_lifetime: timedelta | None,
) -> tuple[ReplayFill | None, dict[str, int]]:
    counts: Counter[str] = Counter()
    for item in prepared:
        if item.quote is None:
            counts[item.error or "invalid_quote"] += 1
            continue
        quote = item.quote
        if order_lifetime is not None and quote.timestamp > cutoff + order_lifetime:
            counts["order_lifetime_expired"] += 1
            break
        if symbol is not None and quote.symbol is not None and quote.symbol != symbol:
            counts["symbol_mismatch"] += 1
            continue
        # Structural validity is checked before temporal eligibility so an
        # invalid quote is never made usable merely by moving its timestamp.
        if quote.bid <= 0 or quote.ask <= 0:
            counts["nonpositive_price"] += 1
            continue
        if quote.bid > quote.ask:
            counts["crossed"] += 1
            continue
        if quote.bid_size <= 0 or quote.ask_size <= 0:
            counts["zero_or_negative_size"] += 1
            continue
        if require_received_at and quote.received_at is None:
            counts["missing_received_at"] += 1
            continue
        if quote.received_at is not None and quote.received_at - quote.timestamp > max_age:
            counts["stale"] += 1
            continue
        if quote.timestamp < cutoff:
            if cutoff - quote.timestamp > max_age:
                counts["stale"] += 1
            else:
                counts["before_eligibility"] += 1
            continue
        if phase == "entry":
            return ReplayFill("entry", quote.timestamp, quote.ask, quote.ask_size, item.source_index, quote.timestamp, quote.received_at), dict(counts)
        return ReplayFill("exit", quote.timestamp, quote.bid, quote.bid_size, item.source_index, quote.timestamp, quote.received_at), dict(counts)
    return None, dict(counts)


def replay_trade(
    candidate: Mapping[str, Any],
    quotes: Iterable[Mapping[str, Any] | Quote],
    *,
    latency: timedelta = DEFAULT_LATENCY,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    admission: bool = False,
    order_lifetime: timedelta | None = None,
    size_units_verified: bool = False,
) -> ReplayResult:
    """Replay one frozen candidate against local quote rows.

    Candidate mappings require ``signal_timestamp`` and
    ``exit_decision_timestamp``.  ``symbol`` is optional for single-symbol
    inputs but should be supplied when a quote file contains several symbols.
    Accepted quote aliases include ``timestamp``/``t``, ``bid``/``bp``,
    ``ask``/``ap``, and the corresponding size fields.  The default is an
    engineering replay.  An admission replay requires provider receipt
    timestamps, an explicit bounded order lifetime and verified quote-size
    units; this is necessary evidence for a paper-fill claim, though it still
    does not guarantee a fill.
    """

    if not isinstance(candidate, Mapping):
        raise ReplayValidationError("candidate must be a mapping")
    signal = _timestamp(candidate.get("signal_timestamp", candidate.get("signal_time")), "signal timestamp")
    exit_decision = _timestamp(
        candidate.get("exit_decision_timestamp", candidate.get("exit_timestamp", candidate.get("exit_decision_time"))),
        "exit decision timestamp",
    )
    if exit_decision < signal:
        raise ReplayValidationError("exit decision timestamp cannot precede signal timestamp")
    if not isinstance(latency, timedelta) or latency < timedelta(0):
        raise ReplayValidationError("latency must be a non-negative timedelta")
    if not isinstance(admission, bool):
        raise ReplayValidationError("admission must be boolean")
    if not isinstance(size_units_verified, bool):
        raise ReplayValidationError("size_units_verified must be boolean")
    if admission and order_lifetime is None:
        raise ReplayValidationError("admission replay requires an explicit order_lifetime")
    if admission and not size_units_verified:
        raise ReplayValidationError("admission replay requires verified quote-size units")
    if order_lifetime is not None:
        order_lifetime = _validate_duration(order_lifetime, "order_lifetime")
    max_age = _validate_duration(max_quote_age, "max_quote_age")
    symbol_raw = candidate.get("symbol", candidate.get("ticker"))
    symbol = None if symbol_raw is None else str(symbol_raw).strip()
    if symbol == "":
        raise ReplayValidationError("candidate symbol cannot be empty")

    prepared = _prepare_quotes(quotes)
    entry, entry_counts = _phase_select(
        prepared,
        phase="entry",
        cutoff=signal + latency,
        symbol=symbol,
        max_age=max_age,
        require_received_at=admission,
        order_lifetime=order_lifetime,
    )
    exit_fill, exit_counts = _phase_select(
        prepared,
        phase="exit",
        cutoff=exit_decision + latency,
        symbol=symbol,
        max_age=max_age,
        require_received_at=admission,
        order_lifetime=order_lifetime,
    )
    reasons: list[str] = []
    if entry is None:
        reasons.append("entry_unresolved_no_valid_quote")
    if exit_fill is None:
        reasons.append("exit_unresolved_no_valid_quote")
    return ReplayResult(
        symbol=symbol,
        signal_timestamp=signal,
        exit_decision_timestamp=exit_decision,
        entry=entry,
        exit=exit_fill,
        rejection_counts={"entry": entry_counts, "exit": exit_counts},
        reasons=tuple(reasons),
        admission=admission,
    )


def replay_candidates(
    candidates: Iterable[Mapping[str, Any]],
    quotes: Iterable[Mapping[str, Any] | Quote],
    *,
    latency: timedelta = DEFAULT_LATENCY,
    max_quote_age: timedelta = DEFAULT_MAX_QUOTE_AGE,
    admission: bool = False,
    order_lifetime: timedelta | None = None,
    size_units_verified: bool = False,
) -> list[ReplayResult]:
    """Replay candidates in supplied order using the same immutable quote set."""

    quote_rows = list(quotes)
    return [
        replay_trade(
            candidate,
            quote_rows,
            latency=latency,
            max_quote_age=max_quote_age,
            admission=admission,
            order_lifetime=order_lifetime,
            size_units_verified=size_units_verified,
        )
        for candidate in candidates
    ]


# A descriptive alias for callers that use the frozen specification's noun.
replay_candidate = replay_trade


__all__ = [
    "DEFAULT_LATENCY",
    "DEFAULT_MAX_QUOTE_AGE",
    "Quote",
    "ReplayFill",
    "ReplayResult",
    "ReplayValidationError",
    "replay_candidate",
    "replay_candidates",
    "replay_trade",
]
