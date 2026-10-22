"""Offline, idempotent internal-paper adapter for STOCH-BTC30-CROSS-v1.

This module consumes caller-supplied public evidence. It has no network,
credential, venue-order, or live-trading path. Simulated fills and marks are
recorded in a dedicated prediction-only ledger with $500 initial paper cash
and 20% available-cash sizing, including fees and price buffer.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import tempfile
import time
from typing import Any, Mapping

from .paper_ledger import InvalidState, MarketMark, MarketRiskPolicy, PaperLedger, Proposal, RiskLimits

UTC = timezone.utc
STRATEGY_ID = "STOCH-BTC30-CROSS-v1"
STRATEGY_VERSION = "v1"
PROFILE_VERSION = "btc-cross30-paper-v1"
DEFAULT_STATE_ROOT = Path("/var/lib/neorome-btc-cross30-paper")
PAPER_CASH_CENTS = 50_000
ALLOCATION_BPS = 2_000
ALLOCATION_POLICY_VERSION = "btc-cash20-v1"
# Ledger limits remain finite, while reservations remain bounded by available
# cash. This headroom avoids a synthetic cap after realized gains.
FULL_CAPITAL_LIMIT_CENTS = 1_000_000_000_000
MAX_SIGNAL_AGE_SECONDS = 90
MAX_BOOK_AGE_SECONDS = 15
BOOK_DELAY_SECONDS = 2
PRICE_BUFFER_CENTS = 1
TAKER_FEE_RATE = Decimal("0.07")
FEE_ROUNDING_POLICY = "conservative_whole_cent_ceiling_approximation"
ENTRY_BLOCKERS = {"global_manual_stop", "recovery_not_fresh_and_verified", "recovery_status_missing_or_invalid"}
_MARKER = "neorome.btc_stochastic_paper"


class BTCStochasticPaperError(ValueError):
    """Evidence is malformed, stale, mismatched, or not eligible."""


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise BTCStochasticPaperError("evidence must contain canonical JSON values") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _hash(value: Any, name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise BTCStochasticPaperError(f"{name} must be a SHA-256 digest")
    return value.lower()


def _time(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            try:
                parsed = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError) as date_exc:
                raise BTCStochasticPaperError(f"{name} must be an ISO-8601 or HTTP-date timestamp") from date_exc
    else:
        raise BTCStochasticPaperError(f"{name} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BTCStochasticPaperError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal(value: Any, name: str, *, low: Decimal, high: Decimal, fixed_string: bool = False,
             max_decimal_places: int = 2) -> Decimal:
    if isinstance(value, bool) or (fixed_string and not isinstance(value, str)) or not isinstance(value, (str, int, float, Decimal)):
        raise BTCStochasticPaperError(f"{name} must be a fixed-decimal value")
    rendered = str(value)
    if fixed_string and "e" in rendered.lower():
        raise BTCStochasticPaperError(f"{name} must be a fixed-decimal string")
    try:
        parsed = Decimal(rendered)
    except (InvalidOperation, ValueError) as exc:
        raise BTCStochasticPaperError(f"{name} must be a fixed-decimal string") from exc
    if (not parsed.is_finite() or parsed < low or parsed > high
            or (fixed_string and parsed.as_tuple().exponent < -max_decimal_places)):
        raise BTCStochasticPaperError(f"{name} is outside its fixed-decimal range")
    return parsed


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BTCStochasticPaperError(f"{name} must be a positive integer")
    return value


def _root(value: str | os.PathLike[str]) -> Path:
    root = Path(value)
    if root.is_symlink():
        raise BTCStochasticPaperError("state root symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    return root


@contextmanager
def _lock(root: Path):
    path = root / "btc-stochastic-paper.lock"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    stream = os.fdopen(descriptor, "a+")
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except BlockingIOError as exc:
        raise BTCStochasticPaperError("already_running") from exc
    finally:
        stream.close()


def initialize(root: str | os.PathLike[str] = DEFAULT_STATE_ROOT) -> PaperLedger:
    """Open only this new parallel $500 experiment; never migrate another ledger."""
    state_root = _root(root)
    balances = {"prediction": PAPER_CASH_CENTS, "equities": 0, "options": 0, "crypto": 0}
    limits = RiskLimits(
        aggregate_cap_cents=FULL_CAPITAL_LIMIT_CENTS,
        group_cap_cents=FULL_CAPITAL_LIMIT_CENTS,
        per_idea_cents={"prediction": FULL_CAPITAL_LIMIT_CENTS, "equities": 1, "options": 1, "crypto": 1},
    )
    return PaperLedger(state_root / "btc_stochastic_paper.sqlite3", balances=balances, limits=limits,
                       market_risk_policy=MarketRiskPolicy(daily_loss_bps=10_000, max_drawdown_bps=10_000),
                       enforce_market_risk=False)


def _event_dir(root: Path) -> Path:
    path = root / "audit"
    if path.exists() and path.is_symlink():
        raise BTCStochasticPaperError("audit directory symlink refused")
    path.mkdir(mode=0o700, exist_ok=True)
    return path


def _event_path(root: Path, event_id: str) -> Path:
    return _event_dir(root) / (hashlib.sha256(event_id.encode("utf-8")).hexdigest() + ".json")


def _write_event(root: Path, event_id: str, event_type: str, payload: Mapping[str, Any], at: datetime) -> dict[str, Any]:
    body = {"event_id": event_id, "event_type": event_type, "at": _iso(at), "payload": dict(payload)}
    event = {**body, "event_sha256": _digest(body)}
    path = _event_path(root, event_id)
    serialized = _canonical(event) + "\n"
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise BTCStochasticPaperError("audit event symlink refused")
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise BTCStochasticPaperError("audit event is unreadable") from exc
        if any(existing.get(key) != event.get(key) for key in ("event_id", "event_type", "payload")):
            raise BTCStochasticPaperError(f"immutable audit event conflict: {event_id}")
        return existing
    descriptor, temporary = tempfile.mkstemp(prefix=".btc-stochastic-audit-", dir=str(path.parent))
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    except FileExistsError:
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise BTCStochasticPaperError("audit event is unreadable") from exc
        if any(existing.get(key) != event.get(key) for key in ("event_id", "event_type", "payload")):
            raise BTCStochasticPaperError(f"immutable audit event conflict: {event_id}")
        return existing
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return event



def _publication_clock() -> tuple[int, int]:
    """Actual post-commit clocks, separate from the modeled quote/fill clock."""
    return time.time_ns(), time.monotonic_ns()


def _publish_fill(root: Path, fill: Any) -> dict[str, Any]:
    """Publish once after the SDK commits; retries preserve the first receipt."""
    event_id = "fill-publication-" + fill.proposal_id
    path = _event_path(root, event_id)
    if path.exists() or path.is_symlink():
        if path.is_symlink():
            raise BTCStochasticPaperError("fill publication symlink refused")
        try:
            existing = json.loads(path.read_text())
            body = {key: existing[key] for key in ("event_id", "event_type", "at", "payload")}
            valid = (body["event_id"] == event_id and body["event_type"] == "fill_publication"
                     and body["payload"]["fill"] == fill.__dict__
                     and body["at"] == body["payload"]["published_at"]
                     and existing["event_sha256"] == _digest(body))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise BTCStochasticPaperError("invalid fill publication receipt") from exc
        if not valid:
            raise BTCStochasticPaperError("immutable fill publication conflict")
        return existing
    wall_ns, monotonic_ns = _publication_clock()
    published = datetime.fromtimestamp(wall_ns / 1_000_000_000, UTC)
    event = _write_event(root, event_id, "fill_publication", {
        "fill": fill.__dict__, "published_at": _iso(published),
        "capture_wall_ns": wall_ns, "capture_monotonic_ns": monotonic_ns,
    }, published)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return event


def _events(root: Path) -> list[dict[str, Any]]:
    directory = _event_dir(root)
    values = []
    for path in sorted(directory.glob("*.json")):
        if path.is_symlink():
            raise BTCStochasticPaperError("audit event symlink refused")
        try:
            event = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise BTCStochasticPaperError("audit event is unreadable") from exc
        if not isinstance(event, dict) or event.get("event_sha256") != _digest({key: event.get(key) for key in ("event_id", "event_type", "at", "payload")}) or path.name != hashlib.sha256(str(event.get("event_id", "")).encode()).hexdigest() + ".json":
            raise BTCStochasticPaperError("audit event integrity check failed")
        values.append(event)
    return values


def _book(value: Any, now: datetime, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BTCStochasticPaperError(f"{name} book evidence is required")
    rest_date = _time(value.get("rest_date"), f"{name}.rest_date")
    received = _time(value.get("received_at"), f"{name}.received_at")
    source_hash = _hash(value.get("source_hash"), f"{name}.source_hash")
    if (received < rest_date or received > now or (now - received).total_seconds() > MAX_BOOK_AGE_SECONDS
            or (received - rest_date).total_seconds() > MAX_BOOK_AGE_SECONDS):
        raise BTCStochasticPaperError(f"{name} REST Date/receipt is future-dated or stale")
    raw_book = value.get("orderbook_fp")
    if not isinstance(raw_book, Mapping):
        raise BTCStochasticPaperError(f"{name}.orderbook_fp is required")

    def levels(field: str) -> list[dict[str, Any]]:
        raw = raw_book.get(field)
        if not isinstance(raw, list):
            raise BTCStochasticPaperError(f"{name}.{field} must be a level list")
        parsed = []
        input_prices = []
        for index, item in enumerate(raw):
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise BTCStochasticPaperError(f"{name}.{field}[{index}] must be [price, quantity]")
            price = _decimal(item[0], f"{name}.{field}[{index}].price", low=Decimal("0"), high=Decimal("1"),
                             fixed_string=True, max_decimal_places=4)
            input_prices.append(price)
            if isinstance(item[1], bool) or not isinstance(item[1], (str, int)):
                raise BTCStochasticPaperError(f"{name}.{field}[{index}].quantity must be numeric")
            try:
                raw_quantity = Decimal(str(item[1]))
            except InvalidOperation as exc:
                raise BTCStochasticPaperError(f"{name}.{field}[{index}].quantity must be numeric") from exc
            if not raw_quantity.is_finite() or raw_quantity <= 0:
                raise BTCStochasticPaperError(f"{name}.{field}[{index}].quantity must be positive")
            quantity = int(raw_quantity)
            if quantity <= 0:
                continue
            price_cents = int((price * 100).to_integral_value(rounding=ROUND_FLOOR))
            parsed.append({"price": format(price, "f"), "price_cents": price_cents, "quantity": quantity})
        if len(set(input_prices)) != len(input_prices):
            raise BTCStochasticPaperError(f"{name}.{field} contains duplicate prices")
        if len(input_prices) > 1:
            ascending = all(left < right for left, right in zip(input_prices, input_prices[1:]))
            descending = all(left > right for left, right in zip(input_prices, input_prices[1:]))
            if not (ascending or descending):
                raise BTCStochasticPaperError(f"{name}.{field} must be strictly monotonic")
        return sorted(parsed, key=lambda row: Decimal(row["price"]), reverse=True)

    yes = levels("yes_dollars")
    no = levels("no_dollars")
    yes_bids = [{"price_cents": row["price_cents"], "quantity": row["quantity"]} for row in yes]
    no_bids = [{"price_cents": row["price_cents"], "quantity": row["quantity"]} for row in no]
    # Exchange depth is expressed as bids for either outcome. Complementing
    # an opposing bid rounds asks up, keeping the normalized executable price
    # conservative at whole-cent precision.
    yes_asks = sorted(({"price_cents": 100 - row["price_cents"], "quantity": row["quantity"]} for row in no_bids), key=lambda row: row["price_cents"])
    no_asks = sorted(({"price_cents": 100 - row["price_cents"], "quantity": row["quantity"]} for row in yes_bids), key=lambda row: row["price_cents"])
    if yes and no and Decimal(yes[0]["price"]) + Decimal(no[0]["price"]) > Decimal("1"):
        raise BTCStochasticPaperError(f"{name} yes book is crossed")
    return {"rest_date": _iso(rest_date), "received_at": _iso(received), "source_hash": source_hash,
            "yes_bids": yes_bids, "yes_asks": yes_asks,
            "no_bids": no_bids, "no_asks": no_asks}


def _market(value: Any, *, now: datetime, allow_settled: bool = False) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BTCStochasticPaperError("market metadata is required")
    ticker = value.get("ticker")
    if not isinstance(ticker, str) or re.fullmatch(r"KXBTC15M-[A-Z0-9_-]{6,64}", ticker) is None or any(tag in ticker for tag in ("SYNTH", "TEST", "MOCK", "FIXTURE")):
        raise BTCStochasticPaperError("an exact discovered KXBTC15M market ticker is required")
    if type(value.get("exchange_index")) is not int or value["exchange_index"] != 2:
        raise BTCStochasticPaperError("the discovered exchange index must be 2")
    status = value.get("status")
    if status != ("settled" if allow_settled else "active"):
        raise BTCStochasticPaperError("market status is not eligible for this lifecycle step")
    if value.get("venue", "kalshi") != "kalshi":
        raise BTCStochasticPaperError("only the discovered Kalshi contract is eligible")
    opened = _time(value.get("open_time"), "market.open_time")
    closed = _time(value.get("close_time"), "market.close_time")
    if opened.microsecond or int(opened.timestamp()) % 900 or closed - opened != timedelta(seconds=900):
        raise BTCStochasticPaperError("market must be one aligned 15-minute UTC event")
    if not allow_settled and not (opened <= now < closed):
        raise BTCStochasticPaperError("market is not currently open")
    if allow_settled and now < closed:
        raise BTCStochasticPaperError("market cannot settle before its close")
    try:
        strike = _decimal(value.get("floor_strike"), "market.floor_strike", low=Decimal("0.01"), high=Decimal("1000000000"))
    except BTCStochasticPaperError as exc:
        raise BTCStochasticPaperError("a positive floor strike is required") from exc
    rules = value.get("rules_primary")
    if not isinstance(rules, str):
        raise BTCStochasticPaperError("primary settlement rules are required")
    normalized_rules = rules.casefold()
    primary_terms = re.split(r"[.;\n]", normalized_rules)
    bullish_at_least = any(
        re.search(r"\byes\b.{0,160}\bat least\b", clause)
        or re.search(r"\bat least\b.{0,160}\byes\b", clause)
        for clause in primary_terms
    )
    if (not all(part in normalized_rules for part in ("cf benchmarks", "brti"))
            or not ("sixty seconds" in normalized_rules or "60 seconds" in normalized_rules)
            or "average" not in normalized_rules or not bullish_at_least):
        raise BTCStochasticPaperError("rules must say YES when the 60-second CF Benchmarks BRTI average is at least the strike")
    return {"ticker": ticker, "exchange_index": 2, "venue": "kalshi", "status": status,
            "open_time": _iso(opened), "close_time": _iso(closed), "floor_strike": format(strike, "f"),
            "rules_primary": rules, "source_hash": _hash(value.get("source_hash"), "market.source_hash")}


def _signal(value: Any, market: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BTCStochasticPaperError("frozen stochastic signal is required")
    if value.get("valid") is not True or value.get("instrument") != "BTC-USD" or value.get("timeframe_minutes") != 30 or value.get("method") != "fresh_cross":
        raise BTCStochasticPaperError("only the fixed 30m fresh-crossover BTC-USD signal is eligible")
    if value.get("strategy") not in (None, STRATEGY_ID):
        raise BTCStochasticPaperError("signal strategy identity differs")
    direction = value.get("direction", "bullish")
    expected_side = "yes" if direction == "bullish" else "no" if direction == "bearish" else None
    contract_side = value.get("contract_side", expected_side)
    if expected_side is None or contract_side != expected_side:
        raise BTCStochasticPaperError("signal direction and contract_side must be a matching bullish/YES or bearish/NO pair")
    raw_previous_k = value.get("previous_k")
    raw_current_k = value.get("k")
    if any(isinstance(item, bool) or not isinstance(item, (int, float, Decimal))
           for item in (raw_previous_k, raw_current_k)):
        raise BTCStochasticPaperError("signal oscillator values must be real numeric values")
    try:
        previous_k = float(raw_previous_k)
        current_k = float(raw_current_k)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BTCStochasticPaperError("signal oscillator values are required") from exc
    if not all(math.isfinite(item) and 0 <= item <= 100 for item in (previous_k, current_k)):
        raise BTCStochasticPaperError("signal oscillator values are outside [0, 100]")
    if direction == "bullish":
        threshold = value.get("oversold", 20)
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float, Decimal)) or threshold != 20:
            raise BTCStochasticPaperError("bullish signal must use the frozen oversold-20 threshold")
        if previous_k > 20 or current_k <= previous_k:
            raise BTCStochasticPaperError("signal does not satisfy the frozen bullish oversold-20 turn rule")
        threshold_field = {"oversold": 20}
    else:
        threshold = value.get("overbought", 80)
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float, Decimal)) or threshold != 80:
            raise BTCStochasticPaperError("bearish signal must use the frozen overbought-80 threshold")
        if previous_k < 80 or current_k >= previous_k:
            raise BTCStochasticPaperError("signal does not satisfy the frozen bearish overbought-80 turn rule")
        threshold_field = {"overbought": 80}
    raw_d = (value.get("previous_d"), value.get("d"))
    if any(isinstance(item, bool) or not isinstance(item, (int, float, Decimal)) for item in raw_d):
        raise BTCStochasticPaperError("fresh crossover requires numeric previous/current D")
    previous_d, current_d = map(float, raw_d)
    if not all(math.isfinite(item) and 0 <= item <= 100 for item in (previous_d, current_d)):
        raise BTCStochasticPaperError("signal D values are outside [0, 100]")
    crossed = (previous_k <= previous_d and current_k > current_d if direction == "bullish"
               else previous_k >= previous_d and current_k < current_d)
    if not crossed:
        raise BTCStochasticPaperError("signal requires a fresh K/D crossover on this closed bar")
    signal_id = value.get("signal_id")
    if not isinstance(signal_id, str) or not signal_id.strip():
        raise BTCStochasticPaperError("signal_id is required")
    try:
        bar_close = datetime.fromtimestamp(int(value.get("bar_close_timestamp")), UTC)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BTCStochasticPaperError("signal bar_close_timestamp is invalid") from exc
    opened = _time(market["open_time"], "market.open_time")
    if bar_close != opened or not (timedelta(0) <= now - opened <= timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)):
        raise BTCStochasticPaperError("signal must close at market open and be within the first 90 seconds")
    receipt = _time(value.get("observed_receipt_at"), "signal.observed_receipt_at")
    if receipt < opened or receipt > now or (now - receipt).total_seconds() > MAX_SIGNAL_AGE_SECONDS:
        raise BTCStochasticPaperError("signal receipt must follow bar close and remain fresh")
    return {"signal_id": signal_id, "instrument": "BTC-USD", "timeframe_minutes": 30,
            "method": "fresh_cross", "bar_close_timestamp": int(value["bar_close_timestamp"]),
            "observed_receipt_at": _iso(receipt), "source_hash": _hash(value.get("source_hash"), "signal.source_hash"),
            "k": current_k, "d": current_d, "previous_k": previous_k,
            "previous_d": previous_d, "direction": direction,
            "contract_side": contract_side, **threshold_field}


def _fee(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("fee_type") != "quadratic":
        raise BTCStochasticPaperError("quadratic taker-fee metadata is required")
    multiplier = _decimal(value.get("fee_multiplier"), "fee.fee_multiplier", low=Decimal("0.000001"), high=Decimal("1000000"))
    return {"fee_type": "quadratic", "fee_multiplier": format(multiplier, "f"),
            "rounding_policy": FEE_ROUNDING_POLICY,
            "source_hash": _hash(value.get("source_hash"), "fee.source_hash")}


def fee_cents(prices_cents: list[int], fee_multiplier: str | Decimal) -> int:
    """Conservatively approximate the quadratic fee with whole-cent ceiling.

    The published fee schedule rounds per contract; this aggregate whole-cent
    ceiling may overstate fees slightly and is used only for paper budgeting.
    """
    if not prices_cents or any(type(price) is not int or not 0 <= price <= 100 for price in prices_cents):
        raise BTCStochasticPaperError("fee calculation needs contract prices in cents")
    multiplier = Decimal(str(fee_multiplier))
    if not multiplier.is_finite() or multiplier <= 0:
        raise BTCStochasticPaperError("fee multiplier must be positive and finite")
    quantity = Decimal(len(prices_cents))
    probability = Decimal(sum(prices_cents)) / (quantity * 100)
    dollars = TAKER_FEE_RATE * multiplier * quantity * probability * (Decimal(1) - probability)
    return int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))


def _walk(levels: list[dict[str, int]], quantity: int) -> list[int] | None:
    remaining = quantity
    prices = []
    for level in levels:
        used = min(remaining, level["quantity"])
        prices.extend([level["price_cents"]] * used)
        remaining -= used
        if remaining == 0:
            return prices
    return None


def _unit_and_fee(prices_cents: list[int], multiplier: str) -> tuple[int, int, int]:
    buffered = [price + PRICE_BUFFER_CENTS for price in prices_cents]
    unit = int((Decimal(sum(buffered)) / Decimal(len(buffered))).to_integral_value(rounding=ROUND_CEILING))
    fees = fee_cents(buffered, multiplier)
    return unit, fees, len(buffered) * unit + fees


def _cost_for_quantity(asks: list[dict[str, int]], quantity: int, multiplier: str) -> tuple[int, int, int, int] | None:
    remaining = quantity
    price_sum = 0
    worst = 0
    for level in asks:
        price = level["price_cents"]
        if price <= 0 or price > 60:
            break
        used = min(remaining, level["quantity"])
        price_sum += used * price
        worst = max(worst, price)
        remaining -= used
        if remaining == 0:
            break
    if remaining:
        return None
    buffered_sum = price_sum + quantity * PRICE_BUFFER_CENTS
    unit = (buffered_sum + quantity - 1) // quantity
    probability = Decimal(buffered_sum) / Decimal(quantity * 100)
    dollars = TAKER_FEE_RATE * Decimal(multiplier) * quantity * probability * (Decimal(1) - probability)
    fees = int((dollars * 100).to_integral_value(rounding=ROUND_CEILING))
    return unit, fees, quantity * unit + fees, worst


def _size(asks: list[dict[str, int]], budget_cents: int, multiplier: str,
          alternative_asks: list[dict[str, int]] | None = None) -> dict[str, int] | None:
    ladders = [asks] + ([alternative_asks] if alternative_asks is not None else [])
    max_depth = min(sum(level["quantity"] for level in ladder if 0 < level["price_cents"] <= 60)
                    for ladder in ladders)
    # Every contract costs at least one buffered cent, bounding work even when
    # a source book advertises millions of contracts.
    max_quantity = min(max_depth, budget_cents)
    best = None
    for quantity in range(1, max_quantity + 1):
        costs = [_cost_for_quantity(ladder, quantity, multiplier) for ladder in ladders]
        if any(cost is None for cost in costs):
            continue
        reserve_cost = max((cost for cost in costs if cost is not None), key=lambda cost: cost[2])
        unit, reserve_fee, reserve_total, _ = reserve_cost
        if reserve_total <= budget_cents:
            # Escrow the more expensive of the independently observed books.
            best = {"quantity": quantity, "unit_cost_cents": unit, "fee_cents": reserve_fee,
                    "reserved_cents": reserve_total,
                    "worst_ask_cents": max(cost[3] for cost in costs if cost is not None),
                    "book1_unit_cost_cents": costs[0][0], "book1_fee_cents": costs[0][1]}
    return best


def _entry_timing(signal: Mapping[str, Any], market: Mapping[str, Any], fee: Mapping[str, Any], book1: Any, book2: Any, now: datetime) -> tuple[dict[str, Any], dict[str, Any] | None]:
    first = _book(book1, now, "book1")
    second = _book(book2, now, "book2") if book2 is not None else None
    if second is not None:
        if _time(second["received_at"], "book2.received_at") < _time(first["received_at"], "book1.received_at") + timedelta(seconds=BOOK_DELAY_SECONDS):
            raise BTCStochasticPaperError("independent book2 must arrive at least two seconds after book1")
        closed = _time(market["close_time"], "market.close_time")
        if _time(second["received_at"], "book2.received_at") >= closed:
            raise BTCStochasticPaperError("book2 must arrive strictly before market close")
        if _time(second["received_at"], "book2.received_at") > now:
            raise BTCStochasticPaperError("book2 receipt is future-dated")
    return first, second


def _proposal_id(signal_id: str, market_ticker: str) -> str:
    return "btc-stoch-" + hashlib.sha256((signal_id + "\0" + market_ticker).encode()).hexdigest()[:40]


def _fill_id(proposal_id: str) -> str:
    return "btc-stoch-fill-" + hashlib.sha256(proposal_id.encode()).hexdigest()[:40]


def _reservation_payload(book: PaperLedger, proposal_id: str) -> Mapping[str, Any]:
    for audit in book.audit():
        payload = audit.payload.get("payload", {})
        if audit.event_type == "reserve" and isinstance(payload, Mapping) and payload.get("proposal_id") == proposal_id:
            return payload
    raise BTCStochasticPaperError("reservation proposal is missing from the ledger audit")


def _active_position(book: PaperLedger) -> tuple[Any, Mapping[str, Any]] | None:
    audits = book.audit()
    for audit in reversed(audits):
        if audit.event_type != "fill":
            continue
        position_id = audit.payload.get("position_id")
        if not position_id:
            continue
        position = book.get_position(position_id)
        if position.state != "open":
            continue
        reserve = next((row for row in audits if row.event_type == "reserve" and row.entity_id == audit.payload.get("reservation_id")), None)
        if reserve is None:
            raise BTCStochasticPaperError("open position has no reservation audit")
        proposal = reserve.payload.get("payload", {})
        metadata = proposal.get("metadata", {}) if isinstance(proposal, Mapping) else {}
        marker = metadata.get(_MARKER) if isinstance(metadata, Mapping) else None
        if not isinstance(marker, Mapping):
            raise BTCStochasticPaperError("open position does not belong to this isolated adapter")
        return position, marker
    return None


def _active_reservation(book: PaperLedger) -> Any | None:
    for audit in reversed(book.audit()):
        if audit.event_type == "reserve":
            try:
                reservation = book.get_reservation(audit.entity_id)
            except Exception:
                continue
            if reservation.state == "reserved":
                payload = audit.payload.get("payload", {})
                metadata = payload.get("metadata", {}) if isinstance(payload, Mapping) else {}
                if isinstance(metadata, Mapping) and _MARKER in metadata:
                    return reservation
    return None


def _available_cash(book: PaperLedger) -> int:
    return next(row.available_cash_cents for row in book.balances() if row.desk == "prediction")


def _input_envelope(input_map: Any, now: datetime) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    if not isinstance(input_map, Mapping):
        raise BTCStochasticPaperError("input_map must be a mapping")
    market = _market(input_map.get("market"), now=now)
    signal = _signal(input_map.get("signal"), market, now)
    fee = _fee(input_map.get("fee"))
    book1, _ = _entry_timing(signal, market, fee, input_map.get("book1"), None, now)
    second = _safe_book_input(input_map.get("book2"))
    evidence = {"signal": signal, "market": market, "fee": fee, "book1": book1, "book2": second}
    return signal, market, fee, book1, second, {"input_sha256": _digest(evidence)}


def _safe_book_input(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise BTCStochasticPaperError("book evidence must be an object")
    raw_book = value.get("orderbook_fp")
    if not isinstance(raw_book, Mapping):
        raise BTCStochasticPaperError("orderbook_fp is required")
    return {"rest_date": value.get("rest_date"), "received_at": value.get("received_at"),
            "source_hash": value.get("source_hash"),
            "orderbook_fp": {"yes_dollars": raw_book.get("yes_dollars"), "no_dollars": raw_book.get("no_dollars")}}


def _cancel(book: PaperLedger, root: Path, reservation: Any, now: datetime, reason: str) -> dict[str, Any]:
    cancel_id = "btc-stoch-cancel-" + hashlib.sha256(reservation.proposal_id.encode()).hexdigest()[:40]
    _write_event(root, "cancel-intent-" + reservation.proposal_id, "cancel_intent",
                 {"proposal_id": reservation.proposal_id, "reservation_id": reservation.reservation_id, "reason": reason}, now)
    result = book.cancel(reservation.reservation_id, cancel_id=cancel_id, reason=reason, cancelled_at=now)
    _write_event(root, "cancelled-" + reservation.proposal_id, "cancelled", {"result": result.__dict__, "reason": reason}, now)
    return {"status": "cancelled", "reason": reason, "reservation_id": reservation.reservation_id,
            "venue_orders_sent": 0, "execution_mode": "internal_simulation"}


def _settle(book: PaperLedger, root: Path, position: Any, marker: Mapping[str, Any], settlement: Any, now: datetime) -> dict[str, Any]:
    if not isinstance(settlement, Mapping):
        raise BTCStochasticPaperError("official final settlement metadata is required")
    ticker = settlement.get("ticker")
    result = settlement.get("result")
    observed = _time(settlement.get("observed_at"), "settlement.observed_at")
    source_hash = _hash(settlement.get("source_hash"), "settlement.source_hash")
    if settlement.get("status") not in ("settled", "finalized") or ticker != marker["market"]["ticker"] or result not in ("yes", "no"):
        raise BTCStochasticPaperError("only matching official settled YES/NO metadata is accepted")
    if observed < _time(marker["market"]["close_time"], "market.close_time") or observed > now:
        raise BTCStochasticPaperError("official settlement timing is invalid")
    held_side = marker.get("contract_side", "yes")
    proceeds = position.quantity * 100 if result == held_side else 0
    close_id = "btc-stoch-settle-" + hashlib.sha256(position.position_id.encode()).hexdigest()[:40]
    _write_event(root, "settlement-intent-" + position.position_id, "settlement_intent",
                 {"ticker": ticker, "result": result, "observed_at": _iso(observed), "source_hash": source_hash,
                  "position_id": position.position_id, "contract_side": held_side,
                  "proceeds_cents": proceeds, "fee_cents": 0}, now)
    closed = book.settle(position.position_id, close_id=close_id, proceeds_cents=proceeds, fee_cents=0,
                         provenance="observed", closed_at=now)
    _write_event(root, "settled-" + position.position_id, "settled",
                 {"result": closed.__dict__, "official_result": result, "source_hash": source_hash}, now)
    return {"status": "settled", "result": result, "contract_side": held_side,
            "close": closed.__dict__, "venue_orders_sent": 0,
            "execution_mode": "internal_simulation"}


def _mark(book: PaperLedger, root: Path, position: Any, marker: Mapping[str, Any], mark_input: Any, now: datetime) -> dict[str, Any]:
    if not isinstance(mark_input, Mapping):
        return {"status": "unvalued_missing_quote", "position_id": position.position_id, "liquidation_value_cents": None}
    if mark_input.get("ticker") != marker["market"]["ticker"]:
        return {"status": "unvalued_wrong_market", "position_id": position.position_id, "liquidation_value_cents": None}
    try:
        normalized = _book(mark_input, now, "mark_book")
    except BTCStochasticPaperError:
        return {"status": "unvalued_stale_or_invalid_quote", "position_id": position.position_id,
                "liquidation_value_cents": None}
    if _time(normalized["received_at"], "mark_book.received_at") < _time(position.opened_at, "position.opened_at"):
        return {"status": "unvalued_stale_or_invalid_quote", "position_id": position.position_id,
                "liquidation_value_cents": None}
    held_side = marker.get("contract_side", "yes")
    bids = normalized[f"{held_side}_bids"]
    executable = _walk(bids, position.quantity)
    if executable is None:
        return {"status": "unvalued_missing_bid_depth", "position_id": position.position_id, "liquidation_value_cents": None}
    net_prices = [max(0, price - PRICE_BUFFER_CENTS) for price in executable]
    gross = sum(net_prices)
    exit_fee = fee_cents(net_prices, marker["fee"]["fee_multiplier"]) if any(net_prices) else 0
    value = max(0, gross - exit_fee)
    batch_id = "btc-stoch-mark-" + hashlib.sha256((position.position_id + normalized["source_hash"] + normalized["received_at"]).encode()).hexdigest()[:40]
    event_id = "mark-" + batch_id
    prior_event = next((event for event in _events(root) if event["event_id"] == event_id), None)
    if prior_event is not None:
        prior = prior_event["payload"]
        return {"status": "valued", "position_id": position.position_id, "quantity": position.quantity,
                "liquidation_value_cents": prior["liquidation_value_cents"], "exit_fee_cents": prior["exit_fee_cents"],
                "source_hash": normalized["source_hash"], "venue_orders_sent": 0}
    mark = MarketMark(position.position_id, value, normalized["received_at"], normalized["source_hash"], "observed")
    snapshot = book.record_marks(batch_id, [mark], as_of=now)
    _write_event(root, event_id, "mark", {"ticker": marker["market"]["ticker"], "position_id": position.position_id,
                "contract_side": held_side, "quantity": position.quantity,
                f"{held_side}_bid_prices_cents": executable, "buffer_cents": PRICE_BUFFER_CENTS * position.quantity,
                "exit_fee_cents": exit_fee, "liquidation_value_cents": value, "source_hash": normalized["source_hash"],
                "received_at": normalized["received_at"], "paper_nav_cents": snapshot.market_nav_cents}, now)
    return {"status": "valued", "position_id": position.position_id, "quantity": position.quantity,
            "contract_side": held_side, "liquidation_value_cents": value,
            "exit_fee_cents": exit_fee, "source_hash": normalized["source_hash"],
            "venue_orders_sent": 0}


def _resume_reservation(book: PaperLedger, root: Path, reservation: Any, input_map: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    proposal = _reservation_payload(book, reservation.proposal_id)
    metadata = proposal.get("metadata", {}).get(_MARKER, {})
    available_before = _available_cash(book) + reservation.reserved_cents
    budget = available_before * ALLOCATION_BPS // 10_000
    if reservation.reserved_cents > budget:
        return _cancel(book, root, reservation, now, "pending reservation exceeds the current 20% cash allocation")
    intent_event = next((event for event in _events(root) if event["event_type"] == "intent" and event["payload"].get("proposal_id") == reservation.proposal_id), None)
    if intent_event is None:
        raise BTCStochasticPaperError("durable intent evidence is missing for a pending reservation")
    intent = intent_event["payload"]
    market = metadata.get("market", {})
    close_time = _time(market.get("close_time"), "market.close_time")
    if now >= close_time:
        return _cancel(book, root, reservation, now, "market closed before modeled fill")
    open_time = _time(market.get("open_time"), "market.open_time")
    entry_deadline = open_time + timedelta(seconds=MAX_SIGNAL_AGE_SECONDS)
    if now > entry_deadline:
        return _cancel(book, root, reservation, now, "signal entry window expired before modeled fill")
    current_input = input_map if isinstance(input_map, Mapping) else {}
    supplied_book2 = current_input.get("book2")
    current_market = current_input.get("market")
    if supplied_book2 is not None and (not isinstance(current_market, Mapping) or current_market.get("ticker") != market.get("ticker")):
        supplied_book2 = None
    book2_value = supplied_book2 if supplied_book2 is not None else intent.get("book2")
    if book2_value is None:
        return {"status": "pending", "reason": "awaiting independent book2", "reservation_id": reservation.reservation_id,
                "venue_orders_sent": 0, "execution_mode": "internal_simulation"}
    try:
        second = _book(book2_value, now, "book2")
        first = intent["book1"]
        if (second["source_hash"] == first["source_hash"]
                or _time(second["received_at"], "book2.received_at") < _time(first["received_at"], "book1.received_at") + timedelta(seconds=BOOK_DELAY_SECONDS)):
            raise BTCStochasticPaperError("book2 is not independent by the two-second minimum")
        if _time(second["received_at"], "book2.received_at") >= close_time:
            raise BTCStochasticPaperError("book2 was received at or after market close")
        if _time(second["received_at"], "book2.received_at") > entry_deadline:
            raise BTCStochasticPaperError("book2 was received after the frozen signal entry window")
        contract_side = metadata.get("contract_side", "yes")
        prices = _walk(second[f"{contract_side}_asks"], reservation.quantity)
        if prices is None or any(price <= 0 or price > 60 for price in prices):
            raise BTCStochasticPaperError("book2 lacks eligible ask price/depth for the full reserved quantity")
        unit, fees, actual_total = _unit_and_fee(prices, metadata["fee"]["fee_multiplier"])
        if (actual_total > reservation.reserved_cents or actual_total > metadata["budget_cents"]
                or actual_total > budget):
            raise BTCStochasticPaperError("book2 all-in fill cost exceeds the reservation or risk budget")
    except BTCStochasticPaperError as exc:
        return _cancel(book, root, reservation, now, str(exc))

    fill_id = _fill_id(reservation.proposal_id)
    fill_intent_id = "fill-intent-" + reservation.proposal_id
    fill_payload = {"proposal_id": reservation.proposal_id, "reservation_id": reservation.reservation_id,
                    "fill_id": fill_id, "quantity": reservation.quantity, "unit_cost_cents": unit,
                    "fee_cents": fees, "book2": second, "all_in_cents": actual_total}
    _write_event(root, fill_intent_id, "fill_intent", fill_payload, _time(second["received_at"], "book2.received_at"))
    fill = book.simulate_fill(reservation.reservation_id, fill_id=fill_id, quantity=reservation.quantity,
                              unit_cost_cents=unit, fee_cents=fees, provenance="observed",
                              filled_at=second["received_at"])
    _publish_fill(root, fill)
    _write_event(root, "filled-" + reservation.proposal_id, "filled", {"fill": fill.__dict__, "book2": second,
                 "all_in_cents": actual_total}, _time(second["received_at"], "book2.received_at"))
    position, marker = _active_position(book)  # Fill record is the authoritative open position.
    mark_input = input_map.get("mark_book") if isinstance(input_map, Mapping) else None
    if mark_input is None:
        mark_input = book2_value
    if isinstance(mark_input, Mapping):
        mark_input = {**mark_input, "ticker": marker["market"]["ticker"]}
    mark = _mark(book, root, position, marker, mark_input, now)
    return {"status": "filled", "fill": fill.__dict__, "mark": mark, "venue_orders_sent": 0,
            "execution_mode": "internal_simulation"}


def run_once(input_map: Mapping[str, Any], root: str | os.PathLike[str] = DEFAULT_STATE_ROOT,
             now: datetime | str | None = None) -> dict[str, Any]:
    """Validate one frozen 30m-fresh crossover signal and advance at most one paper event.

    `input_map` fields are `signal`, `market`, `fee`, `book1`, `book2`, with
    optional `settlement` and `mark_book`. It performs no network requests.
    """
    current = _time(now or datetime.now(UTC), "now")
    state_root = _root(root)
    with _lock(state_root):
        book = initialize(state_root)
        _events(state_root)
        position_info = _active_position(book)
        if position_info is not None:
            position, marker = position_info
            settlement = input_map.get("settlement") if isinstance(input_map, Mapping) else None
            if settlement is not None:
                return _settle(book, state_root, position, marker, settlement, current)
            mark_input = input_map.get("mark_book") if isinstance(input_map, Mapping) else None
            return {"status": "open", "position_id": position.position_id,
                    "mark": _mark(book, state_root, position, marker, mark_input, current),
                    "venue_orders_sent": 0, "execution_mode": "internal_simulation"}
        reservation = _active_reservation(book)
        if reservation is not None:
            blocker = input_map.get("entry_blocker_reason") if isinstance(input_map, Mapping) else None
            if isinstance(blocker, str) and blocker in ENTRY_BLOCKERS:
                return _cancel(book, state_root, reservation, current, f"entry blocked: {blocker}")
            if book.risk_stop_active():
                return _cancel(book, state_root, reservation, current, "manual paper STOP active")
            return _resume_reservation(book, state_root, reservation, input_map, current)
        if book.risk_stop_active():
            return {"status": "abstain", "reason": "manual paper STOP active", "venue_orders_sent": 0}
        if book.active_exposure_count("prediction"):
            return {"status": "abstain", "reason": "unrecognized pending or open prediction exposure", "venue_orders_sent": 0}
        replay_signal = input_map.get("signal") if isinstance(input_map, Mapping) else None
        replay_market = input_map.get("market") if isinstance(input_map, Mapping) else None
        if isinstance(replay_signal, Mapping) and isinstance(replay_market, Mapping):
            replay_id = replay_signal.get("signal_id")
            replay_ticker = replay_market.get("ticker")
            if isinstance(replay_id, str) and isinstance(replay_ticker, str):
                replay_proposal = _proposal_id(replay_id, replay_ticker)
                for audit in book.audit():
                    if audit.event_type != "reserve":
                        continue
                    payload = audit.payload.get("payload", {})
                    if not isinstance(payload, Mapping) or payload.get("proposal_id") != replay_proposal:
                        continue
                    old_reservation = book.get_reservation(audit.entity_id)
                    if old_reservation.state == "cancelled":
                        return {"status": "cancelled_replay", "reservation_id": old_reservation.reservation_id,
                                "venue_orders_sent": 0, "execution_mode": "internal_simulation"}
                    if old_reservation.state == "filled":
                        return {"status": "completed_replay", "reservation_id": old_reservation.reservation_id,
                                "venue_orders_sent": 0, "execution_mode": "internal_simulation"}
        signal, market, fee, first, second, envelope = _input_envelope(input_map, current)
        source_time = datetime.fromtimestamp(signal["bar_close_timestamp"], UTC)
        balances = {row.desk: row for row in book.balances()}
        available = balances["prediction"].available_cash_cents
        budget = available * ALLOCATION_BPS // 10_000
        contract_side = signal["contract_side"]
        asks1 = first[f"{contract_side}_asks"]
        asks2 = _book(second, current, "book2")[f"{contract_side}_asks"] if second is not None else None
        size = _size(asks1, budget, fee["fee_multiplier"], asks2)
        if size is None:
            return {"status": "abstain", "reason": "book1 ask depth cannot fit within the paper risk budget",
                    "budget_cents": budget, "venue_orders_sent": 0}
        proposal_id = _proposal_id(signal["signal_id"], market["ticker"])
        marker = {"profile_version": PROFILE_VERSION, "strategy_id": STRATEGY_ID, "strategy_version": STRATEGY_VERSION,
                  "signal": signal, "market": market, "fee": fee, "book1": first, "book2": second,
                  "budget_cents": budget, "reserved_cents": size["reserved_cents"], "quantity": size["quantity"],
                  "contract_side": contract_side,
                  "book1_unit_cost_cents": size["book1_unit_cost_cents"], "book1_fee_cents": size["book1_fee_cents"],
                  "allocation_bps": ALLOCATION_BPS, "allocation_policy_version": ALLOCATION_POLICY_VERSION,
                  "available_cash_before_cents": available,
                  "source_input_sha256": envelope["input_sha256"]}
        metadata = {_MARKER: marker}
        proposal = Proposal(
            proposal_id=proposal_id,
            desk="prediction",
            strategy=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            source_hash=signal["source_hash"],
            source_timestamp=source_time,
            instrument=market["ticker"],
            exposure_group="prediction:" + market["ticker"],
            idea_id=proposal_id,
            expiry_at=market["close_time"],
            quantity=size["quantity"],
            unit_cost_cents=size["unit_cost_cents"],
            fee_cents=size["fee_cents"],
            provenance="observed",
            metadata=metadata,
        )
        intent_payload = {"proposal_id": proposal_id, "signal_id": signal["signal_id"], "market_ticker": market["ticker"],
                          "input_sha256": envelope["input_sha256"], "signal": signal, "market": market, "fee": fee,
                          "book1": first, "book2": second, "budget_cents": budget, "size": size}
        intent_payload.update({"allocation_bps": ALLOCATION_BPS,
                               "allocation_policy_version": ALLOCATION_POLICY_VERSION,
                               "available_cash_before_cents": available})
        _write_event(state_root, "intent-" + proposal_id, "intent", intent_payload,
                     _time(first["received_at"], "book1.received_at"))
        reservation = book.reserve(proposal, now=first["received_at"])
        _write_event(state_root, "reserved-" + proposal_id, "reserved",
                     {"reservation": reservation.__dict__, "source_input_sha256": envelope["input_sha256"]},
                     _time(first["received_at"], "book1.received_at"))
        return _resume_reservation(book, state_root, reservation, input_map, current)


def set_manual_stop(root: str | os.PathLike[str], stopped: bool, *, now: datetime | str | None = None,
                    reason: str = "operator request") -> bool:
    """Set the manual stop only on this isolated BTC prediction ledger."""
    current = _time(now or datetime.now(UTC), "now")
    state_root = _root(root)
    with _lock(state_root):
        book = initialize(state_root)
        return book.set_risk_stop(stopped, reason=reason, at=current)


def _market_snapshot_or_none(book: PaperLedger, current: datetime) -> dict[str, Any] | None:
    try:
        return book.market_snapshot(as_of=current).__dict__
    except InvalidState:
        return None


def snapshot(root: str | os.PathLike[str] = DEFAULT_STATE_ROOT,
             now: datetime | str | None = None) -> dict[str, Any]:
    """Return dedicated-ledger balances, lifecycle state, and immutable audit."""
    current = _time(now or datetime.now(UTC), "now")
    state_root = _root(root)
    with _lock(state_root):
        book = initialize(state_root)
        position_info = _active_position(book)
        reservation = _active_reservation(book)
        return {
            "profile_version": PROFILE_VERSION,
            "strategy_id": STRATEGY_ID,
            "allocation_bps": ALLOCATION_BPS,
            "allocation_policy_version": ALLOCATION_POLICY_VERSION,
            "ledger_snapshot": book.snapshot(as_of=current).__dict__,
            "market_snapshot": _market_snapshot_or_none(book, current),
            "balances": [row.__dict__ for row in book.balances()],
            "active_position": position_info[0].__dict__ if position_info else None,
            "pending_reservation": reservation.__dict__ if reservation else None,
            "available_cash_before_cents": ((_available_cash(book) + reservation.reserved_cents)
                                             if reservation else (position_info[1].get("available_cash_before_cents")
                                                                  if position_info else None)),
            "manual_stop_active": book.risk_stop_active(),
            "audit_events": _events(state_root),
            "venue_orders_sent": 0,
            "execution_mode": "internal_simulation",
        }


__all__ = ["BTCStochasticPaperError", "DEFAULT_STATE_ROOT", "STRATEGY_ID", "fee_cents",
           "initialize", "run_once", "set_manual_stop", "snapshot"]
