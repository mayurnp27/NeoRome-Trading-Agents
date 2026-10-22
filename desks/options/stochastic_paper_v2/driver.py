#!/usr/bin/env python3
"""Deterministic one-shot driver for the stochastic paper arms.

Only read-only market-data tools are used. The transport keeps OAuth tokens
private; this module records request accounting, input archives and paper
state in the supplied state directory.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import time as elapsed_time
from typing import Callable, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import core

UTC = timezone.utc
MAX_DAILY_CALLS = 1500
MAX_SESSION_CALLS = 96
MAX_INSTRUMENT_PAGES = 12
MAX_CATALOG_CONTRACTS = 2000
QUOTE_BATCH = 20
HISTORY_BATCH = 2
HISTORY_DAYS = 8
SYMBOLS = ("SPY", "IWM", "QQQ", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA")
READ_ONLY_ADDITIONS = {"get_equity_historicals", "get_equity_quotes"}
UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class DriverError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock_must_be_timezone_aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return core.parse_time(value)


def _json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(_json_bytes(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _immutable_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_bytes(value) + b"\n"
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(str(path), flags, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise DriverError("already_running")
    return handle


def _safe_float(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _rows(data: Mapping, key: str) -> list:
    rows = data.get(key, [])
    if not isinstance(rows, list):
        raise DriverError("malformed_market_data")
    return rows


def _cursor(data: Mapping) -> str | None:
    next_url = data.get("next")
    if next_url in (None, ""):
        return None
    if not isinstance(next_url, str) or not next_url or len(next_url) > 4096:
        raise DriverError("opaque_instrument_cursor")
    return next_url


def choose_expiry(expirations: list, session_day: date) -> str | None:
    monday = session_day - timedelta(days=session_day.weekday())
    friday = monday + timedelta(days=4)
    dates = []
    for raw in expirations:
        try:
            parsed = date.fromisoformat(str(raw))
        except ValueError:
            raise DriverError("malformed_expiry_catalog")
        if session_day <= parsed <= friday:
            dates.append(parsed)
    listed = sorted(set(dates))
    if session_day.weekday() == 4:
        return session_day.isoformat() if session_day in listed else None
    selected = listed[1] if len(listed) > 1 else listed[0] if listed else None
    return selected.isoformat() if selected else None


def _chain_underlying_id(chain: Mapping) -> str:
    underlyings = chain.get("underlying_instruments")
    if not isinstance(underlyings, list) or len(underlyings) != 1 or not isinstance(underlyings[0], dict):
        raise DriverError("unsupported_or_adjusted_chain")
    url = underlyings[0].get("instrument")
    if not isinstance(url, str):
        raise DriverError("underlying_identity_missing")
    parsed = urlparse(url)
    identifier = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if parsed.scheme not in ("http", "https") or not parsed.netloc or not UUID_PATTERN.fullmatch(identifier):
        raise DriverError("underlying_identity_invalid")
    return identifier


def _chain_for(data: Mapping, symbol: str, session_day: date,
               verified_underlying_id: str | None = None) -> tuple[Mapping, str]:
    chains = _rows(data, "chains")
    matches = [item for item in chains if isinstance(item, dict) and item.get("symbol") == symbol]
    if len(matches) != 1:
        raise DriverError("ambiguous_or_missing_chain")
    chain = matches[0]
    expiry = choose_expiry(chain.get("expiration_dates", []), session_day)
    if not expiry:
        raise DriverError("no_eligible_weekly_expiry")
    multiplier = _safe_float(chain.get("trade_value_multiplier"))
    cash_component = _safe_float(chain.get("cash_component") or 0)
    underlyings = chain.get("underlying_instruments")
    underlying_id = _chain_underlying_id(chain)
    underlying_symbol = underlyings[0].get("symbol")
    if (("can_open_position" in chain and chain.get("can_open_position") is not True) or
            ("settle_on_open" in chain and chain.get("settle_on_open") is not False) or
            multiplier != 100 or cash_component != 0 or not isinstance(underlyings, list) or len(underlyings) != 1 or
            not isinstance(underlyings[0], dict) or underlying_symbol not in ("", symbol) or
            verified_underlying_id != underlying_id):
        raise DriverError("unsupported_or_adjusted_chain")
    return chain, expiry


def _normalize_instrument(item: Mapping, symbol: str, expiry: str, right: str) -> dict:
    identifier = item.get("id")
    strike = _safe_float(item.get("strike_price"))
    multiplier = _safe_float(item.get("trade_value_multiplier"))
    if (not isinstance(identifier, str) or not identifier or item.get("chain_symbol") != symbol or
            item.get("expiration_date") != expiry or item.get("type") != right or
            item.get("underlying_type") != "equity" or item.get("state") != "active" or
            item.get("tradability") != "tradable" or strike is None or strike <= 0 or multiplier != 100):
        raise DriverError("invalid_standard_instrument")
    return {"option_id": identifier, "symbol": symbol, "expiry": expiry,
            "type": right, "strike": strike, "multiplier": 100}


def _normalize_bars(data: Mapping, expected: list[str]) -> dict[str, list[dict]]:
    result = {symbol: [] for symbol in expected}
    records = _rows(data, "results")
    for record in records:
        if not isinstance(record, dict):
            continue
        symbol = str(record.get("symbol", "")).upper()
        if symbol not in result:
            continue
        rows = record.get("historicals", record.get("bars", []))
        if isinstance(rows, list):
            result[symbol].extend(row for row in rows if isinstance(row, dict))
    return result


def _merge_bars(existing: list[dict], incoming: list[dict]) -> list[dict]:
    by_time = {}
    for row in list(existing) + list(incoming):
        try:
            key = _iso(_parse(row["begins_at"]))
        except (KeyError, TypeError, ValueError):
            continue
        canonical = dict(row)
        if key in by_time and by_time[key] != canonical:
            raise DriverError("conflicting_historical_bar")
        by_time[key] = canonical
    if len(by_time) > 5500:
        raise DriverError("historical_cache_bound_exceeded")
    return [by_time[key] for key in sorted(by_time)]


def _normalize_equity_quotes(data: Mapping, symbols: list[str]) -> list[dict]:
    result = []
    records = _rows(data, "results")
    for record in records:
        if not isinstance(record, dict):
            continue
        row = record.get("quote", record)
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        price = _safe_float(row.get("last_trade_price"))
        updated = row.get("venue_last_trade_time")
        if symbol in symbols and price is not None and price > 0 and isinstance(updated, str):
            result.append({"symbol": symbol, "price": price, "updated_at": updated})
    return result


def _normalize_option_quotes(data: Mapping, metadata: dict[str, dict]) -> list[dict]:
    result = []
    for row in _rows(data, "results"):
        if not isinstance(row, dict) or not isinstance(row.get("quote"), dict):
            continue
        raw = row["quote"]
        identifier = raw.get("instrument_id")
        contract = metadata.get(identifier)
        if contract is None:
            continue
        bid = _safe_float(raw.get("bid_price"))
        ask = _safe_float(raw.get("ask_price"))
        updated = raw.get("updated_at")
        bid_size, ask_size = raw.get("bid_size"), raw.get("ask_size")
        if (bid is None or ask is None or not isinstance(updated, str) or
                isinstance(bid_size, bool) or not isinstance(bid_size, (int, float)) or
                isinstance(ask_size, bool) or not isinstance(ask_size, (int, float))):
            continue
        normalized = {**contract, "bid": bid, "ask": ask, "bid_size": bid_size,
                      "ask_size": ask_size, "updated_at": updated}
        if raw.get("delta") is not None:
            normalized["delta"] = raw["delta"]
        result.append(normalized)
    return result


class OneShotDriver:
    def __init__(self, state_dir: str | Path, config_dir: str | Path,
                 call: Callable[[str, dict], dict], clock: Callable[[], datetime] | None = None):
        self.state_dir = Path(state_dir)
        self.config_dir = Path(config_dir)
        self.call_fn = call
        self.clock = clock or (lambda: datetime.now(UTC))
        self.now = self._clock()
        self.local = self.now.astimezone(ZoneInfo("America/New_York"))
        self.session_day = self.local.date()
        self.call_count = 0
        self.source_counts = {}

    def _clock(self) -> datetime:
        value = self.clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise DriverError("clock_must_be_timezone_aware")
        return value.astimezone(UTC)

    def _refresh_clock(self) -> None:
        current = self._clock()
        local = current.astimezone(ZoneInfo("America/New_York"))
        if local.date() != self.session_day:
            raise DriverError("clock_crossed_session_day")
        self.now = current
        self.local = local

    def _reserve_call(self, tool: str, arguments: dict) -> tuple[Path, int, str]:
        budget_dir = self.state_dir / "call-budget"
        budget_dir.mkdir(parents=True, exist_ok=True)
        path = budget_dir / (self.session_day.isoformat() + ".json")
        lock = _file_lock(budget_dir / (self.session_day.isoformat() + ".lock"))
        try:
            doc = json.loads(path.read_text()) if path.exists() else {"date": self.session_day.isoformat(), "count": 0, "calls": []}
            count = int(doc.get("count", 0))
            if count >= MAX_DAILY_CALLS:
                raise DriverError("daily_data_call_budget_exhausted")
            requested = _iso(self._clock())
            count += 1
            doc["count"] = count
            doc["calls"].append({"sequence": count, "tool": tool, "requested_at": requested,
                                 "arguments_sha256": hashlib.sha256(_json_bytes(arguments)).hexdigest(), "outcome": "reserved"})
            _atomic_json(path, doc)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()
        self.call_count += 1
        return path, count, requested

    def _record_outcome(self, budget_path: Path, sequence: int, outcome: str) -> None:
        lock = _file_lock(budget_path.with_suffix(".lock"))
        try:
            doc = json.loads(budget_path.read_text())
            doc["calls"][sequence - 1]["outcome"] = outcome
            _atomic_json(budget_path, doc)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()

    def data_call(self, tool: str, arguments: dict) -> dict:
        if tool not in {"search", "get_equity_historicals", "get_equity_quotes", "get_option_chains", "get_option_instruments", "get_option_quotes"}:
            raise DriverError("read_only_tool_refused")
        if self.call_count >= MAX_SESSION_CALLS:
            raise DriverError("session_data_call_budget_exhausted")
        budget_path, sequence, requested = self._reserve_call(tool, arguments)
        self.source_counts[tool] = self.source_counts.get(tool, 0) + 1
        try:
            data = self.call_fn(tool, arguments)
            if not isinstance(data, dict):
                raise DriverError("malformed_market_data")
            self._refresh_clock()
            received = _iso(self.now)
            raw = {"schema_version": 1, "tool": tool, "arguments": arguments,
                   "requested_at": requested, "received_at": received, "data": data}
            name = "%s-%04d-%s.json" % (self.session_day.strftime("%Y%m%d"), sequence, tool)
            _immutable_json(self.state_dir / "raw" / self.session_day.isoformat() / name, raw)
            self._record_outcome(budget_path, sequence, "success")
            return data
        except Exception:
            try:
                self._refresh_clock()
            except DriverError:
                pass
            self._record_outcome(budget_path, sequence, "failed")
            raise

    def _configs(self) -> dict[str, dict]:
        configs = {}
        for path in (self.config_dir / 'config-A.json', self.config_dir / 'config-B.json'):
            config = core.load_config(path)
            length = config["stochastic"]["length"]
            arm = "A" if length == 14 else "B" if length == 5 else None
            if arm is not None:
                if arm in configs:
                    raise DriverError("duplicate_arm_config")
                configs[arm] = config
        if set(configs) != {"A", "B"}:
            raise DriverError("missing_arm_config")
        symbols_a, symbols_b = configs["A"]["symbols"], configs["B"]["symbols"]
        if symbols_a != symbols_b or not symbols_a or len(symbols_a) > len(SYMBOLS) or any(symbol not in SYMBOLS for symbol in symbols_a):
            raise DriverError("invalid_arm_symbol_set")
        return configs

    def _state_path(self, arm: str) -> Path:
        return self.state_dir / "arms" / (arm + ".json")

    def _read_state(self, arm: str, config: dict) -> dict:
        path = self._state_path(arm)
        if not path.exists():
            return core.new_state(config)
        state = json.loads(path.read_text())
        if state.get("config_fingerprint") != core.digest(config):
            raise DriverError("arm_config_fingerprint_mismatch")
        return state

    def _write_status(self, value: dict) -> dict:
        _atomic_json(self.state_dir / "current.json", value)
        return value

    def _history_args(self, symbols: list[str], start_time: datetime, end_time: datetime) -> dict:
        return {"symbols": symbols, "start_time": _iso(start_time), "end_time": _iso(end_time),
                "interval": "5minute", "bounds": "regular", "adjustment_type": "split"}

    def _history_call(self, symbols: list[str], start_time: datetime, end_time: datetime) -> dict[str, list[dict]]:
        data = self.data_call("get_equity_historicals", self._history_args(symbols, start_time, end_time))
        normalized = _normalize_bars(data, symbols)
        complete = {symbol: [] for symbol in symbols}
        for symbol, rows in normalized.items():
            for row in rows:
                try:
                    if _parse(row["begins_at"]) + timedelta(minutes=5) <= end_time:
                        complete[symbol].append(row)
                except (KeyError, TypeError, ValueError):
                    continue
        return complete

    def _load_bars(self, symbols: list[str], allow_updates: bool = True) -> dict[str, list[dict]]:
        cache_dir = self.state_dir / "cache" / "bars"
        cache_dir.mkdir(parents=True, exist_ok=True)
        bars = {}
        for symbol in symbols:
            path = cache_dir / (symbol + ".json")
            try:
                saved = json.loads(path.read_text()) if path.exists() else {"symbol": symbol, "bars": []}
            except (ValueError, OSError):
                saved = {"symbol": symbol, "bars": []}
            bars[symbol] = list(saved.get("bars", [])) if saved.get("symbol") == symbol else []
        marker = self.state_dir / "cache" / "history-fetch" / (self.session_day.isoformat() + ".json")
        if not marker.exists():
            for start in range(0, len(symbols), HISTORY_BATCH):
                batch = symbols[start:start + HISTORY_BATCH]
                start_local = datetime.combine(self.session_day - timedelta(days=HISTORY_DAYS), time.min, ZoneInfo("America/New_York"))
                fetched = self._history_call(batch, start_local, self.now)
                for symbol in batch:
                    # A new session uses one fresh provider snapshot for its
                    # warm-up window. Providers can revise completed bars after
                    # yesterday's capture; merging across snapshots incorrectly
                    # treats those legitimate revisions as contradictory rows.
                    # Conflicting duplicates within this response still fail.
                    # Raw responses and prior decisions remain immutable.
                    bars[symbol] = _merge_bars([], fetched[symbol])
            for symbol in symbols:
                _atomic_json(cache_dir / (symbol + ".json"), {"symbol": symbol, "bars": bars[symbol], "asof": _iso(self.now)})
            _atomic_json(marker, {"date": self.session_day.isoformat(), "symbols": symbols,
                                  "fetched_at": _iso(self.now), "refresh_mode": "fresh_completed_snapshot"})
        if allow_updates and self.session_day.weekday() < 5 and time(9, 30) <= self.local.time().replace(tzinfo=None) < time(16):
            expected_start = self.local.replace(minute=(self.local.minute // 5) * 5, second=0, microsecond=0) - timedelta(minutes=5)
            expected_end = expected_start + timedelta(minutes=5)
            age = (self.local - expected_end).total_seconds()
            if 0 <= age <= 90:
                missing = []
                for symbol in symbols:
                    starts = set()
                    for row in bars[symbol]:
                        try:
                            starts.add(_parse(row["begins_at"]).astimezone(self.local.tzinfo).replace(second=0, microsecond=0))
                        except (KeyError, TypeError, ValueError):
                            continue
                    if expected_start not in starts:
                        missing.append(symbol)
                for start in range(0, len(missing), HISTORY_BATCH):
                    batch = missing[start:start + HISTORY_BATCH]
                    latest_starts = []
                    for symbol in batch:
                        cached_starts = [_parse(row["begins_at"]) for row in bars[symbol] if row.get("begins_at")]
                        if cached_starts:
                            latest_starts.append(max(cached_starts))
                    start_time = min(latest_starts) + timedelta(minutes=5) if latest_starts else datetime.combine(self.session_day - timedelta(days=HISTORY_DAYS), time.min, ZoneInfo("America/New_York"))
                    fetched = self._history_call(batch, start_time, self.now)
                    for symbol in batch:
                        bars[symbol] = _merge_bars(bars[symbol], fetched[symbol])
                        _atomic_json(cache_dir / (symbol + ".json"), {"symbol": symbol, "bars": bars[symbol], "asof": _iso(self.now)})
        return bars

    def _quote_call(self, identifiers: list[str], metadata: dict[str, dict]) -> list[dict]:
        output = []
        unique = list(dict.fromkeys(identifiers))
        for start in range(0, len(unique), QUOTE_BATCH):
            batch = unique[start:start + QUOTE_BATCH]
            data = self.data_call("get_option_quotes", {"instrument_ids": batch})
            output.extend(_normalize_option_quotes(data, metadata))
        return output

    def _load_catalog(self, symbol: str, chain: Mapping, expiry: str) -> list[dict]:
        cache_path = self.state_dir / "cache" / "catalog" / self.session_day.isoformat() / (symbol + "-" + expiry + ".json")
        if cache_path.exists():
            saved = json.loads(cache_path.read_text())
            if saved.get("symbol") == symbol and saved.get("expiry") == expiry and isinstance(saved.get("catalog"), list) and saved["catalog"]:
                return saved["catalog"]
        all_rows = []
        seen_ids = set()
        seen_contracts = set()
        pages = 0
        cursor = None
        seen_cursors = set()
        while True:
            if pages >= MAX_INSTRUMENT_PAGES:
                raise DriverError("instrument_page_bound_exceeded")
            arguments = {"chain_id": chain["id"], "expiration_dates": expiry, "state": "active"}
            if cursor is not None:
                arguments["cursor"] = cursor
            data = self.data_call("get_option_instruments", arguments)
            pages += 1
            instruments = _rows(data, "instruments")
            for item in instruments:
                if not isinstance(item, dict):
                    raise DriverError("malformed_instrument_page")
                right = item.get("type")
                if right not in ("call", "put"):
                    raise DriverError("invalid_contract_right")
                normalized = _normalize_instrument(item, symbol, expiry, right)
                ident = normalized["option_id"]
                key = (normalized["type"], normalized["strike"])
                if ident in seen_ids or key in seen_contracts:
                    raise DriverError("duplicate_standard_contract")
                seen_ids.add(ident)
                seen_contracts.add(key)
                all_rows.append(normalized)
                if len(all_rows) > MAX_CATALOG_CONTRACTS:
                    raise DriverError("catalog_contract_bound_exceeded")
            next_cursor = _cursor(data)
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise DriverError("repeated_instrument_cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        if not all_rows:
            raise DriverError("empty_instrument_catalog")
        _atomic_json(cache_path, {"symbol": symbol, "expiry": expiry, "chain_id": chain["id"],
                                  "catalog": all_rows, "cached_at": _iso(self.now)})
        return all_rows

    def _chain_and_catalog(self, symbol: str) -> tuple[list[dict], str]:
        chain_data = self.data_call("get_option_chains", {"underlying_symbol": symbol})
        matches = [item for item in _rows(chain_data, "chains") if isinstance(item, dict) and item.get("symbol") == symbol]
        if len(matches) != 1:
            raise DriverError("ambiguous_or_missing_chain")
        expected_id = _chain_underlying_id(matches[0])
        verified_id = self._verified_underlying_id(symbol, expected_id)
        chain, expiry = _chain_for(chain_data, symbol, self.session_day, verified_id)
        catalog = self._load_catalog(symbol, chain, expiry)
        return catalog, expiry

    def _verified_underlying_id(self, symbol: str, expected_id: str) -> str:
        cache_path = self.state_dir / "cache" / "underlying-identity" / self.session_day.isoformat() / (symbol + ".json")
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text())
            except (ValueError, OSError) as exc:
                raise DriverError("underlying_identity_cache_invalid") from exc
            if (cached.get("date") != self.session_day.isoformat() or cached.get("symbol") != symbol or
                    cached.get("instrument_id") != expected_id):
                raise DriverError("underlying_identity_cache_mismatch")
            return expected_id
        search = self.data_call("search", {"query": symbol, "asset_type": "instrument", "limit": 5})
        rows = _rows(search, "results")
        matching_id = [row for row in rows if isinstance(row, dict) and row.get("instrument_id") == expected_id]
        exact = [row for row in matching_id if row.get("symbol") == symbol]
        if len(matching_id) != 1 or len(exact) != 1:
            raise DriverError("underlying_identity_search_mismatch")
        _atomic_json(cache_path, {"date": self.session_day.isoformat(), "symbol": symbol,
                                  "instrument_id": expected_id, "verified_at": _iso(self.now)})
        return expected_id

    def _fresh_underlying(self, rows: list[dict], asof: datetime) -> dict[str, float]:
        result = {}
        for row in rows:
            try:
                updated = _parse(row["updated_at"])
                age = (asof - updated).total_seconds()
                price = _safe_float(row["price"])
                if price is not None and price > 0 and 0 <= age <= 15:
                    result[row["symbol"]] = price
            except (KeyError, TypeError, ValueError):
                continue
        return result

    def _held_first(self, configs: dict[str, dict], states: dict[str, dict]) -> dict[str, list[dict]]:
        positions = []
        metadata = {}
        for arm in ("A", "B"):
            for position in states[arm]["positions"].values():
                positions.append(position)
                metadata[position["option_id"]] = {"option_id": position["option_id"], "symbol": position["symbol"],
                    "expiry": position["expiry"], "type": position["type"], "strike": position["strike"], "multiplier": 100}
        held_quotes = self._quote_call([p["option_id"] for p in positions], metadata) if positions else []
        if positions:
            self._refresh_clock()
            for arm in ("A", "B"):
                if states[arm]["positions"]:
                    self._refresh_clock()
                    snapshot = {"asof": _iso(self.now), "signals": [], "catalog": [], "underlying_quotes": [], "quotes": held_quotes}
                    core.process_snapshot(snapshot, self._state_path(arm), configs[arm])
                    states[arm] = json.loads(self._state_path(arm).read_text())
        return {"quotes": held_quotes, "metadata": metadata}

    def _preflight(self, configs: dict[str, dict]) -> dict:
        symbols = ["SPY", "IWM"]
        start_local = datetime.combine(self.session_day - timedelta(days=HISTORY_DAYS), time.min, ZoneInfo("America/New_York"))
        histories = self._history_call(symbols, start_local, self.now)
        equity_data = self.data_call("get_equity_quotes", {"symbols": symbols})
        underlyings = _normalize_equity_quotes(equity_data, symbols)
        # Use the last observed print to choose diagnostic contracts even when the
        # market is closed; freshness still controls whether preflight is ready.
        spot_by_symbol = {row["symbol"]: row["price"] for row in underlyings}
        catalog, expiry = self._chain_and_catalog("IWM")
        spot = spot_by_symbol.get("IWM")
        sample_ids = []
        if spot is not None:
            for right in ("call", "put"):
                candidates = [item for item in catalog if item["type"] == right]
                if candidates:
                    atm = min(candidates, key=lambda item: (abs(item["strike"] - spot), item["strike"]))
                    otm = [item for item in candidates if item["strike"] > spot] if right == "call" else [item for item in candidates if item["strike"] < spot]
                    nearest = min(otm, key=lambda item: item["strike"]) if right == "call" and otm else max(otm, key=lambda item: item["strike"]) if otm else None
                    sample_ids.extend([atm["option_id"]])
                    if nearest:
                        sample_ids.append(nearest["option_id"])
        metadata = {item["option_id"]: item for item in catalog}
        sample_ids = list(dict.fromkeys(sample_ids))
        option_quotes = self._quote_call(sample_ids, metadata) if sample_ids else []
        fresh_underlyings = self._fresh_underlying(underlyings, self.now)
        stale = len(fresh_underlyings) != len(symbols) or len(option_quotes) != len(sample_ids) or any(
            (self.now - _parse(q["updated_at"])).total_seconds() < 0 or (self.now - _parse(q["updated_at"])).total_seconds() > 15 for q in option_quotes)
        history_counts = {symbol: len(histories.get(symbol, [])) for symbol in symbols}
        result = {"schema_version": 1, "mode": "preflight", "status": "stale_market_data" if stale else "ready",
                  "ready": not stale, "asof": _iso(self.now), "symbols": symbols,
                  "history_rows": history_counts, "equity_quote_count": len(underlyings),
                  "fresh_underlying_count": len(fresh_underlyings), "selected_expiry": expiry,
                  "catalog_contract_count": len(catalog), "option_quote_count": len(option_quotes),
                  "option_quotes_fresh": not stale, "data_calls": self.call_count,
                  "source_counts": dict(self.source_counts),
                  "orders_sent": 0, "paper_transitions": 0}
        return self._write_status(result)

    def _eligible_session(self, configs: dict[str, dict]) -> bool:
        return (self.session_day.weekday() < 5 and all(core.allowed_day(self.session_day, configs[arm]) for arm in ("A", "B")))

    def _active_positions(self, configs: dict[str, dict]) -> bool:
        return any(self._read_state(arm, configs[arm])["positions"] for arm in ("A", "B"))

    def scheduled_mode(self, preflight: bool = False) -> str:
        configs = self._configs()
        if preflight:
            return "preflight"
        if not self._eligible_session(configs):
            return "calendar_closed"
        clock = self.local.time().replace(tzinfo=None)
        if time(8, 45) <= clock < time(9, 30):
            return "prep" if not (self.state_dir / "cache" / "prep" / (self.session_day.isoformat() + ".json")).exists() else "outside_active_window"
        if time(9, 30) <= clock <= time(11, 5):
            return "runtime"
        if clock > time(11, 5) and self._active_positions(configs):
            return "exit_only"
        return "outside_active_window"

    def _scheduled_status(self, mode: str) -> dict:
        return {"schema_version": 1, "mode": "runtime", "status": mode,
            "asof": _iso(self.now), "data_calls": 0, "source_counts": {}, "orders_sent": 0,
            "paper_transitions": 0}

    def _prep(self, configs: dict[str, dict]) -> dict:
        symbols = configs["A"]["symbols"]
        self._load_bars(symbols, allow_updates=False)
        prepared = []
        for symbol in symbols:
            self._chain_and_catalog(symbol)
            prepared.append(symbol)
        marker = self.state_dir / "cache" / "prep" / (self.session_day.isoformat() + ".json")
        _atomic_json(marker, {"date": self.session_day.isoformat(), "symbols": prepared, "asof": _iso(self.now)})
        return self._write_status({"schema_version": 1, "mode": "prep", "status": "warmup_complete",
            "asof": _iso(self.now), "symbols": prepared, "data_calls": self.call_count,
            "source_counts": dict(self.source_counts), "orders_sent": 0, "paper_transitions": 0})

    def run_once(self, preflight: bool = False) -> dict:
        configs = self._configs()
        schedule = self.scheduled_mode(preflight)
        if schedule in ("calendar_closed", "outside_active_window"):
            return self._scheduled_status(schedule)
        lock = _file_lock(self.state_dir / "driver.lock")
        try:
            configs = self._configs()
            schedule = self.scheduled_mode(preflight)
            if schedule in ("calendar_closed", "outside_active_window"):
                return self._scheduled_status(schedule)
            if preflight:
                return self._preflight(configs)
            if schedule == "prep":
                return self._prep(configs)
            states = {arm: self._read_state(arm, configs[arm]) for arm in ("A", "B")}
            held = self._held_first(configs, states)
            if not self._eligible_session(configs):
                return self._write_status(self._runtime_status("calendar_closed", configs, states, held, [], []))
            if schedule == "exit_only":
                return self._write_status(self._runtime_status("exit_only", configs, states, held, [], {}))
            symbols = configs["A"]["symbols"]
            bars = self._load_bars(symbols)
            self._refresh_clock()
            market_open = time(9, 30) <= self.local.time().replace(tzinfo=None) < time(16)
            candidate_outputs = {}
            all_candidates = []
            if market_open and time(9, 35) <= self.local.time().replace(tzinfo=None) <= time(10, 50):
                document = {"results": [{"symbol": symbol, "bars": bars[symbol]} for symbol in symbols]}
                for arm in ("A", "B"):
                    output = core.signals(document, self.now, configs[arm])
                    candidate_outputs[arm] = output
                    all_candidates.extend(output["candidates"])
            self._refresh_clock()
            expected_boundary = self.local.replace(minute=(self.local.minute // 5) * 5, second=0, microsecond=0)
            candidates = [s for s in all_candidates
                          if _parse(s["signal_time"]).astimezone(self.local.tzinfo) == expected_boundary
                          and 0 <= (self.now - _parse(s["signal_time"])).total_seconds() <= 90]
            candidate_symbols = list(dict.fromkeys(s["symbol"] for s in candidates))
            underlying_quotes = []
            catalog_by_symbol = {}
            expiry_by_symbol = {}
            if candidate_symbols:
                equity_data = self.data_call("get_equity_quotes", {"symbols": candidate_symbols})
                underlying_quotes = _normalize_equity_quotes(equity_data, candidate_symbols)
                for symbol in candidate_symbols:
                    catalog, expiry = self._chain_and_catalog(symbol)
                    catalog_by_symbol[symbol] = catalog
                    expiry_by_symbol[symbol] = expiry
            self._refresh_clock()
            expected_boundary = self.local.replace(minute=(self.local.minute // 5) * 5, second=0, microsecond=0)
            candidates = [s for s in candidates if _parse(s["signal_time"]).astimezone(self.local.tzinfo) == expected_boundary
                          and 0 <= (self.now - _parse(s["signal_time"])).total_seconds() <= 90]
            underlying_prices = self._fresh_underlying(underlying_quotes, self.now)
            eligible_candidates = []
            candidate_metadata = {}
            for signal in candidates:
                symbol = signal["symbol"]
                spot = underlying_prices.get(symbol)
                catalog = catalog_by_symbol.get(symbol, [])
                if spot is None:
                    continue
                rows = [item for item in catalog if item["type"] == signal["direction"]]
                otm = [item for item in rows if item["strike"] > spot] if signal["direction"] == "call" else [item for item in rows if item["strike"] < spot]
                selected = min(otm, key=lambda item: item["strike"]) if signal["direction"] == "call" and otm else max(otm, key=lambda item: item["strike"]) if otm else None
                if selected is None:
                    continue
                signal["expiry"] = expiry_by_symbol[symbol]
                eligible_candidates.append(signal)
                candidate_metadata[selected["option_id"]] = selected
            # The exits were processed first. Entry quotes are fetched only for exact OTM contracts.
            entry_quotes = self._quote_call(list(candidate_metadata), candidate_metadata) if candidate_metadata else []
            held_metadata = held["metadata"]
            combined_metadata = {**held_metadata, **candidate_metadata}
            quotes = held["quotes"] + entry_quotes
            self._refresh_clock()
            expected_boundary = self.local.replace(minute=(self.local.minute // 5) * 5, second=0, microsecond=0)
            eligible_candidates = [s for s in eligible_candidates if _parse(s["signal_time"]).astimezone(self.local.tzinfo) == expected_boundary
                                   and 0 <= (self.now - _parse(s["signal_time"])).total_seconds() <= 90]
            snapshot = {"asof": _iso(self.now), "signals": eligible_candidates,
                        "catalog": [item for symbol in candidate_symbols for item in catalog_by_symbol.get(symbol, [])],
                        "underlying_quotes": underlying_quotes, "quotes": quotes}
            results = {}
            for arm in ("A", "B"):
                self._refresh_clock()
                snapshot["asof"] = _iso(self.now)
                results[arm] = core.process_snapshot(snapshot, self._state_path(arm), configs[arm])
                states[arm] = json.loads(self._state_path(arm).read_text())
            _immutable_json(self.state_dir / "signals" / (self.now.strftime("%Y%m%dT%H%M%S%fZ") + ".json"),
                            {"asof": _iso(self.now), "outputs": candidate_outputs, "selected_signals": eligible_candidates})
            return self._write_status(self._runtime_status("processed", configs, states, held, eligible_candidates, results))
        except DriverError as exc:
            return self._write_status({"schema_version": 1, "mode": "preflight" if preflight else "runtime",
                "status": "blocked", "reason_code": exc.code, "asof": _iso(self.now), "data_calls": self.call_count,
                "source_counts": dict(self.source_counts), "orders_sent": 0})
        except Exception as exc:
            # Do not persist exception text: provider/library errors may contain response data.
            return self._write_status({"schema_version": 1, "mode": "preflight" if preflight else "runtime",
                "status": "failed", "reason_code": type(exc).__name__, "asof": _iso(self.now),
                "data_calls": self.call_count, "source_counts": dict(self.source_counts), "orders_sent": 0})
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()

    def _runtime_status(self, code: str, configs: dict, states: dict, held: dict,
                        candidates: list, results: list | dict) -> dict:
        arms = {}
        for arm in ("A", "B"):
            cfg = configs[arm]
            state = states[arm]
            metrics = core.status(self._state_path(arm), cfg)
            arms[arm] = {"cash": state["cash"], "realized_pnl": state["realized_pnl"],
                         "positions": list(state["positions"].values()),
                         "open_position_count": len(state["positions"]),
                         "equity": metrics.get("equity"),
                         "status": results.get(arm, {}).get("status", "managed" if state["positions"] else "idle") if isinstance(results, dict) else "idle"}
        return {"schema_version": 1, "mode": "runtime", "status": code, "asof": _iso(self.now),
                "symbols": configs["A"]["symbols"], "candidate_count": len(candidates),
                "held_quote_count": len(held.get("quotes", [])), "data_calls": self.call_count,
                "source_counts": dict(self.source_counts),
                "arms": arms, "orders_sent": 0, "execution_mode": "paper"}


def _enter_cloud_session(session, *, monotonic=elapsed_time.monotonic, sleeper=elapsed_time.sleep):
    """Allow a brief shared-token lock handoff before reporting contention."""
    deadline = monotonic() + 5
    while True:
        try:
            session.__enter__()
            return session
        except BlockingIOError:
            if monotonic() >= deadline:
                raise DriverError("provider_session_busy") from None
            sleeper(0.25)


def _cloud_session(args):
    root = Path(args.transport_root)
    if (root / "neorome" / "robinhood_cloud_data.py").is_file():
        module_parent = root
    elif root.name == "neorome" and (root / "robinhood_cloud_data.py").is_file():
        module_parent = root.parent
    else:
        raise DriverError("missing_cloud_transport")
    if str(module_parent) not in sys.path:
        sys.path.insert(0, str(module_parent))
    from neorome import robinhood_cloud_data
    robinhood_cloud_data.READ_ONLY_TOOLS = frozenset(set(robinhood_cloud_data.READ_ONLY_TOOLS) | READ_ONLY_ADDITIONS)
    session = robinhood_cloud_data.CloudDataSession(args.tokens, max_calls=MAX_SESSION_CALLS)
    return _enter_cloud_session(session)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--tokens", default="/var/lib/neorome-options-auth/tokens.json")
    parser.add_argument("--transport-root", default="/opt/neorome")
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args(argv)
    session = None
    desk = None
    try:
        desk = OneShotDriver(args.state_dir, args.config_dir, lambda _tool, _arguments: {})
        schedule = desk.scheduled_mode(args.preflight)
        if schedule in ("calendar_closed", "outside_active_window"):
            result = desk._scheduled_status(schedule)
        else:
            session = _cloud_session(args)
            desk.call_fn = session.call
            result = desk.run_once(args.preflight)
    except Exception as exc:
        result = {"schema_version": 1, "mode": "preflight" if args.preflight else "runtime",
                  "status": "failed", "reason_code": exc.code if isinstance(exc, DriverError) else type(exc).__name__, "orders_sent": 0}
        Path(args.state_dir).mkdir(parents=True, exist_ok=True)
        _atomic_json(Path(args.state_dir) / "current.json", result)
    finally:
        if session is not None:
            session.__exit__(None, None, None)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") in ("processed", "ready", "stale_market_data", "calendar_closed",
                                           "outside_active_window", "warmup_complete", "exit_only") else 1


if __name__ == "__main__":
    raise SystemExit(main())
