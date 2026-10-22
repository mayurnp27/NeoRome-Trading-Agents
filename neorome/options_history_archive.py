"""Resumable, budgeted OPRA archive assembler for the options walk-forward.

The assembler requests point-in-time OPRA definitions first, chooses exact OCC
symbols for the registered 45-DTE vertical, estimates every request through
Databento metadata, then downloads only the targeted ``cbbo-1m`` slice. Raw
responses are content-addressed and session records are append-only. A crash
can resume from ``state.json`` without redownloading completed sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import os
import tempfile
from typing import Any, Callable, Mapping, Sequence

from .options_black_scholes import EnrichmentError, UnderlyingInputs, enrich_quote
from .options_databento import DATASET, DEFAULT_SCHEMA, DatabentoOptionsError, _rows


UTC = timezone.utc
DEFINITION_SCHEMA = "definition"
ENTRY_END = time(20, 0)
LOOKBACK = 200
MIN_ARCHIVE_SESSIONS = 260
TARGET_DTE = 45
MIN_DTE = 30
MAX_DTE = 60
MIN_MONEYNESS = Decimal("0.85")
MAX_MONEYNESS = Decimal("1.15")


class ArchiveError(RuntimeError):
    """The archive cannot safely advance."""


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except Exception:
            pass
    return value


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise ArchiveError("provider response is not canonical JSON") from exc


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise ArchiveError("definition expiration is not a date")


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ArchiveError("provider timestamp is missing")
    if parsed.tzinfo is None:
        raise ArchiveError("provider timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _contract_symbol(row: Mapping[str, Any]) -> str:
    value = row.get("raw_symbol") or row.get("symbol")
    if not isinstance(value, str) or not value.strip():
        raise ArchiveError("definition is missing raw_symbol")
    # Databento expects the exact space-padded 21-character OCC symbol when
    # stype_in=raw_symbol. Keep that provider spelling separately from the
    # compact identity used by the walk-forward validator.
    return value


def _right(row: Mapping[str, Any], symbol: str) -> str:
    value = str(row.get("instrument_class", "")).strip().upper()
    if value in {"C", "CALL"}:
        return "C"
    if value in {"P", "PUT"}:
        return "P"
    if len(symbol) >= 15 and symbol[6] in {"C", "P"}:
        return symbol[6]
    raise ArchiveError("definition is missing option right")


def _strike(row: Mapping[str, Any], symbol: str) -> Decimal:
    value = row.get("strike_price")
    if value is None and len(symbol) >= 21:
        value = Decimal(symbol[-8:]) / Decimal(1000)
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ArchiveError("definition is missing strike") from exc
    if not result.is_finite() or result <= 0:
        raise ArchiveError("definition strike is invalid")
    return result


def _raw_definition(row: Mapping[str, Any], *, underlying: str) -> dict[str, Any] | None:
    provider_symbol = _contract_symbol(row)
    symbol = provider_symbol.replace(" ", "").upper()
    if symbol == underlying or ".OPT" not in symbol and len(symbol) < 15:
        return None
    try:
        expiry = _parse_date(row.get("expiration"))
    except (ArchiveError, TypeError, ValueError):
        return None
    right = _right(row, symbol)
    if right not in {"C", "P"}:
        return None
    strike = _strike(row, symbol)
    return {"symbol": symbol, "provider_symbol": provider_symbol, "underlying": underlying, "expiry": expiry.isoformat(), "right": right, "strike": str(strike), "definition_timestamp": _parse_ts(row.get("ts_event") or row.get("ts_recv")).isoformat().replace("+00:00", "Z")}


def candidate_contracts(definitions: Sequence[Mapping[str, Any]], *, session: date, underlying: str, spot: Decimal, direction: str, max_per_right: int = 24) -> tuple[list[dict[str, Any]], list[str]]:
    """Return a bounded same-expiry candidate universe for quote enrichment."""
    if isinstance(max_per_right, bool) or not 2 <= max_per_right <= 24:
        raise ValueError("max_per_right must be between 2 and 24")
    if spot <= 0:
        raise ValueError("spot must be positive")
    contracts = [item for raw in definitions if (item := _raw_definition(raw, underlying=underlying)) is not None]
    expiries = sorted({item["expiry"] for item in contracts})
    candidates = [item for item in contracts if MIN_DTE <= (_parse_date(item["expiry"]) - session).days <= MAX_DTE]
    if not candidates:
        raise ArchiveError("no 30-60 DTE option definitions for session")
    expiry = min({item["expiry"] for item in candidates}, key=lambda value: (abs((_parse_date(value) - session).days - TARGET_DTE), value))
    same = [item for item in candidates if item["expiry"] == expiry]
    if direction not in {"up", "down"}:
        raise ArchiveError("direction must be up or down")
    right = "C" if direction == "up" else "P"
    right_contracts = [item for item in same if item["right"] == right]
    in_range = [
        item for item in right_contracts
        if MIN_MONEYNESS <= Decimal(item["strike"]) / spot <= MAX_MONEYNESS
    ]
    if len(in_range) < 2:
        raise ArchiveError(f"no two {right} contracts in selected expiry")
    # Preserve the nearest-to-ATM contract, then pick one representative per
    # evenly spaced moneyness stratum. This reaches typical 0.25-delta legs
    # without turning a quote request into a full-chain download.
    atm = min(in_range, key=lambda item: (abs(Decimal(item["strike"]) - spot), Decimal(item["strike"]), item["symbol"]))
    chosen = {atm["symbol"]: atm}
    strata = max_per_right - 1
    span = MAX_MONEYNESS - MIN_MONEYNESS
    for index in range(strata):
        target = MIN_MONEYNESS + span * Decimal(2 * index + 1) / Decimal(2 * strata)
        representative = min(
            in_range,
            key=lambda item: (
                abs(Decimal(item["strike"]) / spot - target),
                abs(Decimal(item["strike"]) - spot),
                item["symbol"],
            ),
        )
        chosen[representative["symbol"]] = representative
    selected = sorted(chosen.values(), key=lambda item: (Decimal(item["strike"]), item["symbol"]))
    return selected[:max_per_right], expiries


def choose_vertical(definitions: Sequence[Mapping[str, Any]], *, session: date, underlying: str, spot: Decimal, direction: str) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Compatibility helper selecting the nearest adjacent vertical."""
    candidates, expiries = candidate_contracts(definitions, session=session, underlying=underlying, spot=spot, direction=direction)
    if direction == "up":
        long = min(candidates, key=lambda item: (abs(Decimal(item["strike"]) - spot), Decimal(item["strike"])))
        short = min((item for item in candidates if Decimal(item["strike"]) > Decimal(long["strike"])), key=lambda item: Decimal(item["strike"]))
    else:
        long = min(candidates, key=lambda item: (abs(Decimal(item["strike"]) - spot), -Decimal(item["strike"])))
        short = max((item for item in candidates if Decimal(item["strike"]) < Decimal(long["strike"])), key=lambda item: Decimal(item["strike"]))
    return long, short, expiries


def select_vertical_from_quotes(candidates: Sequence[Mapping[str, Any]], enriched: Mapping[str, Mapping[str, Any]], *, direction: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Pick the same-expiry pair closest to the registered 0.50/0.25 deltas."""
    right = "C" if direction == "up" else "P"
    target_long, target_short = Decimal("0.50"), Decimal("0.25")
    options = []
    for long in candidates:
        if long["right"] != right or long["symbol"] not in enriched:
            continue
        for short in candidates:
            if short["right"] != right or short["symbol"] not in enriched or short["expiry"] != long["expiry"]:
                continue
            if direction == "up" and not Decimal(long["strike"]) < Decimal(short["strike"]):
                continue
            if direction == "down" and not Decimal(long["strike"]) > Decimal(short["strike"]):
                continue
            long_quote, short_quote = enriched[long["symbol"]], enriched[short["symbol"]]
            long_error = abs(abs(Decimal(str(long_quote["delta"]))) - target_long)
            short_error = abs(abs(Decimal(str(short_quote["delta"]))) - target_short)
            # A debit call spread buys the lower strike and sells the higher
            # strike; a debit put spread buys the higher strike and sells the
            # lower strike.  Keep the width positive in both orientations.
            width = Decimal(short["strike"]) - Decimal(long["strike"]) if direction == "up" else Decimal(long["strike"]) - Decimal(short["strike"])
            debit = Decimal(str(long_quote["ask"])) - Decimal(str(short_quote["bid"]))
            if width <= 0 or debit <= 0 or debit >= width:
                continue
            options.append((long_error + short_error, long_error, short_error, long, short, long_quote, short_quote))
    if not options:
        raise ArchiveError("candidate quotes do not contain a valid 0.50/0.25 delta vertical")
    _, _, _, long, short, long_quote, short_quote = min(options, key=lambda item: item[:3] + (item[3]["symbol"], item[4]["symbol"]))
    return dict(long), dict(short), dict(long_quote), dict(short_quote)


@dataclass(frozen=True)
class ArchiveBudget:
    max_request_cost: Decimal = Decimal("0.25")
    max_total_cost: Decimal = Decimal("10")
    max_records: int = 250_000

    def __post_init__(self) -> None:
        for name, value in (("max_request_cost", self.max_request_cost), ("max_total_cost", self.max_total_cost)):
            decimal = Decimal(str(value))
            if not decimal.is_finite() or decimal <= 0:
                raise ValueError(f"{name} must be positive")
            object.__setattr__(self, name, decimal)
        if isinstance(self.max_records, bool) or not 1 <= self.max_records <= 2_000_000:
            raise ValueError("max_records is outside the bounded limit")


@dataclass(frozen=True)
class ArchiveConfig:
    root: Path
    underlying: str = "SPY"
    dataset: str = DATASET
    quote_schema: str = DEFAULT_SCHEMA
    stype_in: str = "raw_symbol"
    budget: ArchiveBudget = ArchiveBudget()
    max_underlying_quote_age: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.dataset != DATASET or self.quote_schema != DEFAULT_SCHEMA or self.stype_in != "raw_symbol":
            raise ValueError("archive is restricted to OPRA.PILLAR cbbo-1m raw-symbol requests")
        if not self.underlying.isalpha() or not self.underlying.isupper() or len(self.underlying) > 6:
            raise ValueError("underlying must be a short uppercase symbol")
        if self.max_underlying_quote_age <= timedelta(0):
            raise ValueError("max_underlying_quote_age must be positive")


class DatabentoArchiveProvider:
    """Minimal provider wrapper used by the resumable assembler."""

    def __init__(self, client: Any, *, dataset: str = DATASET):
        self.client = client
        self.dataset = dataset

    def estimate(self, *, symbols: Sequence[str] | str, start: Any, end: Any, schema: str, stype_in: str) -> tuple[Decimal, int]:
        metadata = self.client.metadata
        params = {"dataset": self.dataset, "start": start, "end": end, "symbols": symbols, "schema": schema, "stype_in": stype_in}
        cost = metadata.get_cost(**params)
        size = metadata.get_billable_size(**params)
        cost_d, size_i = Decimal(str(cost)), int(size)
        if not cost_d.is_finite() or cost_d < 0 or size_i < 0:
            raise ArchiveError("provider returned invalid cost estimate")
        return cost_d, size_i

    def fetch(self, *, symbols: Sequence[str] | str, start: Any, end: Any, schema: str, stype_in: str, limit: int) -> list[Mapping[str, Any]]:
        value = self.client.timeseries.get_range(dataset=self.dataset, symbols=symbols, schema=schema, stype_in=stype_in, start=start, end=end, limit=limit)
        rows = _rows(value)
        if len(rows) > limit:
            raise ArchiveError("provider returned more records than requested")
        return rows


class ArchiveAssembler:
    """Build and resume a content-addressed walk-forward archive."""

    def __init__(self, provider: DatabentoArchiveProvider, config: ArchiveConfig):
        self.provider, self.config = provider, config
        self.root = config.root
        self.raw = self.root / "raw"
        self.state_path = self.root / "state.json"
        self.manifest_path = self.root / "manifest.jsonl"
        self.output_path = self.root / "options-validation.json"
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"schema": "options-history-archive-v1", "total_cost": "0", "sessions": {}, "active_pair": None}
        value = json.loads(self.state_path.read_text())
        if value.get("schema") != "options-history-archive-v1" or not isinstance(value.get("sessions"), dict):
            raise ArchiveError("archive state has an unsupported schema")
        return value

    def _save(self, state: Mapping[str, Any]) -> None:
        payload = _canonical(state)
        with tempfile.NamedTemporaryFile("wb", delete=False, dir=self.root) as handle:
            temp = Path(handle.name)
            handle.write(payload)
        os.replace(temp, self.state_path)

    def _artifact(self, kind: str, session: date, rows: Sequence[Mapping[str, Any]]) -> str:
        payload = {"kind": kind, "session_date": session.isoformat(), "rows": list(rows)}
        digest = _sha(payload)
        target = self.raw / f"{kind}-{session.isoformat()}-{digest}.json"
        if target.exists() and target.read_bytes() != _canonical(payload):
            raise ArchiveError("content-addressed raw artifact collision")
        if not target.exists():
            target.write_bytes(_canonical(payload))
        return digest

    def _estimate_then_fetch(self, *, symbols: Sequence[str] | str, start: Any, end: Any, schema: str, stype_in: str, state: dict[str, Any]) -> tuple[list[Mapping[str, Any]], Decimal, int]:
        cost, size = self.provider.estimate(symbols=symbols, start=start, end=end, schema=schema, stype_in=stype_in)
        if cost > self.config.budget.max_request_cost:
            raise ArchiveError(f"request estimate {cost} exceeds per-request ceiling")
        total = Decimal(str(state.get("total_cost", "0")))
        if total + cost > self.config.budget.max_total_cost:
            raise ArchiveError(f"request would exceed total cost ceiling {self.config.budget.max_total_cost}")
        # Reserve the estimated charge before any data request. This keeps a
        # failed or interrupted fetch from being retried past the total ceiling.
        state["total_cost"] = str(total + cost)
        self._save(state)
        rows = self.provider.fetch(symbols=symbols, start=start, end=end, schema=schema, stype_in=stype_in, limit=self.config.budget.max_records)
        return rows, cost, size

    @staticmethod
    def _latest(rows: Sequence[Mapping[str, Any]], symbols: Sequence[str], *, as_of: datetime) -> dict[str, Mapping[str, Any]]:
        wanted = {symbol.replace(" ", "").upper() for symbol in symbols}
        best: dict[str, tuple[datetime, Mapping[str, Any]]] = {}
        for row in rows:
            raw = row.get("symbol") or row.get("raw_symbol")
            if not isinstance(raw, str) or raw.replace(" ", "").upper() not in wanted:
                continue
            # CBBO records are interval snapshots; use capture time rather
            # than ts_event, which may be undefined or trade-related.
            timestamp = _parse_ts(row.get("ts_recv"))
            if timestamp > as_of:
                continue
            key = raw.replace(" ", "").upper()
            if key not in best or timestamp > best[key][0]:
                best[key] = (timestamp, row)
        return {key: row for key, (_, row) in best.items()}

    def _quote(self, contract: Mapping[str, Any], raw: Mapping[str, Any], *, inputs: UnderlyingInputs) -> dict[str, Any]:
        from .options_databento import validate_quote_record
        validate_quote_record(raw)
        quote_time = _parse_ts(raw.get("ts_recv"))
        age = quote_time - inputs.timestamp
        if age < timedelta(0) or age > self.config.max_underlying_quote_age:
            raise ArchiveError("underlying input and quote timestamp exceed the configured age bound")
        try:
            enriched = enrich_quote(bid=raw["bid_px_00"], ask=raw["ask_px_00"], contract=contract, quote_timestamp=quote_time, inputs=inputs)
        except EnrichmentError as exc:
            raise ArchiveError(str(exc)) from exc
        return {"symbol": contract["symbol"], "underlying": contract["underlying"], "expiry": contract["expiry"], "right": contract["right"], "strike": contract["strike"], "bid": str(raw["bid_px_00"]), "ask": str(raw["ask_px_00"]), "bid_size": raw["bid_sz_00"], "ask_size": raw["ask_sz_00"], "delta": enriched["delta"], "implied_volatility": enriched["implied_volatility"], "timestamp": quote_time.isoformat().replace("+00:00", "Z"), "source_kind": "historical_executable", "executable": True}

    def run(self, sessions: Sequence[date], underlying_inputs: Mapping[date | str, Mapping[str, Any]]) -> dict[str, Any]:
        ordered = sorted({_parse_date(item) for item in sessions})
        if len(ordered) < MIN_ARCHIVE_SESSIONS:
            raise ArchiveError(f"archive requires at least {MIN_ARCHIVE_SESSIONS} chronological sessions")
        state = self._load()
        rows_by_session: dict[str, dict[str, Any]] = {key: value["row"] for key, value in state["sessions"].items()}
        active = state.get("active_pair")
        for index, session in enumerate(ordered):
            key = session.isoformat()
            if key in state["sessions"]:
                continue
            raw_inputs = underlying_inputs.get(session) or underlying_inputs.get(key)
            if raw_inputs is None:
                raise ArchiveError(f"missing archived underlying/rate/dividend inputs for {key}")
            inputs = UnderlyingInputs.from_mapping(raw_inputs)
            close = Decimal(str(raw_inputs.get("close", inputs.spot)))
            row: dict[str, Any] = {"session_date": key, "signal_timestamp": inputs.timestamp.isoformat().replace("+00:00", "Z"), "close": str(close), "spot": str(inputs.spot)}
            request_cost = Decimal("0")
            definition_hash: str | None = None
            quote_hash: str | None = None
            if active is None and index >= LOOKBACK:
                prior = [Decimal(str(rows_by_session[item.isoformat()]["close"])) for item in ordered[max(0, index - LOOKBACK):index]]
                direction = "up" if close > sum(prior, Decimal(0)) / Decimal(len(prior)) else "down" if close < sum(prior, Decimal(0)) / Decimal(len(prior)) else "flat"
                if direction != "flat":
                    def_start, def_end = session.isoformat(), (session + timedelta(days=1)).isoformat()
                    definitions, cost, size = self._estimate_then_fetch(symbols=f"{self.config.underlying}.OPT", start=def_start, end=def_end, schema=DEFINITION_SCHEMA, stype_in="parent", state=state)
                    request_cost += cost
                    definition_hash = self._artifact("definitions", session, definitions)
                    if len(definitions) >= self.config.budget.max_records:
                        raise ArchiveError("definition response reached the record ceiling; chain completeness is unknown")
                    candidates, expiries = candidate_contracts(definitions, session=session, underlying=self.config.underlying, spot=inputs.spot, direction=direction)
                    candidate_symbols = [item["provider_symbol"] for item in candidates]
                    quote_rows, cost, size = self._estimate_then_fetch(symbols=candidate_symbols, start=session.isoformat(), end=(session + timedelta(days=1)).isoformat(), schema=self.config.quote_schema, stype_in=self.config.stype_in, state=state)
                    request_cost += cost
                    quote_hash = self._artifact("quotes", session, quote_rows)
                    as_of = datetime.combine(session, ENTRY_END, tzinfo=UTC)
                    latest = self._latest(quote_rows, candidate_symbols, as_of=as_of)
                    enriched_candidates: dict[str, dict[str, Any]] = {}
                    for contract in candidates:
                        raw = latest.get(contract["symbol"])
                        if raw is None:
                            continue
                        enriched_candidates[contract["symbol"]] = self._quote(contract, raw, inputs=inputs)
                    long, short, long_quote, short_quote = select_vertical_from_quotes(candidates, enriched_candidates, direction=direction)
                    active = {"direction": direction, "long": long, "short": short, "opened": key, "available_expirations": expiries}
                    row["chain_metadata"] = {"complete": True, "available_expirations": expiries}
                    row["entry_quotes"] = {"long": long_quote, "short": short_quote}
            elif active is not None:
                symbols = [active["long"]["provider_symbol"], active["short"]["provider_symbol"]]
                quote_rows, cost, size = self._estimate_then_fetch(symbols=symbols, start=session.isoformat(), end=(session + timedelta(days=1)).isoformat(), schema=self.config.quote_schema, stype_in=self.config.stype_in, state=state)
                request_cost += cost
                quote_hash = self._artifact("quotes", session, quote_rows)
                as_of = datetime.combine(session, ENTRY_END, tzinfo=UTC)
                latest = self._latest(quote_rows, symbols, as_of=as_of)
                quotes: dict[str, dict[str, Any]] = {}
                for leg in ("long", "short"):
                    contract = active[leg]
                    raw = latest.get(contract["symbol"])
                    if raw is None:
                        raise ArchiveError(f"missing targeted {leg} quote for {key}")
                    quotes[leg] = self._quote(contract, raw, inputs=inputs)
                row["chain_metadata"] = {"complete": True, "available_expirations": active["available_expirations"]}
                row["exit_quotes"] = quotes
                expiry = _parse_date(active["long"]["expiry"])
                if close < Decimal("0") or active["direction"] != ("up" if close >= sum([Decimal(str(rows_by_session[item.isoformat()]["close"])) for item in ordered[max(0, index - LOOKBACK):index]], Decimal(0)) / Decimal(max(1, min(LOOKBACK, index))) else "down") or (expiry - session).days <= 21:
                    active = None
            row_hash = _sha(row)
            state["sessions"][key] = {"row": row, "row_sha256": row_hash, "definition_sha256": definition_hash, "quote_sha256": quote_hash, "estimated_cost": str(request_cost)}
            rows_by_session[key] = row
            state["active_pair"] = active
            self._save(state)
            with self.manifest_path.open("ab") as handle:
                handle.write(_canonical({"session_date": key, "row_sha256": row_hash, "definition_sha256": definition_hash, "quote_sha256": quote_hash, "estimated_cost": str(request_cost), "total_cost": state["total_cost"]}) + b"\n")
        rows = [rows_by_session[item.isoformat()] for item in ordered if item.isoformat() in rows_by_session]
        complete = len(rows) == len(ordered)
        admission = {"source_kind": "historical_executable", "provider": "databento", "entitlement_confirmed": True, "quotes_complete": complete, "provider_timestamps": complete}
        archive = {"schema": "options-validation-evidence-v1", "data_admission": admission, "rows": rows, "archive_sha256": _sha(rows)}
        if complete:
            encoded = _canonical(archive)
            if self.output_path.exists() and self.output_path.read_bytes() != encoded:
                raise ArchiveError("existing archive output differs; refusing overwrite")
            if not self.output_path.exists():
                self.output_path.write_bytes(encoded)
        return {"status": "complete" if complete else "incomplete", "sessions": len(rows), "total_cost": state["total_cost"], "output": str(self.output_path) if complete else None, "admission": admission}
