"""Finite, read-only Robinhood evidence capture for the options shadow bridge.

The adapter captures one complete directional SPY/QQQ expiry slice and an
authoritative options positions/orders snapshot.  It is intentionally upstream
of the selector, paper ledger, risk reservation, and every broker write method.
The injected ``call`` function makes the collection deterministic in tests and
keeps credentials inside the existing OAuth transport.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
import queue
import re
import subprocess
import threading
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlparse


UTC = timezone.utc
PROVIDER = "robinhood-trading"
ARTIFACT_KIND = "options-cloud-evidence-v1"
MAX_AGE = timedelta(seconds=60)
MAX_PAGES = 20
MAX_CONTRACTS = 100
MAX_CATALOG_CONTRACTS = 500
MAX_QUOTE_BATCH = 20
# search + chain + instrument pages + quote batches + accounts + each paged
# positions/orders stream. This is a finite read-only ceiling, not a retry loop.
CLOUD_DATA_CALL_BUDGET = 2 + MAX_PAGES + ((MAX_CATALOG_CONTRACTS + MAX_QUOTE_BATCH - 1) // MAX_QUOTE_BATCH) + 1 + (2 * MAX_PAGES)
ALLOWED_READ_TOOLS = frozenset({
    "search", "get_accounts", "get_option_chains", "get_option_instruments",
    "get_option_quotes", "get_option_positions", "get_option_orders",
})
_SECRET = re.compile(r"(?i)(token|secret|password|authorization|bearer)[^,; ]*|\b\d{8,}\b")


class CloudEvidenceError(ValueError):
    """A provider response failed a fail-closed evidence gate."""


class ExpectedEvidenceAbstention(CloudEvidenceError):
    """Complete read-only evidence cannot form a paper candidate this cycle."""


class IncompleteEvidenceAbstention(ExpectedEvidenceAbstention):
    """A bounded read-only page sequence cannot establish a complete snapshot."""


class CloudEvidenceArtifactConflict(RuntimeError):
    """The requested output path already contains different decision bytes."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise CloudEvidenceError("evidence is not canonical JSON") from exc


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CloudEvidenceError(f"{field} must be ISO-8601") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CloudEvidenceError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CloudEvidenceError(f"{field} must be finite") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise CloudEvidenceError(f"{field} must be finite")
    return parsed


def _cursor(next_url: Any) -> str | None:
    if next_url in (None, ""):
        return None
    if not isinstance(next_url, str):
        raise CloudEvidenceError("provider pagination cursor is not a URL")
    value = parse_qs(urlparse(next_url).query).get("cursor", [None])[0]
    if not value:
        raise IncompleteEvidenceAbstention("provider pagination URL has no cursor")
    return value


def _safe_error(exc: BaseException) -> str:
    # Tool failures are useful evidence, but account identifiers and credentials
    # must never be copied into an artifact or stdout.
    message = str(exc).strip() or exc.__class__.__name__
    return _SECRET.sub("[redacted]", message)[:500]


def _result_data(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CloudEvidenceError("provider response data must be an object")
    return value


class ReadOnlyCloudSession:
    """Ephemeral app-server transport with a strict read-only tool allowlist."""

    def __enter__(self) -> "ReadOnlyCloudSession":
        self.messages = queue.Queue()
        self.counter = 0
        self.process = subprocess.Popen(
            ["codex", "app-server", "--stdio", "-c",
             'mcp_servers={robinhood-trading={url="https://agent.robinhood.com/mcp/trading"}}'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1,
        )

        def reader() -> None:
            for line in self.process.stdout:
                try:
                    self.messages.put(json.loads(line))
                except ValueError:
                    continue
            self.messages.put(None)

        threading.Thread(target=reader, daemon=True).start()
        try:
            self.request("initialize", {"clientInfo": {"name": "neorome-options-cloud-evidence", "version": "1"},
                                         "capabilities": {"experimentalApi": True}})
            self.thread = self.request("thread/start", {"ephemeral": True})["thread"]["id"]
            return self
        except Exception:
            self.__exit__(None, None, None)
            raise

    def request(self, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        self.counter += 1
        self.process.stdin.write(json.dumps({"id": self.counter, "method": method, "params": params}) + "\n")
        self.process.stdin.flush()
        while True:
            message = self.messages.get(timeout=45)
            if message is None:
                raise RuntimeError("Robinhood transport closed")
            if message.get("id") == self.counter:
                if "error" in message:
                    raise RuntimeError("Robinhood request rejected")
                return message["result"]

    def call(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if tool not in ALLOWED_READ_TOOLS:
            raise ValueError(f"read-only adapter refuses tool: {tool}")
        result = self.request("mcpServer/tool/call", {"threadId": self.thread, "server": PROVIDER,
                                                       "tool": tool, "arguments": dict(arguments)})
        if result.get("isError"):
            raise RuntimeError("Robinhood read-only data tool failed")
        body = result.get("structuredContent")
        if not isinstance(body, Mapping):
            texts = [item["text"] for item in result.get("content", []) if item.get("type") == "text"]
            if len(texts) != 1:
                raise CloudEvidenceError("unexpected Robinhood tool response")
            body = json.loads(texts[0])
        return _result_data(body.get("data"))

    def __exit__(self, *_args: Any) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.process.stdin.close()
        self.process.stdout.close()


def _record(call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]], tool: str,
            arguments: Mapping[str, Any], *, clock: Callable[[], datetime], records: list[dict[str, Any]]) -> Mapping[str, Any]:
    requested = clock()
    data = _result_data(call(tool, arguments))
    received = clock()
    if received < requested:
        raise CloudEvidenceError(f"{tool} local receipt chronology is invalid")
    safe_arguments = dict(arguments)
    if "account_number" in safe_arguments:
        safe_arguments["account_number"] = "[redacted]"
    records.append({"tool": tool, "arguments": safe_arguments, "requested_at": _iso(requested),
                    "received_at": _iso(received), "data_sha256": _sha(data)})
    return data


def _underlying_identifier(value: Any) -> str:
    """Extract the opaque returned identifier; the URL itself is never trusted."""
    if not isinstance(value, str):
        raise CloudEvidenceError("underlying contract identity is incomplete")
    parsed = urlparse(value)
    identifier = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if not identifier or parsed.query or not re.fullmatch(r"[A-Za-z0-9-]{1,128}", identifier):
        raise CloudEvidenceError("underlying contract identity is incomplete")
    return identifier


def _select_chain(data: Mapping[str, Any], symbol: str, underlying_id: str, as_of: datetime) -> tuple[Mapping[str, Any], date]:
    if data.get("next") not in (None, ""):
        # Chain pagination is still outside this bounded capture, but a
        # provider URL without a cursor is specifically an incomplete page
        # sequence and must be retained as a safe successful abstention.
        _cursor(data.get("next"))
        raise CloudEvidenceError("Robinhood chain catalog returned pagination; chain pagination is unsupported")
    chains = data.get("chains")
    if not isinstance(chains, list):
        raise CloudEvidenceError("Robinhood chain catalog is incomplete")
    eligible: list[tuple[date, Mapping[str, Any]]] = []
    for chain in chains:
        if not isinstance(chain, Mapping) or chain.get("symbol") != symbol:
            continue
        expirations = chain.get("expiration_dates")
        if not isinstance(expirations, list):
            raise CloudEvidenceError("Robinhood chain expiration catalog is incomplete")
        for item in expirations:
            try:
                expiry = date.fromisoformat(item)
            except (TypeError, ValueError) as exc:
                raise CloudEvidenceError("Robinhood chain expiration is malformed") from exc
            dte = (expiry - as_of.date()).days
            if 30 <= dte <= 60:
                eligible.append((expiry, chain))
    if not eligible:
        raise CloudEvidenceError("no SPY/QQQ expiry is between 30 and 60 days")
    # The nearest-to-45 expiry is the same deterministic rule used by OPT-001.
    expiry, chain = min(eligible, key=lambda item: (abs((item[0] - as_of.date()).days - 45), item[0], str(item[1].get("id"))))
    if chain.get("can_open_position") is not True or chain.get("settle_on_open") is not False:
        raise CloudEvidenceError("option chain is not an eligible standard PM chain")
    if _decimal(chain.get("trade_value_multiplier"), "chain multiplier") != 100:
        raise CloudEvidenceError("option chain multiplier is not the standard 100")
    if chain.get("cash_component") not in (None, "0", "0.0", "0.0000", 0):
        raise CloudEvidenceError("adjusted option chain is unsupported")
    underlyings = chain.get("underlying_instruments")
    if not isinstance(underlyings, list) or len(underlyings) != 1 or not isinstance(underlyings[0], Mapping):
        raise CloudEvidenceError("underlying contract identity is incomplete")
    declared = underlyings[0].get("symbol")
    # Provider blank symbols are acceptable only after the returned opaque ID
    # joins exactly to the unique SPY/QQQ search identity. Never compare URLs.
    if declared not in ("", symbol) or _underlying_identifier(underlyings[0].get("instrument")) != underlying_id:
        raise CloudEvidenceError("underlying contract identity is incomplete")
    return chain, expiry


def _instrument_pages(call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]], chain_id: str,
                      expiry: date, right: str, *, clock: Callable[[], datetime], records: list[dict[str, Any]]) -> tuple[list[Mapping[str, Any]], int]:
    rows: list[Mapping[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    seen_cursors: set[str] = set()
    for page in range(1, MAX_PAGES + 1):
        args: dict[str, Any] = {"chain_id": chain_id, "expiration_dates": expiry.isoformat(), "type": right,
                                "state": "active", "tradability": "tradable"}
        if cursor is not None:
            args["cursor"] = cursor
        data = _record(call, "get_option_instruments", args, clock=clock, records=records)
        instruments = data.get("instruments")
        if not isinstance(instruments, list):
            raise CloudEvidenceError("Robinhood instrument page is incomplete")
        for item in instruments:
            if not isinstance(item, Mapping) or not item.get("id"):
                raise CloudEvidenceError("Robinhood instrument identity is incomplete")
            if item["id"] in seen:
                raise CloudEvidenceError("Robinhood instrument pagination duplicated an identifier")
            seen.add(item["id"])
            rows.append(item)
        if len(rows) > MAX_CATALOG_CONTRACTS:
            raise CloudEvidenceError(f"directional option chain exceeds finite {MAX_CATALOG_CONTRACTS}-contract capture bound")
        next_cursor = _cursor(data.get("next"))
        if next_cursor is None:
            if not rows:
                raise CloudEvidenceError("Robinhood instrument catalog returned no contracts")
            return rows, page
        if next_cursor in seen_cursors:
            raise IncompleteEvidenceAbstention("Robinhood instrument pagination cursor repeated")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise CloudEvidenceError(f"Robinhood instrument pagination exceeded finite {MAX_PAGES}-page bound")


def _normalize_rows(instruments: list[Mapping[str, Any]], quotes: Mapping[str, Mapping[str, Any]], *,
                    symbol: str, expiry: date, as_of: datetime, receipt: datetime) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    result: list[dict[str, Any]] = []
    exclusions: list[dict[str, str]] = []
    for item in instruments:
        identifier = item["id"]
        if (item.get("chain_symbol") != symbol or item.get("expiration_date") != expiry.isoformat() or
            item.get("underlying_type") != "equity" or item.get("state") != "active" or
            item.get("tradability") != "tradable" or _decimal(item.get("trade_value_multiplier"), "multiplier") != 100):
            raise CloudEvidenceError("contract identity or standard multiplier is invalid")
        right = {"call": "C", "put": "P"}.get(item.get("type"))
        if right is None:
            raise CloudEvidenceError("contract right is invalid")
        strike = _decimal(item.get("strike_price"), "strike", positive=True)
        encoded = strike * 1000
        if encoded != encoded.to_integral_value() or not 0 < encoded <= 99999999:
            raise CloudEvidenceError("contract strike cannot form a canonical OCC identity")
        quote = quotes.get(identifier)
        if quote is None:
            raise CloudEvidenceError("quote set is incomplete for the instrument catalog")
        # Catalog completeness means every instrument has a matching quote
        # record. It does not mean every far OTM contract is executable.
        # Retain an identity/reason-only exclusion rather than inventing a
        # price or silently omitting the provider record.
        try:
            stamp = _timestamp(quote.get("updated_at"), "provider quote timestamp")
            if stamp > receipt or stamp > as_of or as_of - stamp > MAX_AGE:
                raise ValueError("stale_or_future")
            bid = _decimal(quote.get("bid_price"), "bid", positive=True)
            ask = _decimal(quote.get("ask_price"), "ask", positive=True)
            if bid > ask:
                raise ValueError("crossed")
            if type(quote.get("bid_size")) is not int or quote["bid_size"] < 1 or type(quote.get("ask_size")) is not int or quote["ask_size"] < 1:
                raise ValueError("zero_depth")
            delta = _decimal(quote.get("delta"), "delta")
            iv = _decimal(quote.get("implied_volatility"), "implied volatility", positive=True)
            if not -1 <= delta <= 1:
                raise ValueError("invalid_delta")
        except ValueError as exc:
            reason = str(exc)
            if reason not in {"stale_or_future", "crossed", "zero_depth", "invalid_delta"}:
                reason = "null_or_nonfinite_quote"
            exclusions.append({"instrument_id": str(identifier), "reason": reason})
            continue
        result.append({"symbol": f"{symbol}{expiry:%y%m%d}{right}{int(encoded):08d}", "underlying": symbol,
                       "expiry": expiry.isoformat(), "right": right, "strike": str(strike), "multiplier": 100,
                       "adjusted": False, "bid": str(bid), "ask": str(ask), "timestamp": quote["updated_at"],
                       "bid_size": quote["bid_size"], "ask_size": quote["ask_size"], "delta": str(delta),
                       "implied_volatility": str(iv), "instrument_id": identifier})
    result.sort(key=lambda row: (row["strike"], row["instrument_id"]))
    exclusions.sort(key=lambda row: row["instrument_id"])
    return result, exclusions


def _has_complete_directional_pair(rows: list[dict[str, Any]], right: str) -> bool:
    long = [row for row in rows if abs(abs(Decimal(row["delta"])) - Decimal("0.50")) <= Decimal("0.10")]
    short = [row for row in rows if abs(abs(Decimal(row["delta"])) - Decimal("0.25")) <= Decimal("0.10")]
    if right == "call":
        return any(Decimal(left["delta"]) > 0 and Decimal(other["delta"]) > 0 and Decimal(left["strike"]) < Decimal(other["strike"])
                   for left in long for other in short)
    return any(Decimal(left["delta"]) < 0 and Decimal(other["delta"]) < 0 and Decimal(left["strike"]) > Decimal(other["strike"])
               for left in long for other in short)


def _bounded_slice(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the selector's 100-row contract after complete catalog pagination.

    Quotes for every paginated instrument are validated first.  The retained
    rows prioritize both registered delta bands, then fill remaining capacity
    by nearest distance to either target, with stable strike/UUID tie-breaks.
    """
    if len(rows) <= MAX_CONTRACTS:
        return rows
    targets = (Decimal("0.50"), Decimal("0.25"))
    in_band = [row for row in rows if any(abs(abs(Decimal(row["delta"])) - target) <= Decimal("0.10") for target in targets)]
    in_band.sort(key=lambda row: (min(abs(abs(Decimal(row["delta"])) - target) for target in targets), row["strike"], row["instrument_id"]))
    chosen = in_band[:MAX_CONTRACTS]
    if len(chosen) < MAX_CONTRACTS:
        selected = {row["instrument_id"] for row in chosen}
        remainder = [row for row in rows if row["instrument_id"] not in selected]
        remainder.sort(key=lambda row: (min(abs(abs(Decimal(row["delta"])) - target) for target in targets), row["strike"], row["instrument_id"]))
        chosen.extend(remainder[:MAX_CONTRACTS - len(chosen)])
    return sorted(chosen, key=lambda row: (row["strike"], row["instrument_id"]))


def _holdings(call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]], *, clock: Callable[[], datetime],
              records: list[dict[str, Any]], symbol_by_id: Mapping[str, str]) -> dict[str, Any]:
    accounts = _record(call, "get_accounts", {}, clock=clock, records=records)
    rows = accounts.get("accounts")
    if not isinstance(rows, list) or not rows:
        raise CloudEvidenceError("authoritative read-only account list is unavailable")
    selected = next((row for row in rows if isinstance(row, Mapping) and row.get("is_default")), None)
    selected = selected or next((row for row in rows if isinstance(row, Mapping)), None)
    account_number = selected.get("account_number") if isinstance(selected, Mapping) else None
    if not isinstance(account_number, str) or not account_number:
        raise CloudEvidenceError("authoritative read-only account identifier is unavailable")

    position_rows: list[Mapping[str, Any]] = []
    order_rows: list[Mapping[str, Any]] = []
    for tool, key, args in (("get_option_positions", "positions", {"account_number": account_number}),
                            ("get_option_orders", "orders", {"account_number": account_number})):
        cursor: str | None = None
        seen: set[str] = set()
        seen_cursors: set[str] = set()
        for _page in range(1, MAX_PAGES + 1):
            request = dict(args)
            if cursor is not None:
                request["cursor"] = cursor
            data = _record(call, tool, request, clock=clock, records=records)
            page_rows = data.get(key)
            if not isinstance(page_rows, list):
                raise CloudEvidenceError(f"authoritative {tool} snapshot is incomplete")
            for row in page_rows:
                if not isinstance(row, Mapping):
                    raise CloudEvidenceError(f"authoritative {tool} row is malformed")
                # Positions have no provider row ID in the observed schema;
                # cursor progression establishes page completeness. Orders do
                # have a unique provider ID and remain record-deduped.
                if key == "orders":
                    identifier = row.get("id")
                    if not isinstance(identifier, str) or not identifier:
                        raise CloudEvidenceError("authoritative order row identifier is missing")
                    if identifier in seen:
                        raise CloudEvidenceError(f"authoritative {tool} pagination duplicated an identifier")
                    seen.add(identifier)
            (position_rows if key == "positions" else order_rows).extend(page_rows)
            next_cursor = _cursor(data.get("next"))
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise IncompleteEvidenceAbstention(f"authoritative {tool} pagination cursor repeated")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            raise CloudEvidenceError(f"authoritative {tool} pagination exceeded finite {MAX_PAGES}-page bound")

    contract_quantities: dict[str, int] = {}
    active_option_ids: set[str] = set()
    pending_option_ids: set[str] = set()
    pending = False
    def whole_quantity(value: Any, field: str) -> Decimal:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise CloudEvidenceError(f"authoritative {field} is malformed") from exc
        if not parsed.is_finite() or parsed != parsed.to_integral_value():
            raise CloudEvidenceError(f"authoritative {field} is not a finite whole contract count")
        return parsed

    for row in position_rows:
        option_id = row.get("option_id")
        if not isinstance(option_id, str) or not option_id:
            raise CloudEvidenceError("authoritative position option_id is malformed")
        for field in ("instrument_id", "option_instrument_id"):
            if field in row and row.get(field) not in (None, option_id):
                raise CloudEvidenceError("authoritative position has conflicting option identity")
        quantity = whole_quantity(row.get("quantity"), "position quantity")
        has_pending = any(whole_quantity(row.get(field, "0"), field) != 0 for field in
                          ("pending_assignment_quantity", "pending_buy_quantity", "pending_exercise_quantity",
                           "pending_expiration_quantity", "pending_sell_quantity"))
        if has_pending:
            pending = True
            pending_option_ids.add(option_id)
        active = quantity != 0 or has_pending
        if active:
            if option_id in active_option_ids:
                raise CloudEvidenceError("authoritative position has duplicate active option identity")
            active_option_ids.add(option_id)
            contract_quantities[option_id] = int(quantity)
    option_holdings = {symbol_by_id.get(option_id, option_id): quantity for option_id, quantity in contract_quantities.items() if quantity != 0}
    open_states = {"queued", "confirmed", "partially_filled", "pending_cancelled"}
    open_orders = []
    for row in order_rows:
        state = row.get("state")
        if state in open_states or whole_quantity(row.get("pending_quantity", "0"), "order pending quantity") != 0:
            pending = True
            open_orders.append({key: row.get(key) for key in
                                ("id", "option_id", "instrument_id", "chain_symbol", "state", "quantity", "processed_quantity", "pending_quantity", "updated_at")})
    latest = clock()
    snapshot = {"complete": True, "source": PROVIDER, "timestamp": _iso(latest), "local_receipt_at": _iso(latest),
                "option_holdings": option_holdings, "pending_option_symbols": sorted(symbol_by_id.get(item, item) for item in pending_option_ids),
                "underlying_quantity": 0, "open_orders": open_orders,
                "open_orders_reconciled": True, "pagination_complete": True,
                "pending_reconciliation": pending}
    snapshot["sha256"] = _sha(snapshot)
    return snapshot


def _artifact(body: Mapping[str, Any], output_path: str | Path | None) -> dict[str, Any]:
    clean = deepcopy(dict(body))
    result = {**clean, "artifact_sha256": _sha(clean)}
    if output_path is None:
        return result
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(result, sort_keys=True, indent=2) + "\n").encode()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise CloudEvidenceArtifactConflict(f"existing artifact {path} differs from canonical bytes")
    else:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
    result["artifact_path"] = str(path)
    return result


def collect_cloud_evidence(call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]], *, symbol: str,
                           right: str, as_of: datetime | str | None = None,
                           output_path: str | Path | None = None,
                           clock: Callable[[], datetime] | None = None) -> dict[str, Any]:
    """Capture one finite, read-only SPY/QQQ call/put slice and account snapshot."""
    if symbol not in {"SPY", "QQQ"} or right not in {"call", "put"}:
        raise ValueError("only SPY/QQQ call or put slices are supported")
    clock = clock or (lambda: datetime.now(UTC))
    evaluation_now = _timestamp(as_of, "as_of") if as_of is not None else None
    selection_now = evaluation_now or _timestamp(clock(), "local clock")
    records: list[dict[str, Any]] = []
    holdings: Mapping[str, Any] | None = None
    try:
        search = _record(call, "search", {"query": symbol, "asset_type": "instrument", "limit": 5}, clock=clock, records=records)
        matches = [row for row in (search.get("results") or []) if isinstance(row, Mapping) and row.get("symbol") == symbol and isinstance(row.get("instrument_id"), str) and row["instrument_id"]]
        if len(matches) != 1:
            raise CloudEvidenceError("underlying security identity is not confirmed by search")
        chain_data = _record(call, "get_option_chains", {"underlying_symbol": symbol}, clock=clock, records=records)
        chain, expiry = _select_chain(chain_data, symbol, matches[0]["instrument_id"], selection_now)
        instruments, page_count = _instrument_pages(call, chain["id"], expiry, right, clock=clock, records=records)
        identifiers = [row["id"] for row in instruments]
        quote_data: dict[str, Mapping[str, Any]] = {}
        for start in range(0, len(identifiers), MAX_QUOTE_BATCH):
            batch = identifiers[start:start + MAX_QUOTE_BATCH]
            data = _record(call, "get_option_quotes", {"instrument_ids": batch}, clock=clock, records=records)
            results = data.get("results")
            if not isinstance(results, list):
                raise CloudEvidenceError("Robinhood quote response is incomplete")
            for item in results:
                if not isinstance(item, Mapping) or not isinstance(item.get("quote"), Mapping):
                    raise CloudEvidenceError("Robinhood quote response has a missing quote")
                quote = item["quote"]
                identifier = quote.get("instrument_id")
                if identifier not in batch or identifier in quote_data:
                    raise CloudEvidenceError("Robinhood quote response has duplicate or unexpected identity")
                quote_data[identifier] = quote
        receipt = _timestamp(records[-1]["received_at"], "local receipt")
        quote_now = evaluation_now or receipt
        catalog_rows, exclusions = _normalize_rows(instruments, quote_data, symbol=symbol, expiry=expiry, as_of=quote_now, receipt=receipt)
        if not _has_complete_directional_pair(catalog_rows, right):
            reasons = ",".join(sorted({item["reason"] for item in exclusions})) or "missing_delta_bands"
            raise ExpectedEvidenceAbstention(f"executable catalog cannot form a complete directional vertical: {reasons}")
        rows = _bounded_slice(catalog_rows)
        holdings = _holdings(call, clock=clock, records=records,
                             symbol_by_id={row["instrument_id"]: row["symbol"] for row in catalog_rows})
        if holdings["pending_reconciliation"]:
            raise ExpectedEvidenceAbstention("authoritative options holdings include pending activity; reconciliation is required")
        # Account pagination finishes after the quote receipt.  `as_of` must
        # encompass its authoritative timestamp too; quote freshness remains
        # bound above to the original quote receipt, not this later clock.
        output_now = max(
            moment for moment in (
                evaluation_now,
                _timestamp(records[-1]["received_at"], "local receipt"),
                _timestamp(holdings["timestamp"], "holdings timestamp"),
                _timestamp(holdings["local_receipt_at"], "holdings local receipt"),
            ) if moment is not None
        )
        body = {"schema_version": 1, "artifact_kind": ARTIFACT_KIND, "status": "captured",
                "reason_code": "evidence_captured", "reason": "Complete bounded option slice and authoritative account snapshot captured",
                "shadow_ready": True,
                "unrelated_holding_count": len(set(holdings["option_holdings"]).union(holdings["pending_option_symbols"])),
                "open_order_count": len(open_orders) if (open_orders := holdings["open_orders"]) else 0}
        body.update({"provider": PROVIDER, "symbol": symbol, "right": right, "expiry": expiry.isoformat(),
                     "local_receipt_at": _iso(receipt), "as_of": _iso(output_now), "chain_complete": True,
                     "size_units": "contracts",
                     "size_units_evidence": "Robinhood get_option_quotes schema defines bid_size and ask_size as total contracts at price",
                     "chain": {"id": chain["id"], "symbol": symbol, "expiry": expiry.isoformat(),
                               "available_expirations": list(chain["expiration_dates"]), "complete": True,
                               "pages": page_count, "next_page_token": None, "limit": MAX_CONTRACTS,
                               "catalog_contract_count": len(instruments), "executable_contract_count": len(catalog_rows), "bounded_slice_count": len(rows),
                               "bounded_slice_policy": "delta_0.50_or_0.25_then_nearest_target"},
                     "selector_contract": {"dte_min": 30, "dte_max": 60, "long_abs_delta_target": "0.50",
                                           "short_abs_delta_target": "0.25", "max_abs_delta_error": "0.10", "candidate_universe_limit": 100},
                     "normalized_quotes": rows, "catalog_quote_count": len(instruments), "catalog_executable_quote_count": len(catalog_rows),
                     "catalog_quotes_sha256": _sha(catalog_rows), "quote_exclusions": exclusions,
                     "quote_exclusions_sha256": _sha(exclusions), "quote_exclusion_counts": {reason: sum(1 for item in exclusions if item["reason"] == reason) for reason in sorted({item["reason"] for item in exclusions})}, "holdings": holdings, "responses": records,
                     "response_evidence_sha256": _sha(records), "read_only_tools_called": sorted({r["tool"] for r in records}),
                     "orders_sent": 0, "broker_write_called": False, "profitability_status": "not_evaluated"})
        return _artifact(body, output_path)
    except (CloudEvidenceError, KeyError, TypeError, ValueError, RuntimeError) as exc:
        incomplete = isinstance(exc, IncompleteEvidenceAbstention)
        body = {"schema_version": 1, "artifact_kind": ARTIFACT_KIND, "status": "abstain",
                "reason_code": "incomplete_pagination" if incomplete else ("holdings_reconciliation_required" if "reconciliation is required" in str(exc) else ("no_viable_pair" if isinstance(exc, ExpectedEvidenceAbstention) else "provider_or_evidence_failure")), "reason": _safe_error(exc),
                "provider": PROVIDER, "symbol": symbol, "right": right,
                "as_of": _iso(evaluation_now or _timestamp(clock(), "local clock")),
                "responses": records, "tool_limitations": [_safe_error(exc)], "shadow_ready": False,
                "incomplete": incomplete, "chain_complete": False,
                "orders_sent": 0, "broker_write_called": False, "profitability_status": "not_evaluated",
                "operational_failure": not isinstance(exc, ExpectedEvidenceAbstention)}
        if holdings is not None:
            body["holdings"] = holdings
        return _artifact(body, output_path)


run = collect_cloud_evidence


def collect_with_cloud_data(*, token_path: str | os.PathLike[str], symbol: str, right: str,
                            output_path: str | Path | None = None, as_of: datetime | str | None = None) -> dict[str, Any]:
    """Run the complete collector through the existing cloud OAuth transport.

    The transport has a strict data-tool allowlist.  This wrapper deliberately
    does not use the local ``codex app-server`` process, which is unsuitable
    for the token-isolated cloud service.
    """
    from .robinhood_cloud_data import CloudDataSession
    with CloudDataSession(token_path, max_calls=CLOUD_DATA_CALL_BUDGET) as session:
        result = collect_cloud_evidence(session.call, symbol=symbol, right=right,
                                        as_of=as_of, output_path=output_path)
    # The call ceiling is configuration, rather than capture evidence.  Do
    # not mutate the already content-addressed artifact after collection.
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", choices=("SPY", "QQQ"), required=True)
    parser.add_argument("--right", choices=("call", "put"))
    parser.add_argument("--trend-state-dir", help="derive the directional right from hash-bound frozen trend inputs")
    parser.add_argument("--output")
    parser.add_argument("--state-dir", help="save a dated capture.json and atomically advance current.json")
    parser.add_argument("--tokens", default="/var/lib/neorome-options-auth/tokens.json")
    parser.add_argument("--transport", choices=("cloud", "app-server"), default="cloud")
    args = parser.parse_args()
    if bool(args.output) == bool(args.state_dir):
        parser.error("provide exactly one of --output or --state-dir")
    if bool(args.right) == bool(args.trend_state_dir):
        parser.error("provide exactly one of --right or --trend-state-dir")
    if args.trend_state_dir:
        from .options_trend_inputs import current_direction_hint
        args.right = current_direction_hint(args.trend_state_dir)["right"]
    if args.transport == "cloud":
        result = collect_with_cloud_data(token_path=args.tokens, symbol=args.symbol, right=args.right,
                                         output_path=args.output)
    else:
        with ReadOnlyCloudSession() as session:
            result = collect_cloud_evidence(session.call, symbol=args.symbol, right=args.right, output_path=args.output)
    if args.state_dir:
        from .options_dated_artifacts import save_stage
        capture_date = _timestamp(result["as_of"], "capture as_of").date().isoformat()
        attempt = _timestamp(result["as_of"], "capture as_of").strftime("%Y%m%dT%H%M%S%fZ")
        saved = save_stage(args.state_dir, capture_date, f"capture-{attempt}.json", result,
                           current_name="capture.json" if result.get("status") == "captured" else None)
        result = {**result, "artifact_path": str(saved)}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "captured" or result.get("operational_failure") is False else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ALLOWED_READ_TOOLS", "ARTIFACT_KIND", "CloudEvidenceArtifactConflict", "CloudEvidenceError", "ExpectedEvidenceAbstention", "IncompleteEvidenceAbstention",
           "ReadOnlyCloudSession", "collect_cloud_evidence", "collect_with_cloud_data", "run"]
