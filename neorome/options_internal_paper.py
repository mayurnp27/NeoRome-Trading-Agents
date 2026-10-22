"""Finite, network-free internal-paper runner for observed options evidence.

The runner is intentionally downstream of the read-only Robinhood collector.
It reads retained JSON only; it never loads a credential, creates a transport,
or calls a broker order method.  A selector candidate can reserve cash, but a
modeled internal fill requires a *later* complete cloud-evidence artifact.  A
provider account snapshot must remain flat: simulated positions live only in
the shared :class:`PaperLedger` and are labelled as internal-paper activity.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import stat
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Mapping

from .options_gateway import reserve_options_proposal
from .options_lifecycle import OptionsLifecycle
from .paper_gateway import PaperProposalGateway
from .paper_ledger import PaperLedger, RiskLimits, ValidationError

UTC = timezone.utc
MAX_EVIDENCE_BYTES = 8_000_000
STATE_VERSION = 1
RECOVERY_MAX_AGE = timedelta(hours=2)
SHARED_BALANCES = {"prediction": 10_000, "equities": 30_000, "options": 30_000, "crypto": 10_000}
FULL_PORT_BALANCES = {"prediction": 0, "equities": 0, "options": 30_000, "crypto": 0}
FULL_PORT_LIMITS = RiskLimits(aggregate_cap_cents=30_000, group_cap_cents=30_000,
                              per_idea_cents={"prediction": 1, "equities": 1, "options": 30_000, "crypto": 1})


class OptionsInternalPaperError(ValueError):
    """A retained-evidence or internal-paper invariant was not met."""


class ProviderHoldingsMismatch(OptionsInternalPaperError):
    """Observed broker activity conflicts with the internal-paper-only model."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _stamp(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OptionsInternalPaperError(f"{field} must be ISO-8601") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsInternalPaperError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OptionsInternalPaperError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OptionsInternalPaperError(f"{field} must be a non-empty string")
    return value.strip()


def _hash(value: Any, field: str) -> str:
    value = _text(value, field).lower()
    if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise OptionsInternalPaperError(f"{field} must be a SHA-256 digest")
    return value


def _decimal(value: Any, field: str, *, nonnegative: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise OptionsInternalPaperError(f"{field} must be finite") from exc
    if not result.is_finite() or (nonnegative and result < 0):
        raise OptionsInternalPaperError(f"{field} must be finite")
    return result


def _cents(value: Decimal, *, rounding: str, field: str) -> int:
    result = (value * Decimal("100")).to_integral_value(rounding=rounding)
    if result < 0:
        raise OptionsInternalPaperError(f"{field} cannot be negative")
    return int(result)


def read_evidence(path: str | os.PathLike[str] | None) -> dict[str, Any] | None:
    """Read one bounded regular JSON artifact, refusing symlinks."""
    if path is None:
        return None
    candidate = Path(path)
    if not candidate.exists():
        return None
    if candidate.is_symlink() or not candidate.is_file() or candidate.stat().st_size > MAX_EVIDENCE_BYTES:
        raise OptionsInternalPaperError("evidence path is not a bounded regular file")
    try:
        value = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OptionsInternalPaperError("evidence JSON is unreadable") from exc
    return dict(_mapping(value, "evidence JSON"))


def _flat_provider_holdings(evidence: Mapping[str, Any], now: datetime, expected_symbols: set[str],
                            identity_to_symbol: Mapping[str, str]) -> None:
    """Require a fresh authoritative snapshot without importing unrelated risk.

    The paper ledger is intentionally independent from a user's real account.
    It may proceed only when no selected leg is held or pending and every open
    order can be proved to reference a different option identity.
    """
    holdings = _mapping(evidence.get("holdings"), "cloud holdings")
    if holdings.get("complete") is not True or holdings.get("open_orders_reconciled") is not True:
        raise OptionsInternalPaperError("authoritative holdings or open orders are unresolved")
    option_holdings = holdings.get("option_holdings")
    if not isinstance(option_holdings, dict):
        raise OptionsInternalPaperError("authoritative options holdings must be an explicit object")
    if int(holdings.get("underlying_quantity", 0)) != 0:
        raise ProviderHoldingsMismatch("authoritative underlying holdings are not flat")
    def resolved(identity: Any) -> str:
        raw = str(identity).replace(" ", "")
        return identity_to_symbol.get(raw, raw)

    active = {resolved(symbol) for symbol, quantity in option_holdings.items()
              if type(quantity) is int and quantity != 0}
    pending = holdings.get("pending_option_symbols", [])
    if not isinstance(pending, list) or any(not isinstance(symbol, str) or not symbol for symbol in pending):
        raise OptionsInternalPaperError("authoritative pending option identities are malformed")
    active.update(resolved(symbol) for symbol in pending)
    if active.intersection(expected_symbols):
        raise ProviderHoldingsMismatch("authoritative holdings conflict with a selected option leg")
    orders = holdings.get("open_orders", [])
    if not isinstance(orders, list):
        raise OptionsInternalPaperError("authoritative open orders are malformed")
    for order in orders:
        if not isinstance(order, Mapping):
            raise OptionsInternalPaperError("authoritative open order is malformed")
        identity = order.get("option_id") or order.get("instrument_id")
        if not isinstance(identity, str) or not identity:
            raise OptionsInternalPaperError("authoritative open order cannot be proven unrelated")
        if resolved(identity) in expected_symbols:
            raise ProviderHoldingsMismatch("authoritative open order conflicts with a selected option leg")
    received = _stamp(holdings.get("local_receipt_at", evidence.get("local_receipt_at")), "holdings receipt")
    if received > now or now - received > timedelta(seconds=60):
        raise OptionsInternalPaperError("authoritative holdings snapshot is stale or future-dated")


def _candidate(decision: Mapping[str, Any]) -> Mapping[str, Any]:
    if decision.get("artifact_kind") != "options-observed-shadow-decision-v1":
        raise OptionsInternalPaperError("candidate is not an observed-shadow artifact")
    if decision.get("status") != "eligible_shadow_candidate":
        raise OptionsInternalPaperError("observed-shadow artifact did not produce an eligible candidate")
    artifact_hash = _hash(decision.get("artifact_sha256"), "candidate artifact_sha256")
    if _digest({key: value for key, value in decision.items() if key != "artifact_sha256"}) != artifact_hash:
        raise OptionsInternalPaperError("candidate artifact hash is invalid")
    selected = _mapping(decision.get("shadow_candidate"), "shadow_candidate")
    if selected.get("status") != "candidate" or selected.get("fill_status") != "not_a_fill":
        raise OptionsInternalPaperError("candidate must be an unfilled frozen selector decision")
    return selected


def _expiry(value: Any, now: datetime) -> str:
    try:
        expiry = date.fromisoformat(str(value))
    except ValueError as exc:
        raise OptionsInternalPaperError("candidate expiry is malformed") from exc
    result = datetime.combine(expiry, time(20, 0), UTC)
    if result <= now:
        raise OptionsInternalPaperError("candidate expiry has passed")
    return _iso(result)


def _candidate_leg(row: Mapping[str, Any], side: str) -> dict[str, Any]:
    return {
        "symbol": _text(row.get("symbol"), "candidate leg symbol").replace(" ", ""),
        "underlying": _text(row.get("underlying"), "candidate underlying"),
        "right": _text(row.get("right"), "candidate right"), "expiry": _text(row.get("expiry"), "candidate expiry"),
        "strike": _text(row.get("strike"), "candidate strike"), "side": side, "quantity": 1,
        "bid": _text(row.get("bid"), "candidate bid"), "ask": _text(row.get("ask"), "candidate ask"),
        "timestamp": _iso(_stamp(row.get("timestamp"), "candidate quote timestamp")), "multiplier": 100,
        "adjusted": False, "bid_size": int(row.get("bid_size", 0)), "ask_size": int(row.get("ask_size", 0)),
    }


def proposal_from_candidate(decision: Mapping[str, Any], *, now: datetime, full_port: bool = False) -> dict[str, Any]:
    """Translate the frozen observed candidate into the narrow gateway input."""
    selected = _candidate(decision)
    if full_port and decision.get("paper_profile") != "options_full_port_v1":
        raise OptionsInternalPaperError("full-port ledger requires an options_full_port_v1 candidate")
    if not full_port and decision.get("paper_profile") not in (None, "standard"):
        raise OptionsInternalPaperError("standard ledger refuses a nonstandard paper profile")
    long = _mapping(selected.get("long"), "candidate long")
    short = _mapping(selected.get("short"), "candidate short")
    evidence = _mapping(decision.get("evidence"), "candidate evidence")
    received = _stamp(evidence.get("local_receipt_at"), "candidate local receipt")
    long_stamp, short_stamp = _stamp(long.get("timestamp"), "long quote"), _stamp(short.get("timestamp"), "short quote")
    observed = min(long_stamp, short_stamp)
    if received < max(long_stamp, short_stamp) or received > now or now - observed > timedelta(seconds=60):
        raise OptionsInternalPaperError("candidate quote/receipt chronology is invalid")
    costs = _mapping(selected.get("cost_model"), "candidate cost model")
    fee = _cents(_decimal(costs.get("fee_per_leg"), "fee_per_leg", nonnegative=True), rounding=ROUND_CEILING, field="fee")
    slippage = _cents(_decimal(costs.get("slippage_per_leg"), "slippage_per_leg", nonnegative=True), rounding=ROUND_CEILING, field="slippage")
    artifact_hash = _hash(decision.get("artifact_sha256"), "candidate artifact_sha256")
    proposal_id = f"options-internal-{artifact_hash[:24]}"
    return {
        "proposal_id": proposal_id, "strategy": "opt-001-directional-vertical",
        "strategy_version": _text(selected.get("strategy_version"), "strategy_version"),
        "source_kind": "observed", "source": {"kind": "observed", "hash": artifact_hash,
            "observed_at": _iso(observed), "received_at": _iso(received)},
        "idea_id": f"options:{_text(selected.get('symbol'), 'symbol')}:{_text(selected.get('expiry'), 'expiry')}:{_text(selected.get('direction'), 'direction')}",
        "expiry_at": _expiry(selected.get("expiry"), now), "execution": {"delay_seconds": 10},
        "legs": [_candidate_leg(long, "buy"), _candidate_leg(short, "sell")],
        "metadata": {"internal_paper": True, "modeled_fill_only": True, "candidate_artifact_sha256": artifact_hash,
                     "selector_evidence_ids": list(selected.get("evidence_ids", []))},
        "fee_cents_per_leg": fee, "slippage_cents_per_leg": slippage,
    } | ({"metadata": {"internal_paper": True, "modeled_fill_only": True,
                        "candidate_artifact_sha256": artifact_hash,
                        "selector_evidence_ids": list(selected.get("evidence_ids", [])),
                        "options_full_port_v1": True}} if full_port else {})


def _cloud_rows(evidence: Mapping[str, Any], expected_symbols: set[str], now: datetime) -> tuple[dict[str, Mapping[str, Any]], datetime, str]:
    sanitized = evidence.get("artifact_kind") == "options-cloud-evidence-sanitized-v1"
    if evidence.get("artifact_kind") not in {"options-cloud-evidence-v1", "options-cloud-evidence-sanitized-v1"} or evidence.get("status") != "captured":
        raise OptionsInternalPaperError("later entry/mark evidence must be a captured complete cloud artifact")
    if evidence.get("provider") != "robinhood-trading" or evidence.get("chain_complete") is not True or evidence.get("broker_write_called") is not False:
        raise OptionsInternalPaperError("cloud evidence lacks the read-only complete-chain attestation")
    if sanitized:
        _hash(evidence.get("source_capture_sha256"), "source cloud artifact_sha256")
    digest = _hash(evidence.get("artifact_sha256"), "cloud artifact_sha256")
    if _digest({key: value for key, value in evidence.items() if key != "artifact_sha256"}) != digest:
        raise OptionsInternalPaperError("cloud evidence artifact hash is invalid")
    rows = evidence.get("normalized_quotes")
    if not isinstance(rows, list):
        raise OptionsInternalPaperError("cloud evidence has no normalized quotes")
    indexed = {str(row.get("symbol", "")).replace(" ", ""): row for row in rows if isinstance(row, Mapping)}
    if set(indexed).intersection(expected_symbols) != expected_symbols:
        raise OptionsInternalPaperError("later cloud evidence does not cover both reserved legs")
    selected = {symbol: indexed[symbol] for symbol in expected_symbols}
    identity_to_symbol = {str(row.get("instrument_id", "")).replace(" ", ""): symbol
                          for symbol, row in indexed.items() if isinstance(row.get("instrument_id"), str)}
    _flat_provider_holdings(evidence, now, expected_symbols, identity_to_symbol)
    receipt = _stamp(evidence.get("local_receipt_at"), "cloud local receipt")
    for row in selected.values():
        stamp = _stamp(row.get("timestamp"), "cloud quote timestamp")
        if stamp > receipt or stamp > now or now - stamp > timedelta(seconds=60):
            raise OptionsInternalPaperError("later cloud quote is stale, future-dated, or after receipt")
    return selected, receipt, digest


def entry_event_from_cloud(reservation: Mapping[str, Any], raw: Mapping[str, Any], cloud: Mapping[str, Any], *, now: datetime) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Create one explicitly labelled modeled full-spread fill from later quotes."""
    raw_legs = raw["legs"]
    symbols = {str(leg["symbol"]) for leg in raw_legs}
    rows, receipt, evidence_hash = _cloud_rows(cloud, symbols, now)
    by_symbol = {str(leg["symbol"]): leg for leg in raw_legs}
    refreshed: list[dict[str, Any]] = []
    for symbol, original in by_symbol.items():
        quote = rows[symbol]
        copy = dict(original)
        copy.update({key: quote[key] for key in ("bid", "ask", "timestamp", "bid_size", "ask_size")})
        refreshed.append(copy)
    long = next(leg for leg in refreshed if leg["side"] == "buy")
    short = next(leg for leg in refreshed if leg["side"] == "sell")
    ask = _cents((_decimal(long["ask"], "long ask") - _decimal(short["bid"], "short bid")) * Decimal(100), rounding=ROUND_CEILING, field="entry spread ask")
    bid = _cents(max(Decimal(0), (_decimal(long["bid"], "long bid") - _decimal(short["ask"], "short ask")) * Decimal(100)), rounding=ROUND_FLOOR, field="entry spread bid")
    if ask <= 0 or bid > ask:
        raise OptionsInternalPaperError("later cloud evidence cannot price a debit spread")
    observed = min(_stamp(row["timestamp"], "later quote") for row in refreshed)
    quantity = int(reservation["quantity"])
    if any(int(row["ask_size"] if row["side"] == "buy" else row["bid_size"]) < quantity for row in refreshed):
        raise OptionsInternalPaperError("later cloud quote has insufficient all-or-none leg depth")
    event = {"reservation_id": reservation["reservation_id"], "fill_id": f"internal-model-{evidence_hash[:24]}",
             "source_kind": "observed", "source": {"kind": "observed", "hash": evidence_hash,
                 "observed_at": _iso(observed), "received_at": _iso(receipt)},
             "quote": {"timestamp": _iso(observed), "bid_cents": bid, "ask_cents": ask, "depth": quantity},
             "quantity": quantity, "cost": {"unit_cost_cents": ask, "fee_cents": int(reservation["fee_cents"])},
             "reservation_cost": {"fee_cents": int(reservation["fee_cents"])}, "filled_at": _iso(receipt),
             "model": "internal_paper_complete_spread_ask", "broker_order_sent": False}
    return event, refreshed


def _validate_unfilled_exit_evidence(raw: Mapping[str, Any], cloud: Mapping[str, Any], *, now: datetime) -> str:
    """Validate a later exact-leg exit observation without attempting a fill.

    An exit observation may conservatively price the spread above the reserved
    debit.  That is evidence to release an unfilled internal reservation, not
    a reason to synthesize a fill.  The normal cloud/holdings checks still
    prove both selected legs and a read-only, fresh provider snapshot.
    """
    symbols = {str(leg["symbol"]) for leg in raw["legs"]}
    _, receipt, evidence_hash = _cloud_rows(cloud, symbols, now)
    source = _mapping(raw.get("source"), "pending proposal source")
    if receipt <= _stamp(source.get("received_at"), "pending proposal receipt"):
        raise OptionsInternalPaperError("exit evidence must be later than the pending proposal")
    return evidence_hash


def _entry_record(raw: Mapping[str, Any], fill: Mapping[str, Any], refreshed_legs: list[dict[str, Any]]) -> dict[str, Any]:
    """Bind the gateway fill to immutable, explicitly modeled lifecycle input."""
    result = {"position_id": fill["fill"]["position_id"], "proposal_id": raw["proposal_id"],
              "strategy": raw["strategy"], "strategy_version": raw["strategy_version"],
              "instrument": fill["fill"]["instrument"], "exposure_group": "us-broad-equity", "idea_id": raw["idea_id"],
              "quantity": fill["fill"]["quantity"], "ledger_cost_cents": fill["fill"]["total_cost_cents"],
              "ledger_fee_cents": fill["fill"]["fee_cents"], "entry_as_of": fill["fill"]["filled_at"],
              "legs": refreshed_legs, "raw_input": {key: value for key, value in raw.items() if key not in {"fee_cents_per_leg", "slippage_cents_per_leg"}},
              "fill_input": fill["fill_event"], "internal_paper": True, "broker_order_sent": False}
    result["raw_input_digest"] = _digest(result["raw_input"])
    result["fill_input_digest"] = _digest(result["fill_input"])
    return result


def _model_reconciliation(entry: Mapping[str, Any], *, ledger: PaperLedger,
                          now: datetime) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Observe current paper holdings while retaining the original fill times.

    This is a fresh read of the internal ledger, never a refreshed broker or
    quote timestamp. The lifecycle still validates the original fill evidence.
    """
    position = ledger.get_position(entry["position_id"])
    if (position.state != "open" or position.proposal_id != entry["proposal_id"]
            or position.instrument != entry["instrument"]
            or position.quantity != entry["quantity"]
            or position.basis_cents != entry["ledger_cost_cents"]
            or _stamp(position.opened_at, "ledger opened_at") != _stamp(entry["entry_as_of"], "entry_as_of")
            or _stamp(entry["entry_as_of"], "entry_as_of") > now):
        raise OptionsInternalPaperError("current paper holdings do not match the durable entry")
    quantity = int(entry["quantity"])
    reports = []
    holdings: dict[str, int] = {}
    for leg in entry["legs"]:
        symbol = leg["symbol"]
        reports.append({"report_id": f"internal-model:{entry['position_id']}:{symbol}",
                        "order_id": f"internal-model:{entry['position_id']}:{symbol}",
                        "symbol": symbol, "intent": "open", "cumulative_quantity": quantity,
                        "timestamp": entry["entry_as_of"]})
        holdings[symbol] = quantity if leg["side"] == "buy" else -quantity
    return reports, {"complete": True, "timestamp": _iso(now), "option_holdings": holdings,
                     "underlying_quantity": 0, "open_orders_reconciled": True, "source": "internal_paper_ledger"}


def _quote_bundle(entry: Mapping[str, Any], cloud: Mapping[str, Any], now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    rows, receipt, digest = _cloud_rows(cloud, {leg["symbol"] for leg in entry["legs"]}, now)
    quotes: dict[str, Any] = {}
    sources: dict[str, Any] = {}
    for leg in entry["legs"]:
        row = rows[leg["symbol"]]
        quote = {key: row[key] for key in ("bid", "ask", "timestamp", "bid_size", "ask_size")}
        quote["timestamp"] = _iso(_stamp(quote["timestamp"], "mark quote timestamp"))
        material = {"symbol": leg["symbol"], "provenance": "observed", "provider": "robinhood-trading", "quote": quote,
                    "observed_at": quote["timestamp"], "received_at": _iso(receipt)}
        sources[leg["symbol"]] = {**material, "hash": _digest(material), "quote_digest": _digest({"symbol": leg["symbol"], "quote": quote}),
                                  "artifact_sha256": digest}
        quotes[leg["symbol"]] = quote
    return quotes, sources


def _recovery_ok(path: str | os.PathLike[str] | None, now: datetime) -> bool:
    if path is None:
        return True
    value = read_evidence(path)
    if value is None or value.get("operational_healthy") is not True:
        return False
    try:
        updated = _stamp(value.get("updated_at"), "recovery updated_at")
    except OptionsInternalPaperError:
        return False
    return updated <= now and now - updated <= RECOVERY_MAX_AGE


def _state_path(root: Path) -> Path:
    return root / "state.json"


def _load_state(root: Path) -> dict[str, Any]:
    path = _state_path(root)
    if not path.exists():
        return {"schema_version": STATE_VERSION}
    if path.is_symlink() or not path.is_file():
        raise OptionsInternalPaperError("internal-paper state is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OptionsInternalPaperError("internal-paper state is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != STATE_VERSION:
        raise OptionsInternalPaperError("internal-paper state version is unsupported")
    return value


def _save_state(root: Path, state: Mapping[str, Any]) -> None:
    path = _state_path(root)
    payload = _canonical(state) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".options-internal-state-", dir=root)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    descriptor = os.open(root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _mark_or_exit(
    *, ledger: PaperLedger, root: Path, entry: Mapping[str, Any], mark_cloud: Mapping[str, Any] | None,
    exit_cloud: Mapping[str, Any] | None, now: datetime,
) -> dict[str, Any]:
    lifecycle = OptionsLifecycle(ledger, root / "lifecycle", clock=lambda: now)
    if ledger.get_position(entry["position_id"]).state == "closed":
        return {"status": "closed_internal_paper", "modeled_fill": True,
                "orders_sent": 0, "broker_order_sent": False}
    reports, model_holdings = _model_reconciliation(entry, ledger=ledger, now=now)
    result: dict[str, Any] = {"status": "holding_internal_paper", "modeled_fill": True,
                              "orders_sent": 0, "broker_order_sent": False}
    for phase, cloud in (("mark", mark_cloud), ("exit", exit_cloud)):
        if cloud is None:
            continue
        try:
            quotes, sources = _quote_bundle(entry, cloud, now)
            mark = lifecycle.position_mark(entry, reports=reports, holdings_snapshot=model_holdings,
                                           quotes=quotes, quote_sources=sources, now=now)
            try:
                ledger.record_marks(f"options-internal-paper:{mark.source_hash[:24]}", [mark], now)
            except ValidationError as exc:
                result["mark_status"] = "shared_mark_coverage_required"
                result["mark_blocker"] = str(exc)
            else:
                result.update(mark_status="persisted", mark_cents=mark.liquidation_value_cents)
                result.pop("mark_blocker", None)
            if phase == "exit":
                result["lifecycle"] = lifecycle.run(entry, reports=reports, holdings_snapshot=model_holdings,
                                                      quotes=quotes, quote_sources=sources, now=now)
        except ProviderHoldingsMismatch as exc:
            # A non-flat provider snapshot is assignment/unexpected-broker-activity
            # evidence. Feed an explicit underlying exposure into the existing
            # lifecycle reconciliation boundary so the accounting latch persists.
            latched = dict(model_holdings)
            latched["underlying_quantity"] = 1
            result["lifecycle"] = lifecycle.run(entry, reports=reports, holdings_snapshot=latched,
                                                  quotes={}, quote_sources={}, now=now)
            result["provider_reconciliation"] = str(exc)
            break
        except (OptionsInternalPaperError, ValidationError) as exc:
            # An invalid mark must not hide a later valid exit observation.
            # No quote or provider timestamp is advanced to bypass freshness.
            result.setdefault("evidence_blockers", {})[phase] = str(exc)
            if result.get("mark_status") != "persisted":
                result.update(mark_status="blocked_evidence", mark_blocker=str(exc))
    return result


def _ensure_initial_market_mark(ledger: PaperLedger, now: datetime) -> None:
    """Seed or refresh an empty batch only while the shared book is flat."""
    # Empty marks are only truthful when no desk has open or pending exposure.
    # Check the ledger API across all registered desks before deciding whether
    # stale coverage can be refreshed; ``record_marks`` also checks open
    # positions, but it does not reject pending reservations.
    if any(ledger.active_exposure_count(desk) for desk in ("prediction", "equities", "options", "crypto")):
        return

    now_utc = _stamp(now, "now")
    mark_events = [event for event in ledger.audit() if event.event_type == "market_marks"]
    if mark_events:
        latest_mark_at = max(_stamp(event.occurred_at, "market mark timestamp") for event in mark_events)
        # Match the SDK's coverage rules for an empty batch: its source time is
        # the batch as_of time, and marks are stale only when age exceeds the
        # configured max_source_age_seconds. Prior UTC-day coverage is always
        # refreshed, even when its age happens to fit that threshold.
        is_prior_utc_day = latest_mark_at.date() < now_utc.date()
        is_stale = (now_utc - latest_mark_at).total_seconds() > ledger.max_source_age_seconds
        if latest_mark_at > now_utc or not (is_prior_utc_day or is_stale):
            return
        batch_id = f"options-internal-flat-refresh:{_iso(now_utc)}"
    else:
        batch_id = "options-internal-initial-cash"

    ledger.record_marks(batch_id, [], now_utc)


def _secure_state_root(path: str | os.PathLike[str]) -> Path:
    root = Path(path)
    try:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = os.lstat(root)
    except OSError as exc:
        raise OptionsInternalPaperError("runtime state directory is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OptionsInternalPaperError("runtime state directory is not a real directory")
    os.chmod(root, 0o700)
    return root


@contextlib.contextmanager
def _runtime_lock(root: Path):
    path = root / "runtime.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise OptionsInternalPaperError("runtime lock cannot be opened safely") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OptionsInternalPaperError("runtime lock is not a regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OptionsInternalPaperError("already_running") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _open_shared_ledger(path: str | os.PathLike[str], now: datetime, *, full_port: bool = False) -> PaperLedger:
    candidate = Path(path)
    try:
        metadata = os.lstat(candidate)
    except FileNotFoundError as exc:
        raise OptionsInternalPaperError("shared paper ledger must already exist") from exc
    except OSError as exc:
        raise OptionsInternalPaperError("shared paper ledger cannot be inspected") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise OptionsInternalPaperError("shared paper ledger must be a regular non-symlink file")
    try:
        return PaperLedger(candidate, balances=FULL_PORT_BALANCES if full_port else SHARED_BALANCES,
                           limits=FULL_PORT_LIMITS if full_port else RiskLimits(),
                           enforce_market_risk=True, clock=lambda: now)
    except Exception as exc:
        raise OptionsInternalPaperError("shared paper ledger configuration is not the required $800 book") from exc


def run_once(*, candidate: Mapping[str, Any] | None, entry_cloud: Mapping[str, Any] | None = None,
             mark_cloud: Mapping[str, Any] | None = None, exit_cloud: Mapping[str, Any] | None = None,
             state_dir: str | os.PathLike[str], ledger_path: str | os.PathLike[str], now: datetime | None = None,
             recovery_status: str | os.PathLike[str] | None = None, full_port: bool = False,
             review_only: bool = False) -> dict[str, Any]:
    """Run one finite idempotent cycle; missing evidence is an honest abstention."""
    current = _stamp(now or datetime.now(UTC), "now")
    try:
        root = _secure_state_root(state_dir)
        with _runtime_lock(root):
            return _run_once(candidate=candidate, entry_cloud=entry_cloud, mark_cloud=mark_cloud,
                             exit_cloud=exit_cloud, root=root, ledger_path=ledger_path, now=current,
                             recovery_status=recovery_status, full_port=full_port, review_only=review_only)
    except OptionsInternalPaperError as exc:
        return {"status": "abstain", "reason": str(exc), "orders_sent": 0, "broker_order_sent": False}


def _run_once(*, candidate: Mapping[str, Any] | None, entry_cloud: Mapping[str, Any] | None,
              mark_cloud: Mapping[str, Any] | None, exit_cloud: Mapping[str, Any] | None, root: Path,
              ledger_path: str | os.PathLike[str], now: datetime, recovery_status: str | os.PathLike[str] | None,
              full_port: bool = False, review_only: bool = False) -> dict[str, Any]:
    """Locked implementation; all entry mutations occur behind ``runtime.lock``."""
    try:
        state = _load_state(root)
        if review_only and not isinstance(state.get("entry_record"), Mapping):
            return {"status": "abstain", "reason": "review_only_no_position", "orders_sent": 0,
                    "broker_order_sent": False}
        ledger = _open_shared_ledger(ledger_path, now, full_port=full_port)
        entry = state.get("entry_record")
        if isinstance(entry, Mapping):
            result = _mark_or_exit(ledger=ledger, root=root, entry=entry, mark_cloud=mark_cloud,
                                   exit_cloud=exit_cloud, now=now)
            state.update(last_result=result, updated_at=_iso(now))
            _save_state(root, state)
            return result
        pending = state.get("pending")
        if isinstance(pending, Mapping):
            raw = _mapping(pending.get("raw"), "pending raw proposal")
            reservation = _mapping(pending.get("reservation"), "pending reservation")
            durable = ledger.get_reservation(_text(reservation.get("reservation_id"), "pending reservation_id"))
            if durable.proposal_id != raw.get("proposal_id"):
                raise OptionsInternalPaperError("pending reservation proposal identity changed")
            paused = (root / "STOP").exists() or not _recovery_ok(recovery_status, now)
            if paused:
                gateway = PaperProposalGateway(ledger, source_kind="observed", clock=lambda: now)
                if durable.state == "reserved":
                    cancelled = gateway.cancel(durable.reservation_id, reason="STOP_or_recovery_gate", now=now)
                elif durable.state == "cancelled":
                    cancelled = {"status": "cancelled", "reservation": dict(reservation)}
                else:
                    raise OptionsInternalPaperError("paused pending reservation is not cancellable")
                state.pop("pending", None)
                state.update(last_result=cancelled, updated_at=_iso(now))
                _save_state(root, state)
                return {"status": "cancelled", "reason": "STOP_or_recovery_gate", "reservation": cancelled["reservation"],
                        "orders_sent": 0, "broker_order_sent": False, "modeled_fill": False}
            if exit_cloud is not None:
                # A later independently retained exit-phase observation is a
                # hard end-of-session boundary for an unfilled simulation. It
                # must release cash even when a prior entry capture became
                # unaffordable; no position was ever created.
                exit_hash = _validate_unfilled_exit_evidence(raw, exit_cloud, now=now)
                gateway = PaperProposalGateway(ledger, source_kind="observed", clock=lambda: now)
                if durable.state == "reserved":
                    cancelled = gateway.cancel(durable.reservation_id, reason="end_of_session_unfilled", now=now)
                elif durable.state == "cancelled":
                    cancelled = {"status": "cancelled", "reservation": dict(reservation)}
                else:
                    raise OptionsInternalPaperError("unfilled exit reservation is not cancellable")
                state.pop("pending", None)
                state["unfilled_exit_cancelled"] = {
                    "proposal_id": raw["proposal_id"],
                    "candidate_artifact_sha256": _text(_mapping(raw.get("metadata"), "pending metadata").get("candidate_artifact_sha256"), "pending candidate hash"),
                    "exit_evidence_sha256": exit_hash,
                    "cancelled_at": _iso(now),
                }
                state.update(last_result=cancelled, updated_at=_iso(now))
                _save_state(root, state)
                return {"status": "cancelled", "reason": "end_of_session_unfilled", "reservation": cancelled["reservation"],
                        "orders_sent": 0, "broker_order_sent": False, "modeled_fill": False}
            if entry_cloud is None:
                return {"status": durable.state, "proposal_id": durable.proposal_id, "reservation": dict(reservation),
                        "orders_sent": 0, "broker_order_sent": False, "modeled_fill": False}
            gateway = PaperProposalGateway(ledger, source_kind="observed", clock=lambda: now)
            event, refreshed = entry_event_from_cloud(reservation, raw, entry_cloud, now=now)
            fill = gateway.record_fill({key: value for key, value in event.items() if key not in {"model", "broker_order_sent"}}, now=now)
            entry = _entry_record(raw, fill, refreshed)
            state.pop("pending", None)
            state.update(entry_record=entry, updated_at=_iso(now))
            _save_state(root, state)
            result = {"status": "filled_internal_paper", "proposal_id": raw["proposal_id"], "reservation": dict(reservation),
                      "orders_sent": 0, "broker_order_sent": False, "modeled_fill": True, "fill": fill["fill"],
                      "entry_record": entry}
            result.update(_mark_or_exit(ledger=ledger, root=root, entry=entry, mark_cloud=mark_cloud,
                                        exit_cloud=exit_cloud, now=now))
            result["status"] = "filled_internal_paper"
            state.update(last_result=result, updated_at=_iso(now))
            _save_state(root, state)
            return result
        # STOP and recovery pause only new modeled entries. Existing lifecycle
        # records above continue to reconcile and preserve risk.
        if (root / "STOP").exists() or not _recovery_ok(recovery_status, now):
            return {"status": "abstain", "reason": "STOP_or_recovery_gate", "orders_sent": 0, "broker_order_sent": False}
        if candidate is None:
            return {"status": "abstain", "reason": "candidate_evidence_missing", "orders_sent": 0, "broker_order_sent": False}
        ended_unfilled = state.get("unfilled_exit_cancelled")
        if isinstance(ended_unfilled, Mapping):
            candidate_hash = _hash(candidate.get("artifact_sha256"), "candidate artifact_sha256")
            if ended_unfilled.get("candidate_artifact_sha256") == candidate_hash:
                return {"status": "abstain", "reason": "end_of_session_unfilled", "orders_sent": 0, "broker_order_sent": False}
            # A later evidence artifact is a distinct proposed session. Its
            # durable reservation will replace this one-shot cancellation
            # latch only after all ordinary candidate gates pass.
            state.pop("unfilled_exit_cancelled", None)
        _ensure_initial_market_mark(ledger, now)
        if full_port and ledger.active_exposure_count("options") != 0:
            raise OptionsInternalPaperError("full-port sleeve permits only one active options exposure")
        raw = proposal_from_candidate(candidate, now=now, full_port=full_port)
        gateway = PaperProposalGateway(ledger, source_kind="observed", clock=lambda: now)
        gateway_raw = {key: value for key, value in raw.items() if key not in {"fee_cents_per_leg", "slippage_cents_per_leg"}}
        reserved = reserve_options_proposal(gateway_raw, gateway, as_of=now, now=now,
                                           fee_cents_per_contract=raw["fee_cents_per_leg"],
                                           slippage_cents_per_contract=raw["slippage_cents_per_leg"],
                                           max_loss_budget="300" if full_port else "15", max_quantity=1,
                                           full_port=full_port)
        result: dict[str, Any] = {"status": "reserved", "proposal_id": gateway_raw["proposal_id"], "reservation": reserved["reservation"],
                                  "orders_sent": 0, "broker_order_sent": False, "modeled_fill": False}
        if reserved["status"] != "reserved":
            return result
        state.update(pending={"raw": gateway_raw, "reservation": reserved["reservation"]}, updated_at=_iso(now))
        _save_state(root, state)
        if entry_cloud is None:
            return result
        # The candidate itself may only be used at its capture time.  The
        # later invocation above consumes this durable reservation and fresh
        # entry evidence, preventing stale candidate replay from inventing a fill.
        event, refreshed = entry_event_from_cloud(reserved["reservation"], gateway_raw, entry_cloud, now=now)
        fill = gateway.record_fill({key: value for key, value in event.items() if key not in {"model", "broker_order_sent"}}, now=now)
        entry = _entry_record(gateway_raw, fill, refreshed)
        state.pop("pending", None)
        state.update(entry_record=entry, updated_at=_iso(now))
        _save_state(root, state)
        result.update(status="filled_internal_paper", modeled_fill=True, fill=fill["fill"], entry_record=entry)
        result.update(_mark_or_exit(ledger=ledger, root=root, entry=entry, mark_cloud=mark_cloud,
                                    exit_cloud=exit_cloud, now=now))
        result["status"] = "filled_internal_paper"
        state.update(last_result=result, updated_at=_iso(now))
        _save_state(root, state)
        return result
    except Exception as exc:
        return {"status": "abstain", "reason": str(exc), "orders_sent": 0, "broker_order_sent": False}


def _worker_exit_code(result: Mapping[str, Any]) -> int:
    """Map normal paper decisions to healthy oneshot-service completion.

    A cancellation and an honest abstention are deliberate risk controls, not
    a worker crash.  Unknown/error statuses remain nonzero so systemd still
    surfaces real runtime failures.
    """
    status = result.get("status")
    if (status in {"filled_internal_paper", "holding_internal_paper"}
            and result.get("mark_status") in {"blocked_evidence", "shared_mark_coverage_required"}):
        return 1  # Trigger OnFailure immediately; the observer verifies recovery.
    if status in {"abstain", "cancelled", "reserved", "filled_internal_paper", "holding_internal_paper", "closed_internal_paper"}:
        return 0
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", help="retained candidate.json and optional entry/mark/exit.json")
    parser.add_argument("--input-artifact-root", help="resolve dated candidate.json from current.json")
    parser.add_argument("--candidate-inbox-root", help="read sanitized candidate inbox published by options service")
    parser.add_argument("--lifecycle-inbox-root", help="read sanitized entry/mark/exit evidence events")
    parser.add_argument("--state-dir", default="/var/lib/neorome-options-internal-paper")
    parser.add_argument("--ledger", default="/var/lib/neorome-paper/paper.sqlite3")
    parser.add_argument("--recovery-status", default="/var/lib/neorome-paper-recovery/status.json")
    parser.add_argument("--now", help="UTC ISO-8601 test clock")
    parser.add_argument("--full-port", action="store_true", help="use the isolated $300 options full-port paper ledger")
    parser.add_argument("--review-only", action="store_true", help="review existing paper holdings; never reserve or enter")
    args = parser.parse_args(argv)
    if sum(bool(item) for item in (args.input_dir, args.input_artifact_root, args.candidate_inbox_root)) != 1:
        parser.error("provide exactly one candidate input route")
    current = _stamp(args.now, "--now") if args.now else datetime.now(UTC)
    inbox_events: dict[str, Mapping[str, Any]] = {}
    if args.lifecycle_inbox_root:
        try:
            from .options_lifecycle_inbox import read_events
            for event in read_events(args.lifecycle_inbox_root):
                # Events are immutable and ordered in the current manifest;
                # a later independent observation supersedes an earlier mark.
                inbox_events[str(event["phase"])] = _mapping(event.get("capture"), "lifecycle inbox capture")
        except Exception:
            inbox_events = {}
    if args.candidate_inbox_root:
        from .options_candidate_inbox import CandidateInboxError, read_current_candidate
        try:
            inbox_candidate = read_current_candidate(args.candidate_inbox_root)
        except CandidateInboxError:
            inbox_candidate = None
        candidate_path = entry_path = mark_path = exit_path = None
    elif args.input_artifact_root:
        from .options_dated_artifacts import DatedArtifactError, resolve_current
        try:
            candidate_path = resolve_current(args.input_artifact_root, "candidate.json")["candidate.json"]
        except DatedArtifactError:
            candidate_path = None
        entry_path = mark_path = exit_path = None
    else:
        root = Path(args.input_dir)
        candidate_path, entry_path, mark_path, exit_path = root / "candidate.json", root / "entry.json", root / "mark.json", root / "exit.json"
    result = run_once(candidate=inbox_candidate if args.candidate_inbox_root else read_evidence(candidate_path), entry_cloud=inbox_events.get("entry") if args.lifecycle_inbox_root else read_evidence(entry_path),
                      mark_cloud=inbox_events.get("mark") if args.lifecycle_inbox_root else read_evidence(mark_path), exit_cloud=inbox_events.get("exit") if args.lifecycle_inbox_root else read_evidence(exit_path),
                      state_dir=args.state_dir, ledger_path=args.ledger, recovery_status=args.recovery_status, now=current,
                      full_port=args.full_port, review_only=args.review_only)
    print(_canonical(result))
    return _worker_exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
