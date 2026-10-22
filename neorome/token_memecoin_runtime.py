"""Observed Solana memecoin discovery and internal-paper lifecycle.

All provider calls are read only. There is no wallet, transaction, signing, or
swap interface in this module.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
from typing import Any, Mapping

from .crypto_research import BudgetOpener, POLICY as DISCOVERY_POLICY, Sources, research
from .jupiter_quotes import QuoteClient, QuoteError, USDC
from .memecoin_experiments import EXPERIMENT_VERSION, assess as assess_experiments
from .paper_ledger import InvalidState, MarketMark, PaperLedger, Proposal, RiskLimits
from .token_social import SocialError, XClient, eligible as social_eligible

UTC = timezone.utc
POLICY_VERSION = "memecoin-paper-v1"
ENTRY_EXTRA_RAW = 50_000
EXIT_EXTRA_RAW = 50_000
EXIT_FEE_CENTS = 5
MAX_HOLD = timedelta(minutes=60)
STOP_FRACTION = Decimal("0.50")
TAKE_FRACTION = Decimal("2.00")
RETRY_INTERVAL = timedelta(minutes=1)
X_RETRY_BACKOFF = timedelta(minutes=15)
PROGRAM_CRYPTO_SLEEVE_CENTS = 30_000
DISCOVERY_CYCLE_SCHEMA = "memecoin-discovery-cycle-v1"
OBSERVATION_INTERVAL_MINUTES = 15
OBSERVATION_INTERVAL = timedelta(minutes=OBSERVATION_INTERVAL_MINUTES)
MAX_CYCLE_REASON = 240


class MemecoinError(ValueError): pass


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value): return hashlib.sha256(_canonical(value)).hexdigest()


def _time(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None: raise MemecoinError("timestamp requires timezone")
    return parsed.astimezone(UTC)


def _iso(value): return _time(value).isoformat().replace("+00:00", "Z")


def _candidate_observation_times(candidate, social):
    """Return provider receipt bounds for a newly persisted candidate.

    ``known_at`` is the first provider receipt that made the candidate
    observable; ``received_at`` is the last provider receipt used to build the
    persisted record.  Missing or malformed provenance stays unknown rather
    than being replaced with the local decision clock.
    """
    receipts = []
    evidence = candidate.get("evidence", [])
    if isinstance(evidence, list):
        records = list(evidence)
    else:
        records = []
    quotes = candidate.get("quotes")
    if isinstance(quotes, Mapping) and isinstance(quotes.get("observations"), list):
        records.extend(quotes["observations"])
    if isinstance(social, Mapping):
        records.append(social)
    for record in records:
        if not isinstance(record, Mapping):
            continue
        value = record.get("received_at")
        if not isinstance(value, str):
            continue
        try:
            receipts.append(_time(value))
        except (TypeError, ValueError, MemecoinError):
            continue
    if not receipts:
        return None, None
    return _iso(min(receipts)), _iso(max(receipts))


def _raw(value, name):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).isdecimal() or int(value) <= 0:
        raise MemecoinError(f"{name} must be positive base units")
    return int(value)


def _ceil_cents(raw): return int((Decimal(raw) / Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING))
def _floor_cents(raw): return raw // 10_000


def _mark_batch_id(position_id: str, mark: Mapping[str, Any]) -> str:
    """Return a stable id for one observed position mark.

    The route response hash and observed mark value are part of the identity so
    a retry of the same provider observation is idempotent while a new route
    produces a new complete market-mark batch.
    """
    return "memecoin-mark-" + _digest({
        "position_id": position_id,
        "source_hash": mark["source_hash"],
        "source_timestamp": mark["received_at"],
        "liquidation_value_cents": int(mark["liquidation_value_cents"]),
    })[:32]


def _root(value):
    root = Path(value)
    if root.exists() and root.is_symlink(): raise MemecoinError("state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    (root / "candidates").mkdir(mode=0o700, exist_ok=True)
    (root / "cycles").mkdir(mode=0o700, exist_ok=True)
    return root


def _write(path, value):
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1; stream.write(_canonical(value) + b"\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: temporary.unlink()
        except FileNotFoundError: pass


def _write_new(path: Path, value: Mapping[str, Any]) -> None:
    """Atomically create an immutable evidence record."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise MemecoinError("evidence path symlink refused")
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(_canonical(value) + b"\n")
            stream.flush(); os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return
        finally:
            try: temporary.unlink()
            except FileNotFoundError: pass
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: temporary.unlink()
        except FileNotFoundError: pass


def _load(path):
    if not path.exists(): return {"schema_version": 1, "policy_version": POLICY_VERSION, "seen": [], "active": None}
    if path.is_symlink(): raise MemecoinError("state symlink refused")
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or value.get("policy_version") != POLICY_VERSION: raise MemecoinError("state invalid")
    return value


def _safe_reason(error: BaseException) -> str:
    """Keep cycle evidence useful without persisting credentials or huge text."""
    text = str(error).replace("\n", " ")
    for marker in ("api-key=", "Bearer ", "bearer ", "token="):
        if marker in text:
            text = text.split(marker, 1)[0] + marker.rstrip("=") + "=<redacted>"
    text = re.sub(r"(?i)(authorization|bearer|api[-_]?key|token|secret|password)\s*[:=]\s*\S+",
                  r"\1=<redacted>", text)
    return text[:MAX_CYCLE_REASON] or type(error).__name__


def _cycle_id(current: datetime) -> str:
    # ISO basic form is sortable and contains no path separators or secrets.
    return "cycle-" + current.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ") + "-" + secrets.token_hex(6)


def _new_cycle(current: datetime) -> dict[str, Any]:
    return {
        "schema_version": DISCOVERY_CYCLE_SCHEMA,
        "cycle_id": _cycle_id(current),
        "started_at": _iso(current),
        "finished_at": None,
        "observation_interval_minutes": OBSERVATION_INTERVAL_MINUTES,
        "provider_profile_count": 0,
        "valid_unique_solana_mint_count": 0,
        "selected_count": 0,
        "interval_skips": 0,
        "processed_count": 0,
        "retained_count": 0,
        "x_queries": 0,
        "x_unavailable": 0,
        "entry_route_count": 0,
        "sell_preflight_count": 0,
        "m0_eligible_count": 0,
        "selected_fill_hash": None,
        "selected_candidate_content_sha256": None,
        "selected_fill_id": None,
        "request_count": 0,
        "terminal_status": "in_progress",
        "terminal_reason": None,
        "source_provenance": [],
    }


def _finish_cycle(root: Path, cycle: dict[str, Any], current: datetime, *, status: str, reason: str | None,
                  request_count: int | None = None) -> None:
    cycle["finished_at"] = _iso(current)
    cycle["terminal_status"] = status
    cycle["terminal_reason"] = reason[:MAX_CYCLE_REASON] if isinstance(reason, str) else reason
    if request_count is not None:
        cycle["request_count"] = int(request_count)
    path = root / "cycles" / (cycle["cycle_id"] + ".json")
    _write_new(path, cycle)


def _observation_map(state: dict[str, Any]) -> dict[str, Any]:
    observations = state.get("observations")
    if not isinstance(observations, dict):
        observations = {}
        state["observations"] = observations
    return observations


def _last_observed(state: dict[str, Any], mint: str) -> datetime | None:
    value = _observation_map(state).get(mint)
    if isinstance(value, Mapping):
        value = value.get("last_observed_at") or value.get("observed_at")
    if not isinstance(value, str):
        return None
    try:
        return _time(value)
    except (TypeError, ValueError, MemecoinError):
        return None


def _m0_eligible(record: Mapping[str, Any]) -> bool:
    experiments = record.get("experiments")
    if not isinstance(experiments, list):
        return False
    return any(isinstance(item, Mapping) and item.get("cohort_id") == "M0" and item.get("status") == "eligible"
               for item in experiments)


def _sqlite_busy(exc: BaseException) -> bool:
    """Return whether SQLite rejected a transaction because it was contended."""
    return isinstance(exc, sqlite3.OperationalError) and any(
        marker in str(exc).lower() for marker in ("database is locked", "database table is locked", "database is busy")
    )


def _proposal_state(proposal: Proposal) -> dict[str, Any]:
    """Serialize the admission proposal so a committed reserve can be resumed."""
    return {
        "proposal_id": proposal.proposal_id,
        "desk": proposal.desk,
        "strategy": proposal.strategy,
        "strategy_version": proposal.strategy_version,
        "source_hash": proposal.source_hash,
        "source_timestamp": _iso(proposal.source_timestamp),
        "instrument": proposal.instrument,
        "exposure_group": proposal.exposure_group,
        "idea_id": proposal.idea_id,
        "expiry_at": _iso(proposal.expiry_at),
        "quantity": proposal.quantity,
        "unit_cost_cents": proposal.unit_cost_cents,
        "fee_cents": proposal.fee_cents,
        "provenance": proposal.provenance,
        "metadata": dict(proposal.metadata),
    }


def _proposal_from_state(value: Mapping[str, Any]) -> Proposal:
    try:
        return Proposal(
            proposal_id=value["proposal_id"], desk=value["desk"], strategy=value["strategy"],
            strategy_version=value["strategy_version"], source_hash=value["source_hash"],
            source_timestamp=value["source_timestamp"], instrument=value["instrument"],
            exposure_group=value["exposure_group"], idea_id=value["idea_id"], expiry_at=value["expiry_at"],
            quantity=value["quantity"], unit_cost_cents=value["unit_cost_cents"], fee_cents=value["fee_cents"],
            provenance=value["provenance"], metadata=value.get("metadata", {}),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MemecoinError("pending memecoin proposal is invalid") from exc


def _record_ledger_contention(state_path: Path, state: dict[str, Any], current: datetime,
                              exc: sqlite3.OperationalError, *, operation: str) -> dict[str, Any]:
    pending = state.get("pending")
    event = {
        "at": _iso(current),
        "operation": operation,
        "error_type": type(exc).__name__,
        "reason": "shared_ledger_busy",
        "retry_at": _iso(current + RETRY_INTERVAL),
        "proposal_id": pending.get("proposal", {}).get("proposal_id") if isinstance(pending, Mapping) else None,
    }
    history = state.setdefault("ledger_contention_events", [])
    if not isinstance(history, list):
        history = []
        state["ledger_contention_events"] = history
    history.append(event)
    state["ledger_contention_events"] = history[-20:]
    state["last_ledger_contention"] = event
    _write(state_path, state)
    return {
        "status": "abstain",
        "reason": "shared_ledger_busy",
        "operation": operation,
        "retry": "next_timer",
        "pending_reconciliation": isinstance(pending, Mapping),
        "orders_sent": 0,
    }


def _fill_from_pending(book: PaperLedger, pending: Mapping[str, Any], current: datetime):
    proposal = _proposal_from_state(pending["proposal"])
    fill_at = pending.get("filled_at", _iso(current))
    return book.simulate_fill(
        str(pending["reservation_id"]), fill_id=str(pending["fill_id"]),
        quantity=int(pending["quantity"]), unit_cost_cents=int(pending["unit_cost_cents"]),
        fee_cents=int(pending["fee_cents"]), provenance=proposal.provenance, filled_at=fill_at,
    )


def _pending_fill_state(pending: Mapping[str, Any], fill) -> dict[str, Any]:
    return {
        "position_id": fill.position_id,
        "mint": pending["mint"],
        "acquired_token_raw": str(pending["acquired_token_raw"]),
        "opened_at": fill.filled_at,
        "candidate_content_sha256": pending["candidate_content_sha256"],
        "latest_mark": None,
    }


def _resume_pending(book: PaperLedger, state: dict[str, Any], current: datetime):
    """Reconcile a prior reserve/fill attempt without discovering a new token."""
    pending = state.get("pending")
    if not isinstance(pending, Mapping):
        return None
    proposal_id = str(pending.get("proposal", {}).get("proposal_id", ""))
    if not proposal_id:
        raise MemecoinError("pending memecoin proposal is missing its identity")
    reserve_event = next((event for event in book.audit()
                          if event.event_type == "reserve"
                          and event.payload.get("proposal_id") == proposal_id), None)
    if reserve_event is None:
        proposal = _proposal_from_state(pending["proposal"])
        reservation = book.reserve(proposal, now=current)
        pending["reservation_id"] = reservation.reservation_id
        pending["filled_at"] = reservation.created_at
    else:
        reservation = book.get_reservation(reserve_event.entity_id)
        pending["reservation_id"] = reservation.reservation_id
    if reservation.state == "filled":
        fill_event = next((event for event in book.audit()
                           if event.event_type == "fill"
                           and event.payload.get("reservation_id") == reservation.reservation_id), None)
        if fill_event is None:
            raise MemecoinError("filled memecoin reservation is missing its fill audit")
        fill = book.get_position(str(fill_event.payload["position_id"]))
        state["active"] = _pending_fill_state(pending, fill)
        state.pop("pending", None)
        return {"status": "simulated_fill", "fill_recovered": True, "fill": asdict(fill), "orders_sent": 0}
    if reservation.state == "cancelled":
        state.pop("pending", None)
        return {"status": "ledger_reconciled_cancelled", "orders_sent": 0}
    if _time(reservation.expiry_at) <= current:
        cancelled = book.cancel(reservation.reservation_id, cancel_id="cancel-" + str(pending["fill_id"]),
                                 reason="pending_memecoin_admission_expired", cancelled_at=current)
        state.pop("pending", None)
        return {"status": "ledger_reconciled_cancelled", "reservation": asdict(cancelled), "orders_sent": 0}
    fill = _fill_from_pending(book, pending, current)
    state["active"] = _pending_fill_state(pending, fill)
    state.pop("pending", None)
    return {"status": "simulated_fill", "fill_recovered": True, "fill": asdict(fill), "orders_sent": 0}


def _quote(record, incoming, outgoing, amount=None):
    quote = record.get("quote") if isinstance(record, Mapping) else None
    if not isinstance(quote, Mapping) or (quote.get("inputMint"), quote.get("outputMint")) != (incoming, outgoing):
        raise MemecoinError("Jupiter route identity mismatch")
    actual = _raw(quote.get("inAmount"), "route input")
    if amount is not None and actual != amount: raise MemecoinError("Jupiter route is not exact-size")
    output = _raw(quote.get("outAmount"), "route output")
    minimum = _raw(quote.get("otherAmountThreshold"), "route minimum output")
    if minimum > output: raise MemecoinError("Jupiter minimum exceeds quote")
    if quote.get("priceImpactUnit") != "percentage_points": raise MemecoinError("Jupiter price impact units missing")
    try: impact = Decimal(str(quote.get("priceImpact")))
    except (InvalidOperation, ValueError): raise MemecoinError("Jupiter impact invalid") from None
    if not impact.is_finite() or abs(impact) > Decimal("2"): raise MemecoinError("Jupiter route impact exceeds 2 percentage points")
    return actual, output, minimum, str(impact)


def _candidate_record(discovery, candidate, social, now):
    mint = candidate["mint"]
    reasons = list(candidate.get("screen_rejections", []))
    pool = candidate.get("pool", {})
    concentration = candidate.get("account_concentration", {})
    try:
        if Decimal(str(concentration.get("largest_account_supply_fraction"))) > Decimal("0.35"):
            reasons.append("largest_token_account_above_35pct")
        if Decimal(str(concentration.get("listed_accounts_supply_fraction"))) > Decimal("0.95"):
            reasons.append("top_accounts_above_95pct_supply")
        if int(pool.get("buys_h1", 0)) < 5 or int(pool.get("sells_h1", 0)) < 2:
            reasons.append("insufficient_two_sided_h1_transactions")
        if Decimal(str(pool.get("volume_h1_usd", 0))) < Decimal("1000"):
            reasons.append("h1_volume_below_1000_usd")
    except (InvalidOperation, TypeError, ValueError): reasons.append("market_or_holder_metrics_incomplete")
    social_ok, social_reasons = social_eligible(social); reasons.extend(social_reasons)
    quotes = candidate.get("quotes", {}).get("observations", []) if isinstance(candidate.get("quotes"), Mapping) else []
    if len(quotes) != 2: reasons.append("entry_or_sell_preflight_missing")
    acquired = None
    if len(quotes) == 2:
        try:
            _, _, acquired, _ = _quote(quotes[0], USDC, mint)
            _quote(quotes[1], mint, USDC, acquired)
            if quotes[0].get("response_sha256") == quotes[1].get("response_sha256"):
                reasons.append("entry_and_sell_preflight_not_independent")
        except MemecoinError as exc: reasons.append(str(exc))
    evidence_hashes = sorted({e.get("response_sha256") for e in candidate.get("evidence", []) if isinstance(e, Mapping) and e.get("response_sha256")})
    evidence_hashes += [q.get("response_sha256") for q in quotes if isinstance(q, Mapping) and q.get("response_sha256")]
    evidence_hashes.append(social.get("response_sha256"))
    known_at, received_at = _candidate_observation_times(candidate, social)
    record = {"schema_version": 1, "policy_version": POLICY_VERSION, "recorded_at": _iso(now),
              "known_at": known_at, "received_at": received_at,
              "source_kind": "observed", "chain": "solana", "mint": mint, "pool": pool,
              "mint_checks": candidate.get("mint_checks"), "account_concentration": concentration,
              "social": social, "entry_quote": quotes[0] if len(quotes) == 2 else None,
              "sell_preflight": quotes[1] if len(quotes) == 2 else None,
              "acquired_token_raw": str(acquired) if acquired else None,
              "costs": {"entry_input_usdc_raw": DISCOVERY_POLICY["quote_input_usdc_base_units"],
                        "entry_extra_usdc_raw": str(ENTRY_EXTRA_RAW), "exit_extra_usdc_raw": str(EXIT_EXTRA_RAW),
                        "embedded_route_fees_readded": False, "slippage_uses_jupiter_minimum_output": True},
              "evidence_hashes": evidence_hashes, "rejection_reasons": sorted(set(reasons)),
              "eligible_for_internal_paper": not reasons and social_ok,
              "experiment_version": EXPERIMENT_VERSION,
              "execution_enabled": False, "orders_sent": 0}
    record["experiments"] = assess_experiments(record)
    record["content_sha256"] = _digest(record)
    return record


def token_mark(book, position, now, state_dir):
    state = _load(Path(state_dir) / "state.json")
    active = state.get("active")
    if not isinstance(active, Mapping) or active.get("position_id") != position.position_id:
        raise MemecoinError("memecoin position state missing")
    mark = active.get("latest_mark")
    if not isinstance(mark, Mapping): raise MemecoinError("memecoin mark missing")
    received = _time(mark.get("received_at"))
    if received < _time(position.opened_at) or not 0 <= (_time(now) - received).total_seconds() <= 90:
        raise MemecoinError("memecoin mark stale or predates entry")
    return MarketMark(position.position_id, int(mark["liquidation_value_cents"]), received,
                      str(mark["source_hash"]), "observed")


def _persist_candidate(root: Path, record: Mapping[str, Any]) -> None:
    digest = record.get("content_sha256")
    if not isinstance(digest, str) or not digest or any(ch not in "0123456789abcdef" for ch in digest.lower()):
        raise MemecoinError("candidate content hash is invalid")
    _write_new(root / "candidates" / (digest + ".json"), record)


def _run_discovery_cycle(root: Path, state: dict[str, Any], state_path: Path, book: PaperLedger,
                         current: datetime, sources=None, quotes=None, social_client=None) -> dict[str, Any]:
    """Run one complete discovery batch and publish an immutable cycle record.

    Selection and social evidence collection are deliberately separate from
    admission: every non-skipped selected mint is persisted before the first
    M0 candidate can reserve paper capital.
    """
    cycle = _new_cycle(current)
    finalized = False
    terminal_status = "operational_failure"
    terminal_reason = "discovery cycle failed"
    budget = BudgetOpener(16)
    source_client = sources or Sources(budget)
    quote_client = quotes or QuoteClient(opener=budget)

    def done(result: dict[str, Any], status: str, reason: str | None) -> dict[str, Any]:
        nonlocal finalized, terminal_status, terminal_reason
        finalized = True
        terminal_status, terminal_reason = status, reason
        return result

    try:
        try:
            discovery = research(source_client, quote_client, 3, current)
        except Exception as exc:
            # A provider/research failure is itself a measured discovery cycle;
            # never omit the denominator because the provider was unavailable.
            terminal_status, terminal_reason = "research_failure", _safe_reason(exc)
            raise MemecoinError("discovery research failure: " + terminal_reason) from exc

        coverage = ((discovery.get("discovery_coverage") or discovery.get("coverage", {}))
                    if isinstance(discovery, Mapping) else {})
        if isinstance(coverage, Mapping):
            for key in ("provider_profile_count", "valid_unique_solana_mint_count", "selected_count"):
                if type(coverage.get(key)) is int and coverage[key] >= 0:
                    cycle[key] = coverage[key]
            provenance = coverage.get("provider_provenance")
            if isinstance(provenance, list):
                cycle["source_provenance"] = provenance[:4]
        candidates = discovery.get("candidates", []) if isinstance(discovery, Mapping) else []
        if not isinstance(candidates, list):
            candidates = []
        # The selected count is a provider/research fact, not a post-admission
        # count.  Keep it truthful even for old research fixtures without the
        # new metadata.
        if cycle["selected_count"] == 0 and candidates:
            cycle["selected_count"] = len(candidates)

        observations = _observation_map(state)
        selected_records: list[dict[str, Any]] = []
        cooldown = False
        retry_not_before = state.get("x_retry_not_before")
        if isinstance(retry_not_before, str):
            try:
                cooldown = current < _time(retry_not_before)
            except (TypeError, ValueError, MemecoinError):
                cooldown = False
        x_failure_reason: str | None = None
        for candidate in candidates:
            if not isinstance(candidate, Mapping) or not isinstance(candidate.get("mint"), str):
                continue
            mint = candidate["mint"]
            last = _last_observed(state, mint)
            if last is not None and current - last < OBSERVATION_INTERVAL:
                cycle["interval_skips"] += 1
                continue

            social: dict[str, Any]
            if cooldown:
                social = {"schema_version": 1, "provider": "x", "source_kind": "unavailable",
                          "mint": mint, "channel_status": "unavailable", "error": "X retry cooldown active",
                          "signals": {}}
                cycle["x_unavailable"] += 1
            elif x_failure_reason is not None:
                social = {"schema_version": 1, "provider": "x", "source_kind": "unavailable",
                          "mint": mint, "channel_status": "unavailable",
                          "error": "X query skipped after first failure: " + x_failure_reason,
                          "signals": {}}
                cycle["x_unavailable"] += 1
            else:
                try:
                    # Count the attempted provider query even when its
                    # response is unavailable; cooldown skips deliberately do
                    # not increment this counter.
                    cycle["x_queries"] += 1
                    social = (social_client or XClient()).search_contract(mint)
                    state.pop("x_retry_not_before", None)
                except Exception as exc:
                    x_failure_reason = _safe_reason(exc)
                    social = {"schema_version": 1, "provider": "x", "source_kind": "unavailable",
                              "mint": mint, "channel_status": "unavailable", "error": x_failure_reason,
                              "signals": {}}
                    cycle["x_unavailable"] += 1
                    state["last_signal_gap"] = {"at": _iso(current), "mint": mint, "channel": "x",
                                                 "reason": x_failure_reason}
                    state["x_retry_not_before"] = _iso(current + X_RETRY_BACKOFF)

            record = _candidate_record(discovery, candidate, social, current)
            _persist_candidate(root, record)
            selected_records.append({"record": record, "social": social})
            for source in list(record.get("evidence_hashes", [])) + [
                item.get("response_sha256") for item in (record.get("entry_quote"), record.get("sell_preflight"), social)
                if isinstance(item, Mapping)
            ]:
                if isinstance(source, str) and len(source) == 64 and all(ch in "0123456789abcdef" for ch in source.lower()):
                    if not any(item.get("response_sha256") == source for item in cycle["source_provenance"]):
                        cycle["source_provenance"].append({"response_sha256": source})
            cycle["source_provenance"] = cycle["source_provenance"][:64]
            observations[mint] = _iso(current)
            state.setdefault("seen", []).append(mint)
            state["seen"] = state["seen"][-500:]
            state["last_candidate"] = record["content_sha256"]
            # Persist each observation before moving to the next selected slot;
            # a later provider or ledger failure must not erase the cohort.
            _write(state_path, state)

        cycle["processed_count"] = len(selected_records)
        cycle["retained_count"] = len(selected_records)
        cycle["entry_route_count"] = sum(1 for item in selected_records if item["record"].get("entry_quote") is not None)
        cycle["sell_preflight_count"] = sum(1 for item in selected_records if item["record"].get("sell_preflight") is not None)
        cycle["m0_eligible_count"] = sum(1 for item in selected_records if _m0_eligible(item["record"]))
        _write(state_path, state)

        chosen = next((item for item in selected_records
                       if item["record"].get("eligible_for_internal_paper") is True
                       and _m0_eligible(item["record"])), None)
        if chosen is None:
            if cooldown and cycle["x_unavailable"]:
                no_fill_reason = "x_signal_unavailable: X retry cooldown active"
            elif x_failure_reason is not None and cycle["x_unavailable"]:
                no_fill_reason = "x_signal_unavailable: " + x_failure_reason
            else:
                no_fill_reason = "no_new_candidate_passed_all_gates"
            _write(state_path, state)
            return done({"status": "abstain", "reason": no_fill_reason, "orders_sent": 0,
                         "candidates": len(candidates), "requests": budget.used, "cycle_id": cycle["cycle_id"]},
                        "abstain", no_fill_reason)

        record = chosen["record"]
        social = chosen["social"]
        mint = record["mint"]
        entry = record["entry_quote"]
        entry_input, _, acquired, _ = _quote(entry, USDC, mint)
        total_cents = _ceil_cents(entry_input) + _ceil_cents(ENTRY_EXTRA_RAW)
        if total_cents > 500:
            raise MemecoinError("entry exceeds crypto idea cap")
        proposal_id = "memecoin-" + record["content_sha256"][:24]
        proposal = Proposal(proposal_id, "crypto", "solana-memecoin-attention", POLICY_VERSION,
                            record["content_sha256"], social["received_at"], "solana:" + mint,
                            "token:solana:" + mint, "token:solana:" + mint, current + timedelta(seconds=90),
                            1, _ceil_cents(entry_input), _ceil_cents(ENTRY_EXTRA_RAW), "observed",
                            {"token_memecoin_v1": {"candidate_content_sha256": record["content_sha256"],
                             "cohort_id": "M0", "acquired_token_raw": str(acquired),
                             "exit_extra_usdc_raw": str(EXIT_EXTRA_RAW)}})
        state["last_candidate"] = record["content_sha256"]
        state["pending"] = {
            "proposal": _proposal_state(proposal), "reservation_id": "",
            "fill_id": "fill-" + proposal_id, "quantity": 1,
            "unit_cost_cents": proposal.unit_cost_cents, "fee_cents": proposal.fee_cents,
            "filled_at": _iso(current), "mint": mint, "acquired_token_raw": str(acquired),
            "candidate_content_sha256": record["content_sha256"],
        }
        _write(state_path, state)
        try:
            try:
                book.market_snapshot(current)
            except InvalidState:
                open_positions = [event.payload.get("position_id") for event in book.audit()
                                  if event.event_type == "fill" and isinstance(event.payload.get("position_id"), str)
                                  and book.get_position(event.payload["position_id"]).state == "open"]
                if open_positions:
                    raise MemecoinError("complete shared marks are unavailable")
                book.record_marks("memecoin-initial-cash-" + record["content_sha256"][:16], [], as_of=current)
            reservation = book.reserve(proposal, now=current)
            state["pending"]["reservation_id"] = reservation.reservation_id
            _write(state_path, state)
            fill = book.simulate_fill(reservation.reservation_id, fill_id="fill-" + proposal_id, quantity=1,
                                      unit_cost_cents=proposal.unit_cost_cents, fee_cents=proposal.fee_cents,
                                      provenance="observed", filled_at=current)
        except sqlite3.OperationalError as exc:
            if _sqlite_busy(exc):
                operation = "reserve_or_fill"
                if state.get("pending", {}).get("reservation_id"):
                    operation = "fill_after_reserve"
                result = _record_ledger_contention(state_path, state, current, exc, operation=operation)
                return done(result, "shared_ledger_busy", "shared_ledger_busy")
            raise
        state["active"] = {"position_id": fill.position_id, "mint": mint, "acquired_token_raw": str(acquired),
                            "opened_at": fill.filled_at, "candidate_content_sha256": record["content_sha256"],
                            "latest_mark": None}
        state.pop("pending", None)
        _write(state_path, state)
        cycle["selected_fill_hash"] = record["content_sha256"]
        cycle["selected_candidate_content_sha256"] = record["content_sha256"]
        cycle["selected_fill_id"] = fill.fill_id
        return done({"status": "simulated_fill", "reservation": asdict(reservation), "fill": asdict(fill),
                     "candidate_content_sha256": record["content_sha256"], "orders_sent": 0,
                     "cycle_id": cycle["cycle_id"]}, "simulated_fill", "selected_m0_candidate")
    except Exception as exc:
        if terminal_status == "operational_failure" and terminal_reason == "discovery cycle failed":
            terminal_status, terminal_reason = "operational_failure", _safe_reason(exc)
        raise
    finally:
        if not finalized:
            _finish_cycle(root, cycle, current, status=terminal_status, reason=terminal_reason,
                          request_count=budget.used + cycle["x_queries"])
        else:
            # ``done`` records the logical result; publish in one place so a
            # cycle exists for every return path.
            _finish_cycle(root, cycle, current, status=terminal_status, reason=terminal_reason,
                          request_count=budget.used + cycle["x_queries"])


def _program_ledger(ledger_path, current, crypto_sleeve_cents):
    """Create an isolated paper ledger for a memecoin-only program sleeve.

    The shared firm ledger remains unchanged.  The three one-cent placeholder
    accounts satisfy the immutable four-desk ledger schema; this runtime only
    ever proposes on the crypto account and reports that account separately.
    """
    if isinstance(crypto_sleeve_cents, bool) or not isinstance(crypto_sleeve_cents, int) or crypto_sleeve_cents < 500:
        raise MemecoinError("crypto program sleeve must be at least 500 cents")
    balances = {"prediction": 1, "equities": 1, "options": 1, "crypto": crypto_sleeve_cents}
    limits = RiskLimits(aggregate_cap_cents=crypto_sleeve_cents, group_cap_cents=crypto_sleeve_cents,
                        per_idea_cents={"prediction": 1, "equities": 1, "options": 1, "crypto": 500})
    return PaperLedger(ledger_path, balances=balances, limits=limits,
                       enforce_market_risk=True, clock=lambda: current)


def run_once(state_dir="/var/lib/neorome-token-paper", ledger_path="/var/lib/neorome-paper/paper.sqlite3",
             *, now=None, sources=None, quotes=None, social_client=None, crypto_sleeve_cents=None):
    current = _time(now or datetime.now(UTC)); root = _root(state_dir); state_path = root / "state.json"
    with (root / "runtime.lock").open("a") as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: return {"status": "already_running", "orders_sent": 0}
        state = _load(state_path)
        try:
            book = (_program_ledger(ledger_path, current, crypto_sleeve_cents)
                    if crypto_sleeve_cents is not None
                    else PaperLedger(ledger_path, enforce_market_risk=True, clock=lambda: current))
        except sqlite3.OperationalError as exc:
            if _sqlite_busy(exc):
                return _record_ledger_contention(state_path, state, current, exc, operation="open_ledger")
            raise
        try:
            recovered = _resume_pending(book, state, current)
        except sqlite3.OperationalError as exc:
            if _sqlite_busy(exc):
                return _record_ledger_contention(state_path, state, current, exc, operation="resume_pending")
            raise
        if recovered is not None:
            _write(state_path, state)
            return recovered
        active = state.get("active")
        if isinstance(active, dict):
            try:
                position = book.get_position(active["position_id"])
            except sqlite3.OperationalError as exc:
                if _sqlite_busy(exc):
                    return _record_ledger_contention(state_path, state, current, exc, operation="read_active_position")
                raise
            if position.state != "open":
                state["active"] = None; _write(state_path, state)
                return {"status": "reconciled_closed", "orders_sent": 0}
            quote_client = quotes or QuoteClient()
            route = quote_client.quote(active["mint"], USDC, active["acquired_token_raw"])
            _, _, minimum, impact = _quote(route, active["mint"], USDC, int(active["acquired_token_raw"]))
            net_raw = max(0, minimum - EXIT_EXTRA_RAW)
            mark = {"received_at": route["received_at"], "source_hash": route["response_sha256"],
                    "minimum_output_usdc_raw": str(minimum), "exit_extra_usdc_raw": str(EXIT_EXTRA_RAW),
                    "gross_proceeds_cents": _floor_cents(minimum),
                    "exit_fee_cents": min(_floor_cents(minimum), EXIT_FEE_CENTS),
                    "liquidation_value_cents": _floor_cents(net_raw), "price_impact_percentage_points": impact}
            active["latest_mark"] = mark; state["active"] = active; _write(state_path, state)
            try:
                # Use the provider receipt as the batch timestamp.  This keeps
                # retries of the same route idempotent even if the timer wakes
                # a few seconds later, while record_marks still validates the
                # source freshness against the ledger clock.
                observed_mark = token_mark(book, position, current, root)
                book.record_marks(_mark_batch_id(position.position_id, mark), [observed_mark],
                                  as_of=observed_mark.source_timestamp)
            except sqlite3.OperationalError as exc:
                if _sqlite_busy(exc):
                    return _record_ledger_contention(state_path, state, current, exc,
                                                     operation="record_active_mark")
                raise
            age = current - _time(active["opened_at"]); basis = Decimal(position.basis_cents)
            value = Decimal(mark["liquidation_value_cents"])
            reason = "max_hold_60m" if age >= MAX_HOLD else ("stop_50pct" if value <= basis * STOP_FRACTION else ("take_2x" if value >= basis * TAKE_FRACTION else None))
            if reason:
                gross_proceeds_cents = int(mark["gross_proceeds_cents"])
                exit_fee_cents = int(mark["exit_fee_cents"])
                try:
                    close = book.close(position.position_id, close_id="memecoin-close-" + route["response_sha256"][:24],
                                       proceeds_cents=gross_proceeds_cents, fee_cents=exit_fee_cents,
                                       provenance="observed", closed_at=current)
                except sqlite3.OperationalError as exc:
                    if _sqlite_busy(exc):
                        return _record_ledger_contention(state_path, state, current, exc, operation="close_active_position")
                    raise
                state["active"] = None; state.setdefault("closed", []).append({"reason": reason, "close": asdict(close), "route": mark}); _write(state_path, state)
                return {"status": "simulated_close", "reason": reason, "close": asdict(close), "orders_sent": 0}
            return {"status": "marked_open", "mark": mark, "orders_sent": 0}
        return _run_discovery_cycle(root, state, state_path, book, current,
                                    sources=sources, quotes=quotes, social_client=social_client)


def main(argv=None):
    parser = argparse.ArgumentParser(); parser.add_argument("--state", default="/var/lib/neorome-token-paper")
    parser.add_argument("--ledger", default="/var/lib/neorome-paper/paper.sqlite3")
    parser.add_argument("--crypto-sleeve-cents", type=int)
    args = parser.parse_args(argv)
    try: result = run_once(args.state, args.ledger, crypto_sleeve_cents=args.crypto_sleeve_cents)
    except Exception as exc: result = {"status": "operational_failure", "error_type": type(exc).__name__, "orders_sent": 0}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 1 if result["status"] == "operational_failure" else 0


if __name__ == "__main__": raise SystemExit(main())
