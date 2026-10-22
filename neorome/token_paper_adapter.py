"""Deterministic offline memecoin paper rehearsal on an isolated ledger.

The adapter accepts only synthetic, prospectively timestamped research
evidence.  It has no network, wallet, signing, swap, scheduler, or shared
runtime integration.
"""

from __future__ import annotations

import contextlib
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import secrets
import tempfile
from typing import Any, Mapping, Sequence

from .crypto_research import TOKEN_PROGRAM
from .jupiter_quotes import USDC
from .paper_ledger import InvalidState, MarketMark, PaperLedger, Proposal
from .token_outcomes import OutcomeError, evaluate, input_sha256

UTC = timezone.utc
SCHEMA_VERSION = 1
POLICY_VERSION = "token-paper-rehearsal-v1"
COST_VERSION = "token-paper-costs-v1"
CONTROL_VERSION = "token-risk-review-v1"
FULL_LOSS_VERSION = "token-full-loss-review-v1"
MAX_IDEA_USDC_RAW = 5_000_000


class TokenPaperError(ValueError):
    pass


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TokenPaperError("evidence must contain JSON values only") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _hash(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise TokenPaperError(f"{name} must be a SHA-256 digest")
    return value.lower()


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TokenPaperError(f"{name} must be a non-empty string")
    return value.strip()


def _time(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc: raise TokenPaperError(f"{name} must be an ISO timestamp") from exc
    else: raise TokenPaperError(f"{name} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TokenPaperError(f"{name} requires a timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _raw_int(value: Any, name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).isdecimal():
        raise TokenPaperError(f"{name} must be integer base units")
    result = int(value)
    if result < (1 if positive else 0) or result > 2**63 - 1:
        raise TokenPaperError(f"{name} is outside its allowed range")
    return result


def _ceil_cents(raw: int) -> int:
    return (raw + 9_999) // 10_000


def _floor_cents(raw: int) -> int:
    return raw // 10_000


def _costs(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("version") != COST_VERSION:
        raise TokenPaperError("token-paper-costs-v1 assumptions are required")
    if value.get("excludes_embedded_route_fees") is not True or set(value) != {
        "version", "excludes_embedded_route_fees", "entry_extra_usdc_raw", "exit_extra_usdc_raw"
    }:
        raise TokenPaperError("cost treatment is unsupported")
    return {"version": COST_VERSION, "excludes_embedded_route_fees": True,
            "entry_extra_usdc_raw": str(_raw_int(value["entry_extra_usdc_raw"], "entry cost")),
            "exit_extra_usdc_raw": str(_raw_int(value["exit_extra_usdc_raw"], "exit cost"))}


def _risk_review(value: Any, *, chain: str, mint: str, entry_hash: str,
                 sell_hash: str, now: datetime, sell_received: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping): raise TokenPaperError("versioned token risk review is required")
    clean = dict(value); supplied = _hash(clean.pop("review_sha256", None), "risk review hash")
    if supplied != _digest(clean): raise TokenPaperError("token risk review hash mismatch")
    required = {"version", "source_kind", "chain", "mint", "reviewed_at", "token_program",
                "mint_authority", "freeze_authority", "transfer_restrictions",
                "sellability", "entry_source_hash", "sell_source_hash",
                "owner_concentration_reviewed", "liquidity_reviewed", "evidence_hashes"}
    if set(clean) != required or clean.get("version") != CONTROL_VERSION or clean.get("source_kind") != "synthetic":
        raise TokenPaperError("risk review schema or provenance is unsupported")
    if (clean.get("chain"), clean.get("mint")) != (chain, mint):
        raise TokenPaperError("risk review token identity mismatch")
    reviewed = _time(clean.get("reviewed_at"), "risk reviewed_at")
    if reviewed < sell_received or reviewed > now:
        raise TokenPaperError("risk review is not prospectively timed")
    if (clean.get("token_program") != TOKEN_PROGRAM or clean.get("mint_authority") is not None
            or clean.get("freeze_authority") is not None
            or clean.get("transfer_restrictions") != "none_reviewed"
            or clean.get("sellability") != "jupiter_size_matched_minimum_output"
            or clean.get("entry_source_hash") != entry_hash
            or clean.get("sell_source_hash") != sell_hash
            or clean.get("owner_concentration_reviewed") is not True
            or clean.get("liquidity_reviewed") is not True):
        raise TokenPaperError("token control or sellability review did not pass")
    hashes = clean.get("evidence_hashes")
    if not isinstance(hashes, Mapping) or set(hashes) != {"mint", "accounts", "pool", "transfer"}:
        raise TokenPaperError("complete token-control evidence hashes are required")
    for key, digest in hashes.items(): _hash(digest, f"risk evidence {key}")
    return {**clean, "review_sha256": supplied, "reviewed_at": _iso(reviewed)}


def _route_latency(record: Mapping[str, Any], name: str) -> None:
    requested = _time(record.get("requested_at"), f"{name}.requested_at")
    received = _time(record.get("received_at"), f"{name}.received_at")
    if received < requested or received - requested > timedelta(seconds=2):
        raise TokenPaperError(f"{name} route latency is unsupported")


def _preflight(record: Any, *, chain: str, mint: str, quantity: int,
               entry_received: datetime, now: datetime) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise TokenPaperError("independent pre-entry sellability evidence is required")
    _route_latency(record, "preflight")
    requested = _time(record.get("requested_at"), "preflight.requested_at")
    received = _time(record.get("received_at"), "preflight.received_at")
    if requested < entry_received or received > now:
        raise TokenPaperError("preflight route is not prospectively timed")
    quote = record.get("quote")
    if not isinstance(quote, Mapping) or record.get("chain") != chain:
        raise TokenPaperError("preflight route chain identity mismatch")
    if (quote.get("inputMint"), quote.get("outputMint")) != (mint, USDC):
        raise TokenPaperError("preflight route token identity mismatch")
    if _raw_int(quote.get("inAmount"), "preflight input", positive=True) != quantity:
        raise TokenPaperError("preflight route is not size-matched")
    output = _raw_int(quote.get("outAmount"), "preflight output", positive=True)
    minimum = _raw_int(quote.get("otherAmountThreshold"), "preflight minimum", positive=True)
    if minimum > output:
        raise TokenPaperError("preflight minimum exceeds quoted output")
    if quote.get("priceImpactUnit") != "percentage_points":
        raise TokenPaperError("preflight priceImpact units are unsupported")
    impact_value = quote.get("priceImpact")
    if isinstance(impact_value, bool) or not isinstance(impact_value, (str, int, float)):
        raise TokenPaperError("preflight priceImpact is invalid")
    try:
        impact = Decimal(str(impact_value))
    except (InvalidOperation, ValueError) as exc:
        raise TokenPaperError("preflight priceImpact is invalid") from exc
    if not impact.is_finite() or abs(impact) > Decimal("2"):
        raise TokenPaperError("preflight priceImpact is outside the replay bound")
    return {"chain": chain, "mint": mint, "requested_at": _iso(requested),
            "received_at": _iso(received),
            "source_hash": _hash(record.get("response_sha256", record.get("source_sha256")),
                                 "preflight source hash"),
            "input_token_raw": str(quantity), "quoted_output_usdc_raw": str(output),
            "minimum_output_usdc_raw": str(minimum), "price_impact": str(impact),
            "observation": dict(record)}


def _firm_ledger(book: PaperLedger) -> None:
    limits = book.limits
    balances = book.balances()
    crypto = next((row for row in balances if row.desk == "crypto"), None)
    if (not book.enforce_market_risk or limits.per_idea_cents.get("crypto", 0) > 500
            or limits.group_cap_cents > 3_000 or limits.aggregate_cap_cents > 8_000
            or crypto is None or crypto.initial_cash_cents > 10_000
            or sum(row.initial_cash_cents for row in balances) > 80_000):
        raise TokenPaperError("firm paper caps or market-risk enforcement are too loose")


def _temporary_root(value: str | os.PathLike[str]) -> Path:
    root = Path(value).resolve()
    allowed = [Path(tempfile.gettempdir()).resolve(), Path("/private/tmp"), Path("/tmp")]
    if not any(root == base or base in root.parents for base in allowed):
        raise TokenPaperError("token rehearsal state must use an isolated temporary directory")
    if root.exists() and root.is_symlink(): raise TokenPaperError("state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    return root


def _write(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            descriptor = -1; stream.write(_canonical(value) + "\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0: os.close(descriptor)
        with contextlib.suppress(FileNotFoundError): temporary.unlink()


class TokenPaperAdapter:
    """One isolated synthetic token rehearsal with durable restart state."""

    def __init__(self, state_dir: str | os.PathLike[str]) -> None:
        self.root = _temporary_root(state_dir)
        self.state_path = self.root / "token_state.json"
        self.ledger = PaperLedger(self.root / "token_paper.sqlite3", enforce_market_risk=True)
        _firm_ledger(self.ledger)

    def _load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"schema_version": SCHEMA_VERSION, "policy_version": POLICY_VERSION,
                    "synthetic": True, "research_only": True, "history": []}
        try: value = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError) as exc: raise TokenPaperError("state is unreadable") from exc
        if (not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION
                or value.get("policy_version") != POLICY_VERSION):
            raise TokenPaperError("state schema is unsupported")
        return value

    def _reconcile(self, state: dict[str, Any]) -> None:
        audits = tuple(self.ledger.audit())
        pending = state.get("pending")
        if not isinstance(pending, Mapping): return
        kind = pending.get("kind"); identity = pending.get("identity")
        event_type = {"reserve": "reserve", "fill": "fill", "close": "close"}.get(kind)
        if kind == "reserve":
            event = next((row for row in audits if row.event_type == "reserve"
                          and isinstance(row.payload.get("payload"), Mapping)
                          and row.payload["payload"].get("proposal_id") == identity), None)
        else:
            event = next((row for row in audits if row.event_type == event_type and row.entity_id == identity), None)
        if event is None: return
        state["history"].append({"event": f"{kind}_recovered", "evidence": dict(pending)})
        if kind == "reserve": state["reservation_id"] = event.entity_id
        if kind == "fill": state["position_id"] = event.payload.get("position_id")
        if kind == "close": state["status"] = pending.get("terminal_status", "closed")
        state.pop("pending", None); _write(self.state_path, state)

    def _bundle(self, discovery: Mapping[str, Any], preflight_observation: Mapping[str, Any],
                routes: Sequence[Mapping[str, Any]],
                costs: Mapping[str, Any], horizon: int, risk_review: Mapping[str, Any], now: datetime) -> dict[str, Any]:
        if discovery.get("execution_enabled") is not False or discovery.get("schema_version") != 1:
            raise TokenPaperError("research-only discovery record is required")
        candidates = discovery.get("candidates")
        if not isinstance(candidates, list) or len(candidates) != 1:
            raise TokenPaperError("rehearsal requires exactly one frozen candidate")
        candidate = candidates[0]
        if not isinstance(candidate, Mapping) or candidate.get("research_status") != "observed_pending_risk_review":
            raise TokenPaperError("candidate was not prospectively observed")
        if candidate.get("screen_rejections") != []:
            raise TokenPaperError("screen-rejected candidate cannot enter rehearsal")
        cost = _costs(costs)
        available_routes = []
        for index, route_record in enumerate(routes):
            if not isinstance(route_record, Mapping):
                raise TokenPaperError(f"outcome route {index} must be an object")
            received_at = _time(route_record.get("received_at"), f"outcome route {index} received_at")
            if received_at <= now:
                available_routes.append(route_record)
        try: outcome = evaluate(discovery, available_routes, costs=cost)
        except OutcomeError as exc: raise TokenPaperError(str(exc)) from exc
        row = outcome["cohort"][0]
        if horizon not in (15, 60, 240): raise TokenPaperError("horizon must be preregistered")
        selected = next(item for item in row["horizons"] if item["horizon_minutes"] == horizon)
        if row.get("status") != "observed_pending" or not isinstance(row.get("entry"), Mapping):
            raise TokenPaperError("entry route evidence is unresolved")
        entry = row["entry"]
        candidate_received = _time(row["received_at"], "candidate received_at")
        entry_requested = _time(entry["requested_at"], "entry requested_at")
        entry_received = _time(entry["received_at"], "entry received_at")
        if not candidate_received <= entry_requested <= entry_received <= now:
            raise TokenPaperError("discovery and entry are not prospective")
        raw_entry = candidate.get("entry_observation")
        if not isinstance(raw_entry, Mapping):
            quotes = candidate.get("quotes", {}); observations = quotes.get("observations", []) if isinstance(quotes, Mapping) else []
            raw_entry = observations[0] if observations and isinstance(observations[0], Mapping) else None
        if not isinstance(raw_entry, Mapping): raise TokenPaperError("raw entry route is missing")
        _route_latency(raw_entry, "entry")
        if raw_entry.get("chain") != row["chain"]:
            raise TokenPaperError("entry route chain identity must be explicit")
        preflight = _preflight(preflight_observation, chain=row["chain"], mint=row["mint"],
                               quantity=int(entry["acquired_raw"]),
                               entry_received=entry_received, now=now)
        if preflight["source_hash"] == entry["source_hash"]:
            raise TokenPaperError("entry and sellability require independent route evidence")
        review = _risk_review(risk_review, chain=row["chain"], mint=row["mint"],
                              entry_hash=entry["source_hash"], sell_hash=preflight["source_hash"],
                              now=now, sell_received=_time(preflight["received_at"], "preflight received_at"))
        total = (int(entry["input_usdc_raw"]) + int(cost["entry_extra_usdc_raw"])
                 + int(cost["exit_extra_usdc_raw"]))
        if total > MAX_IDEA_USDC_RAW:
            raise TokenPaperError("all-in rehearsal cost exceeds the $5 idea cap")
        return {"outcome": outcome, "row": row, "selected": selected, "costs": cost,
                "chain": row["chain"], "mint": row["mint"], "entry": entry, "review": review,
                "preflight": preflight, "outcome_routes": tuple(available_routes)}

    def run(self, *, discovery: Mapping[str, Any] | None = None,
            preflight_observation: Mapping[str, Any] | None = None,
            risk_review: Mapping[str, Any] | None = None, costs: Mapping[str, Any] | None = None,
            sell_observations: Sequence[Mapping[str, Any]] = (), horizon_minutes: int = 15,
            action: str = "mark", full_loss_review: Mapping[str, Any] | None = None,
            allow_entry: bool = True, recovery_ok: bool = True,
            now: datetime | str | None = None) -> dict[str, Any]:
        current = _time(now or datetime.now(UTC), "now")
        if action not in {"mark", "exit", "full_loss"}: raise TokenPaperError("unsupported rehearsal action")
        state = self._load(); self._reconcile(state)
        if str(state.get("status", "")).startswith("closed"):
            return state
        if discovery is None: discovery = state.get("discovery")
        if costs is None: costs = state.get("costs")
        if risk_review is None: risk_review = state.get("risk_review")
        if preflight_observation is None: preflight_observation = state.get("preflight_observation")
        if (not isinstance(discovery, Mapping) or not isinstance(preflight_observation, Mapping)
                or not isinstance(costs, Mapping) or not isinstance(risk_review, Mapping)):
            state.update(status="abstain", blocker="complete synthetic research evidence is required")
            _write(self.state_path, state); return state
        combined_routes = list(sell_observations)
        bundle = self._bundle(discovery, preflight_observation, combined_routes, costs,
                              horizon_minutes, risk_review, current)
        state.update({"discovery": dict(discovery), "discovery_sha256": input_sha256(discovery),
                      "costs": bundle["costs"], "risk_review": dict(risk_review),
                      "chain": bundle["chain"], "mint": bundle["mint"], "research_only": True,
                      "preflight_observation": dict(preflight_observation),
                      "preflight_source_hash": bundle["preflight"]["source_hash"],
                      "synthetic": True, "execution_enabled": False, "paper_admission_evidence": False})
        open_positions = [event.payload["position_id"] for event in self.ledger.audit() if event.event_type == "fill"
                          and self.ledger.get_position(event.payload["position_id"]).state == "open"]
        reservations = [event.entity_id for event in self.ledger.audit() if event.event_type == "reserve"
                        and self.ledger.get_reservation(event.entity_id).state == "reserved"]
        if not open_positions:
            if reservations and (not allow_entry or not recovery_ok or self.ledger.risk_stop_active()):
                for reservation_id in reservations:
                    self.ledger.cancel(reservation_id, reason="STOP or recovery pause", cancelled_at=current)
                state.update(status="entries_disabled", risk_retained=False); _write(self.state_path, state); return state
            if not allow_entry or not recovery_ok or self.ledger.risk_stop_active():
                state.update(status="entries_disabled", risk_retained=False); _write(self.state_path, state); return state
            entry = bundle["entry"]
            fill_time = current
            try: self.ledger.market_snapshot(fill_time)
            except InvalidState: self.ledger.record_marks("token-rehearsal-initial-cash", [], as_of=fill_time)
            proposal_id = "token-rehearsal-" + state["discovery_sha256"][:24]
            entry_raw = int(entry["input_usdc_raw"]); entry_extra = int(bundle["costs"]["entry_extra_usdc_raw"])
            decision_hash = _digest({"entry_source_hash": entry["source_hash"],
                                     "preflight_source_hash": bundle["preflight"]["source_hash"],
                                     "risk_review_sha256": bundle["review"]["review_sha256"],
                                     "costs": bundle["costs"]})
            proposal = Proposal(proposal_id, "crypto", "memecoin-route-rehearsal", POLICY_VERSION,
                                decision_hash, _iso(fill_time), f"{bundle['chain']}:{bundle['mint']}",
                                f"token:{bundle['chain']}:{bundle['mint']}", f"token:{bundle['chain']}:{bundle['mint']}",
                                _iso(fill_time + timedelta(seconds=60)), 1,
                                _ceil_cents(entry_raw), _ceil_cents(entry_extra),
                                "synthetic", {"token_paper_v1": {"chain": bundle["chain"], "mint": bundle["mint"],
                                "acquired_raw": entry["acquired_raw"], "costs": bundle["costs"],
                                "research_only": True, "review_sha256": bundle["review"]["review_sha256"]}})
            state["pending"] = {"kind": "reserve", "identity": proposal_id, "proposal": asdict(proposal)}; _write(self.state_path, state)
            reservation = self.ledger.reserve(proposal, now=fill_time)
            state["reservation_id"] = reservation.reservation_id; state.pop("pending", None); _write(self.state_path, state)
            fill_id = "token-fill-" + proposal_id
            state["pending"] = {"kind": "fill", "identity": fill_id, "reservation_id": reservation.reservation_id}; _write(self.state_path, state)
            fill = self.ledger.simulate_fill(reservation.reservation_id, fill_id=fill_id, quantity=1,
                                             unit_cost_cents=proposal.unit_cost_cents, fee_cents=proposal.fee_cents,
                                             provenance="synthetic", filled_at=fill_time)
            state["position_id"] = fill.position_id; state["history"].append({"event": "synthetic_entry", "fill": asdict(fill)})
            state.pop("pending", None); open_positions = [fill.position_id]; _write(self.state_path, state)
        position_id = open_positions[0]
        position = self.ledger.get_position(position_id)
        if action == "full_loss":
            review = dict(full_loss_review or {}); supplied = _hash(review.pop("review_sha256", None), "full loss review hash")
            reviewed_at = _time(review.get("reviewed_at"), "full loss reviewed_at")
            if (supplied != _digest(review) or review.get("version") != FULL_LOSS_VERSION
                    or (review.get("chain"), review.get("mint")) != (state["chain"], state["mint"])
                    or review.get("decision") != "realize_full_loss"
                    or reviewed_at < _time(position.opened_at, "position opened_at")
                    or reviewed_at > current):
                raise TokenPaperError("explicit full-loss review is invalid")
            close_id = "token-full-loss-" + position_id
            state["pending"] = {"kind": "close", "identity": close_id,
                                "terminal_status": "closed_full_loss",
                                "review": {**review, "review_sha256": supplied}}; _write(self.state_path, state)
            closed = self.ledger.close(position_id, close_id=close_id, proceeds_cents=0, fee_cents=0,
                                       provenance="synthetic", closed_at=current)
            state["history"].append({"event": "explicit_full_loss", "close": asdict(closed)})
            state.pop("pending", None); state.update(status="closed_full_loss", risk_retained=False); _write(self.state_path, state); return state
        selected = bundle["selected"]
        if selected.get("status") != "modeled_outcome":
            state.update(status="unresolved_sell_route", risk_retained=True,
                         blocker="missing or unusable conservative sell route; no invented liquidation")
            _write(self.state_path, state); return state
        route = selected["route_observation"]
        selected_received = _time(route["received_at"], "selected sell received_at")
        selected_hash = _hash(route.get("source_hash"), "selected sell source hash")
        selected_raw = next((item for item in bundle["outcome_routes"] if isinstance(item, Mapping)
                             and item.get("response_sha256", item.get("source_sha256")) == selected_hash), None)
        if selected_raw is None:
            raise TokenPaperError("selected sell route lacks its raw evidence")
        if selected_raw.get("chain") != bundle["chain"]:
            raise TokenPaperError("selected sell route chain identity must be explicit")
        _route_latency(selected_raw, "selected sell")
        if selected_received > current or selected_hash == bundle["entry"]["source_hash"]:
            raise TokenPaperError("selected sell route is future-dated or not independent")
        minimum = int(selected["minimum_output_usdc_raw"])
        exit_extra = int(bundle["costs"]["exit_extra_usdc_raw"])
        if exit_extra > minimum:
            state.update(status="unresolved_sell_route", risk_retained=True,
                         blocker="modeled exit cost exceeds conservative proceeds; liability is unresolved")
            _write(self.state_path, state); return state
        proceeds_raw = minimum - exit_extra
        mark = MarketMark(position_id, _floor_cents(proceeds_raw), route["received_at"], route["source_hash"], "synthetic")
        self.ledger.record_marks("token-mark-" + route["source_hash"][:24], [mark], as_of=route["received_at"])
        state["history"].append({"event": "synthetic_mark", "mark": asdict(mark), "horizon_minutes": horizon_minutes})
        if action == "mark":
            state.update(status="marked_open", risk_retained=True); _write(self.state_path, state); return state
        close_id = "token-route-exit-" + route["source_hash"][:24]
        state["pending"] = {"kind": "close", "identity": close_id,
                            "terminal_status": "closed_route", "route": dict(route)}; _write(self.state_path, state)
        closed = self.ledger.close(position_id, close_id=close_id, proceeds_cents=_floor_cents(proceeds_raw),
                                   fee_cents=0, provenance="synthetic", closed_at=current)
        state["history"].append({"event": "synthetic_route_exit", "close": asdict(closed)})
        state.pop("pending", None); state.update(status="closed_route", risk_retained=False); _write(self.state_path, state); return state


__all__ = ["TokenPaperAdapter", "TokenPaperError", "POLICY_VERSION"]
