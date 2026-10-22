"""Deterministic, offline end-to-end rehearsal for the options desk.

The rehearsal is intentionally a composition boundary.  It accepts normalized
selection evidence, an explicit all-or-none entry event, and two independently
timestamped exit quote bundles.  It routes those values through the existing
selector, options gateway, paper ledger, reconciliation, and lifecycle code.
No provider, broker, scheduler, or profitability evaluator is called.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any, Mapping, Sequence

from desks.options.selection import select
from neorome.options_gateway import convert_options_proposal, reserve_options_proposal
from neorome.options_lifecycle import OptionsLifecycle
from neorome.paper_gateway import PaperProposalGateway
from neorome.paper_ledger import PaperLedger, RiskLimits

UTC = timezone.utc
OPTIONS_CASH_CENTS = 30_000
PER_IDEA_CENTS = 1_500
GROUP_CAP_CENTS = 3_000
AGGREGATE_CAP_CENTS = 8_000


class OptionsShadowRehearsalError(ValueError):
    """The caller did not provide a complete normalized rehearsal contract."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _stamp(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsShadowRehearsalError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OptionsShadowRehearsalError(f"{field} must be an object")
    return value


def _cost_cents(value: Any, field: str) -> int:
    """Convert the selector's per-leg dollar assumption to exact cents."""
    if isinstance(value, bool):
        raise OptionsShadowRehearsalError(f"{field} must be an exact nonnegative cent amount")
    try:
        cents = Decimal(str(value)) * Decimal(100)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OptionsShadowRehearsalError(f"{field} must be an exact nonnegative cent amount") from exc
    if not cents.is_finite() or cents < 0 or cents != cents.to_integral_value():
        raise OptionsShadowRehearsalError(f"{field} must be an exact nonnegative cent amount")
    return int(cents)


def _source(kind: str, material: Any, observed_at: datetime, received_at: datetime) -> dict[str, Any]:
    normalized = {"kind": kind, "material": material}
    return {"kind": kind, "hash": _digest(normalized), "observed_at": _iso(observed_at),
            "received_at": _iso(received_at)}


def _expiry_at(expiry: str, now: datetime) -> datetime:
    try:
        day = date.fromisoformat(expiry)
    except (TypeError, ValueError) as exc:
        raise OptionsShadowRehearsalError("selected expiry must be an ISO date") from exc
    result = datetime.combine(day, time(20, 0), UTC)
    if result <= now:
        raise OptionsShadowRehearsalError("selected expiry is not in the future")
    return result


def _raw_leg(row: Mapping[str, Any], side: str, *, timestamp: datetime) -> dict[str, Any]:
    return {
        "symbol": str(row["symbol"]).replace(" ", ""), "underlying": row["underlying"],
        "right": row["right"], "expiry": row["expiry"], "strike": row["strike"],
        "side": side, "quantity": 1, "bid": row["bid"], "ask": row["ask"],
        "timestamp": _iso(timestamp), "multiplier": 100, "adjusted": False,
        "bid_size": row["bid_size"], "ask_size": row["ask_size"],
    }


def _entry_record(raw: Mapping[str, Any], proposal: Mapping[str, Any], fill: Mapping[str, Any],
                  filled_at: datetime) -> dict[str, Any]:
    fill_event = deepcopy(dict(fill["fill_event"]))
    fill_input_digest = _digest(fill_event)
    entry_legs = deepcopy(list(raw["legs"]))
    for leg in entry_legs:
        leg["timestamp"] = _iso(filled_at)
    record = {
        "position_id": fill["fill"]["position_id"], "proposal_id": proposal["proposal_id"],
        "strategy": proposal["strategy"], "strategy_version": proposal["strategy_version"],
        "instrument": proposal["instrument"], "exposure_group": proposal["exposure_group"],
        "idea_id": proposal["idea_id"], "quantity": fill["fill"]["quantity"],
        "ledger_cost_cents": fill["fill"]["total_cost_cents"],
        "ledger_fee_cents": fill["fill"]["fee_cents"], "entry_as_of": _iso(filled_at),
        "legs": entry_legs, "raw_input": deepcopy(dict(raw)),
        "raw_input_digest": _digest(raw), "fill_input": fill_event,
        "fill_input_digest": fill_input_digest,
    }
    return record


def _report_and_snapshot(legs: Sequence[Mapping[str, Any]], position_qty: int,
                         filled_at: datetime, observed_at: datetime,
                         reports: Sequence[Mapping[str, Any]] | None,
                         holdings_snapshot: Mapping[str, Any] | None) -> tuple[list[Mapping[str, Any]], Mapping[str, Any]]:
    if reports is None or holdings_snapshot is None:
        raise OptionsShadowRehearsalError("caller-supplied reports and complete holdings_snapshot are required")
    return list(reports), holdings_snapshot


def _refusal(reason_code: str, reason: str, *, selection: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "status": "refused", "lifecycle_status": "not_started",
        "admission_status": "not_admitted", "profitability_status": "not_evaluated",
        "reason_code": reason_code, "reason": reason,
        "selection": deepcopy(dict(selection)) if selection is not None else None,
    }


@dataclass(frozen=True)
class OptionsShadowRehearsal:
    """Run one isolated options rehearsal against a temporary or supplied state directory."""

    state_dir: str | Path | None = None

    def run(
        self,
        *,
        observation: Mapping[str, Any],
        context: Mapping[str, Any],
        candidates: Sequence[Mapping[str, Any]],
        reports: Sequence[Mapping[str, Any]] | None,
        holdings_snapshot: Mapping[str, Any] | None,
        exit_marks: Sequence[Mapping[str, Any]],
        fee_per_leg: Any = "0",
        slippage_per_leg: Any = "0",
        proposal_id: str | None = None,
        idea_id: str | None = None,
        fill_id: str = "shadow-entry-001",
    ) -> dict[str, Any]:
        """Select, reserve, fill, mark, close, and replay one paper spread.

        ``exit_marks`` must contain two mappings with ``quotes``,
        ``quote_sources`` and ``now``.  The first creates the lifecycle exit
        intent and the second supplies the independent later quote required for
        close.  Reports and holdings are never synthesized by this method.
        """
        if not isinstance(exit_marks, Sequence) or isinstance(exit_marks, (str, bytes)) or len(exit_marks) != 2:
            return _refusal("two_exit_quote_bundles_required", "two independent exit quote bundles are required")
        selected = select(observation, context, candidates, fee_per_leg=fee_per_leg,
                          slippage_per_leg=slippage_per_leg)
        if selected.get("status") != "candidate":
            return _refusal(str(selected.get("reason_code", "selection_refused")),
                            str(selected.get("reason", "selection refused")), selection=selected)
        try:
            as_of = _stamp(context["now"])
            long = selected["long"]
            short = selected["short"]
            raw_legs = [_raw_leg(long, "buy", timestamp=_stamp(long["timestamp"])),
                        _raw_leg(short, "sell", timestamp=_stamp(short["timestamp"]))]
            quote_observed_at = min(_stamp(long["timestamp"]), _stamp(short["timestamp"]))
            raw = {
                "proposal_id": proposal_id or f"shadow-{_digest(selected)[:20]}",
                "strategy": "opt-001-directional-vertical", "strategy_version": selected["strategy_version"],
                "source_kind": "synthetic", "source": _source("synthetic", selected, quote_observed_at, as_of),
                "idea_id": idea_id or f"options:{selected['symbol']}:{selected['expiry']}:{selected['direction']}",
                "expiry_at": _iso(_expiry_at(selected["expiry"], as_of)), "execution": {"delay_seconds": 10},
                "legs": raw_legs, "metadata": {"shadow_rehearsal": True, "selection_evidence_ids": selected["evidence_ids"]},
            }
            with tempfile.TemporaryDirectory(prefix="options-shadow-") as owned:
                root = Path(self.state_dir) if self.state_dir is not None else Path(owned)
                root.mkdir(parents=True, exist_ok=True)
                ledger_path = root / "paper.sqlite3"
                limits = RiskLimits(aggregate_cap_cents=AGGREGATE_CAP_CENTS,
                                    group_cap_cents=GROUP_CAP_CENTS,
                                    per_idea_cents={"prediction": 500, "equities": PER_IDEA_CENTS,
                                                    "options": PER_IDEA_CENTS, "crypto": 500})
                balances = {"prediction": 10_000, "equities": 30_000,
                            "options": OPTIONS_CASH_CENTS, "crypto": 10_000}
                ledger = PaperLedger(ledger_path, balances=balances, limits=limits,
                                     enforce_market_risk=True, clock=lambda: as_of)
                ledger.record_marks("shadow-initial-mark", [], as_of)
                gateway = PaperProposalGateway(ledger, source_kind="synthetic", clock=lambda: as_of)
                fee_cents = _cost_cents(fee_per_leg, "fee_per_leg")
                slippage_cents = _cost_cents(slippage_per_leg, "slippage_per_leg")
                converted = convert_options_proposal(
                    raw, as_of=as_of, fee_cents_per_contract=fee_cents,
                    slippage_cents_per_contract=slippage_cents,
                )
                reserved = reserve_options_proposal(raw, gateway, as_of=as_of, now=as_of,
                                                    fee_cents_per_contract=fee_cents,
                                                    slippage_cents_per_contract=slippage_cents)
                reservation = reserved["reservation"]
                fill_time = as_of + timedelta(seconds=10)
                spread_quote = raw_legs[0]
                spread_ask = reserved["contract"]["quote"]["ask_cents"]
                spread_bid = reserved["contract"]["quote"]["bid_cents"]
                fill_material = {"proposal_id": raw["proposal_id"], "fill_id": fill_id,
                                 "quote": {"timestamp": _iso(fill_time), "bid_cents": spread_bid,
                                            "ask_cents": spread_ask, "depth": 1}}
                fill_source = _source("synthetic", fill_material, fill_time, fill_time)
                fill_event = {
                    "reservation_id": reservation["reservation_id"], "fill_id": fill_id,
                    "source_kind": "synthetic", "source": fill_source,
                    "quote": {"timestamp": _iso(fill_time), "bid_cents": spread_bid,
                               "ask_cents": spread_ask, "depth": 1}, "quantity": 1,
                    "cost": {"unit_cost_cents": spread_ask,
                             "fee_cents": converted["cost"]["fee_cents"]},
                    "filled_at": _iso(fill_time),
                }
                filled = gateway.record_fill(fill_event, now=fill_time)
                entry = _entry_record(raw, converted, filled, fill_time)
                reports_list, snapshot = _report_and_snapshot(entry["legs"], 1, fill_time, fill_time,
                                                               reports, holdings_snapshot)
                lifecycle_dir = root / "options-lifecycle"
                lifecycle = OptionsLifecycle(ledger, lifecycle_dir, clock=lambda: as_of)
                marks: list[dict[str, Any]] = []
                lifecycle_states: list[dict[str, Any]] = []
                for index, bundle_raw in enumerate(exit_marks):
                    bundle = _mapping(bundle_raw, f"exit_marks[{index}]")
                    at = _stamp(bundle.get("now"))
                    quotes = _mapping(bundle.get("quotes"), f"exit_marks[{index}].quotes")
                    sources = _mapping(bundle.get("quote_sources"), f"exit_marks[{index}].quote_sources")
                    mark = lifecycle.position_mark(entry, reports=reports_list, holdings_snapshot=snapshot,
                                                   quotes=quotes, quote_sources=sources, now=at)
                    ledger.record_marks(f"shadow-mark-{index}", [mark], at)
                    marks.append({"position_id": mark.position_id,
                                  "liquidation_value_cents": mark.liquidation_value_cents,
                                  "source_timestamp": _iso(mark.source_timestamp),
                                  "source_hash": mark.source_hash})
                    state = lifecycle.run(entry, reports=reports_list, holdings_snapshot=snapshot,
                                          quotes=quotes, quote_sources=sources, now=at)
                    lifecycle_states.append(state)
                reopened = PaperLedger(ledger_path, balances=balances, limits=limits,
                                       enforce_market_risk=True, clock=lambda: as_of)
                replay_gateway = PaperProposalGateway(reopened, source_kind="synthetic", clock=lambda: as_of)
                reservation_replay = reserve_options_proposal(
                    raw, replay_gateway, as_of=as_of, now=as_of,
                    fee_cents_per_contract=fee_cents,
                    slippage_cents_per_contract=slippage_cents,
                )
                fill_replay = replay_gateway.record_fill(fill_event, now=fill_time)
                restart = OptionsLifecycle(reopened, lifecycle_dir, clock=lambda: as_of).run()
                final = lifecycle_states[-1]
                complete = final.get("status") == "closed" and restart.get("status") == "closed"
                return {
                    "status": "complete" if complete else "blocked",
                    "lifecycle_status": "simulated_closed" if complete else final.get("status"),
                    "admission_status": "simulation_complete" if complete else "simulation_incomplete",
                    "profitability_status": "not_evaluated",
                    "profitability_claim": False,
                    "selection": selected, "reservation": reservation, "filled": filled["fill"],
                    "marks": marks, "lifecycle": lifecycle_states, "restart": restart,
                    "idempotency": {"reservation_replayed": reservation_replay["reservation"]["reservation_id"] == reservation["reservation_id"],
                                    "fill_replayed": fill_replay["fill"] == filled["fill"]},
                    "risk_limits_cents": {"options_cash": OPTIONS_CASH_CENTS,
                                          "per_idea": PER_IDEA_CENTS, "group": GROUP_CAP_CENTS,
                                          "aggregate": AGGREGATE_CAP_CENTS},
                }
        except Exception as exc:
            return _refusal("rehearsal_blocked", str(exc), selection=selected)


def run_options_shadow_rehearsal(**kwargs: Any) -> dict[str, Any]:
    """Convenience wrapper around :class:`OptionsShadowRehearsal`."""
    return OptionsShadowRehearsal(state_dir=kwargs.pop("state_dir", None)).run(**kwargs)


__all__ = ["OptionsShadowRehearsal", "OptionsShadowRehearsalError", "run_options_shadow_rehearsal"]
