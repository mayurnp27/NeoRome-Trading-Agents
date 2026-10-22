"""Compose a complete cloud capture with frozen selector evidence.

This is a retained-evidence adapter only. Daily bars, session calendar, and
market-session attestation are supplied by their independent collectors; this
module neither fetches them nor substitutes a current value. It binds those
inputs to the complete read-only option capture and delegates the decision to
``options_observed_shadow``.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from desks.options.selection import payload_sha256
from .options_observed_shadow import run_observed_shadow
from .options_dated_artifacts import DatedArtifactError, resolve_current, save_stage

UTC = timezone.utc


class OptionsObservedPipelineError(ValueError):
    pass


def _stamp(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise OptionsObservedPipelineError(f"{field} must be ISO-8601") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OptionsObservedPipelineError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise OptionsObservedPipelineError(f"{field} must be an object")
    return value


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
        raise OptionsObservedPipelineError(f"{field} must be a SHA-256 digest")
    return value.lower()


def build_shadow_input(capture: Mapping[str, Any], observation: Mapping[str, Any], context: Mapping[str, Any]) -> tuple[dict[str, Any], datetime]:
    """Bind one complete, fresh capture to caller-provided frozen trend inputs."""
    capture = _mapping(capture, "capture")
    if capture.get("artifact_kind") != "options-cloud-evidence-v1" or capture.get("status") != "captured":
        raise OptionsObservedPipelineError("complete captured cloud evidence is required")
    if capture.get("provider") != "robinhood-trading" or capture.get("chain_complete") is not True:
        raise OptionsObservedPipelineError("capture is not a complete Robinhood chain")
    if capture.get("shadow_ready") is not True or capture.get("broker_write_called") is not False:
        raise OptionsObservedPipelineError("capture has unresolved holdings or broker-write state")
    capture_hash = _hash(capture.get("artifact_sha256"), "capture artifact_sha256")
    now = _stamp(capture.get("as_of"), "capture as_of")
    receipt = _stamp(capture.get("local_receipt_at"), "capture receipt")
    if receipt > now:
        raise OptionsObservedPipelineError("capture receipt follows capture as_of")
    rows = capture.get("normalized_quotes")
    if not isinstance(rows, list) or not rows or len(rows) > 100:
        raise OptionsObservedPipelineError("capture normalized quote slice must contain 1-100 contracts")
    chain = _mapping(capture.get("chain"), "capture chain")
    right = {"call": "C", "put": "P"}.get(capture.get("right"))
    if right is None or chain.get("complete") is not True or not isinstance(chain.get("available_expirations"), list):
        raise OptionsObservedPipelineError("capture directional chain metadata is incomplete")
    selector_context = deepcopy(dict(_mapping(context, "selector context")))
    selector_context.update({
        "symbol": capture.get("symbol"), "now": now.isoformat().replace("+00:00", "Z"),
        "option_received_at": receipt.isoformat().replace("+00:00", "Z"),
        "option_evidence": {"evidence_id": f"options-cloud:{capture_hash}", "sha256": capture_hash,
                            "source": "robinhood-trading"},
        "chain_metadata": {"symbol": capture.get("symbol"), "complete": True,
                           "available_expirations": {right: list(chain["available_expirations"])}},
        "candidate_universe": {"complete": True, "limit": 100, "next_page_token": None,
                               "right": right, "expiry": capture.get("expiry")},
        "candidate_rows_sha256": payload_sha256(rows),
    })
    selector_context["normalized_payload_sha256"] = payload_sha256(selector_context, hash_field="normalized_payload_sha256")
    return {
        "source": "robinhood-trading", "local_receipt_at": receipt.isoformat().replace("+00:00", "Z"),
        "chain_complete": True, "size_units": capture.get("size_units"),
        "size_units_evidence": capture.get("size_units_evidence"), "normalized_quotes": rows,
        "selector": {"observation": deepcopy(dict(_mapping(observation, "selector observation"))),
                     "context": selector_context}, "holdings": deepcopy(dict(_mapping(capture.get("holdings"), "capture holdings"))),
        "capture_artifact_sha256": capture_hash,
        # The account snapshot is full and hash-bound.  The caller below
        # decides its admissibility only after the deterministic pair exists.
        "holdings_scope": "selected_legs_only",
    }, now


def produce_observed_candidate(capture: Mapping[str, Any], observation: Mapping[str, Any], context: Mapping[str, Any], *,
                               output_path: str | Path | None = None, max_entry_cost: Any | None = None,
                               paper_profile: str = "standard") -> dict[str, Any]:
    """Return the frozen selector candidate or a durable no-trade decision."""
    try:
        shadow_input, now = build_shadow_input(capture, observation, context)
    except OptionsObservedPipelineError:
        # Keep malformed/partial capture outcomes in the existing content-
        # addressed abstention format; do not manufacture selector fields.
        if isinstance(capture, Mapping) and (capture.get("as_of") or capture.get("received_at")):
            now = _stamp(capture.get("as_of") or capture.get("received_at"), "capture receipt")
        else:
            now = datetime.now(UTC)
        shadow_input = {"source": "robinhood-trading", "local_receipt_at": now.isoformat().replace("+00:00", "Z")}
    # Do not persist the pre-admission candidate: a later exact-leg conflict
    # must be the only durable decision for this immutable event path.
    result = run_observed_shadow(shadow_input, as_of=now, max_entry_cost=max_entry_cost,
                                 paper_profile=paper_profile)
    if result.get("status") != "eligible_shadow_candidate" or not isinstance(capture, Mapping):
        return _write_output(result, output_path)
    selected = result.get("shadow_candidate")
    if not isinstance(selected, Mapping):
        return result
    legs = {str(selected.get(side, {}).get("symbol")) for side in ("long", "short")}
    holdings = capture.get("holdings")
    if not isinstance(holdings, Mapping):
        return _write_output(_holdings_abstention(result, "holdings_missing"), output_path)
    active = {str(symbol) for symbol, quantity in dict(holdings.get("option_holdings") or {}).items() if quantity != 0}
    active.update(str(symbol) for symbol in holdings.get("pending_option_symbols", []) if isinstance(symbol, str))
    if legs.intersection(active):
        return _write_output(_holdings_abstention(result, "selected_leg_holding_conflict"), output_path)
    unrelated = len(active)
    for order in holdings.get("open_orders", []):
        if not isinstance(order, Mapping):
            return _write_output(_holdings_abstention(result, "ambiguous_open_order"), output_path)
        identifier = order.get("option_id") or order.get("instrument_id")
        if not isinstance(identifier, str) or not identifier:
            return _write_output(_holdings_abstention(result, "ambiguous_open_order"), output_path)
        # Open-order IDs are provider instrument IDs; translate using the
        # retained normalized rows before comparing selected OCC identities.
        by_id = {str(row.get("instrument_id")): str(row.get("symbol")) for row in capture.get("normalized_quotes", []) if isinstance(row, Mapping)}
        if by_id.get(identifier, identifier) in legs:
            return _write_output(_holdings_abstention(result, "selected_leg_open_order_conflict"), output_path)
    evidence = dict(result.get("evidence") or {})
    evidence["unrelated_holding_count"] = unrelated
    result = {**result, "evidence": evidence}
    result["artifact_sha256"] = _artifact_hash(result)
    return _write_output(result, output_path)


def _artifact_hash(value: Mapping[str, Any]) -> str:
    clean = {key: item for key, item in value.items() if key != "artifact_sha256"}
    return hashlib.sha256(json.dumps(clean, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _write_output(result: Mapping[str, Any], output_path: str | Path | None) -> dict[str, Any]:
    """Persist only the final post-selection decision with replay equality."""
    final = dict(result)
    if output_path is None:
        return final
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(final, sort_keys=True, indent=2) + "\n").encode("utf-8")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        if path.is_symlink() or path.read_bytes() != payload:
            raise OptionsObservedPipelineError("existing candidate output differs from final canonical decision")
    else:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        final["artifact_path"] = str(path)
    return final


def _holdings_abstention(result: Mapping[str, Any], code: str) -> dict[str, Any]:
    body = {**result, "status": "abstain", "reason_code": code, "reason": "authoritative account evidence conflicts with selected internal-paper legs", "shadow_candidate": None,
            "reservations_created": 0, "ledger_written": False}
    body["artifact_sha256"] = _artifact_hash(body)
    return body


def _read(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    return dict(_mapping(value, path))


def save_candidate_attempt(root: str | Path, date: str, capture: Mapping[str, Any], result: Mapping[str, Any]) -> Path:
    """Keep every candidate decision and advance only the manifest alias.

    The capture receipt identifies the observation cycle and its digest
    disambiguates same-receipt retries.  Unlike a fixed `candidate.json`, this
    never overwrites an earlier honest abstention or eligible decision.
    """
    capture = _mapping(capture, "capture")
    receipt = _stamp(capture.get("as_of") or capture.get("local_receipt_at"), "capture as_of")
    digest = _hash(capture.get("artifact_sha256"), "capture artifact_sha256")
    event = receipt.strftime("%Y%m%dT%H%M%S%fZ")
    return save_stage(root, date, f"candidate-{event}-{digest[:16]}.json", result,
                      current_name="candidate.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture")
    parser.add_argument("--observation")
    parser.add_argument("--context")
    parser.add_argument("--output")
    parser.add_argument("--state-dir", help="resolve dated capture/trend inputs and save dated candidate")
    parser.add_argument("--input-state-dir", help="separate dated capture/trend source for a dedicated output state")
    parser.add_argument("--max-entry-cost", help="explicit all-in cap for the options_full_port_v1 profile")
    parser.add_argument("--paper-profile", choices=("standard", "options_full_port_v1"), default="standard")
    parser.add_argument("--publish-inbox", help="sanitized one-way inbox for the neorome paper service")
    args = parser.parse_args()
    if args.input_state_dir and not args.state_dir:
        parser.error("--input-state-dir requires --state-dir")
    if args.paper_profile == "options_full_port_v1":
        if args.max_entry_cost != "300":
            parser.error("options_full_port_v1 requires --max-entry-cost 300")
    elif args.max_entry_cost is not None:
        parser.error("--max-entry-cost is reserved for options_full_port_v1")
    if args.state_dir:
        if any((args.capture, args.observation, args.context, args.output)):
            parser.error("--state-dir cannot be combined with fixed artifact paths")
        try:
            source_state = args.input_state_dir or args.state_dir
            paths = resolve_current(source_state, "capture.json", "trend-observation.json", "trend-context.json")
            capture = _read(str(paths["capture.json"]))
            result = produce_observed_candidate(capture, _read(str(paths["trend-observation.json"])),
                                                _read(str(paths["trend-context.json"])),
                                                max_entry_cost=args.max_entry_cost,
                                                paper_profile=args.paper_profile)
            date = paths["capture.json"].parent.name
            save_candidate_attempt(args.state_dir, date, capture, result)
            if args.publish_inbox and result.get("status") == "eligible_shadow_candidate":
                from .options_candidate_inbox import publish
                publish(result, args.publish_inbox, date=date)
        except (DatedArtifactError, OptionsObservedPipelineError) as exc:
            result = {"status": "abstain", "reason_code": type(exc).__name__, "orders_sent": 0}
    else:
        if args.publish_inbox or not all((args.capture, args.observation, args.context, args.output)):
            parser.error("fixed mode requires --capture, --observation, --context, and --output")
        result = produce_observed_candidate(_read(args.capture), _read(args.observation), _read(args.context), output_path=args.output,
                                            max_entry_cost=args.max_entry_cost, paper_profile=args.paper_profile)
    print(json.dumps({"status": result["status"], "reason_code": result["reason_code"], "orders_sent": 0}, sort_keys=True))
    return 0 if result["status"] in {"eligible_shadow_candidate", "abstain"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
