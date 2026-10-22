"""One-shot observed memecoin shadow recorder with no execution capability."""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Callable, Mapping

from .crypto_research import TOKEN_PROGRAM
from .jupiter_quotes import USDC, validate_mint

UTC = timezone.utc
SCHEMA_VERSION = 1
POLICY_VERSION = "token-observed-shadow-v1"
CONTROL_VERSION = "token-observed-control-review-v1"
COST_VERSION = "token-paper-costs-v1"
MAX_AGE_SECONDS = 60
MAX_RECORD_BYTES = 1_000_000
HORIZONS = (15, 60, 240)


class ShadowError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        data = json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise ShadowError("evidence must contain bounded JSON values") from exc
    if len(data) > MAX_RECORD_BYTES:
        raise ShadowError("evidence exceeds the recorder size limit")
    return data


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _hash(value: Any, name: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(character not in "0123456789abcdefABCDEF" for character in value)):
        raise ShadowError(f"{name} must be a SHA-256 digest")
    return value.lower()


def _time(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise ShadowError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShadowError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowError(f"{name} requires a timezone")
    return parsed.astimezone(UTC)


def _now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ShadowError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _raw(value: Any, name: str, *, positive: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).isdecimal():
        raise ShadowError(f"{name} must be integer base units")
    result = int(value)
    if result < (1 if positive else 0) or result > 2**64 - 1:
        raise ShadowError(f"{name} is outside its allowed range")
    return result


def _root(value: str | os.PathLike[str]) -> Path:
    root = Path(value)
    if root.exists() and root.is_symlink():
        raise ShadowError("state symlink refused")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    return root


def _atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(_canonical(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _publish(root: Path, body: Mapping[str, Any]) -> dict[str, Any]:
    digest = _digest(body)
    output = {**body, "content_sha256": digest}
    path = root / f"{body['status']}-{digest}.json"
    if path.is_symlink():
        raise ShadowError("content-addressed output symlink refused")
    if path.exists():
        if json.loads(path.read_text()) != output:
            raise ShadowError("content-address collision")
    else:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_canonical(output) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    return {**output, "output": str(path), "orders_sent": 0, "cash_reserved_cents": 0}


def _capture(value: Mapping[str, Any] | Callable[[], Mapping[str, Any]], stage: str,
             clock: Callable[[], datetime]) -> tuple[Mapping[str, Any], datetime]:
    try:
        record = value() if callable(value) else value
    except Exception as exc:
        _now(clock)  # Receipt stamp follows every callback attempt, including failure.
        raise ShadowError(f"{stage} callback failed ({type(exc).__name__})") from None
    received = _now(clock)
    if not isinstance(record, Mapping):
        raise ShadowError(f"{stage} callback did not return an object")
    _canonical(record)
    return record, received


def _envelope(record: Mapping[str, Any], *, provider: str, local_received: datetime,
              stage: str) -> tuple[Mapping[str, Any], str, datetime]:
    required = {"schema_version", "source_kind", "provider", "requested_at", "received_at",
                "payload", "payload_sha256"}
    if set(record) != required or record.get("schema_version") != 1:
        raise ShadowError(f"{stage} observed envelope schema is invalid")
    if record.get("source_kind") != "observed" or record.get("provider") != provider:
        raise ShadowError(f"{stage} observed provider is invalid")
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        raise ShadowError(f"{stage} payload is missing")
    supplied = _hash(record.get("payload_sha256"), f"{stage} payload hash")
    if supplied != _digest(payload):
        raise ShadowError(f"{stage} payload hash mismatch")
    requested = _time(record.get("requested_at"), f"{stage}.requested_at")
    provider_received = _time(record.get("received_at"), f"{stage}.received_at")
    if requested > provider_received or provider_received > local_received:
        raise ShadowError(f"{stage} evidence is future-dated or reversed")
    age = (local_received - provider_received).total_seconds()
    if age < 0 or age > MAX_AGE_SECONDS:
        raise ShadowError(f"{stage} evidence is stale")
    return payload, supplied, provider_received


def _quote(payload: Mapping[str, Any], *, incoming: str, outgoing: str,
           amount: int | None, stage: str) -> dict[str, Any]:
    if payload.get("chain") != "solana":
        raise ShadowError(f"{stage} chain identity is invalid")
    quote = payload.get("quote")
    if not isinstance(quote, Mapping):
        raise ShadowError(f"{stage} quote is missing")
    if (quote.get("inputMint"), quote.get("outputMint")) != (incoming, outgoing):
        raise ShadowError(f"{stage} quote mint identity mismatch")
    input_amount = _raw(quote.get("inAmount"), f"{stage} input")
    if amount is not None and input_amount != amount:
        raise ShadowError(f"{stage} quote is not size-matched")
    output = _raw(quote.get("outAmount"), f"{stage} output")
    minimum = _raw(quote.get("otherAmountThreshold"), f"{stage} minimum")
    if minimum > output:
        raise ShadowError(f"{stage} minimum exceeds quoted output")
    if quote.get("priceImpactUnit") != "percentage_points":
        raise ShadowError(f"{stage} priceImpact units are ambiguous")
    impact_value = quote.get("priceImpact")
    if isinstance(impact_value, bool) or not isinstance(impact_value, (str, int, float)):
        raise ShadowError(f"{stage} priceImpact is invalid")
    try:
        impact = Decimal(str(impact_value))
    except (InvalidOperation, ValueError) as exc:
        raise ShadowError(f"{stage} priceImpact is invalid") from exc
    if not impact.is_finite() or abs(impact) > Decimal("2"):
        raise ShadowError(f"{stage} priceImpact is outside the review bound")
    return {"input_raw": input_amount, "output_raw": output, "minimum_raw": minimum,
            "price_impact_percentage_points": str(impact)}


def _costs(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
            "version", "excludes_embedded_route_fees", "entry_extra_usdc_raw",
            "exit_extra_usdc_raw"} or value.get("version") != COST_VERSION:
        raise ShadowError("versioned cost assumptions are invalid")
    if value.get("excludes_embedded_route_fees") is not True:
        raise ShadowError("embedded route fees must not be added twice")
    return {"version": COST_VERSION, "excludes_embedded_route_fees": True,
            "entry_extra_usdc_raw": str(_raw(value["entry_extra_usdc_raw"], "entry cost", positive=False)),
            "exit_extra_usdc_raw": str(_raw(value["exit_extra_usdc_raw"], "exit cost", positive=False))}


def _candidate(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], str]:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 1 or not isinstance(candidates[0], Mapping):
        raise ShadowError("discovery must contain exactly one candidate")
    candidate = candidates[0]
    mint = candidate.get("mint")
    try:
        mint = validate_mint(mint)
    except Exception as exc:
        raise ShadowError("discovery mint identity is invalid") from exc
    if candidate.get("chain") != "solana" or candidate.get("screen_rejections") != []:
        raise ShadowError("discovery candidate failed the frozen screen")
    pool = candidate.get("pool")
    if not isinstance(pool, Mapping):
        raise ShadowError("DexScreener pool evidence is incomplete")
    try:
        validate_mint(pool.get("pair_address"))
        liquidity = Decimal(str(pool.get("liquidity_usd")))
        age = Decimal(str(pool.get("age_hours_at_observation")))
    except (Exception, InvalidOperation) as exc:
        raise ShadowError("DexScreener pool evidence is incomplete") from exc
    if not liquidity.is_finite() or not age.is_finite() or liquidity < Decimal("50000") or age < 24:
        raise ShadowError("DexScreener pool evidence failed the frozen screen")
    checks = candidate.get("mint_checks")
    if (not isinstance(checks, Mapping) or checks.get("program") != TOKEN_PROGRAM
            or checks.get("mintAuthority") is not None or checks.get("freezeAuthority") is not None):
        raise ShadowError("discovery mint controls are unverifiable")
    return candidate, mint


def _control(payload: Mapping[str, Any], *, mint: str, discovery_hash: str,
             entry_hash: str, preflight_hash: str) -> str:
    review = dict(payload)
    supplied = _hash(review.pop("review_manifest_sha256", None), "control review manifest hash")
    if supplied != _digest(review):
        raise ShadowError("control review manifest hash mismatch")
    required = {"version", "chain", "mint", "token_program", "mint_authority",
                "freeze_authority", "transfer_restrictions", "owner_concentration_reviewed",
                "liquidity_reviewed", "discovery_payload_sha256", "entry_payload_sha256",
                "preflight_payload_sha256", "evidence_hashes"}
    if set(review) != required or review.get("version") != CONTROL_VERSION:
        raise ShadowError("control review schema is invalid")
    if (review.get("chain"), review.get("mint"), review.get("token_program")) != (
            "solana", mint, TOKEN_PROGRAM):
        raise ShadowError("control review identity mismatch")
    if (review.get("mint_authority") is not None or review.get("freeze_authority") is not None
            or review.get("transfer_restrictions") != "none_reviewed"
            or review.get("owner_concentration_reviewed") is not True
            or review.get("liquidity_reviewed") is not True):
        raise ShadowError("token controls or sellability remain unresolved")
    if (review.get("discovery_payload_sha256"), review.get("entry_payload_sha256"),
            review.get("preflight_payload_sha256")) != (discovery_hash, entry_hash, preflight_hash):
        raise ShadowError("control review is not bound to the captured evidence")
    hashes = review.get("evidence_hashes")
    if not isinstance(hashes, Mapping) or set(hashes) != {
            "dexscreener_pairs", "helius_mint", "helius_largest_accounts", "transfer_review"}:
        raise ShadowError("control evidence hashes are incomplete")
    for name, value in hashes.items():
        _hash(value, f"control evidence {name}")
    return supplied


def _abstain(root: Path, reason: str, at: datetime) -> dict[str, Any]:
    return _publish(root, {"schema_version": SCHEMA_VERSION, "policy_version": POLICY_VERSION,
                           "status": "abstain", "reason": reason, "recorded_at": _iso(at),
                           "source_kind": "observed", "research_only": True,
                           "shadow_eligible": False, "execution_enabled": False,
                           "profitability_evidence": False})


def run_once(state_dir: str | os.PathLike[str], *,
             discovery: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
             entry_quote: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
             sell_preflight: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
             control_review: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
             costs: Mapping[str, Any] | None = None,
             outcome: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
             horizon_minutes: int | None = None, shadow_action: str = "mark",
             clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> dict[str, Any]:
    """Capture one admission sequence or one later shadow outcome."""
    root = _root(state_dir)
    with (root / "shadow.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return _abstain(root, "already_running", _now(clock))
        state_path = root / "shadow-state.json"
        try:
            if state_path.is_symlink():
                raise ShadowError("persisted shadow state symlink refused")
            try:
                state = json.loads(state_path.read_text()) if state_path.exists() else None
            except (OSError, json.JSONDecodeError) as exc:
                raise ShadowError("persisted shadow state is unreadable") from exc
            if state is None:
                if any(value is None for value in (discovery, entry_quote, sell_preflight, control_review, costs)):
                    raise ShadowError("complete discovery, entry, preflight, controls, and costs are required")
                disc_record, disc_local = _capture(discovery, "discovery", clock)
                disc_payload, disc_hash, disc_received = _envelope(
                    disc_record, provider="dexscreener+helius", local_received=disc_local, stage="discovery")
                candidate, mint = _candidate(disc_payload)
                entry_record, entry_local = _capture(entry_quote, "entry", clock)
                entry_payload, entry_hash, entry_received = _envelope(
                    entry_record, provider="jupiter", local_received=entry_local, stage="entry")
                entry = _quote(entry_payload, incoming=USDC, outgoing=mint, amount=None, stage="entry")
                preflight_record, preflight_local = _capture(sell_preflight, "preflight", clock)
                preflight_payload, preflight_hash, preflight_received = _envelope(
                    preflight_record, provider="jupiter", local_received=preflight_local, stage="preflight")
                preflight = _quote(preflight_payload, incoming=mint, outgoing=USDC,
                                   amount=entry["minimum_raw"], stage="preflight")
                if len({disc_hash, entry_hash, preflight_hash}) != 3:
                    raise ShadowError("each capture requires independent evidence")
                control_record, control_local = _capture(control_review, "controls", clock)
                control_payload, control_hash, control_received = _envelope(
                    control_record, provider="helius+review", local_received=control_local, stage="controls")
                if not (disc_local <= entry_local <= preflight_local <= control_local):
                    raise ShadowError("local callback receipt sequence is reversed")
                if not (disc_received <= entry_received <= preflight_received <= control_received):
                    raise ShadowError("provider evidence sequence is reversed")
                manifest_hash = _control(control_payload, mint=mint, discovery_hash=disc_hash,
                                         entry_hash=entry_hash, preflight_hash=preflight_hash)
                normalized_costs = _costs(costs)  # type: ignore[arg-type]
                all_in = (entry["input_raw"] + int(normalized_costs["entry_extra_usdc_raw"])
                          + int(normalized_costs["exit_extra_usdc_raw"]))
                if all_in > 5_000_000:
                    raise ShadowError("shadow idea exceeds the $5 crypto cap")
                decision = {"schema_version": SCHEMA_VERSION, "policy_version": POLICY_VERSION,
                            "status": "eligible", "source_kind": "observed", "research_only": True,
                            "shadow_eligible": True, "execution_enabled": False,
                            "profitability_evidence": False, "chain": "solana", "mint": mint,
                            "known_at": _iso(disc_received), "received_at": _iso(disc_local),
                            "pair_address": candidate["pool"]["pair_address"],
                            "acquired_token_raw": str(entry["minimum_raw"]),
                            "entry_usdc_raw": str(entry["input_raw"]), "costs": normalized_costs,
                            "evidence_hashes": {"discovery": disc_hash, "entry": entry_hash,
                                                "preflight": preflight_hash, "controls": control_hash,
                                                "control_manifest": manifest_hash},
                            "entry_provider_received_at": _iso(entry_received),
                            "decision_at": _iso(control_local), "shadow_action": "observe_only"}
                result = _publish(root, decision)
                _atomic(state_path, {**decision, "candidate_content_sha256": result["content_sha256"]})
                return result
            if (not isinstance(state, Mapping) or state.get("schema_version") != SCHEMA_VERSION
                    or state.get("policy_version") != POLICY_VERSION or state.get("status") != "eligible"
                    or state.get("source_kind") != "observed" or state.get("execution_enabled") is not False):
                raise ShadowError("persisted shadow candidate state is invalid")
            candidate_digest = _hash(state.get("candidate_content_sha256"), "candidate content hash")
            candidate_path = root / f"eligible-{candidate_digest}.json"
            if not candidate_path.exists():
                raise ShadowError("persisted shadow candidate record is missing")
            try:
                saved_candidate = json.loads(candidate_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise ShadowError("persisted shadow candidate record is unreadable") from exc
            saved_body = dict(saved_candidate)
            if saved_body.pop("content_sha256", None) != candidate_digest or _digest(saved_body) != candidate_digest:
                raise ShadowError("persisted shadow candidate content hash mismatch")
            bound_fields = ("mint", "chain", "known_at", "received_at", "acquired_token_raw", "entry_usdc_raw", "costs",
                            "entry_provider_received_at", "decision_at", "evidence_hashes")
            if any(state.get(name) != saved_candidate.get(name) for name in bound_fields):
                raise ShadowError("persisted state differs from the content-addressed candidate")
            try:
                validate_mint(state.get("mint"))
            except Exception as exc:
                raise ShadowError("persisted candidate mint is invalid") from exc
            acquired_raw = _raw(state.get("acquired_token_raw"), "persisted acquired amount")
            persisted_costs = _costs(state.get("costs"))
            if outcome is None:
                raise ShadowError("later received Jupiter outcome route is required")
            if horizon_minutes not in HORIZONS or shadow_action not in {"mark", "exit"}:
                raise ShadowError("outcome horizon or shadow action is invalid")
            outcome_record, outcome_local = _capture(outcome, "outcome", clock)
            outcome_payload, outcome_hash, provider_received = _envelope(
                outcome_record, provider="jupiter", local_received=outcome_local, stage="outcome")
            decision_at = _time(state.get("decision_at"), "decision_at")
            if provider_received <= decision_at:
                raise ShadowError("outcome route does not follow the shadow decision")
            target = _time(state.get("entry_provider_received_at"), "entry receipt") + timedelta(
                minutes=horizon_minutes)
            if not target <= provider_received <= target + timedelta(seconds=30):
                raise ShadowError("outcome route is outside its preregistered horizon window")
            quote = _quote(outcome_payload, incoming=state["mint"], outgoing=USDC,
                           amount=acquired_raw, stage="outcome")
            exit_extra = int(persisted_costs["exit_extra_usdc_raw"])
            if exit_extra > quote["minimum_raw"]:
                raise ShadowError("exit cost exceeds conservative route proceeds")
            observation = {"schema_version": SCHEMA_VERSION, "policy_version": POLICY_VERSION,
                           "status": "eligible", "source_kind": "observed", "research_only": True,
                           "shadow_eligible": True, "execution_enabled": False,
                           "profitability_evidence": False, "chain": "solana", "mint": state["mint"],
                           "candidate_content_sha256": state["candidate_content_sha256"],
                           "horizon_minutes": horizon_minutes, "shadow_action": shadow_action,
                           "minimum_exit_value_usdc_raw": str(quote["minimum_raw"] - exit_extra),
                           "outcome_payload_sha256": outcome_hash,
                           "provider_received_at": _iso(provider_received),
                           "recorded_at": _iso(outcome_local)}
            return _publish(root, observation)
        except ShadowError as exc:
            return _abstain(root, str(exc), _now(clock))


__all__ = ["run_once", "ShadowError", "POLICY_VERSION", "CONTROL_VERSION"]
