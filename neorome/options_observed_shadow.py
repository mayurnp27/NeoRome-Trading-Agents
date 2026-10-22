"""One-shot, read-only bridge from archived option evidence to a shadow decision.

This module is deliberately upstream of the paper ledger.  It consumes either
an archived Robinhood response bundle or an already-normalized equivalent,
checks local receipt and contract evidence, then calls the existing pure
selector.  It never reserves risk, writes a ledger, calls a broker, or creates
a fill.  Every result is a content-addressed decision artifact.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from desks.options.selection import payload_sha256, select
from .robinhood_options import normalize

UTC = timezone.utc
MAX_AGE = timedelta(seconds=60)
PROVIDER = "robinhood-trading"
ARTIFACT_KIND = "options-observed-shadow-decision-v1"


class ObservedShadowInputError(ValueError):
    """Caller evidence is malformed or fails a fail-closed admission check."""


class ObservedShadowArtifactConflict(RuntimeError):
    """An artifact path already contains different canonical decision bytes."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ObservedShadowInputError("evidence must contain canonical JSON values") from exc


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ObservedShadowInputError(f"{field} must be ISO-8601") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ObservedShadowInputError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise ObservedShadowInputError(f"{field} must be a SHA-256 hex digest")
    return value.lower()


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ObservedShadowInputError(f"{field} must be an object")
    return value


def _abstain(reason_code: str, reason: str, *, evidence: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": ARTIFACT_KIND,
        "status": "abstain",
        "reason_code": reason_code,
        "reason": reason,
        "shadow_candidate": None,
        "orders_sent": 0,
        "reservations_created": 0,
        "ledger_written": False,
        "broker_called": False,
        "profitability_status": "not_evaluated",
    }
    if evidence is not None:
        body["evidence"] = deepcopy(dict(evidence))
    return body


def _reason_code(message: str) -> str:
    """Keep abstentions machine-actionable while retaining the full reason."""
    text = message.lower()
    if "local_receipt_at" in text or "local receipt" in text:
        return "local_receipt_invalid"
    if "chain completeness" in text or "complete chain" in text:
        return "chain_incomplete"
    if "size unit" in text or "contract counts" in text:
        return "size_units_unverified"
    if "holding" in text or "underlying" in text or "outstanding-order" in text:
        return "holdings_reconciliation_required"
    if "contract identity" in text or "normalized option contract" in text or "candidate rows" in text:
        return "contract_identity_invalid"
    if "quote" in text or "snapshot" in text or "provider" in text:
        return "quote_evidence_invalid"
    if "selector" in text or "context" in text:
        return "selector_evidence_invalid"
    return "evidence_rejected"


def _receipt(evidence: Mapping[str, Any], as_of: datetime) -> datetime:
    value = evidence.get("local_receipt_at")
    if value is None:
        raise ObservedShadowInputError("local_receipt_at is required")
    received = _timestamp(value, "local_receipt_at")
    if received > as_of:
        raise ObservedShadowInputError("local receipt is in the future")
    if as_of - received > MAX_AGE:
        raise ObservedShadowInputError("local receipt is stale")
    return received


def _validate_size_units(evidence: Mapping[str, Any]) -> None:
    units = evidence.get("size_units")
    proof = evidence.get("size_units_evidence")
    if units != "contracts" or not isinstance(proof, str) or not proof.strip():
        raise ObservedShadowInputError("quote size units must explicitly be contracts with provider evidence")


def _validate_normalized_rows(rows: Any, *, as_of: datetime, received: datetime) -> list[dict[str, Any]]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)) or not 2 <= len(rows) <= 100:
        raise ObservedShadowInputError("normalized option candidate universe must contain 2-100 contracts")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(rows):
        row = dict(_mapping(raw, f"normalized_quotes[{index}]"))
        for field in ("symbol", "underlying", "expiry", "right", "strike", "instrument_id", "timestamp",
                      "bid", "ask", "bid_size", "ask_size", "delta", "implied_volatility"):
            if field not in row:
                raise ObservedShadowInputError(f"normalized_quotes[{index}].{field} is required")
        if row["symbol"] in seen:
            raise ObservedShadowInputError("normalized option contract identities are duplicated")
        seen.add(row["symbol"])
        if row.get("multiplier") != 100 or row.get("adjusted") is not False:
            raise ObservedShadowInputError("contract identity is missing standard unadjusted metadata")
        stamp = _timestamp(row["timestamp"], f"normalized_quotes[{index}].timestamp")
        if stamp > received or as_of - stamp > MAX_AGE:
            raise ObservedShadowInputError("provider quote is stale, future-dated, or after local receipt")
        if type(row["bid_size"]) is not int or row["bid_size"] < 1 or type(row["ask_size"]) is not int or row["ask_size"] < 1:
            raise ObservedShadowInputError("both option quote sizes must be positive contract counts")
        result.append(row)
    return result


def _validate_holdings(holdings: Mapping[str, Any], *, as_of: datetime, received: datetime,
                       symbols: set[str], allow_unrelated: bool = False) -> str:
    if holdings.get("complete") is not True:
        raise ObservedShadowInputError("holdings snapshot must explicitly be complete")
    local = _timestamp(holdings.get("local_receipt_at"), "holdings.local_receipt_at")
    if local > as_of or as_of - local > MAX_AGE or local < received:
        raise ObservedShadowInputError("holdings local receipt is stale, future-dated, or predates quote receipt")
    source = holdings.get("source")
    if not isinstance(source, str) or not source.strip():
        raise ObservedShadowInputError("holdings source is required")
    supplied_hash = _hash(holdings.get("sha256"), "holdings.sha256")
    clean = {key: value for key, value in holdings.items() if key != "sha256"}
    if _sha(clean) != supplied_hash:
        raise ObservedShadowInputError("holdings.sha256 does not match the normalized snapshot")
    snapshot_at = _timestamp(holdings.get("timestamp"), "holdings.timestamp")
    if snapshot_at > local or snapshot_at > as_of:
        raise ObservedShadowInputError("holdings snapshot chronology is invalid")
    if holdings.get("underlying_quantity") != 0 or holdings.get("open_orders_reconciled") is not True:
        raise ObservedShadowInputError("underlying or outstanding-order holdings reconciliation is unresolved")
    option_holdings = _mapping(holdings.get("option_holdings"), "holdings.option_holdings")
    if any(type(value) is not int for value in option_holdings.values()):
        raise ObservedShadowInputError("holdings option quantities must be whole normalized integers")
    # The standalone bridge remains deliberately strict.  The full observed
    # pipeline may opt into the selected-leg boundary, where it validates the
    # final pair after deterministic selection and retains account-wide proof.
    if not allow_unrelated and (set(option_holdings) - symbols or any(option_holdings.values())):
        raise ObservedShadowInputError("option holdings outside the observed candidate universe are unresolved")
    return supplied_hash


def _load_quotes(evidence: Mapping[str, Any], *, as_of: datetime, received: datetime) -> tuple[list[dict[str, Any]], str]:
    snapshot = evidence.get("snapshot")
    normalized = evidence.get("normalized_quotes")
    if snapshot is not None and normalized is not None:
        raise ObservedShadowInputError("provide either snapshot or normalized_quotes, not both")
    if snapshot is not None:
        snap = _mapping(snapshot, "snapshot")
        if snap.get("source") != PROVIDER:
            raise ObservedShadowInputError("snapshot source is not the Robinhood read-only provider")
        provider_received = _timestamp(snap.get("received_at"), "snapshot.received_at")
        if provider_received > received or as_of - provider_received > MAX_AGE:
            raise ObservedShadowInputError("archived provider receipt is stale or after local receipt")
        try:
            rows = normalize(snap, as_of=as_of)
        except (ValueError, KeyError, TypeError) as exc:
            raise ObservedShadowInputError(str(exc)) from exc
    elif normalized is not None:
        source = evidence.get("normalized_source")
        if isinstance(source, Mapping):
            if source.get("provider") not in (None, PROVIDER):
                raise ObservedShadowInputError("normalized quote source is not Robinhood")
            rows = list(source.get("quotes", normalized))
        else:
            if evidence.get("source") != PROVIDER:
                raise ObservedShadowInputError("normalized quotes require an explicit Robinhood source")
            rows = list(normalized)
    else:
        raise ObservedShadowInputError("normalized Robinhood/archive quote evidence is required")
    checked = _validate_normalized_rows(rows, as_of=as_of, received=received)
    return checked, _sha(checked)


def _artifact(body: Mapping[str, Any], output_path: str | Path | None) -> dict[str, Any]:
    clean = deepcopy(dict(body))
    artifact_hash = _sha(clean)
    result = {**clean, "artifact_sha256": artifact_hash}
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(result, sort_keys=True, indent=2) + "\n").encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            fd = os.open(path, flags, 0o600)
        except FileExistsError:
            try:
                existing = path.read_bytes()
            except OSError as exc:
                raise ObservedShadowArtifactConflict(
                    f"existing artifact {path} cannot be read for replay verification"
                ) from exc
            if existing != payload:
                raise ObservedShadowArtifactConflict(
                    f"existing artifact {path} differs from the canonical decision bytes"
                )
        else:
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
        result["artifact_path"] = str(path)
    return result


def run_observed_shadow(evidence: Mapping[str, Any], *, as_of: datetime | str,
                        output_path: str | Path | None = None, max_entry_cost: Any | None = None,
                        paper_profile: str = "standard") -> dict[str, Any]:
    """Produce one content-addressed observed-shadow candidate or abstention."""
    now = _timestamp(as_of, "as_of")
    try:
        if paper_profile not in {"standard", "options_full_port_v1"}:
            raise ObservedShadowInputError("paper profile is unsupported")
        if paper_profile == "standard" and max_entry_cost is not None:
            raise ObservedShadowInputError("standard profile cannot override its entry cap")
        payload = _mapping(evidence, "evidence")
        received = _receipt(payload, now)
        _validate_size_units(payload)
        if payload.get("chain_complete") is not True:
            raise ObservedShadowInputError("chain completeness must be explicitly attested")
        rows, quote_hash = _load_quotes(payload, as_of=now, received=received)
        selector = _mapping(payload.get("selector"), "selector")
        observation = _mapping(selector.get("observation"), "selector.observation")
        context = deepcopy(dict(_mapping(selector.get("context"), "selector.context")))
        if _timestamp(context.get("now"), "selector.context.now") != now:
            raise ObservedShadowInputError("selector context now must equal as_of")
        if _timestamp(context.get("option_received_at"), "selector.context.option_received_at") != received:
            raise ObservedShadowInputError("selector option receipt must equal local receipt")
        if context.get("candidate_rows_sha256") != payload_sha256(rows):
            raise ObservedShadowInputError("selector candidate rows are not bound to normalized quotes")
        holdings_hash = _validate_holdings(_mapping(payload.get("holdings"), "holdings"),
                                           as_of=now, received=received,
                                           symbols={str(row["symbol"]) for row in rows},
                                           allow_unrelated=payload.get("holdings_scope") == "selected_legs_only")
        selection = select(observation, context, rows,
                           fee_per_leg=payload.get("fee_per_leg", "0"),
                           slippage_per_leg=payload.get("slippage_per_leg", "0"),
                           max_entry_cost=max_entry_cost)
        if selection.get("status") != "candidate":
            body = _abstain(str(selection.get("reason_code", "selector_abstained")),
                            str(selection.get("reason", "selector abstained")))
        else:
            body = {
                "schema_version": 1, "artifact_kind": ARTIFACT_KIND,
                "status": "eligible_shadow_candidate", "reason_code": "eligible",
                "reason": "Observed evidence passed read-only selector and evidence gates",
                "shadow_candidate": deepcopy(selection), "orders_sent": 0,
                "reservations_created": 0, "ledger_written": False, "broker_called": False,
                "profitability_status": "not_evaluated",
            }
        body["evidence"] = {
            "provider": PROVIDER, "local_receipt_at": _iso(received),
            "as_of": _iso(now), "quote_evidence_sha256": quote_hash,
            "holdings_sha256": holdings_hash,
            "chain_metadata_sha256": _sha(context.get("chain_metadata")),
            "contract_identities": [
                {key: row[key] for key in ("symbol", "instrument_id", "underlying", "expiry", "right", "strike", "multiplier", "adjusted")}
                for row in rows
            ],
            "size_units": payload["size_units"],
        }
        if paper_profile != "standard":
            body["paper_profile"] = paper_profile
        return _artifact(body, output_path)
    except (ObservedShadowInputError, KeyError, TypeError, ValueError) as exc:
        body = _abstain(_reason_code(str(exc)), str(exc))
        body["evidence"] = {"provider": PROVIDER, "as_of": _iso(now)}
        return _artifact(body, output_path)


run = run_observed_shadow


__all__ = ["ARTIFACT_KIND", "ObservedShadowArtifactConflict", "ObservedShadowInputError",
           "run_observed_shadow", "run"]
