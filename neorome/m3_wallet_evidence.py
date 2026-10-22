"""Deterministic, paper-only evidence gates for the M3 memecoin cohort.

The evaluator consumes read-only, timestamped observations.  It does not fetch
Helius, sign transactions, route swaps, calculate returns, or write a ledger.

The small input schema is an envelope or a list of observation records::

    {
        "schema_version": "m3-wallet-evidence-v1",
        "as_of": "2026-09-26T12:00:00Z",
        "observations": [
            {
                "observed_at": "2026-09-25T12:00:00Z",
                "source": "helius-fixture",
                "wallet_evidence": {
                    "largest_holder_fraction": "0.20",
                    "top_10_holder_fraction": "0.41",
                    "creator_outflow_fraction": "0.00",
                    "creator_sold": false
                }
            }
        ]
    }

``observed_at`` is required and must be at or before ``as_of``.  An optional
``prior_window`` object may declare ``start_at``, ``end_at``, ``complete`` and
``observation_count``; otherwise the evaluator derives the window from the
observation timestamps.  ``wallet_evidence`` may instead be supplied as a
``wallets`` list containing ``balance_fraction`` (or ``supply_fraction``),
``is_creator`` and creator ``net_flow_fraction`` values.  This is a compact
fixture form for Helius largest-account observations, not an assertion that
account ownership is known.

The result is a mapping with ``status`` equal to ``eligible``, ``unresolved``
or ``rejected``, ordered boolean ``gates``, attributable ``reasons`` and
derived evidence.  Eligibility means only that the supplied M3 evidence gates
are complete and benign for internal paper research; execution remains out of
scope.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "m3-wallet-evidence-v1"
MIN_PRIOR_WINDOW = timedelta(hours=24)
MIN_OBSERVATIONS = 3
MAX_LARGEST_HOLDER_FRACTION = Decimal("0.35")
MAX_TOP_10_HOLDER_FRACTION = Decimal("0.60")
MAX_CREATOR_OUTFLOW_FRACTION = Decimal("0.05")


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field}_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field}_timezone_missing")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fraction(value: Any, field: str, *, allow_negative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{field}_invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field}_invalid") from exc
    if not parsed.is_finite() or parsed > 1 or parsed < -1:
        raise ValueError(f"{field}_invalid")
    if not allow_negative and parsed < 0:
        raise ValueError(f"{field}_invalid")
    return parsed


def _bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field}_invalid")
    return value


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _wallet_metrics(record: Mapping[str, Any]) -> tuple[dict[str, Decimal | bool], list[str], list[str]]:
    """Return metrics, unresolved reasons, and malformed reasons for a record."""
    raw = record.get("wallet_evidence")
    if raw is None:
        raw = record.get("wallet_risk")
    if isinstance(raw, Mapping):
        aliases = {
            "largest_holder_fraction": ("largest_holder_fraction", "largest_account_supply_fraction"),
            "top_10_holder_fraction": ("top_10_holder_fraction", "top_accounts_supply_fraction", "listed_accounts_supply_fraction"),
            "creator_outflow_fraction": ("creator_outflow_fraction", "creator_net_outflow_fraction"),
        }
        metrics: dict[str, Decimal | bool] = {}
        unresolved: list[str] = []
        malformed: list[str] = []
        for name, fields in aliases.items():
            value = next((raw.get(field) for field in fields if field in raw), None)
            if value is None:
                unresolved.append(f"{name}_missing")
                continue
            try:
                metrics[name] = _fraction(value, name)
            except ValueError as exc:
                malformed.append(str(exc))
        if "creator_sold" not in raw:
            unresolved.append("creator_sold_missing")
        else:
            try:
                metrics["creator_sold"] = _bool(raw["creator_sold"], "creator_sold")
            except ValueError as exc:
                malformed.append(str(exc))
        return metrics, unresolved, malformed

    wallets = record.get("wallets")
    if not isinstance(wallets, Sequence) or isinstance(wallets, (str, bytes)) or not wallets:
        return {}, ["wallet_evidence_missing"], []
    balances: list[Decimal] = []
    creator_outflow = Decimal("0")
    creator_seen = False
    creator_sold = False
    malformed: list[str] = []
    for index, wallet in enumerate(wallets):
        if not isinstance(wallet, Mapping):
            malformed.append(f"wallet_{index}_malformed")
            continue
        balance = next((wallet.get(name) for name in ("balance_fraction", "supply_fraction", "token_balance_fraction") if name in wallet), None)
        if balance is None:
            malformed.append(f"wallet_{index}_balance_fraction_missing")
            continue
        try:
            balances.append(_fraction(balance, f"wallet_{index}_balance_fraction"))
        except ValueError as exc:
            malformed.append(str(exc))
            continue
        if wallet.get("is_creator") is True:
            creator_seen = True
            flow = wallet.get("net_flow_fraction")
            if flow is None:
                malformed.append(f"wallet_{index}_net_flow_fraction_missing")
                continue
            try:
                flow_value = _fraction(flow, f"wallet_{index}_net_flow_fraction", allow_negative=True)
            except ValueError as exc:
                malformed.append(str(exc))
                continue
            if flow_value < 0:
                creator_outflow += -flow_value
                creator_sold = True
    if not balances:
        malformed.append("wallet_balances_missing")
    if not creator_seen:
        return {}, ["creator_wallet_evidence_missing"], malformed
    return {
        "largest_holder_fraction": max(balances, default=Decimal("0")),
        "top_10_holder_fraction": min(sum(sorted(balances, reverse=True)[:10]), Decimal("1")),
        "creator_outflow_fraction": min(creator_outflow, Decimal("1")),
        "creator_sold": creator_sold,
    }, [], malformed


def _result(status: str, reasons: Sequence[str], *, as_of: datetime | None, gates: Mapping[str, bool], evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "cohort_id": "M3",
        "status": status,
        "eligible": status == "eligible",
        "paper_execution_enabled": False,
        "as_of": _iso(as_of) if as_of is not None else None,
        "gates": dict(gates),
        "reasons": _dedupe(reasons),
        "evidence": dict(evidence),
    }


def evaluate(observations: Mapping[str, Any] | Sequence[Mapping[str, Any]], *, as_of: datetime | str | None = None) -> dict[str, Any]:
    """Evaluate caller-supplied M3 wallet/prior-window evidence.

    Structural and timestamp violations are ``rejected``.  Missing evidence or
    a shorter-than-required prior window is ``unresolved``.  No value is
    inferred from missing fields.
    """
    envelope: Mapping[str, Any] | None = observations if isinstance(observations, Mapping) else None
    if envelope is not None:
        if "schema_version" in envelope and envelope.get("schema_version") != SCHEMA_VERSION:
            malformed_schema = True
        else:
            malformed_schema = False
        rows = envelope.get("observations", envelope.get("records"))
        if as_of is None:
            as_of = envelope.get("as_of", envelope.get("decision_at"))
    else:
        malformed_schema = False
        rows = observations
    reasons: list[str] = []
    malformed: list[str] = []
    if malformed_schema:
        malformed.append("schema_version_unsupported")
    cutoff: datetime | None = None
    if as_of is None:
        reasons.append("as_of_missing")
    else:
        try:
            if isinstance(as_of, datetime):
                if as_of.tzinfo is None or as_of.utcoffset() is None:
                    raise ValueError("as_of_timezone_missing")
                cutoff = as_of.astimezone(timezone.utc)
            else:
                cutoff = _timestamp(as_of, "as_of")
        except (AttributeError, ValueError) as exc:
            malformed.append(str(exc))
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        malformed.append("observations_malformed")
        rows = []
    if not rows:
        reasons.append("observations_missing")

    parsed: list[tuple[datetime, Mapping[str, Any]]] = []
    metrics: list[dict[str, Decimal | bool]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            malformed.append(f"observation_{index}_malformed")
            continue
        raw_time = row.get("observed_at", row.get("timestamp"))
        if raw_time is None:
            reasons.append("observation_timestamp_missing")
            continue
        try:
            observed = _timestamp(raw_time, f"observation_{index}_timestamp")
        except ValueError as exc:
            malformed.append(str(exc))
            continue
        for field in ("known_at", "received_at"):
            if field in row:
                try:
                    auxiliary = _timestamp(row[field], f"observation_{index}_{field}")
                    if cutoff is not None and auxiliary > cutoff:
                        malformed.append("observation_after_as_of")
                    if auxiliary < observed and field == "received_at":
                        malformed.append("received_before_observed")
                except ValueError as exc:
                    malformed.append(str(exc))
        if cutoff is not None and observed > cutoff:
            malformed.append("observation_after_as_of")
        record_metrics, missing, bad = _wallet_metrics(row)
        reasons.extend(missing)
        malformed.extend(bad)
        parsed.append((observed, row))
        metrics.append(record_metrics)

    if cutoff is not None:
        for _, row in parsed:
            window = row.get("prior_window")
            if not isinstance(window, Mapping):
                continue
            for field in ("start_at", "end_at"):
                if field in window:
                    try:
                        window_time = _timestamp(window[field], f"prior_window_{field}")
                        if window_time > cutoff:
                            malformed.append("prior_window_after_as_of")
                    except ValueError as exc:
                        malformed.append(str(exc))

    if parsed:
        ordered = sorted(parsed, key=lambda item: item[0])
        start = ordered[0][0]
        end = ordered[-1][0]
        prior_hours = Decimal(str((end - start).total_seconds())) / Decimal("3600")
    else:
        prior_hours = Decimal("0")
    explicit_windows = [row.get("prior_window") for _, row in parsed if isinstance(row.get("prior_window"), Mapping)]
    if explicit_windows:
        for window in explicit_windows:
            if window.get("complete") is not True:
                reasons.append("prior_window_incomplete")
            count = window.get("observation_count")
            if isinstance(count, bool) or not isinstance(count, int) or count < MIN_OBSERVATIONS:
                reasons.append("prior_window_observations_below_minimum")
        try:
            starts = [_timestamp(window["start_at"], "prior_window_start_at") for window in explicit_windows]
            ends = [_timestamp(window["end_at"], "prior_window_end_at") for window in explicit_windows]
            prior_hours = max((end - start).total_seconds() for start, end in zip(starts, ends)) / 3600
        except (KeyError, TypeError, ValueError):
            reasons.append("prior_window_malformed")
    if prior_hours < Decimal(str(MIN_PRIOR_WINDOW.total_seconds() / 3600)):
        reasons.append("prior_window_insufficient")
    if len(parsed) < MIN_OBSERVATIONS:
        reasons.append("prior_window_observations_below_minimum")

    concentration_values = [value["largest_holder_fraction"] for value in metrics if "largest_holder_fraction" in value]
    top10_values = [value["top_10_holder_fraction"] for value in metrics if "top_10_holder_fraction" in value]
    outflow_values = [value["creator_outflow_fraction"] for value in metrics if "creator_outflow_fraction" in value]
    creator_sold = any(value.get("creator_sold") is True for value in metrics)
    if concentration_values and max(concentration_values) > MAX_LARGEST_HOLDER_FRACTION:
        reasons.append("wallet_concentration_above_threshold")
    if top10_values and max(top10_values) > MAX_TOP_10_HOLDER_FRACTION:
        reasons.append("wallet_concentration_above_threshold")
    if outflow_values and max(outflow_values) > MAX_CREATOR_OUTFLOW_FRACTION:
        reasons.append("creator_flow_above_threshold")
    if creator_sold:
        reasons.append("creator_selling_detected")

    if malformed:
        status = "rejected"
        reasons = malformed + reasons
    elif any(reason in reasons for reason in ("wallet_concentration_above_threshold", "creator_flow_above_threshold", "creator_selling_detected")):
        status = "rejected"
    elif reasons:
        status = "unresolved"
    else:
        status = "eligible"
    gates = {
        "causal_timestamps": not malformed,
        "prior_window_sufficient": "prior_window_insufficient" not in reasons and "prior_window_incomplete" not in reasons,
        "wallet_concentration_clear": "wallet_concentration_above_threshold" not in reasons,
        "creator_flow_clear": not any(reason in reasons for reason in ("creator_flow_above_threshold", "creator_selling_detected")),
    }
    evidence = {
        "observation_count": len(parsed),
        "prior_window_hours": str(prior_hours),
        "max_largest_holder_fraction": str(max(concentration_values)) if concentration_values else None,
        "max_top_10_holder_fraction": str(max(top10_values)) if top10_values else None,
        "max_creator_outflow_fraction": str(max(outflow_values)) if outflow_values else None,
    }
    return _result(status, reasons, as_of=cutoff, gates=gates, evidence=evidence)


def evaluate_m3(observations: Mapping[str, Any] | Sequence[Mapping[str, Any]], *, as_of: datetime | str | None = None) -> dict[str, Any]:
    """Named alias for callers that prefer an explicit strategy function."""
    return evaluate(observations, as_of=as_of)


assess = evaluate
