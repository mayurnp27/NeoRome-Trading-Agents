"""Frozen, paper-only memecoin experiment definitions.

This module is deliberately an *assessment* layer.  It does not build a swap,
touch a wallet, or make a paper-ledger reservation.  Its job is to retain the
same candidate under every predeclared hypothesis, including the reasons a
hypothesis could not be evaluated from the evidence that existed at the time.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


EXPERIMENT_VERSION = "memecoin-cohorts-v1"

COHORTS: tuple[dict[str, str], ...] = (
    {
        "id": "M0",
        "strategy": "solana-memecoin-attention",
        "version": "memecoin-paper-v1",
        "hypothesis": "screened attention plus executable sellability can support a short paper hold",
        "lifecycle": "active_baseline",
    },
    {
        "id": "M1",
        "strategy": "solana-launch-event-sniper",
        "version": "m1-v1",
        "hypothesis": "a causal launch event may enable a short-horizon entry before liquidity disappears",
        "lifecycle": "blocked_on_launch_feed",
    },
    {
        "id": "M2",
        "strategy": "solana-liquid-momentum-control",
        "version": "m2-v1",
        "hypothesis": "slower liquid-token momentum can survive modeled DEX costs",
        "lifecycle": "blocked_on_point_in_time_history",
    },
    {
        "id": "M3",
        "strategy": "solana-manipulation-aware-momentum",
        "version": "m3-v1",
        "hypothesis": "short-horizon momentum after manipulation-risk filters can improve executable outcomes",
        "lifecycle": "blocked_on_prior_window_and_wallet_evidence",
    },
)


def _number(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _assessment(cohort: Mapping[str, str], status: str, reasons: list[str]) -> dict[str, Any]:
    return {
        "cohort_id": cohort["id"],
        "strategy": cohort["strategy"],
        "strategy_version": cohort["version"],
        "hypothesis": cohort["hypothesis"],
        "lifecycle": cohort["lifecycle"],
        "status": status,
        "reasons": sorted(set(reasons)),
        "paper_execution_enabled": False,
    }


def assess(candidate_record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Assess all frozen cohorts from point-in-time candidate evidence.

    ``eligible_for_internal_paper`` belongs to the baseline's existing gate.
    The other cohorts remain observational until their distinct feed/history
    requirements are present and separately enabled.
    """
    if not isinstance(candidate_record, Mapping):
        raise ValueError("candidate record must be a mapping")
    pool = candidate_record.get("pool") if isinstance(candidate_record.get("pool"), Mapping) else {}
    social = candidate_record.get("social") if isinstance(candidate_record.get("social"), Mapping) else {}
    results: list[dict[str, Any]] = []

    baseline_reasons = [] if candidate_record.get("eligible_for_internal_paper") else ["baseline_admission_failed"]
    results.append(_assessment(COHORTS[0], "eligible" if not baseline_reasons else "rejected", baseline_reasons))

    launch = candidate_record.get("launch_event")
    m1_reasons = []
    if not isinstance(launch, Mapping):
        m1_reasons.append("launch_event_missing")
    else:
        for field in ("signature", "slot", "received_at"):
            if not launch.get(field):
                m1_reasons.append(f"launch_event_{field}_missing")
    results.append(_assessment(COHORTS[1], "unresolved" if m1_reasons else "candidate_ready", m1_reasons))

    history = candidate_record.get("point_in_time_history")
    m2_reasons = []
    if not isinstance(history, Mapping):
        m2_reasons.append("point_in_time_history_missing")
    else:
        if _number(history.get("completed_weeks")) is None or _number(history.get("completed_weeks")) < 26:
            m2_reasons.append("completed_history_below_26_weeks")
        if _number(history.get("market_cap_usd")) is None or _number(history.get("market_cap_usd")) < Decimal("1000000"):
            m2_reasons.append("market_cap_below_1000000_usd")
        if _number(history.get("return_14d")) is None or _number(history.get("amihud_illiquidity")) is None:
            m2_reasons.append("momentum_or_illiquidity_missing")
    results.append(_assessment(COHORTS[2], "unresolved" if m2_reasons else "candidate_ready", m2_reasons))

    m3_reasons = []
    previous_volume = _number(pool.get("volume_previous_h1_usd"))
    current_volume = _number(pool.get("volume_h1_usd"))
    return_h1 = _number(pool.get("return_h1_fraction"))
    impact = _number((candidate_record.get("entry_quote") or {}).get("quote", {}).get("priceImpact"))
    wallet = candidate_record.get("wallet_risk")
    if previous_volume is None or current_volume is None:
        m3_reasons.append("prior_or_current_h1_volume_missing")
    elif previous_volume <= 0 or current_volume < previous_volume * 5:
        m3_reasons.append("volume_acceleration_below_5x")
    if return_h1 is None or return_h1 <= 0:
        m3_reasons.append("positive_h1_return_missing")
    if impact is None or abs(impact) > Decimal("2"):
        m3_reasons.append("route_impact_missing_or_above_2pct")
    if not isinstance(wallet, Mapping):
        m3_reasons.append("wallet_risk_evidence_missing")
    elif wallet.get("circular_volume_flag") or wallet.get("coordinated_buy_cluster_flag"):
        m3_reasons.append("manipulation_risk_flagged")
    if not social:
        m3_reasons.append("social_evidence_missing")
    results.append(_assessment(COHORTS[3], "unresolved" if m3_reasons else "candidate_ready", m3_reasons))
    return results
