"""Deterministic, paper-only Solana wallet scoring and copy-signal analysis.

Inputs are normalized swap fills. This module never fetches data, infers transfers,
submits orders, or promises that a signal can be executed at an observed price.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable

try:
    from neorome.jupiter_quotes import SOL, USDC, validate_mint
except ImportError:  # Allows direct use after this file is installed as neorome.wallet_follow.
    from jupiter_quotes import SOL, USDC, validate_mint  # type: ignore

CURRENCIES = frozenset((USDC, SOL))
PAPER_QUALIFICATION_WARNINGS = frozenset((
    "unpriced_native_cost", "sell_exceeds_known_inventory",
    "sell_currency_mismatch", "history_or_excluded_activity_review_required",
))
_REQUIRED = frozenset((
    "wallet", "mint", "signature", "block_time", "observed_at", "side",
    "quantity", "quote_mint", "quote_quantity", "fee_quote", "source_sha256",
))


def _utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp_not_string")
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError("timestamp_invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp_timezone_required")
    if parsed.utcoffset().total_seconds() != 0:
        raise ValueError("timestamp_must_be_utc")
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any, *, positive: bool) -> Decimal:
    if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
        raise ValueError("amount_invalid")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("amount_invalid") from None
    if not result.is_finite() or (positive and result <= 0) or (not positive and result < 0):
        raise ValueError("amount_out_of_range")
    return result


def _normalized_rows(rows: Iterable[dict], cutoff: datetime, *, allow_incomplete_costs: bool = False):
    accepted = []
    issues = []
    seen = set()
    counts = {"input": 0, "accepted": 0, "duplicate": 0, "rejected": 0, "future": 0,
              "costs_incomplete": 0}
    incomplete = set()
    for index, raw in enumerate(rows):
        counts["input"] += 1
        try:
            if not isinstance(raw, dict) or not _REQUIRED.issubset(raw):
                raise ValueError("required_field_missing")
            wallet = validate_mint(raw["wallet"])
            mint = validate_mint(raw["mint"])
            signature = raw["signature"]
            if not isinstance(signature, str) or not signature or len(signature) > 128:
                raise ValueError("signature_invalid")
            side = raw["side"]
            if side not in ("buy", "sell"):
                raise ValueError("side_invalid")
            quote_mint = raw["quote_mint"]
            if quote_mint not in CURRENCIES:
                raise ValueError("quote_currency_unsupported")
            block_time = _utc(raw["block_time"])
            observed_at = _utc(raw["observed_at"])
            if observed_at < block_time:
                raise ValueError("observed_before_block_time")
            if block_time > cutoff or observed_at > cutoff:
                counts["future"] += 1
                issues.append({"row": index, "reason": "future_after_cutoff"})
                continue
            quantity = _decimal(raw["quantity"], positive=True)
            quote_quantity = _decimal(raw["quote_quantity"], positive=True)
            fee_quote = _decimal(raw["fee_quote"], positive=False)
            digest = raw["source_sha256"]
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
                raise ValueError("source_sha256_invalid")
            version = raw.get("normalization_version")
            if version is not None and (not isinstance(version, str) or not version):
                raise ValueError("normalization_version_invalid")
            costs_complete = raw.get("costs_complete", True)
            if type(costs_complete) is not bool:
                raise ValueError("costs_complete_invalid")
            key = (wallet, signature, mint, side)
            if key in seen:
                counts["duplicate"] += 1
                issues.append({"row": index, "reason": "duplicate_signature_mint_side"})
                continue
            seen.add(key)
            if not costs_complete:
                counts["costs_incomplete"] += 1
                incomplete.add((wallet, quote_mint))
                issues.append({"row": index, "wallet": wallet, "quote_mint": quote_mint,
                               "signature": signature, "reason": "unpriced_native_cost"})
                if not allow_incomplete_costs:
                    continue
            accepted.append({
                "wallet": wallet, "mint": mint, "signature": signature,
                "block_time": block_time, "observed_at": observed_at,
                "side": side, "quantity": quantity, "quote_mint": quote_mint,
                "quote_quantity": quote_quantity, "fee_quote": fee_quote,
                "source_sha256": digest.lower(),
                "normalization_version": version,
                "costs_complete": costs_complete,
            })
            counts["accepted"] += 1
        except Exception as exc:
            reason = str(exc) if isinstance(exc, ValueError) else "address_invalid"
            # QuoteError intentionally exposes only a stable safe validation message.
            counts["rejected"] += 1
            issues.append({"row": index, "reason": reason or "row_invalid"})
    accepted.sort(key=lambda item: (item["block_time"], item["observed_at"], item["wallet"],
                                    item["signature"], item["side"]))
    return accepted, issues, counts, incomplete


def _closed_lots(rows):
    lots_by_wallet_mint = defaultdict(list)
    closed = []
    issues = []
    ineligible = defaultdict(set)
    for row in rows:
        key = (row["wallet"], row["mint"])
        if row["side"] == "buy":
            lots_by_wallet_mint[key].append({
                "wallet": row["wallet"], "mint": row["mint"], "currency": row["quote_mint"],
                "entry_signature": row["signature"], "entry_time": row["block_time"],
                "remaining": row["quantity"], "entry_cost": row["quote_quantity"] + row["fee_quote"],
                "exit_proceeds": Decimal(0), "closed_quantity": Decimal(0),
            })
            continue

        lots = lots_by_wallet_mint[key]
        available = sum((lot["remaining"] for lot in lots), Decimal(0))
        if available < row["quantity"]:
            reason = "sell_exceeds_known_inventory"
            issues.append({"wallet": row["wallet"], "mint": row["mint"],
                           "quote_mint": row["quote_mint"], "signature": row["signature"],
                           "reason": reason})
            ineligible[(row["wallet"], row["quote_mint"])].add(reason)
            for lot in lots:
                if lot["remaining"] > 0:
                    ineligible[(row["wallet"], lot["currency"])].add(reason)
            continue
        if any(lot["remaining"] > 0 and lot["currency"] != row["quote_mint"] for lot in lots):
            reason = "sell_currency_mismatch"
            issues.append({"wallet": row["wallet"], "mint": row["mint"],
                           "quote_mint": row["quote_mint"], "signature": row["signature"],
                           "reason": reason})
            ineligible[(row["wallet"], row["quote_mint"])].add(reason)
            for lot in lots:
                if lot["remaining"] > 0:
                    ineligible[(row["wallet"], lot["currency"])].add(reason)
            continue

        remaining = row["quantity"]
        unit_net_proceeds = (row["quote_quantity"] - row["fee_quote"]) / row["quantity"]
        allocations = []
        for lot in lots:
            if remaining <= 0:
                break
            if lot["remaining"] <= 0:
                continue
            take = min(remaining, lot["remaining"])
            allocations.append((lot, take))
            remaining -= take
        for lot, take in allocations:
            proceeds = unit_net_proceeds * take
            lot["remaining"] -= take
            lot["closed_quantity"] += take
            lot["exit_proceeds"] += proceeds
            if lot["remaining"] == 0:
                closed.append({
                    "wallet": lot["wallet"], "mint": lot["mint"], "currency": lot["currency"],
                    "entry_signature": lot["entry_signature"], "exit_signature": row["signature"],
                    "entry_time": lot["entry_time"], "exit_time": row["block_time"],
                    "quantity": lot["closed_quantity"], "cost": lot["entry_cost"],
                    "proceeds": lot["exit_proceeds"],
                    "pnl": lot["exit_proceeds"] - lot["entry_cost"],
                })
    return closed, issues, ineligible


def score_wallets(rows: Iterable[dict], as_of: str, minimum_closed: int = 10,
                  minimum_mints: int = 5) -> dict:
    """Score fully closed FIFO buy lots available by ``as_of``.

    Qualification is performed independently for each quote currency, so USDC
    and WSOL quantities or PnL are never combined. ``rankings`` includes all
    wallet/currency cohorts with explicit qualification reasons; ``qualified``
    contains the eligible rows consumed by :func:`following_signals`.
    """
    cutoff = _utc(as_of)
    if type(minimum_closed) is not int or minimum_closed < 1:
        raise ValueError("minimum_closed must be a positive integer")
    if type(minimum_mints) is not int or minimum_mints < 1:
        raise ValueError("minimum_mints must be a positive integer")
    normalized, issues, coverage, incomplete = _normalized_rows(rows, cutoff)
    # An unpriced transaction is review evidence only; it can never create an
    # inventory lot or enter priced performance calculations.
    closed, accounting_issues, accounting_ineligible = _closed_lots(
        row for row in normalized if row.get("costs_complete", True))
    issues.extend(accounting_issues)
    cohorts = defaultdict(list)
    for lot in closed:
        cohorts[(lot["wallet"], lot["currency"])].append(lot)
    rankings = []
    for key in incomplete:
        cohorts.setdefault(key, [])
    for (wallet, currency), lots in sorted(cohorts.items()):
        pnl = [lot["pnl"] for lot in lots]
        gross_positive = sum((value for value in pnl if value > 0), Decimal(0))
        gross_negative = sum((-value for value in pnl if value < 0), Decimal(0))
        net = sum(pnl, Decimal(0))
        pf = (gross_positive / gross_negative) if gross_negative else (None if gross_positive == 0 else "infinite")
        largest = max((value for value in pnl if value > 0), default=Decimal(0))
        largest_share = (largest / gross_positive) if gross_positive else Decimal(0)
        mint_count = len({lot["mint"] for lot in lots})
        reasons = []
        if (wallet, currency) in incomplete:
            reasons.append("unpriced_native_cost")
        reasons.extend(sorted(accounting_ineligible.get((wallet, currency), ())))
        if len(lots) < minimum_closed:
            reasons.append("insufficient_closed_buy_lots")
        if mint_count < minimum_mints:
            reasons.append("insufficient_distinct_mints")
        if net <= 0:
            reasons.append("nonpositive_net_pnl")
        if gross_positive == 0 or (gross_negative > 0 and gross_positive / gross_negative < Decimal("1.5")):
            reasons.append("profit_factor_below_1_5")
        if largest_share > Decimal("0.50"):
            reasons.append("largest_winner_over_50_percent")
        rankings.append({
            "wallet": wallet, "quote_mint": currency, "qualified": not reasons,
            "ineligibility_reasons": reasons, "closed_buy_lots": len(lots),
            "distinct_mints": mint_count, "net_pnl": str(net),
            "gross_positive_pnl": str(gross_positive), "gross_negative_pnl": str(gross_negative),
            "profit_factor": pf if isinstance(pf, str) else (str(pf) if pf is not None else None),
            "largest_winner_share": str(largest_share),
            "closed_lots": [{
                "mint": lot["mint"], "entry_signature": lot["entry_signature"],
                "exit_signature": lot["exit_signature"], "cost": str(lot["cost"]),
                "proceeds": str(lot["proceeds"]), "pnl": str(lot["pnl"]),
                "quote_mint": lot["currency"],
            } for lot in lots],
        })
    rankings.sort(key=lambda item: (
        not item["qualified"], item["wallet"], item["quote_mint"]
    ))
    result = {
        "as_of": cutoff.isoformat().replace("+00:00", "Z"),
        "provenance": {"method": "fifo_closed_buy_lots_v1", "paper_only": True,
                       "input_source_hashes": sorted({row["source_sha256"] for row in normalized})},
        "coverage": {**coverage, "closed_buy_lots": len(closed),
                     "wallet_currency_cohorts": len(rankings)},
        "issues": issues,
        "rankings": rankings,
        "qualified": [item for item in rankings if item["qualified"]],
    }
    return apply_paper_eligibility(result)


def apply_paper_eligibility(ranking: dict) -> dict:
    """Add W1 paper eligibility while preserving strict research qualification.

    The helper is safe to apply to an existing frozen ranking: it does not
    recompute metrics or change ``as_of``. Existing strict qualifications and
    their reasons remain authoritative. Only the four named accounting/history
    barriers become paper warnings; every performance/history threshold still
    blocks entry to the paper cohort.
    """
    if not isinstance(ranking, dict) or not isinstance(ranking.get("rankings"), list):
        raise ValueError("ranking must contain ranked wallet cohorts")
    paper_qualified = []
    for item in ranking["rankings"]:
        if not isinstance(item, dict):
            continue
        reasons = item.get("ineligibility_reasons", [])
        valid_reasons = (isinstance(reasons, list)
                         and all(isinstance(reason, str) for reason in reasons))
        if not valid_reasons:
            reasons = ["qualification_metadata_invalid"]
        warnings = sorted({reason for reason in reasons
                           if reason in PAPER_QUALIFICATION_WARNINGS})
        blockers = [reason for reason in reasons
                    if reason not in PAPER_QUALIFICATION_WARNINGS]
        if type(item.get("qualified")) is not bool:
            blockers.append("qualification_metadata_invalid")
        elif not item["qualified"] and not reasons:
            blockers.append("strict_qualification_failed")
        item["paper_warnings"] = warnings
        item["paper_qualified"] = not blockers
        if item["paper_qualified"]:
            paper_qualified.append(item)
    ranking["paper_qualified"] = paper_qualified
    ranking["paper_evidence_label"] = "provisional_wallet_history"
    return ranking


def following_signals(rows: Iterable[dict], ranked: dict, as_of: str,
                      max_age_seconds: int = 120, eligibility: str = "strict") -> dict:
    """Return fresh buys whose on-chain block time follows the ranking cutoff.

    The ranked result must be frozen before the buy's block time. Freshness and
    same-page sell ordering use block time, since fetch observation times can
    be shared by an entire historical page.
    Signals are informational paper records only; observed fills do not imply
    an executable quote, available liquidity, or a matching price.
    """
    now = _utc(as_of)
    if type(max_age_seconds) is not int or max_age_seconds < 0:
        raise ValueError("max_age_seconds must be a nonnegative integer")
    if eligibility not in ("strict", "paper"):
        raise ValueError("eligibility must be 'strict' or 'paper'")
    cohort_key = "paper_qualified" if eligibility == "paper" else "qualified"
    # Older frozen state has only the strict cohort. Preserve compatibility
    # until the next research refresh adds paper_qualified metadata.
    if eligibility == "paper" and isinstance(ranked, dict) and cohort_key not in ranked:
        cohort_key = "qualified"
    if not isinstance(ranked, dict) or not isinstance(ranked.get(cohort_key), list):
        raise ValueError("ranked must be a score_wallets result")
    cutoff = _utc(ranked.get("as_of"))
    qualified = {(item.get("wallet"), item.get("quote_mint")) for item in ranked[cohort_key]
                 if isinstance(item, dict)}
    if cutoff > now:
        raise ValueError("ranking cutoff cannot be after as_of")
    normalized, issues, coverage, _incomplete = _normalized_rows(
        rows, now, allow_incomplete_costs=(eligibility == "paper"))
    recent_sells = defaultdict(list)
    for row in normalized:
        if row["side"] == "sell" and row["block_time"] > cutoff:
            recent_sells[(row["wallet"], row["mint"])].append(row["block_time"])
    signals = []
    herd_counts = defaultdict(set)
    for row in normalized:
        if row["side"] != "buy" or row["block_time"] <= cutoff:
            continue
        if (row["wallet"], row["quote_mint"]) not in qualified:
            continue
        age = (now - row["block_time"]).total_seconds()
        if age < 0 or age > max_age_seconds:
            issues.append({"signature": row["signature"], "reason": "signal_outside_freshness_window"})
            continue
        if any(sold_at >= row["block_time"] for sold_at in recent_sells[(row["wallet"], row["mint"])]):
            issues.append({"signature": row["signature"], "reason": "vetoed_by_pending_sell"})
            continue
        signal = {
            "wallet": row["wallet"], "mint": row["mint"], "signature": row["signature"],
            "block_time": row["block_time"].isoformat().replace("+00:00", "Z"),
            "observed_at": row["observed_at"].isoformat().replace("+00:00", "Z"),
            "quote_mint": row["quote_mint"], "paper_only": True,
            "execution_price_guaranteed": False,
        }
        if not row.get("costs_complete", True):
            signal["paper_warnings"] = ["unpriced_native_cost"]
        signals.append(signal)
        herd_counts[row["mint"]].add(row["wallet"])
    signals.sort(key=lambda item: (item["observed_at"], item["mint"], item["wallet"], item["signature"]))
    herds = [{"mint": mint, "qualified_wallet_count": len(wallets),
              "wallets": sorted(wallets), "alert": "qualified_wallet_herd"}
             for mint, wallets in sorted(herd_counts.items()) if len(wallets) >= 2]
    return {
        "as_of": now.isoformat().replace("+00:00", "Z"),
        "ranking_cutoff": cutoff.isoformat().replace("+00:00", "Z"),
        "paper_only": True,
        "eligibility": ("provider_top10" if ranked.get("selection_basis") == "daily_frozen_provider_leaderboard"
                        else "paper_qualified") if eligibility == "paper" else "qualified",
        "evidence_label": (ranked.get("paper_evidence_label", "provisional_wallet_history")
                           if eligibility == "paper" else "strict_research_qualified"),
        "signals": signals, "herd_alerts": herds,
        "coverage": coverage, "issues": issues,
    }
