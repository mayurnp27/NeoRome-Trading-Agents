"""Prospective weather calibration from registered, paired event records.

The module is intentionally data-only: it receives already archived pairs and
an explicitly reviewed settlement mapping.  It never fetches forecasts,
infers a station mapping, or sends an order.  A missing mapping, missing
identity field, incomplete 30/20 chronological protocol, or weak holdout
comparison yields an abstention instead of a probability.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

from .weather_validation import digest

UTC = timezone.utc
VERSION = "prediction-weather-probability-v1"
PROTOCOL = "prediction-weather-30dev-20holdout-v1"
DEVELOPMENT_EVENTS = 30
HOLDOUT_EVENTS = 20
EPSILON = 0.001


class WeatherProbabilityError(ValueError):
    pass


def _time(value: Any, field: str) -> datetime:
    try:
        value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise WeatherProbabilityError(f"{field} must be an ISO timestamp") from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise WeatherProbabilityError(f"{field} requires a timezone")
    return value.astimezone(UTC)


def _decimal(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WeatherProbabilityError(f"{field} must be numeric") from exc
    if not result.is_finite():
        raise WeatherProbabilityError(f"{field} must be finite")
    return result


def _hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value.lower()):
        raise WeatherProbabilityError(f"{field} must be a SHA-256 digest")
    return value.lower()


def _abstain(reasons: Sequence[str], *, samples: int = 0) -> dict[str, Any]:
    return {"version": VERSION, "protocol": PROTOCOL, "status": "abstained",
            "calibration_eligible": False, "paper_eligible": False,
            "samples": samples, "development_events": 0, "holdout_events": 0,
            "abstention_reasons": sorted(set(reasons)), "probabilities": None,
            "orders_sent": 0, "network_calls": 0}


def mapping_identity(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the reviewed source/station/terms/cutoff identity contract."""
    if not isinstance(mapping, Mapping):
        raise WeatherProbabilityError("reviewed station/source mapping is required")
    body = {key: mapping.get(key) for key in ("version", "station_identity", "source_identity", "terms_schema", "cutoff_policy")}
    if body["version"] != "weather-station-settlement-map-v1" or not isinstance(body["station_identity"], str) or not body["station_identity"]:
        raise WeatherProbabilityError("station mapping identity is unsupported")
    if not isinstance(body["source_identity"], Mapping) or not body["source_identity"]:
        raise WeatherProbabilityError("settlement source identity is required")
    if body["terms_schema"] != "kalshi-weather-terms-v1" or not isinstance(body["cutoff_policy"], str) or not body["cutoff_policy"]:
        raise WeatherProbabilityError("terms/cutoff mapping identity is unsupported")
    expected = _hash(mapping.get("mapping_sha256"), "mapping_sha256")
    if digest(body) != expected:
        raise WeatherProbabilityError("mapping hash mismatch")
    return {**body, "mapping_sha256": expected}


def _rule(contract: Mapping[str, Any]) -> tuple[str, Decimal | None, Decimal | None]:
    if not isinstance(contract, Mapping) or not isinstance(contract.get("ticker"), str):
        raise WeatherProbabilityError("contract definition is malformed")
    rule = contract.get("outcome_rule")
    floor = contract.get("floor_f")
    cap = contract.get("cap_f")
    if rule == "inclusive_range":
        return rule, _decimal(floor, "contract.floor_f"), _decimal(cap, "contract.cap_f")
    if rule == "at_or_above":
        return rule, _decimal(floor, "contract.floor_f"), None
    if rule == "below":
        return rule, None, _decimal(cap, "contract.cap_f")
    raise WeatherProbabilityError("contract outcome rule is unsupported")


def _contains(contract: Mapping[str, Any], value: Decimal) -> bool:
    rule, floor, cap = _rule(contract)
    if rule == "inclusive_range": return floor <= value <= cap
    if rule == "at_or_above": return value >= floor
    return value < cap


def _pair(pair: Mapping[str, Any], identity: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(pair, Mapping): raise WeatherProbabilityError("pair is malformed")
    body = dict(pair); expected = _hash(body.pop("pair_sha256", None), "pair_sha256")
    if digest(body) != expected: raise WeatherProbabilityError("pair hash mismatch")
    if pair.get("source_kind") != "observed" or pair.get("temporal_and_identity_validated") is not True or pair.get("station_mapping_certified") is not True:
        raise WeatherProbabilityError("pair is not prospectively identity validated")
    known, outcome = _time(pair.get("forecast_known_at"), "forecast_known_at"), _time(pair.get("outcome_known_at"), "outcome_known_at")
    if known >= outcome: raise WeatherProbabilityError("forecast was not known before outcome")
    ci = pair.get("calibration_identity")
    required = {key: identity[key] for key in ("mapping_sha256", "station_identity", "source_identity", "terms_schema", "cutoff_policy")}
    if not isinstance(ci, Mapping) or any(ci.get(key) != value for key, value in required.items()):
        raise WeatherProbabilityError("pair station/source/terms/cutoff identity differs")
    contracts = pair.get("contract_definitions")
    labels = pair.get("labels")
    if not isinstance(contracts, list) or not contracts or not isinstance(labels, Mapping):
        raise WeatherProbabilityError("pair contract definitions are required")
    by_ticker = {}
    for contract in contracts:
        if not isinstance(contract, Mapping): raise WeatherProbabilityError("pair contract is malformed")
        ticker = contract.get("ticker")
        if ticker not in labels or ticker in by_ticker or labels[ticker] not in (0, 1): raise WeatherProbabilityError("pair labels are not one-to-one binary")
        _hash(contract.get("terms_sha256"), "contract.terms_sha256"); _time(contract.get("cutoff_at"), "contract.cutoff_at"); _rule(contract)
        by_ticker[ticker] = dict(contract)
    if set(labels) != set(by_ticker) or sum(labels.values()) != 1:
        raise WeatherProbabilityError("pair must contain one mutually exclusive settled outcome")
    return {"event_ticker": pair.get("event_ticker"), "target_date": pair.get("target_date"), "pair_sha256": expected,
            "predictor": _decimal(pair.get("predictor_max_f"), "predictor_max_f"),
            "outcome": _decimal(pair.get("venue_reported_max_f"), "venue_reported_max_f"),
            "residual": _decimal(pair.get("residual_f"), "residual_f"), "contracts": by_ticker,
            "labels": dict(labels), "forecast_known_at": known, "outcome_known_at": outcome}


def _contract_key(contract: Mapping[str, Any]) -> str:
    rule, floor, cap = _rule(contract)
    return f"{rule}:{floor if floor is not None else ''}:{cap if cap is not None else ''}"


def _probability(residuals: Sequence[Decimal], predictor: Decimal, contract: Mapping[str, Any]) -> Decimal:
    if not residuals: raise WeatherProbabilityError("development residuals are missing")
    return Decimal(sum(_contains(contract, predictor + r) for r in residuals)) / Decimal(len(residuals))


def _bounds(successes: int, n: int) -> tuple[Decimal, Decimal]:
    # Wilson 95% bounds are conservative for a small empirical residual sample.
    z = 1.959963984540054
    p = successes / n; denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denominator
    lower = Decimal("0") if successes == 0 else Decimal(str(max(0.0, center - radius)))
    upper = Decimal("1") if successes == n else Decimal(str(min(1.0, center + radius)))
    return lower, upper


def _score(rows: Sequence[dict[str, Any]], residuals: Sequence[Decimal]) -> tuple[dict[str, str], dict[str, str]]:
    totals = [0.0, 0.0]; baseline_totals = [0.0, 0.0]; count = 0
    frequencies: dict[str, tuple[int, int]] = {}
    for row in rows[:DEVELOPMENT_EVENTS]:
        for ticker, contract in row["contracts"].items():
            key = _contract_key(contract); yes, n = frequencies.get(key, (0, 0)); frequencies[key] = (yes + row["labels"][ticker], n + 1)
    for row in rows[DEVELOPMENT_EVENTS:DEVELOPMENT_EVENTS + HOLDOUT_EVENTS]:
        for ticker, contract in row["contracts"].items():
            p = float(_probability(residuals, row["predictor"], contract)); y = row["labels"][ticker]
            yes, n = frequencies.get(_contract_key(contract), (0, 0)); base = yes / n
            p_log, base_log = min(max(p, EPSILON), 1 - EPSILON), min(max(base, EPSILON), 1 - EPSILON)
            totals[0] += (p - y) ** 2; totals[1] += -(y * math.log(p_log) + (1 - y) * math.log(1 - p_log))
            baseline_totals[0] += (base - y) ** 2; baseline_totals[1] += -(y * math.log(base_log) + (1 - y) * math.log(1 - base_log)); count += 1
    if count == 0: raise WeatherProbabilityError("holdout scoring has no contract outcomes")
    return ({"brier": f"{totals[0] / count:.8f}", "log_loss": f"{totals[1] / count:.8f}"},
            {"brier": f"{baseline_totals[0] / count:.8f}", "log_loss": f"{baseline_totals[1] / count:.8f}"})


def calibrate(pairs: Sequence[Mapping[str, Any]], mapping: Mapping[str, Any] | None) -> dict[str, Any]:
    """Freeze 30 development plus 20 untouched chronological events."""
    if mapping is None: return _abstain(["weather_station_source_mapping_missing"], samples=len(pairs))
    try: identity = mapping_identity(mapping)
    except WeatherProbabilityError as exc: return _abstain([str(exc)], samples=len(pairs))
    if len(pairs) < DEVELOPMENT_EVENTS + HOLDOUT_EVENTS:
        return _abstain(["weather_calibration_requires_30_development_and_20_untouched_events"], samples=len(pairs))
    try:
        rows = [_pair(pair, identity) for pair in pairs]
        if len({row["event_ticker"] for row in rows}) != len(rows): raise WeatherProbabilityError("duplicate weather event")
        rows.sort(key=lambda row: (row["target_date"], row["outcome_known_at"]))
        frozen = rows[:DEVELOPMENT_EVENTS + HOLDOUT_EVENTS]
        residuals = [row["residual"] for row in frozen[:DEVELOPMENT_EVENTS]]
        metrics, baseline = _score(frozen, residuals)
        model_no_worse = float(metrics["brier"]) <= float(baseline["brier"]) and float(metrics["log_loss"]) <= float(baseline["log_loss"])
        body = {"version": VERSION, "protocol": PROTOCOL, "mapping": identity,
                "development_pair_sha256": [row["pair_sha256"] for row in frozen[:DEVELOPMENT_EVENTS]],
                "holdout_pair_sha256": [row["pair_sha256"] for row in frozen[DEVELOPMENT_EVENTS:]],
                "development_residual_f": [str(value) for value in residuals], "metrics": metrics, "baseline": baseline}
        calibration_hash = digest(body)
        return {**body, "calibration_sha256": calibration_hash,
                "status": "eligible" if model_no_worse else "abstained", "calibration_eligible": model_no_worse,
                "paper_eligible": False, "samples": len(rows), "development_events": DEVELOPMENT_EVENTS,
                "holdout_events": HOLDOUT_EVENTS, "model_no_worse_than_climatology": model_no_worse,
                "abstention_reasons": [] if model_no_worse else ["weather_holdout_does_not_beat_or_match_climatology"],
                "orders_sent": 0, "network_calls": 0}
    except WeatherProbabilityError as exc:
        return _abstain([str(exc)], samples=len(pairs))


def forecast_probability(calibration: Mapping[str, Any], forecast: Mapping[str, Any], contract: Mapping[str, Any], *, decision_at: Any) -> dict[str, Any]:
    """Derive one conservative probability using only frozen development residuals."""
    if not isinstance(calibration, Mapping) or calibration.get("status") != "eligible" or calibration.get("calibration_eligible") is not True:
        raise WeatherProbabilityError("weather calibration is not eligible")
    identity = calibration.get("mapping")
    mapping_identity(identity)
    ci = forecast.get("calibration_identity") if isinstance(forecast, Mapping) else None
    if not isinstance(ci, Mapping) or any(ci.get(key) != identity[key] for key in ("mapping_sha256", "station_identity", "source_identity", "terms_schema", "cutoff_policy")):
        raise WeatherProbabilityError("forecast station/source/terms/cutoff identity differs")
    known, decision = _time(forecast.get("known_at"), "forecast.known_at"), _time(decision_at, "decision_at")
    if known > decision: raise WeatherProbabilityError("forecast was known after decision time")
    _hash(contract.get("terms_sha256"), "contract.terms_sha256"); cutoff = _time(contract.get("cutoff_at"), "contract.cutoff_at")
    if decision >= cutoff: raise WeatherProbabilityError("forecast decision is at or after cutoff")
    residuals = [_decimal(item, "development_residual_f") for item in calibration.get("development_residual_f", [])]
    predictor = _decimal(forecast.get("predictor_max_f"), "forecast.predictor_max_f")
    successes = sum(_contains(contract, predictor + residual) for residual in residuals)
    value = Decimal(successes) / Decimal(len(residuals)); lower, upper = _bounds(successes, len(residuals))
    payload = {"calibration_sha256": calibration["calibration_sha256"], "forecast_sha256": _hash(forecast.get("forecast_sha256"), "forecast.forecast_sha256"),
               "contract_terms_sha256": contract["terms_sha256"], "decision_at": decision.isoformat().replace("+00:00", "Z"),
               "value": str(value), "lower_bound": str(lower), "upper_bound": str(upper), "successes": successes, "development_events": len(residuals)}
    return {"value": str(value), "lower_bound": str(lower), "upper_bound": str(upper), "model_version": VERSION,
            "calibration": {"eligible": True, "out_of_sample": True, "protocol_version": PROTOCOL,
                            "development_events": DEVELOPMENT_EVENTS, "holdout_events": HOLDOUT_EVENTS},
            "provenance": {"source_kind": "observed", "source_version": VERSION, "known_at": known.isoformat().replace("+00:00", "Z"),
                           "payload": payload, "sha256": digest(payload)}}


__all__ = ["WeatherProbabilityError", "calibrate", "forecast_probability", "mapping_identity"]
