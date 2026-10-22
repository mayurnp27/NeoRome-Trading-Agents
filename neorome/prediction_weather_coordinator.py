"""Finite coordinator from calibrated weather evidence to internal paper.

All inputs are caller-supplied archived records.  This module has no HTTP,
credentials, venue order, or scheduler implementation.  It refuses to invent
a weather probability from a forecast or a venue label; only the frozen
30-development/20-holdout calibration module may supply one.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
from typing import Any, Callable, Mapping

from .paper_ledger import PaperLedger
from .prediction_internal_paper import prepare as paper_prepare, run as paper_run
from .prediction_observed_shadow import SHADOW_VERSION, run_once as shadow_run
from .weather_probability import WeatherProbabilityError, calibrate, forecast_probability
from .weather_validation import digest


MAX_PAIR_FILES = 256
MAX_PAIR_BYTES = 64 * 1024 * 1024
MAX_PAIR_DEPTH = 4
DEFAULT_STATE = "/var/lib/neorome-prediction-weather-coordinator-status"


class WeatherCoordinatorError(ValueError):
    pass


def _abstain(reasons: list[str], calibration: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"mode": "prediction_weather_internal_paper", "status": "abstained",
            "calibration": dict(calibration or {}), "candidate": None,
            "candidate_sha256": None, "abstention_reasons": sorted(set(reasons)),
            "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0,
            "profitability_validated": False}


def coordinate(pairs: list[Mapping[str, Any]], mapping: Mapping[str, Any] | None, *,
               forecast: Mapping[str, Any] | None = None, execution: Mapping[str, Any] | None = None,
               now: Any = None, book: PaperLedger | None = None, state_dir: str | None = None,
               mark_provider: Callable[[PaperLedger, Any], Any] | None = None,
               allow_entry: bool = True, recovery_ok: bool = True,
               later_quote: Mapping[str, Any] | None = None, settlement: Mapping[str, Any] | None = None,
               request_close: bool = False) -> dict[str, Any]:
    """Calibrate then optionally advance one shared-ledger paper action."""
    calibration = calibrate(pairs, mapping)
    if calibration.get("status") != "eligible":
        return _abstain(list(calibration.get("abstention_reasons", ["weather_calibration_not_eligible"])), calibration)
    if not isinstance(forecast, Mapping) or not isinstance(execution, Mapping):
        return _abstain(["fresh_weather_forecast_and_executable_book_required"], calibration)
    try:
        weather_contract = forecast.get("contract")
        if not isinstance(weather_contract, Mapping):
            raise WeatherCoordinatorError("forecast contract identity is required")
        adapter = execution.get("adapter")
        market = execution.get("market_identity")
        timing = execution.get("timing")
        quote = execution.get("quote")
        fees = execution.get("fee_schema")
        settlement_definition = execution.get("settlement_definition")
        if not all(isinstance(item, Mapping) for item in (adapter, market, timing, quote, fees, settlement_definition)):
            raise WeatherCoordinatorError("complete executable weather inputs are required")
        contract = adapter.get("contract")
        if not isinstance(contract, Mapping) or weather_contract.get("terms_sha256") != market.get("terms_sha256") or market.get("terms_sha256") != contract.get("terms", {}).get("terms_sha256"):
            raise WeatherCoordinatorError("forecast, executable book, and contract terms differ")
        if weather_contract.get("cutoff_at") != contract.get("cutoff_at"):
            raise WeatherCoordinatorError("forecast and executable cutoff differ")
        probability = forecast_probability(calibration, forecast, weather_contract, decision_at=timing.get("decision_at"))
        envelope = {"schema_version": 1, "shadow_version": SHADOW_VERSION, "source_kind": "observed",
                    "execution_enabled": False, "adapter": adapter, "market_identity": market,
                    "timing": timing, "quote": quote, "fee_schema": fees,
                    "settlement_definition": settlement_definition, "probability": probability}
        shadow = shadow_run(envelope, now=now)
        if shadow.get("status") != "eligible_shadow_candidate":
            return _abstain(list(shadow.get("abstention_reasons", ["weather_shadow_rejected"])), calibration)
        # Check the same fresh conservative-edge and per-idea admission that
        # the persistent lifecycle will use, even in candidate-only mode.
        # This prevents a stale shadow record from being reported as ready.
        paper_prepare(shadow["candidate"], now=now)
        if book is None or state_dir is None:
            return {"mode": "prediction_weather_internal_paper", "status": "eligible_shadow_candidate",
                    "calibration": calibration, "candidate": shadow["candidate"], "candidate_sha256": shadow["candidate_sha256"],
                    "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0,
                    "profitability_validated": False}
        lifecycle = paper_run(book, state_dir, now=now, candidate=shadow["candidate"], later_quote=later_quote,
                              settlement=settlement, request_close=request_close, mark_provider=mark_provider,
                              allow_entry=allow_entry, recovery_ok=recovery_ok)
        return {"mode": "prediction_weather_internal_paper", "status": "paper_lifecycle",
                "calibration": calibration, "candidate": shadow["candidate"], "candidate_sha256": shadow["candidate_sha256"],
                "lifecycle": lifecycle, "ledger_mutations": len(lifecycle.get("actions", [])),
                "broker_orders_sent": 0, "venue_requests": 0, "profitability_validated": False}
    except (WeatherProbabilityError, WeatherCoordinatorError, ValueError, KeyError, TypeError) as exc:
        return _abstain([str(exc)], calibration)


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat(follow_symlinks=False).st_size > 4 * 1024 * 1024:
        raise WeatherCoordinatorError("unsafe coordinator input")
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "r") as stream:
            value = json.load(stream)
    except OSError as exc:
        raise WeatherCoordinatorError("unsafe coordinator input") from exc
    if not isinstance(value, dict): raise WeatherCoordinatorError("coordinator JSON object required")
    return value


def _pairs(root: str | Path) -> list[dict[str, Any]]:
    """Read only complete, hash-addressed pairs from a bounded subtree."""
    base = Path(root)
    if base.is_symlink() or (base.exists() and not base.is_dir()): raise WeatherCoordinatorError("pairs directory is unsafe")
    if not base.exists(): return []
    selected: list[dict[str, Any]] = []; total = 0; scanned = 0
    for directory, children, files in os.walk(base, topdown=True, followlinks=False):
        current = Path(directory)
        depth = len(current.relative_to(base).parts)
        if depth > MAX_PAIR_DEPTH: children[:] = []; continue
        if any((current / child).is_symlink() for child in children): raise WeatherCoordinatorError("pairs directory symlink refused")
        children.sort(); files.sort()
        for name in files:
            path = current / name
            if path.suffix != ".json" or path.is_symlink(): continue
            scanned += 1
            if scanned > MAX_PAIR_FILES: raise WeatherCoordinatorError("pair file limit exceeded")
            try: total += path.stat(follow_symlinks=False).st_size
            except OSError as exc: raise WeatherCoordinatorError("pair file inspection failed") from exc
            if total > MAX_PAIR_BYTES: raise WeatherCoordinatorError("pair scan size limit exceeded")
            value = _read_json(path)
            supplied = value.get("pair_sha256")
            if not isinstance(supplied, str): continue
            body = dict(value); body.pop("pair_sha256", None)
            if digest(body) != supplied: continue
            selected.append(value)
    return selected


def _state_root(value: str | Path) -> Path:
    root = Path(value)
    if root.exists() and root.is_symlink(): raise WeatherCoordinatorError("coordinator state directory is unsafe")
    root.mkdir(parents=True, exist_ok=True, mode=0o700); os.chmod(root, 0o700)
    return root


def _write_status(root: Path, value: Mapping[str, Any]) -> None:
    target = root / "status.json"; temporary = root / f".status.{secrets.token_hex(8)}.tmp"
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            descriptor = -1; stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if descriptor >= 0: os.close(descriptor)
        try: temporary.unlink()
        except FileNotFoundError: pass


def run_status(pairs_dir: str | Path, state: str | Path, *, mapping: Mapping[str, Any] | None = None) -> dict[str, Any]:
    root = _state_root(state)
    try:
        result = coordinate(_pairs(pairs_dir), mapping)
        result["operational_healthy"] = result.get("status") == "abstained"
    except (WeatherCoordinatorError, WeatherProbabilityError, OSError, json.JSONDecodeError) as exc:
        result = {"mode": "prediction_weather_internal_paper", "status": "failed_closed", "error": str(exc),
                  "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0, "operational_healthy": False}
    _write_status(root, result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs-dir", required=True)
    parser.add_argument("--mapping")
    parser.add_argument("--state", default=DEFAULT_STATE)
    args = parser.parse_args(argv)
    try:
        mapping = _read_json(Path(args.mapping)) if args.mapping else None
        result = run_status(args.pairs_dir, args.state, mapping=mapping)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result.get("operational_healthy") else 1
    except (WeatherCoordinatorError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"mode": "prediction_weather_internal_paper", "status": "failed_closed", "error": str(exc),
                          "ledger_mutations": 0, "broker_orders_sent": 0, "venue_requests": 0}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
