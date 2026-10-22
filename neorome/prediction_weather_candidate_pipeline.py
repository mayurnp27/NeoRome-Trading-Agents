"""Join frozen weather calibration, forecast, and execution snapshot offline."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .prediction_weather_coordinator import _pairs, coordinate
from .prediction_weather_inbox import publish_candidate

UTC = timezone.utc


class PredictionWeatherCandidatePipelineError(ValueError):
    pass


def _read(path: str | os.PathLike[str]) -> dict[str, Any]:
    source = Path(path)
    if source.is_symlink() or not source.is_file() or source.stat().st_size > 8 * 1024 * 1024: raise PredictionWeatherCandidatePipelineError("pipeline input is unsafe")
    try: value = json.loads(source.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise PredictionWeatherCandidatePipelineError("pipeline input is unreadable") from exc
    if not isinstance(value, dict): raise PredictionWeatherCandidatePipelineError("pipeline input must be an object")
    return value


def _now(value: Any) -> datetime:
    if value is None: return datetime.now(UTC)
    try: parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc: raise PredictionWeatherCandidatePipelineError("now must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None: raise PredictionWeatherCandidatePipelineError("now must include timezone")
    return parsed.astimezone(UTC)


def run_once(*, pairs_dir: str | os.PathLike[str], mapping: Mapping[str, Any] | None, forecast: Mapping[str, Any] | None,
             snapshot: Mapping[str, Any] | None, inbox_root: str | os.PathLike[str] | None = None, now: Any = None) -> dict[str, Any]:
    """Produce and optionally publish only an already-eligible candidate."""
    current = _now(now)
    if not isinstance(snapshot, Mapping) or snapshot.get("status") != "eligible_execution_snapshot":
        return {"mode": "prediction_weather_candidate_pipeline", "status": "abstained", "abstention_reasons": ["execution_snapshot_not_eligible"], "orders_sent": 0, "ledger_mutations": 0}
    if not isinstance(forecast, Mapping) or not isinstance(forecast.get("contract"), Mapping):
        return {"mode": "prediction_weather_candidate_pipeline", "status": "abstained", "abstention_reasons": ["weather_forecast_contract_missing"], "orders_sent": 0, "ledger_mutations": 0}
    ticker = forecast["contract"].get("ticker")
    executions = snapshot.get("executions")
    execution = next((dict(item) for item in executions if isinstance(item, Mapping) and item.get("market_identity", {}).get("market_id") == ticker), None) if isinstance(executions, list) else None
    if execution is None:
        return {"mode": "prediction_weather_candidate_pipeline", "status": "abstained", "abstention_reasons": ["forecast_contract_not_in_execution_snapshot"], "orders_sent": 0, "ledger_mutations": 0}
    timing = dict(execution.get("snapshot_timing") or {})
    timing["decision_at"] = current.isoformat().replace("+00:00", "Z")
    execution["timing"] = timing; execution.pop("snapshot_timing", None)
    result = coordinate(_pairs(pairs_dir), mapping, forecast=forecast, execution=execution, now=current)
    result = {"mode": "prediction_weather_candidate_pipeline", **result, "orders_sent": 0, "ledger_mutations": 0}
    if result.get("status") == "eligible_shadow_candidate" and inbox_root is not None:
        published = publish_candidate(result["candidate"], inbox_root, execution_snapshot_sha256=snapshot.get("snapshot_sha256"))
        result["candidate_inbox_path"] = str(published)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs-dir", required=True); parser.add_argument("--mapping"); parser.add_argument("--forecast"); parser.add_argument("--snapshot", required=True)
    parser.add_argument("--publish-inbox"); parser.add_argument("--now")
    args = parser.parse_args(argv)
    try:
        result = run_once(pairs_dir=args.pairs_dir, mapping=_read(args.mapping) if args.mapping else None,
                          forecast=_read(args.forecast) if args.forecast else None, snapshot=_read(args.snapshot), inbox_root=args.publish_inbox, now=args.now)
        print(json.dumps(result, sort_keys=True, separators=(",", ":"))); return 0 if result["status"] in {"abstained", "eligible_shadow_candidate"} else 1
    except (PredictionWeatherCandidatePipelineError, ValueError, OSError) as exc:
        print(json.dumps({"mode": "prediction_weather_candidate_pipeline", "status": "failed_closed", "error": str(exc), "orders_sent": 0, "ledger_mutations": 0}, sort_keys=True)); return 1


if __name__ == "__main__":
    raise SystemExit(main())
