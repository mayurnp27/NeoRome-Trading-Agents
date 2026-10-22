"""Subprocess entry point for the registered deterministic validators.

This module contains only explicit, local dispatch.  It is intentionally not
an adapter layer: every input shape is the native contract of the validator it
names.  Validation errors become a data-not-ready result; import/runtime
failures exit nonzero so the parent can isolate that desk and continue.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def _load(path: Path) -> Any:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _equities(path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    from desks.equities.walkforward import WalkForwardSpec, evaluate, load_csv

    spec = WalkForwardSpec(config["development"][0], config["development"][1], config["validation"][0], config["validation"][1], config["holdout"][0], config["holdout"][1], int(config.get("purge_days", 1)), config.get("cadence", "daily"))
    return evaluate(config["strategy"], load_csv(path), spec, costs_bps_per_side=tuple(config.get("cost_bps", [0.0, 5.0, 15.0, 30.0])))


def _options(path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    from desks.options.walk_forward import load_dataset, run_walk_forward

    return run_walk_forward(load_dataset(path), train_sessions=int(config.get("train_sessions", 200)), validation_sessions=int(config.get("validation_sessions", 30)), holdout_sessions=int(config.get("holdout_sessions", 30)), step_sessions=int(config.get("step_sessions", 30)), allow_synthetic=False)


def _crypto(path: Path, _config: Mapping[str, Any]) -> dict[str, Any]:
    from desks.crypto.walkforward import build_report

    return build_report(_load(path))


def _prediction(path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    from neorome.prediction_walkforward import replay

    data = _load(path)
    if not isinstance(data, Mapping):
        raise ValueError("prediction input must be an object")
    result: dict[str, Any] = {"walkforward": replay(data.get("events", []), fee_schedule=config["fee_schedule"], edge_floor_cents=int(config.get("edge_floor_cents", 3)), development_events=int(config["development_events"]), validation_events=int(config["validation_events"]), holdout_events=int(config["holdout_events"]))}
    baseline = config.get("baseline")
    if baseline is not None:
        if not isinstance(baseline, Mapping):
            raise ValueError("prediction.config.baseline must be an object")
        from neorome.prediction_market_baseline import build_dataset

        result["baseline"] = build_dataset(data.get("baseline_records", []), family=str(baseline["family"]), source_identity=baseline["source_identity"], development_events=int(baseline.get("development_events", 30)), holdout_events=int(baseline.get("holdout_events", 20)))
    return result


_DISPATCH = {"equities": _equities, "options": _options, "crypto": _crypto, "prediction": _prediction}


def execute(desk: str, path: Path, config: Mapping[str, Any]) -> dict[str, Any]:
    if desk not in _DISPATCH:
        raise ValueError(f"unsupported desk: {desk}")
    try:
        report = _DISPATCH[desk](path, config)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        return {"status": "data_not_ready", "blockers": [str(exc)], "report": None}
    return {"status": "completed", "blockers": [], "report": report}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--desk", required=True, choices=sorted(_DISPATCH))
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    config = json.loads(args.config)
    result = execute(args.desk, Path(args.input), config)
    destination = Path(args.output)
    if destination.exists() or destination.is_symlink():
        raise SystemExit("worker output already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["execute", "main"]
