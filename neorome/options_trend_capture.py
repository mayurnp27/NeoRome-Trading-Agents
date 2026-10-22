"""Finite, read-only paired SPY/QQQ daily archive for options trend inputs."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping

from . import recorder
from .equity_context import EquityContextClient
from .equity_data import EquityDataClient
from .options_dated_artifacts import DatedArtifactError, resolve_current, save_stage

UTC = timezone.utc
MAX_REQUESTS_PER_SYMBOL = 5
MAX_TOTAL_REQUESTS = 10


class TrendCaptureError(ValueError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _now(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TrendCaptureError("clock must be timezone-aware")
    return value.astimezone(UTC)


def collect_symbol(symbol: str, clock: Callable[[], datetime]) -> tuple[dict[str, Any], dict[str, Any]]:
    """The only provider route: five GET observations for one fixed ETF."""
    context = EquityContextClient(credentials=recorder.credentials, clock=clock).observe(symbol)
    observation = EquityDataClient(credentials=recorder.credentials, clock=clock).observe(symbol)
    return context, observation


def _record(symbol: str, context: Mapping[str, Any], observation: Mapping[str, Any], collected_at: datetime) -> dict[str, Any]:
    if context.get("source_kind") != "observed" or observation.get("source_kind") != "observed":
        raise TrendCaptureError("only observed daily evidence is accepted")
    if context.get("asset", {}).get("symbol") != symbol or observation.get("symbol") != symbol or observation.get("feed") != "iex":
        raise TrendCaptureError("daily evidence has the wrong symbol or feed")
    if context.get("requests") != 3 or observation.get("request_count") != 2:
        raise TrendCaptureError("daily request budget was not exactly five GETs")
    if context.get("order_mutations") != 0 or observation.get("broker_orders") != 0 or observation.get("execution_enabled") is not False:
        raise TrendCaptureError("daily evidence violates read-only attestations")
    bundle = {"schema_version": 1, "source_kind": "observed", "symbol": symbol, "feed": "iex", "context": dict(context),
              "observation": dict(observation), "order_mutations": 0, "broker_orders_sent": 0,
              "execution_enabled": False, "research_only": True}
    return {"schema_version": 1, "status": "completed", "run_date_utc": collected_at.date().isoformat(),
            "collected_at": collected_at.isoformat(), "source_kind": "observed", "research_only": True,
            "execution_enabled": False, "broker_orders_sent": 0, "bundle": bundle,
            "hashes": {"context_sha256": _sha(context), "observation_sha256": _sha(observation), "bundle_sha256": _sha(bundle)}}


def collect_paired(*, collector: Callable[[str, Callable[[], datetime]], tuple[dict[str, Any], dict[str, Any]]] = collect_symbol,
                   clock: Callable[[], datetime] | None = None) -> dict[str, Any]:
    """Collect exactly SPY and QQQ; no retries, orders, or symbol expansion."""
    clock = clock or (lambda: datetime.now(UTC))
    started = _now(clock())
    captures: dict[str, dict[str, Any]] = {}
    for symbol in ("SPY", "QQQ"):
        context, observation = collector(symbol, clock)
        collected = _now(clock())
        if collected.date() != started.date():
            raise TrendCaptureError("paired capture crossed a UTC date boundary")
        captures[symbol] = _record(symbol, context, observation, collected)
    return {"artifact_kind": "options-paired-daily-evidence-v1", "schema_version": 1, "status": "captured",
            "run_date_utc": started.date().isoformat(), "collected_at": _now(clock()).isoformat(), "source_kind": "observed",
            "research_only": True, "execution_enabled": False, "broker_orders_sent": 0, "orders_sent": 0,
            "symbols": ["SPY", "QQQ"], "request_limit": MAX_TOTAL_REQUESTS, "request_count": MAX_TOTAL_REQUESTS,
            "captures": captures}


def current_pair(state_dir: str | os.PathLike[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the current paired manifest and return its two validated records."""
    try:
        path = resolve_current(state_dir, "paired-daily.json")["paired-daily.json"]
    except DatedArtifactError as exc:
        raise TrendCaptureError("current paired daily manifest is missing") from exc
    if path.is_symlink() or not path.is_file():
        raise TrendCaptureError("current paired daily artifact is unsafe")
    try:
        paired = json.loads(path.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TrendCaptureError("current paired daily artifact is unreadable") from exc
    if not isinstance(paired, Mapping) or paired.get("artifact_kind") != "options-paired-daily-evidence-v1" or paired.get("status") != "captured":
        raise TrendCaptureError("current paired daily artifact is incomplete")
    if paired.get("run_date_utc") != path.parent.name:
        raise TrendCaptureError("current paired daily manifest date does not match its artifact")
    if paired.get("request_count") != MAX_TOTAL_REQUESTS or paired.get("request_limit") != MAX_TOTAL_REQUESTS or paired.get("broker_orders_sent") != 0:
        raise TrendCaptureError("paired daily request/order attestation is invalid")
    captures = paired.get("captures")
    if not isinstance(captures, Mapping) or set(captures) != {"SPY", "QQQ"}:
        raise TrendCaptureError("current paired daily artifact lacks SPY or QQQ")
    return dict(captures["SPY"]), dict(captures["QQQ"])


def _lock(root: Path):
    if root.is_symlink():
        raise TrendCaptureError("paired state root is unsafe")
    descriptor = os.open(root / "paired-capture.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    stream = os.fdopen(descriptor, "a+")
    try:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        stream.close()
        raise TrendCaptureError("paired capture is already running") from None
    return stream


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args(argv)
    root = Path(args.state_dir)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        with _lock(root):
            result = collect_paired()
            path = save_stage(root, result["run_date_utc"], "paired-daily.json", result)
        compact = {"status": "captured", "artifact_path": str(path), "orders_sent": 0}
    except (TrendCaptureError, DatedArtifactError) as exc:
        compact = {"status": "abstain", "reason_code": type(exc).__name__, "orders_sent": 0}
    print(json.dumps(compact, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
