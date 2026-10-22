"""Run the original BTC crossover paper feeds behind an isolated K/D gap gate.

This module owns two separate paper roots and immutable experiment identities.
It adds no market-data or order path: collection, risk, entry, mark, cancellation,
and settlement remain in the original timeframe-specific adapters.
"""
from __future__ import annotations

import argparse
import hashlib
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import stat
import threading

from . import btc_fast_stochastic_feed as fast_feed
from . import btc_fast_stochastic_paper as fast_paper
from . import btc_stochastic_feed as slow_feed
from . import btc_stochastic_paper as slow_paper

UTC = timezone.utc
GAP_MAX = 5.0
CONFIG_NAME = "experiment-config.json"
ALLOCATION_NAME = "allocation-policy.json"
ALLOCATION_BPS = 2000
ALLOCATION_POLICY_VERSION = "btc-cash20-v1"
_LOCK = threading.RLock()

PRODUCTION_ROOTS = {
    "5m": Path("/var/lib/neorome-btc-gap5-fast-paper"),
    "30m": Path("/var/lib/neorome-btc-gap5-slow-paper"),
}
LEGACY_ROOTS = {
    Path("/var/lib/neorome-btc-cross5-paper"),
    Path("/var/lib/neorome-btc-cross30-paper"),
    Path("/var/lib/neorome-btc-fast-stochastic-paper"),
    Path("/var/lib/neorome-btc-stochastic-paper"),
    Path("/var/lib/neorome-btc-brackets-v1"),
}
EXPERIMENTS = {
    "5m": {
        "schema": "btc-kd-gap-experiment-v1",
        "strategy_id": "STOCH-BTC5-533-KDGAP5-CROSS-v1",
        "profile_version": "btc-cross5-kd-gap5-paper-v1",
        "strategy_version": "btc-kd-gap5-5m-r1",
        "timeframe": "5m",
        "timeframe_minutes": 5,
        "k_d_gap_max": GAP_MAX,
        "gap_rule": "bullish:k-d;bearish:d-k;require 0<gap<=5",
        "starting_cash_cents": 50_000,
        "cash_fraction": 0.2,
        "exit_policy": "hold_to_official_settlement",
        "take_profit": None,
        "stop_loss": None,
    },
    "30m": {
        "schema": "btc-kd-gap-experiment-v1",
        "strategy_id": "STOCH-BTC30-KDGAP5-CROSS-v1",
        "profile_version": "btc-cross30-kd-gap5-paper-v1",
        "strategy_version": "btc-kd-gap5-30m-r1",
        "timeframe": "30m",
        "timeframe_minutes": 30,
        "k_d_gap_max": GAP_MAX,
        "gap_rule": "bullish:k-d;bearish:d-k;require 0<gap<=5",
        "starting_cash_cents": 50_000,
        "cash_fraction": 0.2,
        "exit_policy": "hold_to_official_settlement",
        "take_profit": None,
        "stop_loss": None,
    },
}


class GapFeedError(ValueError):
    """Invalid experiment root/configuration or an unsafe paper admission."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _adapter(timeframe):
    if timeframe == "5m":
        return fast_feed, fast_paper
    if timeframe == "30m":
        return slow_feed, slow_paper
    raise GapFeedError("timeframe_must_be_5m_or_30m")


def _lexical_root(path):
    """Normalize only common macOS system aliases without following user paths."""
    lexical = Path(os.path.abspath(path))
    for alias in (Path("/var"), Path("/tmp")):
        if alias.is_symlink() and (lexical == alias or alias in lexical.parents):
            lexical = alias.resolve(strict=True) / lexical.relative_to(alias)
    return lexical


def _reject_symlink_components(path):
    """Reject symlinks at the root or any existing ancestor, including dangling ones."""
    cursor = Path(path.anchor)
    for part in path.parts[1:]:
        cursor = cursor / part
        try:
            mode = cursor.lstat().st_mode
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(mode):
            raise GapFeedError("state_root_symlink_refused")


def _is_within(path, protected):
    return path == protected or protected in path.parents


def _root_for(timeframe, root=None):
    expected = _lexical_root(PRODUCTION_ROOTS[timeframe])
    candidate = Path(root) if root is not None else expected
    lexical = _lexical_root(candidate)
    _reject_symlink_components(lexical)
    legacy_roots = {_lexical_root(path) for path in LEGACY_ROOTS}
    if any(_is_within(lexical, protected) for protected in legacy_roots):
        raise GapFeedError("state_root_alias_or_protected_descendant_refused")
    production_roots = {_lexical_root(path) for path in PRODUCTION_ROOTS.values()}
    if lexical in production_roots:
        if lexical != expected:
            raise GapFeedError("production_state_root_must_match_timeframe")
        return lexical
    if any(_is_within(lexical, protected) for protected in production_roots):
        raise GapFeedError("state_root_other_experiment_or_descendant_refused")
    if root is None:
        if lexical != expected:
            raise GapFeedError("production_state_root_must_match_timeframe")
        return lexical
    return lexical


def _allocation_policy(root, base, *, create=False, prior_bps=None):
    path = root / ALLOCATION_NAME
    if path.is_symlink():
        raise GapFeedError("allocation_policy_symlink_refused")
    expected = {"schema": "btc-allocation-policy-v1", "allocation_bps": ALLOCATION_BPS,
                "policy_version": ALLOCATION_POLICY_VERSION,
                "base_config_sha256": hashlib.sha256(base.encode()).hexdigest(),
                "prior_allocation_bps": prior_bps}
    if path.exists():
        value = json.loads(path.read_text())
        if {k: value.get(k) for k in expected} != expected or set(value) != set(expected) | {"effective_at"}:
            raise GapFeedError("allocation_policy_conflict")
        try:
            stamp = datetime.fromisoformat(value["effective_at"].replace("Z", "+00:00"))
            if stamp.utcoffset() is None:
                raise ValueError
        except (ValueError, TypeError, AttributeError) as exc:
            raise GapFeedError("allocation_policy_timestamp_invalid") from exc
        return value
    if not create:
        raise GapFeedError("allocation_migration_required")
    value = expected | {"effective_at": datetime.now(UTC).isoformat()}
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o400)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(_canonical(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return value


def _configure_root(root, timeframe, *, migrate=False):
    root = Path(root)
    if root.is_symlink():
        raise GapFeedError("state_root_symlink_refused")
    root = root.resolve(strict=False)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink():
        raise GapFeedError("state_root_symlink_refused")
    config_path = root / CONFIG_NAME
    if config_path.is_symlink():
        raise GapFeedError("experiment_config_symlink_refused")
    expected = _canonical(EXPERIMENTS[timeframe]) + "\n"
    legacy = _canonical(EXPERIMENTS[timeframe] | {"cash_fraction": 0.5}) + "\n"
    if config_path.exists():
        try:
            current = config_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise GapFeedError("experiment_config_unreadable") from exc
        if current == legacy:
            return _allocation_policy(root, current, create=migrate, prior_bps=5000)
        if current != expected:
            raise GapFeedError("experiment_config_conflict")
        return _allocation_policy(root, current, create=True)
    if migrate:
        raise GapFeedError("allocation_migration_requires_existing_experiment")
    if any(root.iterdir()):
        raise GapFeedError("existing_state_requires_experiment_config")
    try:
        descriptor = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError:
        if config_path.is_symlink() or config_path.read_text(encoding="utf-8") != expected:
            raise GapFeedError("experiment_config_conflict")
        return _allocation_policy(root, expected, create=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(expected)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        config_path.unlink(missing_ok=True)
        raise
    config_path.chmod(0o400)
    return _allocation_policy(root, expected, create=True)


def migrate_allocation(timeframe, root=None):
    """Authorize the fixed 50%-to-20% risk epoch without touching any ledger."""
    _adapter(timeframe)
    state_root = _root_for(timeframe, root)
    return _configure_root(state_root, timeframe, migrate=True)


def gap_decision(signal):
    """Return the finite, direction-correct K/D gap decision for a signal."""
    if not isinstance(signal, dict):
        return {"decision": "invalid", "reason": "kd_gap_exceeded", "gap": None, "max_gap": GAP_MAX}
    k, d, direction = signal.get("k"), signal.get("d"), signal.get("direction")
    if (type(k) not in (int, float) or type(d) not in (int, float)
            or not math.isfinite(k) or not math.isfinite(d)
            or not 0 <= k <= 100 or not 0 <= d <= 100
            or direction not in ("bullish", "bearish")):
        return {"decision": "invalid", "reason": "kd_gap_exceeded", "gap": None, "max_gap": GAP_MAX}
    gap = k - d if direction == "bullish" else d - k
    accepted = 0 < gap <= GAP_MAX
    return {"decision": "accept" if accepted else "reject",
            "reason": None if accepted else "kd_gap_exceeded",
            "gap": gap, "max_gap": GAP_MAX}


def _gate_evaluator(original):
    def evaluate(*args, **kwargs):
        result = original(*args, **kwargs)
        selected = result.get("selected_signal")
        if selected is None:
            return result
        gate = gap_decision(selected)
        result = dict(result)
        result["kd_gap_gate"] = gate
        if gate["decision"] != "accept":
            result["selected_signal"] = None
            result["status"] = "abstain"
            result["reason"] = "kd_gap_exceeded"
            return result
        signal = dict(selected)
        signal["kd_gap_gate"] = gate
        result["selected_signal"] = signal
        return result
    return evaluate


def _require_gap(signal):
    gate = gap_decision(signal)
    if gate["decision"] != "accept":
        raise GapFeedError("kd_gap_exceeded")
    return gate


@contextmanager
def _isolated_adapter(timeframe, root):
    with _LOCK:
        feed, paper = _adapter(timeframe)
        if getattr(paper, "ALLOCATION_BPS", None) != ALLOCATION_BPS:
            raise GapFeedError("allocation_code_policy_mismatch")
        original_feed_root = feed.ROOT
        identity_names = ("STRATEGY_ID", "PROFILE_VERSION", "STRATEGY_VERSION")
        old_identity = {name: getattr(paper, name) for name in identity_names}
        old_evaluate = feed.tv_signal.evaluate
        old_run_once = paper.run_once
        old_signal_parser = paper._signal
        old_archive = feed.archive_cycle
        experiment = EXPERIMENTS[timeframe]

        def gated_run_once(input_map, *args, **kwargs):
            if isinstance(input_map, Mapping) and "signal" in input_map:
                signal = dict(input_map["signal"])
                if signal.get("strategy") not in (None, experiment["strategy_id"]):
                    raise GapFeedError("signal_strategy_identity_differs")
                signal["strategy"] = experiment["strategy_id"]
                input_map = dict(input_map)
                input_map["signal"] = signal
                now = kwargs.get("now")
                if now is None and len(args) > 1:
                    now = args[1]
                if now is None:
                    now = datetime.now(UTC)
                # Preserve every original strategy admission rule before applying the
                # additional gap limit, then let the adapter revalidate before reserve.
                parsed = old_signal_parser(signal, input_map.get("market"), paper._time(now, "now"))
                _require_gap(parsed)
            return old_run_once(input_map, *args, **kwargs)

        def marked_signal_parser(value, *args, **kwargs):
            parsed = old_signal_parser(value, *args, **kwargs)
            gate = _require_gap(parsed)
            return parsed | {"kd_gap_gate": gate}

        def archive_with_experiment(state_root, result):
            result.setdefault("experiment", {
                "strategy_id": experiment["strategy_id"],
                "profile_version": experiment["profile_version"],
                "strategy_version": experiment["strategy_version"],
                "timeframe": timeframe,
                "k_d_gap_max": GAP_MAX,
                "exit_policy": experiment["exit_policy"],
                "allocation_bps": ALLOCATION_BPS,
                "allocation_policy_version": ALLOCATION_POLICY_VERSION,
            })
            return old_archive(state_root, result)

        try:
            for name, value in (("STRATEGY_ID", experiment["strategy_id"]),
                                ("PROFILE_VERSION", experiment["profile_version"]),
                                ("STRATEGY_VERSION", experiment["strategy_version"])):
                setattr(paper, name, value)
            feed.ROOT = root
            feed.tv_signal.evaluate = _gate_evaluator(old_evaluate)
            paper.run_once = gated_run_once
            paper._signal = marked_signal_parser
            feed.archive_cycle = archive_with_experiment
            yield feed, paper
        finally:
            feed.ROOT = original_feed_root
            feed.tv_signal.evaluate = old_evaluate
            paper.run_once = old_run_once
            paper._signal = old_signal_parser
            feed.archive_cycle = old_archive
            for name, value in old_identity.items():
                setattr(paper, name, value)


def run_cycle(timeframe, root=None):
    """Run one normal adapter cycle under the selected immutable experiment."""
    _adapter(timeframe)
    state_root = _root_for(timeframe, root)
    _configure_root(state_root, timeframe)
    with _isolated_adapter(timeframe, state_root) as (feed, _paper):
        result = feed.cycle(state_root)
    result.setdefault("experiment", {
        "strategy_id": EXPERIMENTS[timeframe]["strategy_id"],
        "profile_version": EXPERIMENTS[timeframe]["profile_version"],
        "strategy_version": EXPERIMENTS[timeframe]["strategy_version"],
        "timeframe": timeframe,
        "k_d_gap_max": GAP_MAX,
        "exit_policy": EXPERIMENTS[timeframe]["exit_policy"],
        "allocation_bps": ALLOCATION_BPS,
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
    })
    return result


def initialize(timeframe, root=None):
    """Create/read only this isolated paper snapshot; performs no provider calls."""
    _adapter(timeframe)
    state_root = _root_for(timeframe, root)
    _configure_root(state_root, timeframe)
    with _isolated_adapter(timeframe, state_root) as (_feed, paper):
        snapshot = paper.snapshot(state_root)
    snapshot["experiment"] = dict(EXPERIMENTS[timeframe])
    snapshot["allocation_policy"] = _allocation_policy(state_root, (state_root / CONFIG_NAME).read_text(),
                                                        prior_bps=5000 if json.loads((state_root / CONFIG_NAME).read_text())["cash_fraction"] == 0.5 else None)
    return snapshot


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframe", required=True, choices=("5m", "30m"))
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--initialize", action="store_true")
    actions.add_argument("--migrate-allocation", action="store_true")
    args = parser.parse_args(argv)
    result = migrate_allocation(args.timeframe) if args.migrate_allocation else initialize(args.timeframe) if args.initialize else run_cycle(args.timeframe)
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
