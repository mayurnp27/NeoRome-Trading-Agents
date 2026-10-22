#!/usr/bin/env python3
"""Bounded, local NeoRome infrastructure health checks and gated recovery."""
from __future__ import annotations

import argparse
from datetime import datetime, time, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import secrets
import stat
import subprocess
import sys
import time as monotonic_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

UTC = timezone.utc
LEGACY_SERVICE_RECIPES = frozenset({"neorome-health.service", "neorome-control-api.service"})
ONESHOT_RETRY_RECIPES = {
    "neorome-memecoin-e2-report.service": "neorome-memecoin-e2-report.timer",
    "neorome-memecoin-e4-report.service": "neorome-memecoin-e4-report.timer",
    "neorome-memecoin-report-dispatcher-96.service": "neorome-memecoin-report-dispatcher-96.timer",
    "neorome-memecoin-report-dispatcher-256.service": "neorome-memecoin-report-dispatcher-256.timer",
    "neorome-ops.service": "neorome-ops.timer",
    "neorome-control-sync.service": "neorome-control-sync.timer",
    "neorome-l7t4-report.service": "neorome-l7t4-report.timer",
}
TIMER_REPAIR_RECIPES = {
    "neorome-control-sync.timer": "neorome-control-sync.service",
    "neorome-equity-intraday-iex-fullfunded.timer": "neorome-equity-intraday-iex-fullfunded.service",
    "neorome-equity-intraday-watchlist.timer": "neorome-equity-intraday-watchlist.service",
    "neorome-stochastic-paper-v2.timer": "neorome-stochastic-paper-v2.service",
    "neorome-btc-stochastic-paper.timer": "neorome-btc-stochastic-paper.service",
    "neorome-crypto-recorder.timer": "neorome-crypto-recorder.service",
    "neorome-paper-runtime.timer": "neorome-paper-runtime.service",
}
UNARMED_TIMER_RESTART = "neorome-control-sync.timer"
OUTCOME_REPAIR = "neorome-options-valuation-recovery.service"
OUTCOME_CAPTURE = "neorome-options-full-port-exit-capture.service"
COLLECTOR_REPAIR = "neorome-bcp3-streaming-collector-diagnostics-r3.service"
COLLECTOR_SOURCE = "/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-upstream-failure-diagnostics-r1/collector_runner_upstream_failure_r1.py"
COLLECTOR_JOURNAL = Path('/var/lib/neorome-bcp3-paper/creation-journal-segmented-r1')
COLLECTOR_PROBE_STATUS = Path('/var/lib/neorome-bcp3-upstream-probe/status.json')
COLLECTOR_PROBE_UNIT = 'neorome-bcp3-upstream-probe.service'
PARENT_REPAIR = 'neorome-bcp3-streaming-parent-resilience-r2.service'
PARENT_SOURCE = '/opt/neorome/operations/maintenance_agent/bcp3_scheduler.py'
PARENT_CHAIN_SOURCE = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/chain_scheduler.py'
PARENT_STARTUP_SOURCE = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-startup-order-r1/wait_for_collector.py'
PARENT_CHAIN_STARTUP_SOURCE = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/startup_gate.py'
PARENT_CHAIN_ACTIVATION = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/ACTIVATION.json'
PARENT_PREFIX_SOURCE = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-rpc-prefix-r1/scheduler.py'
PARENT_PREFIX_STARTUP = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-rpc-prefix-r1/startup.py'
PARENT_PREFIX_MANIFEST = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-rpc-prefix-r1/MANIFEST.json'
PARENT_PROFIT_SOURCE = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-profit-protection-r1/scheduler.py'
PARENT_PROFIT_STARTUP = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-profit-protection-r1/startup.py'
PARENT_PROFIT_MANIFEST = '/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-profit-protection-r1/MANIFEST.json'
PARENT_PROFIT_PREFIX_STARTUP_SHA256 = '394e00100b8bf13b9dfd40ddc211f3a53e6a887a97f33674816cf276969a892c'
ALLOWLIST = frozenset(LEGACY_SERVICE_RECIPES | set(ONESHOT_RETRY_RECIPES) | set(TIMER_REPAIR_RECIPES) | {OUTCOME_REPAIR, COLLECTOR_REPAIR, PARENT_REPAIR})
REVIEWED_SOURCE_ROOTS = ('/opt/neorome/', '/opt/neorome-equity-iex-fullfunded-r2/package/',
                         '/opt/neorome-stochastic-paper/v2/', '/opt/neorome-btc-stochastic-r10/package/',
                         '/opt/neorome-l7t4-reporting-20261007-v2/')
PROPERTIES = (
    "Id", "LoadState", "ActiveState", "SubState", "Result", "Type", "MainPID",
    "Job", "Unit", "UnitFileState", "LastTriggerUSec", "NextElapseUSecRealtime",
    "NextElapseUSecMonotonic", "FragmentPath", "DropInPaths",
    "ExecMainCode", "ExecMainStatus", "ExecMainExitTimestamp",
)
MAX_POLICY_BYTES = 256 * 1024
MAX_STATUS_FILE_BYTES = 256 * 1024
MAX_COMMAND_OUTPUT = 2 * 1024 * 1024
MAX_EVENTS_BYTES = 20 * 1024 * 1024
MAX_POLICY_UNITS = 256
MAX_POLICY_SOURCES = 128
POLL_SECONDS = 5
COOLDOWN_SECONDS = 600
MAX_ATTEMPTS_PER_DAY = 3


class MaintenanceError(Exception):
    pass


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _read_json(path: Path, max_bytes: int) -> Any:
    if path.is_symlink() or not path.is_file():
        raise MaintenanceError("required file unavailable")
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise MaintenanceError("file exceeds size limit")
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MaintenanceError("invalid JSON") from exc


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    data = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        dfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _publish_ai_projection(report: dict[str, Any]) -> bool:
    """Publish only this monitor's non-secret projection into a root-owned inbox."""
    base = Path('/var/lib/neorome-maintenance-ai')
    inbox = base / 'inbox'
    if not inbox.exists():
        return False
    for path in (base, inbox):
        metadata = path.lstat()
        if path.is_symlink() or not path.is_dir() or metadata.st_uid != 0 or metadata.st_mode & 0o022:
            raise MaintenanceError('AI inbox ownership invalid')
    value = dict(report)
    value['incidents'] = report['issues']
    value['agent_package_sha256'] = _hash_file(__file__)
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from operations.maintenance_agent.context_v1 import publisher
        value['repair_context_sha256'] = publisher.publish(value, inbox)
        value['repair_context_status'] = 'published'
    except (OSError, ValueError, KeyError, TypeError, ImportError):
        # Context cannot change deterministic repairs or hide raw incidents.
        value['repair_context_status'] = 'unavailable'
    _atomic_json(inbox / 'status.json', value)
    (inbox / 'status.json').chmod(0o644)
    return True


def _load_policy(path: Path) -> dict[str, Any]:
    value = _read_json(path, MAX_POLICY_BYTES)
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise MaintenanceError("unsupported policy")
    units = value.get("expected_units")
    sources = value.get("source_statuses", [])
    guards = value.get("repair_guards", {})
    if not isinstance(units, list) or not units or len(units) > MAX_POLICY_UNITS:
        raise MaintenanceError("invalid expected_units")
    if not all(isinstance(unit, str) and unit.endswith((".service", ".timer")) and
               unit.startswith("neorome-") and len(unit) <= 160 for unit in units):
        raise MaintenanceError("invalid expected unit name")
    if len(set(units)) != len(units):
        raise MaintenanceError("duplicate expected unit")
    if not isinstance(sources, list) or len(sources) > MAX_POLICY_SOURCES:
        raise MaintenanceError("invalid source_statuses")
    if not isinstance(guards, dict):
        raise MaintenanceError("invalid repair_guards")
    # Policy may narrow or omit repair targets, but can never add one.
    if set(guards) - ALLOWLIST:
        raise MaintenanceError("repair policy names a non-allowlisted service")
    for name, guard in guards.items():
        if not isinstance(guard, dict) or not isinstance(guard.get("unit_sha256"), str) or not isinstance(guard.get("source_path"), str) or not isinstance(guard.get("source_sha256"), str):
            raise MaintenanceError("invalid repair guard")
        if not _is_sha256(guard["unit_sha256"]) or not _is_sha256(guard["source_sha256"]):
            raise MaintenanceError("invalid repair hash")
        _validate_hash_map(name, guard.get("dropin_sha256"), "repair drop-in hashes")
        if name == COLLECTOR_REPAIR and guard.get('source_path') != COLLECTOR_SOURCE:
            raise MaintenanceError('invalid collector recovery source')
        if name == PARENT_REPAIR and (not _parent_source_pair_valid(guard)
                or not _is_sha256(str(guard.get('startup_source_sha256', '')))
                ):
            raise MaintenanceError('invalid parent recovery source')
        if name == PARENT_REPAIR and guard.get('startup_source_path') in {PARENT_CHAIN_STARTUP_SOURCE, PARENT_PREFIX_STARTUP, PARENT_PROFIT_STARTUP}:
            if (not _is_sha256(str(guard.get('chain_policy_sha256', '')))
                    or not _is_sha256(str(guard.get('chain_activation_sha256', '')))):
                raise MaintenanceError('invalid chain-source recovery acceptance')
            if guard['startup_source_path'] in {PARENT_PREFIX_STARTUP, PARENT_PROFIT_STARTUP} and not _is_sha256(str(guard.get('prefix_manifest_sha256', ''))):
                raise MaintenanceError('invalid prefix-source recovery acceptance')
            if guard['startup_source_path'] == PARENT_PROFIT_STARTUP and not _is_sha256(str(guard.get('profit_manifest_sha256', ''))):
                raise MaintenanceError('invalid profit-source recovery acceptance')
        if name == OUTCOME_REPAIR:
            if guard.get("source_path") != "/opt/neorome/neorome/options_internal_paper.py":
                raise MaintenanceError("invalid outcome recovery source")
            if not _is_sha256(str(guard.get("capture_unit_sha256", ""))) or not _is_sha256(str(guard.get("capture_source_sha256", ""))):
                raise MaintenanceError("invalid capture recovery hash")
            _validate_hash_map(OUTCOME_CAPTURE, guard.get("capture_dropin_sha256"), "capture drop-in hashes")
        if name in ONESHOT_RETRY_RECIPES or name in TIMER_REPAIR_RECIPES:
            if (not Path(guard["source_path"]).is_absolute() or
                    '..' in Path(guard['source_path']).parts or
                    not str(guard["source_path"]).startswith(REVIEWED_SOURCE_ROOTS)):
                raise MaintenanceError("source outside reviewed deployment roots")
        if name in TIMER_REPAIR_RECIPES:
            paired = TIMER_REPAIR_RECIPES[name]
            if (guard.get("paired_service") != paired or
                    not isinstance(guard.get("paired_unit_sha256"), str) or
                    not _is_sha256(guard["paired_unit_sha256"])):
                raise MaintenanceError("invalid timer recovery pair")
            _validate_hash_map(paired, guard.get("paired_dropin_sha256"), "paired service drop-in hashes")
    for item in sources:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("path"), str):
            raise MaintenanceError("invalid status source")
        if not isinstance(item.get("max_age_seconds"), int) or not 1 <= item["max_age_seconds"] <= 7 * 86400:
            raise MaintenanceError("invalid source age")
        if item.get("market_window", "always") not in ("always", "equity_rth", "options_intraday"):
            raise MaintenanceError("invalid market window")
        if "max_bytes" in item and (not isinstance(item["max_bytes"], int) or not 1 <= item["max_bytes"] <= MAX_STATUS_FILE_BYTES):
            raise MaintenanceError("invalid source size")
        if "timestamp_field" in item and (not isinstance(item["timestamp_field"], str) or len(item["timestamp_field"]) > 80):
            raise MaintenanceError("invalid timestamp field")
        timestamp_source = item.get("timestamp_source", "json_field")
        if not isinstance(timestamp_source, str) or timestamp_source not in {"json_field", "file_mtime"}:
            raise MaintenanceError("invalid timestamp source")
        if "health_field" in item and (not isinstance(item["health_field"], str) or not item["health_field"] or len(item["health_field"]) > 80):
            raise MaintenanceError("invalid health field")
        if item.get("outcome_kind") not in (None, "options_lifecycle", "stochastic_options"):
            raise MaintenanceError("invalid outcome kind")
    source_names = [item["name"] for item in sources]
    if len(set(source_names)) != len(source_names):
        raise MaintenanceError("duplicate status source")
    return value


def _validate_hash_map(unit_name: str, dropins: Any, error: str) -> None:
    if not isinstance(dropins, dict) or len(dropins) > 32:
        raise MaintenanceError(error)
    for path, digest in dropins.items():
        if (not isinstance(path, str) or not _valid_systemd_dropin_path(unit_name, path) or
                not isinstance(digest, str) or not _is_sha256(digest)):
            raise MaintenanceError(error)


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value.lower())


def _parse_show(output: str) -> dict[str, dict[str, str]]:
    units: dict[str, dict[str, str]] = {}
    current: dict[str, str] = {}
    for line in output.splitlines() + [""]:
        if not line:
            if current.get("Id"):
                units[current["Id"]] = current
            current = {}
        elif "=" in line:
            key, value = line.split("=", 1)
            current[key] = value
    return units


def _systemd_units(run: Callable[..., Any], expected: list[str]) -> tuple[dict[str, dict[str, str]], list[str]]:
    listed = run(["systemctl", "list-units", "--all", "--no-legend", "--plain", "--type=service,timer"],
                 capture_output=True, text=True, timeout=5, check=False)
    if listed.returncode != 0 or len(listed.stdout) > MAX_COMMAND_OUTPUT:
        raise MaintenanceError("systemd unit enumeration failed")
    names = set(expected)
    for line in listed.stdout.splitlines():
        fields = line.split()
        if fields and fields[0].startswith("neorome-") and fields[0].endswith((".service", ".timer")):
            names.add(fields[0])
    if len(names) > MAX_POLICY_UNITS + 512:
        raise MaintenanceError("too many systemd units")
    ordered = sorted(names)
    if not ordered:
        return {}, []
    shown = run(["systemctl", "show", "--no-pager", "--property=" + ",".join(PROPERTIES), *ordered],
                capture_output=True, text=True, timeout=8, check=False)
    if shown.returncode != 0 or len(shown.stdout) > MAX_COMMAND_OUTPUT:
        raise MaintenanceError("systemd property query failed")
    return _parse_show(shown.stdout), ordered


def _source_issues(policy: dict[str, Any], now: datetime) -> list[dict[str, str]]:
    issues = []
    for spec in policy.get("source_statuses", []):
        name = spec["name"]
        window = spec.get("market_window", "always")
        in_window = _in_equity_window(now) if window == "equity_rth" else True
        if window == "options_intraday":
            local = now.astimezone(ZoneInfo("America/New_York"))
            in_window = local.weekday() < 5 and time(9, 35) <= local.time().replace(tzinfo=None) < time(11, 5)
        try:
            source_path = Path(spec["path"])
            data = _read_json(source_path, min(MAX_STATUS_FILE_BYTES, int(spec.get("max_bytes", MAX_STATUS_FILE_BYTES))))
            if not isinstance(data, dict):
                raise MaintenanceError("invalid status object")
            issues.extend(_outcome_issues(name, spec.get("outcome_kind"), data))
            if spec.get("outcome_kind") == "options_lifecycle" and (
                    not isinstance(data.get("entry_record"), dict)
                    or (data.get("last_result") or {}).get("status") == "closed_internal_paper"):
                in_window = False  # A flat book needs no recurring position mark.
            health_field = spec.get("health_field")
            if health_field and type(data.get(health_field)) is not bool:
                issues.append({"key": "source:" + name + ":health", "reason": "status_health_field_invalid"})
            elif health_field and data[health_field] is False:
                issues.append({"key": "source:" + name + ":health", "reason": "status_unhealthy"})
            if spec.get("timestamp_source", "json_field") == "file_mtime":
                # This is completion-file freshness only. The JSON must first
                # pass the bounded parse above; mtime is never quote evidence.
                metadata = source_path.stat(follow_symlinks=False)
                if not stat.S_ISREG(metadata.st_mode):
                    raise MaintenanceError("status path is not a regular file")
                stamp = datetime.fromtimestamp(metadata.st_mtime, UTC)
            else:
                stamp = _parse_time(data.get(spec.get("timestamp_field", "updated_at")))
            if stamp is None and in_window:
                issues.append({"key": "source:" + name, "reason": "timestamp_missing"})
            elif in_window and ((now - stamp).total_seconds() < -5 or (now - stamp).total_seconds() > spec["max_age_seconds"]):
                reason = "status_file_stale" if spec.get("timestamp_source") == "file_mtime" else "status_stale"
                issues.append({"key": "source:" + name, "reason": reason})
        except (OSError, ValueError, MaintenanceError):
            if in_window:
                issues.append({"key": "source:" + name, "reason": "status_unavailable"})
    return issues


def _outcome_issues(name: str, kind: str | None, data: dict[str, Any]) -> list[dict[str, str]]:
    """Check completed business outcomes even when a oneshot exited successfully.

    These incidents persist outside market hours; only recovery is session-gated.
    No arbitrary provider error strings or position details enter the AI inbox.
    """
    reason = None
    if kind == "options_lifecycle" and isinstance(data.get("entry_record"), dict):
        result = data.get("last_result") or {}
        if result.get("status") != "closed_internal_paper":
            if result.get("mark_status") in {"blocked_evidence", "shared_mark_coverage_required"}:
                reason = "position_valuation_blocked"
            elif result.get("mark_status") != "persisted":
                reason = "position_valuation_missing"
            if result.get("provider_reconciliation"):
                reason = "position_reconciliation_failed"
    elif kind == "stochastic_options":
        # Completion file failures used to be invisible when its mtime was fresh.
        if data.get("status") in {"error", "failed", "blocked", "blocked_evidence"} or data.get("error"):
            reason = "strategy_cycle_failed"
        for arm in (data.get("arms") or {}).values():
            if not isinstance(arm, dict):
                reason = "strategy_cycle_failed"
                continue
            if arm.get("status") in {"error", "failed", "blocked", "blocked_evidence"}:
                reason = "strategy_cycle_failed"
            for position in arm.get("positions") or []:
                if isinstance(position, dict) and position.get("mark_status") in {"stale", "missing", "unavailable", "blocked"}:
                    reason = "position_valuation_blocked"
    return [{"key": "outcome:" + name, "reason": reason}] if reason else []


def _in_equity_window(now: datetime) -> bool:
    local = now.astimezone(ZoneInfo("America/New_York"))
    return local.weekday() < 5 and time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0)


def _unit_issues(units: dict[str, dict[str, str]], expected: list[str]) -> list[dict[str, str]]:
    issues = []
    expected_set = set(expected)
    for name in expected:
        item = units.get(name)
        if item is None or item.get("LoadState") != "loaded":
            issues.append({"key": "unit:" + name, "reason": "unit_unavailable"})
            continue
        state = item.get("ActiveState", "")
        if state == "failed" or item.get("Result", "success") not in ("success", "" ):
            issues.append({"key": "unit:" + name, "reason": "unit_failed"})
        elif name.endswith(".timer"):
            if state != "active":
                issues.append({"key": "unit:" + name, "reason": "timer_inactive"})
            else:
                timer_unarmed = not _has_time(item.get("NextElapseUSecRealtime")) and not _has_time(item.get("NextElapseUSecMonotonic"))
                configured_service = item.get("Unit", "").strip()
                service_name = (configured_service if _valid_neorome_service_name(configured_service)
                                else name[:-6] + ".service")
                service = units.get(service_name, {})
                # An ordered start job can wait on another service while this
                # unit is still inactive. Cleanup can also outlive MainPID.
                service_running = (service.get("ActiveState") in {"active", "activating", "deactivating"}
                                   or not _idle_no_job(service))
                if timer_unarmed and not service_running:
                    issues.append({"key": "unit:" + name, "reason": "timer_unarmed"})
        elif state not in ("active", "activating") and name.endswith(".service") and item.get("Type") in {"simple", "exec"}:
            issues.append({"key": "unit:" + name, "reason": "service_inactive"})
    # Newly discovered failed units are useful signals even when policy has not
    # classified them as required. Disabled/inactive non-required timers remain quiet.
    for name, item in units.items():
        if name in expected_set or item.get("LoadState") != "loaded":
            continue
        if item.get("ActiveState") == "failed" or item.get("Result", "success") not in ("success", ""):
            issues.append({"key": "unit:" + name, "reason": "unit_failed"})
    return issues


def _has_time(value: str | None) -> bool:
    return bool(value and value.strip().lower() not in {"n/a", "na", "0", "-", "infinity", "inf"})


def _valid_neorome_service_name(value: str) -> bool:
    return (value.isascii() and value.startswith("neorome-") and value.endswith(".service") and len(value) <= 160 and
            all(char.isalnum() or char in "_.@:-" for char in value))


TIMER_UNARMED_GRACE_SECONDS = 20


def _defer_timer_unarmed(problems: list[dict[str, str]], state: dict[str, Any],
                         now: datetime) -> list[dict[str, str]]:
    """Debounce only timer_unarmed; keep its first-seen time across polls."""
    pending = state.get("timer_unarmed_since")
    if not isinstance(pending, dict):
        pending = {}
    active_keys = {issue["key"] for issue in problems if issue.get("reason") == "timer_unarmed"}
    next_pending: dict[str, str] = {}
    visible: list[dict[str, str]] = []
    for issue in problems:
        if issue.get("reason") != "timer_unarmed":
            visible.append(issue)
            continue
        key = issue["key"]
        started = _parse_time(pending.get(key))
        if started is None or started > now:
            started = now
        next_pending[key] = _utc(started)
        if (now - started).total_seconds() >= TIMER_UNARMED_GRACE_SECONDS:
            visible.append(issue)
    # Healthy observations clear their pending timer episode. Ignore stale or
    # malformed saved entries so they cannot recreate an immediate incident.
    state["timer_unarmed_since"] = {key: value for key, value in next_pending.items() if key in active_keys}
    return visible


def _attach_incident_episodes(problems: list[dict[str, str]], state: dict[str, Any]) -> None:
    """Attach durable identities for continuously active issue keys."""
    previous = state.get("incident_episodes")
    if not isinstance(previous, dict):
        previous = {}
    active: dict[str, dict[str, str]] = {}
    for issue in problems:
        key, reason = issue.get("key"), issue.get("reason")
        if not isinstance(key, str) or not isinstance(reason, str):
            continue
        prior = previous.get(key)
        episode_id = prior.get("episode_id") if isinstance(prior, dict) else None
        if not isinstance(episode_id, str) or not _is_sha256(episode_id):
            episode_id = secrets.token_hex(32)
        else:
            episode_id = episode_id.lower()
        issue["episode_id"] = episode_id
        active[key] = {"reason": reason, "episode_id": episode_id}
    # Inactive entries are removed here; repair and AI claim histories stay intact.
    state["incident_episodes"] = active


def _metrics(state_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        result["load_1m"] = round(os.getloadavg()[0], 2)
    except (AttributeError, OSError):
        result["load_1m"] = None
    try:
        mem = {}
        with open("/proc/meminfo", "r", encoding="ascii") as stream:
            for line in stream:
                key, value = line.split(":", 1)
                mem[key] = int(value.strip().split()[0]) * 1024
        total, available = mem.get("MemTotal"), mem.get("MemAvailable")
        result["memory_total_bytes"] = total
        result["memory_available_bytes"] = available
        result["memory_used_percent"] = round(100 * (total - available) / total, 1) if total and available is not None else None
    except (OSError, ValueError, IndexError):
        result["memory_used_percent"] = None
    try:
        usage = shutil.disk_usage(state_dir)
        result["disk_total_bytes"] = usage.total
        result["disk_free_bytes"] = usage.free
        result["disk_used_percent"] = round(100 * usage.used / usage.total, 1) if usage.total else None
    except OSError:
        result["disk_used_percent"] = None
    return result


def _hash_file(path: str) -> str | None:
    try:
        p = Path(path)
        if p.is_symlink() or not p.is_file() or p.stat().st_size > 16 * 1024 * 1024:
            return None
        digest = hashlib.sha256()
        with p.open("rb") as stream:
            for block in iter(lambda: stream.read(65536), b""):
                digest.update(block)
        return digest.hexdigest()
    except OSError:
        return None


SYSTEMD_DROPIN_ROOTS = (
    Path("/etc/systemd/system"), Path("/run/systemd/system"),
    Path("/usr/lib/systemd/system"), Path("/lib/systemd/system"),
)


def _valid_systemd_dropin_path(unit_name: str, raw_path: str) -> bool:
    path = Path(raw_path)
    if (not path.is_absolute() or any(part in {".", ".."} for part in path.parts) or
            path.parent.name != unit_name + ".d" or path.suffix != ".conf" or
            any(char.isspace() or ord(char) < 32 for char in raw_path)):
        return False
    return any(path.parent.parent == root for root in SYSTEMD_DROPIN_ROOTS)


def _dropin_hashes_match(unit_name: str, item: dict[str, str], guard: dict[str, Any]) -> bool:
    """Require every systemd drop-in to be pinned, with no policy extras."""
    configured = guard.get("dropin_sha256")
    if not isinstance(configured, dict):
        return False
    raw_paths = item.get("DropInPaths", "").strip()
    paths = raw_paths.split() if raw_paths else []
    if len(paths) != len(set(paths)) or set(paths) != set(configured):
        return False
    for path in paths:
        if not _valid_systemd_dropin_path(unit_name, path):
            return False
        if _hash_file(path) != configured[path]:
            return False
    return True


def _collector_probe_evidence(head: dict, guard: dict, units: dict) -> bool:
    """Accept a fresh validated handshake from the separately pinned probe."""
    item = units.get(COLLECTOR_PROBE_UNIT, {})
    source = guard.get('probe_source_path', '')
    if (item.get('LoadState') != 'loaded' or not isinstance(source, str)
            or not source.startswith('/opt/neorome/')
            or '..' in Path(source).parts
            or not _is_sha256(str(guard.get('probe_source_sha256', '')))
            or not _is_sha256(str(guard.get('probe_unit_sha256', '')))
            or _hash_file(source) != guard.get('probe_source_sha256')
            or _hash_file(item.get('FragmentPath', '')) != guard.get('probe_unit_sha256')
            or not _dropin_hashes_match(COLLECTOR_PROBE_UNIT, item,
                                       {'dropin_sha256': guard.get('probe_dropin_sha256')})):
        return False
    probe = _read_json(COLLECTOR_PROBE_STATUS, MAX_STATUS_FILE_BYTES)
    if not isinstance(probe, dict):
        return False
    at = _parse_time(probe.get('asOf'))
    observed_head = probe.get('journalHead')
    return (probe.get('schema') == 'neorome-bcp3-upstream-probe-v1'
            and at is not None and 0 <= (datetime.now(UTC) - at).total_seconds() <= 60
            and probe.get('ready') is True and probe.get('observedHttpStatus') == 101
            and probe.get('failureClass') == 'upstream_upgrade_rejected'
            and probe.get('pendingTailGap') is None and isinstance(observed_head, dict)
            and observed_head.get('sequence') == head.get('sequence')
            and observed_head.get('sha256') == head.get('sha256'))


def _collector_retry_evidence(guard: dict | None = None, units: dict | None = None) -> bool:
    """Retry recorded lock timeout or recovered upstream at unchanged head.

    Runtime still validates the complete chain and records the interruption.
    Capacity, corruption, unknown failures and deliberate stops are not retries.
    """
    try:
        health = _read_json(COLLECTOR_JOURNAL / 'streaming-collector-snapshot-r1-health.json', MAX_STATUS_FILE_BYTES)
        manifest = _read_json(COLLECTOR_JOURNAL / 'manifest.json', MAX_STATUS_FILE_BYTES)
        if not isinstance(health, dict) or not isinstance(manifest, dict):
            return False
        at = _parse_time(health.get('asOf'))
        if at is None or not 0 <= (datetime.now(UTC) - at).total_seconds() <= 86400:
            return False
        head = health.get('journalHead')
        diagnostics = health.get('appendDiagnostics')
        if not isinstance(head, dict) or not isinstance(diagnostics, dict):
            return False
        failures = diagnostics.get('failures')
        common = (health.get('status') == 'source_failure'
                and health.get('reason') == 'bounded_run_or_connection_limit'
                and head.get('capacity_stop') is False and head.get('unresolved_tail_gap') is None
                and diagnostics.get('failureListTruncated') is False
                and isinstance(failures, list) and 0 <= len(failures) <= 16
                and diagnostics.get('failureCount') == len(failures)
                and manifest.get('schema') == 'neorome-p3-segmented-journal-r1'
                and manifest.get('pending_tail_gap') is None
                and type(head.get('sequence')) is int and head['sequence'] > 0
                and head['sequence'] == manifest.get('head_sequence')
                and _is_sha256(str(head.get('sha256', '')))
                and head['sha256'] == manifest.get('head_sha256'))
        if not common:
            return False
        if failures:
            return all(isinstance(row, dict) and row.get('code') == 'writer_busy' for row in failures)
        return (health.get('sessionCount') == 4
                and _collector_probe_evidence(head, guard or {}, units or {}))
    except (MaintenanceError, OSError, ValueError, TypeError):
        return False


def _parent_source_pair_valid(guard: dict) -> bool:
    return (guard.get('source_path'), guard.get('startup_source_path', PARENT_STARTUP_SOURCE)) in {
        (PARENT_SOURCE, PARENT_STARTUP_SOURCE), (PARENT_CHAIN_SOURCE, PARENT_CHAIN_STARTUP_SOURCE),
        (PARENT_PREFIX_SOURCE, PARENT_PREFIX_STARTUP), (PARENT_PROFIT_SOURCE, PARENT_PROFIT_STARTUP)}


def _prefix_acceptance(guard: dict) -> bool:
    expected = guard.get('prefix_manifest_sha256')
    if not _is_sha256(str(expected)) or _protected_prefix_hash(PARENT_PREFIX_MANIFEST) != expected:
        return False
    manifest = _read_json(Path(PARENT_PREFIX_MANIFEST), MAX_STATUS_FILE_BYTES)
    approved = {str(Path(PARENT_PREFIX_SOURCE).parent / name) for name in
                ('prefix_guard.py', 'parent_runtime.py', 'route.py', 'scheduler.py')} | {
                '/opt/neorome-bcp3-child-freshness-r2/entry.cjs',
                '/opt/neorome-bcp3-child-freshness-r2/src/stdio_bridge.cjs'}
    pins = manifest.get('deployedPins') if isinstance(manifest, dict) else None
    return (isinstance(manifest, dict) and manifest.get('schema') == 'neorome-bcp3-rpc-prefix-release-v1'
            and manifest.get('profile') == 'two_identical_complete_account_states_terminal_veto_r1'
            and all(manifest.get(field) is False for field in
                    ('policyChanged', 'quotaChanged', 'sdkArtifactsChanged', 'successPathChecksChanged'))
            and isinstance(pins, dict) and set(pins) == approved
            and all(_is_sha256(str(digest)) and _protected_prefix_hash(path) == digest for path, digest in pins.items())
            and _protected_prefix_hash(PARENT_PREFIX_SOURCE) == guard.get('source_sha256')
            and _protected_prefix_hash(PARENT_PREFIX_STARTUP) == guard.get('startup_source_sha256'))


def _profit_acceptance(guard: dict) -> bool:
    """Authorize only the reviewed overlay while retaining original prefix gates."""
    expected = guard.get('profit_manifest_sha256')
    if (guard.get('source_path') != PARENT_PROFIT_SOURCE
            or guard.get('startup_source_path') != PARENT_PROFIT_STARTUP
            or not _is_sha256(str(expected))
            or _protected_prefix_hash(PARENT_PROFIT_MANIFEST) != expected):
        return False
    manifest = _read_json(Path(PARENT_PROFIT_MANIFEST), MAX_STATUS_FILE_BYTES)
    files = manifest.get('files') if isinstance(manifest, dict) else None
    if (not isinstance(manifest, dict)
            or manifest.get('schema') != 'neorome-bcp3-profit-protection-release-v1'
            or manifest.get('profile') != 'bcp3-profit-protection-r1'
            or not isinstance(files, dict) or set(files) != {'runtime.py', 'engine.py', 'scheduler.py'}
            or not all(_is_sha256(str(digest))
                       and _protected_prefix_hash(str(Path(PARENT_PROFIT_SOURCE).parent / name)) == digest
                       for name, digest in files.items())
            or files['scheduler.py'] != guard.get('source_sha256')
            or _protected_prefix_hash(PARENT_PROFIT_STARTUP) != guard.get('startup_source_sha256')
            or manifest.get('prefixStartupSha256') != PARENT_PROFIT_PREFIX_STARTUP_SHA256):
        return False
    # This is the original immutable release, not the overlay's source pair.
    # The manifest hash is rooted in the repair guard; no arbitrary path is read.
    prefix = _read_json(Path(PARENT_PREFIX_MANIFEST), MAX_STATUS_FILE_BYTES)
    pins = prefix.get('deployedPins') if isinstance(prefix, dict) else None
    if not isinstance(pins, dict):
        return False
    return _prefix_acceptance({**guard,
        'source_sha256': pins.get(PARENT_PREFIX_SOURCE),
        'startup_source_sha256': manifest['prefixStartupSha256']})


def _protected_prefix_hash(path: str) -> str | None:
    try:
        target = Path(path)
        for current in (target, *target.parents):
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                return None
            if current == target and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o222):
                return None
        return _hash_file(path)
    except OSError:
        return None


def _parent_retry_evidence(units: dict, guard: dict | None = None) -> bool:
    """Resume exits independently, or a flat parent only after fresh persistence."""
    try:
        ledger = _read_json(COLLECTOR_JOURNAL.parent / 'ledger.json', MAX_STATUS_FILE_BYTES)
        if not isinstance(ledger, dict) or not {'position', 'activeCandidate'} <= set(ledger):
            return False
        if guard and guard.get('startup_source_path') == PARENT_PREFIX_STARTUP and not _prefix_acceptance(guard):
            return False
        if guard and guard.get('startup_source_path') == PARENT_PROFIT_STARTUP and not _profit_acceptance(guard):
            return False
        if ledger['position'] is not None or ledger['activeCandidate'] is not None:
            return True  # The pinned startup gate gives existing lifecycle work priority.
        if guard and guard.get('startup_source_path') in {PARENT_CHAIN_STARTUP_SOURCE, PARENT_PREFIX_STARTUP, PARENT_PROFIT_STARTUP}:
            # Maintenance deliberately cannot read /etc/neorome credentials or
            # policy. A pinned public root-owned commit receipt authorizes only
            # a bounded start; the service's source gate independently checks
            # real /etc policy/proofs and immutable source pins before running.
            expected = guard.get('chain_activation_sha256')
            if not _is_sha256(str(expected)) or _hash_file(PARENT_CHAIN_ACTIVATION) != expected:
                return False
            activation = _read_json(Path(PARENT_CHAIN_ACTIVATION), MAX_STATUS_FILE_BYTES)
            return (isinstance(activation, dict)
                    and activation.get('schema') == 'neorome-bcp3-chain-source-activation-v1'
                    and activation.get('state') == 'committed'
                    and activation.get('newPolicySha256') == guard.get('chain_policy_sha256')
                    and ledger.get('policySha256') == guard.get('chain_policy_sha256'))
        if units.get(COLLECTOR_REPAIR, {}).get('ActiveState') != 'active':
            return False
        manifest = _read_json(COLLECTOR_JOURNAL / 'manifest.json', MAX_STATUS_FILE_BYTES)
        if not isinstance(manifest, dict):
            return False
        at = _parse_time(manifest.get('head_at'))
        return (manifest.get('schema') == 'neorome-p3-segmented-journal-r1'
                and manifest.get('pending_tail_gap') is None
                and type(manifest.get('head_sequence')) is int and manifest['head_sequence'] > 0
                and _is_sha256(str(manifest.get('head_sha256', '')))
                and at is not None and 0 <= (datetime.now(UTC) - at).total_seconds() <= 60)
    except (MaintenanceError, OSError, ValueError, TypeError):
        return False


def _repair_target(name: str, item: dict[str, str], guard: dict[str, Any], units: dict[str, dict[str, str]]) -> bool:
    if name not in ALLOWLIST or item.get("LoadState") != "loaded":
        return False
    if name == PARENT_REPAIR:
        return (item.get('Type') == 'exec' and item.get('ActiveState') in {'failed', 'inactive'}
                and _idle_no_job(item) and _parent_source_pair_valid(guard)
                and _hash_file(item.get('FragmentPath', '')) == guard.get('unit_sha256')
                and _hash_file(guard['source_path']) == guard.get('source_sha256')
                and guard.get('startup_source_path', PARENT_STARTUP_SOURCE) in {
                    PARENT_STARTUP_SOURCE, PARENT_CHAIN_STARTUP_SOURCE, PARENT_PREFIX_STARTUP, PARENT_PROFIT_STARTUP}
                and _hash_file(guard.get('startup_source_path', PARENT_STARTUP_SOURCE)) == guard.get('startup_source_sha256')
                and _dropin_hashes_match(name, item, guard) and _parent_retry_evidence(units, guard))
    if name == COLLECTOR_REPAIR:
        return (item.get('Type') == 'exec' and item.get('ActiveState') == 'failed'
                and _idle_no_job(item) and guard.get('source_path') == COLLECTOR_SOURCE
                and _hash_file(item.get('FragmentPath', '')) == guard.get('unit_sha256')
                and _hash_file(COLLECTOR_SOURCE) == guard.get('source_sha256')
                and _dropin_hashes_match(name, item, guard) and _collector_retry_evidence(guard, units))
    if name == OUTCOME_REPAIR:
        capture = units.get(OUTCOME_CAPTURE, {})
        original = units.get("neorome-options-full-port-internal-paper.service", {})
        return (item.get("Type") == "oneshot" and item.get("ActiveState") in {"inactive", "failed"}
                and _idle_no_job(item) and _idle_no_job(capture) and _idle_no_job(original)
                and capture.get("LoadState") == "loaded" and capture.get("ActiveState") in {"inactive", "failed"}
                and original.get("LoadState") == "loaded" and original.get("ActiveState") in {"inactive", "failed"}
                and _hash_file(item.get("FragmentPath", "")) == guard.get("unit_sha256")
                and _hash_file(guard.get("source_path", "")) == guard.get("source_sha256")
                and _dropin_hashes_match(name, item, guard)
                and _hash_file(capture.get("FragmentPath", "")) == guard.get("capture_unit_sha256")
                and _hash_file("/opt/neorome/neorome/options_lifecycle_capture.py") == guard.get("capture_source_sha256")
                and _dropin_hashes_match(OUTCOME_CAPTURE, capture, {"dropin_sha256": guard.get("capture_dropin_sha256")}))
    if name in ONESHOT_RETRY_RECIPES:
        return _oneshot_retry_target(name, item, guard)
    if name in TIMER_REPAIR_RECIPES:
        return _timer_repair_target(name, item, guard, units)
    if item.get("Type") not in {"simple", "exec"} or item.get("ActiveState") not in {"failed", "inactive"}:
        return False
    if not _idle_no_job(item):
        return False
    # A timer-triggered or manually queued worker must never be started twice.
    if units.get(name.removesuffix(".service") + ".timer", {}).get("ActiveState") == "active":
        return False
    if not all(isinstance(guard.get(key), str) for key in ("unit_sha256", "source_path", "source_sha256")):
        return False
    unit_hash = _hash_file(item.get("FragmentPath", ""))
    source_hash = _hash_file(guard["source_path"])
    return (unit_hash == guard["unit_sha256"] and source_hash == guard["source_sha256"] and
            _dropin_hashes_match(name, item, guard))


def _idle_no_job(item: dict[str, str]) -> bool:
    job = item.get("Job", "").strip().lower()
    return (item.get("MainPID", "0").strip() in {"", "0"} and
            job in {"", "0", "-", "n/a", "na"})


def _oneshot_retry_target(name: str, item: dict[str, str], guard: dict[str, Any]) -> bool:
    # This exception is deliberately limited to idempotent report enqueue,
    # report dispatch, and read-only operations/control-plane sync oneshots.
    if item.get("Type") != "oneshot" or item.get("ActiveState") != "failed" or not _idle_no_job(item):
        return False
    required = ("unit_sha256", "source_path", "source_sha256")
    if not all(isinstance(guard.get(key), str) for key in required):
        return False
    return (_hash_file(item.get("FragmentPath", "")) == guard["unit_sha256"] and
            _hash_file(guard["source_path"]) == guard["source_sha256"] and
            _dropin_hashes_match(name, item, guard))


def _timer_repair_target(name: str, item: dict[str, str], guard: dict[str, Any],
                         units: dict[str, dict[str, str]]) -> bool:
    # Only the read-only control-sync timer may be restarted, and only when it
    # is demonstrably active-but-unarmed. Other timers are start-only.
    active_unarmed_restart = (
        name == UNARMED_TIMER_RESTART and item.get("ActiveState") == "active" and
        not _has_time(item.get("NextElapseUSecRealtime")) and
        not _has_time(item.get("NextElapseUSecMonotonic"))
    )
    if (item.get("ActiveState") not in {"inactive", "failed"} and not active_unarmed_restart) or not _idle_no_job(item):
        return False
    paired_name = TIMER_REPAIR_RECIPES[name]
    paired = units.get(paired_name, {})
    if (guard.get("paired_service") != paired_name or paired.get("LoadState") != "loaded" or
            paired.get("ActiveState") not in {"inactive", "failed"} or not _idle_no_job(paired)):
        return False
    if not all(isinstance(guard.get(key), str) for key in
               ("unit_sha256", "source_path", "source_sha256", "paired_unit_sha256")):
        return False
    return (
        _hash_file(item.get("FragmentPath", "")) == guard["unit_sha256"] and
        _dropin_hashes_match(name, item, guard) and
        _hash_file(paired.get("FragmentPath", "")) == guard["paired_unit_sha256"] and
        _hash_file(guard["source_path"]) == guard["source_sha256"] and
        _dropin_hashes_match(paired_name, paired, {"dropin_sha256": guard.get("paired_dropin_sha256")})
    )


def _empty_state() -> dict[str, Any]:
    return {"schema_version": 1, "attempts": {}, "incidents": {}, "last_run": None}


def _append_events(path: Path, events: list[dict[str, Any]]) -> bool:
    if not events:
        return (path.stat().st_size if path.exists() else 0) <= MAX_EVENTS_BYTES
    current = path.stat().st_size if path.exists() else 0
    payload = b"".join((json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode() for event in events)
    if current + len(payload) > MAX_EVENTS_BYTES:
        return False
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "ab") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        pass
    return True


def _transitions(old: dict[str, Any], current: dict[str, Any], at: str) -> list[dict[str, Any]]:
    events = []
    before = old.get("incidents", {})
    for key, reason in sorted(current.items()):
        if key not in before:
            events.append({"at": at, "event": "opened", "key": key, "reason": reason})
        elif before[key] != reason:
            events.append({"at": at, "event": "changed", "key": key, "reason": reason})
    for key in sorted(set(before) - set(current)):
        events.append({"at": at, "event": "resolved", "key": key})
    return events


def _recover_incidents(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return fallback
    if path.is_symlink() or path.stat().st_size > MAX_EVENTS_BYTES:
        raise MaintenanceError("event history unavailable")
    active: dict[str, str] = {}
    with path.open("rb") as stream:
        for raw in stream:
            try:
                event = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MaintenanceError("event history invalid") from exc
            key = event.get("key")
            if not isinstance(key, str):
                continue
            if event.get("event") in {"opened", "changed"} and isinstance(event.get("reason"), str):
                active[key] = event["reason"]
            elif event.get("event") == "resolved":
                active.pop(key, None)
    recovered = dict(fallback)
    recovered["incidents"] = active
    return recovered


def _attempt_available(state: dict[str, Any], name: str, now: datetime) -> bool:
    row = state.get("attempts", {}).get(name, {})
    day = now.astimezone(UTC).date().isoformat()
    if row.get("day") != day and row.get("day") is not None:
        row = {}
    if row.get("count", 0) >= MAX_ATTEMPTS_PER_DAY:
        return False
    last = _parse_time(row.get("last_at"))
    return last is None or (now - last).total_seconds() >= COOLDOWN_SECONDS


def _recheck_target(run: Callable[..., Any], name: str, guard: dict[str, Any], initial: dict[str, str], units: dict[str, dict[str, str]]) -> bool:
    if name in TIMER_REPAIR_RECIPES:
        paired = TIMER_REPAIR_RECIPES[name]
    elif name in ONESHOT_RETRY_RECIPES:
        paired = ONESHOT_RETRY_RECIPES[name]
    else:
        paired = name.removesuffix(".service") + ".timer"
    required = [name, paired]
    if name == OUTCOME_REPAIR:
        required += [OUTCOME_CAPTURE, "neorome-options-full-port-internal-paper.service"]
    refreshed, _ = _systemd_units(run, required)
    candidate = refreshed.get(name, {})
    if name == UNARMED_TIMER_RESTART and candidate.get("ActiveState") != initial.get("ActiveState"):
        return False
    return _repair_target(name, candidate, guard, refreshed) and candidate.get("FragmentPath") == initial.get("FragmentPath")


def run_once(
    *, policy_path: str | os.PathLike[str], state_dir: str | os.PathLike[str], apply: bool = False,
    now: datetime | None = None, run: Callable[..., Any] = subprocess.run, publish_ai: bool = False,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    state_root = Path(state_dir)
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if state_root.is_symlink() or not state_root.is_dir():
        raise MaintenanceError("invalid state directory")
    lock_path = state_root / "maintenance.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"schema_version": 1, "status": "already_running", "as_of": _utc(current)}
        policy = _load_policy(Path(policy_path))
        old = _read_json(state_root / "state.json", MAX_STATUS_FILE_BYTES) if (state_root / "state.json").exists() else _empty_state()
        if not isinstance(old, dict) or not isinstance(old.get("attempts", {}), dict) or not isinstance(old.get("incidents", {}), dict):
            raise MaintenanceError("invalid saved state")
        event_path = state_root / "repair-events.jsonl"
        old = _recover_incidents(event_path, old)
        units, queried_names = _systemd_units(run, policy["expected_units"])
        problems = _unit_issues(units, policy["expected_units"])
        problems = _defer_timer_unarmed(problems, old, current)
        problems += _source_issues(policy, current)
        metrics = _metrics(state_root)
        if metrics.get("disk_used_percent") is not None and metrics["disk_used_percent"] >= 90:
            problems.append({"key": "resource:disk", "reason": "disk_space_low"})
        if metrics.get("memory_available_bytes") is not None and metrics["memory_available_bytes"] < 256 * 1024 * 1024:
            problems.append({"key": "resource:memory", "reason": "memory_pressure"})
        _attach_incident_episodes(problems, old)
        active = {row["key"]: row["reason"] for row in problems}
        at = _utc(current)
        events = _transitions(old, active, at)
        history_ok = _append_events(event_path, events)
        actions = []
        if apply and history_ok:
            for issue in problems:
                prefix = "unit:"
                outcome = (issue["key"] in {"outcome:options_full_port", "source:options_full_port"}
                           and issue["reason"] in {"position_valuation_blocked", "position_valuation_missing", "status_stale"})
                if outcome and not _in_equity_window(current):
                    continue
                if not outcome and not issue["key"].startswith(prefix):
                    continue
                name = OUTCOME_REPAIR if outcome else issue["key"][len(prefix):]
                if name == OUTCOME_REPAIR and not outcome:
                    continue
                if name not in policy["expected_units"]:
                    continue
                guard = policy.get("repair_guards", {}).get(name)
                item = units.get(name, {})
                if not guard or not _repair_target(name, item, guard, units) or not _attempt_available(old, name, current):
                    continue
                # Persist the attempt and cooldown before issuing the action so a restart cannot duplicate it.
                prior = old.setdefault("attempts", {}).get(name, {})
                day = current.date().isoformat()
                count = (prior.get("count", 0) if prior.get("day") == day else 0) + 1
                old.setdefault("attempts", {})[name] = {"day": day, "count": count, "last_at": at, "status": "recorded_before_start"}
                old["incidents"] = active
                old["last_run"] = at
                _atomic_json(state_root / "state.json", old)
                if not _recheck_target(run, name, guard, item, units):
                    actions.append({"unit": name, "status": "skipped_recheck_changed"})
                    continue
                repair_event = {"at": at, "event": "repair_attempt", "unit": name, "attempt": count}
                if name == PARENT_REPAIR:
                    repair_event['start_limit_reset_before_start'] = True
                if not _append_events(event_path, [repair_event]):
                    actions.append({"unit": name, "status": "skipped_event_capacity"})
                    history_ok = False
                    continue
                try:
                    if name == PARENT_REPAIR:
                        reset = run(['systemctl', 'reset-failed', name], capture_output=True, text=True, timeout=5, check=False)
                        if reset.returncode != 0:
                            raise OSError('parent_start_limit_reset_failed')
                    restart = name == UNARMED_TIMER_RESTART and item.get("ActiveState") == "active"
                    verb = "restart" if restart else "start"
                    result = run(["systemctl", "--no-block", verb, name], capture_output=True, text=True, timeout=5, check=False)
                    outcome = ("restart_queued" if restart else "start_queued") if result.returncode == 0 else ("restart_failed" if restart else "start_failed")
                except (OSError, subprocess.SubprocessError):
                    outcome = "restart_unknown" if name == UNARMED_TIMER_RESTART and item.get("ActiveState") == "active" else "start_unknown"
                old["attempts"][name]["status"] = outcome
                _atomic_json(state_root / "state.json", old)
                if not _append_events(event_path, [{"at": at, "event": "repair_result", "unit": name, "status": outcome}]):
                    history_ok = False
                actions.append({"unit": name, "status": outcome})
        old["incidents"] = active
        old["last_run"] = at
        state_capacity = not history_ok
        report = {
            "schema_version": 1, "as_of": at, "status": "degraded" if problems or state_capacity else "healthy",
            "apply_enabled": bool(apply), "expected_unit_count": len(policy["expected_units"]),
            "queried_unit_count": len(queried_names), "issues": problems,
            "actions": actions, "event_history_capacity_exhausted": state_capacity,
            "metrics": metrics,
            "units": {name: {key: item.get(key, "") for key in
                       ("ActiveState", "SubState", "Type", "Result", "MainPID", "Job", "Unit",
                        "LastTriggerUSec", "NextElapseUSecRealtime", "NextElapseUSecMonotonic",
                        "ExecMainCode", "ExecMainStatus", "ExecMainExitTimestamp")}
                      for name, item in sorted(units.items())},
            "trade_readiness_claimed": False,
        }
        _atomic_json(state_root / "incidents.json", {"schema_version": 1, "as_of": at, "active": active})
        _atomic_json(state_root / "status.json", report)
        _atomic_json(state_root / "state.json", old)
        if publish_ai:
            _publish_ai_projection(report)
        return report
    finally:
        os.close(lock_fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default="/opt/neorome/operations/maintenance_agent/v1/policy.json")
    parser.add_argument("--state-dir", default="/var/lib/neorome-maintenance")
    parser.add_argument("--apply", action="store_true", help="enable reviewed, hash-guarded recovery recipes")
    parser.add_argument("--once", action="store_true", help="run one pass and exit")
    parser.add_argument("--publish-ai", action="store_true", help="publish the non-secret production projection to the AI inbox")
    args = parser.parse_args(argv)
    last_signature = None
    last_log_time = 0.0
    while True:
        try:
            result = run_once(policy_path=args.policy, state_dir=args.state_dir, apply=args.apply, publish_ai=args.publish_ai)
            code = 0 if result.get("status") in {"healthy", "degraded", "already_running"} else 2
        except (OSError, MaintenanceError, ValueError, subprocess.SubprocessError) as exc:
            result = {"schema_version": 1, "status": "error", "error": type(exc).__name__}
            code = 2
        signature = json.dumps({key: result.get(key) for key in ('status', 'issues', 'actions', 'error')}, sort_keys=True)
        now_monotonic = monotonic_time.monotonic()
        if args.once or signature != last_signature:
            log_result = {key: value for key, value in result.items() if key != "units"}
            print(json.dumps(log_result, sort_keys=True, separators=(",", ":")), flush=True)
            last_signature, last_log_time = signature, now_monotonic
        elif now_monotonic - last_log_time >= 60:
            print(json.dumps({'as_of': result.get('as_of'), 'status': result.get('status'), 'issue_count': len(result.get('issues', []))}), flush=True)
            last_log_time = now_monotonic
        if args.once:
            return code
        monotonic_time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
