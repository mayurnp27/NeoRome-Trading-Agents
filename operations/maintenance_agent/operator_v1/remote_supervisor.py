#!/usr/bin/env python3
"""Keep the installed Codex remote-control runtime connected without LLM calls."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time
from typing import Any, Callable

UTC = timezone.utc
POLL_SECONDS = 30
COMMAND_TIMEOUT_SECONDS = 25
MAX_COMMAND_OUTPUT = 64 * 1024
ERROR_CLASSES = {"executable_unavailable", "timeout", "output_limit", "cli_failed", "launch_error", "runner_error"}
DEFAULT_CODEX = "/opt/neorome-codex/codex"
DEFAULT_BASE = Path("/var/lib/neorome-maintenance-ai")


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _safe_identifier(value: Any) -> str | None:
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        return None
    return value if re.fullmatch(r"[A-Za-z0-9_.:-]+", value) else None


def _minimal_env(home: Path, workspace: Path) -> dict[str, str]:
    for directory in (home, workspace):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.is_symlink() or not directory.is_dir():
            raise OSError("invalid private directory")
        os.chmod(directory, 0o700)
    codex_home = home / ".codex"
    temp = workspace / "tmp"
    for directory in (codex_home, temp):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if directory.is_symlink() or not directory.is_dir():
            raise OSError("invalid private directory")
        os.chmod(directory, 0o700)
    return {"PATH": "/opt/neorome-codex:/usr/bin:/bin", "HOME": str(home), "CODEX_HOME": str(codex_home),
            "TMPDIR": str(temp), "LANG": "C.UTF-8", "TERM": "dumb"}


def _kill(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        pass


def _execute_cli(binary: str, env: dict[str, str], *, timeout: int = COMMAND_TIMEOUT_SECONDS) -> dict[str, Any]:
    if not os.path.isabs(binary) or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        return {"status": "failed", "error_class": "executable_unavailable"}
    process = None
    output = bytearray()
    try:
        process = subprocess.Popen([binary, "remote-control", "start", "--json"], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
                                   start_new_session=True)
        assert process.stdout is not None
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _kill(process)
                return {"status": "failed", "error_class": "timeout"}
            readable, _, _ = select.select([process.stdout], [], [], min(1, remaining))
            if readable:
                chunk = os.read(process.stdout.fileno(), min(8192, MAX_COMMAND_OUTPUT - len(output) + 1))
                if len(output) + len(chunk) > MAX_COMMAND_OUTPUT:
                    _kill(process)
                    return {"status": "failed", "error_class": "output_limit"}
                output.extend(chunk)
            elif process.poll() is not None:
                break
            if process.poll() is not None:
                # The CLI starts its own backend; do not wait for backend-held pipes.
                break
        try:
            returncode = process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            _kill(process)
            return {"status": "failed", "error_class": "timeout"}
        if returncode != 0:
            return {"status": "failed", "error_class": "cli_failed"}
        return {"status": "running", "output": bytes(output)}
    except subprocess.TimeoutExpired:
        if process is not None:
            _kill(process)
        return {"status": "failed", "error_class": "timeout"}
    except FileNotFoundError:
        return {"status": "failed", "error_class": "executable_unavailable"}
    except OSError:
        if process is not None and process.poll() is None:
            _kill(process)
        return {"status": "failed", "error_class": "launch_error"}
    finally:
        if process is not None and process.stdout is not None:
            process.stdout.close()


def _metadata(output: bytes) -> tuple[str | None, str | None, str | None]:
    server_name = environment_id = remote_status = None
    for line in output[:MAX_COMMAND_OUTPUT].decode("utf-8", errors="ignore").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        if value.get("status") in {"connected", "connecting", "disconnected", "failed"}:
            remote_status = value["status"]
        server_name = server_name or _safe_identifier(value.get("serverName", value.get("server_name")))
        environment_value = value.get("environmentId", value.get("environment_id",
            value.get("environmentID", value.get("environmentalID", value.get("environmentalId")))))
        environment_id = environment_id or _safe_identifier(environment_value)
    return server_name, environment_id, remote_status


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.is_symlink():
        raise OSError("unsafe status path")
    temporary = path.with_name(path.name + ".tmp")
    encoded = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(encoded) > 16 * 1024:
        raise OSError("status size limit")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        parent_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def run_once(*, codex_binary: str = DEFAULT_CODEX, home: Path = DEFAULT_BASE / "home",
             workspace: Path = DEFAULT_BASE / "workspace", state: Path = DEFAULT_BASE / "state",
             now: datetime | None = None,
             execute: Callable[[str, dict[str, str]], dict[str, Any]] | None = None) -> dict[str, Any]:
    current = now or datetime.now(UTC)
    env = _minimal_env(home, workspace)
    command = execute or (lambda binary, environ: _execute_cli(binary, environ))
    try:
        outcome = command(codex_binary, env)
    except Exception as exc:
        outcome = {"status": "failed", "error_class": "runner_error"}
    status = outcome.get("status")
    if status == "running":
        server, environment, remote_status = _metadata(outcome.get("output", b""))
        report = {"schema_version": 1, "as_of": _iso(current), "status": remote_status or "unverified"}
        if server:
            report["server_name"] = server
        if environment:
            report["environment_id"] = environment
    else:
        candidate = outcome.get("error_class")
        err = candidate if isinstance(candidate, str) and candidate in ERROR_CLASSES else None
        report = {"schema_version": 1, "as_of": _iso(current), "status": "failed",
                  "error_class": err or "unknown_error"}
    _atomic_json(state / "remote-control.json", report)
    return report


def _signature(report: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(report.get(key) for key in ("status", "server_name", "environment_id", "error_class"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-binary", default=DEFAULT_CODEX)
    parser.add_argument("--home", type=Path, default=DEFAULT_BASE / "home")
    parser.add_argument("--workspace", type=Path, default=DEFAULT_BASE / "workspace")
    parser.add_argument("--state", type=Path, default=DEFAULT_BASE / "state")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    previous = None
    last_emit = 0.0
    while True:
        started = time.monotonic()
        try:
            report = run_once(codex_binary=args.codex_binary, home=args.home,
                              workspace=args.workspace, state=args.state)
        except OSError as exc:
            report = {"schema_version": 1, "as_of": _iso(datetime.now(UTC)),
                      "status": "failed", "error_class": "status_write_error"}
        signature = _signature(report)
        current = time.monotonic()
        if args.once or signature != previous or current - last_emit >= 60:
            print(json.dumps(report, sort_keys=True, separators=(",", ":")), flush=True)
            previous, last_emit = signature, current
        if args.once:
            return 0
        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
