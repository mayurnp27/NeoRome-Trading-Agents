"""Generate one immutable prospective fixed-watchlist universe for EQ-INTRADAY.

It is an offline scheduling artifact, not a historical universe reconstruction
or a liquidity/volatility screen.  The generated membership is content-addressed
and may only be selected before its New York session opens.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from desks.equities.intraday_evidence_archive import canonical_bytes, sha256


UTC = timezone.utc
NY = ZoneInfo("America/New_York")
STATIC_SOURCE = Path(__file__).resolve().parents[1] / "desks" / "equities" / "INTRADAY_PROSPECTIVE_WATCHLIST.json"


class WatchlistError(ValueError):
    pass


def _read_regular(path: Path, label: str) -> bytes:
    """Read one regular non-symlink file without following a final component."""

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise WatchlistError(f"{label} unavailable") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise WatchlistError(f"{label} must be a regular file")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            return handle.read()
    finally:
        os.close(fd)


def _safe_root(value: str | Path) -> Path:
    root = Path(value)
    try:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        mode = os.lstat(root).st_mode
    except OSError as exc:
        raise WatchlistError("watchlist state directory unavailable") from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise WatchlistError("watchlist state directory must not be a symlink")
    return root


def _write_immutable(path: Path, raw: bytes, label: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError:
        if _read_regular(path, label) != raw:
            raise WatchlistError(f"{label} already exists with different bytes")
        return
    except OSError as exc:
        raise WatchlistError(f"{label} unavailable") from exc
    try:
        with os.fdopen(fd, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(fd)
    finally:
        os.close(fd)


def _payload(*, session: date, selected: datetime, source: Mapping[str, Any]) -> dict[str, Any]:
    local = selected.astimezone(NY)
    if local.date() > session or (local.date() == session and local.time() >= time(9, 30)):
        raise WatchlistError("fixed watchlist must be selected before the New York regular session")
    members = [dict(row, watchlist_member=True) for row in source["symbols"]
               if isinstance(row, Mapping) and isinstance(row.get("symbol"), str) and isinstance(row.get("permanent_id"), str)]
    if not members or len({row["symbol"] for row in members}) != len(members):
        raise WatchlistError("reviewed static watchlist membership is invalid")
    selected_iso = selected.isoformat().replace("+00:00", "Z")
    return {
        "schema_version": 1, "method": "prospective_fixed_watchlist",
        "source": source["source"], "source_version": hashlib.sha256(STATIC_SOURCE.read_bytes()).hexdigest(),
        "source_document": source.get("source_document"), "selection_at": selected_iso, "known_at": selected_iso,
        "session_date": session.isoformat(), "frozen": True, "historical_claim": False,
        "research_label": "prospective_fixed_watchlist_evidence_only", "members": members,
        "membership_sha256": sha256(members),
    }


def _validate_existing_payload(raw: bytes, *, session: date, source: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise WatchlistError("immutable watchlist artifact is unreadable") from exc
    if not isinstance(payload, dict) or canonical_bytes(payload) != raw:
        raise WatchlistError("immutable watchlist artifact is not canonical")
    if (payload.get("method") != "prospective_fixed_watchlist" or payload.get("session_date") != session.isoformat()
            or payload.get("frozen") is not True or payload.get("historical_claim") is not False
            or payload.get("source") != source.get("source")
            or payload.get("source_version") != hashlib.sha256(STATIC_SOURCE.read_bytes()).hexdigest()):
        raise WatchlistError("immutable watchlist artifact does not match the requested pilot")
    members = payload.get("members")
    if not isinstance(members, list) or payload.get("membership_sha256") != sha256(members):
        raise WatchlistError("immutable watchlist membership is not content-addressed")
    expected = _payload(session=session, selected=_time(payload.get("selection_at")), source=source)
    if canonical_bytes(payload) != canonical_bytes(expected):
        raise WatchlistError("immutable watchlist artifact does not match the reviewed source")
    return payload


def _time(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try: value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc: raise WatchlistError("selection_at must be ISO timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise WatchlistError("selection_at must be timezone-aware")
    return value.astimezone(UTC)


def _session(value: str | date) -> date:
    if isinstance(value, date) and not isinstance(value, datetime): return value
    try: return date.fromisoformat(str(value))
    except ValueError as exc: raise WatchlistError("session_date must be ISO date") from exc


def _source() -> Mapping[str, Any]:
    if STATIC_SOURCE.is_symlink() or not STATIC_SOURCE.is_file():
        raise WatchlistError("reviewed static watchlist source unavailable")
    try: value = json.loads(STATIC_SOURCE.read_text())
    except (OSError, json.JSONDecodeError) as exc: raise WatchlistError("reviewed static watchlist source unreadable") from exc
    if not isinstance(value, Mapping) or value.get("historical_claim") is not False or not isinstance(value.get("symbols"), list):
        raise WatchlistError("reviewed static watchlist source is invalid")
    return value


def generate(output: str | Path, *, session_date: str | date, selection_at: datetime | str) -> dict[str, Any]:
    session, selected, source = _session(session_date), _time(selection_at), _source()
    payload = _payload(session=session, selected=selected, source=source)
    target = Path(output)
    raw = canonical_bytes(payload)
    _safe_root(target.parent)
    _write_immutable(target, raw, "watchlist output")
    return payload


def _publish_current(root: Path, *, session: date, artifact_name: str, artifact_raw: bytes) -> Path:
    """Atomically publish a regular current manifest while holding a no-follow lock."""

    current, lock_path = root / "frozen-universe-current.json", root / ".frozen-universe-current.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise WatchlistError("current watchlist lock unavailable") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise WatchlistError("current watchlist lock must be a regular file")
        fcntl.flock(fd, fcntl.LOCK_EX)
        manifest = {
            "schema_version": 1, "kind": "prospective_fixed_watchlist_current",
            "session_date": session.isoformat(), "artifact": artifact_name,
            "artifact_sha256": hashlib.sha256(artifact_raw).hexdigest(),
        }
        raw = canonical_bytes(manifest)
        try:
            existing = _read_regular(current, "current watchlist manifest")
        except WatchlistError:
            if current.is_symlink():
                raise
            existing = None
        if existing == raw:
            return current
        if current.is_symlink():
            raise WatchlistError("current watchlist manifest must not be a symlink")
        temporary = root / f".frozen-universe-current.{os.getpid()}.{os.urandom(8).hex()}.tmp"
        _write_immutable(temporary, raw, "temporary current watchlist manifest")
        try:
            # ``replace`` changes the directory entry; it never follows a final symlink.
            if current.is_symlink():
                raise WatchlistError("current watchlist manifest must not be a symlink")
            os.replace(temporary, current)
            directory_fd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try: os.fsync(directory_fd)
            finally: os.close(directory_fd)
        except Exception:
            if temporary.exists() and not temporary.is_symlink():
                temporary.unlink()
            raise
        return current
    finally:
        try: fcntl.flock(fd, fcntl.LOCK_UN)
        finally: os.close(fd)


def generate_daily(state_dir: str | Path, *, session_date: str | date, selection_at: datetime | str) -> dict[str, Any]:
    """Create one immutable date-addressed artifact and publish its current manifest.

    A restart keeps the already frozen artifact for that session; it cannot
    silently replace membership with a later selection timestamp.
    """

    session, selected, source = _session(session_date), _time(selection_at), _source()
    root = _safe_root(state_dir)
    artifact_name = f"frozen-universe-{session.isoformat()}.json"
    artifact_path = root / artifact_name
    if artifact_path.exists() or artifact_path.is_symlink():
        payload = _validate_existing_payload(_read_regular(artifact_path, "immutable watchlist artifact"), session=session, source=source)
    else:
        payload = _payload(session=session, selected=selected, source=source)
        _write_immutable(artifact_path, canonical_bytes(payload), "immutable watchlist artifact")
    manifest = _publish_current(root, session=session, artifact_name=artifact_name, artifact_raw=_read_regular(artifact_path, "immutable watchlist artifact"))
    return {**payload, "artifact_path": str(artifact_path), "current_manifest_path": str(manifest)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output")
    destination.add_argument("--state-dir")
    parser.add_argument("--session-date"); parser.add_argument("--selection-at")
    args = parser.parse_args(argv)
    selected = args.selection_at or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    session = args.session_date or _time(selected).astimezone(NY).date().isoformat()
    try:
        result = (generate_daily(args.state_dir, session_date=session, selection_at=selected)
                  if args.state_dir else generate(args.output, session_date=session, selection_at=selected))
    except WatchlistError as exc: parser.exit(1, str(exc) + "\n")
    print(json.dumps({"output": result.get("artifact_path", args.output), "current_manifest": result.get("current_manifest_path"),
                      "method": result["method"], "membership_sha256": result["membership_sha256"], "broker_orders_sent": 0}, sort_keys=True))
    return 0


if __name__ == "__main__": raise SystemExit(main())
