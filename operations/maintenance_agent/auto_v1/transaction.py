"""Bounded, journaled source-file transaction primitives for maintenance repairs.

This module never executes candidate code or invokes a shell.  The caller must
provide a trusted profile mapping approved relative Python paths to production
paths.  Candidate bytes and exact originals are frozen under a private stage
directory before installation.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import secrets
import stat
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


DEFAULT_FILE_LIMIT = 256 * 1024
DEFAULT_TOTAL_LIMIT = 2 * 1024 * 1024
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class TransactionError(RuntimeError):
    """The candidate cannot be safely frozen, installed, or recovered."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _safe_relpath(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise TransactionError("invalid relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", ".", "..") for part in value.split("/")):
        raise TransactionError("path must be a normalized relative path")
    if path.suffix != ".py":
        raise TransactionError("only existing Python source files may be deployed")
    return value


def _absolute(path: os.PathLike[str] | str) -> str:
    value = os.fspath(path)
    if not os.path.isabs(value):
        raise TransactionError("paths must be absolute")
    return os.path.normpath(value)


def _open_dir_chain(path: str) -> int:
    """Open an absolute directory without following any symlink component."""
    path = _absolute(path)
    fd = os.open(os.path.sep, _DIR_FLAGS)
    try:
        for part in Path(path).parts[1:]:
            nxt = os.open(part, _DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = nxt
        return fd
    except OSError as exc:
        os.close(fd)
        raise TransactionError(f"unsafe or inaccessible directory path: {path}: {exc}") from exc
    except Exception:
        os.close(fd)
        raise


def _open_parent(path: str) -> tuple[int, str]:
    path = _absolute(path)
    parent, name = os.path.split(path)
    if not name or name in (".", ".."):
        raise TransactionError("invalid file path")
    return _open_dir_chain(parent), name


def _read_regular(path: str, *, max_bytes: int) -> tuple[bytes, os.stat_result]:
    parent_fd, name = _open_parent(path)
    try:
        fd = os.open(name, _FILE_FLAGS, dir_fd=parent_fd)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise TransactionError(f"not a regular file: {path}")
            if st.st_nlink != 1:
                raise TransactionError(f"hard-linked file refused: {path}")
            if st.st_size > max_bytes:
                raise TransactionError(f"file exceeds size limit: {path}")
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining:
                chunk = os.read(fd, min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            data = b"".join(chunks)
            if len(data) > max_bytes:
                raise TransactionError(f"file exceeds size limit: {path}")
            after = os.fstat(fd)
            if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
            ):
                raise TransactionError(f"file changed while reading: {path}")
            return data, st
        finally:
            os.close(fd)
    except OSError as exc:
        raise TransactionError(f"cannot safely read {path}: {exc}") from exc
    finally:
        os.close(parent_fd)


def _write_new(dir_fd: int, name: str, data: bytes, mode: int = 0o600) -> None:
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), mode, dir_fd=dir_fd)
    try:
        view = memoryview(data)
        while view:
            count = os.write(fd, view)
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _stage_layout(stage: str, txid: str, *, create: bool = False) -> tuple[int, int]:
    stage_fd = _open_dir_chain(stage)
    try:
        try:
            tx_fd = os.open(txid, _DIR_FLAGS, dir_fd=stage_fd)
        except FileNotFoundError:
            if not create:
                raise TransactionError("transaction staging directory is missing")
            os.mkdir(txid, 0o700, dir_fd=stage_fd)
            os.fsync(stage_fd)
            tx_fd = os.open(txid, _DIR_FLAGS, dir_fd=stage_fd)
        st = os.fstat(tx_fd)
        if not stat.S_ISDIR(st.st_mode):
            os.close(tx_fd)
            raise TransactionError("invalid transaction staging directory")
        return stage_fd, tx_fd
    except Exception:
        os.close(stage_fd)
        raise


def _store_journal(tx_fd: int, journal: dict[str, Any]) -> None:
    data = _json_bytes(journal)
    temp = f".journal-{secrets.token_hex(8)}.tmp"
    _write_new(tx_fd, temp, data)
    os.replace(temp, "journal.json", src_dir_fd=tx_fd, dst_dir_fd=tx_fd)
    os.fsync(tx_fd)


def _manifest_payload(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in manifest.items() if k != "manifest_sha256"}


def _check_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        raise TransactionError("manifest must be a mapping")
    payload = _manifest_payload(manifest)
    if manifest.get("manifest_sha256") != _sha(_json_bytes(payload)):
        raise TransactionError("manifest checksum mismatch")
    txid = payload.get("transaction_id")
    if not isinstance(txid, str) or len(txid) != 32 or any(c not in "0123456789abcdef" for c in txid):
        raise TransactionError("invalid transaction id")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise TransactionError("manifest has no files")
    seen = set()
    seen_indexes = set()
    seen_targets = set()
    for item in files:
        if not isinstance(item, dict):
            raise TransactionError("invalid manifest entry")
        rel = _safe_relpath(item.get("relpath"))
        if rel in seen:
            raise TransactionError("duplicate manifest path")
        seen.add(rel)
        target = _absolute(item.get("target", ""))
        if target in seen_targets:
            raise TransactionError("duplicate manifest target")
        seen_targets.add(target)
        for key in ("base_sha256", "installed_sha256", "original_sha256"):
            if not isinstance(item.get(key), str) or len(item[key]) != 64:
                raise TransactionError("invalid manifest hash")
        if not isinstance(item.get("index"), int) or item["index"] < 0:
            raise TransactionError("invalid manifest index")
        if item["index"] in seen_indexes:
            raise TransactionError("duplicate manifest index")
        seen_indexes.add(item["index"])
        if item.get("candidate_name") != f"candidate-{item['index']:04d}.bin" or item.get("original_name") != f"original-{item['index']:04d}.bin":
            raise TransactionError("invalid snapshot name")
    return payload


def freeze_candidate(
    profile: Mapping[str, Any],
    proposal_files: Mapping[str, str],
    base_hashes: Mapping[str, str],
    stage: os.PathLike[str] | str,
) -> dict[str, Any]:
    """Freeze candidate files and exact originals; compile without executing.

    ``profile["files"]`` maps approved POSIX relative paths to absolute target
    paths. Proposal and baseline keys must match exactly for the submitted set.
    """
    if not isinstance(profile, Mapping) or not isinstance(profile.get("files"), Mapping):
        raise TransactionError("profile must contain an approved files mapping")
    if not isinstance(proposal_files, Mapping) or not proposal_files:
        raise TransactionError("proposal_files must be a non-empty mapping")
    if not isinstance(base_hashes, Mapping) or set(proposal_files) != set(base_hashes):
        raise TransactionError("proposal and baseline path sets differ")
    allowlist = profile["files"]
    file_limit = profile.get("max_bytes", DEFAULT_FILE_LIMIT)
    total_limit = profile.get("max_total_bytes", DEFAULT_TOTAL_LIMIT)
    if (
        not isinstance(file_limit, int) or file_limit <= 0 or file_limit > DEFAULT_FILE_LIMIT
        or not isinstance(total_limit, int) or total_limit <= 0 or total_limit > DEFAULT_TOTAL_LIMIT
    ):
        raise TransactionError("invalid size limits")
    stage_path = _absolute(stage)
    # Opening the full chain verifies the stage directory and every ancestor.
    stage_fd = _open_dir_chain(stage_path)
    os.close(stage_fd)
    txid = secrets.token_hex(16)
    stage_fd, tx_fd = _stage_layout(stage_path, txid, create=True)
    entries: list[dict[str, Any]] = []
    total = 0
    targets: set[str] = set()
    try:
        for index, raw_rel in enumerate(sorted(proposal_files)):
            rel = _safe_relpath(raw_rel)
            if rel not in allowlist:
                raise TransactionError(f"path is not approved by profile: {rel}")
            target = _absolute(allowlist[rel])
            if target in targets:
                raise TransactionError("multiple approved paths resolve to the same target")
            targets.add(target)
            encoded = proposal_files[raw_rel]
            if not isinstance(encoded, str):
                raise TransactionError("candidate content must be base64 text")
            try:
                candidate = base64.b64decode(encoded.encode("ascii"), validate=True)
            except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
                raise TransactionError(f"invalid base64 for {rel}") from exc
            if len(candidate) > file_limit:
                raise TransactionError(f"candidate exceeds size limit: {rel}")
            try:
                text=candidate.decode('utf-8')
            except UnicodeDecodeError as exc:
                raise TransactionError('candidate must be UTF-8') from exc
            for line in text.splitlines()[:2]:
                match=re.search(r'coding[:=]\s*([-\w.]+)',line)
                if match and match[1].lower().replace('_','-') not in ('utf-8','utf8'):
                    raise TransactionError('non-UTF-8 source encoding refused')
            total += len(candidate)
            if total > total_limit:
                raise TransactionError("candidate exceeds total size limit")
            try:
                compile(candidate, rel, "exec", dont_inherit=True)
            except (SyntaxError, ValueError, TypeError) as exc:
                raise TransactionError(f"candidate does not compile: {rel}: {exc}") from exc
            original, target_stat = _read_regular(target, max_bytes=file_limit)
            expected = base_hashes[raw_rel]
            if not isinstance(expected, str) or expected != _sha(original):
                raise TransactionError(f"baseline hash does not match target: {rel}")
            candidate_name = f"candidate-{index:04d}.bin"
            original_name = f"original-{index:04d}.bin"
            _write_new(tx_fd, candidate_name, candidate)
            _write_new(tx_fd, original_name, original)
            entries.append({
                "index": index, "relpath": rel, "target": target,
                "candidate_name": candidate_name, "original_name": original_name,
                "base_sha256": _sha(original), "original_sha256": _sha(original),
                "installed_sha256": _sha(candidate), "size": len(candidate),
                "mode": stat.S_IMODE(target_stat.st_mode),
                "uid": target_stat.st_uid, "gid": target_stat.st_gid,
            })
        os.fsync(tx_fd)
        manifest: dict[str, Any] = {
            "version": 1, "transaction_id": txid, "stage": stage_path,
            "files": entries,
        }
        manifest["manifest_sha256"] = _sha(_json_bytes(manifest))
        journal = {"version": 1, "transaction_id": txid, "state": "frozen", "completed": []}
        _store_journal(tx_fd, journal)
        return manifest
    except Exception:
        # Preserve failed evidence for diagnosis but make its state explicit.
        try:
            _store_journal(tx_fd, {"version": 1, "transaction_id": txid, "state": "freeze_failed", "completed": []})
        except Exception:
            pass
        raise
    finally:
        os.close(tx_fd)
        os.close(stage_fd)


def _read_staged(tx_fd: int, name: str, max_bytes: int) -> bytes:
    fd = os.open(name, _FILE_FLAGS, dir_fd=tx_fd)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size > max_bytes:
            raise TransactionError("invalid staged file")
        chunks = []
        while True:
            chunk = os.read(fd, min(65536, max_bytes + 1))
            if not chunk:
                break
            chunks.append(chunk)
            if sum(map(len, chunks)) > max_bytes:
                raise TransactionError("staged file exceeds size limit")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _current_hash(path: str, max_bytes: int) -> tuple[str, os.stat_result]:
    data, st = _read_regular(path, max_bytes=max_bytes)
    return _sha(data), st


def _replace_target(item: Mapping[str, Any], data: bytes, txid: str) -> None:
    target = str(item["target"])
    parent_fd, name = _open_parent(target)
    temp = f".{name}.maint-{txid[:12]}-{secrets.token_hex(4)}.tmp"
    try:
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600, dir_fd=parent_fd)
        try:
            view = memoryview(data)
            while view:
                count = os.write(fd, view)
                view = view[count:]
            os.fchown(fd, int(item["uid"]), int(item["gid"]))
            os.fchmod(fd, int(item["mode"]))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temp, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
    except Exception:
        try:
            os.unlink(temp, dir_fd=parent_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(parent_fd)


def _journal_update(tx_fd: int, txid: str, state: str, completed: list[int], current: int | None = None) -> None:
    journal: dict[str, Any] = {"version": 1, "transaction_id": txid, "state": state, "completed": completed}
    if current is not None:
        journal["current"] = current
    _store_journal(tx_fd, journal)


def install(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """CAS all targets, then atomically replace each and roll back on error."""
    payload = _check_manifest(manifest)
    _stage_fd, tx_fd = _stage_layout(payload["stage"], payload["transaction_id"])
    try:
        # Verify the frozen candidate and original snapshots, then CAS every
        # destination before the first mutation.
        for item in payload["files"]:
            candidate = _read_staged(tx_fd, item["candidate_name"], DEFAULT_FILE_LIMIT)
            original = _read_staged(tx_fd, item["original_name"], DEFAULT_FILE_LIMIT)
            if _sha(candidate) != item["installed_sha256"] or _sha(original) != item["original_sha256"]:
                raise TransactionError("staged snapshot checksum mismatch")
            current, _ = _current_hash(item["target"], DEFAULT_FILE_LIMIT)
            if current != item["base_sha256"]:
                raise TransactionError(f"compare-and-swap failed before install: {item['relpath']}")
        _journal_update(tx_fd, payload["transaction_id"], "installing", [])
        installed: list[int] = []
        try:
            for item in payload["files"]:
                current, _ = _current_hash(item['target'], DEFAULT_FILE_LIMIT)
                if current != item['base_sha256']:
                    raise TransactionError('compare-and-swap failed immediately before replacement')
                # Write-ahead journal identifies the target before mutation;
                # recovery uses hashes to determine whether replacement landed.
                _journal_update(tx_fd, payload["transaction_id"], "installing", installed, item["index"])
                _replace_target(item, _read_staged(tx_fd, item["candidate_name"], DEFAULT_FILE_LIMIT), payload["transaction_id"])
                installed.append(item["index"])
                _journal_update(tx_fd, payload["transaction_id"], "installing", installed)
            _journal_update(tx_fd, payload["transaction_id"], "installed", installed)
            return {"transaction_id": payload["transaction_id"], "state": "installed", "files": len(installed)}
        except Exception as install_error:
            try:
                _rollback_payload(payload, tx_fd)
            except Exception as rollback_error:
                raise TransactionError(f"install failed and rollback needs recovery: {rollback_error}") from install_error
            raise
    finally:
        os.close(tx_fd)
        os.close(_stage_fd)


def _rollback_payload(payload: Mapping[str, Any], tx_fd: int) -> dict[str, Any]:
    txid = str(payload["transaction_id"])
    _journal_update(tx_fd, txid, "rolling_back", [])
    restored: list[int] = []
    for item in reversed(payload["files"]):
        current, _st = _current_hash(item["target"], DEFAULT_FILE_LIMIT)
        if current == item["base_sha256"]:
            continue
        if current != item["installed_sha256"]:
            raise TransactionError(f"rollback conflict; target changed concurrently: {item['relpath']}")
        original = _read_staged(tx_fd, item["original_name"], DEFAULT_FILE_LIMIT)
        if _sha(original) != item["original_sha256"]:
            raise TransactionError("original snapshot checksum mismatch")
        _journal_update(tx_fd, txid, "rolling_back", restored, int(item["index"]))
        _replace_target(item, original, txid)
        restored.append(int(item["index"]))
        _journal_update(tx_fd, txid, "rolling_back", restored)
    _journal_update(tx_fd, txid, "rolled_back", restored)
    return {"transaction_id": txid, "state": "rolled_back", "restored": len(restored)}


def rollback(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Restore originals only where the destination still has candidate bytes."""
    payload = _check_manifest(manifest)
    _stage_fd, tx_fd = _stage_layout(payload["stage"], payload["transaction_id"])
    try:
        return _rollback_payload(payload, tx_fd)
    finally:
        os.close(tx_fd)
        os.close(_stage_fd)


def recover_transaction(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Resume safe rollback after interruption during a multi-file install."""
    payload = _check_manifest(manifest)
    _stage_fd, tx_fd = _stage_layout(payload["stage"], payload["transaction_id"])
    try:
        journal_data = _read_staged(tx_fd, "journal.json", 64 * 1024)
        try:
            journal = json.loads(journal_data)
        except (ValueError, UnicodeDecodeError) as exc:
            raise TransactionError("invalid transaction journal") from exc
        if journal.get("transaction_id") != payload["transaction_id"]:
            raise TransactionError("journal transaction mismatch")
        if journal.get("state") in ("rolled_back", "frozen"):
            return {"transaction_id": payload["transaction_id"], "state": journal["state"], "restored": 0}
        return _rollback_payload(payload, tx_fd)
    finally:
        os.close(tx_fd)
        os.close(_stage_fd)
