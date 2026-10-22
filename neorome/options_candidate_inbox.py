"""One-way, sanitized candidate handoff from options evidence to paper runtime."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Mapping

from .options_dated_artifacts import DatedArtifactError, canonical, resolve_current, sha256

FORBIDDEN_KEYS = frozenset({"token", "tokens", "authorization", "password", "secret", "raw", "response", "responses"})


class CandidateInboxError(ValueError):
    pass


def _safe(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or key.lower() in FORBIDDEN_KEYS:
                raise CandidateInboxError("candidate contains non-sanitized provider material")
            _safe(item)
    elif isinstance(value, list):
        for item in value:
            _safe(item)
    elif value is None or isinstance(value, (str, int, float, bool)):
        return
    else:
        raise CandidateInboxError("candidate contains a non-JSON value")


def _candidate(candidate: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    if candidate.get("artifact_kind") != "options-observed-shadow-decision-v1" or candidate.get("status") != "eligible_shadow_candidate":
        raise CandidateInboxError("only eligible observed candidates may enter the paper inbox")
    candidate_hash = candidate.get("artifact_sha256")
    if not isinstance(candidate_hash, str) or len(candidate_hash) != 64:
        raise CandidateInboxError("candidate hash is missing")
    if sha256({key: value for key, value in candidate.items() if key != "artifact_sha256"}) != candidate_hash:
        raise CandidateInboxError("candidate hash is invalid")
    clean = {key: candidate[key] for key in ("schema_version", "artifact_kind", "status", "reason_code", "reason", "paper_profile", "shadow_candidate",
                                               "orders_sent", "reservations_created", "ledger_written", "broker_called",
                                               "profitability_status", "evidence", "artifact_sha256") if key in candidate}
    _safe(clean)
    return clean, candidate_hash


def _valid_date(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 10 and value[4] == "-" and value[7] == "-" and value.replace("-", "").isdigit()


def _secure_root(root: str | os.PathLike[str]) -> Path:
    path = Path(root)
    if path.exists():
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o007:
            raise CandidateInboxError("paper inbox root ownership or mode is unsafe")
    else:
        path.mkdir(parents=True, mode=0o750)
    os.chmod(path, 0o750)
    return path


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical(value) + b"\n"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o640)
    except FileExistsError:
        if path.is_symlink() or not path.is_file():
            raise CandidateInboxError("inbox artifact is unsafe")
        try:
            existing = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            raise CandidateInboxError("inbox artifact is unreadable") from exc
        if canonical(existing) != canonical(value):
            raise CandidateInboxError("inbox replay conflicts with retained candidate")
    except OSError as exc:
        raise CandidateInboxError("cannot create inbox artifact") from exc
    else:
        # systemd's UMask=0077 would otherwise reduce the requested 0640
        # mode to 0600 and prevent the sole `neorome` ledger writer from
        # reading this deliberately sanitized, group-shared handoff.
        os.fchmod(descriptor, 0o640)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())


def _replace_pointer(path: Path, value: Mapping[str, Any]) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise CandidateInboxError("inbox current pointer is unsafe")
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".candidate-inbox-", dir=str(path.parent))
        os.fchmod(descriptor, 0o640)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical(value) + b"\n")
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    except OSError as exc:
        raise CandidateInboxError("cannot update inbox current pointer") from exc
    finally:
        if temporary is not None:
            try: os.unlink(temporary)
            except OSError: pass


def publish(candidate: Mapping[str, Any], root: str | os.PathLike[str], *, date: str) -> Path:
    """Atomically publish an eligible, redacted candidate for the neorome reader."""
    clean, candidate_hash = _candidate(candidate)
    if not _valid_date(date):
        raise CandidateInboxError("inbox date is malformed")
    root_path = _secure_root(root)
    directory = root_path / date
    if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
        raise CandidateInboxError("dated inbox path is unsafe")
    directory.mkdir(mode=0o750, exist_ok=True); os.chmod(directory, 0o750)
    envelope = {"artifact_kind": "options-paper-candidate-inbox-v1", "schema_version": 1, "date": date,
                "candidate_sha256": candidate_hash, "candidate": clean}
    envelope["artifact_sha256"] = sha256(envelope)
    # A same-session evidence retry can reach a different honest decision.  Keep
    # every signed candidate immutable and advance only the small current
    # manifest, instead of treating the date as an artifact identity.
    artifact = f"candidate-inbox-{candidate_hash}.json"
    output = directory / artifact
    _write_once(output, envelope)
    current = root_path / "current.json"
    if current.exists():
        try: prior = json.loads(current.read_text())
        except (OSError, ValueError) as exc: raise CandidateInboxError("inbox current pointer is unreadable") from exc
        if not isinstance(prior, Mapping) or not isinstance(prior.get("date"), str) or prior["date"] > date:
            raise CandidateInboxError("inbox current pointer rollback refused")
    _replace_pointer(current, {"schema_version": 1, "date": date, "artifact": artifact, "sha256": envelope["artifact_sha256"]})
    return output


def read_current_candidate(root: str | os.PathLike[str]) -> dict[str, Any]:
    root_path = Path(root)
    pointer = root_path / "current.json"
    if pointer.is_symlink() or not pointer.is_file():
        raise CandidateInboxError("inbox current pointer is missing")
    try: current = json.loads(pointer.read_text())
    except (OSError, ValueError) as exc: raise CandidateInboxError("inbox current pointer is unreadable") from exc
    if not isinstance(current, Mapping) or not _valid_date(current.get("date")) or not isinstance(current.get("sha256"), str) or len(current["sha256"]) != 64:
        raise CandidateInboxError("inbox current pointer is invalid")
    # Accept the fixed historical filename once so an installed runtime can be
    # upgraded without deleting its retained evidence.  New publications always
    # name the immutable candidate by its validated candidate hash.
    artifact = current.get("artifact", "candidate-inbox.json")
    if not isinstance(artifact, str) or Path(artifact).name != artifact or not artifact.endswith(".json"):
        raise CandidateInboxError("inbox current pointer artifact is invalid")
    source = root_path / current["date"] / artifact
    if source.is_symlink() or not source.is_file():
        raise CandidateInboxError("inbox candidate is missing")
    try: envelope = json.loads(source.read_text())
    except (OSError, ValueError) as exc: raise CandidateInboxError("inbox candidate is unreadable") from exc
    if not isinstance(envelope, Mapping) or envelope.get("artifact_kind") != "options-paper-candidate-inbox-v1":
        raise CandidateInboxError("inbox candidate is invalid")
    if sha256({key: value for key, value in envelope.items() if key != "artifact_sha256"}) != envelope.get("artifact_sha256") or envelope.get("artifact_sha256") != current["sha256"]:
        raise CandidateInboxError("inbox candidate hash is invalid")
    clean, digest = _candidate(dict(envelope.get("candidate") or {}))
    if digest != envelope.get("candidate_sha256"):
        raise CandidateInboxError("inbox candidate source hash is invalid")
    return clean
