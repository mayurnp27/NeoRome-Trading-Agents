"""Sanitized immutable lifecycle evidence events for the options paper reader."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping

from .options_dated_artifacts import canonical, sha256

PHASES = frozenset({"entry", "mark", "exit"})
FORBIDDEN = frozenset({"token", "tokens", "authorization", "password", "secret", "raw", "response", "responses"})

class LifecycleInboxError(ValueError): pass

def _safe(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or key.lower() in FORBIDDEN: raise LifecycleInboxError("raw provider material is not publishable")
            _safe(item)
    elif isinstance(value, list):
        for item in value: _safe(item)
    elif value is None or isinstance(value, (str, int, float, bool)): return
    else: raise LifecycleInboxError("non-JSON lifecycle value")

def _root(root: str | os.PathLike[str]) -> Path:
    path = Path(root)
    if path.exists():
        info=os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o007:
            raise LifecycleInboxError("lifecycle inbox ownership or mode is unsafe")
    else: path.mkdir(parents=True, mode=0o750)
    os.chmod(path, 0o750); return path

def _capture(capture: Mapping[str, Any], expected_symbols: set[str]) -> tuple[dict[str, Any], str]:
    if capture.get("artifact_kind") != "options-cloud-evidence-v1" or capture.get("status") != "captured" or capture.get("broker_write_called") is not False:
        raise LifecycleInboxError("complete read-only cloud capture required")
    original = capture.get("artifact_sha256")
    if not isinstance(original, str) or sha256({k:v for k,v in capture.items() if k != "artifact_sha256"}) != original:
        raise LifecycleInboxError("source cloud capture hash is invalid")
    rows=capture.get("normalized_quotes")
    if not isinstance(rows, list) or not expected_symbols.issubset({str(r.get("symbol")) for r in rows if isinstance(r,Mapping)}):
        raise LifecycleInboxError("capture does not cover both exact lifecycle legs")
    keep=("schema_version","status","provider","symbol","right","expiry","local_receipt_at","as_of","chain_complete","size_units","size_units_evidence","chain","normalized_quotes","holdings","orders_sent","broker_write_called","profitability_status")
    clean={k:capture[k] for k in keep if k in capture}
    clean.update(artifact_kind="options-cloud-evidence-sanitized-v1", source_capture_sha256=original)
    clean["artifact_sha256"]=sha256(clean)
    _safe(clean)
    return clean, original

def _write(path: Path, value: Mapping[str,Any]) -> None:
    payload=canonical(value)+b"\n"
    try: fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o640)
    except FileExistsError:
        if path.is_symlink() or not path.is_file(): raise LifecycleInboxError("lifecycle artifact path is unsafe")
        try: current=json.loads(path.read_text())
        except (OSError,ValueError) as exc: raise LifecycleInboxError("lifecycle artifact unreadable") from exc
        if canonical(current)!=canonical(value): raise LifecycleInboxError("lifecycle event collision")
    else:
        # Lifecycle captures are sanitized and intentionally readable by the
        # sole `neorome` paper-ledger writer.  systemd's UMask=0077 must not
        # reduce the requested 0640 handoff mode to 0600.
        os.fchmod(fd,0o640)
        with os.fdopen(fd,"wb") as stream: stream.write(payload);stream.flush();os.fsync(stream.fileno())

def _pointer(path:Path,value:Mapping[str,Any])->None:
    if path.is_symlink() or (path.exists() and not path.is_file()): raise LifecycleInboxError("lifecycle pointer unsafe")
    tmp=None
    try:
        fd,tmp=tempfile.mkstemp(prefix=".lifecycle-",dir=str(path.parent));os.fchmod(fd,0o640)
        with os.fdopen(fd,"wb") as stream: stream.write(canonical(value)+b"\n");stream.flush();os.fsync(stream.fileno())
        os.replace(tmp,path);tmp=None
    finally:
        if tmp:
            try: os.unlink(tmp)
            except OSError: pass

def publish(*, phase:str, event_id:str, date:str, capture:Mapping[str,Any], expected_symbols:set[str], root:str|os.PathLike[str])->Path:
    if phase not in PHASES or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,80}",event_id): raise LifecycleInboxError("lifecycle phase or event id malformed")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}",date): raise LifecycleInboxError("lifecycle date malformed")
    clean, source_hash=_capture(capture,expected_symbols); base=_root(root); directory=base/date
    if directory.exists() and (directory.is_symlink() or not directory.is_dir()): raise LifecycleInboxError("dated lifecycle path unsafe")
    directory.mkdir(mode=0o750,exist_ok=True);os.chmod(directory,0o750)
    envelope={"artifact_kind":"options-paper-lifecycle-inbox-v1","schema_version":1,"phase":phase,"event_id":event_id,"date":date,"source_capture_sha256":source_hash,"capture":clean}
    envelope["artifact_sha256"]=sha256(envelope); output=directory/f"{phase}-{event_id}.json";_write(output,envelope)
    pointer=base/"current.json"; events=[]
    if pointer.exists():
        try: prior=json.loads(pointer.read_text())
        except (OSError,ValueError) as exc: raise LifecycleInboxError("lifecycle pointer unreadable") from exc
        if not isinstance(prior,Mapping) or prior.get("date","")>date: raise LifecycleInboxError("lifecycle pointer rollback")
        if prior.get("date")==date and isinstance(prior.get("events"),list): events=list(prior["events"])
    item={"phase":phase,"event_id":event_id,"file":output.name,"sha256":envelope["artifact_sha256"]}
    if item not in events: events.append(item)
    _pointer(pointer,{"schema_version":1,"date":date,"events":events})
    return output

def read_events(root:str|os.PathLike[str])->list[dict[str,Any]]:
    base=Path(root); pointer=base/"current.json"
    if pointer.is_symlink() or not pointer.is_file(): return []
    try: current=json.loads(pointer.read_text())
    except (OSError,ValueError) as exc: raise LifecycleInboxError("lifecycle pointer unreadable") from exc
    if not isinstance(current,Mapping) or not isinstance(current.get("date"),str) or not isinstance(current.get("events"),list): raise LifecycleInboxError("lifecycle pointer invalid")
    result=[]
    for item in current["events"]:
        if not isinstance(item,Mapping) or item.get("phase") not in PHASES or not isinstance(item.get("file"),str): raise LifecycleInboxError("lifecycle pointer event invalid")
        path=base/current["date"]/item["file"]
        if path.is_symlink() or not path.is_file(): raise LifecycleInboxError("lifecycle event missing")
        try: envelope=json.loads(path.read_text())
        except (OSError,ValueError) as exc: raise LifecycleInboxError("lifecycle event unreadable") from exc
        if not isinstance(envelope,Mapping) or envelope.get("artifact_kind")!="options-paper-lifecycle-inbox-v1" or envelope.get("artifact_sha256")!=item.get("sha256") or sha256({k:v for k,v in envelope.items() if k!="artifact_sha256"})!=item.get("sha256"):
            raise LifecycleInboxError("lifecycle event hash invalid")
        result.append(dict(envelope))
    return result
