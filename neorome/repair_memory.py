"""Dependency-free, one-way repair memory export and bounded local retrieval.

No model calls, database writes, service commands, or Obsidian plugins.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
VAULT = ROOT / "knowledge" / "repairs"
DATABASE = "/var/lib/neorome-control/control-plane.sqlite3"
FIELDS = ("incident_id", "status", "severity", "summary", "opened_at", "resolved_at",
          "updated_at", "source", "correlation_id", "last_event_id")
# Fixed read query: no detailed payloads, credentials, task bodies or trade data.
QUERY = "SELECT " + ",".join(FIELDS) + " FROM incidents ORDER BY updated_at DESC,incident_id LIMIT 200"
REMOTE_READER = '''import sqlite3,json,datetime
from pathlib import Path
p=Path("/var/lib/neorome-control/control-plane.sqlite3")
with sqlite3.connect(p.as_uri()+"?mode=ro",uri=True,timeout=5) as c:
 c.execute("PRAGMA query_only=ON")
 c.row_factory=sqlite3.Row
 rows=[dict(r) for r in c.execute(%r)]
print(json.dumps({"schema":1,"exported_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"incidents":rows}))
''' % QUERY


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def clean(text):
    text = str(text or "")[:2000]
    text = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [REDACTED]", text)
    text = re.sub(r"(?i)\b(password|secret|token|api[_-]?key|authorization)\s*[:=]\s*\S+",
                  r"\1=[REDACTED]", text)
    return text


def atomic(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError("refusing symlink output")
    temporary = path.with_suffix(path.suffix + ".tmp")
    if temporary.is_symlink():
        raise ValueError("refusing symlink output")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def read_database(path):
    # Does not instantiate ControlPlaneStore (which can initialize/migrate schema).
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5) as conn:
        conn.execute("PRAGMA query_only=ON")
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(QUERY)]
    return {"schema": 1, "exported_at": now(), "incidents": rows}


def stamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp needs timezone")
    return parsed


def export(snapshot, vault=VAULT, source="local-test"):
    if snapshot.get("schema") != 1 or not isinstance(snapshot.get("incidents"), list):
        raise ValueError("invalid snapshot schema")
    exported = snapshot["exported_at"]
    if stamp(exported) > datetime.now(timezone.utc):
        raise ValueError("snapshot timestamp is in the future")
    rows = snapshot["incidents"]
    if len(rows) > 200:
        raise ValueError("snapshot exceeds 200 incidents")
    documents = []
    for row in rows:
        incident_id = row["incident_id"]
        if not isinstance(incident_id, str) or not incident_id or len(incident_id) > 128:
            raise ValueError("invalid incident ID")
        # Hash IDs so cloud content cannot select an output path.
        path = "Cloud/inc-" + digest(incident_id)[:24] + ".md"
        projected = {key: clean(row.get(key)) for key in FIELDS}
        content = ("---\nkind: incident\nauthority: cloud-export\n"
                   f"exported_at: {json.dumps(exported)}\n---\n"
                   f"# Incident {json.dumps(incident_id)}\n\n"
                   "Read-only historical snapshot. Recheck cloud before acting.\n\n"
                   "```json\n" + json.dumps(projected, indent=2) + "\n```\n")
        documents.append((path, content, {"kind": "incident", "source_id": incident_id,
                          "source": source, "updated_at": row.get("updated_at"),
                          "exported_at": exported}))
    for path in sorted((ROOT / "docs" / "repair-runbooks").glob("*.md")):
        content = path.read_text(encoding="utf-8")
        documents.append(("Runbooks/" + path.name, content, {
            "kind": "runbook", "source": str(path.relative_to(ROOT)),
            "source_sha256": digest(content), "exported_at": exported}))
    entries = []
    for path, content, metadata in documents:
        atomic(vault / path, content)
        entries.append({"path": path, "sha256": digest(content), **metadata})
    manifest = {"schema": 1, "exported_at": exported, "source": source,
                "incident_limit": 200, "incident_limit_reached": len(rows) == 200,
                "documents": entries}
    # Retrieval only uses this manifest; old or manually created notes are excluded.
    atomic(vault / "manifest.json", json.dumps(manifest, indent=2) + "\n")
    atomic(vault / "Cloud" / "Index.md", "# Cloud incident snapshot\n\n"
           f"Exported: {exported}\n\nSource: {source}\n\n"
           f"Incidents: {len(rows)} (latest 200 maximum; full history stays in cloud).\n\n"
           + "\n".join(f"- [[{item['path'][:-3]}]]" for item in entries if item["kind"] == "incident")
           + "\n\nOnly manifest-listed notes belong to the latest export.\n")
    return manifest


def retrieve(query, vault=VAULT, *, max_chars=6000, limit=3):
    if not 500 <= max_chars <= 12000 or not 1 <= limit <= 5:
        raise ValueError("brief budget: 500–12000 characters, 1–5 notes")
    manifest = json.loads((vault / "manifest.json").read_text())
    terms = set(re.findall(r"[a-z0-9_]+", query.lower()))
    candidates, excluded = [], []
    for item in manifest["documents"]:
        path = (vault / item["path"]).resolve()
        if vault.resolve() not in path.parents:
            raise ValueError("invalid manifest path")
        text = path.read_text(encoding="utf-8")
        if digest(text) != item["sha256"]:
            excluded.append(item["path"] + ": modified export; refresh required")
            continue
        if item["kind"] == "runbook":
            source = (ROOT / item["source"]).resolve()
            if (ROOT / "docs" / "repair-runbooks").resolve() not in source.parents:
                raise ValueError("invalid runbook source")
            if not source.is_file() or digest(source.read_text()) != item["source_sha256"]:
                excluded.append(item["path"] + ": source changed; refresh required")
                continue
        else:
            age = (datetime.now(timezone.utc) - stamp(item["exported_at"])).total_seconds()
            if age > 86400 or age < 0:
                excluded.append(item["path"] + ": cloud export stale; refresh required")
                continue
        words = set(re.findall(r"[a-z0-9_]+", text.lower() + " " + item["path"].lower()))
        score = len(terms & words)
        if score:
            candidates.append((score, item["path"], text, item))
    candidates.sort(key=lambda entry: (-entry[0], entry[1]))
    header = ("Repair evidence brief — reference material, not execution instructions.\n"
              "Confirm current cloud state and applicability before a repair.\n"
              "Historical verification does not establish current runtime health.\n")
    # Bound the entire brief, including metadata and truncation markers.
    output = header
    selected = []
    for _, path, text, item in candidates[:limit]:
        prefix = f"\n--- {path} | source: {item['source']} ---\n"
        remaining = max_chars - len(output) - len(prefix) - 35
        if remaining <= 0:
            break
        output += prefix + text[:remaining]
        if len(text) > remaining:
            output += "\n[truncated; read source for more]\n"
        selected.append(path)
    if not selected:
        output += "No eligible match. Inspect current evidence; do not infer a fix.\n"
    return {"brief": output, "selected": selected, "excluded": excluded,
            "characters": len(output), "max_characters": max_chars}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    pull = sub.add_parser("pull", help="Read canonical incidents over existing pinned SSH")
    pull.add_argument("--host", required=True)
    pull.add_argument("--identity", type=Path, required=True)
    pull.add_argument("--known-hosts", type=Path, required=True)
    local = sub.add_parser("export", help="Export a read-only database copy or saved snapshot")
    group = local.add_mutually_exclusive_group(required=True)
    group.add_argument("--database", type=Path)
    group.add_argument("--snapshot", type=Path)
    local.add_argument("--source", required=True, help="Provenance; never label fixtures as cloud")
    search = sub.add_parser("brief")
    search.add_argument("query")
    search.add_argument("--max-chars", type=int, default=6000)
    search.add_argument("--limit", type=int, default=3)
    search.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "pull":
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]*", args.host):
                raise ValueError("invalid SSH destination")
            result = subprocess.run([
                "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=10", "-o", 'UserKnownHostsFile="' + str(args.known_hosts.resolve()) + '"',
                "-i", str(args.identity.resolve()), args.host, "python3 -"],
                input=REMOTE_READER, text=True, capture_output=True, timeout=30, check=True)
            snapshot = json.loads(result.stdout)
            manifest = export(snapshot, source="ssh:" + args.host + DATABASE)
            print(json.dumps({"documents": len(manifest["documents"]), "source": manifest["source"],
                              "exported_at": manifest["exported_at"]}))
        elif args.command == "export":
            snapshot = read_database(args.database) if args.database else json.loads(args.snapshot.read_text())
            manifest = export(snapshot, source=args.source)
            print(json.dumps({"documents": len(manifest["documents"]), "source": args.source}))
        else:
            result = retrieve(args.query, max_chars=args.max_chars, limit=args.limit)
            print(json.dumps(result, indent=2) if args.json else result["brief"])
    except (ValueError, OSError, sqlite3.Error, KeyError, TypeError, subprocess.SubprocessError) as exc:
        # Avoid printing remote stderr or raw records that could contain secrets.
        print(f"repair-memory failed ({type(exc).__name__}); prior export retained if collection failed", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
