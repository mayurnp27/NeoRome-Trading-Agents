"""Minimal liveness and readiness endpoints for the cloud paper runtime.

``/healthz`` deliberately proves only that this HTTP process can answer.  The
dependency and trading-entry checks live on ``/readyz`` so a safe abstention or
failed recovery verification is never mislabeled as a host outage.
"""
import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path


def healthy(path, now=None, max_age_seconds=660):
    now = now or datetime.now(timezone.utc)
    try:
        source = Path(path)
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor) as stream:
            raw = stream.read(131073)
        if len(raw) > 131072:
            return False
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get('updated_at'), str):
            return False
        timestamp = datetime.fromisoformat(data['updated_at'].replace('Z', '+00:00'))
        age = (now - timestamp).total_seconds()
        return data.get('operational_healthy') is True and 0 <= age <= max_age_seconds
    except (OSError, ValueError, TypeError, KeyError):
        return False


def readiness(checks, now=None):
    """Return a stable, non-secret readiness summary for named status files."""
    current = now or datetime.now(timezone.utc)
    results = []
    for name, path, max_age_seconds in checks:
        ok = healthy(path, current, max_age_seconds=max_age_seconds)
        results.append({"name": name, "ready": ok})
    failed = [item["name"] for item in results if not item["ready"]]
    return {
        "schema_version": 1,
        "ready": not failed,
        "checked_at": current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "checks": results,
        "failed_checks": failed,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--status', default='/var/lib/neorome-ops/status.json')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--runtime-status')
    parser.add_argument('--recovery-status')
    args = parser.parse_args()

    checks = [("operations", args.status, 660)]
    if args.runtime_status:
        checks.append(("paper_runtime", args.runtime_status, 180))
    if args.recovery_status:
        checks.append(("recovery_verification", args.recovery_status, 7200))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/healthz':
                body = b'healthy\n'
                self.send_response(200)
                content_type = 'text/plain'
            elif self.path == '/readyz':
                result = readiness(checks)
                body = (json.dumps(result, sort_keys=True, separators=(',', ':')) + '\n').encode()
                self.send_response(200 if result['ready'] else 503)
                content_type = 'application/json'
            else:
                self.send_error(404)
                return
            self.send_header('Content-Type', content_type)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(body)

        def setup(self):
            self.request.settimeout(2)
            super().setup()

        def log_message(self, *args):
            pass  # Do not persist attacker-controlled paths or headers.

    class Server(HTTPServer):
        allow_reuse_address = True

    Server(('0.0.0.0', args.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
