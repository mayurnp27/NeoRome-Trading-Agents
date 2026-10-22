#!/usr/bin/env python3
"""Fixed read-only maintenance projection; no arguments, shell or provider calls."""
import json
from pathlib import Path
import sys
from datetime import datetime, timezone
import urllib.request
import urllib.error

if len(sys.argv) != 1:
    raise SystemExit('This reader accepts no arguments')
result = {'read_at': datetime.now(timezone.utc).isoformat(), 'read_only': True}
for name in ('status', 'incidents'):
    path = Path('/var/lib/neorome-maintenance') / (name + '.json')
    try:
        if path.is_symlink():
            raise ValueError('symlink')
        with path.open('rb') as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError('oversize')
        result[name] = json.loads(raw)
    except (OSError, ValueError):
        result[name] = {'status': 'unavailable'}
# These existing loopback projections redact sensitive fields at the API boundary.
# Their failure is independent of the maintenance status file.
for name, route in (('firm', '/firm/status'), ('desks', '/desks'), ('health', '/health')):
    try:
        with urllib.request.urlopen('http://127.0.0.1:8765' + route, timeout=3) as response:
            raw = response.read(1048577)
        if len(raw) > 1048576:
            raise ValueError('oversize')
        result[name] = json.loads(raw)
    except (OSError, ValueError, urllib.error.URLError):
        result[name] = {'status': 'unavailable'}
print(json.dumps(result, separators=(',', ':'), sort_keys=True))
