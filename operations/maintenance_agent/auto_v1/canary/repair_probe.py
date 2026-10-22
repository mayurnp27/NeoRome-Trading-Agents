"""Nonfinancial repair canary; deliberately undersized reader for rollout proof."""
from datetime import datetime,timezone
import json
from pathlib import Path
import time

MAX_SOURCE_BYTES=1024
def read_status(path):
    if path.stat().st_size>MAX_SOURCE_BYTES:raise ValueError('status_source_oversized')
    return json.loads(path.read_text())
def main():
    root=Path('/var/lib/neorome-auto-repair-canary')
    while True:
        try:
            value=read_status(root/'input.json');healthy=True;error=None
            count=len(value['items'])
        except (ValueError,OSError,KeyError) as exc:
            healthy=False;count=None;error={'class':type(exc).__name__,'reason':'status_source_oversized' if isinstance(exc,ValueError) else 'source_invalid'}
        report={'as_of':datetime.now(timezone.utc).isoformat(),'healthy':healthy,'records':count}
        p=root/'status.tmp';p.write_text(json.dumps(report));p.replace(root/'status.json')
        print(json.dumps({'status':'published' if healthy else 'publish_failed','error':error}),flush=True)
        time.sleep(5)
if __name__=='__main__':main()
