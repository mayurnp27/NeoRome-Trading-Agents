"""Hourly prospective forecast capture; no market probabilities or orders."""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
from .weather_vintage import collect, save_new


def run(state, fetch=collect, now=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None: raise ValueError('Timezone required')
    now=now.astimezone(timezone.utc)
    root=Path(state);root.mkdir(mode=0o700,parents=True,exist_ok=True)
    if root.is_symlink(): raise ValueError('Symlink state refused')
    with (root/'capture.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if sum(p.stat().st_size for p in root.iterdir() if p.is_file())>=256*1024*1024:
            return {'status':'storage_limit','orders_sent':0}
        slot=now.strftime('%Y%m%dT%H')
        marker=root/(slot+'.attempt.json');output=root/(slot+'.forecast.json')
        if marker.exists():return {'status':'already_attempted','orders_sent':0}
        # Claim slot before external reads, retaining failed/interrupted attempts.
        save_new(marker,{'attempted_at':now.isoformat(),'requests_max':2,'output':output.name})
        try:
            result=fetch()
            save_new(output,result)
            status='recorded' if result.get('normalized') and not result.get('error') else 'source_failed'
        except Exception as exc:
            status='source_failed'
            if not output.exists():save_new(output,{'error_type':type(exc).__name__,'source_kind':'observed'})
        return {'status':status,'output':str(output),'orders_sent':0,'paper_eligible':False}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--state',required=True)
    r=run(p.parse_args().state);print(json.dumps(r))
    raise SystemExit(1 if r['status'] in ('source_failed','storage_limit') else 0)
