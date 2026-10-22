"""Unprivileged bridge from root-owned repair snapshots to candidate proposals."""
from __future__ import annotations
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat

TRUSTED_UID=0
MAX_CATALOG_BYTES=4*1024*1024

def digest(raw):return hashlib.sha256(raw).hexdigest()

def read_catalog(path, now):
    path=Path(path)
    info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=TRUSTED_UID or info.st_mode&0o022 or info.st_nlink!=1 or info.st_size>MAX_CATALOG_BYTES:
        raise ValueError('untrusted_auto_catalog')
    if any(p.is_symlink() for p in (path,*path.parents)):
        raise ValueError('auto_catalog_symlink')
    with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW),'rb') as stream:
        raw=stream.read(MAX_CATALOG_BYTES+1)
    if len(raw)>MAX_CATALOG_BYTES:raise ValueError('auto_catalog_bound')
    value=json.loads(raw)
    age=(now-datetime.fromisoformat(value['as_of'].replace('Z','+00:00'))).total_seconds()
    if value.get('schema_version')!=1 or not -5<=age<=180:raise ValueError('stale_auto_catalog')
    if len(value.get('incidents',[]))>32:raise ValueError('auto_incident_bound')
    return value

def incidents(path, now):
    try:catalog=read_catalog(path,now)
    except FileNotFoundError:return []
    output=[]
    for entry in catalog['incidents']:
        identity=entry['fingerprint']
        if len(identity)!=64 or any(c not in '0123456789abcdef' for c in identity):raise ValueError('auto_identity')
        output.append({'key':'auto:'+entry['profile_id'],'reason':'operational_repair_required',
                       'fingerprint':identity,'agent_package_sha256':entry['snapshot_hash'],
                       'episode_id':identity,'auto_repair':entry})
    return output

def prepare(workspace, entry):
    target=Path(workspace)/'repair_target';target.mkdir(mode=0o700)
    for name,item in entry['sources'].items():
        relative=PurePosixPath(name)
        if relative.is_absolute() or '..' in relative.parts or str(relative)!=name or not name.endswith('.py'):
            raise ValueError('auto_source_path')
        raw=base64.b64decode(item['content'],validate=True)
        if len(raw)>256*1024 or digest(raw)!=item['sha256']:raise ValueError('auto_source_hash')
        p=target/name;p.parent.mkdir(parents=True,exist_ok=True,mode=0o700);p.write_bytes(raw)
    metadata={k:v for k,v in entry.items() if k!='sources'}
    metadata['source_hashes']={n:s['sha256'] for n,s in entry['sources'].items()}
    (Path(workspace)/'auto-repair.json').write_text(json.dumps(metadata,indent=2)+'\n')

def proposal(workspace, entry, result):
    changed={}
    for name,source in entry['sources'].items():
        p=Path(workspace)/'repair_target'/name
        if any(q.is_symlink() for q in (p,*p.parents)) or not p.is_file() or p.stat().st_nlink!=1 or p.stat().st_size>256*1024:
            raise ValueError('unsafe_candidate')
        raw=p.read_bytes()
        if digest(raw)!=source['sha256']:changed[name]=base64.b64encode(raw).decode()
    if result.get('escalation','').strip():changed={}
    return {'schema_version':1,'profile_id':entry['profile_id'],'incident_fingerprint':entry['fingerprint'],
            'snapshot_hash':entry['snapshot_hash'],'base_hashes':{n:s['sha256'] for n,s in entry['sources'].items()},
            'attempt_key':entry.get('attempt_key'),'attempt':entry.get('attempt',0),
            'files':changed,'diagnosis':result['diagnosis'][:8000],'escalation':result.get('escalation','')[:4000],
            'created_at':datetime.now(timezone.utc).isoformat()}

AUTO_PROMPT=(
    '\nAutomatic operational repair authorization is active for this incident. '
    'Read auto-repair.json for the root-collected current evidence and exact production source hashes. '
    'Edit only listed editable_files under repair_target; those are actual deployed snapshots. '
    'If previous_attempt_feedback is present, correct the specific rejected test/review issue before submitting again. '
    'Add a focused regression test to an allowed existing test file: it must fail with the old runtime '
    'and pass with the fix. Run the fixed profile test modules from auto-repair.json. '
    'Preserve trading strategy, entry/exit rules, risk limits, freshness rules, financial data, '
    'authentication, permission boundaries and spending limits. Never suppress health checks or fabricate observations. '
    'An independent automated reviewer and isolated trusted test runner will gate automatic deployment. '
    'You cannot deploy directly. If a justified tested fix is complete return empty escalation; '
    'otherwise state the concrete missing evidence or blocked scope.\n')
