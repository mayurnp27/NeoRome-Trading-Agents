"""Fixed canonical incident writer, executed as the existing database owner."""
import datetime,json,os,stat,sys,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
from operations.maintenance_agent.auto_v1 import transaction

def main():
    db=Path('/var/lib/neorome-control/control-plane.sqlite3');s=db.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink!=1:raise ValueError('canonical_store_identity')
    if sys.argv[1]=='--stdin':
        if os.geteuid()!=s.st_uid or os.getegid()!=s.st_gid:raise ValueError('canonical_writer_identity')
        raw=sys.stdin.buffer.read(1024*1024+1)
        if len(raw)>1024*1024:raise ValueError('receipt_size')
    else:
        path=Path(sys.argv[1])
        if path.parent!=Path('/var/lib/neorome-auto-repair/receipts') or not path.name.endswith('.json'):raise ValueError('receipt_path')
        raw,metadata=transaction._read_regular(str(path),max_bytes=1024*1024)
        if metadata.st_uid!=0 or metadata.st_mode&0o022:raise ValueError('receipt_owner')
        os.setgroups([]);os.setgid(s.st_gid);os.setuid(s.st_uid)
    r=json.loads(raw)
    from neorome.control_plane_store import ControlPlaneStore,NeoRomeEvent
    good=r['status']=='verified_deployed';incident='auto-repair-'+r['incident_fingerprint'][:32]
    at=r['finished_at'];event=str(uuid.uuid5(uuid.NAMESPACE_URL,incident+':'+r['status']))
    with ControlPlaneStore(db) as store:
        existing=store.get_event(event)
        if existing is not None:
            if existing['source']!='neorome.auto-repair-controller' or existing['correlation_id']!=incident:raise ValueError('canonical_event_identity')
            print(json.dumps({'incident_id':incident,'event_id':event,'status':'resolved' if good else 'open'}));return
        # Bookkeeping may change when a pending record is retried.
        r={k:v for k,v in r.items() if k not in ('cloud_record','record_status','record_error_class','record_error_detail','record_retry_after','vault_exported_at','source_runbook')}
        out=store.record_incident(NeoRomeEvent.create(event_id=event,timestamp=at,desk='firm',event_type='RECOVERY_VERIFICATION_RESTORED' if good else 'SYSTEM_ERROR',severity='info' if good else 'warning',summary='Automatic operational repair '+r['status']+' for '+r['profile_id'],source='neorome.auto-repair-controller',correlation_id=incident,idempotency_key=event,data={'incident_id':incident,'status':'resolved' if good else 'open','opened_at':r['as_of'],'resolved_at':at if good else None,'details':r}))
        assert store.get_event(out['last_event_id']) is not None
    print(json.dumps({'incident_id':incident,'event_id':event,'status':'resolved' if good else 'open'}))
if __name__=='__main__':main()
