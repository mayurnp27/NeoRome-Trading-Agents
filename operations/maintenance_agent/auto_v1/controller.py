"""Root-owned automatic operational repair gate; candidates never execute as root."""
from __future__ import annotations
import base64,csv,datetime as dt,fcntl,hashlib,json,os,re,select,shutil,signal,stat,subprocess,sys,threading,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
from operations.maintenance_agent.auto_v1 import transaction

STATE=Path('/var/lib/neorome-auto-repair')
INBOX=Path('/var/lib/neorome-maintenance-ai/inbox')
RUNS=Path('/var/lib/neorome-maintenance-ai/auto-runs')
POLICY=Path('/etc/neorome-auto-repair/policy.json')
OPERATIONAL_RUNTIME_PATHS={
 '/opt/neorome-stochastic-paper/f15-10x10-v1/comparison.py',
 '/opt/neorome-stochastic-paper/f15-10x10-v1/feedback.py',
 '/opt/neorome/operations/maintenance_agent/operator_v1/paper_feedback.py',
 '/opt/neorome/operations/maintenance_agent/operator_v1/desk_readiness.py',
 '/opt/neorome/neorome/equities_validation_publisher.py',
 '/opt/neorome/neorome/options_validation_publisher.py',
 '/opt/neorome/neorome/prediction_validation_publisher.py',
 '/opt/neorome/neorome/crypto_validation_publisher.py',
 '/opt/neorome-auto-canary/repair_probe.py',
}
def now():return dt.datetime.now(dt.timezone.utc)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(v):return json.dumps(v,sort_keys=True,separators=(',',':')).encode()
def atomic(path,value,mode=0o600):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.tmp')
    fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,mode)
    with os.fdopen(fd,'wb') as f:f.write(encode(value));f.flush();os.fsync(f.fileno())
    os.chmod(temp,mode);os.replace(temp,path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def read(path,limit=2*1024*1024):return json.loads(transaction._read_regular(str(path),max_bytes=limit)[0])
def trusted(path):
    _,s=transaction._read_regular(str(path),max_bytes=2*1024*1024)
    if s.st_uid!=0 or s.st_mode&0o022:raise ValueError('untrusted_root_configuration')
def unit_state(unit):
    r=subprocess.run(['systemctl','show',unit,'--property=ActiveState,Result,ExecMainStatus,MainPID,FragmentPath','--no-pager'],capture_output=True,text=True,timeout=8)
    if r.returncode:raise ValueError('unit_observation_failed')
    return dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
def health(profile):
    if profile.get('unit_only'):
        u=unit_state(profile['unit'])
        return u.get('Result')=='success' and u.get('ExecMainStatus')=='0',{'healthy':u.get('Result')=='success','as_of':now().isoformat(),'basis':'unit_result_only','result':u.get('Result')}
    try:
        value=read(profile['status_path']);field=profile.get('health_field');ts=value.get(profile.get('timestamp_field','as_of'))
        age=(now()-dt.datetime.fromisoformat(ts.replace('Z','+00:00'))).total_seconds()
        healthy=(value.get(field) is True if field else True) and -5<=age<=profile.get('max_age_seconds',90)
        requirements={}
        for path,expected in profile.get('required_fields',{}).items():
            actual=value
            for key in path.split('.'):actual=actual.get(key) if isinstance(actual,dict) else None
            requirements[path]=actual
            healthy=healthy and actual==expected
        return healthy,{'age_seconds':round(age,2),'healthy':healthy,'status':value.get('status'),'reason':value.get('reason'),'as_of':ts,'requirements':requirements}
    except (OSError,ValueError,KeyError,TypeError,AttributeError,transaction.TransactionError):return False,{'healthy':False,'reason':'unreadable_or_invalid_status'}
def validate_policy(policy):
    if policy.get('schema_version')!=1 or len(policy.get('profiles',[]))>32:raise ValueError('policy_schema')
    ids=set()
    for p in policy['profiles']:
        if not re.fullmatch('[a-z][a-z0-9_-]{0,63}',p['id']) or p['id'] in ids:raise ValueError('profile_id')
        ids.add(p['id'])
        if not re.fullmatch('neorome-[a-z0-9@_.-]+\\.service',p['unit']):raise ValueError('unit_name')
        if not set(p.get('editable_files',p['files']))<=set(p['files']):raise ValueError('editable_scope')
        for relative in p.get('editable_files',p['files']):
            target=p['files'][relative]
            if not os.path.isabs(target) or target!=os.path.normpath(target) or '..' in Path(target).parts or Path(target).suffix!='.py':raise ValueError('non_normalized_target')
            if target not in OPERATIONAL_RUNTIME_PATHS and not (Path(target).name.startswith('test_') and target.startswith(('/opt/neorome/tests/','/opt/neorome/operations/maintenance_agent/operator_v1/','/opt/neorome-stochastic-paper/f15-10x10-v1/','/opt/neorome-auto-canary/'))):raise ValueError('non_operational_edit')
        for relative,target in p['files'].items():
            transaction._safe_relpath(relative)
            if not os.path.isabs(target) or target!=os.path.normpath(target) or '..' in Path(target).parts:raise ValueError('non_normalized_target')
            if not target.startswith(('/opt/neorome/','/opt/neorome-stochastic-paper/','/opt/neorome-auto-canary/')):raise ValueError('target_root')
            if any(s in target for s in ('/auth','/secret','/auto_v1/','/ai_v1/','/context_v1/','/maintenance_agent/v1/')):raise ValueError('protected_maintenance_boundary')
            trusted(Path(target))
        if not p.get('tests') or any(not re.fullmatch('[A-Za-z0-9_.]+',m) for m in p['tests']):raise ValueError('test_modules')
        if p.get('test_runner','unittest') not in ('unittest','functions'):raise ValueError('test_runner')
        trusted(Path('/etc/systemd/system')/p['unit'])
    return policy
def snapshot(profile):
    sources={}
    for name,path in profile['files'].items():
        raw,_=transaction._read_regular(path,max_bytes=256*1024)
        sources[name]={'sha256':sha(raw),'content':base64.b64encode(raw).decode()}
    return sources
def evidence(profile):
    healthy,status=health(profile);unit=unit_state(profile['unit']);logs=[]
    r=subprocess.run(['journalctl','-u',profile['unit'],'-n','12','--no-pager','-o','cat'],capture_output=True,text=True,timeout=8)
    for line in r.stdout[-16384:].splitlines():
        try:
            v=json.loads(line);item={k:v[k] for k in ('status','consecutive_failures') if k in v}
            error=v.get('error')
            if isinstance(error,dict):item['error']={k:x for k,x in error.items() if k in ('class','reason') and isinstance(x,str) and re.fullmatch('[A-Za-z0-9_]{1,80}',x)}
            if item:logs.append(item)
        except (ValueError,TypeError):pass
    sizes={}
    for path in profile.get('diagnostic_paths',[]):
        try:sizes[path]=Path(path).stat().st_size
        except OSError:sizes[path]=None
    return healthy,{'status':status,'unit':unit,'journal':logs[-6:],'source_sizes':sizes}
def catalog(policy):
    entries=[];epath=STATE/'episodes.json';episodes=read(epath) if epath.exists() else {}
    receipts=[read(p) for p in (STATE/'receipts').glob('*.json')]
    for p in policy['profiles']:
        healthy,ev=evidence(p)
        episode=episodes.setdefault(p['id'],{'active':False,'generation':0})
        if healthy and ev['unit'].get('Result')=='success':episode['active']=False;continue
        if not episode['active']:episode.update(active=True,generation=episode['generation']+1)
        sources=snapshot(p);identity=sha(encode({'id':p['id'],'bases':{n:v['sha256'] for n,v in sources.items()},'unit':p['unit_sha256']}))
        # Exactly one repair chain per profile/source/reopened episode. A changing
        # failure counter or journal message cannot create extra repair chains.
        key=sha(encode([identity,episode['generation']]))
        prior=sorted([r for r in receipts if r.get('attempt_key')==key],key=lambda r:r['as_of'])
        failed=[r for r in prior if r.get('status') in ('rejected','rolled_back','recovered_rollback')]
        attempt=min(len(failed),2);fingerprint=sha(encode([key,attempt]))
        memory_path=STATE/'memory'/(identity+'.json')
        if memory_path.exists():memory=read(memory_path)
        else:
            query=p['unit']+' '+json.dumps(ev['status'])+' '+json.dumps(ev['journal'][-1:])
            try:
                from neorome import repair_memory
                from operations.maintenance_agent.context_v1 import publisher
                vault=INBOX/'repair-memory';manifest,_=publisher._memory(vault,now())
                memory={**repair_memory.retrieve(query,vault=vault,max_chars=4000,limit=2),'query':query,'status':'available','exported_at':manifest['exported_at']}
            except (OSError,ValueError,KeyError,TypeError):memory={'status':'unavailable','query':query,'selected':[],'brief':'Memory unavailable. Diagnose only from supplied current evidence; do not infer a prior fix.'}
            atomic(memory_path,memory)
        feedback={}
        if failed:
            previous=failed[-1]
            feedback={k:previous[k] for k in ('error_code','baseline_test','candidate_test','review','diagnosis','escalation') if k in previous}
        entries.append({'profile_id':p['id'],'fingerprint':fingerprint,'snapshot_hash':identity,'attempt_key':key,'attempt':attempt,'previous_attempt_feedback':feedback,'repair_memory':memory,'sources':sources,'tests':p['tests'],'test_runner':p.get('test_runner','unittest'),'editable_files':p.get('editable_files',list(p['files'])),'unit':p['unit'],'evidence':ev})
    atomic(epath,episodes)
    value={'schema_version':1,'as_of':now().isoformat(),'incidents':entries}
    if len(encode(value))>4*1024*1024:raise ValueError('catalog_bound')
    atomic(INBOX/'auto-repair-catalog.json',value,0o644)
    atomic(INBOX/'auto-status.json',{'schema_version':1,'as_of':value['as_of'],'agent_package_sha256':sha(Path(__file__).read_bytes()),'incidents':[]},0o644)
    return value
def isolated_test(folder,profile,label):
    name='neorome-repair-test-'+label[:24]
    hidden=[str(p) for p in Path('/var/lib').glob('neorome*') if p.name!='neorome-auto-repair']
    argv=['systemd-run','--quiet','--wait','--pipe','--collect','--unit='+name,'-p','DynamicUser=yes','-p','PrivateNetwork=yes','-p','PrivateTmp=yes','-p','ProtectSystem=strict','-p','ProtectHome=yes','-p','NoNewPrivileges=yes','-p','CapabilityBoundingSet=','-p','RestrictAddressFamilies=AF_UNIX','-p','MemoryMax=256M','-p','CPUQuota=25%','-p','TasksMax=48','-p','RuntimeMaxSec=150','-p','KillMode=control-group','-p','InaccessiblePaths=/etc/neorome /etc/ssh /root /home '+' '.join(hidden),'-p','WorkingDirectory='+str(folder),'-p','Environment=PYTHONDONTWRITEBYTECODE=1','/usr/bin/python3','-B',str(Path(__file__).with_name('test_runner.py')),profile.get('test_runner','unittest'),*profile['tests']]
    proc=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True)
    data=bytearray();deadline=time.monotonic()+170;failure=None
    try:
        while True:
            if time.monotonic()>deadline:failure='test_timeout';break
            ready,_,_=select.select([proc.stdout],[],[],0.25)
            if ready:
                chunk=os.read(proc.stdout.fileno(),8192)
                if not chunk:break
                data.extend(chunk)
                if len(data)>65536:failure='test_output_limit';break
            elif proc.poll() is not None:break
        if failure:
            os.killpg(proc.pid,signal.SIGKILL)
            subprocess.run(['systemctl','stop',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=12)
        code=proc.wait(timeout=5)
    finally:
        if proc.poll() is None:os.killpg(proc.pid,signal.SIGKILL);proc.wait(timeout=5)
        proc.stdout.close()
    return {'passed':code==0 and failure is None,'returncode':code,'failure':failure,'output_tail':bytes(data[-6000:]).decode(errors='replace')}
def review(stage,patch_hash):
    output=STATE/'review-output'/stage.name;output.mkdir(parents=True,exist_ok=True)
    import pwd
    uid=pwd.getpwnam('neorome-maintenance-ai').pw_uid;os.chown(output,uid,-1);os.chmod(output,0o700)
    args=['systemd-run','--quiet','--wait','--pipe','--collect','--unit=neorome-repair-review-'+stage.name[:20],'-p','User=neorome-maintenance-ai','-p','PrivateTmp=yes','-p','ProtectSystem=strict','-p','ProtectHome=yes','-p','NoNewPrivileges=yes','-p','CapabilityBoundingSet=','-p','MemoryMax=512M','-p','CPUQuota=25%','-p','TasksMax=96','-p','KillMode=control-group','-p','RuntimeMaxSec=270','-p','ReadWritePaths='+str(output)+' /var/lib/neorome-maintenance-ai/home','/usr/bin/python3','-B',str(Path(__file__).with_name('review.py')),str(stage),str(output)]
    r=subprocess.run(args,capture_output=True,text=True,timeout=290)
    if r.returncode:raise ValueError('independent_review_failed')
    value=read(output/'review.json',16384)
    if value.get('approved') is not True or value.get('preserves_rules') is not True or value.get('patch_sha256')!=patch_hash:raise ValueError('independent_review_rejected')
    return value
def restart(profile):
    unitfile=Path('/etc/systemd/system')/profile['unit']
    if sha(unitfile.read_bytes())!=profile['unit_sha256']:raise ValueError('unit_drift')
    dropdir=Path('/etc/systemd/system')/(profile['unit']+'.d')
    actual={p.name:sha(transaction._read_regular(str(p),max_bytes=65536)[0]) for p in dropdir.glob('*.conf')}
    if actual!=profile.get('dropins_sha256',{}):raise ValueError('unit_dropin_drift')
    r=subprocess.run(['systemctl','restart',profile['unit']],capture_output=True,text=True,timeout=30)
    if r.returncode:raise ValueError('restart_failed')
def verify(profile):
    samples=[]
    for _ in range(3):
        time.sleep(profile.get('verify_interval_seconds',16));ok,data=health(profile);unit=unit_state(profile['unit'])
        samples.append(data)
        if not ok or unit.get('Result')!='success':raise ValueError('production_health_failed')
    if not profile.get('unit_only') and len({s['as_of'] for s in samples})<2:raise ValueError('output_not_advancing')
    return samples
def process(profile,proposal,receipt):
    stage=STATE/'public'/proposal['incident_fingerprint'];stage.mkdir(parents=True,exist_ok=False,mode=0o755)
    base=stage/'baseline';candidate=stage/'candidate';base.mkdir(mode=0o755);candidate.mkdir(mode=0o755)
    sources=snapshot(profile)
    if proposal['base_hashes']!={n:s['sha256'] for n,s in sources.items()}:raise ValueError('stale_candidate')
    if not proposal['files'] or not set(proposal['files'])<=set(profile.get('editable_files',sources)):raise ValueError('candidate_scope')
    for name,item in sources.items():
        raw=base64.b64decode(item['content']);new=base64.b64decode(proposal['files'].get(name,item['content']),validate=True)
        for folder,data in ((base,raw),(candidate,new)):
            p=folder/name;p.parent.mkdir(parents=True,exist_ok=True,mode=0o755);p.write_bytes(data);p.chmod(0o644)
    # Candidate regression tests also run against original runtime code.
    for name in proposal['files']:
        if Path(name).name.startswith('test_'):shutil.copyfile(candidate/name,base/name)
    patch_hash=sha(encode({'base':proposal['base_hashes'],'files':proposal['files']}))
    atomic(stage/'review-input.json',{'patch_sha256':patch_hash,'profile':profile['id'],'diagnosis':proposal.get('diagnosis',''),'changed_files':list(proposal['files']),'evidence':profile.get('last_evidence',{})},0o644)
    receipt.update(stage=str(stage),patch_sha256=patch_hash,status='testing');atomic(STATE/'receipts'/receipt['id'],receipt)
    receipt['baseline_test']=isolated_test(base,profile,stage.name[:16]+'-base')
    receipt['candidate_test']=isolated_test(candidate,profile,stage.name[:16]+'-new')
    if receipt['baseline_test']['passed'] or not receipt['candidate_test']['passed']:raise ValueError('regression_not_demonstrated')
    review_input=read(stage/'review-input.json');review_input.update(baseline_test=receipt['baseline_test'],candidate_test=receipt['candidate_test'])
    atomic(stage/'review-input.json',review_input,0o644)
    receipt['review']=review(stage,patch_hash)
    if proposal['base_hashes']!={n:s['sha256'] for n,s in snapshot(profile).items()}:raise ValueError('source_changed_during_testing')
    txstage=STATE/'transactions';txstage.mkdir(exist_ok=True,mode=0o700)
    manifest=transaction.freeze_candidate(profile,proposal['files'],{n:proposal['base_hashes'][n] for n in proposal['files']},txstage)
    receipt.update(transaction=manifest,status='installing');atomic(STATE/'receipts'/receipt['id'],receipt)
    try:
        transaction.install(manifest);restart(profile)
        receipt['verification']=verify(profile)
        receipt['status']='verified_deployed'
    except BaseException:
        receipt['status']='recovery_required';atomic(STATE/'receipts'/receipt['id'],receipt)
        transaction.rollback(manifest);restart(profile)
        receipt['status']='rolled_back';atomic(STATE/'receipts'/receipt['id'],receipt)
        raise
    return receipt
def record_outcome(receipt):
    rpath=STATE/'receipts'/receipt['id']
    # PID 1 starts the fixed writer as the existing store owner. The controller
    # never changes its own UID and candidate code never sees this transport.
    db_identity=Path('/var/lib/neorome-control/control-plane.sqlite3').stat()
    args=['systemd-run','--quiet','--wait','--pipe','--collect','--unit=neorome-repair-record-'+receipt['incident_fingerprint'][:20],'-p','User='+str(db_identity.st_uid),'-p','Group='+str(db_identity.st_gid),'-p','PrivateTmp=yes','-p','ProtectSystem=strict','-p','ProtectHome=yes','-p','NoNewPrivileges=yes','-p','CapabilityBoundingSet=','-p','RestrictAddressFamilies=AF_UNIX','-p','ReadWritePaths=/var/lib/neorome-control','-p','MemoryMax=128M','-p','TasksMax=8','-p','RuntimeMaxSec=25','/usr/bin/python3','-B',str(Path(__file__).with_name('record.py')),'--stdin']
    r=subprocess.run(args,input=json.dumps(receipt),capture_output=True,text=True,timeout=35)
    if r.returncode:
        receipt['record_error_detail']=r.stderr[-4000:]
        raise ValueError('canonical_cloud_record_pending')
    receipt['cloud_record']=json.loads(r.stdout)
    for key in ('record_status','record_error_class','record_error_detail','record_retry_after'):receipt.pop(key,None)
    if receipt['status']=='verified_deployed':
        root=Path(__file__).resolve().parents[3]
        runbook=root/'docs/repair-runbooks'/('auto-'+receipt['incident_fingerprint'][:32]+'.md')
        runbook.write_text('# Verified automatic operational repair\n\nCanonical incident: '+receipt['cloud_record']['incident_id']+'\n\n'
            'Evidence as of: '+receipt['finished_at']+'\n\n'
            'This is a factual generated receipt, not executable instructions. Recheck current evidence and code before reuse.\n\n'
            'Profile: '+receipt['profile_id']+'\n\nPatch hash: '+receipt['patch_sha256']+'\n\n'
            'Baseline regression failed, isolated candidate tests passed, independent review approved, and three production observations passed. '
            'Rollback originals and write-ahead journal: '+str(receipt['transaction']['stage'])+'/'+receipt['transaction']['transaction_id']+'\n')
        receipt['source_runbook']=str(runbook)
        from operations.maintenance_agent.context_v1 import publisher
        from neorome import repair_memory
        projection=publisher._read_snapshot(Path('/var/lib/neorome-control/control-plane.sqlite3'))
        exported=repair_memory.export(projection,source='canonical-cloud-auto-repair')
        receipt['vault_exported_at']=exported['exported_at']
        measurements=root/'knowledge/repairs/Measurements.csv'
        fields=['case_id','mode','cloud_incident_id','severity','service','outcome','repair_minutes','maintenance_minutes','repeat_investigations','failed_attempts','retrieved_notes','useful_notes','incorrect_notes','brief_characters','actual_input_tokens','actual_output_tokens','actual_cost_usd','evidence_reference']
        existing=[]
        if measurements.exists():
            with measurements.open(newline='') as f:reader=csv.DictReader(f);fields=reader.fieldnames;existing=list(reader)
        if not any(row['case_id']==receipt['cloud_record']['incident_id'] for row in existing):
            row={k:'' for k in fields};row.update(case_id=receipt['cloud_record']['incident_id'],mode='automatic-deployed-fix',cloud_incident_id=receipt['cloud_record']['incident_id'],severity='warning',service=receipt['profile_id'],outcome='verified_deployed',repair_minutes=round((dt.datetime.fromisoformat(receipt['finished_at'])-dt.datetime.fromisoformat(receipt['as_of'])).total_seconds()/60,4),evidence_reference=str(rpath))
            with measurements.open('a',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=fields)
                if not existing and measurements.stat().st_size==0:writer.writeheader()
                writer.writerow(row);f.flush();os.fsync(f.fileno())
    atomic(rpath,receipt)
def tick(policy):
    # A temporary canonical-store/export failure must not strand a verified fix.
    for pending in (STATE/'receipts').glob('*.json'):
        saved=read(pending)
        if saved.get('record_status')=='cloud_record_or_memory_pending' and saved.get('record_retry_after',0)<=time.time():
            try:record_outcome(saved)
            except Exception as exc:
                saved.update(record_status='cloud_record_or_memory_pending',record_error_class=type(exc).__name__,record_retry_after=time.time()+60);atomic(pending,saved)
            break
    profiles={p['id']:p for p in policy['profiles']};current=catalog(policy)
    active={e['fingerprint']:e for e in current['incidents']}
    for path in sorted(RUNS.glob('*/auto-proposal.json'),key=lambda p:p.stat().st_mtime):
        proposal=read(path);ident=proposal.get('incident_fingerprint')
        if not isinstance(ident,str) or not re.fullmatch('[a-f0-9]{64}',ident):continue
        rpath=STATE/'receipts'/(ident+'.json')
        if rpath.exists():continue
        receipt={'id':ident+'.json','incident_fingerprint':ident,'as_of':now().isoformat(),'profile_id':proposal.get('profile_id'),'attempt_key':proposal.get('attempt_key'),'status':'claimed'};atomic(rpath,receipt)
        try:
            p=profiles[proposal['profile_id']]
            if ident not in active or proposal['snapshot_hash']!=active[ident]['snapshot_hash']:raise ValueError('incident_no_longer_current')
            receipt['attempt_key']=active[ident]['attempt_key']
            receipt['diagnosis']=str(proposal.get('diagnosis',''))[:8000];receipt['escalation']=str(proposal.get('escalation',''))[:4000]
            if not proposal.get('files') or proposal.get('escalation'):raise ValueError('diagnosis_no_tested_patch')
            p={**p,'last_evidence':active[ident]['evidence']}
            stop=threading.Event();started=time.monotonic()
            def heartbeat():
                while not stop.is_set():
                    overdue=time.monotonic()-started>850
                    atomic(STATE/'status.json',{'schema_version':1,'as_of':now().isoformat(),'operational_healthy':not overdue,'status':'repair_deadline_exceeded' if overdue else 'repairing','profile_id':p['id'],'phase':receipt['status']},0o644)
                    stop.wait(5)
            thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
            try:receipt=process(p,proposal,receipt)
            finally:stop.set();thread.join(timeout=10)
        except Exception as exc:
            if receipt['status'] not in ('rolled_back','recovery_required','installing'):receipt['status']='rejected'
            receipt['error_class']=type(exc).__name__;receipt['error_code']=str(exc)[:160] if isinstance(exc,ValueError) else 'controller_error'
        receipt['finished_at']=now().isoformat();atomic(rpath,receipt)
        try:record_outcome(receipt)
        except Exception as exc:
            receipt['record_status']='cloud_record_or_memory_pending';receipt['record_error_class']=type(exc).__name__;atomic(rpath,receipt)
        break
    blocked=[]
    for ident in active:
        p=STATE/'receipts'/(ident+'.json')
        if p.exists():
            r=read(p)
            if r.get('status') in ('rejected','rolled_back','recovered_rollback','recovery_required','installing'):blocked.append({'profile_id':r['profile_id'],'status':r['status'],'error_code':r.get('error_code')})
    ai_status='starting'
    try:ai_status=read(Path('/var/lib/neorome-maintenance-ai/auto-state/status.json'))['status']
    except (OSError,ValueError,KeyError,transaction.TransactionError):pass
    ai_blocked=ai_status in ('authentication_required','command_runner_unavailable','workspace_capacity_exhausted','workspace_prepare_failed','worker_error','stale_or_invalid_input','claim_capacity_exhausted')
    pending_records=sum(read(path).get('record_status')=='cloud_record_or_memory_pending' for path in (STATE/'receipts').glob('*.json'))
    atomic(STATE/'status.json',{'schema_version':1,'as_of':now().isoformat(),'pending_cloud_records':pending_records,'operational_healthy':not blocked and not ai_blocked and not pending_records,'status':'repair_blocked' if blocked or ai_blocked or pending_records else 'watching','ai_worker_status':ai_status,'active_supported_incidents':len(active),'blocked':blocked,'profiles':[p['id'] for p in policy['profiles']]},0o644)
def recover(policy):
    profiles={p['id']:p for p in policy['profiles']}
    for path in (STATE/'receipts').glob('*.json'):
        r=read(path)
        if r.get('status') in ('installing','recovery_required') and 'transaction' in r:
            transaction.recover_transaction(r['transaction']);restart(profiles[r['profile_id']]);r['status']='recovered_rollback';atomic(path,r)
def main():
    STATE.mkdir(exist_ok=True,mode=0o755);(STATE/'receipts').mkdir(exist_ok=True,mode=0o700)
    fd=os.open(STATE/'controller.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    trusted(POLICY);policy=validate_policy(read(POLICY));recover(policy)
    while True:
        try:recover(policy);tick(policy)
        except Exception as exc:
            atomic(STATE/'status.json',{'as_of':now().isoformat(),'operational_healthy':False,'status':'controller_error','error_class':type(exc).__name__},0o644)
        time.sleep(5)
if __name__=='__main__':main()
