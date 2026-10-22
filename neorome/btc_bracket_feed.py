"""Shared 1Hz public Kalshi book recorder and prospective bracket-paper driver.

GET-only, no credentials or venue-order client. Original crossover books are
read-only source cohorts. Each new comparison arm has independent paper cash.
"""
from __future__ import annotations
import argparse,collections,html,concurrent.futures,fcntl,gzip,hashlib,http.client,json,math,os,re,shutil,signal,sqlite3,statistics,tempfile,threading,time
from datetime import datetime,timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from . import btc_fast_stochastic_paper as fast
from . import btc_stochastic_paper as slow
from .btc_bracket_engine import ALLOCATION_BPS, ALLOCATION_POLICY_VERSION

ROOT=Path('/var/lib/neorome-btc-brackets-v1')
SOURCES={'5m':Path('/var/lib/neorome-btc-cross5-paper/btc_fast_stochastic_paper.sqlite3'),
         '30m':Path('/var/lib/neorome-btc-cross30-paper/btc_stochastic_paper.sqlite3')}
HOST='external-api.kalshi.com';API='/trade-api/v2'
TICKER=re.compile(r'KXBTC15M-[A-Z0-9-]{1,60}\Z')
MAX_SOURCE_AGE_SECONDS=2.0
MAX_PROCESSING_LAG_SECONDS=.5
MAX_REQUESTS_PER_DAY=120000
MAX_ARCHIVE_BYTES=2*1024**3
RETENTION_SECONDS=3*86400
PUBLICATION_PROTOCOL='btc-bracket-source-fill-publication-v1'
GLOBAL_STOP=Path('/var/lib/neorome-paper/STOP')
RECOVERY=Path('/var/lib/neorome-paper-recovery/status.json')

def iso(t=None):return datetime.fromtimestamp(time.time() if t is None else t,timezone.utc).isoformat()
def stamp(s):return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(v):return hashlib.sha256(canonical(v).encode()).hexdigest()
def atomic(path,v):
    if path.is_symlink():raise ValueError('symlink refused')
    tmp=path.with_suffix(path.suffix+'.tmp')
    if tmp.is_symlink():raise ValueError('temporary symlink refused')
    with tmp.open('w') as f:f.write(canonical(v)+'\n');f.flush();os.fsync(f.fileno())
    tmp.chmod(0o600);tmp.replace(path)

def migrate_allocation(root):
    """Resume-preserving, fixed 50%-to-20% config migration under runner.lock."""
    root=Path(root);path=root/'config.json';policy_path=root/'allocation-policy.json'
    if root.is_symlink() or path.is_symlink() or policy_path.is_symlink():raise ValueError('allocation migration symlink refused')
    config=json.loads(path.read_text())
    if policy_path.exists():
        policy=json.loads(policy_path.read_text());old=policy['prior_config']
        expected=old|{'allocation_bps':ALLOCATION_BPS,'allocation_policy_version':ALLOCATION_POLICY_VERSION,'allocation_effective_at':policy['effective_at']}
        if policy.get('policy_version')!=ALLOCATION_POLICY_VERSION or policy.get('from_bps')!=5000 or policy.get('to_bps')!=ALLOCATION_BPS or policy.get('new_config')!=expected or old.get('allocation_bps')!=5000:raise ValueError('allocation policy conflict')
        if config==expected:return policy
        if config!=old:raise ValueError('allocation config changed during migration')
    else:
        if config.get('allocation_bps')==ALLOCATION_BPS and config.get('allocation_policy_version')==ALLOCATION_POLICY_VERSION:return {'already_current':True,'allocation_bps':ALLOCATION_BPS}
        if config.get('version')!='BTC-BRACKETS-1HZ-v1' or config.get('allocation_bps')!=5000 or config.get('initial_cash_cents_each')!=50000 or config.get('target_cadence_seconds')!=1 or not isinstance(config.get('started_at'),(int,float)) or isinstance(config.get('started_at'),bool) or not math.isfinite(config['started_at']):raise ValueError('unknown legacy allocation config')
        effective=iso();expected=config|{'allocation_bps':ALLOCATION_BPS,'allocation_policy_version':ALLOCATION_POLICY_VERSION,'allocation_effective_at':effective}
        policy={'policy_version':ALLOCATION_POLICY_VERSION,'from_bps':5000,'to_bps':ALLOCATION_BPS,'effective_at':effective,'prior_config':config,'new_config':expected}
        descriptor=os.open(policy_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o400)
        with os.fdopen(descriptor,'w') as stream:stream.write(canonical(policy)+'\n');stream.flush();os.fsync(stream.fileno())
    atomic(path,expected)
    return policy

class Archive:
    def __init__(self,root):
        self.root=Path(root)/'quotes';self.root.mkdir(mode=0o700,parents=True,exist_ok=True)
        if self.root.is_symlink():raise ValueError('quote directory symlink refused')
        self.stream=None;self.slot=None;self.last_prune=0;self.bytes=0
    def append(self,record):
        now=record['capture_wall_ns']/1e9;slot=int(now//300)
        if slot!=self.slot:
            self.close();self.slot=slot
            self.path=self.root/(datetime.fromtimestamp(slot*300,timezone.utc).strftime('%Y%m%dT%H%MZ')+'.jsonl.gz')
            if self.path.is_symlink():raise ValueError('archive symlink refused')
            self.stream=gzip.open(self.path,'ab',compresslevel=5);self.path.chmod(0o600)
        self.stream.write((canonical(record)+'\n').encode());self.stream.flush()
        if now-self.last_prune>=60:
            self.prune(now);self.last_prune=now
    def prune(self,now):
        files=sorted(self.root.glob('*.jsonl.gz'))
        for p in files:
            if p.is_symlink():raise ValueError('archive symlink refused')
            if p!=getattr(self,'path',None) and p.stat().st_mtime<now-RETENTION_SECONDS:p.unlink()
        files=sorted(self.root.glob('*.jsonl.gz'));self.bytes=sum(p.stat().st_size for p in files)
        # Stop before discarding the recent requested evidence to make space.
        if self.bytes>MAX_ARCHIVE_BYTES:raise RuntimeError('quote_archive_storage_limit')
    def close(self):
        if self.stream:self.stream.close();self.stream=None

class PublicClient:
    def __init__(self):self.local=threading.local()
    def request(self,kind,ticker=None):
        if kind=='markets':path=API+'/markets?series_ticker=KXBTC15M&status=open&limit=10&exchange_index=2'
        elif kind in ('book','market') and isinstance(ticker,str) and TICKER.fullmatch(ticker):
            path=API+'/markets/'+ticker+('/orderbook?depth=100&exchange_index=2' if kind=='book' else '?exchange_index=2')
        else:raise ValueError('public route refused')
        start_wall=time.time_ns();start_mono=time.monotonic_ns()
        record={'kind':kind,'ticker':ticker,'request_started_wall_ns':start_wall,'request_started_monotonic_ns':start_mono,'path':path}
        try:
            con=getattr(self.local,'connection',None)
            if con is None:con=http.client.HTTPSConnection(HOST,timeout=.9);self.local.connection=con
            con.request('GET',path,headers={'User-Agent':'NeoRome-Bracket-Research/1.0','Accept':'application/json','Cache-Control':'no-cache'})
            response=con.getresponse();raw=response.read(500001)
            record.update(http_status=response.status,http_date=response.getheader('Date'),cache_age=response.getheader('Age'),retry_after=response.getheader('Retry-After'),response_body=raw.decode('utf-8',errors='replace'),body_sha256=hashlib.sha256(raw).hexdigest())
            if len(raw)>500000:record['transport_error']='oversized_response';con.close();self.local.connection=None
        except Exception as e:
            record.update(transport_error=type(e).__name__+': '+str(e)[:160])
            con=getattr(self.local,'connection',None)
            if con:con.close()
            self.local.connection=None
        record['capture_wall_ns']=time.time_ns();record['capture_monotonic_ns']=time.monotonic_ns()
        record['request_duration_ms']=(record['capture_monotonic_ns']-start_mono)/1e6
        record['wall_vs_monotonic_elapsed_delta_ms']=((record['capture_wall_ns']-start_wall)-(record['capture_monotonic_ns']-start_mono))/1e6
        return record

def decode(record,now=None):
    """Validate independently timestamped raw public receipts; no invented event clock."""
    now=time.time() if now is None else now;receipt=record['capture_wall_ns']/1e9
    if record.get('transport_error'):raise ValueError(record['transport_error'])
    if record.get('http_status')!=200:raise ValueError('http_status_'+str(record.get('http_status')))
    if not record.get('http_date'):raise ValueError('http_date_missing')
    source=parsedate_to_datetime(record['http_date']).timestamp();age=receipt-source
    record['http_date_capture_delta_ms']=age*1000
    # HTTP Date is a response clock, not an exchange order-book event timestamp.
    record['source_time_kind']='http_response_date';record['raw_exchange_timestamps']=None
    if not 0<=age<=MAX_SOURCE_AGE_SECONDS:raise ValueError('source_clock_stale_or_future')
    if not 0<=now-receipt<=MAX_PROCESSING_LAG_SECONDS:raise ValueError('processing_lag_exceeded')
    if record.get('cache_age') is not None and not 0<=float(record['cache_age'])<=1:raise ValueError('cache_age_exceeded')
    body=json.loads(record['response_body'])
    if not isinstance(body,dict):raise ValueError('response_object_required')
    record['source_hash']=digest({k:v for k,v in record.items() if k not in ('valid','validation_error','processing_lag_ms','source_hash')})
    if record['kind']!='book':return body
    normalized=fast._book({'received_at':iso(receipt),'rest_date':iso(source),'source_hash':record['source_hash'],'orderbook_fp':body.get('orderbook_fp')},datetime.fromtimestamp(receipt,timezone.utc),'1hz_book')
    return {'received_at':receipt,'source_hash':record['source_hash'],**{k:normalized[k] for k in ['yes_bids','yes_asks','no_bids','no_asks']}}

_SOURCE_CACHE={}
def source_signature(path):
    files=[path,Path(str(path)+'-wal'),Path(str(path)+'-journal')]
    result={}
    for f in files:
        if f.is_symlink():raise ValueError('source database sidecar symlink refused')
        if f.exists():
            st=f.stat();result[str(f)]=(st.st_ino,st.st_size,st.st_mtime_ns)
    if sum(x[1] for x in result.values())>128*1024*1024:raise ValueError('source snapshot exceeds read budget')
    return result

def source_connection(path,*,force_snapshot=False):
    """Read WAL safely on a read-only mount without creating source sidecars.

    SQLite can require shared-memory initialization even for mode=ro. On that
    specific failure recover a byte-stable private DB+WAL+journal copy. Reuse a
    copied snapshot only while original inode/size/nanosecond-mtime signatures
    are unchanged. Never use immutable=1 on an active WAL database.
    """
    signature=source_signature(path);cached=_SOURCE_CACHE.get(str(path))
    if cached and cached[0]==signature:return cached[2],False
    if cached:cached[2].close();cached[1].cleanup();del _SOURCE_CACHE[str(path)]
    con=None
    try:
        if force_snapshot:
            exc=sqlite3.OperationalError('private snapshot requested');exc.sqlite_errorcode=sqlite3.SQLITE_CANTOPEN;raise exc
        con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.1)
        con.execute('pragma query_only=ON');con.execute('select name from sqlite_master limit 1').fetchone()
        return con,True
    except sqlite3.OperationalError as exc:
        if con:con.close()
        if getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_READONLY,sqlite3.SQLITE_CANTOPEN,sqlite3.SQLITE_IOERR):raise
    for attempt in range(3):
        temporary=tempfile.TemporaryDirectory(prefix='neorome-bracket-source-')
        try:
            before=source_signature(path)
            for filename in before:shutil.copyfile(filename,Path(temporary.name)/Path(filename).name)
            if before!=source_signature(path):temporary.cleanup();continue
            copy=sqlite3.connect(Path(temporary.name)/path.name,timeout=.1)
            copy.execute('select name from sqlite_master limit 1').fetchone();copy.execute('pragma query_only=ON')
            while len(_SOURCE_CACHE)>=4:
                _,old=_SOURCE_CACHE.popitem();old[2].close();old[1].cleanup()
            _SOURCE_CACHE[str(path)]=(before,temporary,copy)
            return copy,False
        except BaseException:temporary.cleanup();raise
    raise ValueError('source changed during every snapshot copy')

def source_fill_count(path,started_at):
    """Count source fills with the same bounded readonly-snapshot retry as cohort reads."""
    for attempt in range(2):
        con,owned=source_connection(path,force_snapshot=bool(attempt))
        try:
            con.execute('pragma query_only=ON')
            return con.execute('select count(*) from fills where julianday(filled_at)>=julianday(?)',(iso(started_at),)).fetchone()[0]
        except sqlite3.OperationalError as exc:
            if attempt or not owned or getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_READONLY,sqlite3.SQLITE_CANTOPEN,sqlite3.SQLITE_IOERR):raise
        finally:
            if owned:con.close()
    raise ValueError('source fill count unavailable')

def source_cohorts(path,timeframe,started_at,now,after_rowid=0,*,clock=None):
    """Only new, promptly observed original fills create shared prospective cohorts."""
    if path.is_symlink() or not path.is_file():raise ValueError('source_ledger_missing_or_symlink')
    for attempt in range(2):
        con,owned=source_connection(path,force_snapshot=bool(attempt));con.row_factory=sqlite3.Row
        try:
            con.execute('pragma query_only=ON')
            rows=con.execute("select f.rowid as source_rowid,f.*,p.payload from fills f join proposals p using(proposal_id) where f.rowid>? and julianday(f.filled_at)>=julianday(?) order by f.rowid limit 501",(after_rowid,iso(started_at))).fetchall()
            break
        except sqlite3.OperationalError as exc:
            if attempt or not owned or getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_READONLY,sqlite3.SQLITE_CANTOPEN,sqlite3.SQLITE_IOERR):raise
        finally:
            if owned:con.close()
    if len(rows)>500:raise RuntimeError("source_cohort_backlog_limit")
    output=[]
    module=fast if timeframe=='5m' else slow
    for row in rows:
        f=dict(row);p=json.loads(f.pop('payload'));meta=p.get('metadata',{}).get(module._MARKER)
        if not isinstance(meta,dict) or meta.get('strategy_id')!=module.STRATEGY_ID:raise ValueError('source_strategy_mismatch')
        filled=stamp(f['filled_at']);signal_time=meta['signal']['bar_close_timestamp']
        # Revalidate admitted signal and contract from immutable source evidence.
        # The source stores its validated normalized signal (without the input-only valid flag).
        module._signal({**meta['signal'],'valid':True},meta['market'],datetime.fromtimestamp(filled,timezone.utc))
        module._market(meta['market'],now=datetime.fromtimestamp(filled,timezone.utc))
        source_hash=digest({'fill':f,'proposal':p})
        cohort={'cohort_id':timeframe+':'+f['fill_id'],'timeframe':timeframe,'ticker':f['instrument'],'side':meta['signal']['contract_side'],
                'observed_at':now,'signal_time':float(signal_time),'close_time':stamp(meta['market']['close_time']),
                'fee_multiplier':meta['fee']['fee_multiplier'],'source_hash':source_hash}
        publication_error=None
        try:publication=source_publication(path,f,now,clock=clock)
        except ValueError as exc:publication=None;publication_error=str(exc)[:160]
        evidence={'fill':f,'proposal':p,'new_cohort':cohort}
        if publication is not None:evidence['publication']=publication
        if publication_error is not None:evidence['publication_error']=publication_error
        output.append((cohort,evidence,filled))
    return output

def source_publication(path,fill,now,*,clock=None):
    """Validate the immutable receipt written immediately after the source DB commit."""
    proposal_id=fill.get('proposal_id')
    if not isinstance(proposal_id,str) or not proposal_id:raise ValueError('source_fill_proposal_id_invalid')
    event_id='fill-publication-'+proposal_id
    audit=Path(path).parent/'audit'
    target=audit/(hashlib.sha256(event_id.encode('utf-8')).hexdigest()+'.json')
    if Path(path).parent.is_symlink() or audit.is_symlink() or target.is_symlink():raise ValueError('source_publication_symlink_refused')
    if not target.exists():return None
    if not target.is_file() or target.stat().st_size>65536:raise ValueError('source_publication_receipt_invalid')
    try:event=json.loads(target.read_text())
    except (OSError,json.JSONDecodeError) as exc:raise ValueError('source_publication_receipt_invalid') from exc
    if not isinstance(event,dict):raise ValueError('source_publication_receipt_invalid')
    body={k:event.get(k) for k in ('event_id','event_type','at','payload')}
    if (not isinstance(event,dict) or set(event)!={'event_id','event_type','at','payload','event_sha256'}
            or event.get('event_id')!=event_id or event.get('event_type')!='fill_publication'
            or event.get('event_sha256')!=digest(body)):
        raise ValueError('source_publication_receipt_integrity_failed')
    payload=event.get('payload')
    expected_fill={k:v for k,v in fill.items() if k!='source_rowid'}
    receipt_fill=payload.get('fill') if isinstance(payload,dict) else None
    if (not isinstance(payload,dict) or set(payload)!={'fill','published_at','capture_wall_ns','capture_monotonic_ns'}
            or not isinstance(receipt_fill,dict) or set(receipt_fill)!=set(expected_fill)|{'execution_mode'}
            or any(receipt_fill.get(k)!=v for k,v in expected_fill.items())
            or receipt_fill.get('proposal_id')!=proposal_id or receipt_fill.get('execution_mode')!='internal_simulation'):
        raise ValueError('source_publication_fill_mismatch')
    published=payload.get('published_at');wall=payload.get('capture_wall_ns');mono=payload.get('capture_monotonic_ns')
    if not isinstance(published,str) or not published.endswith('Z') or not isinstance(event.get('at'),str):
        raise ValueError('source_publication_time_invalid')
    try:published_time=stamp(published);event_time=stamp(event['at'])
    except (TypeError,ValueError,OverflowError) as exc:raise ValueError('source_publication_time_invalid') from exc
    if (not isinstance(wall,int) or isinstance(wall,bool) or not 0<wall<=2**63-1
            or not isinstance(mono,int) or isinstance(mono,bool) or not 0<mono<=2**63-1
            or abs(wall/1e9-published_time)>0.000002 or published_time!=event_time):
        raise ValueError('source_publication_clock_incoherent')
    filled=stamp(fill['filled_at'])
    observed_at=max(now,clock()) if clock is not None else now
    if published_time<filled or published_time>observed_at:raise ValueError('source_publication_clock_out_of_range')
    return {'event_id':event_id,'published_at':published,'capture_wall_ns':wall,'capture_monotonic_ns':mono,
            'event_sha256':event['event_sha256'],'observed_at':observed_at,'receipt':event}

def entry_health(now):
    if GLOBAL_STOP.exists():return 'global_manual_stop'
    try:
        recovery=json.loads(RECOVERY.read_text());age=now-stamp(recovery['updated_at'])
        if recovery.get('operational_healthy') is not True or not 0<=age<=7200:return 'recovery_not_verified'
    except (OSError,ValueError,KeyError,TypeError):return 'recovery_status_unavailable'
    return None

class Driver:
    def __init__(self,root,engine,client=None,*,data_only=False):
        self.root=Path(root);self.engine=engine;self.client=client or PublicClient();self.archive=Archive(root);self.data_only=data_only
        self.config=json.loads((self.root/'config.json').read_text());self.started_at=self.config['started_at']
        self.cursor=self.root/'source-cursors.json';self.seen=json.loads(self.cursor.read_text()) if self.cursor.exists() else {}
        self.coverage_policy_path=self.root/'coverage-policy.json';self.coverage_policy=None
        self.pending_publications=self.seen.setdefault('_pending_publications',{})
        self.budget_path=self.root/'request-budget.json';self.budget=json.loads(self.budget_path.read_text()) if self.budget_path.exists() else {'day':datetime.now(timezone.utc).date().isoformat(),'requests':0}
        self.request_count=0;self.valid_count=0;self.invalid_count=0;self.exceptions=collections.Counter();self.recent=collections.deque(maxlen=120)
        self.last_quote={};self.last_valid_quote={};self.intervals=collections.deque(maxlen=3600);self.valid_intervals=collections.deque(maxlen=3600);self.engine_quote_count=0;self.last_discovery=0;self.current_market=None;self.markets={};self.last_meta={};self.backoff_until=0;self.last_blocker='__initial__'
        self.pool=concurrent.futures.ThreadPoolExecutor(max_workers=3);self.last_status=0;self.last_budget_save=0
    def reserve_requests(self,n):
        day=datetime.now(timezone.utc).date().isoformat()
        if self.budget['day']!=day:self.budget={'day':day,'requests':0}
        if self.budget['requests']+n>MAX_REQUESTS_PER_DAY:raise RuntimeError('daily_public_request_budget')
        # Reserve before I/O, persist once for each batch; restart never refills a used allowance.
        self.budget['requests']+=n;atomic(self.budget_path,self.budget);self.request_count+=n
    def process(self,record):
        record['processing_started_monotonic_ns']=time.monotonic_ns();record['processing_lag_ms']=(record['processing_started_monotonic_ns']-record['capture_monotonic_ns'])/1e6
        decoded=None
        try:decoded=decode(record);record['valid']=True;self.valid_count+=1
        except (ValueError,TypeError,KeyError,fast.BTCStochasticPaperError) as e:
            record['valid']=False;record['validation_error']=str(e)[:180];self.invalid_count+=1;self.exceptions[record['validation_error']]+=1
        if record.get('http_status')==429:
            try:delay=max(5,min(300,float(record.get('retry_after') or 10)))
            except ValueError:delay=10
            self.backoff_until=time.time()+delay
        self.archive.append(record)
        if record['kind']=='book':
            ticker=record['ticker'];receipt=record['capture_wall_ns']/1e9
            previous=self.last_quote.get(ticker)
            if previous is not None:
                interval=(record['capture_monotonic_ns']-previous)/1e9;self.intervals.append(interval)
            self.last_quote[ticker]=record['capture_monotonic_ns']
            self.recent.append({k:record.get(k) for k in ['ticker','capture_wall_ns','capture_monotonic_ns','request_duration_ms','http_date_capture_delta_ms','processing_lag_ms','valid','validation_error','body_sha256','source_hash']})
            if decoded is not None:
                # Durably archive first, then recheck actual processing age before paper decisions.
                if time.time()-receipt>MAX_PROCESSING_LAG_SECONDS:
                    self.exceptions['archive_processing_lag_exceeded']+=1;return
                if not self.data_only:self.engine.on_quote(ticker,decoded);self.engine_quote_count+=1
                prior_valid=self.last_valid_quote.get(ticker)
                if prior_valid is not None:self.valid_intervals.append((record['capture_monotonic_ns']-prior_valid)/1e9)
                self.last_valid_quote[ticker]=record['capture_monotonic_ns']
        elif decoded is not None and record['kind']=='markets':
            now=record['capture_wall_ns']/1e9
            eligible=[m for m in decoded.get('markets',[]) if m.get('exchange_index')==2 and TICKER.fullmatch(m.get('ticker','')) and stamp(m['open_time'])<=now<stamp(m['close_time'])]
            if len(eligible)==1:
                candidate=eligible[0];known=self.markets.get(candidate['ticker'])
                if known and stamp(candidate['close_time'])!=stamp(known['close_time']):raise ValueError('discovered_market_close_mismatch')
                self.current_market=candidate;self.markets[candidate['ticker']]=candidate
            else:self.exceptions['exact_current_market_not_discovered']+=1
        elif decoded is not None and record['kind']=='market':
            m=decoded.get('market',{});ticker=record['ticker'];known=self.markets.get(ticker)
            if m.get('ticker')!=ticker or m.get('exchange_index')!=2:raise ValueError('official_market_identity_mismatch')
            if known and stamp(m['close_time'])!=stamp(known['close_time']):raise ValueError('official_market_close_mismatch')
            if m.get('status') in ('settled','finalized') and m.get('result') in ('yes','no'):
                if not known:raise ValueError('official_market_close_not_bound')
                if record['capture_wall_ns']/1e9<stamp(m['close_time']):raise ValueError('official_resolution_before_close')
                if not self.data_only:self.engine.on_settlement(ticker,m['result'],record['capture_wall_ns']/1e9,record['source_hash'])
            self.markets[ticker]=m
    def cohorts(self,now,blocker):
        if self.data_only:return
        policy=self._coverage_policy()
        effective=stamp(policy['effective_at'])
        changed=False
        watermarks=self.seen.setdefault("_watermarks",{})
        for timeframe,path in SOURCES.items():
            prior_watermark=watermarks.get(timeframe,0);candidate=prior_watermark;pending_barrier=False
            for cohort,evidence,filled in source_cohorts(path,timeframe,self.started_at,now,prior_watermark,
                    clock=lambda:max(now,time.time())):
                key=cohort['cohort_id'];fingerprint=cohort['source_hash']
                rowid=evidence['fill']['source_rowid']
                if key in self.seen:
                    if self.seen[key]['source_hash']!=fingerprint:raise ValueError('source_cohort_changed')
                    if not pending_barrier:candidate=rowid
                    continue
                publication=evidence.get('publication');publication_error=evidence.get('publication_error')
                if publication_error is not None:
                    self.pending_publications.pop(key,None)
                    coverage_expected=cohort['signal_time']+90>=effective
                    reason='source_publication_invalid'
                elif publication is None:
                    is_repair_fill=cohort['signal_time']+90>=effective
                    if not is_repair_fill:
                        # Do not seed pre-activation source history into the repaired cohort set.
                        self.seen[key]={'source_hash':fingerprint,'reason':'before_coverage_policy','observed_at':cohort['observed_at'],
                                        'coverage_expected':False}
                        if not pending_barrier:candidate=rowid
                        changed=True
                        continue
                    if is_repair_fill and now<=cohort['signal_time']+90:
                        self.pending_publications[key]={'timeframe':timeframe,'source_rowid':rowid,'source_hash':fingerprint,
                            'first_seen_at':self.pending_publications.get(key,{}).get('first_seen_at',now),
                            'deadline':cohort['signal_time']+90}
                        pending_barrier=True;changed=True
                        continue
                    self.pending_publications.pop(key,None)
                    reason='fill_publication_missing_expired' if is_repair_fill else 'legacy_fill_missing_publication_receipt'
                    publication_time=None;coverage_expected=is_repair_fill
                else:
                    self.pending_publications.pop(key,None)
                    publication_time=stamp(publication['published_at'])
                    cohort['observed_at']=publication['observed_at']
                    if publication_time<effective:
                        # Receipt predates the repair activation; leave historical records untouched.
                        self.seen[key]={'source_hash':fingerprint,'reason':'before_coverage_policy','observed_at':cohort['observed_at'],
                                        'coverage_expected':False}
                        if not pending_barrier:candidate=rowid
                        changed=True
                        continue
                    coverage_expected=True
                    observed_at=publication['observed_at']
                    reason=blocker
                    if not reason and not 0<=observed_at-publication_time<=5:reason='source_fill_observed_late'
                    if not reason and not 0<=observed_at-filled:reason='source_fill_clock_future'
                    if not reason and observed_at+2>cohort['signal_time']+90:reason='entry_window_too_short'
                if reason is None:reason=blocker
                directory=self.root/'cohorts';directory.mkdir(mode=0o700,exist_ok=True)
                target=directory/(hashlib.sha256(key.encode()).hexdigest()+'.json')
                if target.is_symlink():raise ValueError('cohort symlink refused')
                evidence['admission_blocker']=reason
                evidence['coverage_expected']=coverage_expected
                if target.exists():
                    # Archive-before-engine crash recovery keeps the original decision clock.
                    prior=json.loads(target.read_text());cohort=prior['new_cohort'];reason=prior['admission_blocker']
                else:atomic(target,evidence)
                bound=evidence['proposal']['metadata'][fast._MARKER if timeframe=='5m' else slow._MARKER]['market']
                known=self.markets.get(cohort['ticker'])
                if known and stamp(known['close_time'])!=stamp(bound['close_time']):raise ValueError('source_cohort_close_conflict')
                if not reason:self.engine.register_cohort(cohort)
                else:self.exceptions[reason]+=1
                self.markets[cohort['ticker']]=bound
                self.seen[key]={'source_hash':fingerprint,'reason':reason,'observed_at':cohort['observed_at'],
                    'coverage_expected':coverage_expected,'publication_at':None if publication is None else publication['published_at']}
                if not pending_barrier:candidate=rowid
                changed=True
            if candidate!=prior_watermark:watermarks[timeframe]=candidate;changed=True
        if changed:atomic(self.cursor,self.seen)
    def _coverage_policy(self):
        path=self.coverage_policy_path
        if path.is_symlink():raise ValueError('coverage_policy_symlink_refused')
        if not path.is_file() or path.stat().st_size>4096:raise ValueError('coverage_policy_missing_or_invalid')
        try:value=json.loads(path.read_text())
        except (OSError,json.JSONDecodeError) as exc:raise ValueError('coverage_policy_missing_or_invalid') from exc
        if (not isinstance(value,dict) or set(value)!={'protocol','effective_at'}
                or value.get('protocol')!=PUBLICATION_PROTOCOL):raise ValueError('coverage_policy_invalid')
        if not isinstance(value['effective_at'],str) or not value['effective_at'].endswith('Z'):raise ValueError('coverage_policy_invalid')
        try:effective=stamp(value['effective_at'])
        except (TypeError,ValueError,OverflowError) as exc:raise ValueError('coverage_policy_invalid') from exc
        if not math.isfinite(effective):raise ValueError('coverage_policy_invalid')
        if self.coverage_policy is not None and value!=self.coverage_policy:raise ValueError('coverage_policy_changed_while_running')
        self.coverage_policy=value
        return value
    def coverage_status(self):
        policy=self._coverage_policy();expected={}
        for key,value in self.seen.items():
            if key.startswith('_') or not isinstance(value,dict) or not value.get('coverage_expected'):continue
            expected[key]=value
        for key,value in self.pending_publications.items():
            if value.get('source_rowid') is not None and key not in expected:
                expected[key]={'reason':'fill_publication_pending','coverage_expected':True}
        arm_ids=[f'{tf}-hold' if tp is None else f'{tf}-tp{tp}-sl{sl}'
                 for tf in ('5m','30m') for tp,sl in ((None,None),(10,10),(10,20),(20,20),(20,10),(30,10),(30,30),(40,10),(40,40),(50,20),(50,50))]
        events={};global_events={}
        db=getattr(self.engine,'db',None)
        if db is not None:
            for row in db.execute("select cohort_id,arm_id,kind,payload from events where cohort_id is not null and kind in ('entry','skip')"):
                target=events if row['arm_id'] is not None else global_events
                key=(row['cohort_id'],row['arm_id']) if row['arm_id'] is not None else row['cohort_id']
                target.setdefault(key,[]).append((row['kind'],json.loads(row['payload'])))
        items=[];admitted=missed=pending_publication=admitted_awaiting_execution=0
        for key,decision in sorted(expected.items()):
            reason=decision.get('reason')
            if reason=='fill_publication_pending':
                pending_publication+=1;arm_decisions=[{'arm_id':arm,'status':'pending','reason':reason} for arm in arm_ids if arm.startswith(key.split(':',1)[0]+'-')]
            elif reason:
                missed+=1;arm_decisions=[{'arm_id':arm,'status':'blocked','reason':reason} for arm in arm_ids if arm.startswith(key.split(':',1)[0]+'-')]
            else:
                arm_decisions=[];cohort_pending=False
                for arm in arm_ids:
                    if not arm.startswith(key.split(':',1)[0]+'-'):continue
                    rows=events.get((key,arm),[])
                    global_rows=global_events.get(key,[])
                    if any(kind=='skip' for kind,_ in global_rows):arm_decisions.append({'arm_id':arm,'status':'blocked','reason':global_rows[-1][1].get('reason','skipped')})
                    elif any(kind=='entry' for kind,_ in rows):arm_decisions.append({'arm_id':arm,'status':'entered','reason':None})
                    elif rows:arm_decisions.append({'arm_id':arm,'status':'blocked','reason':rows[-1][1].get('reason','skipped')})
                    else:arm_decisions.append({'arm_id':arm,'status':'pending','reason':'awaiting_two_distinct_quotes'});cohort_pending=True
                admitted+=1
                if cohort_pending:admitted_awaiting_execution+=1
            items.append({'cohort_id':key,'reason':reason,'arm_decisions':arm_decisions})
        historical=[]
        for key,decision in sorted(self.seen.items()):
            if key.startswith('_') or not isinstance(decision,dict) or decision.get('coverage_expected') or not decision.get('reason'):continue
            if decision['reason']=='before_coverage_policy':continue
            timeframe=key.split(':',1)[0]
            arm_decisions=[{'arm_id':arm,'status':'blocked','reason':decision['reason']} for arm in arm_ids if arm.startswith(timeframe+'-')]
            historical.append({'cohort_id':key,'reason':decision['reason'],'arm_decisions':arm_decisions})
        source_counts={}
        for timeframe,path in SOURCES.items():
            source_counts[timeframe]=source_fill_count(path,self.started_at)
        seen_counts={tf:sum(1 for key in self.seen if key.startswith(tf+':')) for tf in SOURCES}
        for key in self.pending_publications:
            tf=self.pending_publications[key].get('timeframe')
            if tf in seen_counts and key not in self.seen:seen_counts[tf]+=1
        unclassified={tf:max(0,source_counts[tf]-seen_counts[tf]) for tf in SOURCES}
        admission_reconciled=(admitted+missed+pending_publication==len(expected))
        reconciled=admission_reconciled and all(source_counts[tf]==seen_counts[tf] for tf in SOURCES)
        return {'protocol':policy['protocol'],'effective_at':policy['effective_at'],
                'original_source_fills_since_experiment_start':source_counts,
                'source_fills_decided':seen_counts,'unclassified_source_fills':unclassified,
                'expected_source_cohorts':len(expected),'admitted':admitted,'missed':missed,
                'pending_publication':pending_publication,'pending':pending_publication,
                'admitted_awaiting_execution':admitted_awaiting_execution,'admission_reconciled':admission_reconciled,'reconciled':reconciled,'cohorts':items,
                'historical_missed':len(historical),'historical_missed_cohorts':historical}
    def tick(self):
        now=time.time();blocker='data_only_preflight' if self.data_only else entry_health(now)
        if now<self.backoff_until:blocker='public_rate_limit_backoff'
        if blocker!=self.last_blocker:
            if not self.data_only:self.engine.set_entry_blocker(blocker,now)
            self.last_blocker=blocker
        if not self.data_only:self.cohorts(now,blocker);self.engine.expire(now)
        jobs=[]
        if now>=self.backoff_until:
            if now-self.last_discovery>=15:
                jobs.append(('markets',None));self.last_discovery=now
            tickers=set(self.engine.tickers())
            if self.current_market and stamp(self.current_market['close_time'])>now:tickers.add(self.current_market['ticker'])
            if len(tickers)>3:raise RuntimeError('too_many_active_tickers')
            for ticker in sorted(tickers):
                market=self.markets.get(ticker)
                # After restart recover static market identity from registered source evidence.
                if market is None:
                    for file in (self.root/'cohorts').glob('*.json'):
                        ev=json.loads(file.read_text());cohort=ev['new_cohort']
                        if cohort['ticker']==ticker:self.markets[ticker]=market={'ticker':ticker,'close_time':iso(cohort['close_time'])};break
                if not market or stamp(market['close_time'])>now:jobs.append(('book',ticker))
                if ticker in self.engine.tickers() and now-self.last_meta.get(ticker,0)>=10:
                    jobs.append(('market',ticker));self.last_meta[ticker]=now
            if jobs:
                self.reserve_requests(len(jobs));futures=[self.pool.submit(self.client.request,k,t) for k,t in jobs]
                # Process in actual receipt order, not submission order. Each HTTP task is bounded.
                for future in concurrent.futures.as_completed(futures):self.process(future.result())
        if now-self.last_status>=5:self.publish();self.last_status=now
    def publish(self):
        now=time.time();state=self.engine.status(now)
        value={'mode':'internal-paper','experiment':'BTC-BRACKETS-1HZ-v1','as_of':iso(now),'started_at':iso(self.started_at),'target_cadence_seconds':1,
               'source':'public Kalshi REST orderbook, exchange_index=2','venue_orders_sent':0,'live_execution_enabled':False,
               'request_budget':self.budget,'engine_accepted_quotes_since_restart':self.engine_quote_count,'request_count_since_restart':self.request_count,'valid_responses_since_restart':self.valid_count,'invalid_responses_since_restart':self.invalid_count,
               'exceptions':dict(self.exceptions),'entry_blocker':None if self.last_blocker=='__initial__' else self.last_blocker,
               'cadence':{'samples':len(self.intervals),'median_seconds':statistics.median(self.intervals) if self.intervals else None,'p95_seconds':sorted(self.intervals)[int(.95*(len(self.intervals)-1))] if self.intervals else None,'max_seconds':max(self.intervals) if self.intervals else None},
               'valid_quote_cadence':{'samples':len(self.valid_intervals),'median_seconds':statistics.median(self.valid_intervals) if self.valid_intervals else None,'p95_seconds':sorted(self.valid_intervals)[int(.95*(len(self.valid_intervals)-1))] if self.valid_intervals else None,'max_seconds':max(self.valid_intervals) if self.valid_intervals else None},
               'archive_bytes_last_scan':self.archive.bytes,'retention_seconds':RETENTION_SECONDS,'recent_quotes':list(self.recent),'comparison':state}
        if not self.data_only:value['source_fill_coverage']=self.coverage_status()
        atomic(self.root/'status.json',value)
        # Browser-native comparison, independent of the SDK-only legacy dashboard.
        def money(n):return '—' if n is None else f'${n/100:,.2f}'
        rows=''
        for arm in state.get('arms',[]):
            nav=arm['available_cash_cents'] if arm['position'] is None else arm['available_cash_cents']+arm['latest_mark_cents'] if arm.get('mark_fresh') else None
            values=[arm['arm_id'],money(arm['available_cash_cents']),money(arm['open_cost_cents']),money(nav),money(arm['realized_pnl_cents']),arm['trades'],arm['wins'],arm['losses']]
            rows+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in values)+'</tr>'
        cadence=value['valid_quote_cadence']['median_seconds']
        coverage=value.get('source_fill_coverage')
        coverage_html=''
        if coverage:
            reasons=collections.Counter(x['reason'] for x in coverage['cohorts'] if x.get('reason'))
            reason_text=', '.join(f'{html.escape(k)}: {v}' for k,v in sorted(reasons.items())) or 'none'
            coverage_html=('<p>Source fills since start: '+html.escape(canonical(coverage['original_source_fills_since_experiment_start']))+
                ' | repair cohorts expected/admitted/missed/pending: '+str(coverage['expected_source_cohorts'])+'/'+str(coverage['admitted'])+'/'+
                str(coverage['missed'])+'/'+str(coverage['pending_publication'])+' | admitted awaiting arm decisions: '+
                str(coverage['admitted_awaiting_execution'])+' | historical missed: '+str(coverage['historical_missed'])+
                ' | unclassified source fills: '+html.escape(canonical(coverage['unclassified_source_fills']))+
                ' | admission reasons: '+reason_text+'</p>')
        body='<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="5"><title>BTC bracket paper comparison</title><style>body{font:15px system-ui;background:#f6f8fb;color:#18243b;padding:25px}td,th{padding:8px;text-align:right;border-bottom:1px solid #ddd}table{background:white;border-collapse:collapse}th:first-child,td:first-child{text-align:left}</style><h1>BTC bracket paper comparison</h1><p>As of '+iso(now)+' | 1-second target | independent alternative accounts initially $500 each | 20% entry size including costs</p><p>Valid quote interval median: '+str(cadence)+'s | entry blocker: '+html.escape(str(value['entry_blocker']))+'</p>'+coverage_html+'<p>NAV uses fresh net liquidation marks; — means a current complete mark is unavailable. Accounts are alternative experiments, not additive capital.</p><table><tr><th>Arm</th><th>Cash</th><th>Open cost</th><th>Fresh NAV</th><th>Realized P&amp;L</th><th>Trades</th><th>Wins</th><th>Losses</th></tr>'+rows+'</table>'
        p=self.root/'comparison.html';temp=p.with_suffix('.tmp');temp.write_text(body);temp.chmod(0o600);temp.replace(p)
    def close(self):self.archive.close();self.pool.shutdown(wait=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--seconds',type=float);actions=parser.add_mutually_exclusive_group();actions.add_argument('--initialize',action='store_true');actions.add_argument('--migrate-allocation',action='store_true');parser.add_argument('--data-only',action='store_true');args=parser.parse_args()
    root=args.root
    if args.data_only and root.resolve(strict=False)==ROOT.resolve(strict=False):raise ValueError('data-only mode requires a separate preflight state root')
    if root.is_symlink():raise ValueError('state root symlink refused')
    root.mkdir(parents=True,mode=0o700,exist_ok=True)
    with (root/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.migrate_allocation:print(canonical(migrate_allocation(root)));return
        config_path=root/'config.json'
        if config_path.exists():
            config=json.loads(config_path.read_text())
            if config.get('data_only',False)!=args.data_only:raise ValueError('preflight/paper state mode mismatch')
            if config.get('allocation_bps')!=ALLOCATION_BPS or config.get('allocation_policy_version')!=ALLOCATION_POLICY_VERSION:raise ValueError('explicit allocation migration required before resume')
        else:
            config={'version':'BTC-BRACKETS-1HZ-v1','started_at':time.time(),'initial_cash_cents_each':50000,'allocation_bps':ALLOCATION_BPS,'allocation_policy_version':ALLOCATION_POLICY_VERSION,'source_cohorts':'new original crossover fills, observed prospectively','target_cadence_seconds':1,'data_only':args.data_only}
            atomic(config_path,config)
        from .btc_bracket_engine import Engine
        engine=Engine(root/'comparison.sqlite3',started_at=config['started_at'])
        driver=Driver(root,engine,data_only=args.data_only)
        if args.initialize:driver.publish();print(canonical(engine.status(time.time())));driver.close();return
        stop=threading.Event()
        signal.signal(signal.SIGTERM,lambda *a:stop.set());signal.signal(signal.SIGINT,lambda *a:stop.set())
        deadline=time.monotonic()+args.seconds if args.seconds is not None else math.inf;next_tick=time.monotonic()
        try:
            while not stop.is_set() and time.monotonic()<deadline:
                try:driver.tick()
                except Exception as e:
                    driver.exceptions[type(e).__name__+': '+str(e)[:140]]+=1
                    engine.set_entry_blocker('runtime_error',time.time());driver.last_blocker='runtime_error';driver.publish()
                    # Disk/config/state integrity failures fail closed; never loop through them.
                    if isinstance(e,(sqlite3.DatabaseError,OSError,RuntimeError)):raise
                next_tick+=1
                if next_tick<time.monotonic():next_tick=time.monotonic()+1 # Preserve gaps; no catch-up burst.
                stop.wait(max(0,next_tick-time.monotonic()))
        finally:driver.publish();driver.close()
if __name__=='__main__':main()
