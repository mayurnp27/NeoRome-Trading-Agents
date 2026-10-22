"""Separate 54-account paper extension; original services and ledgers stay read-only."""
from __future__ import annotations
import argparse,asyncio,fcntl,json,math,signal,sqlite3,threading,time
from pathlib import Path
from . import btc_sma1m_feed as sma
from . import btc_bracket_feed as shared
from . import tradingview_stoch15_source as source
from .btc_sma1m_engine import SMAEngine
from .btc_interval_expansion_signal import evaluate
from .btc_interval_expansion_engine import ExpansionEngine

ROOT=Path('/var/lib/neorome-btc-interval-expansion-v1')
SOURCE_ROOT=Path('/var/lib/neorome-btc-sma1m-v1')
VERSION='BTC-15M-AND-SMA-BRACKETS-v1'
SOURCE_ARMS={'1m-sma2-5-hold':'1m-sma-brackets','1m-sma2-5-stoch-hold':'1m-sma-stoch-brackets','1m-sma2-5-reversal':'1m-sma-reversal-brackets','1m-sma2-5-stoch-reversal':'1m-sma-stoch-reversal-brackets'}

def chart_and_fee():
    chart=asyncio.run(source.fetch_bars())
    # Same reviewed fixed public series route/fee parser as the existing source.
    con=sma.http.client.HTTPSConnection(shared.HOST,timeout=2)
    try:
        con.request('GET',shared.API+'/series/KXBTC15M',headers={'Accept':'application/json','Cache-Control':'no-cache','User-Agent':'NeoRome-Interval-Paper/1.0'})
        r=con.getresponse();body=r.read(200001);receipt=time.time();date=r.getheader('Date');age=r.getheader('Age')
        if len(body)>200000 or r.status!=200 or not date:raise ValueError('series response invalid')
        if not 0<=receipt-sma.parsedate_to_datetime(date).timestamp()<=15 or age is not None and not 0<=float(age)<=15:raise ValueError('series response stale')
        raw={'received_at':shared.iso(receipt),'http_date':date,'cache_age':age,'body':body.decode(),'body_sha256':sma.hashlib.sha256(body).hexdigest()};raw['source_hash']=shared.digest(raw)
        s=json.loads(body)['series']
        if s.get('ticker')!='KXBTC15M' or s.get('exchange_index')!=2:raise ValueError('series identity changed')
        fee=shared.fast._fee({'fee_type':s.get('fee_type'),'fee_multiplier':s.get('fee_multiplier'),'source_hash':raw['source_hash']})
        return {'chart':chart,'series':raw,'fee':fee}
    except Exception as exc:return {'chart':chart,'series_error':type(exc).__name__+': '+str(exc)[:150]}
    finally:con.close()

def source_rows(path,started_at,after_id):
    """Bounded read-only joined query, with the existing private-WAL fallback."""
    path=Path(path)
    if path.is_symlink() or not path.is_file():raise ValueError('SMA source missing or symlink')
    for attempt in range(2):
        owned=False
        if attempt:con,owned=shared.source_connection(path,force_snapshot=True)
        else:con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.1);owned=True
        try:
            con.row_factory=sqlite3.Row;con.execute('pragma query_only=ON');con.execute('BEGIN')
            config=json.loads(con.execute("select value from meta where key='config'").fetchone()[0])
            if config!=SMAEngine.CONFIG:raise ValueError('SMA source frozen engine identity differs')
            entries=[dict(r) for r in con.execute("select e.*,c.payload as cohort_payload from events e join cohorts c using(cohort_id) where e.kind='entry' and e.event_id>? and e.at>=? order by e.event_id limit 501",(after_id,started_at))]
            cutoff=int(time.time()//60)*60-60
            decisions=[dict(r) for r in con.execute('select target,payload from signal_decisions where target>=? and applied=1 and payload is not null order by target limit 3',(cutoff,))]
            con.rollback()
            if len(entries)>500:raise ValueError('SMA source entry backlog exceeds bound')
            return entries,decisions
        except sqlite3.OperationalError as exc:
            if attempt or getattr(exc,'sqlite_errorcode',0)&255 not in (sqlite3.SQLITE_READONLY,sqlite3.SQLITE_CANTOPEN,sqlite3.SQLITE_IOERR):raise
        finally:
            if owned:con.close()
    raise ValueError('SMA source query unavailable')

class ExpansionDriver(sma.SMADriver):
    def __init__(self,root,engine,client=None,*,archive_root=sma.trailing.SHARED/'quotes',source_path=SOURCE_ROOT/'sma.sqlite3',data_only=False):
        super().__init__(root,engine,client,archive_root=archive_root,data_only=data_only)
        self.source_path=Path(source_path);self.last_source_error=None;self.source_reads=0;self.last_source_read=0;self.last_contract_wait=None
        engine.db.execute('create table if not exists source_decisions(source_key text primary key, observed real not null,payload text not null,applied integer not null default 0)')
        self._recover_sources()

    def _recover_sources(self):
        for row in self.engine.db.execute('select * from source_decisions where applied=0 order by observed,source_key limit 1000').fetchall():
            p=json.loads(row['payload'])
            if p.get('reversal'):self.engine.on_reversal(**p['reversal'])
            if p.get('cohort'):self.engine.register_cohort(p['cohort'])
            for cohort in p.get('cohorts',[]):self.engine.register_cohort(cohort)
            self.engine.expire(time.time())
            self.engine.db.execute('update source_decisions set applied=1 where source_key=?',(row['source_key'],))

    def _save_source(self,key,now,payload):
        old=self.engine.db.execute('select applied from source_decisions where source_key=?',(key,)).fetchone()
        if old is not None:
            if not old['applied']:self._recover_sources()
            return False
        with self.engine._tx():self.engine.db.execute('insert into source_decisions(source_key,observed,payload) values(?,?,?)',(key,now,shared.canonical(payload)))
        self._recover_sources();return True

    def source_cohorts(self,now,blocker):
        if now-self.last_source_read<.5:return
        self.last_source_read=now
        cursor=self.engine.db.execute("select value from meta where key='source_entry_cursor'").fetchone();after=int(cursor[0]) if cursor else 0
        try:
            entries,decisions=source_rows(self.source_path,self.started_at,after);now=time.time();self.source_reads+=1;self.last_source_error=None
            for row in decisions:
                p=json.loads(row['payload']);target=row['target']
                if not self.started_at<=target<=now<=target+30:continue
                for rev in p.get('reversals',[]):
                    reversal=dict(rev,observed_at=now,source_hash=shared.digest({'source_reversal':rev,'known_at':now}))
                    payload={'source_target':target,'source_reversal':rev,'reversal':None if self.data_only else reversal,'reason':'data_only_preflight' if self.data_only else None}
                    self._save_source('reversal:'+str(target),now,payload)
            for e in entries:
                group=SOURCE_ARMS.get(e['arm_id']);c=json.loads(e['cohort_payload']);reason=blocker or ('data_only_preflight' if self.data_only else None)
                if not group:raise ValueError('unknown source account identity')
                if not self.started_at<=e['at']<=now or c['side'] not in ('yes','no'):raise ValueError('invalid source entry timing or side')
                if now>=min(c['signal_time']+30,c['close_time']):reason=reason or 'original_signal_window_expired'
                cohort=None
                if not reason:
                    cohort={**c,'cohort_id':group+':source-entry-'+str(e['event_id']),'timeframe':group,'observed_at':now,'source_hash':shared.digest({'source_entry':e,'known_at':now})}
                self._save_source('entry:'+str(e['event_id']),now,{'source_entry':e,'group':group,'reason':reason,'cohort':cohort})
                with self.engine._tx():self.engine.db.execute("insert into meta(key,value) values('source_entry_cursor',?) on conflict(key) do update set value=excluded.value",(str(e['event_id']),))
        except (ValueError,TypeError,KeyError,sqlite3.Error,OSError) as exc:
            self.last_source_error=type(exc).__name__+': '+str(exc)[:150]

    def local_comparisons(self,now,blocker):
        rows=self.engine.db.execute("select e.*,c.payload as cohort_payload from events e join cohorts c using(cohort_id) where e.kind='entry' and e.arm_id='15m-original-hold' and e.at>=? and not exists(select 1 from source_decisions s where s.source_key='15m-entry:'||e.event_id) order by e.event_id limit 100",(self.started_at,)).fetchall()
        for row in rows:
            e=dict(row);c=json.loads(e['cohort_payload']);now=time.time();reason=blocker or ('data_only_preflight' if self.data_only else None)
            if now>=min(c['signal_time']+90,c['close_time']):reason=reason or 'original_signal_window_expired'
            cohorts=[]
            if not reason:
                for group in ('15m-comparison','15m-trailing'):
                    cohorts.append({**c,'cohort_id':group+':original-entry-'+str(e['event_id']),'timeframe':group,'observed_at':now,'source_hash':shared.digest({'original_15m_entry':e,'known_at':now,'group':group})})
            self._save_source('15m-entry:'+str(e['event_id']),now,{'original_15m_entry':e,'cohorts':cohorts,'reason':reason})

    def cohorts(self,now,blocker):
        self.source_cohorts(now,blocker)
        self.local_comparisons(time.time(),blocker)
        now=time.time()
        if self.chart_future is not None and self.chart_future.done():
            target=self.chart_target;future=self.chart_future;self.chart_future=None
            decision={'target_close':target,'observed_at':now,'cohorts':[]}
            try:
                result=future.result();chart=result['chart'];meta=chart['source_metadata'];received=shared.stamp(meta['received_at'])
                if not 0<=now-received<=15 or chart.get('symbol')!='COINBASE:BTCUSD' or chart.get('timeframe')!='15m':raise ValueError('15m chart profile or freshness differs')
                if chart['bars'][-1]['bar_close_timestamp']!=target:raise ValueError('latest completed 15m bar absent')
                sig=evaluate(chart['bars'],observed_at=now,target_close=target);decision.update(source=result,signal=sig);self.last_signal=sig;self.last_signal_error=None
                decision['market_diagnostic']=self.decision_market_diagnostic(target,now,meta)
                reason=blocker or result.get('series_error') or ('data_only_preflight' if self.data_only else None)
                m=self.current_market
                if not reason and (m is None or not 0<=now-self.current_market_received<=30):reason='current_market_not_fresh'
                if not reason and (shared.stamp(m['open_time'])!=target or shared.stamp(m['close_time'])!=target+900):reason='15m_signal_not_at_contract_open'
                decision['admission_blocker']=reason
                if not reason and sig['qualifies']:
                    groups=['15m-original']+(['15m-gap5'] if sig['gap_qualifies'] else [])
                    for group in groups:decision['cohorts'].append({'cohort_id':group+':'+str(target),'timeframe':group,'ticker':m['ticker'],'side':sig['direction'],'observed_at':now,'signal_time':float(target),'close_time':target+900.,'fee_multiplier':result['fee']['fee_multiplier'],'source_hash':shared.digest({'signal':sig,'chart':meta['source_response_sha256'],'market':m,'fee':result['fee']})})
            except Exception as exc:self.last_signal_error=type(exc).__name__+': '+str(exc)[:160];decision['error']=self.last_signal_error;self.exceptions[self.last_signal_error]+=1
            with self.engine._tx():self.engine.db.execute('update signal_decisions set observed=?,payload=? where target=?',(now,shared.canonical(decision),target))
            self._recover_decisions()
        target=int(now//900)*900
        # Data-only preflight may inspect the current closed 15m bar outside its
        # trading admission window. It still cannot create an entry or reversal.
        if self.chart_future is not None or not self.data_only and (target<self.started_at or not 3<=now-target<=45):return
        if self.engine.db.execute('select 1 from signal_decisions where target=?',(target,)).fetchone():return
        # Do not spend the one decision for this target on the previous
        # interval's still-fresh metadata. Wait within the same3..45s request
        # window; chart/candle/admission checks below remain authoritative.
        if not self.data_only:
            m=self.current_market
            reason=None
            if m is None or not 0<=now-self.current_market_received<=30:reason='current_market_not_fresh'
            elif shared.stamp(m['open_time'])!=target or shared.stamp(m['close_time'])!=target+900:reason='15m_signal_not_at_contract_open'
            if reason:
                self.last_contract_wait={'target_close':target,'observed_at':now,'reason':reason}
                return
        self.last_contract_wait=None
        self.reserve_requests(2)
        with self.engine._tx():self.engine.db.execute('insert into signal_decisions(target) values(?)',(target,))
        self.chart_target=target;self.chart_future=self.pool.submit(chart_and_fee)

    def coverage_status(self):
        rows=[json.loads(r[0]) for r in self.engine.db.execute('select payload from source_decisions')]
        entries=[p for p in rows if 'source_entry' in p]
        return {'pending_source_intents':self.engine.db.execute('select count(*) from source_decisions where applied=0').fetchone()[0],'sma_source_entries_classified':len(entries),'sma_entry_cohorts_admitted':sum(p.get('cohort') is not None for p in entries),'sma_entries_blocked':[{'source_event_id':p['source_entry']['event_id'],'group':p['group'],'reason':p.get('reason')} for p in entries if p.get('reason')],'original_15m_entries_classified':sum('original_15m_entry' in p for p in rows),'original_15m_comparison_cohorts_admitted':sum(len(p.get('cohorts',[])) for p in rows),'original_15m_entries_blocked':[{'original_event_id':p['original_15m_entry']['event_id'],'reason':p.get('reason')} for p in rows if 'original_15m_entry' in p and p.get('reason')],'reversal_decisions':sum('source_reversal' in p for p in rows),'cohort_outcomes':[dict(r) for r in self.engine.db.execute('select timeframe,state,count(*) as count from cohorts group by timeframe,state')],'entry_events':self.engine.db.execute("select count(*) from events where kind='entry'").fetchone()[0],'skip_events':self.engine.db.execute("select count(*) from events where kind='skip'").fetchone()[0]}

    def publish(self):
        now=time.time();value={'mode':'internal-paper','experiment':VERSION,'as_of':shared.iso(now),'started_at':shared.iso(self.started_at),'new_accounts':54,'existing_accounts_unchanged':32,'allocation_bps':2000,'initial_cash_cents_each':50000,'venue_orders_sent':0,'live_execution_enabled':False,'quote_source':str(self.follower.root),'source_ledger':str(self.source_path),'engine_accepted_quotes_since_restart':self.engine_quote_count,'consumer_lag_ms':{'median':__import__('statistics').median(self.consumer_lags)*1000 if self.consumer_lags else None,'max':max(self.consumer_lags)*1000 if self.consumer_lags else None},'exceptions':dict(self.exceptions),'entry_blocker':None if self.last_blocker=='__initial__' else self.last_blocker,'last_15m_contract_wait':self.last_contract_wait,'last_15m_signal':self.last_signal,'last_15m_signal_error':self.last_signal_error,'last_sma_source_error':self.last_source_error,'market_discovery':self.market_discovery_status(),'source_reads':self.source_reads,'request_budget':self.budget,'coverage':self.coverage_status(),'comparison':self.engine.status(now)}
        shared.atomic(self.root/'status.json',value)
        with self.engine._tx():self.engine.db.execute("delete from signal_decisions where target<? and applied=1 and target not in (select cast(substr(cohort_id,instr(cohort_id,':')+1) as integer) from cohorts where timeframe like '15m-%')",(int(now)-3*86400,))
        if self.engine.path.stat().st_size>2*1024**3:raise RuntimeError('expansion evidence storage limit')

def validate_root(root):
    sma.validate_root(root);r=Path(root).resolve();p=SOURCE_ROOT.resolve()
    if r==p or p in r.parents or r in p.parents:raise ValueError('existing SMA state root refused')

def initialize(root,*,data_only=False):
    root=Path(root);validate_root(root);root.mkdir(mode=0o700,parents=True,exist_ok=True);p=root/'config.json'
    expected={'version':VERSION,'new_accounts':54,'initial_cash_cents_each':50000,'allocation_bps':2000,'include_reversal_brackets':True,'chart_timeframe_minutes':15,'stochastic':[14,3,3],'source_ledger':str(SOURCE_ROOT/'sma.sqlite3'),'data_only':data_only}
    if p.is_symlink():raise ValueError('expansion config symlink refused')
    if p.exists():
        c=json.loads(p.read_text());start=c.get('started_at')
        if isinstance(start,bool) or not isinstance(start,(int,float)) or not math.isfinite(start) or c!={**expected,'started_at':start}:raise ValueError('frozen expansion configuration mismatch')
    else:c={**expected,'started_at':time.time()};shared.atomic(p,c)
    return c

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--seconds',type=float);parser.add_argument('--initialize',action='store_true');parser.add_argument('--data-only',action='store_true');args=parser.parse_args()
    validate_root(args.root)
    if args.data_only and args.root.resolve()==ROOT.resolve():raise ValueError('data-only requires separate root')
    args.root.mkdir(mode=0o700,parents=True,exist_ok=True);p=args.root/'runner.lock'
    if p.is_symlink():raise ValueError('expansion lock symlink refused')
    with p.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);c=initialize(args.root,data_only=args.data_only)
        engine=ExpansionEngine(args.root/'expansion.sqlite3',started_at=c['started_at'],include_reversal_brackets=True);d=ExpansionDriver(args.root,engine,data_only=args.data_only)
        if args.initialize:d.publish();print(shared.canonical(engine.status(time.time())));d.close();engine.close();return
        stop=threading.Event();signal.signal(signal.SIGTERM,lambda *a:stop.set());signal.signal(signal.SIGINT,lambda *a:stop.set());deadline=time.monotonic()+args.seconds if args.seconds is not None else math.inf
        try:
            while not stop.is_set() and time.monotonic()<deadline:
                try:d.tick()
                except Exception as exc:d.exceptions[type(exc).__name__+': '+str(exc)[:120]]+=1;engine.set_entry_blocker('runtime_error',time.time());d.last_blocker='runtime_error';d.publish();raise
                stop.wait(.05)
        finally:d.publish();d.close();engine.close()

if __name__=='__main__':main()
