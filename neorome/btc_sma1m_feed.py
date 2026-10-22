"""Separate one-minute SMA paper books; no venue order or credential interface."""
from __future__ import annotations
import argparse,asyncio,collections,fcntl,hashlib,http.client,json,math,signal,threading,time
from datetime import datetime,timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from . import btc_bracket_feed as shared
from . import btc_trailing_feed as trailing
from . import tradingview_sma1m_source as source
from .btc_sma1m_engine import SMAEngine
from .btc_sma1m_signal import evaluate

ROOT=Path('/var/lib/neorome-btc-sma1m-v1')
VERSION='BTC-SMA1M-2-5-ZONEEXIT-v1'
_COVERAGE_ABSENT=object()
_COVERAGE_KEYS=('completed_minute_decisions','pure_sma_signals','confirmed_zone_exit_signals','source_errors','admission_blocked','pending')

def chart_and_fee():
    """Bounded fixed public routes. No auth, order clients or redirects."""
    async def chart():return await source.fetch_bars()
    # Chart collection runs in a worker thread, leaving shared-book reception free.
    data=asyncio.run(chart())
    con=http.client.HTTPSConnection(shared.HOST,timeout=2)
    try:
        con.request('GET',shared.API+'/series/KXBTC15M',headers={'Accept':'application/json','Cache-Control':'no-cache','User-Agent':'NeoRome-SMA1m-Paper/1.0'})
        r=con.getresponse();body=r.read(200001);receipt=time.time();date=r.getheader('Date');age=r.getheader('Age')
        if len(body)>200000 or r.status!=200 or not date:raise ValueError('series response invalid')
        if not 0<=receipt-parsedate_to_datetime(date).timestamp()<=15 or age is not None and not 0<=float(age)<=15:raise ValueError('series response stale')
        raw={'received_at':shared.iso(receipt),'http_date':date,'cache_age':age,'body':body.decode(),'body_sha256':hashlib.sha256(body).hexdigest()};raw['source_hash']=shared.digest(raw)
        s=json.loads(body)['series']
        if s.get('ticker')!='KXBTC15M' or s.get('exchange_index')!=2:raise ValueError('series identity changed')
        fee=shared.fast._fee({'fee_type':s.get('fee_type'),'fee_multiplier':s.get('fee_multiplier'),'source_hash':raw['source_hash']})
        return {'chart':data,'series':raw,'fee':fee}
    except Exception as exc:return {'chart':data,'series_error':type(exc).__name__+': '+str(exc)[:120]}
    finally:con.close()

class SMADriver(trailing.TrailingDriver):
    def __init__(self,root,engine,client=None,*,archive_root=trailing.SHARED/'quotes',data_only=False):
        super().__init__(root,engine,client,archive_root=archive_root,data_only=data_only)
        engine.db.execute('create table if not exists signal_decisions(target integer primary key,observed real,payload text,applied integer not null default 0)')
        self.chart_future=None;self.chart_target=None;self.current_market=None;self.current_market_received=0
        self.market_discovery_counts=collections.Counter();self.latest_market_discovery=None
        self.last_market_discovery_reject_reason=None
        self._decision_coverage_cache=None
        self.last_signal=None;self.last_signal_error=None
        self._recover_decisions()

    @staticmethod
    def _decision_coverage_delta(payload):
        delta={key:0 for key in _COVERAGE_KEYS}
        if payload is _COVERAGE_ABSENT:return delta
        if payload is None:
            delta['pending']=1
            return delta
        p=json.loads(payload) if isinstance(payload,(str,bytes,bytearray)) else payload
        delta['completed_minute_decisions']=1
        delta['source_errors']=int(bool(p.get('error')))
        delta['admission_blocked']=int(bool(p.get('admission_blocker')))
        signal=p.get('signal',{})
        delta['pure_sma_signals']=int(bool(signal.get('pure_qualifies')))
        delta['confirmed_zone_exit_signals']=int(bool(signal.get('confirmed_qualifies')))
        return delta

    def _ensure_decision_coverage_cache(self):
        if self._decision_coverage_cache is not None:return
        counts={key:0 for key in _COVERAGE_KEYS}
        cursor=self.engine.db.execute('select payload from signal_decisions')
        while True:
            rows=cursor.fetchmany(64)
            if not rows:break
            for row in rows:
                delta=self._decision_coverage_delta(row['payload'])
                for key,value in delta.items():counts[key]+=value
        self._decision_coverage_cache=counts

    def _change_decision_coverage(self,old_payload,new_payload):
        if self._decision_coverage_cache is None:return
        before=self._decision_coverage_delta(old_payload);after=self._decision_coverage_delta(new_payload)
        for key in _COVERAGE_KEYS:self._decision_coverage_cache[key]+=after[key]-before[key]

    def _save_decision(self,target,observed,decision):
        payload=shared.canonical(decision)
        row=self.engine.db.execute('select payload from signal_decisions where target=?',(target,)).fetchone()
        old_payload=row['payload'] if row is not None else _COVERAGE_ABSENT
        with self.engine._tx():
            self.engine.db.execute('update signal_decisions set observed=?,payload=? where target=?',(observed,payload,target))
        self._change_decision_coverage(old_payload,payload)

    def _insert_pending_decision(self,target):
        with self.engine._tx():self.engine.db.execute('insert into signal_decisions(target) values(?)',(target,))
        self._change_decision_coverage(_COVERAGE_ABSENT,None)

    def _prune_decision_evidence(self,cutoff):
        where="target<? and applied=1 and target not in (select cast(substr(cohort_id,instr(cohort_id,':')+1) as integer) from cohorts)"
        removed=0
        while True:
            rows=self.engine.db.execute('select target,payload from signal_decisions where '+where+' order by target limit 64',(cutoff,)).fetchall()
            if not rows:break
            targets=[row['target'] for row in rows];marks=','.join('?' for _ in targets)
            with self.engine._tx():
                self.engine.db.execute('delete from signal_decisions where target in ('+marks+') and '+where,(*targets,cutoff))
            for row in rows:self._change_decision_coverage(row['payload'],_COVERAGE_ABSENT)
            removed+=len(rows)
        return removed

    def _record_market_discovery(self,reason,receipt,now,source_hash,*,ticker=None,open_time=None,close_time=None):
        lag=now-receipt
        self.market_discovery_counts[reason]+=1
        diagnostic={'reason':reason,'capture_time':shared.iso(receipt),'processing_time':shared.iso(now),
                    'processing_lag_seconds':lag,'source_hash':source_hash}
        if ticker is not None:diagnostic['ticker']=ticker
        if open_time is not None:diagnostic['open_time']=open_time
        if close_time is not None:diagnostic['close_time']=close_time
        self.latest_market_discovery=diagnostic
        if reason!='accepted':self.last_market_discovery_reject_reason=reason

    def market_discovery_status(self):
        return {'latest':self.latest_market_discovery,'reason_counts':dict(self.market_discovery_counts),
                'last_reject_reason':self.last_market_discovery_reject_reason}

    def decision_market_diagnostic(self,target,now,chart_meta):
        m=self.current_market
        selected=None
        if m is not None:
            opened=shared.stamp(m['open_time']);closed=shared.stamp(m['close_time'])
            selected={'ticker':m.get('ticker'),'open_time':m['open_time'],'close_time':m['close_time'],
                      'receipt_time':shared.iso(self.current_market_received),
                      'receipt_age_seconds':now-self.current_market_received,
                      'source_hash':m.get('source_hash')}
        chart_received=shared.stamp(chart_meta['received_at']) if chart_meta and chart_meta.get('received_at') else None
        return {'target_close':target,'selected_market':selected,
                'last_discovery':self.latest_market_discovery,
                'last_discovery_reject_reason':self.last_market_discovery_reject_reason,
                'chart_receipt_time':chart_meta.get('received_at') if chart_meta else None,
                'chart_receipt_age_seconds':now-chart_received if chart_received is not None else None,
                'open_minus_target_seconds':opened-target if m is not None else None,
                'close_minus_target_plus_900_seconds':closed-(target+900) if m is not None else None}

    def _recover_decisions(self):
        rows=self.engine.db.execute('select * from signal_decisions where applied=0 order by target limit 100').fetchall()
        for row in rows:
            if row['payload'] is None:
                decision={'target_close':row['target'],'error':'interrupted_chart_request','cohorts':[]}
                payload=shared.canonical(decision)
                self.engine.db.execute('update signal_decisions set payload=? where target=?',(payload,row['target']))
                self._change_decision_coverage(row['payload'],payload)
            else:decision=json.loads(row['payload'])
            for reversal in decision.get('reversals',[]):self.engine.on_reversal(**reversal)
            for cohort in decision.get('cohorts',[]):self.engine.register_cohort(cohort)
            self.engine.expire(time.time())
            self.engine.db.execute('update signal_decisions set applied=1 where target=?',(row['target'],))

    def consume(self,record,path=None,*,now=None):
        if record.get('kind')!='markets':return super().consume(record,path,now=now)
        now=time.time() if now is None else now;receipt=record['capture_wall_ns']/1e9
        old=record.get('source_hash');lag=now-receipt
        if receipt<self.started_at:self._record_market_discovery('before_start',receipt,now,old);return
        if record.get('valid') is not True:self._record_market_discovery('invalid_upstream',receipt,now,old);return
        if not 0<=lag<=.5:self._record_market_discovery('consumer_lag',receipt,now,old);return
        try:data=shared.decode(record,now)
        except Exception:
            self._record_market_discovery('validation_failure',receipt,now,old);raise
        if old!=record['source_hash']:
            self._record_market_discovery('hash_failure',receipt,now,old)
            raise ValueError('market discovery hash changed')
        candidates=[m for m in data.get('markets',[]) if m.get('exchange_index')==2 and shared.stamp(m['open_time'])<=receipt<shared.stamp(m['close_time'])]
        if not candidates:self._record_market_discovery('no_active_market',receipt,now,old);return
        if len(candidates)!=1:self._record_market_discovery('ambiguous_active_market',receipt,now,old);return
        m=shared.fast._market(dict(candidates[0],source_hash=record['source_hash']),now=datetime.fromtimestamp(receipt,timezone.utc))
        known=self.markets.get(m['ticker'])
        if known and shared.stamp(known['close_time'])!=shared.stamp(m['close_time']):
            self._record_market_discovery('market_close_conflict',receipt,now,old,ticker=m['ticker'],open_time=m['open_time'],close_time=m['close_time'])
            raise ValueError('market close conflict')
        self.markets[m['ticker']]=self.current_market=m;self.current_market_received=receipt
        self._record_market_discovery('accepted',receipt,now,old,ticker=m['ticker'],open_time=m['open_time'],close_time=m['close_time'])

    def cohorts(self,now,blocker):
        if self.chart_future is not None and self.chart_future.done():
            target=self.chart_target;future=self.chart_future;self.chart_future=None
            decision={'target_close':target,'observed_at':now,'cohorts':[]}
            try:
                result=future.result();chart=result['chart'];meta=chart['source_metadata']
                received=shared.stamp(meta['received_at'])
                if not 0<=now-received<=15:raise ValueError('chart processing stale')
                if chart.get('symbol')!='COINBASE:BTCUSD' or chart.get('timeframe')!='1m':raise ValueError('chart identity mismatch')
                sig=evaluate(chart['bars'],observed_at=now,target_close=target)
                decision.update(source=result,signal=sig);self.last_signal=sig;self.last_signal_error=None
                if not self.data_only and sig['pure_qualifies']:
                    decision['reversals']=[{'side':sig['direction'],'observed_at':now,'signal_time':float(target),'source_hash':shared.digest({'chart':meta['source_response_sha256'],'signal':sig})}]
                m=self.current_market
                decision['market_diagnostic']=self.decision_market_diagnostic(target,now,meta)
                reason=blocker or result.get('series_error') or ('data_only_preflight' if self.data_only else None)
                if not reason and (m is None or not 0<=now-self.current_market_received<=30):reason='current_market_not_fresh'
                if not reason and not shared.stamp(m['open_time'])<=target<shared.stamp(m['close_time']):reason='signal_outside_contract'
                decision['admission_blocker']=reason
                if not reason:
                    variants=[('1m-sma',sig['pure_qualifies'],sig['direction']),('1m-sma-stoch',sig['confirmed_qualifies'],sig.get('zone_exit_direction'))]
                    variants+=[(tf+'-reversal',q,s) for tf,q,s in variants[:]]
                    for tf,qualifies,side in variants:
                        if not qualifies:continue
                        c={'cohort_id':tf+':'+str(target),'timeframe':tf,'ticker':m['ticker'],'side':side,'observed_at':now,'signal_time':float(target),'close_time':shared.stamp(m['close_time']),'fee_multiplier':result['fee']['fee_multiplier'],'source_hash':shared.digest({'chart':meta['source_response_sha256'],'signal':sig,'market':m,'fee':result['fee']})}
                        decision['cohorts'].append(c)
            except Exception as exc:
                self.last_signal_error=type(exc).__name__+': '+str(exc)[:180];decision['error']=self.last_signal_error;self.exceptions[self.last_signal_error]+=1
            # Persist evidence before admitting the signal. Replay is idempotent
            # and an expired pending cohort can only be skipped on a restart.
            self._save_decision(target,now,decision)
            self._recover_decisions()
        target=int(now//60)*60
        if target<self.started_at or not 3<=now-target<=14 or self.chart_future is not None:return
        if self.engine.db.execute('select 1 from signal_decisions where target=?',(target,)).fetchone():return
        self.reserve_requests(2)
        self._insert_pending_decision(target)
        self.chart_target=target;self.chart_future=self.pool.submit(chart_and_fee)

    def tick(self):
        super().tick()
        # The inherited data-only runner skips cohort processing. This feature
        # still needs to validate its public candle source without admissions.
        if self.data_only:self.cohorts(time.time(),'data_only_preflight')

    def coverage_status(self):
        self._ensure_decision_coverage_cache()
        counts=dict(self._decision_coverage_cache)
        counts['cohorts']=[dict(r) for r in self.engine.db.execute('select cohort_id,timeframe,ticker,side,state from cohorts order by observed')]
        counts['entry_events']=self.engine.db.execute("select count(*) from events where kind='entry'").fetchone()[0]
        counts['skip_events']=self.engine.db.execute("select count(*) from events where kind='skip'").fetchone()[0]
        return counts

    def publish(self):
        now=time.time();state=self.engine.status(now)
        value={'mode':'internal-paper','experiment':VERSION,'as_of':shared.iso(now),'started_at':shared.iso(self.started_at),'allocation_bps':2000,'initial_cash_cents_each':50000,'venue_orders_sent':0,'live_execution_enabled':False,'quote_source':str(self.follower.root),'engine_accepted_quotes_since_restart':self.engine_quote_count,'consumer_lag_ms':{'median':__import__('statistics').median(self.consumer_lags)*1000 if self.consumer_lags else None,'max':max(self.consumer_lags)*1000 if self.consumer_lags else None},'exceptions':dict(self.exceptions),'entry_blocker':None if self.last_blocker=='__initial__' else self.last_blocker,'last_signal':self.last_signal,'last_signal_error':self.last_signal_error,'market_discovery':self.market_discovery_status(),'request_budget':self.budget,'signal_coverage':self.coverage_status(),'comparison':state}
        shared.atomic(self.root/'status.json',value)
        # Bound raw chart evidence and retain every trade/cohort accounting row.
        self._prune_decision_evidence(int(now)-3*86400)
        if self.engine.path.stat().st_size>2*1024**3:raise RuntimeError('SMA evidence storage limit')

def validate_root(root):
    root=Path(root);trailing.validate_root(root)
    old=trailing.ROOT.resolve();resolved=root.resolve()
    if resolved==old or old in resolved.parents or resolved in old.parents:raise ValueError('existing trailing root refused')

def initialize(root,*,data_only=False):
    root=Path(root);validate_root(root)
    root.mkdir(mode=0o700,parents=True,exist_ok=True);p=root/'config.json'
    if p.is_symlink():raise ValueError('SMA config symlink refused')
    expected={'version':VERSION,'allocation_bps':2000,'initial_cash_cents_each':50000,'exit_policies':['hold_official_settlement','opposite_sma_cross_or_settlement'],'sma_lengths':[2,5],'stochastic':[5,3,3],'stochastic_confirmation':'both_lines_extreme_then_KD_cross_then_both_exit_zone','sma_confirmation':'correct_side_on_zone_exit','data_only':data_only}
    if p.exists():
        c=json.loads(p.read_text());start=c.get('started_at')
        if isinstance(start,bool) or not isinstance(start,(int,float)) or not math.isfinite(start) or c!={**expected,'started_at':start}:raise ValueError('SMA config mismatch')
    else:c={**expected,'started_at':time.time()};shared.atomic(p,c)
    return c

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--seconds',type=float);parser.add_argument('--initialize',action='store_true');parser.add_argument('--data-only',action='store_true');args=parser.parse_args()
    # Validation precedes any file creation; only the new state root is writable
    # in the service sandbox as a second independent boundary.
    validate_root(args.root)
    if args.data_only and args.root.resolve()==ROOT.resolve():raise ValueError('data-only requires separate preflight root')
    args.root.mkdir(mode=0o700,parents=True,exist_ok=True);lockpath=args.root/'runner.lock'
    if lockpath.is_symlink():raise ValueError('SMA runner lock symlink refused')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        c=initialize(args.root,data_only=args.data_only)
        engine=SMAEngine(args.root/'sma.sqlite3',started_at=c['started_at']);d=SMADriver(args.root,engine,data_only=args.data_only)
        if args.initialize:d.publish();print(shared.canonical(engine.status(time.time())));d.close();engine.close();return
        stop=threading.Event();signal.signal(signal.SIGTERM,lambda *a:stop.set());signal.signal(signal.SIGINT,lambda *a:stop.set())
        deadline=time.monotonic()+args.seconds if args.seconds is not None else math.inf
        try:
            while not stop.is_set() and time.monotonic()<deadline:
                try:d.tick()
                except Exception as exc:
                    d.exceptions[type(exc).__name__+': '+str(exc)[:120]]+=1;engine.set_entry_blocker('runtime_error',time.time());d.last_blocker='runtime_error';d.publish();raise
                stop.wait(.05)
        finally:d.publish();d.close();engine.close()

if __name__=='__main__':main()
