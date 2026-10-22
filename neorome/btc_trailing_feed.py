"""Two prospective trailing-paper books consuming the existing 1 Hz archive.

Only the separate trailing state root is writable. Sources and shared quotes are
read-only; network requests are bounded public GETs for official settlements.
"""
from __future__ import annotations

import argparse,collections,fcntl,html,json,math,os,signal,statistics,threading,time,zlib
from datetime import datetime,timezone
from pathlib import Path
from . import btc_bracket_feed as shared

ROOT=Path('/var/lib/neorome-btc-trailing-v1')
SHARED=Path('/var/lib/neorome-btc-brackets-v1')
VERSION='BTC-TRAILING-A50-D50-v1'
MAX_SETTLEMENT_REQUESTS_PER_DAY=18_000


class GzipFollower:
    """Incrementally read flushed gzip members, including an unfinished member."""
    def __init__(self,root):
        self.root=Path(root);self.readers={}
        if self.root.is_symlink():raise ValueError('shared quote directory symlink refused')

    def poll(self,now):
        slot=int(now//300)
        paths=[self.root/(datetime.fromtimestamp(s*300,timezone.utc).strftime('%Y%m%dT%H%MZ')+'.jsonl.gz') for s in (slot-1,slot)]
        for old in list(self.readers):
            if old not in paths:del self.readers[old]
        output=[]
        for path in paths:
            if not path.exists():continue
            if path.is_symlink() or not path.is_file():raise ValueError('shared quote archive path refused')
            st=path.stat()
            if st.st_size>8_000_000:raise RuntimeError('shared quote archive size limit')
            state=self.readers.get(path)
            if state is None:
                state={'inode':st.st_ino,'offset':0,'decoder':zlib.decompressobj(16+zlib.MAX_WBITS),'pending':b''}
                self.readers[path]=state
            if st.st_ino!=state['inode'] or st.st_size<state['offset']:raise RuntimeError('shared quote archive replaced or truncated')
            with path.open('rb') as stream:
                stream.seek(state['offset'])
                while True:
                    compressed=stream.read(262144)
                    if not compressed:break
                    state['offset']+=len(compressed)
                    while compressed:
                        if state['decoder'].eof:state['decoder']=zlib.decompressobj(16+zlib.MAX_WBITS)
                        decoded=state['decoder'].decompress(compressed,16_000_000)
                        if state['decoder'].unconsumed_tail:raise RuntimeError('shared quote decompression limit')
                        compressed=state['decoder'].unused_data
                        state['pending']+=decoded
                        while b'\n' in state['pending']:
                            line,state['pending']=state['pending'].split(b'\n',1)
                            if len(line)>1_000_000:raise RuntimeError('shared quote record size limit')
                            if line:output.append((json.loads(line),str(path)))
                        if len(state['pending'])>1_000_000:raise RuntimeError('shared quote partial record size limit')
        return sorted(output,key=lambda item:item[0]['capture_wall_ns'])


class TrailingDriver(shared.Driver):
    def __init__(self,root,engine,client=None,*,archive_root=SHARED/'quotes',data_only=False):
        super().__init__(root,engine,client,data_only=data_only)
        self.follower=GzipFollower(archive_root)
        self.inflight={};self.last_control=0
        self.consumer_lags=collections.deque(maxlen=3600)
        self.shared_skipped=collections.Counter();self.shared_records=0

    def reserve_requests(self,n):
        day=datetime.now(timezone.utc).date().isoformat()
        if self.budget['day']!=day:self.budget={'day':day,'requests':0}
        if self.budget['requests']+n>MAX_SETTLEMENT_REQUESTS_PER_DAY:raise RuntimeError('settlement_public_request_budget')
        self.budget['requests']+=n;shared.atomic(self.budget_path,self.budget);self.request_count+=n

    def consume(self,record,path=None,*,now=None):
        now=time.time() if now is None else now
        receipt=record['capture_wall_ns']/1e9
        if receipt<self.started_at:
            self.shared_skipped['before_experiment_start']+=1;return
        kind=record.get('kind');ticker=record.get('ticker')
        if kind not in ('book','market'):return
        if not isinstance(ticker,str) or not shared.TICKER.fullmatch(ticker):raise ValueError('shared BTC ticker identity refused')
        if record.get('http_status')==429:
            try:delay=max(5,min(300,float(record.get('retry_after') or 10)))
            except ValueError:delay=10
            self.backoff_until=time.time()+delay
        if path is not None and record.get('valid') is not True:
            self.shared_skipped['upstream_rejected']+=1;return
        lag=now-receipt
        if not 0<=lag<=shared.MAX_PROCESSING_LAG_SECONDS:
            self.shared_skipped['consumer_lag_exceeded']+=1;return
        # Keep the original clock fields, so its source hash remains identical.
        prior_hash=record.get('source_hash')
        try:
            decoded=shared.decode(record,now)
            if prior_hash is not None and record['source_hash']!=prior_hash:raise ValueError('shared_source_hash_changed')
        except (ValueError,KeyError,TypeError,shared.fast.BTCStochasticPaperError) as exc:
            self.invalid_count+=1;self.exceptions[str(exc)[:180]]+=1;return
        if kind=='book':
            # Only the shared collector supplies books; this runner has no book
            # request path and cannot manufacture historical paper executions.
            if path is None:raise ValueError('books must come from the shared archive')
            previous=self.engine.db.execute('select received from quotes where ticker=?',(ticker,)).fetchone()
            if previous and receipt<=previous['received']:
                self.shared_skipped['duplicate_or_out_of_order']+=1;return
            self.archive.append(record)
            if time.time()-receipt>shared.MAX_PROCESSING_LAG_SECONDS:
                self.shared_skipped['archive_processing_lag_exceeded']+=1;return
            if not self.data_only:self.engine.on_quote(ticker,decoded)
            self.engine_quote_count+=1;self.valid_count+=1;self.shared_records+=1
            self.consumer_lags.append(lag)
            prior=self.last_valid_quote.get(ticker)
            if prior is not None:self.valid_intervals.append(receipt-prior)
            self.last_valid_quote[ticker]=receipt
            self.recent.append({'ticker':ticker,'capture_wall_ns':record['capture_wall_ns'],'source_hash':record['source_hash'],'consumer_lag_ms':lag*1000,'archive':path})
        else:
            known=self.markets.get(ticker)
            if known is None:return
            m=decoded.get('market',{})
            if (m.get('ticker')!=ticker or m.get('exchange_index')!=2
                    or shared.stamp(m['close_time'])!=shared.stamp(known['close_time'])):
                raise ValueError('official_market_identity_or_close_mismatch')
            self.archive.append(record)
            if m.get('status') in ('settled','finalized') and m.get('result') in ('yes','no'):
                if receipt<shared.stamp(m['close_time']):raise ValueError('official_resolution_before_close')
                if not self.data_only:self.engine.on_settlement(ticker,m['result'],receipt,record['source_hash'])
            self.markets[ticker]=m

    def coverage_status(self):
        policy=self._coverage_policy();items=[]
        arm_ids={r['timeframe']:r['arm_id'] for r in self.engine.db.execute('select timeframe,arm_id from arms')}
        expected={k:v for k,v in self.seen.items() if not k.startswith('_') and isinstance(v,dict) and v.get('coverage_expected')}
        for k,v in self.pending_publications.items():expected.setdefault(k,{'reason':'fill_publication_pending'})
        for key,decision in sorted(expected.items()):
            reason=decision.get('reason');arm=arm_ids[key.split(':',1)[0]]
            rows=self.engine.db.execute("select kind,payload from events where cohort_id=? and (arm_id=? or arm_id is null) and kind in ('entry','skip') order by event_id",(key,arm)).fetchall()
            if reason:status='pending' if reason=='fill_publication_pending' else 'blocked'
            elif any(r['kind']=='entry' for r in rows):status='entered'
            elif rows:status='blocked';reason=json.loads(rows[-1]['payload']).get('reason')
            else:status='pending';reason='awaiting_two_distinct_quotes'
            items.append({'cohort_id':key,'arm_id':arm,'status':status,'reason':reason})
        counts={tf:shared.source_fill_count(p,self.started_at) for tf,p in shared.SOURCES.items()}
        classified={tf:len({k for k in list(self.seen)+list(self.pending_publications) if k.startswith(tf+':')}) for tf in shared.SOURCES}
        return {'protocol':policy['protocol'],'effective_at':policy['effective_at'],'original_source_fills_since_experiment_start':counts,'source_fills_decided':classified,'reconciled':counts==classified,'unclassified_source_fills':{tf:max(0,counts[tf]-classified[tf]) for tf in counts},'cohorts':items,'entered':sum(v['status']=='entered' for v in items),'blocked':sum(v['status']=='blocked' for v in items),'pending':sum(v['status']=='pending' for v in items)}

    def tick(self):
        now=time.time()
        for rec,path in self.follower.poll(now):self.consume(rec,path)
        for ticker,future in list(self.inflight.items()):
            if future.done():
                del self.inflight[ticker];record=future.result()
                record['processing_started_monotonic_ns']=time.monotonic_ns()
                record['processing_lag_ms']=(record['processing_started_monotonic_ns']-record['capture_monotonic_ns'])/1e6
                self.consume(record)
        now=time.time()
        if now-self.last_control>=.5:
            blocker='data_only_preflight' if self.data_only else shared.entry_health(now)
            if blocker!=self.last_blocker:
                if not self.data_only:self.engine.set_entry_blocker(blocker,now)
                self.last_blocker=blocker
            if not self.data_only:self.cohorts(now,blocker);self.engine.expire(now)
            self.last_control=now
            # Source evidence binds every contract's identity. Restore these
            # static bounds after a restart, without creating old entries.
            for c in self.engine.db.execute('select ticker,close_time from cohorts'):
                self.markets.setdefault(c['ticker'],{'ticker':c['ticker'],'close_time':shared.iso(c['close_time'])})
            for ticker in self.engine.tickers():
                m=self.markets.get(ticker)
                if m and now>=self.backoff_until and shared.stamp(m['close_time'])<=now and now-self.last_meta.get(ticker,0)>=10 and ticker not in self.inflight:
                    self.reserve_requests(1);self.inflight[ticker]=self.pool.submit(self.client.request,'market',ticker);self.last_meta[ticker]=now
        if now-self.last_status>=5:self.publish();self.last_status=now

    def publish(self):
        now=time.time();state=self.engine.status(now)
        lags=sorted(self.consumer_lags);intervals=sorted(self.valid_intervals)
        value={'mode':'internal-paper','experiment':VERSION,'as_of':shared.iso(now),'started_at':shared.iso(self.started_at),'initial_cash_cents_each':50000,'allocation_bps':2000,'activation_net_profit_pct':50,'trailing_drop_pct_of_peak_net_liquidation_value':50,'target_cadence_seconds':1,'quote_source':str(self.follower.root),'source':'original5m/30m fills; shared public Kalshi one-second books','venue_orders_sent':0,'live_execution_enabled':False,'entry_blocker':None if self.last_blocker=='__initial__' else self.last_blocker,'exceptions':dict(self.exceptions),'shared_records_since_restart':self.shared_records,'shared_skipped_since_restart':dict(self.shared_skipped),'engine_accepted_quotes_since_restart':self.engine_quote_count,'settlement_request_budget':self.budget,'consumer_lag_ms':{'samples':len(lags),'median':statistics.median(lags)*1000 if lags else None,'p95':lags[int(.95*(len(lags)-1))]*1000 if lags else None,'max':max(lags)*1000 if lags else None},'valid_quote_cadence':{'samples':len(intervals),'median_seconds':statistics.median(intervals) if intervals else None,'p95_seconds':intervals[int(.95*(len(intervals)-1))] if intervals else None,'max_seconds':max(intervals) if intervals else None},'recent_quotes':list(self.recent),'comparison':state}
        if not self.data_only:value['source_fill_coverage']=self.coverage_status()
        shared.atomic(self.root/'status.json',value)
        def money(n):return '—' if n is None else f'${n/100:,.2f}'
        rows=''
        for a in state['arms']:
            p=a['position'];nav=a['available_cash_cents']+(a['latest_mark_cents'] or 0) if p is None or a['mark_fresh'] else None
            values=[a['arm_id'],money(a['available_cash_cents']),money(a['open_cost_cents']),money(nav),money(a['realized_pnl_cents']),a['trades'],a['wins'],a['losses'],'flat' if p is None else 'exiting' if p.get('trailing_trigger') else 'armed' if p.get('trailing_activated_at') is not None else 'not armed']
            rows+='<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in values)+'</tr>'
        body='<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="5"><title>BTC trailing paper accounts</title><style>body{font:15px system-ui;padding:25px;background:#f6f8fb}td,th{padding:9px;border-bottom:1px solid #ddd;text-align:right}table{background:white;border-collapse:collapse}</style><h1>BTC +50% activation / 50% trailing paper</h1><p>As of '+html.escape(value['as_of'])+' | $500 each initially | 20% own available cash including costs | shared one-second quotes</p><p>Activate when net liquidation value reaches 150% of entry cost; exit after a 50% fall from peak net value, on a later quote. Activation does not guarantee profit. Original, gap and TP/SL accounts unchanged. Alternative experiments are not additive firm capital.</p><p>Entry blocker: '+html.escape(str(value['entry_blocker']))+'</p><table><tr><th>Account</th><th>Cash</th><th>Open cost</th><th>Fresh NAV</th><th>Realized P&amp;L</th><th>Trades</th><th>Wins</th><th>Losses</th><th>State</th></tr>'+rows+'</table>'
        p=self.root/'comparison.html';temp=p.with_suffix('.tmp');temp.write_text(body);temp.chmod(0o600);temp.replace(p)


def validate_root(root):
    root=Path(root)
    protected=[SHARED,*[p.parent for p in shared.SOURCES.values()],Path('/var/lib/neorome-btc-gap5-fast-paper'),Path('/var/lib/neorome-btc-gap5-slow-paper')]
    resolved=root.resolve()
    if root.is_symlink() or any(resolved==p.resolve() or p.resolve() in resolved.parents or resolved in p.resolve().parents for p in protected):raise ValueError('existing account state roots refused')


def initialize(root):
    root=Path(root);validate_root(root)
    root.mkdir(parents=True,mode=0o700,exist_ok=True)
    path=root/'config.json'
    if path.is_symlink():raise ValueError('config symlink refused')
    expected={'version':VERSION,'initial_cash_cents_each':50000,'allocation_bps':2000,'activation_net_profit_pct':50,'trailing_drop_pct_of_peak_net_liquidation_value':50,'target_cadence_seconds':1,'quote_source':str(SHARED/'quotes'),'source_cohorts':'prospective original5m/30m fills only'}
    if path.exists():
        config=json.loads(path.read_text());start=config.get('started_at')
        if isinstance(start,bool) or not isinstance(start,(int,float)) or not math.isfinite(start) or config!={**expected,'started_at':start}:raise ValueError('frozen trailing configuration mismatch')
    else:
        config={**expected,'started_at':time.time()};shared.atomic(path,config)
    policy={'protocol':shared.PUBLICATION_PROTOCOL,'effective_at':shared.iso(config['started_at']).replace('+00:00','Z')}
    p=root/'coverage-policy.json'
    if p.is_symlink():raise ValueError('coverage policy symlink refused')
    if p.exists():
        if json.loads(p.read_text())!=policy:raise ValueError('frozen trailing coverage policy mismatch')
    else:shared.atomic(p,policy)
    return config


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,default=ROOT);parser.add_argument('--seconds',type=float);parser.add_argument('--initialize',action='store_true');args=parser.parse_args()
    validate_root(args.root)
    args.root.mkdir(parents=True,mode=0o700,exist_ok=True)
    lock_path=args.root/'runner.lock'
    if lock_path.is_symlink():raise ValueError('runner lock symlink refused')
    with lock_path.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);config=initialize(args.root)
        from .btc_trailing_engine import TrailingEngine
        engine=TrailingEngine(args.root/'trailing.sqlite3',started_at=config['started_at']);driver=TrailingDriver(args.root,engine)
        if args.initialize:driver.publish();print(shared.canonical(engine.status(time.time())));driver.close();engine.close();return
        stop=threading.Event();signal.signal(signal.SIGTERM,lambda *a:stop.set());signal.signal(signal.SIGINT,lambda *a:stop.set())
        deadline=time.monotonic()+args.seconds if args.seconds is not None else math.inf
        try:
            while not stop.is_set() and time.monotonic()<deadline:
                try:driver.tick()
                except Exception as exc:
                    driver.exceptions[type(exc).__name__+': '+str(exc)[:140]]+=1
                    engine.set_entry_blocker('runtime_error',time.time());driver.last_blocker='runtime_error';driver.publish()
                    raise
                stop.wait(.05)
        finally:driver.publish();driver.close();engine.close()

if __name__=='__main__':main()
