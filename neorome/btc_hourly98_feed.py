"""Isolated dual-venue hourly Stoch98 paper runner. Fixed GET/market-stream routes only."""
from __future__ import annotations
import argparse,asyncio,collections,datetime as dt,fcntl,gzip,hashlib,json,os,re,signal,time,threading,http.client,statistics
from decimal import Decimal,ROUND_FLOOR
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.request import Request,HTTPRedirectHandler,build_opener
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo
import websockets
from .btc_hourly98_engine import Engine,canonical,digest,dec,CONFIG
from .btc_hourly98_signal import evaluate
from .tradingview_stoch98_source import fetch_bars

ROOT=Path('/var/lib/neorome-btc-hourly98-v1');AUTH=Path('/var/lib/neorome-btc-hourly98-auth/markets-ws.json')
STOP=Path('/var/lib/neorome-paper/STOP');RECOVERY=Path('/var/lib/neorome-paper-recovery/status.json')
UTC=dt.timezone.utc;KAPI='https://external-api.kalshi.com/trade-api/v2';PAPI='https://gateway.polymarket.us/v1'
PWS='wss://api.polymarket.us/v1/ws/markets';TICKER=re.compile(r'KXBTCD-[A-Z0-9.\-]+\Z')
_BUDGET=None
_HTTP_LOCAL=threading.local()

class Budget:
    def __init__(self,path):self.path=path;self.lock=threading.Lock();self.state=json.loads(path.read_text()) if path.exists() else {}
    def reserve(self):
        with self.lock:
            day=dt.datetime.now(UTC).date().isoformat()
            if self.state.get('day')!=day:self.state={'day':day,'requests':0}
            if self.state['requests']>=120000:raise RuntimeError('daily_public_request_budget')
            self.state['requests']+=1;tmp=self.path.with_suffix('.tmp')
            if self.path.is_symlink() or tmp.is_symlink():raise ValueError('budget symlink')
            tmp.write_text(canonical(self.state)+'\n');tmp.replace(self.path)

def iso(t):return dt.datetime.fromtimestamp(t,UTC).isoformat().replace('+00:00','Z')
def stamp(s):return dt.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def slug(hour):return 'btc-updown-1h-'+dt.datetime.fromtimestamp(hour,UTC).strftime('%Y-%m-%d-%H%M')+'z'
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None

def get(url,max_age=60,cache_max_age=None):
    if not any(url.startswith(p) for p in (KAPI+'/',PAPI+'/', 'https://api.exchange.coinbase.com/products/BTC-USD/candles?')):raise ValueError('unapproved public host/route')
    if _BUDGET is not None:_BUDGET.reserve()
    started=time.time();mono=time.monotonic_ns()
    parsed=urlsplit(url);connections=getattr(_HTTP_LOCAL,'connections',None)
    if connections is None:connections={};_HTTP_LOCAL.connections=connections
    connection=connections.get(parsed.netloc)
    if connection is None:connection=http.client.HTTPSConnection(parsed.netloc,timeout=5);connections[parsed.netloc]=connection
    try:
        connection.request('GET',parsed.path+('?' + parsed.query if parsed.query else ''),headers={'User-Agent':'NeoRome-hourly98-paper/1','Accept':'application/json','Cache-Control':'no-cache'})
        response=connection.getresponse()
        raw=response.read(4*2**20+1);at=time.time();date=response.headers.get('Date');age=response.headers.get('Age')
        if response.status!=200:raise ValueError('public HTTP status '+str(response.status))
        if len(raw)>4*2**20 or not date or not -1<=at-parsedate_to_datetime(date).timestamp()<=max_age:raise ValueError('public response clock/size invalid')
        if age is not None and not 0<=float(age)<=(max_age if cache_max_age is None else cache_max_age):raise ValueError('cached response too old')
        data=json.loads(raw)
        return {'url':url,'request_started':started,'received':at,'capture_monotonic_ns':time.monotonic_ns(),'rtt_ms':(time.monotonic_ns()-mono)/1e6,
                'http_date':date,'cache_age':age,'data':data,'body_sha256':hashlib.sha256(raw).hexdigest()}
    except BaseException:
        connection.close();connections.pop(parsed.netloc,None);raise

def choose_kalshi(markets,hour,reference):
    candidates=[]
    for m in markets:
        if m.get('exchange_index')!=2 or m.get('strike_type')!='greater' or not TICKER.fullmatch(m.get('ticker','')):continue
        if stamp(m['close_time'])!=hour+3600 or stamp(m['open_time'])>hour or m.get('status') not in ('active','open'):continue
        strike=dec(m['floor_strike'])
        if 'BRTI' not in m.get('rules_primary','') or strike<=0:continue
        candidates.append((abs(strike-reference),strike,m['ticker'],m))
    if not candidates:raise ValueError('no eligible hourly greater-than market')
    return min(candidates,key=lambda r:r[:3])[3]

def discover_kalshi(hour):
    close_local=dt.datetime.fromtimestamp(hour+3600,ZoneInfo('America/New_York'))
    event_ticker='KXBTCD-'+close_local.strftime('%y%b%d%H').upper()
    catalog=get(KAPI+'/events/'+event_ticker+'?with_nested_markets=true&exchange_index=2')
    event=catalog['data']['event']
    if event.get('event_ticker')!=event_ticker or event.get('series_ticker')!='KXBTCD' or event.get('exchange_index')!=2 or stamp(event['strike_date'])!=hour+3600:raise ValueError('hourly event identity or close changed')
    markets=catalog['data'].get('markets') or event.get('markets',[])
    candle=get('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=300&start='+iso(hour-300)+'&end='+iso(hour),60,3600)
    # This is a historical selector reference, never an executable quote.
    # A cached response must have been produced after this candle closed.
    reference_response_time=parsedate_to_datetime(candle['http_date']).timestamp()-float(candle['cache_age'] or 0)
    if reference_response_time<hour:raise ValueError('reference candle response predates its close')
    rows=[r for r in candle['data'] if r[0]==hour-300]
    if len(rows)!=1:raise ValueError('pre-hour Coinbase reference absent')
    reference=dec(rows[0][4]);m=choose_kalshi(markets,hour,reference)
    series=get(KAPI+'/series/KXBTCD');s=series['data']['series']
    if s.get('ticker')!='KXBTCD' or s.get('exchange_index')!=2 or s.get('fee_type')!='quadratic':raise ValueError('hourly series identity/fees changed')
    coef=Decimal('.07')*dec(s['fee_multiplier'])
    market={'venue':'kalshi','ticker':m['ticker'],'hour_start':hour,'close':hour+3600,'fee_coefficient':str(coef),
            'event_ticker':event_ticker,'strike':str(dec(m['floor_strike'])),'selection_reference_coinbase_close':str(reference),'reference_response_time':reference_response_time,'selection_rule':'nearest strike to pre-hour5mclose; ties lower strike then ticker',
            'definition':'YES above fixed strike; NO below, using official BRTI final-minute settlement','raw_market':m}
    return {'market':market,'sources':[catalog,candle,series]}

def discover_us(hour):
    record=get(PAPI+'/events/slug/'+slug(hour));event=record['data']['event'];expected='cpc-'+slug(hour)
    matches=[m for m in event['markets'] if m.get('slug')==expected]
    if len(matches)!=1:raise ValueError('US hourly market mapping')
    m=matches[0];terms=m['assetPriceTerms']
    if terms.get('indexSymbol')!='BRTI' or terms.get('horizon')!='1h' or stamp(terms['windowStart'])!=hour or stamp(terms['windowEnd'])!=hour+3600:raise ValueError('US hourly reference/window changed')
    sides=m.get('marketSides',[])
    if not any(s.get('long') is True and s.get('description')=='Yes' for s in sides):raise ValueError('US long/yes mapping changed')
    coefficient=dec(m['feeCoefficient'])
    if not 0<coefficient<=1:raise ValueError('US fee coefficient')
    market={'venue':'polymarket_us','ticker':expected,'hour_start':hour,'close':hour+3600,'fee_coefficient':str(coefficient),
            'price_to_beat':terms.get('priceToBeat'),'definition':'YES Up, NO Down relative to hourly BRTI boundary averages','raw_market':m}
    return {'market':market,'sources':[record]}

def kalshi_book(record):
    data=record['data']['orderbook_fp']
    def side(name):
        rows=data.get(name,[]);out=[]
        for p,q in rows:
            p,q=dec(p),dec(q)
            if not 0<=p<=1 or q<0:raise ValueError('Kalshi level invalid')
            out.append({'price':str(p),'quantity':str(q)})
        return out
    y,n=side('yes_dollars'),side('no_dollars')
    complement=lambda rows:[{'price':str(1-dec(r['price'])),'quantity':r['quantity']} for r in rows]
    return {'yes_bids':y,'yes_asks':complement(n),'no_bids':n,'no_asks':complement(y)}

def us_book(md):
    def side(rows):
        out=[]
        for r in rows:
            p,q=dec(r['px']['value']),dec(r['qty'])
            if r['px']['currency']!='USD' or not 0<=p<=1 or q<0:raise ValueError('US retail units invalid')
            out.append({'price':str(p),'quantity':str(q)})
        return out
    y,a=side(md.get('bids',[])),side(md.get('offers',[]))
    complement=lambda rows:[{'price':str(1-dec(r['price'])),'quantity':r['quantity']} for r in rows]
    return {'yes_bids':y,'yes_asks':a,'no_bids':complement(a),'no_asks':complement(y)}

def auth_headers(path=AUTH):
    if path.is_symlink():raise ValueError('auth symlink')
    h=json.loads(path.read_text())
    if set(h)!={'X-PM-Access-Key','X-PM-Timestamp','X-PM-Signature'} or abs(time.time()*1000-int(h['X-PM-Timestamp']))>20000:raise ValueError('fresh market-stream signature unavailable')
    return h

def blocker():
    if STOP.exists():return 'global_STOP'
    try:
        r=json.loads(RECOVERY.read_text())
        if r.get('operational_healthy') is not True or not 0<=time.time()-stamp(r['updated_at'])<=7200:return 'recovery_entry_hold'
    except (OSError,ValueError,KeyError,TypeError):return 'recovery_status_unavailable'
    return None

class Runner:
    def __init__(self,root,data_only=False):
        self.root=Path(root);validate_root(self.root);self.root.mkdir(parents=True,exist_ok=True)
        path=self.root/'config.json';expected={'version':'hourly98-v1','config':CONFIG,'data_only':data_only}
        if path.is_symlink():raise ValueError('config symlink')
        if path.exists():
            conf=json.loads(path.read_text());self.started_at=conf['started_at']
            if conf!={**expected,'started_at':self.started_at}:raise ValueError('configuration conflict')
        else:
            self.started_at=time.time();conf={**expected,'started_at':self.started_at};self.atomic(path,conf)
        self.engine=Engine(self.root/'hourly98.sqlite3',self.started_at);self.data_only=data_only;self.queue=asyncio.Queue(4096)
        self.stop=asyncio.Event();self.markets={};self.metadata_at={};self.errors=collections.Counter();self.quotes=collections.Counter();self.last_quotes={};self.signals={};self.requests=0
        self.stream=None;self.slot=None;self.archive_bytes=0;self.last_publish=0;self.last_settle=0;self.last_prune=0;self.storage_block=None
        self.consumer_lags=collections.defaultdict(lambda:collections.deque(maxlen=1200));self.engine_quotes=collections.Counter();self.mark_accounted={}
        self.engine.db.execute('create table if not exists decisions(key text primary key,at real not null,payload text not null,applied integer not null default 0)')
        for r in self.engine.db.execute('select * from decisions where applied=0').fetchall():
            p=json.loads(r['payload']);self.apply_decision(r['key'],p)
    def atomic(self,path,payload):
        if path.is_symlink():raise ValueError('status symlink')
        temp=path.with_suffix('.tmp')
        if temp.is_symlink():raise ValueError('temporary symlink')
        temp.write_text(canonical(payload)+'\n');temp.replace(path)
    def archive(self,row):
        if self.storage_block:return
        slot=int(time.time()//300)
        if self.slot!=slot:
            if self.stream:self.stream.close()
            folder=self.root/'evidence';folder.mkdir(exist_ok=True);path=folder/(str(slot)+'.jsonl.gz')
            if path.is_symlink():raise ValueError('archive symlink')
            self.stream=gzip.open(path,'ab',compresslevel=1);self.slot=slot
        encoded=(canonical(row)+'\n').encode();self.stream.write(encoded);self.stream.flush();self.archive_bytes+=len(encoded)
        if time.time()-self.last_prune>60:
            protected={int(json.loads(r[0])['at']//300) for r in self.engine.db.execute('select position from accounts where position is not null')}
            for p in (self.root/'evidence').glob('*.jsonl.gz'):
                if p.is_symlink():raise ValueError('archive symlink')
                if int(p.stem.split('.')[0]) not in protected and int(p.stem.split('.')[0])!=slot and time.time()-p.stat().st_mtime>7*86400:p.unlink()
            total=sum(p.stat().st_size for p in (self.root/'evidence').glob('*.jsonl.gz'))
            self.storage_block='evidence_storage_cap' if total>2*2**30 else None;self.last_prune=time.time()
    async def metadata_loop(self,venue):
        while not self.stop.is_set():
            hour=int(time.time()//3600)*3600
            try:
                current=self.markets.get(venue)
                if venue=='kalshi' and current and current['hour_start']==hour:
                    detail=await asyncio.to_thread(get,KAPI+'/markets/'+current['ticker']+'?exchange_index=2')
                    series=await asyncio.to_thread(get,KAPI+'/series/KXBTCD');m=detail['data']['market'];s=series['data']['series']
                    if m.get('ticker')!=current['ticker'] or stamp(m['close_time'])!=current['close'] or dec(m['floor_strike'])!=dec(current['strike']) or m.get('status') not in ('active','open') or s.get('fee_type')!='quadratic':raise ValueError('fixed hourly market changed')
                    result={'market':dict(current,fee_coefficient=str(Decimal('.07')*dec(s['fee_multiplier'])),raw_market=m),'sources':[detail,series]}
                else:result=await asyncio.to_thread(discover_kalshi if venue=='kalshi' else discover_us,hour)
                await self.queue.put({'kind':'metadata','venue':venue,'received':time.time(),'result':result})
            except Exception as exc:await self.queue.put({'kind':'error','source':venue+'-metadata','error_type':type(exc).__name__,'public_detail':str(exc)[:180]})
            await asyncio.sleep(15)
    async def kalshi_quotes(self):
        while not self.stop.is_set():
            start=time.monotonic();market=self.markets.get('kalshi')
            if market and time.time()<market['close']:
                try:
                    record=await asyncio.to_thread(get,KAPI+'/markets/'+market['ticker']+'/orderbook?depth=100&exchange_index=2',2)
                    if record['cache_age'] is not None and float(record['cache_age'])>1:raise ValueError('book cache age')
                    await self.queue.put({'kind':'book','venue':'kalshi','ticker':market['ticker'],'received':record['received'],'book':kalshi_book(record),'raw':record})
                except Exception as exc:await self.queue.put({'kind':'error','source':'kalshi-book','error_type':type(exc).__name__})
            await asyncio.sleep(max(.01,1-(time.monotonic()-start)))
    async def us_quotes(self):
        while not self.stop.is_set():
            market=self.markets.get('polymarket_us')
            if not market or time.time()>=market['close']:await asyncio.sleep(1);continue
            ticker=market['ticker'];connection=str(time.time_ns());previous=None
            try:
                async with websockets.connect(PWS,additional_headers=auth_headers(),open_timeout=10,close_timeout=2,max_size=2**20,max_queue=128) as ws:
                    await ws.send(canonical({'subscribe':{'requestId':connection,'subscriptionType':'SUBSCRIPTION_TYPE_MARKET_DATA','marketSlugs':[ticker],'responsesDebounced':False}}))
                    await self.queue.put({'kind':'connection','venue':'polymarket_us','connection':connection,'ticker':ticker,'received':time.time()})
                    last_message=time.monotonic()
                    while not self.stop.is_set() and self.markets.get('polymarket_us',{}).get('ticker')==ticker and time.time()<market['close']:
                        try:raw=await asyncio.wait_for(ws.recv(),1)
                        except asyncio.TimeoutError:
                            if time.monotonic()-last_message>10:raise TimeoutError('public market stream silent')
                            continue
                        received=time.time();mono=time.monotonic_ns();payload=json.loads(raw);md=payload.get('marketData')
                        if not isinstance(md,dict):
                            await self.queue.put({'kind':'protocol','venue':'polymarket_us','ticker':ticker,'connection':connection,'received':received,'envelope_keys':list(payload)})
                            continue
                        last_message=time.monotonic()
                        if md.get('marketSlug')!=ticker:raise ValueError('wrong streamed market')
                        source=stamp(md['transactTime']);reason=None
                        if md.get('state')!='MARKET_STATE_OPEN':reason='market_not_open'
                        if not -1<=received-source<=2:reason='source_activity_timestamp_stale_or_future'
                        if previous is not None and source<previous:reason='source_timestamp_regression'
                        previous=source
                        await self.queue.put({'kind':'book' if reason is None else 'rejected_book','venue':'polymarket_us','ticker':ticker,'received':received,
                            'capture_monotonic_ns':mono,'source_event_time':md['transactTime'],'source_time_semantics':'marketData transactTime may include stats activity',
                            'connection':connection,'book':us_book(md),'raw':payload,'reason':reason})
            except Exception as exc:await self.queue.put({'kind':'error','source':'us-stream','error_type':type(exc).__name__})
            await self.queue.put({'kind':'disconnect','venue':'polymarket_us','ticker':ticker,'received':time.time(),'connection':connection})
            await asyncio.sleep(2)
    async def charts(self,tf):
        attempted=collections.Counter();next_attempt={}
        while not self.stop.is_set():
            now=time.time();target=int(now//(tf*60))*tf*60
            key=str(tf)+':'+str(target)
            if attempted[target]<3 and now>=next_attempt.get(target,0) and not self.engine.db.execute('select 1 from decisions where key=?',(key,)).fetchone() and (self.data_only or self.started_at<=target and 3<=now-target<=60):
                attempted[target]+=1;next_attempt[target]=now+5
                try:
                    chart=await fetch_bars(timeframe_minutes=tf);observed=time.time()
                    sig=evaluate(chart['bars'],observed_at=observed,timeframe_minutes=tf,target_close=target)
                    await self.queue.put({'kind':'chart','tf':tf,'received':observed,'target':target,'source':chart,'signal':sig})
                except Exception as exc:await self.queue.put({'kind':'error','source':str(tf)+'m-chart','error_type':type(exc).__name__,'public_detail':str(exc)[:180]})
            await asyncio.sleep(1)
    def apply_decision(self,key,p):
        prior=self.engine.db.execute('select applied from decisions where key=?',(key,)).fetchone()
        if prior and prior['applied']:return
        now=time.time();outcomes=p.setdefault('venue_outcomes',{})
        for venue in ('kalshi','polymarket_us'):
            if venue in outcomes:continue
            reason=p.get('blocker') or ('data_only' if self.data_only else None)
            if not p['signal'].get('direction'):reason=reason or 'no_zone_exit_signal'
            if not self.started_at<=p['target']<=p['received']<p['target']+90:reason=reason or 'nonprospective_or_stale_chart'
            if now>=p['target']+90:reason=reason or 'signal_window_expired'
            if reason:outcomes[venue]={'state':'skipped','reason':reason};continue
            arm=venue+'-'+str(p['tf'])+'m';identity=arm+':'+str(int(p['target']))
            if self.engine.db.execute('select 1 from intents where id=?',(identity,)).fetchone():
                outcomes[venue]={'state':'classified','reason':'durable_intent_exists'};continue
            market=self.markets.get(venue)
            if market is None or not market['hour_start']<=p['target']<market['close'] or not 0<=now-self.metadata_at.get(venue,0)<=30:continue
            reason=blocker() or self.storage_block
            if reason:outcomes[venue]={'state':'skipped','reason':reason};continue
            self.engine.admit(arm,market,p['signal']['direction'],p['target'],now,digest({'decision':p,'market':market,'admitted_at':now}))
            outcomes[venue]={'state':'classified','observed':now,'ticker':market['ticker']}
        self.engine.db.execute('update decisions set applied=?,payload=? where key=?',(int(len(outcomes)==2),canonical(p),key))
    def needs_quote_accounting(self,row):
        venue,ticker,received=row['venue'],row['ticker'],row['received']
        pending=self.engine.db.execute("select first_quote from intents where venue=? and ticker=? and state='waiting'",(venue,ticker)).fetchall()
        if any(r[0] is None or received>=json.loads(r[0])['received']+2 for r in pending):return True
        key=venue+':'+ticker
        if received-self.mark_accounted.get(key,0)<1:return False
        for r in self.engine.db.execute('select position from accounts where position is not null'):
            p=json.loads(r[0])
            if p['venue']==venue and p['ticker']==ticker:return True
        return False
    async def settlements(self):
        # Official proof only: no payout inferred from chart, bid, last trade or expiry.
        while not self.stop.is_set():
            positions=[json.loads(r[0]) for r in self.engine.db.execute('select position from accounts where position is not null')]
            unique={(p['venue'],p['ticker']):p for p in positions if time.time()>=p['close']}
            for (venue,ticker),pos in unique.items():
                try:
                    route=KAPI+'/markets/'+ticker+'?exchange_index=2' if venue=='kalshi' else PAPI+'/market/slug/'+ticker
                    record=await asyncio.to_thread(get,route);m=record['data']['market'];result=None
                    if venue=='kalshi' and m.get('ticker')==ticker and m.get('status') in ('finalized','settled') and m.get('result') in ('yes','no'):result=int(m['result']=='yes')
                    if venue=='polymarket_us' and m.get('slug')==ticker and m.get('closed') is True and m.get('status')=='MARKET_STATUS_RESOLVED':
                        payout=await asyncio.to_thread(get,PAPI+'/markets/'+ticker+'/settlement');settlement=payout['data'].get('settlement')
                        record['official_payout_response']=payout
                        if payout['data'].get('slug')==ticker and not isinstance(settlement,bool) and settlement is not None and dec(settlement) in (0,1):result=int(dec(settlement))
                    await self.queue.put({'kind':'settlement_check','venue':venue,'ticker':ticker,'received':record['received'],'official_result':result,'raw':record})
                except Exception as exc:await self.queue.put({'kind':'error','source':venue+'-settlement','error_type':type(exc).__name__})
            await asyncio.sleep(10)
    def publish(self):
        now=time.time();self.atomic(self.root/'status.json',{'as_of':iso(now),'started_at':iso(self.started_at),'mode':'internal-paper','data_only':self.data_only,
            'new_accounts':4,'allocation_bps':2000,'existing_accounts_modified':False,'venue_orders':0,'quotes':dict(self.quotes),
            'last_quote_age_seconds':{v:now-t for v,t in self.last_quotes.items()},'errors':dict(self.errors),'last_signals':self.signals,
            'engine_accounted_quotes':dict(self.engine_quotes),'consumer_lag_ms':{v:{'median':statistics.median(samples),'p95':sorted(samples)[int((len(samples)-1)*.95)],'max':max(samples),'samples':len(samples)} for v,samples in self.consumer_lags.items() if samples},
            'markets':{v:{k:x for k,x in m.items() if k!='raw_market'} for v,m in self.markets.items()},'entry_blocker':blocker() or self.storage_block,
            'us_auth_age_seconds':None if not AUTH.exists() else now-AUTH.stat().st_mtime,'ledger':self.engine.status(now)})
    async def run(self,seconds=None):
        tasks=[asyncio.create_task(self.metadata_loop(v)) for v in ('kalshi','polymarket_us')]
        tasks +=[asyncio.create_task(f()) for f in (self.kalshi_quotes,self.us_quotes,self.settlements)]
        tasks +=[asyncio.create_task(self.charts(tf)) for tf in (15,30)]
        end=time.monotonic()+seconds if seconds else float('inf')
        try:
            while not self.stop.is_set() and time.monotonic()<end:
                for task in tasks:
                    if task.done():task.result();raise RuntimeError('feed task ended')
                try:row=await asyncio.wait_for(self.queue.get(),.2)
                except asyncio.TimeoutError:row=None
                now=time.time()
                if row:
                    self.archive(row);kind=row['kind']
                    if kind=='metadata':
                        m=row['result']['market'];self.markets[row['venue']]=m;self.metadata_at[row['venue']]=now
                    elif kind=='chart':
                        key=str(row['tf'])+':'+str(row['target']);self.signals[str(row['tf'])]=row['signal']
                        current={v:m for v,m in self.markets.items() if 0<=now-self.metadata_at[v]<=30}
                        decision={**row,'markets':current,'blocker':blocker() or self.storage_block or ('data_only' if self.data_only else None)}
                        with self.engine.tx():self.engine.db.execute('insert or ignore into decisions values(?,?,?,0)',(key,now,canonical(decision)))
                        self.apply_decision(key,decision)
                    elif kind=='book':
                        self.quotes[row['venue']]+=1;self.last_quotes[row['venue']]=row['received']
                        self.consumer_lags[row['venue']].append((time.time()-row['received'])*1000)
                        if not self.data_only and not blocker() and not self.storage_block and self.needs_quote_accounting(row):
                            try:
                                self.engine.quote(row['venue'],row['ticker'],row['received'],digest(row),row['book'],time.time())
                                self.engine_quotes[row['venue']]+=1;self.mark_accounted[row['venue']+':'+row['ticker']]=row['received']
                            except ValueError as exc:self.errors['quote:'+str(exc)]+=1
                    elif kind=='settlement_check' and row['official_result'] is not None:
                        with self.engine.tx():self.engine.event(row['received'],'official_settlement_evidence',venue=row['venue'],ticker=row['ticker'],raw=row['raw'])
                        self.engine.settle(row['venue'],row['ticker'],row['official_result'],row['received'],digest(row))
                    elif kind=='error':self.errors[row['source']+':'+row['error_type']]+=1
                deadline=self.engine.db.execute("select min(min(signal+90,close)) from intents where state='waiting'").fetchone()[0]
                if deadline is not None and now>=deadline:self.engine.expire(now)
                hold=blocker() or self.storage_block
                if hold:
                    with self.engine.tx():
                        for intent in self.engine.db.execute("select * from intents where state='waiting'").fetchall():self.engine.skip(intent,now,'entry_blocked:'+hold)
                for decision in self.engine.db.execute('select * from decisions where applied=0').fetchall():self.apply_decision(decision['key'],json.loads(decision['payload']))
                if now-self.last_publish>=1:self.publish();self.last_publish=now
        finally:
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True);self.publish()
            if self.stream:self.stream.close()
            self.engine.close()

def validate_root(root):
    p=Path(root)
    if p.is_symlink() or any(x.is_symlink() for x in p.parents):raise ValueError('state root symlink')
    protected=['/var/lib/neorome-btc-'+n for n in ['cross5-paper','cross30-paper','gap5-fast-paper','gap5-slow-paper','brackets-v1','trailing-v1','sma1m-v1','interval-expansion-v1']]
    r=p.resolve()
    for old in map(Path,protected):
        if r==old or r in old.parents or old in r.parents:raise ValueError('existing paper root refused')

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--seconds',type=float);p.add_argument('--data-only',action='store_true');args=p.parse_args()
    validate_root(args.root)
    if args.data_only and args.root.resolve()==ROOT.resolve():raise ValueError('preflight needs separate root')
    args.root.mkdir(parents=True,exist_ok=True);lockpath=args.root/'runner.lock'
    if lockpath.is_symlink():raise ValueError('runner lock symlink')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        async def start():
            global _BUDGET
            _BUDGET=Budget(args.root/'request-budget.json')
            runner=Runner(args.root,args.data_only);loop=asyncio.get_running_loop()
            for sig in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(sig,runner.stop.set)
            await runner.run(args.seconds)
        asyncio.run(start())
if __name__=='__main__':main()
