"""GET-only cloud paper collector; exact frozen hourly decision, no broker credentials."""
from __future__ import annotations
import argparse, collections, datetime as dt, fcntl, hashlib, http.client, json, os, re, signal, time
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo
from .btc_distance_engine import Engine, CONFIG, canonical, dec, digest
from .btc_distance_signal import evaluate

ROOT=Path('/var/lib/neorome-btc-distance-v1')
KAPI='https://external-api.kalshi.com/trade-api/v2'
CAPI='https://api.exchange.coinbase.com/products/BTC-USD/candles?'
STOP=Path('/var/lib/neorome-paper/STOP'); RECOVERY=Path('/var/lib/neorome-paper-recovery/status.json')
TICKER=re.compile(r'KXBTCD-[A-Z0-9.\-]+\Z'); CONNECTIONS={}

def iso(t): return dt.datetime.fromtimestamp(t,dt.timezone.utc).isoformat().replace('+00:00','Z')
def stamp(s): return dt.datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
def atomic(path, data):
    tmp=path.with_suffix('.tmp')
    if path.is_symlink() or tmp.is_symlink(): raise ValueError('symlink output')
    tmp.write_text(canonical(data)+'\n');tmp.replace(path)

def get(url, quote=False):
    if not (url.startswith(CAPI) or re.fullmatch(re.escape(KAPI)+r'/(?:events/KXBTCD-[A-Z0-9]+|markets/KXBTCD-[A-Z0-9.\-]+(?:/orderbook)?|series/KXBTCD|events/fee_changes)(?:\?[^#]*)?',url)):
        raise ValueError('unapproved GET route')
    u=urlsplit(url); started=time.time(); mono=time.monotonic()
    conn=CONNECTIONS.setdefault(u.netloc,http.client.HTTPSConnection(u.netloc,timeout=5))
    try:
        conn.request('GET',u.path+('?' + u.query if u.query else ''),headers={'User-Agent':'NeoRome-distance-paper/1','Accept':'application/json','Cache-Control':'no-cache'})
        response=conn.getresponse(); raw=response.read(4*2**20+1); received=time.time()
        date=response.headers.get('Date'); age=float(response.headers.get('Age','0'))
        if response.status!=200: raise ValueError('public HTTP '+str(response.status))
        if len(raw)>4*2**20 or not date: raise ValueError('public response size/date')
        lag=received-parsedate_to_datetime(date).timestamp()
        if not -1<=lag<=(2 if quote else 60) or not 0<=age<=(0 if quote else 3600): raise ValueError('stale public response')
        if quote and time.monotonic()-mono>2: raise ValueError('slow quote response')
        return {'url':url,'request_started':started,'received':received,'http_date':date,'cache_age':age,
                'rtt_seconds':time.monotonic()-mono,'body_sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw)}
    except BaseException:
        conn.close(); CONNECTIONS.pop(u.netloc,None); raise

def candle_chunk(start,end):
    r=get(CAPI+'granularity=300&start='+iso(start)+'&end='+iso(end))
    origin=parsedate_to_datetime(r['http_date']).timestamp()-r['cache_age']
    if origin<end: raise ValueError('candle publication predates requested close')
    if not isinstance(r['data'],list): raise ValueError('candles schema')
    return r

def candles(decision):
    receipts=[candle_chunk(decision-90000,decision-45000),candle_chunk(decision-45000,decision)]
    rows={}
    for receipt in receipts:
        for row in receipt['data']:
            if len(row)!=6 or int(row[0])%300: raise ValueError('candle timestamp/schema')
            if decision-90000 <= row[0] and row[0]+300<=decision:
                if row[0] in rows and rows[row[0]]!=row: raise ValueError('conflicting candle revisions')
                rows[row[0]]=row
    return sorted(rows.values()),receipts

def validate_market(m,hour,ticker=None,strike=None):
    if (m.get('exchange_index')!=2 or m.get('strike_type')!='greater' or not TICKER.fullmatch(m.get('ticker',''))
        or not m['ticker'].startswith(m.get('event_ticker','')+'-') or stamp(m['close_time'])!=hour+3600
        or stamp(m['open_time'])>hour or stamp(m['created_time'])>hour or dec(m['floor_strike'])<=0
        or 'BRTI' not in m.get('rules_primary','') or 'sixty' not in m.get('rules_primary','').lower()):
        raise ValueError('hourly market terms changed')
    if ticker and m['ticker']!=ticker or strike is not None and dec(m['floor_strike'])!=dec(strike): raise ValueError('selected market changed')

def fees(event_ticker):
    series=get(KAPI+'/series/KXBTCD'); overrides=get(KAPI+'/events/fee_changes?limit=1000')
    s=series['data']['series']; f=overrides['data']
    if s.get('ticker')!='KXBTCD' or s.get('exchange_index')!=2 or s.get('fee_type')!='quadratic' or dec(s['fee_multiplier'])!=1:
        raise ValueError('frozen standard fee schedule changed')
    if f.get('cursor') or any(v.get('event_ticker')==event_ticker for v in f['event_fee_changes']): raise ValueError('unverified event fee override')
    return [series,overrides]

def discover(hour):
    event='KXBTCD-'+dt.datetime.fromtimestamp(hour+3600,ZoneInfo('America/New_York')).strftime('%y%b%d%H').upper()
    r=get(KAPI+'/events/'+event+'?with_nested_markets=true&exchange_index=2'); e=r['data']['event']
    if e.get('event_ticker')!=event or e.get('series_ticker')!='KXBTCD' or e.get('exchange_index')!=2 or stamp(e['strike_date'])!=hour+3600:
        raise ValueError('hourly event identity changed')
    c=candle_chunk(hour-300,hour); rows=[v for v in c['data'] if v[0]==hour-300]
    if len(rows)!=1: raise ValueError('missing pre-hour reference')
    ref=dec(rows[0][4]); eligible=[]
    for m in (r['data'].get('markets') or e.get('markets',[])):
        try: validate_market(m,hour)
        except (KeyError,ValueError,TypeError): continue
        if m.get('event_ticker')!=event or m.get('status') not in ('active','open'): continue
        strike=dec(m['floor_strike']);eligible.append((abs(strike-ref),strike,m['ticker'],m))
    if not eligible: raise ValueError('no eligible hourly greater-than market')
    m=min(eligible,key=lambda x:x[:3])[3]; fr=fees(event)
    return {'ticker':m['ticker'],'event_ticker':event,'hour_start':hour,'close':hour+3600,
            'strike':str(dec(m['floor_strike'])),'reference':str(ref),'raw_market':m,'fee_coefficient':'0.07'},[r,c]+fr

def book(receipt):
    raw=receipt['data']['orderbook_fp']
    def side(name):
        out=[]
        for p,q in raw[name]:
            p,q=dec(p),dec(q)
            if not 0<=p<=1 or q<0: raise ValueError('invalid book')
            out.append({'price':str(p),'quantity':str(q)})
        return out
    y,n=side('yes_dollars'),side('no_dollars')
    complement=lambda v:[{'price':str(1-dec(r['price'])),'quantity':r['quantity']} for r in v]
    return {'yes_bids':y,'yes_asks':complement(n),'no_bids':n,'no_asks':complement(y)}

def blocker():
    if STOP.exists(): return 'global_STOP'
    try:
        r=json.loads(RECOVERY.read_text())
        if r.get('operational_healthy') is not True or not 0<=time.time()-stamp(r['updated_at'])<=7200: return 'recovery_entry_hold'
    except (OSError,ValueError,KeyError,TypeError): return 'recovery_status_unavailable'
    return None

def official_result(m,market,observed):
    validate_market(m,market['hour_start'],market['ticker'],market['strike'])
    if m.get('status')!='finalized' or m.get('result') not in ('yes','no'): return None
    ts=stamp(m['settlement_ts'])
    if not market['close']<=ts<=observed: raise ValueError('invalid official settlement time')
    result=int(m['result']=='yes')
    if int(dec(m['expiration_value'])>dec(market['strike']))!=result: raise ValueError('official value/result disagree')
    return result

class Runner:
    def __init__(self,root,data_only=False):
        self.root=Path(root)
        if self.root.is_symlink() or not self.root.is_absolute() or (self.root!=ROOT and not self.root.name.startswith('neorome-btc-distance-')): raise ValueError('invalid isolated root')
        if data_only and self.root==ROOT: raise ValueError('data-only cannot target production')
        self.root.mkdir(parents=True,exist_ok=True)
        path=self.root/'config.json'; expected={'config':CONFIG,'data_only':data_only}
        if path.exists():
            old=json.loads(path.read_text()); self.started=old['started_at']
            if old!={**expected,'started_at':self.started}: raise ValueError('frozen config differs')
        else: self.started=time.time();atomic(path,{**expected,'started_at':self.started})
        self.engine=Engine(self.root/'ledger.sqlite3',self.started);self.data_only=data_only;self.stop=False
        self.errors=collections.Counter();self.last_error=None;self.books={};self.last_publish=0;self.last_mark=0;self.last_settle=0
        self.market=None;self.hour=None;self.verified=None;self.last_attempt={};self.preflight=None;self.next_hour=(int(self.started//3600)+1)*3600
        self.engine.db.execute('create table if not exists marks(at real primary key,payload text not null)')
    def remember(self,receipts): return [self.engine.source(r) for r in receipts]
    def due(self,key,seconds):
        now=time.time()
        if now-self.last_attempt.get(key,0)<seconds: return False
        self.last_attempt[key]=now;return True
    def run_preflight(self):
        h=int(time.time()//3600)*3600;market,receipts=discover(h);decision=int(time.time()//900)*900
        rows,cs=candles(decision);q=get(KAPI+'/markets/'+market['ticker']+'/orderbook?depth=100&exchange_index=2',True)
        b=book(q);sig=evaluate(rows,decision,market['strike'])
        self.remember(receipts+cs+[q]);self.preflight={'at':time.time(),'market':market,'candle_rows':len(rows),'signal':sig,
                                                        'book_levels':{k:len(v) for k,v in b.items()},'source_hashes':[digest(v) for v in receipts+cs+[q]]}
    def tick(self):
        now=time.time();hour=int(now//3600)*3600
        if self.data_only:
            if self.preflight is None and self.due('preflight',10): self.run_preflight()
        else:
            if self.hour!=hour:
                self.hour=hour;self.verified=None
                row=self.engine.db.execute('select payload from selections where hour=?',(hour,)).fetchone()
                self.market=json.loads(row[0]) if row else None
            if hour>=self.next_hour and self.market is None and 2<=now-hour<50 and self.due('selection',5):
                market,receipts=discover(hour);market['source_hashes']=self.remember(receipts)
                self.engine.selection(hour,time.time(),market);self.market=market
            if self.market and hour+903<=now<hour+945 and self.due('decision',5) and not self.engine.db.execute('select 1 from decisions where hour=?',(hour,)).fetchone():
                rows,receipts=candles(hour+900);sig=evaluate(rows,hour+900,self.market['strike']);sig['source_hashes']=self.remember(receipts)
                if time.time()<hour+950:self.engine.decision(hour,time.time(),sig,blocker())
            pending=self.engine.db.execute("select hour from decisions where state='waiting'").fetchall()
            for pending_hour, in pending:
                if now>=pending_hour+970:self.engine.skip(pending_hour,now,'entry_window_expired')
            waiting=self.engine.db.execute("select 1 from decisions where hour=? and state='waiting'",(hour,)).fetchone()
            if waiting and hour+945<=now<hour+958 and self.verified is None and self.due('verify',3):
                r=get(KAPI+'/markets/'+self.market['ticker']+'?exchange_index=2');m=r['data']['market'];validate_market(m,hour,self.market['ticker'],self.market['strike'])
                if m.get('status') not in ('active','open'): raise ValueError('market not open')
                receipts=[r]+fees(self.market['event_ticker']);self.remember(receipts);self.verified=time.time()
            if waiting and hour+960<=now<hour+970:
                hold=blocker() or self.storage_hold()
                if hold:self.engine.skip(hour,now,hold)
                elif self.verified is None or not 0<=now-self.verified<=30:self.engine.skip(hour,now,'fresh_metadata_missing')
                elif self.due('entry',.8):
                    r=get(KAPI+'/markets/'+self.market['ticker']+'/orderbook?depth=100&exchange_index=2',True)
                    self.engine.fill(hour,r,book(r),time.time())
        now=time.time();positions={}
        for row in self.engine.db.execute('select position from accounts where position is not null'):
            p=json.loads(row[0]);positions[p['ticker']]=p
        if now-self.last_settle>=10:
            self.last_settle=now
            for ticker,p in positions.items():
                if now<p['close']:continue
                r=get(KAPI+'/markets/'+ticker+'?exchange_index=2');result=official_result(r['data']['market'],p['market'],r['received'])
                if result is not None:self.engine.settle(ticker,result,r)
        if now-self.last_mark>=10:
            self.last_mark=now
            for ticker,p in positions.items():
                if now>=p['close']:continue
                r=get(KAPI+'/markets/'+ticker+'/orderbook?depth=100&exchange_index=2',True)
                self.books[ticker]={'received':r['received'],'book':book(r),'source_hash':self.engine.source(r)}
            before={a['arm']:a['mark_dd'] for a in self.engine.db.execute('select arm,mark_dd from accounts')}
            status=self.engine.status(time.time(),self.books)
            if positions:
                self.engine.db.execute('insert or replace into marks values(?,?)',(time.time(),canonical({'accounts':status['accounts'],'quotes':self.books})))
                # Retain full receipts for every new observed drawdown extreme.
                for a in status['accounts']:
                    if dec(a['mark_dd'])>dec(before[a['arm']]) and a['position']:
                        self.engine.event(time.time(),'drawdown',a['arm'],drawdown=a['mark_dd'],nav=a['nav'],quote=self.books.get(a['position']['ticker']))
    def storage_hold(self):
        return 'ledger_storage_cap' if sum(p.stat().st_size for p in self.root.glob('ledger.sqlite3*'))>4*2**30 else None
    def publish(self):
        now=time.time();s=self.engine.status(now,self.books)
        s.update({'updated_at':iso(now),'mode':'data-only' if self.data_only else 'prospective internal paper',
                  'entry_blocker':blocker() or self.storage_hold(),'errors':dict(self.errors),'last_error':self.last_error,
                  'next_full_hour':iso(max(self.next_hour,(int(now//3600)+(1 if now%3600>=970 else 0))*3600)),
                  'selected_market':self.market,'preflight':self.preflight,
                  'latest_decision':None})
        row=self.engine.db.execute('select payload,state,at from decisions order by hour desc limit 1').fetchone()
        if row:s['latest_decision']={'at':row['at'],'state':row['state'],**json.loads(row['payload'])}
        atomic(self.root/'status.json',s);self.last_publish=now
    def run(self,seconds=None):
        end=time.monotonic()+seconds if seconds else float('inf')
        while not self.stop and time.monotonic()<end:
            try:self.tick()
            except Exception as exc:
                key=type(exc).__name__+':'+str(exc)[:180];self.errors[key]+=1;self.last_error={'at':iso(time.time()),'detail':key}
                self.engine.event(time.time(),'error',detail=key)
            if time.time()-self.last_publish>=5:self.publish()
            time.sleep(.2)
        self.publish();self.engine.db.close()

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--seconds',type=int);p.add_argument('--data-only',action='store_true');a=p.parse_args()
    if a.root.is_symlink():raise ValueError('root symlink')
    a.root.mkdir(parents=True,exist_ok=True);lock_path=a.root/'runner.lock'
    if lock_path.is_symlink():raise ValueError('lock symlink')
    with lock_path.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);r=Runner(a.root,a.data_only)
        for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:setattr(r,'stop',True))
        r.run(a.seconds)
if __name__=='__main__':main()
