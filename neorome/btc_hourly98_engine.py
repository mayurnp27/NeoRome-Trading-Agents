"""Four isolated hold-to-settlement paper accounts; decimal venue quotes."""
from __future__ import annotations
import contextlib, hashlib, json, math, sqlite3
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

ARMS=tuple(f'{v}-{tf}m' for v in ('kalshi','polymarket_us') for tf in (15,30))
CONFIG={'version':'hourly98-v1','arms':list(ARMS),'initial_cash_usd':'500.00','allocation_bps':2000,
        'stochastic':[98,3,3],'thresholds':[20,80],'entry_window_seconds':90,'entry_quotes':2,
        'quote_separation_seconds':2,'maximum_ask_usd':'0.60','adverse_buffer_usd':'0.01',
        'exit':'official_settlement','sma_filter':False,'quantity':'whole contracts; decimal US prices retained',
        'fee':'ceil(coefficient*N*p*(1-p)*100)/100; conservative cent rounding per order'}

def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(v):return hashlib.sha256(canonical(v).encode()).hexdigest()
def dec(v):
    if isinstance(v,bool):raise ValueError('boolean numeric')
    d=Decimal(str(v))
    if not d.is_finite():raise ValueError('nonfinite number')
    return d
def number(v):
    n=float(dec(v))
    if not math.isfinite(n):raise ValueError('nonfinite timestamp')
    return n
def cents(v):return dec(v).quantize(Decimal('.01'),rounding=ROUND_CEILING)
def fee(q,p,coefficient):return cents(dec(coefficient)*q*p*(1-p))
def levels(rows,reverse=False):
    out=[]
    if not isinstance(rows,list):raise ValueError('book side must be list')
    for row in rows:
        p,q=dec(row['price']),dec(row['quantity'])
        if not 0<=p<=1 or q<0:raise ValueError('invalid price/quantity')
        if q:out.append((p,q))
    return sorted(out,reverse=reverse)
def vwap(rows,quantity):
    remaining=dec(quantity);total=Decimal(0)
    for p,q in rows:
        take=min(remaining,q);total+=take*p;remaining-=take
        if remaining==0:return total/quantity
    return None

class Engine:
    def __init__(self,path,started_at):
        self.path=Path(path);self.started_at=number(started_at)
        if self.path.is_symlink():raise ValueError('ledger symlink')
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(self.path,isolation_level=None);self.db.row_factory=sqlite3.Row
        self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS accounts(arm TEXT PRIMARY KEY,cash TEXT NOT NULL,position TEXT,realized TEXT NOT NULL,entries INTEGER NOT NULL,wins INTEGER NOT NULL,losses INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS intents(id TEXT PRIMARY KEY,arm TEXT NOT NULL,venue TEXT NOT NULL,ticker TEXT NOT NULL,signal REAL NOT NULL,observed REAL NOT NULL,close REAL NOT NULL,payload TEXT NOT NULL,first_quote TEXT,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotes(key TEXT PRIMARY KEY,received REAL NOT NULL,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settlements(key TEXT PRIMARY KEY,result INTEGER NOT NULL,observed REAL NOT NULL,source_hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,at REAL NOT NULL,kind TEXT NOT NULL,arm TEXT,payload TEXT NOT NULL);
        ''')
        with self.tx():
            conf={'config':CONFIG,'started_at':self.started_at};old=self.db.execute("SELECT value FROM meta WHERE key='identity'").fetchone()
            if old and json.loads(old[0])!=conf:raise ValueError('frozen identity differs')
            if old is None:
                if self.db.execute('select count(*) from accounts').fetchone()[0]:raise ValueError('unknown ledger refused')
                self.db.execute('insert into meta values(?,?)',('identity',canonical(conf)))
                self.db.executemany('insert into accounts values(?,?,NULL,?,0,0,0)',[(arm,'500.00','0.00') for arm in ARMS])
            if [r[0] for r in self.db.execute('select arm from accounts order by arm')]!=sorted(ARMS):raise ValueError('account set differs')

    @contextlib.contextmanager
    def tx(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:yield;self.db.commit()
        except BaseException:self.db.rollback();raise
    def event(self,event_at,kind,arm=None,**payload):self.db.execute('insert into events(at,kind,arm,payload) values(?,?,?,?)',(event_at,kind,arm,canonical(payload)))
    def admit(self,arm,market,side,signal,observed,source_hash):
        if arm not in ARMS or side not in ('yes','no'):raise ValueError('account or side')
        venue=arm.rsplit('-',1)[0];tf=int(arm.rsplit('-',1)[1][:-1]);signal=number(signal);observed=number(observed)
        if market['venue']!=venue or not self.started_at<=signal<=observed<signal+90 or signal%(tf*60):raise ValueError('nonprospective signal')
        start,close=number(market['hour_start']),number(market['close'])
        if close-start!=3600 or start%3600 or not start<=signal<close or observed>=close:raise ValueError('wrong hourly contract')
        coefficient=dec(market['fee_coefficient'])
        if not 0<coefficient<=1 or len(source_hash)!=64:raise ValueError('invalid fee/source')
        payload={'market':market,'side':side,'signal':signal,'observed':observed,'source_hash':source_hash}
        identity=arm+':'+str(int(signal))
        with self.tx():
            old=self.db.execute('select payload from intents where id=?',(identity,)).fetchone()
            if old:
                if old[0]!=canonical(payload):raise ValueError('signal identity conflict')
                return
            a=self.db.execute('select * from accounts where arm=?',(arm,)).fetchone();reason='position_open' if a['position'] else None
            self.db.execute('insert into intents values(?,?,?,?,?,?,?,?,NULL,?)',(identity,arm,venue,market['ticker'],signal,observed,close,canonical(payload),'skipped' if reason else 'waiting'))
            self.event(observed,'skip' if reason else 'signal',arm,id=identity,reason=reason,**payload)
    def expire(self,now):
        with self.tx():
            for r in self.db.execute("select * from intents where state='waiting'").fetchall():
                if now>=min(r['signal']+90,r['close']):self.skip(r,now,'entry_window_expired')
    def skip(self,intent,now,reason):
        self.db.execute("update intents set state='skipped' where id=?",(intent['id'],));self.event(now,'skip',intent['arm'],intent=intent['id'],reason=reason)
    def quote(self,venue,ticker,received,source_hash,book,now):
        received=number(received);now=number(now)
        if venue not in ('kalshi','polymarket_us') or len(source_hash)!=64 or not 0<=now-received<=1:raise ValueError('quote freshness/source')
        clean={s:levels(book[s],reverse=s.endswith('bids')) for s in ('yes_bids','yes_asks','no_bids','no_asks')}
        for side in ('yes','no'):
            if clean[side+'_bids'] and clean[side+'_asks'] and clean[side+'_bids'][0][0]>=clean[side+'_asks'][0][0]:raise ValueError('crossed/locked book')
        key=venue+':'+ticker;payload={'received':received,'source_hash':source_hash,'book':book}
        with self.tx():
            old=self.db.execute('select received from quotes where key=?',(key,)).fetchone()
            if old and received<=old[0]:raise ValueError('duplicate/out-of-order quote')
            self.db.execute('insert into quotes values(?,?,?) on conflict(key) do update set received=excluded.received,payload=excluded.payload',(key,received,canonical(payload)))
            for r in self.db.execute("select * from intents where venue=? and ticker=? and state='waiting' order by observed,id",(venue,ticker)).fetchall():
                if received>=min(r['signal']+90,r['close']):self.skip(r,received,'entry_window_expired');continue
                if received<=r['observed']:continue
                if r['first_quote'] is None:
                    self.db.execute('update intents set first_quote=? where id=?',(canonical(payload),r['id']));continue
                first=json.loads(r['first_quote'])
                if source_hash==first['source_hash'] or received<first['received']+2:continue
                self.fill(r,clean,first,received,source_hash)
    def fill(self,intent,book,first,at,source_hash):
        a=self.db.execute('select * from accounts where arm=?',(intent['arm'],)).fetchone()
        if a['position']:self.skip(intent,at,'position_open');return
        p=json.loads(intent['payload']);side=p['side'];asks=[r for r in book[side+'_asks'] if 0<r[0]<=Decimal('.60')]
        firstasks=[r for r in levels(first['book'][side+'_asks']) if 0<r[0]<=Decimal('.60')]
        depth=int(min(sum((q for _,q in asks),Decimal(0)),sum((q for _,q in firstasks),Decimal(0))))
        cash=dec(a['cash']);budget=(cash*Decimal('.20')).quantize(Decimal('.01'),rounding=ROUND_FLOOR);coef=dec(p['market']['fee_coefficient'])
        def cost(rows,q):
            px=vwap(rows,q)+Decimal('.01');charge=fee(q,px,coef);premium=cents(q*px);return premium+charge,px,charge
        low,high=1,min(depth,int(budget*100));best=None
        while low<=high:
            q=(low+high)//2;b=cost(asks,q);f=cost(firstasks,q)
            if max(b[0],f[0])<=budget:best=(q,*b);low=q+1
            else:high=q-1
        if best is None:self.skip(intent,at,'insufficient_depth_or_cash_or_price_cap');return
        q,total,px,charge=best
        pos={'ticker':intent['ticker'],'venue':intent['venue'],'side':side,'quantity':q,'basis':str(total),'fee':str(charge),'fill_price_usd':str(px),'at':at,
             'close':intent['close'],'market':p['market'],'source_hash':source_hash,'intent':intent['id'],'available_cash_before':str(cash),'budget':str(budget),
             'first_quote':first,'second_quote':{s:[{'price':str(px),'quantity':str(qty)} for px,qty in rows] for s,rows in book.items()}}
        self.db.execute('update accounts set cash=?,position=?,entries=entries+1 where arm=?',(str(cash-total),canonical(pos),intent['arm']))
        self.db.execute("update intents set state='entered' where id=?",(intent['id'],));self.event(at,'entry',intent['arm'],**pos)
    def settle(self,venue,ticker,result,observed,source_hash):
        if type(result) is not int or result not in (0,1) or len(source_hash)!=64:raise ValueError('settlement proof')
        observed=number(observed);key=venue+':'+ticker
        with self.tx():
            old=self.db.execute('select result from settlements where key=?',(key,)).fetchone()
            if old:
                if old[0]!=result:raise ValueError('settlement conflict')
                return
            matching=[]
            for a in self.db.execute('select * from accounts where position is not null').fetchall():
                pos=json.loads(a['position'])
                if pos['venue']==venue and pos['ticker']==ticker:
                    if observed<pos['close']:raise ValueError('settlement before close')
                    matching.append((a,pos))
            if not matching:return
            self.db.execute('insert into settlements values(?,?,?,?)',(key,result,observed,source_hash))
            for a,pos in matching:
                won=(result==1)==(pos['side']=='yes');payout=Decimal(pos['quantity'] if won else 0);pnl=payout-dec(pos['basis'])
                self.db.execute('update accounts set cash=?,position=NULL,realized=?,wins=wins+?,losses=losses+? where arm=?',
                    (str(dec(a['cash'])+payout),str(dec(a['realized'])+pnl),int(won),int(not won),a['arm']))
                self.event(observed,'settlement',a['arm'],ticker=ticker,official_yes_result=result,payout=str(payout),pnl=str(pnl),source_hash=source_hash,position=pos)
    def status(self,now):
        accounts=[]
        for a in self.db.execute('select * from accounts order by arm'):
            r=dict(a);r['position']=json.loads(r['position']) if r['position'] else None;r['mark_value_usd']=None
            if r['position']:
                p=r['position'];quote=self.db.execute('select * from quotes where key=?',(p['venue']+':'+p['ticker'],)).fetchone()
                if quote and 0<=now-quote['received']<=2 and now<p['close']:
                    b=json.loads(quote['payload']);px=vwap(levels(b['book'][p['side']+'_bids'],True),p['quantity'])
                    if px is not None:
                        px=max(Decimal(0),px-Decimal('.01'));r['mark_value_usd']=str(max(Decimal(0),p['quantity']*px-fee(p['quantity'],px,dec(p['market']['fee_coefficient']))))
            r['nav_usd']=str(dec(r['cash'])+(dec(r['mark_value_usd']) if r['position'] and r['mark_value_usd'] is not None else Decimal(0))) if not r['position'] or r['mark_value_usd'] is not None else None
            accounts.append(r)
        return {'accounts':accounts,'pending_intents':self.db.execute("select count(*) from intents where state='waiting'").fetchone()[0],
                'event_counts':dict(self.db.execute('select kind,count(*) from events group by kind')),'config':CONFIG,'venue_orders':0}
    def close(self):self.db.close()
