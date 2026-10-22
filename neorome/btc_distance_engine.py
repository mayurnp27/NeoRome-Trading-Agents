"""Prospective Kalshi paper ledgers for the frozen BTC distance experiment."""
from __future__ import annotations
import contextlib, datetime, hashlib, json, math, sqlite3
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

ARMS = {'1pct': '0.01', '2pct': '0.02', '5pct': '0.05', '25pct': '0.25'}
CONFIG = {'version': 'btc-distance-v1', 'initial_cash': '500.0000', 'allocations': ARMS,
          'stochastic': [15, 67, 1, 1, 'd_slope'], 'sma': [45, 2, 3, 'fresh_cross'],
          'distance_min': 1, 'volatility': '288 simple 5m returns; sample std; sqrt(9)',
          'decision_offset_seconds': 900, 'entry_offset_seconds': 960, 'entry_window_seconds': 10,
          'selection_window_seconds': 60, 'exit': 'official_settlement', 'fee_coefficient': '0.07',
          'balance_precision': '0.0001', 'fee_precision': '0.000001', 'loss_halt': False,
          'fill': 'observed ask depth; whole contracts; independent alternative accounts',
          'mark_interval_seconds': 10, 'live_orders': False}

def canonical(v): return json.dumps(v, sort_keys=True, separators=(',', ':'), allow_nan=False)
def digest(v): return hashlib.sha256(canonical(v).encode()).hexdigest()
def dec(v):
    if isinstance(v, bool): raise ValueError('boolean numeric')
    d = Decimal(str(v))
    if not d.is_finite(): raise ValueError('nonfinite number')
    return d

def levels(rows, reverse=False):
    out = []
    for r in rows:
        p, q = dec(r['price']), dec(r['quantity'])
        if not 0 <= p <= 1 or q < 0: raise ValueError('invalid book level')
        if q: out.append((p, q))
    return sorted(out, reverse=reverse)

def execution(rows, quantity, buy=True, precision=Decimal('.0001')):
    """Per-price fill fee + order-level rounding accumulator, using published mechanics."""
    remaining = dec(quantity); premium = fee_total = accumulator = Decimal(0); fills = []
    for p, available in rows:
        q = min(remaining, available)
        if q <= 0: continue
        revenue = (-1 if buy else 1) * p * q
        trade_fee = (Decimal('.07') * q * p * (1-p)).quantize(Decimal('.000001'), rounding=ROUND_CEILING)
        aligned = (revenue-trade_fee).quantize(precision, rounding=ROUND_FLOOR)
        rounding_fee = revenue-trade_fee-aligned; accumulator += rounding_fee
        rebate = min((accumulator/precision).to_integral_value(rounding=ROUND_FLOOR),
                     ((trade_fee+rounding_fee)/precision).to_integral_value(rounding=ROUND_FLOOR)) * precision
        accumulator -= rebate; net_fee = trade_fee+rounding_fee-rebate
        premium += p*q; fee_total += net_fee
        fills.append({'price': str(p), 'quantity': str(q), 'trade_fee': str(trade_fee),
                      'rounding_fee': str(rounding_fee), 'rebate': str(rebate), 'net_fee': str(net_fee)})
        remaining -= q
        if not remaining:
            return {'quantity': int(quantity), 'premium': str(premium), 'fee': str(fee_total),
                    'amount': str(premium+fee_total if buy else max(Decimal(0), premium-fee_total)),
                    'vwap': str(premium/dec(quantity)), 'fills': fills}
    return None

class Engine:
    def __init__(self, path, started_at):
        self.path = Path(path); self.started_at = float(started_at)
        if not math.isfinite(self.started_at) or self.path.is_symlink(): raise ValueError('invalid ledger identity')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, isolation_level=None); self.db.row_factory = sqlite3.Row
        self.db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sources(hash TEXT PRIMARY KEY,at REAL NOT NULL,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS selections(hour INTEGER PRIMARY KEY,at REAL NOT NULL,payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS decisions(hour INTEGER PRIMARY KEY,at REAL NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS accounts(arm TEXT PRIMARY KEY,cash TEXT NOT NULL,position TEXT,realized TEXT NOT NULL,fees TEXT NOT NULL,entries INTEGER NOT NULL,wins INTEGER NOT NULL,losses INTEGER NOT NULL,peak TEXT NOT NULL,max_dd TEXT NOT NULL,mark_peak TEXT NOT NULL,mark_dd TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS settlements(ticker TEXT PRIMARY KEY,result INTEGER NOT NULL,at REAL NOT NULL,hash TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,at REAL NOT NULL,kind TEXT NOT NULL,arm TEXT,payload TEXT NOT NULL);
        ''')
        with self.tx():
            identity = canonical({'config': CONFIG, 'started_at': self.started_at})
            old = self.db.execute("select value from meta where key='identity'").fetchone()
            if old and old[0] != identity: raise ValueError('frozen identity differs')
            if not old:
                if self.db.execute('select count(*) from accounts').fetchone()[0]: raise ValueError('unknown ledger')
                self.db.execute('insert into meta values(?,?)', ('identity', identity))
                self.db.executemany('insert into accounts values(?,?,NULL,?,?,0,0,0,?,?,?,?)',
                                    [(a,'500.0000','0','0','500','0','500','0') for a in ARMS])
            if {r[0] for r in self.db.execute('select arm from accounts')} != set(ARMS): raise ValueError('account set differs')
    @contextlib.contextmanager
    def tx(self):
        self.db.execute('BEGIN IMMEDIATE')
        try: yield; self.db.commit()
        except BaseException: self.db.rollback(); raise
    def event(self, event_at, kind, arm=None, **payload):
        self.db.execute('insert into events(at,kind,arm,payload) values(?,?,?,?)', (event_at,kind,arm,canonical(payload)))
    def source(self, receipt):
        h = digest(receipt)
        self.db.execute('insert or ignore into sources values(?,?,?)', (h,receipt['received'],canonical(receipt)))
        return h
    def selection(self, hour, at, market):
        if not self.started_at <= hour <= at < hour+60 or market['hour_start'] != hour: raise ValueError('nonprospective selection')
        with self.tx():
            old = self.db.execute('select payload from selections where hour=?',(hour,)).fetchone()
            if old:
                if old[0] != canonical(market): raise ValueError('selection conflict')
                return
            self.db.execute('insert into selections values(?,?,?)',(hour,at,canonical(market)))
    def decision(self, hour, at, signal, blocker=None):
        marketrow = self.db.execute('select payload from selections where hour=?',(hour,)).fetchone()
        if not marketrow or not hour+900 <= at < hour+960 or hour < self.started_at: raise ValueError('nonprospective decision')
        signal_time=signal.get('decision_time')
        if isinstance(signal_time,str): signal_time=datetime.datetime.fromisoformat(signal_time.replace('Z','+00:00')).timestamp()
        if signal_time != hour+900: raise ValueError('wrong signal timestamp')
        with self.tx():
            if self.db.execute('select 1 from decisions where hour=?',(hour,)).fetchone(): return
            p = {'market': json.loads(marketrow[0]), 'signal': signal, 'blocker': blocker}
            state = 'waiting' if signal.get('qualifies') and signal.get('signal') in (-1,1) and not blocker else 'skipped'
            self.db.execute('insert into decisions values(?,?,?,?)',(hour,at,canonical(p),state))
            self.event(at,'decision',hour=hour,state=state,**p)
    def skip(self, hour, at, reason):
        with self.tx():
            r = self.db.execute('select state from decisions where hour=?',(hour,)).fetchone()
            if r and r[0]=='waiting':
                self.db.execute("update decisions set state='skipped' where hour=?",(hour,))
                self.event(at,'skip',hour=hour,reason=reason)
    def fill(self, hour, receipt, book, now):
        r = self.db.execute('select * from decisions where hour=?',(hour,)).fetchone()
        if not r or r['state']!='waiting': return
        received = float(receipt['received']); start = float(receipt['request_started'])
        if not hour+960 <= start <= received <= now < hour+970 or now-received > 1: raise ValueError('entry quote outside frozen window/freshness')
        p = json.loads(r['payload']); market = p['market']; side = 'yes' if p['signal']['signal']==1 else 'no'
        asks = levels(book[side+'_asks']); bids = levels(book[side+'_bids'], True)
        if bids and asks and bids[0][0] >= asks[0][0]: raise ValueError('crossed book')
        asks = [(px,q) for px,q in asks if 0<px<1]
        h = self.source(receipt)
        with self.tx():
            for a in self.db.execute('select * from accounts order by arm').fetchall():
                if a['position']:
                    self.event(now,'skip',a['arm'],hour=hour,reason='unsettled_position'); continue
                cash = dec(a['cash']); budget = cash*dec(ARMS[a['arm']]); depth = int(sum((q for _,q in asks),Decimal(0)))
                low, high, best = 1, depth, None
                while low <= high:
                    q = (low+high)//2; e = execution(asks,q)
                    if dec(e['amount']) <= budget: best=e;low=q+1
                    else: high=q-1
                if best is None:
                    self.event(now,'skip',a['arm'],hour=hour,reason='insufficient_cash_or_depth'); continue
                pos = {**best,'ticker':market['ticker'],'close':market['close'],'side':side,'market':market,
                       'at':received,'hour':hour,'source_hash':h,'signal':p['signal'],'cash_before':str(cash),'budget':str(budget),
                       'entry_delay_seconds':received-(hour+960),'provenance':'simulated fill at observed public ask depth'}
                self.db.execute('update accounts set cash=?,position=?,fees=?,entries=entries+1 where arm=?',
                                (str(cash-dec(best['amount'])),canonical(pos),str(dec(a['fees'])+dec(best['fee'])),a['arm']))
                self.event(received,'entry',a['arm'],**pos)
            self.db.execute("update decisions set state='processed' where hour=?",(hour,))
    def settle(self, ticker, result, receipt):
        if type(result) is not int or result not in (0,1): raise ValueError('invalid settlement')
        at = receipt['received']; h = self.source(receipt)
        with self.tx():
            old = self.db.execute('select result from settlements where ticker=?',(ticker,)).fetchone()
            if old:
                if old[0]!=result: raise ValueError('settlement conflict')
                return
            matches = [(a,json.loads(a['position'])) for a in self.db.execute('select * from accounts where position is not null')]
            matches = [(a,p) for a,p in matches if p['ticker']==ticker]
            if not matches: return
            if any(at<p['close'] for _,p in matches): raise ValueError('settlement before close')
            self.db.execute('insert into settlements values(?,?,?,?)',(ticker,result,at,h))
            for a,p in matches:
                won = (result==1)==(p['side']=='yes'); payout = Decimal(p['quantity'] if won else 0)
                cash = dec(a['cash'])+payout; pnl = payout-dec(p['amount']); peak=max(dec(a['peak']),cash)
                dd=max(dec(a['max_dd']), (peak-cash)/peak)
                self.db.execute('update accounts set cash=?,position=NULL,realized=?,wins=wins+?,losses=losses+?,peak=?,max_dd=? where arm=?',
                                (str(cash),str(dec(a['realized'])+pnl),int(won),int(not won),str(peak),str(dd),a['arm']))
                self.event(at,'settlement',a['arm'],ticker=ticker,result=result,payout=str(payout),net_profit=str(pnl),cash=str(cash),source_hash=h,position=p)
    def status(self, now, books=None):
        books=books or {}; accounts=[]
        with self.tx():
            for a in self.db.execute('select * from accounts order by arm').fetchall():
                row=dict(a); p=json.loads(a['position']) if a['position'] else None; row['position']=p
                nav=dec(a['cash']) if not p else None
                if p and p['ticker'] in books and now<p['close']:
                    quote=books[p['ticker']]
                    if 0<=now-quote['received']<=15:
                        mark=execution(levels(quote['book'][p['side']+'_bids'],True),p['quantity'],False)
                        if mark: nav=dec(a['cash'])+dec(mark['amount'])
                if nav is not None:
                    peak=max(dec(a['mark_peak']),nav); dd=max(dec(a['mark_dd']),(peak-nav)/peak)
                    self.db.execute('update accounts set mark_peak=?,mark_dd=? where arm=?',(str(peak),str(dd),a['arm']))
                    row['mark_peak']=str(peak);row['mark_dd']=str(dd)
                row.update({'nav':str(nav) if nav is not None else None,'unrealized_profit':str(nav-500-dec(a['realized'])) if nav is not None else None,
                            'allocation':ARMS[a['arm']], 'mark_missing':p is not None and nav is None})
                accounts.append(row)
        return {'accounts':accounts,'event_counts':dict(self.db.execute('select kind,count(*) from events group by kind')),
                'config':CONFIG,'started_at':self.started_at,'venue_orders':0}
