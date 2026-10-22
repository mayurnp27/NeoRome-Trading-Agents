"""Frozen finite loss diagnostics. Original research artifacts remain immutable."""
from pathlib import Path
from decimal import Decimal as D, ROUND_FLOOR
from fractions import Fraction as F
from datetime import datetime, timezone
import json, hashlib, importlib.util, math
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent
SRC=R.parent/'btc-hourly-early-entry'
def module(name,path):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
study=module('study',SRC/'study.py')
e=module('engine',R.parent/'btc-priced-paper/replay.py')
def dump(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False))
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def summarize(rows):
 if not rows:return {'n':0}
 nets=np.array([float(r['net']) for r in rows]);days=sorted(set(r['day'] for r in rows))
 stats=np.array([[sum(float(r['net']) for r in rows if r['day']==d),sum(r['day']==d for r in rows)] for d in days])
 rng=np.random.default_rng(20261018);bs=stats[rng.integers(0,len(days),(2000,len(days)))].sum(1)
 return {'n':len(rows),'wins':sum(r['won'] for r in rows),'accuracy':sum(r['won'] for r in rows)/len(rows),'mean_price':np.mean([float(r['price']) for r in rows]),'mean_fee':np.mean([float(r['fee']) for r in rows]),'mean_net':nets.mean(),'net':float(sum((D(r['net']) for r in rows),D(0))),'mean_net_daily_block95':np.quantile(bs[:,0]/bs[:,1],[.025,.975]).tolist()}

def replay(name,rows,allocation,halt=None):
 cash=D(500);peak=cash;drawdown=D(0);pending=None;trades=[];skips={};stop_time=None;alloc=D(str(allocation))
 def settle():
  nonlocal cash,pending,peak,drawdown,stop_time
  if pending is None:return
  cash+=D(pending['payout']);pending['cash_after_settlement']=str(cash)
  peak=max(peak,cash);dd=(peak-cash)/peak;drawdown=max(drawdown,dd)
  if halt is not None and dd>=D(str(halt)) and stop_time is None:stop_time=pending['settlement_time']
  pending=None
 for row in rows:
  entry=e.stamp(row['entry_time'])
  if pending is not None and e.stamp(pending['settlement_time'])<=entry:settle()
  reason='halted' if stop_time else 'prior_position_unsettled' if pending else None
  if not reason:
   p=D(row['price']);budget=cash*alloc;q=int((budget/p).to_integral_value(rounding=ROUND_FLOOR))
   while q and q*p+e.fee(q,p)>budget:q-=1
   if q==0:reason='insufficient_budget'
  if reason:skips[reason]=skips.get(reason,0)+1;continue
  fee=e.fee(q,p);premium=q*p;debit=premium+fee;payout=D(q) if row['won'] else D(0)
  t={k:row[k] for k in ('ticker','entry_time','settlement_time','won','price')}
  t.update(quantity=q,cash_before=str(cash),budget=str(budget),premium=str(premium),fee=str(fee),debit=str(debit),payout=str(payout),net_pnl=str(payout-debit))
  cash-=debit;trades.append(t);pending=t
 settle()
 # Independent exact-rational fee, maximum integer quantity and settled-cash audit.
 bal=F(500)
 for t in trades:
  p=F(t['price']);q=t['quantity'];budget=bal*F(str(allocation))
  raw=F(math.ceil(F(7,100)*q*p*(1-p)*10**6),10**6)
  cost=F(math.ceil((q*p+raw)*10000),10000)
  nq=q+1;next_raw=F(math.ceil(F(7,100)*nq*p*(1-p)*10**6),10**6)
  next_cost=F(math.ceil((nq*p+next_raw)*10000),10000)
  assert F(t['cash_before'])==bal and F(t['debit'])==cost and cost<=budget<next_cost
  assert F(t['fee'])==cost-q*p and F(t['payout'])==(q if t['won'] else 0)
  bal+=F(t['payout'])-cost;assert F(t['cash_after_settlement'])==bal
 assert F(cash)==bal
 path=R/'accounts'/name;path.mkdir(parents=True,exist_ok=True)
 (path/'trades.jsonl').write_text(''.join(json.dumps(t)+'\n' for t in trades))
 out={'name':name,'eligible_signals':len(rows),'allocation':allocation,'ending_cash':float(cash),'trades':len(trades),'wins':sum(t['won'] for t in trades),'fees':float(sum((D(t['fee']) for t in trades),D(0))),'max_settled_cash_drawdown_pct':float(drawdown*100),'halt_time':stop_time,'skips':skips}
 dump(path/'account.json',out);return out

def main():
 protocol=json.loads((R/'protocol.json').read_text())
 z=np.load(SRC/'locked-predictions.npz');lock=json.loads((SRC/'selection-lock.json').read_text());keys=list(lock['selected'])
 ms=pd.read_csv(SRC/'selected-markets.csv');index=pd.DatetimeIndex(pd.to_datetime(ms.start,utc=True));decision=index+pd.Timedelta(minutes=15)
 qs=json.loads((SRC/'data/quotes.json').read_text());quotes={t:{c['end_period_ts']:c for c in cs} for t,cs in qs.items()}
 raw=pd.DataFrame(json.loads((SRC/'data/candles.json').read_text()),columns=['timestamp','low','high','open','close','volume']);raw.index=pd.to_datetime(raw.timestamp,unit='s',utc=True);raw=raw.reindex(pd.date_range(raw.index.min(),raw.index.max(),freq='5min'))
 bars15=study.aggregate(raw,15);bars45=study.aggregate(raw,45)
 st=study.align_matrix(study.stoch_values(bars15,67,1,1)['d_slope'],bars15,index,False)[1]
 sums=study.cent_sums(bars45);gap=sums[2]*3-sums[3]*2;prev=np.roll(gap,1);prev[0]=np.nan
 cross=np.where((gap>0)&(prev<=0),1.,np.where((gap<0)&(prev>=0),-1.,0.));cross[~(np.isfinite(gap)&np.isfinite(prev))]=np.nan
 ma=study.align_matrix(cross,bars45,index,True)[1]
 mixed=z['s'+str(keys.index('overall/mixed'))];np.testing.assert_array_equal(mixed,np.sign(st+ma))
 closed=raw.close.copy();closed.index+=pd.Timedelta(minutes=5)
 price=closed.reindex(decision).to_numpy();vol=(closed/closed.shift()-1).rolling(288,min_periods=288).std(ddof=1).reindex(decision).to_numpy()
 baseline=np.sign(price-ms.floor_strike.to_numpy());np.testing.assert_array_equal(baseline,z['baseline_15_spot_strike'])
 distance=abs(price-ms.floor_strike.to_numpy())/(price*vol*np.sqrt(9))
 families={};all_rows=[]
 for family in ('stochastic','sma','mixed'):
  selected=lock['selected']['overall/'+family];assert selected['meta']['offset']==15
  signal=z['s'+str(keys.index('overall/'+family))];rows=[]
  reference={r['ticker']:r for r in json.loads((SRC/(family+'-unit-shadow.json')).read_text())}
  for i in np.where(z['evaluation'])[0]:
   if not np.isfinite(signal[i]) or not signal[i]:continue
   m=ms.iloc[i];entry=decision[i]+pd.Timedelta(minutes=1);q=quotes[m.ticker][int(entry.timestamp())];p,reason=e.quote_price(q,int(signal[i]),'delayed_close');assert reason is None
   won=bool((signal[i]>0)==(m.result=='yes'));fee=e.fee(1,p);net=D(int(won))-p-fee
   r={'ticker':m.ticker,'start':str(index[i]),'day':str(index[i].date()),'entry_time':entry.isoformat(),'settlement_time':e.iso(e.stamp(m.settlement_ts)),'won':won,'price':str(p),'fee':str(fee),'net':str(net),'signal':int(signal[i])}
   old=reference[m.ticker];assert old['won']==won and D(old['price'])==p and D(old['net'])==net
   if family=='mixed':
    spread=D(q['yes_ask']['close_dollars'])-D(q['yes_bid']['close_dollars']);v=float(distance[i]) if np.isfinite(distance[i]) else None
    flags={'unfiltered':True,'align_benchmark':bool(signal[i]==baseline[i]),'both_indicators':bool(st[i]==ma[i] and st[i]!=0),'ask_le_60':p<=D('.60'),'ask_le_70':p<=D('.70'),'spread_le_3':spread<=D('.03'),'distance_ge_05':v is not None and v>=.5,'distance_ge_1':v is not None and v>=1}
    r.update(filters=flags,spot=float(price[i]),strike=float(m.floor_strike),stochastic=int(st[i]),sma=int(ma[i]),spread=str(spread),normalized_distance=v,spot_bar_end=decision[i].isoformat(),volatility_return_count=288)
   rows.append(r)
  assert len(rows)==len(reference);families[family]=rows
 all_rows=families['mixed'];assert len(all_rows)==774
 filters={}
 for name in all_rows[0]['filters']:
  rows=[r for r in all_rows if r['filters'][name]]
  periods={'all':rows,'early':[r for r in rows if r['day']<'2026-09-29'],'late':[r for r in rows if r['day']>='2026-09-29']}
  filters[name]={k:summarize(v) for k,v in periods.items()}
  filters[name]['account_25pct']=replay('filter_'+name,rows,.25)
 sizing={fam:[replay(f'{fam}_size_{alloc}',rows,alloc) for alloc in (.25,.05,.02,.01)] for fam,rows in families.items()}
 halt=replay('mixed_halt10pct',all_rows,.25,.10)
 original=json.loads((SRC/'paper-results.json').read_text())
 for fam in families:
  new=sizing[fam][0];old=next(x for x in original['accounts'] if x['strategy']==fam and x['scenario']=='delayed_close')
  assert new['ending_cash']==old['ending_cash'] and new['trades']==old['trades'] and new['wins']==old['wins']
  a=[json.loads(s) for s in (R/'accounts'/f'{fam}_size_0.25'/'trades.jsonl').read_text().splitlines()];b=[json.loads(s) for s in (SRC/'accounts'/fam/'delayed_close'/'trades.jsonl').read_text().splitlines()]
  for t,u in zip(a,b):
   for k in ('ticker','entry_time','settlement_time','won','price','quantity','cash_before','budget','premium','fee','debit','payout','net_pnl','cash_after_settlement'):assert t[k]==u[k],(fam,k)
 bins={}
 for low,high in zip(protocol['price_bins'],protocol['price_bins'][1:]):
  rows=[r for r in all_rows if D(str(low))<D(r['price'])<=D(str(high))]
  bins[f'({low},{high}]']=summarize(rows)
 result={'protocol':protocol,'filters':filters,'price_bins':bins,'sizing':sizing,'halt':halt,'verification':{'original_three25pct_ledgers_reproduced':True,'exact_rational_cash_and_maximum_quantity_audit':True,'mixed_components_match_entire_locked_array':True,'decision_price_matches_locked_baseline':True,'all774_shadow_rows_match':True},'source_hashes':{str(p.relative_to(R.parent)):digest(p) for p in [SRC/'selection-lock.json',SRC/'locked-predictions.npz',SRC/'selected-markets.csv',SRC/'data/candles.json',SRC/'data/quotes.json',R/'protocol.json',R/'analyze.py',R.parent/'btc-priced-paper/replay.py']},'generated_at':datetime.now(timezone.utc).isoformat()}
 (R/'observations.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in all_rows));dump(R/'results.json',result)
 for name,v in filters.items():print(name,[(k,v[k]['n'],round(v[k].get('accuracy',0)*100,2),round(v[k].get('mean_net',0)*100,3)) for k in ('all','early','late')], 'cash',v['account_25pct']['ending_cash'])
 print('SIZING',json.dumps(sizing));print('HALT',json.dumps(halt))
if __name__=='__main__':main()
