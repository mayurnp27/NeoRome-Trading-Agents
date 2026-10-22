"""Independent scalar/window reconstruction and exact-rational cash audit."""
from pathlib import Path
from fractions import Fraction as F
from datetime import datetime
import csv,gzip,json,hashlib,math
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def roll(a,n,how='mean'):
 out=np.full(len(a),np.nan)
 if len(a)<n:return out
 w=np.lib.stride_tricks.sliding_window_view(a,n);out[n-1:]=getattr(np,how)(w,axis=1)
 return out

def audit_target(folder,candle_path):
 raw=np.array(json.loads(candle_path.read_text()),float);lock=json.loads((folder/'selection-lock.json').read_text());proof=json.loads((folder/'prediction-lock.json').read_text());z=np.load(folder/'locked-predictions.npz')
 assert sha(folder/'selection-lock.json')==proof['selection_sha256'] and sha(folder/'locked-predictions.npz')==proof['predictions_sha256']
 ms=pd.read_csv(folder/'selected-markets.csv');start=pd.to_datetime(ms.start,utc=True).astype('int64').to_numpy()//10**9;yy=np.where(ms.result=='yes',1.,-1.)
 np.testing.assert_array_equal(yy,z['y']);np.testing.assert_array_equal(start,z['time']//10**9)
 settled=pd.to_datetime(ms.settlement_ts,utc=True,format='mixed');a=pd.Timestamp('2026-08-07',tz='UTC');b=pd.Timestamp('2026-09-12',tz='UTC');ii=pd.to_datetime(start,unit='s',utc=True)
 np.testing.assert_array_equal(z['train'],(ii<a)&(settled<a));np.testing.assert_array_equal(z['validation'],(ii>=a+pd.Timedelta(days=1))&(ii<b)&(settled<b))
 np.testing.assert_array_equal(z['evaluation'],ii>=b+pd.Timedelta(days=1));assert not (z['train']&z['evaluation']).any()
 bars={};cache={}
 for tf in (15,30,45,60):
  sec=tf*60;last=(raw[:,0].astype(np.int64)//sec+1)*sec;labels=np.arange(last.min(),last.max()+sec,sec);lookup={int(t):i for i,t in enumerate(raw[:,0])};data=np.full((len(labels),3),np.nan)
  for j,end in enumerate(labels):
   positions=[lookup.get(t) for t in range(int(end)-sec,int(end),300)]
   if any(p is None for p in positions):continue
   block=raw[positions];data[j]=[min(block[:,1]),max(block[:,2]),block[-1,4]]
  bars[tf]=(labels,data)
 def reconstruct(meta):
  key=meta['id']
  if key in cache:return cache[key]
  if 'components' in meta:
   xs=np.array([reconstruct(c) for c in meta['components']]);x=np.where((xs==xs[0]).all(0),xs[0],0.) if meta['rule']=='agree' else np.sign(xs.sum(0));x[~np.isfinite(xs).all(0)]=np.nan
  else:
   tf=meta['tf'];labels,data=bars[tf];lo,hi,close=data.T;rule=meta['rule']
   if key.startswith('ST:'):
    low=roll(lo,meta['n'],'min');high=roll(hi,meta['n'],'max')
    with np.errstate(divide='ignore',invalid='ignore'):rawk=100*(close-low)/(high-low)
    rawk[high==low]=np.nan;K=roll(rawk,meta['k']);D=roll(K,meta['d']);prev=np.r_[np.nan,K[:-1]];gap=K-D;pg=np.r_[np.nan,gap[:-1]];valid=np.isfinite(K)&np.isfinite(D)&np.isfinite(prev)&np.isfinite(pg)
    if rule=='level':value=np.sign(K-50)
    elif rule=='gap':value=np.sign(gap)
    elif rule=='k_slope':value=np.sign(K-prev)
    elif rule=='d_slope':value=np.sign(D-np.r_[np.nan,D[:-1]])
    elif rule=='cross':value=np.where((gap>0)&(pg<=0),1.,np.where((gap<0)&(pg>=0),-1.,0.))
    else:
     q=int(rule[4:]);value=np.where((prev<=q)&(K>prev),1.,np.where((prev>=100-q)&(K<prev),-1.,0.))
   else:
    gap=roll(close,meta['fast'])-roll(close,meta['slow']);pg=np.r_[np.nan,gap[:-1]];valid=np.isfinite(gap)&np.isfinite(pg)
    value=np.sign(gap) if rule=='state' else np.where((gap>0)&(pg<=0),1.,np.where((gap<0)&(pg>=0),-1.,0.))
   value[~valid]=np.nan;dec=start+meta['offset']*60;bar_end=dec//(tf*60)*(tf*60);pos=(bar_end-labels[0])//(tf*60);assert (pos>=0).all() and (pos<len(labels)).all();x=value[pos]*meta['orientation']
   if rule=='cross' or rule.startswith('zone'):x[dec%(tf*60)!=0]=0.
  cache[key]=x;return x
 differences=[]
 for j,(name,c) in enumerate(lock['selected'].items()):
  expected=reconstruct(c['meta']);actual=z[f's{j}'];same=(expected==actual)|(np.isnan(expected)&np.isnan(actual));bad=np.flatnonzero(~same)
  if len(bad):differences.append({'name':name,'count':len(bad),'first_times':[str(ii[k]) for k in bad[:5]]})
 assert not differences,differences
 counts={};nrows=0
 with gzip.open(folder/'trials.csv.gz','rt') as f:
  for row in csv.DictReader(f):
   key=(row['family'],row['tf'],row['offset']);counts[key]=counts.get(key,0)+1;nrows+=1
 assert nrows==1875264
 assert all(v==(38400 if k[0]=='stochastic' else 78804) for k,v in counts.items()) and len(counts)==32
 results=json.loads((folder/'results.json').read_text());h=z['evaluation']
 for j,(name,c) in enumerate(lock['selected'].items()):
  x=z[f's{j}'][h];y=yy[h];active=np.isfinite(x)&(x!=0);m=results['selected'][name]['evaluation'];assert int(active.sum())==m['active'] and int(((x==y)&active).sum())==m['wins']
 return {'hours':len(ms),'selected_predictions':len(lock['selected']),'independently_reconstructed_cells':len(ms)*len(lock['selected']),'trial_rows':nrows,'target_groups':len(counts),'mismatches':0,'source_candles_sha256':sha(candle_path)}

def ceiling(v,grid):return F(-(-v//grid))*grid

def audit_cash():
 decisions=trades=events=0;accounts=[]
 quotes={t:{c['end_period_ts']:c for c in cs} for t,cs in json.loads((R/'data/quotes.json').read_text()).items()}
 markets={m['ticker']:m for m in json.loads((R/'data/quote-market-universe.json').read_text())}
 for path in sorted((R/'accounts').glob('*/*/account.json')):
  account=json.loads(path.read_text());folder=path.parent;ts=[json.loads(l) for l in (folder/'trades.jsonl').read_text().splitlines()];es=[json.loads(l) for l in (folder/'events.jsonl').read_text().splitlines()];ds=[json.loads(l) for l in (folder/'decisions.jsonl').read_text().splitlines()]
  cash=F(500);previous=None;grid=F(1,100) if account['scenario']=='cent_precision' else F(1,10000)
  def fee(q,p):return ceiling(q*p+ceiling(F(7,100)*q*p*(1-p),F(1,1000000)),grid)-q*p
  for j,t in enumerate(ts):
   q=t['quantity'];p=F(t['price']);before=F(t['cash_before']);budget=before/4
   entry=int(pd.Timestamp(t['entry_time']).timestamp());book=quotes[t['ticker']][entry]
   price=F(book['yes_ask']['close_dollars']) if t['side']=='yes' else 1-F(book['yes_bid']['close_dollars'])
   if account['scenario']=='plus_one_cent':price=min(F(1),price+F(1,100))
   assert p==price and t['quote_end_period_ts']==entry
   assert t['won']==((t['side']=='yes')==(markets[t['ticker']]['result']=='yes'))
   assert F(t['fee'])==fee(q,p);assert F(t['debit'])==q*p+fee(q,p)<=budget
   assert (q+1)*p+fee(q+1,p)>budget
   assert F(t['payout'])==(q if t['won'] else 0)
   assert F(t['cash_after_entry'])==before-F(t['debit']) and F(t['net_pnl'])==F(t['payout'])-F(t['debit'])
   assert pd.Timestamp(t['signal_time'])<pd.Timestamp(t['entry_time'])<pd.Timestamp(t['close_time'])<=pd.Timestamp(t['settlement_time'])
   if previous:assert pd.Timestamp(previous['settlement_time'])<=pd.Timestamp(t['entry_time'])
   previous=t
   assert F(es[2*j]['delta'])==-F(t['debit']) and F(es[2*j]['cash_after'])==before-F(t['debit'])
   assert F(es[2*j+1]['delta'])==F(t['payout']) and F(es[2*j+1]['cash_after'])==F(t['cash_after_settlement'])
  for e in es:
   cash+=F(e['delta']);assert cash==F(e['cash_after']) and cash>=0
  assert abs(float(cash)-account['ending_cash'])<1e-8;assert len(es)==2*len(ts)
  decisions+=len(ds);trades+=len(ts);events+=len(es);accounts.append({'strategy':account['strategy'],'scenario':account['scenario'],'ending_cash':float(cash)})
 return {'accounts':accounts,'decisions':decisions,'trades':trades,'cash_events':events,'exact_rational_audit':'passed'}

def main():
 out={'kalshi':audit_target(R,R/'data/candles.json'),'polymarket':audit_target(R/'polymarket',R/'data/binance-candles.json'),'cash':audit_cash(),'focused_tests':6}
 (R/'verification.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
if __name__=='__main__':main()
