"""Replay frozen Kalshi indicator winners using previously audited account arithmetic."""
from pathlib import Path
import importlib.util,json,csv,hashlib
import numpy as np
from study import R,dump,digest
from evaluate import block_intervals
import pandas as pd
source=R.parent/'btc-priced-paper/replay.py'
spec=importlib.util.spec_from_file_location('verified_account_engine',source);engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)

def main():
 proof=json.loads((R/'prediction-lock.json').read_text());assert digest(R/'selection-lock.json')==proof['selection_sha256'] and digest(R/'locked-predictions.npz')==proof['predictions_sha256']
 series=json.loads((R/'data/series.json').read_text())['data']['series'];history=json.loads((R/'data/fee-history.json').read_text())['data'];events=json.loads((R/'data/event-fee-history.json').read_text())['data']
 assert series['fee_type']=='quadratic' and series['fee_multiplier']==1
 assert not history['series_fee_change_arr'] and not events.get('cursor') and not [e for e in events['event_fee_changes'] if e['series_ticker']=='KXBTCD']
 lock=json.loads((R/'selection-lock.json').read_text());z=np.load(R/'locked-predictions.npz');h=z['evaluation'];selected=list(lock['selected']);markets=list(csv.DictReader((R/'selected-markets.csv').open()))
 full={m['ticker']:m for m in json.loads((R/'data/quote-market-universe.json').read_text())}
 assert set(m['ticker'] for i,m in enumerate(markets) if h[i])==set(full), 'quote selection must equal completed full dataset'
 qs=json.loads((R/'data/quotes.json').read_text());quotes={t:{c['end_period_ts']:c for c in cs} for t,cs in qs.items()}
 assert all(len(quotes[t])==len(qs[t]) for t in qs)
 engine.R=R;accounts=[];shadows={}
 for family in ('stochastic','sma','mixed'):
  key='overall/'+family;c=lock['selected'][key];off=c['meta']['offset'];assert off+1<=30;x=z[f's{selected.index(key)}'];engine.STRATEGIES={family:(off,'signal')}
  inputs=[({'market_start':m['start'],'signal':float(x[i])},full[m['ticker']]) for i,m in enumerate(markets) if h[i]]
  for scenario in ('delayed_close','plus_one_cent','cent_precision'):
   out=engine.run_account(family,scenario,inputs,quotes);out['selected_id']=c['meta']['id'];out['offset']=off;out['entry_offset_minutes']=off+1;accounts.append(out)
  reference=[]
  for r,m in inputs:
   side=r['signal']
   if not np.isfinite(side) or side==0:continue
   entry=int(engine.stamp(r['market_start']))+off*60+60
   price,reason=engine.quote_price(quotes.get(m['ticker'],{}).get(entry),int(side),'delayed_close')
   if reason:continue
   won=(side==1)==(m['result']=='yes');fee=engine.fee(1,price);net=engine.D(int(won))-price-fee
   reference.append({'ticker':m['ticker'],'start':r['market_start'],'won':won,'price':str(price),'fee':str(fee),'net':str(net)})
  (R/(family+'-unit-shadow.json')).write_text(json.dumps(reference))
  if reference:
   frame=pd.DataFrame(reference);day=pd.to_datetime(frame.start,utc=True,format='mixed').dt.floor('D');unique=day.unique();stats=np.array([[sum(float(v['net']) for v in reference if pd.Timestamp(v['start']).floor('D')==d),sum(pd.Timestamp(v['start']).floor('D')==d for v in reference)] for d in unique])
   rng=np.random.default_rng(20261018);sample=stats[rng.integers(0,len(stats),(2000,len(stats)))].sum(1);ci=np.quantile(sample[:,0]/sample[:,1],[.025,.975]).tolist()
   shadows[family]={'signals':len(reference),'win_rate':sum(v['won'] for v in reference)/len(reference),'mean_price':sum(float(v['price']) for v in reference)/len(reference),'mean_fee':sum(float(v['fee']) for v in reference)/len(reference),'mean_net':sum(float(v['net']) for v in reference)/len(reference),'mean_net_daily_block95':ci}
 result={'initial_cash_per_account':500,'allocation':.25,'fee_model':'ceil6dp(.07*q*p*(1-p)), total debit rounded upward to .0001 (.01 sensitivity)','accounts':accounts,'unit_shadows':shadows,'quote_markets':len(quotes),'source_engine':str(source.relative_to(R.parent)),'source_engine_sha256':digest(source),'quotes_sha256':digest(R/'data/quotes.json'),'execution':'hypothetical full fill at signal+60sec historical ask; entry minute1/16/30; at least30min to close; one unsettled position maximum; no historical depth; no delayed replacement fills','polymarket':'No executable historical bid/ask archive established; no actual-entry profit replay claimed.'}
 dump('paper-results.json',result)
 for c in accounts:print(c['strategy'],c['scenario'],c['trades'],c['ending_cash'],c['win_rate'],c['one_contract_mean_net_all_eligible'],flush=True)
if __name__=='__main__':main()
