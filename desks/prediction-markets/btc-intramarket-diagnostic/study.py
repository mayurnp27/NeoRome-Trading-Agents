from pathlib import Path
import importlib.util,json,hashlib
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent;S=R.parent/'btc-stochastic-search'
spec=importlib.util.spec_from_file_location('prior',S/'search.py');p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

def run():
 rows=json.loads((S/'data/candles.json').read_text());f=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume']);f.index=pd.to_datetime(f.timestamp,unit='s',utc=True);f=f.reindex(pd.date_range(f.index.min(),f.index.max(),freq='5min'))
 # Right label: close is known no earlier than candle start + 5 minutes.
 prices=f.close.copy();prices.index+=pd.Timedelta(minutes=5);sigma=np.log(prices).diff().rolling(12).std(ddof=1)
 ms=pd.read_csv(S/'matched-outcomes.csv');ix=pd.DatetimeIndex(pd.to_datetime(ms.start,utc=True));y=np.where(ms.result=='yes',1.,-1.);strike=ms.floor_strike.to_numpy(float)
 final=ix>=pd.Timestamp('2026-09-13',tz='UTC');start_price=f.open.reindex(ix).to_numpy(float);cases=[];pred=pd.DataFrame({'market_start':ix,'outcome':y,'strike':strike})
 for minute in (5,10):
  decision=ix+pd.Timedelta(minutes=minute);price=prices.reindex(decision).to_numpy(float);vol=sigma.reindex(decision).to_numpy(float)
  raw=np.log(price/strike);den=vol*np.sqrt((15-minute)/5);z=raw/den;valid=np.isfinite(z)&(vol>0)
  pred[f'price_{minute}']=price;pred[f'sigma_{minute}']=vol;pred[f'z_{minute}']=z
  for threshold in (0,.5,1):
   score=np.where(valid,np.where(np.abs(z)>=threshold,np.sign(raw),0),np.nan);key=f'{minute}min_lead{threshold}';pred[key]=score
   cases.append({'id':key,'minutes_elapsed':minute,'lead_threshold':threshold,'full':p.metrics(score,y),'final_reused':p.metrics(score[final],y[final]),'bootstrap':p.bootstrap(score[final],y[final],ix[final])})
  proxy=np.sign(price-start_price);proxy[~valid]=np.nan;pred[f'spot_reference_{minute}']=proxy
  cases.append({'id':f'{minute}min_spot_start_reference','minutes_elapsed':minute,'reference':'Coinbase candle open at market start','full':p.metrics(proxy,y),'final_reused':p.metrics(proxy[final],y[final]),'bootstrap':p.bootstrap(proxy[final],y[final],ix[final])})
 out={'purpose':'retrospective information-timing diagnostic, not executable backtest','selection':'none; all fixed cases reported','cases':cases,'source_input_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [S/'data/candles.json',S/'matched-outcomes.csv']},'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
 (R/'results.json').write_text(json.dumps(out,indent=2,allow_nan=False));pred.to_csv(R/'predictions.csv',index=False)
 for c in cases:print(c['id'],c['final_reused'],c['bootstrap'])
if __name__=='__main__':run()
