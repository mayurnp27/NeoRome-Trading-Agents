"""Apply one frozen rule; write prediction lock before quote collection/P&L."""
from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,importlib.util,collections
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent;SRC=R.parent/'btc-hourly-early-entry';D=R/'data'
s=importlib.util.spec_from_file_location('original_study',SRC/'study.py');st=importlib.util.module_from_spec(s);s.loader.exec_module(st)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False))
def main():
 protocol=json.loads((R/'protocol.json').read_text());info={}
 old=pd.read_csv(SRC/'selected-markets.csv');oldstarts=set(pd.to_datetime(old.start,utc=True));oldtickers=set(old.ticker)
 for name,source in [('older_transfer','older'),('new_day','new')]:
  period=protocol['periods'][name];lo=pd.Timestamp(period['start']);hi=pd.Timestamp(period['end']);root=R/name;root.mkdir(exist_ok=True)
  rows=json.loads((D/(source+'-candles.json')).read_text());raw=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume']);raw.index=pd.to_datetime(raw.timestamp,unit='s',utc=True);assert not raw.index.duplicated().any();raw=raw.reindex(pd.date_range(raw.index.min(),raw.index.max(),freq='5min'))
  assert ((raw.low<=raw[['open','close']].min(axis=1))&(raw.high>=raw[['open','close']].max(axis=1))|raw.close.isna()).all()
  closed=raw.close.copy();closed.index+=pd.Timedelta(minutes=5);best={};excluded=collections.Counter()
  for m in json.loads((D/(source+'-markets.json')).read_text()):
   end=pd.Timestamp(m['close_time']);start=end-pd.Timedelta(hours=1)
   if not lo<=start<hi:excluded['outside_window']+=1;continue
   if end.value%(3600*10**9) or m.get('strike_type')!='greater' or m.get('floor_strike') is None:excluded['wrong_shape']+=1;continue
   if pd.Timestamp(m['created_time'])>start or pd.Timestamp(m['open_time'])>start:excluded['not_available_at_start']+=1;continue
   spot=closed.get(start,np.nan)
   if not np.isfinite(spot):excluded['missing_start_price']+=1;continue
   strike=float(m['floor_strike']);key=(abs(strike-spot),strike,m['ticker'])
   if start not in best or key<best[start][0]:best[start]=(key,m)
  index=pd.DatetimeIndex(sorted(best));assert not set(index)&oldstarts;selected=[best[t][1] for t in index];assert not set(m['ticker'] for m in selected)&oldtickers
  decision=index+pd.Timedelta(minutes=15);prices=closed.reindex(decision).to_numpy();sigmas=(closed/closed.shift()-1).rolling(288,min_periods=288).std(ddof=1).reindex(decision).to_numpy();strikes=np.array([float(m['floor_strike']) for m in selected]);distance=abs(prices-strikes)/(prices*sigmas*np.sqrt(9))
  b15=st.aggregate(raw,15);b45=st.aggregate(raw,45);sto=st.align_matrix(st.stoch_values(b15,67,1,1)['d_slope'],b15,index,False)[1]
  sums=st.cent_sums(b45);gap=sums[2]*3-sums[3]*2;prev=np.roll(gap,1);prev[0]=np.nan;cross=np.where((gap>0)&(prev<=0),1.,np.where((gap<0)&(prev>=0),-1.,0.));cross[~(np.isfinite(gap)&np.isfinite(prev))]=np.nan;ma=st.align_matrix(cross,b45,index,True)[1]
  signal=np.sign(sto+ma);valid=np.isfinite(signal)&np.isfinite(distance)&(sigmas>0);qualifies=valid&(signal!=0)&(distance>=1);pred=[]
  for i,(start,m) in enumerate(zip(index,selected)):
   pred.append({'ticker':m['ticker'],'market_start':start.isoformat(),'decision_time':decision[i].isoformat(),'entry_time':(decision[i]+pd.Timedelta(minutes=1)).isoformat(),'strike':str(m['floor_strike']),'spot':float(prices[i]) if np.isfinite(prices[i]) else None,'sigma5m':float(sigmas[i]) if np.isfinite(sigmas[i]) else None,'distance':float(distance[i]) if np.isfinite(distance[i]) else None,'stochastic':float(sto[i]) if np.isfinite(sto[i]) else None,'sma':float(ma[i]) if np.isfinite(ma[i]) else None,'raw_signal':float(signal[i]) if np.isfinite(signal[i]) else None,'signal':int(signal[i]) if qualifies[i] else 0,'qualifies':bool(qualifies[i])})
  write(root/'selected-markets.json',selected);write(root/'predictions.json',pred)
  expected=pd.date_range(lo,hi,freq='h',inclusive='left');missing=expected.difference(index)
  info[name]={'hours_expected':len(expected),'hours_selected':len(index),'missing_hours':[str(t) for t in missing],'selection_exclusions':dict(excluded),'candidate_count':int(qualifies.sum()),'valid_features':int(valid.sum()),'missing_source5m':int(raw.close.isna().sum()),'first':str(index.min()),'last':str(index.max()),'zero_target_overlap_with_original':True,'predictions_sha256':sha(root/'predictions.json'),'markets_sha256':sha(root/'selected-markets.json'),'candles_sha256':sha(D/(source+'-candles.json'))}
  print(name,'selected',len(index),'expected',len(expected),'qualified',qualifies.sum(),'missing candles',raw.close.isna().sum(),flush=True)
 write(R/'prediction-lock.json',{'frozen_at':datetime.now(timezone.utc).isoformat(),'protocol_sha256':sha(R/'protocol.json'),'features_sha256':sha(R/'features.py'),'source_study_sha256':sha(SRC/'study.py'),'performance_evaluated':False,'periods':info})
if __name__=='__main__':main()
