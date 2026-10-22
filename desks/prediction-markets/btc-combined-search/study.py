from pathlib import Path
import importlib.util,itertools,json,hashlib
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent; S=R.parent/'btc-stochastic-search'
spec=importlib.util.spec_from_file_location('sma_study',R.parent/'btc-sma-search/study.py');sma=importlib.util.module_from_spec(spec);spec.loader.exec_module(sma); p=sma.prior

def agree(*arrays):
 xs=np.stack(arrays);valid=np.isfinite(xs).all(axis=0)
 return np.where(valid,np.where((xs==xs[0]).all(axis=0),xs[0],0.),np.nan)

def paired(x,anchor,y,index):
 valid=np.isfinite(x)&np.isfinite(anchor);a=anchor.copy();a[~valid]=np.nan
 days=index.floor('D');groups=[np.flatnonzero(days==d) for d in days.unique()];rng=np.random.default_rng(20261018);vals=[]
 def diff(ix):
  xx=x[ix];aa=a[ix];yy=y[ix];m=p.metrics(xx,yy);b=p.metrics(aa,yy)
  reject=np.isfinite(xx)&(xx==0)&np.isfinite(aa)&(aa!=0)
  r=float((np.sign(aa[reject])==yy[reject]).mean()) if reject.any() else np.nan
  return [m['accuracy']-b['accuracy'] if m['accuracy'] is not None and b['accuracy'] is not None else np.nan,m['correlation']-b['correlation'],m['accuracy']-r if m['accuracy'] is not None else np.nan]
 for _ in range(2000):
  ix=np.concatenate([groups[j] for j in rng.integers(0,len(groups),len(groups))]);vals.append(diff(ix))
 return {'anchor_same_valid_universe':sma.detail(a,y),'point_differences':diff(np.arange(len(x))),'difference_order':['accuracy_vs_anchor','correlation_vs_anchor','accuracy_retained_minus_rejected'],'daily_block_95':np.nanquantile(vals,[.025,.975],axis=0).T.tolist()}

def main():
 rows=json.loads((S/'data/candles.json').read_text());raw=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume']);raw.index=pd.to_datetime(raw.timestamp,unit='s',utc=True);raw=raw.reindex(pd.date_range(raw.index.min(),raw.index.max(),freq='5min'))
 ms=pd.read_csv(S/'matched-outcomes.csv');ix=pd.DatetimeIndex(pd.to_datetime(ms.start,utc=True));y=np.where(ms.result=='yes',1.,-1.)
 old=json.loads((S/'results.json').read_text());a,b=map(pd.Timestamp,old['data']['split_boundaries']);masks={'train':ix<a,'validation':(ix>=a+pd.Timedelta(days=1))&(ix<b),'final_reused':ix>=b+pd.Timedelta(days=1)}
 scores={};stoch={};sm={};meta={}
 for tf in (5,15,30):
  bars=p.aggregate(raw,tf);c=old['selected'][str(tf)]['meta'];fs=p.features(bars,c['n'],c['k'],c['d'])
  event=f'stoch:{tf}m:event';state=f'stoch:{tf}m:state';scores[event]=p.align(fs[c['rule']],ix,True);scores[state]=np.sign(p.align(fs['gap' if c['rule']=='cross' else 'k_slope'],ix));stoch[tf]=(event,state)
  meta[event]=c;meta[state]={'description':'K-D sign' if tf==5 else 'K slope sign'}
  for fast,slow in ((2,5),(5,10)):
   ce,cs=sma.signals(bars,fast,slow);en=f'sma:{tf}m:{fast}/{slow}:cross';sn=f'sma:{tf}m:{fast}/{slow}:state';scores[en]=p.align(ce,ix,True);scores[sn]=p.align(cs,ix);sm[(tf,fast,slow)]=(en,sn)
 candidates=[]
 def add(recipe,anchor,parts):
  name=recipe+'|'+'|'.join(parts);scores[name]=agree(*(scores[k] for k in parts));c={'id':name,'recipe':recipe,'anchor':anchor,'components':parts}
  c.update({split:sma.detail(scores[name][mask],y[mask]) for split,mask in masks.items() if split!='final_reused'});candidates.append(c)
 for tf,(se,ss) in stoch.items():
  for key,(me,mt) in sm.items():
   add('both_events',se,[se,me]);add('stoch_event_sma_state',se,[se,mt]);add('sma_event_stoch_state',me,[me,ss])
  for pair in itertools.combinations((5,15,30),2):
   for choices in itertools.product(((2,5),(5,10)),repeat=2):
    parts=[se]+[sm[(t,*lengths)][1] for t,lengths in zip(pair,choices)];add('stoch_event_two_sma_states',se,parts)
 assert len(candidates)==90
 eligible=[c for c in candidates if c['train']['active']>=200 and c['validation']['active']>=100]
 winners={recipe:max((c for c in eligible if recipe=='overall' or c['recipe']==recipe),key=lambda c:c['validation']['correlation'],default=None) for recipe in ['overall']+sorted({c['recipe'] for c in candidates})}
 selection={k:v['id'] if v else None for k,v in winners.items()};(R/'selection-lock.json').write_text(json.dumps({'selection':selection,'candidates':candidates},indent=2))
 h=masks['final_reused'];pred=pd.DataFrame({'start':ix[h],'outcome':y[h]});baselines={}
 for name,x in scores.items():
  pred[name]=x[h]
  if '|' not in name:baselines[name]=sma.detail(x[h],y[h])
 for c in candidates:
  x=scores[c['id']][h];c['final_reused']=sma.detail(x,y[h]);c['bootstrap']=p.bootstrap(x,y[h],ix[h]) if c['final_reused']['active'] else None
  if c['id'] in selection.values():
   c['paired_comparison']=paired(x,scores[c['anchor']][h],y[h],ix[h]);c['subperiods']=[sma.detail(x[z],y[h][z]) for z in np.array_split(np.arange(h.sum()),3)]
 result={'data':old['data'],'status':'exploratory reused evaluation, no fresh holdout','trial_count':len(candidates),'selection':selection,'stochastic_components':meta,'baselines':baselines,'candidates':candidates,'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
 (R/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False));pred.to_csv(R/'final-predictions.csv',index=False)
 print(json.dumps({'selection':selection,'selected_results':[c for c in candidates if c['id'] in selection.values()]},indent=2))
if __name__=='__main__':main()
