"""Only run after immutable model and prediction selection locks exist."""
from pathlib import Path
import json,hashlib
import numpy as np
import pandas as pd
from study import R,metrics,dump,digest

def block_intervals(x,y,days,baseline=None):
 unique=np.unique(days);stats=[]
 for d in unique:
  take=days==d;xx=x[take];yy=y[take];valid=np.isfinite(xx);a=valid&(xx!=0);v=np.where(valid,xx,0.)
  row=[valid.sum(),v.sum(),yy[valid].sum(),(v*v).sum(),(v*yy).sum(),a.sum(),((v==yy)&a).sum()]
  if baseline is not None:
   bb=baseline[take];match=a&np.isfinite(bb)&(bb!=0);row.extend([match.sum(),((xx==yy)&match).sum(),((bb==yy)&match).sum()])
  stats.append(row)
 stats=np.array(stats,float);rng=np.random.default_rng(20261018);tot=stats[rng.integers(0,len(unique),(2000,len(unique)))].sum(1)
 n,sx,sy,sxx,sxy,a,w=tot[:,:7].T
 with np.errstate(divide='ignore',invalid='ignore'):
  cor=(sxy-sx*sy/n)/np.sqrt((sxx-sx*sx/n)*(n-sy*sy/n));acc=w/a
 def bounds(v):
  v=v[np.isfinite(v)];return np.quantile(v,[.025,.975]).tolist() if len(v) else [None,None]
 out={'blocks':len(unique),'resamples':2000,'accuracy95':bounds(acc),'correlation95':bounds(cor)}
 if baseline is not None:
  with np.errstate(divide='ignore',invalid='ignore'):adv=(tot[:,8]-tot[:,9])/tot[:,7]
  out['paired_accuracy_advantage95']=bounds(adv)
 return out

def main():
 lock=json.loads((R/'selection-lock.json').read_text());proof=json.loads((R/'prediction-lock.json').read_text())
 assert digest(R/'selection-lock.json')==proof['selection_sha256'] and digest(R/'locked-predictions.npz')==proof['predictions_sha256']
 z=np.load(R/'locked-predictions.npz');y=z['y'];h=z['evaluation'];ii=pd.to_datetime(z['time'],utc=True);days=ii[h].floor('D').asi8;majority=1 if y[z['train']].mean()>=0 else -1
 results={};baselines={}
 for off in (0,15,29):
  baselines[str(off)]={name:metrics(z[f'baseline_{off}_{name}'][h],y[h]) for name in ('spot_strike','momentum60')}
 baselines['majority']={'direction':majority,**metrics(np.full(h.sum(),majority),y[h])}
 for i,(key,c) in enumerate(lock['selected'].items()):
  x=z[f's{i}'][h];yy=y[h];off=c['meta']['offset'];b=z[f'baseline_{off}_spot_strike'][h];active=np.isfinite(x)&(x!=0);same=active&np.isfinite(b)&(b!=0)
  row=dict(c);row['evaluation']=metrics(x,yy);row['spot_on_same_trades']=metrics(b[active],yy[active]) if active.any() else None
  row['majority_on_same_trades']=float((yy[active]==majority).mean()) if active.any() else None
  row['paired_accuracy_advantage_vs_spot']=float((x[same]==yy[same]).mean()-(b[same]==yy[same]).mean()) if same.any() else None
  row['bootstrap']=block_intervals(x,yy,days,b)
  row['thirds']=[metrics(x[chunk],yy[chunk]) for chunk in np.array_split(np.arange(len(x)),3)]
  results[key]=row
 dump('results.json',{'period':['2026-04-21','2026-10-18'],'evaluation_period':['2026-09-13','2026-10-18'],'evaluation_status':'chronological exploratory; this time period was previously inspected for 15-minute research','data':json.loads((R/'data-summary.json').read_text()),'grid_trials':lock['grid_trials'],'combination_trials':lock['combination_trials'],'baselines':baselines,'selected':results,'selection_sha256':digest(R/'selection-lock.json'),'predictions_sha256':digest(R/'locked-predictions.npz')})
 for key in ('overall/stochastic','overall/sma','overall/mixed'):
  c=results[key];print(key,c['meta']['id'],c['evaluation'],c['bootstrap'],flush=True)
if __name__=='__main__':
 import sys,study
 if '--polymarket' in sys.argv:
  R=R/'polymarket';study.R=R
 main()
