"""Fixed SMA rules; uses cached outcome study and no network or trading API."""
from pathlib import Path
import importlib.util,json,hashlib
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parent
SOURCE=ROOT.parent/'btc-stochastic-search'
spec=importlib.util.spec_from_file_location('prior_study',SOURCE/'search.py')
prior=importlib.util.module_from_spec(spec); spec.loader.exec_module(prior)

def signals(bars,fast,slow):
    gap=bars.close.rolling(fast).mean()-bars.close.rolling(slow).mean()
    old=gap.shift(1); valid=gap.notna()&old.notna()
    cross=pd.Series(np.where((gap>0)&(old<=0),1.,np.where((gap<0)&(old>=0),-1.,0.)),index=bars.index).where(valid)
    return cross,np.sign(gap)

def detail(x,y):
    out=prior.metrics(x,y)
    for name,sign in [('up',1),('down',-1)]:
        active=x==sign;out[name+'_signals']=int(active.sum());out[name+'_accuracy']=float((y[active]==sign).mean()) if active.any() else None
    if out['active']:
        out['best_constant_accuracy_active']=max(out['up_fraction_active'],1-out['up_fraction_active'])
    out['sparse']=out['active']<100
    return out

def run(units):
    # Load existing prepared outcomes directly; do not rewrite prior study artifacts.
    rows=json.loads((SOURCE/'data/candles.json').read_text())
    raw=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume'])
    raw.index=pd.to_datetime(raw.timestamp,unit='s',utc=True)
    raw=raw.reindex(pd.date_range(raw.index.min(),raw.index.max(),freq='5min'))
    ms=pd.read_csv(SOURCE/'matched-outcomes.csv'); index=pd.DatetimeIndex(pd.to_datetime(ms.start,utc=True)); y=np.where(ms.result=='yes',1.,-1.)
    old=json.loads((SOURCE/'results.json').read_text()); a,b=map(pd.Timestamp,old['data']['split_boundaries'])
    masks={'train':index<a,'validation':(index>=a+pd.Timedelta(days=1))&(index<b),'final_reused':index>=b+pd.Timedelta(days=1)}
    candidates=[]; scores={}
    for unit in units:
        for tf in (5,15,30):
            bars=prior.aggregate(raw,tf)
            for fast,slow in ((2,5),(5,10)):
                factor=1440//tf if unit=='days' else 1
                event,state=signals(bars,fast*factor,slow*factor)
                for rule,s in [('cross',event),('state',state)]:
                    name=f'{unit}:{tf}m:{fast}/{slow}:{rule}'
                    x=prior.align(s,index,event=(rule=='cross'));scores[name]=x
                    candidates.append({'id':name,'unit':unit,'timeframe':tf,'pair':[fast,slow],'bar_lookbacks':[fast*factor,slow*factor],'rule':rule,**{split:detail(x[mask],y[mask]) for split,mask in masks.items() if split!='final_reused'}})
    selected={}
    for unit in units:
        eligible=[c for c in candidates if c['unit']==unit and c['rule']=='cross' and c['train']['active']>=200 and c['validation']['active']>=100]
        selected[unit]=max(eligible,key=lambda c:c['validation']['correlation'])['id'] if eligible else None
    (ROOT/'selection-lock.json').write_text(json.dumps({'selected_by_validation':selected,'pre_final_candidates':candidates},indent=2))
    final=masks['final_reused']; predictions=pd.DataFrame({'start':index[final],'outcome':y[final]})
    for c in candidates:
        x=scores[c['id']][final]; yy=y[final]; ix=index[final]
        c['final_reused']=detail(x,yy)
        c['bootstrap']=prior.bootstrap(x,yy,ix) if np.isfinite(x).any() and np.count_nonzero(np.nan_to_num(x)) else None
        c['subperiods']=[detail(x[ch],yy[ch]) for ch in np.array_split(np.arange(len(x)),3)]
        predictions[c['id']]=x
    result={'units':units,'source_data':old['data'],'evaluation_status':'reused final period, exploratory follow-up, no new untouched holdout','selected_by_validation':selected,'candidates':candidates,'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (ROOT/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    predictions.to_csv(ROOT/'final-predictions.csv',index=False)
    for c in candidates:print(c['id'],c['final_reused'],c['bootstrap'],flush=True)

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--units',choices=['bars','days','both'],required=True);args=p.parse_args()
    run(['bars','days'] if args.units=='both' else [args.units])
