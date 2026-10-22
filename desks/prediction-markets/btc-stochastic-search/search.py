"""Frozen stochastic grid, chronological selection, and one-shot holdout report."""
from pathlib import Path
import csv, hashlib, itertools, json
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parent

def aggregate(raw, minutes):
    f=raw.resample(f'{minutes}min',label='right',closed='left').agg(
        {'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
    count=raw.close.resample(f'{minutes}min',label='right',closed='left').count()
    f.loc[count!=minutes//5,:]=np.nan
    return f

def corr(x,y):
    x=x-x.mean(); y=y-y.mean()
    den=np.sqrt(np.dot(x,x)*np.dot(y,y))
    return float(np.dot(x,y)/den) if den>0 else 0.0

def metrics(x,y):
    valid=np.isfinite(x); a=valid & (np.abs(x)>1e-12)
    n=int(a.sum()); yv=y[valid]
    return {'n':int(valid.sum()),'active':n,'coverage':float(a.mean()),
            'correlation':corr(x[valid],yv) if len(yv)>1 else 0.,
            'accuracy':float((np.sign(x[a])==y[a]).mean()) if n else None,
            'up_fraction_active':float((y[a]>0).mean()) if n else None}

def features(bars,n,k,d):
    low=bars.low.rolling(n).min(); high=bars.high.rolling(n).max()
    raw=100*(bars.close-low)/(high-low).replace(0,np.nan)
    K=raw.rolling(k).mean(); D=K.rolling(d).mean()
    prev=K.shift(1); gap=K-D; pgap=gap.shift(1)
    valid=K.notna() & D.notna() & prev.notna() & pgap.notna()
    out={'level':(K-50)/50,'gap':gap/100,'k_slope':K.diff()/100,'d_slope':D.diff()/100,
         'cross':pd.Series(np.where((gap>0)&(pgap<=0),1,np.where((gap<0)&(pgap>=0),-1,0)),index=bars.index,dtype=float)}
    for threshold in (10,20,30):
        out[f'turn{threshold}']=pd.Series(np.where((prev<=threshold)&(K>prev),1,
                        np.where((prev>=100-threshold)&(K<prev),-1,0)),index=bars.index,dtype=float)
    return {name:s.where(valid) for name,s in out.items()}

def align(series,index,event=False):
    result=series.reindex(index,method='ffill')
    if event: result.loc[~index.isin(series.index)]=0.
    return result.to_numpy(dtype=float)

def prepare():
    rows=json.loads((ROOT/'data/candles.json').read_text())
    f=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume'])
    assert not f.timestamp.duplicated().any()
    assert (f.timestamp%300==0).all()
    assert np.isfinite(f.to_numpy()).all()
    assert ((f.low<=f[['open','close']].min(axis=1)) & (f.high>=f[['open','close']].max(axis=1))).all()
    f.index=pd.to_datetime(f.timestamp,unit='s',utc=True)
    full=pd.date_range(f.index.min(),f.index.max(),freq='5min')
    missing=len(full)-len(f); f=f.reindex(full)
    ms=pd.DataFrame(json.loads((ROOT/'data/markets.json').read_text()))
    ms['start']=pd.to_datetime(ms.open_time,utc=True); ms['end']=pd.to_datetime(ms.close_time,utc=True)
    end=pd.Timestamp('2026-10-18',tz='UTC'); start=end-pd.Timedelta(days=180)
    keep=(ms.result.isin(['yes','no']) & (ms.start>=start) & (ms.end<=end)
          & ((ms.end-ms.start)==pd.Timedelta(minutes=15)) & (ms.start.astype('int64')%(900*10**9)==0))
    ms=ms.loc[keep].sort_values('start').copy()
    assert not ms.start.duplicated().any(), 'multiple markets at one boundary'
    assert len(ms)>=2000, 'insufficient official outcomes'
    index=pd.DatetimeIndex(ms.start); y=np.where(ms.result=='yes',1.,-1.)
    a=index[int(len(index)*.6)].floor('D'); b=index[int(len(index)*.8)].floor('D')
    masks={'train':index<a,'validation':(index>=a+pd.Timedelta(days=1))&(index<b),
           'holdout':index>=b+pd.Timedelta(days=1)}
    info={'raw_candles':len(rows),'missing_5m_bars':missing,'official_markets':len(ms),
          'first_market':str(index[0]),'last_market':str(index[-1]),'split_boundaries':[str(a),str(b)],
          'split_counts':{s:int(m.sum()) for s,m in masks.items()},
          'rules_primary_examples':ms.rules_primary.iloc[[0,len(ms)//2,-1]].tolist(),
          'data_hashes':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'data/candles.json',ROOT/'data/markets.json']}}
    ms[['ticker','start','end','result','floor_strike','expiration_value']].to_csv(ROOT/'matched-outcomes.csv',index=False)
    return f,index,y,masks,info

def bootstrap(x,y,index):
    days=index.floor('D'); unique=days.unique(); rng=np.random.default_rng(20261018)
    groups=[np.flatnonzero(days==d) for d in unique]
    vals=[]
    for _ in range(2000):
        ix=np.concatenate([groups[j] for j in rng.integers(0,len(groups),len(groups))])
        m=metrics(x[ix],y[ix]); vals.append([m['correlation'],m['accuracy'] if m['accuracy'] is not None else np.nan])
    ci=np.nanquantile(vals,[.025,.975],axis=0)
    return {'daily_blocks':len(groups),'resamples':2000,'correlation_95':ci[:,0].tolist(),'accuracy_95':ci[:,1].tolist()}

def run():
    raw,index,y,masks,info=prepare(); pools={}; winners={}; all_winner=None; total=0
    columns=['id','tf','n','k','d','rule','orientation','train_n','train_active','train_correlation','train_accuracy','validation_n','validation_active','validation_correlation','validation_accuracy']
    with (ROOT/'trials.csv').open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=columns); writer.writeheader()
        def evaluate(meta,x):
            nonlocal all_winner,total
            total+=1
            t=metrics(x[masks['train']],y[masks['train']]); v=metrics(x[masks['validation']],y[masks['validation']])
            row={**meta,**{'train_'+z:t[z] for z in ('n','active','correlation','accuracy')},**{'validation_'+z:v[z] for z in ('n','active','correlation','accuracy')}}
            writer.writerow(row)
            candidate={'meta':meta,'train':t,'validation':v,'x':x}
            if t['active']>=200 and v['active']>=100 and t['correlation']>0:
                if all_winner is None or v['correlation']>all_winner['validation']['correlation']: all_winner=candidate
                key=str(meta['tf'])
                if key not in winners or v['correlation']>winners[key]['validation']['correlation']: winners[key]=candidate
            return candidate
        for tf in (5,15,30):
            bars=aggregate(raw,tf); pool=[]
            for n,k,d in itertools.product(range(5,61),range(1,6),range(1,6)):
                for rule,s in features(bars,n,k,d).items():
                    x=align(s,index,rule=='cross' or rule.startswith('turn'))
                    for orientation in (1,-1):
                        meta={'id':f'{tf}:{n}:{k}:{d}:{rule}:{orientation}','tf':tf,'n':n,'k':k,'d':d,'rule':rule,'orientation':orientation}
                        c=evaluate(meta,x*orientation)
                        if c['train']['active']>=200 and c['train']['correlation']>0:
                            sig=hashlib.sha256(np.nan_to_num(c['x'][masks['train']]).tobytes()).hexdigest()
                            if not any(p['sig']==sig for p in pool):
                                c['sig']=sig; pool.append(c); pool.sort(key=lambda p:p['train']['correlation'],reverse=True); pool=pool[:10]
            pools[tf]=pool
            print('timeframe',tf,'trials',total,'training_best',pool[0]['meta'],flush=True)
        (ROOT/'training-shortlists.json').write_text(json.dumps({tf:[{k:v for k,v in c.items() if k not in ('x','sig')} for c in pool] for tf,pool in pools.items()},indent=2))
        for size in (2,3):
            for tfs in itertools.combinations((5,15,30),size):
                for cs in itertools.product(*(pools[t] for t in tfs)):
                    xs=np.stack([c['x'] for c in cs]); signs=np.sign(xs)
                    for rule in ('mean','unanimous'):
                        x=xs.mean(axis=0) if rule=='mean' else np.where(np.all(signs==signs[0],axis=0),signs[0],0.)
                        x[~np.isfinite(xs).all(axis=0)]=np.nan
                        meta={'id':rule+'|'+'|'.join(c['meta']['id'] for c in cs),'tf':'+'.join(map(str,tfs)),'n':'','k':'','d':'','rule':rule,'orientation':1}
                        evaluate(meta,x)
    # Freeze selection before accessing holdout metrics.
    selected={'overall':all_winner,**{str(tf):winners[str(tf)] for tf in (5,15,30)}}
    (ROOT/'selection-lock.json').write_text(json.dumps({name:{k:v for k,v in c.items() if k not in ('x','sig')} for name,c in selected.items()},indent=2))
    h=masks['holdout']; train_side=1 if y[masks['train']].mean()>=0 else -1
    result={'data':info,'trials':total,'baseline':{'training_majority_direction':train_side,'holdout':metrics(np.full(h.sum(),train_side),y[h])},'selected':{}}
    for name,c in selected.items():
        x=c['x'][h]; yy=y[h]; ii=index[h]
        result['selected'][name]={k:v for k,v in c.items() if k not in ('x','sig')}
        result['selected'][name].update(holdout=metrics(x,yy),bootstrap=bootstrap(x,yy,ii),
             subperiods=[metrics(x[chunk],yy[chunk]) for chunk in np.array_split(np.arange(len(x)),3)])
        if name=='overall': pd.DataFrame({'start':ii,'score':x,'result':yy}).to_csv(ROOT/'holdout-predictions.csv',index=False)
    result['code_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (ROOT/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    print(json.dumps(result,indent=2),flush=True)

if __name__=='__main__': run()
