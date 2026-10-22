"""Hourly indicator grid. Selection sees training then validation; evaluation separate."""
from pathlib import Path
from datetime import datetime, timezone
import csv,gzip,hashlib,itertools,json,math,sys
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent
BASE=R
TFS=(15,30,45,60); OFFSETS=(0,15,29)
def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def dump(name,x): (R/name).write_text(json.dumps(x,indent=2,allow_nan=False))
def aggregate(raw,tf):
 kw=dict(rule=f'{tf}min',label='right',closed='left',origin='epoch')
 f=raw.resample(**kw).agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
 count=raw.close.resample(**kw).count();f.loc[count!=tf//5,:]=np.nan
 return f

def prepare():
 rows=json.loads((R/'data/candles.json').read_text())
 f=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume'])
 assert not f.timestamp.duplicated().any() and (f.timestamp%300==0).all()
 assert np.isfinite(f.to_numpy()).all()
 assert ((f.low<=f[['open','close']].min(axis=1))&(f.high>=f[['open','close']].max(axis=1))).all()
 f.index=pd.to_datetime(f.timestamp,unit='s',utc=True);f=f.reindex(pd.date_range(f.index.min(),f.index.max(),freq='5min'))
 prices={int(r[0])+300:float(r[4]) for r in rows}; best={};count=0; exclusions={}; seen=set()
 # Raw response pages preserve every considered market; reduce to one ex-ante contract/hour.
 for path in sorted((R/'data').glob('*markets-*.json')):
  if path.stem.endswith('error'):continue
  data=json.loads(path.read_text()).get('data',{}); markets=data.get('markets',[])
  for m in markets:
   if m['ticker'] in seen:continue
   seen.add(m['ticker']);count+=1
   def skip(k):exclusions[k]=exclusions.get(k,0)+1
   end=pd.Timestamp(m['close_time']); start=end-pd.Timedelta(hours=1)
   if not(pd.Timestamp('2026-04-21',tz='UTC')<=start<pd.Timestamp('2026-10-18',tz='UTC')):skip('outside_period');continue
   if end.value%(3600*10**9) or m.get('result') not in ('yes','no') or m.get('strike_type')!='greater':skip('wrong_shape_or_unsettled');continue
   if pd.Timestamp(m['open_time'])>start or pd.Timestamp(m['created_time'])>start:skip('not_available_at_hour_start');continue
   s=m.get('floor_strike');spot=prices.get(int(start.timestamp()))
   if s is None or spot is None:skip('missing_strike_or_spot');continue
   try:v=float(m['expiration_value']);settled=pd.Timestamp(m['settlement_ts'])
   except (KeyError,ValueError,TypeError):skip('missing_settlement');continue
   if pd.isna(settled) or not math.isfinite(v) or (v>float(s))!=(m['result']=='yes') or settled<end:skip('incoherent_settlement');continue
   key=(abs(float(s)-spot),float(s),m['ticker'])
   if end.value not in best or key<best[end.value][0]:best[end.value]=(key,dict(m,start=start.isoformat(),end=end.isoformat(),spot_start=spot))
 ms=pd.DataFrame([best[k][1] for k in sorted(best)])
 assert len(ms)>=3000, f'Only {len(ms)} eligible hours'
 ms['start']=pd.to_datetime(ms.start,utc=True);ms['end']=pd.to_datetime(ms.end,utc=True);ms['settled']=pd.to_datetime(ms.settlement_ts,utc=True,format='mixed')
 assert not ms.start.duplicated().any()
 ii=pd.DatetimeIndex(ms.start); y=np.where(ms.result=='yes',1.,-1.)
 a=pd.Timestamp('2026-08-07',tz='UTC');b=pd.Timestamp('2026-09-12',tz='UTC')
 masks={'train':np.asarray((ii<a)&(ms.settled<a)), 'validation':np.asarray((ii>=a+pd.Timedelta(days=1))&(ii<b)&(ms.settled<b)), 'evaluation':np.asarray(ii>=b+pd.Timedelta(days=1))}
 keep=['ticker','event_ticker','start','end','result','floor_strike','expiration_value','settlement_ts','created_time','open_time','spot_start']
 ms[keep].to_csv(R/'selected-markets.csv',index=False)
 baseline={}
 for off in OFFSETS:
  tt=ii+pd.Timedelta(minutes=(off//5)*5);current=np.array([prices.get(int(t.timestamp()),np.nan) for t in tt]);prior=np.array([prices.get(int(t.timestamp())-3600,np.nan) for t in tt])
  baseline[str(off)]={'spot_strike':np.sign(current-ms.floor_strike.to_numpy(float)),'momentum60':np.sign(current-prior)}
 info={'raw_candles':len(rows),'missing_5m_bars':int(f.close.isna().sum()),'metadata_considered':count,'selected_hours':len(ms),'first':str(ii[0]),'last':str(ii[-1]),'exclusions':exclusions,'counts':{k:int(v.sum()) for k,v in masks.items()},'candles_sha256':digest(R/'data/candles.json'),'selected_markets_sha256':digest(R/'selected-markets.csv'),'price_reference':'Coinbase BTC-USD; official target BRTI60-second average versus listed strike'}
 dump('data-summary.json',info)
 return f,ii,y,masks,baseline,info

def stats_batch(x,y):
 x=np.atleast_2d(x);finite=np.isfinite(x);v=np.where(finite,x,0);n=finite.sum(1);a=(finite&(v!=0));active=a.sum(1)
 sx=v.sum(1);sy=(finite*y).sum(1);sxx=(v*v).sum(1);sxy=(v*y).sum(1)
 with np.errstate(divide='ignore',invalid='ignore'):
  den=np.sqrt(np.maximum(0,(sxx-sx*sx/n)*(n-sy*sy/n)))
  corr=np.divide(sxy-sx*sy/n,den,out=np.zeros(len(n)),where=den>0)
 wins=((v==y)&a).sum(1)
 return n,active,wins,corr

def prepare_polymarket():
 rows=json.loads((BASE/'data/binance-candles.json').read_text())
 f=pd.DataFrame(rows,columns=['timestamp','low','high','open','close','volume'])
 assert not f.timestamp.duplicated().any() and (f.timestamp%300==0).all()
 assert np.isfinite(f.to_numpy()).all()
 assert ((f.low<=f[['open','close']].min(axis=1))&(f.high>=f[['open','close']].max(axis=1))).all()
 f.index=pd.to_datetime(f.timestamp,unit='s',utc=True);f=f.reindex(pd.date_range(f.index.min(),f.index.max(),freq='5min'))
 closes={r[0]+300:r[4] for r in rows};opens={r[0]:r[3] for r in rows};records=[];exclusions={};disagreements=[]
 events=json.loads((BASE/'data/polymarket-events.json').read_text())
 for e in events:
  def skip(k):exclusions[k]=exclusions.get(k,0)+1
  if len(e.get('markets',[]))!=1:skip('not_single_market');continue
  m=e['markets'][0];end=pd.Timestamp(m['endDate']);start=pd.Timestamp(m.get('eventStartTime',end-pd.Timedelta(hours=1)))
  if not(pd.Timestamp('2026-04-21',tz='UTC')<=start<pd.Timestamp('2026-10-18',tz='UTC')):skip('outside_period');continue
  if end-start!=pd.Timedelta(hours=1) or start.value%(3600*10**9):skip('not_hourly');continue
  if 'binance.com' not in m.get('resolutionSource','') or '1 hour candle' not in m.get('description',''):skip('different_resolution_rule');continue
  if pd.Timestamp(m['createdAt'])>start or pd.Timestamp(m['startDate'])>start:skip('not_available_at_start');continue
  outcomes=json.loads(m['outcomes']);payouts=json.loads(m['outcomePrices'])
  if not m.get('closed') or m.get('umaResolutionStatus')!='resolved' or outcomes!=['Up','Down'] or payouts not in (['1','0'],['0','1']):skip('not_final_binary');continue
  try:settled=max(pd.Timestamp(m['closedTime']),pd.Timestamp(m['umaEndDate']))
  except (KeyError,ValueError,TypeError):skip('missing_resolution_time');continue
  if pd.isna(settled) or settled<end:skip('invalid_resolution_time');continue
  t=int(start.timestamp());opening=opens.get(t);closing=closes.get(t+3600)
  if opening is None:skip('missing_open_reference');continue
  up=payouts[0]=='1'
  if closing is not None and (closing>=opening)!=up:disagreements.append(m['slug'])
  records.append({'ticker':m['slug'],'event_ticker':e['id'],'start':start,'end':end,'result':'yes' if up else 'no','floor_strike':opening,'expiration_value':closing,'settlement_ts':settled.isoformat(),'created_time':m['createdAt'],'open_time':m['startDate'],'spot_start':closes.get(t),'settled':settled})
 ms=pd.DataFrame(records).sort_values('start');assert len(ms)>=3000 and not ms.start.duplicated().any()
 ii=pd.DatetimeIndex(ms.start);y=np.where(ms.result=='yes',1.,-1.)
 a=pd.Timestamp('2026-08-07',tz='UTC');b=pd.Timestamp('2026-09-12',tz='UTC')
 masks={'train':np.asarray((ii<a)&(ms.settled<a)), 'validation':np.asarray((ii>=a+pd.Timedelta(days=1))&(ii<b)&(ms.settled<b)), 'evaluation':np.asarray(ii>=b+pd.Timedelta(days=1))}
 ms.drop(columns=['settled']).to_csv(R/'selected-markets.csv',index=False)
 baseline={}
 for off in OFFSETS:
  tt=ii+pd.Timedelta(minutes=(off//5)*5);current=np.array([closes.get(int(t.timestamp()),np.nan) for t in tt]);prior=np.array([closes.get(int(t.timestamp())-3600,np.nan) for t in tt])
  lead=np.sign(current-ms.floor_strike.to_numpy(float))
  if off==0:lead[:]=np.nan # Opening trade is not observable before the hour starts.
  baseline[str(off)]={'spot_strike':lead,'momentum60':np.sign(current-prior)}
 info={'target':'Polymarket series10114 hourly Up/Down; Up includes ties','raw_candles':len(rows),'missing_5m_bars':int(f.close.isna().sum()),'metadata_considered':len(events),'selected_hours':len(ms),'first':str(ii[0]),'last':str(ii[-1]),'exclusions':exclusions,'counts':{k:int(v.sum()) for k,v in masks.items()},'candles_sha256':digest(BASE/'data/binance-candles.json'),'selected_markets_sha256':digest(R/'selected-markets.csv'),'price_reference':'Binance BTCUSDT; official Polymarket resolved payout labels','resolution_price_disagreements':disagreements}
 dump('data-summary.json',info)
 return f,ii,y,masks,baseline,info

def metrics(x,y):
 n,a,w,c=stats_batch(x,y);valid=np.isfinite(x);active=valid&(x!=0)
 return {'valid':int(n[0]),'active':int(a[0]),'coverage':float(a[0]/len(y)),'wins':int(w[0]),'accuracy':float(w[0]/a[0]) if a[0] else None,'correlation':float(c[0]),'yes_fraction_active':float((y[active]>0).mean()) if a[0] else None}

def align_matrix(values,bars,index,event):
 tt=np.stack([(index+pd.Timedelta(minutes=off)).asi8 for off in OFFSETS]); bi=bars.index.asi8
 pos=np.searchsorted(bi,tt,side='right')-1;valid=pos>=0;out=np.asarray(values)[np.maximum(pos,0)].astype(float)
 out[~valid]=np.nan
 if event:out[valid&(bi[np.maximum(pos,0)]!=tt)]=0.
 return out

def stoch_values(bars,n,k,d):
 lo=bars.low.rolling(n).min();hi=bars.high.rolling(n).max();raw=100*(bars.close-lo)/(hi-lo).replace(0,np.nan)
 K=raw.rolling(k).mean();D=K.rolling(d).mean();prev=K.shift();gap=K-D;pgap=gap.shift()
 valid=np.asarray(K.notna()&D.notna()&prev.notna()&pgap.notna())
 out={'level':np.sign(K.to_numpy()-50),'gap':np.sign(gap.to_numpy()),'k_slope':np.sign(K.diff().to_numpy()),'d_slope':np.sign(D.diff().to_numpy()),'cross':np.where((gap>0)&(pgap<=0),1.,np.where((gap<0)&(pgap>=0),-1.,0.))}
 for q in (10,20,30):out[f'zone{q}']=np.where((prev<=q)&(K>prev),1.,np.where((prev>=100-q)&(K<prev),-1.,0.))
 for v in out.values():v[~valid]=np.nan
 return out

def cent_sums(bars):
 close=bars.close.to_numpy();valid=np.isfinite(close)
 assert np.all(np.abs(close[valid]*100-np.rint(close[valid]*100))<1e-6), 'Non-cent source prices require a different exact scale'
 cents=pd.Series(np.rint(close*100),index=bars.index)
 return {n:cents.rolling(n).sum().to_numpy() for n in range(2,201)}

def candidate(meta,x,m):return {'meta':meta,'x':x.copy(),'train':m,'signature':hashlib.sha256(np.nan_to_num(x,nan=9).tobytes()).hexdigest()}
def clean(c):return {k:v for k,v in c.items() if k not in ('x','signature')}
def quality(c,period='train'):return (-c[period]['correlation'],c['meta']['id'])

def search():
 raw,index,y,masks,baseline,info=prepare();train=masks['train'];val=masks['validation'];pools={};total=0
 with gzip.open(R/'trials.csv.gz','wt',compresslevel=3,newline='') as stream:
  writer=csv.writer(stream);writer.writerow(['id','family','tf','offset','train_valid','train_active','train_wins','train_correlation'])
  def eval_matrix(meta,mat):
   nonlocal total
   st=stats_batch(mat[:,train],y[train])
   for oi,off in enumerate(OFFSETS):
    for orient in (1,-1):
     total+=1;mm=dict(meta,offset=off,orientation=orient,id=f"{meta['id']}:{off}:{orient}")
     nn,aa,ww,cc=(s[oi].item() for s in st); ww=ww if orient==1 else aa-ww; cc*=orient
     writer.writerow([mm['id'],mm['family'],mm['tf'],off,nn,aa,ww,format(cc,'.12g')])
     key=(mm['family'],mm['tf'],off);pool=pools.setdefault(key,[])
     if aa<200 or cc<=0 or (len(pool)==10 and (-cc,mm['id'])>=quality(pool[-1])):continue
     x=mat[oi]*orient; sig=hashlib.sha256(np.nan_to_num(x[train],nan=9).tobytes()).hexdigest()
     duplicate=next((p for p in pool if p['signature']==sig),None)
     if duplicate is not None:
      if mm['id']>=duplicate['meta']['id']:continue
      pool.remove(duplicate)
     c={'meta':mm,'x':x.copy(),'signature':sig,'train':metrics(x[train],y[train])};pool.append(c);pool.sort(key=quality);del pool[10:]
  for tf in TFS:
   bars=aggregate(raw,tf)
   for n in range(5,101):
    for k,d in itertools.product(range(1,6),repeat=2):
     for rule,v in stoch_values(bars,n,k,d).items():
      eval_matrix({'id':f'ST:{tf}:{n}:{k}:{d}:{rule}','family':'stochastic','tf':tf,'n':n,'k':k,'d':d,'rule':rule},align_matrix(v,bars,index,rule=='cross' or rule.startswith('zone')))
   print('stochastic',tf,total,flush=True)
   sma=cent_sums(bars)
   for fast in range(2,200):
    for slow in range(fast+1,201):
     gap=sma[fast]*slow-sma[slow]*fast;prev=np.roll(gap,1);prev[0]=np.nan;valid=np.isfinite(gap)&np.isfinite(prev)
     state=np.sign(gap);cross=np.where((gap>0)&(prev<=0),1.,np.where((gap<0)&(prev>=0),-1.,0.))
     for rule,v in [('state',state),('cross',cross)]:
      v[~valid]=np.nan
      eval_matrix({'id':f'MA:{tf}:{fast}:{slow}:{rule}','family':'sma','tf':tf,'fast':fast,'slow':slow,'rule':rule},align_matrix(v,bars,index,rule=='cross'))
   print('sma',tf,total,flush=True)
 assert total==1406448,total
 dump('training-shortlists.json',{'|'.join(map(str,key)):[clean(c) for c in pool] for key,pool in pools.items()})
 # Validate frozen pure shortlists; retain all chosen chart and family-offset winners.
 selected={};all_candidates=[]
 for (fam,tf,off),pool in pools.items():
  for c in pool:c['validation']=metrics(c['x'][val],y[val])
  eligible=[c for c in pool if c['validation']['active']>=50]
  if eligible:selected[f'{fam}/chart{tf}/offset{off}']=min(eligible,key=lambda c:quality(c,'validation'))
  all_candidates.extend(eligible)
 combo_counts={}; combo_best={}; combo_trial_count=0
 with gzip.open(R/'combination-trials.csv.gz','wt',compresslevel=3,newline='') as stream:
  writer=csv.writer(stream);writer.writerow(['id','family','offset','train_active','train_correlation','validation_active','validation_correlation'])
  def combine(cs,off,rule):
   nonlocal combo_trial_count
   fams={c['meta']['family'] for c in cs};family=next(iter(fams)) if len(fams)==1 else 'mixed';family=family+'_combination'
   xs=np.stack([c['x'] for c in cs]);finite=np.isfinite(xs).all(0)
   if rule=='agree':x=np.where((xs==xs[0]).all(0),xs[0],0.)
   else:x=np.sign(xs.sum(0))
   x[~finite]=np.nan
   meta={'id':rule+'|'+'|'.join(c['meta']['id'] for c in cs),'family':family,'tf':'+'.join(str(c['meta']['tf']) for c in cs),'offset':off,'rule':rule,'components':[c['meta'] for c in cs]}
   t=metrics(x[train],y[train]);v=metrics(x[val],y[val]);combo_trial_count+=1;combo_counts[family]=combo_counts.get(family,0)+1
   writer.writerow([meta['id'],family,off,t['active'],t['correlation'],v['active'],v['correlation']])
   if t['active']<200 or t['correlation']<=0 or v['active']<50:return
   c={'meta':meta,'train':t,'validation':v,'x':x};key=(family,off)
   if key not in combo_best or quality(c,'validation')<quality(combo_best[key],'validation'):combo_best[key]=c
  for off in OFFSETS:
   shortlist=[c for (fam,tf,o),pool in pools.items() if o==off for c in pool[:5]]
   for a,b in itertools.combinations(shortlist,2):
    if a['meta']['family']==b['meta']['family'] and a['meta']['tf']==b['meta']['tf']:continue
    for rule in ('agree','vote'):combine([a,b],off,rule)
   # Top1 of each family/chart, one component for each selected chart.
   for size in (3,4):
    for tfs in itertools.combinations(TFS,size):
     for fams in itertools.product(('stochastic','sma'),repeat=size):
      if all(pools[(fam,tf,off)] for fam,tf in zip(fams,tfs)):
       combine([pools[(fam,tf,off)][0] for fam,tf in zip(fams,tfs)],off,'agree')
 for (fam,off),c in combo_best.items():selected[f'{fam}/offset{off}']=c;all_candidates.append(c)
 for fam in ('stochastic','sma'):
  for off in OFFSETS:
   candidates=[c for c in all_candidates if c['meta']['offset']==off and c['meta']['family'] in (fam,fam+'_combination')]
   if candidates:selected[f'{fam}/offset{off}']=min(candidates,key=lambda c:quality(c,'validation'))
 for fam in ('stochastic','sma','mixed'):
  candidates=[c for key,c in selected.items() if (key.startswith(fam+'/offset') if fam!='mixed' else key.startswith('mixed_combination/offset'))]
  if candidates:selected['overall/'+fam]=min(candidates,key=lambda c:quality(c,'validation'))
 for tf in TFS:
  bars=aggregate(raw,tf);sums=cent_sums(bars)
  for fast,slow in ((2,5),(5,10)):
   gap=pd.Series(sums[fast]*slow-sums[slow]*fast,index=bars.index);prev=gap.shift();valid=gap.notna()&prev.notna()
   for rule,v in [('state',np.sign(gap.to_numpy())),('cross',np.where((gap>0)&(prev<=0),1.,np.where((gap<0)&(prev>=0),-1.,0.)))]:
    v[~valid]=np.nan;mat=align_matrix(v,bars,index,rule=='cross')
    for oi,off in enumerate(OFFSETS):
     x=mat[oi];meta={'id':f'MA:{tf}:{fast}:{slow}:{rule}:{off}:1','family':'sma_control','tf':tf,'fast':fast,'slow':slow,'rule':rule,'offset':off,'orientation':1}
     selected[f'control/{tf}/{fast}-{slow}/{rule}/{off}']={'meta':meta,'x':x,'train':metrics(x[train],y[train]),'validation':metrics(x[val],y[val])}
 lock={'created_at':datetime.now(timezone.utc).isoformat(),'protocol_sha256':digest(BASE/'EXPERIMENT.md'),'code_sha256':digest(__file__),'grid_trials':total,'combination_trials':combo_trial_count,'combination_counts':combo_counts,'selected':{k:clean(c) for k,c in selected.items()}}
 dump('selection-lock.json',lock)
 arrays={f's{i}':c['x'] for i,c in enumerate(selected.values())};arrays.update(y=y,time=index.asi8,**masks)
 for off,bases in baseline.items():
  for name,x in bases.items():arrays[f'baseline_{off}_{name}']=x
 np.savez_compressed(R/'locked-predictions.npz',**arrays)
 dump('prediction-lock.json',{'selection_sha256':digest(R/'selection-lock.json'),'predictions_sha256':digest(R/'locked-predictions.npz'),'created_at':datetime.now(timezone.utc).isoformat(),'evaluation_metrics_accessed':False})
 print('selection frozen',total,combo_trial_count,len(selected),flush=True)

if __name__=='__main__':
 if '--polymarket' in sys.argv:
  R=BASE/'polymarket';R.mkdir(exist_ok=True);prepare=prepare_polymarket
 search()
