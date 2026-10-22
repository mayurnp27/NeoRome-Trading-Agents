"""Frozen, quote-aware settlement probability models; no cash replay here."""
from pathlib import Path
from datetime import datetime,timezone
import importlib.util,json,csv,math,hashlib,collections
import numpy as np

R=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('account_base',R.parent/'btc-priced-paper/replay.py')
base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)
stamp=base.stamp
PERIODS={'train':('2026-08-18','2026-09-01'),'calibration':('2026-09-02','2026-09-07'),'validation':('2026-09-08','2026-09-12'),'final':('2026-09-13','2026-10-18')}
FEATURES=['intercept','logit_side_midpoint','logit_gaussian_lead_input','side']

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def sigmoid(a):return 1/(1+np.exp(-np.clip(a,-40,40)))
def logit(p):
    p=min(.995,max(.005,p));return math.log(p/(1-p))

def feature_vector(z,bid,ask):
    side=1 if z>0 else -1
    midpoint=(bid+ask)/2 if side==1 else 1-(bid+ask)/2
    gaussian=.5*(1+math.erf(abs(z)/math.sqrt(2)))
    return [1.,logit(midpoint),logit(gaussian),float(side)],midpoint

def fit_logistic(x,y,prior,penalty=10.):
    x=np.asarray(x,float);y=np.asarray(y,float);prior=np.asarray(prior,float);b=prior.copy()
    def loss(beta):
        s=x@beta
        return float(np.sum(np.logaddexp(0,s)-y*s)+penalty*.5*np.sum((beta-prior)**2))
    for iteration in range(100):
        p=sigmoid(x@b);g=x.T@(p-y)+penalty*(b-prior)
        if np.max(np.abs(g))<1e-7:break
        h=x.T@((p*(1-p))[:,None]*x)+penalty*np.eye(x.shape[1])
        step=np.linalg.solve(h,g);scale=1.;old=loss(b)
        while loss(b-scale*step)>old-1e-4*scale*float(g@step) and scale>1e-10:scale*=.5
        b-=scale*step
        if np.max(np.abs(scale*step))<1e-11:break
    grad=x.T@(sigmoid(x@b)-y)+penalty*(b-prior)
    assert np.max(np.abs(grad))<1e-4, 'Optimizer did not converge'
    return b,{'iterations':iteration+1,'penalized_loss':loss(b),'max_abs_gradient':float(np.max(np.abs(grad)))}

def score(row,model):
    raw=float(np.dot(row['x'],model['coefficients']))
    return float(sigmoid(model['calibrator'][0]+model['calibrator'][1]*raw))

def metrics(rows,field):
    if not rows:return {'n':0}
    y=np.array([r['won'] for r in rows],float);p=np.clip([r[field] for r in rows],1e-9,1-1e-9)
    bins=[]
    indices=np.minimum(9,np.floor(p*10).astype(int))
    for i in range(10):
        lo=i/10;mask=indices==i
        if mask.any():bins.append({'from':round(float(lo),2),'to':round(float(lo+.1),2),'n':int(mask.sum()),'predicted':float(p[mask].mean()),'actual':float(y[mask].mean())})
    return {'n':len(rows),'mean_probability':float(p.mean()),'win_rate':float(y.mean()),'brier':float(np.mean((p-y)**2)),'log_loss':float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p))),'ece_10bins':sum(b['n']*abs(b['predicted']-b['actual']) for b in bins)/len(rows),'reliability':bins}

def source_rows():
    markets=json.loads((R.parent/'btc-stochastic-search/data/markets.json').read_text())
    bystart={stamp(m['open_time']):m for m in markets}
    pre=json.loads((R/'data/quotes.json').read_text());final=json.loads((R.parent/'btc-priced-paper/data/quotes.json').read_text())
    assert not (set(pre)&set(final))
    books={k:{c['end_period_ts']:c for c in v} for k,v in {**pre,**final}.items()}
    rows=[];counts=collections.Counter()
    for r in csv.DictReader((R.parent/'btc-intramarket-diagnostic/predictions.csv').open()):
        day=r['market_start'][:10];period=next((p for p,(a,b) in PERIODS.items() if a<=day<b),None)
        if period is None:continue
        start=stamp(r['market_start']);m=bystart[start];settle=stamp(m['settlement_ts'])
        for minute in (5,10):
            row={'ticker':m['ticker'],'period':period,'minute':minute,'start':start,'signal_time':base.iso(start+minute*60),'entry_time':base.iso(start+(minute+1)*60),'settlement_time':m['settlement_ts'],'close_time':m['close_time'],'result':m['result'],'settlement_value_dollars':m['settlement_value_dollars']}
            z=float(r['z_'+str(minute)]);row['z']=z if math.isfinite(z) else None
            reason=None
            if not math.isfinite(z) or not z:reason='invalid_or_zero_signal'
            quote=books.get(m['ticker'],{}).get(int(start+(minute+1)*60))
            side=1 if z>0 else -1
            price,quote_error=base.quote_price(quote,side,'delayed_close')
            if reason is None and quote_error:reason=quote_error
            if reason is None:
                assert start+(minute+1)*60<stamp(m['close_time'])<=settle
                assert m['result'] in ('yes','no') and not m.get('is_provisional')
                assert float(m['settlement_value_dollars'])==int(m['result']=='yes')
                assert int(float(r['outcome']))==(1 if m['result']=='yes' else -1)
                bid=float(quote['yes_bid']['close_dollars']);ask=float(quote['yes_ask']['close_dollars'])
                x,mid=feature_vector(z,bid,ask)
                row.update(side=side,price=str(price),bid=str(quote['yes_bid']['close_dollars']),ask=str(quote['yes_ask']['close_dollars']),x=x,midpoint=mid,won=(side==1)==(m['result']=='yes'),quote_end_period_ts=quote['end_period_ts'])
                if period in ('train','calibration') and settle>=stamp(PERIODS[period][1]+'T00:00:00Z'):reason='unsettled_at_fit_cutoff'
            row['excluded_reason']=reason;counts[f'{period}/{minute}/{reason or "valid"}']+=1;rows.append(row)
    return rows,dict(counts)

def main():
    rows,counts=source_rows();models={};diagnostics={}
    for minute in (5,10):
        valid=[r for r in rows if r['minute']==minute and r['excluded_reason'] is None]
        train=[r for r in valid if r['period']=='train'];cal=[r for r in valid if r['period']=='calibration']
        assert len(train)>1000 and len(cal)>300
        b,fit=fit_logistic([r['x'] for r in train],[r['won'] for r in train],[0,1,0,0])
        calibration_x=[[1.,float(np.dot(r['x'],b))] for r in cal]
        c,calfit=fit_logistic(calibration_x,[r['won'] for r in cal],[0,1])
        model={'minute':minute,'features':FEATURES,'coefficients':b.tolist(),'calibrator':c.tolist(),'penalty':10,'fit':fit,'calibration_fit':calfit,'training_rows':[r['ticker'] for r in train],'calibration_rows':[r['ticker'] for r in cal],'train_max_settlement':base.iso(max(stamp(r['settlement_time']) for r in train)),'calibration_max_settlement':base.iso(max(stamp(r['settlement_time']) for r in cal)),'training_cutoff':'2026-09-01T00:00:00Z','calibration_cutoff':'2026-09-07T00:00:00Z'}
        models[str(minute)]=model
        for r in valid:r['probability']=score(r,model);r['uncalibrated']=float(sigmoid(np.dot(r['x'],b)))
        diagnostics[str(minute)]={}
        for period in PERIODS:
            cohort=[r for r in valid if r['period']==period]
            diagnostics[str(minute)][period]={field:metrics(cohort,field) for field in ('probability','uncalibrated','midpoint')}
            diagnostics[str(minute)][period]['by_side']={str(side):metrics([r for r in cohort if r['side']==side],'probability') for side in (-1,1)}
    (R/'models.json').write_text(json.dumps(models,indent=2))
    with (R/'probabilities.jsonl').open('w') as f:
        for r in rows:f.write(json.dumps(r,allow_nan=False)+'\n')
    (R/'probability-diagnostics.json').write_text(json.dumps({'counts':counts,'metrics':diagnostics},indent=2))
    lock={'created_at':datetime.now(timezone.utc).isoformat(),'periods':PERIODS,'frozen_before_cash_replay':True,'hashes':{str(p.relative_to(R.parent)):sha(p) for p in [R/'EXPERIMENT.md',R/'model.py',R/'models.json',R/'probabilities.jsonl',R/'data/quotes.json',R.parent/'btc-priced-paper/data/quotes.json',R.parent/'btc-stochastic-search/data/markets.json',R.parent/'btc-intramarket-diagnostic/predictions.csv']}}
    (R/'prediction-lock.json').write_text(json.dumps(lock,indent=2))
    print(json.dumps({'counts':counts,'models':{k:{z:v[z] for z in ('coefficients','calibrator','train_max_settlement','calibration_max_settlement')} for k,v in models.items()}},indent=2))

if __name__=='__main__':main()
