"""Independent model-input and Fraction-based cash/edge audit."""
from pathlib import Path
from fractions import Fraction as F
from datetime import datetime,timezone
import json,csv,math,hashlib
import numpy as np

R=Path(__file__).resolve().parent
def lines(path):return [json.loads(s) for s in Path(path).read_text().splitlines()]
def ts(s):return datetime.fromisoformat(s.replace(' ','T').replace('Z','+00:00')).timestamp()
def up(x,denom):
    a=x*denom
    return F(-(-a.numerator//a.denominator),denom)
def debit(q,p,denom):return up(q*p+up(F(7,100)*q*p*(1-p),1000000),denom)
def sig(x):return 1/(1+math.exp(-max(-40,min(40,x))))
def logit(p):
    p=min(.995,max(.005,p));return math.log(p/(1-p))

def main():
    rows=lines(R/'probabilities.jsonl');models=json.loads((R/'models.json').read_text());results=json.loads((R/'results.json').read_text())
    raw_predictions={ts(r['market_start']):r for r in csv.DictReader((R.parent/'btc-intramarket-diagnostic/predictions.csv').open())}
    official={m['ticker']:m for m in json.loads((R.parent/'btc-stochastic-search/data/markets.json').read_text())}
    raw_quotes={**json.loads((R/'data/quotes.json').read_text()),**json.loads((R.parent/'btc-priced-paper/data/quotes.json').read_text())}
    quotes={k:{c['end_period_ts']:c for c in v} for k,v in raw_quotes.items()}
    valid=[r for r in rows if r['excluded_reason'] is None]
    for r in valid:
        m=official[r['ticker']];entry=ts(r['entry_time']);start=ts(m['open_time'])
        assert entry==start+(r['minute']+1)*60 and r['quote_end_period_ts']==entry
        assert ts(r['signal_time'])==start+r['minute']*60
        assert entry<ts(m['close_time'])<=ts(m['settlement_ts'])
        z=float(raw_predictions[start]['z_'+str(r['minute'])]);assert z==r['z'] and math.isfinite(z) and z!=0
        bid=float(quotes[r['ticker']][entry]['yes_bid']['close_dollars']);ask=float(quotes[r['ticker']][entry]['yes_ask']['close_dollars'])
        assert 0<=bid<=ask<=1 and bid==float(r['bid']) and ask==float(r['ask'])
        side=1 if z>0 else -1;mid=(bid+ask)/2 if side==1 else 1-(bid+ask)/2
        features=[1,logit(mid),logit((1+math.erf(abs(z)/math.sqrt(2)))/2),side]
        assert np.allclose(features,r['x'],rtol=0,atol=1e-12) and abs(mid-r['midpoint'])<1e-12
        model=models[str(r['minute'])];raw=sum(a*b for a,b in zip(features,model['coefficients']));prob=sig(model['calibrator'][0]+model['calibrator'][1]*raw)
        assert abs(prob-r['probability'])<1e-12
        assert r['won']==((side==1)==(m['result']=='yes'))
    fit_checks=[]
    for minute in (5,10):
        model=models[str(minute)];train=[r for r in valid if r['minute']==minute and r['period']=='train'];cal=[r for r in valid if r['minute']==minute and r['period']=='calibration']
        assert model['training_rows']==[r['ticker'] for r in train] and model['calibration_rows']==[r['ticker'] for r in cal]
        assert max(ts(r['settlement_time']) for r in train)<ts(model['training_cutoff'])
        assert max(ts(r['settlement_time']) for r in cal)<ts(model['calibration_cutoff'])
        assert set(model['training_rows']).isdisjoint(model['calibration_rows'])
        final_ids={r['ticker'] for r in valid if r['period'] in ('validation','final')}
        assert final_ids.isdisjoint(set(model['training_rows'])|set(model['calibration_rows']))
        for cohort,beta,prior,kind in [(train,np.array(model['coefficients']),np.array([0,1,0,0]),'fit'),(cal,np.array(model['calibrator']),np.array([0,1]),'calibration')]:
            x=np.array([r['x'] if kind=='fit' else [1,float(np.dot(r['x'],model['coefficients']))] for r in cohort]);y=np.array([r['won'] for r in cohort]);probs=np.array([sig(v) for v in x@beta]);gradient=x.T@(probs-y)+10*(beta-prior)
            assert np.max(np.abs(gradient))<1e-4
        fit_checks.append({'minute':minute,'train':len(train),'calibration':len(cal),'labels_before_cutoff':True,'no_validation_or_final_rows_in_fit':True,'independent_gradients_passed':True})
    audited=[]
    for summary in results['accounts']:
        strategy=summary['strategy'];scenario=summary['scenario'];minute=5 if strategy=='A_5min' else 10;threshold=1 if strategy=='C_10min_lead1' else 0
        folder=R/'accounts'/strategy/scenario;trades=lines(folder/'trades.jsonl');decisions=lines(folder/'decisions.jsonl');events=lines(folder/'events.jsonl')
        bytrade={t['ticker']:t for t in trades};sample=[r for r in rows if r['period']=='final' and r['minute']==minute]
        assert len(decisions)==len(sample)==3320 and len(bytrade)==len(trades)==summary['trades']
        denom=100 if scenario=='cent_precision' else 10000;slip=F(1,100) if scenario=='cost_plus_1c' else F(0);margin=F(1,100) if scenario=='edge_gt_1c' else F(3,100) if scenario=='edge_gt_3c' else F(0)
        cash=F(500);pending=None;fees=F(0);profit=F(0)
        for row,d in zip(sample,decisions):
            assert row['ticker']==d['ticker']
            if pending and ts(pending['settlement_time'])<=ts(row['entry_time']):cash+=F(pending['payout']);assert cash==F(pending['cash_after_settlement']);pending=None
            assert cash==F(d['cash_before'])
            reason=row['excluded_reason']
            if reason is None and abs(row['z'])<threshold:reason='signal_filter'
            if reason is None and pending is not None:reason='prior_position_unsettled'
            if reason is None:
                price=min(F(1),F(row['price'])+slip);budget=cash/4;q=int(budget/price)
                while q and debit(q,price,denom)>budget:q-=1
                assert q==d['quantity'] and F(d['budget'])==budget and F(d['price'])==price
                if not q:reason='insufficient_quarter_budget'
                else:
                    cost=debit(q,price,denom);expected=q*F(str(row['probability']))-cost
                    assert expected==F(d['expected_net']) and cost==F(d['debit'])
                    if expected<=q*margin:reason='no_net_edge'
            assert d['status']==(reason or 'simulated_fill')
            if reason:assert row['ticker'] not in bytrade;continue
            t=bytrade[row['ticker']];assert t['quantity']==q and F(t['price'])==price
            assert cost<=cash/4<debit(q+1,price,denom)
            assert t['probability']==row['probability'] and F(t['expected_net'])==expected and expected>margin*q
            payout=q if row['won'] else 0
            assert F(t['payout'])==payout and F(t['debit'])==cost and F(t['premium'])==q*price
            assert F(t['fee'])==cost-q*price and F(t['net_pnl'])==payout-cost
            assert ts(t['settlement_time'])==ts(official[t['ticker']]['settlement_ts'])
            cash-=cost;assert cash==F(t['cash_after_entry']) and cash>=0
            fees+=cost-q*price;profit+=payout-cost;pending=t
        if pending:cash+=F(pending['payout']);assert cash==F(pending['cash_after_settlement'])
        assert float(cash)==summary['ending_cash'] and cash==500+profit and float(fees)==summary['fees']
        event_cash=F(500);last=0
        for e in events:
            assert ts(e['timestamp'])>=last;last=ts(e['timestamp']);event_cash+=F(e['delta']);assert event_cash==F(e['cash_after']) and event_cash>=0
        assert event_cash==cash and len(events)==2*len(trades)
        audited.append({'strategy':strategy,'scenario':scenario,'trades':len(trades),'events':len(events),'all_3320_decisions_reconstructed':True,'passed':True})
    out={'verified_at':datetime.now(timezone.utc).isoformat(),'probability_rows_verified':len(valid),'models':fit_checks,'account_audits':audited,'trades_audited':sum(x['trades'] for x in audited),'events_audited':sum(x['events'] for x in audited),'decisions_audited':len(audited)*3320,'focused_tests':6,'scope':'Independent source features, quote/label clocks, fit objective gradients, exclusion of final/validation labels, every probability and all fee-inclusive entry/skip decisions, exact cash/events/settlement. No depth or live execution verification.'}
    (R/'verification.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))

if __name__=='__main__':main()
