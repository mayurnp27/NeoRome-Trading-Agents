"""Three independent probability-gated cash accounts, all scenarios reported."""
from pathlib import Path
from decimal import Decimal as D
import json,collections,math,hashlib
import numpy as np
from model import base,stamp,R,sha

STRATEGIES={'A_5min':(5,0),'B_10min':(10,0),'C_10min_lead1':(10,1)}
SCENARIOS={'edge_gt_0':(D(0),D(0),D('.0001')),'edge_gt_1c':(D('.01'),D(0),D('.0001')),'edge_gt_3c':(D('.03'),D(0),D('.0001')),'cost_plus_1c':(D(0),D('.01'),D('.0001')),'cent_precision':(D(0),D(0),D('.01'))}

def eligible_signal(row,threshold):
    return row.get('excluded_reason') is None and row.get('z') is not None and abs(row['z'])>=threshold

def gate(probability,q,price,precision,margin):
    debit=q*price+base.fee(q,price,precision)
    expected=D(str(probability))*q-debit
    return expected>margin*q,expected,debit

def unit_reference(rows,threshold,scenario):
    margin,slip,precision=SCENARIOS[scenario];out=[]
    for row in rows:
        if not eligible_signal(row,threshold):continue
        price=min(D(1),D(row['price'])+slip)
        passes,expected,cost=gate(row['probability'],1,price,precision,margin)
        if passes:
            out.append({'ticker':row['ticker'],'day':row['entry_time'][:10],'probability':row['probability'],'price':str(price),'fee':str(cost-price),'expected_net':str(expected),'won':row['won'],'net':str(D(int(row['won']))-cost)})
    return out

def unit_stats(rows,all_days):
    if not rows:return {'trades':0,'daily_block_net_95':None}
    daygroups={d:[] for d in all_days}
    for r in rows:daygroups[r['day']].append(r)
    sums=np.array([sum(float(r['net']) for r in v) for v in daygroups.values()]);counts=np.array([len(v) for v in daygroups.values()])
    rng=np.random.default_rng(20261018);ix=rng.integers(0,len(all_days),(2000,len(all_days)))
    denom=counts[ix].sum(axis=1);valid=denom>0;ratios=sums[ix].sum(axis=1)[valid]/denom[valid]
    return {'trades':len(rows),'wins':sum(r['won'] for r in rows),'win_rate':sum(r['won'] for r in rows)/len(rows),'mean_probability':sum(r['probability'] for r in rows)/len(rows),'mean_net':sum(float(r['net']) for r in rows)/len(rows),'mean_expected_net':sum(float(r['expected_net']) for r in rows)/len(rows),'mean_price':sum(float(r['price']) for r in rows)/len(rows),'total_net':sum(float(r['net']) for r in rows),'daily_blocks':len(all_days),'daily_block_net_95':np.quantile(ratios,[.025,.975]).tolist()}

def run(strategy,scenario,rows,save=True):
    minute,threshold=STRATEGIES[strategy];margin,slip,precision=SCENARIOS[scenario]
    cash=D(500);pending=None;trades=[];decisions=[];events=[];reasons=collections.Counter();peak=cash;dd=D(0)
    def settle():
        nonlocal cash,pending,peak,dd
        if pending is None:return
        cash+=D(pending['payout']);pending['cash_after_settlement']=str(cash)
        events.append({'timestamp':pending['settlement_time'],'kind':'settlement','ticker':pending['ticker'],'delta':pending['payout'],'cash_after':str(cash)})
        peak=max(peak,cash);dd=max(dd,(peak-cash)/peak);pending=None
    for row in rows:
        if pending and stamp(pending['settlement_time'])<=stamp(row['entry_time']):settle()
        decision={'ticker':row['ticker'],'entry_time':row['entry_time'],'cash_before':str(cash),'probability':row.get('probability'),'z':row.get('z')}
        reason=row.get('excluded_reason')
        if reason is None and abs(row['z'])<threshold:reason='signal_filter'
        if reason is None and pending is not None:reason='prior_position_unsettled'
        if reason is None:
            price=min(D(1),D(row['price'])+slip);q,budget=base.size(cash,price,precision)
            decision.update(price=str(price),quantity=q,budget=str(budget))
            if q==0:reason='insufficient_quarter_budget'
            else:
                passes,expected,total=gate(row['probability'],q,price,precision,margin)
                decision.update(expected_net=str(expected),required_margin=str(margin*q),debit=str(total))
                if not passes:reason='no_net_edge'
        if reason:
            reasons[reason]+=1;decision['status']=reason;decisions.append(decision);continue
        before=cash;cash-=total;pay=D(q) if row['won'] else D(0)
        assert total<=budget and cash>=0
        t={k:row[k] for k in ('ticker','signal_time','entry_time','settlement_time','close_time','side','z','probability','quote_end_period_ts','bid','ask','midpoint','won')}
        t.update(strategy=strategy,scenario=scenario,price=str(price),quoted_price=row['price'],quantity=q,cash_before=str(before),budget=str(budget),premium=str(q*price),fee=str(total-q*price),debit=str(total),cash_after_entry=str(cash),payout=str(pay),net_pnl=str(pay-total),expected_net=str(expected),margin_per_contract=str(margin),probability_model=str(minute))
        trades.append(t);pending=t;events.append({'timestamp':row['entry_time'],'kind':'entry','ticker':row['ticker'],'delta':str(-total),'cash_after':str(cash)})
        decision['status']='simulated_fill';decisions.append(decision)
    settle()
    fees=sum((D(t['fee']) for t in trades),D(0));gross=sum((D(t['payout'])-D(t['premium']) for t in trades),D(0));expected=sum((D(t['expected_net']) for t in trades),D(0))
    assert cash==D(500)+gross-fees
    qty=sum(t['quantity'] for t in trades);wins=sum(t['won'] for t in trades)
    byday=collections.defaultdict(lambda:D(0))
    for t in trades:byday[t['entry_time'][:10]]+=D(t['net_pnl'])
    reference=unit_reference(rows,threshold,scenario)
    summary={'strategy':strategy,'scenario':scenario,'starting_cash':500,'ending_cash':float(cash),'net_pnl':float(cash-500),'return_pct':float((cash/500-1)*100),'trades':len(trades),'wins':wins,'win_rate':wins/len(trades) if trades else None,'mean_estimated_probability':sum(t['probability'] for t in trades)/len(trades) if trades else None,'mean_estimated_probability_weighted':sum(t['probability']*t['quantity'] for t in trades)/qty if qty else None,'contract_weighted_win_rate':sum(float(t['payout']) for t in trades)/qty if qty else None,'fees':float(fees),'gross_pnl_same_fills':float(gross),'estimated_net_pnl':float(expected),'contracts':qty,'max_quantity':max((t['quantity'] for t in trades),default=0),'max_settled_drawdown_pct':float(dd*100),'skips':dict(reasons),'open_positions_end':0,'coverage':len(trades)/len(rows),'one_contract_reference':unit_stats(reference,sorted({r['entry_time'][:10] for r in rows})),'daily_net':{k:float(v) for k,v in sorted(byday.items())},'top_five_gains':sorted([float(t['net_pnl']) for t in trades],reverse=True)[:5],'top_five_losses':sorted([float(t['net_pnl']) for t in trades])[:5]}
    if save:
        folder=R/'accounts'/strategy/scenario;folder.mkdir(parents=True,exist_ok=True)
        for name,data in [('trades',trades),('decisions',decisions),('events',events),('one-contract-reference',reference)]:
            with (folder/(name+'.jsonl')).open('w') as f:
                for item in data:f.write(json.dumps(item,allow_nan=False)+'\n')
        (folder/'account.json').write_text(json.dumps(summary,indent=2))
    return summary

def main():
    lock=json.loads((R/'prediction-lock.json').read_text())
    for path,digest in lock['hashes'].items():assert sha(R.parent/path)==digest
    prior=json.loads((R.parent/'btc-priced-paper/results.json').read_text())
    for path,digest in prior['source_hashes'].items():assert sha(R.parent/path)==digest
    rows=[json.loads(s) for s in (R/'probabilities.jsonl').read_text().splitlines()]
    results=[]
    for strategy,(minute,threshold) in STRATEGIES.items():
        sample=[r for r in rows if r['period']=='final' and r['minute']==minute];assert len(sample)==3320
        assert [r['start'] for r in sample]==sorted(r['start'] for r in sample)
        for scenario in SCENARIOS:results.append(run(strategy,scenario,sample))
    result={'period':['2026-09-13T00:00:00Z','2026-10-18T00:00:00Z'],'primary_scenario':'edge_gt_0','markets':3320,'accounts':results,'unfiltered_comparators':[a for a in prior['accounts'] if a['scenario']=='delayed_close'],'model_lock_sha256':sha(R/'prediction-lock.json'),'code_sha256':sha(__file__),'fee_model':prior['fee_model'],'limits':['Prior inspected final period','Short pre-period fit and calibration windows','Estimated edge may be miscalibrated','No depth or actual fills','Coinbase/BRTI basis and strike publication unverified','Same-time quote feature and assumed fill: no order latency proof']}
    (R/'results.json').write_text(json.dumps(result,indent=2,allow_nan=False))
    for r in results:print(r['strategy'],r['scenario'],'cash',r['ending_cash'],'pnl',r['net_pnl'],'trades',r['trades'],'win',r['win_rate'],'p_est',r['mean_estimated_probability'],'fees',r['fees'],'expected',r['estimated_net_pnl'],'skips',r['skips'])

if __name__=='__main__':main()
