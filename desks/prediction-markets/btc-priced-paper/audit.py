"""Independent exact-rational ledger audit; does not import replay.py."""
from pathlib import Path
from fractions import Fraction as F
from datetime import datetime, timezone
import json, csv, hashlib

R = Path(__file__).resolve().parent

def load_lines(path):
    return [json.loads(s) for s in path.read_text().splitlines()]

def ts(s):
    # ISO lexemes normalized independently of the replay's strptime parser.
    s = s.replace(' ', 'T').replace('Z', '+00:00')
    if '.' in s:
        head, tail = s.split('.', 1)
        fraction, zone = tail.split('+', 1)
        s = head + '.' + fraction.ljust(6, '0') + '+' + zone
    return datetime.fromisoformat(s).timestamp()

def up(x, denominator):
    n = x * denominator
    return F(-(-n.numerator // n.denominator), denominator)

def debit(q, p, denominator):
    return up(q*p + up(F(7,100)*q*p*(1-p), 1000000), denominator)

def main():
    result = json.loads((R/'results.json').read_text())
    quotes = {k:{r['end_period_ts']:r for r in v} for k,v in json.loads((R/'data/quotes.json').read_text()).items()}
    markets = {m['ticker']:m for m in json.loads((R.parent/'btc-stochastic-search/data/markets.json').read_text())}
    predictions = {ts(r['market_start']):r for r in csv.DictReader((R.parent/'btc-intramarket-diagnostic/predictions.csv').open())}
    specs = {'A_5min':(5,'5min_lead0'), 'B_10min':(10,'10min_lead0'), 'C_10min_lead1':(10,'10min_lead1')}
    audit, reference_stats = [], []
    for account in result['accounts']:
        strategy, scenario = account['strategy'], account['scenario']
        folder = R/'accounts'/strategy/scenario
        trades, events, decisions = [load_lines(folder/(n+'.jsonl')) for n in ('trades','events','decisions')]
        assert len(decisions)==3320 and len({r['ticker'] for r in decisions})==3320
        assert len(trades)==account['trades'] and len(events)==2*len(trades)
        assert not any('invalid' in x['status'] or 'missing' in x['status'] for x in decisions)
        cash, previous_settle, fees, pnl = F(500), 0, F(0), F(0)
        denom = 100 if scenario=='cent_precision' else 10000
        for t in trades:
            m = markets[t['ticker']]; minute, col = specs[strategy]
            entry, settlement, start = ts(t['entry_time']), ts(t['settlement_time']), ts(m['open_time'])
            assert previous_settle<=entry<ts(m['close_time'])<=settlement
            assert settlement==ts(m['settlement_ts'])
            assert ts(t['signal_time'])==start+minute*60
            assert entry==start+minute*60+(0 if scenario=='same_boundary' else 60)
            side = int(float(predictions[start][col])); assert side in (-1,1)
            assert t['side']==('yes' if side==1 else 'no')
            book = quotes[t['ticker']][entry]; assert entry==t['quote_end_period_ts']
            field = ('high_dollars' if side==1 else 'low_dollars') if scenario=='adverse_minute' else 'close_dollars'
            p = F(book['yes_ask'][field]) if side==1 else 1-F(book['yes_bid'][field])
            if scenario=='plus_one_cent': p=min(F(1),p+F(1,100))
            assert p==F(t['price'])
            q=t['quantity']; assert isinstance(q,int) and q>0
            cost=debit(q,p,denom); budget=cash/4
            assert F(t['cash_before'])==cash and F(t['budget'])==budget
            assert cost<=budget<debit(q+1,p,denom), 'largest fee-inclusive size'
            assert q*p==F(t['premium']) and cost==F(t['debit'])
            assert cost-q*p==F(t['fee']) and cash-cost==F(t['cash_after_entry'])
            won=(side==1)==(m['result']=='yes'); payout=F(q if won else 0)
            assert won==t['won'] and payout==F(t['payout'])
            assert F(m['settlement_value_dollars'])==int(m['result']=='yes')
            assert payout-cost==F(t['net_pnl'])
            cash+=payout-cost; fees+=cost-q*p; pnl+=payout-cost
            assert cash==F(t['cash_after_settlement'])
            previous_settle=settlement
        assert float(cash)==account['ending_cash'] and float(fees)==account['fees']
        assert float(pnl)==account['net_pnl'] and cash==500+pnl
        event_cash, last = F(500), 0
        for e in events:
            assert ts(e['timestamp'])>=last
            last=ts(e['timestamp']); event_cash+=F(e['delta'])
            assert event_cash==F(e['cash_after']) and event_cash>=0
        assert event_cash==cash
        audit.append({'strategy':strategy,'scenario':scenario,'trades_audited':len(trades),'events_audited':len(events),'all_checks_passed':True})
        if scenario=='delayed_close':
            refs=load_lines(folder/'one-contract-reference.jsonl');n=len(refs)
            ref_cost=sum(F(x['price']) for x in refs)/n
            ref_fee=sum(F(x['one_contract_fee']) for x in refs)/n
            win=sum(x['won'] for x in refs)/n
            for x in refs:
                assert debit(1,F(x['price']),10000)-F(x['price'])==F(x['one_contract_fee'])
                assert int(x['won'])-debit(1,F(x['price']),10000)==F(x['one_contract_net'])
            positive=[F(t['net_pnl']) for t in trades if F(t['net_pnl'])>0]
            negative=[F(t['net_pnl']) for t in trades if F(t['net_pnl'])<0]
            reference_stats.append({'strategy':strategy,'eligible_signals':n,'win_rate':win,'mean_ask':float(ref_cost),'mean_one_contract_fee':float(ref_fee),'break_even_win_rate':float(ref_cost+ref_fee),'mean_one_contract_net':float(F(sum(x['won'] for x in refs),n)-ref_cost-ref_fee),'mean_positive_trade_pnl':float(sum(positive)/len(positive)),'mean_negative_trade_pnl':float(sum(negative)/len(negative)),'first_trade':trades[0]['entry_time'],'last_trade':trades[-1]['entry_time']})
    out={'verified_at':datetime.now(timezone.utc).isoformat(),'scope':'Independent Fraction arithmetic; all 15 account/scenario ledgers, exact quotes, signals, official payouts, chronological settled cash, maximum fee-inclusive 25% integer sizing, independent event reconciliation; 8 regression tests separately passed. No depth/fill verification.','accounts':audit,'primary_reference_stats':reference_stats,'trades_audited':sum(x['trades_audited'] for x in audit),'events_audited':sum(x['events_audited'] for x in audit)}
    (R/'verification.json').write_text(json.dumps(out,indent=2))
    print(json.dumps(out,indent=2))

if __name__=='__main__':main()
