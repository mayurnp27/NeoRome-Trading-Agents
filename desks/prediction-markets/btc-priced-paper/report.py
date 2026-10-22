"""Build the research report and standalone balance chart from saved ledgers."""
from pathlib import Path
from datetime import datetime, timezone
import json, hashlib, html, math
from reportlab.graphics.shapes import Drawing, String, Line
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics import renderSVG
from reportlab.lib.colors import HexColor

R=Path(__file__).resolve().parent
data=json.loads((R/'results.json').read_text())
verification=json.loads((R/'verification.json').read_text())
primary=[a for a in data['accounts'] if a['scenario']=='delayed_close']
names={'A_5min':'A · Follow the lead at minute 5','B_10min':'B · Follow the lead at minute 10','C_10min_lead1':'C · Minute 10, normalized lead ≥1'}
colors=['#cb691d','#007e84','#7353b6']
fig=Drawing(1100,440)
ax=LinePlot();ax.x=80;ax.y=75;ax.width=980;ax.height=310
plot_data=[]
for a,color in zip(primary,colors):
    rows=[json.loads(s) for s in (R/'accounts'/a['strategy']/'delayed_close'/'trades.jsonl').read_text().splitlines()]
    dates=[datetime(2026,9,13,tzinfo=timezone.utc)]+[datetime.fromisoformat(t['settlement_time']) for t in rows]+[datetime(2026,10,18,tzinfo=timezone.utc)]
    balances=[500]+[float(t['cash_after_settlement']) for t in rows]+[a['ending_cash']]
    points=[(dates[0].timestamp(),math.log10(balances[0]))]
    for i in range(1,len(dates)):
        points.extend([(dates[i].timestamp(),math.log10(balances[i-1])),(dates[i].timestamp(),math.log10(balances[i]))])
    plot_data.append(points)
ax.data=plot_data;ax.joinedLines=1
ax.xValueAxis.valueMin=dates[0].timestamp();ax.xValueAxis.valueMax=dates[-1].timestamp()
ax.xValueAxis.valueSteps=[datetime(2026,9,13,tzinfo=timezone.utc).timestamp()+i*86400 for i in (0,7,14,21,28,35)]
ax.xValueAxis.labelTextFormat=lambda v:datetime.fromtimestamp(v,timezone.utc).strftime('%b %d')
ax.yValueAxis.valueMin=-.5;ax.yValueAxis.valueMax=3.5;ax.yValueAxis.valueSteps=[0,1,2,math.log10(500),3]
ax.yValueAxis.labelTextFormat=lambda v:f'${10**v:,.0f}'
ax.yValueAxis.visibleGrid=1;ax.yValueAxis.gridStrokeColor=HexColor('#e2e7e4')
for i,c in enumerate(colors):ax.lines[i].strokeColor=HexColor(c);ax.lines[i].strokeWidth=1.6
fig.add(ax);fig.add(String(80,410,'Settled balance in USD · log scale · 2026 UTC',fontName='Helvetica',fontSize=13))
for i,(a,c) in enumerate(zip(primary,colors)):
    x=80+i*330;fig.add(Line(x,27,x+22,27,strokeColor=HexColor(c),strokeWidth=2));fig.add(String(x+29,23,names[a['strategy']],fontSize=10))
renderSVG.drawToFile(fig,str(R/'balances.svg'))

def money(v):return f'${v:,.2f}'
def pct(v):return f'{100*v:.2f}%'
def table(headers,rows):
    return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows)+'</tbody></table></div>'

main_table=table(['Strategy','Trades','Win rate¹','Fees','Ending cash','Net P&L','Return'],[[names[a['strategy']],f"{a['trades']:,}",pct(a['win_rate']),money(a['fees']),money(a['ending_cash']),money(a['net_pnl']),f"{a['return_pct']:.2f}%"] for a in primary])
ref_table=table(['Strategy','Eligible signals','All-signal win rate','Mean ask','Mean fee²','Break-even rate²','Net per contract²'],[[names[a['strategy']],a['eligible_signals'],pct(a['win_rate']),f"{100*a['mean_ask']:.3f}¢",f"{100*a['mean_one_contract_fee']:.3f}¢",pct(a['break_even_win_rate']),f"{100*a['mean_one_contract_net']:.3f}¢"] for a in verification['primary_reference_stats']])
sensitivity=table(['Pricing scenario','A ending cash','B ending cash','C ending cash'],[[label]+[money(next(a['ending_cash'] for a in data['accounts'] if a['strategy']==s and a['scenario']==scenario)) for s in names] for scenario,label in [('delayed_close','Primary: quote close 60 seconds after signal'),('same_boundary','Optimistic: quote close at signal boundary'),('adverse_minute','Adverse quote during next minute'),('plus_one_cent','Primary plus 1¢ per contract'),('cent_precision','Primary with cent balance rounding')]])
detail=table(['Strategy','Gross P&L, same fills','Largest quantity','Settled drawdown','Longest losing streak','Capital-budget skips'],[[names[a['strategy']],money(a['gross_pnl_same_fills']),a['max_trade_quantity'],f"{a['max_settled_balance_drawdown_pct']:.2f}%",a['longest_net_losing_streak'],a['skips'].get('insufficient_quarter_budget',0)] for a in primary])

page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Three Kalshi BTC paper replays</title>
<style>body{font:16px/1.55 system-ui,sans-serif;color:#152a35;background:#f4f5f1;margin:0}main{max-width:1160px;margin:auto;padding:42px 28px 70px}h1{font-size:42px;line-height:1.12;max-width:800px}h2{margin-top:38px;font-size:23px}p{max-width:970px}.kicker{font-size:13px;letter-spacing:.14em;text-transform:uppercase;color:#547067}.lead{font-size:21px}.box{border-left:4px solid #b55631;padding:12px 20px;background:#fff4ed}.scroll{overflow-x:auto}table{width:100%;border-collapse:collapse;background:white;font-size:14px}th,td{text-align:right;border-bottom:1px solid #e2e7e4;padding:12px 10px;white-space:nowrap}th:first-child,td:first-child{text-align:left;white-space:normal}th{background:#e9eeeb;font-weight:650}img{width:100%;background:white}small,.muted{color:#536570}a{color:#03687a}.links a{margin-right:18px}footer{margin-top:38px;padding-top:18px;border-top:1px solid #ccd8d0;font-size:13px}</style><main>
<div class="kicker">NeoRome · Historical internal paper simulation · 18 October 2026</div>
<h1>Higher accuracy did not produce profit.</h1>
<p class="lead">All three $500 accounts lost more than 99% under the requested rule: allocate 25% of current settled cash per trade, including fees.</p>
<p>September 13–October 17, 2026 UTC · 3,320 eligible Kalshi KXBTC15M markets inherited from the prior study. These are finite historical paper accounts, not broker demo accounts or observed fills. No running strategy or live order was enabled.</p>
<div class="box"><strong>What is actual, and what is modeled?</strong> Entry prices come from historical Kalshi bid/ask candles. Fees are calculated from the published taker schedule and archived fee metadata. The full order filling at the quoted price is hypothetical; historical order-book depth and queue execution are unavailable.</div>
<h2>Three independent $500 accounts</h2>
MAIN_TABLE
<p class="muted">¹ Win rate on trades that the funded account could take. It differs from the full signal set because an account stops placing trades when its 25% budget cannot buy one contract. Each account begins with a $125 maximum fee-inclusive debit. Cash compounds after official settlement; accounts never borrow, share funds or reset.</p>
<img src="balances.svg" alt="Log-scale settled balance chart for the three paper accounts, showing depletion over the 35-day window">
<p class="muted">The plot and drawdown use balances after settlement, not marked-to-market equity. Some cheaper contracts can become affordable again after earlier budget skips.</p>
<h2>Why 93% accuracy still lost money</h2>
<p>Strategy C predicted 1,495 of 1,605 eligible outcomes correctly (93.15%). Buying one contract on every signal would have cost an average 93.632¢ plus 0.370¢ in fees: approximately a 94.00% break-even win rate. The realized average was a loss of 0.855¢ per contract. The loss remained negative before fees. A high probability of winning was already reflected in the purchase price.</p>
REF_TABLE
<p class="muted">² One-contract reference across every eligible signal, regardless of available account cash. These are descriptive realized averages, not estimates proven to persist out of sample. Rounding is applied per single-contract order. Portfolio contract counts vary with price and cash, so trade-level win rate must not be compared directly with a contract-weighted purchase price.</p>
<p>In the funded C account, profitable trades averaged $1.66 and losing trades averaged −$28.47. With 25% of available cash exposed each time, occasional losses overwhelmed the frequent small gains. Reducing the stake would reduce the speed of depletion but would not establish a positive trading edge.</p>
<h2>Pricing and fee sensitivity</h2>
SENSITIVITY
<p>Each scenario independently starts at $500. All 15 strategy/scenario combinations lost more than 99%. Later expensive prices can change quantities and which later trades fit the budget, so ending balances are not monotonic in price. The adverse-minute case deliberately uses the worst recorded quote in that minute and is a stress test, not a decision rule.</p>
DETAIL
<p class="muted">Gross P&L removes fees from the identical filled quantities; it does not re-simulate a fee-free sizing path. Capital-budget skips are not missing data. The C filter also rejects 1,715 markets before sizing.</p>
<h2>Execution rules and evidence</h2>
<p>A follows the side of the original settlement strike at minute 5. B follows it at minute 10. C uses minute 10 only when absolute normalized lead is at least 1: log(current Coinbase price / strike) divided by the standard deviation of the last twelve completed five-minute log returns, scaled by the square root of remaining five-minute units. These rules and the threshold were fixed before inspecting the quote-priced returns.</p>
<p>The main replay buys at the close of the Kalshi minute candle ending 60 seconds after the signal. YES uses the ask; NO uses one minus the YES bid. It requires an exact quote timestamp, valid noncrossed prices, and an entry before market close. Whole contracts are sized to the largest quantity whose premium plus fees fits 25% of settled cash. Only one position may be open per account. Official <code>settlement_ts</code> controls when a payout becomes spendable.</p>
<p>The modeled taker fee is <code>0.07 × contracts × price × (1 − price)</code>, rounded upward to six decimals, with the total buyer debit aligned upward to $0.0001 for a direct member. Cent balance precision is separately tested. There is no settlement fee. Archived series metadata specifies a quadratic fee and multiplier 1; the historical series fee-change response is empty, and none of the 19 event fee overrides returned applies to KXBTC15M.</p>
<p>Sources: <a href="https://kalshi.com/docs/kalshi-fee-schedule.pdf">Kalshi fee schedule, effective July 7, 2026</a>; <a href="https://docs.kalshi.com/getting_started/fee_rounding">fee rounding</a>; <a href="https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks">bid/ask candlesticks API</a>; <a href="https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes">series fee history</a>; <a href="https://docs.kalshi.com/api-reference/events/get-event-fee-changes">event fee overrides</a>.</p>
<h2>Validation and limits</h2>
<p>All 3,320 market quote histories were retrieved in 166 successful cached requests. Eight focused tests passed. An independent audit using exact rational arithmetic reconciled 7,334 trades and 14,668 cash events across all 15 scenarios, including quote side/time, signal, fee, maximum size, payout, and cash chronology. Independent Luna review found no accounting or timing defect and requested full input hashes, now included.</p>
<p>This reuses a period already inspected during earlier stochastic/SMA research. It is not fresh out-of-sample confirmation. Coinbase candles are not Kalshi's BRTI settlement feed; original strike publication times are not archived. The one-minute delay does not reconstruct tick-level latency. Candle quotes do not prove the quoted size was fillable. The data are the prior eligible market subset, not every possible market. The conclusion is limited to these rules, this period, and these fill assumptions.</p>
<p>Research decision: none of the three rules qualifies as a profitable strategy on this evidence. The next useful experiment would forecast settlement probability, compare it with executable cost after fees, and permit a trade only for a calibrated positive price edge. It needs fresh prospective quote/depth observations and a separate sizing decision; it should not tune against these already inspected returns.</p>
<p class="links"><a href="results.json">All account results</a><a href="verification.json">Independent audit</a><a href="manifest.json">Input provenance</a><a href="EXPERIMENT.md">Frozen experiment</a><a href="accounts/C_10min_lead1/delayed_close/trades.jsonl">C trade ledger</a></p>
<footer>Canonical report: <code>btc-priced-paper-20261018-v1</code>. Research input repair task: <code>btc-priced-inputs-20261018-v1</code>. No live execution or account-permission changes. Source folder: <code>desks/prediction-markets/btc-priced-paper/</code>.</footer>
</main></html>'''
for key,value in [('MAIN_TABLE',main_table),('REF_TABLE',ref_table),('SENSITIVITY',sensitivity),('DETAIL',detail)]:page=page.replace(key,value)
(R/'report.html').write_text(page)
inputs=[R.parent/'btc-stochastic-search/data/markets.json',R.parent/'btc-stochastic-search/data/candles.json',R.parent/'btc-intramarket-diagnostic/predictions.csv']+sorted((R/'data').glob('*.json'))
code=[R/n for n in ('collect.py','replay.py','audit.py','test_replay.py','report.py','EXPERIMENT.md')]
manifest={'created_at':datetime.now(timezone.utc).isoformat(),'branch':'master; research files uncommitted','window':data['period'],'scope':'Three historical internal-paper accounts; five fixed pricing scenarios; hypothetical top-of-book full fills. No broker/demo orders.','hashes':{str(p.relative_to(R.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs+code},'request_batches':166,'markets':3320,'primary_reference_stats':verification['primary_reference_stats'],'source_urls':['https://kalshi.com/docs/kalshi-fee-schedule.pdf','https://docs.kalshi.com/getting_started/fee_rounding','https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks','https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes','https://docs.kalshi.com/api-reference/events/get-event-fee-changes'],'limits':['No depth or actual execution/charged fees','Inspected prior period; not fresh holdout','Final-metadata strike publication unverified','Coinbase/BRTI basis and tick latency','Inherited eligible market subset']}
(R/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(R/'report.html')
