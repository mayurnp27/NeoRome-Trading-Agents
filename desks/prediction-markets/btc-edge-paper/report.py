"""Publish every frozen probability-filter scenario and its limitations."""
from pathlib import Path
from datetime import datetime,timezone
import json,html,math,hashlib
from reportlab.graphics.shapes import Drawing,String,Line
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics import renderSVG
from reportlab.lib.colors import HexColor

R=Path(__file__).resolve().parent
result=json.loads((R/'results.json').read_text());diag=json.loads((R/'probability-diagnostics.json').read_text());verify=json.loads((R/'verification.json').read_text())
primary=[a for a in result['accounts'] if a['scenario']=='edge_gt_0']
names={'A_5min':'A · Minute 5','B_10min':'B · Minute 10','C_10min_lead1':'C · Minute 10, strong lead'}
def money(x):return f'${x:,.2f}'
def pct(x):return '—' if x is None else f'{100*x:.2f}%'
def table(headers,rows):return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(x))+'</td>' for x in r)+'</tr>' for r in rows)+'</tbody></table></div>'
main=table(['Account','Trades','Win rate','Fees','Ending balance','Net P&L','Return'],[[names[a['strategy']],a['trades'],pct(a['win_rate']),money(a['fees']),money(a['ending_cash']),money(a['net_pnl']),f"{a['return_pct']:.2f}%"] for a in primary])
sensitivity=table(['Required estimated edge / cost scenario']+[names[s] for s in names],[[label]+[money(next(a['ending_cash'] for a in result['accounts'] if a['strategy']==s and a['scenario']==scenario)) for s in names] for scenario,label in [('edge_gt_0','Strictly positive net edge'),('edge_gt_1c','More than 1¢ net edge per contract'),('edge_gt_3c','More than 3¢ net edge per contract'),('cost_plus_1c','Fill 1¢ worse; gate includes this cost'),('cent_precision','Cent account rounding')]])
ref=table(['Account','Qualifying one-contract signals','Mean estimated win probability','Actual win rate','Mean realized net / contract','Daily-block 95% interval'],[[names[a['strategy']],a['one_contract_reference']['trades'],pct(a['one_contract_reference']['mean_probability']),pct(a['one_contract_reference']['win_rate']),f"{100*a['one_contract_reference']['mean_net']:.3f}¢",' to '.join(f'{100*x:.3f}¢' for x in a['one_contract_reference']['daily_block_net_95'])] for a in primary])
calibration=table(['Forecast','Period','Observations','Model Brier score','Midpoint Brier score'],[[f'Minute {m}',period,diag['metrics'][m][period]['probability']['n'],f"{diag['metrics'][m][period]['probability']['brier']:.6f}",f"{diag['metrics'][m][period]['midpoint']['brier']:.6f}"] for period in ('validation','final') for m in ('5','10')])
cs=[json.loads(s) for s in (R/'accounts/C_10min_lead1/edge_gt_0/trades.jsonl').read_text().splitlines()]
examples=table(['Entry UTC','Model win probability','Quote paid','Fee / contract','Estimated edge / contract','Trade P&L','Cash after settlement'],[[t['entry_time'][:16].replace('T',' '),pct(t['probability']),f"{100*float(t['price']):.2f}¢",f"{100*float(t['fee'])/t['quantity']:.3f}¢",f"{100*float(t['expected_net'])/t['quantity']:.3f}¢",money(float(t['net_pnl'])),money(float(t['cash_after_settlement']))] for t in cs])
details=table(['Account','Model-estimated total net P&L¹','Realized total net P&L','Max settled drawdown','Previous unfiltered balance'],[[names[a['strategy']],money(a['estimated_net_pnl']),money(a['net_pnl']),f"{a['max_settled_drawdown_pct']:.2f}%",money(next(p['ending_cash'] for p in result['unfiltered_comparators'] if p['strategy']==a['strategy']))] for a in primary])

fig=Drawing(1100,440);plot=LinePlot();plot.x=80;plot.y=75;plot.width=980;plot.height=310;points=[];colors=['#cb691d','#007e84','#7353b6']
first=datetime(2026,9,13,tzinfo=timezone.utc).timestamp();last=datetime(2026,10,18,tzinfo=timezone.utc).timestamp()
for a in primary:
    trades=[json.loads(s) for s in (R/'accounts'/a['strategy']/'edge_gt_0/trades.jsonl').read_text().splitlines()]
    ps=[(first,math.log10(500))];prior=500
    for t in trades:
        timestamp=datetime.fromisoformat(t['settlement_time'].replace('Z','+00:00')).timestamp();cash=float(t['cash_after_settlement'])
        ps.extend([(timestamp,math.log10(prior)),(timestamp,math.log10(cash))]);prior=cash
    ps.append((last,math.log10(prior)));points.append(ps)
plot.data=points;plot.joinedLines=1;plot.xValueAxis.valueMin=first;plot.xValueAxis.valueMax=last;plot.xValueAxis.valueSteps=[first+i*86400 for i in (0,7,14,21,28,35)];plot.xValueAxis.labelTextFormat=lambda x:datetime.fromtimestamp(x,timezone.utc).strftime('%b %d')
plot.yValueAxis.valueMin=-.5;plot.yValueAxis.valueMax=3.5;plot.yValueAxis.valueSteps=[0,1,2,math.log10(500),3];plot.yValueAxis.labelTextFormat=lambda x:f'${10**x:,.0f}';plot.yValueAxis.visibleGrid=1;plot.yValueAxis.gridStrokeColor=HexColor('#e0e7e2')
for i,c in enumerate(colors):plot.lines[i].strokeColor=HexColor(c);plot.lines[i].strokeWidth=1.7
fig.add(plot);fig.add(String(80,410,'Settled balance · USD, log scale · positive estimated edge only',fontSize=13))
for i,(s,c) in enumerate(zip(names,colors)):
    x=80+i*330;fig.add(Line(x,27,x+22,27,strokeColor=HexColor(c),strokeWidth=2));fig.add(String(x+29,23,names[s],fontSize=11))
renderSVG.drawToFile(fig,str(R/'balances.svg'))

page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC probability versus Kalshi price</title>
<style>body{margin:0;background:#f4f6f2;color:#162d36;font:16px/1.55 system-ui,sans-serif}main{max-width:1180px;margin:auto;padding:40px 26px 70px}h1{font-size:42px;line-height:1.14;max-width:850px}h2{font-size:23px;margin-top:36px}.kicker{font-size:13px;text-transform:uppercase;letter-spacing:.12em;color:#527467}.lead{font-size:21px}p{max-width:1020px}.box{background:#fff2e6;border-left:4px solid #bd6b2d;padding:14px 20px}.scroll{overflow-x:auto}table{width:100%;border-collapse:collapse;background:white;font-size:14px}th,td{padding:11px 10px;border-bottom:1px solid #dfe7e2;text-align:right;white-space:nowrap}th{background:#e7eeea}th:first-child,td:first-child{text-align:left;white-space:normal}img{width:100%;background:white}.muted{color:#586b70;font-size:14px}a{color:#087786}footer{border-top:1px solid #cfdbd4;margin-top:40px;padding-top:18px;font-size:13px}</style><main>
<div class="kicker">NeoRome · 18 October 2026 · Historical internal-paper research</div>
<h1>A positive estimated edge still did not produce profit.</h1>
<p class="lead">The filter preserved much more capital in the strong-lead account by rejecting almost every trade. None of the three primary accounts finished above $500.</p>
<p>September 13–October 17, 2026 UTC · Same 3,320 eligible markets as the earlier test · Each independent account starts with $500 and allocates 25% of current settled cash, including fees. Prices are historical Kalshi quotes; fills are hypothetical. No real funds were traded, and no broker/demo account or running strategy was enabled.</p>
<div class="box"><strong>The tested rule:</strong> buy only when <code>quantity × estimated win probability &gt; premium + taker fees</code>. The probability model is fitted on earlier data and includes the current Kalshi quote. Passing this rule guarantees only that the <em>model's estimate</em> is positive, not that the true expected return is positive.</div>
<h2>Three $500 accounts</h2>MAIN
<p class="muted">A follows the lead relative to the original strike at minute 5; B at minute 10; C at minute 10 only when normalized lead is at least 1. The quote and fill are taken at signal time plus 60 seconds. The win rates above apply only to funded, selected trades, not the original full signal set.</p>
<img src="balances.svg" alt="Settled balances after filtering on estimated net edge: A and B deplete, while C takes three trades and finishes near442 dollars">
<h2>Why the estimated advantage failed</h2>
<p>The model overestimated the win rates of selected A and B opportunities. Across every qualifying one-contract signal, A predicted 82.59% wins and achieved 79.29%; B predicted 58.64% and achieved 53.10%. Those errors were larger than the estimated trading advantages. Both forecasts also had slightly worse Brier scores than the Kalshi midpoint baseline on the chronological validation slice and the final period.</p>
REF
<p class="muted">The one-contract reference ignores account depletion and applies fees per individual contract. It measures selection quality without capital-path censoring; it is not the compounded account return. Intervals resample 35 daily blocks 2,000 times and are descriptive, not proof of future performance. C has only three observations, so its uncertainty is enormous.</p>
CALIBRATION
<p class="muted">Lower Brier score is better. Validation covers September8–11 (376 eligible observations per model); final covers September13–October17. These are point comparisons, not a claim of statistically significant superiority or inferiority.</p>
DETAILS
<p class="muted">¹ Sum of each funded trade's model-estimated net P&L at its actual simulated quantity. It is not a realized return or independently validated expectation. Drawdown uses cash after settlement rather than marked-to-market equity. The requested 25% stake makes losses and compounding severe.</p>
<h2>The three strong-lead trades</h2>
<p>C rejected 1,602 of its 1,605 original strong-lead signals for insufficient estimated edge. The three accepted opportunities had very small estimated advantages, averaging less than 0.1¢ per contract. They were lower-probability, cheaper contracts; the high-probability contracts generally cost too much after fees. Their resulting 33.33% win rate does not contradict the earlier 93.15% rate across all strong-lead signals.</p>
EXAMPLES
<h2>Fixed sensitivity checks</h2>SENSITIVITY
<p>All 15 account/scenario runs are retained. None earned a positive return. Requiring more than 1¢ or 3¢ of estimated net edge, or adding 1¢ to the modeled fill cost, made C take no trades and preserve $500. B's 3¢ threshold ended at $178.77 on 28 trades. This is capital preservation or reduced loss, not an identified profitable strategy.</p>
<h2>How probabilities were estimated</h2>
<p>Two fixed ridge-logistic models predict whether the original +5/+10 signal side wins. Inputs are the chosen-side Kalshi midpoint at delayed entry, a Gaussian transform of the prior normalized BTC lead, and trade direction. Coefficients shrink toward a midpoint-only prediction. A separate logistic intercept/slope calibrates probabilities. Feature choices, ridge penalty 10 and all edge thresholds were fixed before the cash results were inspected; no winner was chosen afterward.</p>
<p>Training: August 18–31, with 1,326 valid +5-minute and 1,325 valid +10-minute valid rows; calibration: September 2–6, 471 rows each. Only outcomes settled before the respective September 1 and 7 cutoffs are used. September 1, 7 and 12 market-start days are embargoed. Validation is assigned by market start; a late September 11 outcome may settle during September 12, but validation never influences fitting or selection. Models and row hashes were saved before cash replay.</p>
<p>The original longer July archive plan was revised only because public archived requests hit repeated rate limits, before any model fit or new P&L inspection. It is retained as a superseded plan. The revised complete pre-test quote window contains 2,367 markets retrieved in 119 public batches; partial July samples are excluded entirely. The short training and calibration windows are a material limitation.</p>
<h2>Accounting, validation and limits</h2>
<p>YES uses the historical ask; NO uses one minus the YES bid. The largest whole-contract order whose premium plus fees fits 25% of available cash is sized first, then the estimated edge gate is applied. Cash is released only at official settlement. There is at most one open position per account, no leverage, resets or shared funds. The audited standard quadratic taker fee and direct-member rounding are inherited from the prior replay; cent rounding is separately tested.</p>
<p>Six focused tests passed. The independent audit reconstructed all 49,800 decisions, 3,247 trades and 6,494 cash events. It checked source quotes/features, every predicted probability, fitted-objective gradients, label cutoffs, no final/validation labels in fitting, exact fee-inclusive sizing/gates, official payouts and cash chronology. Read-only Luna review accepted the methodology with the documented validation-boundary note.</p>
<p>This is an already inspected historical period, not a fresh untouched holdout. Depth and fills are unverified; using the quote as a feature and assuming immediate execution at it does not establish order latency. Final-metadata strike publication and Coinbase/BRTI basis remain unresolved. The results test this specific estimated-probability model, not every possible model or whether genuinely positive expected value can be profitable.</p>
<p>Research decision: the price filter alone does not establish an edge. None of these accounts qualifies for promotion on this evidence. A new experiment would need more pre-period data, demonstrably better probability calibration conditional on price, and prospective quote/depth validation before any profitability claim.</p>
<p>Sources: <a href="https://kalshi.com/docs/kalshi-fee-schedule.pdf">Kalshi fee schedule</a>, <a href="https://docs.kalshi.com/getting_started/fee_rounding">rounding rules</a>, <a href="https://docs.kalshi.com/getting_started/historical_data">historical data tiers</a>, <a href="https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks">bid/ask candles</a>.</p>
<p><a href="results.json">All results</a> · <a href="verification.json">Independent audit</a> · <a href="EXPERIMENT.md">Frozen v2 experiment</a> · <a href="models.json">Models and fitting rows</a> · <a href="prediction-lock.json">Pre-replay model lock</a> · <a href="manifest.json">Provenance</a> · <a href="accounts/C_10min_lead1/edge_gt_0/trades.jsonl">C trade ledger</a></p>
<footer>Canonical research report: <code>btc-edge-paper-20261018-v1</code>. Research-only scope; no runtime repair, trading-service deployment or permission change.</footer></main></html>'''
for key,value in [('MAIN',main),('REF',ref),('CALIBRATION',calibration),('DETAILS',details),('EXAMPLES',examples),('SENSITIVITY',sensitivity)]:page=page.replace(key,value)
(R/'report.html').write_text(page)
inputs=[R/'data/quotes.json',R/'data/cutoff.json',R.parent/'btc-intramarket-diagnostic/predictions.csv',R.parent/'btc-stochastic-search/data/markets.json',R.parent/'btc-priced-paper/data/quotes.json',R.parent/'btc-priced-paper/data/series.json',R.parent/'btc-priced-paper/data/fee-history.json',R.parent/'btc-priced-paper/data/event-fee-history-00.json']+sorted((R/'data').glob('current-*.json'))
files=inputs+[R/name for name in ('EXPERIMENT.md','EXPERIMENT-v1-archive-plan.md','collect.py','model.py','replay.py','audit.py','test_edge.py','report.py','models.json','prediction-lock.json','results.json','verification.json','review.json','probability-diagnostics.json')]
manifest={'created_at':datetime.now(timezone.utc).isoformat(),'branch':'master; new research files uncommitted','hashes':{str(p.relative_to(R.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},'preperiod_markets':2367,'successful_current_quote_requests':119,'excluded_partial_archive_samples':True,'experiment_revision_reason':'Historical endpoint429, before fit/P&L; no result-driven tuning','profit_evaluation_period':result['period'],'cash_accounts':'Three separate500USD internal-paper accounts,25%current settled cash including fees; five fixed scenarios each','verification':verify,'model_and_threshold_selection':'None after profit evaluation; all scenarios reported','memory_policy':'New research, no runtime/software issue investigation. No unrelated repair-memory lookup or repair closeout.'}
(R/'manifest.json').write_text(json.dumps(manifest,indent=2));print(R/'report.html')
