"""Render the saved result; never select or retune a model."""
import html,json,hashlib
from pathlib import Path
R=Path(__file__).resolve().parent
j=json.loads((R/'results.json').read_text()); data=j['data']; chosen=j['selected']['overall']
def pct(x):return f'{100*x:.2f}%'
rows=[]
for name,c in j['selected'].items():
 h=c['holdout']; ci=c['bootstrap']
 rows.append('<tr>'+''.join('<td>'+html.escape(str(x))+'</td>' for x in [name,c['meta']['id'],f"{c['train']['correlation']:.4f}",f"{c['validation']['correlation']:.4f}",f"{h['correlation']:.4f}",pct(h['accuracy']),h['active'],pct(h['coverage']),f"{pct(ci['accuracy_95'][0])}–{pct(ci['accuracy_95'][1])}"] )+'</tr>')
h=chosen['holdout']; ci=chosen['bootstrap']; established=ci['correlation_95'][0]>0
conclusion=('The selected signal retained positive holdout correlation with a daily-block interval above zero. This is directional evidence, not verified trading profitability.' if established else 'The selected signal did not establish a reliable positive holdout correlation: its daily-block confidence interval includes zero or negative values. No trading edge is established.')
body=f'''<!doctype html><html><head><meta charset="utf-8"><title>BTC stochastic search</title><style>body{{font:16px/1.6 system-ui;max-width:1250px;margin:40px auto;padding:0 24px;color:#182535}}h1{{font-size:32px}}table{{border-collapse:collapse;font-size:14px;width:100%}}td,th{{padding:10px;border-bottom:1px solid #ccd4dd;text-align:left}}td:nth-child(2){{max-width:380px;overflow-wrap:anywhere}}.lead{{background:#eef3f9;padding:20px;border-radius:12px}}code{{overflow-wrap:anywhere}}small{{color:#47566b}}</style></head><body>
<h1>BTC stochastic search · Kalshi 15-minute markets</h1><p>October 18, 2026 · Research only</p>
<p class="lead"><b>{conclusion}</b></p>
<p>{j['trials']:,} trials: 67,200 single-timeframe variants plus 2,600 combinations of training-selected candidates. These exhaust the defined grid and shortlist, not every mathematical stochastic setting or unrestricted multi-timeframe tuple.</p>
<p><b>Data:</b> {data['official_markets']:,} official Kalshi outcomes from {data['first_market']} through {data['last_market']}. Coinbase BTC-USD supplies chart features. {data['raw_candles']:,} five-minute candles; {data['missing_5m_bars']} missing slots remain missing. Splits: {html.escape(str(data['split_counts']))}; one-day embargo at each boundary.</p>
<h2>Frozen selection and final evaluation</h2><p>The overall model was chosen on validation correlation, before opening holdout results. The three timeframe rows are prespecified secondary comparisons, not replacements selected from holdout.</p>
<table><thead><tr>{''.join('<th>'+x+'</th>' for x in ['Candidate','Parameters / rule','Train r','Validation r','Holdout r','Accuracy','Signals','Coverage','Accuracy 95% block CI'])}</tr></thead><tbody>{''.join(rows)}</tbody></table>
<p>Parameter notation: timeframe : lookback : K smoothing : D smoothing : rule : orientation. Orientation −1 reverses the signal. Mean combines normalized scores; unanimous trades only when all component signs agree.</p>
<p>Overall correlation 95% daily-block interval: {ci['correlation_95'][0]:.4f} to {ci['correlation_95'][1]:.4f}. Always predicting the training-majority direction scored {pct(j['baseline']['holdout']['accuracy'])} on all holdout markets. On the selected model's active markets, always-UP scored {pct(h['up_fraction_active'])}; always-DOWN scored {pct(1-h['up_fraction_active'])}.</p>
<p>The prespecified 15-minute secondary candidate, STOCH(5,1,1) with a turn from 20/80, scored 53.57% across 995 holdout signals (daily-block 95% interval 51.10%–56.07%, correlation 0.0392). This is the more promising candidate for fresh prospective testing, but it is not a replacement primary winner selected on untouched validation. D smoothing does not affect this K-turn rule.</p><p>The strongest combined candidate on validation used unanimous reversed K-level signs from 5m(53,3,1) and 30m(8,1,1): validation correlation 0.0342 and accuracy 51.77% across 3,019 signals. It did not beat the single-timeframe validation winner, so no new holdout selection was made.</p><h2>Holdout stability</h2><ul>{''.join('<li>Third '+str(i+1)+': '+pct(s['accuracy'])+' accuracy, '+str(s['active'])+' signals, correlation '+f'{s["correlation"]:.4f}'+'</li>' for i,s in enumerate(chosen['subperiods']))}</ul>
<h2>What this can and cannot support</h2><ul>
<li>Official Kalshi yes/no results are the target. Kalshi uses averages of CF Benchmarks BRTI around the contract boundaries, not Coinbase close-to-close prices.</li>
<li>Only fully closed candles are used. Coinbase history is a retrospective data source; availability delay at the exact boundary and vendor revisions are not modeled. Events occur only at their chart's close boundary.</li>
<li>Lookback 5–60; K and D SMA smoothing 1–5; 5/15/30-minute charts; eight rules in both orientations. Combinations use ten distinct training-selected signals per timeframe, all pairs and triples, with mean and unanimous rules.</li>
<li>Final confidence intervals use 2,000 daily-block resamples. They do not remove every selection or regime risk; the three secondary comparisons are not multiplicity-adjusted. The inspected holdout cannot be reused as untouched data.</li>
<li>Observed accuracy of {pct(h['accuracy'])} implies a descriptive average all-in break-even cost of {h['accuracy']:.4f} dollars per $1 payout, assuming the same future hit rate and fixed cost. Its lower block bound is {ci['accuracy_95'][0]:.4f}. Neither is a quote or validated buying limit. Entry asks, fees, depth and latency were not backtested; no P&amp;L claim is made.</li>
<li>Existing strategy code, paper accounts, and live trading settings were not changed.</li></ul>
<h2>Reproduce and inspect</h2><p>Run <code>collect.py</code> once (cached public responses), <code>test_search.py</code>, then <code>search.py</code> with NumPy and pandas. <code>selection-lock.json</code> records the pre-holdout choices; <code>trials.csv</code> retains all train/validation trials; <code>holdout-predictions.csv</code> retains the final winner's scores; <code>results.json</code> contains metrics and input hashes.</p>
<p><a href="https://docs.kalshi.com/api-reference/market/get-markets">Kalshi market API</a> · <a href="https://docs.kalshi.com/api-reference/historical/get-historical-markets">Kalshi historical API</a> · <a href="https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles">Coinbase candles API</a></p></body></html>'''
(R/'report.html').write_text(body)
manifest={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in R.iterdir() if p.is_file() and p.name not in ('manifest.json','run.log')}
(R/'manifest.json').write_text(json.dumps(manifest,indent=2))
print(conclusion)
