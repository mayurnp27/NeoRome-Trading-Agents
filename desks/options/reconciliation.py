"""Offline option holdings reconciliation. No cash release or broker mutations."""
import json
from .verticals import _aware_datetime


def reconcile(entry, reports, snapshot, *, as_of):
    """Compare cumulative per-order leg fills to a complete holdings snapshot.

    Reports must contain order_id, report_id, symbol, intent (open/close),
    cumulative_quantity and timestamp. Assignment is flagged, never inferred
    or automatically hedged. Caller must supply all relevant orders/holdings.
    """
    now=_aware_datetime(as_of,'as_of')
    symbols={leg.symbol:leg for leg in entry.legs}
    expected={s:0 for s in symbols};orders={};seen={};last=entry.as_of
    def blocked(reason):
        return {'state':'reconciliation_required','blockers':[reason],
                'expected_holdings':expected,'spread_valuation_allowed':False,
                'risk_release_allowed':False,'orders_enabled':False}
    for report in reports:
        key=report['report_id']
        if not isinstance(key,str) or not key:return blocked('Missing report ID')
        payload=json.dumps(report,sort_keys=True,allow_nan=False)
        if key in seen:
            if seen[key]!=payload:return blocked('Conflicting duplicate report')
            continue
        seen[key]=payload
        stamp=_aware_datetime(report['timestamp'],'report timestamp')
        if stamp<last or stamp>now:return blocked('Report ordering or timing invalid')
        last=stamp
        symbol=report['symbol'];intent=report['intent'];order=report['order_id'];qty=report['cumulative_quantity']
        if symbol not in symbols or intent not in ('open','close') or not isinstance(order,str) or not order:
            return blocked('Invalid order identity')
        if type(qty) is not int or qty<0 or qty>entry.quantity:return blocked('Invalid cumulative fill quantity')
        prior=orders.get(order)
        if prior and (prior[0:2]!=(symbol,intent) or qty<prior[2]):return blocked('Conflicting order or decreasing cumulative fill')
        delta=qty-(prior[2] if prior else 0)
        direction=1 if symbols[symbol].side.lower()=='buy' else -1
        expected[symbol]+=direction*delta*(1 if intent=='open' else -1)
        if not 0<=expected[symbol]*direction<=entry.quantity:return blocked('Overfill or close exceeds open holdings')
        orders[order]=(symbol,intent,qty)
    if snapshot.get('complete') is not True:return blocked('Incomplete holdings snapshot')
    stamp=_aware_datetime(snapshot['timestamp'],'snapshot timestamp')
    if stamp<last or stamp>now or (now-stamp).total_seconds()>60:return blocked('Snapshot stale or predates reports')
    holdings=snapshot['option_holdings'];underlying=snapshot['underlying_quantity']
    if set(holdings)!=set(symbols) or any(type(v) is not int for v in holdings.values()):return blocked('Invalid snapshot leg holdings')
    if type(underlying) is not int:return blocked('Invalid underlying holdings')
    if underlying!=0:return blocked('Underlying exposure requires assignment/exercise reconciliation')
    if holdings!=expected:return blocked('Holdings differ from execution reports; possible assignment or missing activity')
    if snapshot.get('open_orders_reconciled') is not True:return blocked('Outstanding order state unresolved')
    intact=all(expected[s]==(entry.quantity if leg.side.lower()=='buy' else -entry.quantity) for s,leg in symbols.items())
    flat=all(q==0 for q in expected.values())
    return {'state':'intact' if intact else 'flat' if flat else 'partial',
            'expected_holdings':expected,'blockers':[] if intact or flat else ['Incomplete spread'],
            'spread_valuation_allowed':intact,'risk_release_allowed':False,
            'orders_enabled':False,'unique_reports':len(seen),
            'note':'Quantity reconciliation only; cash, fees and actual fill basis remain separate.'}
