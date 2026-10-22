"""Exact, offline statement cash check for supplied option execution records.

Does not release reserves, infer fills, or reconcile account-wide transfers.
"""
from decimal import Decimal
import json
from .verticals import _decimal, parse_occ_symbol


def reconcile_cash(executions, *, opening_cash_cents, closing_cash_cents):
    if type(opening_cash_cents) is not int or type(closing_cash_cents) is not int:
        raise ValueError('Cash balances must be integer cents')
    seen={};delta=0;fees=0
    for e in executions:
        key=e['execution_id']
        if not isinstance(key,str) or not key:raise ValueError('Execution ID required')
        canonical=json.dumps(e,sort_keys=True,allow_nan=False)
        if key in seen:
            if seen[key]!=canonical:raise ValueError('Conflicting duplicate execution')
            continue
        seen[key]=canonical
        parse_occ_symbol(e['symbol'])
        qty=e['quantity'];fee=e['fee_cents'];price=_decimal(e['price'],'execution price')
        if type(qty) is not int or qty<=0 or type(fee) is not int or fee<0 or price<0:
            raise ValueError('Invalid execution quantity, price or fee')
        if e.get('multiplier')!=100 or type(e.get('multiplier')) is not int or e.get('adjusted') is not False:
            raise ValueError('Only verified standard 100-share contracts supported')
        if e['side'] not in ('buy','sell'):raise ValueError('Invalid side')
        amount=price*qty*100*100
        if amount!=amount.to_integral_value():raise ValueError('Sub-cent cash requires explicit broker rounding policy')
        delta+=(1 if e['side']=='sell' else -1)*int(amount)-fee;fees+=fee
    difference=closing_cash_cents-opening_cash_cents-delta
    return {'execution_cash_delta_cents':delta,'fees_cents':fees,'unexplained_cash_cents':difference,
            'cash_matches':difference==0,'unique_executions':len(seen),'risk_release_allowed':False,
            'note':'Only supplied fills/fees reconciled; deposits, assignments, interest and other activity require separate records.'}
