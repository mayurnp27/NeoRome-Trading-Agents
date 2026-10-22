"""Offline mark/exit review for intact, fully filled debit verticals only.

No order, fill, assignment processing, or shared-ledger mutation is performed.
"""
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal, ROUND_FLOOR
from zoneinfo import ZoneInfo
from .verticals import validate_vertical, _decimal, _aware_datetime, _date


def review_position(entry, quotes, holdings, *, as_of, exit_fee_per_spread,
                    exit_slippage_per_spread, signal_reversed=False):
    """Caller supplies actual holdings and current leg quotes keyed by symbol.

    Negative close values are retained as liabilities, not clamped to zero.
    Such values cannot enter the current nonnegative shared mark adapter.
    """
    result={'status':'blocked','liquidation_value_cents':None,'net_pnl_cents':None,
            'exit_reasons':[],'blockers':[],'broker_execution_enabled':False,
            'shared_mark_eligible':False,'fill_guaranteed':False}
    try:
        now=_aware_datetime(as_of,'as_of')
        if now < entry.as_of:
            raise ValueError('Review precedes entry')
        if type(signal_reversed) is not bool:
            raise ValueError('Signal reversal must be explicit boolean')
        if entry.net_debit <= 0 or entry.net_credit != 0:
            raise ValueError('Only debit vertical positions supported')
        symbols={leg.symbol for leg in entry.legs}
        if set(holdings)!=symbols or set(quotes)!=symbols:
            raise ValueError('Holdings and quotes must match both original legs exactly')
        if any(type(holdings[leg.symbol]) is not int or holdings[leg.symbol] !=
               (entry.quantity if leg.side.lower()=='buy' else -entry.quantity) for leg in entry.legs):
            raise ValueError('Partial, assigned, missing or mismatched holdings require reconciliation')
        days=(_date(entry.legs[0].expiry,'expiry')-now.astimezone(ZoneInfo('America/New_York')).date()).days
        if days<=0:
            raise ValueError('Expiration-day or expired position requires lifecycle reconciliation')
        fee=_decimal(exit_fee_per_spread,'exit fee');slip=_decimal(exit_slippage_per_spread,'exit slippage')
        if fee<0 or slip<0:raise ValueError('Exit costs must be nonnegative')
        current=[]
        for leg in entry.legs:
            quote=quotes[leg.symbol]
            timestamp=_aware_datetime(quote['timestamp'],'quote timestamp')
            if timestamp < entry.as_of:
                raise ValueError('Cannot reuse a pre-entry quote as a position mark')
            size=quote['bid_size' if leg.side.lower()=='buy' else 'ask_size']
            if type(size) is not int or size<entry.quantity:
                raise ValueError('Insufficient closing-side depth')
            current.append(replace(leg,bid=quote['bid'],ask=quote['ask'],timestamp=timestamp,quote_timestamp=None))
        checked=validate_vertical(current,as_of=now,max_quote_age=timedelta(seconds=60))
        # Close the long at bid; buy the short back at ask. Do not use entry sides.
        gross=sum((_decimal(leg.bid,'bid') if leg.side.lower()=='buy' else
                   -_decimal(leg.ask,'ask')) for leg in checked.legs)*100*entry.quantity
        net=gross-(fee+slip)*entry.quantity
        cost=entry.net_debit*100*entry.quantity+entry.fees_and_slippage
        pnl=net-cost
        max_profit=(entry.width-entry.net_debit)*100*entry.quantity-entry.fees_and_slippage
        reasons=[]
        if max_profit>0 and pnl>=max_profit*Decimal('.5'):reasons.append('half_maximum_profit')
        if pnl<=-cost*Decimal('.5'):reasons.append('half_entry_cost_lost')
        if days<=21:reasons.append('21_or_fewer_calendar_days_to_expiry')
        if signal_reversed:reasons.append('signal_reversed')
        result.update(status='reviewed',liquidation_value_cents=int((net*100).to_integral_value(rounding=ROUND_FLOOR)),
                      net_pnl_cents=int((pnl*100).to_integral_value(rounding=ROUND_FLOOR)),
                      gross_close_dollars=str(gross),modeled_exit_cost_dollars=str((fee+slip)*entry.quantity),
                      days_to_expiry=days,exit_reasons=reasons,
                      shared_mark_eligible=net>=0)
        if net<0:result['blockers'].append('Negative liquidation liability unsupported by shared mark adapter')
    except (ValueError,KeyError,TypeError,AttributeError) as exc:
        result['blockers'].append(str(exc))
    return result
