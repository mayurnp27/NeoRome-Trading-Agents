"""Offline, conservative route replay. Never submits or admits an order.

Quoted amounts already include route/platform economics. Explicit additional
fees below must exclude those embedded fees. Full-loss stress is a scenario,
not an assertion that an unresolved position has settled at zero.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from .jupiter_quotes import USDC, QuoteError, validate_mint, raw_amount


def instant(value):
    if not isinstance(value, str):
        raise ValueError('timestamp required')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.utcoffset() is None:
        raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def nonnegative(value):
    if type(value) not in (str, int):
        raise ValueError('exact decimal required')
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError('invalid decimal') from None
    if not result.is_finite() or result < 0:
        raise ValueError('nonnegative finite amount required')
    return result


def additional_fee(cost):
    if not isinstance(cost, dict) or not cost.get('version'):
        raise ValueError('versioned cost assumptions required')
    if cost.get('excludes_embedded_route_fees') is not True:
        raise ValueError('embedded fee treatment required')
    # Extra USD cash cost converted conservatively to integer USDC micro-units.
    return int((nonnegative(cost['additional_usd']) * 1000000).to_integral_value(rounding=ROUND_CEILING))


def route(record, input_mint, output_mint, amount, after, until, max_age):
    requested, received = instant(record['requested_at']), instant(record['received_at'])
    if requested < after or received < requested or received > until:
        raise ValueError('route outside decision/order window')
    if (received-requested).total_seconds() > max_age:
        raise ValueError('stale route')
    q = record['quote']
    if (q['inputMint'], q['outputMint'], int(raw_amount(q['inAmount']))) != (input_mint, output_mint, amount):
        raise ValueError('route identity/quantity mismatch')
    output = int(raw_amount(q['outAmount']))
    minimum = int(raw_amount(q['otherAmountThreshold']))
    if minimum > output:
        raise ValueError('minimum output exceeds quote')
    if q.get('expireAt') and instant(q['expireAt']) <= received:
        raise ValueError('expired route')
    if q.get('priceImpactUnit') != 'percentage_points':
        raise ValueError('current impact units unverified')
    impact_raw = q['priceImpact']
    if type(impact_raw) not in (str, int):
        raise ValueError('exact impact decimal required')
    try:
        impact = Decimal(impact_raw)
    except InvalidOperation:
        raise ValueError('invalid impact') from None
    if not impact.is_finite() or abs(impact) > Decimal('2'):
        raise ValueError('route impact exceeds replay limit')
    if not isinstance(record.get('response_sha256'), str) or len(record['response_sha256']) != 64:
        raise ValueError('source hash required')
    return minimum


def replay(spec, entry, exit_quote=None):
    """One isolated candidate, $5 all-in cap. Does not reserve shared cash.

    Strict read-only research evidence: even complete replay is not an
    eligibility certificate. Real admission also needs independent token-risk
    verification and the shared ledger. A delayed sell must quote the actual
    conservatively modeled acquired quantity, not the optimistic outAmount.
    """
    result = {'schema_version': 1, 'research_only': True,
              'paper_admitted': False, 'execution_enabled': False,
              'status': 'entry_rejected', 'reasons': []}
    try:
        mint = validate_mint(spec['mint'])
        if mint == USDC:
            raise ValueError('token must differ from funding mint')
        decision = instant(spec['decision_at'])
        exit_at = instant(spec['exit_decision_at'])
        if exit_at <= decision:
            raise ValueError('exit must follow entry decision')
        latency = nonnegative(spec['latency_seconds'])
        if latency > 60:
            raise ValueError('unsupported latency')
        delay = timedelta(seconds=float(latency))
        amount = int(raw_amount(spec['input_usdc_raw']))
        entry_fee = additional_fee(spec['entry_cost'])
        exit_fee = additional_fee(spec['exit_cost'])
        reserved = amount + entry_fee + exit_fee
        if reserved > 5000000 or reserved > int(raw_amount(spec['available_cash_usdc_raw'])):
            raise ValueError('all-in cash/idea limit exceeded')
        quantity = route(entry, USDC, mint, amount, decision+delay,
                         min(decision+delay+timedelta(seconds=5), exit_at), 2)
        result.update(status='unresolved', acquired_raw=str(quantity),
                      reserved_usdc_raw=str(reserved),
                      entry_debit_usdc_raw=str(amount+entry_fee),
                      full_loss_stress_usdc_raw=str(-reserved),
                      realized_pnl_usdc_raw=None)
    except (ValueError, KeyError, TypeError, QuoteError) as exc:
        result['reasons'].append(str(exc))
        return result
    if exit_quote is None:
        result['reasons'].append('missing sell route; no invented liquidation')
        return result
    try:
        proceeds = route(exit_quote, mint, USDC, quantity, exit_at+delay,
                         exit_at+delay+timedelta(seconds=5), 2)
        result.update(status='modeled_closed', exit_proceeds_usdc_raw=str(proceeds),
                      realized_pnl_usdc_raw=str(proceeds-reserved))
    except (ValueError, KeyError, TypeError, QuoteError) as exc:
        result['reasons'].append(str(exc))
    return result


def main():
    import argparse
    import hashlib
    import json
    from pathlib import Path
    from .jupiter_quotes import save_new
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.input).read_bytes()
    if len(raw) > 2000000:
        parser.error('input exceeds limit')
    data = json.loads(raw)
    result = replay(data['spec'], data['entry'], data.get('exit_quote'))
    result['input_sha256'] = hashlib.sha256(raw).hexdigest()
    save_new(args.output, result)
    print(json.dumps({'status':result['status'], 'paper_admitted':False}))

if __name__ == '__main__': main()
