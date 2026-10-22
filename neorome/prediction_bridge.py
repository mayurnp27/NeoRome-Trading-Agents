"""Offline synthetic prediction export -> shared risk reservation; never fills."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
from .paper_ledger import Proposal


def number(value):
    if isinstance(value, bool):
        raise ValueError('Boolean is not a number')
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError('Invalid numeric input') from exc
    if not result.is_finite():
        raise ValueError('Non-finite numeric input')
    return result


def timestamp(value):
    try:
        return datetime.fromtimestamp(float(number(value)), timezone.utc)
    except (OverflowError, OSError) as exc:
        raise ValueError("Timestamp out of range") from exc


def cents(value):
    return int((value * 100).to_integral_value(rounding=ROUND_CEILING))


def prepare(result, config, *, now, exposure_groups, risk_policy):
    """Validate the supported admission subset, without importing the old bot.

    This does not certify a signal, probability calibration, or settlement rule.
    Only synthetic proposals are supported. Exposure grouping is supplied by the
    firm, never inferred from a candidate-generated proposal ID.
    """
    if risk_policy != 'firm_shared_v1':
        raise ValueError('Explicit firm_shared_v1 risk ownership required')
    p = result.get('proposal')
    if p is None:
        return None
    if now.tzinfo is None:
        raise ValueError('Aware evaluation time required')
    if config.get('mode') != 'paper_only' or p.get('synthetic') is not True:
        raise ValueError('Only synthetic paper exports supported')
    if result.get('reason') != 'ok':
        raise ValueError('Candidate was not accepted by its strategy')
    for key in ('proposal_id', 'strategy_id', 'event_id'):
        if not isinstance(p.get(key), str) or not p[key].strip():
            raise ValueError('Missing proposal identity')
    strategy = config['strategies'].get(p['strategy_id'], {})
    if strategy.get('enabled') is not True:
        raise ValueError('Strategy disabled or unknown')
    risk = config['risk']
    max_age = min(number(risk['max_age_seconds']), Decimal(60))
    input_age = min(number(risk['max_input_age_seconds']), Decimal(300))
    if max_age < 0 or input_age < 0:
        raise ValueError('Invalid age limits')
    def fresh(value, age):
        t = timestamp(value)
        if t > now or (now - t).total_seconds() > float(age):
            raise ValueError('Stale or future input')
        return t
    created = fresh(p['created_at'], max_age)
    expiry = timestamp(p['expires_at'])
    if expiry <= now or expiry <= created:
        raise ValueError('Expired proposal')
    if not isinstance(p['input_times'], list) or not p['input_times']:
        raise ValueError('Missing input timestamps')
    for value in p['input_times']:
        fresh(value, input_age)
    ids = p['evidence_ids']
    if not isinstance(ids, list) or not ids or any(not isinstance(x,str) or not x for x in ids) or len(set(ids)) != len(ids):
        raise ValueError('Missing or duplicate evidence')
    evidence = {}
    for key in ids:
        e = config['evidence'][key]
        if e.get('synthetic') is not True or e['event_id'] != p['event_id']:
            raise ValueError('Evidence provenance or event mismatch')
        fresh(e['observed_at'], input_age)
        if timestamp(e['valid_until']) <= now:
            raise ValueError('Expired evidence')
        evidence[key] = e
    if len(p['legs']) != 1:
        raise ValueError('Multi-leg atomicity is not implemented in this bridge')
    leg = p['legs'][0]
    if leg['execution'] != 'taker' or leg['outcome'] not in ('yes', 'no'):
        raise ValueError('Only single-leg taker binary buys supported')
    quantity = leg['quantity']
    if type(risk['max_contracts']) is not int or risk['max_contracts'] <= 0:
        raise ValueError('Invalid source contract limit')
    if leg['venue'] not in ('kalshi', 'polymarket_us'):
        raise ValueError('Unsupported venue')
    if type(quantity) is not int or quantity <= 0 or quantity > risk['max_contracts']:
        raise ValueError('Invalid contract quantity')
    matches = [x for x in config['instruments'] if (x['venue'], x['market_id']) == (leg['venue'], leg['market_id'])]
    if len(matches) != 1:
        raise ValueError('Missing or duplicate instrument review')
    instrument = matches[0]
    if instrument.get('reviewed') is not True or instrument['event_id'] != p['event_id'] or timestamp(instrument['review_valid_until']) <= now:
        raise ValueError('Instrument review invalid')
    q = leg['quote']
    received = fresh(q['received_at'], max_age)
    source = fresh(q['source_time'], max_age)
    if source > received:
        raise ValueError('Quote source is after receipt')
    bid, ask, limit = map(number, (q['bid'], q['ask'], leg['limit_price']))
    if type(q['depth']) is not int or q['depth'] < quantity:
        raise ValueError('Invalid or insufficient depth')
    if not 0 <= bid <= ask <= limit < 1 or ask <= 0:
        raise ValueError('Quote price or depth invalid')
    fee = instrument['fee']
    coefficient, quantum = number(fee['coefficient']), number(fee['round_up'])
    if coefficient < 0 or quantum <= 0:
        raise ValueError('Invalid fee policy')
    factor = Decimal('.25') if limit >= Decimal('.5') else limit * (1-limit)
    per_fee = (coefficient * factor / quantum).to_integral_value(rounding=ROUND_CEILING) * quantum
    cost, fees = cents(limit * quantity), cents(per_fee * quantity)
    # The explicit firm policy replaces the source demo's independent cash,
    # per-strategy/venue budgets and realized-only loss stop. Never imply parity.
    ceiling = 500
    if cost + fees > ceiling:
        raise ValueError('Prediction proposal exceeds funded-cost ceiling')
    group = exposure_groups.get(p['event_id'])
    if not isinstance(group, str) or not group.strip():
        raise ValueError('Firm exposure mapping required')
    payload = {'result': result, 'config': config, 'exposure_groups': exposure_groups}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    return Proposal('prediction:'+p['proposal_id'], 'prediction', p['strategy_id'], 'synthetic-export-bridge-v1',
        digest, source, ':'.join((leg['venue'], leg['market_id'], leg['outcome'])), group,
        'prediction-event:'+p['event_id'], min(expiry, source+timedelta(seconds=60)),
        1, cost, fees, 'synthetic',
        {'instrument_units': quantity, 'lot_kind':'aggregate_funded_proposal',
         'source_proposal':p, 'source_diagnostics':result.get('diagnostics', {}),
         'evidence_projection':evidence, 'instrument_review':instrument,
         'signal_validated':False, 'broker_execution_enabled':False,
         'risk_policy':'firm_shared_v1', 'source_risk_policy_replaced':risk,
         'source_strategy_config':strategy})


def reserve(result, config, book, *, now, exposure_groups, risk_policy):
    """Reserve only. Caller supplies an existing book with current full marks.

    Never seeds or refreshes marks, simulates fills, imports source accounting,
    or creates a second independently funded firm portfolio.
    """
    if not book.enforce_market_risk:
        raise ValueError('Shared market-risk enforcement required')
    limits = book.limits
    if limits.per_idea_cents['prediction'] > 500 or limits.group_cap_cents > 3000 or limits.aggregate_cap_cents > 8000:
        raise ValueError('Ledger limits exceed firm_shared_v1 policy')
    balances = book.balances()
    if sum(b.initial_cash_cents for b in balances) > 80000 or any(b.desk == 'prediction' and b.initial_cash_cents > 10000 for b in balances):
        raise ValueError('Ledger capital exceeds firm_shared_v1 allocation')
    proposal = prepare(result, config, now=now, exposure_groups=exposure_groups, risk_policy=risk_policy)
    return None if proposal is None else book.reserve(proposal, now=now)
