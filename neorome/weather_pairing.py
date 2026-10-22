"""Audit prospective day-ahead forecast/outcome pairs; never creates trades."""
from datetime import datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
from zoneinfo import ZoneInfo
from .weather_vintage import aware


def evaluate(archive, outcome_capture, *, target_date, event_ticker):
    """Require pre-midnight receipt and full local-day coverage for this design.

    Venue-reported finalized labels are observations, not independent station
    certification. No probability or strategy score is inferred from temperature.
    """
    raw=outcome_capture['raw'].encode()
    if hashlib.sha256(raw).hexdigest()!=outcome_capture['sha256']:
        raise ValueError('Outcome response hash mismatch')
    data=json.loads(raw)
    local=ZoneInfo('America/New_York')
    day=datetime.strptime(target_date,'%Y-%m-%d').date()
    start=datetime.combine(day,time(),local);end=datetime.combine(day+timedelta(days=1),time(),local)
    reasons=[]
    if archive.get('source_kind')!='observed' or archive.get('error') or not archive.get('normalized'):
        raise ValueError('Valid observed forecast archive required')
    f=archive['normalized'];known=aware(f['known_at'])
    if known>=start:reasons.append('Forecast was not received before target local day began')
    cursor=start
    for p in f['periods']:
        a,b=aware(p['start_at']),aware(p['end_at'])
        if b<=start or a>=end:continue
        if a>cursor:break
        cursor=max(cursor,b)
    if cursor<end:reasons.append('Forecast does not cover the entire target local day')
    if data.get('cursor'):reasons.append('Outcome response is paginated/incomplete')
    markets=data.get('markets',[])
    if not markets or any(m.get('event_ticker')!=event_ticker for m in markets):
        raise ValueError('Wrong or absent event markets')
    if aware(outcome_capture['received_at'])<end:
        reasons.append('Outcome captured before target local day ended')
    labels=[]
    values=set()
    for m in markets:
        result=m.get('result');payout=Decimal(str(m.get('settlement_value_dollars','NaN')))
        value=Decimal(str(m.get('expiration_value','NaN')))
        valid=(m.get('status')=='finalized' and result in ('yes','no') and payout.is_finite()
               and payout==Decimal(1 if result=='yes' else 0) and value.is_finite())
        if not valid:reasons.append('Nonfinal or nonbinary/incomplete outcome: '+m['ticker'])
        else:values.add(str(value))
        labels.append({'ticker':m['ticker'],'status':m.get('status'),'result':result,
                       'settlement_value_dollars':m.get('settlement_value_dollars'),
                       'expiration_value':m.get('expiration_value'),'ordinary_binary_label':valid})
    if len(values)!=1:reasons.append('Event expiration values missing or inconsistent')
    return {'target_date':target_date,'event_ticker':event_ticker,'forecast_known_at':known.isoformat(),
            'design':'day-ahead-before-local-midnight-v1','temporal_and_label_checks_pass':not reasons,
            'rejections':reasons,'venue_reported_temperature_f':next(iter(values)) if len(values)==1 else None,
            'labels':labels,'independent_settlement_source_verified':False,
            'calibration_eligible':False,'paper_eligible':False,'probability':None,
            'remaining_gate':'Certified station/source mapping and reviewed forecast/outcome dataset required'}
