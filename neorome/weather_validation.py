"""Prospective weather research registration and finalized-label validation.

No probabilities, trading admission, network, or ledger mutation. NWS remains
only a predictor; outcomes are explicitly venue-reported TWC-target labels.
"""
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal
import base64, hashlib, json, re
from zoneinfo import ZoneInfo
from .weather_vintage import POINT, HOURLY, aware, normalize

NY=ZoneInfo('America/New_York')
VERSION='PM-WEATHER-PAIR-v1'
EVENT=re.compile(r'KXHIGHNY-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})\Z')
MONTHS='JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split()


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


def target_event(day):return f'KXHIGHNY-{day.year%100:02d}{MONTHS[day.month-1]}{day.day:02d}'


def number(value):
    if isinstance(value,bool):raise ValueError('Boolean numeric field')
    n=Decimal(str(value))
    if not n.is_finite():raise ValueError('Nonfinite numeric field')
    return n


def raw_capture(capture, *, source_key, raw_key):
    raw=base64.b64decode(capture[raw_key],validate=True)
    if len(raw)>8*1024*1024 or hashlib.sha256(raw).hexdigest()!=capture[source_key]:
        raise ValueError('Raw evidence hash mismatch')
    requested,received=aware(capture['requested_at']),aware(capture['received_at'])
    if requested>received:raise ValueError('Invalid source chronology')
    data=json.loads(raw)
    if not isinstance(data,dict):raise ValueError('Evidence object required')
    return data,received


def forecast(archive, day):
    if archive.get('source_kind')!='observed' or archive.get('error'):
        raise ValueError('Observed successful forecast required')
    point=archive.get('research_point',{})
    if point.get('latitude')!=40.7812 or point.get('longitude')!=-73.9665:
        raise ValueError('Research point mismatch')
    captures=archive.get('captures',[])
    if len(captures)!=2 or any(c.get('status')!=200 for c in captures):
        raise ValueError('Complete NWS captures required')
    p,pr=raw_capture(captures[0],source_key='sha256',raw_key='raw_base64')
    h,hr=raw_capture(captures[1],source_key='sha256',raw_key='raw_base64')
    url=captures[1].get('url','')
    if captures[0].get('url')!=POINT or not HOURLY.fullmatch(url) or p['properties']['forecastHourly']!=url:
        raise ValueError('NWS endpoint provenance mismatch')
    if pr>aware(captures[1]['requested_at']):raise ValueError('Overlapping collection chronology')
    normalized=normalize(h,captures[1]['received_at'])
    if normalized!=archive.get('normalized'):raise ValueError('Forecast normalization differs from raw evidence')
    start=datetime.combine(day,time(),NY);end=datetime.combine(day+timedelta(days=1),time(),NY)
    if hr>=start:raise ValueError('Forecast received after target day began')
    cursor=start;temperatures=[]
    for period in normalized['periods']:
        a,b=aware(period['start_at']),aware(period['end_at'])
        if b<=start or a>=end:continue
        if b-a!=timedelta(hours=1):raise ValueError('Expected hourly predictor periods')
        if a>cursor:raise ValueError('Forecast coverage gap')
        cursor=max(cursor,b);temperatures.append(number(period['temperature_f']))
    if cursor<end or not temperatures:raise ValueError('Incomplete local-day forecast')
    return {'known_at':hr.isoformat(),'maximum_hourly_predictor_f':str(max(temperatures)),
            'input_sha256':digest(archive),'source_hash':captures[1]['sha256']}


def markets(capture,status):
    if capture.get('source_kind')!='observed' or capture.get('series_ticker')!='KXHIGHNY' or capture.get('venue')!='kalshi_public':
        raise ValueError('Wrong market provenance')
    rows=[r for r in capture.get('responses',[]) if r.get('status')==status]
    if len(rows)!=1:raise ValueError('One market response required')
    row=rows[0];data,received=raw_capture(row,source_key='source_hash',raw_key='raw_response_b64')
    if data.get('cursor')!='' or not isinstance(data.get('markets'),list):raise ValueError('Incomplete markets')
    if row.get('raw_response')!=data or row.get('markets')!=data['markets']:
        raise ValueError('Market normalization differs from raw evidence')
    if row.get('query',{}).get('series_ticker')!='KXHIGHNY' or row.get('query',{}).get('status')!=status:
        raise ValueError('Market query mismatch')
    if received>aware(capture['collected_at']):raise ValueError('Invalid market capture chronology')
    tickers=[m.get('ticker') for m in data['markets']]
    if any(not isinstance(t,str) for t in tickers) or len(set(tickers))!=len(tickers):raise ValueError('Duplicate/missing ticker')
    return data['markets'],received,row['source_hash']


def terms(row):
    return digest({k:row.get(k) for k in ('ticker','event_ticker','market_type','rules_primary','rules_secondary','strike_type','floor_strike','cap_strike','notional_value_dollars','early_close_condition','open_time','close_time','expiration_time','expected_expiration_time','latest_expiration_time')})


def _calibration_contracts(selected, mapping):
    """Bind a reviewed per-event rule map before a forecast is registered.

    The dataset never derives an outcome interval from venue prose.  A future
    approved mapping must name every current ticker, its frozen terms digest,
    close/cutoff, and its mathematical payout interval before registration.
    """
    if mapping is None:return None,None
    from .weather_probability import mapping_identity
    identity=mapping_identity(mapping)
    supplied=mapping.get('contract_definitions')
    if not isinstance(supplied,list) or len(supplied)!=len(selected):raise ValueError('Reviewed contract definitions are incomplete')
    by_ticker={item.get('ticker'):item for item in supplied if isinstance(item,dict)}
    if set(by_ticker)!={m['ticker'] for m in selected}:raise ValueError('Reviewed contract definition tickers differ')
    definitions=[]
    for market in sorted(selected,key=lambda item:item['ticker']):
        item=by_ticker[market['ticker']]
        if item.get('terms_sha256')!=terms(market) or item.get('cutoff_at')!=market.get('close_time'):
            raise ValueError('Reviewed contract definition differs from frozen terms/cutoff')
        # Let the calibration module enforce the narrow mathematical rule
        # grammar.  Importing here avoids making mapping approval implicit in
        # ordinary research-only registration.
        from .weather_probability import _rule
        _rule(item)
        definitions.append(dict(item))
    return identity,definitions


def register(archive,open_capture,*,target_date,registered_at,calibration_mapping=None):
    day=date.fromisoformat(target_date);now=aware(registered_at)
    local=now.astimezone(NY)
    if day!=local.date()+timedelta(days=1) or not time(20,10)<=local.time()<time(20,20):
        raise ValueError('Registration requires day-before 20:10–20:20 New York window')
    f=forecast(archive,day);known=aware(f['known_at'])
    if not time(20)<=known.astimezone(NY).time()<time(20,10) or known.astimezone(NY).date()!=local.date() or known>now:
        raise ValueError('Forecast outside fixed 20:00–20:10 predictor window')
    rows,received,source_hash=markets(open_capture,'open')
    if not timedelta(0)<=now-received<=timedelta(hours=2):raise ValueError('Open-market evidence too old or future')
    event=target_event(day);selected=[m for m in rows if m.get('event_ticker')==event]
    if len(selected)!=6:raise ValueError('Expected complete fixed six-market event')
    for m in selected:
        if (m.get('status')!='active' or m.get('market_type')!='binary' or m.get('is_provisional') is True
            or number(m.get('notional_value_dollars'))!=1 or 'The Weather Company' not in m.get('rules_primary','')
            or 'CLINYC' not in m.get('rules_primary','') or not m['ticker'].startswith(event+'-')):
            raise ValueError('Unsupported current market identity/terms')
        if not aware(m['open_time'])<=received<aware(m['close_time']):raise ValueError('Open-market timing mismatch')
    identity,definitions=_calibration_contracts(selected,calibration_mapping)
    record={'version':VERSION,'target_date':target_date,'event_ticker':event,'registered_at':registered_at,
            'predictor':f,'open_capture_sha256':digest(open_capture),'open_response_sha256':source_hash,
            'contracts':{m['ticker']:terms(m) for m in sorted(selected,key=lambda x:x['ticker'])},
            'source_kind':'observed','label_source':'venue_reported_finalized','paper_eligible':False,
            'calibration_identity':identity,'contract_definitions':definitions}
    record['registration_sha256']=digest(record)
    return record


def pair(registration,archive,open_capture,outcome_capture):
    r=dict(registration);expected=r.pop('registration_sha256',None)
    if digest(r)!=expected or r.get('version')!=VERSION:raise ValueError('Registration hash/version mismatch')
    rebuilt=register(archive,open_capture,target_date=r['target_date'],registered_at=r['registered_at'],calibration_mapping=None)
    # Mapping-approved registrations retain their immutable mapping and
    # definitions. Rebuild the raw forecast/market portion without trying to
    # reconstruct a mapping from the outcome-time call.
    if r.get('calibration_identity') is not None:
        rebuilt['calibration_identity']=r['calibration_identity'];rebuilt['contract_definitions']=r['contract_definitions'];rebuilt['registration_sha256']=digest({k:v for k,v in rebuilt.items() if k!='registration_sha256'})
    if rebuilt!=registration:raise ValueError('Registration differs from original inputs')
    day=date.fromisoformat(r['target_date']);end=datetime.combine(day+timedelta(days=1),time(),NY)
    rows,received,out_hash=markets(outcome_capture,'settled')
    if received<end:raise ValueError('Outcome received before target day ended')
    selected=[m for m in rows if m.get('event_ticker')==r['event_ticker']]
    if set(m['ticker'] for m in selected)!=set(r['contracts']):raise ValueError('Outcome contract set mismatch')
    values=set();labels={}
    for m in selected:
        if terms(m)!=r['contracts'][m['ticker']]:raise ValueError('Contract terms changed since registration')
        if (m.get('status')!='finalized' or m.get('is_provisional') is True or m.get('result') not in ('yes','no')
            or any(m.get(k) not in (None,False,'') for k in ('held','disputed','corrected'))):
            raise ValueError('Outcome not ordinary finalized binary')
        payout=number(m['settlement_value_dollars'])
        if payout!=int(m['result']=='yes'):raise ValueError('Nonbinary or inconsistent payout')
        if not end<=aware(m['settlement_ts'])<=received:raise ValueError('Invalid settlement chronology')
        values.add(number(m['expiration_value']));labels[m['ticker']]=int(payout)
    if len(values)!=1 or sum(labels.values())!=1:raise ValueError('Inconsistent event temperature/one-hot labels')
    value=next(iter(values));prediction=number(r['predictor']['maximum_hourly_predictor_f'])
    result={'version':VERSION,'event_ticker':r['event_ticker'],'target_date':r['target_date'],
            'registration_sha256':expected,'forecast_known_at':r['predictor']['known_at'],
            'registered_at':r['registered_at'],'outcome_known_at':received.isoformat(),
            'outcome_sha256':out_hash,'predictor_max_f':str(prediction),'venue_reported_max_f':str(value),
            'residual_f':str(value-prediction),'labels':labels,'source_kind':'observed',
            'settlement_metadata_sha256':digest([{k:m.get(k) for k in ('ticker','status','is_provisional','result','settlement_value_dollars','expiration_value','settlement_ts')} for m in sorted(selected,key=lambda m:m['ticker'])]),
            'temporal_and_identity_validated':True,'station_mapping_certified':r.get('calibration_identity') is not None,
            'calibration_eligible':False,'paper_eligible':False}
    if r.get('calibration_identity') is not None:
        result['calibration_identity']=r['calibration_identity'];result['contract_definitions']=r['contract_definitions']
    result['pair_sha256']=digest(result)
    return result


def summarize(pairs):
    """Descriptive only; one temperature event is one sample, not six bets."""
    unique={}
    for p in pairs:
        body=dict(p);expected=body.pop('pair_sha256',None)
        if digest(body)!=expected:raise ValueError('Pair hash mismatch')
        if number(p['venue_reported_max_f'])-number(p['predictor_max_f'])!=number(p['residual_f']):
            raise ValueError('Residual projection mismatch')
        event=p['event_ticker']
        if event in unique and unique[event]!=p:raise ValueError('Conflicting event revisions')
        unique[event]=p
    rows=sorted(unique.values(),key=lambda x:x['target_date'])
    residuals=[number(p['residual_f']) for p in rows]
    return {'events':len(rows),'events_required_before_first_calibration_review':50,
            'planned_training_events':30,'planned_untouched_validation_events':20,
            'mean_error_f':str(sum(residuals)/len(rows)) if rows else None,
            'mean_absolute_error_f':str(sum(abs(r) for r in residuals)/len(rows)) if rows else None,
            'calibration_eligible':False,'paper_eligible':False,
            'blockers':['Certified station/settlement methodology','Prospective calibration and holdout evidence','Executable quote/fee and lifecycle acceptance'],
            'orders_sent':0}
