"""Bounded local prospective weather dataset maintenance; zero network/orders."""
import argparse, fcntl, json, os
from datetime import datetime,timedelta,time,timezone
from pathlib import Path
from .weather_validation import NY, register, pair, summarize, target_event, markets, aware
from .prediction_evidence import read_evidence
from .crypto_pilot import _atomic_json

DEFAULT_STATE='/var/lib/neorome-weather-dataset'
DEFAULT_FORECASTS='/var/lib/neorome-weather-recorder'
DEFAULT_MARKETS='/var/lib/neorome-prediction-evidence/evidence'
MAX_STATE=32*1024*1024


def read(path,limit=16*1024*1024):
    path=Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size>limit:raise ValueError('Unsafe dataset input')
    value=json.loads(path.read_text())
    if not isinstance(value,dict):raise ValueError('Dataset object required')
    return value


def referenced(root,name):
    if not isinstance(name,str) or Path(name).name!=name:raise ValueError('Invalid evidence reference')
    path=root/name
    if path.is_symlink():raise ValueError('Evidence symlink refused')
    return path


def run(state=DEFAULT_STATE,forecasts=DEFAULT_FORECASTS,market_root=DEFAULT_MARKETS,*,now=None,calibration_mapping=None):
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None:raise ValueError('Aware clock required')
    local=now.astimezone(NY);root=Path(state);fr=Path(forecasts);mr=Path(market_root)
    for p in (root,fr,mr):
        if p.is_symlink():raise ValueError('Dataset root symlink refused')
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (root/'dataset.lock').open('a+') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if any(p.is_symlink() for p in root.rglob('*')):raise ValueError('State symlink refused')
        if sum(p.stat().st_size for p in root.rglob('*') if p.is_file())>MAX_STATE:raise ValueError('Dataset storage cap')
        for name in ('registrations','pairs','attempts'):(root/name).mkdir(exist_ok=True,mode=0o700)
        captures=sorted([*mr.glob('*.json.gz'),*mr.glob('*.json')],key=lambda p:p.name.split('.')[0])
        # Collector names are UTC hour keys; reject mismatched names instead of
        # trusting an arbitrary lexicographic filename as capture chronology.
        newest=None
        for candidate in captures[-48:]:
            item=read_evidence(candidate);collected=aware(item['collected_at'])
            if candidate.name.split('.')[0]!=collected.astimezone(timezone.utc).strftime('%Y%m%dT%H'):
                raise ValueError('Market archive filename/collection mismatch')
            if collected>now:raise ValueError('Future market archive')
            if newest is None or collected>newest[0]:newest=(collected,candidate,item)
        latest_path,latest=(None,None) if newest is None else newest[1:]
        if latest is not None:
            # Validate both raw projections before accepting any stored response.
            for kind in ('open','settled'):
                _,received,_=markets(latest,kind)
                if received>now:raise ValueError('Future market evidence')
        results=[]
        if time(20,10)<=local.time()<time(20,20):
            target=local.date()+timedelta(days=1);attempt=root/'attempts'/f'{target}.json'
            if not attempt.exists():
                _atomic_json(attempt,{'status':'claimed','claimed_at':now.isoformat(),'target_date':str(target)})
                forecasts_recent=sorted(fr.glob('*.forecast.json'))[-3:]
                candidates=[];reasons=[]
                if latest is not None:
                    for path in forecasts_recent:
                        archive=read(path)
                        try:
                            record=register(archive,latest,target_date=str(target),registered_at=now.isoformat(),calibration_mapping=calibration_mapping)
                            candidates.append((record['predictor']['known_at'],path,record))
                        except (ValueError,KeyError,TypeError) as exc:reasons.append(type(exc).__name__)
                if candidates:
                    # Freeze the first valid capture from the declared predictor window.
                    _,forecast_path,record=min(candidates,key=lambda x:x[0])
                    _atomic_json(root/'registrations'/f'{target}.json',{'registration':record,'forecast_file':forecast_path.name,'open_file':latest_path.name})
                    _atomic_json(attempt,{'status':'registered','target_date':str(target),'registered_at':now.isoformat()})
                else:
                    _atomic_json(attempt,{'status':'no_eligible_inputs','target_date':str(target),'checked_at':now.isoformat(),'rejection_types':reasons})
        settled_rows=[]
        if latest is not None:settled_rows=markets(latest,'settled')[0]
        settled_events={m.get('event_ticker') for m in settled_rows}
        prior=read(root/'status.json') if (root/'status.json').exists() else {}
        revisions=list(prior.get('revision_alerts',[]))
        for path in sorted((root/'registrations').glob('*.json')):
            item=read(path);reg=item['registration'];day=reg['target_date'];event=target_event(datetime.fromisoformat(day).date())
            if reg['event_ticker']!=event:raise ValueError('Registration event mismatch')
            paired=root/'pairs'/f'{day}.json'
            if event not in settled_events:continue
            archive=read(referenced(fr,item['forecast_file']))
            opened=read_evidence(referenced(mr,item['open_file']))
            try:result=pair(reg,archive,opened,latest)
            except (ValueError,KeyError,TypeError) as exc:
                results.append({'event_ticker':event,'status':'outcome_rejected','error_type':type(exc).__name__})
                if paired.exists():revisions.append(event)
                continue
            if paired.exists():
                old=read(paired)
                keys=('registration_sha256','labels','venue_reported_max_f','predictor_max_f','residual_f','settlement_metadata_sha256')
                if any(old.get(k)!=result.get(k) for k in keys):revisions.append(event)
            else:_atomic_json(paired,result)
        pairs=[read(p) for p in sorted((root/'pairs').glob('*.json'))]
        incomplete=[p.name for p in (root/'attempts').glob('*.json') if read(p).get('status')=='claimed']
        status=summarize(pairs)
        status.update(updated_at=now.isoformat(),registrations=len(list((root/'registrations').glob('*.json'))),
                      rejected_outcomes=results,revision_alerts=sorted(set(revisions)),incomplete_registration_attempts=incomplete,operational_healthy=not revisions and not incomplete,
                      status='research_only' if latest is not None else 'waiting_for_market_archive')
        _atomic_json(root/'status.json',status)
        return status


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--state',default=DEFAULT_STATE)
    p.add_argument('--forecasts',default=DEFAULT_FORECASTS);p.add_argument('--markets',default=DEFAULT_MARKETS)
    p.add_argument('--calibration-mapping',help='Reviewed prospective mapping JSON; omitted keeps research-only registration')
    a=p.parse_args()
    try:
        mapping=read(a.calibration_mapping) if a.calibration_mapping else None
        r=run(a.state,a.forecasts,a.markets,calibration_mapping=mapping);print(json.dumps(r,sort_keys=True));raise SystemExit(0 if r['operational_healthy'] else 1)
    except Exception as exc:
        print(json.dumps({'operational_healthy':False,'error_type':type(exc).__name__,'orders_sent':0}));raise SystemExit(1)
