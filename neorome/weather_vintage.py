"""Bounded public NWS research archive; no probabilities or trading path."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import urllib.request

POINT = 'https://api.weather.gov/points/40.7812,-73.9665'
HOURLY = re.compile(r'https://api\.weather\.gov/gridpoints/[A-Z]{3}/[0-9]+,[0-9]+/forecast/hourly\Z')
MAX_BYTES = 2_000_000


def aware(value):
    if not isinstance(value, str):
        raise ValueError('Timestamp must be a string')
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('Timestamp needs timezone')
    return dt.astimezone(timezone.utc)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def transport(url):
    request = urllib.request.Request(url, headers={
        'User-Agent': 'NeoRome-Research/1.0', 'Accept': 'application/geo+json'})
    with urllib.request.build_opener(NoRedirect).open(request, timeout=15) as response:
        raw = response.read(MAX_BYTES + 1)
        return response.status, dict(response.headers), raw


def normalize(body, received_at):
    receipt = aware(received_at)
    properties = body['properties']
    timestamps = {}
    for key in ('updateTime', 'generatedAt'):
        if properties.get(key) is not None:
            t = aware(properties[key])
            if t > receipt:
                raise ValueError('Provider timestamp is after receipt')
            timestamps[key] = t.isoformat()
    if not timestamps:
        raise ValueError('Provider vintage timestamp missing')
    periods = properties['periods']
    if not isinstance(periods, list) or not periods or len(periods) > 500:
        raise ValueError('Invalid forecast periods')
    normalized = []
    last_end = None
    for period in periods:
        start, end = aware(period['startTime']), aware(period['endTime'])
        temp = period['temperature']
        if end <= start or (last_end is not None and start < last_end):
            raise ValueError('Overlapping or unordered forecast periods')
        if isinstance(temp, bool) or not isinstance(temp, (int, float)) or not math.isfinite(temp):
            raise ValueError('Invalid temperature')
        if period['temperatureUnit'] != 'F':
            raise ValueError('Expected Fahrenheit')
        normalized.append({'start_at': start.isoformat(), 'end_at': end.isoformat(),
                           'temperature_f': temp})
        last_end = end
    if last_end <= receipt:
        raise ValueError('Forecast contains no future periods')
    return {'known_at': received_at, 'provider_timestamps': timestamps, 'periods': normalized}


def collect(fetch=transport, clock=lambda: datetime.now(timezone.utc)):
    captures = []
    def get(url):
        if url != POINT and not HOURLY.fullmatch(url):
            raise ValueError('Endpoint outside allowlist')
        if len(captures) >= 2:
            raise ValueError('Request budget exhausted')
        started = clock().isoformat()
        status, headers, raw = fetch(url)
        received = clock().isoformat()
        if len(raw) > MAX_BYTES:
            raise ValueError('Response exceeds limit')
        captures.append({'url': url, 'status': status, 'requested_at': started,
                         'received_at': received, 'sha256': hashlib.sha256(raw).hexdigest(),
                         'raw_base64': base64.b64encode(raw).decode(),
                         'http_headers': {k:v for k,v in headers.items() if k.lower() in
                             ('date','age','cache-control','last-modified','etag')}})
        if status != 200:
            raise ValueError('Unexpected HTTP status')
        return json.loads(raw), received
    result = {'schema_version': 1, 'source_kind': 'observed', 'research_point':
              {'latitude':40.7812,'longitude':-73.9665,'settlement_station_certified':False},
              'captures': captures, 'paper_eligible': False, 'probability': None,
              'orders_enabled': False, 'normalized': None, 'error': None}
    try:
        point, _ = get(POINT)
        url = point['properties']['forecastHourly']
        forecast, received = get(url)
        result['normalized'] = normalize(forecast, received)
    except Exception as exc:
        # Preserve successful raw reads even if normalization or the next request
        # fails. Errors carry no provider body or credentials.
        result['error'] = {'type': type(exc).__name__, 'message': str(exc)[:240]}
    result['request_responses'] = len(captures)
    return result


def save_new(path, result):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(result, indent=2, allow_nan=False)+'\n').encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as out:
        out.write(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    if Path(args.output).exists():
        parser.error('Output exists; choose a new archive name')
    result = collect()
    save_new(args.output, result)
    print(json.dumps({'output':args.output, 'responses':result['request_responses'],
                      'periods':len((result['normalized'] or {}).get('periods', [])),
                      'error':result['error'], 'paper_eligible':False}))
    return 1 if result['error'] else 0

if __name__ == '__main__':
    raise SystemExit(main())
