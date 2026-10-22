"""Finite read-only capability probe; never an admission proof or trading loop."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import ssl
import sys
import time
from urllib.parse import quote

ROOT = Path('/opt/neorome')
PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
DISCOVERY_ADDRESS = 'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM'
HOST = 'mainnet.helius-rpc.com'
MAX_REPLY = 1024 * 1024
MAX_CALLS = 21


def utc():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def compact(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class RpcReader:
    """Fixed TLS origin, bounded reads and pre-I/O shared/lane reservation."""
    def __init__(self, quota, archive, key_loader, *, max_calls=MAX_CALLS):
        self.quota, self.archive, self.key_loader = quota, Path(archive), key_loader
        self.max_calls = max_calls
        self.records = []
        self.response_bytes = 0

    def __call__(self, method, params):
        if method not in {'getSignaturesForAddress', 'getTransaction'} or len(self.records) >= self.max_calls:
            raise ValueError('discovery_rpc_bound')
        if method == 'getSignaturesForAddress' and params != [DISCOVERY_ADDRESS, {'limit':20, 'commitment':'confirmed'}]:
            raise ValueError('discovery_index_request_invalid')
        if method == 'getTransaction' and (not isinstance(params,list) or len(params)!=2
                or not isinstance(params[0],str) or not re.fullmatch(r'[1-9A-HJ-NP-Za-km-z]{64,88}',params[0])
                or params[1]!={'encoding':'json','commitment':'confirmed','maxSupportedTransactionVersion':1}):
            raise ValueError('discovery_transaction_request_invalid')
        remaining = min(MAX_REPLY, 8*1024*1024-self.response_bytes)
        if remaining <= 0:
            raise ValueError('discovery_total_reply_bound')
        request_id = len(self.records) + 1
        request = compact({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params})
        if len(request) > 2048:
            raise ValueError('discovery_request_bound')
        requested = utc()
        reservation = self.quota.reserve_actual_attempt('helius_rpc', requested[:10])
        # Keep attempted calls counted even on a transport failure.
        record = {'method': method, 'params': params, 'requestId': request_id,
                  'requestedAt': requested, 'provider': 'Helius',
                  'endpoint': 'https://' + HOST + '/', 'productionTransport': True,
                  'tlsPeerIdentityVerified': True, 'creditReservation': reservation,
                  'rawRequestBase64': base64.b64encode(request).decode(), 'rawRequestSha256': sha(request)}
        self.records.append(record)
        connection = None
        try:
            key = self.key_loader()
            if not isinstance(key, str) or not 1 <= len(key) <= 256 or any(ord(c) < 33 or ord(c) > 126 for c in key):
                raise ValueError('discovery_credential_unavailable')
            connection = http.client.HTTPSConnection(HOST, timeout=5, context=ssl.create_default_context())
            connection.request('POST', '/?api-key=' + quote(key, safe=''), body=request,
                               headers={'Content-Type': 'application/json', 'Accept': 'application/json',
                                        'Accept-Encoding': 'identity', 'User-Agent': 'NeoRome-readonly-source-probe/1'})
            response = connection.getresponse()
            if response.status != 200 or response.getheader('Content-Encoding', 'identity') != 'identity':
                raise ValueError('discovery_http_rejected')
            raw = response.read(remaining + 1)
            if len(raw) > remaining:
                raise ValueError('discovery_reply_bound')
            self.response_bytes += len(raw)
            record.update(receivedAt=utc(), rawResponseBase64=base64.b64encode(raw).decode(), rawResponseSha256=sha(raw))
            path = self.archive / ('rpc-%02d.json' % request_id)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(compact(record)); stream.flush(); os.fsync(stream.fileno())
            return record
        finally:
            if connection is not None:
                connection.close()


def main():
    wrapper = ROOT / 'operations/maintenance_agent/bcp3_scheduler.py'
    if sha(wrapper.read_bytes()) != '09ab4d8db27eedee71a11b2bc28dd40b7da3041bed509f904f639b121e9f1e13':
        raise ValueError('scheduler_pin_changed')
    scheduler = load(wrapper, 'bcp3_probe_budget_wrapper')
    base = scheduler.load_base()
    parent = base._load('bcp3_probe_baseline', ROOT / base.PARENT, base.PARENT_SHA256)
    base.install_production_route(parent, repo_root=ROOT, test_mode=False)
    modules = parent._production_modules()
    capture_error = modules['account_capture'].CaptureError
    def reserve(day):
        started = time.monotonic()
        for attempt in range(11):
            try:
                return modules['account_capture']._reserve_credit(parent.SHARED_STATE_ROOT,day=day,daily_credits=1929)
            except capture_error as error:
                if error.code!='state_busy' or attempt==10 or time.monotonic()-started>=0.5:
                    raise
                time.sleep(0.05)
    reserve.capture_error_type = capture_error
    quota = modules['quota'].LaneQuota(parent.QUOTA_ROOT,shared_rpc_reserver=reserve)
    # Tighten only this reader's instance; normal strategy/global ceilings stay fixed.
    quota._LIMITS = dict(quota._LIMITS,helius_rpc=529)
    # Preserve full normal lifecycle headroom even though this probe cannot trade.
    day = utc()[:10]
    plan = quota.plan_actual_attempt('helius_rpc', day)
    if plan['used'] + MAX_CALLS > plan['limit'] or parent._shared_credit_remaining(parent.SHARED_STATE_ROOT, day) < MAX_CALLS + 71:
        raise ValueError('discovery_budget_headroom_unavailable')
    state = Path('/var/lib/neorome-bcp3-chain-source-preflight')
    archive = state / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    archive.mkdir(mode=0o700)
    reader = RpcReader(quota, archive, parent._credential('NEOROME_HELIUS_API_KEY'))
    started = time.monotonic()
    first = reader('getSignaturesForAddress', [DISCOVERY_ADDRESS, {'limit': 20, 'commitment': 'confirmed'}])
    data = json.loads(base64.b64decode(first['rawResponseBase64']))
    if data.get('id') != 1 or 'error' in data or type(data.get('result')) is not list:
        raise ValueError('discovery_list_rpc_invalid')
    rows = data['result']
    summaries = []
    for item in reversed(rows):
        if time.monotonic() - started > 20 or len(reader.records) >= MAX_CALLS:
            break
        if item.get('err') is not None or item.get('confirmationStatus') != 'confirmed' or type(item.get('blockTime')) is not int:
            continue
        age = time.time() - item['blockTime']
        if not 0 <= age <= 30:
            continue
        record = reader('getTransaction', [item['signature'], {'encoding':'json', 'commitment':'confirmed', 'maxSupportedTransactionVersion':1}])
        reply = json.loads(base64.b64decode(record['rawResponseBase64']))
        if reply.get('id') != record['requestId'] or 'error' in reply:
            summaries.append({'status':'rpc_error','code':(reply.get('error') or {}).get('code')})
            continue
        result = reply.get('result')
        if not isinstance(result, dict):
            summaries.append({'status':'null'}); continue
        meta = result.get('meta') or {}
        summaries.append({'slot':result.get('slot'),'blockTime':result.get('blockTime'),
                          'successful':meta.get('err') is None,
                          'createLogHint':any(x in {'Program log: Instruction: Create','Program log: Instruction: CreateV2'} for x in meta.get('logMessages',[])),
                          'replyBytes':len(base64.b64decode(record['rawResponseBase64']))})
        # Capability only: logs are NOT admissible creation identity evidence.
        if summaries[-1]['createLogHint']:
            break
    receipt = {'schema':'neorome-bcp3-chain-source-capability-v1','asOf':utc(),
               'provider':'Helius','discoveryAddress':DISCOVERY_ADDRESS,'calls':len(reader.records),'signatureRows':len(rows),
               'transactions':summaries,'archive':str(archive),'elapsedSeconds':round(time.monotonic()-started,3),
               'admissionProof':False,'paperCycleDispatched':False,'ordersSent':0}
    path=archive/'receipt.json'; path.write_bytes(compact(receipt)); path.chmod(0o600)
    print(json.dumps(receipt))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Never print transport exceptions or authenticated request paths.
        code = getattr(error, 'code', None)
        if code is None and type(error) is ValueError:
            code = str(error)
        if not isinstance(code, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,95}', code):
            code = 'probe_' + type(error).__name__.lower()
        print(json.dumps({'status':'capability_probe_failed','code':code,'admissionProof':False,'ordersSent':0}))
        raise SystemExit(2)
