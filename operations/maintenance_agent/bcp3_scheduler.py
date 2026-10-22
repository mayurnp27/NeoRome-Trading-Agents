"""Pinned BC-P3 parent with explicit budget and rejected-clock diagnostics.

No quotas, lifecycle reserves, source admission, or retry intervals are changed.
"""
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import stat
import sys
import time

BASE = Path('/opt/neorome/coordination/releases/crypto-rebuild-20261005/p3-callback-handoff-r1/coalesced_parent_readfix_dispatch_r1.py')
BASE_SHA256 = '82ebd9781e9c15dc90472102e92ec91a49cd0a995b9aab8b672af26ef2c4306e'


def budget_report(parent, quota, ledger, shared_root, now):
    day = now.astimezone(timezone.utc).date().isoformat()
    result = {'available': False, 'reason': 'budget_check_error', 'utcDay': day}
    try:
        rpc, quote = parent._rpc_headroom_needed(ledger, now)
        for kind, needed in (('helius_rpc', rpc), ('jupiter_quote', quote)):
            result['gate'] = kind
            plan = quota.plan_actual_attempt(kind, day)
            if (type(plan) is not dict or type(plan.get('used')) is not int
                    or type(plan.get('limit')) is not int):
                result['reason'] = 'budget_plan_invalid'
                return result
            result[kind] = {'needed': needed, 'used': plan['used'], 'limit': plan['limit']}
            if plan['used'] + needed > plan['limit']:
                result['reason'] = kind + '_headroom_insufficient'
                return result
        result['gate'] = 'shared_helius'
        remaining = parent._shared_credit_remaining(shared_root, day)
        result['shared_helius'] = {'needed': rpc, 'remaining': remaining}
        result['available'] = remaining >= rpc
        result['reason'] = 'budget_ready' if result['available'] else 'shared_helius_headroom_insufficient'
    except Exception as error:
        code = getattr(error, 'code', '')
        result['code'] = code if isinstance(code, str) and re.fullmatch(r'[a-z_]{1,80}', code) else 'budget_check_failed'
        result['reason'] = 'budget_check_busy' if result['code'].endswith('_busy') else 'budget_check_error'
    return result


def install_diagnostics(base, emit=None, clock=time.monotonic):
    emit = emit or (lambda value: print(json.dumps(value, sort_keys=True), flush=True))
    previous = [None, 0.0]
    def check(parent, modules, quota, ledger, shared_root, now):
        report = budget_report(parent, quota, ledger, shared_root, now)
        key = (report['reason'], report.get('gate'), report.get('code'))
        moment = clock()
        if key != previous[0] or moment - previous[1] >= 60:
            emit({'status': 'budget_check', **report})
            previous[:] = [key, moment]
        return report['available']
    base.budget_headroom = check
    if hasattr(base, '_production_dispatcher'):
        original_dispatcher = base._production_dispatcher
        def dispatcher():
            parent, modules, journal, dispatch = original_dispatcher()
            instrument_validators(modules, emit)
            original_modules = parent._production_modules
            def production_modules():
                result = original_modules()
                instrument_validators(result, emit)
                return result
            parent._production_modules = production_modules
            return parent, modules, journal, dispatch
        base._production_dispatcher = dispatcher


def instrument_validators(modules, emit):
    """Observe rejected source clocks only; preserve every validation result."""
    def timestamp(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str) and len(value) <= 40:
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00')).isoformat()
            except ValueError:
                pass
        return None
    for name in ('callbacks', 'r8_callbacks'):
        module = modules.get(name)
        original = getattr(module, '_validate_queue_selected', None)
        if original is None or getattr(original, '_clock_diagnostic', False):
            continue
        def validate(captured, now, _original=original, _stage=name):
            try:
                return _original(captured, now)
            except Exception as error:
                try:
                    row = {'status': 'source_clock_rejection', 'stage': _stage,
                           'validationAt': timestamp(now),
                           'observedAt': datetime.now(timezone.utc).isoformat()}
                    code = getattr(error, 'code', '')
                    row['code'] = code if isinstance(code, str) and re.fullmatch(r'[a-z_]{1,80}', code) else 'validation_failed'
                    for section, field, label in (('hint', 'receivedAt', 'hintAt'),
                            ('subscriptionAck', 'receivedAt', 'ackAt'), ('capture', 'decisionAt', 'decisionAt')):
                        obj = captured.get(section) if isinstance(captured, dict) else None
                        row[label] = timestamp(obj.get(field)) if isinstance(obj, dict) else None
                    emit(row)
                except Exception:
                    pass  # Diagnostics must never replace the original exception.
                raise
        validate._clock_diagnostic = True
        module._validate_queue_selected = validate


def load_base():
    for path in (BASE, *BASE.parents):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise RuntimeError('scheduler_source_identity_invalid')
        if path == BASE and (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o222 or info.st_nlink != 1):
            raise RuntimeError('scheduler_source_identity_invalid')
    if hashlib.sha256(BASE.read_bytes()).hexdigest() != BASE_SHA256:
        raise RuntimeError('scheduler_source_hash_mismatch')
    spec = importlib.util.spec_from_file_location('bcp3_budget_diagnostic_base', BASE)
    base = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = base
    spec.loader.exec_module(base)
    return base


def main():
    base = load_base()
    install_diagnostics(base)
    return base.main()


if __name__ == '__main__':
    raise SystemExit(main())
