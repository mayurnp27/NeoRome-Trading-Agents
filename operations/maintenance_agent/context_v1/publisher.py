"""Root observer's deterministic context publisher; read-only canonical lookup.

One bounded memory lookup per unchanged group/content revision. Refresh a small
one-way private projection at most every five minutes; no model/provider calls.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import re
import subprocess
import os
import sqlite3
import stat
import time
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from operations.maintenance_agent.context_v1 import context
from neorome import repair_memory

UTC = timezone.utc


def _read_snapshot(path):
    """Native read first; reuse verified stable private DB+WAL snapshot pattern."""
    try:
        return repair_memory.read_database(path)
    except sqlite3.OperationalError as exc:
        if getattr(exc, 'sqlite_errorcode', None) != sqlite3.SQLITE_CANTOPEN:
            raise
    sources = [path, Path(str(path) + '-wal'), Path(str(path) + '-journal')]
    def identity(info):
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    def signatures():
        result = {}
        for item in sources:
            try: info = item.lstat()
            except FileNotFoundError:
                if item == path: raise
                continue
            if not stat.S_ISREG(info.st_mode): raise ValueError('memory_sqlite_identity_invalid')
            result[item] = identity(info)
        if sum(info[2] for info in result.values()) > 128 * 1024 * 1024:
            raise ValueError('memory_sqlite_snapshot_oversize')
        return result
    started = time.monotonic()
    for _ in range(3):
        before = signatures()
        with tempfile.TemporaryDirectory(prefix='neorome-maintenance-memory-') as temporary:
            for source, expected in before.items():
                fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as stream:
                    if identity(os.fstat(stream.fileno())) != expected: raise ValueError('memory_sqlite_changed_during_open')
                    with (Path(temporary) / source.name).open('xb') as output:
                        remaining = expected[2]
                        while remaining:
                            block = stream.read(min(256 * 1024, remaining))
                            if not block: raise ValueError('memory_sqlite_short_copy')
                            output.write(block); remaining -= len(block)
                            if time.monotonic() - started > 8: raise ValueError('memory_sqlite_snapshot_timeout')
                        if stream.read(1): raise ValueError('memory_sqlite_grew_during_copy')
            if before != signatures(): continue
            if time.monotonic() - started > 8: raise ValueError('memory_sqlite_snapshot_timeout')
            # Only the private copy may initialize shared memory/recover its WAL.
            conn = sqlite3.connect(Path(temporary) / path.name, timeout=2)
            try:
                conn.execute('PRAGMA query_only=ON')
                conn.execute('PRAGMA cache_size=-4096')
                conn.execute('PRAGMA mmap_size=0')
                conn.execute('PRAGMA temp_store=FILE')
                conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 16384)
                conn.set_progress_handler(lambda: time.monotonic() - started > 10, 1000)
                conn.row_factory = sqlite3.Row
                rows, total = [], 0
                for row in conn.execute(repair_memory.QUERY):
                    projected = {key: repair_memory.clean(row[key]) for key in repair_memory.FIELDS}
                    total += len(json.dumps(projected))
                    if total > 512 * 1024: raise ValueError('memory_sqlite_projection_oversize')
                    rows.append(projected)
            finally: conn.close()
            return {'schema': 1, 'exported_at': repair_memory.now(), 'incidents': rows}
    raise ValueError('memory_sqlite_changed_during_every_copy')


def _query(entry):
    tokens={member['reason'] for member in entry['members']}
    tokens.add(entry['members'][0]['key'].split('@')[0])
    for member in entry['members']:
        tokens.update(member['observation'].get(key,'') for key in ('journal_cause_code','parent_failure_code','parent_failure_reason'))
    return ' '.join(sorted(token for token in tokens if token))


def observations(report, inbox, run=subprocess.run, *, now=None):
    """Retain only terminal child error tokens/hashes, never arbitrary journal text."""
    path = inbox / 'repair-observation-cache.json'
    try: cache = _read(path)
    except (OSError, ValueError, KeyError, TypeError): cache = {}
    requested = {}
    for issue in report['incidents']:
        if not context.CHILD.fullmatch(issue['key']): continue
        name = issue['key'][5:]
        unit = report.get('units', {}).get(name, {})
        signature = context.sha({'name': name, 'result': unit.get('Result'),
                                 'exit': unit.get('ExecMainStatus'), 'at': unit.get('ExecMainExitTimestamp')})
        requested[name] = signature
    now=now or datetime.now(UTC)
    def due(signature):
        entry=cache.get(signature,{})
        if entry.get('parent_failure_code'): return False
        try:
            age = (now-context.stamp(entry['_checked_at'])).total_seconds()
            return age >= 60 or age < -5
        except (KeyError, TypeError, ValueError): return True
    missing = [name for name, signature in requested.items() if due(signature)][:256]
    if missing:
        argv = ['journalctl', '--no-pager', '-o', 'json', '-n', '16', '--since=-8h',
                '--grep=^p3-quote-child:[a-z][a-z0-9_]{0,63}$|"status"[[:space:]]*:[[:space:]]*"cycle_error"',
                '--output-fields=MESSAGE,_PID,_SYSTEMD_UNIT,__REALTIME_TIMESTAMP', '_SYSTEMD_UNIT=' + context.PARENT]
        for name in missing: argv += ['_SYSTEMD_UNIT=' + name]
        found, parent_events = {}, []
        try:
            result = run(argv, capture_output=True, text=True, timeout=8, check=False)
            if result.returncode == 0 and len(result.stdout) <= 2 * 1024 * 1024:
                for line in result.stdout.splitlines():
                    row = json.loads(line); name = row.get('_SYSTEMD_UNIT')
                    message = row.get('MESSAGE', '')
                    if not isinstance(message, str) or len(message) > 4096: continue
                    raw_time = row.get('__REALTIME_TIMESTAMP', '')
                    if not isinstance(raw_time, str) or not raw_time.isdigit(): continue
                    if name == context.PARENT:
                        try:
                            data=json.loads(message)
                            if (data.get('status')=='cycle_error' and isinstance(data.get('code'), str)
                                    and re.fullmatch(r'[a-z][a-z0-9_]{0,95}', data['code'])
                                    and isinstance(data.get('reason'), str)
                                    and re.fullmatch(r'[a-z][a-z0-9_]{0,95}', data['reason'])
                                    and str(row.get('_PID','')).isdigit()):
                                parent_events.append({'parent_pid':str(row['_PID']), 'parent_failure_code':data['code'],
                                    'parent_failure_status':data['status'],'parent_failure_reason':data['reason'],
                                    'parent_failure_observed_at':datetime.fromtimestamp(int(raw_time)/1000000,UTC).isoformat(),
                                    'parent_message_sha256':hashlib.sha256(message.encode()).hexdigest(), '_micros':int(raw_time)})
                        except (ValueError, TypeError, AttributeError): pass
                        continue
                    if name not in missing: continue
                    match = re.fullmatch(r'p3-quote-child:([a-z][a-z0-9_]{0,63})', message.strip())
                    if match and isinstance(raw_time, str) and raw_time.isdigit():
                        found[name] = {'cause_code': match[1], 'cause_message_sha256': hashlib.sha256(message.encode()).hexdigest(),
                                       'cause_observed_at': datetime.fromtimestamp(int(raw_time) / 1000000, UTC).isoformat()}
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError): pass
        for name, value in found.items():
            parent_pid=context.CHILD.fullmatch('unit:'+name)[2]
            at=int(context.stamp(value['cause_observed_at']).timestamp()*1000000)
            matches=[event for event in parent_events if event['parent_pid']==parent_pid and abs(event['_micros']-at)<=2000000]
            identities={(event['parent_failure_code'],event['parent_failure_reason']) for event in matches}
            if len(identities)==1:
                event=min(matches,key=lambda event:abs(event['_micros']-at))
                value.update({key:val for key,val in event.items() if key!='_micros'})
        for name in missing: cache[requested[name]] = dict(found.get(name, {}), _checked_at=now.isoformat())
    cache = {signature: cache.get(signature, {}) for signature in requested.values()}
    context.atomic(path, cache)
    result = dict(report, incidents=[dict(issue) for issue in report['incidents']])
    for issue in result['incidents']:
        signature = requested.get(issue['key'].removeprefix('unit:'))
        if signature: issue.update({key:value for key,value in cache.get(signature, {}).items() if not key.startswith('_')})
    # Pin actual fixed first-party source content, not timestamps/health claims.
    paths = ['operations/maintenance_agent/ai_v1/worker.py',
             'coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/ACTIVATION.json',
             'coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/chain_scheduler.py',
             'neorome/options_internal_paper.py','operations/maintenance_agent/bcp3_scheduler.py',
             'coordination/releases/crypto-rebuild-20261005/p3-callback-handoff-r1/coalesced_parent_readfix_dispatch_r1.py',
             'coordination/releases/crypto-rebuild-20261005/p3-callback-handoff-r1/production_route_readfix_r1.py',
             'coordination/releases/crypto-rebuild-20261005/p3-callback-handoff-r1/production_route_readfix_r4.py',
             'coordination/releases/crypto-rebuild-20261005/p3-callback-handoff-r1/strict_parent_route_readfix_r1.py',
             'coordination/releases/crypto-rebuild-20261005/p3-freshness-report-identity-r3/production_parent_r3.py',
             'coordination/releases/crypto-rebuild-20261005/p3-queue-resilience-r1/freshness_diagnostic_route_r4.py',
             'coordination/releases/crypto-rebuild-20261005/p3-streaming-freshness-production-route-r2/launcher/ready_connector.py']
    absolute = [Path('/opt/neorome-bcp3-child-freshness-r2/entry.cjs'),
                Path('/opt/neorome-bcp3-child-freshness-r2/src/stdio_bridge.cjs'),
                Path('/opt/neorome-p3-artifacts/20261009-child-freshness-r2/artifact/src/p3_finite_cycle.cjs')]
    try:
        activation=_read(ROOT/'coordination/releases/crypto-rebuild-20261005/p3-chain-route-r1/ACTIVATION.json')
        for relative in list(activation.get('codePins',{}))[:32]:
            if re.fullmatch(r'coordination/releases/crypto-rebuild-20261005/[A-Za-z0-9_./-]+\.(py|cjs|json)',relative) and '..' not in Path(relative).parts:
                paths.append(relative)
    except (OSError,ValueError,KeyError,TypeError): pass
    versions = {}
    for source in [ROOT/relative for relative in sorted(set(paths))[:48]]+absolute:
        relative=str(source.relative_to(ROOT)) if source.is_relative_to(ROOT) else str(source)
        try:
            if not source.is_symlink() and source.stat().st_size <= 256 * 1024:
                versions[relative] = hashlib.sha256(source.read_bytes()).hexdigest()
        except OSError: pass
    result['runtime_version'] = context.sha(versions) if versions else 'unknown'
    result['runtime_sources'] = versions
    return result


def _read(path, limit=context.MAX_BYTES):
    if path.is_symlink() or path.stat().st_size > limit:
        raise ValueError('publisher_input_invalid')
    return json.loads(path.read_bytes())


def _memory(vault, now):
    manifest = _read(vault / 'manifest.json')
    if not -5 <= (now - context.stamp(manifest['exported_at'])).total_seconds() <= 86400:
        raise ValueError('memory_stale')
    if manifest.get('schema') != 1 or len(manifest.get('documents', [])) > 256:
        raise ValueError('memory_manifest_invalid')
    total = 0
    for item in manifest['documents']:
        relative = item['path']
        if not re.fullmatch(r'(Cloud/inc-[a-f0-9]{24}|Runbooks/[A-Za-z0-9_.-]+)\.md', relative):
            raise ValueError('memory_path_invalid')
        note = vault / relative
        if note.is_symlink() or note.parent.is_symlink() or note.stat().st_size > 65536:
            raise ValueError('memory_note_invalid')
        raw = note.read_bytes(); total += len(raw)
        if total > 2 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != item['sha256']:
            raise ValueError('memory_hash_invalid')
        if item['kind'] == 'runbook':
            if not re.fullmatch(r'docs/repair-runbooks/[A-Za-z0-9_.-]+\.md', item['source']):
                raise ValueError('memory_source_invalid')
            source = repair_memory.ROOT / item['source']
            if source.is_symlink() or source.stat().st_size > 65536 or hashlib.sha256(source.read_bytes()).hexdigest() != item['source_sha256']:
                raise ValueError('memory_source_changed')
    # Semantic revision excludes export time so an unchanged refresh is not a new lookup.
    semantic=[]
    for item in manifest['documents']:
        raw=(vault/item['path']).read_text()
        # Export-header churn is not new research, but any note body change is.
        semantic.append({**{key:item.get(key) for key in ('path','kind','source_id','source_sha256','updated_at')},
                         'body_sha256':hashlib.sha256(re.sub(r'^exported_at:.*\n','',raw,flags=re.MULTILINE).encode()).hexdigest()})
    revision = context.sha(semantic)
    return manifest, revision


def publish(report, inbox, *, now=None, refresh=True):
    now = now or datetime.now(UTC)
    vault = inbox / 'repair-memory'
    memory_status, manifest, revision = 'unavailable', {}, 'unavailable'
    try:
        try: manifest, revision = _memory(vault, now)
        except (OSError, ValueError, KeyError, TypeError): manifest = {}
        old_stamp = context.stamp(manifest['exported_at']) if manifest else None
        if refresh and (old_stamp is None or (now - old_stamp).total_seconds() >= 300):
            snapshot = _read_snapshot(Path(repair_memory.DATABASE))
            repair_memory.export(snapshot, vault=vault, source='cloud:read-only-control-store')
            manifest, revision = _memory(vault, now)
        if manifest: memory_status = 'available'
    except (OSError, ValueError, KeyError, TypeError, __import__('sqlite3').Error):
        # A failed refresh never grants authority or hides a missing/stale memory check.
        if not manifest: memory_status = 'unavailable'
    cache_path = inbox / 'repair-context-cache.json'
    try: cache = _read(cache_path)
    except (OSError, ValueError, KeyError, TypeError): cache = {'entries': {}, 'lookups': 0}
    cache.setdefault('entries', {}); cache.setdefault('lookups', 0)
    raw_groups = context.groups(observations(report, inbox, now=now))
    prior_epochs=cache.get('epochs',{});epochs={};output={}
    for entry in raw_groups.values():
        signature=entry['signature_sha256']
        members={member['key']:member['episode_id'] for member in entry['members']}
        previous=prior_epochs.get(signature)
        reopened=previous and any(key in previous.get('members',{}) and previous['members'][key]!=episode for key,episode in members.items())
        if previous and not reopened: epoch=previous['epoch']
        else:
            cache['epoch_counter']=cache.get('epoch_counter',0)+1
            epoch=context.sha({'signature':signature,'initial_members':members,'generation':cache['epoch_counter']})
        entry['dispatch_fingerprint']=context.sha({'signature':signature,'epoch':epoch})
        seen_members=dict(previous.get('members',{})) if previous and not reopened else {}
        seen_members.update(members)
        epochs[signature]={'epoch':epoch,'members':seen_members}
        output[entry['dispatch_fingerprint']]=entry
    cache['epochs']=epochs
    for entry in output.values():
        # Stable causal signatures reuse memory even as additional siblings arrive.
        query = _query(entry)
        cache_key = context.sha({'query': query, 'dispatch': entry['dispatch_fingerprint'], 'revision': revision})
        memory = cache['entries'].get(cache_key)
        if memory is None:
            memory = {'status': memory_status, 'brief': 'No verified repair memory available. Diagnose current evidence; do not infer a fix.',
                      'selected': [], 'exported_at': manifest.get('exported_at')}
            if memory_status == 'available':
                try:
                    found = repair_memory.retrieve(query, vault=vault, max_chars=4000, limit=2)
                    selected = [item for item in manifest['documents'] if item['path'] in found['selected']]
                    memory.update(status='available' if selected else 'no_match', brief=repair_memory.clean(found['brief'])[:4000],
                                  selected=[{key: item.get(key) for key in ('path', 'source', 'source_id', 'sha256', 'source_sha256', 'updated_at')} for item in selected])
                    cache['lookups'] += 1
                except (OSError, ValueError, KeyError, TypeError): memory['status'] = 'invalid'
            cache['entries'][cache_key] = memory
        memory = dict(memory, exported_at=manifest.get('exported_at'))
        entry['repair_memory'] = memory
        entry['invalidation'] = ['new cause/episode/reason/outcome or verified recovery/reopen', 'deployed version change', 'source provenance mismatch',
                                 'memory stale/missing/corrupt; do not treat historical notes as current health']
        entry['reference_only'] = True
    # Retain only current group lookups; no unbounded episode history in the root cache.
    current_keys = {context.sha({'query': _query(e),
                                'dispatch': e['dispatch_fingerprint'], 'revision': revision}) for e in output.values()}
    cache['entries'] = {key: value for key, value in cache['entries'].items() if key in current_keys}
    context.atomic(cache_path, cache)
    document = {'schema_version': 1, 'as_of': report['as_of'], 'observer_package_sha256': report['agent_package_sha256'],
                'groups': output, 'memory_status': memory_status, 'memory_lookup_count': cache['lookups'],
                'raw_incident_count': len(report['incidents']), 'diagnosis_group_count': len(output),
                'authority': 'reference_only_no_deployment_or_trading_authority'}
    return context.atomic(inbox / 'repair-context.json', document, mode=0o644)
