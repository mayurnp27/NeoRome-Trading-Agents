"""Bounded root-owned repair context, reference evidence only; no repair authority."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

UTC = timezone.utc
MAX_BYTES = 512 * 1024
MAX_GROUPS = 64
MAX_MEMBERS = 256
MAX_BRIEF = 4000
TRUSTED_UID = 0
CHILD = re.compile(r'unit:neorome-bcp3-child-freshness-r2@(\d+)-(\d+)-(\d+)\.service\Z')
PARENT = 'neorome-bcp3-streaming-parent-resilience-r2.service'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def fingerprint(key, reason, package, episode=None):
    return hashlib.sha256(json.dumps([key, reason, episode or package], ensure_ascii=True,
                                    separators=(',', ':')).encode()).hexdigest()


def stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('context_time_invalid')
    return parsed.astimezone(UTC)


def _safe_token(value, default='unknown'):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_.:@+ -]{1,160}', value) else default


def groups(report):
    """Batch a fixed snapshot of historical siblings, never infer shared cause.

    New cause, reopened/recovered episodes, changed outcomes or deployed versions
    change dispatch identity. New sibling membership alone is data, not a reason
    for another diagnosis. Missing or ambiguous parent cause stays individual.
    Raw observer incidents and events are not modified or cleared.
    """
    package = report['agent_package_sha256']
    units = report.get('units', {})
    active_parent = str(units.get(PARENT, {}).get('MainPID', '0'))
    buckets = {}
    for issue in report['incidents'][:MAX_MEMBERS]:
        key, reason, episode = issue['key'], issue['reason'], issue.get('episode_id')
        raw_fp = fingerprint(key, reason, package, episode)
        match = CHILD.fullmatch(key)
        historical = bool(match and active_parent not in ('', '0') and match[2] != active_parent)
        marker = issue.get('cause_code')
        known_marker = isinstance(marker, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,63}', marker)
        parent_code = issue.get('parent_failure_code')
        correlated = (match and known_marker and isinstance(parent_code, str)
                      and re.fullmatch(r'[a-z][a-z0-9_]{0,95}', parent_code)
                      and issue.get('parent_pid') == match[2]
                      and issue.get('parent_failure_status') == 'cycle_error'
                      and isinstance(issue.get('parent_failure_reason'), str)
                      and re.fullmatch(r'[a-z][a-z0-9_]{0,95}', issue['parent_failure_reason']))
        # EOF is only a terminal symptom. Unknown/different upstream causes
        # must never collapse merely because the children all closed stdin.
        bucket = ('child-parent:' + match[2] + ':cause:' + parent_code + ':' + issue['parent_failure_status'] + ':'
                  + issue['parent_failure_reason'] + ':marker:' + marker) if correlated else raw_fp
        unit = units.get(key.removeprefix('unit:'), {})
        observation = {name: _safe_token(unit.get(name)) for name in
                       ('ActiveState', 'SubState', 'Result', 'ExecMainCode', 'ExecMainStatus', 'ExecMainExitTimestamp')}
        observation['reason'] = reason
        if known_marker:
            observation['journal_cause_code'] = marker
            observation['journal_message_sha256'] = issue.get('cause_message_sha256', '')
            observation['journal_observed_at'] = issue.get('cause_observed_at', '')
        if correlated:
            for field in ('parent_failure_code', 'parent_failure_status', 'parent_failure_reason',
                          'parent_failure_observed_at', 'parent_message_sha256', 'parent_pid'):
                observation[field] = issue.get(field, '')
        # The observer supplies only sanitized hashes/tokens, never source bodies.
        if isinstance(issue.get('evidence_version'), str) and re.fullmatch(r'[a-f0-9]{64}', issue['evidence_version']):
            observation['source_evidence_version'] = issue['evidence_version']
        buckets.setdefault(bucket, {'historical_batch': historical, 'members': []})['members'].append({
            'fingerprint': raw_fp, 'key': key, 'reason': reason, 'episode_id': episode,
            'observation': observation})
    output = {}
    for bucket, value in sorted(buckets.items()):
        if len(output) >= MAX_GROUPS:
            break
        value['members'].sort(key=lambda member: member['fingerprint'])
        basis = {'observer_package_sha256': package, 'runtime_version': report.get('runtime_version', 'unknown'),
                 'bucket': bucket, 'members': value['members']}
        value['evidence_sha256'] = sha(basis)
        is_causal = all('parent_failure_code' in member['observation'] for member in value['members'])
        outcomes = sorted({tuple(member['observation'][key] for key in ('ActiveState','SubState','Result','ExecMainCode','ExecMainStatus')) for member in value['members']})
        signature = {'schema':1,'package':package,'runtime_version':basis['runtime_version'],'bucket':bucket,'outcomes':outcomes}
        value['signature_sha256'] = sha(signature) if is_causal else value['evidence_sha256']
        value['dispatch_fingerprint'] = value['signature_sha256']
        value['runtime_version'] = basis['runtime_version']
        value['current_deployed_sources'] = report.get('runtime_sources', {})
        value['historical_deployment_version_verified'] = False
        value['causal_batch'] = is_causal
        value['observed_at'] = report['as_of']
        value['batch_scope'] = ('Historical sibling evidence; correlated parent markers are not a full callback trace. '
                                'New siblings update evidence without repeating the same causal diagnosis; cause/episode/outcome/version/recovery changes require fresh review.' if value['historical_batch']
                                else 'Current/unclassified evidence. EOF alone never establishes shared cause; only matching parent PID/code/status/reason and current version may share one diagnosis.')
        output[value['dispatch_fingerprint']] = value
    return output


def atomic(path, value, mode=0o600):
    raw = encoded(value)
    if len(raw) > MAX_BYTES:
        raise ValueError('context_size_limit')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or path.parent.is_symlink():
        raise ValueError('context_symlink')
    temporary = path.with_name(path.name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    os.chmod(temporary, mode)
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
    return hashlib.sha256(raw).hexdigest()


def load(path, report, now):
    """Only consume an exact hash-bound current root publication; otherwise fail open to original incident dispatch."""
    metadata = path.lstat()
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != TRUSTED_UID
            or metadata.st_mode & 0o022 or metadata.st_size > MAX_BYTES or path.parent.is_symlink()):
        raise ValueError('context_untrusted_file')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream: raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != report.get('repair_context_sha256'):
        raise ValueError('context_hash_invalid')
    value = json.loads(raw)
    if (value.get('schema_version') != 1 or value.get('as_of') != report['as_of']
            or value.get('observer_package_sha256') != report['agent_package_sha256']
            or not -5 <= (now - stamp(value['as_of'])).total_seconds() <= 180):
        raise ValueError('context_binding_invalid')
    entries = value.get('groups')
    if not isinstance(entries, dict) or len(entries) > MAX_GROUPS:
        raise ValueError('context_groups_invalid')
    for group_id, entry in entries.items():
        if not re.fullmatch(r'[a-f0-9]{64}', group_id) or entry.get('dispatch_fingerprint') != group_id:
            raise ValueError('context_group_identity_invalid')
        members = entry.get('members')
        if not isinstance(members, list) or not 1 <= len(members) <= MAX_MEMBERS:
            raise ValueError('context_members_invalid')
        memory = entry.get('repair_memory', {})
        if (not isinstance(memory, dict) or not isinstance(memory.get('brief', ''), str)
                or len(memory.get('brief', '')) > MAX_BRIEF):
            raise ValueError('context_brief_invalid')
    return value


def attach(incidents, document, claims=None):
    bindings = {}
    for group in document['groups'].values():
        for member in group['members']:
            raw_fp = member.get('fingerprint')
            if raw_fp in bindings:
                raise ValueError('context_duplicate_binding')
            bindings[raw_fp] = (member, group)
    output, seen = [], set()
    for incident in incidents:
        binding = bindings.get(incident['fingerprint'])
        if binding is None:
            output.append(incident); continue
        member, group = binding
        if any(member.get(key) != incident.get(key) for key in ('key', 'reason', 'episode_id')):
            raise ValueError('context_incident_mismatch')
        dispatch = group['dispatch_fingerprint']
        legacy = (claims or {}).get(incident['fingerprint'])
        if (legacy is not None and not legacy.get('context_evidence_sha256')
                and len(group['members']) == 1 and not group.get('causal_batch')):
            # Installing a context publisher is not itself a new failure. Do
            # not spend again on old, still-unexplained retained unit history.
            # A new episode/member or newly correlated cause is a new identity.
            dispatch = incident['fingerprint']
        if dispatch in seen:
            continue
        seen.add(dispatch)
        projected = dict(incident, fingerprint=dispatch, repair_context=group)
        output.append(projected)
    return output
