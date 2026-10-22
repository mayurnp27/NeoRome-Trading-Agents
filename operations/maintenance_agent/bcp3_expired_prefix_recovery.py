"""One-time bounded catch-up of the verified pre-capacity expired source prefix."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path('/opt/neorome')
PACKAGE = ROOT / 'coordination/releases/crypto-rebuild-20261005/p3-capacity-r1'
BOUNDARY = 32768
BOUNDARY_SHA = 'ca165303859310a5df098da80226784772230e048bdcaa84de304e992750bf39'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--journal-root', type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(PACKAGE))
    scheduler = load('expired_prefix_scheduler', PACKAGE / 'coalesced_parent_capacity_r1.py')
    parent = scheduler._load('expired_prefix_parent', ROOT / scheduler.PARENT, scheduler.PARENT_SHA256)
    scheduler.install_production_route(parent, repo_root=ROOT, test_mode=False)
    modules = parent._production_modules()
    from strict_parent_route_capacity_r1 import build_queue_transport
    route = build_queue_transport(modules, ROOT, args.journal_root,
                                  modules['source_transport'], test_mode=False)
    reader = route.reader
    adapter = modules['streaming_source_adapter']
    anchor = reader._event(BOUNDARY)
    assert anchor['sha256'] == BOUNDARY_SHA
    assert anchor['event_type'] == 'frame', 'Boundary must be a complete classified pair'
    assert (datetime.now(timezone.utc) - datetime.fromisoformat(anchor['at'].replace('Z', '+00:00'))).total_seconds() > 30
    initial = reader._read_state()
    deadline = time.monotonic() + 300
    for batch in range(64):
        if time.monotonic() >= deadline:
            raise RuntimeError('Expired-prefix recovery time bound reached')
        with adapter._lock(reader.lock_path):
            state = reader._read_state()
            assert state['offerCount'] == initial['offerCount'], 'Concurrent consumer activity'
            if state['cursorSequence'] >= BOUNDARY:
                print(json.dumps({'status': 'caught_up', 'cursor_before': initial['cursorSequence'],
                    'cursor_after': state['cursorSequence'], 'batches': batch,
                    'offer_count_unchanged': state['offerCount'] == initial['offerCount'],
                    'stale_count_added': state['staleHintsSeen'] - initial['staleHintsSeen']}))
                return 0
            head = reader.journal.head()
            rows, scanned, counts, incomplete = reader._scan(state['cursorSequence'], head)
            if incomplete:
                raise RuntimeError('Incomplete source prefix; no checkpoint advance for this batch')
            now = datetime.now(timezone.utc)
            considered = []
            for row in rows:
                if row['sequence'] > BOUNDARY:
                    break
                original = {key: row.get(key) for key in
                    ('sequence', 'mint', 'signature', 'receivedAt', 'rawSha256', 'reason')}
                age = (now - datetime.fromisoformat(row['receivedAt'].replace('Z', '+00:00'))).total_seconds()
                # Even non-eligible records must be expired, never fresh/future.
                if age <= 30:
                    raise RuntimeError('Refusing to consume a fresh or future record')
                if row['reason'] == 'eligible_fifo_candidate':
                    original['reason'] = 'original_hint_stale'
                considered.append(original)
            # If the scan reached/passed the anchored boundary and did not
            # truncate below it, all old candidate rows have been dispositioned.
            end = considered[-1]['sequence'] if considered else state['cursorSequence']
            if scanned >= BOUNDARY and (len(rows) < adapter.MAX_CANDIDATES or
                                        any(row['sequence'] > BOUNDARY for row in rows)):
                end = BOUNDARY
            if end <= state['cursorSequence'] or end > BOUNDARY:
                raise RuntimeError('No safe bounded prefix progress')
            next_state = dict(state)
            next_state['cursorSequence'] = end
            next_state['readCount'] += 1
            for field, reason in [('staleHintsSeen', 'original_hint_stale'),
                                  ('unsupportedHintsSeen', 'explicit_unsupported_mode'),
                                  ('duplicateHintsSeen', 'duplicate_creation_identity'),
                                  ('invalidHintsSeen', 'source_frame_invalid')]:
                next_state[field] += sum(row['reason'] == reason for row in considered)
            assert next_state['offerCount'] == initial['offerCount']
            stamp = now.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            result = reader._empty(next_state, head, considered, counts, now, stamp)
            assert result['status'] == 'empty'
            print(json.dumps({'batch': batch + 1, 'cursor_after': end,
                              'expired_rows': len(considered)}), flush=True)
    raise RuntimeError('Expired-prefix recovery batch bound reached')


if __name__ == '__main__':
    raise SystemExit(main())
