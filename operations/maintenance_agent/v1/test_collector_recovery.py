from datetime import datetime, timezone
from copy import deepcopy
from unittest.mock import patch
import unittest
import agent


class CollectorRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.health = {'status': 'source_failure', 'reason': 'bounded_run_or_connection_limit',
            'asOf': datetime.now(timezone.utc).isoformat(),
            'journalHead': {'capacity_stop': False, 'unresolved_tail_gap': None, 'sequence': 33146, 'sha256': 'a'*64},
            'appendDiagnostics': {'failureListTruncated': False, 'failureCount': 1, 'failures': [{'code': 'writer_busy'}]}}
        self.manifest = {'schema': 'neorome-p3-segmented-journal-r1', 'pending_tail_gap': None,
                         'head_sequence': 33146, 'head_sha256': 'a'*64}

    def allowed(self, health=None, manifest=None):
        with patch.object(agent, '_read_json', side_effect=[health or self.health, manifest or self.manifest]):
            return agent._collector_retry_evidence()

    def test_known_lock_timeout_only(self):
        self.assertTrue(self.allowed())
        for code in ('segment_invalid', 'record_size_invalid', 'capacity_stop'):
            health = deepcopy(self.health)
            health['appendDiagnostics']['failures'][0]['code'] = code
            self.assertFalse(self.allowed(health))

    def test_integrity_capacity_stale_or_changed_head_fail_closed(self):
        for key, value in [('capacity_stop', True), ('unresolved_tail_gap', {}), ('sequence', 33145)]:
            health = deepcopy(self.health)
            health['journalHead'][key] = value
            self.assertFalse(self.allowed(health))
        health = dict(self.health, asOf='2026-01-01T00:00:00Z')
        self.assertFalse(self.allowed(health))
        manifest = dict(self.manifest, head_sha256='b'*64)
        self.assertFalse(self.allowed(manifest=manifest))

    def test_no_restart_of_active_or_intentionally_stopped_service(self):
        guard = {'source_path': agent.COLLECTOR_SOURCE, 'unit_sha256': 'a'*64, 'source_sha256': 'a'*64}
        item = {'LoadState': 'loaded', 'Type': 'exec', 'ActiveState': 'failed', 'MainPID': '0', 'Job': '', 'FragmentPath': '/unit'}
        with patch.object(agent, '_hash_file', return_value='a'*64), patch.object(agent, '_dropin_hashes_match', return_value=True), patch.object(agent, '_collector_retry_evidence', return_value=True):
            self.assertTrue(agent._repair_target(agent.COLLECTOR_REPAIR, item, guard, {}))
            for active in ('active', 'inactive', 'activating'):
                self.assertFalse(agent._repair_target(agent.COLLECTOR_REPAIR, dict(item, ActiveState=active), guard, {}))
            self.assertFalse(agent._repair_target(agent.COLLECTOR_REPAIR, dict(item, MainPID='9'), guard, {}))

    def test_original_retry_bounds_preserved(self):
        now = datetime.now(timezone.utc)
        row = {'day': now.date().isoformat(), 'count': 3, 'last_at': now.isoformat()}
        self.assertFalse(agent._attempt_available({'attempts': {agent.COLLECTOR_REPAIR: row}}, agent.COLLECTOR_REPAIR, now))

    def test_upstream_failure_requires_fresh_pinned_handshake_and_matching_head(self):
        health = deepcopy(self.health)
        health['sessionCount'] = 4
        health['appendDiagnostics'].update(failureCount=0, failures=[])
        probe = {'schema': 'neorome-bcp3-upstream-probe-v1',
                 'asOf': datetime.now(timezone.utc).isoformat(), 'ready': True,
                 'observedHttpStatus': 101, 'failureClass': 'upstream_upgrade_rejected',
                 'pendingTailGap': None, 'journalHead': {'sequence': 33146, 'sha256': 'a'*64}}
        guard = {'probe_source_path': '/opt/neorome/probe.py', 'probe_source_sha256': 'c'*64,
                 'probe_unit_sha256': 'c'*64, 'probe_dropin_sha256': {}}
        units = {agent.COLLECTOR_PROBE_UNIT: {'LoadState': 'loaded', 'FragmentPath': '/probe.service'}}
        def allowed(value, *, pins=True):
            with patch.object(agent, '_read_json', side_effect=[health, self.manifest, value]), \
                 patch.object(agent, '_hash_file', return_value='c'*64 if pins else 'd'*64), \
                 patch.object(agent, '_dropin_hashes_match', return_value=True):
                return agent._collector_retry_evidence(guard, units)
        self.assertTrue(allowed(probe))
        self.assertFalse(allowed(probe, pins=False))
        for key, value in [('ready', False), ('observedHttpStatus', 502),
                           ('failureClass', None), ('pendingTailGap', {}),
                           ('schema', 'wrong'), ('asOf', '2026-01-01T00:00:00Z'),
                           ('journalHead', {'sequence': 33147, 'sha256': 'a'*64})]:
            with self.subTest(key=key):
                self.assertFalse(allowed(dict(probe, **{key: value})))

    def test_unknown_source_failure_does_not_use_lock_timeout_recipe(self):
        health = deepcopy(self.health)
        health['appendDiagnostics'].update(failureCount=0, failures=[])
        self.assertFalse(self.allowed(health))

    def test_flat_parent_requires_active_collector_and_fresh_gap_free_head(self):
        ledger = {'position': None, 'activeCandidate': None}
        manifest = dict(self.manifest, head_at=datetime.now(timezone.utc).isoformat())
        units = {agent.COLLECTOR_REPAIR: {'ActiveState': 'active'}}
        def allowed(value=manifest, states=units):
            with patch.object(agent, '_read_json', side_effect=[ledger, value]):
                return agent._parent_retry_evidence(states)
        self.assertTrue(allowed())
        self.assertFalse(allowed(states={}))
        for key, value in [('pending_tail_gap', {}), ('head_at', '2026-01-01T00:00:00Z'),
                           ('head_at', '2099-01-01T00:00:00Z'), ('head_sha256', 'invalid')]:
            self.assertFalse(allowed(dict(manifest, **{key: value})))

    def test_parent_lifecycle_recovery_does_not_wait_for_collector(self):
        for ledger in ({'position': {'id': 'paper'}, 'activeCandidate': None},
                       {'position': None, 'activeCandidate': {'id': 'candidate'}}):
            with patch.object(agent, '_read_json', return_value=ledger):
                self.assertTrue(agent._parent_retry_evidence({}))
        with patch.object(agent, '_read_json', return_value={}):
            self.assertFalse(agent._parent_retry_evidence({}))

    def test_parent_recovery_pins_source_startup_gate_and_all_dropins(self):
        guard = {'source_path': agent.PARENT_SOURCE, 'source_sha256': 'a'*64,
                 'unit_sha256': 'a'*64, 'startup_source_sha256': 'a'*64}
        item = {'LoadState':'loaded','Type':'exec','ActiveState':'failed','MainPID':'0','Job':'','FragmentPath':'/unit'}
        with patch.object(agent, '_hash_file', return_value='a'*64), \
             patch.object(agent, '_dropin_hashes_match', return_value=True), \
             patch.object(agent, '_parent_retry_evidence', return_value=True):
            self.assertTrue(agent._repair_target(agent.PARENT_REPAIR,item,guard,{}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR,dict(item,ActiveState='active'),guard,{}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR,dict(item,MainPID='7'),guard,{}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR,item,dict(guard,startup_source_sha256='b'*64),{}))
        with patch.object(agent, '_hash_file', return_value='a'*64), \
             patch.object(agent, '_dropin_hashes_match', return_value=False):
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR,item,guard,{}))

    def test_chain_parent_start_requires_pinned_committed_source_transition(self):
        ledger = {'position': None, 'activeCandidate': None, 'policySha256': 'b'*64}
        guard = {'startup_source_path': agent.PARENT_CHAIN_STARTUP_SOURCE,
                 'chain_policy_sha256': 'b'*64, 'chain_activation_sha256': 'a'*64}
        activation = {'schema': 'neorome-bcp3-chain-source-activation-v1',
                      'state': 'committed', 'newPolicySha256': 'b'*64}
        def allowed(value=activation, digest='a'*64, state=ledger):
            with patch.object(agent, '_hash_file', return_value=digest), \
                 patch.object(agent, '_read_json', side_effect=[state, value]):
                return agent._parent_retry_evidence({}, guard)
        self.assertTrue(allowed())  # PumpPortal need not exist for this source.
        self.assertFalse(allowed(digest='c'*64))
        self.assertFalse(allowed(dict(activation, state='prepared')))
        self.assertFalse(allowed(dict(activation, newPolicySha256='c'*64)))
        self.assertFalse(allowed(state=dict(ledger, policySha256='c'*64)))

    def test_source_and_startup_pairs_cannot_be_mixed(self):
        self.assertTrue(agent._parent_source_pair_valid({'source_path': agent.PARENT_SOURCE}))
        self.assertTrue(agent._parent_source_pair_valid({'source_path': agent.PARENT_CHAIN_SOURCE,
            'startup_source_path': agent.PARENT_CHAIN_STARTUP_SOURCE}))
        self.assertFalse(agent._parent_source_pair_valid({'source_path': agent.PARENT_SOURCE,
            'startup_source_path': agent.PARENT_CHAIN_STARTUP_SOURCE}))
        self.assertFalse(agent._parent_source_pair_valid({'source_path': agent.PARENT_CHAIN_SOURCE}))

    def test_prefix_source_requires_exact_new_pair_and_pinned_manifest(self):
        guard={'source_path':agent.PARENT_PREFIX_SOURCE,'startup_source_path':agent.PARENT_PREFIX_STARTUP,
               'source_sha256':'a'*64,'startup_source_sha256':'a'*64,
               'prefix_manifest_sha256':'a'*64,'chain_activation_sha256':'a'*64,'chain_policy_sha256':'b'*64}
        self.assertTrue(agent._parent_source_pair_valid(guard))
        self.assertFalse(agent._parent_source_pair_valid(dict(guard,source_path=agent.PARENT_CHAIN_SOURCE)))
        manifest={'schema':'neorome-bcp3-rpc-prefix-release-v1',
            'profile':'two_identical_complete_account_states_terminal_veto_r1',
            'policyChanged':False,'quotaChanged':False,'sdkArtifactsChanged':False,'successPathChecksChanged':False}
        manifest['deployedPins']={str(agent.Path(agent.PARENT_PREFIX_SOURCE).parent/name):'a'*64 for name in
                                 ('prefix_guard.py','parent_runtime.py','route.py','scheduler.py')}
        manifest['deployedPins'].update({'/opt/neorome-bcp3-child-freshness-r2/entry.cjs':'a'*64,
            '/opt/neorome-bcp3-child-freshness-r2/src/stdio_bridge.cjs':'a'*64})
        ledger={'position':None,'activeCandidate':None,'policySha256':'b'*64}
        activation={'schema':'neorome-bcp3-chain-source-activation-v1','state':'committed','newPolicySha256':'b'*64}
        with patch.object(agent,'_hash_file',return_value='a'*64), patch.object(agent,'_protected_prefix_hash',return_value='a'*64), patch.object(agent,'_read_json',side_effect=[ledger,manifest,activation]):
            self.assertTrue(agent._parent_retry_evidence({},guard))
        for field in ('policyChanged','quotaChanged','sdkArtifactsChanged','successPathChecksChanged'):
            with patch.object(agent,'_hash_file',return_value='a'*64), patch.object(agent,'_protected_prefix_hash',return_value='a'*64), patch.object(agent,'_read_json',side_effect=[ledger,dict(manifest,**{field:True})]):
                self.assertFalse(agent._parent_retry_evidence({},guard))
        with patch.object(agent,'_hash_file',return_value='c'*64), patch.object(agent,'_read_json',return_value=ledger):
            self.assertFalse(agent._parent_retry_evidence({},guard))
        # New launcher may resume lifecycle work only with its own accepted pins.
        held=dict(ledger,position={'id':'paper'})
        with patch.object(agent,'_hash_file',return_value='c'*64), patch.object(agent,'_read_json',return_value=held):
            self.assertFalse(agent._parent_retry_evidence({},guard))

if __name__ == '__main__':
    unittest.main()
