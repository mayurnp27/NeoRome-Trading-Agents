"""Offline acceptance tests for the exact profit-overlay maintenance source pair."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import agent


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class ProfitGuardTests(unittest.TestCase):
    def setUp(self):
        prefix_root = Path(agent.PARENT_PREFIX_SOURCE).parent
        paths = [str(prefix_root / name) for name in
                 ('prefix_guard.py', 'parent_runtime.py', 'route.py', 'scheduler.py')]
        paths += ['/opt/neorome-bcp3-child-freshness-r2/entry.cjs',
                  '/opt/neorome-bcp3-child-freshness-r2/src/stdio_bridge.cjs']
        self.hashes = {path: digest(path) for path in paths}
        self.hashes[agent.PARENT_PREFIX_STARTUP] = agent.PARENT_PROFIT_PREFIX_STARTUP_SHA256
        self.prefix = {
            'schema': 'neorome-bcp3-rpc-prefix-release-v1',
            'profile': 'two_identical_complete_account_states_terminal_veto_r1',
            'deployedPins': {path: self.hashes[path] for path in paths},
            **{key: False for key in ('policyChanged', 'quotaChanged',
                                     'sdkArtifactsChanged', 'successPathChecksChanged')},
        }
        profit_root = Path(agent.PARENT_PROFIT_SOURCE).parent
        self.profit = {
            'schema': 'neorome-bcp3-profit-protection-release-v1',
            'profile': 'bcp3-profit-protection-r1',
            'files': {name: digest(name) for name in ('runtime.py', 'engine.py', 'scheduler.py')},
            'prefixStartupSha256': self.hashes[agent.PARENT_PREFIX_STARTUP],
        }
        self.hashes.update({str(profit_root / name): value for name, value in self.profit['files'].items()})
        self.hashes[agent.PARENT_PROFIT_STARTUP] = digest('new startup')
        self.hashes[agent.PARENT_CHAIN_ACTIVATION] = digest('original activation')
        self.hashes['/parent.service'] = digest('parent unit')
        self.guard = {
            'unit_sha256': self.hashes['/parent.service'],
            'source_path': agent.PARENT_PROFIT_SOURCE,
            'source_sha256': self.hashes[agent.PARENT_PROFIT_SOURCE],
            'startup_source_path': agent.PARENT_PROFIT_STARTUP,
            'startup_source_sha256': self.hashes[agent.PARENT_PROFIT_STARTUP],
            'dropin_sha256': {},
            'chain_policy_sha256': digest('unchanged policy'),
            'chain_activation_sha256': self.hashes[agent.PARENT_CHAIN_ACTIVATION],
        }
        self.bind_manifests()
        self.ledger = {'position': None, 'activeCandidate': None,
                       'policySha256': self.guard['chain_policy_sha256']}
        self.activation = {'schema': 'neorome-bcp3-chain-source-activation-v1',
                           'state': 'committed', 'newPolicySha256': self.guard['chain_policy_sha256']}
        self.item = {'LoadState': 'loaded', 'Type': 'exec', 'ActiveState': 'failed',
                     'MainPID': '0', 'Job': '', 'FragmentPath': '/parent.service'}
        self.protected = patch.object(agent, '_protected_prefix_hash', side_effect=self.hashes.get)
        self.read = patch.object(agent, '_read_json', side_effect=self.read_json)
        self.hash = patch.object(agent, '_hash_file', side_effect=self.hashes.get)
        self.protected.start(); self.read.start(); self.hash.start()
        self.addCleanup(self.protected.stop)
        self.addCleanup(self.read.stop)
        self.addCleanup(self.hash.stop)

    def bind_manifests(self):
        for value, path, key in [(self.prefix, agent.PARENT_PREFIX_MANIFEST, 'prefix_manifest_sha256'),
                                 (self.profit, agent.PARENT_PROFIT_MANIFEST, 'profit_manifest_sha256')]:
            fingerprint = digest(json.dumps(value, sort_keys=True))
            self.hashes[path] = self.guard[key] = fingerprint

    def read_json(self, path, limit):
        values = {agent.PARENT_PROFIT_MANIFEST: self.profit, agent.PARENT_PREFIX_MANIFEST: self.prefix,
                  agent.PARENT_CHAIN_ACTIVATION: self.activation,
                  str(agent.COLLECTOR_JOURNAL.parent / 'ledger.json'): self.ledger}
        if str(path) not in values:
            raise agent.MaintenanceError('unexpected file')
        return deepcopy(values[str(path)])

    def load_guard(self, guard):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'policy.json'
            value = {'schema_version': 1, 'expected_units': [agent.PARENT_REPAIR],
                     'source_statuses': [], 'repair_guards': {agent.PARENT_REPAIR: guard}}
            # Restore only the public policy reader: production filesystem checks
            # remain mocked, and no host state, credentials or services are used.
            path.write_text(json.dumps(value))
            with patch.object(agent, '_read_json', return_value=value):
                return agent._load_policy(path)

    def test_exact_new_pair_and_required_pins_load(self):
        self.assertTrue(agent._parent_source_pair_valid(self.guard))
        self.assertEqual(self.load_guard(self.guard)['repair_guards'][agent.PARENT_REPAIR], self.guard)

    def test_cross_release_and_unknown_pairs_rejected(self):
        for source, startup in [(agent.PARENT_PROFIT_SOURCE, agent.PARENT_PREFIX_STARTUP),
                                (agent.PARENT_PREFIX_SOURCE, agent.PARENT_PROFIT_STARTUP),
                                ('/opt/neorome/other.py', agent.PARENT_PROFIT_STARTUP),
                                (agent.PARENT_PROFIT_SOURCE, '/opt/neorome/other-startup.py')]:
            with self.subTest(source=source, startup=startup):
                guard = {**self.guard, 'source_path': source, 'startup_source_path': startup}
                self.assertFalse(agent._parent_source_pair_valid(guard))
                with self.assertRaises(agent.MaintenanceError):
                    self.load_guard(guard)

    def test_all_inherited_and_new_policy_pins_required(self):
        for key in ['chain_policy_sha256', 'chain_activation_sha256', 'prefix_manifest_sha256',
                    'profit_manifest_sha256', 'startup_source_sha256']:
            for value in [None, 'bad-hash']:
                with self.subTest(key=key, value=value):
                    guard = deepcopy(self.guard)
                    if value is None:
                        del guard[key]
                    else:
                        guard[key] = value
                    with self.assertRaises(agent.MaintenanceError):
                        self.load_guard(guard)

    def test_original_prefix_acceptance_still_works(self):
        guard = {**self.guard, 'source_path': agent.PARENT_PREFIX_SOURCE,
                 'source_sha256': self.hashes[agent.PARENT_PREFIX_SOURCE],
                 'startup_source_path': agent.PARENT_PREFIX_STARTUP,
                 'startup_source_sha256': self.hashes[agent.PARENT_PREFIX_STARTUP]}
        self.load_guard(guard)
        self.assertTrue(agent._prefix_acceptance(guard))
        self.assertTrue(agent._parent_retry_evidence({}, guard))

    def test_profit_acceptance_retains_original_prefix_gate(self):
        self.assertTrue(agent._profit_acceptance(self.guard))
        with patch.object(agent, '_prefix_acceptance', return_value=False) as original:
            self.assertFalse(agent._profit_acceptance(self.guard))
        original.assert_called_once()
        self.assertEqual(original.call_args.args[0]['source_sha256'], self.hashes[agent.PARENT_PREFIX_SOURCE])
        self.assertEqual(original.call_args.args[0]['startup_source_sha256'], self.hashes[agent.PARENT_PREFIX_STARTUP])

    def test_missing_changed_or_unprotected_dependencies_rejected(self):
        paths = [agent.PARENT_PROFIT_MANIFEST, agent.PARENT_PROFIT_STARTUP,
                 agent.PARENT_PREFIX_MANIFEST, agent.PARENT_PREFIX_STARTUP,
                 *self.prefix['deployedPins'],
                 *(str(Path(agent.PARENT_PROFIT_SOURCE).parent / name) for name in self.profit['files'])]
        for path in paths:
            with self.subTest(path=path):
                previous = self.hashes[path]
                self.hashes[path] = None  # Missing, writable, symlinked or wrong-owner protected path.
                self.assertFalse(agent._profit_acceptance(self.guard))
                self.hashes[path] = digest('changed bytes')
                self.assertFalse(agent._profit_acceptance(self.guard))
                self.hashes[path] = previous

    def test_exact_manifest_schema_profile_files_and_startup_pin_required(self):
        valid = deepcopy(self.profit)
        variants = [dict(valid, schema='other'), dict(valid, profile='other'),
                    dict(valid, files={}), dict(valid, files={**valid['files'], '../escape.py': 'a' * 64}),
                    dict(valid, prefixStartupSha256='bad')]
        missing_startup = deepcopy(valid); del missing_startup['prefixStartupSha256']; variants.append(missing_startup)
        invalid_digest = deepcopy(valid); invalid_digest['files']['engine.py'] = 'bad'; variants.append(invalid_digest)
        for manifest in variants:
            with self.subTest(manifest=manifest):
                self.profit = manifest; self.bind_manifests()
                self.assertFalse(agent._profit_acceptance(self.guard))
        self.profit = valid; self.bind_manifests()

    def test_new_guard_source_and_startup_hash_must_match(self):
        for key in ['source_sha256', 'startup_source_sha256']:
            with self.subTest(key=key):
                self.assertFalse(agent._profit_acceptance({**self.guard, key: 'e' * 64}))
        for key in ['source_path', 'startup_source_path']:
            with self.subTest(key=key):
                self.assertFalse(agent._profit_acceptance({**self.guard, key: '/opt/neorome/other.py'}))

    def test_original_manifest_shape_and_safety_flags_still_enforced(self):
        valid = deepcopy(self.prefix)
        for key in ['policyChanged', 'quotaChanged', 'sdkArtifactsChanged', 'successPathChecksChanged']:
            self.prefix = {**deepcopy(valid), key: True}; self.bind_manifests()
            self.assertFalse(agent._profit_acceptance(self.guard))
        self.prefix = deepcopy(valid)
        self.prefix['deployedPins']['/opt/neorome/extra.py'] = 'a' * 64
        self.bind_manifests()
        self.assertFalse(agent._profit_acceptance(self.guard))

    def test_flat_restart_requires_original_committed_epoch_and_ledger_binding(self):
        self.assertTrue(agent._parent_retry_evidence({}, self.guard))
        self.activation['state'] = 'prepared'
        self.assertFalse(agent._parent_retry_evidence({}, self.guard))
        self.activation['state'] = 'committed'
        self.ledger['policySha256'] = 'e' * 64
        self.assertFalse(agent._parent_retry_evidence({}, self.guard))

    def test_open_lifecycle_priority_still_requires_both_protected_releases(self):
        self.ledger['position'] = {'positionId': 'existing'}
        self.activation['state'] = 'prepared'  # Startup still owns full private-policy validation.
        self.assertTrue(agent._parent_retry_evidence({}, self.guard))
        self.hashes[agent.PARENT_PROFIT_MANIFEST] = None
        self.assertFalse(agent._parent_retry_evidence({}, self.guard))

    def test_unreadable_manifest_fails_closed_in_retry_evidence(self):
        with patch.object(agent, '_read_json', side_effect=agent.MaintenanceError('invalid JSON')):
            self.assertFalse(agent._parent_retry_evidence({}, self.guard))

    def test_existing_unit_dropin_and_idle_requirements_remain(self):
        with patch.object(agent, '_dropin_hashes_match', return_value=True):
            self.assertTrue(agent._repair_target(agent.PARENT_REPAIR, self.item, self.guard, {}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR, {**self.item, 'MainPID': '123'}, self.guard, {}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR, {**self.item, 'Job': '45'}, self.guard, {}))
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR, {**self.item, 'ActiveState': 'active'}, self.guard, {}))
        with patch.object(agent, '_dropin_hashes_match', return_value=False):
            self.assertFalse(agent._repair_target(agent.PARENT_REPAIR, self.item, self.guard, {}))


if __name__ == '__main__':
    unittest.main()
