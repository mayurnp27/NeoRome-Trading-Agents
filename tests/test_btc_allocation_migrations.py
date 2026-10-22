import json
import tempfile
import unittest
from pathlib import Path

from neorome import btc_kd_gap_feed as gap
from neorome import btc_bracket_feed as brackets


class AllocationMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def legacy_gap(self, timeframe='5m'):
        root = self.root / timeframe
        root.mkdir()
        config = gap._canonical(gap.EXPERIMENTS[timeframe] | {'cash_fraction': 0.5}) + '\n'
        (root / gap.CONFIG_NAME).write_text(config)
        (root / 'ledger-sentinel').write_bytes(b'preserve financial history')
        return root, config

    def legacy_brackets(self):
        config = {'version': 'BTC-BRACKETS-1HZ-v1', 'started_at': 12345.0,
                  'initial_cash_cents_each': 50000, 'allocation_bps': 5000,
                  'source_cohorts': 'new original crossover fills, observed prospectively',
                  'target_cadence_seconds': 1, 'data_only': False}
        brackets.atomic(self.root / 'config.json', config)
        (self.root / 'comparison.sqlite3').write_bytes(b'database must not be reopened or reset')
        return config

    def test_gap_legacy_requires_explicit_migration_and_preserves_base_and_history(self):
        for timeframe in ['5m', '30m']:
            root, before = self.legacy_gap(timeframe)
            with self.assertRaisesRegex(gap.GapFeedError, 'migration_required'):
                gap._configure_root(root, timeframe)
            policy = gap.migrate_allocation(timeframe, root)
            self.assertEqual(policy['allocation_bps'], 2000)
            self.assertEqual(policy['prior_allocation_bps'], 5000)
            self.assertEqual((root / gap.CONFIG_NAME).read_text(), before)
            self.assertEqual((root / 'ledger-sentinel').read_bytes(), b'preserve financial history')
            first = (root / gap.ALLOCATION_NAME).read_bytes()
            self.assertEqual(gap.migrate_allocation(timeframe, root), policy)
            self.assertEqual((root / gap.ALLOCATION_NAME).read_bytes(), first)
            self.assertEqual(gap._configure_root(root, timeframe), policy)

    def test_gap_refuses_unknown_base_or_tampered_policy(self):
        root, before = self.legacy_gap()
        (root / gap.CONFIG_NAME).write_text(before.replace('"k_d_gap_max":5.0', '"k_d_gap_max":6.0'))
        with self.assertRaisesRegex(gap.GapFeedError, 'config_conflict'):
            gap.migrate_allocation('5m', root)
        (root / gap.CONFIG_NAME).write_text(before)
        gap.migrate_allocation('5m', root)
        path = root / gap.ALLOCATION_NAME
        value = json.loads(path.read_text());value['allocation_bps'] = 5000
        path.chmod(0o600);path.write_text(json.dumps(value))
        with self.assertRaisesRegex(gap.GapFeedError, 'policy_conflict'):
            gap._configure_root(root, '5m')

    def test_gap_policy_symlink_refused(self):
        root, _ = self.legacy_gap()
        (root / gap.ALLOCATION_NAME).symlink_to(self.root / 'elsewhere')
        with self.assertRaisesRegex(gap.GapFeedError, 'symlink'):
            gap.migrate_allocation('5m', root)

    def test_bracket_migration_preserves_inception_and_financial_bytes(self):
        before = self.legacy_brackets()
        policy = brackets.migrate_allocation(self.root)
        after = json.loads((self.root / 'config.json').read_text())
        self.assertEqual(after['started_at'], before['started_at'])
        self.assertEqual(after['allocation_bps'], 2000)
        self.assertEqual(policy['prior_config'], before)
        self.assertEqual((self.root / 'comparison.sqlite3').read_bytes(), b'database must not be reopened or reset')
        self.assertEqual(brackets.migrate_allocation(self.root), policy)

    def test_bracket_interrupted_policy_write_resumes_without_rewriting_history(self):
        before = self.legacy_brackets()
        policy = brackets.migrate_allocation(self.root)
        history = (self.root / 'allocation-policy.json').read_bytes()
        brackets.atomic(self.root / 'config.json', before)
        self.assertEqual(brackets.migrate_allocation(self.root), policy)
        self.assertEqual((self.root / 'allocation-policy.json').read_bytes(), history)
        self.assertEqual(json.loads((self.root / 'config.json').read_text())['allocation_bps'], 2000)

    def test_bracket_unknown_config_and_symlink_refused(self):
        before = self.legacy_brackets()
        brackets.atomic(self.root / 'config.json', before | {'allocation_bps': 3000})
        with self.assertRaisesRegex(ValueError, 'unknown legacy'):
            brackets.migrate_allocation(self.root)
        brackets.atomic(self.root / 'config.json', before)
        (self.root / 'allocation-policy.json').symlink_to(self.root / 'elsewhere')
        with self.assertRaisesRegex(ValueError, 'symlink'):
            brackets.migrate_allocation(self.root)


if __name__ == '__main__':
    unittest.main()
