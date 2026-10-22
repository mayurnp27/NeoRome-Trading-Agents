from datetime import datetime, timezone
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location('subject', Path(__file__).with_name('bcp3_scheduler.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.parent = SimpleNamespace(_rpc_headroom_needed=lambda *a: (71, 32),
                                      _shared_credit_remaining=lambda *a: 1784)
        self.quota = SimpleNamespace(plan_actual_attempt=lambda kind, day:
                                     {'used': 0, 'limit': 600 if kind == 'helius_rpc' else 100})
        self.now = datetime(2026, 10, 16, tzinfo=timezone.utc)

    def report(self):
        return m.budget_report(self.parent, self.quota, {}, Path('/unused'), self.now)

    def test_ready_and_no_debits(self):
        self.assertEqual(self.report()['reason'], 'budget_ready')
        self.assertTrue(self.report()['available'])

    def test_lifecycle_headroom_not_just_next_call(self):
        self.quota.plan_actual_attempt = lambda *a: {'used': 530, 'limit': 600}
        self.assertEqual(self.report()['reason'], 'helius_rpc_headroom_insufficient')

    def test_quote_and_shared_shortfalls(self):
        self.quota.plan_actual_attempt = lambda kind, day: {'used': 0, 'limit': 600 if kind == 'helius_rpc' else 31}
        self.assertEqual(self.report()['reason'], 'jupiter_quote_headroom_insufficient')
        self.setUp()
        self.parent._shared_credit_remaining = lambda *a: 70
        self.assertEqual(self.report()['reason'], 'shared_helius_headroom_insufficient')

    def test_lock_busy_is_not_budget_exhaustion(self):
        class Busy(Exception):
            code = 'shared_budget_busy'
        def busy(*a): raise Busy()
        self.parent._shared_credit_remaining = busy
        result = self.report()
        self.assertEqual(result['reason'], 'budget_check_busy')
        self.assertFalse(result['available'])

    def test_invalid_and_unexpected_failure_stay_closed(self):
        self.quota.plan_actual_attempt = lambda *a: {'used': True, 'limit': 600}
        self.assertEqual(self.report()['reason'], 'budget_plan_invalid')
        def broken(*a): raise ValueError('private details')
        self.quota.plan_actual_attempt = broken
        result = self.report()
        self.assertFalse(result['available'])
        self.assertNotIn('private details', str(result))

    def test_diagnostics_throttled_without_caching_authorization(self):
        base = SimpleNamespace()
        reports = []
        m.install_diagnostics(base, emit=reports.append, clock=lambda: 1)
        for _ in range(3):
            self.assertTrue(base.budget_headroom(self.parent, {}, self.quota, {}, Path('/unused'), self.now))
        self.assertEqual(len(reports), 1)
        self.parent._shared_credit_remaining = lambda *a: 0
        self.assertFalse(base.budget_headroom(self.parent, {}, self.quota, {}, Path('/unused'), self.now))
        self.assertEqual(len(reports), 2)

    def test_validator_diagnostics_preserve_success_and_failure(self):
        class Rejected(Exception):
            code = 'continuous_queue_hint_stale_or_future'
        failure = Rejected('private raw data')
        def validate(value, now):
            if value.get('reject'): raise failure
            return ('unchanged', now)
        module = SimpleNamespace(_validate_queue_selected=validate)
        reports = []
        m.instrument_validators({'callbacks': module}, reports.append)
        self.assertEqual(module._validate_queue_selected({}, self.now), ('unchanged', self.now))
        with self.assertRaises(Rejected) as caught:
            module._validate_queue_selected({'reject': True, 'hint': {'receivedAt': 'secret'}}, self.now)
        self.assertIs(caught.exception, failure)
        self.assertIsNone(reports[0]['hintAt'])
        self.assertNotIn('secret', str(reports))
        self.assertNotIn('private', str(reports))
        wrapped = module._validate_queue_selected
        m.instrument_validators({'callbacks': module}, reports.append)
        self.assertIs(module._validate_queue_selected, wrapped)

if __name__ == '__main__':
    unittest.main()
