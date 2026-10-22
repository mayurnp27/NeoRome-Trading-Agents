from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from neorome.health_endpoint import healthy, readiness


class HealthTests(unittest.TestCase):
    def test_readiness_names_failed_dependencies_without_exposing_paths(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as root:
            good = Path(root) / 'good.json'
            missing = Path(root) / 'missing.json'
            good.write_text(json.dumps({'updated_at': now.isoformat(), 'operational_healthy': True}))
            result = readiness([
                ('paper_runtime', good, 180),
                ('recovery_verification', missing, 7200),
            ], now)
            self.assertFalse(result['ready'])
            self.assertEqual(result['failed_checks'], ['recovery_verification'])
            self.assertNotIn(str(root), json.dumps(result))

    def test_readiness_succeeds_when_all_dependencies_are_current(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as root:
            first = Path(root) / 'first.json'
            second = Path(root) / 'second.json'
            payload = json.dumps({'updated_at': now.isoformat(), 'operational_healthy': True})
            first.write_text(payload)
            second.write_text(payload)
            result = readiness([('operations', first, 660), ('paper_runtime', second, 180)], now)
            self.assertTrue(result['ready'])
            self.assertEqual(result['failed_checks'], [])

    def test_runtime_and_recovery_freshness_limits(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'status.json'
            path.write_text(json.dumps({'updated_at': (now-timedelta(seconds=181)).isoformat(), 'operational_healthy': True}))
            self.assertFalse(healthy(path, now, max_age_seconds=180))
            self.assertTrue(healthy(path, now, max_age_seconds=7200))

    def test_missing_stale_future_failure_and_valid(self):
        now = datetime.now(timezone.utc)
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'status.json'
            self.assertFalse(healthy(path, now))
            for seconds, flag, expected in [(0, True, True), (700, True, False), (-1, True, False), (0, False, False), (0, 'true', False)]:
                path.write_text(json.dumps({'updated_at': (now-timedelta(seconds=seconds)).isoformat(), 'operational_healthy': flag}))
                self.assertEqual(healthy(path, now), expected)
            path.write_text('{}')
            self.assertFalse(healthy(path, now))
            for malformed in [[], None, {'updated_at': 123}, {'updated_at': []}]:
                path.write_text(json.dumps(malformed))
                self.assertFalse(healthy(path, now))
