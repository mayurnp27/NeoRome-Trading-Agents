import subprocess
from types import SimpleNamespace
import unittest
from neorome.unit_health import check_units, TIMERS, _service_units


class UnitHealthTests(unittest.TestCase):
    def response(self, failed=None, disabled=None, missing=None):
        blocks = []
        for unit in TIMERS + _service_units():
            if unit == missing:
                continue
            active = 'active' if unit.endswith('.timer') else 'inactive'
            if unit == disabled:
                active = 'inactive'
            result = 'exit-code' if unit == failed else 'success'
            blocks.append(f'Id={unit}\nLoadState=loaded\nActiveState={active}\nResult={result}')
        return SimpleNamespace(returncode=0, stdout='\n\n'.join(blocks))

    def test_success_failure_missing_and_disabled(self):
        self.assertTrue(check_units(lambda *a, **k: self.response())['operational_healthy'])
        for kwargs in ({'failed': 'neorome-equity-daily.service'}, {'disabled': TIMERS[0]}, {'missing': TIMERS[1]}):
            self.assertFalse(check_units(lambda *a, **k: self.response(**kwargs))['operational_healthy'])

    def test_query_timeout_fails_closed(self):
        def failed(*a, **k):
            raise subprocess.TimeoutExpired('systemctl', 5)
        self.assertFalse(check_units(failed)['operational_healthy'])

    def test_current_fleet_does_not_require_archived_timer(self):
        units = ['neorome-crypto-recorder.timer', 'neorome-new-paper.service']
        def response(cmd, **kw):
            self.assertNotIn('neorome-recorder.timer', cmd)
            return SimpleNamespace(returncode=0, stdout='Id=neorome-crypto-recorder.timer\nLoadState=loaded\nActiveState=active\n\nId=neorome-new-paper.service\nLoadState=loaded\nActiveState=inactive\nType=oneshot\nResult=success')
        self.assertTrue(check_units(response, required_units=units)['operational_healthy'])
        for invalid in ([], ['other.service'], units + units, ['neorome-x.service --bad']):
            self.assertFalse(check_units(response, required_units=invalid)['operational_healthy'])

    def test_inactive_daemon_is_failure_but_inactive_oneshot_is_normal(self):
        def response(*a, **kw):
            return SimpleNamespace(returncode=0, stdout='Id=neorome-health.service\nLoadState=loaded\nActiveState=inactive\nType=simple\nMainPID=0\nResult=success')
        self.assertFalse(check_units(response, required_units=['neorome-health.service'])['operational_healthy'])
