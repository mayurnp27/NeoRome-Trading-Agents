"""Read-only, bounded systemd health checks for the paper services."""
import subprocess
import re

TIMERS = (
    'neorome-recorder.timer', 'neorome-crypto-recorder.timer',
    'neorome-weather-recorder.timer', 'neorome-crypto-daily.timer',
    'neorome-equity-daily.timer', 'neorome-paper-runtime.timer',
    'neorome-paper-recovery.timer', 'neorome-equity-actions.timer', 'neorome-prediction-evidence.timer',
    'neorome-weather-dataset.timer',
    'neorome-equity-intraday-watchlist.timer', 'neorome-equity-intraday-sip.timer',
    'neorome-equity-intraday-opportunity.timer',
    'neorome-prediction-internal-paper.timer', 'neorome-prediction-weather-coordinator.timer',
    'neorome-prediction-weather-execution-snapshot.timer', 'neorome-prediction-weather-candidate.timer',
    'neorome-prediction-weather-paper.timer',
    'neorome-prediction-24x7.timer',
    'neorome-options-trend-capture.timer', 'neorome-options-trend-inputs.timer',
    'neorome-options-cloud-evidence.timer', 'neorome-options-observed-candidate.timer',
    'neorome-options-internal-paper.timer', 'neorome-options-entry-capture.timer',
    'neorome-options-internal-paper-entry.timer', 'neorome-options-mark-capture.timer',
    'neorome-options-internal-paper-mark.timer', 'neorome-options-exit-capture.timer',
    'neorome-options-internal-paper-exit.timer',
    'neorome-options-full-port-candidate.timer',
    'neorome-options-full-port-internal-paper-reserve.timer',
    'neorome-options-full-port-entry-capture.timer',
    'neorome-options-full-port-internal-paper-entry.timer',
    'neorome-options-full-port-mark-capture.timer',
    'neorome-options-full-port-internal-paper-mark.timer',
    'neorome-options-full-port-exit-capture.timer',
    'neorome-options-full-port-internal-paper-exit.timer',
    'neorome-token-paper.timer',
    'neorome-equities-validation-publisher.timer',
    'neorome-options-validation-publisher.timer',
    'neorome-crypto-validation-publisher.timer',
    'neorome-prediction-validation-publisher.timer',
    'neorome-validation-manifest.timer',
    'neorome-validation-daily.timer', 'neorome-validation-weekly.timer',
)

_SHARED_SERVICE_TIMERS = {
    'neorome-options-internal-paper-entry.timer',
    'neorome-options-internal-paper-mark.timer',
    'neorome-options-internal-paper-exit.timer',
    'neorome-options-full-port-internal-paper-reserve.timer',
    'neorome-options-full-port-internal-paper-entry.timer',
    'neorome-options-full-port-internal-paper-mark.timer',
    'neorome-options-full-port-internal-paper-exit.timer',
}

_VALIDATION_SERVICE_TIMERS = {
    'neorome-validation-daily.timer',
    'neorome-validation-weekly.timer',
}


def _service_units():
    """Return actual service targets, including timers sharing one worker."""
    services = []
    for name in TIMERS:
        if name.startswith('neorome-options-full-port-internal-paper-'):
            service = 'neorome-options-full-port-internal-paper.service'
        elif name in _VALIDATION_SERVICE_TIMERS:
            service = 'neorome-validation.service'
        elif name in _SHARED_SERVICE_TIMERS:
            service = 'neorome-options-internal-paper.service'
        else:
            service = name.replace('.timer', '.service')
        if service not in services:
            services.append(service)
    return tuple(services)


def validated_units(value):
    if (not isinstance(value, (list, tuple)) or not value or len(value) > 512 or
            not all(isinstance(unit, str) and re.fullmatch(r'neorome-[a-zA-Z0-9_.@-]{1,140}\.(service|timer)', unit) for unit in value) or
            len(set(value)) != len(value)):
        raise ValueError('invalid required units')
    return tuple(value)


def check_units(run=subprocess.run, *, required_units=None):
    try:
        units = validated_units(required_units) if required_units is not None else TIMERS + _service_units()
        response = run(
            ['systemctl', 'show', '--no-pager', '--property=Id,LoadState,ActiveState,Result,Type,MainPID', *units],
            capture_output=True, text=True, timeout=5, check=False)
        if response.returncode or len(response.stdout) > 2 * 1024 * 1024:
            raise ValueError('unit query failed')
        rows = {}
        for block in response.stdout.strip().split('\n\n'):
            fields = dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
            if fields.get('Id') in rows:
                raise ValueError('duplicate unit')
            rows[fields.get('Id')] = fields
        issues = []
        for unit in units:
            row = rows.get(unit, {})
            if row.get('LoadState') != 'loaded':
                issues.append(unit + ': unavailable')
            elif unit.endswith('.timer') and row.get('ActiveState') != 'active':
                issues.append(unit + ': timer inactive')
            elif row.get('ActiveState') == 'failed' or row.get('Result', 'success') != 'success':
                issues.append(unit + ': service failed')
            elif row.get('Type') in {'simple', 'exec'} and (row.get('ActiveState') != 'active' or row.get('MainPID', '0') == '0'):
                issues.append(unit + ': daemon inactive')
        return {'operational_healthy': not issues, 'issues': issues, 'checked_units': list(units)}
    except (OSError, ValueError, subprocess.SubprocessError):
        return {'operational_healthy': False, 'issues': ['unit health unavailable']}
