"""One locked cloud cycle for admitted internal-paper desks, with no broker API."""
from __future__ import annotations
import contextlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo
from . import paper_runtime
from .crypto_pilot import _time
from .shared_marks import equity_quote, mark_provider, MarkUnavailable

DEFAULT_ACTIONS='/var/lib/neorome-equity-actions'
DEFAULT_DAILY='/var/lib/neorome-equity-daily'
DEFAULT_SPY='/var/lib/neorome-recorder'
DEFAULT_BTC='/var/lib/neorome-crypto-recorder'
DEFAULT_TOKEN='/var/lib/neorome-token-paper'
NY=ZoneInfo('America/New_York')


def read_object(path):
    path=Path(path)
    if path.is_symlink():raise ValueError('Evidence symlink refused')
    if not path.exists():return None
    if not path.is_file() or path.stat().st_size>8_000_000:raise ValueError('Invalid evidence file')
    value=json.loads(path.read_text())
    if not isinstance(value,dict):raise ValueError('Evidence object required')
    return value


def build_adapters(*, state, ledger, actions_root=DEFAULT_ACTIONS, daily_root=DEFAULT_DAILY,
                   spy_state=DEFAULT_SPY, btc_state=DEFAULT_BTC, token_state=DEFAULT_TOKEN):
    from .equity_shared import run, position_mark
    def actions(now):
        return read_object(Path(actions_root)/'evidence'/f'{now.astimezone(NY).date()}.json')
    def equity_mark(book,position,now):
        quote,source=equity_quote(spy_state,now)
        if _time(quote['timestamp'], 'SPY quote time') < _time(position.opened_at, 'position open'):
            raise MarkUnavailable('SPY quote precedes position')
        evidence=actions(now)
        if evidence is None:raise MarkUnavailable('SPY corporate-action evidence missing')
        return position_mark(book,position,now,quote=quote,quote_source=source,corporate_actions=evidence)
    from .token_memecoin_runtime import token_mark
    provider=mark_provider(crypto_state=btc_state,equity_state=spy_state,equity_marker=equity_mark,
                           token_marker=lambda book,position,now: token_mark(book,position,now,token_state))
    def cycle(now,allow_entry):
        bundle=read_object(Path(daily_root)/'runs'/f'{now.date()}.json')
        evidence=actions(now)
        quote=source=None
        try:quote,source=equity_quote(spy_state,now)
        except MarkUnavailable:pass
        report=run(bundle,state,ledger_path=ledger,quote=quote,quote_source=source,
                   now=now,mark_provider=provider,corporate_actions=evidence,
                   runtime_lock=contextlib.nullcontext(),allow_entry=allow_entry,
                   stop_path=Path(state)/'STOP')
        blocker=report.get('blocker')
        expected={None,'no_entry','stop_active','entries_disabled','entry_already_attempted',
                  'awaiting_independent_post_reservation_quote','insufficient_fill_depth',
                  'post_reservation_cost_exceeds_reserved_cash','global_marks_unavailable',
                  'corporate_action_entry_blocked','awaiting_independent_exit_quote','close_intent_expired',
                  'insufficient_exit_depth','insufficient_entry_depth',
                  'entry_size_exceeds_firm_cap','daily_bundle_not_current'}
        healthy=blocker in expected
        if bundle is None and str(blocker).startswith('daily_bundle_'):
            healthy=True
        # Missing daily action inputs is an admission gate; malformed inputs
        # must remain failures rather than being treated as a normal no-trade.
        if evidence is None and str(blocker).startswith('accounting_blocked: corporate_action_evidence_missing'):
            healthy=True
        report.update(operational_healthy=healthy,
                      desk_status='active' if bundle is not None and evidence is not None and healthy else 'waiting_for_evidence')
        return report
    return provider,cycle


def run_once(*,state=paper_runtime.DEFAULT_STATE,ledger=paper_runtime.DEFAULT_LEDGER,
             actions_root=DEFAULT_ACTIONS,daily_root=DEFAULT_DAILY,
             spy_state=DEFAULT_SPY,btc_state=DEFAULT_BTC,token_state=DEFAULT_TOKEN,
             allow_crypto_entry=False,**kwargs):
    provider,cycle=build_adapters(state=state,ledger=ledger,actions_root=actions_root,
                                 daily_root=daily_root,spy_state=spy_state,btc_state=btc_state,token_state=token_state)
    return paper_runtime.run_once(state,ledger_path=ledger,observation_state=btc_state,
                                 mark_provider=provider,equity_cycle=cycle,
                                 allow_crypto_entry=allow_crypto_entry,**kwargs)

if __name__=='__main__':
    try:
        result=run_once()
        print(json.dumps(result,sort_keys=True,separators=(',',':')))
        raise SystemExit(0 if result.get('operational_healthy') else 1)
    except Exception as exc:
        print(json.dumps({'operational_healthy':False,'error_type':type(exc).__name__,'broker_orders_sent':0}))
        raise SystemExit(1)
