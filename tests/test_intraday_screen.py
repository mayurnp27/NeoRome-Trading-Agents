"""Run using the isolated VectorBT research Python interpreter."""
import hashlib
import tempfile
import unittest
from pathlib import Path

try:
    import pandas as pd
    import vectorbt
except ImportError:
    raise unittest.SkipTest('requires isolated vectorbt research environment')

from desks.equities.intraday_screen import ScreenValidationError, build_trades, run_csv, screen_frame, validate_bars


def bars(minutes=390, cross=30):
    rows = []
    start = pd.Timestamp('2026-09-21T13:30:00Z')
    for minute in range(minutes):
        price = 9.90 if minute < cross else 10.20
        if minute == minutes - 5: price = 11.0
        rows.append(dict(symbol='TEST', timestamp=(start + pd.Timedelta(minutes=minute)).isoformat(),
                         open=price, high=max(price, 10.0)+0.01, low=min(price, 9.9)-0.01,
                         close=price, volume=1000, vwap=10.0,
                         session_close=(start+pd.Timedelta(minutes=minutes)).isoformat()))
    return pd.DataFrame(rows)


class IntradayScreenTests(unittest.TestCase):
    def test_eight_trials_costs_and_next_open(self):
        result=screen_frame(bars())
        self.assertEqual(len(result['trials']),8)
        for trial in result['trials']:
            trade=trial['trades'][0]
            self.assertEqual(trade['entry_timestamp'],'2026-09-21T14:01:00+00:00')
            self.assertEqual(trade['exit_timestamp'],'2026-09-21T19:55:00+00:00')
            self.assertAlmostEqual(trial['portfolio']['total_profit'],trade['net_pnl_one_share'],places=8)
            self.assertEqual(trial['controls']['cash']['total_return'],0)

    def test_missing_bar_rejected(self):
        for missing in [0,8,31,385]:
            with self.subTest(missing=missing), self.assertRaises(ScreenValidationError):
                validate_bars(bars().drop(index=missing))

    def test_early_close(self):
        trades=build_trades(validate_bars(bars(210)), 'orb15')
        self.assertEqual(trades[0].exit_timestamp.isoformat(),'2026-09-21T16:55:00+00:00')

    def test_no_1100_entry(self):
        for candidate in ['orb15','vwap_reclaim']:
            self.assertEqual(build_trades(validate_bars(bars(cross=89)),candidate),[])

    def test_provided_signal_cannot_change_rule(self):
        data=bars(); data['orb_signal']=False
        with self.assertRaises(ScreenValidationError):
            build_trades(validate_bars(data),'orb15')

    def test_future_prices_cannot_change_entry(self):
        first=build_trades(validate_bars(bars()),'orb15')[0]
        data=bars(); data.loc[100:200,['open','high','low','close']]=100.0
        second=build_trades(validate_bars(data),'orb15')[0]
        self.assertEqual(first.entry_timestamp,second.entry_timestamp)
        self.assertEqual(first.entry_open,second.entry_open)

    def test_insufficient_cash_cannot_invent_one_share_fill(self):
        with self.assertRaises(ScreenValidationError): screen_frame(bars(),init_cash=5.0)

    def test_naive_time_rejected(self):
        data=bars(); data.loc[0,'timestamp']='2026-09-21T13:30:00'
        with self.assertRaises(ScreenValidationError): validate_bars(data)

    def test_exclusive_hash_bound_report(self):
        with tempfile.TemporaryDirectory() as root:
            source=Path(root)/'bars.csv'; output=Path(root)/'result.json'
            bars().to_csv(source,index=False)
            result=run_csv(source,output)
            self.assertEqual(result['input_sha256'],hashlib.sha256(source.read_bytes()).hexdigest())
            with self.assertRaises(FileExistsError): run_csv(source,output)


if __name__=='__main__': unittest.main()
