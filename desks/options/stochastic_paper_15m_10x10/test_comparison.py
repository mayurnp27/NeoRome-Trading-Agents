import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import comparison
import feedback
from comparison import comparison_row

class ComparisonTests(unittest.TestCase):
    def test_old_history_is_excluded_from_monday_return(self):
        reference={'equity':1200.2,'realized_pnl':200.2}
        p={'state_status':'available','cash':1250.2,'realized_pnl':250.2,'position_count':0}
        r=comparison_row(p,reference)
        self.assertEqual(r['net_pnl_since_launch'],50)
        self.assertEqual(r['realized_pnl_since_launch'],50)
        self.assertAlmostEqual(r['return_pct'],50/1200.2*100,places=5)
    def test_open_positions_do_not_use_cash_as_portfolio_equity(self):
        r=comparison_row({'state_status':'available','cash':600,'realized_pnl':0,'position_count':1,'equity_status':'not_observed'}, {'equity':1000,'realized_pnl':0})
        self.assertIsNone(r['equity']);self.assertIsNone(r['return_pct'])
    def test_open_positions_use_fresh_observed_equity(self):
        r=comparison_row({'state_status':'available','cash':600,'realized_pnl':0,'position_count':1,'equity_status':'observed','equity':1050},{'equity':1000,'realized_pnl':0})
        self.assertEqual(r['return_pct'],5)
    def test_missing_state_is_explicit(self):
        self.assertEqual(comparison_row({'state_status':'unavailable'},{'equity':1000,'realized_pnl':0})['status'],'unavailable')
    def test_large_ledger_read_without_relaxing_config_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp).resolve()/'A.json'
            p.write_text(json.dumps({'cash':1133.8,'padding':'x'*(feedback.MAX_FILE_BYTES+100)}))
            with self.assertRaisesRegex(feedback.SourceError,'oversized'):
                feedback._read_source(p)
            self.assertEqual(feedback._read_source(p,max_bytes=feedback.MAX_LEDGER_BYTES)[0]['cash'],1133.8)
            with patch.object(feedback,'MAX_LEDGER_BYTES',32):
                with self.assertRaisesRegex(feedback.SourceError,'oversized'):
                    feedback._read_source(p,max_bytes=feedback.MAX_LEDGER_BYTES)
    def test_repeated_publish_error_exits_for_native_restart(self):
        with patch.object(feedback,'_read_source',return_value=({},None)),patch.object(comparison,'build',side_effect=feedback.SourceError('oversized')),patch.object(comparison.time,'sleep') as sleep,patch('builtins.print') as output:
            self.assertEqual(comparison.main([]),1)
            self.assertEqual(sleep.call_count,2)
            self.assertIn('oversized',output.call_args.args[0])
    def test_transient_failure_recovers_and_resets_streak(self):
        good=({'structural_healthy':True},{})
        outcomes=[feedback.SourceError('changed_during_read'),good,feedback.SourceError('unreadable'),feedback.SourceError('unreadable'),feedback.SourceError('unreadable')]
        with patch.object(feedback,'_read_source',return_value=({},None)),patch.object(comparison,'build',side_effect=outcomes) as build,patch.object(feedback,'_atomic_publish'),patch.object(comparison.time,'sleep'),patch('builtins.print'):
            self.assertEqual(comparison.main([]),1)
            self.assertEqual(build.call_count,5)

if __name__=='__main__':unittest.main()
