import unittest
from desks.crypto.micro_entry_audit import replay

class MinuteAuditTest(unittest.TestCase):
    def bars(self):
        return [[0,1,1,1,1,10],[60,1,1,1,1,10],
                [120,1,1.1,1,1.1,20],[180,1.1,1.8,.7,1.2,20]]
    def test_signal_enters_next_bar_and_stop_wins_ambiguity(self):
        r=replay(self.bars(),cutoff=240,earliest=0,cost_bps=0)
        self.assertEqual(r['entry_at'],180)
        self.assertEqual(r['exit_reason'],'stop')
        self.assertAlmostEqual(r['return_pct'],-25)
    def test_incomplete_future_bar_cannot_fill(self):
        self.assertEqual(replay(self.bars(),cutoff=239,earliest=0)['status'],'no_signal')
    def test_discovery_boundary_and_costs(self):
        self.assertEqual(replay(self.bars(),cutoff=240,earliest=181)['status'],'no_signal')
        self.assertLess(replay(self.bars(),cutoff=240,earliest=0)['return_pct'],-25)
    def test_gap_cannot_form_signal(self):
        b=self.bars();b[-1][0]=240
        self.assertEqual(replay(b,cutoff=300,earliest=0)['status'],'no_signal')
    def test_duplicate_and_nan_rejected(self):
        with self.assertRaises(ValueError):replay(self.bars()+[self.bars()[-1]],cutoff=300,earliest=0)
        b=self.bars();b[-1][4]=float('nan')
        with self.assertRaises(ValueError):replay(b,cutoff=300,earliest=0)
