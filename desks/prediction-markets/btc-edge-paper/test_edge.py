import unittest
from decimal import Decimal as D
import numpy as np
import model,replay

class TestEdge(unittest.TestCase):
    def test_fee_inclusive_gate_not_raw_price(self):
        self.assertFalse(replay.gate(.51,100,D('.5'),D('.0001'),D(0))[0])
        passed,expected,debit=replay.gate(.53,100,D('.5'),D('.0001'),D(0))
        self.assertTrue(passed);self.assertEqual(expected,D('1.25'));self.assertEqual(debit,D('51.75'))
    def test_strict_margin_and_zero_edge(self):
        self.assertFalse(replay.gate(.5175,100,D('.5'),D('.0001'),D(0))[0])
        self.assertFalse(replay.gate(.5275,100,D('.5'),D('.0001'),D('.01'))[0])
        self.assertTrue(replay.gate(.5276,100,D('.5'),D('.0001'),D('.01'))[0])
    def test_side_midpoint_features(self):
        yes,mid=model.feature_vector(1.,.69,.71);no,nmid=model.feature_vector(-1.,.69,.71)
        self.assertAlmostEqual(mid,.7);self.assertAlmostEqual(nmid,.3)
        self.assertAlmostEqual(yes[1],-no[1]);self.assertEqual(yes[2],no[2]);self.assertEqual(no[3],-1)
    def test_ridge_fit_converges(self):
        x=np.array([[1,-2],[1,-1],[1,0],[1,1],[1,2]],float);y=np.array([0,0,1,1,1])
        b,evidence=model.fit_logistic(x,y,[0,1]);self.assertLess(evidence['max_abs_gradient'],1e-4)
        self.assertTrue(np.all(np.diff(model.sigmoid(x@b))>0))
    def test_reliability_bins_do_not_double_count(self):
        rows=[{'won':True,'probability':v} for v in (.1,.2,.5,.9)]
        out=model.metrics(rows,'probability');self.assertEqual(sum(r['n'] for r in out['reliability']),4)
    def test_rejected_edge_does_not_debit_cash(self):
        t=1790000100
        row={'ticker':'fixture','minute':5,'z':1.,'excluded_reason':None,'probability':.51,'price':'.5','start':t,'entry_time':model.base.iso(t+360),'won':True}
        out=replay.run('A_5min','edge_gt_0',[row],False)
        self.assertEqual(out['trades'],0);self.assertEqual(out['ending_cash'],500);self.assertEqual(out['skips']['no_net_edge'],1)

if __name__=='__main__':unittest.main()
