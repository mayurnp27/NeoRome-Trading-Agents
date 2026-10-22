import unittest
from desks.options.cash_reconciliation import reconcile_cash
class CashTests(unittest.TestCase):
    def execution(self):return {'execution_id':'fill1','symbol':'SPY261030C00650000','quantity':1,'price':'0.10','side':'buy','fee_cents':65,'multiplier':100,'adjusted':False}
    def test_fees_and_retry(self):
        e=self.execution();r=reconcile_cash([e,dict(e)],opening_cash_cents=30000,closing_cash_cents=28935)
        self.assertTrue(r['cash_matches']);self.assertEqual(r['unique_executions'],1);self.assertEqual(r['fees_cents'],65)
    def test_unexplained_cash_blocks(self):
        r=reconcile_cash([self.execution()],opening_cash_cents=30000,closing_cash_cents=28900)
        self.assertFalse(r['cash_matches']);self.assertEqual(r['unexplained_cash_cents'],-35);self.assertFalse(r['risk_release_allowed'])
    def test_conflicting_retry(self):
        e=self.execution();other=dict(e,fee_cents=66)
        with self.assertRaises(ValueError):reconcile_cash([e,other],opening_cash_cents=30000,closing_cash_cents=0)
    def test_sell_and_fractional_cash(self):
        e=self.execution();e['side']='sell'
        self.assertTrue(reconcile_cash([e],opening_cash_cents=30000,closing_cash_cents=30935)['cash_matches'])
        e['price']='0.100001'
        with self.assertRaises(ValueError):reconcile_cash([e],opening_cash_cents=30000,closing_cash_cents=0)
if __name__=='__main__':unittest.main()
