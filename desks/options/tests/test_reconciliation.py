from datetime import timedelta
import unittest
import test_position_review as fixtures
from desks.options.reconciliation import reconcile
class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        x=fixtures.PositionReviewTests();x.setUp();self.entry=x.entry;self.now=x.now+timedelta(seconds=10)
        self.reports=[{'report_id':str(i),'order_id':str(i),'symbol':leg.symbol,'intent':'open','cumulative_quantity':1,'timestamp':x.now.isoformat()} for i,leg in enumerate(self.entry.legs)]
        self.snap={'timestamp':self.now.isoformat(),'complete':True,'option_holdings':x.holdings,'underlying_quantity':0,'open_orders_reconciled':True}
    def check(self):return reconcile(self.entry,self.reports,self.snap,as_of=self.now)
    def test_intact_and_idempotent(self):
        self.reports.append(dict(self.reports[0]));r=self.check();self.assertTrue(r['spread_valuation_allowed']);self.assertEqual(r['unique_reports'],2)
    def test_conflict(self):
        r=dict(self.reports[0]);r['cumulative_quantity']=0;self.reports.append(r)
        self.assertEqual(self.check()['state'],'reconciliation_required')
    def test_partial_leg(self):
        self.reports.pop();self.snap['option_holdings'][self.entry.legs[1].symbol]=0
        self.assertEqual(self.check()['state'],'partial')
    def test_assignment_exposure(self):
        self.snap['underlying_quantity']=-100;self.assertFalse(self.check()['spread_valuation_allowed'])
    def test_unexplained_missing_leg(self):
        self.snap['option_holdings'][self.entry.legs[1].symbol]=0
        self.assertEqual(self.check()['state'],'reconciliation_required')
    def test_full_close_no_automatic_release(self):
        for i,leg in enumerate(self.entry.legs):
            self.reports.append({'report_id':'close'+str(i),'order_id':'close'+str(i),'symbol':leg.symbol,'intent':'close','cumulative_quantity':1,'timestamp':self.now.isoformat()})
            self.snap['option_holdings'][leg.symbol]=0
        r=self.check();self.assertEqual(r['state'],'flat');self.assertFalse(r['risk_release_allowed'])
    def test_unresolved_orders(self):
        self.snap['open_orders_reconciled']=False
        self.assertEqual(self.check()['state'],'reconciliation_required')
if __name__=='__main__':unittest.main()
