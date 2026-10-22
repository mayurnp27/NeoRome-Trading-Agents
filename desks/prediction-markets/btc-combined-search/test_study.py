import unittest
import numpy as np
import pandas as pd
import study
class Tests(unittest.TestCase):
 def test_agreement_and_conflict(self):np.testing.assert_equal(study.agree(np.array([1.,-1.,0.,1.,-1.]),np.array([1.,-1.,1.,-1.,0.])),[1,-1,0,0,0])
 def test_missing_is_not_abstention(self):np.testing.assert_equal(study.agree(np.array([1.,np.nan,0.]),np.array([np.nan,1.,np.nan])),[np.nan,np.nan,np.nan])
 def test_triple_requires_all(self):np.testing.assert_equal(study.agree(np.array([1.,-1.,1.]),np.array([1.,-1.,1.]),np.array([1.,1.,0.])),[1,0,0])
 def test_no_new_events(self):
  a=np.array([0.,1.,-1.,0.]);c=study.agree(a,np.array([1.,1.,-1.,-1.]));self.assertTrue(np.all((c==0)|(c==a)))
 def test_event_alignment(self):
  ix=pd.date_range('2026-01-01',periods=2,freq='30min',tz='UTC');s=pd.Series([1.,-1.],index=ix)
  np.testing.assert_equal(study.p.align(s,pd.date_range(ix[0],periods=4,freq='15min'),True),[1,0,-1,0])
if __name__=='__main__':unittest.main()
