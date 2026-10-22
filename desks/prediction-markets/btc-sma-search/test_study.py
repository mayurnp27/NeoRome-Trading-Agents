import unittest
import numpy as np
import pandas as pd
import study
class Tests(unittest.TestCase):
 def bars(self,values):return pd.DataFrame({'close':values},index=pd.date_range('2026-01-01',periods=len(values),freq='5min',tz='UTC'))
 def test_manual_crosses(self):
  f=self.bars([5.,4.,3.,2.,1.,2.,3.,4.,3.,2.,1.]);c,s=study.signals(f,2,5)
  expected=[]
  gaps=[]
  for i in range(len(f)):
   gaps.append(sum(f.close.iloc[i-1:i+1])/2-sum(f.close.iloc[i-4:i+1])/5 if i>=4 else float('nan'))
   expected.append(float('nan') if i<5 else (1. if gaps[i]>0 and gaps[i-1]<=0 else (-1. if gaps[i]<0 and gaps[i-1]>=0 else 0.)))
  np.testing.assert_allclose(c,expected,equal_nan=True)
  self.assertEqual(c.iloc[6],1);self.assertEqual(c.iloc[9],-1)
 def test_future_invariance(self):
  f=self.bars(np.sin(np.arange(100))+10);a=study.signals(f,2,5);f.iloc[60:]*=2;b=study.signals(f,2,5)
  for x,y in zip(a,b):pd.testing.assert_series_equal(x.iloc[:60],y.iloc[:60])
 def test_gap_invalidates_full_window(self):
  f=self.bars(np.arange(20,dtype=float));f.iloc[8]=np.nan;c,s=study.signals(f,2,5)
  self.assertTrue(s.iloc[8:13].isna().all());self.assertTrue(c.iloc[8:14].isna().all())
 def test_equal_averages_are_not_events(self):
  c,s=study.signals(self.bars([10.]*20),2,5);self.assertTrue((c.dropna()==0).all())
 def test_30m_event_not_forward_filled(self):
  ix=pd.date_range('2026-01-01',periods=2,freq='30min',tz='UTC');s=pd.Series([1.,-1.],index=ix)
  np.testing.assert_array_equal(study.prior.align(s,pd.date_range(ix[0],periods=4,freq='15min'),True),[1,0,-1,0])
if __name__=='__main__':unittest.main()
