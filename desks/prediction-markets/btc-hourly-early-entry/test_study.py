import unittest
import numpy as np
import pandas as pd
from study import aggregate,align_matrix,metrics,stoch_values,cent_sums
class TimingAndScoring(unittest.TestCase):
 def raw(self):
  idx=pd.date_range('2026-04-10',periods=60,freq='5min',tz='UTC');x=np.arange(60,dtype=float)+100
  return pd.DataFrame({'open':x,'high':x+1,'low':x-1,'close':x+.5,'volume':1},index=idx)
 def test_45_minute_alignment_and_only_complete_bars(self):
  f=self.raw();b=aggregate(f,45)
  self.assertEqual(b.index[0],pd.Timestamp('2026-04-10T00:45Z'))
  self.assertEqual(b.index[1],pd.Timestamp('2026-04-10T01:30Z'))
  self.assertEqual(b.close.iloc[0],108.5);self.assertEqual(b.close.iloc[1],117.5)
  f.loc[f.index[10],'close']=np.nan;self.assertTrue(np.isnan(aggregate(f,45).close.iloc[1]))
 def test_no_future_candle_and_fresh_event(self):
  b=aggregate(self.raw(),60);ii=pd.DatetimeIndex(['2026-04-10T01:00Z','2026-04-10T02:00Z']);values=np.arange(len(b),dtype=float)+1
  state=align_matrix(values,b,ii,False);event=align_matrix(values,b,ii,True)
  np.testing.assert_array_equal(state,np.tile([1,2],(3,1)))
  np.testing.assert_array_equal(event[0],[1,2]);np.testing.assert_array_equal(event[1:],0)
 def test_missing_is_not_an_active_direction(self):
  x=np.array([1.,0.,-1.,np.nan]);y=np.array([1.,1.,1.,-1.]);m=metrics(x,y)
  self.assertEqual(m['active'],2);self.assertEqual(m['wins'],1);self.assertEqual(m['valid'],3);self.assertEqual(m['coverage'],.5)
 def test_correlation_matches_independent_numpy(self):
  x=np.array([1.,0.,-1.,-1.,1.,np.nan]);y=np.array([1.,1.,-1.,1.,-1.,-1.]);keep=np.isfinite(x)
  self.assertAlmostEqual(metrics(x,y)['correlation'],np.corrcoef(x[keep],y[keep])[0,1])
  self.assertAlmostEqual(metrics(-x,y)['correlation'],-metrics(x,y)['correlation'])
 def test_perturb_future_candles_does_not_change_signal(self):
  f=self.raw();a=stoch_values(f,5,2,3);changed=f.copy();changed.loc[changed.index[30]:,['high','low','close']]+=500;b=stoch_values(changed,5,2,3)
  for key in a:np.testing.assert_array_equal(a[key][:30],b[key][:30])
 def test_equal_smas_have_exact_zero_gap(self):
  f=self.raw();f.loc[f.index[-3:],'close']=[75.01,75.02,75.00];s=cent_sums(f)
  self.assertEqual(s[2][-1]*3-s[3][-1]*2,0.)
 def test_long_ma_gap_is_not_compressed(self):
  f=self.raw();f.loc[f.index[20],:]=np.nan;b=aggregate(f,15);ma=b.close.rolling(5).mean()
  self.assertTrue(ma.iloc[6:11].isna().all());self.assertTrue(np.isfinite(ma.iloc[11]))
class EarlyDeadline(unittest.TestCase):
 def test_29_minute_signal_cannot_see_30_minute_bar(self):
  f=TimingAndScoring().raw();b=aggregate(f,30);index=pd.DatetimeIndex(['2026-04-10T01:00Z']);v=np.arange(len(b),dtype=float)
  actual=align_matrix(v,b,index,False)
  self.assertEqual(actual[2,0],actual[0,0])
  self.assertEqual(align_matrix(v,b,index,True)[2,0],0)
 def test_all_decision_offsets_leave_at_least_30_minutes_after_delay(self):
  from study import OFFSETS
  self.assertEqual(OFFSETS,(0,15,29))
  for off in OFFSETS:self.assertGreaterEqual(60-(off+1),30)
if __name__=='__main__':unittest.main()
