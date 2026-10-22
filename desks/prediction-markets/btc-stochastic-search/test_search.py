import unittest
import numpy as np
import pandas as pd
import search

class ResearchTests(unittest.TestCase):
    def bars(self):
        idx=pd.date_range('2026-01-01',periods=150,freq='5min',tz='UTC')
        c=100+np.sin(np.arange(150)/3)*5+np.arange(150)/10
        return pd.DataFrame({'open':c,'high':c+2,'low':c-2,'close':c,'volume':1},index=idx)
    def test_aggregation_closes_after_inputs(self):
        f=self.bars(); a=search.aggregate(f,15)
        self.assertEqual(a.index[0],f.index[0]+pd.Timedelta(minutes=15))
        self.assertEqual(a.close.iloc[0],f.close.iloc[2])
        self.assertEqual(a.high.iloc[0],f.high.iloc[:3].max())
    def test_missing_candle_invalidates_aggregate(self):
        f=self.bars(); f.iloc[1]=np.nan
        self.assertTrue(search.aggregate(f,15).iloc[0].isna().all())
    def test_future_mutation_cannot_change_signal(self):
        f=self.bars(); before=search.features(search.aggregate(f,30),5,2,2)
        cutoff=f.index[100]; f.loc[f.index>=cutoff,['open','high','low','close']]*=2
        after=search.features(search.aggregate(f,30),5,2,2)
        for k in before: pd.testing.assert_series_equal(before[k].loc[:cutoff],after[k].loc[:cutoff])
    def test_30m_asof_alignment_and_event_not_reused(self):
        ix=pd.date_range('2026-01-01',periods=2,freq='30min',tz='UTC')
        s=pd.Series([1.,-1.],index=ix); target=pd.date_range(ix[0],periods=4,freq='15min')
        np.testing.assert_array_equal(search.align(s,target),[1,1,-1,-1])
        np.testing.assert_array_equal(search.align(s,target,True),[1,0,-1,0])
    def test_flat_range_is_missing(self):
        f=self.bars(); f[['open','high','low','close']]=100
        for s in search.features(search.aggregate(f,5),5,1,1).values(): self.assertTrue(s.isna().all())
    def test_hand_computed_fast_stochastic(self):
        f=self.bars(); a=search.aggregate(f,5); out=search.features(a,5,1,1)
        i=10; k=(a.close.iloc[i]-a.low.iloc[i-4:i+1].min())/(a.high.iloc[i-4:i+1].max()-a.low.iloc[i-4:i+1].min())
        self.assertAlmostEqual(out['level'].iloc[i],2*k-1)
    def test_metrics_abstention_and_correlation(self):
        m=search.metrics(np.array([1.,-1.,0.,np.nan]),np.array([1.,-1.,1.,1.]))
        self.assertEqual(m['active'],2); self.assertEqual(m['accuracy'],1); self.assertEqual(m['n'],3)

if __name__=='__main__': unittest.main()
