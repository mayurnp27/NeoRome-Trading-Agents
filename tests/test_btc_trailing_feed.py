from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
import gzip,json,tempfile,unittest
from neorome import btc_trailing_feed as feed
from neorome import btc_bracket_feed as shared
from neorome.btc_trailing_engine import TrailingEngine
from tests.test_btc_bracket_feed import record
from tests.test_btc_fast_stochastic_paper import evidence as fast_input,OPEN
from tests.test_btc_stochastic_paper import make_input as slow_input,at


class TrailingFeedTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
    def tearDown(self):self.tmp.cleanup()
    def file(self,now):return self.root/(datetime.fromtimestamp(int(now//300)*300,timezone.utc).strftime('%Y%m%dT%H%MZ')+'.jsonl.gz')
    def test_follows_flushes_unfinished_member_and_concatenated_restart_members(self):
        now=100.25;p=self.file(now);f=gzip.open(p,'ab');f.write((json.dumps(record(now))+'\n').encode());f.flush()
        follower=feed.GzipFollower(self.root)
        self.assertEqual(len(follower.poll(now)),1);self.assertEqual(follower.poll(now),[])
        f.write((json.dumps(record(now+1))+'\n').encode());f.flush()
        self.assertEqual(len(follower.poll(now+1)),1);f.close()
        with gzip.open(p,'ab') as f:f.write((json.dumps(record(now+2))+'\n').encode())
        self.assertEqual(len(follower.poll(now+2)),1);self.assertEqual(follower.poll(now+2),[])
    def test_rotation_reads_both_slots_once_and_rejects_truncation(self):
        for now in (299.25,300.25):
            with gzip.open(self.file(now),'ab') as f:f.write((json.dumps(record(now))+'\n').encode())
        follower=feed.GzipFollower(self.root);self.assertEqual(len(follower.poll(300.5)),2)
        self.assertEqual(follower.poll(300.6),[])
        self.file(300.25).write_bytes(b'')
        with self.assertRaisesRegex(RuntimeError,'truncated'):follower.poll(300.7)
    def test_archive_symlinks_and_existing_account_roots_refused(self):
        self.file(100).symlink_to(self.root/'other')
        # Existing symlink targets are refused even if the link points elsewhere.
        (self.root/'other').write_bytes(b'')
        with self.assertRaises(ValueError):feed.GzipFollower(self.root).poll(100)
        for root in [feed.SHARED,feed.SHARED/'child',Path('/var/lib'),next(iter(shared.SOURCES.values())).parent]:
            with self.assertRaisesRegex(ValueError,'state roots refused'):feed.initialize(root)
    def driver(self):
        root=self.root/'trailing'
        with patch.object(feed.time,'time',return_value=OPEN.timestamp()):config=feed.initialize(root)
        e=TrailingEngine(root/'trailing.sqlite3',started_at=config['started_at'])
        d=feed.TrailingDriver(root,e,archive_root=self.root/'shared-quotes');return d,e
    def saved(self,t,**kwargs):
        r=record(t,**kwargs);shared.decode(r,t);r['valid']=True;return r
    def consume(self,d,r,now):
        with patch.object(feed.time,'time',return_value=now):d.consume(r,'shared-archive',now=now)
    def test_causal_consumer_preserves_original_hash_rejects_stale_and_duplicate(self):
        d,e=self.driver();t=OPEN.timestamp()+1.25;r=self.saved(t);h=r['source_hash']
        try:
            self.consume(d,r,t+.1);self.assertEqual(e.db.execute('select source_hash from quotes').fetchone()[0],h)
            self.consume(d,r,t+.2);self.assertEqual(d.engine_quote_count,1)
            self.consume(d,self.saved(t+1),t+2);self.assertEqual(d.engine_quote_count,1)
            self.assertEqual(d.shared_skipped['consumer_lag_exceeded'],1)
            before=self.saved(OPEN.timestamp()-.1);self.consume(d,before,OPEN.timestamp())
            self.assertEqual(d.shared_skipped['before_experiment_start'],1)
        finally:d.close();e.close()
    def test_post_archive_delay_rejects_book_before_any_execution(self):
        d,e=self.driver();t=OPEN.timestamp()+1.25;r=self.saved(t)
        try:
            with patch.object(feed.time,'time',return_value=t+.7):d.consume(r,'shared',now=t+.1)
            self.assertEqual(e.db.execute('select count(*) from quotes').fetchone()[0],0)
        finally:d.close();e.close()
    def test_original_publications_create_two_prospective_entries_and_coverage(self):
        d,e=self.driver();sources={};tickers=set()
        try:
            for tf,module,data,db in [('5m',shared.fast,fast_input(),'btc_fast_stochastic_paper.sqlite3'),('30m',shared.slow,slow_input(),'btc_stochastic_paper.sqlite3')]:
                p=self.root/tf
                with patch.object(module,'_publication_clock',return_value=(int((OPEN.timestamp()+10)*1e9),123456789)):
                    result=module.run_once(data,p,at(10))
                self.assertEqual(result['status'],'filled');sources[tf]=p/db;tickers.add(result['fill']['instrument'])
            with patch.dict(shared.SOURCES,sources,clear=True),patch.object(feed.time,'time',return_value=OPEN.timestamp()+11):d.cohorts(OPEN.timestamp()+11,None)
            for t in [OPEN.timestamp()+12.25,OPEN.timestamp()+14.25]:
                for ticker in tickers:self.consume(d,self.saved(t,ticker=ticker),t+.1)
            entries=e.db.execute("select payload from events where kind='entry'").fetchall();self.assertEqual(len(entries),2)
            self.assertTrue(all(json.loads(x[0])['cost_cents']<=10000 for x in entries))
            with patch.dict(shared.SOURCES,sources,clear=True):coverage=d.coverage_status()
            self.assertTrue(coverage['reconciled']);self.assertEqual(coverage['entered'],2);self.assertEqual(coverage['pending'],0)
            # A restart cannot duplicate source entries or reset any original ledger.
            prior={tf:p.read_bytes() for tf,p in sources.items()}
            d.close();d=feed.TrailingDriver(d.root,e,archive_root=self.root/'shared-quotes')
            with patch.dict(shared.SOURCES,sources,clear=True),patch.object(feed.time,'time',return_value=OPEN.timestamp()+15):d.cohorts(OPEN.timestamp()+15,None)
            self.assertEqual(len(e.db.execute("select * from events where kind='entry'").fetchall()),2)
            self.assertEqual(prior,{tf:p.read_bytes() for tf,p in sources.items()})
        finally:d.close();e.close()
    def test_settlement_requires_bound_identity_and_does_not_request_books(self):
        d,e=self.driver();t=OPEN.timestamp()+100
        try:
            c={'cohort_id':'30m:x','timeframe':'30m','ticker':'KXBTC15M-26OCT160000-00','side':'yes','observed_at':OPEN.timestamp()+1,'signal_time':OPEN.timestamp(),'close_time':t,'fee_multiplier':'1','source_hash':'a'*64}
            e.register_cohort(c);d.markets[c['ticker']]={'ticker':c['ticker'],'close_time':shared.iso(t)}
            data={'market':{'ticker':c['ticker'],'exchange_index':2,'close_time':shared.iso(t),'status':'settled','result':'no'}}
            r=record(t+.25,kind='market',data=data)
            with patch.object(feed.time,'time',return_value=t+.3):d.consume(r,now=t+.3)
            self.assertEqual(e.db.execute('select result from settlements').fetchone()[0],'no')
            bad=record(t+1.25,kind='market',data={'market':dict(data['market'],close_time=shared.iso(t+1))})
            with self.assertRaisesRegex(ValueError,'identity_or_close'):d.consume(bad,now=t+1.3)
        finally:d.close();e.close()
    def test_resume_configuration_is_frozen(self):
        root=self.root/'new';config=feed.initialize(root);self.assertEqual(feed.initialize(root),config)
        config['allocation_bps']=5000;shared.atomic(root/'config.json',config)
        with self.assertRaisesRegex(ValueError,'configuration mismatch'):feed.initialize(root)

if __name__=='__main__':unittest.main()
