from datetime import datetime,timezone
from email.utils import format_datetime
import gzip,json,sqlite3,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from neorome import btc_bracket_feed as feed
from tests.test_btc_stochastic_paper import make_input,at,OPEN
from neorome import btc_stochastic_paper as original
from neorome import btc_fast_stochastic_paper as fast_paper
from tests.test_btc_fast_stochastic_paper import evidence as fast_input
from neorome.btc_bracket_engine import Engine

class FakeEngine:
    def __init__(self):self.actions=[]
    def on_quote(self,*a):self.actions.append(('quote',a))
    def on_settlement(self,*a):self.actions.append(('settlement',a))
    def register_cohort(self,*a):self.actions.append(('cohort',a))
    def set_entry_blocker(self,*a):self.actions.append(('blocker',a))
    def expire(self,*a):pass
    def tickers(self):return []
    def status(self,*a):return {'arms':[]}

def record(now=100.25,kind='book',ticker='KXBTC15M-26OCT160000-00',data=None):
    data=data or {'orderbook_fp':{'yes_dollars':[['0.4500','1000.00']],'no_dollars':[['0.5000','1000.00']]}}
    return {'kind':kind,'ticker':ticker,'capture_wall_ns':int(now*1e9),'capture_monotonic_ns':int(now*1e9),'http_status':200,
            'http_date':format_datetime(datetime.fromtimestamp(int(now),timezone.utc),usegmt=True),'cache_age':None,
            'response_body':json.dumps(data),'body_sha256':'c'*64,'request_duration_ms':20}

class BracketFeedTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def driver(self,root=None):
        root=self.root if root is None else Path(root)
        root.mkdir(parents=True,exist_ok=True)
        feed.atomic(root/'config.json',{'started_at':OPEN.timestamp()-1})
        feed.atomic(root/'coverage-policy.json',{'protocol':feed.PUBLICATION_PROTOCOL,'effective_at':feed.iso(OPEN.timestamp()+9).replace('+00:00','Z')})
        return feed.Driver(root,FakeEngine())
    def source(self):
        source=self.root/'original'
        pub=(int((OPEN.timestamp()+10)*1e9),123456789)
        with patch.object(original,'_publication_clock',return_value=pub):original.run_once(make_input(),source,at(10))
        return source/'btc_stochastic_paper.sqlite3'
    def source_for(self,timeframe):
        if timeframe=='30m':return self.source()
        source=self.root/'original-5m';pub=(int((OPEN.timestamp()+10)*1e9),223456789)
        with patch.object(fast_paper,'_publication_clock',return_value=pub):fast_paper.run_once(fast_input(),source,at(10))
        return source/'btc_fast_stochastic_paper.sqlite3'
    def cohorts_at(self,d,now,blocker=None,actual_now=None):
        with patch.object(feed.time,'time',return_value=now if actual_now is None else actual_now):d.cohorts(now,blocker)
    def test_raw_book_timestamp_gates_and_normalized_depth(self):
        rec=record();q=feed.decode(rec,100.3)
        self.assertEqual(q['yes_asks'],[{'price_cents':50,'quantity':1000}])
        self.assertEqual(rec['source_time_kind'],'http_response_date')
        self.assertIsNone(rec['raw_exchange_timestamps'])
        self.assertEqual(feed.decode(rec,100.3)['source_hash'],q['source_hash'])
        self.assertAlmostEqual(rec['http_date_capture_delta_ms'],250)
    def test_stale_future_lag_cache_errors_never_become_quotes(self):
        for change,now in [({'http_date':'Thu, 01 Jan 1970 00:01:35 GMT'},100.3),
                           ({'http_date':'Thu, 01 Jan 1970 00:01:41 GMT'},100.3),
                           ({},101),({'cache_age':'2'},100.3),({'http_status':429},100.3),
                           ({'transport_error':'timeout'},100.3),({'response_body':'[]'},100.3)]:
            r=record();r.update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):feed.decode(r,now)
    def test_archive_preserves_raw_records_and_rejects_symlink(self):
        a=feed.Archive(self.root);a.append(record());a.close()
        p=next((self.root/'quotes').glob('*.gz'))
        with gzip.open(p,'rt') as f:self.assertEqual(json.loads(f.readline())['response_body'],record()['response_body'])
        p.unlink();p.symlink_to(self.root/'elsewhere')
        with self.assertRaises(ValueError):feed.Archive(self.root).append(record())
    def test_stale_rejection_is_archived_and_never_sent_to_engine(self):
        d=self.driver()
        try:
            with patch.object(feed.time,'time',return_value=101):d.process(record())
            self.assertEqual(d.engine.actions,[]);self.assertEqual(d.invalid_count,1)
        finally:d.close()
        with gzip.open(next((self.root/'quotes').glob('*.gz')),'rt') as f:self.assertFalse(json.loads(f.readline())['valid'])
    def test_source_cohorts_bind_valid_original_fill_and_rowid_watermark(self):
        source=self.source()
        rows=feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11)
        self.assertEqual(len(rows),1);cohort,evidence,filled=rows[0]
        self.assertEqual(cohort['timeframe'],'30m');self.assertEqual(cohort['side'],'yes')
        self.assertEqual(feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11,evidence['fill']['source_rowid']),[])
    def test_cohort_watcher_rejects_late_fills_and_does_not_reset_clock_on_retry(self):
        source=self.source().parent
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):
                self.cohorts_at(d,OPEN.timestamp()+20);self.cohorts_at(d,OPEN.timestamp()+21)
            self.assertFalse(any(a[0]=='cohort' for a in d.engine.actions))
            self.assertEqual(d.exceptions['source_fill_observed_late'],1)
        finally:d.close()
    def test_future_execution_uses_durable_original_cohort_observation(self):
        source=self.source().parent
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            cohorts=[x[1][0] for x in d.engine.actions if x[0]=='cohort']
            self.assertEqual(len(cohorts),1);self.assertEqual(cohorts[0]['observed_at'],OPEN.timestamp()+11)
            self.assertEqual(len(list((self.root/'cohorts').glob('*.json'))),1)
        finally:d.close()
    def test_publication_age_admits_old_model_fill_but_delayed_observation_misses(self):
        source=self.source().parent;d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):
                self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(sum(a[0]=='cohort' for a in d.engine.actions),1)
            # Retry the same immutable fill after its publication receipt has aged past five seconds.
            d2=self.driver(self.root/'delayed-observation')
            try:
                with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):self.cohorts_at(d2,OPEN.timestamp()+16)
                self.assertEqual(sum(a[0]=='cohort' for a in d2.engine.actions),0)
                key=feed.source_cohorts(source/'btc_stochastic_paper.sqlite3','30m',OPEN.timestamp(),OPEN.timestamp()+16)[0][0]['cohort_id']
                self.assertEqual(d2.seen[key]['reason'],'source_fill_observed_late')
            finally:d2.close()
        finally:d.close()
    def test_missing_publication_retries_without_advancing_watermark_and_restart_preserves_first_seen(self):
        source=self.source();rows=feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11)
        event_id='fill-publication-'+rows[0][1]['fill']['proposal_id'];cohort_key=rows[0][0]['cohort_id']
        receipt=source.parent/'audit'/(feed.hashlib.sha256(event_id.encode()).hexdigest()+'.json');saved=receipt.read_bytes();receipt.unlink()
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(d.seen['_watermarks'].get('30m',0),0)
            pending=d.pending_publications[cohort_key]
            first_seen=pending['first_seen_at']
        finally:d.close()
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+12)
            self.assertEqual(d.pending_publications[cohort_key]['first_seen_at'],first_seen)
            self.assertEqual(d.seen['_watermarks'].get('30m',0),0)
            receipt.write_bytes(saved)
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+13)
            self.assertEqual(d.seen['_watermarks']['30m'],1)
            self.assertEqual(sum(a[0]=='cohort' for a in d.engine.actions),1)
        finally:d.close()
    def test_missing_publication_expires_at_signal_deadline_and_clears_pending(self):
        source=self.source();rows=feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11)
        key=rows[0][0]['cohort_id'];event_id='fill-publication-'+rows[0][1]['fill']['proposal_id']
        receipt=source.parent/'audit'/(feed.hashlib.sha256(event_id.encode()).hexdigest()+'.json');receipt.unlink()
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+91)
            self.assertNotIn(key,d.pending_publications)
            self.assertEqual(d.seen[key]['reason'],'fill_publication_missing_expired')
            self.assertEqual(d.seen['_watermarks']['30m'],1)
        finally:d.close()
    def test_tampered_or_future_publication_receipt_fails_closed(self):
        source=self.source();rows=feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11)
        event_id='fill-publication-'+rows[0][1]['fill']['proposal_id'];path=source.parent/'audit'/(feed.hashlib.sha256(event_id.encode()).hexdigest()+'.json');saved=json.loads(path.read_text());event=path
        d=self.driver()
        try:
            bad=dict(saved);bad['payload']=dict(saved['payload']);bad['payload']['capture_wall_ns']+=1_000_000_000
            event.write_text(feed.canonical(bad))
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            key=rows[0][0]['cohort_id'];self.assertEqual(d.seen[key]['reason'],'source_publication_invalid')
            self.assertFalse(any(a[0]=='cohort' for a in d.engine.actions))
            event.write_text(feed.canonical(saved))
            d.seen.pop(key);d.seen['_watermarks'].pop('30m',None);(self.root/'source-cursors.json').unlink()
            d.seen['_watermarks']={};d.pending_publications={}
            future=dict(saved);future['payload']=dict(saved['payload']);future['payload']['published_at']=feed.iso(OPEN.timestamp()+20).replace('+00:00','Z')
            future['at']=future['payload']['published_at'];future['payload']['capture_wall_ns']=int((OPEN.timestamp()+20)*1e9)
            body={k:future[k] for k in ('event_id','event_type','at','payload')};future['event_sha256']=feed.digest(body);event.write_text(feed.canonical(future))
            # Direct validation raises; the scanner records a per-cohort miss.
            fill=rows[0][1]['fill']
            with self.assertRaisesRegex(ValueError,'clock_out_of_range'):feed.source_publication(source,fill,OPEN.timestamp()+11)
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(d.seen[key]['reason'],'source_publication_invalid')
        finally:d.close()
    def test_receipt_published_after_tick_start_uses_post_read_observation_clock(self):
        source=self.root/'clock-race-source';pub=(int((OPEN.timestamp()+10.5)*1e9),223456789)
        with patch.object(original,'_publication_clock',return_value=pub):original.run_once(make_input(),source,at(10))
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):
                self.cohorts_at(d,OPEN.timestamp()+10,None,actual_now=OPEN.timestamp()+10.6)
            cohort=[action[1][0] for action in d.engine.actions if action[0]=='cohort'][0]
            self.assertAlmostEqual(cohort['observed_at'],OPEN.timestamp()+10.6)
        finally:d.close()
    def test_invalid_receipt_is_recorded_and_does_not_starve_later_valid_row(self):
        source=self.source();original_row=feed.source_cohorts(source,'30m',OPEN.timestamp(),OPEN.timestamp()+11)[0]
        cohort1,evidence1,filled=original_row;cohort2=json.loads(json.dumps(cohort1));evidence2=json.loads(json.dumps(evidence1))
        cohort2['cohort_id']='30m:later-valid-fill';cohort2['source_hash']='b'*64
        evidence1['publication_error']='source_publication_clock_incoherent'
        evidence2['fill']['source_rowid']=2;evidence2['fill']['fill_id']='later-valid-fill';evidence2['new_cohort']=cohort2
        evidence2['publication']={'published_at':feed.iso(OPEN.timestamp()+10).replace('+00:00','Z'),'observed_at':OPEN.timestamp()+11}
        d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source},clear=True),patch.object(feed,'source_cohorts',return_value=[(cohort1,evidence1,filled),(cohort2,evidence2,filled)]):
                self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(d.seen[cohort1['cohort_id']]['reason'],'source_publication_invalid')
            self.assertIsNone(d.seen[cohort2['cohort_id']]['reason'])
            self.assertEqual(d.seen['_watermarks']['30m'],2)
            self.assertEqual(sum(a[0]=='cohort' for a in d.engine.actions),1)
        finally:d.close()
    def test_policy_is_required_and_coverage_reports_all_arms_after_admission(self):
        source=self.source().parent;d=self.driver();(self.root/'coverage-policy.json').unlink()
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True),self.assertRaisesRegex(ValueError,'coverage_policy_missing'):
                self.cohorts_at(d,OPEN.timestamp()+11)
            feed.atomic(self.root/'coverage-policy.json',{'protocol':feed.PUBLICATION_PROTOCOL,'effective_at':feed.iso(OPEN.timestamp()+9).replace('+00:00','Z')})
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):coverage=d.coverage_status()
            self.assertEqual(coverage['expected_source_cohorts'],1);self.assertEqual(coverage['admitted'],1)
            arms=coverage['cohorts'][0]['arm_decisions'];self.assertEqual(len(arms),11)
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True):d.publish()
            status=json.loads((self.root/'status.json').read_text())
            self.assertTrue(status['source_fill_coverage']['reconciled'])
            self.assertIn('repair cohorts expected/admitted/missed/pending',(self.root/'comparison.html').read_text())
        finally:d.close()
    def test_both_source_timeframes_enter_all_eleven_arms_from_two_future_quotes_at_twenty_percent(self):
        root=self.root/'engine-backed';root.mkdir();started=OPEN.timestamp()-1
        feed.atomic(root/'config.json',{'started_at':started})
        feed.atomic(root/'coverage-policy.json',{'protocol':feed.PUBLICATION_PROTOCOL,'effective_at':feed.iso(OPEN.timestamp()+9).replace('+00:00','Z')})
        engine=Engine(root/'comparison.sqlite3',started_at=started);d=feed.Driver(root,engine)
        sources={'5m':self.source_for('5m'),'30m':self.source_for('30m')}
        try:
            with patch.dict(feed.SOURCES,sources,clear=True):self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(engine.status(OPEN.timestamp()+11)['counts']['waiting'],2)
            def quote(t,label):
                return {'received_at':t,'source_hash':feed.hashlib.sha256(label.encode()).hexdigest(),
                    'yes_bids':[{'price_cents':40,'quantity':2000}],'yes_asks':[{'price_cents':40,'quantity':2000}],
                    'no_bids':[{'price_cents':40,'quantity':2000}],'no_asks':[{'price_cents':40,'quantity':2000}]}
            ticker='KXBTC15M-26OCT141200'
            engine.on_quote(ticker,quote(OPEN.timestamp()+12,'first-future'))
            engine.on_quote(ticker,quote(OPEN.timestamp()+14,'second-future'))
            status=engine.status(OPEN.timestamp()+14)
            entries=engine.db.execute("select arm_id,cohort_id,payload from events where kind='entry' order by arm_id").fetchall()
            self.assertEqual(len(entries),22)
            by_tf={'5m':0,'30m':0}
            for row in entries:
                event=json.loads(row['payload']);tf=row['arm_id'].split('-',1)[0];by_tf[tf]+=1
                self.assertEqual(event['allocation_bps'],2000)
                self.assertLessEqual(event['cost_cents'],10000)
            self.assertEqual(by_tf,{'5m':11,'30m':11})
            self.assertTrue(all(arm['position']['allocation_bps']==2000 for arm in status['arms'] if arm['position']))
            with patch.dict(feed.SOURCES,sources,clear=True):coverage=d.coverage_status()
            self.assertEqual(coverage['original_source_fills_since_experiment_start'],{'5m':1,'30m':1})
            self.assertEqual((coverage['expected_source_cohorts'],coverage['admitted'],coverage['missed'],coverage['pending']),(2,2,0,0))
            self.assertTrue(coverage['reconciled'])
            self.assertTrue(all(len(item['arm_decisions'])==11 and {arm['status'] for arm in item['arm_decisions']}=={'entered'}
                                for item in coverage['cohorts']))
        finally:d.close()
    def test_coverage_reconciled_requires_every_original_fill_classified(self):
        source=self.source();d=self.driver()
        try:
            with patch.dict(feed.SOURCES,{'30m':source},clear=True):
                coverage=d.coverage_status()
                self.assertTrue(coverage['admission_reconciled'])
                self.assertFalse(coverage['reconciled'])
                self.assertEqual(coverage['unclassified_source_fills'],{'30m':1})
                self.cohorts_at(d,OPEN.timestamp()+11)
                self.assertTrue(d.coverage_status()['reconciled'])
        finally:d.close()

    def test_budget_reserved_before_io_and_persists_without_restart_refill(self):
        d=self.driver()
        try:d.reserve_requests(3);self.assertEqual(json.loads((self.root/'request-budget.json').read_text())['requests'],3)
        finally:d.close()
        d=self.driver()
        try:
            self.assertEqual(d.budget['requests'],3);d.budget['requests']=feed.MAX_REQUESTS_PER_DAY
            with self.assertRaises(RuntimeError):d.reserve_requests(1)
        finally:d.close()
    def test_official_settlement_requires_matching_identity_and_close(self):
        d=self.driver();ticker='KXBTC15M-26OCT160000-00';d.markets[ticker]={'close_time':feed.iso(90)}
        try:
            rec=record(kind='market',data={'market':{'ticker':ticker,'exchange_index':2,'close_time':feed.iso(90),'status':'finalized','result':'yes'}})
            with patch.object(feed.time,'time',return_value=100.3):d.process(rec)
            self.assertEqual(d.engine.actions[0][0],'settlement')
            rec=record(kind='market',data={'market':{'ticker':ticker,'exchange_index':2,'close_time':feed.iso(91),'status':'finalized','result':'yes'}})
            # Previous matching identity was90; replacement must not silently redefine settlement time.
            d.markets[ticker]={'close_time':feed.iso(90)}
            with patch.object(feed.time,'time',return_value=100.3),self.assertRaises(ValueError):d.process(rec)
        finally:d.close()
    def test_rejected_final_and_nonfinal_metadata_never_rebind_close_on_retry(self):
        d=self.driver();ticker='KXBTC15M-26OCT160000-00';d.markets[ticker]={'close_time':feed.iso(90)}
        try:
            for status in ('open','finalized','finalized'):
                rec=record(kind='market',data={'market':{'ticker':ticker,'exchange_index':2,'close_time':feed.iso(91),'status':status,'result':'yes'}})
                with patch.object(feed.time,'time',return_value=100.3),self.assertRaises(ValueError):d.process(rec)
                self.assertEqual(d.markets[ticker]['close_time'],feed.iso(90))
            self.assertFalse(d.engine.actions)
        finally:d.close()

    def test_source_cohort_cannot_replace_discovered_close_binding(self):
        source=self.source().parent
        d=self.driver();d.markets['KXBTC15M-26OCT141200']={'close_time':at(901)}
        try:
            with patch.dict(feed.SOURCES,{'30m':source/'btc_stochastic_paper.sqlite3'},clear=True),self.assertRaisesRegex(ValueError,'close_conflict'):
                self.cohorts_at(d,OPEN.timestamp()+11)
            self.assertEqual(d.markets['KXBTC15M-26OCT141200']['close_time'],at(901))
            self.assertFalse(d.engine.actions)
        finally:d.close()

    def test_data_only_process_never_calls_paper_quote_or_settlement_methods(self):
        feed.atomic(self.root/'config.json',{'started_at':0})
        engine=FakeEngine();d=feed.Driver(self.root,engine,data_only=True)
        ticker='KXBTC15M-26OCT160000-00';d.markets[ticker]={'close_time':feed.iso(90)}
        try:
            with patch.object(feed.time,'time',return_value=100.3):
                d.process(record())
                d.process(record(kind='market',data={'market':{'ticker':ticker,'exchange_index':2,'close_time':feed.iso(90),'status':'finalized','result':'yes'}}))
            self.assertEqual(engine.actions,[]);self.assertEqual(d.engine_quote_count,0)
        finally:d.close()

    def test_data_only_default_root_alias_is_rejected_before_initialization(self):
        alias=str(self.root/'live'/'..'/'live')
        with patch.object(feed,'ROOT',self.root/'live'),patch('sys.argv',['feed','--root',alias,'--data-only']),self.assertRaisesRegex(ValueError,'separate preflight'):
            feed.main()
        self.assertFalse((self.root/'live').exists())

    def test_readonly_wal_reader_fallback_copies_and_invalidates_on_source_change(self):
        path=self.root/'source.sqlite3';writer=sqlite3.connect(path);writer.execute('pragma journal_mode=WAL');writer.execute('create table t(value integer)');writer.execute('insert into t values (1)');writer.commit()
        real_connect=sqlite3.connect
        def no_sidecar_connect(target,*a,**k):
            if isinstance(target,str) and target.startswith('file:'):
                e=sqlite3.OperationalError('unable to open database file');e.sqlite_errorcode=sqlite3.SQLITE_CANTOPEN;raise e
            return real_connect(target,*a,**k)
        try:
            with patch.object(feed.sqlite3,'connect',side_effect=no_sidecar_connect):
                copy,owned=feed.source_connection(path);self.assertFalse(owned);self.assertEqual(copy.execute('select value from t').fetchall(),[(1,)])
                again,_=feed.source_connection(path);self.assertIs(copy,again)
                writer.execute('insert into t values (2)');writer.commit()
                updated,_=feed.source_connection(path);self.assertEqual(updated.execute('select value from t').fetchall(),[(1,),(2,)])
            self.assertEqual(writer.execute('select value from t').fetchall(),[(1,),(2,)])
        finally:
            writer.close();cached=feed._SOURCE_CACHE.pop(str(path),None)
            if cached:cached[2].close();cached[1].cleanup()

    def test_global_stop_and_recovery_pause_only_new_risk(self):
        stop=self.root/'STOP';recovery=self.root/'recovery.json'
        feed.atomic(recovery,{'updated_at':feed.iso(100),'operational_healthy':True})
        with patch.object(feed,'GLOBAL_STOP',stop),patch.object(feed,'RECOVERY',recovery):
            self.assertIsNone(feed.entry_health(101));stop.touch();self.assertEqual(feed.entry_health(101),'global_manual_stop')
            stop.unlink();self.assertEqual(feed.entry_health(8000),'recovery_not_verified')

    def test_wal_failure_during_join_retries_private_snapshot(self):
        path=self.source()
        real=feed.source_connection;calls=[]
        class SchemaReadableRowsUnavailable:
            row_factory=None
            closed=False
            def execute(self,sql,*args):
                if sql.startswith('select f.rowid'):
                    exc=sqlite3.OperationalError('unable to open database file');exc.sqlite_errorcode=sqlite3.SQLITE_CANTOPEN;raise exc
                return self
            def close(self):self.closed=True
        direct=SchemaReadableRowsUnavailable()
        def connect(p,*,force_snapshot=False):
            calls.append(force_snapshot)
            return real(p,force_snapshot=True) if force_snapshot else (direct,True)
        try:
            with patch.object(feed,'source_connection',side_effect=connect):
                rows=feed.source_cohorts(path,'30m',OPEN.timestamp()-1,OPEN.timestamp()+12)
            self.assertEqual(len(rows),1);self.assertEqual(calls,[False,True]);self.assertTrue(direct.closed)
        finally:
            cached=feed._SOURCE_CACHE.pop(str(path),None)
            if cached:cached[2].close();cached[1].cleanup()
    def test_source_fill_count_retries_private_snapshot_on_read_failure(self):
        path=self.source();real=feed.source_connection;calls=[]
        class DirectReadFails:
            closed=False
            def execute(self,sql,*args):
                if sql=='pragma query_only=ON':return self
                exc=sqlite3.OperationalError('unable to open database file');exc.sqlite_errorcode=sqlite3.SQLITE_CANTOPEN;raise exc
            def close(self):self.closed=True
        direct=DirectReadFails()
        def connect(p,*,force_snapshot=False):
            calls.append(force_snapshot)
            return real(p,force_snapshot=True) if force_snapshot else (direct,True)
        with patch.object(feed,'source_connection',side_effect=connect):
            self.assertEqual(feed.source_fill_count(path,OPEN.timestamp()),1)
        self.assertEqual(calls,[False,True]);self.assertTrue(direct.closed)
if __name__=='__main__':unittest.main()
