import json
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import server


class ProjectionTests(unittest.TestCase):
    def incident_fixture(self):
        now = datetime(2026, 10, 18, 4, 30, tzinfo=timezone.utc)
        unit = 'neorome-bcp3-child-freshness-r2@130-1559076-997.service'
        report = 'neorome-control-report.service'
        maintenance = {'as_of': now.isoformat(), 'status': 'degraded',
            'issues': [{'key': 'unit:' + unit, 'reason': 'unit_failed', 'episode_id': 'preserved'},
                       {'key': 'unit:' + report, 'reason': 'unit_failed'},
                       {'key': 'source:memecoin_report_l6', 'reason': 'status_file_stale'}],
            'units': {server.BCP3_PARENT: {'ActiveState': 'active', 'MainPID': '1711426'},
                      unit: {'ActiveState': 'failed', 'MainPID': '0',
                             'ExecMainExitTimestamp': 'Sat 2026-10-17 00:16:11 UTC'},
                      report: {'ActiveState': 'failed', 'MainPID': '0',
                               'ExecMainExitTimestamp': 'Fri 2026-10-16 22:00:56 UTC'}}}
        return now, unit, maintenance

    def test_historical_child_is_retained_without_resolving_raw_incident(self):
        now, unit, maintenance = self.incident_fixture()
        before = deepcopy(maintenance)
        result = server.incident_groups(maintenance, [], now)
        self.assertEqual([x['key'] for x in result['historical']], ['unit:' + unit])
        self.assertEqual(len(result['current']), 2)
        self.assertEqual(result['historical'][0]['episode_id'], 'preserved')
        self.assertIn('does not mean its cause is resolved', result['historical'][0]['classification_detail'])
        self.assertEqual(maintenance, before)

    def test_recent_or_future_child_failure_stays_current(self):
        now, unit, maintenance = self.incident_fixture()
        for delta in [timedelta(minutes=-59), timedelta(seconds=1)]:
            with self.subTest(delta=delta):
                maintenance['units'][unit]['ExecMainExitTimestamp'] = (now + delta).isoformat()
                result = server.incident_groups(maintenance, [], now)
                self.assertFalse(result['historical'])
                self.assertEqual(len(result['current']), 3)

    def test_unknown_stale_or_nonterminal_evidence_never_hides_issue(self):
        now, unit, original = self.incident_fixture()
        variants = []
        for key in ['ExecMainExitTimestamp', 'MainPID', 'ActiveState']:
            value = deepcopy(original); del value['units'][unit][key]; variants.append(value)
        value = deepcopy(original); value['units'][unit]['MainPID'] = '42'; variants.append(value)
        value = deepcopy(original); value['units'][unit]['ExecMainExitTimestamp'] = 'unknown'; variants.append(value)
        value = deepcopy(original); value['units'][server.BCP3_PARENT]['ActiveState'] = 'failed'; variants.append(value)
        value = deepcopy(original); value['units'][server.BCP3_PARENT]['MainPID'] = '0'; variants.append(value)
        value = deepcopy(original); value['units'][server.BCP3_PARENT]['MainPID'] = '²'; variants.append(value)
        value = deepcopy(original); value['status'] = 'stale'; variants.append(value)
        value = deepcopy(original); value['as_of'] = (now - timedelta(seconds=31)).isoformat(); variants.append(value)
        value = deepcopy(original); value['as_of'] = (now + timedelta(seconds=6)).isoformat(); variants.append(value)
        value = deepcopy(original); del value['units']; variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                result = server.incident_groups(value, [], now)
                self.assertFalse(result['historical'])
                self.assertEqual(len(result['current']), 3)

    def test_persistent_reports_sources_and_snapshot_errors_always_current(self):
        now, unit, maintenance = self.incident_fixture()
        result = server.incident_groups(maintenance, ['book: coverage incomplete'], now)
        self.assertEqual(len(result['current']), 3)
        self.assertEqual(result['current'][-1]['key'], 'snapshot-error-1')
        self.assertEqual(result['snapshot_error_count'], 1)
        self.assertEqual(result['raw_issue_count'], 3)

    def test_missing_or_invalid_incident_list_is_not_reported_healthy(self):
        now, _, maintenance = self.incident_fixture()
        maintenance['issues'] = None
        self.assertEqual(server.incident_groups(maintenance, [], now)['current'][0]['reason'], 'incident_list_unavailable')
        maintenance['issues'] = [None]
        self.assertEqual(server.incident_groups(maintenance, [], now)['current'][0]['reason'], 'incident_record_invalid')

    def test_only_exact_child_template_is_historical(self):
        now, unit, maintenance = self.incident_fixture()
        for other in [unit + '.extra', 'neorome-unrelated@130-1559076-997.service']:
            value = deepcopy(maintenance)
            value['issues'][0]['key'] = 'unit:' + other
            value['units'][other] = value['units'][unit]
            self.assertFalse(server.incident_groups(value, [], now)['historical'])

    def test_html_has_three_separate_regions_and_closed_history(self):
        html = Path(server.__file__).with_name('index.html').read_text()
        for heading in ['Current issues', 'Historical failures', 'Data notes']:
            self.assertIn('<h3>' + heading + '</h3>', html)
        self.assertIn('<details id="historical-details">', html)
        self.assertNotIn('<details id="historical-details" open', html)
        self.assertIn('raw observer', html)
        self.assertNotIn('Incidents &amp; data notes', html)

    def test_existing_volume_experiment_books_remain_registered(self):
        for suffix in ('e1v100','e1v150'):
            self.assertIn('neorome-memecoin-program/paper-'+suffix+'.sqlite3',server.BOOKS)

    def test_bcp3_source_uses_committed_epoch_not_old_template_or_service_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();book=root/'neorome-bcp3-paper';book.mkdir()
            (book/'ledger.json').write_text(json.dumps({'policySha256':'a'*64}))
            activation=root/'activation.json'
            value={'schema':'neorome-bcp3-chain-source-activation-v1','state':'committed','asOf':'2026-10-16T17:40:00Z',
                'newPolicySha256':'a'*64,'sourceConfigSha256':'b'*64,
                'activeDiscoverySource':{'provider':'Helius','endpoint':'https://mainnet.helius-rpc.com/',
                    'adapterId':'p3-parent-chain-discovery-v1'}}
            activation.write_text(json.dumps(value))
            source=server.bcp3_source(root,activation)
            self.assertEqual(source['provider'],'Helius')
            self.assertIn('awaiting',source['status'])
            value['state']='prepared';activation.write_text(json.dumps(value))
            with self.assertRaises(ValueError):server.bcp3_source(root,activation)
            value.update(state='committed',newPolicySha256='c'*64);activation.write_text(json.dumps(value))
            with self.assertRaises(ValueError):server.bcp3_source(root,activation)

    def test_arm_multiple_contracts_and_window_boundaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp).resolve()/'arm.json'
            trade={'position_id':'a','symbol':'SPY','expiry':'2026-11-30','strike':779,'type':'call',
                   'quantity':3,'entry_time':'2026-10-15T03:59:59Z','entry_debit':300.3,
                   'exit_time':'2026-10-15T15:00:00Z','exit_credit':330.0,'net_pnl':29.7}
            held=dict(trade,position_id='b',entry_time='2026-10-15T14:00:00Z',last_bid=1.2,last_quote_time='2026-10-15T14:15:00Z')
            del held['exit_time']
            path.write_text(json.dumps({'strategy_id':'A','trades':[trade],'positions':{'b':held}}))
            trades,positions=server.read_arm(path,'2026-10-15T04:00:00Z','2026-10-16T02:00:00Z')
            self.assertEqual(len(trades),2)
            self.assertEqual(sum(t['pnl_cents'] or 0 for t in trades),2970)
            self.assertEqual(positions[0]['mark_cents'],36000)
            self.assertEqual(positions[0]['pnl_cents'],5970)
            self.assertEqual(positions[0]['mark_at'],'2026-10-15T14:15:00Z')

    def test_book_read_is_readonly_and_null_mark_stays_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp).resolve()/'paper.sqlite3'
            c=sqlite3.connect(path)
            c.executescript("""
            CREATE TABLE settings(key TEXT,value TEXT);
            INSERT INTO settings VALUES('ledger_marker','neorome-paper-ledger-v1');
            CREATE TABLE fills(fill_id TEXT,position_id TEXT,desk TEXT,instrument TEXT,quantity INTEGER,total_cost_cents INTEGER,filled_at TEXT);
            CREATE TABLE closes(close_id TEXT,position_id TEXT,proceeds_cents INTEGER,fee_cents INTEGER,realized_pnl_cents INTEGER,closed_at TEXT);
            CREATE TABLE positions(position_id TEXT,desk TEXT,instrument TEXT,quantity INTEGER,basis_cents INTEGER,state TEXT);
            CREATE TABLE market_marks(position_id TEXT,liquidation_value_cents INTEGER,source_timestamp TEXT);
            INSERT INTO positions VALUES('p','options','SPY spread',1,26900,'open');
            INSERT INTO fills VALUES('f','p','options','SPY spread',1,26900,'2026-10-15T13:00:00Z');
            """)
            c.close();before=path.read_bytes()
            trades,positions=server.read_book(path,'2026-10-15T04:00:00Z','2026-10-16T02:00:00Z')
            self.assertEqual(len(trades),1)
            self.assertIsNone(positions[0]['pnl_cents'])
            self.assertEqual(path.read_bytes(),before)
            con=server.connection(path)
            with self.assertRaises(sqlite3.OperationalError):con.execute('DELETE FROM positions')
            con.close()

    def test_symlink_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'target';p.write_text('{}')
            q=Path(tmp)/'link';q.symlink_to(p)
            with self.assertRaises(ValueError):server.read_json(q)

    def test_private_copy_includes_uncheckpointed_wal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp).resolve()/'paper.sqlite3'
            writer=sqlite3.connect(path)
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE example(value INTEGER)');writer.commit()
            writer.execute('INSERT INTO example VALUES(42)');writer.commit()
            before=path.read_bytes()
            copy=server.copied_connection(path)
            self.assertEqual(copy.execute('SELECT value FROM example').fetchone()[0],42)
            self.assertEqual(path.read_bytes(),before)
            private=Path(copy.workspace.name)
            copy.close();self.assertFalse(private.exists());writer.close()

    def test_missing_sources_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            result=server.snapshot(Path(tmp))
            self.assertEqual(result['coverage']['books'],0)
            self.assertGreater(len(result['errors']),40)
            self.assertFalse(result['trades'])

    @staticmethod
    def arm_book(path, arm_count=1):
        path.parent.mkdir(parents=True,exist_ok=True)
        con=sqlite3.connect(path)
        con.executescript("""
        CREATE TABLE arms(arm_id TEXT,timeframe TEXT,initial_cash INTEGER,available_cash INTEGER,
            open_cost INTEGER,realized INTEGER,reserved INTEGER,position TEXT,latest_mark INTEGER,mark_time REAL);
        CREATE TABLE events(event_id INTEGER PRIMARY KEY,at REAL,kind TEXT,ticker TEXT,cohort_id TEXT,
            arm_id TEXT,source_hash TEXT,payload TEXT);
        """)
        for index in range(arm_count):
            con.execute('INSERT INTO arms VALUES(?,?,?,?,?,?,?,?,?,?)',
                (f'arm-{index}','5m',50000,49000,1000,0,0,None,None,None))
        con.commit();con.close()

    @staticmethod
    def sdk_book(path, initial_cash=10000, fill=False, foreign_activity=False):
        path.parent.mkdir(parents=True,exist_ok=True)
        con=sqlite3.connect(path)
        con.executescript("""
        CREATE TABLE settings(key TEXT,value TEXT);
        INSERT INTO settings VALUES('ledger_marker','neorome-paper-ledger-v1');
        CREATE TABLE accounts(desk TEXT,initial_cash_cents INTEGER,available_cash_cents INTEGER,reserved_cash_cents INTEGER);
        CREATE TABLE fills(fill_id TEXT,position_id TEXT,desk TEXT,instrument TEXT,quantity INTEGER,total_cost_cents INTEGER,filled_at TEXT);
        CREATE TABLE positions(position_id TEXT,desk TEXT,instrument TEXT,quantity INTEGER,basis_cents INTEGER,state TEXT);
        CREATE TABLE closes(close_id TEXT,position_id TEXT,proceeds_cents INTEGER,fee_cents INTEGER,realized_pnl_cents INTEGER,closed_at TEXT);
        CREATE TABLE market_marks(position_id TEXT,liquidation_value_cents INTEGER,source_timestamp TEXT);
        """)
        for desk in ('prediction','equities','options','crypto'):
            cash=initial_cash if desk=='prediction' else 0
            con.execute('INSERT INTO accounts VALUES(?,?,?,?)',(desk,cash,cash,0))
        if fill:
            con.execute('INSERT INTO fills VALUES(?,?,?,?,?,?,?)',('fill-1','position-1','prediction','BTC 15m yes',2,700,'2026-10-18T12:00:00Z'))
        if foreign_activity:
            con.execute('INSERT INTO fills VALUES(?,?,?,?,?,?,?)',('foreign-fill','foreign-position','equities','SPY',1,10000,'2026-10-18T12:01:00Z'))
            con.execute('INSERT INTO positions VALUES(?,?,?,?,?,?)',('foreign-position','equities','SPY',1,10000,'open'))
            con.execute('INSERT INTO positions VALUES(?,?,?,?,?,?)',('foreign-closed','options','SPY call',1,5000,'closed'))
            con.execute('INSERT INTO closes VALUES(?,?,?,?,?,?)',('foreign-close','foreign-closed',9000,100,3900,'2026-10-18T12:02:00Z'))
        con.commit();con.close()

    def test_prediction_inventory_86_and_today_window_with_real_sdk_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve()
            for index,(_,_,_,relative) in enumerate(server.PREDICTION_SDK_BOOKS):
                self.sdk_book(base/relative,initial_cash=10000+index,fill=index==0,foreign_activity=index==0)
            for _,_,relative,expected in server.PREDICTION_ARM_BOOKS:
                self.arm_book(base/relative,expected)
            start='2026-10-18T04:00:00Z';end='2026-10-19T04:00:00Z'
            now=datetime(2026,10,18,16,tzinfo=timezone.utc)
            accounts,trades,positions,coverage=server.prediction_accounts(base,start,end,now)
            self.assertEqual(len(accounts),86)
            self.assertEqual(len({a['id'] for a in accounts}),86)
            self.assertTrue(coverage['complete'])
            self.assertEqual((coverage['expected'],coverage['observed']),(86,86))
            self.assertEqual(len(trades),1)
            self.assertEqual(trades[0]['id'],'btc-stochastic-5m:fill-1')
            self.assertEqual(accounts[0]['initial_cash_cents'],10000)
            self.assertEqual(accounts[0]['entries'],1)
            self.assertEqual((accounts[0]['exits'],accounts[0]['open_positions'],accounts[0]['realized_pnl_today_cents']),(0,0,0))
            snapshot=server.snapshot(base,now)
            self.assertEqual(snapshot['coverage']['books'],8)

    def test_partial_exit_before_midnight_and_zero_payout_settlement_are_not_double_counted(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            yday=datetime(2026,10,17,23,59,tzinfo=timezone.utc).timestamp()
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            events=[
                (1,yday,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':2,'cost_cents':100})),
                (2,yday,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':40,'fee_cents':5})),
                (3,today,'settlement','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'payout_cents':0,'pnl_cents':-60})),
            ]
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',events)
            con.execute('UPDATE arms SET realized=-60')
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(accounts[0]['realized_pnl_today_cents'],-60)
            self.assertEqual(accounts[0]['exits'],0)
            self.assertEqual(accounts[0]['settlements'],1)
            self.assertEqual([trade['side'] for trade in trades],['settlement'])
            self.assertEqual(trades[0]['pnl_cents'],-50)
            self.assertEqual(trades[0]['pnl_status'],'known')

    def test_authoritative_settlement_total_survives_missing_prior_exit_proceeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',[
                (1,today,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':2,'cost_cents':100})),
                (2,today,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'basis_cents':50})),
                (3,today,'settlement','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'payout_cents':0,'pnl_cents':-60})),
            ])
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(accounts[0]['realized_pnl_today_cents'],-60)
            settlement=next(trade for trade in trades if trade['side']=='settlement')
            self.assertIsNone(settlement['pnl_cents'])
            self.assertEqual(settlement['pnl_status'],'unknown')

    def test_completed_partial_exit_cohort_uses_net_proceeds_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            prior=datetime(2026,10,17,23,59,tzinfo=timezone.utc).timestamp()
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            events=[
                (1,prior,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':2,'cost_cents':100})),
                (2,prior,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':40,'fee_cents':5})),
                (3,today,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':65,'fee_cents':7})),
            ]
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',events)
            con.execute('UPDATE arms SET realized=5')
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(accounts[0]['realized_pnl_today_cents'],5)
            self.assertEqual(accounts[0]['exits'],1)
            self.assertEqual(trades[0]['pnl_cents'],15)
            self.assertEqual(trades[0]['pnl_status'],'known')

    def test_exit_fill_pnl_is_net_proceeds_less_basis_including_zero(self):
        for proceeds, expected in [(125,25),(75,-25),(100,0)]:
            with self.subTest(proceeds=proceeds), tempfile.TemporaryDirectory() as tmp:
                base=Path(tmp).resolve();path=base/'ledger.sqlite3'
                self.arm_book(path)
                con=sqlite3.connect(path)
                today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
                con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',[
                    (1,today,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'cost_cents':100})),
                    (2,today,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':proceeds,'fee_cents':999,'basis_cents':100})),
                ])
                con.commit();con.close()
                _,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                    '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
                exit_trade=next(trade for trade in trades if trade['side']=='exit')
                self.assertEqual(exit_trade['pnl_cents'],expected)
                self.assertEqual(exit_trade['pnl_status'],'known')
                self.assertIsNone(exit_trade['pnl_reason'])

    def test_legacy_exit_basis_is_derived_and_missing_proceeds_stays_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',[
                (1,today,'entry','ticker','derived','arm-0',None,json.dumps({'quantity':2,'cost_cents':101})),
                (2,today,'exit_fill','ticker','derived','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':60})),
                (3,today,'entry','ticker','missing','arm-0',None,json.dumps({'quantity':1,'cost_cents':100})),
                (4,today,'exit_fill','ticker','missing','arm-0',None,json.dumps({'quantity':1,'basis_cents':100})),
                (5,today,'exit_fill','ticker','no-entry','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':100})),
            ])
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            derived=next(trade for trade in trades if trade['side']=='exit' and 'event:2' in trade['id'])
            missing=next(trade for trade in trades if trade['side']=='exit' and 'event:4' in trade['id'])
            no_entry=next(trade for trade in trades if trade['side']=='exit' and 'event:5' in trade['id'])
            self.assertEqual(derived['pnl_cents'],9)  # ceil(101 / 2) allocated basis
            self.assertEqual(derived['pnl_status'],'known')
            self.assertIsNone(missing['pnl_cents'])
            self.assertEqual(missing['pnl_status'],'unknown')
            self.assertIn('net proceeds',missing['pnl_reason'])
            self.assertIsNone(no_entry['pnl_cents'])
            self.assertEqual(no_entry['pnl_status'],'unknown')
            self.assertIsNone(accounts[0]['realized_pnl_today_cents'])
            self.assertIsNone(server.prediction_realized_pnl(accounts, []))

    def test_entry_pnl_is_not_applicable_and_flat_account_mark_is_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            con.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',
                (1,today,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'cost_cents':100})))
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(trades[0]['pnl_status'],'not_applicable')
            self.assertIsNone(trades[0]['pnl_cents'])
            self.assertEqual(accounts[0]['open_positions'],0)
            self.assertIsNone(accounts[0]['mark_cents'])
            self.assertIsNone(accounts[0]['mark_at'])
            self.assertEqual(server.prediction_realized_pnl(accounts, []), 0)

    def test_arm_open_position_uses_retained_mark_and_one_second_freshness(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            now=datetime(2026,10,18,5,tzinfo=timezone.utc)
            con=sqlite3.connect(path)
            position={'cohort_id':'cohort','ticker':'KXBTC-15M','quantity':2,'cost_cents':100,
                      'remaining_basis_cents':100,'accrued_net_proceeds_cents':0}
            con.execute('UPDATE arms SET open_cost=100,position=?,latest_mark=125,mark_time=?',
                        (json.dumps(position),now.timestamp()-0.5))
            con.commit();con.close()
            accounts,_,positions=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',now)
            self.assertEqual(accounts[0]['open_positions'],1)
            self.assertTrue(accounts[0]['mark_fresh'])
            self.assertEqual(positions[0]['id'],'btc-test-arm-0:cohort')
            self.assertEqual(positions[0]['mark_cents'],125)
            self.assertEqual(positions[0]['pnl_cents'],25)

    def test_open_position_without_mark_remains_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            now=datetime(2026,10,18,5,tzinfo=timezone.utc)
            con=sqlite3.connect(path)
            position={'cohort_id':'cohort','ticker':'KXBTC-15M','quantity':2,'cost_cents':100,
                      'remaining_basis_cents':100,'accrued_net_proceeds_cents':0}
            con.execute('UPDATE arms SET open_cost=100,position=?',(json.dumps(position),))
            con.commit();con.close()
            accounts,_,positions=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',now)
            self.assertEqual(accounts[0]['open_positions'],1)
            self.assertIsNone(accounts[0]['mark_cents'])
            self.assertFalse(accounts[0]['mark_fresh'])
            self.assertIsNone(positions[0]['mark_cents'])
            self.assertIsNone(positions[0]['pnl_cents'])
            self.assertIsNone(positions[0]['mark_at'])

    def test_old_unrelated_event_history_does_not_exhaust_today_row_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path)
            con=sqlite3.connect(path)
            prior=datetime(2026,10,17,23,59,tzinfo=timezone.utc).timestamp()
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',[
                (1,prior,'entry','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'cost_cents':100})),
                (2,today,'exit_fill','ticker','cohort','arm-0',None,json.dumps({'quantity':1,'proceeds_cents':110,'fee_cents':20})),
            ])
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',[
                (event_id,prior-100,'quote','ticker',f'irrelevant-{event_id}','arm-0',None,'{}')
                for event_id in range(3,10006)])
            con.commit();con.close()
            accounts,trades,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',1,
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(accounts[0]['realized_pnl_today_cents'],10)
            self.assertEqual(accounts[0]['exits'],1)
            self.assertEqual(len(trades),1)

    def test_many_arms_share_one_batched_lineage_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp).resolve();path=base/'ledger.sqlite3'
            self.arm_book(path,54)
            con=sqlite3.connect(path)
            prior=datetime(2026,10,17,23,59,tzinfo=timezone.utc).timestamp()
            today=datetime(2026,10,18,4,10,tzinfo=timezone.utc).timestamp()
            events=[]
            for index in range(54):
                arm_id=f'arm-{index}'
                events.append((index*2+1,prior,'entry','ticker','shared-cohort',arm_id,None,
                               json.dumps({'quantity':1,'cost_cents':100})))
                events.append((index*2+2,today,'settlement','ticker','shared-cohort',arm_id,None,
                               json.dumps({'quantity':1,'payout_cents':90,'pnl_cents':-10})))
            con.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?)',events)
            con.commit();con.close()
            queries=[]
            original=server.connection
            def traced_connection(source):
                connection=original(source);connection.set_trace_callback(queries.append);return connection
            with patch.object(server,'connection',side_effect=traced_connection):
                accounts,_,_=server.read_prediction_arms(base,'test','Test','ledger.sqlite3',54,
                    '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            lineage_queries=[query for query in queries if 'FROM events WHERE cohort_id IN' in query]
            self.assertEqual(len(lineage_queries),1)
            self.assertEqual(len(accounts),54)
            self.assertTrue(all(account['realized_pnl_today_cents']==-10 for account in accounts))

    def test_missing_prediction_sources_are_not_healthy_zero_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            accounts,_,_,coverage=server.prediction_accounts(Path(tmp).resolve(),
                '2026-10-18T04:00:00Z','2026-10-19T04:00:00Z',datetime(2026,10,18,5,tzinfo=timezone.utc))
            self.assertEqual(accounts,[])
            self.assertFalse(coverage['complete'])
            self.assertEqual(coverage['observed'],0)
            self.assertEqual(coverage['expected'],86)
            self.assertEqual(len(coverage['errors']),8)

    def test_account_inventory_and_trade_filters_are_present_in_html(self):
        html=Path(server.__file__).with_name('index.html').read_text()
        for marker in ['prediction_accounts','prediction_account_coverage','account-filter',
                       'trade-account-filter','Settlement outcomes are shown separately',
                       'Realized today','Exit fills','Activity events in window','[\'Settlements\', desk.settlements]',
                       'Exit P&amp;L is net proceeds less allocated basis','trade.pnl_status === \'not_applicable\'',
                       "const flat = account.open_positions === 0", "flat ? 'N/A' : money(account.mark_cents)"]:
            self.assertIn(marker,html)


if __name__=='__main__':unittest.main()
