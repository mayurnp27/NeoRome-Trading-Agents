import unittest,json,tempfile,fcntl,os
from pathlib import Path
from datetime import datetime,timedelta,timezone
import core

class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'A.json'
        self.cfg=core.load_config({'strategy_id':'A','symbols':['SPY']})
    def tearDown(self):self.tmp.cleanup()
    def test_kernel_lock_survives_stale_pid_file_and_blocks_active_writer(self):
        lock=self.path.with_name(self.path.name+'.lock');lock.write_text('999999')
        snap={'asof':'2026-10-15T13:35:00Z','signals':[],'quotes':[]}
        core.process_snapshot(snap,self.path,self.cfg)
        fd=os.open(lock,os.O_RDWR);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            with self.assertRaises(RuntimeError):core.process_snapshot(snap,self.path,self.cfg)
        finally:os.close(fd)
    def test_bounded_replay_cache_preserves_out_of_order_guard(self):
        start=datetime(2026,10,15,13,35,tzinfo=timezone.utc)
        for i in range(270):core.process_snapshot({'asof':(start+timedelta(seconds=i)).isoformat(),'signals':[],'quotes':[]},self.path,self.cfg)
        state=json.loads(self.path.read_text());self.assertEqual(len(state['processed_snapshots']),256)
        self.assertEqual(state['cash'],1000)
        with self.assertRaises(ValueError):core.process_snapshot({'asof':start.isoformat(),'signals':[],'quotes':[]},self.path,self.cfg)
    def test_audit_recovers_only_uncommitted_tail_without_duplicates(self):
        state=core.new_state(self.cfg);audit=self.path.with_name(self.path.stem+'.audit.jsonl')
        old=json.dumps({'event_id':'old','payload':'x'*2_100_000}).encode()+b'\n'
        pending=[{'event_id':'one','kind':'paper_mark'},{'event_id':'two','kind':'paper_mark'}]
        audit.write_bytes(old+json.dumps(pending[0]).encode()+b'\n'+b'{"event_id":')
        state['audit_offset']=len(old);state['pending_audit']=pending
        core.flush_audit(self.path,state)
        with audit.open('rb') as handle:handle.seek(len(old));tail=handle.read()
        self.assertEqual([json.loads(line)['event_id'] for line in tail.splitlines()],['one','two'])
        self.assertEqual(state['audit_offset'],audit.stat().st_size);self.assertEqual(state['pending_audit'],[])
    def test_trade_metrics_do_not_count_partial_fills_as_separate_wins(self):
        state=core.new_state(self.cfg)
        state['trades']=[{'position_id':'closed','net_pnl':10,'quantity':1},{'position_id':'closed','net_pnl':-4,'quantity':1},{'position_id':'open','net_pnl':2,'quantity':1}]
        state['positions']={'open':{'quantity':1,'mark_status':'stale'}};state['realized_pnl']=8
        core.atomic_write(self.path,state);r=core.status(self.path,self.cfg)
        self.assertEqual(r['closed_trades'],1);self.assertEqual(r['realized_exit_fills'],3)
        self.assertEqual(r['win_rate'],1);self.assertEqual(r['mean_win'],6)
        self.assertEqual(r['net_expectancy_per_trade'],6);self.assertEqual(r['realized_pnl_from_open_positions'],2)

    def test_complete_recovered_event_without_newline_preserves_record_boundary(self):
        state=core.new_state(self.cfg);audit=self.path.with_name(self.path.stem+'.audit.jsonl')
        pending=[{'event_id':'one','kind':'paper_entry'},{'event_id':'two','kind':'paper_exit'}]
        audit.write_text(json.dumps(pending[0]));state['pending_audit']=pending
        core.flush_audit(self.path,state)
        self.assertEqual([json.loads(line)['event_id'] for line in audit.read_text().splitlines()],['one','two'])

if __name__=='__main__':unittest.main()
