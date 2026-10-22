import base64,json,os,tempfile,unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
from operations.maintenance_agent.auto_v1 import bridge,controller

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.prod=self.root/'production';self.prod.mkdir()
        self.code=self.prod/'code.py';self.code.write_text('VALUE=1\n');self.test=self.prod/'test_code.py';self.test.write_text('import unittest\n')
        self.profile={'id':'unit-test','files':{'code.py':str(self.code),'test_code.py':str(self.test)},'tests':['test_code'],'unit':'neorome-test.service'}
        self.sources=controller.snapshot(self.profile)
        self.entry={'profile_id':'unit-test','fingerprint':'a'*64,'snapshot_hash':'b'*64,'sources':self.sources,'tests':['test_code']}
    def candidate(self):
        return {'incident_fingerprint':'a'*64,'base_hashes':{n:s['sha256'] for n,s in self.sources.items()},'files':{'code.py':base64.b64encode(b'VALUE=2\n').decode()},'diagnosis':'tested repair'}
    def state(self):
        p=self.root/'state';p.mkdir();(p/'receipts').mkdir();return p
    def test_snapshot_bridge_detects_actual_edit_and_retains_base(self):
        workspace=self.root/'workspace';workspace.mkdir();bridge.prepare(workspace,self.entry)
        (workspace/'repair_target/code.py').write_text('VALUE=2\n')
        result=bridge.proposal(workspace,self.entry,{'diagnosis':'cause','escalation':''})
        self.assertEqual(set(result['files']),{'code.py'})
        self.assertEqual(result['base_hashes']['code.py'],self.sources['code.py']['sha256'])
    def test_escalation_never_submits_patch(self):
        workspace=self.root/'workspace';workspace.mkdir();bridge.prepare(workspace,self.entry)
        result=bridge.proposal(workspace,self.entry,{'diagnosis':'unknown cause','escalation':'unknown cause'})
        self.assertEqual(result['files'],{});self.assertEqual(result['escalation'],'unknown cause')
    def test_catalog_owner_freshness_and_symlink(self):
        p=self.root/'catalog.json';value={'schema_version':1,'as_of':datetime.now(timezone.utc).isoformat(),'incidents':[self.entry]};p.write_text(json.dumps(value));p.chmod(0o644)
        with patch.object(bridge,'TRUSTED_UID',os.getuid()):
            self.assertEqual(len(bridge.incidents(p,datetime.now(timezone.utc))),1)
            p.chmod(0o666)
            with self.assertRaises(ValueError):bridge.read_catalog(p,datetime.now(timezone.utc))
            p.chmod(0o644);value['as_of']='2020-01-01T00:00:00Z';p.write_text(json.dumps(value))
            with self.assertRaises(ValueError):bridge.read_catalog(p,datetime.now(timezone.utc))
    def test_stale_baseline_rejected_before_testing_or_install(self):
        state=self.state();proposal=self.candidate();self.code.write_text('CONCURRENT=True\n')
        with patch.object(controller,'STATE',state),patch.object(controller,'isolated_test') as test:
            with self.assertRaisesRegex(ValueError,'stale_candidate'):controller.process(self.profile,proposal,{'id':'a.json'})
            test.assert_not_called()
        self.assertEqual(self.code.read_text(),'CONCURRENT=True\n')
    def test_baseline_must_fail_and_candidate_must_pass(self):
        state=self.state()
        with patch.object(controller,'STATE',state),patch.object(controller,'isolated_test',return_value={'passed':True}),patch.object(controller.transaction,'install') as install:
            with self.assertRaisesRegex(ValueError,'regression_not_demonstrated'):controller.process(self.profile,self.candidate(),{'id':'a.json'})
            install.assert_not_called()
    def test_failed_production_probe_rolls_back_actual_bytes(self):
        state=self.state()
        with patch.object(controller,'STATE',state),patch.object(controller,'isolated_test',side_effect=[{'passed':False},{'passed':True}]),patch.object(controller,'review',return_value={'approved':True}),patch.object(controller,'restart') as restart,patch.object(controller,'verify',side_effect=ValueError('production_health_failed')):
            receipt={'id':'a.json'}
            with self.assertRaisesRegex(ValueError,'production_health_failed'):controller.process(self.profile,self.candidate(),receipt)
            self.assertEqual(receipt['status'],'rolled_back');self.assertEqual(restart.call_count,2)
        self.assertEqual(self.code.read_text(),'VALUE=1\n')
    def test_independent_review_rejection_prevents_install(self):
        state=self.state()
        with patch.object(controller,'STATE',state),patch.object(controller,'isolated_test',side_effect=[{'passed':False},{'passed':True}]),patch.object(controller,'review',side_effect=ValueError('independent_review_rejected')),patch.object(controller.transaction,'install') as install:
            with self.assertRaisesRegex(ValueError,'independent_review_rejected'):controller.process(self.profile,self.candidate(),{'id':'a.json'})
            install.assert_not_called()
    def test_read_only_profile_file_cannot_be_deployed(self):
        state=self.state();self.profile['editable_files']=['test_code.py']
        with patch.object(controller,'STATE',state):
            with self.assertRaisesRegex(ValueError,'candidate_scope'):controller.process(self.profile,self.candidate(),{'id':'a.json'})
    def test_failed_rollback_remains_recoverable(self):
        state=self.state()
        with patch.object(controller,'STATE',state),patch.object(controller,'isolated_test',side_effect=[{'passed':False},{'passed':True}]),patch.object(controller,'review',return_value={'approved':True}),patch.object(controller,'restart'),patch.object(controller,'verify',side_effect=ValueError('production_health_failed')),patch.object(controller.transaction,'rollback',side_effect=OSError('temporary rollback failure')):
            receipt={'id':'a.json'}
            with self.assertRaises(OSError):controller.process(self.profile,self.candidate(),receipt)
            self.assertEqual(receipt['status'],'recovery_required')
            self.assertEqual(json.loads((state/'receipts/a.json').read_text())['status'],'recovery_required')
    def test_failure_counters_do_not_create_new_repair_chains(self):
        state=self.state();inbox=self.root/'inbox';inbox.mkdir();self.profile['unit_sha256']='c'*64
        observations=[(False,{'unit':{'Result':'success'},'status':{'reason':'oversized'},'journal':[{'status':'publish_failed','consecutive_failures':n}]}) for n in (1,2)]
        with patch.object(controller,'STATE',state),patch.object(controller,'INBOX',inbox),patch.object(controller,'evidence',side_effect=observations):
            a=controller.catalog({'profiles':[self.profile]})['incidents'][0]
            b=controller.catalog({'profiles':[self.profile]})['incidents'][0]
            self.assertEqual(a['fingerprint'],b['fingerprint'])
    def test_failed_cloud_record_keeps_private_bounded_diagnostic(self):
        from types import SimpleNamespace
        receipt={'id':'a.json','incident_fingerprint':'a'*64}
        from types import SimpleNamespace
        with patch.object(controller.Path,'stat',return_value=SimpleNamespace(st_uid=997,st_gid=987)),patch.object(controller.subprocess,'run',return_value=SimpleNamespace(returncode=1,stderr='x'*9000,stdout='')):
            with self.assertRaisesRegex(ValueError,'canonical_cloud_record_pending'):controller.record_outcome(receipt)
        self.assertEqual(len(receipt['record_error_detail']),4000)
    def test_pending_cloud_record_is_retried_without_new_repair(self):
        state=self.state();inbox=self.root/'inbox';inbox.mkdir();runs=self.root/'runs';runs.mkdir()
        receipt={'id':'a.json','record_status':'cloud_record_or_memory_pending','status':'verified_deployed'}
        (state/'receipts/a.json').write_text(json.dumps(receipt))
        with patch.object(controller,'STATE',state),patch.object(controller,'INBOX',inbox),patch.object(controller,'RUNS',runs),patch.object(controller,'catalog',return_value={'incidents':[]}),patch.object(controller,'record_outcome') as record:
            controller.tick({'profiles':[]})
            record.assert_called_once_with(receipt)
    def test_strategy_runtime_cannot_be_admitted_by_a_profile(self):
        profile={**self.profile,'files':{'core.py':'/opt/neorome-stochastic-paper/v2/core.py'}}
        with self.assertRaisesRegex(ValueError,'non_operational_edit'):
            controller.validate_policy({'schema_version':1,'profiles':[profile]})
    def test_test_path_traversal_cannot_escape_approved_directories(self):
        for target in ('/opt/neorome/tests/../../neorome/neorome/test_runtime.py','/opt/neorome/tests/../../../other/test_backdoor.py'):
            profile={**self.profile,'files':{'test_runtime.py':target}}
            with self.assertRaisesRegex(ValueError,'non_normalized_target'):
                controller.validate_policy({'schema_version':1,'profiles':[profile]})

if __name__=='__main__':unittest.main()
