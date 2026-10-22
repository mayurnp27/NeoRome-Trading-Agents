import json,os,tempfile,unittest
from pathlib import Path
from neorome.robinhood_cloud_data import CloudDataSession
from neorome.robinhood_cloud_auth import RESOURCE
class CloudDataTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'tokens.json'
  self.path.write_text(json.dumps({'access_token':'fake','resource':RESOURCE,'obtained_at':100,'expires_in':5000}));self.path.chmod(0o600)
 def tearDown(self):self.temp.cleanup()
 def test_no_order_method_even_internal_rpc(self):
  s=CloudDataSession(self.path)
  for operation in [lambda:s.call('place_option_order',{}),lambda:s._rpc('tools/call',{'name':'place_option_order'}),lambda:s._rpc('orders/create',{})]:
   with self.assertRaises(ValueError):operation()
 def test_public_credential_file_refused(self):
  self.path.chmod(0o644)
  with self.assertRaises(ValueError):CloudDataSession(self.path).__enter__()
 def test_call_budget(self):
  s=CloudDataSession(self.path);s.calls=5
  with self.assertRaises(ValueError):s.call('get_option_quotes',{})
  with self.assertRaises(ValueError):s._rpc('tools/call',{'name':'get_option_quotes','arguments':{}})
 def test_refresh_retains_rotated_credentials(self):
  s=CloudDataSession(self.path,clock=lambda:1000)
  s.tokens={'access_token':'old','refresh_token':'old-refresh','client_id':'client','resource':RESOURCE,'obtained_at':100,'expires_in':200}
  s._http=lambda *args:{'access_token':'new','refresh_token':'new-refresh','token_type':'bearer','expires_in':300}
  s._refresh();saved=json.loads(self.path.read_text())
  self.assertEqual(saved['refresh_token'],'new-refresh');self.assertEqual(saved['obtained_at'],1000)
  self.assertEqual(self.path.stat().st_mode & 0o077,0)
