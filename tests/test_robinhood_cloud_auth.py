import unittest
import urllib.parse
from neorome.robinhood_cloud_auth import auth_request,callback_code,exchange,AuthorizationDenied,BoundedHTTPServer
class AuthTests(unittest.TestCase):
 def test_pkce_and_state(self):
  url,state,verifier=auth_request('client')
  values=urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
  self.assertEqual(values['code_challenge_method'],['S256'])
  self.assertGreaterEqual(len(verifier),43)
  self.assertEqual(callback_code('/callback?code=abc&state='+state,state),'abc')
 def test_reject_bad_callbacks(self):
  for value in ['/wrong?code=abc&state=s','/callback?code=abc&state=wrong','/callback?code=a&code=b&state=s','/callback?state=s','/callback?code=a&state=s&error=x']:
   with self.subTest(value=value),self.assertRaises(ValueError):callback_code(value,'s')
 def test_token_request_fixed_endpoint_no_secret_output(self):
  import json
  class Response:
   status=200
   def __enter__(self):return self
   def __exit__(self,*a):pass
   def read(self,n):return json.dumps({'access_token':'test-only','token_type':'Bearer','expires_in':300}).encode()
  class Opener:
   def open(self,request,timeout):
    self.request=request
    return Response()
  opener=Opener();result=exchange('client','code','verifier',opener)
  self.assertEqual(opener.request.full_url,'https://api.robinhood.com/oauth2/token/')
  self.assertEqual(result['access_token'],'test-only')

 def test_valid_state_denial_terminates(self):
  with self.assertRaises(AuthorizationDenied):callback_code('/callback?error=access_denied&state=s','s')
 def test_accepted_socket_has_timeout(self):
  from unittest.mock import patch,Mock
  connection=Mock()
  with patch('http.server.HTTPServer.get_request',return_value=(connection,('127.0.0.1',1))):
   BoundedHTTPServer.get_request(BoundedHTTPServer.__new__(BoundedHTTPServer))
  connection.settimeout.assert_called_once_with(5)
