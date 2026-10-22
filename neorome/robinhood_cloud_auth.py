"""One-use loopback PKCE login for NeoRome's read-only Robinhood collector.

Tokens are written only to a private server-side directory. No order API,
password collection, desktop credential extraction, or credential logging.
"""
from __future__ import annotations
import argparse
import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import secrets
import time
import urllib.parse
import urllib.request
from .equity_data import NoRedirect
from .crypto_pilot import _atomic_json

PORT=48763
REDIRECT=f'http://127.0.0.1:{PORT}/callback'
RESOURCE='https://agent.robinhood.com/mcp/trading'
TOKEN_URL='https://api.robinhood.com/oauth2/token/'


def auth_request(client_id):
    state=secrets.token_urlsafe(32)
    verifier=secrets.token_urlsafe(64)
    challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
    query={'response_type':'code','client_id':client_id,'redirect_uri':REDIRECT,
           'scope':'internal','resource':RESOURCE,'state':state,
           'code_challenge':challenge,'code_challenge_method':'S256'}
    return 'https://robinhood.com/oauth?'+urllib.parse.urlencode(query),state,verifier


class AuthorizationDenied(ValueError):
    pass


class BoundedHTTPServer(HTTPServer):
    def get_request(self):
        connection,address=super().get_request()
        connection.settimeout(5)
        return connection,address


def callback_code(path, expected_state):
    parsed=urllib.parse.urlsplit(path)
    if parsed.path != '/callback' or parsed.fragment:
        raise ValueError('Invalid callback path')
    values=urllib.parse.parse_qs(parsed.query,strict_parsing=True)
    if set(values)-{'code','state','scope','error','error_description','error_uri'} or any(len(v)!=1 for v in values.values()):
        raise ValueError('Invalid callback parameters')
    if not hmac.compare_digest(values.get('state',[''])[0],expected_state):
        raise ValueError('Invalid callback state')
    if 'error' in values:
        if 'code' in values:raise ValueError('Conflicting authorization response')
        raise AuthorizationDenied('Authorization was declined')
    code=values.get('code',[''])[0]
    if not code or len(code)>8192:
        raise ValueError('Missing authorization code')
    return code


def exchange(client_id, code, verifier, opener=None):
    params={'grant_type':'authorization_code','client_id':client_id,'code':code,
            'code_verifier':verifier,'redirect_uri':REDIRECT,'resource':RESOURCE}
    req=urllib.request.Request(TOKEN_URL,data=urllib.parse.urlencode(params).encode(),
        headers={'Content-Type':'application/x-www-form-urlencoded'},method='POST')
    with (opener or urllib.request.build_opener(NoRedirect)).open(req,timeout=20) as response:
        if response.status!=200:raise ValueError('Token exchange failed')
        raw=response.read(65537)
    if len(raw)>65536:raise ValueError('Token response too large')
    result=json.loads(raw)
    if not isinstance(result,dict) or not isinstance(result.get('access_token'),str) or not result['access_token']:
        raise ValueError('Missing access token')
    if str(result.get('token_type','')).lower()!='bearer':raise ValueError('Unsupported token type')
    expiry=result.get('expires_in')
    if isinstance(expiry,bool) or not isinstance(expiry,(int,float)) or not 0<expiry<=31536000:
        raise ValueError('Invalid token expiry')
    result.update(client_id=client_id,obtained_at=time.time(),resource=RESOURCE)
    return result


def serve(client_file, state_dir, timeout=1200):
    root=Path(state_dir)
    if root.is_symlink():raise ValueError('Unsafe credential directory')
    root.mkdir(parents=True,exist_ok=True,mode=0o700);root.chmod(0o700)
    token_path=root/'tokens.json'
    if token_path.exists() or token_path.is_symlink():raise ValueError('Existing credentials refused')
    client=json.loads(Path(client_file).read_text())
    if client.get('redirect_uris')!=[REDIRECT] or client.get('client_secret'):
        raise ValueError('Unsupported client registration')
    url,state,verifier=auth_request(client['client_id'])
    completed=False
    attempted=False
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_GET(self):
            nonlocal completed,attempted
            if self.headers.get('Host') not in (f'127.0.0.1:{PORT}',f'localhost:{PORT}'):
                self.send_error(400,'Invalid host');return
            try:code=callback_code(self.path,state)
            except AuthorizationDenied:
                attempted=True
                self.send_error(403,'Authorization declined; no connection made');return
            except (ValueError,TypeError):
                self.send_error(400,'Invalid authorization response');return
            if attempted:
                self.send_error(409,'Authorization already used');return
            attempted=True
            try:
                tokens=exchange(client['client_id'],code,verifier)
                _atomic_json(token_path,tokens)
                _atomic_json(root/'status.json',{'authenticated':True,'obtained_at':tokens['obtained_at'],'orders_sent':0})
                completed=True
                payload=b'NeoRome cloud connection authenticated. No trades were placed. You may close this tab.'
                self.send_response(200);self.send_header('Content-Type','text/plain');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(payload)
            except Exception:
                self.send_error(502,'Authentication exchange failed; no credentials displayed')
    with BoundedHTTPServer(('127.0.0.1',PORT),Handler) as server:
        server.timeout=1
        print(json.dumps({'authorization_url':url,'expires_in':timeout}),flush=True)
        deadline=time.monotonic()+timeout
        while not attempted and time.monotonic()<deadline:server.handle_request()
    if not completed:raise ValueError('Authentication incomplete')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-file',required=True);parser.add_argument('--state-dir',required=True)
    args=parser.parse_args()
    try:serve(args.client_file,args.state_dir)
    except Exception as exc:
        print(json.dumps({'authenticated':False,'error_type':type(exc).__name__}),flush=True)
        raise SystemExit(1)
