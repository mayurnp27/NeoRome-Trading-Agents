"""Bounded read-only Robinhood MCP transport using separately authorized OAuth.

Exactly one initialization plus allowlisted data calls; never an order method.
Secrets remain in the dedicated cloud collector's private directory.
"""
from __future__ import annotations
import argparse
import fcntl
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request
from .equity_data import NoRedirect
from .crypto_pilot import _atomic_json
from .robinhood_cloud_auth import RESOURCE, TOKEN_URL
from .robinhood_options import collect
from .jupiter_quotes import save_new

MAX_RESPONSE=2_000_000
READ_ONLY_TOOLS=frozenset({
 'search','get_option_chains','get_option_instruments','get_option_quotes',
 'get_accounts','get_option_positions','get_option_orders',
})

class CloudDataSession:
    def __init__(self, token_file, opener=None, clock=time.time, max_calls=5):
        # A complete, bounded option-chain slice needs at most 88 read calls
        # (catalog, finite pages, batched quotes, and account reconciliation).
        if isinstance(max_calls,bool) or not isinstance(max_calls,int) or not 1<=max_calls<=96:
            raise ValueError('Invalid cloud data call budget')
        self.path=Path(token_file);self.opener=opener or urllib.request.build_opener(NoRedirect)
        self.clock=clock;self.calls=0;self.counter=0;self.session=None;self.lock=None;self.max_calls=max_calls
    def __enter__(self):
        if self.path.is_symlink() or not self.path.is_file() or self.path.stat().st_mode & 0o077:
            raise ValueError('Private cloud credentials required')
        lock_path=self.path.with_suffix('.lock')
        fd=os.open(lock_path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        self.lock=os.fdopen(fd,'a+')
        try:
            fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.tokens=json.loads(self.path.read_text())
            if self.tokens.get('resource')!=RESOURCE:raise ValueError('Credential resource mismatch')
            if self.clock() < self.tokens['obtained_at']:raise ValueError('Clock rollback')
            if self.clock() >= self.tokens['obtained_at']+self.tokens['expires_in']-60:
                self._refresh()
            self._rpc('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'neorome-paper-data','version':'1'}})
            self._rpc('notifications/initialized',{},notification=True)
            return self
        except Exception:
            self.__exit__(None,None,None);raise
    def __exit__(self,*args):
        if self.lock:self.lock.close();self.lock=None
    def _http(self,url,payload,headers):
        request=urllib.request.Request(url,data=payload,headers=headers,method='POST')
        try:
            with self.opener.open(request,timeout=30) as response:
                body=response.read(MAX_RESPONSE+1)
                status=response.status
                session=response.headers.get('Mcp-Session-Id')
                kind=response.headers.get('Content-Type','')
        except Exception:raise ValueError('Cloud options transport failed') from None
        if status not in (200,202,204) or len(body)>MAX_RESPONSE:raise ValueError('Invalid MCP response')
        if session:
            if len(session)>1024 or not all(33<=ord(c)<127 for c in session):raise ValueError('Invalid MCP session')
            self.session=session
        if not body:return None
        if 'text/event-stream' in kind:
            events=[]
            for block in body.decode().replace('\r\n','\n').split('\n\n'):
                data='\n'.join(line[5:].lstrip() for line in block.splitlines() if line.startswith('data:'))
                if data:events.append(json.loads(data))
            matching=[e for e in events if isinstance(e,dict) and e.get('id')==self.counter]
            if len(matching)!=1:raise ValueError('Unexpected MCP event response')
            return matching[0]
        return json.loads(body)
    def _refresh(self):
        refresh=self.tokens.get('refresh_token')
        if not isinstance(refresh,str) or not refresh:raise ValueError('Cloud options reauthentication required')
        params={'grant_type':'refresh_token','refresh_token':refresh,'client_id':self.tokens['client_id'],'resource':RESOURCE}
        body=self._http(TOKEN_URL,urllib.parse.urlencode(params).encode(),{'Content-Type':'application/x-www-form-urlencoded'})
        if not isinstance(body,dict) or not isinstance(body.get('access_token'),str) or not body['access_token']:
            raise ValueError('Invalid refreshed token')
        expiry=body.get('expires_in')
        if isinstance(expiry,bool) or not isinstance(expiry,(int,float)) or not 0<expiry<=31536000:raise ValueError('Invalid refreshed expiry')
        if str(body.get('token_type','')).lower()!='bearer':raise ValueError('Invalid refreshed token type')
        updated={**self.tokens,**body,'obtained_at':self.clock()}
        _atomic_json(self.path,updated);self.tokens=updated
    def _rpc(self,method,params,notification=False):
        allowed={'initialize','notifications/initialized','tools/call'}
        if method not in allowed:raise ValueError('MCP method refused')
        if method=='tools/call' and params.get('name') not in READ_ONLY_TOOLS:raise ValueError('Order/non-data tool refused')
        if method=='tools/call':
            if self.calls>=self.max_calls:raise ValueError('Options request budget exhausted')
            self.calls+=1
        self.counter+=1
        payload={'jsonrpc':'2.0','method':method,'params':params}
        if not notification:payload['id']=self.counter
        headers={'Authorization':'Bearer '+self.tokens['access_token'],'Content-Type':'application/json','Accept':'application/json, text/event-stream','MCP-Protocol-Version':'2025-03-26'}
        if self.session:headers['Mcp-Session-Id']=self.session
        result=self._http(RESOURCE,json.dumps(payload).encode(),headers)
        if notification:return None
        if not isinstance(result,dict) or result.get('id')!=self.counter or 'error' in result:raise ValueError('MCP request rejected')
        return result.get('result')
    def call(self,tool,arguments):
        if tool not in READ_ONLY_TOOLS:raise ValueError('Only read-only options tools permitted')
        if self.calls>=self.max_calls:raise ValueError('Options request budget exhausted')
        result=self._rpc('tools/call',{'name':tool,'arguments':arguments})
        if not isinstance(result,dict) or result.get('isError'):raise ValueError('Options data call failed')
        body=result.get('structuredContent')
        if not isinstance(body,dict):
            texts=[b['text'] for b in result.get('content',[]) if b.get('type')=='text']
            if len(texts)!=1:raise ValueError('Unexpected options result')
            body=json.loads(texts[0])
        if not isinstance(body.get('data'),dict):raise ValueError('Options data missing')
        return body['data']

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tokens',required=True);p.add_argument('--output',required=True)
    p.add_argument('--symbol',choices=['SPY','QQQ'],default='SPY');p.add_argument('--expiry',required=True)
    p.add_argument('--right',choices=['call','put'],default='call');p.add_argument('--strikes',nargs=2,required=True)
    a=p.parse_args()
    try:
        if Path(a.output).exists():raise ValueError('Output already exists')
        with CloudDataSession(a.tokens) as session:
            result=collect(session.call,symbol=a.symbol,expiry=a.expiry,right=a.right,strikes=a.strikes)
        save_new(a.output,result)
        print(json.dumps({'status':'captured','data_calls':5,'orders_sent':0}))
    except Exception as exc:
        print(json.dumps({'status':'blocked','error_type':type(exc).__name__,'orders_sent':0}))
        raise SystemExit(1)
