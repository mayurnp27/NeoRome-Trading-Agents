"""Read-only BRTI/Polymarket US streaming recorder; no order routes.

Authentication remains in memory on the user's Mac. Stop at duration/disk cap;
network interruptions and rollovers are explicit, never interpolated away.
"""
from __future__ import annotations
import argparse, asyncio, base64, calendar, collections, datetime as dt
import gzip, hashlib, json, os, re, signal, socket, struct, threading, time, uuid
from decimal import Decimal
from pathlib import Path
from urllib.request import HTTPRedirectHandler, Request, build_opener
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, ed25519

UTC=dt.timezone.utc
KURL='wss://external-api-ws.kalshi.com/trade-api/ws/v2'
PURL='wss://api.polymarket.us/v1/ws/markets'
GATEWAY='https://gateway.polymarket.us/v1/events/slug/'

def stamp():
    return {'capture_utc_ns':time.time_ns(),'capture_monotonic_ns':time.monotonic_ns()}

def iso_ns(value):
    """Parse RFC3339 without losing the source's sub-microsecond precision."""
    match=re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z',value)
    if not match: raise ValueError('unsupported source timestamp')
    seconds=calendar.timegm(time.strptime(match[1],'%Y-%m-%dT%H:%M:%S'))
    return seconds*10**9+int((match[2] or '').ljust(9,'0'))

def event_slug(now=None):
    value=dt.datetime.fromtimestamp(time.time() if now is None else now,UTC)
    return 'btc-updown-15m-'+value.replace(minute=value.minute//15*15).strftime('%Y-%m-%d-%H%M')+'z'

def headers(venue,key_id,secret):
    ts=str(time.time_ns()//1_000_000)
    if venue=='kalshi':
        signature=secret.sign((ts+'GET/trade-api/ws/v2').encode(),padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=32),hashes.SHA256())
        return {'KALSHI-ACCESS-KEY':key_id,'KALSHI-ACCESS-TIMESTAMP':ts,'KALSHI-ACCESS-SIGNATURE':base64.b64encode(signature).decode()}
    signature=secret.sign((ts+'GET/v1/ws/markets').encode())
    return {'X-PM-Access-Key':key_id,'X-PM-Timestamp':ts,'X-PM-Signature':base64.b64encode(signature).decode()}

def level(rows,bid):
    valid=[]
    for row in rows:
        px=row['px']; price=Decimal(px['value']); qty=Decimal(row['qty'])
        if px['currency']!='USD' or not price.is_finite() or not qty.is_finite() or not 0<=price<=1 or qty<0:
            raise ValueError('invalid retail level')
        if qty>0: valid.append((price,qty))
    if not valid:return None
    price=(max if bid else min)(p for p,q in valid)
    return {'price_usd':str(price),'quantity_contracts':str(sum(q for p,q in valid if p==price))}

def normalize(source,payload,receipt):
    out={'source':source,**receipt,'source_event_ns':None,'source_time_semantics':'unavailable'}
    if source=='brti':
        msg=payload['msg']; upstream=json.loads(msg['data'])
        if upstream.get('id')!='BRTI' or upstream.get('type')!='value':raise ValueError('wrong index')
        ts=int(upstream['time']); value=Decimal(upstream['value'])
        if not value.is_finite() or value<=0:raise ValueError('invalid BRTI')
        if 'source_ts_ms' in msg and int(msg['source_ts_ms'])!=ts:raise ValueError('timestamp mismatch')
        out.update(source_event_ns=ts*1_000_000,source_time_semantics='CF calculation time relayed by Kalshi',
                   value_usd=str(value),relay_received_ms=msg.get('received_at'),relay_sending_ms=payload.get('sending_ts_ms'),
                   sid=payload.get('sid'),seq=payload.get('seq'),amend_time_ms=upstream.get('amendTime'),
                   repeat_of_previous=upstream.get('repeatOfPreviousValue',False))
    else:
        md=payload['marketData']; b=level(md.get('bids',[]),True); a=level(md.get('offers',[]),False)
        if md.get('transactTime'):
            out.update(source_event_ns=iso_ns(md['transactTime']),source_time_semantics='US marketData transactTime; may represent stats activity, not a unique BBO change time')
        out.update(market_slug=md['marketSlug'],state=md.get('state'),bid=b,ask=a,
                   two_sided=bool(b and a),crossed=bool(b and a and Decimal(b['price_usd'])>=Decimal(a['price_usd'])),
                   sequence_available=False)
    if out['source_event_ns'] is not None:
        out['raw_capture_minus_source_ms']=(receipt['capture_utc_ns']-out['source_event_ns'])/1_000_000
    return out

class Archive:
    def __init__(self,root,cap):
        self.root=root;root.mkdir(parents=True,exist_ok=False);self.cap=cap;self.bytes=0
        self.lock=threading.Lock();self.file=None;self.bucket=None;self.counts=collections.Counter();self.last={};self.started=stamp();self.compressed_bytes=0;self.closed_bytes=0;self.path=None
    def emit(self,row):
        encoded=(json.dumps(row,separators=(',',':'))+'\n').encode()
        with self.lock:
            if self.bytes+len(encoded)>self.cap*16 or self.compressed_bytes+len(encoded)>self.cap:raise RuntimeError('archive byte cap reached')
            bucket=row.get('capture_utc_ns',time.time_ns())//(300*10**9)
            if bucket!=self.bucket:
                if self.file:
                    self.file.close();self.closed_bytes+=self.path.stat().st_size
                self.path=self.root/f'{bucket}.jsonl.gz'
                self.file=gzip.open(self.path,'xb');self.bucket=bucket
            self.file.write(encoded);self.file.flush();self.bytes+=len(encoded);self.compressed_bytes=self.closed_bytes+self.path.stat().st_size
            self.counts[row['kind']]+=1
            if row['kind']=='message':self.last[row['source']]=row['capture_utc_ns']
    def status(self,extra=None):
        with self.lock:
            out={'started':self.started,**stamp(),'pid':os.getpid(),'counts':dict(self.counts),'last_message_ns':dict(self.last),
                 'uncompressed_bytes':self.bytes,'compressed_bytes':self.compressed_bytes,'compressed_cap_bytes':self.cap,'uncompressed_cap_bytes':self.cap*16,
                 'orders':0,'paper_strategies_modified':False,**(extra or {})}
            tmp=self.root/'status.tmp';tmp.write_text(json.dumps(out,indent=2)+'\n');tmp.replace(self.root/'status.json')
    def close(self):
        with self.lock:
            if self.file:self.file.close()

class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None

def metadata(slug):
    request_started=stamp()
    req=Request(GATEWAY+slug,headers={'User-Agent':'NeoRome-readonly-lag-capture/1'})
    with build_opener(NoRedirect()).open(req,timeout=8) as response:
        raw=response.read(2**20);receipt=stamp();data=json.loads(raw);event=data['event']
        markets=event.get('markets',[])
        expected='cpc-'+slug
        matches=[m for m in markets if m.get('slug')==expected]
        if len(matches)!=1:raise ValueError('ambiguous market mapping')
        terms=matches[0].get('assetPriceTerms',{})
        start=iso_ns(terms['windowStart']);end=iso_ns(terms['windowEnd'])
        if terms.get('indexSymbol')!='BRTI' or end-start!=900*10**9 or event_slug(start/1e9)!=slug:
            raise ValueError('invalid BRTI interval identity')
        return {'kind':'metadata',**receipt,'event_slug':slug,'market_slug':expected,'event':event,
                'request_started':request_started,'window_start_ns':start,'window_end_ns':end,
                'http_date':response.headers.get('Date'),'age':response.headers.get('Age'),'raw_sha256':hashlib.sha256(raw).hexdigest()}

def ntp_sample():
    """Read-only unauthenticated NTP audit; never changes the host clock."""
    epoch=2208988800
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sock:
        sock.settimeout(3);sock.connect(('time.cloudflare.com',123))
        t1=time.time();secs=int(t1)+epoch;frac=int((t1-int(t1))*2**32)
        token=struct.pack('!II',secs,frac);packet=b'\x23'+b'\0'*39+token
        sock.send(packet);reply=sock.recv(512);t4=time.time()
        if len(reply)<48 or reply[0]&7!=4 or reply[0]>>6==3 or not 1<=reply[1]<=15 or reply[24:32]!=token:raise ValueError('invalid NTP reply')
        def read(offset):
            sec,f=struct.unpack('!II',reply[offset:offset+8]);return sec-epoch+f/2**32
        t2,t3=read(32),read(40)
        if not t2 or not t3 or t3<t2:raise ValueError('invalid NTP times')
        return {'kind':'clock_audit',**stamp(),'method':'unauthenticated NTP query; no clock adjustment','server':'time.cloudflare.com',
                'offset_server_minus_local_ms':((t2-t1)+(t3-t4))*500,'round_trip_ms':((t4-t1)-(t3-t2))*1000,'stratum':reply[1],
                'root_delay_seconds':struct.unpack('!i',reply[4:8])[0]/65536,'root_dispersion_seconds':struct.unpack('!I',reply[8:12])[0]/65536}

async def receiver(source,key_id,key,archive,stop):
    attempt=0
    while not stop.is_set():
        connection=str(uuid.uuid4());market=None;previous={};attempt+=1
        try:
            if source=='polymarket':
                meta=await asyncio.to_thread(metadata,event_slug());await asyncio.to_thread(archive.emit,meta);market=meta['market_slug'];current=meta['event_slug']
            async with websockets.connect(KURL if source=='brti' else PURL,additional_headers=headers('kalshi' if source=='brti' else 'poly',key_id,key),
                                          open_timeout=10,close_timeout=2,max_size=2**20,max_queue=256,compression=None) as ws:
                await asyncio.to_thread(archive.emit,{'kind':'connect',**stamp(),'source':source,'connection':connection,'market':market})
                subs=[{'id':1,'cmd':'subscribe','params':{'channels':['cfbenchmarks_value_5hz'],'index_ids':['BRTI']}},
                      {'id':2,'cmd':'subscribe','params':{'channels':['cfbenchmarks_value'],'index_ids':['BRTI']}}] if source=='brti' else [
                      {'subscribe':{'requestId':connection,'subscriptionType':'SUBSCRIPTION_TYPE_MARKET_DATA','marketSlugs':[market],'responsesDebounced':False}}]
                for sub in subs:await ws.send(json.dumps(sub))
                last_receipt=time.monotonic();silence_reported=False
                while not stop.is_set():
                    if source=='polymarket' and event_slug()!=current:
                        await asyncio.to_thread(archive.emit,{'kind':'rollover',**stamp(),'source':source,'old_event':current,'new_event':event_slug(),'connection':connection});break
                    try:raw=await asyncio.wait_for(ws.recv(),1)
                    except asyncio.TimeoutError:
                        silence=time.monotonic()-last_receipt
                        if silence>(2 if source=='brti' else 10) and not silence_reported:
                            await asyncio.to_thread(archive.emit,{'kind':'silence_interval',**stamp(),'source':source,'connection':connection,
                                'duration_seconds':silence,'meaning':'no application frames; missed updates not proven'})
                            silence_reported=True
                        continue
                    receipt=stamp();payload=json.loads(raw)
                    if silence_reported:
                        await asyncio.to_thread(archive.emit,{'kind':'silence_end',**receipt,'source':source,'connection':connection,'duration_seconds':time.monotonic()-last_receipt})
                    last_receipt=time.monotonic();silence_reported=False
                    known=payload.get('type') in ('cfbenchmarks_value','cfbenchmarks_value_5hz') if source=='brti' else isinstance(payload.get('marketData'),dict)
                    if not known:
                        await asyncio.to_thread(archive.emit,{'kind':'protocol',**receipt,'source':source,'connection':connection,
                            'message_type':payload.get('type'),'public_error_code':payload.get('msg',{}).get('code') if isinstance(payload.get('msg'),dict) else None})
                        continue
                    normalized=normalize(source,payload,receipt)
                    if source=='polymarket' and normalized['market_slug']!=market:raise ValueError('unexpected market')
                    stream=payload.get('sid',market);prior=previous.get(stream);seq=payload.get('seq')
                    flags=[]
                    if prior:
                        if seq is not None and prior['seq'] is not None and seq!=prior['seq']+1:flags.append('sequence_discontinuity')
                        if normalized.get('source_event_ns') is not None and prior['source_event_ns'] is not None and normalized['source_event_ns']<prior['source_event_ns']:flags.append('source_time_regression')
                    book_hash=hashlib.sha256(json.dumps({k:normalized.get(k) for k in ('bid','ask','state')},sort_keys=True).encode()).hexdigest() if source=='polymarket' else None
                    if source=='polymarket':normalized['bbo_changed_since_received_snapshot']=None if prior is None else book_hash!=prior['book_hash']
                    previous[stream]={'seq':seq,'source_event_ns':normalized.get('source_event_ns'),'book_hash':book_hash}
                    row={'kind':'message',**receipt,'source':source,'connection':connection,'initial_snapshot':prior is None,'integrity_flags':flags,
                         'normalized':normalized,'raw_public_payload':payload}
                    await asyncio.to_thread(archive.emit,row)
                await asyncio.to_thread(archive.emit,{'kind':'disconnect',**stamp(),'source':source,'connection':connection,'reason':'rollover_or_stop'})
        except asyncio.CancelledError:raise
        except RuntimeError:stop.set();raise
        except Exception as exc:
            response=getattr(exc,'response',None);code=getattr(response,'status_code',None)
            await asyncio.to_thread(archive.emit,{'kind':'gap',**stamp(),'source':source,'connection':connection,'error_type':type(exc).__name__,'http_status':code,'attempt':attempt})
            if code in (401,403):stop.set();return
            try:await asyncio.wait_for(stop.wait(),min(30,2**min(attempt,4)))
            except asyncio.TimeoutError:pass

async def audit(archive,stop):
    prior=stamp();count=0
    while not stop.is_set():
        now=stamp();drift=((now['capture_utc_ns']-prior['capture_utc_ns'])-(now['capture_monotonic_ns']-prior['capture_monotonic_ns']))/1e6
        if abs(drift)>5:await asyncio.to_thread(archive.emit,{'kind':'wall_clock_jump',**now,'wall_minus_monotonic_change_ms':drift})
        prior=now
        if count%60==0:
            try:row=await asyncio.to_thread(ntp_sample)
            except Exception as exc:row={'kind':'clock_audit_unavailable',**stamp(),'error_type':type(exc).__name__}
            await asyncio.to_thread(archive.emit,row)
        archive.status({'state':'collecting','clock_offset_is_not_exchange_offset':True})
        count+=1
        try:await asyncio.wait_for(stop.wait(),1)
        except asyncio.TimeoutError:pass

async def run(args):
    # Secret files are read in memory and never copied to the archive or cloud.
    key=serialization.load_pem_private_key(args.kalshi_secret.read_bytes(),password=None)
    raw=args.poly_secret.read_bytes()
    if raw.lstrip().startswith(b'{\\rtf'):
        import subprocess
        raw=subprocess.run(['/usr/bin/textutil','-convert','txt','-stdin','-stdout'],input=raw,capture_output=True,check=True).stdout
    candidates=set()
    for token in re.findall(rb'(?<![A-Za-z0-9+/=])[A-Za-z0-9+/]{43,88}={0,2}(?![A-Za-z0-9+/=])',raw):
        try:
            decoded=base64.b64decode(token,validate=True)
            if len(decoded) in (32,64):candidates.add(decoded[:32])
        except ValueError:pass
    if len(candidates)!=1:raise ValueError('ambiguous US secret')
    pk=ed25519.Ed25519PrivateKey.from_private_bytes(candidates.pop())
    archive=Archive(args.output,args.max_mib*2**20);stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(sig,stop.set)
    archive.emit({'kind':'start',**stamp(),'schema':1,'duration_seconds':args.seconds,'routes':[KURL,PURL,GATEWAY],
                  'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'paper_strategy_access':'none'})
    tasks=[asyncio.create_task(receiver('brti',str(uuid.UUID(args.kalshi_id)),key,archive,stop)),
           asyncio.create_task(receiver('polymarket',str(uuid.UUID(args.poly_id)),pk,archive,stop)),asyncio.create_task(audit(archive,stop))]
    reason='duration_limit'
    try:
        stopper=asyncio.create_task(stop.wait())
        done,_=await asyncio.wait([*tasks,stopper],timeout=args.seconds,return_when=asyncio.FIRST_COMPLETED)
        if done:reason='stop_signal_or_feed_task_end'
        for task in done:
            if task in tasks:task.result()
    finally:
        stop.set()
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        archive.status({'state':'stopped','stop_reason':reason});archive.close()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kalshi-id',required=True);parser.add_argument('--poly-id',required=True)
    parser.add_argument('--kalshi-secret',type=Path,required=True);parser.add_argument('--poly-secret',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--seconds',type=int,default=86400)
    parser.add_argument('--max-mib',type=int,default=1024)
    args=parser.parse_args()
    if not 1<=args.seconds<=86400 or not 1<=args.max_mib<=1024:parser.error('capture bounds exceeded')
    asyncio.run(run(args))

if __name__=='__main__':main()
