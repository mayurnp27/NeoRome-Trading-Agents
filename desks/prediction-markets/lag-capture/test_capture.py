import importlib.util, json, tempfile, unittest, gzip, asyncio
from unittest.mock import patch
from pathlib import Path

spec=importlib.util.spec_from_file_location('capture',Path(__file__).with_name('capture.py'))
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)

class CaptureTests(unittest.TestCase):
    def test_nanosecond_timestamp(self):
        self.assertEqual(c.iso_ns('2026-10-18T06:14:05.524011572Z'),1792304045524011572)
        self.assertEqual(c.iso_ns('1970-01-01T00:00:01Z'),10**9)
        with self.assertRaises(ValueError):c.iso_ns('2026-10-18T06:14:05')

    def test_rollover(self):
        self.assertEqual(c.event_slug(1792304099),'btc-updown-15m-2026-10-18-0600z')
        self.assertEqual(c.event_slug(1792304100),'btc-updown-15m-2026-10-18-0615z')

    def test_brti_source_vs_relay(self):
        payload={'sid':1,'seq':3,'sending_ts_ms':1010,'msg':{'index_id':'BRTI','received_at':1009,'source_ts_ms':1000,
            'data':json.dumps({'type':'value','id':'BRTI','time':1000,'value':'8.1E4','amendTime':2000,'repeatOfPreviousValue':True})}}
        row=c.normalize('brti',payload,{'capture_utc_ns':999_000_000,'capture_monotonic_ns':20})
        self.assertEqual(row['source_event_ns'],10**9)
        self.assertEqual(row['raw_capture_minus_source_ms'],-1)
        self.assertTrue(row['repeat_of_previous']);self.assertEqual(row['amend_time_ms'],2000)
        payload['msg']['source_ts_ms']=1001
        with self.assertRaises(ValueError):c.normalize('brti',payload,{'capture_utc_ns':1})

    def test_retail_decimal_quantity_and_empty_side(self):
        md={'marketSlug':'test','transactTime':'1970-01-01T00:00:01.000000001Z','state':'MARKET_STATE_OPEN',
            'bids':[{'px':{'value':'0.55','currency':'USD'},'qty':'1.25'}],'offers':[]}
        row=c.normalize('polymarket',{'marketData':md},{'capture_utc_ns':2*10**9})
        self.assertFalse(row['two_sided']);self.assertFalse(row['sequence_available'])
        self.assertEqual(row['bid']['quantity_contracts'],'1.25');self.assertEqual(row['source_event_ns'],1000000001)

    def test_sorted_levels_and_crossed_book(self):
        def lev(p,q):return {'px':{'value':p,'currency':'USD'},'qty':q}
        md={'marketSlug':'test','bids':[lev('.4','3'),lev('.5','2'),lev('.5','1')],'offers':[lev('.5','2')]}
        row=c.normalize('polymarket',{'marketData':md},{'capture_utc_ns':1})
        self.assertEqual(row['bid']['quantity_contracts'],'3');self.assertTrue(row['crossed'])
        self.assertIsNone(row['source_event_ns'])
        md['bids']=[lev('NaN','2')]
        with self.assertRaises(ValueError):c.normalize('polymarket',{'marketData':md},{'capture_utc_ns':1})

    def test_archive_cap_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'new';a=c.Archive(root,1000)
            a.emit({'kind':'test','capture_utc_ns':1});a.close()
            rows=[json.loads(line) for line in gzip.open(next(root.glob('*.gz')),'rt')]
            self.assertEqual(rows[0]['kind'],'test')
            with self.assertRaises(FileExistsError):c.Archive(root,1000)
            b=c.Archive(Path(d)/'cap',1)
            with self.assertRaises(RuntimeError):b.emit({'kind':'test'})
            b.close()

    def test_rollover_reconnect_has_new_snapshot(self):
        rows=[];sent=[];calls=[]
        class Log:
            def emit(self,row):rows.append(row)
        async def exercise():
            stop=asyncio.Event()
            class WS:
                async def __aenter__(self):return self
                async def __aexit__(self,*args):return None
                async def send(self,data):sent.append(json.loads(data))
                async def recv(self):
                    stop.set()
                    return json.dumps({'marketData':{'marketSlug':'cpc-new','bids':[],'offers':[]}})
            def connect(url,**kwargs):calls.append(url);return WS()
            def meta(slug):return {'kind':'metadata','event_slug':slug,'market_slug':'cpc-'+slug}
            with patch.object(c,'event_slug',side_effect=['old','new','new','new','new']),patch.object(c,'metadata',side_effect=meta),patch.object(c,'headers',return_value={}),patch.object(c.websockets,'connect',side_effect=connect):
                await c.receiver('polymarket','not-read',None,Log(),stop)
        asyncio.run(exercise())
        self.assertEqual(calls,[c.PURL,c.PURL]);self.assertEqual(len(sent),2)
        self.assertEqual([x['subscribe']['marketSlugs'] for x in sent],[['cpc-old'],['cpc-new']])
        self.assertEqual(sum(r['kind']=='rollover' for r in rows),1)
        self.assertTrue(next(r for r in rows if r['kind']=='message')['initial_snapshot'])

    def test_sample_public_payloads_parse(self):
        evidence=Path(__file__).resolve().parents[3]/'coordination/verification/polymarket-timestamp-streams-20261018/authenticated-probe.json'
        data=json.loads(evidence.read_text())
        for source in data['results']:
            for sample in source['public_samples']:
                c.normalize('brti' if source['source']=='kalshi_brti' else 'polymarket',sample['payload'],
                            {'capture_utc_ns':sample['received_utc_ns'],'capture_monotonic_ns':sample['received_monotonic_ns']})

if __name__=='__main__':unittest.main()
