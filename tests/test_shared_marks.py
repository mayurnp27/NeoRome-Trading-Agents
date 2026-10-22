import hashlib,json,sqlite3,tempfile,unittest
from pathlib import Path
from datetime import datetime,timezone,timedelta
from neorome.shared_marks import equity_quote,MarkUnavailable,mark_provider
from neorome.paper_ledger import PaperLedger, Proposal, MarketMark
NOW=datetime(2026,9,22,14,tzinfo=timezone.utc)
class SharedMarksTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
 def tearDown(self):self.temp.cleanup()
 def make_quote(self,*,at=NOW,feed='iex',digest=True,bid=100,ask=101):
  raw=json.dumps({'evidence':{'provider':'alpaca','feed':feed},'raw_response':{'quotes':{'SPY':{'t':at.isoformat(),'bp':bid,'ap':ask,'bs':5,'as':6}}}})
  with sqlite3.connect(self.root/'recorder.sqlite3') as c:
   c.execute('CREATE TABLE IF NOT EXISTS attempts(id INTEGER PRIMARY KEY,observation TEXT,sha256 TEXT,status TEXT)')
   c.execute('INSERT INTO attempts(observation,sha256,status) VALUES(?,?,?)',(raw,hashlib.sha256(raw.encode()).hexdigest() if digest else None,'fresh'))
 def test_quote_preserves_time_depth_hash(self):
  self.make_quote();q,s=equity_quote(self.root,NOW)
  self.assertEqual(q['timestamp'],NOW.isoformat());self.assertEqual(q['bid_size'],'5');self.assertEqual(len(s['sha256']),64)
 def test_quote_rejects_stale_unhashed_wrong_feed_and_cross(self):
  for changes in [{'at':NOW-timedelta(seconds=61)},{'digest':False},{'feed':'sip'},{'bid':102}]:
   self.make_quote(**changes)
   with self.assertRaises(ValueError):equity_quote(self.root,NOW)
 def test_empty_shared_book_needs_no_fake_quotes(self):
  b=PaperLedger(self.root/'paper.sqlite3',enforce_market_risk=True,clock=lambda:NOW)
  self.assertEqual(mark_provider(crypto_state='missing',equity_state='missing')(b,NOW),[])
 def test_memecoin_marker_is_required_and_propagated(self):
  b=PaperLedger(self.root/'paper.sqlite3',enforce_market_risk=True,clock=lambda:NOW)
  b.record_marks('initial',[],as_of=NOW)
  proposal=Proposal('meme-p','crypto','solana-memecoin-attention','memecoin-paper-v1','a'*64,
                    NOW,'solana:11111111111111111111111111111111','token:solana:test','token:solana:test',
                    NOW+timedelta(minutes=1),1,400,50,'observed',{'token_memecoin_v1':{'acquired_token_raw':'1'}})
  reservation=b.reserve(proposal,now=NOW)
  fill=b.simulate_fill(reservation.reservation_id,fill_id='meme-f',provenance='observed',filled_at=NOW)
  with self.assertRaisesRegex(MarkUnavailable,'Memecoin position'):
   mark_provider(crypto_state='missing',equity_state='missing')(b,NOW)
  callback=lambda book,position,now: MarketMark(position.position_id,420,now,'b'*64,'observed')
  marks=mark_provider(crypto_state='missing',equity_state='missing',token_marker=callback)(b,NOW)
  self.assertEqual(marks[0].position_id,fill.position_id)
  self.assertEqual(marks[0].liquidation_value_cents,420)
