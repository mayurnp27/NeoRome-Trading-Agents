import json
from pathlib import Path
import unittest
from neorome.wallet_history import normalize_page, candidate_wallets, WalletHistoryClient, WalletHistoryError
from neorome.jupiter_quotes import SOL, USDC
W='11111111111111111111111111111111'
M='4B84tQkYs2RaaKGpWw4KGGaKCik8WHiyB3EujCxJpump'
def page(side='buy'):
 s={'type':'swap','input_mint':SOL if side=='buy' else M,'output_mint':M if side=='buy' else SOL,'in_amount':'1000000000' if side=='buy' else '1000','actual_out_amount':'1000' if side=='buy' else '2000000000'}
 t={'mint':M,'decimals':2,'rawTokenAmount':1000,'toUserAccount':W if side=='buy' else M,'fromUserAccount':M if side=='buy' else W}
 q={'mint':SOL,'decimals':9,'rawTokenAmount':1000000000 if side=='buy' else 2000000000,'toUserAccount':M if side=='buy' else W,'fromUserAccount':W if side=='buy' else M}
 return {'received_at':'2026-09-30T17:00:00+00:00','source_sha256':'a'*64,'pagination_token':None,'data':[{'signature':'test','parserStatus':'OK','parsed':{'feePayer':W,'transactionStatus':'OK','blockTime':1790787500,'fee':5000,'summary':{'type':'swap','parsedData':s},'tokenTransfers':[t,q],'nativeTransfers':[]}}]}
class HistoryTests(unittest.TestCase):
 def test_buy_and_sell(self):
  for side in ('buy','sell'):
   n=normalize_page(page(side),W);self.assertEqual(n['rows'][0]['side'],side);self.assertEqual(n['rows'][0]['quantity'],'10');self.assertEqual(n['rows'][0]['fee_quote'],'0.000005')
 def test_transfer_not_trade(self):
  p=page();p['data'][0]['parsed']['summary']['type']='transfer';self.assertFalse(normalize_page(p,W)['rows'])
 def test_transfer_actor_mismatch(self):
  p=page();p['data'][0]['parsed']['tokenTransfers'][0]['toUserAccount']=M
  self.assertEqual(normalize_page(p,W)['exclusions'][0]['reason'],'wallet_transfer_swap_mismatch')
 def test_failed_tx_no_signal(self):
  p=page();p['data'][0]['parsed']['transactionStatus']='ERROR';self.assertFalse(normalize_page(p,W)['rows']);self.assertFalse(candidate_wallets(p))
 def test_null_parser_excluded(self):
  p=page();p['data'][0]['parsed']=None;self.assertFalse(normalize_page(p,W)['rows'])
 def test_sponsor_excluded(self):
  p=page();p['data'][0]['parsed']['feePayer']=M
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['wallet'],W);self.assertEqual(row['sponsor_network_fee_lamports'],5000)
 def test_sponsorship_without_actor_transfer_evidence_is_excluded(self):
  p=page();p['data'][0]['parsed']['feePayer']=M
  for t in p['data'][0]['parsed']['tokenTransfers']:
   t['toUserAccount']=M;t['fromUserAccount']=M
  self.assertEqual(normalize_page(p,W)['exclusions'][0]['reason'],'wallet_transfer_swap_mismatch')
  self.assertNotIn(W,candidate_wallets(p))
 def test_candidate_is_actor_not_sponsor_and_requires_two_legs(self):
  p=page();p['data'][0]['parsed']['feePayer']=M
  self.assertEqual(candidate_wallets(p),[W])
  p['data'][0]['parsed']['tokenTransfers'][1]['fromUserAccount']=M
  self.assertNotIn(W,candidate_wallets(p))
 def test_quote_mismatch_internal_routing_cancels(self):
  p=page();p['data'][0]['parsed']['tokenTransfers'][1]['rawTokenAmount']=999999999
  self.assertEqual(normalize_page(p,W)['exclusions'][0]['reason'],'wallet_quote_transfer_swap_mismatch')
  p=page();q=p['data'][0]['parsed']['tokenTransfers'][1]
  p['data'][0]['parsed']['tokenTransfers'] += [dict(q,toUserAccount=M,fromUserAccount=W),dict(q,toUserAccount=W,fromUserAccount=M)]
  self.assertEqual(normalize_page(p,W)['rows'][0]['fee_quote'],'0.000005')
 def test_usdc_quote_fees_and_normalization_provenance(self):
  p=page('sell');item=p['data'][0];tr=item['parsed']['tokenTransfers'];s=item['parsed']['summary']['parsedData']
  item['parsed']['feePayer']=M
  s.update(input_mint=M,output_mint=USDC,in_amount='1000',actual_out_amount='90903649')
  tr[:] = [t for t in tr if t['mint']==M]
  tr[0].update(rawTokenAmount=1000,toUserAccount=M,fromUserAccount=W)
  tr.append({'mint':USDC,'decimals':6,'rawTokenAmount':89953649,'toUserAccount':W,'fromUserAccount':M})
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['quote_mint'],USDC);self.assertEqual(row['quote_quantity'],'90.903649')
  self.assertEqual(row['fee_quote'],'0.95');self.assertTrue(row['costs_complete'])
  self.assertEqual(row['normalization_version'],'wallet-swap-v2')
  self.assertEqual(row['source_sha256'],p['source_sha256']);self.assertEqual(row['observed_at'],p['received_at'])
  p=page();item=p['data'][0];tr=item['parsed']['tokenTransfers'];s=item['parsed']['summary']['parsedData']
  s.update(input_mint=USDC,output_mint=M,in_amount='497503000',actual_out_amount='1000')
  tr[:] = [t for t in tr if t['mint']==M]
  tr[0].update(rawTokenAmount=1000,toUserAccount=W,fromUserAccount=M)
  tr.append({'mint':USDC,'decimals':6,'rawTokenAmount':500000000,'toUserAccount':M,'fromUserAccount':W})
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['quote_quantity'],'497.503');self.assertEqual(row['fee_quote'],'2.497')
 def test_usdc_with_trader_native_cost_is_explicitly_unpriced(self):
  p=page();item=p['data'][0];tr=item['parsed']['tokenTransfers'];s=item['parsed']['summary']['parsedData']
  s.update(input_mint=USDC,output_mint=M,in_amount='1000000',actual_out_amount='1000')
  tr[:] = [t for t in tr if t['mint']==M]
  tr[0].update(rawTokenAmount=1000,toUserAccount=W,fromUserAccount=M)
  tr.append({'mint':USDC,'decimals':6,'rawTokenAmount':1000000,'toUserAccount':M,'fromUserAccount':W})
  item['parsed']['nativeTransfers']=[{'fromUserAccount':W,'toUserAccount':M,'amount':1000}]
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['native_cost_lamports'],6000);self.assertFalse(row['costs_complete'])
 def test_native_cost_uses_only_unambiguous_same_transaction_conversion(self):
  p=page();item=p['data'][0];item['parsed']['feePayer']=W
  tr=item['parsed']['tokenTransfers'];s=item['parsed']['summary']['parsedData']
  s.update(input_mint=USDC,output_mint=M,in_amount='1000000',actual_out_amount='1000')
  s['inner_swaps']=[
   {'input_mint':SOL,'output_mint':USDC,'input_amount':'1000000000','output_amount':'100000000'},
   {'input_mint':SOL,'output_mint':USDC,'input_amount':'1000000000','output_amount':'100000000'},
  ]
  tr[:] = [t for t in tr if t['mint']==M]
  tr[0].update(rawTokenAmount=1000,toUserAccount=W,fromUserAccount=M)
  tr.append({'mint':USDC,'decimals':6,'rawTokenAmount':1000000,'toUserAccount':M,'fromUserAccount':W})
  item['parsed']['nativeTransfers']=[{'fromUserAccount':W,'toUserAccount':M,'amount':1000}]
  row=normalize_page(p,W)['rows'][0]
  self.assertTrue(row['costs_complete']);self.assertEqual(row['fee_quote'],'0.0006')
  self.assertEqual(row['native_cost_conversion']['usdc_per_sol'],'100')
  self.assertEqual(row['native_cost_conversion']['usdc_cost'],'0.0006')
  s['inner_swaps'].append({'input_mint':USDC,'output_mint':SOL,'input_amount':'100000000','output_amount':'1000000000'})
  row=normalize_page(p,W)['rows'][0]
  self.assertFalse(row['costs_complete']);self.assertIsNone(row['native_cost_conversion'])
 def test_invalid_decimals_and_amount_types_fail_closed(self):
  for field,value in [('decimals',True),('decimals',19),('rawTokenAmount',True),('rawTokenAmount',-1),('rawTokenAmount',2**64)]:
   p=page();p['data'][0]['parsed']['tokenTransfers'][0][field]=value
   self.assertFalse(normalize_page(p,W)['rows'],(field,value))
  p=page();p['data'][0]['parsed']['tokenTransfers'][1]['decimals']=6
  self.assertEqual(normalize_page(p,W)['exclusions'][0]['reason'],'quote_decimals_unverified')
 def test_quote_sign_and_native_wrapping_costs(self):
  p=page();p['data'][0]['parsed']['tokenTransfers'][1]['toUserAccount']=W
  p['data'][0]['parsed']['tokenTransfers'][1]['fromUserAccount']=W
  self.assertEqual(normalize_page(p,W)['exclusions'][0]['reason'],'wallet_quote_transfer_swap_mismatch')
  p=page();p['data'][0]['parsed']['tokenTransfers'][1]['rawTokenAmount']=1000005000
  p['data'][0]['parsed']['nativeTransfers']=[{'fromUserAccount':W,'toUserAccount':M,'amount':1000005000}]
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['trader_native_direct_outflow_lamports'],0)
  self.assertEqual(row['native_cost_lamports'],5000)
  self.assertEqual(row['fee_quote'],'0.00001')
  p['data'][0]['parsed']['tokenTransfers'][1]['rawTokenAmount']=1000007000
  p['data'][0]['parsed']['nativeTransfers']=[{'fromUserAccount':W,'toUserAccount':M,'amount':1000009000},
                                             {'fromUserAccount':M,'toUserAccount':W,'amount':9000000}]
  row=normalize_page(p,W)['rows'][0]
  self.assertEqual(row['trader_native_direct_outflow_lamports'],2000)
 def test_malformed_nested_records_fail_closed(self):
  for mutate in (
   lambda p: p['data'][0]['parsed'].__setitem__('feePayer', None),
   lambda p: p['data'][0]['parsed']['summary'].__setitem__('parsedData', []),
   lambda p: p['data'][0]['parsed']['tokenTransfers'].__setitem__(0, 'bad'),
   lambda p: p['data'].__setitem__(0, None),
   lambda p: p['data'][0]['parsed']['nativeTransfers'].__setitem__(0, 'bad'),
  ):
   p=page();p['data'][0]['parsed']['nativeTransfers']=[{'fromUserAccount':W,'toUserAccount':M,'amount':1}]
   mutate(p)
   self.assertEqual(len(normalize_page(p,W)['exclusions']),1)
 def test_source_and_time_validation_is_preserved_in_scorer(self):
  from tests.test_wallet_follow import fill, MINTS, CUTOFF
  from neorome.wallet_follow import score_wallets
  row=fill('bad-observation',MINTS[0],'buy','2026-09-30T11:00:00Z',observed='2026-09-30T10:59:00Z')
  row['normalization_version']='wallet-swap-v2'
  self.assertEqual(score_wallets([row],CUTOFF)['issues'][0]['reason'],'observed_before_block_time')
  row=fill('bad-source',MINTS[0],'buy','2026-09-30T11:00:00Z');row['source_sha256']='bad'
  self.assertEqual(score_wallets([row],CUTOFF)['issues'][0]['reason'],'source_sha256_invalid')
 def test_captured_pages_replay_actual_swaps(self):
  evidence=Path(__file__).resolve().parents[1]/'coordination/verification/fomo-free-20260930/wallet-verification.json'
  if not evidence.exists():
   self.skipTest('captured Helius pages are available in the main checkout')
  data=json.loads(evidence.read_text())
  report=[]
  for wallet in data['wallets']:
   page=wallet['chain_history'];actual=[item for item in page['data'] if ((item.get('parsed') or {}).get('summary') or {}).get('type')=='swap']
   replay={**page,'data':actual};result=normalize_page(replay,wallet['wallet'])
   report.append((wallet['handle'],len(actual),len(result['rows']),[x['reason'] for x in result['exclusions']]))
  self.assertEqual(sum(x[1] for x in report),17)
  self.assertEqual([x[2] for x in report],[12,4])
  self.assertEqual([x[3] for x in report],[['failed_or_unparsed_transaction'],[]])
 def test_ceiling_prevents_call(self):
  c=WalletHistoryClient(max_calls=1,credential_loader=lambda:'unused');c.used=1
  with self.assertRaises(WalletHistoryError):c.history(W)
