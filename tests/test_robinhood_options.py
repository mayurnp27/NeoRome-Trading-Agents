import copy
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
from neorome.robinhood_options import collect, normalize, preview, ReadOnlySession

NOW = datetime.now(timezone.utc)
EXPIRY = (NOW + timedelta(days=40)).date().isoformat()


def source(tool, args):
    if tool == 'search': return {'results':[{'instrument_id':'spy-id','symbol':'SPY'}]}
    if tool == 'get_option_chains':
        return {'chains': [{'id':'chain','symbol':'SPY','expiration_dates':[EXPIRY],
            'underlying_instruments':[{'symbol':'','instrument':'https://example.com/instruments/spy-id/'}], 'can_open_position':True,
            'settle_on_open':False,'cash_component':None,'trade_value_multiplier':'100'}]}
    if tool == 'get_option_instruments':
        strike=args['strike_price']
        return {'instruments':[{'id':strike,'chain_id':'chain','chain_symbol':'SPY',
            'expiration_date':EXPIRY,'type':'call','strike_price':strike,
            'underlying_type':'equity','state':'active','tradability':'tradable','trade_value_multiplier':'100'}]}
    return {'results':[{'quote':{'instrument_id':i,'bid_price':'1.00' if i=='650' else '.95',
        'ask_price':'1.05' if i=='650' else '1.00','bid_size':2,'ask_size':3,
        'delta':'.5','implied_volatility':'.2','updated_at':NOW.isoformat()}} for i in args['instrument_ids']]}


def snapshot(callback=source):
    return collect(callback, symbol='SPY', expiry=EXPIRY, right='call', strikes=['650','655'])


class RobinhoodOptionsTests(unittest.TestCase):
    def test_bounded_collection_and_preview(self):
        calls=[]
        def call(t,a): calls.append(t); return source(t,a)
        data=snapshot(call)
        self.assertEqual(len(calls),5)
        rows=normalize(data,as_of=datetime.now(timezone.utc))
        self.assertEqual(rows[0]['timestamp'],NOW.isoformat())
        result=preview(data,as_of=datetime.now(timezone.utc),long_id='650',fee_per_spread='1',slippage_per_spread='1')
        self.assertEqual(result['status'],'preview_only',result)
        self.assertEqual(result['risk_including_costs'],'12.00')
        self.assertEqual(result['simulated_fills'],0)

    def test_provider_failures_block(self):
        cases=[('updated_at',(NOW-timedelta(days=2)).isoformat()),
               ('updated_at',(NOW+timedelta(minutes=1)).isoformat()),
               ('bid_price','NaN'),('bid_price','2'),('ask_price','0'),
               ('bid_size',0),('ask_size',True),('delta',None)]
        for field,value in cases:
            with self.subTest(field=field,value=value):
                def bad(t,a):
                    d=source(t,a)
                    if t=='get_option_quotes': d['results'][0]['quote'][field]=value
                    return d
                with self.assertRaises(ValueError): normalize(snapshot(bad),as_of=datetime.now(timezone.utc))

    def test_exact_identity_join(self):
        for mode in ('missing','duplicate','unknown'):
            def bad(t,a):
                d=source(t,a)
                if t=='get_option_quotes':
                    if mode=='missing':d['results'].pop()
                    elif mode=='duplicate':d['results'].append(copy.deepcopy(d['results'][0]))
                    else:d['results'][0]['quote']['instrument_id']='unknown'
                return d
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                normalize(snapshot(bad),as_of=datetime.now(timezone.utc))

    def test_wrong_underlying_identity_blocked(self):
        def bad(t,a):
            d=source(t,a)
            if t=='search': d['results'][0]['symbol']='QQQ'
            return d
        with self.assertRaisesRegex(ValueError,'identity mismatch'):
            normalize(snapshot(bad),as_of=datetime.now(timezone.utc))

    def test_adjusted_contract_blocked(self):
        def bad(t,a):
            d=source(t,a)
            if t=='get_option_chains':d['chains'][0]['cash_component']='1'
            return d
        with self.assertRaises(ValueError): normalize(snapshot(bad),as_of=datetime.now(timezone.utc))

    def test_archive_age_and_integrity(self):
        data=snapshot()
        with self.assertRaises(ValueError): normalize(data,as_of=NOW+timedelta(minutes=5))
        data['responses'][-1]['data']['results'][0]['quote']['ask_price']='.01'
        with self.assertRaisesRegex(ValueError,'checksum'): normalize(data,as_of=datetime.now(timezone.utc))

    def test_budget_blocks(self):
        result=preview(snapshot(),as_of=datetime.now(timezone.utc),long_id='650',fee_per_spread='10',slippage_per_spread='1')
        self.assertEqual(result['status'],'blocked')
        self.assertEqual(result['simulated_fills'],0)

    def test_transport_refuses_order_tools(self):
        session=ReadOnlySession()
        with patch.object(session,'request') as request:
            with self.assertRaises(ValueError):session.call('place_option_order',{})
            request.assert_not_called()

if __name__=='__main__':unittest.main()
