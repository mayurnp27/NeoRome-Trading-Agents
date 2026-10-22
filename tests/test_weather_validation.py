import base64,copy,hashlib,json,unittest
from datetime import datetime,timedelta,timezone
from neorome.weather_validation import register,pair,summarize,digest,terms
from neorome.weather_vintage import POINT,normalize
UTC=timezone.utc
DAY='2026-09-23';EVENT='KXHIGHNY-26SEP23'
H='https://api.weather.gov/gridpoints/OKX/33,37/forecast/hourly'


def raw(body,start,end,**kw):
 b=json.dumps(body).encode()
 return dict(raw_response=body,raw_response_b64=base64.b64encode(b).decode(),source_hash=hashlib.sha256(b).hexdigest(),requested_at=start,received_at=end,**kw)


def fixture():
 start=datetime(2026,9,23,4,tzinfo=UTC)
 body={'properties':{'updateTime':'2026-09-23T00:00:00Z','periods':[{'startTime':(start+timedelta(hours=i)).isoformat(),'endTime':(start+timedelta(hours=i+1)).isoformat(),'temperature':70+i%4,'temperatureUnit':'F'} for i in range(24)]}}
 captures=[]
 for b,url,a,z in [({'properties':{'forecastHourly':H}},POINT,'2026-09-23T00:00:00Z','2026-09-23T00:00:01Z'),(body,H,'2026-09-23T00:00:02Z','2026-09-23T00:00:03Z')]:
  c=raw(b,a,z);captures.append({'url':url,'status':200,'raw_base64':c['raw_response_b64'],'sha256':c['source_hash'],'requested_at':a,'received_at':z})
 forecast={'source_kind':'observed','error':None,'research_point':{'latitude':40.7812,'longitude':-73.9665},'captures':captures,'normalized':normalize(body,'2026-09-23T00:00:03Z')}
 rows=[{'ticker':EVENT+'-B'+str(i),'event_ticker':EVENT,'status':'active','market_type':'binary','notional_value_dollars':'1','rules_primary':'The Weather Company CLINYC test bin '+str(i),'rules_secondary':'fixture','open_time':'2026-09-22T14:00:00Z','close_time':'2026-09-24T04:00:00Z'} for i in range(6)]
 def market_capture(rows,status,a,z):
  p={'cursor':'','markets':rows};c=raw(p,a,z,status=status,query={'series_ticker':'KXHIGHNY','status':status},markets=rows)
  return {'source_kind':'observed','series_ticker':'KXHIGHNY','venue':'kalshi_public','collected_at':z,'responses':[c]}
 opened=market_capture(rows,'open','2026-09-22T23:20:00Z','2026-09-22T23:20:01Z')
 closed=copy.deepcopy(rows)
 for i,m in enumerate(closed):m.update(status='finalized',result='yes' if i==2 else 'no',settlement_value_dollars='1' if i==2 else '0',expiration_value='74',settlement_ts='2026-09-24T12:00:00Z')
 settled=market_capture(closed,'settled','2026-09-24T12:20:00Z','2026-09-24T12:20:01Z')
 return forecast,opened,settled


def alter(capture,fn):
 c=capture['responses'][0];fn(c['markets']);p={'cursor':'','markets':c['markets']};b=json.dumps(p).encode();c.update(raw_response=p,raw_response_b64=base64.b64encode(b).decode(),source_hash=hashlib.sha256(b).hexdigest())


class WeatherValidationTests(unittest.TestCase):
 def reg(self,f,o):return register(f,o,target_date=DAY,registered_at='2026-09-23T00:10:00Z')
 def test_prospective_one_event_not_six_samples(self):
  f,o,s=fixture();r=self.reg(f,o);p=pair(r,f,o,s)
  self.assertEqual(p['residual_f'],'1');self.assertFalse(p['calibration_eligible'])
  self.assertEqual(summarize([p,p])['events'],1)
  conflict=dict(p,outcome_sha256='a'*64);conflict.pop('pair_sha256');conflict['pair_sha256']=digest(conflict)
  with self.assertRaisesRegex(ValueError,'Conflicting'):summarize([p,conflict])
 def test_raw_and_normalized_forecast_tamper_rejected(self):
  for kind in ('raw','normalized'):
   f,o,s=fixture()
   if kind=='raw':f['captures'][1]['raw_base64']=base64.b64encode(b'{}').decode()
   else:f['normalized']['periods'][0]['temperature_f']=999
   with self.assertRaises(ValueError):self.reg(f,o)
 def test_retrospective_registration_and_wrong_point_blocked(self):
  f,o,s=fixture()
  with self.assertRaises(ValueError):register(f,o,target_date=DAY,registered_at='2026-09-23T04:10:00Z')
  f['research_point']['latitude']=42
  with self.assertRaises(ValueError):self.reg(f,o)
 def test_incomplete_or_changed_contract_set_blocks(self):
  for fn in (lambda rows:rows.pop(),lambda rows:rows[0].update(rules_secondary='changed'),lambda rows:rows[0].update(close_time='2026-09-25T04:00:00Z')):
   f,o,s=fixture();r=self.reg(f,o);alter(s,fn)
   with self.assertRaises(ValueError):pair(r,f,o,s)
 def test_nonfinal_provisional_nonbinary_and_multiple_yes_blocked(self):
  for fields in ({'status':'settled'},{'is_provisional':True},{'settlement_value_dollars':'.4'},{'result':'yes','settlement_value_dollars':'1'},{'settlement_ts':'2026-09-25T00:00:00Z'}):
   f,o,s=fixture();r=self.reg(f,o);alter(s,lambda rows:rows[0].update(fields))
   with self.assertRaises(ValueError):pair(r,f,o,s)
 def test_registration_and_market_projection_hashes_checked(self):
  f,o,s=fixture();r=self.reg(f,o);r['predictor']['maximum_hourly_predictor_f']='999'
  with self.assertRaises(ValueError):pair(r,f,o,s)
  f,o,s=fixture();o['responses'][0]['markets'][0]['rules_secondary']='tamper'
  with self.assertRaises(ValueError):self.reg(f,o)

 def test_reviewed_mapping_is_frozen_into_new_pair_only(self):
  f,o,s=fixture();rows=o['responses'][0]['markets']
  body={'version':'weather-station-settlement-map-v1','station_identity':'CLINYC-reviewed-v1',
        'source_identity':{'provider':'The Weather Company','station':'CLINYC'},'terms_schema':'kalshi-weather-terms-v1','cutoff_policy':'reviewed-close-time-v1'}
  mapping={**body,'mapping_sha256':digest(body),'contract_definitions':[
   {'ticker':m['ticker'],'terms_sha256':terms(m),'cutoff_at':m['close_time'],'outcome_rule':'inclusive_range','floor_f':str(70+i),'cap_f':str(70+i)} for i,m in enumerate(rows)]}
  r=register(f,o,target_date=DAY,registered_at='2026-09-23T00:10:00Z',calibration_mapping=mapping)
  p=pair(r,f,o,s)
  self.assertTrue(p['station_mapping_certified']);self.assertEqual(len(p['contract_definitions']),6)

if __name__=='__main__':unittest.main()
