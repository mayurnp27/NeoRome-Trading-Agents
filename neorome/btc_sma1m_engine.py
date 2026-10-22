"""Independent four-account hold and reversal-exit ledger for one-minute SMA tests."""
from __future__ import annotations
import json
from . import btc_bracket_engine as base
from .btc_trailing_engine import TrailingEngine

class SMAEngine(TrailingEngine):
    ARMS=(("1m-sma","1m-sma2-5-hold"),("1m-sma-stoch","1m-sma2-5-stoch-hold"),("1m-sma-reversal","1m-sma2-5-reversal"),("1m-sma-stoch-reversal","1m-sma2-5-stoch-reversal"))
    CONFIG={"version":1,"engine":"btc-sma1m-paper","initial_cash_cents":50000,"timeframes":[tf for tf,_ in ARMS],"arms":[a for _,a in ARMS],"chart_timeframe_seconds":60,"allocation_bps":2000,"sma_close_lengths":[2,5],"confirmed_stochastic":[5,3,3],"oversold":20,"overbought":80,"stochastic_require_both":True,"exit_policies":["hold_official_settlement","opposite_sma_cross_or_settlement"],"entry_window_seconds":30,"entry_buffer_cents":1,"exit_buffer_cents":1,"fee":"ceil(0.07*N*p*(1-p)*multiplier*100)"}

    def register_cohort(self,cohort):
        required={"cohort_id","timeframe","ticker","side","observed_at","signal_time","close_time","fee_multiplier","source_hash"}
        if not isinstance(cohort,dict) or set(cohort)!=required:raise base.BracketError('SMA cohort fields do not match protocol')
        cid,tf,ticker,side=(cohort[k] for k in ('cohort_id','timeframe','ticker','side'))
        if tf not in dict(self.ARMS) or side not in ('yes','no'):raise base.BracketError('invalid SMA strategy or side')
        if not all(isinstance(x,str) and 0<len(x)<=256 for x in (cid,ticker)):raise base.BracketError('invalid SMA cohort identity')
        observed,signal,close=(base._finite(cohort[k],k) for k in ('observed_at','signal_time','close_time'))
        if observed<self.started_at or observed<signal or observed>signal+30 or observed>=close:raise base.BracketError('SMA cohort is outside prospective window')
        try:multiplier=base.Decimal(cohort['fee_multiplier'])
        except (base.InvalidOperation,TypeError):raise base.BracketError('invalid fee multiplier') from None
        if not isinstance(cohort['fee_multiplier'],str) or not multiplier.is_finite() or not 0<multiplier<=1000000:raise base.BracketError('invalid fee multiplier')
        source_hash=base._hash(cohort['source_hash'],'source_hash')
        body={**cohort,'observed_at':observed,'signal_time':signal,'close_time':close,'fee_multiplier':str(multiplier),'source_hash':source_hash};payload=base._json(body)
        with self._tx():
            old=self.db.execute('select payload from cohorts where cohort_id=?',(cid,)).fetchone()
            if old:
                if old[0]!=payload:raise base.BracketError('cohort payload conflict')
                return
            bound=self.db.execute('select close_time from cohorts where ticker=? limit 1',(ticker,)).fetchone()
            if bound and bound[0]!=close:raise base.BracketError('market expiry conflict')
            arm=self.db.execute('select * from arms where timeframe=?',(tf,)).fetchone()
            blocker=self.db.execute("select value from controls where key='entry_blocker'").fetchone()
            pos=json.loads(arm['position']) if arm['position'] else None
            waiting_for_exit=pos is not None and tf.endswith('-reversal') and pos['side']!=side and pos.get('reversal_trigger') is not None and pos['ticker']==ticker
            reason='position_already_open' if pos is not None and not waiting_for_exit else blocker[0] if blocker and blocker[0] else None
            self.db.execute('insert into cohorts(cohort_id,timeframe,ticker,side,observed,signal,close_time,multiplier,source_hash,payload,state) values(?,?,?,?,?,?,?,?,?,?,?)',(cid,tf,ticker,side,observed,signal,close,str(multiplier),source_hash,payload,'waiting'))
            self._event(observed,'cohort',ticker,cid,source_hash=source_hash,timeframe=tf,side=side)
            if reason:self._skip(self.db.execute('select * from cohorts where cohort_id=?',(cid,)).fetchone(),observed,reason,source_hash)

    def expire(self,now):
        now=base._finite(now,'now')
        with self._tx():
            for c in self.db.execute("select * from cohorts where state='waiting'").fetchall():
                if now>=min(c['signal']+30,c['close_time']):self._skip(c,now,'entry_window_expired',c['source_hash'])

    def on_quote(self,ticker,quote):
        received,_,_=self._normalized_quote(quote);self.expire(received)
        return base.Engine.on_quote(self,ticker,quote)

    def on_reversal(self,side,observed_at,source_hash,signal_time):
        if side not in ('yes','no'):raise base.BracketError('invalid reversal side')
        at=base._finite(observed_at,'observed_at');signal_time=base._finite(signal_time,'signal_time');source_hash=base._hash(source_hash,'source_hash')
        if not self.started_at<=signal_time<=at<=signal_time+30:raise base.BracketError('reversal outside prospective window')
        with self._tx():
            for arm in self.db.execute("select * from arms where position is not null and timeframe like '%-reversal'").fetchall():
                pos=json.loads(arm['position'])
                if pos['side']==side or at>=pos['close_time'] or pos.get('reversal_trigger'):continue
                pos['reversal_trigger']={'at':at,'signal_time':signal_time,'source_hash':source_hash,'new_side':side}
                self.db.execute('update arms set position=? where arm_id=?',(base._json(pos),arm['arm_id']))
                self._event(at,'reversal_trigger',pos['ticker'],pos['cohort_id'],arm['arm_id'],source_hash,new_side=side,signal_time=signal_time)

    def _try_entries(self,c,at,source_hash,book,first_book):
        arm=self.db.execute('select * from arms where timeframe=?',(c['timeframe'],)).fetchone()
        if arm['position'] is not None and c['timeframe'].endswith('-reversal'):
            pos=json.loads(arm['position'])
            if pos.get('reversal_trigger') and pos['side']!=c['side'] and pos['ticker']==c['ticker']:return
        return base.Engine._try_entries(self,c,at,source_hash,book,first_book)

    def _mark_and_manage(self,ticker,at,source_hash,book):
        base.Engine._mark_and_manage(self,ticker,at,source_hash,book)
        for arm in self.db.execute("select * from arms where position is not null and timeframe like '%-reversal'").fetchall():
            pos=json.loads(arm['position']);trigger=pos.get('reversal_trigger');used=set(pos.get('reversal_exit_hashes',[]))
            if pos['ticker']!=ticker or not trigger or at>=pos['close_time'] or at<trigger['at']+1 or source_hash==trigger['source_hash'] or source_hash in used:continue
            bids=book[pos['side']+'_bids']
            if not sum(x['quantity'] for x in bids):continue
            pos['reversal_exit_hashes']=sorted(used|{source_hash});self._exit(arm,pos,at,source_hash,bids)

    def status(self,now):
        result=base.Engine.status(self,now);result['config']=json.loads(base._json(self.CONFIG))
        for arm in result['arms']:arm['exit_policy']='opposite_sma_cross_or_settlement' if arm['timeframe'].endswith('-reversal') else 'hold_official_settlement'
        return result
