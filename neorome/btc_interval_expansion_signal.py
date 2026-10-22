"""Completed 15-minute Stoch(14,3,3) crossover; original 30m rules."""
from decimal import Decimal,InvalidOperation

def number(x):
    if isinstance(x,bool):raise ValueError('boolean numeric value')
    try:d=Decimal(str(x))
    except (InvalidOperation,ValueError):raise ValueError('invalid numeric value') from None
    if not d.is_finite():raise ValueError('nonfinite value')
    return d

def stochastic(bars):
    raw=[];ks=[];ds=[]
    def smooth(values,i):
        w=values[i-2:i+1]
        return sum(w,Decimal(0))/3 if i>=2 and len(w)==3 and all(x is not None for x in w) else None
    for i,b in enumerate(bars):
        w=bars[i-13:i+1] if i>=13 else []
        low=min((x['low'] for x in w),default=None);high=max((x['high'] for x in w),default=None)
        raw.append(100*(b['close']-low)/(high-low) if low is not None and high>low else None)
        ks.append(smooth(raw,i));ds.append(smooth(ks,i))
    return ks,ds

def evaluate(bars,*,observed_at,target_close):
    out={'stochastic_settings':[14,3,3],'target_close':None,'qualifies':False,'gap_qualifies':False,'direction':None,'previous_k':None,'previous_d':None,'k':None,'d':None,'directed_gap':None,'reasons':[]}
    try:
        now=number(observed_at);target=number(target_close)
        if target!=int(target) or int(target)%900:raise ValueError('unaligned target')
        out['target_close']=int(target)
        if not 0<=now-target<=90:raise ValueError('signal outside 90-second window')
        if not isinstance(bars,list) or not 19<=len(bars)<=300:raise ValueError('bar count outside range')
        clean=[];last=None;current=int(now//900)*900
        for b in bars:
            ts=number(b['timestamp']);close_time=number(b['bar_close_timestamp'])
            if ts!=int(ts) or int(ts)%900 or close_time!=ts+900 or ts>current:raise ValueError('invalid bar timestamp')
            if last is not None and ts-last!=900:raise ValueError('bar gap or disorder')
            last=ts
            p={k:number(b[k]) for k in ('open','high','low','close')}
            if min(p.values())<=0 or p['low']>min(p['open'],p['close']) or p['high']<max(p['open'],p['close']) or p['low']>p['high']:raise ValueError('invalid OHLC')
            if close_time<=target:clean.append(p)
        if last is None or not any(number(b['bar_close_timestamp'])==target for b in bars) or len(clean)<19:raise ValueError('latest completed bar absent')
        k,d=stochastic(clean);pk,pd,ck,cd=k[-2],d[-2],k[-1],d[-1]
        if any(x is None for x in (pk,pd,ck,cd)):raise ValueError('undefined stochastic')
        out.update(previous_k=float(pk),previous_d=float(pd),k=float(ck),d=float(cd))
        side='yes' if pk<=20 and ck>pk and pk<=pd and ck>cd else 'no' if pk>=80 and ck<pk and pk>=pd and ck<cd else None
        if side:
            gap=ck-cd if side=='yes' else cd-ck
            out.update(qualifies=True,direction=side,directed_gap=float(gap),gap_qualifies=0<gap<=5)
        else:out['reasons'].append('no_closed_15m_extreme_fresh_cross')
    except (ValueError,TypeError,KeyError,OverflowError) as exc:out['reasons'].append(str(exc))
    return out
