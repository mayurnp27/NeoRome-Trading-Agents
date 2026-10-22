"""Causal minute-bar entry diagnostics. Never creates orders or ledger fills.

These are price-path hypotheses, not executable backtests: token controls,
quotes, wallet provenance and discovery-time data availability are external.
"""
from math import isfinite


def replay(rows, *, cutoff, earliest, mode='breakout', target=.5, stop=.25,
           cost_bps=100):
    if mode not in ('breakout', 'reclaim', 'first_seen'):
        raise ValueError('unknown entry hypothesis')
    if not 0 < stop < 1 or target <= 0 or not 0 <= cost_bps < 10000:
        raise ValueError('invalid exit or cost assumptions')
    bars = sorted([tuple(r) for r in rows if r[0] + 60 <= cutoff])
    for i, r in enumerate(bars):
        if len(r) != 6 or any(not isfinite(float(v)) for v in r):
            raise ValueError('invalid candle')
        t, o, h, l, c, v = r
        if min(o, h, l, c) <= 0 or v < 0 or l > min(o,c) or h < max(o,c):
            raise ValueError('invalid candle geometry')
        if i and t <= bars[i-1][0]:
            raise ValueError('duplicate candle')
    entry = None
    for i in range(3, len(bars)):
        b, p, q, n = bars[i-3:i+1]
        # Only completed history before the next-minute entry; no gap bridging.
        if any(y[0]-x[0] != 60 for x,y in zip((b,p,q),(p,q,n))):
            continue
        if n[0] < earliest:
            continue
        breakout = q[4] > max(b[2], p[2]) and q[5] >= 1.2 * (b[5]+p[5])/2
        reclaim = p[4] <= .92*b[4] and q[4] >= .98*b[4] and q[4] >= 1.05*p[4]
        if mode == 'first_seen' or (mode == 'breakout' and breakout) or (mode == 'reclaim' and reclaim):
            entry = i
            break
    if entry is None:
        return {'status':'no_signal', 'mode':mode, 'execution_enabled':False}
    raw = bars[entry][1]; rate=cost_bps/10000
    paid = raw*(1+rate); floor=raw*(1-stop); ceiling=raw*(1+target)
    reason='terminal_mark'; exit_price=bars[-1][4]; exit_t=bars[-1][0]
    for i in range(entry,len(bars)):
        t,o,h,l,c,v=bars[i]
        if i>entry and t-bars[i-1][0] != 60:
            reason='data_gap'; exit_price=bars[i-1][4];exit_t=bars[i-1][0];break
        # Stop-first on ambiguous bars; gap-down exits use the worse open.
        if l <= floor:
            exit_price=min(o,floor);exit_t=t;reason='stop';break
        if h >= ceiling:
            exit_price=ceiling;exit_t=t;reason='target';break
    return {'status':'price_path_diagnostic','mode':mode,'entry_at':bars[entry][0],
            'exit_at':exit_t,'exit_reason':reason,'entry_reference':raw,
            'exit_reference':exit_price,'cost_bps_per_side':cost_bps,
            'return_pct':100*(exit_price*(1-rate)/paid-1),
            'execution_enabled':False,'historical_executability_verified':False}
