"""Summarize capture quality; this does not estimate a trading edge."""
import collections,gzip,json,sys
from pathlib import Path

def summarize(root):
    counts=collections.Counter();sources=collections.Counter();times={};deltas=collections.defaultdict(list)
    books=collections.Counter();clock=[];flags=collections.Counter();incomplete=[];markets=set()
    for path in sorted(root.glob('*.jsonl.gz')):
        try:
            with gzip.open(path,'rt') as stream:
                for line in stream:
                    row=json.loads(line);counts[row['kind']]+=1
                    if row['kind']=='clock_audit':clock.append({k:row[k] for k in ('capture_utc_ns','offset_server_minus_local_ms','round_trip_ms','root_delay_seconds','root_dispersion_seconds')})
                    if row['kind']!='message':continue
                    n=row['normalized'];source=row['source'];channel=row['raw_public_payload'].get('type',source)
                    sources[channel]+=1;time=row['capture_utc_ns'];times.setdefault(source,[time,time])[1]=time
                    if n.get('raw_capture_minus_source_ms') is not None:deltas[channel].append(n['raw_capture_minus_source_ms'])
                    flags.update(row['integrity_flags'])
                    if source=='polymarket':
                        markets.add(n['market_slug']);books['two_sided' if n['two_sided'] else 'one_or_zero_sided']+=1
                        books['crossed']+=int(n['crossed']);books['bbo_changes']+=n.get('bbo_changed_since_received_snapshot') is True
                        books['open_two_sided_uncrossed']+=int(n['two_sided'] and not n['crossed'] and n.get('state')=='MARKET_STATE_OPEN')
        except EOFError:incomplete.append(path.name)
    def distribution(values):
        values=sorted(values)
        return {'n':len(values),'min':values[0],'median':values[len(values)//2],'p95':values[min(len(values)-1,int(len(values)*.95))],'max':values[-1]}
    return {'counts':dict(counts),'source_messages':dict(sources),'receive_windows_ns':times,'markets':sorted(markets),'books':dict(books),
            'raw_wall_clock_deltas_ms':{k:distribution(v) for k,v in deltas.items()},'clock_audits':clock,'integrity_flags':dict(flags),
            'currently_open_or_truncated_gzip':incomplete,'paired_feed_delivery_verified':all(sources[k]>0 for k in ('cfbenchmarks_value_5hz','polymarket')),
            'latency_or_profitability_established':False}

if __name__=='__main__':
    root=Path(sys.argv[1]);out=summarize(root);target=root/'quality-summary.json';target.write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps(out))
