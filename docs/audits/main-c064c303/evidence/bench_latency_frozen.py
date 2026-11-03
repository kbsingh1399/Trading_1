import sys, time, json
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, '/home/user/Trading_2/.audit/main')
import Terminal.olap_engine as oe
import Terminal.auction_indicators as ai
import Terminal.regime_detector as rd
ASOF=datetime(2026,11,3,10,40,47,tzinfo=timezone.utc)
time.time=lambda: ASOF.timestamp()
class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return ASOF if tz is None else ASOF.astimezone(tz)
oe.datetime=FrozenDateTime
ai.datetime=FrozenDateTime
if hasattr(rd,'datetime'): rd.datetime=FrozenDateTime
from Terminal.universal_footprint_engine import UniversalFootprintEngine
import pandas as pd

def pct(xs,p):
    xs=sorted(xs); k=(len(xs)-1)*p/100; f=int(k); c=min(f+1,len(xs)-1)
    return xs[f]+(xs[c]-xs[f])*(k-f)
syms=['BTC','ETH','SOL','XRP','DOGE','BNB','ADA','LINK','AVAX','SUI','NEAR','HYPE']
eng=oe.OlapEngine()
res={'as_of_utc':ASOF.isoformat(),'note':'datetime.now frozen to main commit time for validity; latency measured live on stored parquet','symbols':syms,'queries':{}}
for name,fn in [('whale',eng.query_whale_walls),('liquidation',eng.query_liquidation_confluence),('cvd',eng.query_cvd_trend),('auction',eng.query_auction_profile)]:
    lat=[]; valid=0; errors={}
    for s in syms:
        t=time.perf_counter(); r=fn(s); lat.append((time.perf_counter()-t)*1000)
        if r.get('data_valid'): valid+=1
        else: errors[s]=r.get('error')
    res['queries'][name]={'p50_ms':round(pct(lat,50),2),'p95_ms':round(pct(lat,95),2),'max_ms':round(max(lat),2),'valid':valid,'n':len(syms),'errors':errors}
t=time.perf_counter(); scan=eng.scan_all_confluences(syms[:8]); scan_ms=(time.perf_counter()-t)*1000
res['scan_all_confluences']={'symbols':8,'elapsed_ms':round(scan_ms,2),'results':[{k:r.get(k) for k in ('symbol','confluence_score','bias','poc_price')} for r in scan]}
eng.close()
raw=pd.read_parquet('/home/user/Trading_2/.audit/main/Database/Candle_Data/BTC.parquet')
bars=raw.to_dict('records')
ufe=UniversalFootprintEngine()
lat=[]; sig_counts=[]
for _ in range(10):
    t=time.perf_counter(); df,sigs=ufe.analyze_bars(bars=bars,symbol='BTC',asset_class='Crypto',mt5_symbol='BTCUSDT',rolling_window=300,inspect_last_n_bars=2); lat.append((time.perf_counter()-t)*1000); sig_counts.append(len(sigs))
res['universal_footprint_btc_300_bars']={'runs':10,'p50_ms':round(pct(lat,50),2),'max_ms':round(max(lat),2),'signals_last_run':sig_counts[-1]}
Path('/home/user/Trading_2/.audit/latency_benchmark_at_commit.json').write_text(json.dumps(res,indent=2))
print(json.dumps({k:v for k,v in res['queries'].items()},indent=2))
