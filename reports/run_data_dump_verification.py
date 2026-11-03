"""Bounded ingestion-only verification; suppresses evaluation and trade dispatch."""
import asyncio,json,sys,time,logging
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
import pandas as pd
import pyarrow.parquet as pq
from Terminal.runtime_io import acquire_singleton,atomic_json
from Terminal.collector_lock import BrowserLock
from Terminal import direct_footprint_collector as footprint
from Terminal import whale_orders_collector as whales
from Terminal import liquidation_map_collector as maps
from Terminal import coinglass_tv_liquidation_collector as tv
from Terminal import coinalyze_rest_collector as rest
DB=ROOT/'Database'
REPORT=ROOT/'reports/data_dump_verification_latest.json'
class DataOnly:
    def evaluate_new_signals(self, signals):return []
for module in (footprint,tv,rest):module.evaluator=DataOnly()
class Redact(logging.Filter):
    def filter(self,record):
        record.msg=record.getMessage().replace(rest.API_KEY,'[REDACTED]')
        record.args=()
        return True
for logger in (logging.getLogger(),rest.logger):
    for handler in logger.handlers:handler.addFilter(Redact())
def inventory():
    return {str(p.relative_to(DB)): {'mtime_ns':p.stat().st_mtime_ns,'bytes':p.stat().st_size} for p in DB.rglob('*.parquet')}
result={'started_at_utc':datetime.now(timezone.utc).isoformat(),'mode':'DATA_ONLY_NO_TRADE_DISPATCH','collectors':[]}
async def footprint_once():
    tab, url=footprint.open_coinglass_footprint_tab()
    if not tab or not url:raise RuntimeError('Footprint tab unavailable')
    try:
        await asyncio.sleep(3)
        async with footprint.websockets.connect(url,open_timeout=5,close_timeout=2,max_size=50000000) as ws:
            return await footprint.footprint_sweep_cycle(ws)
    finally:footprint.close_tab(tab)
async def tv_once():
    tab=tv.get_tv_tab()
    if not tab:raise RuntimeError('TV tab unavailable')
    return await tv.sweep_all_liquidations(tab)
async def run():
    for name,module,call,browser,deadline in [
        ('footprint',footprint,footprint_once,True,480),
        ('whale_orders',whales,whales.sweep_all_cryptos,True,240),
        ('liquidation_maps',maps,maps.execute_liquidation_sweep,True,240),
        ('tv_liquidations',tv,tv_once,True,480),
        ('coinalyze_rest',rest,rest.sweep_coinalyze,False,300)]:
        before=inventory();start=time.monotonic();entry={'collector':name,'started_at_utc':datetime.now(timezone.utc).isoformat()}
        print('START '+name,flush=True)
        try:
            acquire_singleton(module.__name__.split('.')[-1],DB/'.locks')
            if browser:
                with BrowserLock('DumpVerification-'+name) as lock:
                    if not lock.acquired:raise RuntimeError('SKIPPED_LOCK_CONTENTION')
                    await asyncio.wait_for(call(),deadline)
            else:await asyncio.wait_for(call(),deadline)
            entry['run_status']='RETURNED'
        except Exception as exc:entry.update(run_status='ERROR',error=type(exc).__name__+': '+str(exc).replace(rest.API_KEY,'[REDACTED]'))
        after=inventory();entry['updated_files']=[k for k,v in after.items() if before.get(k)!=v]
        entry['seconds']=round(time.monotonic()-start,2)
        result['collectors'].append(entry);atomic_json(REPORT,result)
        print('END '+json.dumps(entry),flush=True)
    files=[];now=time.time()
    for p in sorted(DB.rglob('*.parquet')):
        row={'file':str(p.relative_to(DB)),'file_age_s':round(now-p.stat().st_mtime,1)}
        try:
            pf=pq.ParquetFile(p);row['rows']=pf.metadata.num_rows
            columns=[c for c in ('snapshot_time_utc','dumped_at_utc','close_time_ms','open_time_ms','datetime_utc','updated_at_utc') if c in pf.schema_arrow.names]
            if columns:
                frame=pq.read_table(p,columns=columns).to_pandas()
                for col in columns:
                    values=frame[col].dropna()
                    if len(values):row['latest_'+col]=str(values.max())
            # Read every column to detect encoding/schema corruption, not just footer damage.
            pq.read_table(p)
            row['read_status']='OK'
        except Exception as exc:row.update(read_status='ERROR',error=str(exc))
        files.append(row)
    result.update(finished_at_utc=datetime.now(timezone.utc).isoformat(),files=files)
    atomic_json(REPORT,result)
    print('FINISHED report='+str(REPORT)+' files='+str(len(files))+' unreadable='+str(sum(x['read_status']!='OK' for x in files)),flush=True)
asyncio.run(run())
