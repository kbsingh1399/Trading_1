import asyncio,json,sys,time
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from Terminal import coinalyze_rest_collector as rest,coinglass_tv_liquidation_collector as tv
from Terminal.collector_lock import BrowserLock
from Terminal.runtime_io import acquire_singleton,atomic_json
from Terminal.data_integrity_audit import audit_all_dumps
class Sink:
 def evaluate_new_signals(self,signals):return []
rest.evaluator=tv.evaluator=Sink()
async def fetch_tv():
 acquire_singleton('coinglass_tv_liquidation_collector',ROOT/'Database/.locks')
 with BrowserLock('FinalTVVerification') as lock:
  if not lock.acquired:raise RuntimeError('Browser busy')
  return await tv.sweep_all_liquidations(tv.get_tv_tab())
async def fetch_rest():
 acquire_singleton('coinalyze_rest_collector',ROOT/'Database/.locks')
 await rest.sweep_coinalyze()
async def run():
 results=await asyncio.gather(asyncio.wait_for(fetch_tv(),480),asyncio.wait_for(fetch_rest(),360),return_exceptions=True)
 report={'completed_at_utc':datetime.now(timezone.utc).isoformat(),'tv_assets':len(results[0]) if isinstance(results[0],dict) else 0,'errors':[str(r) for r in results if isinstance(r,BaseException)],'data_health':audit_all_dumps(),'rest_coverage':json.loads((ROOT/'Database/coinalyze_collection_health.json').read_text())}
 atomic_json(ROOT/'reports/ingestion_repair_verification.json',report)
 print('FINAL',json.dumps({k:v for k,v in report.items() if k not in ('data_health','rest_coverage')}),flush=True)
asyncio.run(run())
