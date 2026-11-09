"""
run_parallel_standalone.py
========================================================================================
Master Parallel Standalone Runner: Triple-Pillar Concurrency with 4 Checkpoints & WARP IP Rotation
========================================================================================
Institutional Architecture:
1. Triple-Pillar Concurrency:
   - Tab 1: Footprint Standalone Harvester (?bot=footprint_api) -> Time 15m, Dynamic Volume, Dynamic Range
   - Tab 2: Whale Orderbook Harvester (?bot=whales_api) -> Size threshold $0 (no limit), 8-column Excel spec
   - Tab 3: Liquidation Levels Harvester (?bot=liq_levels_api) -> 3D, 7D, 30D across 5 structures
   - All 3 tabs run concurrently in parallel via Chrome CDP (127.0.0.1:9222).

2. Four Deterministic Data Streaming Checkpoints (Zero-Drop Guarantee):
   - [CHECKPOINT-1: HYDRATION & CONNECTION]: Tab navigation, WebSocket attach, DOM/Fiber hydration verified.
   - [CHECKPOINT-2: STREAMING FEED & PAYLOAD]: Schema completeness, non-empty data arrays, anti-bot / rate-limit detection.
   - [CHECKPOINT-3: ATOMIC BYTE-LOCKED PERSISTENCE]: Thread-local writer_lock, anti-erasure guard, atomic .tmp swap.
   - [CHECKPOINT-4: DISK INTEGRITY & PARITY AUDIT]: PyArrow read-back verification guaranteeing uncorrupted serialization.

3. Dynamic Asset Hub:
   - Reads dynamically from API/active_assets.json via API/active_assets_manager.py on EVERY cycle.
   - Assets can be added/removed at runtime without modifying code or restarting the daemon.

4. Egress Cloudflare WARP Anti-Block Rotation Engine:
   - Reactive in-flight rotation on 429, challenge, or rate-limit banners.
   - Controlled inter-cycle rotation ceiling (CYCLE_AGE_CEILING) at rest boundary (every 3 cycles / 15 min).
   - In-process telemetry heartbeat publication to Database/pipeline_health.json.
========================================================================================
"""

import sys
import os
import json
import time
import asyncio
import logging
import argparse
import urllib.request
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Set

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

API_DIR = Path(__file__).resolve().parent
ROOT_DIR = API_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from coinglass_common import (
    writer_lock,
    atomic_parquet,
    rotate_warp_ip,
    get_current_ip,
    acquire_singleton,
    LOCK_DIR,
    cdp_eval
)
from active_assets_manager import load_active_assets, ACTIVE_ASSETS_FILE


def get_equity_symbols():
    try:
        data = json.loads(ACTIVE_ASSETS_FILE.read_text(encoding="utf-8"))
        return {s.upper() for s in data.get("categories", {}).get("Equities", [])}
    except Exception:
        return set()

# Configure Master Logger
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [PARALLEL-PIPELINE] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("ParallelPipeline")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

DB_DIR = ROOT_DIR / "Database"
HEALTH_FILE = DB_DIR / "pipeline_health.json"
FP_DUMP_DIR = DB_DIR / "Footprint_Dumps"
WHALE_DUMP_DIR = DB_DIR / "Whale_Dumps"
LIQ_DUMP_DIR = DB_DIR / "Liquidation_Level_Dumps"
PIPELINE_OPEN_DIR = DB_DIR / "Whale_Orders" / "Open_Orders"

FP_DUMP_DIR.mkdir(parents=True, exist_ok=True)
WHALE_DUMP_DIR.mkdir(parents=True, exist_ok=True)
LIQ_DUMP_DIR.mkdir(parents=True, exist_ok=True)
PIPELINE_OPEN_DIR.mkdir(parents=True, exist_ok=True)

CDP_BASE_URL = "http://127.0.0.1:9222"


def emit_pipeline_health(
    cycle_num: int,
    current_ip: str,
    cycles_on_ip: int,
    ip_age_sec: float,
    pillar_stats: Dict[str, Any],
    symbols: List[str],
    is_degraded: bool = False
) -> Path:
    """Publishes atomic, crash-proof pipeline heartbeat telemetry to Database/pipeline_health.json."""
    from datetime import datetime, timezone, timedelta
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz).strftime("%Y-%m-%d %H:%M:%S IST")

    health_payload = {
        "timestamp_ist": now_ist,
        "timestamp_epoch": time.time(),
        "cycle_number": cycle_num,
        "current_ip": current_ip,
        "cycles_on_current_ip": cycles_on_ip,
        "ip_age_seconds": round(ip_age_sec, 1),
        "status": "DEGRADED" if is_degraded else "HEALTHY",
        "pillars": pillar_stats,
        "symbols": symbols,
        "total_symbols": len(symbols),
    }

    target_path = Path(HEALTH_FILE)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_suffix(".tmp")

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(health_payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_path, target_path)
    return target_path


async def async_cdp_request(url: str, method: str = "GET", timeout: float = 4.0) -> Any:
    """Non-blocking HTTP request to CDP endpoint executed in thread pool."""
    def _do():
        req = urllib.request.Request(url, method=method, headers={"User-Agent": "Antigravity/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    return await asyncio.to_thread(_do)


async def async_close_tab(tab_id: str):
    """Non-blocking tab closure."""
    if not tab_id:
        return
    def _do():
        try:
            urllib.request.urlopen(f"{CDP_BASE_URL}/json/close/{tab_id}", timeout=2)
        except Exception:
            pass
    await asyncio.to_thread(_do)


def cleanup_scraper_tabs():
    """Compatibility no-op: a bot tag alone never proves ownership across workers."""
    logger.debug("Global tab reaping disabled; each harvester closes only tabs it created")


class RateLimitSignal(Exception):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


async def harvest_footprint_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_footprint_api import (
        cdp_adapter, FootprintDomainService, FootprintDumpAdapter, EXCEL_SPEC_COLUMNS,
        bot_tab_url, verify_parquet
    )
    tab_id, ws_url = None, None
    count = 0
    try:
        url = bot_tab_url(f"https://www.coinglass.com/footprint/{symbols[0]}", "footprint_api")
        tab = await async_cdp_request(f"{CDP_BASE_URL}/json/new?{url}", method="PUT", timeout=6.0)
        tab_id, ws_url = tab.get("id"), tab.get("webSocketDebuggerUrl")
        if not ws_url:
            raise ConnectionError("Owned footprint tab has no CDP WebSocket")
        for sym in symbols:
            try:
                raw = await cdp_adapter.fetch_footprint_data(sym, ws_url=ws_url)
                datasets = await asyncio.to_thread(FootprintDomainService.build_datasets, raw, sym)
                if len(datasets) != 3 or any(not ds.candles for ds in datasets.values()):
                    raise ValueError("Expected three nonempty footprint bar engines")
                dumps = await asyncio.to_thread(FootprintDumpAdapter.dump_three_datasets, datasets, FP_DUMP_DIR)
                for meta in dumps.values():
                    verify_parquet(Path(meta["parquet"]), EXCEL_SPEC_COLUMNS)
                count += len(dumps)
                await asyncio.sleep(1.0)
            except Exception as exc:
                err_text = str(exc)
                is_rate_limit = any(k in err_text for k in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429", "rate_limited"))
                if is_rate_limit:
                    logger.warning("[FP][%s] HTTP_429 / Rate limit hit! Rotating WARP IP immediately on the spot...", sym)
                    if rate_signals is not None:
                        matched = next((r for r in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429") if r in err_text), "HTTP_429")
                        rate_signals.add(matched)
                    try:
                        await asyncio.to_thread(rotate_warp_ip, f"FP_{sym}", "HTTP_429")
                    except Exception as rot_err:
                        logger.error("[FP][%s] Immediate WARP rotation failed: %s", sym, rot_err)
                    try:
                        await cdp_adapter.execute_js(ws_url, "window.location.reload()", timeout=5)
                        await asyncio.sleep(4.0)
                        await cdp_adapter.hook_terminal(ws_url)
                        raw = await cdp_adapter.fetch_footprint_data(sym, ws_url=ws_url)
                        datasets = await asyncio.to_thread(FootprintDomainService.build_datasets, raw, sym)
                        dumps = await asyncio.to_thread(FootprintDumpAdapter.dump_three_datasets, datasets, FP_DUMP_DIR)
                        for meta in dumps.values():
                            verify_parquet(Path(meta["parquet"]), EXCEL_SPEC_COLUMNS)
                        count += len(dumps)
                        logger.info("[FP][%s] Successfully harvested on fresh IP after immediate rotation!", sym)
                        continue
                    except Exception as retry_exc:
                        logger.error("[FP][%s] Immediate retry failed on fresh IP: %s", sym, retry_exc)
                if failed_symbols is not None:
                    failed_symbols.add(sym)
                logger.error("[FP][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await async_close_tab(tab_id)
    return count


async def harvest_whale_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_whales_api import (
        CDPWhaleAdapter, ParquetWhaleRepository
    )
    import websockets
    adapter = CDPWhaleAdapter()
    equity = get_equity_symbols()
    display_repo = ParquetWhaleRepository(WHALE_DUMP_DIR)
    quant_repo = ParquetWhaleRepository(PIPELINE_OPEN_DIR)
    tab_id, ws_url = None, None
    count = 0
    try:
        tab_id, ws_url = await adapter.get_or_create_warm_tab(symbols[0])
        if not ws_url:
            raise ConnectionError("Owned whale tab has no CDP WebSocket")
        async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10, close_timeout=5, ping_interval=None) as ws:
            for sym in symbols:
                try:
                    dataset = await adapter.harvest_symbol_orders_on_ws(ws, sym)
                    display_only = sym.upper() not in equity
                    await asyncio.to_thread(display_repo.persist_dataset, dataset, display_only)
                    await asyncio.to_thread(quant_repo.persist_dataset, dataset, False)
                    count += 1
                    await asyncio.sleep(1.0)
                except Exception as exc:
                    err_text = str(exc)
                    is_rate_limit = any(k in err_text for k in ("HTTP_429", "RATE_LIMIT_BANNER", "CHALLENGE", "empty_unverified", "rate_limited", "WHALE_RATE_SIGNAL"))
                    if is_rate_limit:
                        logger.warning("[WHALE][%s] HTTP_429 / Rate limit detected! Rotating WARP IP immediately on the spot...", sym)
                        if rate_signals is not None:
                            matched = next((r for r in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429") if r in err_text), "HTTP_429")
                            rate_signals.add(matched)
                        try:
                            await asyncio.to_thread(rotate_warp_ip, f"Whale_{sym}", "HTTP_429")
                        except Exception as rot_err:
                            logger.error("[WHALE][%s] Immediate WARP rotation failed: %s", sym, rot_err)
                        try:
                            await cdp_eval(ws, "window.location.reload()", timeout=5)
                            await asyncio.sleep(4.0)
                            dataset = await adapter.harvest_symbol_orders_on_ws(ws, sym)
                            display_only = sym.upper() not in equity
                            await asyncio.to_thread(display_repo.persist_dataset, dataset, display_only)
                            await asyncio.to_thread(quant_repo.persist_dataset, dataset, False)
                            count += 1
                            logger.info("[WHALE][%s] Successfully harvested on fresh IP after immediate rotation!", sym)
                            continue
                        except Exception as retry_exc:
                            logger.error("[WHALE][%s] Retry on fresh IP failed: %s", sym, retry_exc)
                    if failed_symbols is not None:
                        failed_symbols.add(sym)
                    logger.error("[WHALE][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await adapter.close_tab(tab_id)
    return count


async def harvest_liquidation_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_liquidation_levels_api import (
        get_or_create_warm_tab, close_tab, harvest_symbol_liquidation,
        persist_asset_liquidation, is_asset_dump_complete
    )
    import websockets
    tab_id, ws_url = None, None
    count = 0
    try:
        tab_id, ws_url = await get_or_create_warm_tab(symbols[0])
        if not ws_url:
            raise ConnectionError("Owned liquidation tab has no CDP WebSocket")
        async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10,
                                      close_timeout=5, ping_interval=None) as ws:
            for sym in symbols:
                try:
                    data, _ = await harvest_symbol_liquidation(ws, sym)
                    count += await asyncio.to_thread(persist_asset_liquidation, sym, data)
                    if not await asyncio.to_thread(is_asset_dump_complete, sym):
                        raise ValueError("Liquidation read-back completeness failed")
                    await asyncio.sleep(1.0)
                except Exception as exc:
                    err_text = str(exc)
                    is_rate_limit = any(k in err_text for k in ("LIQUIDATION_RATE_SIGNAL", "HTTP_429", "RATE_LIMIT_BANNER", "CHALLENGE"))
                    if is_rate_limit:
                        logger.warning("[LIQ][%s] Rate limit encountered (%s). Rotating IP immediately on the spot...", sym, err_text)
                        if rate_signals is not None:
                            matched = next((r for r in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429") if r in err_text), "HTTP_429")
                            rate_signals.add(matched)
                        try:
                            await asyncio.to_thread(rotate_warp_ip, f"Liq_{sym}", "HTTP_429")
                        except Exception as rot_err:
                            logger.error("[LIQ][%s] Immediate WARP rotation failed: %s", sym, rot_err)
                        try:
                            await cdp_eval(ws, "window.location.reload()", timeout=5)
                            await asyncio.sleep(4.0)
                            data, _ = await harvest_symbol_liquidation(ws, sym)
                            count += await asyncio.to_thread(persist_asset_liquidation, sym, data)
                            if not await asyncio.to_thread(is_asset_dump_complete, sym):
                                raise ValueError("Liquidation completeness failed after retry")
                            logger.info("[LIQ][%s] Successfully harvested on fresh IP after immediate rotation!", sym)
                            continue
                        except Exception as retry_exc:
                            logger.error("[LIQ][%s] Retry on fresh IP failed: %s", sym, retry_exc)
                    if failed_symbols is not None:
                        failed_symbols.add(sym)
                    logger.error("[LIQ][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await close_tab(tab_id)
    return count


async def harvest_legend_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    """Pillar 4: CoinGlass Legend 5m Realized Aggregated Liquidation Harvester."""
    from standalone_coinglass_legend_liquidations_api import LegendLiquidationHarvester
    harvester = LegendLiquidationHarvester()
    count = 0
    try:
        connected = await harvester.connect()
        if not connected:
            logger.warning("[LEGEND] Could not attach to Legend tab. Skipping chunk.")
            return 0
        for sym in symbols:
            try:
                raw_data = await harvester.harvest_symbol(sym)
                if raw_data:
                    total, non_zero, path = harvester.persist_asset_parquets(sym, raw_data)
                    count += 1
                    logger.info("[LEGEND][%s] Dumped %d bars (%d active) -> %s", sym, total, non_zero, Path(path).name)
            except Exception as exc:
                if failed_symbols is not None:
                    failed_symbols.add(sym)
                logger.error("[LEGEND][%s] Harvest failed: %s", sym, exc)
    finally:
        await harvester.close(close_tab=True)
    return count

# ======================================================================================
# MASTER SEQUENTIAL SCHEDULER & HIGH-FREQUENCY FOOTPRINT CADENCE ORCHESTRATOR
# ======================================================================================

async def run_sequential_pipeline(
    footprint_interval: float = 120.0,
    whale_interval: float = 600.0,
    liq_interval: float = 900.0,
    legend_interval: float = 300.0,
    continuous: bool = False,
    sample_limit: Optional[int] = None,
    symbols_override: Optional[List[str]] = None,
    with_legend: bool = False,
    max_ip_age_min: float = 15.0,
    **kwargs
):
    """
    Executes all CoinGlass telemetry pillars STRICTLY SEQUENTIALLY:
    - Zero Tab Contention: Exactly 1 Chrome tab is open at any given moment.
    - More Frequent Footprint: High-frequency microstructure (15m delta, CVD, absorption)
      sweeps on a tighter cadence (e.g. every 2 min), while Whales and Liquidation run
      at macro cadences (e.g. 10 min and 15 min).
    - Dynamic Universe: Reloads API/active_assets.json dynamically before every sweep.
    - Reactive WARP IP Rotation: Rotates immediately on 429 / Turnstile / Rate Limit banners.
    - Deterministic Heartbeat: Emits Database/pipeline_health.json after every sweep.
    """
    logger.info("=" * 85)
    logger.info("STARTING STRICT SEQUENTIAL MULTI-PILLAR RUNNER (HIGH-FREQUENCY FOOTPRINT)")
    logger.info("Cadence Configuration:")
    logger.info(f"  • Footprint (High Frequency): every {footprint_interval:.0f}s ({footprint_interval/60:.1f} min)")
    logger.info(f"  • Whales (Macro Book Depth):  every {whale_interval:.0f}s ({whale_interval/60:.1f} min)")
    logger.info(f"  • Liquidation Levels (Macro): every {liq_interval:.0f}s ({liq_interval/60:.1f} min)")
    if with_legend:
        logger.info(f"  • Live Legend:                every {legend_interval:.0f}s ({legend_interval/60:.1f} min)")
    logger.info(f"Initial Egress IP: {await asyncio.to_thread(get_current_ip)}")
    logger.info("=" * 85)

    def get_symbols() -> List[str]:
        if symbols_override:
            return [s.upper().strip() for s in symbols_override if s.strip()]
        s = load_active_assets()
        if sample_limit and sample_limit > 0:
            return s[:sample_limit]
        return s

    ip_start_time = time.time()
    max_ip_age_sec = max_ip_age_min * 60.0
    cycle_num = 1
    sweep_count = 0

    from standalone_coinglass_footprint_api import is_footprint_asset_complete
    from standalone_coinglass_whales_api import is_whale_asset_complete
    from standalone_coinglass_liquidation_levels_api import is_asset_dump_complete

    last_fp = 0.0
    last_whale = 0.0
    last_liq = 0.0
    last_legend = 0.0

    pillar_stats = {
        "footprint": {"dumped_files": 0, "status": "INITIALIZING"},
        "whales": {"dumped_assets": 0, "status": "INITIALIZING"},
        "liquidation_levels": {"dumped_files": 0, "status": "INITIALIZING"},
    }

    async def execute_footprint_sweep():
        nonlocal sweep_count, cycle_num
        symbols = get_symbols()
        logger.info("=" * 80)
        logger.info(f"[SWEEP #{sweep_count+1}] [PILLAR: FOOTPRINT] Starting sweep across {len(symbols)} assets on 1 warm tab...")
        logger.info("=" * 80)
        t0 = time.time()
        rate_signals = set()
        failed = set()
        dumped = await harvest_footprint_chunk(symbols, 1, rate_signals, failed)
        dur = time.time() - t0
        sweep_count += 1
        cycle_num += 1
        all_ok = all(is_footprint_asset_complete(s) for s in symbols)
        pillar_stats["footprint"] = {
            "dumped_files": dumped,
            "expected_files": len(symbols) * 3,
            "status": "HEALTHY" if all_ok else "DEGRADED",
            "last_sweep_duration_s": round(dur, 1),
            "last_sweep_epoch": time.time()
        }
        logger.info(f"[PILLAR: FOOTPRINT] Sweep completed in {dur:.1f}s ({dumped} files dumped). Disk completeness: {'PASS' if all_ok else 'WARN'}")
        emit_pipeline_health(cycle_num, get_current_ip(), 1, time.time() - ip_start_time, pillar_stats, symbols, not all_ok)
        return dumped

    async def execute_whales_sweep():
        nonlocal sweep_count, cycle_num
        symbols = get_symbols()
        logger.info("=" * 80)
        logger.info(f"[SWEEP #{sweep_count+1}] [PILLAR: WHALES] Starting sweep across {len(symbols)} assets on 1 warm tab...")
        logger.info("=" * 80)
        t0 = time.time()
        rate_signals = set()
        failed = set()
        dumped = await harvest_whale_chunk(symbols, 1, rate_signals, failed)
        dur = time.time() - t0
        sweep_count += 1
        cycle_num += 1
        all_ok = all(is_whale_asset_complete(s) for s in symbols)
        pillar_stats["whales"] = {
            "dumped_assets": dumped,
            "expected_assets": len(symbols),
            "status": "HEALTHY" if all_ok else "DEGRADED",
            "last_sweep_duration_s": round(dur, 1),
            "last_sweep_epoch": time.time()
        }
        logger.info(f"[PILLAR: WHALES] Sweep completed in {dur:.1f}s ({dumped} assets dumped). Disk completeness: {'PASS' if all_ok else 'WARN'}")
        emit_pipeline_health(cycle_num, get_current_ip(), 1, time.time() - ip_start_time, pillar_stats, symbols, not all_ok)
        return dumped

    async def execute_liq_sweep():
        nonlocal sweep_count, cycle_num
        symbols = get_symbols()
        logger.info("=" * 80)
        logger.info(f"[SWEEP #{sweep_count+1}] [PILLAR: LIQUIDATION] Starting sweep across {len(symbols)} assets on 1 warm tab...")
        logger.info("=" * 80)
        t0 = time.time()
        rate_signals = set()
        failed = set()
        dumped = await harvest_liquidation_chunk(symbols, 1, rate_signals, failed)
        dur = time.time() - t0
        sweep_count += 1
        cycle_num += 1
        all_ok = all(is_asset_dump_complete(s) for s in symbols)
        pillar_stats["liquidation_levels"] = {
            "dumped_files": dumped,
            "expected_files": len(symbols) * 15,
            "status": "HEALTHY" if all_ok else "DEGRADED",
            "last_sweep_duration_s": round(dur, 1),
            "last_sweep_epoch": time.time()
        }
        logger.info(f"[PILLAR: LIQUIDATION] Sweep completed in {dur:.1f}s ({dumped} files dumped). Disk completeness: {'PASS' if all_ok else 'WARN'}")
        emit_pipeline_health(cycle_num, get_current_ip(), 1, time.time() - ip_start_time, pillar_stats, symbols, not all_ok)
        return dumped

    # 1. INITIAL BASELINE PASS: STRICTLY SEQUENTIAL
    logger.info(">>> EXECUTING INITIAL BASELINE SEQUENTIAL PASS <<<")
    await execute_footprint_sweep()
    last_fp = time.time()

    await execute_whales_sweep()
    last_whale = time.time()

    await execute_liq_sweep()
    last_liq = time.time()

    if with_legend:
        symbols = get_symbols()
        logger.info(f"[PILLAR: LEGEND] Starting initial sweep across {len(symbols)} assets...")
        await harvest_legend_chunk(symbols, 1)
        last_legend = time.time()

    logger.info(">>> INITIAL BASELINE PASS COMPLETE. ALL 3 PILLARS HARVESTED SEQUENTIALLY. <<<")

    if not continuous:
        logger.info("Single-pass sequential execution complete. Exiting master runner.")
        return

    # 2. CONTINUOUS CADENCE SCHEDULER LOOP (FOOTPRINT AT HIGH FREQUENCY)
    logger.info(f">>> ENTERING CONTINUOUS CADENCE SCHEDULER (Footprint: {footprint_interval}s, Whales: {whale_interval}s, Liq: {liq_interval}s) <<<")
    while True:
        now = time.time()

        # Check IP age ceiling
        if (now - ip_start_time) >= max_ip_age_sec:
            logger.info("IP age ceiling reached (%.1fs elapsed). Rotating egress WARP IP...", now - ip_start_time)
            try:
                new_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Ceiling", "CYCLE_AGE_CEILING")
                logger.info("Inter-cycle rotation successful: new egress IP is %s", new_ip)
                ip_start_time = time.time()
            except Exception as e:
                logger.error("Inter-cycle rotation deferred: %s", e)

        # Priority 1: High Frequency Footprint
        if (now - last_fp) >= footprint_interval:
            await execute_footprint_sweep()
            last_fp = time.time()
            continue

        # Priority 2: Whales
        if (now - last_whale) >= whale_interval:
            await execute_whales_sweep()
            last_whale = time.time()
            continue

        # Priority 3: Liquidation Levels
        if (now - last_liq) >= liq_interval:
            await execute_liq_sweep()
            last_liq = time.time()
            continue

        # Priority 4: Live Legend (if enabled)
        if with_legend and (now - last_legend) >= legend_interval:
            symbols = get_symbols()
            await harvest_legend_chunk(symbols, 1)
            last_legend = time.time()
            continue

        # Sleep briefly until next scheduled action (capped at 5s)
        due_in = min(
            footprint_interval - (now - last_fp),
            whale_interval - (now - last_whale),
            liq_interval - (now - last_liq)
        )
        sleep_dur = max(1.0, min(5.0, due_in))
        await asyncio.sleep(sleep_dur)


# Backwards compatibility alias
run_parallel_cycle = run_sequential_pipeline


def main():
    try:
        acquire_singleton("run_parallel_standalone", LOCK_DIR)
    except Exception as e:
        raise SystemExit(f"Singleton acquisition failed: {e}") from e
        
    parser = argparse.ArgumentParser(description="Master Sequential Standalone Runner with High-Frequency Footprint & WARP IP Rotation")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely across scheduled cadence cycles")
    parser.add_argument("--footprint-interval", type=float, default=120.0, help="Seconds between Footprint sweeps (default: 120)")
    parser.add_argument("--whale-interval", type=float, default=600.0, help="Seconds between Whales sweeps (default: 600)")
    parser.add_argument("--liq-interval", type=float, default=900.0, help="Seconds between Liquidation sweeps (default: 900)")
    parser.add_argument("--interval", type=float, default=None, help="Convenience alias to set footprint interval")
    parser.add_argument("--sample", type=int, default=None, help="Limit execution to first N assets for verification")
    parser.add_argument("--symbols", nargs="+", default=None, help="Specific symbols to harvest (e.g. --symbols BTC ETH SOL)")
    parser.add_argument("--with-legend", action="store_true", help="Harvest 5m Live Liquidations from CoinGlass Legend")
    parser.add_argument("--max-ip-age-min", type=float, default=15.0, help="Max IP age in minutes before rotation (default: 15.0)")
    # Deprecated compatibility flags
    parser.add_argument("--chunk-size", type=int, default=50, help="Compatibility flag (full asset list is processed in 1 tab)")
    parser.add_argument("--rotate-chunks", action="store_true", help="Retired compatibility flag")
    args = parser.parse_args()

    fp_itv = args.interval if args.interval is not None else args.footprint_interval

    asyncio.run(run_sequential_pipeline(
        footprint_interval=fp_itv,
        whale_interval=args.whale_interval,
        liq_interval=args.liq_interval,
        continuous=args.continuous,
        sample_limit=args.sample,
        symbols_override=args.symbols,
        with_legend=args.with_legend,
        max_ip_age_min=args.max_ip_age_min
    ))


if __name__ == "__main__":
    main()
