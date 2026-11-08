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

4. Reactive-Only Cloudflare WARP Anti-Block Rotation Engine:
   - Scheduled chunk/cycle rotation is permanently disabled.
   - Only HTTP 429, challenge, or rate-limit evidence may trigger rotation.
   - Reactive In-Flight Failover: Immediate IP rotation and retry if 429 / 'failed to load' is encountered.
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
    LOCK_DIR
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
FP_DUMP_DIR = DB_DIR / "Footprint_Dumps"
WHALE_DUMP_DIR = DB_DIR / "Whale_Dumps"
LIQ_DUMP_DIR = DB_DIR / "Liquidation_Level_Dumps"
PIPELINE_OPEN_DIR = DB_DIR / "Whale_Orders" / "Open_Orders"

FP_DUMP_DIR.mkdir(parents=True, exist_ok=True)
WHALE_DUMP_DIR.mkdir(parents=True, exist_ok=True)
LIQ_DUMP_DIR.mkdir(parents=True, exist_ok=True)
PIPELINE_OPEN_DIR.mkdir(parents=True, exist_ok=True)

CDP_BASE_URL = "http://127.0.0.1:9222"


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


async def harvest_footprint_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None) -> int:
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
            except Exception as exc:
                if rate_signals is not None:
                    for reason in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429"):
                        if reason in str(exc):
                            rate_signals.add(reason)
                logger.error("[FP][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await async_close_tab(tab_id)
    return count


async def harvest_whale_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None) -> int:
    from standalone_coinglass_whales_api import (
        CDPWhaleAdapter, ParquetWhaleRepository, bot_tab_url, cdp_eval
    )
    import websockets
    adapter = CDPWhaleAdapter()
    equity = get_equity_symbols()
    display_repo = ParquetWhaleRepository(WHALE_DUMP_DIR)
    quant_repo = ParquetWhaleRepository(PIPELINE_OPEN_DIR)
    tab_id, ws_url = None, None
    count = 0
    try:
        tab_id, ws_url = await adapter.open_whale_tab(symbols[0])
        if not ws_url:
            raise ConnectionError("Owned whale tab has no CDP WebSocket")
        async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10,
                                      close_timeout=5, ping_interval=None) as ws:
            for idx, sym in enumerate(symbols):
                try:
                    if idx:
                        url = bot_tab_url(f"https://www.coinglass.com/large-orderbook-statistics/{sym}", "whales_api")
                        await cdp_eval(ws, f"window.location.href={json.dumps(url)}", timeout=5)
                    dataset = await adapter.extract_on_ws(ws, sym)
                    display_only = sym.upper() not in equity
                    await asyncio.to_thread(display_repo.persist_dataset, dataset, display_only)
                    await asyncio.to_thread(quant_repo.persist_dataset, dataset, display_only)
                    count += 1
                except Exception as exc:
                    text = str(exc)
                    if rate_signals is not None:
                        for reason in ("CHALLENGE", "RATE_LIMIT_BANNER", "HTTP_429"):
                            if reason in text:
                                rate_signals.add(reason)
                    logger.error("[WHALE][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await adapter.close_tab(tab_id)
    return count


async def harvest_liquidation_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None) -> int:
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
                except Exception as exc:
                    if "LIQUIDATION_RATE_SIGNAL:" in str(exc) and rate_signals is not None:
                        rate_signals.add(str(exc).split("LIQUIDATION_RATE_SIGNAL:", 1)[1].strip())
                    logger.error("[LIQ][%s] Harvest failed: %s", sym, exc)
    finally:
        if tab_id:
            await close_tab(tab_id)
    return count


# ======================================================================================
# MASTER SCHEDULER & CONCURRENCY ORCHESTRATOR
# ======================================================================================

async def run_parallel_cycle(chunk_size: int = 15, continuous: bool = False, delay_between_cycles: float = 300.0, sample_limit: Optional[int] = None, symbols_override: Optional[List[str]] = None, rotate_chunks: bool = False):
    """
    Executes complete cycles across active assets with:
    - Triple-Pillar Concurrency: Footprint, Whales, and Liquidation Levels run simultaneously in 3 parallel tabs.
    - Dynamic Universe: Reloads active assets from API/active_assets.json inside the while True: loop.
    - No scheduled IP rotation. Only observed blocking signals can request rotation.
    - Rotation is coordinated after all owned tabs close, never inside a pillar.
    - 4 Deterministic Checkpoints validating streaming feed through local disk parity.
    - Protected asyncio.gather(return_exceptions=True) isolating sub-pillar exceptions.
    """
    logger.info("=" * 85)
    logger.info(f"STARTING TRIPLE-PILLAR PARALLEL STANDALONE RUNNER")
    logger.info(f"Target Pillars: 1. Footprint (?bot=footprint_api) | 2. Whales (?bot=whales_api) | 3. Liquidation Levels (?bot=liq_levels_api)")
    if chunk_size <= 0 or delay_between_cycles < 0:
        raise ValueError("chunk_size must be positive and interval nonnegative")
    if rotate_chunks:
        logger.warning("--rotate-chunks is retired; scheduled rotations are disabled")
    logger.info("Initial egress IP: %s", await asyncio.to_thread(get_current_ip))
    logger.info("=" * 85)

    cycle_num = 1
    while True:
        t_cycle_start = time.time()
        logger.info(f">>>>>>>>>>>> BEGINNING HARVEST CYCLE #{cycle_num} <<<<<<<<<<<<")
        
        # Dynamically reload active assets per cycle
        if symbols_override:
            symbols = [s.upper().strip() for s in symbols_override if s.strip()]
        else:
            symbols = load_active_assets()
            if sample_limit and sample_limit > 0:
                symbols = symbols[:sample_limit]
        total_symbols = len(symbols)

        logger.info(f"Universe for Cycle #{cycle_num}: {total_symbols} Active Assets | Window Chunk Size: {chunk_size}")

        cleanup_scraper_tabs()
        total_fp_dumped = 0
        total_whale_dumped = 0
        total_liq_dumped = 0

        total_chunks = (total_symbols + chunk_size - 1) // chunk_size

        for c_idx in range(0, total_symbols, chunk_size):
            chunk = symbols[c_idx : c_idx + chunk_size]
            c_num = (c_idx // chunk_size) + 1
            
            logger.info("-" * 80)
            logger.info(f"CHUNK [{c_num}/{total_chunks}] ({len(chunk)} assets: {chunk[0]}..{chunk[-1]}) - 3 PARALLEL TABS ACTIVE")
            logger.info(f"Active Egress IP: {get_current_ip()}")
            logger.info("-" * 80)

            # RUN FOOTPRINT, WHALES, AND LIQUIDATION LEVELS IN PARALLEL TABS SIMULTANEOUSLY
            t_chunk_start = time.time()
            rate_signals = set()
            fp_task = asyncio.create_task(harvest_footprint_chunk(chunk, c_num, rate_signals))
            whale_task = asyncio.create_task(harvest_whale_chunk(chunk, c_num, rate_signals))
            liq_task = asyncio.create_task(harvest_liquidation_chunk(chunk, c_num, rate_signals))

            # Isolated gather protecting sibling pillars from cancellation
            results = await asyncio.gather(fp_task, whale_task, liq_task, return_exceptions=True)
            fp_res = results[0] if isinstance(results[0], int) else 0
            whale_res = results[1] if isinstance(results[1], int) else 0
            liq_res = results[2] if isinstance(results[2], int) else 0

            for name, r in zip(["Footprint", "Whale", "Liquidation"], results):
                if isinstance(r, Exception):
                    logger.error(f"[CHUNK {c_num}] Sub-pillar {name} failed: {r}")

            total_fp_dumped += fp_res
            total_whale_dumped += whale_res
            total_liq_dumped += liq_res
            
            chunk_duration = time.time() - t_chunk_start
            logger.info(f"Chunk [{c_num}/{total_chunks}] Complete in {chunk_duration:.1f}s. (FP: {fp_res} files, Whales: {whale_res} assets, Liq: {liq_res} files)")

            # All three finally blocks have completed before any host-wide network change.
            if rate_signals:
                reason = "CHALLENGE" if "CHALLENGE" in rate_signals else "RATE_LIMIT_BANNER"
                try:
                    new_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Chunk{c_num}", reason)
                    logger.info("Reactive rotation after observed %s: %s", reason, new_ip)
                except Exception as exc:
                    logger.error("Reactive rotation deferred: %s", exc)

        cycle_duration = time.time() - t_cycle_start
        logger.info("=" * 85)
        logger.info(f"HARVEST CYCLE #{cycle_num} COMPLETED in {cycle_duration:.1f}s ({cycle_duration/60:.2f} min)")
        logger.info(f"Summary: Footprint Parquets = {total_fp_dumped} | Whale Asset Blotters = {total_whale_dumped} | Liquidation Parquets = {total_liq_dumped}")
        logger.info("=" * 85)

        if not continuous:
            if total_fp_dumped != total_symbols*3 or total_whale_dumped != total_symbols or total_liq_dumped != total_symbols*15:
                raise RuntimeError("Cycle incomplete: inspect per-asset errors; historical files were preserved")
            logger.info("Single-cycle execution complete. Exiting master runner.")
            break

        logger.info(f"Sleeping {delay_between_cycles:.1f}s until next scheduled cycle...")
        await asyncio.sleep(delay_between_cycles)
        cycle_num += 1


def main():
    try:
        acquire_singleton("run_parallel_standalone", LOCK_DIR)
    except Exception as e:
        raise SystemExit(f"Singleton acquisition failed: {e}") from e
        
    parser = argparse.ArgumentParser(description="Master Triple-Pillar Parallel Standalone Runner with Cloudflare WARP Anti-Block Rotation")
    parser.add_argument("--chunk-size", type=int, default=15, help="Assets per owned-tab window (default: 15)")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely across scheduled cycles")
    parser.add_argument("--interval", type=float, default=300.0, help="Seconds between cycles if continuous (default: 300)")
    parser.add_argument("--sample", type=int, default=None, help="Limit execution to first N assets for verification")
    parser.add_argument("--symbols", nargs="+", default=None, help="Specific symbols to harvest (e.g. --symbols BTC ETH SOL)")
    parser.add_argument("--rotate-chunks", action="store_true", help="Retired compatibility flag; never enables scheduled rotation")
    args = parser.parse_args()

    asyncio.run(run_parallel_cycle(
        chunk_size=args.chunk_size,
        continuous=args.continuous,
        delay_between_cycles=args.interval,
        sample_limit=args.sample,
        symbols_override=args.symbols,
        rotate_chunks=args.rotate_chunks
    ))


if __name__ == "__main__":
    main()
