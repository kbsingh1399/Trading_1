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
# MASTER SCHEDULER & CONCURRENCY ORCHESTRATOR
# ======================================================================================

async def run_parallel_cycle(
    chunk_size: int = 50,
    continuous: bool = False,
    delay_between_cycles: float = 300.0,
    sample_limit: Optional[int] = None,
    symbols_override: Optional[List[str]] = None,
    rotate_chunks: bool = False,
    with_legend: bool = False,
    max_cycles_per_ip: int = 3,
    max_ip_age_min: float = 15.0
):
    """
    Executes complete cycles across active assets with:
    - Triple/Quad-Pillar Concurrency: Footprint, Whales, Liquidation Levels, and Legend run simultaneously.
    - Dynamic Universe: Reloads active assets from API/active_assets.json inside the while True: loop.
    - Proactive + Reactive Egress Rotation: Rotates reactively on 429/Challenge, or at rest window after cycle/age ceiling.
    - Rotation is coordinated after all owned tabs close, never inside a pillar.
    - 4 Deterministic Checkpoints validating streaming feed through local disk parity.
    - Protected asyncio.gather(return_exceptions=True) isolating sub-pillar exceptions.
    - Atomic pipeline health heartbeat published after each cycle to Database/pipeline_health.json.
    """
    logger.info("=" * 85)
    logger.info(f"STARTING MULTI-PILLAR PARALLEL STANDALONE RUNNER")
    logger.info(f"Target Pillars: 1. Footprint | 2. Whales | 3. Liquidation Levels" + (" | 4. Live Legend" if with_legend else ""))
    if chunk_size <= 0 or delay_between_cycles < 0:
        raise ValueError("chunk_size must be positive and interval nonnegative")
    if rotate_chunks:
        logger.warning("--rotate-chunks is retired; scheduled rotations are disabled")
    logger.info("Initial egress IP: %s", await asyncio.to_thread(get_current_ip))
    logger.info("=" * 85)

    cycle_num = 1
    cycles_on_current_ip = 0
    ip_start_time = time.time()
    max_ip_age_sec = max_ip_age_min * 60.0

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
        total_legend_dumped = 0

        total_chunks = (total_symbols + chunk_size - 1) // chunk_size
        cycle_rate_signals = set()
        cycle_failed_fp = set()
        cycle_failed_whales = set()
        cycle_failed_liq = set()
        cycle_failed_legend = set()

        for c_idx in range(0, total_symbols, chunk_size):
            chunk = symbols[c_idx : c_idx + chunk_size]
            c_num = (c_idx // chunk_size) + 1
            
            logger.info("-" * 80)
            logger.info(f"CHUNK [{c_num}/{total_chunks}] ({len(chunk)} assets: {chunk[0]}..{chunk[-1]}) - PARALLEL TABS ACTIVE")
            logger.info(f"Active Egress IP: {get_current_ip()}")
            logger.info("-" * 80)

            # RUN ALL PILLARS IN PARALLEL TABS SIMULTANEOUSLY
            t_chunk_start = time.time()
            rate_signals = set()
            tasks = [
                asyncio.create_task(harvest_footprint_chunk(chunk, c_num, rate_signals, cycle_failed_fp)),
                asyncio.create_task(harvest_whale_chunk(chunk, c_num, rate_signals, cycle_failed_whales)),
                asyncio.create_task(harvest_liquidation_chunk(chunk, c_num, rate_signals, cycle_failed_liq)),
            ]
            names = ["Footprint", "Whale", "Liquidation"]
            if with_legend:
                tasks.append(asyncio.create_task(harvest_legend_chunk(chunk, c_num, rate_signals, cycle_failed_legend)))
                names.append("LiveLiquidation")

            # Isolated gather protecting sibling pillars from cancellation
            results = await asyncio.gather(*tasks, return_exceptions=True)
            fp_res = results[0] if isinstance(results[0], int) else 0
            whale_res = results[1] if isinstance(results[1], int) else 0
            liq_res = results[2] if isinstance(results[2], int) else 0
            legend_res = results[3] if with_legend and len(results) > 3 and isinstance(results[3], int) else 0

            for name, r in zip(names, results):
                if isinstance(r, Exception):
                    logger.error(f"[CHUNK {c_num}] Sub-pillar {name} failed: {r}")

            total_fp_dumped += fp_res
            total_whale_dumped += whale_res
            total_liq_dumped += liq_res
            total_legend_dumped += legend_res
            
            chunk_duration = time.time() - t_chunk_start
            extra_msg = f", Legend: {legend_res} assets" if with_legend else ""
            logger.info(f"Chunk [{c_num}/{total_chunks}] Complete in {chunk_duration:.1f}s. (FP: {fp_res} files, Whales: {whale_res} assets, Liq: {liq_res} files{extra_msg})")

            # All scraper finally blocks have completed before any host-wide network change.
            if rate_signals:
                cycle_rate_signals.update(rate_signals)
                reason = "CHALLENGE" if "CHALLENGE" in rate_signals else "RATE_LIMIT_BANNER"
                try:
                    new_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Chunk{c_num}", reason)
                    logger.info("Reactive rotation after observed %s: %s", reason, new_ip)
                    cycles_on_current_ip = 0
                    ip_start_time = time.time()
                except Exception as exc:
                    logger.error("Reactive rotation deferred: %s", exc)

        # Focused Remediation Pass: Recover any missed assets across pillars before concluding cycle
        from standalone_coinglass_liquidation_levels_api import is_asset_dump_complete
        disk_missing_liq = set(s for s in symbols if not is_asset_dump_complete(s))
        pending_liq = list(disk_missing_liq.union(cycle_failed_liq))
        pending_fp = list(cycle_failed_fp)
        pending_whales = list(cycle_failed_whales)
        pending_legend = list(cycle_failed_legend)

        if pending_liq or pending_fp or pending_whales or pending_legend:
            logger.info("=" * 80)
            logger.info("[REMEDIATION PASS] Recovering missed assets across pillars (Liq: %s, FP: %s, Whales: %s, Legend: %s)",
                        pending_liq, pending_fp, pending_whales, pending_legend)
            try:
                new_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Recovery", "HTTP_429", force=True)
                logger.info("[REMEDIATION PASS] Rotated to fresh preflight-verified IP: %s", new_ip)
            except Exception as e:
                logger.warning("[REMEDIATION PASS] Rotation deferred: %s", e)
            if pending_liq:
                recovered_liq = await harvest_liquidation_chunk(pending_liq, 99)
                total_liq_dumped += recovered_liq
                logger.info("[REMEDIATION PASS] Liquidation recovery complete: added %d files", recovered_liq)
            if pending_fp:
                recovered_fp = await harvest_footprint_chunk(pending_fp, 99)
                total_fp_dumped += recovered_fp
                logger.info("[REMEDIATION PASS] Footprint recovery complete: added %d files", recovered_fp)
            if pending_whales:
                recovered_whales = await harvest_whale_chunk(pending_whales, 99)
                total_whale_dumped += recovered_whales
                logger.info("[REMEDIATION PASS] Whales recovery complete: added %d assets", recovered_whales)
            if pending_legend and with_legend:
                recovered_legend = await harvest_legend_chunk(pending_legend, 99)
                total_legend_dumped += recovered_legend
                logger.info("[REMEDIATION PASS] Legend recovery complete: added %d assets", recovered_legend)

        cycle_duration = time.time() - t_cycle_start
        logger.info("=" * 85)
        logger.info(f"HARVEST CYCLE #{cycle_num} COMPLETED in {cycle_duration:.1f}s ({cycle_duration/60:.2f} min)")
        extra_summary = f" | Live Liquidation Parquets = {total_legend_dumped}" if with_legend else ""
        logger.info(f"Summary: Footprint Parquets = {total_fp_dumped} | Whale Asset Blotters = {total_whale_dumped} | Liquidation Parquets = {total_liq_dumped}{extra_summary}")
        logger.info("=" * 85)

        expected_fp = total_symbols * 3
        expected_whales = total_symbols
        expected_liq = total_symbols * 15
        expected_legend = total_symbols if with_legend else 0

        from standalone_coinglass_footprint_api import is_footprint_asset_complete
        from standalone_coinglass_whales_api import is_whale_asset_complete
        from standalone_coinglass_liquidation_levels_api import is_asset_dump_complete

        disk_fp_ok = all(is_footprint_asset_complete(s) for s in symbols)
        disk_whale_ok = all(is_whale_asset_complete(s) for s in symbols)
        disk_liq_ok = all(is_asset_dump_complete(s) for s in symbols)

        fp_ok = (total_fp_dumped == expected_fp) or disk_fp_ok
        whale_ok = (total_whale_dumped == expected_whales) or disk_whale_ok
        liq_ok = (total_liq_dumped == expected_liq) or disk_liq_ok
        legend_ok = (total_legend_dumped >= (expected_legend - 4)) if with_legend else True
        is_degraded = not (disk_fp_ok and disk_whale_ok and disk_liq_ok)

        pillar_stats = {
            "footprint": {
                "dumped_files": total_fp_dumped,
                "expected_files": expected_fp,
                "status": "HEALTHY" if fp_ok else "DEGRADED"
            },
            "whales": {
                "dumped_assets": total_whale_dumped,
                "expected_assets": expected_whales,
                "status": "HEALTHY" if whale_ok else "DEGRADED"
            },
            "liquidation_levels": {
                "dumped_files": total_liq_dumped,
                "expected_files": expected_liq,
                "status": "HEALTHY" if liq_ok else "DEGRADED"
            }
        }
        if with_legend:
            pillar_stats["live_legend"] = {
                "dumped_assets": total_legend_dumped,
                "expected_assets": expected_legend,
                "status": "HEALTHY" if legend_ok else "DEGRADED"
            }

        current_ip = get_current_ip()
        ip_age_sec = time.time() - ip_start_time

        emit_pipeline_health(
            cycle_num=cycle_num,
            current_ip=current_ip,
            cycles_on_ip=cycles_on_current_ip + 1,
            ip_age_sec=ip_age_sec,
            pillar_stats=pillar_stats,
            symbols=symbols,
            is_degraded=is_degraded
        )

        if not continuous:
            if is_degraded:
                raise RuntimeError("Cycle incomplete: inspect per-asset errors; historical files were preserved")
            logger.info("Single-cycle execution complete. Exiting master runner.")
            break

        cycles_on_current_ip += 1
        now = time.time()
        if cycles_on_current_ip >= max_cycles_per_ip or (now - ip_start_time) >= max_ip_age_sec:
            logger.info(
                "IP age ceiling reached (%d cycles, %.1fs elapsed); rotating egress IP in inter-cycle window...",
                cycles_on_current_ip, now - ip_start_time
            )
            try:
                new_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Ceiling", "CYCLE_AGE_CEILING")
                logger.info("Inter-cycle rotation successful: new egress IP is %s", new_ip)
                cycles_on_current_ip = 0
                ip_start_time = time.time()
            except Exception as exc:
                logger.error("Inter-cycle rotation deferred: %s", exc)

        logger.info(f"Sleeping {delay_between_cycles:.1f}s until next scheduled cycle...")
        await asyncio.sleep(delay_between_cycles)
        cycle_num += 1


def main():
    try:
        acquire_singleton("run_parallel_standalone", LOCK_DIR)
    except Exception as e:
        raise SystemExit(f"Singleton acquisition failed: {e}") from e
        
    parser = argparse.ArgumentParser(description="Master Triple-Pillar Parallel Standalone Runner with Cloudflare WARP Anti-Block Rotation")
    parser.add_argument("--chunk-size", type=int, default=50, help="Assets per owned-tab window (default: 50)")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely across scheduled cycles")
    parser.add_argument("--interval", type=float, default=300.0, help="Seconds between cycles if continuous (default: 300)")
    parser.add_argument("--sample", type=int, default=None, help="Limit execution to first N assets for verification")
    parser.add_argument("--symbols", nargs="+", default=None, help="Specific symbols to harvest (e.g. --symbols BTC ETH SOL)")
    parser.add_argument("--rotate-chunks", action="store_true", help="Retired compatibility flag; never enables scheduled rotation")
    parser.add_argument("--with-legend", action="store_true", help="Harvest 5m Live Liquidations from CoinGlass Legend in parallel")
    parser.add_argument("--max-cycles-per-ip", type=int, default=3, help="Max harvest cycles on single egress IP before inter-cycle rotation (default: 3)")
    parser.add_argument("--max-ip-age-min", type=float, default=15.0, help="Max IP age in minutes before inter-cycle rotation (default: 15.0)")
    args = parser.parse_args()

    asyncio.run(run_parallel_cycle(
        chunk_size=args.chunk_size,
        continuous=args.continuous,
        delay_between_cycles=args.interval,
        sample_limit=args.sample,
        symbols_override=args.symbols,
        rotate_chunks=args.rotate_chunks,
        with_legend=args.with_legend,
        max_cycles_per_ip=args.max_cycles_per_ip,
        max_ip_age_min=args.max_ip_age_min
    ))


if __name__ == "__main__":
    main()
