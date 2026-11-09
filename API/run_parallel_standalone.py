"""Run sequential CoinGlass sweeps with owned Chrome tabs and WARP recovery."""

import sys
import json
import time
import random
import math
import asyncio
import logging
import argparse
import urllib.request
from pathlib import Path
from typing import List, Dict, Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

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
    atomic_json,
    rotate_warp_ip,
    get_current_ip,
    acquire_singleton,
    LOCK_DIR,
    is_cdp_alive,
    ensure_tor_daemon,
)
from active_assets_manager import load_active_assets, ACTIVE_ASSETS_FILE, _normalize_symbols


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
logger.propagate = False

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
    is_degraded: bool = False,
    legend_enabled: bool = False,
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
        "legend_enabled": legend_enabled,
        "pillars": pillar_stats,
        "symbols": symbols,
        "total_symbols": len(symbols),
    }

    atomic_json(HEALTH_FILE, health_payload)
    return HEALTH_FILE


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
            with urllib.request.urlopen(f"{CDP_BASE_URL}/json/close/{tab_id}", timeout=2):
                pass
        except Exception as exc:
            logger.warning("Could not close owned tab %s: %s", tab_id, exc)
    await asyncio.to_thread(_do)


RATE_REASONS = ("HTTP_429", "CHALLENGE", "RATE_LIMIT_BANNER")
PAGE_FAILURES = (
    "PAGE_LOAD_FAILURE", "ERR_PROXY_", "ERR_SOCKS_", "ERR_TUNNEL_",
    "ERR_CONNECTION_", "ERR_TIMED_OUT", "EXECUTION CONTEXT WAS DESTROYED",
    "WHALE_EXTRACTION_FAILURE", "WHALE_RATE_SIGNAL", "CDP_TAB_UNAVAILABLE",
)


def _page_failure(exc):
    message = str(exc).upper()
    for reason in RATE_REASONS:
        if reason in message:
            return reason
    if isinstance(exc, (TimeoutError, ConnectionClosed, ConnectionError, urllib.error.URLError)):
        return "PAGE_LOAD_FAILURE"
    if any(marker in message for marker in PAGE_FAILURES):
        return "PAGE_LOAD_FAILURE"
    return None


async def _harvest_warm_tab(symbols, route, open_tab, close_tab, collect, persist, rate_signals, failed_symbols):
    """Reuse a healthy owned tab. Retry page failures on a new tab, with one WARP action per sweep."""
    tab_id = ws_url = None
    count = 0
    warp_attempted = False
    try:
        for symbol in symbols:
            data = None
            page_retried = False
            for attempt in range(3):
                try:
                    if not ws_url:
                        tab_id, ws_url = await open_tab(symbol)
                        if not tab_id or not ws_url:
                            raise ConnectionError("CDP_TAB_UNAVAILABLE: no owned tab WebSocket")
                    data = await collect(ws_url, symbol)
                    break
                except Exception as exc:
                    reason = _page_failure(exc)
                    logger.warning("[%s][%s] Attempt %d: %s: %s", route, symbol, attempt + 1,
                                   type(exc).__name__, exc)
                    if reason is None:
                        break
                    if tab_id:
                        await close_tab(tab_id)
                    tab_id = ws_url = None
                    if reason in RATE_REASONS and rate_signals is not None:
                        rate_signals.add(reason)
                    if attempt == 2:
                        break

                    if reason == "PAGE_LOAD_FAILURE" and not page_retried:
                        page_retried = True
                        continue
                    if warp_attempted or not await asyncio.to_thread(is_cdp_alive):
                        break
                    if reason == "PAGE_LOAD_FAILURE":
                        if await asyncio.to_thread(get_current_ip) != "unknown":
                            break
                        reason = "PROXY_UNAVAILABLE"
                        if rate_signals is not None:
                            rate_signals.add(reason)

                    warp_attempted = True
                    url = f"https://www.coinglass.com/{route}/{symbol}"
                    try:
                        await asyncio.to_thread(rotate_warp_ip, f"{route}/{symbol}", reason, target_url=url)
                    except Exception as rotation_error:
                        logger.warning("[%s][%s] WARP recovery unavailable: %s", route, symbol, rotation_error)
                    # Retry the actual dashboard once, even if a peer or cooldown handled recovery.
            if data is None:
                if failed_symbols is not None:
                    failed_symbols.add(symbol)
                continue
            try:
                count += await asyncio.to_thread(persist, symbol, data)
                # Human pacing jitter (2.5s - 3.5s) preventing Cloudflare rate limit bans
                await asyncio.sleep(random.uniform(2.5, 3.5))
            except Exception as exc:
                if failed_symbols is not None:
                    failed_symbols.add(symbol)
                logger.error("[%s][%s] Dump failed: %s", route, symbol, exc)
    finally:
        if tab_id:
            await close_tab(tab_id)
    return count


async def harvest_footprint_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_footprint_api import (
        cdp_adapter, FootprintDomainService, FootprintDumpAdapter, bot_tab_url,
    )

    async def open_tab(symbol):
        url = bot_tab_url(f"https://www.coinglass.com/footprint/{symbol}", "footprint_api")
        tab = await async_cdp_request(f"{CDP_BASE_URL}/json/new?{url}", method="PUT", timeout=6.0)
        return tab.get("id"), tab.get("webSocketDebuggerUrl")

    async def collect(ws_url, symbol):
        return await cdp_adapter.fetch_footprint_data(symbol, ws_url=ws_url)

    def persist(symbol, raw):
        datasets = FootprintDomainService.build_datasets(raw, symbol)
        if len(datasets) != 3 or any(not ds.candles for ds in datasets.values()):
            raise ValueError("Expected three nonempty footprint bar engines")
        return len(FootprintDumpAdapter.dump_three_datasets(datasets, FP_DUMP_DIR))

    return await _harvest_warm_tab(symbols, "footprint", open_tab, async_close_tab, collect, persist,
                                   rate_signals, failed_symbols)


async def harvest_whale_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_whales_api import CDPWhaleAdapter, ParquetWhaleRepository

    adapter = CDPWhaleAdapter()
    equity = get_equity_symbols()
    display_repo = ParquetWhaleRepository(WHALE_DUMP_DIR)
    quant_repo = ParquetWhaleRepository(PIPELINE_OPEN_DIR)

    async def collect(ws_url, symbol):
        async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10,
                                      close_timeout=5, ping_interval=None) as ws:
            return await adapter.harvest_symbol_orders_on_ws(ws, symbol)

    def persist(symbol, dataset):
        display_repo.persist_dataset(dataset, display_only=symbol not in equity, mirror=False)
        quant_repo.persist_dataset(dataset, display_only=False)
        return 1

    return await _harvest_warm_tab(symbols, "large-orderbook-statistics", adapter.get_or_create_warm_tab,
                                   adapter.close_tab, collect, persist, rate_signals, failed_symbols)


async def harvest_liquidation_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    from standalone_coinglass_liquidation_levels_api import (
        get_or_create_warm_tab, close_tab, harvest_symbol_liquidation, persist_asset_liquidation,
    )

    async def collect(ws_url, symbol):
        async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10,
                                      close_timeout=5, ping_interval=None) as ws:
            data, _ = await harvest_symbol_liquidation(ws, symbol)
            return data

    return await _harvest_warm_tab(symbols, "liquidation-levels", get_or_create_warm_tab, close_tab,
                                   collect, persist_asset_liquidation, rate_signals, failed_symbols)


async def harvest_legend_chunk(symbols: List[str], chunk_idx: int, rate_signals: Optional[set] = None, failed_symbols: Optional[set] = None) -> int:
    """Harvest optional 5m realized liquidations and count successful writes."""
    from standalone_coinglass_legend_liquidations_api import LegendLiquidationHarvester

    harvester = LegendLiquidationHarvester()
    count = 0
    try:
        if not await harvester.connect():
            raise ConnectionError("Could not attach to owned Legend tab")
        for symbol in symbols:
            try:
                data = await harvester.harvest_symbol(symbol)
                if not data:
                    raise ValueError("No Legend data returned")
                rows, non_zero, path = await asyncio.to_thread(harvester.persist_asset_parquets, symbol, data)
                if rows <= 0:
                    raise ValueError("No Legend data written")
                count += 1
                logger.info("[LEGEND][%s] Dumped %d bars (%d active) -> %s",
                            symbol, rows, non_zero, Path(path).name)
            except Exception as exc:
                if failed_symbols is not None:
                    failed_symbols.add(symbol)
                logger.error("[LEGEND][%s] Harvest failed: %s", symbol, exc)
    finally:
        await harvester.close(close_tab=True)
    return count


async def run_sequential_pipeline(
    footprint_interval: float = 120.0,
    whale_interval: float = 600.0,
    liq_interval: float = 900.0,
    legend_interval: float = 300.0,
    continuous: bool = False,
    sample_limit: Optional[int] = None,
    symbols_override: Optional[List[str]] = None,
    with_legend: bool = False,
    max_ip_age_min: Optional[float] = None,
    **kwargs
):
    """Run one baseline pass, then service the earliest due sweep in continuous mode."""
    sweeps = {
        "footprint": (harvest_footprint_chunk, footprint_interval, 3, "files"),
        "whales": (harvest_whale_chunk, whale_interval, 1, "assets"),
        "liquidation_levels": (harvest_liquidation_chunk, liq_interval, 15, "files"),
    }
    if with_legend:
        sweeps["live_legend"] = (harvest_legend_chunk, legend_interval, 1, "assets")
    if any(not math.isfinite(interval) or interval <= 0 for _, interval, _, _ in sweeps.values()):
        raise ValueError("Sweep intervals must be finite and positive")
    if max_ip_age_min is not None and (not math.isfinite(max_ip_age_min) or max_ip_age_min <= 0):
        raise ValueError("Maximum IP age must be finite and positive")
    override = _normalize_symbols(symbols_override) if symbols_override else None

    pillar_stats = {
        name: {"status": "INITIALIZING", "interval_seconds": interval}
        for name, (_, interval, _, _) in sweeps.items()
    }
    due = dict.fromkeys(sweeps, 0.0)
    cycle_num = 0
    ip_since = time.monotonic()
    last_ip = "unknown"
    cycles_on_ip = 0
    last_scheduled_attempt = ip_since

    async def execute_sweep(name):
        nonlocal cycle_num, last_ip, ip_since, cycles_on_ip
        harvest, interval, outputs_per_symbol, unit = sweeps[name]
        start = time.monotonic()
        symbols = []
        failed = set()
        rate_signals = set()
        dumped = 0
        error = None
        try:
            symbols = override if override is not None else load_active_assets()
            if override is None and sample_limit and sample_limit > 0:
                symbols = symbols[:sample_limit]
            if not symbols:
                raise ValueError("No active symbols for sweep")
            if not await asyncio.to_thread(is_cdp_alive):
                raise ConnectionError(f"Chrome CDP is unavailable at {CDP_BASE_URL}")
            logger.info("[%s] Starting sweep across %d assets", name, len(symbols))
            dumped = await harvest(symbols, 1, rate_signals, failed)
        except Exception as exc:
            failed.update(symbols)
            error = str(exc)
            logger.error("[%s] Sweep failed: %s", name, exc)

        expected = len(symbols) * outputs_per_symbol
        status = "DEGRADED" if error or failed or dumped != expected else "HEALTHY"
        pillar_stats[name] = {
            f"dumped_{unit}": dumped,
            f"expected_{unit}": expected,
            "status": status,
            "symbols": symbols,
            "failed_symbols": sorted(failed),
            "rate_signals": sorted(rate_signals),
            "interval_seconds": interval,
            "last_sweep_duration_s": round(time.monotonic() - start, 1),
            "last_sweep_epoch": time.time(),
        }
        if error:
            pillar_stats[name]["error"] = error
        cycle_num += 1
        ip = await asyncio.to_thread(get_current_ip)
        if ip != "unknown" and ip != last_ip:
            last_ip = ip
            ip_since = time.monotonic()
            cycles_on_ip = 0
        if ip != "unknown":
            cycles_on_ip += 1
        complete = all(
            stats["status"] == "HEALTHY" and stats["symbols"] == symbols
            for stats in pillar_stats.values()
        )
        emit_pipeline_health(cycle_num, ip, cycles_on_ip if ip != "unknown" else 0,
                             time.monotonic() - ip_since if ip != "unknown" else 0.0,
                             pillar_stats, symbols, not complete, legend_enabled=with_legend)
        logger.info("[%s] %s: wrote %d/%d %s, failed=%s", name, status, dumped, expected, unit, sorted(failed))
        due[name] = time.monotonic() + interval
        return complete

    for name in sweeps:
        complete = await execute_sweep(name)
    if not continuous:
        if not complete:
            raise RuntimeError("Pipeline incomplete. See per-pillar results in pipeline_health.json.")
        return

    max_ip_age_sec = max_ip_age_min * 60.0 if max_ip_age_min is not None else math.inf
    while True:
        now = time.monotonic()
        if last_ip != "unknown" and now - ip_since >= max_ip_age_sec and now - last_scheduled_attempt >= 60.0:
            last_scheduled_attempt = now
            try:
                last_ip = await asyncio.to_thread(rotate_warp_ip, f"Cycle{cycle_num}_Ceiling", "CYCLE_AGE_CEILING")
                ip_since = time.monotonic()
                cycles_on_ip = 0
            except Exception as exc:
                logger.warning("Scheduled WARP rotation deferred: %s", exc)

        next_pillar = min(due, key=due.get)
        delay = due[next_pillar] - time.monotonic()
        if delay <= 0:
            await execute_sweep(next_pillar)
        else:
            await asyncio.sleep(min(5.0, delay))


# Backwards compatibility alias
run_parallel_cycle = run_sequential_pipeline


def main():
    parser = argparse.ArgumentParser(description="Master Sequential Standalone Runner with High-Frequency Footprint & WARP IP Rotation")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely across scheduled cadence cycles")
    parser.add_argument("--footprint-interval", type=float, default=120.0, help="Seconds between Footprint sweeps (default: 120)")
    parser.add_argument("--whale-interval", type=float, default=600.0, help="Seconds between Whales sweeps (default: 600)")
    parser.add_argument("--liq-interval", type=float, default=900.0, help="Seconds between Liquidation sweeps (default: 900)")
    parser.add_argument("--interval", type=float, default=None, help="Convenience alias to set footprint interval")
    parser.add_argument("--sample", type=int, default=None, help="Limit execution to first N assets for verification")
    parser.add_argument("--symbols", nargs="+", default=None, help="Specific symbols to harvest (e.g. --symbols BTC ETH SOL)")
    parser.add_argument("--with-legend", action="store_true", help="Harvest 5m Live Liquidations from CoinGlass Legend")
    parser.add_argument("--max-ip-age-min", type=float, help="Opt in to scheduled WARP rotation after this many minutes on one IP")
    # Deprecated compatibility flags
    parser.add_argument("--chunk-size", type=int, default=50, help="Compatibility flag (full asset list is processed in 1 tab)")
    parser.add_argument("--rotate-chunks", action="store_true", help="Retired compatibility flag")
    args = parser.parse_args()

    try:
        acquire_singleton("run_parallel_standalone", LOCK_DIR)
    except Exception as exc:
        raise SystemExit(f"Singleton acquisition failed: {exc}") from exc

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
