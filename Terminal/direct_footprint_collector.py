"""
CoinGlass Direct Footprint & Signals Collector (Standalone Daemon)
==================================================================
Runs continuously in background:
- Triggers every 2 minutes.
- Dynamic tab lifecycle: Opens tab, harvests all assets, dumps Parquet, and CLOSES the tab.
- Embedded Cloudflare WARP IP rotation between sweeps.
- Extracts order-flow signals (Absorption, Trapped Traders, Divergence, Exhaustion).
- Extracts 15m order-flow candles (Volume, Delta, Cumulative Delta, POC).

Persists to:
- Database/coinglass_aggregated_futures_signals.parquet
- Database/Candle_Data/<SYMBOL>.parquet
"""

import sys
import os
import json
import time
import logging
import asyncio
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.runtime_io import acquire_singleton, atomic_parquet, upsert_parquet, cdp_result
from Terminal.collector_lock import rotate_warp_ip, BrowserLock, is_scheduled_for_other_collector
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"
WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [FOOTPRINT] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("FootprintCollector")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# In-memory tracking of known signals to trigger instant evaluation on newly appeared signals
KNOWN_SIGNAL_KEYS = set()
evaluator: Optional[RealtimeSignalEvaluator] = None

def load_known_signals():
    global KNOWN_SIGNAL_KEYS
    if SIGNALS_PARQUET.exists():
        try:
            df = pd.read_parquet(SIGNALS_PARQUET)
            for _, r in df.iterrows():
                key = (r['symbol'], str(r.get('sig_code', '')), int(r.get('time_ms', 0)), float(r.get('price', 0.0)))
                KNOWN_SIGNAL_KEYS.add(key)
            logger.info(f"[REALTIME SENTINEL] Pre-loaded {len(KNOWN_SIGNAL_KEYS)} known signal keys into memory.")
        except Exception as e:
            logger.warning(f"Could not pre-load known signals: {e}")


def load_registry_symbols():
    if not REGISTRY_FILE.exists():
        return [], {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    symbols = []
    mt5_map = {}
    for cat, assets in reg.items():
        for sym, meta in assets.items():
            if meta.get("footprint", False):
                symbols.append(sym)
                mt5_map[sym] = meta.get("mt5", "")
    return symbols, mt5_map


TARGET_SYMBOLS, MT5_MAP = load_registry_symbols()


def get_current_ip() -> str:
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as response:
            return json.loads(response.read().decode()).get("ip", "unknown")
    except Exception:
        return "unknown"


def rotate_cloudflare_ip():
    """Disconnect and reconnect Cloudflare WARP to rotate IP and re-key tunnel."""
    if not WARP_CLI.exists():
        logger.warning(f"WARP CLI binary not found at: {WARP_CLI}")
        return

    old_ip = get_current_ip()
    logger.info(f"[Cloudflare WARP] Current IP: {old_ip}. Rotating tunnel...")
    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(1.5)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(3.5)
        new_ip = get_current_ip()
        logger.info(f"[Cloudflare WARP] Rotation complete. New IP: {new_ip} (Old: {old_ip})")
    except Exception as e:
        logger.error(f"[Cloudflare WARP] Error during IP rotation: {e}")


def cleanup_coinglass_tabs(keep_tab_id: Optional[str] = None):
    """Closes all orphaned CoinGlass footprint tabs in Chrome to keep browser lean and free."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "coinglass.com/footprint" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Cleaned up orphan Footprint tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Tab cleanup error: {e}")


def acquire_pid_lock(name):
    acquire_singleton(name, DB_DIR / '.locks')


def open_coinglass_footprint_tab() -> Tuple[Optional[str], Optional[str]]:
    """Opens a fresh CoinGlass footprint tab and returns (tab_id, ws_url)."""
    # First clean up any orphaned tabs before creating a new one
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json/new?https://www.coinglass.com/footprint", method="PUT")
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened fresh CoinGlass Footprint tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open footprint tab via CDP: {e}")
        return None, None


def close_tab(tab_id: str):
    """Closes tab immediately after sweep to keep browser lean."""
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed footprint tab ({tab_id}) - Browser lean & free.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    # Enforce zero leftover coinglass tabs


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    return await cdp_result(ws, {"id": msg_id, "method": "Runtime.evaluate", "params": {
        "expression": expr, "returnByValue": True, "awaitPromise": await_promise}})


async def ensure_terminal_ready(ws) -> bool:
    init_js = """(() => {
        if (window.__cg_term) return true;
        const nextEl = document.getElementById('__next');
        if (!nextEl) return false;
        const containerKey = Object.keys(nextEl).find(k => k.startsWith('__reactContainer$'));
        if (!containerKey) return false;
        const rootFiber = nextEl[containerKey];

        let term = null;
        function findTerm(fiber, depth = 0) {
            if (!fiber || depth > 80 || term) return;
            try {
                if (fiber.memoizedProps && fiber.memoizedProps.value && fiber.memoizedProps.value.term) {
                    term = fiber.memoizedProps.value.term;
                    return;
                }
            } catch(e) {}
            findTerm(fiber.child, depth + 1);
            findTerm(fiber.sibling, depth);
        }
        findTerm(rootFiber);
        if (term) {
            window.__cg_term = term;
            return true;
        }
        return false;
    })()"""
    for _ in range(8):
        ready = await cdp_eval(ws, init_js)
        if ready:
            return True
        await asyncio.sleep(1.0)
    return False


def dump_signals_parquet(new_signals):
    if new_signals:
        upsert_parquet(pd.DataFrame(new_signals), SIGNALS_PARQUET,
                       ['symbol', 'sig_code', 'time_ms'], sort_by='time_ms', limit=100000)


def upsert_candle_parquet(symbol, mt5_sym, candles):
    if not candles:
        return
    frame = pd.DataFrame(candles)
    frame['dumped_at_utc'] = datetime.now(timezone.utc).isoformat()
    upsert_parquet(frame, CANDLE_DIR / f'{symbol}.parquet', ['open_time_ms'],
                   sort_by='open_time_ms', limit=10000)


async def extract_symbol_footprint(ws, sym: str, mt5_sym: str) -> dict:
    js = f"""(async () => {{
        const term = window.__cg_term;
        if (!term) return {{ error: 'no __cg_term' }};
        const sym = '{sym}';
        const mt5 = '{mt5_sym}';

        return new Promise((resolve) => {{
            let unsub = () => {{}};
            let loadedOlder = false;
            const timeout = setTimeout(() => {{
                unsub();
                resolve({{ signals: [], candles: [] }});
            }}, 3500);

            unsub = term.subscribe(async () => {{
                const sigSets = term.sigSets ? term.sigSets() : null;
                if (sigSets && sigSets.length === 3) {{
                    if (!loadedOlder) {{
                        loadedOlder = true;
                        try {{ await term.maybeOlder(); }} catch(e) {{}}
                    }}
                    clearTimeout(timeout);
                    unsub();

                    const signals = [];
                    for (const set of sigSets) {{
                        const X = set.X;
                        for (const s of set.sigs) {{
                            const text = term.sigText(s, X);
                            signals.push({{
                                symbol: sym,
                                mt5_symbol: mt5,
                                market: 'agg-perp',
                                bar_type: set.bt,
                                bar_label: set.label,
                                time_ms: X && X.T ? X.T[s.i] : null,
                                datetime_utc: X && X.T ? new Date(X.T[s.i]).toISOString() : null,
                                sig_code: s.t,
                                sig_name: term.sigName(s),
                                side: s.side === 'buy' ? 'bullish' : 'bearish',
                                price: s.p,
                                title: text[0] || '',
                                description: text[1] || ''
                            }});
                        }}
                    }}

                    const timeSet = sigSets.find(s => s.bt === 'time');
                    const bars = [];
                    if (timeSet && timeSet.X && timeSet.X.T) {{
                        const X = timeSet.X;
                        const n = X.T.length;
                        let cumDelta = 0;
                        for (let i = 0; i < n; i++) {{
                            const t = X.T[i];
                            const te = X.TE ? X.TE[i] : (t + 15 * 60 * 1000);
                            const o = X.O[i];
                            const h = X.H[i];
                            const l = X.L[i];
                            const c = X.C[i];
                            const F = X.F ? X.F[i] : null;

                            let buyVol = 0, sellVol = 0, maxVol = 0, pocIdx = -1;
                            if (F && F.b && F.s) {{
                                for (let j = 0; j < F.b.length; j++) {{
                                    const b = F.b[j] || 0;
                                    const s = F.s[j] || 0;
                                    buyVol += b;
                                    sellVol += s;
                                    if (b + s > maxVol) {{
                                        maxVol = b + s;
                                        pocIdx = j;
                                    }}
                                }}
                            }}
                            const delta = buyVol - sellVol;
                            cumDelta += delta;
                            let pocPrice = (F && pocIdx >= 0) ? (F.lo + pocIdx * F.tick) : null;

                            bars.push({{
                                symbol: sym,
                                mt5_symbol: mt5,
                                timeframe: '15m',
                                open_time_ms: t,
                                close_time_ms: te,
                                open_time_utc: new Date(t).toISOString(),
                                close_time_utc: new Date(te).toISOString(),
                                open: o, high: h, low: l, close: c,
                                total_volume: buyVol + sellVol,
                                delta: delta,
                                cumulative_delta: cumDelta,
                                poc_price: pocPrice
                            }});
                        }}
                    }}

                    resolve({{ signals, candles: bars }});
                }}
            }});

            term.pickPair(sym, 'agg-perp');
        }});
    }})()"""
    res = await cdp_eval(ws, js, msg_id=int(time.time() * 1000) % 1000000, await_promise=True)
    return res or {"signals": [], "candles": []}


async def footprint_sweep_cycle(ws) -> int:
    ready = await ensure_terminal_ready(ws)
    if not ready:
        logger.error("Terminal not ready on footprint page. Aborting cycle.")
        return 0

    all_signals = []
    logger.info(f"Starting Footprint sweep across {len(TARGET_SYMBOLS)} symbols...")
    for i, sym in enumerate(TARGET_SYMBOLS, 1):
        try:
            mt5_sym = MT5_MAP.get(sym, "")
            res = await extract_symbol_footprint(ws, sym, mt5_sym)
            sigs = res.get("signals", [])
            bars = res.get("candles", [])
            if sigs:
                all_signals.extend(sigs)
            if bars:
                upsert_candle_parquet(sym, mt5_sym, bars)
            if i % 5 == 0 or i == len(TARGET_SYMBOLS):
                logger.info(f"[{i}/{len(TARGET_SYMBOLS)}] {sym}: {len(sigs)} signals, {len(bars)} candles")
        except Exception as e:
            logger.warning(f"[{sym}] Extraction error: {e}")

    if all_signals:
        global KNOWN_SIGNAL_KEYS, evaluator
        new_signals = []
        for s in all_signals:
            key = (s['symbol'], str(s.get('sig_code', '')), int(s.get('time_ms', 0)), float(s.get('price', 0.0)))
            if key not in KNOWN_SIGNAL_KEYS:
                new_signals.append(s)
                KNOWN_SIGNAL_KEYS.add(key)

        dump_signals_parquet(all_signals)

        if new_signals and evaluator:
            logger.info(f"🚨 [REALTIME SENTINEL] Discovered {len(new_signals)} brand NEW signal(s) in this iteration! Waking up deep evaluation pipeline...")
            evaluator.evaluate_new_signals(new_signals)

    logger.info(f"Footprint sweep complete: {len(all_signals)} total signals harvested.")
    return len(all_signals)


async def run_collector_daemon(cycle_seconds: int = 120):
    acquire_pid_lock("direct_footprint_collector")
    global evaluator
    logger.info("Initializing RealtimeSignalEvaluator with DeepMind TimesFM & OLAP...")
    evaluator = RealtimeSignalEvaluator()
    load_known_signals()
    logger.info(f"=== Footprint & Signals Daemon Active (Cadence: Every 2 Minutes / {cycle_seconds}s) ===")
    while True:
        # Check if current minute is exclusively reserved for Whales or Liquidation dump
        cur_min = datetime.now().minute
        if is_scheduled_for_other_collector(cur_min, "footprint"):
            logger.info(f"[FOOTPRINT] Current minute (:{cur_min:02d}) is reserved for Whale/Liquidation sweep. Yielding 45s...")
            await asyncio.sleep(45.0)
            continue

        cycle_start = time.time()
        with BrowserLock("Footprint") as browser_lock:
            if not browser_lock.acquired:
                await asyncio.sleep(2.0)
                continue
            # 1. Cloudflare WARP IP Rotation BEFORE sweep
            logger.info("Rotating Cloudflare WARP IP BEFORE Footprint dump cycle...")
            await asyncio.to_thread(rotate_warp_ip, "Footprint-Pre")

            tab_id, ws_url = open_coinglass_footprint_tab()
            if tab_id and ws_url:
                try:
                    # Allow page DOM to settle
                    await asyncio.sleep(3.5)
                    async with websockets.connect(ws_url, open_timeout=5, close_timeout=2, max_size=50_000_000) as ws:
                        await footprint_sweep_cycle(ws)
                except Exception as e:
                    logger.error(f"Error during footprint sweep: {e}")
                finally:
                    # Closes tab once done with dumping
                    close_tab(tab_id)

            # 2. Cloudflare WARP IP Rotation AFTER sweep
            logger.info("Rotating Cloudflare WARP IP AFTER Footprint dump cycle...")
            await asyncio.to_thread(rotate_warp_ip, "Footprint-Post")

        # Sleep for remainder of 2 minutes
        elapsed = time.time() - cycle_start
        rest_time = max(5.0, cycle_seconds - elapsed)
        logger.info(f"Resting {rest_time:.1f}s until next 2-minute cycle...")
        await asyncio.sleep(rest_time)


if __name__ == "__main__":
    asyncio.run(run_collector_daemon(120))
