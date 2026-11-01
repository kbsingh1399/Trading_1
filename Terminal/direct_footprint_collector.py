"""
Direct WebSocket CoinGlass Footprint & Signals Collector
========================================================
Ultra-fast, non-blocking ingestion engine using native Page WebSocket CDP.
Connects directly to the existing CoinGlass Footprint tab:
- Extracts real-time order-flow signals (Absorption, Trapped Traders, Divergence, Exhaustion).
- Extracts 15m order-flow candles with volume, delta, cumulative delta, and POC.
- Triggers reactive trade evaluation immediately on new signals.
"""

import sys
import os
import json
import time
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(ROOT_DIR / "Terminal") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "Terminal"))

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("DirectFootprint")
logger.setLevel(logging.INFO)
logger.handlers = [handler]


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


def get_footprint_ws_url() -> Optional[str]:
    try:
        req = urllib.request.urlopen("http://127.0.0.1:9222/json", timeout=3)
        tabs = json.loads(req.read().decode())
        for t in tabs:
            if "footprint" in t.get("url", ""):
                return t.get("webSocketDebuggerUrl")
    except Exception as e:
        logger.warning(f"Error finding footprint tab: {e}")
    return None


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    payload = {
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {
            "expression": expr,
            "returnByValue": True,
            "awaitPromise": await_promise
        }
    }
    await ws.send(json.dumps(payload))
    while True:
        raw = await ws.recv()
        resp = json.loads(raw)
        if resp.get("id") == msg_id:
            res = resp.get("result", {})
            return res.get("result", {}).get("value")


def dump_signals_parquet(new_signals: List[dict]):
    if not new_signals:
        return
    DB_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(new_signals)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["dumped_at_utc"] = now_utc

    if SIGNALS_PARQUET.exists():
        try:
            old_df = pd.read_parquet(SIGNALS_PARQUET)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["symbol", "sig_code", "time_ms", "price"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df

    combined = combined.sort_values(by=["symbol", "time_ms"], ascending=[True, True]).reset_index(drop=True)
    combined.to_parquet(SIGNALS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Signals Parquet updated: {SIGNALS_PARQUET.name} ({len(combined)} rows across {combined['symbol'].nunique()} assets)")

    # Trigger reactive trade engine
    try:
        from Terminal.reactive_trade_engine import scan_and_react_to_signals
        scan_and_react_to_signals()
    except Exception as e:
        logger.warning(f"[Wakeup-Engine] Error during reactive trade formulation: {e}")


def upsert_candle_parquet(symbol: str, mt5_sym: str, bars: List[dict]):
    if not bars:
        return
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    out_file = CANDLE_DIR / f"{symbol}.parquet"
    new_df = pd.DataFrame(bars)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["dumped_at_utc"] = now_utc

    if out_file.exists():
        try:
            old_df = pd.read_parquet(out_file)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["open_time_ms"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df

    combined = combined.sort_values(by="open_time_ms", ascending=True).reset_index(drop=True)
    combined.to_parquet(out_file, index=False, engine="pyarrow")


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
    for _ in range(5):
        ready = await cdp_eval(ws, init_js)
        if ready:
            return True
        await asyncio.sleep(1)
    return False


async def footprint_sweep_cycle(ws) -> int:
    await ensure_terminal_ready(ws)
    all_signals = []
    logger.info(f"Starting Footprint extraction cycle across {len(TARGET_SYMBOLS)} symbols...")
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
            logger.info(f"[{i}/{len(TARGET_SYMBOLS)}] {sym}: {len(sigs)} signals, {len(bars)} candles")
        except Exception as e:
            logger.warning(f"[{sym}] Extraction error: {e}")

    if all_signals:
        dump_signals_parquet(all_signals)
    logger.info(f"Footprint extraction complete: {len(all_signals)} total signals harvested.")
    return len(all_signals)


async def run_collector_daemon(interval_seconds: int = 60):
    logger.info(f"Starting Direct WebSocket Footprint Collector Daemon (Cadence: {interval_seconds}s)")
    while True:
        try:
            ws_url = get_footprint_ws_url()
            if not ws_url:
                logger.warning("Footprint tab not found! Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            async with websockets.connect(ws_url, max_size=50_000_000) as ws:
                await footprint_sweep_cycle(ws)

        except Exception as e:
            logger.error(f"Error in footprint daemon cycle: {e}")

        logger.info(f"Resting for {interval_seconds}s before next footprint sweep...")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run_collector_daemon(60))
