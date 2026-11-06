"""
coinglass_footprint_api.py
========================================================================================
Browser-Based Local REST & Streaming API for CoinGlass Footprint (Zero DOM Scraping)
========================================================================================

Architecture:
- Bridges to Chrome via Chrome DevTools Protocol (CDP port 9222).
- Directly hooks into the in-memory React Fiber runtime (window.__cg_term).
- Extracts raw numerical arrays from terminal memory:
    * OHLCV price series
    * Footprint Bid/Ask tick ladders (F.b, F.s, F.lo, F.tick)
    * Point of Control (POC), Value Area, Bar Delta, Cumulative Delta
    * Native footprint signals (Absorption, Exhaustion, Trapped Orders, CVD Divergence)
- Exposes clean REST endpoints & WebSocket streams for local clients (Python, DuckDB, MT5, Node.js).
- Provides an optional local dump handler to persist data to Parquet / JSONL.
========================================================================================
"""

import sys
import os
import json
import time
import asyncio
import logging
import urllib.request
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import websockets

ROOT_DIR = Path(__file__).resolve().parent
DB_DIR = ROOT_DIR / "Database"
DUMP_DIR = DB_DIR / "Footprint_Dumps"
DUMP_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [FOOTPRINT-API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("FootprintAPI")

app = FastAPI(
    title="CoinGlass Footprint Browser API",
    description="Browser-driven local API accessing CoinGlass in-memory order flow without DOM scraping.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CDP_BASE_URL = "http://127.0.0.1:9222"
_ACTIVE_TAB_ID: Optional[str] = None
_ACTIVE_WS_URL: Optional[str] = None
_LOCK = asyncio.Lock()


def get_cdp_tabs() -> List[Dict[str, Any]]:
    """Fetches all open tabs from the CDP endpoint."""
    try:
        req = urllib.request.Request(f"{CDP_BASE_URL}/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.warning(f"Failed to query CDP port 9222: {e}")
        return []


def open_or_get_footprint_tab(symbol: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    """Locates an existing CoinGlass footprint tab or opens a new one."""
    tabs = get_cdp_tabs()
    # 1. Look for an existing footprint tab
    for t in tabs:
        url = t.get("url", "").lower()
        if "coinglass.com/footprint" in url and t.get("webSocketDebuggerUrl"):
            return t["id"], t["webSocketDebuggerUrl"]

    # 2. If none open, launch a new tab via CDP
    try:
        url = f"https://www.coinglass.com/footprint/{symbol}?bot=footprint_api"
        req = urllib.request.Request(f"{CDP_BASE_URL}/json/new?{url}", method="PUT")
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode())
            logger.info(f"Launched fresh CoinGlass footprint tab ({data.get('id')}) for {symbol}")
            return data.get("id"), data.get("webSocketDebuggerUrl")
    except Exception as e:
        logger.error(f"Failed to open footprint tab via CDP: {e}")
        return None, None


async def cdp_eval_js(ws_url: str, js_code: str, timeout: float = 12.0) -> Any:
    """Executes a JavaScript expression inside the page runtime and returns the result."""
    async with websockets.connect(ws_url, ping_interval=None, max_size=50 * 1024 * 1024) as ws:
        msg_id = int(time.time() * 1000) % 10000000
        payload = {
            "id": msg_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": js_code,
                "returnByValue": True,
                "awaitPromise": True
            }
        }
        await ws.send(json.dumps(payload))
        start_time = time.time()
        while time.time() - start_time < timeout:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            msg = json.loads(raw)
            if msg.get("id") == msg_id:
                if "error" in msg or msg.get("result", {}).get("exceptionDetails"):
                    err = msg.get("result", {}).get("exceptionDetails", msg.get("error"))
                    raise RuntimeError(f"CDP JS Execution Error: {err}")
                return msg.get("result", {}).get("result", {}).get("value")
    raise TimeoutError("CDP evaluation timed out")


HOOK_TERMINAL_JS = """(() => {
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


def build_footprint_extraction_js(symbol: str, timeframe: str = "15m", include_ladder: bool = True) -> str:
    """
    Constructs in-page JS to command window.__cg_term to select the requested pair and timeframe,
    and returns raw structured data without DOM interaction.
    """
    return f"""(async () => {{
        const term = window.__cg_term;
        if (!term) return {{ error: 'Terminal object window.__cg_term not initialized' }};

        const targetSym = '{symbol.upper()}';
        const targetItv = '{timeframe}';

        // Select requested pair & interval if needed
        if (term.prefs.sym !== targetSym) {{
            term.pickPair(targetSym, 'agg-perp');
        }}
        if (term.prefs.itv !== targetItv) {{
            term.setItv(targetItv);
        }}

        return new Promise((resolve) => {{
            let checks = 0;
            const timeout = setTimeout(() => {{
                clearInterval(poll);
                resolve({{ error: 'timeout_waiting_for_footprint_data' }});
            }}, 6000);

            const poll = setInterval(() => {{
                checks++;
                const sigSets = term.sigSets ? term.sigSets() : null;
                const timeSet = sigSets ? sigSets.find(s => s.bt === 'time') : null;
                const X = timeSet ? timeSet.X : null;

                if (term.prefs.sym === targetSym && X && X.T && X.T.length > 0) {{
                    clearInterval(poll);
                    clearTimeout(timeout);

                    // 1. Harvest Signals
                    const signals = [];
                    for (const set of (sigSets || [])) {{
                        const setX = set.X;
                        for (const s of (set.sigs || [])) {{
                            const timeMs = setX && setX.T ? setX.T[s.i] : null;
                            if (!timeMs) continue;
                            const text = (term.sigText ? term.sigText(s, setX) : null) || ['', ''];
                            signals.push({{
                                symbol: targetSym,
                                bar_type: set.bt || 'time',
                                bar_label: set.label || set.bt || targetItv,
                                time_ms: timeMs,
                                datetime_utc: new Date(timeMs).toISOString(),
                                sig_code: s.t,
                                sig_name: term.sigName ? term.sigName(s) : (s.t || 'Signal'),
                                side: s.side === 'buy' ? 'bullish' : 'bearish',
                                price: s.p,
                                title: text[0] || '',
                                description: text[1] || ''
                            }});
                        }}
                    }}

                    // 2. Harvest Completed Candles & Footprint Cluster Matrices
                    const candles = [];
                    const n = X.T.length;
                    let cumDelta = 0;
                    const includeLadder = {'true' if include_ladder else 'false'};

                    for (let i = 0; i < n; i++) {{
                        const t = X.T[i];
                        const te = X.TE ? X.TE[i] : (t + 15 * 60 * 1000);
                        const o = X.O[i];
                        const h = X.H[i];
                        const l = X.L[i];
                        const c = X.C[i];
                        const F = X.F ? X.F[i] : null;

                        let buyVol = 0, sellVol = 0, maxVol = 0, pocIdx = -1;
                        const ladder = [];

                        if (F && F.b && F.s) {{
                            const len = Math.min(F.b.length, F.s.length);
                            for (let j = 0; j < len; j++) {{
                                const b = F.b[j] || 0;
                                const s = F.s[j] || 0;
                                buyVol += b;
                                sellVol += s;
                                const tot = b + s;
                                if (tot > maxVol) {{
                                    maxVol = tot;
                                    pocIdx = j;
                                }}
                                if (includeLadder && tot > 0) {{
                                    const p = (F.lo !== undefined && F.tick) ? (F.lo + j * F.tick) : null;
                                    ladder.push({{
                                        price: p,
                                        bid_vol: b,
                                        ask_vol: s,
                                        level_delta: b - s
                                    }});
                                }}
                            }}
                        }}

                        const delta = buyVol - sellVol;
                        cumDelta += delta;
                        const pocPrice = (F && pocIdx >= 0 && F.lo !== undefined && F.tick) 
                            ? (F.lo + pocIdx * F.tick) 
                            : null;

                        const bar = {{
                            open_time_ms: t,
                            close_time_ms: te,
                            open_time_utc: new Date(t).toISOString(),
                            close_time_utc: new Date(te).toISOString(),
                            open: o, high: h, low: l, close: c,
                            total_volume: buyVol + sellVol,
                            buy_volume: buyVol,
                            sell_volume: sellVol,
                            delta: delta,
                            cumulative_delta: cumDelta,
                            poc_price: pocPrice
                        }};

                        if (includeLadder) {{
                            bar.footprint_ladder = ladder;
                        }}
                        candles.push(bar);
                    }}

                    resolve({{
                        symbol: targetSym,
                        market: term.prefs.mkt || 'agg-perp',
                        timeframe: targetItv,
                        candle_count: candles.length,
                        candles: candles,
                        signals: signals
                    }});
                }}
            }}, 50);
        }});
    }})()"""


@app.get("/")
def root():
    return {
        "service": "CoinGlass Footprint Browser API",
        "status": "online",
        "docs": "/docs",
        "endpoints": [
            "GET /api/v1/footprint/{symbol}",
            "GET /api/v1/footprint/{symbol}/signals",
            "POST /api/v1/footprint/{symbol}/dump",
            "GET /api/v1/health"
        ]
    }


@app.get("/api/v1/health")
async def health_check():
    tabs = get_cdp_tabs()
    has_footprint = any("coinglass.com/footprint" in t.get("url", "").lower() for t in tabs)
    return {
        "cdp_alive": len(tabs) > 0,
        "cdp_port": 9222,
        "tabs_count": len(tabs),
        "footprint_tab_detected": has_footprint
    }


@app.get("/api/v1/footprint/{symbol}")
async def get_footprint(
    symbol: str,
    timeframe: str = Query("15m", regex="^(1m|3m|5m|15m|30m|1h|4h|1d)$"),
    include_ladder: bool = Query(True, description="Include price-by-price bid/ask tick clusters"),
    limit: Optional[int] = Query(None, description="Limit number of recent candles returned")
):
    """
    Returns native footprint data directly from browser memory (React Fiber window.__cg_term).
    Contains:
    - Candle OHLCV
    - Footprint Ladder (per-tick bid/ask volume)
    - Delta & Cumulative Delta
    - POC (Point of Control)
    - Algo signals (Trapped longs/shorts, Absorptions, Exhaustions, CVD Div)
    """
    symbol = symbol.upper()
    async with _LOCK:
        tab_id, ws_url = open_or_get_footprint_tab(symbol)
        if not ws_url:
            raise HTTPException(
                status_code=503,
                detail="Could not connect to Chrome CDP (port 9222). Ensure Chrome is running with --remote-debugging-port=9222."
            )

        # Ensure terminal is hooked
        hooked = False
        for _ in range(8):
            try:
                hooked = await cdp_eval_js(ws_url, HOOK_TERMINAL_JS)
                if hooked:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.5)

        if not hooked:
            raise HTTPException(
                status_code=504,
                detail="Footprint terminal not ready in browser. Ensure the page has loaded and Cloudflare check is passed."
            )

        # Extract structured data
        js = build_footprint_extraction_js(symbol, timeframe=timeframe, include_ladder=include_ladder)
        data = await cdp_eval_js(ws_url, js, timeout=10.0)

        if not data or "error" in data:
            raise HTTPException(status_code=500, detail=data.get("error", "Data extraction failed"))

        if limit and "candles" in data and len(data["candles"]) > limit:
            data["candles"] = data["candles"][-limit:]
            data["candle_count"] = len(data["candles"])

        return JSONResponse(content=data)


@app.get("/api/v1/footprint/{symbol}/signals")
async def get_footprint_signals(symbol: str, timeframe: str = "15m"):
    """Returns detected footprint order flow signals for the symbol."""
    res = await get_footprint(symbol, timeframe=timeframe, include_ladder=False, limit=50)
    body = json.loads(res.body.decode())
    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "count": len(body.get("signals", [])),
        "signals": body.get("signals", [])
    }


@app.post("/api/v1/ingest")
async def ingest_footprint_data(payload: Dict[str, Any], background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Receives direct in-memory dumps pushed from the in-browser Tampermonkey/userscript bridge.
    Zero scraping - pure JSON payload directly persisted to disk.
    """
    sym = payload.get("symbol", "UNKNOWN").upper()
    itv = payload.get("timeframe", "15m")
    candles_count = payload.get("candle_count", len(payload.get("candles", [])))
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    out_file = DUMP_DIR / f"{sym}_footprint_{itv}_live_{timestamp_str}.json"

    def _write_file(data: dict, path: Path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Ingested and saved {sym} ({itv}) footprint data ({candles_count} candles) to {path.name}")

    background_tasks.add_task(_write_file, payload, out_file)
    return {
        "status": "ingested",
        "symbol": sym,
        "timeframe": itv,
        "candles_received": candles_count,
        "file": str(out_file)
    }


@app.post("/api/v1/footprint/{symbol}/dump")
async def dump_footprint_data(
    symbol: str,
    timeframe: str = "15m",
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Pulls footprint data from the browser and dumps it locally as JSON and Parquet.
    Zero DOM scraping: saves pure high-fidelity tick and auction data.
    """
    res = await get_footprint(symbol, timeframe=timeframe, include_ladder=True)
    payload = json.loads(res.body.decode())

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = DUMP_DIR / f"{symbol.upper()}_footprint_{timeframe}_{timestamp_str}.json"

    def _write_dump(data_to_write: dict, dest: Path):
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(data_to_write, f, indent=2)
        logger.info(f"Successfully dumped {symbol} footprint to {dest.name}")

    background_tasks.add_task(_write_dump, payload, out_file)

    return {
        "status": "dump_queued",
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "candles_saved": payload.get("candle_count", 0),
        "target_file": str(out_file)
    }


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting CoinGlass Footprint Browser API on http://127.0.0.1:8008...")
    uvicorn.run("coinglass_footprint_api:app", host="127.0.0.1", port=8008, reload=False, log_level="info")
