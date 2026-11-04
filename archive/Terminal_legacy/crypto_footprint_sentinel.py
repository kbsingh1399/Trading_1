"""
========================================================================================
CoinGlass Crypto Footprint & Signals Sentinel (100% Free CDP Order-Flow Harvester)
========================================================================================
- Ephemeral Tab Lifecycle: Opens tagged tab (bot=footprint_sentinel), sweeps, and CLOSES it.
- Event-Driven WARP IP Rotation: Rotates automatically on Cloudflare challenge or empty scrapes.
- Tracks ALL crypto assets from CoinGlass (Tier 1: BTC, ETH, SOL, XRP, XAUT, DOGE, BNB + Tier 2/3 catalog).
- Captures EVERY Footprint Order-Flow signal from the live chart & sidebar:
    * Absorption (Buying/Selling absorbed at highs/lows)
    * Exhaustion (Buyer/Seller exhaustion)
    * Divergence & Cumulative Delta Divergence (Bull div, Bear div)
    * Trapped Shorts & Failed Breakdowns
- Persists all harvested signals to Database/Signals/footprint_signals.parquet & .jsonl.
- Automatically triggers RealtimeSignalEvaluator on every newly detected signal.
- Enforces strict prop-firm safety guardrails (confluence >= 75.0, broker-side SL/TP).
========================================================================================
"""

import sys
import os
import json
import time
import math
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_parquet, upsert_parquet, atomic_json
from Terminal.collector_lock import rotate_warp_ip
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.auction_indicators import update_all_auction_profiles

DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_PARQUET = SIGNALS_DIR / "footprint_signals.parquet"
SIGNALS_JSONL = SIGNALS_DIR / "footprint_signals.jsonl"
ASSETS_CATALOG = DB_DIR / "coinglass_footprint_assets.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [FOOTPRINT-SENTINEL] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("FootprintSentinel")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# High-priority assets swept on every tight cycle
TIER_1_CORE = ["BTC", "ETH", "SOL", "XRP", "XAUT", "DOGE", "BNB"]

# In-memory tracking of seen signals to prevent duplicate evaluations
SEEN_SIGNAL_KEYS: Set[Tuple[str, str, str, float]] = set()
evaluator: Optional[RealtimeSignalEvaluator] = None


def load_all_crypto_symbols() -> List[str]:
    """Loads all crypto assets from the 94-asset catalog, ensuring metals (XAUT, PAXG, XAU, XAG) are included."""
    symbols = list(TIER_1_CORE)
    if ASSETS_CATALOG.exists():
        try:
            with open(ASSETS_CATALOG, "r", encoding="utf-8") as f:
                cat = json.load(f)
                agg = cat.get("aggregatedSymbols", [])
                for s in agg:
                    if s not in symbols:
                        symbols.append(s)
        except Exception as e:
            logger.warning(f"Could not read assets catalog: {e}")
    # Explicitly ensure crypto-metal perpetuals are tracked
    for metal in ["XAUT", "PAXG", "XAU", "XAG"]:
        if metal not in symbols:
            symbols.append(metal)
    return symbols


def load_seen_signals():
    global SEEN_SIGNAL_KEYS
    if SIGNALS_PARQUET.exists():
        try:
            df = pd.read_parquet(SIGNALS_PARQUET)
            for _, r in df.iterrows():
                key = (str(r.get("symbol")), str(r.get("tag")), str(r.get("time")), float(r.get("price") or 0.0))
                SEEN_SIGNAL_KEYS.add(key)
            logger.info(f"Loaded {len(SEEN_SIGNAL_KEYS)} known footprint signal keys into memory.")
        except Exception as e:
            logger.warning(f"Could not load historical signals: {e}")


def cleanup_footprint_tabs(keep_tab_id: Optional[str] = None):
    """Closes all orphaned CoinGlass Footprint collector tabs in Chrome to keep browser lean and free."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "bot=footprint_sentinel" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Cleaned up orphan Footprint tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Tab cleanup error: {e}")


def open_ephemeral_footprint_tab(initial_sym: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    """Opens a fresh ephemeral CoinGlass Footprint tab tagged with bot=footprint_sentinel."""
    cleanup_footprint_tabs()
    try:
        url = f"https://www.coinglass.com/footprint?symbol={initial_sym}&bot=footprint_sentinel"
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened ephemeral CoinGlass Footprint tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open footprint tab via CDP: {e}")
        return None, None


def close_footprint_tab(tab_id: Optional[str]):
    """Closes ephemeral Footprint tab immediately after sweep to keep browser lean & free."""
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed Footprint tab ({tab_id}) - Tab footprint zero.")
        except Exception as e:
            logger.warning(f"Could not close Footprint tab {tab_id}: {e}")
    cleanup_footprint_tabs()


async def extract_symbol_signals(ws, symbol: str) -> List[Dict[str, Any]]:
    """Navigates to symbol and extracts all sidebar order-flow signals from the live DOM."""
    url = f"https://www.coinglass.com/footprint?symbol={symbol}&bot=footprint_sentinel"
    await ws.send(json.dumps({
        "id": 1,
        "method": "Page.navigate",
        "params": {"url": url}
    }))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("id") == 1:
            break

    # Wait for initial render
    await asyncio.sleep(2.8)

    # Check for Cloudflare challenge
    check_expr = """
    (() => {
        const title = document.title || '';
        const body = document.body ? document.body.innerText : '';
        const isCf = title.includes('Just a moment') || body.includes('Verify you are human') || document.querySelector('#challenge-running, #cf-turnstile');
        return { isChallenge: !!isCf, title: title };
    })()
    """
    await ws.send(json.dumps({"id": 11, "method": "Runtime.evaluate", "params": {"expression": check_expr, "returnByValue": True}}))
    while True:
        cf_msg = json.loads(await ws.recv())
        if cf_msg.get("id") == 11:
            break
    cf_val = cf_msg.get("result", {}).get("result", {}).get("value", {})
    if cf_val.get("isChallenge"):
        logger.warning(f"[{symbol}] Cloudflare challenge detected ({cf_val.get('title')})! Rotating WARP IP...")
        rotate_warp_ip("FootprintSentinel", reason="CHALLENGE", force=True)
        await asyncio.sleep(5.0)
        return []

    extract_expr = f"""
    (() => {{
        const sym = '{symbol}';
        const rows = document.querySelectorAll('.SidePanel_lrow__m40N_');
        return Array.from(rows).map(row => {{
            const tm = row.querySelector('.SidePanel_tm__ELzVF');
            const timeText = tm ? (tm.querySelector('b') ? tm.querySelector('b').innerText : '') : '';
            const tfText = tm ? (tm.querySelector('small') ? tm.querySelector('small').innerText : '') : '';
            
            const tagEl = row.querySelector('.SidePanel_tag__LWh0H');
            const tag = tagEl ? tagEl.innerText : '';
            const tagClass = tagEl ? tagEl.className : '';
            const side = tagClass.includes('buy') ? 'BUY' : tagClass.includes('sell') ? 'SELL' : 'NEUTRAL';
            
            const descEl = row.querySelector('.SidePanel_d__e4aqB');
            const title = descEl ? (descEl.querySelector('b') ? descEl.querySelector('b').innerText : '') : '';
            const desc = descEl ? (descEl.querySelector('span') ? descEl.querySelector('span').innerText : '') : '';
            
            let price = null;
            if (desc.includes('·')) {{
                const parts = desc.split('·');
                price = parseFloat(parts[parts.length - 1].replace(/,/g, '').trim()) || null;
            }}
            
            return {{
                symbol: sym,
                time: timeText,
                timeframe: tfText,
                tag: tag,
                side: side,
                title: title,
                description: desc,
                price: price
            }};
        }});
    }})()
    """
    await ws.send(json.dumps({
        "id": 2,
        "method": "Runtime.evaluate",
        "params": {"expression": extract_expr, "returnByValue": True}
    }))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("id") == 2:
            break

    raw_signals = msg.get("result", {}).get("result", {}).get("value") or []
    now_utc = datetime.now(timezone.utc).isoformat()
    enriched = []
    for s in raw_signals:
        s["harvested_at_utc"] = now_utc
        enriched.append(s)
    return enriched


def persist_footprint_signals(signals: List[Dict[str, Any]]):
    """Persists signals atomically to both Parquet and JSONL."""
    if not signals:
        return
    df = pd.DataFrame(signals)
    upsert_parquet(df, SIGNALS_PARQUET, ["symbol", "tag", "time", "price"], sort_by="time", limit=50000)
    with open(SIGNALS_JSONL, "a", encoding="utf-8") as f:
        for s in signals:
            f.write(json.dumps(s) + "\n")


async def run_footprint_sentinel_sweep(ws, symbols_to_sweep: List[str]) -> List[Dict[str, Any]]:
    """Runs a sweep over the target symbols and triggers real-time evaluation on new signals."""
    global SEEN_SIGNAL_KEYS, evaluator
    new_signals_found = []

    for sym in symbols_to_sweep:
        try:
            t0 = time.time()
            sigs = await extract_symbol_signals(ws, sym)
            dt = time.time() - t0
            
            new_for_sym = []
            for s in sigs:
                key = (s["symbol"], s["tag"], s["time"], float(s["price"] or 0.0))
                if key not in SEEN_SIGNAL_KEYS:
                    SEEN_SIGNAL_KEYS.add(key)
                    new_for_sym.append(s)
                    new_signals_found.append(s)
            
            status_str = f"({len(new_for_sym)} NEW)" if new_for_sym else ""
            logger.info(f"[{sym}] {len(sigs)} signals in {dt:.1f}s {status_str}")
            
            if new_for_sym:
                persist_footprint_signals(new_for_sym)
                if evaluator:
                    for ns in new_for_sym:
                        # Translate footprint signal to evaluator format
                        eval_sig = {
                            "symbol": ns["symbol"],
                            "sig_name": f"{ns['tag']}: {ns['title']}",
                            "sig_code": ns["tag"].lower()[:4],
                            "side": "bullish" if ns["side"] == "BUY" else "bearish",
                            "title": ns["title"],
                            "description": ns["description"],
                            "price": ns["price"] or 0.0,
                            "detection_time_utc": datetime.now(timezone.utc).isoformat(),
                            "timeframe": ns.get("timeframe", "15m")
                        }
                        logger.info(f"🚨 [REALTIME SENTINEL ALERT] Evaluating {ns['symbol']} {ns['side']} {ns['tag']} @ {ns['price']}...")
                        res = evaluator.evaluate_signal(eval_sig)
                        if res and res.get("is_actionable"):
                            logger.info(f"🎯 [ACTIONABLE CONFLUENCE] {res['symbol']} Score: {res['confluence_score']}/100 -> Entry: {res['trade_levels']['entry_price']}, SL: {res['trade_levels']['stop_loss']}, TP1: {res['trade_levels']['take_profit_1']}")
        except Exception as e:
            logger.warning(f"[{sym}] Sweep extraction error: {e}")

    return new_signals_found


async def run_sentinel_daemon():
    """Continuous daemon that runs Tier 1 on every cycle and rotates through Tier 2/3 with ephemeral tabs."""
    acquire_singleton("crypto_footprint_sentinel", DB_DIR / ".locks")
    global evaluator
    evaluator = RealtimeSignalEvaluator()
    load_seen_signals()
    all_crypto = load_all_crypto_symbols()
    logger.info(f"Initialized Crypto Footprint Sentinel across {len(all_crypto)} crypto assets.")

    tier2_pool = [s for s in all_crypto if s not in TIER_1_CORE]
    tier2_idx = 0
    batch_size = 5

    while True:
        tab_id, ws_url = open_ephemeral_footprint_tab("BTC")
        if not ws_url:
            logger.warning("Footprint tab could not be opened on Chrome 9222. Retrying in 10s...")
            await asyncio.sleep(10.0)
            continue

        try:
            async with websockets.connect(ws_url, max_size=20_000_000, ping_interval=None) as ws:
                # 1. Sweep Tier 1 Core
                logger.info(f"--- Starting Core Crypto Sweep ({', '.join(TIER_1_CORE)}) ---")
                await run_footprint_sentinel_sweep(ws, TIER_1_CORE)

                # 2. Sweep a batch of Rotating Altcoins
                if tier2_pool:
                    batch = tier2_pool[tier2_idx:tier2_idx + batch_size]
                    tier2_idx = (tier2_idx + batch_size) % len(tier2_pool)
                    logger.info(f"--- Sweeping Altcoin Batch ({', '.join(batch)}) ---")
                    await run_footprint_sentinel_sweep(ws, batch)

        except Exception as e:
            logger.error(f"CDP WebSocket execution error: {e}")
        finally:
            close_footprint_tab(tab_id)

        logger.info("Cycle complete. Tab closed. Yielding 60 seconds before next sweep...")
        await asyncio.sleep(60.0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--single-sweep":
        async def single_sweep():
            load_seen_signals()
            global evaluator
            evaluator = RealtimeSignalEvaluator()
            tab_id, ws_url = open_ephemeral_footprint_tab("BTC")
            if ws_url:
                try:
                    async with websockets.connect(ws_url, max_size=20_000_000) as ws:
                        symbols = TIER_1_CORE + ["DOGE", "BNB"]
                        await run_footprint_sentinel_sweep(ws, symbols)
                finally:
                    close_footprint_tab(tab_id)
        asyncio.run(single_sweep())
    else:
        asyncio.run(run_sentinel_daemon())
