import re
"""
2_coinglass_whales.py
========================================================================================
Standalone CoinGlass Large Orderbook & Whale Blotter Scraper (In-Browser CDP Bridge)
========================================================================================
Pillar 2 of 5 Standalone Crypto Pipeline:
- Strictly operates via Chrome DevTools Protocol (CDP) on 127.0.0.1:9222.
- Ephemeral tab lifecycle: opens tab (?bot=whale_scraper), harvests, closes immediately.
- Reaps any orphaned bot tabs; strictly preserves user & delta exchange tabs.
- Scrapes:
  1. Whale Market Snapshots (Wall imbalance bid:ask, bid walls USD, ask walls USD, open walls)
  2. Active Open Whale Orders (Table 2 virtual scrolling in .lo-bt-body)
  3. Whale History Orders (completed/cancelled whale walls)
- Continuous structural decay: computes exp(-age/tau) with tau=1800/ln(2) (30m half-life).
- Persists to:
  * Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet
  * Database/Whale_Orders/History_Orders/<SYMBOL>_whale_history_orders.parquet
  * Database/Whale_Orders/whale_market_snapshots.parquet
- Primary IST columns enriched across all tables.
- Runs on scheduled cadences (minutes :00, :15, :35, :50) or on-demand (--once).
========================================================================================
"""

import sys
import os
import json
import time
import logging
import asyncio
import argparse
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_parquet, upsert_parquet, cdp_result
from Terminal.collector_lock import BrowserLock, rotate_warp_ip, WHALE_MINUTES, calculate_wall_decay_weight
from Terminal.universal_footprint_engine import utc_ms_to_ist_str

DB_DIR = ROOT_DIR / "Database"
WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_DIR = WHALE_DIR / "Open_Orders"
HIST_DIR = WHALE_DIR / "History_Orders"
SNAPSHOT_PATH = WHALE_DIR / "whale_market_snapshots.parquet"

OPEN_DIR.mkdir(parents=True, exist_ok=True)
HIST_DIR.mkdir(parents=True, exist_ok=True)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [CG-WHALES] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("WhaleScraper")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# High-liquidity assets with active orderbook walls
CORE_WHALE_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "SUI", "NEAR", "HYPE"
]


def cleanup_orphan_tabs(keep_tab_id: Optional[str] = None):
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "bot=whale_scraper" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Reaped orphaned whale tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Whale tab reaper: {e}")


def open_whale_tab(initial_sym: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    cleanup_orphan_tabs()
    try:
        url = f"https://www.coinglass.com/large-orderbook-statistics?symbol={initial_sym}&bot=whale_scraper"
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=6)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened fresh ephemeral Whale tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open whale tab via CDP: {e}")
        return None, None


def close_tab(tab_id: Optional[str]):
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed whale orders tab ({tab_id}) - Browser clean.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    cleanup_orphan_tabs()


def parse_currency(val_str: Optional[str]) -> float:
    if not val_str or pd.isna(val_str):
        return 0.0
    s = str(val_str).replace('$', '').replace(',', '').strip()
    multiplier = 1.0
    if s.endswith('M') or s.endswith('m'):
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s.endswith('K') or s.endswith('k'):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith('B') or s.endswith('b'):
        multiplier = 1_000_000_000.0
        s = s[:-1]
    try:
        return float(s) * multiplier
    except Exception:
        return 0.0


def parse_percentage(val_str: Optional[str]) -> float:
    if not val_str or pd.isna(val_str):
        return 0.0
    s = str(val_str).replace('%', '').replace('+', '').replace('−', '-').replace('–', '-').strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_duration_seconds(dur_str: Optional[str]) -> float:
    if not dur_str or pd.isna(dur_str):
        return 0.0
    s = str(dur_str).strip()
    total = 0.0
    d_m = re.search(r'(\d+)\s*d', s)
    if d_m: total += float(d_m.group(1)) * 86400
    h_m = re.search(r'(\d+)\s*h', s)
    if h_m: total += float(h_m.group(1)) * 3600
    m_m = re.search(r'(\d+)\s*m', s)
    if m_m: total += float(m_m.group(1)) * 60
    s_m = re.search(r'(\d+)\s*s', s)
    if s_m: total += float(s_m.group(1))
    if total == 0.0:
        sp = s.replace('s', '').split()
        if len(sp) == 2:
            try:
                total = float(sp[0]) * 60 + float(sp[1])
            except Exception:
                pass
    return total


def persist_whale_open_orders(symbol: str, rows: List[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    now_utc = datetime.now(timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)
    df["snapshot_time_utc"] = now_utc.isoformat()
    df["snapshot_time_ist"] = utc_ms_to_ist_str(now_ms)
    df["time_ist"] = df["snapshot_time_ist"]
    df["time_ms"] = now_ms

    # Apply continuous age decay
    df["decay_weight"] = 1.0
    for idx, r in df.iterrows():
        secs = float(r.get("duration_seconds", 0.0) or parse_duration_seconds(r.get("duration_str")))
        df.at[idx, "decay_weight"] = calculate_wall_decay_weight(secs)

    out_path = OPEN_DIR / f"{symbol}_whale_open_orders.parquet"
    atomic_parquet(df, out_path)
    logger.info(f"[{symbol}] Saved {out_path.name}: {len(df)} active open whale walls.")


def persist_whale_history_orders(symbol: str, rows: List[dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    now_utc = datetime.now(timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)
    df["snapshot_time_utc"] = now_utc.isoformat()
    df["snapshot_time_ist"] = utc_ms_to_ist_str(now_ms)
    df["time_ist"] = df["snapshot_time_ist"]
    df["time_ms"] = now_ms

    out_path = HIST_DIR / f"{symbol}_whale_history_orders.parquet"
    upsert_parquet(df, out_path, ["venue", "side", "price", "initial_usd", "duration_str"], sort_by="time_ms", limit=50000)


def persist_whale_market_snapshots(snapshots: List[dict]):
    if not snapshots:
        return
    df = pd.DataFrame(snapshots)
    now_utc = datetime.now(timezone.utc)
    now_ms = int(now_utc.timestamp() * 1000)
    df["snapshot_time_utc"] = now_utc.isoformat()
    df["snapshot_time_ist"] = utc_ms_to_ist_str(now_ms)
    df["time_ist"] = df["snapshot_time_ist"]
    df["time_ms"] = now_ms

    upsert_parquet(df, SNAPSHOT_PATH, ["symbol", "time_ms"], sort_by="time_ms", limit=50000)


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    return await cdp_result(ws, {
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {"expression": expr, "returnByValue": True, "awaitPromise": await_promise}
    })


async def harvest_whale_orders_for_symbol(ws, symbol: str) -> dict:
    url = f"https://www.coinglass.com/large-orderbook-statistics?symbol={symbol}"
    logger.info(f"[{symbol}] Navigating to {url}...")
    await ws.send(json.dumps({"id": 100, "method": "Page.navigate", "params": {"url": url}}))
    await ws.recv()
    await asyncio.sleep(3.5)

    # 1. Macro Indicators
    macro = await cdp_eval(ws, '''(() => {
        const body = document.body.innerText;
        let lastPrice = null;
        const pMatch = body.match(/Last price\\s*([\\d,\\.]+)/i);
        if (pMatch) lastPrice = parseFloat(pMatch[1].replace(/,/g, ''));
        
        let bidImb = null, askImb = null;
        const imbMatch = body.match(/Wall imbalance bid : ask\\s*(\\d+)\\s*:\\s*(\\d+)/i);
        if (imbMatch) {
            bidImb = parseFloat(imbMatch[1]);
            askImb = parseFloat(imbMatch[2]);
        }
        
        let openWalls = null;
        const owMatch = body.match(/Open walls\\s*([\\d,]+)/i);
        if (owMatch) openWalls = parseInt(owMatch[1].replace(/,/g, ''));
        
        let bidWallsTotal = null, askWallsTotal = null;
        const bMatch = body.match(/Bid walls\\s*([\\$\\d\\.,]+[KMB]?)/i);
        if (bMatch) bidWallsTotal = bMatch[1];
        const aMatch = body.match(/Ask walls\\s*([\\$\\d\\.,]+[KMB]?)/i);
        if (aMatch) askWallsTotal = aMatch[1];
        
        let wallStructure = null;
        const wsMatch = body.match(/Wall structure[^\\n]*\\n([^\\n]+)/i);
        if (wsMatch) wallStructure = wsMatch[1].trim();

        return {
            last_price: lastPrice,
            bid_imbalance: bidImb,
            ask_imbalance: askImb,
            open_walls_count: openWalls,
            bid_walls_total: bidWallsTotal,
            ask_walls_total: askWallsTotal,
            wall_structure: wallStructure
        };
    })()''', msg_id=101) or {}

    # Scroll down to Table 2 virtual container
    await cdp_eval(ws, 'window.scrollTo(0, 1600);', msg_id=102)
    await asyncio.sleep(0.5)

    # Ensure Open Orders Tab is selected
    await cdp_eval(ws, '''(() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const openTab = tabs.find(t => (t.innerText || '').startsWith('Open'));
        if (openTab) openTab.click();
    })()''', msg_id=103)
    await asyncio.sleep(0.6)

    # 2. Harvest Open Orders Virtual Table
    raw_open = await cdp_eval(ws, '''(async () => {
        const container = document.querySelector('.lo-bt-body');
        if (!container) return [];
        const seen = new Map();
        container.scrollTop = 0;
        await new Promise(r => setTimeout(r, 100));
        let currentScroll = 0;
        const maxScroll = container.scrollHeight;
        let steps = 0;
        while (currentScroll <= maxScroll + 850 && steps < 60) {
            container.scrollTop = currentScroll;
            await new Promise(r => setTimeout(r, 35));
            const trs = Array.from(container.querySelectorAll('table tbody tr'));
            for (const tr of trs) {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                if (tds.length >= 8) {
                    const key = `${tds[0]}_${tds[1]}_${tds[3]}_${tds[7]}`;
                    if (!seen.has(key)) seen.set(key, tds);
                }
            }
            currentScroll += 850;
            steps++;
        }
        return Array.from(seen.values());
    })()''', msg_id=104, await_promise=True) or []

    # 3. Harvest History Orders Tab
    await cdp_eval(ws, '''(() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const histTab = tabs.find(t => (t.innerText || '').startsWith('History'));
        if (histTab) histTab.click();
    })()''', msg_id=105)
    await asyncio.sleep(0.6)

    raw_history = await cdp_eval(ws, '''(async () => {
        const container = document.querySelector('.lo-bt-body');
        if (!container) return [];
        const seen = new Map();
        container.scrollTop = 0;
        await new Promise(r => setTimeout(r, 100));
        let currentScroll = 0;
        const maxScroll = container.scrollHeight;
        let steps = 0;
        while (currentScroll <= maxScroll + 850 && steps < 60) {
            container.scrollTop = currentScroll;
            await new Promise(r => setTimeout(r, 35));
            const trs = Array.from(container.querySelectorAll('table tbody tr'));
            for (const tr of trs) {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                if (tds.length >= 8) {
                    const key = `${tds[0]}_${tds[1]}_${tds[3]}_${tds[7]}`;
                    if (!seen.has(key)) seen.set(key, tds);
                }
            }
            currentScroll += 850;
            steps++;
        }
        return Array.from(seen.values());
    })()''', msg_id=106, await_promise=True) or []

    # Parse open orders with accurate column offsets
    open_orders = []
    for r in raw_open:
        if len(r) < 8 or not r[0]:
            continue
        try:
            v_side = r[0].replace('★', '').strip()
            side = 'bid' if 'bid' in v_side.lower() else ('ask' if 'ask' in v_side.lower() else 'unknown')
            venue = re.sub(r'\s+(Bid|Ask)$', '', v_side, flags=re.I).strip()
            p = parse_currency(r[1])
            dist = parse_percentage(r[2])
            init_usd = parse_currency(r[3])
            rem_usd = parse_currency(r[4])
            filled_usd = parse_currency(r[5])
            fills = int(r[6].replace(',', '')) if r[6].replace(',', '').isdigit() else 0
            dur_str = r[7]
            dur_secs = parse_duration_seconds(dur_str)

            if p > 0:
                open_orders.append({
                    "symbol": symbol,
                    "venue": venue,
                    "side": side,
                    "price": p,
                    "from_price_pct": dist,
                    "initial_usd": init_usd,
                    "remaining_usd": rem_usd,
                    "filled_usd": filled_usd,
                    "fills": fills,
                    "duration_str": dur_str,
                    "duration_seconds": dur_secs
                })
        except Exception:
            continue

    # Parse history orders with accurate column offsets
    history_orders = []
    for r in raw_history:
        if len(r) < 8 or not r[0]:
            continue
        try:
            v_side = r[0].replace('★', '').strip()
            side = 'bid' if 'bid' in v_side.lower() else ('ask' if 'ask' in v_side.lower() else 'unknown')
            venue = re.sub(r'\s+(Bid|Ask)$', '', v_side, flags=re.I).strip()
            p = parse_currency(r[1])
            dist = parse_percentage(r[2])
            init_usd = parse_currency(r[3])
            rem_usd = parse_currency(r[4])
            filled_usd = parse_currency(r[5])
            fills = int(r[6].replace(',', '')) if r[6].replace(',', '').isdigit() else 0
            dur_str = r[7]
            ended_str = r[8] if len(r) > 8 else ""
            status_str = r[9] if len(r) > 9 else ""
            dur_secs = parse_duration_seconds(dur_str)

            if p > 0:
                history_orders.append({
                    "symbol": symbol,
                    "venue": venue,
                    "side": side,
                    "price": p,
                    "from_price_pct": dist,
                    "initial_usd": init_usd,
                    "remaining_usd": rem_usd,
                    "filled_usd": filled_usd,
                    "fills": fills,
                    "duration_str": dur_str,
                    "duration_seconds": dur_secs,
                    "ended_str": ended_str,
                    "status": status_str
                })
        except Exception:
            continue

    # Build market snapshot
    snapshot = {
        "symbol": symbol,
        "last_price": macro.get("last_price"),
        "bid_imbalance_ratio": (macro.get("bid_imbalance") or 50.0) / 100.0,
        "ask_imbalance_ratio": (macro.get("ask_imbalance") or 50.0) / 100.0,
        "open_walls_count": macro.get("open_walls_count") or len(open_orders),
        "total_bid_walls_usd": parse_currency(macro.get("bid_walls_total")),
        "total_ask_walls_usd": parse_currency(macro.get("ask_walls_total")),
        "wall_structure": macro.get("wall_structure")
    }

    return {
        "macro": snapshot,
        "open_orders": open_orders,
        "history_orders": history_orders
    }


async def run_sweep(symbols: Optional[List[str]] = None) -> bool:
    target_symbols = symbols or CORE_WHALE_SYMBOLS
    acquire_singleton("coinglass_whales", DB_DIR / ".locks")

    with BrowserLock("WhaleScraper", timeout_secs=20.0) as lock:
        if not lock.acquired:
            logger.warning("Browser lock contention for Whale sweep. Yielding.")
            return False

        tab_id, ws_url = open_whale_tab(target_symbols[0])
        if not tab_id or not ws_url:
            logger.error("Could not open Whale tab via CDP.")
            return False

        try:
            async with websockets.connect(ws_url, open_timeout=8, close_timeout=3, max_size=50_000_000, ping_interval=None) as ws:
                snapshots = []
                for sym in target_symbols:
                    try:
                        res = await harvest_whale_orders_for_symbol(ws, sym)
                        if res.get("open_orders"):
                            persist_whale_open_orders(sym, res["open_orders"])
                        if res.get("history_orders"):
                            persist_whale_history_orders(sym, res["history_orders"])
                        if res.get("macro") and res["macro"].get("last_price"):
                            snapshots.append(res["macro"])
                    except Exception as e:
                        logger.error(f"[{sym}] Error harvesting whale orders: {e}")

                if snapshots:
                    persist_whale_market_snapshots(snapshots)
                    logger.info(f"Saved {len(snapshots)} whale market snapshots.")
                return True
        except Exception as e:
            logger.error(f"WebSocket execution error: {e}")
            return False
        finally:
            close_tab(tab_id)


def get_seconds_until_next_whale_minute() -> float:
    now = datetime.now()
    for m in sorted(WHALE_MINUTES):
        candidate = now.replace(minute=m, second=5, microsecond=0)
        if candidate > now:
            return (candidate - now).total_seconds()
    next_hour = now + timedelta(hours=1)
    target = next_hour.replace(minute=sorted(WHALE_MINUTES)[0], second=5, microsecond=0)
    return max(1.0, (target - now).total_seconds())


async def run_daemon():
    acquire_singleton("coinglass_whales", DB_DIR / ".locks")
    logger.info(f"CoinGlass Whales Daemon active. Scheduled for minutes {WHALE_MINUTES}.")

    while True:
        wait_s = get_seconds_until_next_whale_minute()
        logger.info(f"Sleeping {wait_s:.1f}s until next whale sweep window...")
        await asyncio.sleep(wait_s)
        try:
            success = await run_sweep()
            if success is False:
                logger.info("Whale sweep yielded on lock contention; retrying in 15s...")
                await asyncio.sleep(15.0)
                await run_sweep()
        except Exception as e:
            logger.error(f"Scheduled sweep error: {e}")


def main():
    parser = argparse.ArgumentParser(description="CoinGlass Whales Scraper")
    parser.add_argument("--daemon", action="store_true", help="Run daemon on :00, :15, :35, :50")
    parser.add_argument("--once", action="store_true", help="Run single sweep and exit")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to sweep (e.g. BTC ETH SOL)")
    args = parser.parse_args()

    if args.daemon:
        asyncio.run(run_daemon())
    else:
        asyncio.run(run_sweep(args.symbols))


if __name__ == "__main__":
    main()
