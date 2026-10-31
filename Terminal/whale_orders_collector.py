"""
Whale Orders & Large Order Book Blotter Collector (Direct WebSocket CDP)
=======================================================================
Ultra-fast, non-blocking ingestion engine using native Page WebSocket CDP:
1. Open Whale Orders (Active limit walls, venue, initial, remaining, fills, duration)
2. History Whale Orders (Completed, partially filled, and cancelled whale orders)
3. Whale Market Snapshots (Wall imbalance, bid/ask totals, nearest walls, largest wall)

Persists to:
- Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet
- Database/Whale_Orders/History_Orders/<SYMBOL>_whale_history_orders.parquet
- Database/Whale_Orders/whale_market_snapshots.parquet
"""

import sys
import re
import json
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(r"c:\Users\SIGMA\Documents\_Trading_1")
sys.path.insert(0, str(ROOT_DIR))
DB_DIR = ROOT_DIR / "Database"
WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_DIR = WHALE_DIR / "Open_Orders"
HIST_DIR = WHALE_DIR / "History_Orders"
SNAPSHOT_PATH = WHALE_DIR / "whale_market_snapshots.parquet"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("WhaleCollector")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "SUI", "NEAR", "HYPE"
]

MT5_SYMBOL_MAP = {
    "BTC": "BTCUSD.pi",
    "ETH": "ETHUSD.pi",
    "SOL": "SOLUSD.p",
    "XRP": "XRPUSD.pi",
    "DOGE": "DOGUSD.p",
    "BNB": "BNBUSD.p",
    "ADA": "ADAUSD.p",
    "LINK": "LNKUSD.p",
    "AVAX": "AVXUSD.p",
    "SUI": "SUIUSD.p",
    "NEAR": "NERUSD.p",
    "HYPE": "HYPEUSD.p",
}


def parse_currency(val_str: Optional[str]) -> float:
    if not val_str or pd.isna(val_str):
        return 0.0
    s = str(val_str).replace('$', '').replace(',', '').strip()
    multiplier = 1.0
    if s.endswith('K') or s.endswith('k'):
        multiplier = 1_000.0
        s = s[:-1]
    elif s.endswith('M') or s.endswith('m'):
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s.endswith('B') or s.endswith('b'):
        multiplier = 1_000_000_000.0
        s = s[:-1]
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
        return 0.0


def parse_duration(dur_str: Optional[str]) -> int:
    if not dur_str or pd.isna(dur_str):
        return 0
    s = str(dur_str).strip()
    total_seconds = 0
    d_match = re.search(r'(\d+)\s*d', s)
    h_match = re.search(r'(\d+)\s*h', s)
    m_match = re.search(r'(\d+)\s*m', s)
    s_match = re.search(r'(\d+)\s*s', s)
    if d_match: total_seconds += int(d_match.group(1)) * 86400
    if h_match: total_seconds += int(h_match.group(1)) * 3600
    if m_match: total_seconds += int(m_match.group(1)) * 60
    if s_match: total_seconds += int(s_match.group(1))
    return total_seconds


def upsert_whale_open_orders(symbol: str, rows: List[dict]):
    if not rows:
        return
    OPEN_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OPEN_DIR / f"{symbol}_whale_open_orders.parquet"
    df = pd.DataFrame(rows)
    df["snapshot_time_utc"] = datetime.now(timezone.utc).isoformat()
    df["dumped_at_utc"] = datetime.now(timezone.utc).isoformat()
    df.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {out_path.name}: {len(df)} active open whale walls")


def upsert_whale_history_orders(symbol: str, rows: List[dict]):
    if not rows:
        return
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = HIST_DIR / f"{symbol}_whale_history_orders.parquet"
    new_df = pd.DataFrame(rows)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if out_path.exists():
        try:
            old_df = pd.read_parquet(out_path)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["venue", "price", "initial_usd", "duration_str"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {out_path.name}: {len(combined)} historical whale orders")


def upsert_whale_market_snapshots(snapshots: List[dict]):
    if not snapshots:
        return
    WHALE_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(snapshots)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if SNAPSHOT_PATH.exists():
        try:
            old_df = pd.read_parquet(SNAPSHOT_PATH)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["symbol", "snapshot_time_utc"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(SNAPSHOT_PATH, index=False, engine="pyarrow")
    logger.info(f"Whale Market Snapshots saved: {SNAPSHOT_PATH.name} ({len(combined)} records)")


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


def get_large_orderbook_ws_url() -> Optional[str]:
    req = urllib.request.urlopen("http://127.0.0.1:9222/json", timeout=3)
    tabs = json.loads(req.read().decode())
    for t in tabs:
        if "large-orderbook" in t.get("url", ""):
            return t.get("webSocketDebuggerUrl")
    return None


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

        let nearestAsk = null, nearestBid = null, largestWall = null;
        const nAskMatch = body.match(/Nearest ask wall\\s*([\\d,\\.]+)[^\\n]*\\n([^\\n]+)/i);
        if (nAskMatch) nearestAsk = `${nAskMatch[1]} (${nAskMatch[2]})`;
        
        const nBidMatch = body.match(/Nearest bid wall\\s*([\\d,\\.]+)[^\\n]*\\n([^\\n]+)/i);
        if (nBidMatch) nearestBid = `${nBidMatch[1]} (${nBidMatch[2]})`;
        
        const lWallMatch = body.match(/Largest wall\\s*([\\$\\d\\.,]+[KMB]?)[^\\n]*\\n([^\\n]+)/i);
        if (lWallMatch) largestWall = `${lWallMatch[1]} (${lWallMatch[2]})`;
        
        return {
            lastPrice, bidImb, askImb, openWalls, bidWallsTotal, askWallsTotal,
            wallStructure, nearestAsk, nearestBid, largestWall
        };
    })()''', msg_id=101) or {}

    # Scroll down to Table 2
    await cdp_eval(ws, "window.scrollTo(0, 1600)", msg_id=102)
    await asyncio.sleep(0.5)

    # 2. Harvest Open Orders Tab
    await cdp_eval(ws, '''(() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const openTab = tabs.find(t => (t.innerText || '').startsWith('Open'));
        if (openTab) openTab.click();
    })()''', msg_id=103)
    await asyncio.sleep(0.8)

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
    await asyncio.sleep(0.8)

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

    mt5_sym = MT5_SYMBOL_MAP.get(symbol, f"{symbol}USD")

    def parse_rows(raw_list, tab_name):
        records = []
        for tds in raw_list:
            v_raw = tds[0].replace('★', '').replace('?', '').strip()
            side = "Ask" if "Ask" in v_raw else ("Bid" if "Bid" in v_raw else "Unknown")
            pure_v = v_raw.replace("Ask", "").replace("Bid", "").strip()
            m_type = "Spot" if "Spot" in pure_v else "Perp"
            ex = pure_v.replace("Spot", "").replace("Perp", "").strip().upper()
            try: price = float(tds[1].replace(',', ''))
            except: price = 0.0
            try: dist_pct = float(tds[2].replace('%', '').replace('+', '').replace('−', '-').replace('-', '-').strip())
            except: dist_pct = 0.0
            init_usd = parse_currency(tds[3])
            rem_usd = parse_currency(tds[4])
            fill_usd = parse_currency(tds[5]) if len(tds) > 5 else 0.0
            try: fills = int(tds[6].replace(',', '')) if len(tds) > 6 else 0
            except: fills = 0
            dur_str = tds[7] if len(tds) > 7 else ""
            dur_sec = parse_duration(dur_str)
            status = tds[8] if len(tds) > 8 else ("Open" if tab_name == "open" else "Finished")
            records.append({
                "symbol": symbol, "mt5_symbol": mt5_sym, "venue": pure_v, "exchange": ex,
                "market_type": m_type, "side": side, "price": price, "from_price_pct": dist_pct,
                "initial_usd": init_usd, "remaining_usd": rem_usd, "filled_usd": fill_usd,
                "fills_count": fills, "duration_str": dur_str, "duration_seconds": dur_sec,
                "status": status, "tab": tab_name
            })
        return records

    open_orders = parse_rows(raw_open, "open")
    history_orders = parse_rows(raw_history, "history")

    snap = {
        "symbol": symbol, "mt5_symbol": mt5_sym,
        "last_price": macro.get("lastPrice"),
        "wall_imbalance_bid_pct": macro.get("bidImb"),
        "wall_imbalance_ask_pct": macro.get("askImb"),
        "bid_walls_usd": parse_currency(macro.get("bidWallsTotal")),
        "ask_walls_usd": parse_currency(macro.get("askWallsTotal")),
        "nearest_ask_wall": macro.get("nearestAsk"),
        "nearest_bid_wall": macro.get("nearestBid"),
        "largest_wall": macro.get("largestWall"),
        "open_walls_count": macro.get("openWalls"),
        "wall_structure": macro.get("wallStructure"),
        "harvested_open_orders": len(open_orders),
        "harvested_history_orders": len(history_orders)
    }

    upsert_whale_open_orders(symbol, open_orders)
    upsert_whale_history_orders(symbol, history_orders)
    upsert_whale_market_snapshots([snap])

    return {
        "symbol": symbol,
        "open_orders_count": len(open_orders),
        "history_orders_count": len(history_orders),
        "snapshot": snap
    }


async def sweep_all_cryptos(symbols: list = None) -> dict:
    target_symbols = symbols or CRYPTO_SYMBOLS
    ws_url = get_large_orderbook_ws_url()
    if not ws_url:
        logger.error("No large-orderbook tab found in Edge!")
        return {}

    summary = {}
    async with websockets.connect(ws_url, max_size=50_000_000) as ws:
        for sym in target_symbols:
            try:
                res = await harvest_whale_orders_for_symbol(ws, sym)
                summary[sym] = res
                logger.info(f"[{sym}] Complete: {res['open_orders_count']} open, {res['history_orders_count']} history")
            except Exception as e:
                logger.error(f"[{sym}] Error: {e}")
                summary[sym] = {"error": str(e)}
    return summary


async def daemon_loop(interval_seconds: int = 180):
    logger.info(f"Starting Whale Orders Direct CDP Daemon (Interval: {interval_seconds}s)")
    while True:
        try:
            await sweep_all_cryptos()
        except Exception as e:
            logger.error(f"Error during whale sweep loop: {e}")
        logger.info(f"Cycle complete. Sleeping {interval_seconds}s before next pass...")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CoinGlass Whale Orders Collector")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon loop")
    parser.add_argument("--interval", type=int, default=180, help="Seconds between sweeps in daemon mode")
    parser.add_argument("--symbol", type=str, default=None, help="Target a single symbol")
    args = parser.parse_args()

    if args.daemon:
        asyncio.run(daemon_loop(args.interval))
    elif args.symbol:
        asyncio.run(sweep_all_cryptos([args.symbol.upper()]))
    else:
        asyncio.run(sweep_all_cryptos())
