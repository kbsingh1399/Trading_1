"""
Whale Orders & Large Order Book Blotter Collector
=================================================
Automated ingestion engine for CoinGlass Large Order Book Statistics (Table 2):
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
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

import pandas as pd
from playwright.async_api import async_playwright

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

# Target Live Crypto Assets
CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "SUI", "NEAR", "HYPE"
]

MT5_SYMBOL_MAP = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
    "XRP": "XRPUSD",
    "DOGE": "DOGEUSD",
    "BNB": "BNBUSD",
    "ADA": "ADAUSD",
    "LINK": "LINKUSD",
    "AVAX": "AVAXUSD",
    "SUI": "SUIUSD",
    "NEAR": "NEARUSD",
    "HYPE": "HYPEUSD"
}


def parse_currency(val_str: str) -> float:
    """Convert currency string (e.g. '$1.05M', '$500K', '$2B') to numeric float."""
    if not val_str:
        return 0.0
    s = val_str.replace('$', '').replace(',', '').strip()
    mult = 1.0
    if s.endswith('K') or s.endswith('k'):
        mult = 1e3
        s = s[:-1]
    elif s.endswith('M') or s.endswith('m'):
        mult = 1e6
        s = s[:-1]
    elif s.endswith('B') or s.endswith('b'):
        mult = 1e9
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return 0.0


def parse_duration(dur_str: str) -> int:
    """Convert human duration string (e.g. '17h 59m', '1m 27s', '42s') to total seconds."""
    if not dur_str:
        return 0
    s = dur_str.strip()
    total = 0
    days = re.search(r'(\d+)\s*d', s)
    if days: total += int(days.group(1)) * 86400
    hours = re.search(r'(\d+)\s*h', s)
    if hours: total += int(hours.group(1)) * 3600
    mins = re.search(r'(\d+)\s*m', s)
    if mins: total += int(mins.group(1)) * 60
    secs = re.search(r'(\d+)\s*s', s)
    if secs: total += int(secs.group(1))
    return total


def upsert_whale_open_orders(symbol: str, orders: list):
    """Persist active open whale orders to Parquet."""
    if not orders:
        return
    OPEN_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OPEN_DIR / f"{symbol}_whale_open_orders.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for o in orders:
        o["snapshot_time_utc"] = now_utc
        o["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(orders)
    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["symbol", "venue", "side", "price", "initial_usd", "remaining_usd"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge open orders: {e}")
            combined = new_df
    else:
        combined = new_df

    combined.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {out_path.name}: {len(combined)} active open whale walls")


def upsert_whale_history_orders(symbol: str, orders: list):
    """Persist executed/cancelled whale orders history to Parquet."""
    if not orders:
        return
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    out_path = HIST_DIR / f"{symbol}_whale_history_orders.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for o in orders:
        o["snapshot_time_utc"] = now_utc
        o["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(orders)
    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["symbol", "venue", "side", "price", "initial_usd", "filled_usd", "status"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge history orders: {e}")
            combined = new_df
    else:
        combined = new_df

    combined.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {out_path.name}: {len(combined)} historical whale orders")


def upsert_whale_market_snapshots(snapshots: list):
    """Persist macro whale market snapshots across all assets to Parquet."""
    if not snapshots:
        return
    WHALE_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc).isoformat()

    for s in snapshots:
        s["snapshot_time_utc"] = now_utc
        s["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(snapshots)
    if SNAPSHOT_PATH.exists():
        try:
            existing = pd.read_parquet(SNAPSHOT_PATH)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["snapshot_time_utc", "symbol"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"Failed to merge snapshots: {e}")
            combined = new_df
    else:
        combined = new_df

    combined.to_parquet(SNAPSHOT_PATH, index=False, engine="pyarrow")
    logger.info(f"Whale Market Snapshots saved: {SNAPSHOT_PATH.name} ({len(combined)} records)")


async def harvest_whale_orders_for_symbol(page, symbol: str) -> dict:
    """Harvests Table 2 Open Orders, History Orders, and Macro Snapshot for a single crypto asset."""
    url = f"https://www.coinglass.com/large-orderbook-statistics?symbol={symbol}"
    logger.info(f"[{symbol}] Navigating to {url}...")
    await page.goto(url, timeout=30000)
    await asyncio.sleep(3.0)

    # Extract Macro Indicators from Header
    macro = await page.evaluate('''() => {
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
            lastPrice,
            bidImb,
            askImb,
            openWalls,
            bidWallsTotal,
            askWallsTotal,
            wallStructure,
            nearestAsk,
            nearestBid,
            largestWall
        };
    }''')

    # Scroll down to Table 2
    await page.evaluate("window.scrollTo(0, 1600)")
    await asyncio.sleep(0.5)

    # 1. Harvest 'Open' Orders Tab
    await page.evaluate('''() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const openTab = tabs.find(t => (t.innerText || '').startsWith('Open'));
        if (openTab) openTab.click();
    }''')
    await asyncio.sleep(0.8)

    raw_open = await page.evaluate('''async () => {
        const container = document.querySelector('.lo-bt-body');
        if (!container) return [];
        
        const seen = new Map();
        container.scrollTop = 0;
        await new Promise(r => setTimeout(r, 120));
        
        let currentScroll = 0;
        let lastScroll = -1;
        const maxScroll = container.scrollHeight;
        const step = 850;
        let steps = 0;
        
        while (currentScroll <= maxScroll + step && steps < 70) {
            container.scrollTop = currentScroll;
            await new Promise(r => setTimeout(r, 35));
            
            const trs = Array.from(container.querySelectorAll('table tbody tr'));
            for (const tr of trs) {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                if (tds.length >= 8) {
                    const key = `${tds[0]}_${tds[1]}_${tds[3]}_${tds[7]}`;
                    if (!seen.has(key)) {
                        seen.set(key, tds);
                    }
                }
            }
            if (container.scrollTop === lastScroll && steps > 5) break;
            lastScroll = container.scrollTop;
            currentScroll += step;
            steps++;
        }
        return Array.from(seen.values());
    }''')

    # 2. Harvest 'History' Orders Tab
    await page.evaluate('''() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const histTab = tabs.find(t => (t.innerText || '').startsWith('History'));
        if (histTab) histTab.click();
    }''')
    await asyncio.sleep(0.8)

    raw_history = await page.evaluate('''async () => {
        const container = document.querySelector('.lo-bt-body');
        if (!container) return [];
        
        const seen = new Map();
        container.scrollTop = 0;
        await new Promise(r => setTimeout(r, 120));
        
        let currentScroll = 0;
        let lastScroll = -1;
        const maxScroll = container.scrollHeight;
        const step = 850;
        let steps = 0;
        
        while (currentScroll <= maxScroll + step && steps < 70) {
            container.scrollTop = currentScroll;
            await new Promise(r => setTimeout(r, 35));
            
            const trs = Array.from(container.querySelectorAll('table tbody tr'));
            for (const tr of trs) {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                if (tds.length >= 8) {
                    const key = `${tds[0]}_${tds[1]}_${tds[3]}_${tds[7]}`;
                    if (!seen.has(key)) {
                        seen.set(key, tds);
                    }
                }
            }
            if (container.scrollTop === lastScroll && steps > 5) break;
            lastScroll = container.scrollTop;
            currentScroll += step;
            steps++;
        }
        return Array.from(seen.values());
    }''')

    now_utc = datetime.now(timezone.utc).isoformat()
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
                "symbol": symbol,
                "mt5_symbol": mt5_sym,
                "venue": pure_v,
                "exchange": ex,
                "market_type": m_type,
                "side": side,
                "price": price,
                "from_price_pct": dist_pct,
                "initial_usd": init_usd,
                "remaining_usd": rem_usd,
                "filled_usd": fill_usd,
                "fills_count": fills,
                "duration_str": dur_str,
                "duration_seconds": dur_sec,
                "status": status,
                "tab": tab_name
            })
        return records

    open_orders = parse_rows(raw_open, "open")
    history_orders = parse_rows(raw_history, "history")

    snap = {
        "symbol": symbol,
        "mt5_symbol": mt5_sym,
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

    # Persist
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
    """Orchestrates full sweep across all active crypto assets."""
    target_symbols = symbols or CRYPTO_SYMBOLS
    logger.info(f"Starting Whale Orders Dump across {len(target_symbols)} crypto assets: {target_symbols}")
    
    summary = {}
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        ctx = browser.contexts[0]
        pages = [pg for pg in ctx.pages if "large-orderbook" in pg.url]
        page = pages[0] if pages else await ctx.new_page()

        for sym in target_symbols:
            try:
                res = await harvest_whale_orders_for_symbol(page, sym)
                summary[sym] = res
                logger.info(f"[{sym}] Complete: {res['open_orders_count']} open, {res['history_orders_count']} history")
            except Exception as e:
                logger.error(f"[{sym}] Error during whale harvest: {e}")
                summary[sym] = {"error": str(e)}

    return summary


async def daemon_loop(interval_seconds: int = 180):
    logger.info(f"Starting Whale Orders Collector Daemon (Cycle Interval: {interval_seconds}s)")
    while True:
        try:
            await sweep_all_cryptos()
        except Exception as e:
            logger.error(f"Error during whale sweep cycle: {e}")
        logger.info(f"Whale sweep cycle complete. Sleeping {interval_seconds}s before next pass...")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CoinGlass Whale Orders Collector")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon loop")
    parser.add_argument("--interval", type=int, default=180, help="Seconds between sweeps in daemon mode (default 180s)")
    parser.add_argument("--symbol", type=str, default=None, help="Target a single symbol")
    args = parser.parse_args()

    if args.daemon:
        asyncio.run(daemon_loop(args.interval))
    elif args.symbol:
        asyncio.run(sweep_all_cryptos([args.symbol.upper()]))
    else:
        asyncio.run(sweep_all_cryptos())

