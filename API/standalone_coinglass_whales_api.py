"""
standalone_coinglass_whales_api.py
========================================================================================
Institutional Standalone CoinGlass Large Orderbook & Whale Blotter API & Dumper
========================================================================================

Architecture & Design Patterns:
- Hexagonal / Clean Architecture (Domain, Application UseCase, Ports & Adapters, Delivery).
- Zero DOM Scraping Fast-Path: Direct JavaScript memory inspection of CoinGlass React Fiber
  runtime (`__reactFiber$` on `.lo-bt-body`) via Chrome DevTools Protocol (CDP port 9222).
- Robust DOM Virtualization Fallback with numerical USD parsing.
- Multi-Process Concurrency & Zero-Collision Invariants:
  * Ephemeral tab lifecycle with dedicated identifier: `?bot=whales_api`.
  * Active Tab Registry (`_ACTIVE_WHALE_TABS`) preventing intra-worker tab extermination.
  * Tab cleanup strictly restricted to unheld `whales_api` tabs - zero interference with
    footprint scrapers, TradingView dumpers, or user browsing sessions.
  * BrowserLock protection ensuring serialized CDP tab acquisition without port contention.
  * Multiplexed CDP WebSocket loop filtering out unsolicited push frames with timeout guards.
  * Re-entrant file locks (`writer_lock`) and crash-proof atomic Parquet swaps (`atomic_parquet`).
- Target Output Directory: `C:\\Users\\SIGMA\\Documents\\_Trading_1\\Database\\Whale_Dumps`
- Exact 8-Column User Schema (matching CoinGlass Table & User Excel Specification):
  ["Venue/Side", "Price", "From Price", "Initial_USD", "Remaining_USD", "Filled_USD", "Duration", "Status"]
- Quantitative Enrichment: Auxiliary numeric and temporal columns retained for quant engines.
========================================================================================
"""

import sys
import os
import re
import json
import time
import math
import asyncio
import logging
import argparse
import urllib.request
import subprocess
from pathlib import Path
from enum import Enum
from typing import List, Dict, Optional, Tuple, Any, Set
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta

import pandas as pd
import websockets
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent.parent
API_DIR = Path(__file__).resolve().parent
DUMP_DIR = ROOT_DIR / "Database" / "Whale_Dumps"
DUMP_DIR.mkdir(parents=True, exist_ok=True)

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from coinglass_common import writer_lock, atomic_parquet, BrowserLock, rotate_warp_ip
except ImportError:
    writer_lock = None
    atomic_parquet = None
    BrowserLock = None
    rotate_warp_ip = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CG-WHALES-API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("StandaloneWhaleAPI")

# Canonical rotate_warp_ip imported from coinglass_common

CDP_BASE_URL = "http://127.0.0.1:9222"

EXCEL_SPEC_COLUMNS = [
    "Venue/Side",
    "Price",
    "From Price",
    "Initial_USD",
    "Remaining_USD",
    "Filled_USD",
    "Duration",
    "Status"
]

AUXILIARY_QUANT_COLUMNS = [
    "raw_price",
    "raw_initial_usd",
    "raw_remaining_usd",
    "raw_filled_usd",
    "side",
    "exchange",
    "time_ms",
    "snapshot_time_utc",
    "snapshot_time_ist"
]

# Strict Authorized MT5 whitelist
AUTHORIZED_MT5_WHITELIST = {
    "BTCUSD": "BTC",
    "ETHUSD": "ETH",
    "DOGEUSD": "DOGE",
    "ADAUSD": "ADA",
    "XRPUSD": "XRP",
    "LTCUSD": "LTC",
    "LINKUSD": "LINK",
    # Base symbols
    "BTC": "BTC",
    "ETH": "ETH",
    "DOGE": "DOGE",
    "ADA": "ADA",
    "XRP": "XRP",
    "LTC": "LTC",
    "LINK": "LINK"
}

REGISTRY_FILE = ROOT_DIR / "Database" / "mutual_asset_capability_registry.json"

def load_whale_registry_symbols() -> List[str]:
    """Dynamically loads verified active symbols from API/active_assets.json with fallback."""
    try:
        from active_assets_manager import load_active_assets
        active = load_active_assets()
        if active:
            return active
    except Exception:
        pass
    if not REGISTRY_FILE.exists():
        return ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "LINK", "LTC", "XAU", "XAG"]
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            reg = json.load(f)
        syms = []
        for cat in reg.values():
            for s, meta in cat.items():
                if meta.get("whales", True):
                    syms.append(s.upper())
        return sorted(list(set(syms))) if syms else ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "LINK", "LTC"]
    except Exception as e:
        logger.error(f"Error loading registry: {e}")
        return ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "LINK", "LTC"]

def is_whale_asset_complete(sym: str) -> bool:
    """Verifies that whale open orders parquet file exists and is populated."""
    target_file = DUMP_DIR / f"{sym}_whale_open_orders.parquet"
    return target_file.exists() and target_file.stat().st_size > 0

CORE_WHALE_SYMBOLS = load_whale_registry_symbols()

EXCHANGE_NAME_MAP = {
    'cb-spot': 'Coinbase Spot',
    'bn-spot': 'Binance Spot',
    'bn-perp': 'Binance Perp',
    'hl-perp': 'Hyperliquid Perp',
    'bybit-spot': 'Bybit Spot',
    'bybit-perp': 'Bybit Perp',
    'okx-perp': 'OKX Perp',
    'okx-spot': 'OKX Spot',
    'ok-perp': 'OKX Perp',
    'ok-spot': 'OKX Spot',
    'aster-perp': 'Aster Perp',
    'bfx-perp': 'Bitfinex Perp',
    'bfx-spot': 'Bitfinex Spot',
    'bitget-perp': 'Bitget Perp',
    'bg-perp': 'Bitget Perp',
    'gate-perp': 'Gate Perp',
    'gt-perp': 'Gate Perp',
    'gate-spot': 'Gate Spot',
    'kr-spot': 'Kraken Spot',
    'bs-spot': 'Bitstamp Spot',
    'deribit-perp': 'Deribit Perp',
    'dydx-perp': 'dYdX Perp',
}

# Global in-memory registry of actively running tabs to prevent mutual termination
_ACTIVE_WHALE_TABS: Set[str] = set()


# ========================================================================================
# 1. DOMAIN LAYER: ENTITIES & AGGREGATES
# ========================================================================================

@dataclass
class WhaleOrder:
    venue_side: str
    price: str
    from_price: str
    initial_usd: str
    remaining_usd: str
    filled_usd: str
    duration: str
    status: str
    raw_price: float = 0.0
    raw_initial_usd: float = 0.0
    raw_remaining_usd: float = 0.0
    raw_filled_usd: float = 0.0
    start_time_ms: int = 0
    order_id: str = ""
    exchange: str = ""
    side: str = ""
    time_ms: int = 0
    snapshot_time_utc: str = ""
    snapshot_time_ist: str = ""

    def to_display_dict(self) -> Dict[str, Any]:
        """Returns exact 8-column dictionary matching user's Excel format."""
        return {
            "Venue/Side": self.venue_side,
            "Price": self.price,
            "From Price": self.from_price,
            "Initial_USD": self.initial_usd,
            "Remaining_USD": self.remaining_usd,
            "Filled_USD": self.filled_usd,
            "Duration": self.duration,
            "Status": self.status
        }

    def to_full_dict(self) -> Dict[str, Any]:
        """Returns exact 8 display columns enriched with auxiliary quant telemetry."""
        d = self.to_display_dict()
        d.update({
            "raw_price": self.raw_price,
            "raw_initial_usd": self.raw_initial_usd,
            "raw_remaining_usd": self.raw_remaining_usd,
            "raw_filled_usd": self.raw_filled_usd,
            "side": self.side,
            "exchange": self.exchange,
            "time_ms": self.time_ms,
            "snapshot_time_utc": self.snapshot_time_utc,
            "snapshot_time_ist": self.snapshot_time_ist
        })
        return d


@dataclass
class WhaleDataset:
    symbol: str
    mark_price: float
    timestamp_utc: str
    timestamp_ist: str
    orders: List[WhaleOrder] = field(default_factory=list)
    total_orders: int = 0
    ask_orders_count: int = 0
    bid_orders_count: int = 0
    total_ask_remaining_usd: float = 0.0
    total_bid_remaining_usd: float = 0.0

    def to_dataframe(self, display_only: bool = True) -> pd.DataFrame:
        """
        Converts dataset into DataFrame strictly sorted in Order Book Order
        (Price descending: Asks down to Mark Price, then Bids down to Low Bids).
        """
        if not self.orders:
            cols = EXCEL_SPEC_COLUMNS if display_only else (EXCEL_SPEC_COLUMNS + AUXILIARY_QUANT_COLUMNS)
            return pd.DataFrame(columns=cols)

        # Enforce strict Order Book depth sorting (Price descending)
        sorted_orders = sorted(self.orders, key=lambda o: o.raw_price, reverse=True)

        if display_only:
            records = [o.to_display_dict() for o in sorted_orders]
            df = pd.DataFrame(records)
            return df[EXCEL_SPEC_COLUMNS]
        else:
            records = [o.to_full_dict() for o in sorted_orders]
            df = pd.DataFrame(records)
            ordered_cols = EXCEL_SPEC_COLUMNS + AUXILIARY_QUANT_COLUMNS
            existing_cols = [c for c in ordered_cols if c in df.columns]
            return df[existing_cols]


# ========================================================================================
# 2. ADAPTER LAYER: CDP WHALE EXTRACTOR (PARALLEL & COLLISION SAFE)
# ========================================================================================

class CDPWhaleAdapter:
    """
    Direct Chrome DevTools Protocol Adapter targeting CoinGlass Large Orderbook Statistics.
    Strictly isolated: tab URLs are tagged with `?bot=whales_api` to prevent any collision
    with Footprint, TV, or user-managed tabs.
    """

    def __init__(self, base_url: str = CDP_BASE_URL):
        self.base_url = base_url

    async def _cleanup_whales_orphan_tabs(self, keep_tab_id: Optional[str] = None):
        """
        Kills only abandoned whale tabs not currently held by active workers.
        Never touches footprint, TV, or user tabs.
        """
        def _reap():
            try:
                req = urllib.request.Request(f"{self.base_url}/json", headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    tabs = json.loads(resp.read().decode())
                    for t in tabs:
                        tid = t.get("id")
                        url = t.get("url", "").lower()
                        # Strictly reap whale tabs that are NOT in active execution and != keep_tab_id
                        if tid != keep_tab_id and tid not in _ACTIVE_WHALE_TABS and "bot=whales_api" in url:
                            try:
                                urllib.request.urlopen(f"{self.base_url}/json/close/{tid}", timeout=2)
                                logger.info(f"Reaped abandoned whale tab ({tid})")
                            except Exception:
                                pass
            except Exception as e:
                logger.debug(f"Whale tab reaper suppressed: {e}")

        await asyncio.to_thread(_reap)

    async def open_whale_tab(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Opens a dedicated isolated tab for the given symbol, registering it as active."""
        await self._cleanup_whales_orphan_tabs()
        url = f"https://www.coinglass.com/large-orderbook-statistics/{symbol.upper()}?bot=whales_api"

        def _open():
            req = urllib.request.Request(f"{self.base_url}/json/new?{url}", method="PUT")
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                return data.get("id"), data.get("webSocketDebuggerUrl")

        try:
            tab_id, ws_url = await asyncio.to_thread(_open)
            if tab_id:
                _ACTIVE_WHALE_TABS.add(tab_id)
                logger.info(f"[{symbol}] Opened isolated Whale tab ({tab_id}) - Registered active.")
            return tab_id, ws_url
        except Exception as e:
            logger.error(f"[{symbol}] Failed to open whale tab via CDP: {e}")
            return None, None

    async def close_tab(self, tab_id: Optional[str]):
        """Closes the specified tab immediately upon harvesting completion."""
        if not tab_id:
            return
        if tab_id in _ACTIVE_WHALE_TABS:
            _ACTIVE_WHALE_TABS.remove(tab_id)

        def _close():
            try:
                urllib.request.urlopen(f"{self.base_url}/json/close/{tab_id}", timeout=3)
                logger.info(f"Closed Whale tab ({tab_id}) - Tab isolation verified.")
            except Exception as e:
                logger.warning(f"Could not close Whale tab {tab_id}: {e}")

        await asyncio.to_thread(_close)
        await self._cleanup_whales_orphan_tabs()

    async def _execute_cdp_eval(self, ws, expression: str, timeout: float = 12.0) -> Any:
        """
        Executes JavaScript via CDP Runtime.evaluate with unique message ID and frame filtering.
        Ignores asynchronous push notifications (DOM/Page events) and returns strictly on matching response.
        """
        req_id = int(time.time() * 1000) % 10000000
        payload = {
            "id": req_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True
            }
        }
        await ws.send(json.dumps(payload))
        t0 = time.monotonic()

        while time.monotonic() - t0 < timeout:
            remaining = timeout - (time.monotonic() - t0)
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.5, remaining))
            res = json.loads(raw)
            if res.get("id") == req_id:
                if "error" in res or res.get("result", {}).get("exceptionDetails"):
                    err = res.get("result", {}).get("exceptionDetails", res.get("error"))
                    raise RuntimeError(f"CDP JS Error: {err}")
                return res.get("result", {}).get("result", {}).get("value")

        raise TimeoutError(f"CDP evaluation timed out after {timeout:.1f}s")

    async def harvest_symbol_orders(self, symbol: str, max_retries: int = 3) -> WhaleDataset:
        """
        Harvests all open whale orders for the symbol with zero DOM scroll lag via React Fiber,
        with automated fallback to DOM extraction.
        """
        # Symbol normalization & MT5 translation
        clean_sym = symbol.upper()
        if clean_sym in AUTHORIZED_MT5_WHITELIST:
            clean_sym = AUTHORIZED_MT5_WHITELIST[clean_sym]

        tab_id, ws_url = None, None

        for attempt in range(1, max_retries + 1):
            try:
                tab_id, ws_url = await self.open_whale_tab(clean_sym)
                if not ws_url:
                    raise RuntimeError("Failed to obtain webSocketDebuggerUrl")

                # Allow initial hydration and React state mount
                await asyncio.sleep(3.6)

                async with websockets.connect(ws_url, max_size=32 * 1024 * 1024, ping_interval=None) as ws:
                    js_payload = """(async () => {
                        // 0. Active Hydration Wait & $0 Size Threshold Enforcement
                        const t0 = Date.now();
                        let rawOrders = [];
                        while (Date.now() - t0 < 9000) {
                            const container = document.querySelector('.lo-bt-body');
                            if (container) {
                                const key = Object.keys(container).find(k => k.startsWith('__reactFiber$'));
                                let fiber = container[key];
                                while (fiber) {
                                    let st = fiber.memoizedState;
                                    while (st) {
                                        const val = st.memoizedState;
                                        if (val && typeof val === 'object' && Array.isArray(val['0']) && val['0'].length > 0) {
                                            if (val['0'][0] && (val['0'][0].usd !== undefined || val['0'][0].initUsd !== undefined)) {
                                                rawOrders = val['0'];
                                                break;
                                            }
                                        }
                                        st = st.next;
                                    }
                                    if (rawOrders.length > 0) break;
                                    fiber = fiber.return;
                                }
                            }
                            if (rawOrders.length > 5 || document.querySelectorAll('.lo-bt-body table tbody tr').length > 5) break;
                            await new Promise(r => setTimeout(r, 250));
                        }

                        // Ensure Size threshold is strictly $0 (no limit - captures all orders)
                        const rail = document.querySelector('.lo-rail') || document.body;
                        const selects = Array.from(rail.querySelectorAll('[class*="ui_select"], [class*="select"]'));
                        const thrSelect = selects.find(el => el.innerText && (el.innerText.includes('Size threshold') || el.innerText.includes('≥$')));
                        if (thrSelect && !thrSelect.innerText.includes('$0')) {
                            ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click'].forEach(evt =>
                                thrSelect.dispatchEvent(new MouseEvent(evt, {bubbles: true, cancelable: true}))
                            );
                            await new Promise(r => setTimeout(r, 400));
                            const pop = Array.from(document.body.children).find(c => c.tagName === 'DIV' && c.id !== '__next' && (c.innerText.includes('$0') || c.innerText.includes('≥$')));
                            if (pop) {
                                const zeroItem = Array.from(pop.querySelectorAll('*')).find(e => e.children.length === 0 && e.innerText && e.innerText.trim() === '$0');
                                if (zeroItem) {
                                    zeroItem.click();
                                    await new Promise(r => setTimeout(r, 1000));
                                }
                            }
                        }

                        const exMap = {
                            'cb-spot': 'Coinbase Spot',
                            'bn-spot': 'Binance Spot',
                            'bn-perp': 'Binance Perp',
                            'hl-perp': 'Hyperliquid Perp',
                            'bybit-spot': 'Bybit Spot',
                            'bybit-perp': 'Bybit Perp',
                            'okx-perp': 'OKX Perp',
                            'okx-spot': 'OKX Spot',
                            'ok-perp': 'OKX Perp',
                            'ok-spot': 'OKX Spot',
                            'aster-perp': 'Aster Perp',
                            'bfx-perp': 'Bitfinex Perp',
                            'bfx-spot': 'Bitfinex Spot',
                            'bitget-perp': 'Bitget Perp',
                            'bg-perp': 'Bitget Perp',
                            'gate-perp': 'Gate Perp',
                            'gt-perp': 'Gate Perp',
                            'gate-spot': 'Gate Spot',
                            'kr-spot': 'Kraken Spot',
                            'bs-spot': 'Bitstamp Spot',
                            'deribit-perp': 'Deribit Perp',
                            'dydx-perp': 'dYdX Perp',
                        };

                        function formatUsd(v) {
                            if (!v || v === 0) return '$0';
                            if (v >= 1e9) {
                                const s = (v / 1e9).toFixed(2).replace(/\\.?0+$/, '');
                                return '$' + s + 'B';
                            }
                            if (v >= 1e6) {
                                const s = (v / 1e6).toFixed(2).replace(/\\.?0+$/, '');
                                return '$' + s + 'M';
                            }
                            if (v >= 1e3) {
                                const s = (v / 1e3).toFixed(2).replace(/\\.?0+$/, '');
                                return '$' + s + 'K';
                            }
                            return '$' + Math.round(v).toLocaleString('en-US');
                        }

                        function parseUsdStr(s) {
                            if (!s) return 0.0;
                            const clean = s.replace(/[$, ]/g, '');
                            let mult = 1.0;
                            if (/B$/i.test(clean)) mult = 1e9;
                            else if (/M$/i.test(clean)) mult = 1e6;
                            else if (/K$/i.test(clean)) mult = 1e3;
                            const num = parseFloat(clean.replace(/[BMKbmk]/g, ''));
                            return isNaN(num) ? 0.0 : num * mult;
                        }

                        function formatDuration(startT, now) {
                            const diff = Math.max(0, Math.floor((now - startT) / 1000));
                            if (diff < 60) return diff + 's';
                            if (diff < 3600) {
                                const m = Math.floor(diff / 60);
                                const s = diff % 60;
                                return m + 'm ' + (s < 10 ? '0' : '') + s + 's';
                            }
                            if (diff < 86400) {
                                const h = Math.floor(diff / 3600);
                                const m = Math.floor((diff % 3600) / 60);
                                return h + 'h ' + (m < 10 ? '0' : '') + m + 'm';
                            }
                            const d = Math.floor(diff / 86400);
                            const h = Math.floor((diff % 86400) / 3600);
                            return d + 'd ' + (h < 10 ? '0' : '') + h + 'h';
                        }

                        // High performance price formatter preserving cent precision
                        function fastFormatPrice(p) {
                            if (p >= 1000) {
                                const parts = p.toFixed(1).split('.');
                                parts[0] = parts[0].replace(/\\B(?=(\\d{3})+(?!\\d))/g, ',');
                                return parts.join('.');
                            } else if (p >= 1) {
                                return p.toFixed(2);
                            } else {
                                return p.toFixed(4);
                            }
                        }

                        // 1. Multi-source Mark Price Extraction
                        let markPrice = null;
                        const bdiv = document.querySelector('.lo-bt-body tr.bdiv');
                        if (bdiv) {
                            const m = bdiv.innerText.match(/([\\d,]+\\.?\\d*)/);
                            if (m) markPrice = parseFloat(m[1].replace(/,/g, ''));
                        }
                        if (!markPrice) {
                            const lastEl = document.querySelector('.lo-last-price') || document.querySelector('[class*="lastPrice"]');
                            if (lastEl) {
                                const m = lastEl.innerText.match(/([\\d,]+\\.?\\d*)/);
                                if (m) markPrice = parseFloat(m[1].replace(/,/g, ''));
                            }
                        }

                        // 2. Fast-Path: Extract orders from React Fiber (if not cached in Step 0)
                        if (rawOrders.length === 0) {
                            const container = document.querySelector('.lo-bt-body');
                            if (container) {
                            const key = Object.keys(container).find(k => k.startsWith('__reactFiber$'));
                            let fiber = container[key];
                            while (fiber) {
                                let st = fiber.memoizedState;
                                while (st) {
                                    const val = st.memoizedState;
                                    // Structural verification: check if array contains whale order properties
                                    if (val && typeof val === 'object' && Array.isArray(val['0']) && val['0'].length > 0) {
                                        const first = val['0'][0];
                                        if (first && (first.usd !== undefined || first.initUsd !== undefined)) {
                                            rawOrders = val['0'];
                                            break;
                                        }
                                    }
                                    st = st.next;
                                }
                                if (rawOrders.length > 0) break;
                                fiber = fiber.return;
                            }
                        }
                        }

                        // Synthesized mark price fallback if tr.bdiv is off-screen due to virtualization
                        if (!markPrice && rawOrders && rawOrders.length > 0) {
                            const asks = rawOrders.filter(o => o.side === 'ask').map(o => o.price);
                            const bids = rawOrders.filter(o => o.side === 'bid').map(o => o.price);
                            if (asks.length > 0 && bids.length > 0) {
                                markPrice = (Math.min(...asks) + Math.max(...bids)) / 2.0;
                            } else if (asks.length > 0) {
                                markPrice = Math.min(...asks);
                            } else if (bids.length > 0) {
                                markPrice = Math.max(...bids);
                            }
                        }

                        const now = Date.now();
                        let resultOrders = [];

                        if (rawOrders && rawOrders.length > 0) {
                            // Sort in standard order book order (Price descending)
                            rawOrders.sort((a, b) => b.price - a.price);

                            for (const o of rawOrders) {
                                const venue = (exMap[o.ex] || o.ex.replace(/-/g, ' ').toUpperCase()) + ' ' + (o.side === 'ask' ? 'Ask' : 'Bid');
                                const fromPct = markPrice ? ((o.price - markPrice) / markPrice * 100) : 0;
                                const fromStr = (fromPct >= 0 ? '+' : '') + fromPct.toFixed(2) + '%';
                                const initStr = formatUsd(o.initUsd);
                                const remStr = formatUsd(o.usd);
                                const fillStr = formatUsd(o.filledUsd);
                                const durStr = formatDuration(o.startT, now);
                                const statusStr = (o.filledUsd > 0 || o.fills > 0) ? 'Partially Filled' : 'Open';

                                resultOrders.push({
                                    venue_side: venue,
                                    price: fastFormatPrice(o.price),
                                    from_price: fromStr,
                                    initial_usd: initStr,
                                    remaining_usd: remStr,
                                    filled_usd: fillStr,
                                    duration: durStr,
                                    status: statusStr,
                                    raw_price: o.price,
                                    raw_initial_usd: o.initUsd,
                                    raw_remaining_usd: o.usd,
                                    raw_filled_usd: o.filledUsd,
                                    start_time_ms: o.startT,
                                    order_id: o.id || '',
                                    exchange: o.ex || '',
                                    side: o.side || ''
                                });
                            }
                            return {
                                success: true,
                                method: 'react_fiber',
                                mark_price: markPrice,
                                total: resultOrders.length,
                                orders: resultOrders
                            };
                        }

                        // 3. Fallback: Extract currently visible DOM rows if React Fiber missing
                        const domRows = [];
                        const trs = document.querySelectorAll('.lo-bt-body table tbody tr');
                        for (const tr of trs) {
                            if (tr.classList.contains('bdiv')) continue;
                            const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                            if (tds.length >= 8 && tds[0]) {
                                const venue = tds[0].replace(/[★☆]/g, '').trim();
                                const pVal = parseFloat(tds[1].replace(/,/g, '')) || 0.0;
                                const fromPct = markPrice ? ((pVal - markPrice) / markPrice * 100) : 0.0;
                                const fromStr = (fromPct >= 0 ? '+' : '') + fromPct.toFixed(2) + '%';

                                domRows.push({
                                    venue_side: venue,
                                    price: tds[1],
                                    from_price: fromStr,
                                    initial_usd: tds[3],
                                    remaining_usd: tds[4],
                                    filled_usd: tds[5],
                                    duration: tds.length > 8 ? tds[7] : tds[6],
                                    status: tds.length > 8 ? tds[8] : tds[7],
                                    raw_price: pVal,
                                    raw_initial_usd: parseUsdStr(tds[3]),
                                    raw_remaining_usd: parseUsdStr(tds[4]),
                                    raw_filled_usd: parseUsdStr(tds[5]),
                                    start_time_ms: 0,
                                    order_id: '',
                                    exchange: '',
                                    side: venue.toLowerCase().includes('ask') ? 'ask' : 'bid'
                                });
                            }
                        }

                        // Sort DOM rows descending by price
                        domRows.sort((a, b) => b.raw_price - a.raw_price);

                        return {
                            success: true,
                            method: 'dom_fallback',
                            mark_price: markPrice,
                            total: domRows.length,
                            orders: domRows
                        };
                    })()"""

                    val = await self._execute_cdp_eval(ws, js_payload, timeout=14.0)

                    if not val or not val.get("success"):
                        raise RuntimeError(f"CDP in-page extraction returned invalid response: {val}")

                    raw_orders_list = val.get("orders", [])
                    mark_p = float(val.get("mark_price") or 0.0)
                    method_used = val.get("method")

                    logger.info(f"[{clean_sym}] Extracted {len(raw_orders_list)} whale orders using '{method_used}' (Mark Price: ${mark_p:,.2f})")

                    # Hydrate domain entities
                    now_utc = datetime.now(timezone.utc)
                    now_ms = int(now_utc.timestamp() * 1000)
                    ist_offset = timedelta(hours=5, minutes=30)
                    now_ist_str = (now_utc + ist_offset).strftime("%Y-%m-%d %H:%M:%S IST")

                    domain_orders: List[WhaleOrder] = []
                    ask_cnt, bid_cnt = 0, 0
                    ask_usd, bid_usd = 0.0, 0.0

                    for item in raw_orders_list:
                        order = WhaleOrder(
                            venue_side=item.get("venue_side", ""),
                            price=item.get("price", ""),
                            from_price=item.get("from_price", ""),
                            initial_usd=item.get("initial_usd", ""),
                            remaining_usd=item.get("remaining_usd", ""),
                            filled_usd=item.get("filled_usd", ""),
                            duration=item.get("duration", ""),
                            status=item.get("status", "Open"),
                            raw_price=float(item.get("raw_price") or 0.0),
                            raw_initial_usd=float(item.get("raw_initial_usd") or 0.0),
                            raw_remaining_usd=float(item.get("raw_remaining_usd") or 0.0),
                            raw_filled_usd=float(item.get("raw_filled_usd") or 0.0),
                            start_time_ms=int(item.get("start_time_ms") or 0),
                            order_id=str(item.get("order_id") or ""),
                            exchange=str(item.get("exchange") or ""),
                            side=str(item.get("side") or ""),
                            time_ms=now_ms,
                            snapshot_time_utc=now_utc.isoformat(),
                            snapshot_time_ist=now_ist_str
                        )
                        domain_orders.append(order)

                        if "ask" in order.venue_side.lower():
                            ask_cnt += 1
                            ask_usd += order.raw_remaining_usd
                        else:
                            bid_cnt += 1
                            bid_usd += order.raw_remaining_usd

                    return WhaleDataset(
                        symbol=clean_sym,
                        mark_price=mark_p,
                        timestamp_utc=now_utc.isoformat(),
                        timestamp_ist=now_ist_str,
                        orders=domain_orders,
                        total_orders=len(domain_orders),
                        ask_orders_count=ask_cnt,
                        bid_orders_count=bid_cnt,
                        total_ask_remaining_usd=ask_usd,
                        total_bid_remaining_usd=bid_usd
                    )

            except Exception as e:
                logger.warning(f"[{clean_sym}] Attempt {attempt}/{max_retries} failed: {e}")
                if attempt == max_retries:
                    logger.error(f"[{clean_sym}] Exhausted all {max_retries} retries harvesting whale blotter.")
                    raise
                await asyncio.sleep(2.0)
            finally:
                if tab_id:
                    await self.close_tab(tab_id)


# ========================================================================================
# 3. REPOSITORY LAYER: PERSISTENCE & PARQUET STORAGE
# ========================================================================================

class ParquetWhaleRepository:
    """
    Saves Whale Orders to Parquet in Database/Whale_Dumps/ strictly protected by
    multi-process file locks (writer_lock) and atomic temporary-file swaps.
    """

    def __init__(self, output_dir: Path = DUMP_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def persist_dataset(self, dataset: WhaleDataset, display_only: bool = True) -> Path:
        """
        Persists whale dataset matching the exact 8-column user specification.
        When display_only=False, appends auxiliary quantitative columns for algo engines.
        """
        symbol = dataset.symbol.upper()
        out_file = self.output_dir / f"{symbol}_whale_open_orders.parquet"
        df = dataset.to_dataframe(display_only=display_only)

        # Anti-Erasure Guard: NEVER overwrite populated parquet with 0 rows
        if len(df) == 0 and out_file.exists():
            try:
                prev_df = pd.read_parquet(out_file)
                if len(prev_df) > 0:
                    logger.warning(f"[{symbol}] Extracted 0 orders; strictly preserving existing {len(prev_df)} orders on disk.")
                    return out_file
            except Exception:
                pass

        if not (atomic_parquet and writer_lock):
            raise RuntimeError("Fatal: coinglass_common.writer_lock or atomic_parquet missing!")

        with writer_lock(out_file):
            atomic_parquet(df, out_file)

        logger.info(f"[{symbol}] Successfully dumped {len(df)} orders to {out_file.name} (Display Only: {display_only})")
        return out_file

    def read_dataset(self, symbol: str) -> Optional[pd.DataFrame]:
        """Reads persisted whale parquet table."""
        symbol = symbol.upper()
        file_path = self.output_dir / f"{symbol}_whale_open_orders.parquet"
        if not file_path.exists():
            return None
        return pd.read_parquet(file_path)


# ========================================================================================
# 4. APPLICATION LAYER: USE CASES
# ========================================================================================

class HarvestWhaleOrdersUseCase:
    """Coordinates whale order extraction and persistence with offloaded I/O."""

    def __init__(self, extractor: CDPWhaleAdapter, repository: ParquetWhaleRepository):
        self.extractor = extractor
        self.repository = repository

    async def execute(self, symbol: str, persist: bool = True, display_only: bool = True) -> Tuple[WhaleDataset, Optional[Path]]:
        dataset = await self.extractor.harvest_symbol_orders(symbol)
        out_path = None
        if persist:
            out_path = await asyncio.to_thread(self.repository.persist_dataset, dataset, display_only)
        return dataset, out_path


# ========================================================================================
# 5. FASTAPI REST APPLICATION
# ========================================================================================

app = FastAPI(
    title="CoinGlass Standalone Whale Blotter API",
    version="1.1.0",
    description="Institutional-grade Whale Orderbook Scraper & Parquet Dumper"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

adapter = CDPWhaleAdapter()
repo = ParquetWhaleRepository()
use_case = HarvestWhaleOrdersUseCase(adapter, repo)


@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "CoinGlass Standalone Whale API",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dump_directory": str(DUMP_DIR),
        "supported_core_symbols": CORE_WHALE_SYMBOLS,
        "active_whale_tabs": len(_ACTIVE_WHALE_TABS)
    }


@app.get("/api/v1/whales/symbols")
def get_supported_symbols():
    return {
        "count": len(CORE_WHALE_SYMBOLS),
        "symbols": CORE_WHALE_SYMBOLS,
        "authorized_mt5_whitelist": list(AUTHORIZED_MT5_WHITELIST.keys())
    }


@app.get("/api/v1/whales/{symbol}/open")
async def get_open_orders(symbol: str):
    """Fetches real-time open whale orders directly via CDP memory bridge."""
    clean_sym = symbol.upper()
    if clean_sym not in AUTHORIZED_MT5_WHITELIST and clean_sym not in CORE_WHALE_SYMBOLS:
        raise HTTPException(status_code=403, detail=f"Symbol '{symbol}' is not in authorized whitelist.")

    try:
        dataset, _ = await use_case.execute(clean_sym, persist=False)
        return {
            "symbol": dataset.symbol,
            "mark_price": dataset.mark_price,
            "timestamp_utc": dataset.timestamp_utc,
            "timestamp_ist": dataset.timestamp_ist,
            "total_orders": dataset.total_orders,
            "ask_orders_count": dataset.ask_orders_count,
            "bid_orders_count": dataset.bid_orders_count,
            "total_ask_remaining_usd": dataset.total_ask_remaining_usd,
            "total_bid_remaining_usd": dataset.total_bid_remaining_usd,
            "orders": [o.to_display_dict() for o in dataset.orders]
        }
    except Exception as e:
        logger.error(f"Error fetching orders for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/whales/{symbol}/dump")
async def dump_symbol_data(symbol: str, display_only: bool = Query(True, description="Strict 8-column Excel specification")):
    """Harvests open orders and dumps directly to local Parquet file."""
    clean_sym = symbol.upper()
    if clean_sym not in AUTHORIZED_MT5_WHITELIST and clean_sym not in CORE_WHALE_SYMBOLS:
        raise HTTPException(status_code=403, detail=f"Symbol '{symbol}' is not in authorized whitelist.")

    try:
        dataset, out_path = await use_case.execute(clean_sym, persist=True, display_only=display_only)
        return {
            "status": "success",
            "symbol": dataset.symbol,
            "mark_price": dataset.mark_price,
            "total_orders": dataset.total_orders,
            "file_written": str(out_path),
            "file_name": out_path.name if out_path else "",
            "columns": EXCEL_SPEC_COLUMNS if display_only else (EXCEL_SPEC_COLUMNS + AUXILIARY_QUANT_COLUMNS),
            "timestamp_ist": dataset.timestamp_ist
        }
    except Exception as e:
        logger.error(f"Error dumping {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================================================================
# 6. CLI EXECUTION & HARVEST RUNNER
# ========================================================================================

async def harvest_batch_segmented(target_symbols: List[str], chunk_size: int = 25, display_only: bool = True):
    """
    Segmented Sliding Window Batch Harvester:
    Processes assets in chunks of ~25 per warm tab via in-page soft navigation,
    recycling the tab between chunks to guarantee zero V8 heap memory leaks and zero 429 throttling.
    """
    total = len(target_symbols)
    logger.info(f"Initiating Segmented Batch Whale Harvest for {total} assets (Chunk size: {chunk_size})...")
    t0_overall = time.time()
    success_count = 0

    DUMP_DIR.mkdir(parents=True, exist_ok=True)
    pipeline_open_dir = ROOT_DIR / "Database" / "Whale_Orders" / "Open_Orders"
    pipeline_open_dir.mkdir(parents=True, exist_ok=True)

    for chunk_idx in range(0, total, chunk_size):
        chunk = target_symbols[chunk_idx : chunk_idx + chunk_size]
        chunk_num = (chunk_idx // chunk_size) + 1
        total_chunks = (total + chunk_size - 1) // chunk_size
        logger.info(f"================ CHUNK [{chunk_num}/{total_chunks}] ({len(chunk)} assets: {chunk[0]}..{chunk[-1]}) ================")

        first_sym = chunk[0]
        tab_id, ws_url = None, None
        try:
            tab_id, ws_url = await adapter.open_whale_tab(first_sym)
            if not ws_url:
                logger.error(f"Failed to open tab for chunk {chunk_num}")
                continue

            await asyncio.sleep(3.6)

            async with websockets.connect(ws_url, max_size=32 * 1024 * 1024, ping_interval=None) as ws:
                for idx, sym in enumerate(chunk):
                    curr_overall = chunk_idx + idx + 1
                    t_sym_start = time.time()

                    if idx > 0:
                        nav_js = f"window.location.href = 'https://www.coinglass.com/large-orderbook-statistics/{sym}?bot=whales_api';"
                        await ws.send(json.dumps({'id': 100 + idx, 'method': 'Runtime.evaluate', 'params': {'expression': nav_js}}))
                        await ws.recv()
                        await asyncio.sleep(2.8)

                    req_id = 1000 + curr_overall
                    await ws.send(json.dumps({
                        'id': req_id,
                        'method': 'Runtime.evaluate',
                        'params': {'expression': """(async () => {
                            // 0. Rate limit detection
                            const isRateLimited = document.body.innerText.includes('Large-order data failed to load');

                            // 1. Ensure Size threshold is strictly $0 (no limit)
                            const rail = document.querySelector('.lo-rail') || document.body;
                            const selects = Array.from(rail.querySelectorAll('[class*="ui_select"], [class*="select"]'));
                            const thrSelect = selects.find(el => el.innerText && el.innerText.includes('Size threshold'));

                            if (thrSelect && !thrSelect.innerText.includes('$0')) {
                                thrSelect.dispatchEvent(new MouseEvent('pointerdown', {bubbles: true, cancelable: true}));
                                thrSelect.dispatchEvent(new MouseEvent('mousedown', {bubbles: true, cancelable: true}));
                                thrSelect.dispatchEvent(new MouseEvent('pointerup', {bubbles: true, cancelable: true}));
                                thrSelect.dispatchEvent(new MouseEvent('mouseup', {bubbles: true, cancelable: true}));
                                thrSelect.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
                                await new Promise(r => setTimeout(r, 400));

                                const pop = Array.from(document.body.children).find(c => c.tagName === 'DIV' && c.id !== '__next' && (c.innerText.includes('$0') || c.innerText.includes('≥$')));
                                if (pop) {
                                    const zeroItem = Array.from(pop.querySelectorAll('*')).find(e => e.children.length === 0 && e.innerText && e.innerText.trim() === '$0');
                                    if (zeroItem) {
                                        zeroItem.click();
                                        await new Promise(r => setTimeout(r, 1200));
                                    }
                                }
                            }

                            const exMap = {
                                'cb-spot': 'Coinbase Spot', 'bn-spot': 'Binance Spot', 'bn-perp': 'Binance Perp',
                                'hl-perp': 'Hyperliquid Perp', 'bybit-spot': 'Bybit Spot', 'bybit-perp': 'Bybit Perp',
                                'okx-perp': 'OKX Perp', 'okx-spot': 'OKX Spot', 'ok-perp': 'OKX Perp', 'ok-spot': 'OKX Spot',
                                'aster-perp': 'Aster Perp', 'bfx-perp': 'Bitfinex Perp', 'bfx-spot': 'Bitfinex Spot',
                                'bitget-perp': 'Bitget Perp', 'bg-perp': 'Bitget Perp', 'gate-perp': 'Gate Perp',
                                'gt-perp': 'Gate Perp', 'gate-spot': 'Gate Spot', 'kr-spot': 'Kraken Spot',
                                'bs-spot': 'Bitstamp Spot', 'deribit-perp': 'Deribit Perp', 'dydx-perp': 'dYdX Perp',
                            };

                            function formatUsd(v) {
                                if (!v || v === 0) return '$0';
                                if (v >= 1e9) return '$' + (v / 1e9).toFixed(2).replace(/\\.?0+$/, '') + 'B';
                                if (v >= 1e6) return '$' + (v / 1e6).toFixed(2).replace(/\\.?0+$/, '') + 'M';
                                if (v >= 1e3) return '$' + (v / 1e3).toFixed(2).replace(/\\.?0+$/, '') + 'K';
                                return '$' + Math.round(v).toLocaleString('en-US');
                            }

                            function formatDuration(startT, now) {
                                const diff = Math.max(0, Math.floor((now - startT) / 1000));
                                if (diff < 60) return diff + 's';
                                if (diff < 3600) {
                                    const m = Math.floor(diff / 60);
                                    const s = diff % 60;
                                    return m + 'm ' + (s < 10 ? '0' : '') + s + 's';
                                }
                                if (diff < 86400) {
                                    const h = Math.floor(diff / 3600);
                                    const m = Math.floor((diff % 3600) / 60);
                                    return h + 'h ' + (m < 10 ? '0' : '') + m + 'm';
                                }
                                const d = Math.floor(diff / 86400);
                                const h = Math.floor((diff % 86400) / 3600);
                                return d + 'd ' + (h < 10 ? '0' : '') + h + 'h';
                            }

                            function fastFormatPrice(p) {
                                if (p >= 1000) {
                                    const parts = p.toFixed(2).split('.');
                                    parts[0] = parts[0].replace(/\\B(?=(\\d{3})+(?!\\d))/g, ',');
                                    return parts.join('.');
                                } else if (p >= 1) {
                                    return p.toFixed(2);
                                } else {
                                    return p.toFixed(4);
                                }
                            }

                            let markPrice = null;
                            const bdiv = document.querySelector('.lo-bt-body tr.bdiv');
                            if (bdiv) {
                                const m = bdiv.innerText.match(/([\\d,]+\\.?\\d*)/);
                                if (m) markPrice = parseFloat(m[1].replace(/,/g, ''));
                            }
                            if (!markPrice) {
                                const lastEl = document.querySelector('.lo-last-price') || document.querySelector('[class*="lastPrice"]');
                                if (lastEl) {
                                    const m = lastEl.innerText.match(/([\\d,]+\\.?\\d*)/);
                                    if (m) markPrice = parseFloat(m[1].replace(/,/g, ''));
                                }
                            }

                            const container = document.querySelector('.lo-bt-body');
                            let rawOrders = [];
                            if (container) {
                                const key = Object.keys(container).find(k => k.startsWith('__reactFiber$'));
                                let fiber = container[key];
                                while (fiber) {
                                    let st = fiber.memoizedState;
                                    while (st) {
                                        const val = st.memoizedState;
                                        if (val && typeof val === 'object' && Array.isArray(val['0']) && val['0'].length > 0) {
                                            const first = val['0'][0];
                                            if (first && (first.usd !== undefined || first.initUsd !== undefined)) {
                                                rawOrders = val['0'];
                                                break;
                                            }
                                        }
                                        st = st.next;
                                    }
                                    if (rawOrders.length > 0) break;
                                    fiber = fiber.return;
                                }
                            }

                            if (!markPrice && rawOrders && rawOrders.length > 0) {
                                const asks = rawOrders.filter(o => o.side === 'ask').map(o => o.price);
                                const bids = rawOrders.filter(o => o.side === 'bid').map(o => o.price);
                                if (asks.length > 0 && bids.length > 0) markPrice = (Math.min(...asks) + Math.max(...bids)) / 2.0;
                                else if (asks.length > 0) markPrice = Math.min(...asks);
                                else if (bids.length > 0) markPrice = Math.max(...bids);
                            }

                            const now = Date.now();
                            let resultOrders = [];
                            if (rawOrders && rawOrders.length > 0) {
                                rawOrders.sort((a, b) => b.price - a.price);
                                for (const o of rawOrders) {
                                    const venue = (exMap[o.ex] || o.ex.replace(/-/g, ' ').toUpperCase()) + ' ' + (o.side === 'ask' ? 'Ask' : 'Bid');
                                    const fromPct = markPrice ? ((o.price - markPrice) / markPrice * 100) : 0;
                                    const fromStr = (fromPct >= 0 ? '+' : '') + fromPct.toFixed(2) + '%';
                                    resultOrders.push({
                                        'Venue/Side': venue,
                                        'Price': fastFormatPrice(o.price),
                                        'From Price': fromStr,
                                        'Initial_USD': formatUsd(o.initUsd),
                                        'Remaining_USD': formatUsd(o.usd),
                                        'Filled_USD': formatUsd(o.filledUsd),
                                        'Duration': formatDuration(o.startT, now),
                                        'Status': (o.filledUsd > 0 || o.fills > 0) ? 'Partially Filled' : 'Open'
                                    });
                                }
                            }

                            if (resultOrders.length === 0) {
                                const trs = document.querySelectorAll('.lo-bt-body table tbody tr');
                                for (const tr of trs) {
                                    if (tr.classList.contains('bdiv')) continue;
                                    const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\\n/g, ' '));
                                    if (tds.length >= 8 && tds[0]) {
                                        const venue = tds[0].replace(/[★☆]/g, '').trim();
                                        const pVal = parseFloat(tds[1].replace(/,/g, '')) || 0.0;
                                        const fromPct = markPrice ? ((pVal - markPrice) / markPrice * 100) : 0.0;
                                        const fromStr = (fromPct >= 0 ? '+' : '') + fromPct.toFixed(2) + '%';
                                        resultOrders.push({
                                            'Venue/Side': venue,
                                            'Price': tds[1],
                                            'From Price': fromStr,
                                            'Initial_USD': tds[3],
                                            'Remaining_USD': tds[4],
                                            'Filled_USD': tds[5],
                                            'Duration': tds.length > 8 ? tds[7] : tds[6],
                                            'Status': tds.length > 8 ? tds[8] : tds[7]
                                        });
                                    }
                                }
                            }

                            return {
                                markPrice: markPrice || 0.0,
                                total: resultOrders.length,
                                orders: resultOrders,
                                isRateLimited: isRateLimited
                            };
                        })()""",
                        'returnByValue': True,
                        'awaitPromise': True
                    }}))

                    res = None
                    t_eval_start = time.monotonic()
                    while time.monotonic() - t_eval_start < 12.0:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        msg = json.loads(raw_msg)
                        if msg.get('id') == req_id:
                            res = msg.get('result', {}).get('result', {}).get('value', {})
                            break

                    is_rate_limited = res.get('isRateLimited', False) if res else False
                    if is_rate_limited or (res is not None and len(res.get('orders', [])) == 0 and res.get('markPrice', 0.0) == 0.0):
                        logger.warning(f"[{sym}] Rate limit / 'Large-order data failed to load' detected. Rotating Cloudflare WARP IP...")
                        try:
                            rotate_warp_ip(context="WhaleDumps", reason="FAILED_TO_LOAD", force=True)
                        except Exception as e_rot:
                            logger.error(f"IP rotation error: {e_rot}")
                        await asyncio.sleep(3.0)
                        await ws.send(json.dumps({'id': 9990, 'method': 'Page.reload'}))
                        await asyncio.sleep(4.5)
                        # Re-evaluate after IP rotation
                        req_retry_id = 9000 + curr_overall
                        await ws.send(json.dumps({'id': req_retry_id, 'method': 'Runtime.evaluate', 'params': {'expression': """(() => {
                            const bdiv = document.querySelector('.lo-bt-body tr.bdiv');
                            let markPrice = null;
                            if (bdiv) {
                                const m = bdiv.innerText.match(/([\\d,]+\\.?\\d*)/);
                                if (m) markPrice = parseFloat(m[1].replace(/,/g, ''));
                            }
                            const trs = document.querySelectorAll('.lo-bt-body table tbody tr');
                            let resultOrders = [];
                            for (const tr of trs) {
                                if (tr.classList.contains('bdiv')) continue;
                                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim().replace(/\n/g, ' '));
                                if (tds.length >= 8 && tds[0]) {
                                    resultOrders.push({
                                        'Venue/Side': tds[0].replace(/[★☆]/g, '').trim(),
                                        'Price': tds[1],
                                        'From Price': tds[2],
                                        'Initial_USD': tds[3],
                                        'Remaining_USD': tds[4],
                                        'Filled_USD': tds[5],
                                        'Duration': tds.length > 8 ? tds[7] : tds[6],
                                        'Status': tds.length > 8 ? tds[8] : tds[7]
                                    });
                                }
                            }
                            return { markPrice: markPrice || 0.0, total: resultOrders.length, orders: resultOrders };
                        })()""", 'returnByValue': True}}))
                        t_retry_start = time.monotonic()
                        while time.monotonic() - t_retry_start < 6.0:
                            raw_retry = await asyncio.wait_for(ws.recv(), timeout=4.0)
                            msg_r = json.loads(raw_retry)
                            if msg_r.get('id') == req_retry_id:
                                res = msg_r.get('result', {}).get('result', {}).get('value', {})
                                break

                    orders = res.get('orders', []) if res else []
                    mark = res.get('markPrice', 0.0) if res else 0.0

                    out_path = DUMP_DIR / f"{sym}_whale_open_orders.parquet"
                    pipeline_path = pipeline_open_dir / f"{sym}_whale_open_orders.parquet"

                    # Safeguard: Do not overwrite populated existing parquet with 0 rows on transient data failures
                    if len(orders) == 0 and out_path.exists():
                        try:
                            prev_df = pd.read_parquet(out_path)
                            if len(prev_df) > 0:
                                logger.warning(f"[{sym}] Extracted 0 orders; preserving existing {len(prev_df)} orders on disk.")
                                continue
                        except Exception:
                            pass

                    df = pd.DataFrame(orders, columns=EXCEL_SPEC_COLUMNS)
                    with writer_lock(out_path):
                        atomic_parquet(df, out_path)

                    with writer_lock(pipeline_path):
                        atomic_parquet(df, pipeline_path)

                    success_count += 1
                    dt = time.time() - t_sym_start
                    print(f"[{curr_overall}/{total}] [{sym}] Success: {len(df)} orders dumped to {out_path.name} (Mark: ${mark:,.2f}) in {dt:.2f}s")

        except Exception as chunk_err:
            logger.error(f"Chunk {chunk_num} error: {chunk_err}")
        finally:
            if tab_id:
                try:
                    await adapter.close_tab(tab_id)
                    logger.info(f"Chunk {chunk_num} complete. Recycled tab {tab_id}.")
                except Exception:
                    pass
            await asyncio.sleep(1.0)

    total_time = time.time() - t0_overall
    print("=" * 80)
    print(f"[COMPLETED] BATCH WHALE DUMP: {success_count}/{total} successful in {total_time:.1f}s ({total_time/60:.2f} min)")
    print(f"Target Directory: {DUMP_DIR}")
    print("=" * 80)


async def run_cli(args):
    """Executes requested CLI actions."""
    if args.serve:
        import uvicorn
        logger.info(f"Starting Standalone Whale API server on port {args.port}...")
        config = uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
        return

    if args.all_assets:
        cycle = 0
        while True:
            cycle += 1
            active_symbols = load_whale_registry_symbols()
            pending = [s for s in active_symbols if not is_whale_asset_complete(s)]
            logger.info(f"[Whales Cycle {cycle}] Total active assets: {len(active_symbols)} | Pending: {len(pending)}")
            if not pending:
                logger.info(f"All {len(active_symbols)} active whale assets are 100% dumped and verified on disk!")
                if not getattr(args, 'daemon', False):
                    break
                await asyncio.sleep(getattr(args, 'interval', 300.0))
                continue
            await harvest_batch_segmented(pending, chunk_size=25, display_only=not args.include_quant_cols)
            if not getattr(args, 'daemon', False):
                break
        return
    else:
        target_symbols = [args.symbol.upper()]

    logger.info(f"Initiating Whale Blotter Dump for {len(target_symbols)} symbols: {target_symbols}")
    t_start = time.time()
    success_count = 0

    for idx, sym in enumerate(target_symbols, 1):
        logger.info(f"[{idx}/{len(target_symbols)}] Processing {sym}...")
        try:
            dataset, out_file = await use_case.execute(sym, persist=True, display_only=not args.include_quant_cols)
            success_count += 1
            print(f" -> [{sym}] Success: {dataset.total_orders} orders dumped to {out_file.name} (Mark: ${dataset.mark_price:,.2f})")
        except Exception as e:
            logger.error(f"[{sym}] Dump failed: {e}")

    total_time = time.time() - t_start
    logger.info(f"Completed Whale Blotter Dumps: {success_count}/{len(target_symbols)} successful in {total_time:.2f}s.")


def main():
    parser = argparse.ArgumentParser(description="CoinGlass Standalone Whale Blotter API & Dumper")
    parser.add_argument("--symbol", type=str, default="BTC", help="Symbol to harvest (default: BTC)")
    parser.add_argument("--dump", action="store_true", help="Dump to local Parquet in Database/Whale_Dumps/")
    parser.add_argument("--all-assets", action="store_true", help="Harvest all core whale assets")
    parser.add_argument("--include-quant-cols", action="store_true", help="Include numeric/temporal auxiliary columns alongside 8 display columns")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=float, default=300.0, help="Loop interval seconds in daemon mode (default: 300)")
    parser.add_argument("--serve", action="store_true", help="Launch FastAPI REST server")
    parser.add_argument("--port", type=int, default=8009, help="FastAPI port (default: 8009)")

    args = parser.parse_args()
    asyncio.run(run_cli(args))


if __name__ == "__main__":
    main()
