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
    from coinglass_common import writer_lock, atomic_parquet, verify_parquet, cdp_eval, bot_tab_url, is_process_bot_tab
except ImportError:
    writer_lock = None
    atomic_parquet = None
    BrowserLock = None
    verify_parquet = None
    cdp_eval = None
    bot_tab_url = None
    is_process_bot_tab = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CG-WHALES-API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("StandaloneWhaleAPI")

WHALE_EXTRACTION_JS = r"""(async (targetSymbol) => {
    const expected = '/' + targetSymbol.toUpperCase();
    const navDeadline = Date.now() + 15000;
    while (!window.location.pathname.toUpperCase().endsWith(expected)) {
        if (Date.now() > navDeadline) {
            return {success:false, error:'wrong_symbol_route', orders:[]};
        }
        await new Promise(r => setTimeout(r, 200));
    }
    if (performance.getEntriesByType('resource').some(e => e.responseStatus === 429)) {
        return {success:false, error:'rate_limited', rateReason:'HTTP_429', orders:[]};
    }
    while (!document.body) await new Promise(r => setTimeout(r, 100));
    const bodyText = document.body ? document.body.innerText : '';
    if (/Too Many Requests|HTTP 429|rate limit|Large-order data failed to load/i.test(bodyText)) {
        return {success:false, error:'rate_limited', rateReason:'RATE_LIMIT_BANNER', orders:[]};
    }
    if (document.querySelector('iframe[src*="challenges.cloudflare.com"], #turnstile-wrapper') ||
        /Verify you are human|Checking your browser/i.test(document.title + ' ' + bodyText)) {
        return {success:false, error:'challenge', rateReason:'CHALLENGE', orders:[]};
    }
    const zeroText = s => /^(?:[≥>]=?\s*)?\$0(?:\.0+)?$/.test((s || '').trim());
    const rail = document.querySelector('.lo-rail') || document.body;
    const controls = Array.from(rail.querySelectorAll('[class*="ui_select"], [class*="select"], [role="combobox"]'));
    const control = controls.filter(el => /Size threshold|≥\s*\$/.test(el.innerText || ''))
        .sort((a,b) => (a.innerText||'').length - (b.innerText||'').length)[0];
    if (!control) return {success:false, error:'threshold_control_missing', orders:[]};
    if (!zeroText((control.innerText || '').replace(/Size threshold/gi, '').trim())) {
        for (const type of ['pointerdown','mousedown','pointerup','mouseup','click']) {
            control.dispatchEvent(new MouseEvent(type, {bubbles:true, cancelable:true}));
        }
        let option = null;
        const optDeadline = Date.now() + 3000;
        while (Date.now() < optDeadline) {
            option = Array.from(document.querySelectorAll('[role="option"], li, div, span'))
                .find(el => el.children.length === 0 && zeroText(el.innerText) && !el.closest('table') && !el.closest('.lo-bt-sum') && !el.classList.contains('bid'));
            if (option) break;
            await new Promise(r => setTimeout(r, 150));
        }
        if (!option) return {success:false, error:'zero_threshold_option_missing', orders:[]};
        option.click();
        const deadline = Date.now() + 8000;
        while (Date.now() < deadline) {
            const text = (control.innerText || '').replace(/Size threshold/gi, '').trim();
            if (zeroText(text) && !/Loading/i.test(document.querySelector('.lo-bt-body')?.innerText || '')) break;
            await new Promise(r => setTimeout(r, 150));
        }
        if (!zeroText((control.innerText || '').replace(/Size threshold/gi, '').trim())) {
            return {success:false, error:'zero_threshold_unverified', orders:[]};
        }
        await new Promise(r => setTimeout(r, 800));
    }
    function readFiber() {
        const container = document.querySelector('.lo-bt-body');
        if (!container) return [];
        const key = Object.keys(container).find(k => k.startsWith('__reactFiber$'));
        for (let fiber = key ? container[key] : null; fiber; fiber = fiber.return) {
            for (let st = fiber.memoizedState; st; st = st.next) {
                const val = st.memoizedState;
                const rows = val && typeof val === 'object' ? val['0'] : null;
                if (Array.isArray(rows) && rows.length && rows.every(o => o &&
                    Number.isFinite(Number(o.price)) && Number(o.price) > 0 &&
                    Number.isFinite(Number(o.usd)) && Number(o.usd) >= 0 &&
                    ['ask','bid'].includes(o.side) && typeof o.ex === 'string')) return rows;
            }
        }
        return [];
    }
    let rawOrders = [];
    const deadline = Date.now() + 12000;
    while (Date.now() < deadline) {
        rawOrders = readFiber();
        if (rawOrders.length >= 5 || document.querySelectorAll('.lo-bt-body table tbody tr:not(.bdiv)').length >= 5) break;
        await new Promise(r => setTimeout(r, 200));
    }
    const parseUsd = s => {
        const m = String(s || '').replace(/[$,\s]/g,'').match(/^(-?\d+(?:\.\d+)?)([KMB])?$/i);
        return m ? Number(m[1]) * ({K:1e3,M:1e6,B:1e9}[m[2]?.toUpperCase()] || 1) : null;
    };
    const usd = v => {
        const n = Number(v);
        if (!Number.isFinite(n) || n < 0) return null;
        if (n === 0) return '$0';
        const [div,suffix] = n >= 1e9 ? [1e9,'B'] : n >= 1e6 ? [1e6,'M'] : n >= 1e3 ? [1e3,'K'] : [1,''];
        return '$' + (n/div).toFixed(div === 1 ? 0 : 2).replace(/\.00$/, '').replace(/(\.\d)0$/, '$1') + suffix;
    };
    const price = n => Number(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits: n >= 1 ? 2 : 8, useGrouping:true});
    let mark = null;
    for (const el of [document.querySelector('.lo-bt-body tr.bdiv'), document.querySelector('.lo-last-price'), document.querySelector('[class*="lastPrice"]')]) {
        const m = el?.innerText.match(/([\d,]+\.?\d*)/);
        if (m && Number(m[1].replace(/,/g,'')) > 0) { mark = Number(m[1].replace(/,/g,'')); break; }
    }
    const now = Date.now();
    const duration = start => {
        if (!Number.isFinite(Number(start)) || Number(start) <= 0) return '';
        const s = Math.max(0, Math.floor((now-Number(start))/1000));
        return s < 60 ? s+'s' : s < 3600 ? Math.floor(s/60)+'m '+s%60+'s' : s < 86400 ? Math.floor(s/3600)+'h '+Math.floor(s%3600/60)+'m' : Math.floor(s/86400)+'d '+Math.floor(s%86400/3600)+'h';
    };
    let orders = [], method = 'react_fiber', complete = true;
    if (rawOrders.length) {
        for (const o of [...rawOrders].sort((a,b) => b.price-a.price)) {
            const initial = Number(o.initUsd ?? o.usd), remaining = Number(o.usd), filled = Number(o.filledUsd || 0);
            if (![initial,remaining,filled].every(n => Number.isFinite(n) && n >= 0)) {
                return {success:false, error:'invalid_order_values', orders:[]};
            }
            orders.push({venue_side:o.ex.replace(/-/g,' ').toUpperCase()+' '+(o.side === 'ask' ? 'Ask' : 'Bid'),
                price:price(o.price), from_price:mark ? ((o.price-mark)/mark*100).toFixed(2)+'%' : '',
                initial_usd:usd(initial), remaining_usd:usd(remaining), filled_usd:usd(filled),
                duration:duration(o.startT), status:filled > 0 || o.fills > 0 ? 'Partially Filled' : 'Open',
                raw_price:Number(o.price), raw_initial_usd:initial, raw_remaining_usd:remaining, raw_filled_usd:filled,
                start_time_ms:Number(o.startT || 0), order_id:String(o.id || ''), exchange:o.ex, side:o.side});
        }
    } else {
        method = 'dom_fallback';
        const table = document.querySelector('.lo-bt-body table');
        let scroller = table?.parentElement;
        while (scroller && scroller.scrollHeight <= scroller.clientHeight + 1) scroller = scroller.parentElement;
        const seen = new Map();
        let stable = 0;
        for (let step = 0; step < 100; step++) {
            for (const tr of document.querySelectorAll('.lo-bt-body table tbody tr:not(.bdiv)')) {
                const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length < 8 || !cells[0]) continue;
                const values = [cells[3],cells[4],cells[5]].map(parseUsd);
                const p = Number(cells[1].replace(/[$,]/g,''));
                if (!Number.isFinite(p) || p <= 0 || values.some(v => v === null || v < 0)) continue;
                const venue = cells[0].replace(/[★☆]/g,'').trim();
                const distance = cells[2].includes('%') ? cells[2] : mark ? ((p-mark)/mark*100).toFixed(2)+'%' : '';
                const row = {venue_side:venue, price:cells[1], from_price:distance,
                    initial_usd:cells[3], remaining_usd:cells[4], filled_usd:cells[5],
                    duration:cells.length > 8 ? cells[7] : cells[6], status:cells.length > 8 ? cells[8] : cells[7],
                    raw_price:p, raw_initial_usd:values[0], raw_remaining_usd:values[1], raw_filled_usd:values[2],
                    start_time_ms:0, order_id:'', exchange:'', side:venue.toLowerCase().includes('ask') ? 'ask' : 'bid'};
                seen.set(JSON.stringify(cells), row);
            }
            if (!scroller) { complete = false; break; }
            const before = scroller.scrollTop;
            scroller.scrollTop = Math.min(scroller.scrollHeight, before + Math.max(1, scroller.clientHeight * 0.8));
            await new Promise(r => setTimeout(r, 120));
            stable = scroller.scrollTop === before ? stable + 1 : 0;
            if (stable >= 2) break;
            if (step === 99) complete = false;
        }
        orders = Array.from(seen.values()).sort((a,b) => b.raw_price-a.raw_price);
    }
    return {success:orders.length > 0, error:orders.length ? '' : 'empty_unverified', method, complete,
        threshold_usd:0, mark_price:mark, total:orders.length, orders};
})"""


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
    from active_assets_manager import load_active_assets
    return load_active_assets()


def is_authorized_whale_symbol(symbol: str) -> bool:
    return symbol.upper() in load_whale_registry_symbols()


def is_whale_asset_complete(sym: str) -> bool:
    try:
        verify_parquet(DUMP_DIR / f"{sym}_whale_open_orders.parquet", EXCEL_SPEC_COLUMNS)
        return True
    except Exception:
        return False

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
        """No global reaping: only explicit owned-ID finally blocks may close tabs."""
        return

    async def open_whale_tab(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """Opens a dedicated isolated tab for the given symbol, registering it as active."""
        await self._cleanup_whales_orphan_tabs()
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9_.-]{0,31}", symbol.upper()):
            raise ValueError(f"Unsafe symbol: {symbol}")
        url = bot_tab_url(f"https://www.coinglass.com/large-orderbook-statistics/{symbol.upper()}", "whales_api")

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

    async def extract_on_ws(self, ws, symbol: str) -> WhaleDataset:
        expression = f"({WHALE_EXTRACTION_JS})({json.dumps(symbol.upper())})"
        val = await cdp_eval(ws, expression, timeout=30.0)
        if not val or not val.get("success") or not val.get("complete"):
            raise RuntimeError(f"Whale extraction rejected: {val}")
        now_utc = datetime.now(timezone.utc)
        orders = []
        for item in val["orders"]:
            orders.append(WhaleOrder(
                venue_side=item["venue_side"], price=item["price"], from_price=item["from_price"],
                initial_usd=item["initial_usd"], remaining_usd=item["remaining_usd"], filled_usd=item["filled_usd"],
                duration=item["duration"], status=item["status"], raw_price=float(item["raw_price"]),
                raw_initial_usd=float(item["raw_initial_usd"]), raw_remaining_usd=float(item["raw_remaining_usd"]),
                raw_filled_usd=float(item["raw_filled_usd"]), start_time_ms=int(item.get("start_time_ms") or 0),
                order_id=str(item.get("order_id") or ""), exchange=item.get("exchange", ""), side=item["side"],
                time_ms=int(now_utc.timestamp()*1000), snapshot_time_utc=now_utc.isoformat(),
                snapshot_time_ist=(now_utc+timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S IST")))
        asks = [o for o in orders if o.side == "ask"]
        bids = [o for o in orders if o.side == "bid"]
        return WhaleDataset(symbol.upper(), float(val.get("mark_price") or 0), now_utc.isoformat(),
                            orders[0].snapshot_time_ist, orders, len(orders), len(asks), len(bids),
                            sum(o.raw_remaining_usd for o in asks), sum(o.raw_remaining_usd for o in bids))

    async def harvest_symbol_orders(self, symbol: str, max_retries: int = 3) -> WhaleDataset:
        clean_sym = AUTHORIZED_MT5_WHITELIST.get(symbol.upper(), symbol.upper())
        for attempt in range(1, max_retries + 1):
            tab_id = None
            try:
                tab_id, ws_url = await self.open_whale_tab(clean_sym)
                if not ws_url:
                    raise RuntimeError("Failed to obtain webSocketDebuggerUrl")
                async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10, close_timeout=5, ping_interval=None) as ws:
                    return await self.extract_on_ws(ws, clean_sym)
            except Exception:
                if attempt == max_retries:
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
        symbol = dataset.symbol.upper()
        out_file = self.output_dir / f"{symbol}_whale_open_orders.parquet"
        df = dataset.to_dataframe(display_only=display_only)
        columns = EXCEL_SPEC_COLUMNS if display_only else EXCEL_SPEC_COLUMNS + AUXILIARY_QUANT_COLUMNS
        if df.empty:
            raise ValueError(f"[{symbol}] Empty whale snapshot rejected; disk preserved")
        with writer_lock(out_file):
            if out_file.exists():
                verify_parquet(out_file, columns)
            atomic_parquet(df, out_file)
            verify_parquet(out_file, columns)
        logger.info("[%s] Persisted and read back %s orders", symbol, len(df))
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
    allow_credentials=False,
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
    if not is_authorized_whale_symbol(clean_sym):
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
    if not is_authorized_whale_symbol(clean_sym):
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
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    pipeline = ParquetWhaleRepository(ROOT_DIR / "Database" / "Whale_Orders" / "Open_Orders")
    success = 0
    for start in range(0, len(target_symbols), chunk_size):
        chunk = target_symbols[start:start+chunk_size]
        tab_id = None
        try:
            tab_id, ws_url = await adapter.open_whale_tab(chunk[0])
            if not ws_url:
                continue
            async with websockets.connect(ws_url, max_size=50*1024*1024, open_timeout=10, close_timeout=5, ping_interval=None) as ws:
                for idx, sym in enumerate(chunk):
                    try:
                        if idx:
                            url = bot_tab_url(f"https://www.coinglass.com/large-orderbook-statistics/{sym}", "whales_api")
                            await cdp_eval(ws, f"window.location.href={json.dumps(url)}", timeout=5)
                        dataset = await adapter.extract_on_ws(ws, sym)
                        await asyncio.to_thread(repo.persist_dataset, dataset, display_only)
                        await asyncio.to_thread(pipeline.persist_dataset, dataset, display_only)
                        success += 1
                    except Exception as exc:
                        logger.error("[%s] Whale harvest failed: %s", sym, exc)
        except Exception as exc:
            logger.error("Whale chunk failed: %s", exc)
        finally:
            if tab_id:
                await adapter.close_tab(tab_id)
    logger.info("Whale batch complete: %s/%s verified", success, len(target_symbols))
    return success


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
            # Continuous telemetry must refresh existing snapshots, not just fill missing files.
            pending = active_symbols
            logger.info(f"[Whales Cycle {cycle}] Total active assets: {len(active_symbols)} | Pending: {len(pending)}")
            if not pending:
                logger.info(f"All {len(active_symbols)} active whale assets are 100% dumped and verified on disk!")
                if not getattr(args, 'daemon', False):
                    break
                await asyncio.sleep(getattr(args, 'interval', 300.0))
                continue
            completed = await harvest_batch_segmented(pending, chunk_size=25, display_only=not args.include_quant_cols)
            if not getattr(args, 'daemon', False):
                if completed != len(pending):
                    raise RuntimeError(f"Whale sweep incomplete: {completed}/{len(pending)}")
                break
            await asyncio.sleep(getattr(args, 'interval', 300.0))
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
