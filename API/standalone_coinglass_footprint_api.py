"""
standalone_coinglass_footprint_api.py
========================================================================================
Institutional Standalone CoinGlass Footprint API & Multi-Horizon Data Dumper
========================================================================================

Architecture & Design Patterns:
- Hexagonal / Clean Architecture (Ports & Adapters).
- Zero DOM Scraping: Direct JavaScript memory inspection of CoinGlass React Fiber
  runtime (`window.__cg_term`) via Chrome DevTools Protocol (CDP port 9222).
- Extracts and computes pure order flow telemetry across 3 bar aggregation engines:
    1. Time Bars   : 15-Minute (15Min)
    2. Volume Bars : Discrete Dynamic Volume Bars (e.g. 2.0K BTC, 50K ETH)
    3. Range Bars  : Discrete Dynamic Price Range Bars (e.g. 200 USDT BTC, 10 ETH)
- Exports high-fidelity tabular data matching the exact 16-column specification:
  [Time, Open, High, Low, Close, Candle_Vwap_Price, Candle_Volume, Candle_Delta,
   Candle_Delta_Percent, Candle_CVD, Candle_Max_Delta, Candle_Min_Delta,
   Candle_Trades, Candle_Avg_Trade, Candle_BuySell_Ratio, Candle_Signal]
- Hardened Features (Multi-Agent Council Audited):
    * Raw floating-point precision (prevents truncating low-priced altcoins to 0.0)
    * Continuous mathematical CVD (eliminates viewport-boundary step jumps)
    * Midpoint VWAP integration (eliminates -0.5 * tick downward bias)
    * Post-condition hard assertion for Row Size = Auto
    * Safe incremental Parquet merge protected by re-entrant writer_lock
    * Dynamic FastAPI bar_type routing (eliminating 404 bugs)
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
from pathlib import Path
from enum import Enum
from typing import List, Dict, Optional, Tuple, Any
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
DUMP_DIR = ROOT_DIR / "Database" / "Footprint_Dumps"
DUMP_DIR.mkdir(parents=True, exist_ok=True)

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from coinglass_common import writer_lock, atomic_parquet, verify_parquet, cdp_eval, bot_tab_url, is_process_bot_tab
except ImportError:
    writer_lock = None
    atomic_parquet = None
    verify_parquet = None
    cdp_eval = None
    bot_tab_url = None
    is_process_bot_tab = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CG-FOOTPRINT-API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("StandaloneFootprintAPI")

CDP_BASE_URL = "http://127.0.0.1:9222"

EXCEL_SPEC_COLUMNS = [
    "Time",
    "Open",
    "High",
    "Low",
    "Close",
    "Candle_Vwap_Price",
    "Candle_Volume",
    "Candle_Delta",
    "Candle_Delta_Percent",
    "Candle_CVD",
    "Candle_Max_Delta",
    "Candle_Min_Delta",
    "Candle_Trades",
    "Candle_Avg_Trade",
    "Candle_BuySell_Ratio",
    "Candle_Signal",
    "Candle_POC",
    "Candle_VAH",
    "Candle_VAL"
]

AUTHORIZED_MT5_WHITELIST = {
    "BTCUSD": "BTC",
    "ETHUSD": "ETH",
    "DOGEUSD": "DOGE",
    "ADAUSD": "ADA",
    "XRPUSD": "XRP",
    "LTCUSD": "LTC",
    "LINKUSD": "LINK",
    "BTC": "BTC",
    "ETH": "ETH",
    "DOGE": "DOGE",
    "ADA": "ADA",
    "XRP": "XRP",
    "LTC": "LTC",
    "LINK": "LINK"
}

MEME_COINS_BLACKLIST = {
    '1000LUNC', '1000XEC', 'AIXBT', 'ANIME', 'BOME', 'BRETT', 'DOGE', 'PEOPLE', 
    'POPCAT', 'SHIB', 'PEPE', 'BONK', 'FLOKI', 'WIF', 'MEME', 'TURBO', 
    'BABYDOGE', 'NEIRO', 'MOODENG', 'DOGS', 'MOG', 'SPX', 'CAT', '1000BONK', 
    '1000PEPE', '1000SHIB', '1000FLOKI'
}



# ======================================================================================
# 1. DOMAIN LAYER (Entities, Value Objects & Aggregates)
# ======================================================================================

class BarType(str, Enum):
    TIME = "time"
    VOLUME = "volume"
    RANGE = "range"


@dataclass(frozen=True)
class FootprintSignal:
    time_ms: int
    sig_code: str
    sig_name: str
    side: str
    price: float
    title: str = ""
    description: str = ""


@dataclass
class FootprintCandle:
    time_ms: int
    open: float
    high: float
    low: float
    close: float
    candle_volume: float
    buy_volume: float
    sell_volume: float
    candle_delta: float
    candle_delta_percent: float
    candle_cvd: float
    candle_max_delta: float
    candle_min_delta: float
    candle_vwap_price: float
    candle_trades: int
    candle_avg_trade: float
    candle_buysell_ratio: float
    candle_signal: str = ""
    formatted_time: str = ""
    candle_poc: Optional[float] = None
    candle_vah: Optional[float] = None
    candle_val: Optional[float] = None

    def to_spec_row(self) -> Dict[str, Any]:
        """Maps domain attributes directly to the exact spec columns with full floating precision."""
        return {
            "Time": self.formatted_time or datetime.fromtimestamp(self.time_ms / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "Open": float(self.open),
            "High": float(self.high),
            "Low": float(self.low),
            "Close": float(self.close),
            "Candle_Vwap_Price": float(self.candle_vwap_price),
            "Candle_Volume": float(self.candle_volume),
            "Candle_Delta": float(self.candle_delta),
            "Candle_Delta_Percent": round(float(self.candle_delta_percent), 2),
            "Candle_CVD": float(self.candle_cvd),
            "Candle_Max_Delta": float(self.candle_max_delta),
            "Candle_Min_Delta": float(self.candle_min_delta),
            "Candle_Trades": int(self.candle_trades),
            "Candle_Avg_Trade": float(self.candle_avg_trade),
            "Candle_BuySell_Ratio": float(self.candle_buysell_ratio),
            "Candle_Signal": str(self.candle_signal or ""),
            "Candle_POC": self.candle_poc,
            "Candle_VAH": self.candle_vah,
            "Candle_VAL": self.candle_val
        }


@dataclass
class FootprintDataset:
    symbol: str
    bar_type: BarType
    bar_label: str
    extracted_at_utc: str
    candles: List[FootprintCandle] = field(default_factory=list)
    signals: List[FootprintSignal] = field(default_factory=list)

    def to_dataframe(self) -> pd.DataFrame:
        rows = [c.to_spec_row() for c in self.candles]
        df = pd.DataFrame(rows, columns=EXCEL_SPEC_COLUMNS)
        return df


# ======================================================================================
# 2. IN-PAGE CDP EXTRACTION JAVASCRIPT
# ======================================================================================

INJECT_REACT_FIBER_JS = """(() => {
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

GET_ALL_AGGREGATED_SYMBOLS_JS = """(() => {
    const term = window.__cg_term;
    if (!term || !term.pairs) return ['BTC', 'ETH', 'DOGE', 'ADA', 'XRP', 'LTC', 'LINK'];
    const pairs = term.pairs();
    const aggPerp = pairs.filter(p => p.m && p.m.id === 'agg-perp').map(p => p.s);
    return [...new Set(aggPerp)];
})()"""

EXTRACT_MULTI_BAR_FOOTPRINT_JS = """(async (targetSymbol) => {
    if (performance.getEntriesByType('resource').some(e => e.responseStatus === 429)) {
        return {error:'rate_limited', rateReason:'HTTP_429'};
    }
    if (document.querySelector('iframe[src*="challenges.cloudflare.com"], #turnstile-wrapper')) {
        return {error:'challenge', rateReason:'CHALLENGE'};
    }
    const term = window.__cg_term;
    if (!term) return { error: 'CoinGlass terminal window.__cg_term is not hooked' };

    const sym = targetSymbol || 'BTC';
    const previousSymbolD = term.D;
    const symbolChanged = term.prefs.sym !== sym || term.prefs.itv !== '15m';
    if (term.prefs.sym !== sym) term.pickPair(sym, 'agg-perp');
    if (term.prefs.itv !== '15m') {
        if (typeof term.setItv !== 'function') return {error:'15m interval control unavailable'};
        term.setItv('15m');
    }
    const symDeadline = Date.now() + 15000;
    while (Date.now() < symDeadline) {
        if (term.prefs.sym === sym && term.prefs.itv === '15m' && !term.D.loading && term.D.T?.length && (!symbolChanged || term.D !== previousSymbolD)) break;
        await new Promise(r => setTimeout(r, 100));
    }
    if (term.prefs.sym !== sym || term.prefs.itv !== '15m' || term.D.loading || !term.D.T?.length ||
        (symbolChanged && term.D === previousSymbolD)) {
        return {error:'symbol/15m hydration unverified'};
    }

    // Hard assert Row Size = Auto
    if (term.prefs.tick !== 'auto' && typeof term.setTick === 'function') {
        term.setTick('auto');
    }
    if (term.prefs.tick !== 'auto') {
        return { error: 'Invariant failure: term.prefs.tick could not be set to auto' };
    }

    const results = {
        symbol: sym,
        market: term.prefs.mkt || 'agg-perp',
        sets: []
    };

    function computeAna(D, idx) {
        const cached = term.cache?.ana?.get(idx);
        if (cached && cached.mx !== undefined && cached.mn !== undefined) {
            return cached;
        }
        const i = D.F ? D.F[idx] : null;
        if (!i || !i.b) return { mx: 0, mn: 0 };
        
        let a = D.obs ? D.obs.get(D.T[idx]) : null;
        let r = 0, n = 0;
        if (a && a.full) {
            r = a.mn;
            n = a.mx;
        } else {
            let e = i.b.length, l = i.lo, o_tick = i.tick;
            if (!o_tick || isNaN(o_tick) || isNaN(l)) return { mx: 0, mn: 0 };
            let h = val => Math.max(0, Math.min(Math.floor((val - l) / o_tick + 1e-9), e - 1));
            let d = h(D.O[idx]), c = h(D.H[idx]), p = h(D.L[idx]), u = h(D.C[idx]);
            if (isNaN(d) || isNaN(c) || isNaN(p) || isNaN(u)) return { mx: 0, mn: 0 };
            let f = D.C[idx] >= D.O[idx] ? [[d, p], [p, c], [c, u]] : [[d, c], [c, p], [p, u]];
            let g = new Uint16Array(e), m = [];
            for (let [e1, t1] of f) {
                let step = e1 <= t1 ? 1 : -1;
                for (let j = e1; m.push(j), g[j]++, j !== t1; j += step);
            }
            for (let j = 0; j < e; j++) {
                if (!g[j] && (i.b[j] || i.s[j])) {
                    m.push(j);
                    g[j] = 1;
                }
            }
            let b = 0;
            for (let j of m) {
                b += (i.b[j] - i.s[j]) / g[j];
                if (b < r) r = b;
                if (b > n) n = b;
            }
            if (a && !a.full) {
                r = Math.min(r, a.mn);
                n = Math.max(n, a.mx);
            }
        }
        return { mx: n, mn: r };
    }

    const barModes = ['time', 'vol', 'range'];

    for (const btType of barModes) {
        const previousD = term.D;
        const modeChanged = term.prefs.bt !== btType;
        if (modeChanged) term.setBt(btType);
        if (term.prefs.tick !== 'auto' && typeof term.setTick === 'function') {
            term.setTick('auto');
        }

        // Reactive micro-polling instead of static sleeps
        const modeDeadline = Date.now() + 15000;
        while (Date.now() < modeDeadline) {
            if (term.prefs.bt === btType && term.prefs.tick === 'auto' && !term.D.loading &&
                term.D.T?.length) break;
            await new Promise(r => setTimeout(r, 100));
        }
        const D = term.D;
        if (term.prefs.bt !== btType || term.prefs.tick !== 'auto' || D.loading || !D.T?.length) {
            return {error:'bar-mode hydration unverified: ' + btType};
        }

        // Dynamic query of sigSets AFTER switching bar modes so volume & range signals populate
        const currentSigSets = term.sigSets ? term.sigSets() : [];
        const matchingSet = currentSigSets.find(s => s.bt === btType);
        let rawLabel = matchingSet ? matchingSet.label : btType;
        if (btType === 'time' && term.prefs.itv) {
            rawLabel = term.prefs.itv;
        }

        const signals = [];
        const sigMap = {};

        if (matchingSet && matchingSet.sigs) {
            const sigX = matchingSet.X || D;
            for (const s of matchingSet.sigs) {
                const timeMs = sigX.T ? sigX.T[s.i] : null;
                if (!timeMs) continue;
                const text = (term.sigText ? term.sigText(s, sigX) : null) || ['', ''];
                const sigName = term.sigName ? term.sigName(s) : (s.t || 'Signal');
                const side = (s.side === 'buy' || s.side === 'Bullish') ? 'Bullish' : 'Bearish';
                const title = text[0] || '';
                const desc = text[1] || '';
                const priceSuffix = s.p ? (' · ' + s.p) : '';
                
                signals.push({
                    time_ms: timeMs,
                    sig_code: s.t || '',
                    sig_name: sigName,
                    side: side,
                    price: s.p || 0.0,
                    title: title,
                    description: desc
                });

                const fullSigStr = title ? `${sigName} (${side}): ${title} - ${desc}${priceSuffix}` : `${sigName} (${side})${desc ? ' - ' + desc : ''}${priceSuffix}`;
                if (!sigMap[timeMs]) {
                    sigMap[timeMs] = [];
                }
                sigMap[timeMs].push(fullSigStr);
            }
        }

        const bars = [];
        const n = D.T.length;

        const deltas = [];
        const clusterMetrics = [];
        for (let i = 0; i < n; i++) {
            const F = D.F ? D.F[i] : null;
            let buyVol = 0.0, sellVol = 0.0, buyCnt = 0, sellCnt = 0, sumPV = 0.0;
            let poc = null, vah = null, val = null;

            if (F && F.b && F.s) {
                const len = Math.min(F.b.length, F.s.length);
                for (let j = 0; j < len; j++) {
                    const b = F.b[j] || 0.0;
                    const s = F.s[j] || 0.0;
                    const bc = (F.bc && F.bc[j]) || 0;
                    const sc = (F.sc && F.sc[j]) || 0;
                    // Midpoint VWAP: adds +0.5 * tick to eliminate -0.5*tick downward bias
                    const p = (F.lo !== undefined && F.tick) ? (F.lo + (j + 0.5) * F.tick) : D.C[i];
                    buyVol += b;
                    sellVol += s;
                    buyCnt += bc;
                    sellCnt += sc;
                    sumPV += p * (b + s);
                }
            }

            if (F && F.b && F.s && Number.isFinite(F.lo) && Number.isFinite(F.tick) && F.tick > 0) {
                const volumes = Array.from({length:Math.min(F.b.length,F.s.length)}, (_,j) => (F.b[j]||0)+(F.s[j]||0));
                const total = volumes.reduce((a,b) => a+b, 0);
                if (total > 0) {
                    let hi = volumes.reduce((best,n,j) => n > volumes[best] ? j : best, 0), lo = hi, included = volumes[hi];
                    while (included < total*0.70 && (lo > 0 || hi < volumes.length-1)) {
                        const down = lo > 0 ? volumes[lo-1] : -1, up = hi < volumes.length-1 ? volumes[hi+1] : -1;
                        if (up >= down) included += volumes[++hi]; else included += volumes[--lo];
                    }
                    poc = F.lo + volumes.reduce((best,n,j) => n > volumes[best] ? j : best, 0)*F.tick;
                    val = F.lo + lo*F.tick; vah = F.lo + hi*F.tick;
                }
            }
            const totalVol = buyVol + sellVol;
            const delta = buyVol - sellVol;
            deltas.push(delta);
            clusterMetrics.push({
                totalVol: totalVol,
                buyVol: buyVol,
                sellVol: sellVol,
                delta: delta,
                vwap: totalVol > 0 ? (sumPV / totalVol) : ((D.H[i] + D.L[i] + D.C[i]) / 3.0),
                trades: buyCnt + sellCnt, poc, vah, val
            });
        }

        // True continuous cumulative delta (CVD) eliminating viewport step jumps
        let runningCVD = 0.0;
        const continuousCVDs = [];
        for (let i = 0; i < n; i++) {
            runningCVD += deltas[i];
            continuousCVDs.push(runningCVD);
        }

        for (let i = 0; i < n; i++) {
            const t = D.T[i];
            const o = D.O[i];
            const h = D.H[i];
            const l = D.L[i];
            const c = D.C[i];
            const m = clusterMetrics[i];
            const ana = computeAna(D, i);

            // Pure UTC ISO timestamp formatting (eliminating 5.5h local time shift)
            const d = new Date(t);
            const pad = num => String(num).padStart(2, '0');
            const timeUtc = d.getUTCFullYear() + '-' + pad(d.getUTCMonth() + 1) + '-' + pad(d.getUTCDate()) + ' ' +
                            pad(d.getUTCHours()) + ':' + pad(d.getUTCMinutes()) + ':' + pad(d.getUTCSeconds());

            const candleSig = sigMap[t] ? sigMap[t].join(' | ') : '';

            bars.push({
                time_ms: t,
                time_utc: timeUtc,
                open: o,
                high: h,
                low: l,
                close: c,
                volume: m.totalVol,
                buy_volume: m.buyVol,
                sell_volume: m.sellVol,
                delta: m.delta,
                cvd: continuousCVDs[i],
                vwap: m.vwap,
                max_delta_level: ana.mx,
                min_delta_level: ana.mn,
                trades: m.trades,
                signal: candleSig,
                poc: m.poc, vah: m.vah, val: m.val
            });
        }

        results.sets.push({
            bar_type: btType,
            label: rawLabel,
            bars: bars,
            signals: signals
        });
    }

    return results;
})"""


# ======================================================================================
# 3. INFRASTRUCTURE LAYER (CDP Browser Adapter & Order Flow Engine)
# ======================================================================================

class CDPBrowserAdapter:
    """Manages Chrome connection and in-memory terminal execution via CDP."""

    def __init__(self, cdp_url: str = CDP_BASE_URL):
        self.cdp_url = cdp_url

    def get_open_tabs(self) -> List[Dict[str, Any]]:
        try:
            req = urllib.request.Request(f"{self.cdp_url}/json", headers={"User-Agent": "Antigravity/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug(f"CDP probe error: {e}")
            return []

    def close_tab(self, tab_id: str):
        if not tab_id:
            return
        try:
            req = urllib.request.Request(f"{self.cdp_url}/json/close/{tab_id}", method="PUT")
            with urllib.request.urlopen(req, timeout=3) as resp:
                pass
        except Exception as e:
            logger.debug(f"Failed to close tab {tab_id}: {e}")

    def get_or_create_footprint_tab(self, symbol: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
        # Always create an owned tab; a matching bot tag is not a lease.
        try:
            if not re.fullmatch(r"[A-Z0-9][A-Z0-9_.-]{0,31}", symbol.upper()):
                raise ValueError(f"Unsafe symbol: {symbol}")
            url = bot_tab_url(f"https://www.coinglass.com/footprint/{symbol.upper()}", "footprint_api")
            req = urllib.request.Request(f"{self.cdp_url}/json/new?{url}", method="PUT")
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                return data.get("id"), data.get("webSocketDebuggerUrl")
        except Exception as e:
            logger.error(f"Failed to create new footprint tab via CDP: {e}")
            return None, None

    async def get_all_aggregated_symbols(self, ws_url: str) -> List[str]:
        """Queries CoinGlass pairs catalog and returns all symbols with agg-perp markets."""
        symbols = await self.execute_js(ws_url, GET_ALL_AGGREGATED_SYMBOLS_JS, timeout=5.0)
        if isinstance(symbols, list) and len(symbols) > 0:
            return [s for s in symbols if s.upper() not in MEME_COINS_BLACKLIST]
        return ["BTC", "ETH", "DOGE", "ADA", "XRP", "LTC", "LINK"]

    async def execute_js(self, ws_url: str, expression: str, timeout: float = 20.0) -> Any:
        async with websockets.connect(ws_url, ping_interval=None, max_size=50*1024*1024,
                                      open_timeout=10, close_timeout=5) as ws:
            return await cdp_eval(ws, expression, timeout=timeout)

    async def hook_terminal(self, ws_url: str) -> bool:
        for _ in range(12):
            try:
                ready = await self.execute_js(ws_url, INJECT_REACT_FIBER_JS, timeout=2.0)
                if ready:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
        return False

    async def fetch_footprint_data(self, symbol: str = "BTC", ws_url: Optional[str] = None) -> Dict[str, Any]:
        owned_tab = None
        try:
            if ws_url is None:
                owned_tab, ws_url = await asyncio.to_thread(self.get_or_create_footprint_tab, symbol)
            if not ws_url:
                raise ConnectionError(f"Chrome CDP not accessible at {self.cdp_url}")
            if not await self.hook_terminal(ws_url):
                raise RuntimeError("Failed to attach window.__cg_term")
            js_call = f"({EXTRACT_MULTI_BAR_FOOTPRINT_JS})({json.dumps(symbol.upper())})"
            data = await self.execute_js(ws_url, js_call, timeout=70.0)
            if not data or data.get("error") or data.get("symbol") != symbol.upper():
                raise RuntimeError(f"Footprint extraction rejected: {data}")
            return data
        finally:
            if owned_tab:
                await asyncio.to_thread(self.close_tab, owned_tab)


# ======================================================================================
# 4. TRANSFORMATION & DOMAIN BUILDER SERVICE
# ======================================================================================

class FootprintDomainService:
    """Processes raw mathematical arrays from browser into rich FootprintDatasets."""

    @staticmethod
    def clean_file_tag(raw_label: str, bt_type: str) -> str:
        tag = raw_label.replace(" ", "").replace("-", "_").replace(".", "_")
        tag = re.sub(r"[^a-zA-Z0-9_]", "", tag)
        if not tag:
            tag = "default"
        if bt_type == "time":
            tag = tag.replace("m", "Min").replace("h", "Hour").replace("d", "Day")
            if not tag.endswith("Min") and not tag.endswith("Hour") and not tag.endswith("Day"):
                tag += "Min"
        return tag

    @staticmethod
    def build_datasets(raw_payload: Dict[str, Any], symbol: str = "BTC") -> Dict[str, FootprintDataset]:
        """
        Parses all extracted sets dynamically using CoinGlass's native bar configurations
        for whatever asset was harvested.
        """
        if not raw_payload or "error" in raw_payload:
            return {}

        raw_sets = raw_payload.get("sets", [])
        datasets: Dict[str, FootprintDataset] = {}

        for s_idx, raw_set in enumerate(raw_sets):
            bt_name = str(raw_set.get("bar_type", "time")).lower()
            raw_label = str(raw_set.get("label", "")).strip()
            file_tag = FootprintDomainService.clean_file_tag(raw_label, bt_name)
            standard_bt = "volume" if bt_name == "vol" else bt_name
            final_bar_label = f"{file_tag}_{standard_bt}"
            raw_bars = raw_set.get("bars", [])
            raw_sigs = raw_set.get("signals", [])

            # Map signals by bar timestamp
            sig_map: Dict[int, List[str]] = {}
            parsed_signals: List[FootprintSignal] = []
            for s in raw_sigs:
                t_ms = s.get("time_ms")
                side_str = "Bullish" if str(s.get("side", "")).lower() in ["buy", "bullish"] else "Bearish"
                sig_obj = FootprintSignal(
                    time_ms=t_ms,
                    sig_code=s.get("sig_code", ""),
                    sig_name=s.get("sig_name", "Signal"),
                    side=side_str,
                    price=float(s.get("price", 0.0)),
                    title=s.get("title", ""),
                    description=s.get("description", "")
                )
                parsed_signals.append(sig_obj)
                if t_ms:
                    p_suffix = f" · {sig_obj.price}" if sig_obj.price > 0 else ""
                    if sig_obj.title and sig_obj.description:
                        full_s = f"{sig_obj.sig_name} ({sig_obj.side}): {sig_obj.title} - {sig_obj.description}{p_suffix}"
                    elif sig_obj.title:
                        full_s = f"{sig_obj.sig_name} ({sig_obj.side}): {sig_obj.title}{p_suffix}"
                    elif sig_obj.description:
                        full_s = f"{sig_obj.sig_name} ({sig_obj.side}) - {sig_obj.description}{p_suffix}"
                    else:
                        full_s = f"{sig_obj.sig_name} ({sig_obj.side}){p_suffix}"
                    sig_map.setdefault(t_ms, []).append(full_s)

            # Build candles
            candles: List[FootprintCandle] = []
            cum_delta = 0.0

            for b in raw_bars:
                t_ms = b.get("time_ms", 0)
                vol = float(b.get("volume", 0.0))
                b_vol = float(b.get("buy_volume", 0.0))
                s_vol = float(b.get("sell_volume", 0.0))
                delta = float(b.get("delta", 0.0))
                cum_delta += delta

                delta_pct = (delta / vol * 100.0) if vol > 0 else 0.0
                buysell_ratio = (b_vol / max(s_vol, 1e-6)) if s_vol > 0 else (1.0 if b_vol == 0 else 999.0)
                trades = int(b.get("trades", 0))
                avg_trade = (vol / max(trades, 1)) if trades > 0 else vol

                time_str = b.get("time_utc") or datetime.fromtimestamp(t_ms / 1000.0, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                sig_str = b.get("signal") or " | ".join(sig_map.get(t_ms, []))

                candle = FootprintCandle(
                    time_ms=t_ms,
                    formatted_time=time_str,
                    open=float(b.get("open", 0.0)),
                    high=float(b.get("high", 0.0)),
                    low=float(b.get("low", 0.0)),
                    close=float(b.get("close", 0.0)),
                    candle_volume=vol,
                    buy_volume=b_vol,
                    sell_volume=s_vol,
                    candle_delta=delta,
                    candle_delta_percent=delta_pct,
                    candle_cvd=float(b.get("cvd", cum_delta)),
                    candle_max_delta=float(b.get("max_delta_level", 0.0)),
                    candle_min_delta=float(b.get("min_delta_level", 0.0)),
                    candle_vwap_price=float(b.get("vwap", 0.0)),
                    candle_trades=trades,
                    candle_avg_trade=avg_trade,
                    candle_buysell_ratio=buysell_ratio,
                    candle_signal=sig_str,
                    candle_poc=b.get("poc"), candle_vah=b.get("vah"), candle_val=b.get("val")
                )
                candles.append(candle)

            bar_type_enum = BarType.TIME
            if "vol" in bt_name:
                bar_type_enum = BarType.VOLUME
            elif "range" in bt_name:
                bar_type_enum = BarType.RANGE

            datasets[final_bar_label] = FootprintDataset(
                symbol=symbol.upper(),
                bar_type=bar_type_enum,
                bar_label=final_bar_label,
                extracted_at_utc=datetime.now(timezone.utc).isoformat(),
                candles=candles,
                signals=parsed_signals
            )

        return datasets


# ======================================================================================
# 5. STORAGE / DUMP ADAPTER (With Windows writer_lock & Atomic Safe Merge)
# ======================================================================================

class FootprintDumpAdapter:
    """Persists datasets to local Parquet files with atomic reliability and OS locking."""

    @staticmethod
    def dump_three_datasets(datasets: Dict[str, FootprintDataset], dest_dir: Path = DUMP_DIR) -> Dict[str, Dict[str, Any]]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        results = {}

        for key, ds in datasets.items():
            if not ds or not ds.candles:
                continue
            df = ds.to_dataframe()
            sym = ds.symbol
            file_label = ds.bar_label

            parquet_path = dest_dir / f"{sym}_{file_label}_footprint.parquet"

            def _write_with_lock():
                nonlocal df
                if parquet_path.exists():
                    try:
                        df_existing = pd.read_parquet(parquet_path)
                        missing = [c for c in EXCEL_SPEC_COLUMNS if c not in df_existing.columns]
                        for column in missing:
                            df_existing[column] = None
                        df_existing = df_existing[EXCEL_SPEC_COLUMNS]
                        if not df_existing.empty:
                            combined = pd.concat([df_existing, df], ignore_index=True)
                            # Deduplicate: time bars key strictly on Time to update forming candles cleanly;
                            # range/volume bars key on OHLC boundaries to allow concurrent closes.
                            if "time" in file_label.lower():
                                dedup_subset = ["Time"]
                            else:
                                dedup_subset = ["Time", "Open", "High", "Low", "Close"]
                            df = combined.drop_duplicates(subset=dedup_subset, keep="last").sort_values(by="Time", kind="stable").reset_index(drop=True)
                            df["Candle_CVD"] = df["Candle_Delta"].cumsum()
                            logger.info(f"🔄 Deduplicated merge for {parquet_path.name}: {len(df_existing)} -> {len(df)} rows")
                    except Exception as me:
                        logger.error(f"❌ Read failure on {parquet_path.name}: {me}. Aborting write to prevent data loss!")
                        raise RuntimeError(f"Cannot overwrite historical data on merge read failure: {me}")

                if atomic_parquet is None or verify_parquet is None:
                    raise RuntimeError("Required locked storage kernel unavailable")
                atomic_parquet(df, parquet_path, compression="snappy")
                verify_parquet(parquet_path, EXCEL_SPEC_COLUMNS)

            if writer_lock is not None:
                with writer_lock(parquet_path, timeout=10.0):
                    _write_with_lock()
            else:
                _write_with_lock()

            results[key] = {
                "parquet": str(parquet_path),
                "rows": len(df),
                "symbol": sym,
                "label": file_label
            }
            logger.info(f"✅ Dumped [{file_label}] -> {parquet_path.name} ({len(df)} rows)")

        return results


# ======================================================================================
# 6. FASTAPI WEB CONTROLLER
# ======================================================================================

app = FastAPI(
    title="CoinGlass Standalone Footprint API",
    description="Browser-driven institutional Footprint API dumping 15Min Time, Dynamic Volume, and Range bars without DOM scraping.",
    version="2.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

cdp_adapter = CDPBrowserAdapter()


def is_authorized_footprint_symbol(symbol: str) -> bool:
    from active_assets_manager import load_active_assets
    return symbol.upper() in load_active_assets()


@app.get("/")
def api_root():
    return {
        "service": "CoinGlass Standalone Footprint API",
        "status": "online",
        "precision": "full_float64",
        "cvd_mode": "continuous_monotonic",
        "endpoints": [
            "GET /api/v1/footprint/{symbol}",
            "POST /api/v1/footprint/{symbol}/dump-all",
            "GET /api/v1/health"
        ]
    }


@app.get("/api/v1/health")
def health():
    tabs = cdp_adapter.get_open_tabs()
    has_footprint = any("coinglass.com/footprint" in t.get("url", "").lower() for t in tabs)
    return {
        "cdp_connected": len(tabs) > 0,
        "cdp_port": 9222,
        "tabs_count": len(tabs),
        "footprint_tab_open": has_footprint
    }


@app.get("/api/v1/footprint/{symbol}")
async def get_footprint(
    symbol: str = "BTC",
    bar_type: Optional[str] = Query(None, description="Bar type filter: time, volume, or range"),
    format: str = Query("json", pattern="^(json|records)$")
):
    clean_symbol = symbol.upper().strip()
    if not is_authorized_footprint_symbol(clean_symbol):
        raise HTTPException(status_code=403, detail=f"Symbol '{symbol}' is not active")
    try:
        raw_data = await cdp_adapter.fetch_footprint_data(clean_symbol)
        datasets = FootprintDomainService.build_datasets(raw_data, clean_symbol)
        if not datasets:
            raise HTTPException(status_code=503, detail=f"CoinGlass terminal returned empty data for {clean_symbol}")

        if bar_type:
            bt_clean = bar_type.lower()
            target_ds = None
            for k, ds in datasets.items():
                if bt_clean in ds.bar_type.value or bt_clean in k.lower():
                    target_ds = ds
                    break
            if not target_ds:
                raise HTTPException(status_code=404, detail=f"Bar type '{bar_type}' not found. Available: {list(datasets.keys())}")

            df = target_ds.to_dataframe()
            return JSONResponse(content={
                "symbol": target_ds.symbol,
                "bar_type": target_ds.bar_type.value,
                "label": target_ds.bar_label,
                "rows_count": len(df),
                "data": df.to_dict(orient="records") if format == "records" else df.to_dict(orient="list")
            })

        summary = {}
        for k, ds in datasets.items():
            df = ds.to_dataframe()
            summary[k] = {
                "symbol": ds.symbol,
                "bar_type": ds.bar_type.value,
                "label": ds.bar_label,
                "rows": len(df),
                "data": df.to_dict(orient="records") if format == "records" else df.to_dict(orient="list")
            }
        return JSONResponse(content={"symbol": clean_symbol, "datasets": summary})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching footprint data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/footprint/{symbol}/dump-all")
async def trigger_dump_all(symbol: str = "BTC"):
    clean_symbol = symbol.upper().strip()
    if not is_authorized_footprint_symbol(clean_symbol):
        raise HTTPException(status_code=403, detail=f"Symbol '{symbol}' is not active")
    try:
        raw_data = await cdp_adapter.fetch_footprint_data(clean_symbol)

        def _build_and_dump():
            datasets = FootprintDomainService.build_datasets(raw_data, clean_symbol)
            if not datasets:
                raise ValueError(f"Failed to extract valid footprint datasets for {clean_symbol}")
            return FootprintDumpAdapter.dump_three_datasets(datasets, DUMP_DIR)

        dump_results = await asyncio.to_thread(_build_and_dump)
        return {
            "status": "success",
            "symbol": clean_symbol,
            "dumps": dump_results
        }
    except Exception as e:
        logger.error(f"Dump execution failed for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================================
# 7. MOCK GENERATOR FOR OFFLINE / VALIDATION TESTING
# ======================================================================================

def generate_mock_footprint_payload(symbol: str = "BTC") -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    bars = []
    base_price = 76000.0 if symbol.upper() == "BTC" else 2200.0
    cum_delta = 0.0

    for i in range(20):
        t = now_ms - (19 - i) * 15 * 60 * 1000
        o = base_price + i * 10.0
        h = o + 25.0
        l = o - 15.0
        c = o + 12.0
        vol = 150.0 + (i * 7.5)
        buy_vol = vol * 0.55
        sell_vol = vol * 0.45
        delta = buy_vol - sell_vol
        cum_delta += delta

        d = datetime.fromtimestamp(t / 1000.0, timezone.utc)
        time_utc = d.strftime("%Y-%m-%d %H:%M:%S")

        bars.append({
            "time_ms": t,
            "time_utc": time_utc,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": vol,
            "buy_volume": buy_vol,
            "sell_volume": sell_vol,
            "delta": delta,
            "cvd": cum_delta,
            "vwap": (o + h + l + c) / 4.0,
            "max_delta_level": delta * 1.2,
            "min_delta_level": -delta * 0.4,
            "trades": 120 + i * 5,
            "signal": "Absorption (Bullish): Selling absorbed at low · 75800.0" if i == 18 else ""
        })

    return {
        "symbol": symbol.upper(),
        "market": "agg-perp",
        "sets": [
            {
                "bar_type": "time",
                "label": "15Min",
                "signals": [
                    {
                        "time_ms": now_ms - 15 * 60 * 1000,
                        "sig_code": "abs",
                        "sig_name": "Absorption",
                        "side": "Bullish",
                        "price": 75800.0,
                        "title": "Selling absorbed at low",
                        "description": "30% of bar selling, yet closed near high"
                    }
                ],
                "bars": bars
            }
        ]
    }


# ======================================================================================
# 8. COMMAND LINE RUNNER & ENTRYPOINT
# ======================================================================================

async def run_cli_dump(symbol: str = "BTC", use_mock: bool = False):
    logger.info(f"Initiating multi-bar footprint harvest for {symbol.upper()}...")
    try:
        if use_mock:
            logger.info("⚡ Using synthetic validation mode (offline mock)...")
            raw_data = generate_mock_footprint_payload(symbol)
        else:
            raw_data = await cdp_adapter.fetch_footprint_data(symbol)

        datasets = FootprintDomainService.build_datasets(raw_data, symbol)
        dumps = FootprintDumpAdapter.dump_three_datasets(datasets, DUMP_DIR)

        print("\n" + "=" * 80)
        print(f"[SUCCESS] COINGLASS FOOTPRINT DATA DUMP COMPLETE FOR {symbol.upper()}")
        print("=" * 80)
        for k, v in dumps.items():
            print(f"  * Type    : {k.upper()}")
            print(f"    PARQUET : {v.get('parquet')}")
            print(f"    Rows    : {v['rows']}")
            print("-" * 80)
    finally:
        # fetch_footprint_data owns and closes its own tab; mock mode touches no tabs.
        pass


def is_footprint_asset_complete(sym: str) -> bool:
    """Verifies that all 3 bar engine Parquets exist on disk for the asset."""
    for bar_type in ["time", "volume", "range"]:
        matches = [f for f in DUMP_DIR.glob(f"{sym}_*_{bar_type}_footprint.parquet") if f.stat().st_size > 0]
        if not matches:
            f_direct = DUMP_DIR / f"{sym}_{bar_type}_footprint.parquet"
            if not f_direct.exists() or f_direct.stat().st_size == 0:
                return False
    return True

async def run_cli_all_assets(daemon: bool = False, loop_interval: float = 300.0):
    """Discovers all aggregated futures assets on CoinGlass Footprint and dumps all of them."""
    try:
        from active_assets_manager import load_active_assets
    except ImportError:
        def load_active_assets():
            return ["BTC", "ETH", "SOL", "XAU", "XAG"]

    cycle = 0
    while True:
        cycle += 1
        symbols = load_active_assets()
        pending = symbols if daemon else [s for s in symbols if not is_footprint_asset_complete(s)]
        logger.info(f"[Footprint Cycle {cycle}] Active assets: {len(symbols)} | Pending: {len(pending)}")

        if not pending:
            logger.info(f"All {len(symbols)} active footprint assets are 100% dumped and verified on disk!")
            if not daemon:
                break
            await asyncio.sleep(loop_interval)
            continue

        tab_id, ws_url = cdp_adapter.get_or_create_footprint_tab(pending[0])
        if not ws_url:
            raise ConnectionError("Chrome CDP not accessible at 127.0.0.1:9222")
        await cdp_adapter.hook_terminal(ws_url)

        total_dumped = 0
        start_time = time.time()
        try:
            for idx, sym in enumerate(pending, 1):
                print(f"\n[{idx}/{len(pending)}] Harvesting Aggregated Footprint for {sym}...")
                try:
                    raw_data = await cdp_adapter.fetch_footprint_data(sym, ws_url=ws_url)
                    if not raw_data or "error" in raw_data:
                        logger.warning(f"Skipping {sym}: {raw_data.get('error') if raw_data else 'Empty payload'}")
                        continue
                    datasets = FootprintDomainService.build_datasets(raw_data, sym)
                    dumps = FootprintDumpAdapter.dump_three_datasets(datasets, DUMP_DIR)
                    total_dumped += len(dumps)
                except Exception as e:
                    logger.error(f"Error harvesting {sym}: {e}")
        finally:
            if tab_id:
                cdp_adapter.close_tab(tab_id)
                logger.info(f"Closed footprint tab {tab_id} (ephemeral cleanup)")

        elapsed = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"[CYCLE {cycle} COMPLETE] Dumped {total_dumped} Parquets in {elapsed:.1f}s")
        print("=" * 80)
        if not daemon:
            break
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Standalone CoinGlass Footprint API & Dumper")
    parser.add_argument("--symbol", type=str, default="BTC", help="Target trading pair (default: BTC)")
    parser.add_argument("--dump-all", action="store_true", help="Harvest and dump all 3 bar types immediately to disk")
    parser.add_argument("--all-assets", action="store_true", help="Dump all active assets from API/active_assets.json")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=float, default=300.0, help="Loop interval seconds in daemon mode (default: 300)")
    parser.add_argument("--serve", action="store_true", help="Start FastAPI REST server")
    parser.add_argument("--port", type=int, default=8008, help="FastAPI port (default: 8008)")
    parser.add_argument("--test-mock", action="store_true", help="Run test dump with mock data to verify format")

    args = parser.parse_args()

    if args.serve:
        import uvicorn
        logger.info(f"Starting Footprint FastAPI Server on http://0.0.0.0:{args.port}...")
        uvicorn.run(app, host="0.0.0.0", port=args.port)
    elif args.test_mock:
        asyncio.run(run_cli_dump(args.symbol, use_mock=True))
    elif args.all_assets:
        asyncio.run(run_cli_all_assets(daemon=args.daemon, loop_interval=args.interval))
    elif args.dump_all or args.symbol:
        asyncio.run(run_cli_dump(args.symbol, use_mock=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
