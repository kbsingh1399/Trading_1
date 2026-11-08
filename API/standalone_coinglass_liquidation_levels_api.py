"""
standalone_coinglass_liquidation_levels_api.py
========================================================================================
Institutional Standalone CoinGlass Liquidation Levels & Magnet Zones API & Dumper
========================================================================================
Pillar 3 Standalone Telemetry Dumper (API Directory Edition):
- Strictly operates via Chrome DevTools Protocol (CDP port 9222).
- Dynamic Active Assets: reads live active assets from API/active_assets.json on every loop.
- In-Page Tab Navigation: reuses warm CDP tab across chunk sliding windows for ultra-fast scraping.
- Completion Guarantee While-Loop: runs continuously until 100% of assets in API/active_assets.json
  are fully dumped (15 Parquets per asset) and verified on disk.
- Extracts all 5 institutional liquidation structures across 3 lookbacks (3D, 7D, 30D):
    1. Key liquidation levels (Within ±4% · sorted by strength)
    2. Strength by distance (Total strength in each band from last price)
    3. Magnet zones (Top 3 short & long clusters within ±10%)
    4. By leverage tier (Open-level strength per leverage tier)
    5. Near-field balance (Open strength within ±3% of price & 24h short share drift)
- Persists exactly 15 Parquet files per asset to:
    Database/Liquidation_Level_Dumps/<SYMBOL>/
      ├── 3D_<SYMBOL>_KeyLiquidationLevel.parquet
      ├── 3D_<SYMBOL>_StrengthByDistance.parquet
      ├── 3D_<SYMBOL>_MagnetZones.parquet
      ├── 3D_<SYMBOL>_OpenLevelLeverageStrength.parquet
      ├── 3D_<SYMBOL>_NearFieldBalance.parquet
      ├── 7D_<SYMBOL>_KeyLiquidationLevel.parquet
      ├── 7D_<SYMBOL>_StrengthByDistance.parquet
      ├── 7D_<SYMBOL>_MagnetZones.parquet
      ├── 7D_<SYMBOL>_OpenLevelLeverageStrength.parquet
      ├── 7D_<SYMBOL>_NearFieldBalance.parquet
      ├── 30D_<SYMBOL>_KeyLiquidationLevel.parquet
      ├── 30D_<SYMBOL>_StrengthByDistance.parquet
      ├── 30D_<SYMBOL>_MagnetZones.parquet
      ├── 30D_<SYMBOL>_OpenLevelLeverageStrength.parquet
      └── 30D_<SYMBOL>_NearFieldBalance.parquet
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
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

API_DIR = Path(__file__).resolve().parent
ROOT_DIR = API_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from coinglass_common import (
    acquire_singleton, atomic_parquet, writer_lock,
    LOCK_DIR, verify_parquet, cdp_eval, bot_tab_url, is_process_bot_tab
)
from active_assets_manager import load_active_assets
from coinglass_common import rotate_warp_ip

DB_DIR = ROOT_DIR / "Database"
LIQ_DUMP_DIR = DB_DIR / "Liquidation_Level_Dumps"
CDP_BASE_URL = "http://127.0.0.1:9222"

LIQ_DUMP_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("LiquidationAPI")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [LIQ-API] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


HORIZONS = [
    {"name": "3D · 1m", "key": "3D"},
    {"name": "7D · 3m", "key": "7D"},
    {"name": "30D · 15m", "key": "30D"}
]

REQUIRED_STRUCTURES = [
    "KeyLiquidationLevel",
    "StrengthByDistance",
    "MagnetZones",
    "OpenLevelLeverageStrength",
    "NearFieldBalance"
]

EXTRACTION_JS = r"""(async () => {
    while (!document.body) await new Promise(r => setTimeout(r, 100));
    const horizons = [
        { label: '3D · 1m', key: '3D' },
        { label: '7D · 3m', key: '7D' },
        { label: '30D · 15m', key: '30D' }
    ];

    if (performance.getEntriesByType('resource').some(e => e.responseStatus === 429)) {
        return {allData:{}, isRateLimited:true, rateReason:'HTTP_429'};
    }
    const allData = {};
    let isRateLimited = false;
    const numeric = (value) => {
        const m = String(value ?? '').replace(/[$,\s]/g,'').match(/^([+-]?\d+(?:\.\d+)?)([KMB])?$/i);
        return m ? Number(m[1]) * ({K:1e3,M:1e6,B:1e9}[m[2]?.toUpperCase()] || 1) : null;
    };
    const distance = value => numeric(String(value).replace(/[−–—]/g,'-').replace('%',''));
    const isSelected = el => {
        const node = el.closest('[aria-selected], li, button, [class*="tab"]') || el;
        if (node && node.hasAttribute('aria-selected')) return node.getAttribute('aria-selected') === 'true';
        const classes = String(node.className || '');
        if (/\b(active|selected|is-active|is-selected|c-eUsVIq)\b/i.test(classes)) return true;
        try {
            const c = window.getComputedStyle(el).color;
            if (c && c !== 'rgb(183, 191, 204)' && c !== 'rgba(183, 191, 204, 1)') return true;
        } catch(e) {}
        return false;
    };

    for (const h of horizons) {
        // 1. Switch horizon tab
        const tfBtn = Array.from(document.querySelectorAll('*')).find(el => el.children.length === 0 && el.innerText && el.innerText.trim() === h.label);
        if (!tfBtn) return {allData:{}, isRateLimited:false, error:'horizon control missing: '+h.key};
        tfBtn.click();
        const horizonDeadline = Date.now() + 8000;
        while (Date.now() < horizonDeadline) {
            if (isSelected(tfBtn) && !/Loading(?:…|\.\.\.)/.test(document.body.innerText) && document.querySelectorAll('tbody tr').length) break;
            await new Promise(r => setTimeout(r, 150));
        }
        if (!isSelected(tfBtn)) {
            return {allData:{}, isRateLimited:false, error:'horizon selection unverified: '+h.key};
        }

        // 2. Ensure ±4% tab is selected for Key liquidation levels
        const pctBtn = Array.from(document.querySelectorAll('.tab-inner-label, button, div, span, *')).find(el => (el.children.length === 0 || el.classList.contains('tab-inner-label')) && el.innerText && el.innerText.trim() === '±4%');
        if (pctBtn) {
            pctBtn.click();
            await new Promise(r => setTimeout(r, 400));
        }

        // 3. Active Hydration Polling: Wait for Loading... to clear
        const t0 = Date.now();
        while (Date.now() - t0 < 6000) {
            if (!/Loading(?:…|\.\.\.)/.test(document.body.innerText)) break;
            await new Promise(r => setTimeout(r, 200));
        }

        if (/failed to load|Too Many Requests|HTTP 429|rate limit/i.test(document.body.innerText)) {
            return {allData:{}, isRateLimited:true, rateReason:'RATE_LIMIT_BANNER'};
        }
        if (document.querySelector('iframe[src*="challenges.cloudflare.com"], #turnstile-wrapper')) {
            return {allData:{}, isRateLimited:true, rateReason:'CHALLENGE'};
        }

        // A. Key Liquidation Levels (Within ±4%)
        const keyLevels = [];
        const trs = Array.from(document.querySelectorAll('tbody tr'));
        for (const tr of trs) {
            const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
            if (tds.length >= 5 && (tds[0].toLowerCase().includes('short') || tds[0].toLowerCase().includes('long'))) {
                const side = tds[0].toLowerCase().includes('short') ? 'Short' : 'Long';
                const pVal = numeric(tds[1]);
                const dVal = tds[2].replace(/[−–—]/g, '-').trim();
                const lev = tds[3].trim();
                const strVal = numeric(tds[4]);
                const dist = distance(dVal);
                if (pVal !== null && pVal > 0 && strVal !== null && strVal >= 0 && dist !== null && Math.abs(dist) <= 4) {
                    keyLevels.push({
                        Side: side,
                        Price: pVal,
                        Distance: dVal,
                        Leverage: lev,
                        Strength: strVal
                    });
                }
            }
        }

        const fullText = document.body.innerText;

        // B. Strength By Distance
        const sbdList = [];
        const sbdMatch = fullText.match(/Strength by distance[\s\S]*?Total strength in each band[^\n]*\n([\s\S]*?)(?=By leverage tier|Magnet zones|Near-field balance|$)/i);
        if (sbdMatch) {
            const block = sbdMatch[1];
            const bandRegex = /(\d+[\–\-]\d+%)\s*(?:\n|\s)\s*([\d,.]+[KMB]?)\s*(?:\n|\s)\s*·\s*(?:\n|\s)\s*([\d,.]+[KMB]?)/g;
            let m;
            while ((m = bandRegex.exec(block)) !== null) {
                const band = m[1].replace('–', '-');
                const sStr = numeric(m[2]);
                const lStr = numeric(m[3]);
                if (sStr === null || lStr === null || sStr < 0 || lStr < 0) continue;
                const tot = sStr + lStr;
                sbdList.push({
                    Band: band,
                    Short_Strength: sStr,
                    Long_Strength: lStr,
                    Total_Strength: tot,
                    Short_Share_Pct: tot > 0 ? ((sStr / tot) * 100).toFixed(2) + '%' : '0.00%',
                    Long_Share_Pct: tot > 0 ? ((lStr / tot) * 100).toFixed(2) + '%' : '0.00%'
                });
            }
        }

        // C. By Leverage Tier (OpenLevelLeverageStrength)
        const levList = [];
        const levMatch = fullText.match(/By leverage tier[\s\S]*?Open-level strength per leverage tier[^\n]*\n([\s\S]*?)(?=Strength by distance|Magnet zones|Near-field balance|$)/i);
        if (levMatch) {
            const block = levMatch[1];
            const tierRegex = /(High leverage|Mid leverage|Low leverage)\s*(?:\n|\s)\s*([\d,.]+[KMB]?)\s*(?:\n|\s)\s*·\s*(?:\n|\s)\s*([\d,.]+[KMB]?)/g;
            let m;
            while ((m = tierRegex.exec(block)) !== null) {
                const tier = m[1];
                const sStr = numeric(m[2]);
                const lStr = numeric(m[3]);
                if (sStr === null || lStr === null || sStr < 0 || lStr < 0) continue;
                const tot = sStr + lStr;
                levList.push({
                    Tier: tier,
                    Short_Strength: sStr,
                    Long_Strength: lStr,
                    Total_Strength: tot,
                    Short_Share_Pct: tot > 0 ? ((sStr / tot) * 100).toFixed(2) + '%' : '0.00%',
                    Long_Share_Pct: tot > 0 ? ((lStr / tot) * 100).toFixed(2) + '%' : '0.00%'
                });
            }
        }

        // D. Magnet Zones
        const magnetList = [];
        const parseSide = (block, sideName) => {
            const items = [];
            const lines = block.split('\n').map(l => l.trim()).filter(Boolean);
            let cur = null;
            for (let idx = 0; idx < lines.length; idx++) {
                const l = lines[idx];
                const rankM = l.match(/^([123])$/);
                if (rankM) {
                    if (cur) items.push(cur);
                    cur = { Rank: parseInt(rankM[1]), Side: sideName };
                    continue;
                }
                if (cur) {
                    if (!cur.Price_Range && ((l.includes('−') && !l.includes('%')) || (l.includes('-') && !l.includes('%')))) {
                        const parts = l.split(/[−-]/).map(p => parseFloat(p.replace(/[$,]/g, ''))).filter(p => !isNaN(p));
                        cur.Price_Range = l;
                        cur.Price_Low = parts[0] || 0.0;
                        cur.Price_High = parts[1] || parts[0] || 0.0;
                        continue;
                    }
                    if (cur.Price_Range && !cur.Strength && numeric(l) !== null && numeric(l) >= 0) {
                        cur.Strength = numeric(l);
                        continue;
                    }
                    if (l.startsWith('+') || l.startsWith('-') || l.startsWith('−')) {
                        cur.Distance = l.replace(/[−–]/g, '-');
                        continue;
                    }
                    if (l.toLowerCase().includes('width')) {
                        cur.Band_Width = l.replace(/width\s*/i, '').trim();
                        continue;
                    }
                }
                if (l.toLowerCase().includes('no significant zone')) {
                    items.push({
                        Rank: 1,
                        Side: sideName,
                        Price_Range: 'None',
                        Price_Low: 0.0,
                        Price_High: 0.0,
                        Distance: '0.00%',
                        Strength: 0.0,
                        Band_Width: '0.00%',
                        Status: 'No significant zone on this side'
                    });
                }
            }
            if (cur) items.push(cur);
            return items.filter(it => it.Status || (it.Distance && Math.abs(distance(it.Distance)) <= 10))
                .sort((a,b) => (b.Strength || 0)-(a.Strength || 0)).slice(0,3)
                .map((it,idx) => ({...it, Rank:idx+1})).map(it => ({
                Side: it.Side,
                Rank: it.Rank,
                Price_Range: it.Price_Range || 'None',
                Price_Low: it.Price_Low || 0.0,
                Price_High: it.Price_High || 0.0,
                Distance: it.Distance || '0.00%',
                Strength: it.Strength || 0.0,
                Band_Width: it.Band_Width || '0.00%',
                Status: it.Status || 'Active'
            }));
        };

        const magMatch = fullText.match(/Magnet zones[\s\S]*?Where leveraged positions[^\n]*\n([\s\S]*?)(?=Near-field balance|Related tools|$)/i);
        if (magMatch) {
            const block = magMatch[1];
            const shortBlockM = block.match(/Shorts get liquidated([\s\S]*?)(?=Longs get liquidated|$)/i);
            const longBlockM = block.match(/Longs get liquidated([\s\S]*?)$/i);
            if (shortBlockM) magnetList.push(...parseSide(shortBlockM[1], 'Shorts get liquidated'));
            if (longBlockM) magnetList.push(...parseSide(longBlockM[1], 'Longs get liquidated'));
        }

        // E. Near-Field Balance
        let nearField = null;
        const nfMatch = fullText.match(/Near-field balance[\s\S]*?Open strength within ±3%[\s\S]*?\n([\s\S]*?)(?=Short share, last 24h|Frequently asked questions|$)/i);
        if (nfMatch) {
            const block = nfMatch[1];
            const longsM = block.match(/Longs\s*([\d.]+)%\s*·\s*([\d,.]+[KMB]?)/i);
            const shortsM = block.match(/Shorts\s*([\d.]+)%\s*·\s*([\d,.]+[KMB]?)/i);
            const interpM = block.match(/(Heavier on [^\n]+)/i);
            
            const lPct = longsM ? Number(longsM[1]) : null;
            const lStr = longsM ? numeric(longsM[2]) : null;
            const sPct = shortsM ? Number(shortsM[1]) : null;
            const sStr = shortsM ? numeric(shortsM[2]) : null;
            const interp = interpM ? interpM[1].trim() : '';

            if (lStr !== null && sStr !== null && lStr >= 0 && sStr >= 0) nearField = {
                Long_Pct: lPct + '%',
                Long_Strength: lStr,
                Short_Pct: sPct + '%',
                Short_Strength: sStr,
                Total_Strength: lStr + sStr,
                Bias: sStr > lStr ? 'Short-Heavy' : (lStr > sStr ? 'Long-Heavy' : 'Balanced'),
                Interpretation: interp
            };
        }

        allData[h.key] = {
            horizon:h.key,
            keyLevels,
            strengthByDistance: sbdList,
            leverageStrength: levList,
            magnetZones: magnetList,
            nearField
        };
    }

    return { allData, isRateLimited };
})()"""


STRUCTURE_SCHEMAS = {
    "KeyLiquidationLevel": (["Asset","Time","Side","Price","Distance","Leverage","Strength"], "keyLevels"),
    "StrengthByDistance": (["Asset","Time","Band","Short_Strength","Long_Strength","Total_Strength","Short_Share_Pct","Long_Share_Pct"], "strengthByDistance"),
    "MagnetZones": (["Asset","Time","Side","Rank","Price_Range","Price_Low","Price_High","Distance","Strength","Band_Width","Status"], "magnetZones"),
    "OpenLevelLeverageStrength": (["Asset","Time","Tier","Short_Strength","Long_Strength","Total_Strength","Short_Share_Pct","Long_Share_Pct"], "leverageStrength"),
    "NearFieldBalance": (["Asset","Time","Long_Pct","Long_Strength","Short_Pct","Short_Strength","Total_Strength","Bias","Interpretation"], "nearField"),
}


def is_asset_dump_complete(sym: str) -> bool:
    try:
        for tf in ("3D", "7D", "30D"):
            for structure, (columns, _) in STRUCTURE_SCHEMAS.items():
                verify_parquet(LIQ_DUMP_DIR / sym / f"{tf}_{sym}_{structure}.parquet", columns)
        return True
    except Exception:
        return False


async def get_or_create_warm_tab(initial_symbol: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9_.-]{0,31}", initial_symbol.upper()):
        raise ValueError(f"Unsafe symbol: {initial_symbol}")
    target_url = bot_tab_url(f"https://www.coinglass.com/liquidation-levels/{initial_symbol.upper()}", "liq_levels_api")
    def _open():
        req = urllib.request.Request(f"{CDP_BASE_URL}/json/new?{target_url}", method="PUT")
        with urllib.request.urlopen(req, timeout=5) as resp:
            tab = json.loads(resp.read().decode())
            return tab.get("id"), tab.get("webSocketDebuggerUrl")
    try:
        return await asyncio.to_thread(_open)
    except Exception as exc:
        logger.error("Failed to create owned liquidation tab: %s", exc)
        return None, None


async def close_tab(tab_id: str):
    if not tab_id:
        return
    def _close():
        try:
            with urllib.request.urlopen(f"{CDP_BASE_URL}/json/close/{tab_id}", timeout=2):
                pass
        except Exception as exc:
            logger.warning("Could not close owned liquidation tab %s: %s", tab_id, exc)
    await asyncio.to_thread(_close)


def persist_asset_liquidation(sym: str, all_data: Dict[str, Any]) -> int:
    # Validate the entire 15-structure snapshot before publishing any file.
    prepared = []
    for tf in ("3D", "7D", "30D"):
        data = all_data.get(tf)
        if not isinstance(data, dict) or data.get("horizon") != tf:
            raise ValueError(f"Missing/unverified liquidation horizon: {tf}")
        for structure, (columns, key) in STRUCTURE_SCHEMAS.items():
            rows = data.get(key)
            rows = [rows] if structure == "NearFieldBalance" and isinstance(rows, dict) else rows
            if not isinstance(rows, list) or not rows:
                raise ValueError(f"Empty liquidation structure {tf}/{structure}; disk preserved")
            df = pd.DataFrame(rows)
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            if list(df.columns) != columns or df.empty:
                raise ValueError(f"Liquidation schema mismatch {tf}/{structure}: {list(df.columns)}")
            prepared.append((LIQ_DUMP_DIR / sym / f"{tf}_{sym}_{structure}.parquet", df, columns))
    saved = 0
    for path, df, columns in prepared:
        with writer_lock(path):
            if path.exists():
                verify_parquet(path, columns)
            atomic_parquet(df, path)
            verify_parquet(path, columns)
        saved += 1
    return saved


async def harvest_symbol_liquidation(ws, symbol: str) -> Tuple[Dict[str, Any], bool]:
    target_url = bot_tab_url(f"https://www.coinglass.com/liquidation-levels/{symbol.upper()}", "liq_levels_api")
    try:
        curr_path = await cdp_eval(ws, "window.location.pathname", timeout=5)
    except Exception:
        curr_path = ""
    if not curr_path or not str(curr_path).upper().endswith(f"/{symbol.upper()}"):
        await cdp_eval(ws, f"window.location.href={json.dumps(target_url)}", timeout=5)
    expression = f"""(async () => {{
        const expected = '/{symbol.upper()}';
        const navDeadline = Date.now() + 15000;
        while (!window.location.pathname.toUpperCase().endsWith(expected)) {{
            if (Date.now() > navDeadline) return {{allData:{{}}, isRateLimited:false, error:'nav_timeout'}};
            await new Promise(r => setTimeout(r, 200));
        }}
        const deadline = Date.now() + 15000;
        while (Date.now() < deadline) {{
            if (document.body && !/Loading(?:…|\\.\\.\\.)/.test(document.body.innerText || '') && document.querySelectorAll('tbody tr').length >= 5) break;
            await new Promise(r => setTimeout(r, 200));
        }}
        return await ({EXTRACTION_JS});
    }})()"""
    res = await cdp_eval(ws, expression, timeout=40.0)
    if not res or res.get("error"):
        raise RuntimeError(f"Liquidation extraction rejected: {res}")
    if res.get("isRateLimited"):
        raise RuntimeError(f"LIQUIDATION_RATE_SIGNAL:{res.get('rateReason', 'RATE_LIMIT_BANNER')}")
    return res.get("allData", {}), False


async def sweep_active_assets_loop(chunk_size: int = 15, daemon: bool = False, loop_interval: float = 300.0):
    """
    Persistent Completion Guarantee Engine:
    - Reads active assets dynamically from API/active_assets.json on every pass.
    - Evaluates which assets are pending (missing 15 Parquet files).
    - Loops until all active assets are 100% complete.
    - Reuses warm browser tab in sliding windows for high throughput.
    - Automatically captures newly added assets without restarts.
    """
    logger.info("=" * 80)
    logger.info("Initiating Liquidation Telemetry Completion Guarantee Engine...")
    logger.info(f"Registry: {API_DIR / 'active_assets.json'} | Output: {LIQ_DUMP_DIR}")
    logger.info("=" * 80)

    cycle_count = 0

    while True:
        cycle_count += 1
        active_assets = load_active_assets()
        pending = active_assets if daemon else [s for s in active_assets if not is_asset_dump_complete(s)]
        
        logger.info(f"[Cycle {cycle_count}] Active assets universe: {len(active_assets)} symbols.")
        if pending:
            logger.info(f"[Cycle {cycle_count}] Pending sweep: {len(pending)}/{len(active_assets)} assets require dumping.")
        else:
            logger.info(f"[Cycle {cycle_count}] All {len(active_assets)} active assets are 100% dumped (15/15 files verified).")
            if not daemon:
                logger.info("Sweep complete and verified. Exiting.")
                break
            else:
                logger.info(f"Daemon mode active. Sleeping for {loop_interval:.0f}s before next cycle...")
                await asyncio.sleep(loop_interval)
                continue

        # Process pending assets in sliding window chunks with warm tab reuse
        chunks = [pending[i:i + chunk_size] for i in range(0, len(pending), chunk_size)]
        
        for chunk_idx, chunk in enumerate(chunks, 1):
            logger.info("=" * 80)
            logger.info(f"--- [Cycle {cycle_count}] Processing Chunk {chunk_idx}/{len(chunks)} ({len(chunk)} assets: {chunk}) ---")
            logger.info("=" * 80)

            tab_id, ws_url = None, None
            try:
                tab_id, ws_url = await get_or_create_warm_tab(chunk[0])
                if not ws_url:
                    logger.error(f"[Chunk {chunk_idx}] Failed to acquire warm CDP tab. Retrying...")
                    continue

                await asyncio.sleep(3.0)

                async with websockets.connect(ws_url, max_size=50 * 1024 * 1024, ping_interval=None) as ws:
                    for sym in chunk:
                        t_sym = time.time()
                        try:
                            all_data, _ = await harvest_symbol_liquidation(ws, sym)
                            saved_count = await asyncio.to_thread(persist_asset_liquidation, sym, all_data)
                        except Exception as exc:
                            logger.error("[%s] Liquidation asset failed: %s", sym, exc)
                            continue
                        dt = time.time() - t_sym
                        logger.info(f" -> [{sym}] Dumped {saved_count}/15 files in {dt:.2f}s -> Database/Liquidation_Level_Dumps/{sym}/")

            except Exception as e:
                logger.error(f"[Chunk {chunk_idx}] Execution error: {e}")
            finally:
                if tab_id:
                    await close_tab(tab_id)

        if not daemon:
            incomplete = [s for s in load_active_assets() if not is_asset_dump_complete(s)]
            if incomplete:
                raise RuntimeError(f"Liquidation sweep incomplete: {incomplete}")
            break
        await asyncio.sleep(loop_interval)


def main():
    acquire_singleton("coinglass_liquidation_levels_api", DB_DIR / ".locks")
    parser = argparse.ArgumentParser(description="CoinGlass Standalone Liquidation Levels API & Dumper")
    parser.add_argument("--chunk-size", type=int, default=15, help="Window size before proactive IP rotation (default: 15)")
    parser.add_argument("--symbol", type=str, help="Specific symbol to dump (e.g. BTC)")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=float, default=300.0, help="Loop interval seconds in daemon mode (default: 300)")
    args = parser.parse_args()

    if args.symbol:
        async def single_sym():
            sym = args.symbol.upper().strip()
            tab_id, ws_url = await get_or_create_warm_tab(sym)
            if ws_url:
                try:
                    await asyncio.sleep(3.0)
                    async with websockets.connect(ws_url, max_size=50*1024*1024) as ws:
                        all_data, _ = await harvest_symbol_liquidation(ws, sym)
                        saved = persist_asset_liquidation(sym, all_data)
                        logger.info(f"[{sym}] Dumped {saved}/15 files to {LIQ_DUMP_DIR / sym}")
                finally:
                    await close_tab(tab_id)
        asyncio.run(single_sym())
    else:
        asyncio.run(sweep_active_assets_loop(chunk_size=args.chunk_size, daemon=args.daemon, loop_interval=args.interval))


if __name__ == "__main__":
    main()
