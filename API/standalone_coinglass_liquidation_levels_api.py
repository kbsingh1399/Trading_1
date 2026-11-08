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
    rotate_warp_ip, LOCK_DIR
)
from active_assets_manager import load_active_assets

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
    const horizons = [
        { label: '3D · 1m', key: '3D' },
        { label: '7D · 3m', key: '7D' },
        { label: '30D · 15m', key: '30D' }
    ];

    const allData = {};
    let isRateLimited = false;

    for (const h of horizons) {
        // 1. Switch horizon tab
        const tfBtn = Array.from(document.querySelectorAll('*')).find(el => el.children.length === 0 && el.innerText && el.innerText.trim() === h.label);
        if (tfBtn) {
            tfBtn.click();
            await new Promise(r => setTimeout(r, 600));
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
            if (!document.body.innerText.includes('Loading…')) break;
            await new Promise(r => setTimeout(r, 200));
        }

        if (document.body.innerText.includes('failed to load') || document.body.innerText.includes('Too Many Requests')) {
            isRateLimited = true;
        }

        // A. Key Liquidation Levels (Within ±4%)
        const keyLevels = [];
        const trs = Array.from(document.querySelectorAll('tbody tr'));
        for (const tr of trs) {
            const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
            if (tds.length >= 5 && (tds[0].toLowerCase().includes('short') || tds[0].toLowerCase().includes('long'))) {
                const side = tds[0].toLowerCase().includes('short') ? 'Short' : 'Long';
                const pVal = parseFloat(tds[1].replace(/[$,]/g, '')) || 0.0;
                const dVal = tds[2].replace(/[−–—]/g, '-').trim();
                const lev = tds[3].trim();
                const strVal = parseFloat(tds[4].replace(/,/g, '')) || 0.0;
                if (!isNaN(pVal) && pVal > 0) {
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
            const bandRegex = /(\d+[\–\-]\d+%)\s*(?:\n|\s)\s*([\d,]+)\s*(?:\n|\s)\s*·\s*(?:\n|\s)\s*([\d,]+)/g;
            let m;
            while ((m = bandRegex.exec(block)) !== null) {
                const band = m[1].replace('–', '-');
                const sStr = parseFloat(m[2].replace(/,/g, '')) || 0.0;
                const lStr = parseFloat(m[3].replace(/,/g, '')) || 0.0;
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
            const tierRegex = /(High leverage|Mid leverage|Low leverage)\s*(?:\n|\s)\s*([\d,]+)\s*(?:\n|\s)\s*·\s*(?:\n|\s)\s*([\d,]+)/g;
            let m;
            while ((m = tierRegex.exec(block)) !== null) {
                const tier = m[1];
                const sStr = parseFloat(m[2].replace(/,/g, '')) || 0.0;
                const lStr = parseFloat(m[3].replace(/,/g, '')) || 0.0;
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
                    if (!cur.Price_Range && l.includes('−') || (l.includes('-') && !l.includes('%'))) {
                        const parts = l.split(/[−-]/).map(p => parseFloat(p.replace(/[$,]/g, ''))).filter(p => !isNaN(p));
                        cur.Price_Range = l;
                        cur.Price_Low = parts[0] || 0.0;
                        cur.Price_High = parts[1] || parts[0] || 0.0;
                        continue;
                    }
                    if (cur.Price_Range && !cur.Strength && /^\d+$/.test(l.replace(/,/g, ''))) {
                        cur.Strength = parseFloat(l.replace(/,/g, '')) || 0.0;
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
            return items.map(it => ({
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
            const longsM = block.match(/Longs\s*(\d+)%\s*·\s*([\d,]+)/i);
            const shortsM = block.match(/Shorts\s*(\d+)%\s*·\s*([\d,]+)/i);
            const interpM = block.match(/(Heavier on [^\n]+)/i);
            
            const lPct = longsM ? parseInt(longsM[1]) : 0;
            const lStr = longsM ? parseFloat(longsM[2].replace(/,/g, '')) : 0.0;
            const sPct = shortsM ? parseInt(shortsM[1]) : 0;
            const sStr = shortsM ? parseFloat(shortsM[2].replace(/,/g, '')) : 0.0;
            const interp = interpM ? interpM[1].trim() : '';

            nearField = {
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
            keyLevels,
            strengthByDistance: sbdList,
            leverageStrength: levList,
            magnetZones: magnetList,
            nearField
        };
    }

    return { allData, isRateLimited };
})()"""


def is_asset_dump_complete(sym: str) -> bool:
    """Verifies that all 15 Parquet files exist on disk for the specified asset."""
    sym_dir = LIQ_DUMP_DIR / sym
    if not sym_dir.exists():
        return False
    for tf in ["3D", "7D", "30D"]:
        for s in REQUIRED_STRUCTURES:
            f = sym_dir / f"{tf}_{sym}_{s}.parquet"
            if not f.exists() or f.stat().st_size == 0:
                return False
    return True


async def get_or_create_warm_tab(initial_symbol: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    """Opens a single warm CDP tab and connects to WebSocket."""
    url = f"{CDP_BASE_URL}/json/list"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                if "bot=liq_levels_api" in t.get("url", ""):
                    return t["id"], t.get("webSocketDebuggerUrl")
    except Exception:
        pass

    target_url = f"https://www.coinglass.com/liquidation-levels/{initial_symbol}?bot=liq_levels_api"
    new_url = f"{CDP_BASE_URL}/json/new?{target_url}"
    try:
        req = urllib.request.Request(new_url, method="PUT")
        with urllib.request.urlopen(req, timeout=5) as resp:
            tab = json.loads(resp.read().decode())
            return tab.get("id"), tab.get("webSocketDebuggerUrl")
    except Exception as e:
        logger.error(f"Failed to create warm liquidation tab: {e}")
        return None, None


async def close_tab(tab_id: str):
    """Closes an ephemeral CDP tab."""
    if not tab_id:
        return
    url = f"{CDP_BASE_URL}/json/close/{tab_id}"
    try:
        urllib.request.urlopen(url, timeout=2)
    except Exception:
        pass


def persist_asset_liquidation(sym: str, all_data: Dict[str, Any]) -> int:
    """Persists extracted liquidation structures as atomic Parquet files."""
    asset_dir = LIQ_DUMP_DIR / sym
    asset_dir.mkdir(parents=True, exist_ok=True)
    saved_files = 0

    for tf in ["3D", "7D", "30D"]:
        data = all_data.get(tf, {})

        # 1. Key Liquidation Level
        kl_list = data.get("keyLevels", [])
        f_kl = asset_dir / f"{tf}_{sym}_KeyLiquidationLevel.parquet"
        if kl_list:
            df = pd.DataFrame(kl_list)
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            with writer_lock(f_kl):
                atomic_parquet(df, f_kl)
            saved_files += 1
        elif not f_kl.exists():
            df = pd.DataFrame(columns=["Asset", "Time", "Side", "Price", "Distance", "Leverage", "Strength"])
            with writer_lock(f_kl):
                atomic_parquet(df, f_kl)

        # 2. Strength By Distance
        sbd_list = data.get("strengthByDistance", [])
        f_sbd = asset_dir / f"{tf}_{sym}_StrengthByDistance.parquet"
        if sbd_list:
            df = pd.DataFrame(sbd_list)
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            with writer_lock(f_sbd):
                atomic_parquet(df, f_sbd)
            saved_files += 1
        elif not f_sbd.exists():
            df = pd.DataFrame(columns=["Asset", "Time", "Band", "Short_Strength", "Long_Strength", "Total_Strength", "Short_Share_Pct", "Long_Share_Pct"])
            with writer_lock(f_sbd):
                atomic_parquet(df, f_sbd)

        # 3. Magnet Zones
        mag_list = data.get("magnetZones", [])
        f_mag = asset_dir / f"{tf}_{sym}_MagnetZones.parquet"
        if mag_list:
            df = pd.DataFrame(mag_list)
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            with writer_lock(f_mag):
                atomic_parquet(df, f_mag)
            saved_files += 1
        elif not f_mag.exists():
            df = pd.DataFrame(columns=["Asset", "Time", "Side", "Rank", "Price_Range", "Price_Low", "Price_High", "Distance", "Strength", "Band_Width", "Status"])
            with writer_lock(f_mag):
                atomic_parquet(df, f_mag)

        # 4. Open Level Leverage Strength
        lev_list = data.get("leverageStrength", [])
        f_lev = asset_dir / f"{tf}_{sym}_OpenLevelLeverageStrength.parquet"
        if lev_list:
            df = pd.DataFrame(lev_list)
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            with writer_lock(f_lev):
                atomic_parquet(df, f_lev)
            saved_files += 1
        elif not f_lev.exists():
            df = pd.DataFrame(columns=["Asset", "Time", "Tier", "Short_Strength", "Long_Strength", "Total_Strength", "Short_Share_Pct", "Long_Share_Pct"])
            with writer_lock(f_lev):
                atomic_parquet(df, f_lev)

        # 5. Near Field Balance
        nf = data.get("nearField")
        f_nf = asset_dir / f"{tf}_{sym}_NearFieldBalance.parquet"
        if nf:
            df = pd.DataFrame([nf])
            df.insert(0, "Time", tf)
            df.insert(0, "Asset", sym)
            with writer_lock(f_nf):
                atomic_parquet(df, f_nf)
            saved_files += 1
        elif not f_nf.exists():
            df = pd.DataFrame(columns=["Asset", "Time", "Long_Pct", "Long_Strength", "Short_Pct", "Short_Strength", "Total_Strength", "Bias", "Interpretation"])
            with writer_lock(f_nf):
                atomic_parquet(df, f_nf)

    return saved_files


async def harvest_symbol_liquidation(ws, symbol: str) -> Tuple[Dict[str, Any], bool]:
    """In-page navigation to symbol route and evaluation of liquidation data."""
    target_url = f"https://www.coinglass.com/liquidation-levels/{symbol}?bot=liq_levels_api"
    await ws.send(json.dumps({"id": 101, "method": "Page.navigate", "params": {"url": target_url}}))
    await asyncio.sleep(2.0)

    # Active Hydration Wait Loop
    wait_js = r"""(() => {
        return !document.body.innerText.includes('Loading…') && document.querySelectorAll('tbody tr').length > 0;
    })()"""
    
    t0 = time.time()
    while time.time() - t0 < 6.0:
        await ws.send(json.dumps({"id": 102, "method": "Runtime.evaluate", "params": {"expression": wait_js, "returnByValue": True}}))
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.5)
            msg = json.loads(raw)
            if msg.get("id") == 102 and msg.get("result", {}).get("result", {}).get("value") is True:
                break
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0.3)

    # Execute extraction
    await ws.send(json.dumps({
        "id": 103,
        "method": "Runtime.evaluate",
        "params": {
            "expression": EXTRACTION_JS,
            "awaitPromise": True,
            "returnByValue": True
        }
    }))

    res = None
    t1 = time.time()
    while time.time() - t1 < 10.0:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
            msg = json.loads(raw)
            if msg.get("id") == 103:
                res = msg.get("result", {}).get("result", {}).get("value")
                break
        except asyncio.TimeoutError:
            continue

    if not res:
        return {}, False

    all_data = res.get("allData", {})
    is_rate_limited = res.get("isRateLimited", False)
    return all_data, is_rate_limited


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
        pending = [s for s in active_assets if not is_asset_dump_complete(s)]
        
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
                        all_data, is_rate_limited = await harvest_symbol_liquidation(ws, sym)

                        if is_rate_limited:
                            logger.warning(f"[{sym}] Hit CoinGlass rate limit! Triggering reactive Cloudflare WARP rotation...")
                            rotate_warp_ip(context=f"Liq_{sym}", reason="RATE_LIMITED", force=True)
                            await asyncio.sleep(2.0)
                            all_data, is_rate_limited = await harvest_symbol_liquidation(ws, sym)

                        saved_count = persist_asset_liquidation(sym, all_data)
                        dt = time.time() - t_sym
                        logger.info(f" -> [{sym}] Dumped {saved_count}/15 files in {dt:.2f}s -> Database/Liquidation_Level_Dumps/{sym}/")

            except Exception as e:
                logger.error(f"[Chunk {chunk_idx}] Execution error: {e}")
            finally:
                if tab_id:
                    await close_tab(tab_id)

            # Proactive WARP rotation between chunks
            if chunk_idx < len(chunks):
                logger.info("Rotating Cloudflare WARP IP between chunk windows...")
                new_ip = rotate_warp_ip(context=f"Chunk_{chunk_idx}", reason="SCHEDULED", force=True)
                logger.info(f"Fresh Egress IP established: {new_ip}")
                await asyncio.sleep(2.5)

        # Loop will re-evaluate pending vs active_assets dynamically on next pass!


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
