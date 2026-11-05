"""

3_coinglass_liquidation_levels.py

========================================================================================

Standalone CoinGlass Liquidation Levels & Magnet Zones Scraper (In-Browser CDP Bridge)

========================================================================================

Pillar 3 of 5 Standalone Crypto Pipeline:

- Strictly operates via Chrome DevTools Protocol (CDP) on 127.0.0.1:9222.

- Ephemeral tab lifecycle: opens tab (?bot=liq_scraper), harvests, closes immediately.

- Reaps any orphaned bot tabs; strictly preserves user & delta exchange tabs.

- Scrapes all 6 institutional liquidation structures:

  1. Key Liquidation Levels (ranked strike barriers by side, price, leverage tier, distance %, strength)

  2. Magnet Zones (Top 3 short & long liquidation cluster bands, zone_price, distance %, score)

  3. Near-Field Balance (±3% price proximity longs vs shorts % and 24h short share delta)

  4. Strength by Distance (0-1%, 1-2%, 2-3%, 3-5%, 5-10% density bands)

  5. Leverage Tiers (25x, 50x, 100x clusters)

  6. Liquidation Snapshots (CLLD net delta, sentiment, last price)

- Persists to:

  * Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet

  * Database/Liquidation_Data/Magnet_Zones/<SYMBOL>_magnet_zones.parquet

  * Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet

  * Database/Liquidation_Data/Strength_By_Distance/<SYMBOL>_strength_by_distance.parquet

  * Database/Liquidation_Data/Leverage_Tiers/<SYMBOL>_leverage_tiers.parquet

  * Database/Liquidation_Data/liquidation_snapshots.parquet

- Enriched with primary IST columns across all datasets.

- Scheduled daemon mode for minutes :10, :25, :55, or on-demand (--once).

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



from coinglass_common import acquire_singleton, atomic_parquet, upsert_parquet, cdp_result

from coinglass_common import BrowserLock, rotate_warp_ip, LIQUIDATION_MINUTES

from coinglass_common import utc_ms_to_ist_str



DB_DIR = ROOT_DIR / "Database"

LIQ_DIR = DB_DIR / "Liquidation_Data"

KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"

MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"

NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"

STRENGTH_BANDS_DIR = LIQ_DIR / "Strength_By_Distance"

LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"

SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"



for d in [KEY_LEVELS_DIR, MAGNET_ZONES_DIR, NEAR_FIELD_DIR, STRENGTH_BANDS_DIR, LEVERAGE_TIERS_DIR]:

    d.mkdir(parents=True, exist_ok=True)



handler = logging.StreamHandler(sys.stdout)

handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [CG-LIQUIDATION] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

logger = logging.getLogger("LiquidationScraper")

logger.setLevel(logging.INFO)

logger.handlers = [handler]



# Mutual crypto assets with deep liquidation map profiles

CORE_LIQ_SYMBOLS = [

    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "NEAR", "SUI", "TRX", "LTC"

]





def cleanup_orphan_tabs(keep_tab_id: Optional[str] = None):

    try:

        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})

        with urllib.request.urlopen(req, timeout=3) as resp:

            tabs = json.loads(resp.read().decode())

            for t in tabs:

                tid = t.get("id")

                url = t.get("url", "").lower()

                if tid != keep_tab_id and "bot=liq_scraper" in url:

                    try:

                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)

                        logger.info(f"Reaped orphaned liquidation tab ({tid})")

                    except Exception:

                        pass

    except Exception as e:

        logger.debug(f"Liquidation tab reaper: {e}")





def open_liquidation_tab(initial_sym: str = "BTC") -> Tuple[Optional[str], Optional[str]]:

    cleanup_orphan_tabs()

    try:

        url = f"https://www.coinglass.com/liquidation-levels?symbol={initial_sym}&bot=liq_scraper"

        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")

        resp = urllib.request.urlopen(req, timeout=6)

        data = json.loads(resp.read().decode())

        tab_id = data.get("id")

        ws_url = data.get("webSocketDebuggerUrl")

        logger.info(f"Opened fresh ephemeral Liquidation tab ({tab_id})")

        return tab_id, ws_url

    except Exception as e:

        logger.error(f"Failed to open liquidation tab via CDP: {e}")

        return None, None





def close_tab(tab_id: Optional[str]):

    if tab_id:

        try:

            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)

            logger.info(f"Closed liquidation tab ({tab_id}) - Tab footprint clean.")

        except Exception as e:

            logger.warning(f"Could not close tab {tab_id}: {e}")

    cleanup_orphan_tabs()





# ---------------------------------------------------------------------------

# Data Persistence Functions with Primary IST Columns

# ---------------------------------------------------------------------------

def enrich_liq_df(df: pd.DataFrame) -> pd.DataFrame:

    now_utc = datetime.now(timezone.utc)

    now_ms = int(now_utc.timestamp() * 1000)

    df["snapshot_time_utc"] = now_utc.isoformat()

    df["snapshot_time_ist"] = utc_ms_to_ist_str(now_ms)

    df["time_ist"] = df["snapshot_time_ist"]

    df["time_ms"] = now_ms

    return df





def atomic_csv(frame: pd.DataFrame, path: Path):

    from coinglass_common import _publish

    _publish(path, lambda p: frame.to_csv(p, index=False, encoding='utf-8'))





def persist_key_levels(symbol: str, levels: List[dict]):

    if not levels:

        return

    df = pd.DataFrame(levels)

    df["Asset"] = symbol

    

    # Standard IST timestamps

    now_utc = datetime.now(timezone.utc)

    now_ms = int(now_utc.timestamp() * 1000)

    df["snapshot_time_utc"] = now_utc.isoformat()

    df["snapshot_time_ist"] = utc_ms_to_ist_str(now_ms)

    df["time_ist"] = df["snapshot_time_ist"]

    df["time_ms"] = now_ms

    

    # Downstream backwards-compatible columns

    df["symbol"] = symbol

    df["side"] = df["Side"].str.upper()

    df["price"] = pd.to_numeric(df["Price"], errors='coerce')

    df["distance_pct"] = pd.to_numeric(df["Distance"].astype(str).str.replace(r'[%+]', '', regex=True), errors='coerce')

    df["leverage_tier"] = df["Leverage"].astype(str).str.upper().str.replace(r'\s+', '_', regex=True)

    df["strength"] = pd.to_numeric(df["Strength"], errors='coerce')

    if "percent_val" in df.columns:

        df["range_pct_val"] = df["percent_val"].abs()

    

    # User's primary columns first

    lead_cols = ['Asset', 'Time', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']

    other_cols = [c for c in df.columns if c not in lead_cols]

    df = df[lead_cols + other_cols]

    

    # 1. Atomic Master Parquet

    atomic_parquet(df, KEY_LEVELS_DIR / f"{symbol}_key_levels.parquet")

    

    # 2. Atomic Master Combined CSV

    atomic_csv(df[lead_cols], KEY_LEVELS_DIR / f"{symbol}_key_levels.csv")

    

    # 3. Discrete Timeframe CSVs strictly matching user's exact 7-column layout:

    # ['Asset', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']

    exact_7_cols = ['Asset', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']

    for tf_key, tf_label in [('3D', '3D · 1m'), ('7D', '7D · 3m'), ('30D', '30D · 15m')]:

        sub_df = df[df['Time'] == tf_label]

        if not sub_df.empty:

            atomic_csv(sub_df[exact_7_cols], KEY_LEVELS_DIR / f"{symbol}_{tf_key}.csv")



    # 4. Multi-tab Excel Workbook (.xlsx) with dedicated tabs per lookback

    xlsx_path = KEY_LEVELS_DIR / f"{symbol}_key_levels.xlsx"

    try:

        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:

            df[lead_cols].to_excel(writer, sheet_name='All_Combinations', index=False)

            for tf_key, tf_label in [('3D', '3D · 1m'), ('7D', '7D · 3m'), ('30D', '30D · 15m')]:

                sub_df = df[df['Time'] == tf_label]

                if not sub_df.empty:

                    sub_df[exact_7_cols].to_excel(writer, sheet_name=tf_key, index=False)

    except Exception as e:

        logger.debug(f"Excel export notice: {e}")





def persist_magnet_zones(symbol: str, magnet_zones: List[dict]):

    if not magnet_zones:

        return

    df = enrich_liq_df(pd.DataFrame(magnet_zones))

    df["symbol"] = symbol

    atomic_parquet(df, MAGNET_ZONES_DIR / f"{symbol}_magnet_zones.parquet")





def persist_near_field(symbol: str, near_field: Optional[dict]):

    if not near_field:

        return

    df = enrich_liq_df(pd.DataFrame([near_field]))

    df["symbol"] = symbol

    atomic_parquet(df, NEAR_FIELD_DIR / f"{symbol}_near_field.parquet")





def persist_strength_by_distance(symbol: str, bands: List[dict]):

    if not bands:

        return

    df = enrich_liq_df(pd.DataFrame(bands))

    df["symbol"] = symbol

    atomic_parquet(df, STRENGTH_BANDS_DIR / f"{symbol}_strength_by_distance.parquet")





def persist_leverage_tiers(symbol: str, tiers: List[dict]):

    if not tiers:

        return

    df = enrich_liq_df(pd.DataFrame(tiers))

    df["symbol"] = symbol

    atomic_parquet(df, LEVERAGE_TIERS_DIR / f"{symbol}_leverage_tiers.parquet")





def persist_liquidation_snapshots(snapshots: List[dict]):

    if not snapshots:

        return

    df = enrich_liq_df(pd.DataFrame(snapshots))

    upsert_parquet(df, SNAPSHOTS_PARQUET, ["symbol", "time_ms"], sort_by="time_ms", limit=50000)





async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):

    return await cdp_result(ws, {

        "id": msg_id,

        "method": "Runtime.evaluate",

        "params": {"expression": expr, "returnByValue": True, "awaitPromise": await_promise}

    })





# ---------------------------------------------------------------------------

# Comprehensive DOM & Text Extraction Routine

# ---------------------------------------------------------------------------

EXTRACTION_JS = r"""(() => {

    const text = document.body.innerText;

    

    // 1. Last price

    let last_price = null;

    const p_match = text.match(/Last price\s*[\n\r]+\s*([\d,]+\.?\d*)/);

    if (p_match) {

        last_price = parseFloat(p_match[1].replace(/,/g, ''));

    }



    // 2. Lookback intervals

    let lookback_interval = '3D · 1m';

    if (text.includes('30D · 15m')) lookback_interval = '30D · 15m';

    if (text.includes('3D · 1m')) lookback_interval = '3D · 1m';



    // 3. CLLD

    let clld = null;

    const clld_match = text.match(/CLLD[^\n\r]*[\n\r]+\s*([+\-−–]?[\d,]+\.?\d*)\s*·\s*([^\n\r]+)/);

    if (clld_match) {

        const net_val = parseFloat(clld_match[1].replace(/,/g, '').replace(/−/g, '-').replace(/–/g, '-'));

        const interp = clld_match[2].trim();

        clld = {

            net_delta_usd: net_val,

            interpretation: interp,

            sentiment: interp.includes('Long') ? 'BULLISH' : (interp.includes('Short') ? 'BEARISH' : 'NEUTRAL')

        };

    }



    // 4. Strength by distance

    let strength_by_distance = [];

    const sbd_match = text.match(/Strength by distance[\s\S]*?(\d+-\d+%[\s\S]*?)(?=Leverage tiers|Magnet zones|Near-field|$)/);

    if (sbd_match) {

        const block = sbd_match[1];

        const band_lines = block.split(/[\n\r]+/).map(s => s.trim()).filter(Boolean);

        for (let i = 0; i < band_lines.length - 2; i++) {

            if (/^\d+-\d+%$/.test(band_lines[i])) {

                const band = band_lines[i];

                const s_val = parseFloat(band_lines[i+1].replace(/,/g, ''));

                const l_val = parseFloat(band_lines[i+2].replace(/,/g, ''));

                if (!isNaN(s_val) && !isNaN(l_val)) {

                    strength_by_distance.push({

                        band: band,

                        shorts_strength: s_val,

                        longs_strength: l_val

                    });

                }

            }

        }

    }



    // 5. Leverage tiers

    let leverage_tiers = [];

    const lev_match = text.match(/Leverage tiers[\s\S]*?(\d+x[\s\S]*?)(?=Magnet zones|Near-field|Key liquidation|$)/);

    if (lev_match) {

        const block = lev_match[1];

        const lev_lines = block.split(/[\n\r]+/).map(s => s.trim()).filter(Boolean);

        for (let i = 0; i < lev_lines.length - 2; i++) {

            if (/^\d+x$/.test(lev_lines[i])) {

                const tier = lev_lines[i];

                const s_val = parseFloat(lev_lines[i+1].replace(/,/g, ''));

                const l_val = parseFloat(lev_lines[i+2].replace(/,/g, ''));

                if (!isNaN(s_val) && !isNaN(l_val)) {

                    leverage_tiers.push({

                        tier: tier,

                        shorts_strength: s_val,

                        longs_strength: l_val

                    });

                }

            }

        }

    }



    // 6. Magnet zones (Top 3 short & long clusters)

    let magnet_zones = [];

    const short_block_m = text.match(/Short magnet zones[\s\S]*?(Rank[\s\S]*?)(?=Long magnet zones|Near-field|$)/);

    if (short_block_m) {

        const sm_lines = short_block_m[1].split(/[\n\r]+/).map(s => s.trim()).filter(Boolean);

        for (const line of sm_lines) {

            const sm = line.match(/^(\d+)\s+([\d,]+\.?\d*)\s*–\s*([\d,]+\.?\d*)\s+([+\-−–]?[\d\.]+%?)\s+([\d,]+)\s+([\d\.]+%?)/);

            if (sm) {

                const low = parseFloat(sm[2].replace(/,/g, ''));

                const high = parseFloat(sm[3].replace(/,/g, ''));

                magnet_zones.push({

                    side: 'SHORT',

                    rank: parseInt(sm[1]),

                    price_low: low,

                    price_high: high,

                    zone_price: Math.round(((low + high) / 2.0) * 100) / 100,

                    distance_pct: parseFloat(sm[4].replace(/[%+]/g, '').replace(/−/g, '-').replace(/–/g, '-')),

                    score: parseFloat(sm[5].replace(/,/g, '')),

                    width_pct: parseFloat(sm[6].replace(/%/g, ''))

                });

            }

        }

    }



    const long_block_m = text.match(/Long magnet zones[\s\S]*?(Rank[\s\S]*?)(?=Near-field balance|Key liquidation|$)/);

    if (long_block_m) {

        const lm_lines = long_block_m[1].split(/[\n\r]+/).map(s => s.trim()).filter(Boolean);

        for (const line of lm_lines) {

            const lm = line.match(/^(\d+)\s+([\d,]+\.?\d*)\s*–\s*([\d,]+\.?\d*)\s+([+\-−–]?[\d\.]+%?)\s+([\d,]+)\s+([\d\.]+%?)/);

            if (lm) {

                const low = parseFloat(lm[2].replace(/,/g, ''));

                const high = parseFloat(lm[3].replace(/,/g, ''));

                magnet_zones.push({

                    side: 'LONG',

                    rank: parseInt(lm[1]),

                    price_low: low,

                    price_high: high,

                    zone_price: Math.round(((low + high) / 2.0) * 100) / 100,

                    distance_pct: parseFloat(lm[4].replace(/[%+]/g, '').replace(/−/g, '-').replace(/–/g, '-')),

                    score: parseFloat(lm[5].replace(/,/g, '')),

                    width_pct: parseFloat(lm[6].replace(/%/g, ''))

                });

            }

        }

    }



    // 7. Near-field balance (±3% range)

    let near_field = null;

    const nf_match = text.match(/Near-field balance[\s\S]*?Longs\s*(\d+)%\s*·\s*([\d,]+)\s*[\n\r]+\s*Shorts\s*(\d+)%\s*·\s*([\d,]+)\s*[\n\r]+\s*([^\n\r]+)/);

    if (nf_match) {

        const l_cnt = parseInt(nf_match[2].replace(/,/g, ''));

        const s_cnt = parseInt(nf_match[4].replace(/,/g, ''));

        const interp = nf_match[5].trim();

        const trend_m = interp.match(/([+\-−–]?\d+\.?\d*)\s*pts\s*vs\s*24h\s*ago/);

        const trend_pts = trend_m ? parseFloat(trend_m[1].replace(/−/g, '-').replace(/–/g, '-')) : 0.0;

        near_field = {

            longs_pct: parseFloat(nf_match[1]),

            longs_count: l_cnt,

            shorts_pct: parseFloat(nf_match[3]),

            shorts_count: s_cnt,

            total_count: l_cnt + s_cnt,

            interpretation: interp,

            short_share_24h_pts: trend_pts

        };

    }



    return {

        last_price: last_price,

        lookback_interval: lookback_interval,

        clld: clld,

        strength_by_distance: strength_by_distance,

        leverage_tiers: leverage_tiers,

        magnet_zones: magnet_zones,

        near_field: near_field

    };

})()"""



HARVEST_ALL_COMBINATIONS_JS = r"""(async () => {

    const results = [];

    const timeframes = ['3D · 1m', '7D · 3m', '30D · 15m'];

    const pctTabs = ['±1%', '±2%', '±3%', '±4%', '±5%', '±6%', '±7%', '±8%'];



    for (const tf of timeframes) {

        // Switch Timeframe

        const allElements = Array.from(document.querySelectorAll('*'));

        const tfBtn = allElements.find(el => el.children.length === 0 && el.innerText && el.innerText.trim() === tf);

        if (tfBtn) {

            tfBtn.click();

            await new Promise(r => setTimeout(r, 450));

        }



        for (const pct of pctTabs) {

            const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));

            const btn = tabs.find(t => t.innerText && t.innerText.trim() === pct);

            if (btn) {

                btn.click();

                await new Promise(r => setTimeout(r, 150));



                const num_pct = parseInt(pct.replace(/[±%]/g, ''));

                const trs = Array.from(document.querySelectorAll('tbody tr'));

                let r_short = 1, r_long = 1;



                for (const r of trs) {

                    const cells = Array.from(r.querySelectorAll('td')).map(c => c.innerText.trim());

                    if (cells.length >= 5 && (cells[0].toLowerCase() === 'short' || cells[0].toLowerCase() === 'long')) {

                        const side_raw = cells[0].toLowerCase();

                        const is_short = side_raw === 'short';

                        const side = is_short ? 'Short' : 'Long';

                        const rank = is_short ? r_short++ : r_long++;

                        const p = parseFloat(cells[1].replace(/[$,]/g, ''));

                        const d_raw = cells[2].replace(/[−–—]/g, '-');

                        const lev_raw = cells[3];

                        const str = parseFloat(cells[4].replace(/,/g, ''));



                        // Signed percent: '1%' for Short, '-1%' for Long

                        const signed_pct_str = is_short ? `${num_pct}%` : `-${num_pct}%`;

                        const signed_pct_val = is_short ? num_pct : -num_pct;



                        if (!isNaN(p) && p > 0) {

                            results.push({

                                Asset: '%SYMBOL%',

                                Side: side,

                                Price: p,

                                Distance: d_raw,

                                Leverage: lev_raw,

                                Strength: str,

                                Percent: signed_pct_str,

                                Time: tf,

                                percent_val: signed_pct_val,

                                rank: rank

                            });

                        }

                    }

                }

            }

        }

    }

    return results;

})()"""





async def sweep_symbol_liquidation(ws, symbol: str) -> dict:

    target_url = f"https://www.coinglass.com/liquidation-levels?symbol={symbol}"

    logger.info(f"[{symbol}] Navigating to {target_url}...")

    await ws.send(json.dumps({"id": 1, "method": "Page.navigate", "params": {"url": target_url}}))

    await ws.recv()

    await asyncio.sleep(3.5)



    # Check and Auto-Recover from "Failed to load liquidation levels [Retry]" or Cloudflare blocks

    check_block_js = """(() => {

        const text = document.body.innerText || "";

        const hasError = text.includes("Failed to load liquidation levels");

        const retryBtn = document.querySelector(".ll-overlay button, .ll-stage.error button");

        const hasCloudflare = document.title.includes("Just a moment...") || !!document.querySelector("#challenge-running");

        return {

            is_blocked: hasError || hasCloudflare,

            has_retry: !!retryBtn,

            reason: hasCloudflare ? "CLOUDFLARE_CHALLENGE" : (hasError ? "FAILED_TO_LOAD" : "OK")

        };

    })()"""

    block_status = await cdp_eval(ws, check_block_js, msg_id=10) or {}

    if block_status.get("is_blocked"):

        logger.warning(f"[{symbol}] Detected CoinGlass block signature: {block_status.get('reason')}")

        if block_status.get("has_retry"):

            logger.info(f"[{symbol}] Clicking in-page 'Retry' button...")

            await cdp_eval(ws, 'const b = document.querySelector(".ll-overlay button, .ll-stage.error button"); if (b) b.click();', msg_id=11)

            await asyncio.sleep(2.5)

        

        # If still blocked post-click, trigger True IP Rotation & Tab Reload

        recheck = await cdp_eval(ws, check_block_js, msg_id=12) or {}

        if recheck.get("is_blocked"):

            logger.warning(f"[{symbol}] Page still blocked. Triggering True WARP IP Rotation...")

            new_ip = rotate_warp_ip("LiquidationScraper", reason="FAILED_TO_LOAD")

            logger.info(f"[{symbol}] Rotated IP to {new_ip}. Reloading liquidation tab...")

            await ws.send(json.dumps({"id": 13, "method": "Page.reload"}))

            await ws.recv()

            await asyncio.sleep(4.5)



    # 1. Harvest general page structures (last_price, clld, magnet_zones, near_field, etc.)

    state = await cdp_eval(ws, EXTRACTION_JS, msg_id=2, await_promise=False)

    if not state or not isinstance(state, dict):

        return {}



    # 2. Harvest all 24 Time x Percent combinations for Key Liquidation Levels

    harvest_code = HARVEST_ALL_COMBINATIONS_JS.replace('%SYMBOL%', symbol)

    key_levels = await cdp_eval(ws, harvest_code, msg_id=3, await_promise=True) or []



    cur_price = state.get("last_price")

    clld = state.get("clld") or {}

    near_field = state.get("near_field") or {}

    magnet_zones = state.get("magnet_zones") or []

    strength_bands = state.get("strength_by_distance") or []

    leverage_tiers = state.get("leverage_tiers") or []



    # Persist all 5 individual structures

    persist_key_levels(symbol, key_levels)

    persist_magnet_zones(symbol, magnet_zones)

    persist_near_field(symbol, near_field)

    persist_strength_by_distance(symbol, strength_bands)

    persist_leverage_tiers(symbol, leverage_tiers)



    # Return summary snapshot

    return {

        "symbol": symbol,

        "last_price": cur_price,

        "clld_net_usd": clld.get("net_delta_usd"),

        "clld_sentiment": clld.get("sentiment"),

        "near_field_shorts_pct": near_field.get("shorts_pct"),

        "near_field_longs_pct": near_field.get("longs_pct"),

        "short_share_24h_pts": near_field.get("short_share_24h_pts"),

        "total_magnet_zones": len(magnet_zones),

        "total_key_levels": len(key_levels)

    }





async def run_sweep(symbols: Optional[List[str]] = None) -> bool:

    target_symbols = symbols or CORE_LIQ_SYMBOLS

    acquire_singleton("coinglass_liquidation_levels", DB_DIR / ".locks")



    with BrowserLock("LiquidationScraper", timeout_secs=20.0) as lock:

        if not lock.acquired:

            logger.warning("Browser lock contention for Liquidation sweep. Yielding.")

            return False



        tab_id, ws_url = open_liquidation_tab(target_symbols[0])

        if not tab_id or not ws_url:

            logger.error("Could not open Liquidation tab via CDP.")

            return False



        try:

            async with websockets.connect(ws_url, open_timeout=8, close_timeout=3, max_size=50_000_000, ping_interval=None) as ws:

                snapshots = []

                for sym in target_symbols:

                    try:

                        snap = await sweep_symbol_liquidation(ws, sym)

                        if snap and snap.get("last_price"):

                            snapshots.append(snap)

                    except Exception as e:

                        logger.error(f"[{sym}] Error harvesting liquidation levels: {e}")



                if snapshots:

                    persist_liquidation_snapshots(snapshots)

                    logger.info(f"Saved {len(snapshots)} liquidation market snapshots.")

                return True

        except Exception as e:

            logger.error(f"WebSocket execution error: {e}")

            return False

        finally:

            close_tab(tab_id)





def get_seconds_until_next_liq_minute() -> float:

    now = datetime.now()

    for m in sorted(LIQUIDATION_MINUTES):

        candidate = now.replace(minute=m, second=5, microsecond=0)

        if candidate > now:

            return (candidate - now).total_seconds()

    next_hour = now + timedelta(hours=1)

    target = next_hour.replace(minute=sorted(LIQUIDATION_MINUTES)[0], second=5, microsecond=0)

    return max(1.0, (target - now).total_seconds())





async def run_daemon():

    acquire_singleton("coinglass_liquidation_levels", DB_DIR / ".locks")

    logger.info(f"CoinGlass Liquidation Levels Daemon active. Scheduled for minutes {LIQUIDATION_MINUTES}.")



    while True:

        wait_s = get_seconds_until_next_liq_minute()

        logger.info(f"Sleeping {wait_s:.1f}s until next liquidation sweep window...")

        await asyncio.sleep(wait_s)

        try:

            success = await run_sweep()
            if success is False:
                logger.info("Liquidation sweep yielded on lock contention; retrying in 15s...")
                await asyncio.sleep(15.0)
                await run_sweep()

        except Exception as e:

            logger.error(f"Scheduled sweep error: {e}")





def main():

    parser = argparse.ArgumentParser(description="CoinGlass Liquidation Levels Scraper")

    parser.add_argument("--daemon", action="store_true", help="Run daemon on :10, :25, :55")

    parser.add_argument("--once", action="store_true", help="Run single sweep and exit")

    parser.add_argument("--symbols", nargs="+", help="Specific symbols to sweep (e.g. BTC ETH SOL)")

    args = parser.parse_args()



    if args.daemon:

        asyncio.run(run_daemon())

    else:

        asyncio.run(run_sweep(args.symbols))





if __name__ == "__main__":

    main()

