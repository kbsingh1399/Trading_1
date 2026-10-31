"""
CoinGlass Master Data Collector (Footprint Signals, 15m Candles, & Liquidation Data)
------------------------------------------------------------------------------------
Automates parallel, multi-stream data collection across the mutual intersection of
assets supported by both Blueberry Markets MT5 and CoinGlass:

Data Streams Collected:
1. Footprint Order-Flow Signals & 15m Candles (Continuous Pipe):
   - Real-time trapped longs/shorts, absorption, delta divergence, exhaustion.
   - Stored in: `Database/coinglass_aggregated_futures_signals.parquet`
   - 15m Order-flow candles stored in: `Database/Candle_Data/<SYMBOL>.parquet`
   - Continuous loop with automated Cloudflare WARP IP rotation.
2. Liquidation Intelligence (Scheduled Pipe at 00th & 30th minute):
   - Triggered at local wall-clock :00:05 and :30:05.
   - Dynamic tab lifecycle: Opens tab, sweeps all 44 mutual assets, dumps to Parquet,
     and IMMEDIATELY CLOSES the tab (`page.close()`) to keep memory and CPU lean.
   - Stored in: `Database/Liquidation_Data/Levels/<SYMBOL>_levels.parquet`,
                `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet`,
                `Database/Liquidation_Data/liquidation_snapshots.parquet`.
3. Concurrency & Network Safety:
   - Coordinated via `asyncio.Lock` (`warp_rotation_lock`):
     WARP IP rotation only executes when the Liquidation sweep is idle, preventing network drops.
"""

import sys
import os
import time
import json
import logging
import argparse
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Configuration & Registry Loading
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"
LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"

OUTPUT_SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"

WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")
CDP_URL = "http://127.0.0.1:9222"
CHUNK_SIZE = 10           # Footprint assets per in-page batch chunk
FOOTPRINT_REST_SECONDS = 60  # Rest between footprint passes

REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DataCollector")


def load_capability_registry() -> tuple[dict, list, list, list, dict, dict]:
    """Load the verified mutual asset capability registry."""
    if not REGISTRY_FILE.exists():
        logger.error(f"Capability registry missing at: {REGISTRY_FILE}")
        return {}, [], [], [], {}, {}

    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)

    mt5_map = {}
    fp_symbols = []
    liq_level_symbols = []
    liq_snap_symbols = []
    liq_exchanges_map = {}
    liq_contracts_map = {}

    for category, assets in reg.items():
        for sym, meta in assets.items():
            mt5_map[sym] = meta["mt5"]
            
            # Build target exchanges
            exs = []
            if meta.get("primary_exchange"):
                exs.append(meta["primary_exchange"])
            if meta.get("alt_exchange"):
                exs.append(meta["alt_exchange"])
            if not exs:
                exs = meta.get("liq_exchanges", ["Binance"])
            liq_exchanges_map[sym] = exs

            # Build contract map per exchange
            c_map = {}
            if meta.get("primary_exchange") and meta.get("liq_contract"):
                c_map[meta["primary_exchange"]] = meta["liq_contract"]
            if meta.get("alt_exchange") and meta.get("alt_contract"):
                c_map[meta["alt_exchange"]] = meta["alt_contract"]
            liq_contracts_map[sym] = c_map

            if meta.get("footprint", False):
                fp_symbols.append(sym)
            if meta.get("liq_levels", False):
                liq_level_symbols.append(sym)
            if meta.get("liq_snapshots", False):
                liq_snap_symbols.append(sym)

    return mt5_map, fp_symbols, liq_level_symbols, liq_snap_symbols, liq_exchanges_map, liq_contracts_map


MT5_ASSET_MAP, FP_TARGET_SYMBOLS, LIQ_LEVEL_SYMBOLS, LIQ_SNAP_SYMBOLS, LIQ_EXCHANGES_MAP, LIQ_CONTRACTS_MAP = load_capability_registry()


def get_current_ip() -> str:
    """Fetch external public IP address."""
    try:
        res = requests.get("https://api.ipify.org", timeout=5)
        if res.status_code == 200:
            return res.text.strip()
    except Exception as e:
        logger.warning(f"Failed to fetch public IP: {e}")
    return "Unknown"


def rotate_cloudflare_ip():
    """Disconnect and reconnect Cloudflare WARP to rotate IP and re-key tunnel."""
    if not WARP_CLI.exists():
        logger.error(f"WARP CLI binary not found at: {WARP_CLI}")
        return

    old_ip = get_current_ip()
    logger.info(f"[Cloudflare WARP] Current outbound IP: {old_ip}. Initiating IP rotation...")

    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(2)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(4)

        new_ip = get_current_ip()
        logger.info(f"[Cloudflare WARP] Rotation complete. New outbound IP: {new_ip} (Old: {old_ip})")
    except Exception as e:
        logger.error(f"[Cloudflare WARP] Error during IP rotation: {e}")


def get_seconds_until_next_sweep() -> float:
    """Calculate exact seconds until next :00:05 or :30:05 local wall-clock time."""
    now = datetime.now()
    if now.minute < 30 or (now.minute == 30 and now.second < 5):
        target = now.replace(minute=30, second=5, microsecond=0)
    else:
        next_hour = now + timedelta(hours=1)
        target = next_hour.replace(minute=0, second=5, microsecond=0)
    diff = (target - now).total_seconds()
    return max(1.0, diff)


# ---------------------------------------------------------------------------
# Footprint Page Helpers
# ---------------------------------------------------------------------------
async def get_coinglass_tab(browser_context):
    """Locate the existing CoinGlass footprint tab, or open a new one if missing."""
    for page in browser_context.pages:
        if "coinglass.com/footprint" in page.url:
            return page
    
    logger.info("CoinGlass footprint tab not found in active tabs. Navigating new page...")
    page = await browser_context.new_page()
    await page.goto("https://www.coinglass.com/footprint?symbol=BTC", wait_until="networkidle")
    await asyncio.sleep(3)
    return page


async def ensure_terminal_ready(page):
    """Ensure terminal controller is attached to window.__cg_term."""
    init_script = """
    () => {
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
            } catch (e) {}
            findTerm(fiber.child, depth + 1);
            findTerm(fiber.sibling, depth);
        }
        findTerm(rootFiber);
        if (term) {
            window.__cg_term = term;
            return true;
        }
        return false;
    }
    """
    ready = await page.evaluate(init_script)
    if not ready:
        await asyncio.sleep(2)
        ready = await page.evaluate(init_script)
    return ready


def verify_candle_sanity(df: pd.DataFrame, symbol: str) -> bool:
    """Rigorous mathematical and data sanity verification on candle records."""
    if df.empty:
        logger.error(f"[{symbol}] Sanity check failed: DataFrame is empty!")
        return False

    if not df["open_time_ms"].is_monotonic_increasing:
        logger.error(f"[{symbol}] Sanity check failed: Timestamps are not strictly monotonic increasing!")
        return False

    ohlc_valid = (
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["high"] >= df["low"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    ).all()
    if not ohlc_valid:
        logger.error(f"[{symbol}] Sanity check failed: Invalid OHLC price relationships detected!")
        return False

    vol_valid = (abs((df["buy_volume"] + df["sell_volume"]) - df["total_volume"]) < 1e-4).all()
    if not vol_valid:
        logger.error(f"[{symbol}] Sanity check failed: Volume conservation violation (buy + sell != total)!")
        return False

    delta_valid = (abs((df["buy_volume"] - df["sell_volume"]) - df["delta"]) < 1e-4).all()
    if not delta_valid:
        logger.error(f"[{symbol}] Sanity check failed: Delta calculation mismatch (buy - sell != delta)!")
        return False

    non_neg = ((df["total_volume"] >= 0) & (df["total_trades"] >= 0) & (df["open"] > 0)).all()
    if not non_neg:
        logger.error(f"[{symbol}] Sanity check failed: Negative volume, negative trades, or non-positive price!")
        return False

    return True


def upsert_candle_parquet(symbol: str, mt5_symbol: str, new_candles: list):
    """Upsert 15m candle records into asset-specific Parquet with deduplication."""
    if not new_candles:
        return

    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = CANDLE_DIR / f"{symbol}.parquet"

    new_df = pd.DataFrame(new_candles)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["open_time_ms"], keep="last")
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to read existing parquet for merge ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df = combined_df.sort_values(by="open_time_ms", ascending=True).reset_index(drop=True)
    combined_df["cumulative_delta"] = combined_df["delta"].cumsum()
    combined_df["dumped_at_utc"] = datetime.now(timezone.utc).isoformat()

    lead_cols = ["symbol", "mt5_symbol", "timeframe", "open_time_ms", "close_time_ms", "open_time_utc", "close_time_utc", "open", "high", "low", "close"]
    other_cols = [c for c in combined_df.columns if c not in lead_cols]
    combined_df = combined_df[lead_cols + other_cols]

    if verify_candle_sanity(combined_df, symbol):
        combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} candles (range: {combined_df['open_time_utc'].iloc[0][:16]} -> {combined_df['open_time_utc'].iloc[-1][:16]})")
    else:
        logger.error(f"[{symbol}] Aborting parquet write due to sanity check failure!")


def dump_signals_parquet(signals: list):
    """Persist aggregated signals across all mutual assets."""
    if not signals:
        logger.warning("Empty signal set received. Skipping signals parquet write.")
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(signals)
    df["dumped_at_utc"] = datetime.now(timezone.utc).isoformat()

    cols = ["symbol", "mt5_symbol"] + [c for c in df.columns if c not in ["symbol", "mt5_symbol"]]
    df = df[cols]

    if "time_ms" in df.columns:
        df = df.sort_values(by=["symbol", "time_ms"], ascending=[True, True]).reset_index(drop=True)

    df.to_parquet(OUTPUT_SIGNALS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Signals Parquet updated: {OUTPUT_SIGNALS_PARQUET.name} ({len(df)} rows across {df['symbol'].nunique()} assets)")

    # Reactive Wake-Up Engine: Evaluate new CoinGlass signals immediately against MT5 live quotes
    try:
        from Terminal.reactive_trade_engine import scan_and_react_to_signals
        scan_and_react_to_signals()
    except Exception as e:
        logger.warning(f"[Wakeup-Engine] Error during reactive trade formulation: {e}")


async def extract_footprint_chunk(page, symbols_chunk: list) -> dict:
    """Extract Footprint signals and 15m order flow candles for a chunk of symbols."""
    combined_extract_js = """
    async ({ symbolsChunk, mt5Map }) => {
        const term = window.__cg_term;
        const result = { signals: [], candles: {} };

        for (const sym of symbolsChunk) {
            await new Promise((resolve) => {
                let unsub = () => {};
                let loadedOlder = false;

                const timeout = setTimeout(() => {
                    unsub();
                    resolve();
                }, 4000);

                unsub = term.subscribe(async (event) => {
                    const sigSets = term.sigSets ? term.sigSets() : null;
                    if (sigSets && sigSets.length === 3) {
                        if (!loadedOlder) {
                            loadedOlder = true;
                            try {
                                await term.maybeOlder();
                            } catch (e) {}
                        }
                        clearTimeout(timeout);
                        unsub();

                            // 1. Extract Signals
                            for (const set of sigSets) {
                                const X = set.X;
                                for (const s of set.sigs) {
                                    const text = term.sigText(s, X);
                                    result.signals.push({
                                        symbol: sym,
                                        mt5_symbol: mt5Map[sym] || '',
                                        market: 'agg-perp',
                                        bar_type: set.bt,
                                        bar_label: set.label,
                                        time_ms: X && X.T ? X.T[s.i] : null,
                                        datetime_utc: X && X.T ? new Date(X.T[s.i]).toISOString() : null,
                                        sig_code: s.t,
                                        sig_name: term.sigName(s),
                                        side: s.side === 'buy' ? 'bullish' : 'bearish',
                                        price: s.p,
                                        title: text[0] || '',
                                        description: text[1] || ''
                                    });
                                }
                            }

                            // 2. Extract 15m Order-Flow Candle Data
                            const timeSet = sigSets.find(s => s.bt === 'time');
                            if (timeSet && timeSet.X && timeSet.X.T) {
                                const X = timeSet.X;
                                const n = X.T.length;
                                const bars = [];
                                let cumDelta = 0;

                                for (let i = 0; i < n; i++) {
                                    const t = X.T[i];
                                    const te = X.TE ? X.TE[i] : (t + 15 * 60 * 1000);
                                    const o = X.O[i];
                                    const h = X.H[i];
                                    const l = X.L[i];
                                    const c = X.C[i];

                                    const oio = X.OIo ? X.OIo[i] : null;
                                    const oic = X.OIc ? X.OIc[i] : null;
                                    const oi_change = (oio != null && oic != null) ? (oic - oio) : null;

                                    const F = X.F ? X.F[i] : null;
                                    let buyVol = 0, sellVol = 0, buyTrades = 0, sellTrades = 0;
                                    let maxVol = 0, pocIdx = -1;
                                    let nB = 0, nS = 0;

                                    if (F && F.b && F.s) {
                                        const numLevels = F.b.length;
                                        for (let j = 0; j < numLevels; j++) {
                                            const b = F.b[j] || 0;
                                            const s = F.s[j] || 0;
                                            const bc = (F.bc && F.bc[j]) || 0;
                                            const sc = (F.sc && F.sc[j]) || 0;

                                            buyVol += b;
                                            sellVol += s;
                                            buyTrades += bc;
                                            sellTrades += sc;

                                            const lvlVol = b + s;
                                            if (lvlVol > maxVol) {
                                                maxVol = lvlVol;
                                                pocIdx = j;
                                            }

                                            if (j > 0 && s > 0 && b >= 3 * (F.s[j - 1] || 0)) nB++;
                                            if (j < numLevels - 1 && b > 0 && s >= 3 * (F.b[j + 1] || 0)) nS++;
                                        }
                                    }

                                    const totVol = buyVol + sellVol;
                                    const delta = buyVol - sellVol;
                                    cumDelta += delta;
                                    const totTrades = buyTrades + sellTrades;

                                    let pocPrice = null, pocPriceHigh = null;
                                    let pocB = null, pocS = null, pocVol = null, pocDelta = null;
                                    let pocBuyTrades = null, pocSellTrades = null, pocTotalTrades = null;

                                    if (F && pocIdx >= 0) {
                                        pocPrice = F.lo + pocIdx * F.tick;
                                        pocPriceHigh = F.lo + (pocIdx + 1) * F.tick;
                                        pocB = F.b[pocIdx] || 0;
                                        pocS = F.s[pocIdx] || 0;
                                        pocVol = pocB + pocS;
                                        pocDelta = pocB - pocS;
                                        pocBuyTrades = (F.bc && F.bc[pocIdx]) || 0;
                                        pocSellTrades = (F.sc && F.sc[pocIdx]) || 0;
                                        pocTotalTrades = pocBuyTrades + pocSellTrades;
                                    }

                                    bars.push({
                                        symbol: sym,
                                        mt5_symbol: mt5Map[sym] || '',
                                        timeframe: '15m',
                                        open_time_ms: t,
                                        close_time_ms: te,
                                        open_time_utc: new Date(t).toISOString(),
                                        close_time_utc: new Date(te).toISOString(),
                                        open: o,
                                        high: h,
                                        low: l,
                                        close: c,
                                        total_volume: totVol,
                                        buy_volume: buyVol,
                                        sell_volume: sellVol,
                                        delta: delta,
                                        delta_pct: totVol > 0 ? (delta / totVol) * 100 : 0,
                                        cumulative_delta: cumDelta,
                                        total_trades: totTrades,
                                        buy_trades: buyTrades,
                                        sell_trades: sellTrades,
                                        avg_trade_size: totTrades > 0 ? (totVol / totTrades) : 0,
                                        buy_sell_ratio: sellVol > 0 ? (buyVol / sellVol) : null,
                                        poc_price: pocPrice,
                                        poc_price_high: pocPriceHigh,
                                        poc_volume: pocVol,
                                        poc_buy_volume: pocB,
                                        poc_sell_volume: pocS,
                                        poc_delta: pocDelta,
                                        poc_pct_of_bar: totVol > 0 ? (pocVol / totVol) * 100 : 0,
                                        poc_total_trades: pocTotalTrades,
                                        open_interest_open: oio,
                                        open_interest_close: oic,
                                        oi_change: oi_change,
                                        buy_imbalances_count: nB,
                                        sell_imbalances_count: nS
                                    });
                                }
                                result.candles[sym] = bars;
                            }

                            resolve();
                        }
                });

                term.pickPair(sym, 'agg-perp');
            });
        }
        return result;
    }
    """
    return await page.evaluate(combined_extract_js, {"symbolsChunk": symbols_chunk, "mt5Map": MT5_ASSET_MAP})


# ---------------------------------------------------------------------------
# Liquidation Intelligence Helpers (Multi-Exchange & Multi-TF Architecture)
# ---------------------------------------------------------------------------
async def discover_perpetual_exchanges(page, symbol: str = None) -> list[dict]:
    """Discover all available perpetual contracts and exchanges for the current asset."""
    btn = page.locator("button.ll-symBtn").first
    if await btn.count() == 0:
        return []

    menu = page.locator(".ll-menu").first
    if await menu.count() == 0 or not await menu.is_visible():
        await btn.click()
        await asyncio.sleep(0.5)

    if symbol:
        inp = page.locator("input[placeholder*='Search exchange or pair']").first
        if await inp.count() > 0:
            await inp.fill(symbol)
            await asyncio.sleep(0.4)

    exchanges = await page.evaluate('''() => {
        const menu = document.querySelector('.ll-menu');
        if (!menu) return [];
        const list = [];
        let curEx = '';
        Array.from(menu.children).forEach(el => {
            if (el.classList.contains('grp')) {
                curEx = el.innerText.trim();
            } else if (el.tagName === 'BUTTON') {
                const txt = el.innerText.trim();
                const lines = txt.split('\\n').map(s => s.trim()).filter(Boolean);
                const con = lines[0] || '';
                const typ = lines[1] || '';
                if (typ.toLowerCase().includes('perpetual')) {
                    list.push({ exchange: curEx, contract: con, contract_type: typ });
                }
            }
        });
        return list;
    }''')

    if await menu.is_visible():
        await btn.click()
        await asyncio.sleep(0.3)

    return exchanges


async def switch_liquidation_exchange(page, target_exchange: str, target_contract: str = None, symbol: str = None) -> bool:
    """Natively switches to target exchange and contract via CoinGlass .ll-menu."""
    btn = page.locator("button.ll-symBtn").first
    if await btn.count() == 0:
        return False

    cur_text = (await btn.inner_text()).upper()
    if target_exchange.upper() in cur_text and (not target_contract or target_contract.upper() in cur_text):
        return True

    menu = page.locator(".ll-menu").first
    if await menu.count() == 0 or not await menu.is_visible():
        await btn.click()
        await asyncio.sleep(0.5)

    if symbol:
        inp = page.locator("input[placeholder*='Search exchange or pair']").first
        if await inp.count() > 0:
            await inp.fill(symbol)
            await asyncio.sleep(0.4)

    switched = await page.evaluate('''({ targetEx, targetCon }) => {
        const menu = document.querySelector('.ll-menu');
        if (!menu) return { success: false, reason: "no menu" };

        let curEx = '';
        let matchedBtn = null;
        for (const el of Array.from(menu.children)) {
            if (el.classList.contains('grp')) {
                curEx = el.innerText.trim().toUpperCase();
            } else if (el.tagName === 'BUTTON') {
                if (curEx === targetEx.toUpperCase()) {
                    const txt = el.innerText.toUpperCase();
                    if (!targetCon || txt.includes(targetCon.toUpperCase())) {
                        matchedBtn = el;
                        break;
                    }
                }
            }
        }

        if (matchedBtn) {
            matchedBtn.click();
            return { success: true };
        }
        return { success: false, reason: "not found" };
    }''', {"targetEx": target_exchange, "targetCon": target_contract})

    if switched.get("success"):
        await asyncio.sleep(2.5)
        return True
    return False


async def extract_liquidation_data_point(
    page,
    symbol: str,
    mt5_symbol: str,
    exchange: str,
    contract: str,
    tf_label: str,
    range_filters: list = None
) -> dict:
    """
    Extracts all 4 core intelligence tables and macro snapshot for a given exchange and lookback timeframe:
    1. Key liquidation levels across range filters ('±1%' to '±8%')
    2. Top 3 Shorts & Longs Magnet Zones
    3. Leverage Tiers strength breakdown
    4. Near-field balance (±3% longs vs shorts and 24h shift)
    5. Macro snapshot metrics
    """
    if range_filters is None:
        range_filters = ['±1%', '±2%', '±3%', '±4%', '±5%', '±6%', '±7%', '±8%']

    state = await page.evaluate('''() => {
        const bodyText = document.body.innerText;
        const allDivs = Array.from(document.querySelectorAll('div'));

        // Price
        let curPrice = null;
        const priceMatch = bodyText.match(/Last price\\s*([\\d,\\.]+)/i);
        if (priceMatch) curPrice = parseFloat(priceMatch[1].replace(/,/g, ''));

        // 1. Magnet Zones (Top 3 Shorts + Top 3 Longs)
        const magnetZones = [];
        const mzDiv = allDivs.find(d => (d.innerText || '').startsWith('Magnet zones\\nWhere leveraged positions'));
        if (mzDiv) {
            const lines = mzDiv.innerText.split('\\n').map(x => x.trim()).filter(Boolean);
            const shortsIdx = lines.indexOf('Shorts get liquidated');
            const longsIdx = lines.indexOf('Longs get liquidated');

            function parseBlock(startIdx, endIdx, side) {
                let i = startIdx + 1;
                while (i + 4 <= endIdx) {
                    const rank = parseInt(lines[i]);
                    const zoneStr = lines[i+1];
                    const distStr = lines[i+2];
                    const scoreStr = lines[i+3];
                    const widthStr = lines[i+4];

                    if (!isNaN(rank) && zoneStr && (zoneStr.includes('\u2013') || zoneStr.includes('-')) && distStr && distStr.includes('%')) {
                        const delim = zoneStr.includes('\u2013') ? '\u2013' : '-';
                        const parts = zoneStr.split(delim).map(s => parseFloat(s.replace(/,/g, '').trim()));
                        const distPct = parseFloat(distStr.replace('%', '').trim());
                        const score = parseFloat(scoreStr.replace(/,/g, '').trim());
                        const widthPct = parseFloat(widthStr.replace(/width/i, '').replace('%', '').trim());

                        magnetZones.push({
                            side: side,
                            rank: rank,
                            zone_label: zoneStr.replace(/\\s+/g, ' '),
                            price_low: Math.min(...parts),
                            price_high: Math.max(...parts),
                            distance_pct: distPct,
                            strength: score,
                            width_pct: widthPct
                        });
                        i += 5;
                    } else {
                        i += 1;
                    }
                }
            }

            if (shortsIdx >= 0 && longsIdx > shortsIdx) {
                parseBlock(shortsIdx, longsIdx, 'short');
                parseBlock(longsIdx, lines.length, 'long');
            }
        }

        // 2. Leverage Tiers
        const leverageTiers = [];
        let levTotals = {};
        const levDiv = allDivs.find(d => (d.innerText || '').startsWith('By leverage tier\\nOpen-level strength'));
        if (levDiv) {
            const text = levDiv.innerText;
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const mTot = text.match(/Shorts\\s*([\\d,]+)[\\s\\S]*?Longs\\s*([\\d,]+)[\\s\\S]*?Total\\s*([\\d,]+)/i);
            if (mTot) {
                levTotals = {
                    total_shorts: parseInt(mTot[1].replace(/,/g, '')),
                    total_longs: parseInt(mTot[2].replace(/,/g, '')),
                    total_aggregate: parseInt(mTot[3].replace(/,/g, ''))
                };
            }

            ['High leverage', 'Mid leverage', 'Low leverage'].forEach(tier => {
                const idx = lines.indexOf(tier);
                if (idx >= 0) {
                    const slice = lines.slice(idx, idx + 6).join(' ');
                    const match = slice.match(new RegExp(tier + '\\\\s*([\\\\d,]+)\\\\s*[·•]\\\\s*([\\\\d,]+)', 'i'));
                    if (match) {
                        leverageTiers.push({
                            tier: tier.replace(' leverage', '').toLowerCase(),
                            short_strength: parseInt(match[1].replace(/,/g, '')),
                            long_strength: parseInt(match[2].replace(/,/g, '')),
                            ...levTotals
                        });
                    }
                }
            });
        }

        // 3. Near-field balance
        let nearField = null;
        const nfbDiv = allDivs.find(d => (d.innerText || '').startsWith('Near-field balance\\nOpen strength'));
        if (nfbDiv) {
            const text = nfbDiv.innerText;
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const mLong = text.match(/Longs\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const mShort = text.match(/Shorts\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const descLine = lines.find(l => l.includes('Balanced:') || l.includes('Dominant') || l.includes('short share') || l.includes('vs 24h ago'));
            const mShift = text.match(/short share\\s*([+\\-\\d\\.]+)\\s*pts/i);

            nearField = {
                longs_pct: mLong ? parseFloat(mLong[1]) : null,
                longs_count: (mLong && mLong[2]) ? parseInt(mLong[2].replace(/,/g, '')) : null,
                shorts_pct: mShort ? parseFloat(mShort[1]) : null,
                shorts_count: (mShort && mShort[2]) ? parseInt(mShort[2].replace(/,/g, '')) : null,
                shift_24h_pts: mShift ? parseFloat(mShift[1]) : null,
                status_desc: descLine || null
            };
        }

        // 4. CLLD & Snapshot metrics
        let clldVal = null, clldSentiment = null, clldTurnTime = null, clld1hChange = null;
        const clldMatch = bodyText.match(/CLLD\\s*·\\s*Cumulative Liquidation Levels Delta\\s*([−\\-\\+\\d,]+)\\s*(?:[▲▼]\\s*)?([A-Za-z]+ turn)\\s*([\\d\\-\\s:]+)\\s*More [^·]+·\\s*1h change\\s*([+\\-\\d,]+)/i);
        if (clldMatch) {
            clldVal = parseFloat(clldMatch[1].replace('−', '-').replace(/,/g, ''));
            clldSentiment = clldMatch[2].trim();
            clldTurnTime = clldMatch[3].trim();
            clld1hChange = parseFloat(clldMatch[4].replace(/,/g, ''));
        }

        // Nearest magnet zones
        let shortMagZone = null, shortMagDist = null, shortMagStr = null;
        const shortMagMatch = bodyText.match(/Nearest short magnet zone\\s*([\\d,\\.]+\\s*–\\s*[\\d,\\.]+)\\s*([+\\-\\d\\.]+%)\\s*·\\s*Strength\\s*(\\d+)/i);
        if (shortMagMatch) {
            shortMagZone = shortMagMatch[1].replace(/\\s+/g, ' ');
            shortMagDist = parseFloat(shortMagMatch[2].replace('%', ''));
            shortMagStr = parseFloat(shortMagMatch[3]);
        }

        let longMagZone = null, longMagDist = null, longMagStr = null;
        const longMagMatch = bodyText.match(/Nearest long magnet zone\\s*([\\d,\\.]+\\s*–\\s*[\\d,\\.]+)\\s*([+\\-\\d\\.]+%)\\s*·\\s*Strength\\s*(\\d+)/i);
        if (longMagMatch) {
            longMagZone = longMagMatch[1].replace(/\\s+/g, ' ');
            longMagDist = parseFloat(longMagMatch[2].replace('%', ''));
            longMagStr = parseFloat(longMagMatch[3]);
        }

        return {
            curPrice,
            magnetZones,
            leverageTiers,
            nearField,
            clldVal,
            clldSentiment,
            clldTurnTime,
            clld1hChange,
            shortMagZone,
            shortMagDist,
            shortMagStr,
            longMagZone,
            longMagDist,
            longMagStr
        };
    }''')

    cur_price = state.get("curPrice")

    # Extract Key liquidation levels across range filters
    key_levels = []
    for rf in range_filters:
        await page.evaluate(f'''() => {{
            const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
            const target = tabs.find(t => (t.innerText || '').trim() === '{rf}');
            if (target) target.click();
        }}''')
        await asyncio.sleep(0.25)

        rows = await page.evaluate(f'''() => {{
            const table = document.querySelector('table');
            if (!table) return [];
            const currentSideRank = {{ 'short': 1, 'long': 1 }};
            const trs = Array.from(table.querySelectorAll('tbody tr'));
            const res = [];
            for (const r of trs) {{
                const cells = r.querySelectorAll('td');
                if (cells.length >= 5) {{
                    const parts = Array.from(cells).map(c => c.innerText.trim());
                    const s = parts[0].toLowerCase();
                    res.push({{
                        side: s,
                        rank: currentSideRank[s]++,
                        price: parseFloat(parts[1].replace(/,/g, '')),
                        distance_pct: parseFloat(parts[2].replace('%', '').replace('+', '')),
                        leverage_tier: parts[3].toLowerCase().replace(' leverage', ''),
                        strength: parseFloat(parts[4]),
                        range_filter: '{rf}'
                    }});
                }}
            }}
            return res;
        }}''')

        for r in rows:
            r["symbol"] = symbol
            r["mt5_symbol"] = mt5_symbol
            r["exchange"] = exchange
            r["contract"] = contract
            r["lookback_tf"] = tf_label
            r["last_price"] = cur_price
            key_levels.append(r)

    # Tag magnet zones
    magnet_zones = []
    for mz in state.get("magnetZones", []):
        mz["symbol"] = symbol
        mz["mt5_symbol"] = mt5_symbol
        mz["exchange"] = exchange
        mz["contract"] = contract
        mz["lookback_tf"] = tf_label
        magnet_zones.append(mz)

    # Tag leverage tiers
    leverage_tiers = []
    for lt in state.get("leverageTiers", []):
        lt["symbol"] = symbol
        lt["mt5_symbol"] = mt5_symbol
        lt["exchange"] = exchange
        lt["contract"] = contract
        lt["lookback_tf"] = tf_label
        leverage_tiers.append(lt)

    # Tag near field
    near_field = None
    if state.get("nearField"):
        near_field = state["nearField"]
        near_field["symbol"] = symbol
        near_field["mt5_symbol"] = mt5_symbol
        near_field["exchange"] = exchange
        near_field["contract"] = contract
        near_field["lookback_tf"] = tf_label

    # Consolidated Snapshot
    snapshot = {
        "symbol": symbol,
        "mt5_symbol": mt5_symbol,
        "exchange": exchange,
        "pair": contract,
        "lookback_tf": tf_label,
        "last_price": cur_price,
        "clld_current": state.get("clldVal"),
        "clld_sentiment": state.get("clldSentiment"),
        "clld_turn_time": state.get("clldTurnTime"),
        "clld_1h_change": state.get("clld1hChange"),
        "short_magnet_zone": state.get("shortMagZone"),
        "short_magnet_dist_pct": state.get("shortMagDist"),
        "short_magnet_strength": state.get("shortMagStr"),
        "long_magnet_zone": state.get("longMagZone"),
        "long_magnet_dist_pct": state.get("longMagDist"),
        "long_magnet_strength": state.get("longMagStr"),
        "near_field_long_pct": near_field.get("longs_pct") if near_field else None,
        "near_field_short_pct": near_field.get("shorts_pct") if near_field else None,
        "near_field_bias": near_field.get("status_desc") if near_field else None,
        "magnet_zones_count": len(magnet_zones),
        "key_levels_count": len(key_levels)
    }

    return {
        "snapshot": snapshot,
        "key_levels": key_levels,
        "magnet_zones": magnet_zones,
        "leverage_tiers": leverage_tiers,
        "near_field": near_field
    }


async def harvest_asset_liquidations(
    page,
    symbol: str,
    mt5_symbol: str,
    target_exchanges: list = None,
    timeframes: list = None,
    range_filters: list = None
) -> tuple[list, list, list, list, list]:
    """
    Sweeps an asset across all requested exchanges and lookback timeframes:
    - 3D - 1m, 7D - 3m, 30D - 15m
    - Ranges: ±1% up to ±8%
    Returns: (snapshots, key_levels, magnet_zones, leverage_tiers, near_fields)
    """
    if timeframes is None:
        timeframes = [
            {"label": "3D - 1m", "kw": "3D"},
            {"label": "7D - 3m", "kw": "7D"},
            {"label": "30D - 15m", "kw": "30D"}
        ]

    # Navigate to symbol page if not already there
    cur_url = page.url
    if f"symbol={symbol}" not in cur_url:
        await page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={symbol}", timeout=20000)
        await asyncio.sleep(2.5)

    # Auto-discover exchanges if not strictly passed
    discovered = await discover_perpetual_exchanges(page, symbol=symbol)
    ex_map = {d["exchange"].upper(): d["contract"] for d in discovered}

    if not target_exchanges or target_exchanges == ["all"]:
        # Prioritize major liquid exchanges
        major_order = ["BINANCE", "BYBIT", "OKX", "GATE", "MEXC", "HYPERLIQUID", "BITGET"]
        selected_exs = [ex for ex in major_order if ex in ex_map]
        if not selected_exs and discovered:
            selected_exs = list(ex_map.keys())[:3]
    else:
        selected_exs = [ex.upper() for ex in target_exchanges if ex.upper() in ex_map]
        if not selected_exs:
            selected_exs = list(ex_map.keys())[:1] if ex_map else ["BINANCE"]

    all_snapshots = []
    all_key_levels = []
    all_magnet_zones = []
    all_leverage_tiers = []
    all_near_fields = []

    for ex in selected_exs:
        contract = ex_map.get(ex, f"{symbol}USDT")
        logger.info(f"[{symbol}] Switching exchange to {ex} ({contract})...")
        await switch_liquidation_exchange(page, target_exchange=ex, target_contract=contract, symbol=symbol)

        for tf in timeframes:
            # Click timeframe tab
            try:
                await page.evaluate(f'''() => {{
                    const allLi = Array.from(document.querySelectorAll('li'));
                    const el = allLi.find(x => (x.innerText || '').includes('{tf['kw']}'));
                    if (el) el.click();
                }}''')
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.warning(f"[{symbol} - {ex}] Failed to click TF tab {tf['kw']}: {e}")

            data = await extract_liquidation_data_point(
                page,
                symbol=symbol,
                mt5_symbol=mt5_symbol,
                exchange=ex,
                contract=contract,
                tf_label=tf["label"],
                range_filters=range_filters
            )

            if data.get("snapshot"):
                all_snapshots.append(data["snapshot"])
            all_key_levels.extend(data.get("key_levels", []))
            all_magnet_zones.extend(data.get("magnet_zones", []))
            all_leverage_tiers.extend(data.get("leverage_tiers", []))
            if data.get("near_field"):
                all_near_fields.append(data["near_field"])

    return all_snapshots, all_key_levels, all_magnet_zones, all_leverage_tiers, all_near_fields


def upsert_liquidation_snapshots(snapshots: list):
    """Persist macro snapshots across all assets to Parquet."""
    if not snapshots:
        return

    LIQ_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc).isoformat()
    for s in snapshots:
        s["snapshot_time_utc"] = now_utc
        s["dumped_at_utc"] = now_utc

    snap_df = pd.DataFrame(snapshots)

    if SNAPSHOTS_PARQUET.exists():
        try:
            existing_snap = pd.read_parquet(SNAPSHOTS_PARQUET)
            combined = pd.concat([existing_snap, snap_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["snapshot_time_utc", "symbol", "exchange", "lookback_tf"], keep="last")
        except Exception as e:
            logger.warning(f"Failed to merge existing snapshots ({e}). Writing new data.")
            combined = snap_df
    else:
        combined = snap_df

    combined.to_parquet(SNAPSHOTS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Liquidation Snapshots saved: {SNAPSHOTS_PARQUET.name} ({len(combined)} rows across {combined['symbol'].nunique()} assets)")


def upsert_asset_key_levels(symbol: str, key_levels: list):
    """Persist asset-partitioned key liquidation levels (ranked barriers) to Parquet."""
    if not key_levels:
        return

    KEY_LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = KEY_LEVELS_DIR / f"{symbol}_key_levels.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for kl in key_levels:
        kl["snapshot_time_utc"] = now_utc
        kl["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(key_levels)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["snapshot_time_utc", "exchange", "lookback_tf", "range_filter", "side", "rank", "price"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing key levels ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    if "rank" in combined_df.columns:
        combined_df = combined_df.sort_values(by=["lookback_tf", "range_filter", "side", "rank"]).reset_index(drop=True)

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} ranked strike barriers")


def upsert_asset_magnet_zones(symbol: str, magnet_zones: list):
    """Persist asset-partitioned Top 3 Short & Long Magnet Zones to Parquet."""
    if not magnet_zones:
        return

    MAGNET_ZONES_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = MAGNET_ZONES_DIR / f"{symbol}_magnet_zones.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for mz in magnet_zones:
        mz["snapshot_time_utc"] = now_utc
        mz["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(magnet_zones)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["snapshot_time_utc", "exchange", "lookback_tf", "side", "rank"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing magnet zones ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    if "rank" in combined_df.columns:
        combined_df = combined_df.sort_values(by=["lookback_tf", "side", "rank"]).reset_index(drop=True)

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} magnet zone bands")


def upsert_asset_leverage_tiers(symbol: str, leverage_tiers: list):
    """Persist asset-partitioned leverage tier breakdown to Parquet."""
    if not leverage_tiers:
        return

    LEVERAGE_TIERS_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = LEVERAGE_TIERS_DIR / f"{symbol}_leverage_tiers.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for lt in leverage_tiers:
        lt["snapshot_time_utc"] = now_utc
        lt["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(leverage_tiers)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["snapshot_time_utc", "exchange", "lookback_tf", "tier"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing leverage tiers ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} leverage tier records")


def upsert_asset_near_field(symbol: str, near_field_list: list):
    """Persist asset-partitioned near-field balance to Parquet."""
    if not near_field_list:
        return

    NEAR_FIELD_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = NEAR_FIELD_DIR / f"{symbol}_near_field.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for nf in near_field_list:
        nf["snapshot_time_utc"] = now_utc
        nf["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(near_field_list)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["snapshot_time_utc", "exchange", "lookback_tf"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing near-field balance ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} near-field balance records")


# ---------------------------------------------------------------------------
# Parallel Pipes: Footprint Worker & Liquidation Worker
# ---------------------------------------------------------------------------
async def footprint_worker(context, warp_lock: asyncio.Lock):
    """Continuous Pipe 1: Dumps 15m order-flow candles and footprint signals with WARP rotation."""
    logger.info("[Pipe 1: Footprint] Starting continuous order-flow collector...")
    fp_page = await get_coinglass_tab(context)
    await ensure_terminal_ready(fp_page)

    cycle_count = 1
    total_chunks = (len(FP_TARGET_SYMBOLS) + CHUNK_SIZE - 1) // CHUNK_SIZE

    while True:
        logger.info(f"[Pipe 1: Footprint] ========== STARTING FOOTPRINT CYCLE #{cycle_count} ==========")
        t0 = time.time()
        all_signals = []

        try:
            for i in range(0, len(FP_TARGET_SYMBOLS), CHUNK_SIZE):
                chunk = FP_TARGET_SYMBOLS[i:i + CHUNK_SIZE]
                chunk_num = i // CHUNK_SIZE + 1
                logger.info(f"[Pipe 1: Footprint] Processing Chunk {chunk_num}/{total_chunks} ({chunk[0]}..{chunk[-1]})...")

                chunk_result = await extract_footprint_chunk(fp_page, chunk)
                signals = chunk_result.get("signals", [])
                all_signals.extend(signals)

                candles_dict = chunk_result.get("candles", {})
                for sym, bar_list in candles_dict.items():
                    upsert_candle_parquet(sym, MT5_ASSET_MAP.get(sym, ""), bar_list)

            dump_signals_parquet(all_signals)
            await fp_page.evaluate("() => window.__cg_term.pickPair('BTC', 'agg-perp')")
            elapsed = round(time.time() - t0, 1)
            logger.info(f"[Pipe 1: Footprint] Cycle #{cycle_count} completed in {elapsed}s: {len(all_signals)} signals extracted.")
        except Exception as e:
            logger.error(f"[Pipe 1: Footprint] Error in cycle #{cycle_count}: {e}", exc_info=True)

        # Automated Cloudflare WARP IP Rotation (Mutex Protected)
        if not warp_lock.locked():
            logger.info("[Pipe 1: Footprint] Requesting Cloudflare WARP IP rotation...")
            async with warp_lock:
                await asyncio.to_thread(rotate_cloudflare_ip)
        else:
            logger.info("[Pipe 1: Footprint] Liquidation sweep in progress; deferring IP rotation to next cycle.")

        logger.info(f"[Pipe 1: Footprint] Resting for {FOOTPRINT_REST_SECONDS}s before next cycle...")
        await asyncio.sleep(FOOTPRINT_REST_SECONDS)
        cycle_count += 1


async def execute_liquidation_sweep(context, warp_lock: asyncio.Lock, target_symbol: str = None):
    """Executes a structured liquidation dump across assets, then immediately closes the tab."""
    symbols_to_harvest = [target_symbol] if target_symbol else LIQ_LEVEL_SYMBOLS
    logger.info(f"[Pipe 2: Liquidation] Initiating liquidation sweep across {len(symbols_to_harvest)} asset(s)...")
    async with warp_lock:
        liq_page = await context.new_page()
        try:
            start_sym = symbols_to_harvest[0] if symbols_to_harvest else "BTC"
            await liq_page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={start_sym}", timeout=20000)
            await asyncio.sleep(3.0)

            all_snapshots = []
            total_key_levels_count = 0
            total_magnet_zones_count = 0
            total_leverage_tiers_count = 0
            total_near_field_count = 0

            for idx, sym in enumerate(symbols_to_harvest, start=1):
                exs = LIQ_EXCHANGES_MAP.get(sym, ["Binance"])
                mt5_sym = MT5_ASSET_MAP.get(sym, "")
                logger.info(f"[Pipe 2: Liquidation] [{idx}/{len(symbols_to_harvest)}] Harvesting {sym} ({mt5_sym}) on {exs}...")

                snaps, key_lvls, mag_zones, lev_tiers, near_fields = await harvest_asset_liquidations(
                    liq_page,
                    sym,
                    mt5_sym,
                    target_exchanges=exs
                )

                all_snapshots.extend(snaps)
                if key_lvls:
                    upsert_asset_key_levels(sym, key_lvls)
                    total_key_levels_count += len(key_lvls)
                if mag_zones:
                    upsert_asset_magnet_zones(sym, mag_zones)
                    total_magnet_zones_count += len(mag_zones)
                if lev_tiers:
                    upsert_asset_leverage_tiers(sym, lev_tiers)
                    total_leverage_tiers_count += len(lev_tiers)
                if near_fields:
                    upsert_asset_near_field(sym, near_fields)
                    total_near_field_count += len(near_fields)

            if all_snapshots:
                upsert_liquidation_snapshots(all_snapshots)

            # Sync live MT5 ticks & basis differentials across all assets
            try:
                try:
                    from Terminal.mt5_market_sync import sync_mt5_ticks
                except ImportError:
                    from mt5_market_sync import sync_mt5_ticks
                sync_mt5_ticks()
            except Exception as e:
                logger.warning(f"Error during MT5 ticks synchronization: {e}")

            logger.info(f"[Pipe 2: Liquidation] Sweep complete: {total_key_levels_count} barriers, {total_magnet_zones_count} magnet zones across {len(LIQ_LEVEL_SYMBOLS)} assets.")
        except Exception as e:
            logger.error(f"[Pipe 2: Liquidation] Error during sweep: {e}", exc_info=True)
        finally:
            logger.info("[Pipe 2: Liquidation] Sweep finished. Closing liquidation tab to keep browser lean...")
            try:
                await liq_page.close()
            except Exception:
                pass


async def liquidation_worker(context, warp_lock: asyncio.Lock, run_initial: bool = False):
    """Scheduled Pipe 2: Triggers full 44-asset liquidation sweep at :00:05 and :30:05 local wall-clock."""
    logger.info("[Pipe 2: Liquidation] Scheduler initialized. Target cadence: :00:05 and :30:05 local time.")

    if run_initial:
        logger.info("[Pipe 2: Liquidation] Immediate initial sweep requested...")
        await execute_liquidation_sweep(context, warp_lock)

    while True:
        wait_secs = get_seconds_until_next_sweep()
        next_dt = datetime.now() + timedelta(seconds=wait_secs)
        logger.info(f"[Pipe 2: Liquidation] Next sweep scheduled at {next_dt.strftime('%H:%M:%S')} (in {round(wait_secs / 60, 1)} mins). Sleeping...")
        await asyncio.sleep(wait_secs)

        # Trigger sweep at :00:05 or :30:05
        await execute_liquidation_sweep(context, warp_lock)


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------
async def main_collector(args):
    """Main orchestrator connecting to Edge CDP and launching parallel workers."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    LIQ_DIR.mkdir(parents=True, exist_ok=True)
    KEY_LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    MAGNET_ZONES_DIR.mkdir(parents=True, exist_ok=True)
    LEVERAGE_TIERS_DIR.mkdir(parents=True, exist_ok=True)
    NEAR_FIELD_DIR.mkdir(parents=True, exist_ok=True)

    warp_lock = asyncio.Lock()

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            logger.error(f"Cannot connect to CDP at {CDP_URL}: {e}")
            logger.error("Please ensure Edge/Chrome is running with: --remote-debugging-port=9222")
            sys.exit(1)

        ctx = browser.contexts[0] if browser.contexts else await browser.new_context()

        # Mode A: Liquidation-only single sweep
        if args.liquidations_only:
            target_str = f"for {args.symbol}" if args.symbol else "across all assets"
            logger.info(f"Executing liquidation-only one-shot dump {target_str}...")
            await execute_liquidation_sweep(ctx, warp_lock, target_symbol=args.symbol)
            logger.info("Liquidation one-shot dump complete. Exiting.")
            return

        # Mode B: Footprint-only single cycle
        if args.footprint_only:
            logger.info("Executing footprint-only single cycle...")
            fp_page = await get_coinglass_tab(ctx)
            await ensure_terminal_ready(fp_page)
            all_signals = []
            total_chunks = (len(FP_TARGET_SYMBOLS) + CHUNK_SIZE - 1) // CHUNK_SIZE
            for i in range(0, len(FP_TARGET_SYMBOLS), CHUNK_SIZE):
                chunk = FP_TARGET_SYMBOLS[i:i + CHUNK_SIZE]
                chunk_result = await extract_footprint_chunk(fp_page, chunk)
                all_signals.extend(chunk_result.get("signals", []))
                for sym, bar_list in chunk_result.get("candles", {}).items():
                    upsert_candle_parquet(sym, MT5_ASSET_MAP.get(sym, ""), bar_list)
            dump_signals_parquet(all_signals)
            logger.info(f"Footprint single cycle complete: {len(all_signals)} signals extracted.")
            return

        # Mode C: One-shot full pass (both footprint and liquidation)
        if args.once:
            logger.info("Executing one-shot full dump across both footprint and liquidation streams...")
            # Run footprint single pass
            fp_page = await get_coinglass_tab(ctx)
            await ensure_terminal_ready(fp_page)
            all_signals = []
            for i in range(0, len(FP_TARGET_SYMBOLS), CHUNK_SIZE):
                chunk = FP_TARGET_SYMBOLS[i:i + CHUNK_SIZE]
                chunk_result = await extract_footprint_chunk(fp_page, chunk)
                all_signals.extend(chunk_result.get("signals", []))
                for sym, bar_list in chunk_result.get("candles", {}).items():
                    upsert_candle_parquet(sym, MT5_ASSET_MAP.get(sym, ""), bar_list)
            dump_signals_parquet(all_signals)

            # Run liquidation sweep
            await execute_liquidation_sweep(ctx, warp_lock)
            logger.info("One-shot full cycle complete. Exiting.")
            return

        # Mode D: Parallel continuous production engine
        logger.info("Launching parallel continuous production pipes:")
        logger.info(" - Pipe 1: Continuous Footprint stream with Cloudflare WARP IP rotation.")
        logger.info(" - Pipe 2: Scheduled 44-asset Liquidation sweep at :00:05 and :30:05 local time.")
        logger.info(" - Automatic tab lifecycle: Liquidation tab will open on schedule and close when done.")

        await asyncio.gather(
            footprint_worker(ctx, warp_lock),
            liquidation_worker(ctx, warp_lock, run_initial=args.initial_liquidation)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CoinGlass Master Parallel Data Collector")
    parser.add_argument("--symbol", type=str, default=None, help="Target specific symbol for collection (e.g. BTC)")
    parser.add_argument("--once", action="store_true", help="Run a single full collection cycle and exit")
    parser.add_argument("--liquidations-only", action="store_true", help="Harvest only liquidation data across all 44 assets, close tab, and exit")
    parser.add_argument("--footprint-only", action="store_true", help="Harvest only footprint candles and signals for one cycle and exit")
    parser.add_argument("--initial-liquidation", action="store_true", help="Run an immediate liquidation sweep upon starting before settling into the :00/:30 cadence")
    cli_args = parser.parse_args()

    asyncio.run(main_collector(cli_args))
