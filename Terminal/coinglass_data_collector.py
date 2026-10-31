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
LEVELS_DIR = LIQ_DIR / "Levels"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"

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
# Liquidation Intelligence Helpers
# ---------------------------------------------------------------------------
async def switch_liquidation_pair(page, symbol: str, target_contract: str, target_exchange: str = "Binance") -> bool:
    """Robust native pair selection via CoinGlass UI symbol switcher, with fallback to URL navigation."""
    try:
        btn = page.locator("button.ll-symBtn")
        if await btn.count() > 0:
            cur_text = (await btn.inner_text()).strip()
            norm_target = target_contract.lower().replace("_", "").replace("-", "")
            norm_cur = cur_text.lower().replace("_", "").replace("-", "")
            norm_ex = target_exchange.lower()

            # If already on the correct exchange and contract, no switch needed
            if norm_target in norm_cur and (norm_ex in norm_cur or norm_ex == "binance"):
                return True

            await btn.click()
            await asyncio.sleep(0.8)

            inp = page.locator("input[aria-label='Search trading pairs'], input[placeholder*='Search' i]").first
            if await inp.count() > 0 and await inp.is_visible():
                search_key = symbol.lower()
                await inp.fill(search_key)
                await asyncio.sleep(1.2)

                # Match target contract button
                target_btn = page.locator("button").filter(has_text=target_contract)
                if await target_btn.count() == 0:
                    target_btn = page.locator("button").filter(has_text=symbol)

                if await target_btn.count() > 0:
                    await target_btn.first.click()
                    await asyncio.sleep(3.5)
                    return True

        # Fallback to direct navigation and localStorage
        await page.evaluate(f"""
            () => {{
                const current = JSON.parse(localStorage.getItem('liqLevelsPrefs') || '{{}}');
                current.ex = '{target_exchange}';
                current.sym = '{target_contract}';
                localStorage.setItem('liqLevelsPrefs', JSON.stringify(current));
            }}
        """)
        await page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={symbol}", timeout=15000)
        await asyncio.sleep(3.0)
        return True
    except Exception as e:
        logger.warning(f"[{symbol}] Error during pair switch to {target_exchange} ({target_contract}): {e}")
        return False


async def extract_single_liquidation_state(page, symbol: str, mt5_symbol: str, tf_label: str) -> dict:
    """Extract macro snapshot, discrete strike levels, and ranked key barriers for a specific timeframe."""
    js_extractor = """
    ({ symbol, mt5Symbol, tfLabel }) => {
        const nextEl = document.getElementById('__next');
        const containerKey = Object.keys(nextEl).find(k => k.startsWith('__reactContainer$'));
        const rootFiber = nextEl[containerKey];

        let targetFiber = null;
        function walk(f, depth = 0) {
            if (!f || depth > 80 || targetFiber) return;
            try {
                if (f.memoizedProps && f.memoizedProps.model && f.memoizedProps.ex) {
                    targetFiber = f;
                    return;
                }
            } catch(e) {}
            walk(f.child, depth + 1);
            walk(f.sibling, depth);
        }
        walk(rootFiber);
        const p = targetFiber ? targetFiber.memoizedProps : null;
        const m = p ? p.model : null;
        if (!m) return null;

        const curPrice = m.curPrice;
        const bodyText = document.body.innerText;

        // Magnet Zones
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

        // CLLD Card
        let clldVal = null, clldSentiment = null, clldTurnTime = null, clld1hChange = null;
        const clldMatch = bodyText.match(/CLLD\\s*·\\s*Cumulative Liquidation Levels Delta\\s*([−\\-\\+\\d,]+)\\s*(?:[▲▼]\\s*)?([A-Za-z]+ turn)\\s*([\\d\\-\\s:]+)\\s*More [^·]+·\\s*1h change\\s*([+\\-\\d,]+)/i);
        if (clldMatch) {
            clldVal = parseFloat(clldMatch[1].replace('−', '-').replace(/,/g, ''));
            clldSentiment = clldMatch[2].trim();
            clldTurnTime = clldMatch[3].trim();
            clld1hChange = parseFloat(clldMatch[4].replace(/,/g, ''));
        }

        // Near-field balance
        let nfbLongPct = null, nfbShortPct = null, nfbBias = null;
        const nfbMatch = bodyText.match(/Near-field balance[^\\n]*\\s*Longs\\s*(\\d+)%[^\\n]*Shorts\\s*(\\d+)%[^\\n]*\\n([^\\n]+)/i);
        if (nfbMatch) {
            nfbLongPct = parseFloat(nfbMatch[1]);
            nfbShortPct = parseFloat(nfbMatch[2]);
            nfbBias = nfbMatch[3].trim();
        }

        // Strength by distance
        const sbdMatch = bodyText.match(/Strength by distance[\\s\\S]*?Shorts\\s*([\\d,]+)[\\s\\S]*?Longs\\s*([\\d,]+)[\\s\\S]*?0[–-]1%\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?1[–-]2%\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?2[–-]3%\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?3[–-]5%\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?5[–-]10%\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)/i);
        let sbd = {};
        if (sbdMatch) {
            sbd = {
                sbd_shorts_tot: parseFloat(sbdMatch[1].replace(/,/g, '')),
                sbd_longs_tot: parseFloat(sbdMatch[2].replace(/,/g, '')),
                sbd_0_1_short: parseFloat(sbdMatch[3].replace(/,/g, '')),
                sbd_0_1_long: parseFloat(sbdMatch[4].replace(/,/g, '')),
                sbd_1_2_short: parseFloat(sbdMatch[5].replace(/,/g, '')),
                sbd_1_2_long: parseFloat(sbdMatch[6].replace(/,/g, '')),
                sbd_2_3_short: parseFloat(sbdMatch[7].replace(/,/g, '')),
                sbd_2_3_long: parseFloat(sbdMatch[8].replace(/,/g, '')),
                sbd_3_5_short: parseFloat(sbdMatch[9].replace(/,/g, '')),
                sbd_3_5_long: parseFloat(sbdMatch[10].replace(/,/g, '')),
                sbd_5_10_short: parseFloat(sbdMatch[11].replace(/,/g, '')),
                sbd_5_10_long: parseFloat(sbdMatch[12].replace(/,/g, ''))
            };
        }

        // Leverage totals
        const levTotals = {};
        const levKeys = ['highLevShorts', 'midLevShorts', 'lowLevShorts', 'highLevLongs', 'midLevLongs', 'lowLevLongs'];
        levKeys.forEach(k => {
            if (m[k] !== undefined) levTotals[k] = m[k];
        });

        // 1. Discrete strike levels
        const activeLevels = [];
        if (m.levels && Array.isArray(m.levels)) {
            m.levels.forEach(lvl => {
                const px = lvl.p;
                const side = px > curPrice ? 'short' : 'long';
                const distPct = curPrice > 0 ? ((px - curPrice) / curPrice) * 100 : 0;
                const levMap = { 0: 'high', 1: 'mid', 2: 'low' };
                activeLevels.push({
                    price: px,
                    side: side,
                    distance_pct: distPct,
                    leverage_tier: levMap[lvl.l] || 'mid',
                    tier_level: lvl.t !== undefined ? (lvl.t + 1) : 1,
                    volume: lvl.v || null,
                    strength: lvl.s || null
                });
            });
        }

        // 2. Key liquidation levels table
        const keyLevels = [];
        const table = document.querySelector('table');
        if (table) {
            const rows = table.querySelectorAll('tbody tr');
            const currentSideRank = { 'short': 1, 'long': 1 };
            for (const r of rows) {
                const cells = r.querySelectorAll('td');
                if (cells.length >= 5) {
                    const parts = Array.from(cells).map(c => c.innerText.trim());
                    const s = parts[0].toLowerCase();
                    keyLevels.push({
                        side: s,
                        rank: currentSideRank[s]++,
                        price: parseFloat(parts[1].replace(/,/g, '')),
                        distance_pct: parseFloat(parts[2].replace('%', '').replace('+', '')),
                        leverage_tier: parts[3].toLowerCase().replace(' leverage', ''),
                        strength: parseFloat(parts[4]),
                        range_filter: '±8%',
                        last_price: curPrice
                    });
                }
            }
        }

        const snapshot = {
            exchange: p.ex || 'Binance',
            symbol: symbol,
            mt5_symbol: mt5Symbol,
            pair: p.sym || `${symbol}USDT`,
            lookback_tf: tfLabel,
            last_price: curPrice,
            clld_current: clldVal,
            clld_sentiment: clldSentiment,
            clld_turn_time: clldTurnTime,
            clld_1h_change: clld1hChange,
            short_magnet_zone: shortMagZone,
            short_magnet_dist_pct: shortMagDist,
            short_magnet_strength: shortMagStr,
            long_magnet_zone: longMagZone,
            long_magnet_dist_pct: longMagDist,
            long_magnet_strength: longMagStr,
            near_field_long_pct: nfbLongPct,
            near_field_short_pct: nfbShortPct,
            near_field_bias: nfbBias,
            ...sbd,
            ...levTotals,
            active_levels_count: activeLevels.length,
            key_levels_count: keyLevels.length
        };

        return { snapshot, levels: activeLevels, key_levels: keyLevels };
    }
    """
    return await page.evaluate(js_extractor, {"symbol": symbol, "mt5Symbol": mt5_symbol, "tfLabel": tf_label})


async def harvest_asset_liquidations(page, symbol: str, mt5_symbol: str, target_exchanges: list = None, is_deep_level: bool = True, contracts_override: dict = None) -> tuple[list, list, list]:
    """Harvest liquidation snapshots, multi-timeframe discrete levels, and key levels for an asset across exchanges."""
    snapshots = []
    levels = []
    key_levels = []

    if not target_exchanges:
        target_exchanges = ["Binance"]

    c_map = contracts_override or LIQ_CONTRACTS_MAP.get(symbol, {})

    timeframes = [
        {"label": "30D-15m", "kw": "30D"},
        {"label": "7D-3m", "kw": "7D"},
        {"label": "3D-1m", "kw": "3D"}
    ] if is_deep_level else [{"label": "30D-15m", "kw": "30D"}]

    for ex in target_exchanges:
        contract_sym = c_map.get(ex, f"{symbol}USDT")
        logger.info(f"[{symbol}] Switching exchange to {ex} (contract: {contract_sym})...")
        await switch_liquidation_pair(page, symbol=symbol, target_contract=contract_sym, target_exchange=ex)

        # Auto-healing: click Retry if present
        retry_btn = page.locator("button:has-text('Retry')")
        if await retry_btn.count() > 0:
            logger.info(f"[{symbol} - {ex}] Retry button detected. Clicking to heal...")
            try:
                await retry_btn.click()
                await asyncio.sleep(2)
            except Exception:
                pass

        for tf in timeframes:
            # Click timeframe tab
            try:
                await page.evaluate(f"""
                    () => {{
                        const allLi = Array.from(document.querySelectorAll('li'));
                        const el = allLi.find(x => (x.innerText || '').includes('{tf['kw']}'));
                        if (el) el.click();
                    }}
                """)
                await asyncio.sleep(1.2)
            except Exception as e:
                logger.warning(f"[{symbol} - {ex}] Failed to click TF tab {tf['kw']}: {e}")

            extracted = await extract_single_liquidation_state(page, symbol, mt5_symbol, tf["label"])
            if extracted and extracted.get("snapshot"):
                snap = extracted["snapshot"]
                snapshots.append(snap)
                
                # Discrete strike levels
                if is_deep_level:
                    for lvl in extracted.get("levels", []):
                        lvl["symbol"] = symbol
                        lvl["exchange"] = snap["exchange"]
                        lvl["lookback_tf"] = tf["label"]
                        levels.append(lvl)

                # Key liquidation levels table
                for kl in extracted.get("key_levels", []):
                    kl["symbol"] = symbol
                    kl["mt5_symbol"] = mt5_symbol
                    kl["exchange"] = snap["exchange"]
                    kl["lookback_tf"] = tf["label"]
                    key_levels.append(kl)

    return snapshots, levels, key_levels


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
    logger.info(f"Liquidation Snapshots saved: {SNAPSHOTS_PARQUET.name} ({len(combined)} total rows across {combined['symbol'].nunique()} assets)")


def upsert_asset_levels(symbol: str, levels: list):
    """Persist asset-partitioned discrete strike levels to Parquet."""
    if not levels:
        return

    LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = LEVELS_DIR / f"{symbol}_levels.parquet"
    now_utc = datetime.now(timezone.utc).isoformat()

    for lvl in levels:
        lvl["snapshot_time_utc"] = now_utc
        lvl["dumped_at_utc"] = now_utc

    new_df = pd.DataFrame(levels)

    if parquet_path.exists():
        try:
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["snapshot_time_utc", "exchange", "lookback_tf", "price"], keep="last")
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing levels ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} discrete strike levels")


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
                subset=["snapshot_time_utc", "exchange", "lookback_tf", "side", "rank", "price"],
                keep="last"
            )
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to merge existing key levels ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    if "rank" in combined_df.columns:
        combined_df = combined_df.sort_values(by=["lookback_tf", "side", "rank"]).reset_index(drop=True)

    combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} key strike barriers")


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


async def execute_liquidation_sweep(context, warp_lock: asyncio.Lock):
    """Executes a full 44-asset liquidation dump, then immediately closes the tab."""
    logger.info("[Pipe 2: Liquidation] Initiating scheduled liquidation sweep across all 44 assets...")
    async with warp_lock:
        liq_page = await context.new_page()
        try:
            await liq_page.goto("https://www.coinglass.com/liquidation-levels?symbol=BTC", timeout=20000)
            await asyncio.sleep(3.0)

            all_snapshots = []
            total_levels_count = 0
            total_key_levels_count = 0

            for idx, sym in enumerate(LIQ_LEVEL_SYMBOLS, start=1):
                exs = LIQ_EXCHANGES_MAP.get(sym, ["Binance"])
                mt5_sym = MT5_ASSET_MAP.get(sym, "")
                logger.info(f"[Pipe 2: Liquidation] [{idx}/{len(LIQ_LEVEL_SYMBOLS)}] Harvesting {sym} ({mt5_sym}) on {exs}...")

                snaps, levels, key_lvls = await harvest_asset_liquidations(
                    liq_page,
                    sym,
                    mt5_sym,
                    target_exchanges=exs,
                    is_deep_level=True
                )

                all_snapshots.extend(snaps)
                if levels:
                    upsert_asset_levels(sym, levels)
                    total_levels_count += len(levels)
                if key_lvls:
                    upsert_asset_key_levels(sym, key_lvls)
                    total_key_levels_count += len(key_lvls)

            if all_snapshots:
                upsert_liquidation_snapshots(all_snapshots)

            logger.info(f"[Pipe 2: Liquidation] Sweep complete: {total_levels_count} discrete levels, {total_key_levels_count} barriers across {len(LIQ_LEVEL_SYMBOLS)} assets.")
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
    LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    KEY_LEVELS_DIR.mkdir(parents=True, exist_ok=True)

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
            logger.info("Executing liquidation-only one-shot dump across all 44 assets...")
            await execute_liquidation_sweep(ctx, warp_lock)
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
    parser.add_argument("--once", action="store_true", help="Run a single full collection cycle and exit")
    parser.add_argument("--liquidations-only", action="store_true", help="Harvest only liquidation data across all 44 assets, close tab, and exit")
    parser.add_argument("--footprint-only", action="store_true", help="Harvest only footprint candles and signals for one cycle and exit")
    parser.add_argument("--initial-liquidation", action="store_true", help="Run an immediate liquidation sweep upon starting before settling into the :00/:30 cadence")
    cli_args = parser.parse_args()

    asyncio.run(main_collector(cli_args))
