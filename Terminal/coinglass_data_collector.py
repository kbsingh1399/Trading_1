"""
CoinGlass Master Data Collector (Footprint Signals, 15m Candles, & Liquidation Data)
------------------------------------------------------------------------------------
Automates continuous, multi-stream data collection across the mutual intersection of
assets supported by both Blueberry Markets MT5 and CoinGlass:

Data Streams Collected:
1. Footprint Order-Flow Signals:
   - Trapped Longs, Trapped Shorts, Absorption, Exhaustion, Divergence
   - Stored in: `Database/coinglass_aggregated_futures_signals.parquet`
2. 15-Minute Order-Flow Candle Datasets:
   - Full OHLC, Volume (Buy/Sell/Total), Delta, Cumulative Delta, Trades,
     Avg Trade Size, Buy/Sell Ratio, POC Price/Volume/Delta, Open Interest (Open/Close/Change)
   - Stored in: `Database/Candle_Data/<SYMBOL>.parquet` (Deduplicated Upsert)
   - Scope: 35 assets (31 Cryptos + Gold XAU, Silver XAG, Copper COPPER, Natural Gas GAS)
3. Liquidation Intelligence (Snapshots & Discrete Strike Levels):
   - Macro Snapshots: Nearest Short/Long Magnet Zones, CLLD (Cumulative Delta),
     Sentiment turns, Near-field balance, Distance bands, and Leverage breakdown.
     Stored in: `Database/Liquidation_Data/liquidation_snapshots.parquet`
   - Discrete Strike Levels (Multi-Timeframe & Multi-Exchange):
     Exact liquidation trigger prices, sides, distance %, leverage tiers (High/Mid/Low),
     granularity tiers (Tier 1/2/3), volumes, and strength ratings.
     Partitioned asset-wise: `Database/Liquidation_Data/Levels/<SYMBOL>_levels.parquet`
4. Reliability & Network:
   - Automated Cloudflare WARP IP rotation after every cycle.
   - Auto-retry healing for canvas/DOM timeouts.
   - 2-minute configurable rest interval between cycles.
"""

import sys
import os
import time
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd
from playwright.sync_api import sync_playwright

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
CYCLE_REST_SECONDS = 120  # 2 minutes rest between cycles
CHUNK_SIZE = 10           # Assets per in-page batch chunk

REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DataCollector")


def load_capability_registry() -> tuple[dict, list, list, list, dict]:
    """Load the verified mutual asset capability registry."""
    if not REGISTRY_FILE.exists():
        logger.error(f"Capability registry missing at: {REGISTRY_FILE}")
        return {}, [], [], [], {}

    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)

    mt5_map = {}
    fp_symbols = []
    liq_level_symbols = []
    liq_snap_symbols = []
    liq_exchanges_map = {}

    for category, assets in reg.items():
        for sym, meta in assets.items():
            mt5_map[sym] = meta["mt5"]
            liq_exchanges_map[sym] = meta.get("liq_exchanges", ["Binance"])
            if meta.get("footprint", False):
                fp_symbols.append(sym)
            if meta.get("liq_levels", False):
                liq_level_symbols.append(sym)
            if meta.get("liq_snapshots", False):
                liq_snap_symbols.append(sym)

    return mt5_map, fp_symbols, liq_level_symbols, liq_snap_symbols, liq_exchanges_map


MT5_ASSET_MAP, FP_TARGET_SYMBOLS, LIQ_LEVEL_SYMBOLS, LIQ_SNAP_SYMBOLS, LIQ_EXCHANGES_MAP = load_capability_registry()


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


def get_coinglass_tab(browser_context):
    """Locate the existing CoinGlass footprint tab, or open a new one if missing."""
    for page in browser_context.pages:
        if "coinglass.com/footprint" in page.url:
            return page
    
    logger.info("CoinGlass footprint tab not found in active tabs. Navigating new page...")
    page = browser_context.new_page()
    page.goto("https://www.coinglass.com/footprint?symbol=BTC", wait_until="networkidle")
    time.sleep(3)
    return page


def get_liquidation_tab(browser_context):
    """Locate the existing CoinGlass liquidation-levels tab, or open a new one if missing."""
    for page in browser_context.pages:
        if "coinglass.com/liquidation-levels" in page.url:
            return page
    
    logger.info("Liquidation levels tab not found. Navigating new page...")
    page = browser_context.new_page()
    page.goto("https://www.coinglass.com/liquidation-levels?symbol=BTC", wait_until="networkidle")
    time.sleep(3)
    return page


def ensure_terminal_ready(page):
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
    ready = page.evaluate(init_script)
    if not ready:
        time.sleep(2)
        ready = page.evaluate(init_script)
    return ready


def verify_candle_sanity(df: pd.DataFrame, symbol: str) -> bool:
    """Rigorous mathematical and data sanity verification on candle records."""
    if df.empty:
        logger.error(f"[{symbol}] Sanity check failed: DataFrame is empty!")
        return False

    # 1. Monotonic timestamps
    if not df["open_time_ms"].is_monotonic_increasing:
        logger.error(f"[{symbol}] Sanity check failed: Timestamps are not strictly monotonic increasing!")
        return False

    # 2. OHLC Price integrity
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

    # 3. Volume conservation: buy_volume + sell_volume == total_volume
    vol_valid = (abs((df["buy_volume"] + df["sell_volume"]) - df["total_volume"]) < 1e-4).all()
    if not vol_valid:
        logger.error(f"[{symbol}] Sanity check failed: Volume conservation violation (buy + sell != total)!")
        return False

    # 4. Delta consistency: buy_volume - sell_volume == delta
    delta_valid = (abs((df["buy_volume"] - df["sell_volume"]) - df["delta"]) < 1e-4).all()
    if not delta_valid:
        logger.error(f"[{symbol}] Sanity check failed: Delta calculation mismatch (buy - sell != delta)!")
        return False

    # 5. Non-negativity
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


def extract_footprint_chunk(page, symbols_chunk: list) -> dict:
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
    return page.evaluate(combined_extract_js, {"symbolsChunk": symbols_chunk, "mt5Map": MT5_ASSET_MAP})


def extract_single_liquidation_state(page, symbol: str, mt5_symbol: str, tf_label: str) -> dict:
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

        // By leverage tier
        const levMatch = bodyText.match(/By leverage tier[\\s\\S]*?Shorts\\s*([\\d,]+)[\\s\\S]*?Longs\\s*([\\d,]+)[\\s\\S]*?Total\\s*([\\d,]+)[\\s\\S]*?High leverage\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?Mid leverage\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)[\\s\\S]*?Low leverage\\s*([\\d,]+)\\s*[·•]\\s*([\\d,]+)/i);
        let levTotals = {};
        if (levMatch) {
            levTotals = {
                lev_shorts_tot: parseFloat(levMatch[1].replace(/,/g, '')),
                lev_longs_tot: parseFloat(levMatch[2].replace(/,/g, '')),
                lev_tot: parseFloat(levMatch[3].replace(/,/g, '')),
                high_lev_short: parseFloat(levMatch[4].replace(/,/g, '')),
                high_lev_long: parseFloat(levMatch[5].replace(/,/g, '')),
                mid_lev_short: parseFloat(levMatch[6].replace(/,/g, '')),
                mid_lev_long: parseFloat(levMatch[7].replace(/,/g, '')),
                low_lev_short: parseFloat(levMatch[8].replace(/,/g, '')),
                low_lev_long: parseFloat(levMatch[9].replace(/,/g, ''))
            };
        }

        // Granularity volume thresholds
        const granThr = m.GRAN_THR || [0, 1000, 2500];

        // Active Segments
        const activeLevels = [];
        let maxSz = 0;
        for (let i = 0; i < m.nSeg; i++) {
            if (!m.segHit || m.segHit[i] === 0) {
                const sz = m.segSz ? m.segSz[i] : 0;
                if (sz > maxSz) maxSz = sz;
            }
        }

        for (let i = 0; i < m.nSeg; i++) {
            if (!m.segHit || m.segHit[i] === 0) {
                const px = m.segPx[i];
                const sz = m.segSz[i];
                const lev = m.segLev[i] === 0 ? 'high' : (m.segLev[i] === 1 ? 'mid' : 'low');
                const side = m.segSide[i] === 1 ? 'short' : 'long';
                const dist = ((px - curPrice) / curPrice) * 100;
                const weight = maxSz > 0 ? (sz / maxSz) : 0;
                const strength = Math.round(weight * 1000) / 10;

                let tierLevel = 1;
                if (sz >= granThr[2]) tierLevel = 3;
                else if (sz >= granThr[1]) tierLevel = 2;

                activeLevels.push({
                    price: Math.round(px * 10) / 10,
                    volume: Math.round(sz * 100) / 100,
                    side: side,
                    leverage_tier: lev,
                    tier_level: tierLevel,
                    distance_pct: Math.round(dist * 100) / 100,
                    strength: strength,
                    weight: Math.round(weight * 10000) / 10000,
                    is_active: true
                });
            }
        }

        // Key Liquidation Levels Table (Parsed directly from the table widget)
        const keyLevels = [];
        const allDivs = Array.from(document.querySelectorAll('div'));
        const keyDiv = allDivs.find(d => (d.innerText || '').startsWith('Key liquidation levels') && (d.innerText || '').includes('Within'));
        if (keyDiv) {
            const lines = keyDiv.innerText.split('\\n').map(l => l.trim()).filter(Boolean);
            let currentSideRank = { short: 1, long: 1 };
            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];
                const parts = line.split('\\t').map(x => x.trim()).filter(Boolean);
                if (parts.length >= 5 && (parts[0] === 'Short' || parts[0] === 'Long')) {
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
    return page.evaluate(js_extractor, {"symbol": symbol, "mt5Symbol": mt5_symbol, "tfLabel": tf_label})


def harvest_asset_liquidations(page, symbol: str, mt5_symbol: str, target_exchanges: list = None, is_deep_level: bool = True) -> tuple[list, list, list]:
    """Harvest liquidation snapshots, multi-timeframe discrete levels, and key levels for an asset across exchanges."""
    snapshots = []
    levels = []
    key_levels = []

    if not target_exchanges:
        target_exchanges = ["Binance"]

    timeframes = [
        {"label": "30D-15m", "kw": "30D"},
        {"label": "7D-3m", "kw": "7D"},
        {"label": "3D-1m", "kw": "3D"}
    ] if is_deep_level else [{"label": "30D-15m", "kw": "30D"}]

    for ex in target_exchanges:
        logger.info(f"[{symbol}] Switching exchange to {ex}...")
        # Set exchange in localStorage and navigate
        page.evaluate(f"""
            () => {{
                const current = JSON.parse(localStorage.getItem('liqLevelsPrefs') || '{{}}');
                current.ex = '{ex}';
                current.sym = '{symbol}USDT';
                localStorage.setItem('liqLevelsPrefs', JSON.stringify(current));
            }}
        """)
        try:
            page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={symbol}", timeout=15000)
            time.sleep(2.5)
        except Exception as e:
            logger.warning(f"[{symbol}] Navigation error on exchange {ex}: {e}")

        # Auto-healing: click Retry if present
        retry_btn = page.locator("button:has-text('Retry')")
        if retry_btn.count() > 0:
            logger.info(f"[{symbol} - {ex}] Retry button detected. Clicking to heal...")
            try:
                retry_btn.click()
                time.sleep(2)
            except Exception:
                pass

        for tf in timeframes:
            # Click timeframe tab
            try:
                page.evaluate(f"""
                    () => {{
                        const allLi = Array.from(document.querySelectorAll('li'));
                        const el = allLi.find(x => (x.innerText || '').includes('{tf['kw']}'));
                        if (el) el.click();
                    }}
                """)
                time.sleep(1.2)
            except Exception as e:
                logger.warning(f"[{symbol} - {ex}] Failed to click TF tab {tf['kw']}: {e}")

            extracted = extract_single_liquidation_state(page, symbol, mt5_symbol, tf["label"])
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


def run_cycle(context) -> dict:
    """Execute a single full collection cycle across footprint and liquidation streams."""
    # -----------------------------------------------------------------------
    # Step 1: Footprint Signals & 15m Candles across all 35 mutual assets
    # -----------------------------------------------------------------------
    fp_page = get_coinglass_tab(context)
    ensure_terminal_ready(fp_page)

    all_signals = []
    total_chunks = (len(FP_TARGET_SYMBOLS) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(FP_TARGET_SYMBOLS), CHUNK_SIZE):
        chunk = FP_TARGET_SYMBOLS[i:i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        logger.info(f"[Footprint] Processing Chunk {chunk_num}/{total_chunks} ({chunk[0]}..{chunk[-1]})...")

        chunk_result = extract_footprint_chunk(fp_page, chunk)
        signals = chunk_result.get("signals", [])
        all_signals.extend(signals)

        candles_dict = chunk_result.get("candles", {})
        for sym, bar_list in candles_dict.items():
            upsert_candle_parquet(sym, MT5_ASSET_MAP.get(sym, ""), bar_list)

    dump_signals_parquet(all_signals)
    fp_page.evaluate("() => window.__cg_term.pickPair('BTC', 'agg-perp')")

    # -----------------------------------------------------------------------
    # Step 2: Liquidation Intelligence (Snapshots, Discrete Levels, Key Levels)
    # -----------------------------------------------------------------------
    liq_page = get_liquidation_tab(context)
    all_snapshots = []
    total_levels_count = 0
    total_key_levels_count = 0

    # 2A. Deep Strike Levels (Multi-TF 3D/7D/30D) for top priority assets
    priority_level_assets = [s for s in ["BTC", "ETH", "SOL", "XRP", "XAU"] if s in LIQ_LEVEL_SYMBOLS]
    for sym in priority_level_assets:
        exs = LIQ_EXCHANGES_MAP.get(sym, ["Binance", "Bybit"])
        logger.info(f"[Liquidations] Harvesting multi-TF levels & key barriers for {sym} across {exs}...")
        snaps, levels, key_lvls = harvest_asset_liquidations(liq_page, sym, MT5_ASSET_MAP.get(sym, ""), target_exchanges=exs, is_deep_level=True)
        all_snapshots.extend(snaps)
        upsert_asset_levels(sym, levels)
        upsert_asset_key_levels(sym, key_lvls)
        total_levels_count += len(levels)
        total_key_levels_count += len(key_lvls)

    # 2B. Macro Liquidation Snapshots for Forex & Secondary Assets
    macro_only_assets = [s for s in LIQ_SNAP_SYMBOLS if s not in priority_level_assets]
    for sym in macro_only_assets[:4]:  # Harvest top macro assets per cycle
        exs = LIQ_EXCHANGES_MAP.get(sym, ["Bybit"])
        logger.info(f"[Liquidations] Harvesting macro snapshot for {sym} ({exs})...")
        snaps, _, key_lvls = harvest_asset_liquidations(liq_page, sym, MT5_ASSET_MAP.get(sym, ""), target_exchanges=exs, is_deep_level=False)
        all_snapshots.extend(snaps)
        if key_lvls:
            upsert_asset_key_levels(sym, key_lvls)
            total_key_levels_count += len(key_lvls)

    upsert_liquidation_snapshots(all_snapshots)

    return {
        "signals_count": len(all_signals),
        "candles_updated": len(FP_TARGET_SYMBOLS),
        "liquidation_snapshots_count": len(all_snapshots),
        "liquidation_levels_count": total_levels_count,
        "key_levels_count": total_key_levels_count
    }


def run_continuous_collector():
    """Main continuous loop: Multi-stream Dump -> Rotate Cloudflare IP -> 2 min rest -> Repeat."""
    logger.info("Initializing CoinGlass Master Data Collector...")
    DB_DIR.mkdir(parents=True, exist_ok=True)
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    LIQ_DIR.mkdir(parents=True, exist_ok=True)
    LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    KEY_LEVELS_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            logger.error(f"Cannot connect to CDP at {CDP_URL}: {e}")
            logger.error("Please ensure Edge/Chrome is running with: --remote-debugging-port=9222")
            sys.exit(1)

        context = browser.contexts[0] if browser.contexts else browser.new_context()

        cycle_count = 1
        while True:
            logger.info(f"========== STARTING MASTER DATA DUMP CYCLE #{cycle_count} ==========")
            start_time = time.time()

            try:
                stats = run_cycle(context)
                elapsed = round(time.time() - start_time, 1)
                logger.info(f"Cycle #{cycle_count} completed in {elapsed}s: {stats['signals_count']} signals, {stats['candles_updated']} candle files, {stats['liquidation_snapshots_count']} snapshots, {stats['liquidation_levels_count']} liquidation levels.")
            except Exception as e:
                logger.error(f"Error during cycle #{cycle_count}: {e}", exc_info=True)

            # Rotate Cloudflare IP after every cycle
            rotate_cloudflare_ip()

            # 2 minutes rest before next cycle
            logger.info(f"Resting for {CYCLE_REST_SECONDS} seconds (2 minutes) before starting cycle #{cycle_count + 1}...")
            time.sleep(CYCLE_REST_SECONDS)
            cycle_count += 1


if __name__ == "__main__":
    run_continuous_collector()
