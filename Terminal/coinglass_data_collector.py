"""
CoinGlass Master Data Collector (Footprint Signals, 15m Candles, & Liquidation Data)
------------------------------------------------------------------------------------
Automates continuous, multi-stream data collection across the 31 cryptocurrency
assets supported by your Blueberry Markets MT5 trading account:

Data Streams Collected:
1. Footprint Order-Flow Signals:
   - Trapped Longs, Trapped Shorts, Absorption, Exhaustion, Divergence
   - Stored in: `Database/coinglass_aggregated_futures_signals.parquet`
2. 15-Minute Order-Flow Candle Datasets:
   - Full OHLC, Volume (Buy/Sell/Total), Delta, Cumulative Delta, Trades,
     Avg Trade Size, Buy/Sell Ratio, POC Price/Volume/Delta, Open Interest (Open/Close/Change)
   - Stored in: `Database/Candle_Data/<SYMBOL>.parquet` (Deduplicated Upsert)
3. Liquidation Intelligence (Snapshots & Discrete Strike Levels):
   - Macro Snapshots: Nearest Short/Long Magnet Zones, CLLD (Cumulative Delta),
     Sentiment turns, Near-field balance, Distance bands, and Leverage breakdown.
     Stored in: `Database/Liquidation_Data/liquidation_snapshots.parquet`
   - Granular Strike Levels: Exact liquidation trigger prices, sides, distance %,
     leverage tiers, volume, strength ratings, and active status.
     Stored in: `Database/Liquidation_Data/liquidation_levels.parquet`
4. Reliability & Network:
   - Automated Cloudflare WARP IP rotation after every cycle.
   - 2-minute configurable rest interval between cycles.
"""

import sys
import os
import time
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Configuration & MT5 Asset Mapping
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"

OUTPUT_SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
LEVELS_PARQUET = LIQ_DIR / "liquidation_levels.parquet"

WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")
CDP_URL = "http://127.0.0.1:9222"
CYCLE_REST_SECONDS = 120  # 2 minutes rest between cycles
CHUNK_SIZE = 10           # Assets per in-page batch chunk

# Verified mapping: CoinGlass symbol -> Blueberry Markets MT5 ticker
MT5_ASSET_MAP = {
    "BTC": "BTCUSD.pi",
    "ETH": "ETHUSD.pi",
    "SOL": "SOLUSD.p",
    "XRP": "XRPUSD.pi",
    "DOGE": "DOGUSD.p",
    "BNB": "BNBUSD.p",
    "ADA": "ADAUSD.p",
    "LINK": "LNKUSD.p",
    "AVAX": "AVXUSD.p",
    "LTC": "LTCUSD.pi",
    "DOT": "DOTUSD.pi",
    "BCH": "BCHUSD.p",
    "NEAR": "NERUSD.p",
    "TRX": "TRXUSD.p",
    "UNI": "UNIUSD.p",
    "AAVE": "AVEUSD.p",
    "FIL": "FILUSD.p",
    "CRV": "CRVUSD.p",
    "ATOM": "ATMUSD.p",
    "ALGO": "ALGUSD.p",
    "AXS": "AXSUSD.p",
    "BAT": "BATUSD.p",
    "CHZ": "CHZUSD.p",
    "AR": "ARWUSD.p",
    "COMP": "COMUSD.p",
    "1INCH": "INCUSD.p",
    "SUSHI": "XSIUSD.p",
    "SNX": "SNXUSD.p",
    "NEO": "NEOUSD.p",
    "ZEC": "ZECUSD.p",
    "HBAR": "BARUSD.p"
}

TARGET_SYMBOLS = list(MT5_ASSET_MAP.keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DataCollector")


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
    combined_df["dumped_at_utc"] = datetime.utcnow().isoformat()

    lead_cols = ["symbol", "mt5_symbol", "timeframe", "open_time_ms", "close_time_ms", "open_time_utc", "close_time_utc", "open", "high", "low", "close"]
    other_cols = [c for c in combined_df.columns if c not in lead_cols]
    combined_df = combined_df[lead_cols + other_cols]

    if verify_candle_sanity(combined_df, symbol):
        combined_df.to_parquet(parquet_path, index=False, engine="pyarrow")
        logger.info(f"[{symbol}] Saved {parquet_path.name}: {len(combined_df)} candles (range: {combined_df['open_time_utc'].iloc[0][:16]} -> {combined_df['open_time_utc'].iloc[-1][:16]})")
    else:
        logger.error(f"[{symbol}] Aborting parquet write due to sanity check failure!")


def dump_signals_parquet(signals: list):
    """Persist aggregated signals across all MT5 assets."""
    if not signals:
        logger.warning("Empty signal set received. Skipping signals parquet write.")
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(signals)
    df["dumped_at_utc"] = datetime.utcnow().isoformat()

    cols = ["symbol", "mt5_symbol"] + [c for c in df.columns if c not in ["symbol", "mt5_symbol"]]
    df = df[cols]

    if "time_ms" in df.columns:
        df = df.sort_values(by=["symbol", "time_ms"], ascending=[True, True]).reset_index(drop=True)

    df.to_parquet(OUTPUT_SIGNALS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Signals Parquet updated: {OUTPUT_SIGNALS_PARQUET.name} ({len(df)} rows across {df['symbol'].nunique()} MT5 assets)")


def extract_footprint_chunk(page, symbols_chunk: list) -> dict:
    """In-page combined extraction: captures signals AND 15m order flow candles."""
    combined_extract_js = """
    async (args) => {
        const { symbolsChunk, mt5Map } = args;
        const term = window.__cg_term;
        const result = { signals: [], candles: {} };

        for (const sym of symbolsChunk) {
            await new Promise((resolve) => {
                const timeout = setTimeout(() => {
                    unsub();
                    resolve();
                }, 8000);

                let loadedOlder = false;

                const unsub = term.subscribe(async () => {
                    if (term.status === 'ready' && term.prefs.sym === sym && !term.D.err) {
                        if (!loadedOlder) {
                            loadedOlder = true;
                            try {
                                await term.maybeOlder();
                            } catch (e) {}
                        }

                        const sigSets = term.sigSets();
                        if (sigSets && sigSets.length === 3) {
                            clearTimeout(timeout);
                            unsub();

                            // 1. Extract Signals across all bar types
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

                            // 2. Extract 15m Order-Flow Candle Data from timeSet
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
                                    let maxVol = -1, pocIdx = -1;
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
                    }
                });

                term.pickPair(sym, 'agg-perp');
            });
        }
        return result;
    }
    """
    return page.evaluate(combined_extract_js, {"symbolsChunk": symbols_chunk, "mt5Map": MT5_ASSET_MAP})


def extract_liquidation_data(page) -> dict:
    """Extract macro liquidation snapshot and all active discrete strike levels."""
    js_extractor = """
    () => {
        const nextEl = document.getElementById('__next');
        const containerKey = Object.keys(nextEl).find(k => k.startsWith('__reactContainer$'));
        const rootFiber = nextEl[containerKey];

        let targetFiber = null;
        function walk(f, depth = 0) {
            if (!f || depth > 60 || targetFiber) return;
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
        if (!m) return { error: "model_not_found" };

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

        // Strength by Distance bands
        let dist0_1_S = null, dist0_1_L = null;
        let dist1_2_S = null, dist1_2_L = null;
        let dist2_3_S = null, dist2_3_L = null;
        let dist3_5_S = null, dist3_5_L = null;
        let dist5_10_S = null, dist5_10_L = null;

        const d0_1 = bodyText.match(/0–1%\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (d0_1) { dist0_1_S = parseFloat(d0_1[1]); dist0_1_L = parseFloat(d0_1[2]); }
        const d1_2 = bodyText.match(/1–2%\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (d1_2) { dist1_2_S = parseFloat(d1_2[1]); dist1_2_L = parseFloat(d1_2[2]); }
        const d2_3 = bodyText.match(/2–3%\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (d2_3) { dist2_3_S = parseFloat(d2_3[1]); dist2_3_L = parseFloat(d2_3[2]); }
        const d3_5 = bodyText.match(/3–5%\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (d3_5) { dist3_5_S = parseFloat(d3_5[1]); dist3_5_L = parseFloat(d3_5[2]); }
        const d5_10 = bodyText.match(/5–10%\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (d5_10) { dist5_10_S = parseFloat(d5_10[1]); dist5_10_L = parseFloat(d5_10[2]); }

        // Leverage Tiers
        let highLev_S = null, highLev_L = null;
        let midLev_S = null, midLev_L = null;
        let lowLev_S = null, lowLev_L = null;

        const hl = bodyText.match(/High leverage\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (hl) { highLev_S = parseFloat(hl[1]); highLev_L = parseFloat(hl[2]); }
        const ml = bodyText.match(/Mid leverage\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (ml) { midLev_S = parseFloat(ml[1]); midLev_L = parseFloat(ml[2]); }
        const ll = bodyText.match(/Low leverage\\s*(\\d+)\\s*·\\s*(\\d+)/);
        if (ll) { lowLev_S = parseFloat(ll[1]); lowLev_L = parseFloat(ll[2]); }

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

                activeLevels.push({
                    price: Math.round(px * 10) / 10,
                    volume: Math.round(sz * 100) / 100,
                    side: side,
                    leverage_tier: lev,
                    distance_pct: Math.round(dist * 100) / 100,
                    strength: strength,
                    weight: Math.round(weight * 10000) / 10000,
                    is_active: true
                });
            }
        }

        const snapshot = {
            exchange: p.ex || 'Binance',
            symbol: 'BTC',
            mt5_symbol: 'BTCUSD.pi',
            pair: p.sym || 'BTCUSDT',
            lookback_tf: p.tf === 0 ? '3D-1m' : (p.tf === 1 ? '7D-3m' : '30D-15m'),
            tier: 'Tier 1',
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
            dist_0_1_short_strength: dist0_1_S,
            dist_0_1_long_strength: dist0_1_L,
            dist_1_2_short_strength: dist1_2_S,
            dist_1_2_long_strength: dist1_2_L,
            dist_2_3_short_strength: dist2_3_S,
            dist_2_3_long_strength: dist2_3_L,
            dist_3_5_short_strength: dist3_5_S,
            dist_3_5_long_strength: dist3_5_L,
            dist_5_10_short_strength: dist5_10_S,
            dist_5_10_long_strength: dist5_10_L,
            high_lev_short_strength: highLev_S,
            high_lev_long_strength: highLev_L,
            mid_lev_short_strength: midLev_S,
            mid_lev_long_strength: midLev_L,
            low_lev_short_strength: lowLev_S,
            low_lev_long_strength: lowLev_L,
            active_levels_count: activeLevels.length
        };

        return { snapshot, levels: activeLevels };
    }
    """
    return page.evaluate(js_extractor)


def upsert_liquidation_data(liq_result: dict):
    """Persist macro snapshots and discrete levels to Parquet."""
    if not liq_result or "error" in liq_result:
        return

    LIQ_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.utcnow().isoformat()

    # 1. Macro Snapshot
    snap = liq_result["snapshot"]
    snap["snapshot_time_utc"] = now_utc
    snap["dumped_at_utc"] = now_utc
    snap_df = pd.DataFrame([snap])

    if SNAPSHOTS_PARQUET.exists():
        try:
            existing_snap = pd.read_parquet(SNAPSHOTS_PARQUET)
            combined_snap = pd.concat([existing_snap, snap_df], ignore_index=True)
            combined_snap = combined_snap.drop_duplicates(subset=["snapshot_time_utc", "symbol", "exchange"], keep="last")
        except Exception as e:
            logger.warning(f"Failed to merge existing snapshots: {e}")
            combined_snap = snap_df
    else:
        combined_snap = snap_df

    combined_snap.to_parquet(SNAPSHOTS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Liquidation Snapshot saved: {SNAPSHOTS_PARQUET.name} ({len(combined_snap)} total snapshots)")

    # 2. Granular Strike Levels
    levels = liq_result["levels"]
    for lvl in levels:
        lvl["snapshot_time_utc"] = now_utc
        lvl["symbol"] = snap["symbol"]
        lvl["exchange"] = snap["exchange"]
        lvl["lookback_tf"] = snap["lookback_tf"]
        lvl["tier"] = snap["tier"]
        lvl["dumped_at_utc"] = now_utc

    levels_df = pd.DataFrame(levels)
    levels_df.to_parquet(LEVELS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Liquidation Levels saved: {LEVELS_PARQUET.name} ({len(levels_df)} discrete strike levels)")


def run_cycle(context) -> dict:
    """Execute a single full collection cycle across footprint and liquidation streams."""
    fp_page = get_coinglass_tab(context)
    ensure_terminal_ready(fp_page)

    all_signals = []
    total_chunks = (len(TARGET_SYMBOLS) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # Step 1: Footprint Signals & 15m Candles across all 31 MT5 assets
    for i in range(0, len(TARGET_SYMBOLS), CHUNK_SIZE):
        chunk = TARGET_SYMBOLS[i:i + CHUNK_SIZE]
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

    # Step 2: Liquidation Intelligence (Snapshots & Discrete Strike Levels)
    liq_page = get_liquidation_tab(context)
    logger.info("[Liquidations] Extracting snapshot & key levels from active liquidation tab...")
    liq_result = extract_liquidation_data(liq_page)
    upsert_liquidation_data(liq_result)

    return {
        "signals_count": len(all_signals),
        "candles_updated": len(TARGET_SYMBOLS),
        "liquidation_levels_count": len(liq_result.get("levels", []))
    }


def run_continuous_collector():
    """Main continuous loop: Multi-stream Dump -> Rotate Cloudflare IP -> 2 min rest -> Repeat."""
    logger.info("Initializing CoinGlass Master Data Collector...")
    DB_DIR.mkdir(parents=True, exist_ok=True)
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)
    LIQ_DIR.mkdir(parents=True, exist_ok=True)

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
                logger.info(f"Cycle #{cycle_count} completed in {elapsed}s: {stats['signals_count']} signals, {stats['candles_updated']} candle files, {stats['liquidation_levels_count']} liquidation levels.")
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
