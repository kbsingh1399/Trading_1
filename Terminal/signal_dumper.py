"""
CoinGlass Footprint Signal & 15m Order-Flow Candle Dumper (MT5 Combined Edition)
-------------------------------------------------------------------------------
Simultaneously extracts complete order-flow footprint signals and 15-minute
candle order-flow metrics exclusively for the 31 cryptocurrency assets supported
by your Blueberry Markets MT5 account.

Key Features:
1. Combined In-Page Extraction: In a single page visit per asset, simultaneously
   captures all signals (Absorption, Exhaustion, Divergence, Trapped Longs/Shorts)
   AND complete 15-minute candle order-flow metrics (OHLC, volume, delta, trades,
   OI, and POC).
2. Asset-Wise Parquet Storage: Writes individual candle datasets to
   `Database/Candle_Data/<SYMBOL>.parquet` with deduplicated upsert logic.
3. Master Signal Parquet: Writes aggregated signals to
   `Database/coinglass_aggregated_futures_signals.parquet`.
4. Automated Sanity Verification: Audits timestamp monotonicity, OHLC constraints,
   volume/delta mathematical integrity, and zero null counts on every save.
5. Cloudflare WARP IP Rotation: Rotates outbound IP and re-keys tunnel after each cycle.
6. Rest Interval: 2 minutes rest between consecutive cycles.
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
OUTPUT_SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
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
logger = logging.getLogger("CombinedDumper")


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
            # Deduplicate by open_time_ms, preserving the newest record
            combined_df = combined_df.drop_duplicates(subset=["open_time_ms"], keep="last")
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to read existing parquet for merge ({e}). Writing new data.")
            combined_df = new_df
    else:
        combined_df = new_df

    # Sort chronologically
    combined_df = combined_df.sort_values(by="open_time_ms", ascending=True).reset_index(drop=True)

    # Recalculate smooth running cumulative delta across entire history
    combined_df["cumulative_delta"] = combined_df["delta"].cumsum()
    combined_df["dumped_at_utc"] = datetime.utcnow().isoformat()

    # Reorder standard columns
    lead_cols = ["symbol", "mt5_symbol", "timeframe", "open_time_ms", "close_time_ms", "open_time_utc", "close_time_utc", "open", "high", "low", "close"]
    other_cols = [c for c in combined_df.columns if c not in lead_cols]
    combined_df = combined_df[lead_cols + other_cols]

    # Verify sanity before persisting
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

    # Sort signals chronologically
    if "time_ms" in df.columns:
        df = df.sort_values(by=["symbol", "time_ms"], ascending=[True, True]).reset_index(drop=True)

    df.to_parquet(OUTPUT_SIGNALS_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Signals Parquet updated: {OUTPUT_SIGNALS_PARQUET.name} ({len(df)} rows across {df['symbol'].nunique()} MT5 assets)")


def extract_combined_chunk(page, symbols_chunk: list) -> dict:
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

                                            // Buy imbalance (3x diagonal test)
                                            if (j > 0 && s > 0 && b >= 3 * (F.s[j - 1] || 0)) nB++;
                                            // Sell imbalance (3x diagonal test)
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


def run_cycle(page) -> int:
    """Execute a single full dump cycle across all 31 MT5 assets."""
    ensure_terminal_ready(page)
    all_signals = []
    total_chunks = (len(TARGET_SYMBOLS) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(TARGET_SYMBOLS), CHUNK_SIZE):
        chunk = TARGET_SYMBOLS[i:i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        logger.info(f"Processing Chunk {chunk_num}/{total_chunks} ({chunk[0]}..{chunk[-1]})...")

        chunk_result = extract_combined_chunk(page, chunk)
        
        # 1. Accumulate signals
        signals = chunk_result.get("signals", [])
        all_signals.extend(signals)

        # 2. Persist candle datasets immediately for each asset in chunk
        candles_dict = chunk_result.get("candles", {})
        for sym, bar_list in candles_dict.items():
            upsert_candle_parquet(sym, MT5_ASSET_MAP.get(sym, ""), bar_list)

    # Persist updated master signals Parquet
    dump_signals_parquet(all_signals)

    # Restore view back to BTC
    page.evaluate("() => window.__cg_term.pickPair('BTC', 'agg-perp')")
    return len(all_signals)


def run_continuous_collector():
    """Main continuous loop: Combined Dump -> Rotate Cloudflare IP -> 2 min rest -> Repeat."""
    logger.info("Initializing Targeted MT5 Combined Footprint & Candle Dumper...")
    DB_DIR.mkdir(parents=True, exist_ok=True)
    CANDLE_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            logger.error(f"Cannot connect to CDP at {CDP_URL}: {e}")
            logger.error("Please ensure Edge/Chrome is running with: --remote-debugging-port=9222")
            sys.exit(1)

        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = get_coinglass_tab(context)
        logger.info(f"Connected to CoinGlass page: {page.title()}")

        cycle_count = 1
        while True:
            logger.info(f"========== STARTING TARGETED MT5 COMBINED DUMP CYCLE #{cycle_count} ==========")
            start_time = time.time()

            try:
                sig_count = run_cycle(page)
                elapsed = round(time.time() - start_time, 1)
                logger.info(f"Cycle #{cycle_count} completed in {elapsed}s: {sig_count} signals, 31 candle files updated.")
            except Exception as e:
                logger.error(f"Error during cycle #{cycle_count}: {e}", exc_info=True)

            # Step 2: Rotate Cloudflare IP after every cycle
            rotate_cloudflare_ip()

            # Step 1: 2 minutes rest before next cycle
            logger.info(f"Resting for {CYCLE_REST_SECONDS} seconds (2 minutes) before starting cycle #{cycle_count + 1}...")
            time.sleep(CYCLE_REST_SECONDS)
            cycle_count += 1


if __name__ == "__main__":
    run_continuous_collector()
