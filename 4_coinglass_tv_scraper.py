"""
4_coinglass_tv_scraper.py
========================================================================================
Standalone CoinGlass TV Liquidation & Reversal Spike Scraper (In-Browser CDP Bridge)
========================================================================================
Pillar 4 of 5 Standalone Crypto Pipeline:
- Strictly operates via Chrome DevTools Protocol (CDP) on 127.0.0.1:9222.
- In-Chart TradingView API Bridge: opens ONE ephemeral tab (?bot=tv_scraper) and switches
  symbols sub-second via window.chartWidgetCollection.setSymbol(tv_sym).
- Speed Benchmark: ~0.50s per asset (full 36-asset sweep in under 20 seconds).
- Harvests:
  1. 15-minute liquidation timeseries (long liq USD, short liq USD)
  2. Rolling 50-bar 90th percentile threshold spike detection
  3. Reversal Bias (BULLISH_REVERSAL seller capitulation / BEARISH_REVERSAL short squeeze)
  4. Exhaustion Intensity Score (50–100)
- Persists to:
  * Database/Live_Liquidations/<SYMBOL>_tv_liquidations.parquet (capped at 10,000 bars)
  * Database/Live_Liquidations/latest_reversal_signals.json
  * Database/Live_Liquidations/summary_liquidations.parquet
- Primary IST columns enriched across all outputs.
- Runs every 60 seconds in daemon mode or on-demand (--once).
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
import numpy as np
import websockets

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from coinglass_common import acquire_singleton, atomic_parquet, atomic_json, cdp_result
from coinglass_common import BrowserLock, rotate_warp_ip, is_scheduled_for_other_collector
from coinglass_common import utc_ms_to_ist_str

DB_DIR = ROOT_DIR / "Database"
LIVE_LIQ_DIR = DB_DIR / "Live_Liquidations"
LIVE_LIQ_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_JSON_FILE = LIVE_LIQ_DIR / "latest_reversal_signals.json"
SUMMARY_PARQUET_FILE = LIVE_LIQ_DIR / "summary_liquidations.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [CG-TV] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("TVLiquidationScraper")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

DEFAULT_SYMBOL_MAP = {
    "BTC": "Binance_BTCUSDT",
    "ETH": "Binance_ETHUSDT",
    "SOL": "Binance_SOLUSDT",
    "XRP": "Binance_XRPUSDT",
    "DOGE": "Binance_DOGEUSDT",
    "BNB": "Binance_BNBUSDT",
    "ADA": "Binance_ADAUSDT",
    "AVAX": "Binance_AVAXUSDT",
    "LINK": "Binance_LINKUSDT",
    "SUI": "Binance_SUIUSDT",
    "NEAR": "Binance_NEARUSDT",
    "TRX": "Binance_TRXUSDT",
    "LTC": "Binance_LTCUSDT",
    "HYPE": "Bybit_HYPEUSDT",
    "DOT": "Binance_DOTUSDT",
    "BCH": "Binance_BCHUSDT",
    "UNI": "Binance_UNIUSDT",
    "APT": "Binance_APTUSDT",
    "ATOM": "Binance_ATOMUSDT",
    "ARB": "Binance_ARBUSDT",
    "OP": "Binance_OPUSDT",
    "FIL": "Binance_FILUSDT"
}


def load_tv_symbols() -> Dict[str, str]:
    sym_map_file = DB_DIR / "coinglass_tv_symbol_map.json"
    if sym_map_file.exists():
        try:
            with open(sym_map_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_SYMBOL_MAP


def cleanup_orphan_tabs(keep_tab_id: Optional[str] = None):
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "bot=tv_scraper" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Reaped orphaned TV tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"TV tab reaper: {e}")


def open_tv_tab() -> Tuple[Optional[str], Optional[str]]:
    cleanup_orphan_tabs()
    try:
        url = "https://www.coinglass.com/tv/Binance_BTCUSDT?bot=tv_scraper"
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=6)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened fresh ephemeral TV tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open TV tab via CDP: {e}")
        return None, None


def close_tab(tab_id: Optional[str]):
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed TV tab ({tab_id}) - Browser clean.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    cleanup_orphan_tabs()


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False, context_id: Optional[int] = None):
    params = {"expression": expr, "returnByValue": True, "awaitPromise": await_promise}
    if context_id is not None:
        params["contextId"] = context_id
    return await cdp_result(ws, {"id": msg_id, "method": "Runtime.evaluate", "params": params}, timeout=12)


async def setup_cdp_session(ws) -> bool:
    """Attaches to execution contexts and ensures TradingView chart widget and liquidation study are active."""
    check_chart = """(() => {
        const iframe = document.querySelector('iframe[id^=tradingview_]');
        const win = iframe ? iframe.contentWindow : window;
        const cwc = win.chartWidgetCollection || window.chartWidgetCollection;
        return !!cwc;
    })()"""
    chart_ready = False
    for attempt in range(40):
        eval_res = await cdp_eval(ws, check_chart, msg_id=10 + attempt)
        if eval_res is True:
            chart_ready = True
            break
        await asyncio.sleep(0.5)

    if not chart_ready:
        logger.error("TradingView chart widget not ready within 20.0s.")
        return False

    ensure_study = """(async () => {
        const iframe = document.querySelector('iframe[id^=tradingview_]');
        const win = iframe ? iframe.contentWindow : window;
        const cwc = win.chartWidgetCollection || window.chartWidgetCollection;
        if (!cwc) return { ok: false, error: "no_cwc" };
        const chart = cwc.activeChartWidget ? cwc.activeChartWidget.value() : cwc.getAll()[0];
        if (!chart) return { ok: false, error: "no_chart" };
        const model = chart.model().model ? chart.model().model() : chart.model();
        let study = model.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
        if (!study) {
            const inserter = model.createStudyInserter({ type: 'java', studyId: 'aggregated_liq_kline@tv-basicstudies' });
            await inserter.insert();
            study = model.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
        }
        return { ok: true, studyId: study ? study.id() : null };
    })()"""
    inserted = await cdp_eval(ws, ensure_study, msg_id=50, await_promise=True)
    if not inserted or not inserted.get("ok"):
        logger.warning(f"Could not ensure liquidation study: {inserted}")
        return False

    logger.info("TradingView chart collection and liquidation study ready.")
    return True


async def fetch_symbol_liquidations(ws, tv_sym: str) -> Tuple[List[List], float]:
    """Switches symbol inside TradingView chart widget and harvests raw liquidation series."""
    script = f"""(() => {{
        return new Promise((resolve) => {{
            try {{
                const iframe = document.querySelector('iframe[id^=tradingview_]');
                const win = iframe ? iframe.contentWindow : window;
                const cwc = win.chartWidgetCollection || window.chartWidgetCollection;
                if (!cwc) return resolve({{ ok: false, error: "no_cwc" }});
                
                cwc.setSymbol("{tv_sym}");
                let checks = 0;
                const interval = setInterval(() => {{
                    checks++;
                    const chart = cwc.activeChartWidget ? cwc.activeChartWidget.value() : cwc.getAll()[0];
                    const model = chart.model().model ? chart.model().model() : chart.model();
                    const ms = model.mainSeries();
                    const currentSym = ms.symbol ? ms.symbol() : '';
                    const study = model.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
                    const dataObj = study ? study.data() : null;
                    const size = dataObj ? dataObj.size() : 0;
                    
                    if (currentSym === "{tv_sym}" && size > 10) {{
                        clearInterval(interval);
                        const rows = [];
                        const firstIdx = dataObj.firstIndex();
                        const lastIdx = dataObj.lastIndex();
                        for (let i = firstIdx; i <= lastIdx; i++) {{
                            const liq = dataObj.valueAt(i);
                            if (liq && liq[0]) {{
                                rows.push([
                                    liq[0], // timestamp sec
                                    liq[1] !== null ? Math.abs(liq[1]) : 0.0, // long liq
                                    liq[2] !== null ? Math.abs(liq[2]) : 0.0  // short liq
                                ]);
                            }}
                        }}
                        const lastBar = ms.data().last();
                        let lastPrice = 0.0;
                        if (lastBar) {{
                            lastPrice = lastBar[4] !== undefined ? lastBar[4] : (lastBar.value && lastBar.value[4] !== undefined ? lastBar.value[4] : 0.0);
                        }}
                        if (!Number.isFinite(lastPrice)) lastPrice = 0;
                        return resolve({{ ok: true, symbol: currentSym, rows: rows, lastPrice: lastPrice }});
                    }}
                    if (checks >= 35) {{
                        clearInterval(interval);
                        return resolve({{ ok: false, symbol: "{tv_sym}", error: "timeout" }});
                    }}
                }}, 50);
            }} catch(e) {{
                return resolve({{ ok: false, error: e.message }});
            }}
        }});
    }})()"""
    res = await cdp_eval(ws, script, msg_id=int(time.time() * 1000) % 1000000, await_promise=True)
    if not res or not res.get("ok"):
        return [], 0.0
    return res.get("rows", []), float(res.get("lastPrice", 0.0))


def process_and_persist(base_sym: str, tv_sym: str, raw_rows: List[List], last_price: float = 0.0) -> Optional[Dict]:
    if not raw_rows:
        return None
    new_df = pd.DataFrame(raw_rows, columns=["timestamp", "long_liq_usd", "short_liq_usd"])
    for col in new_df.columns:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce").fillna(0.0)

    out_file = LIVE_LIQ_DIR / f"{base_sym}_tv_liquidations.parquet"
    if out_file.exists():
        try:
            existing_df = pd.read_parquet(out_file)
            core_cols = ["timestamp", "long_liq_usd", "short_liq_usd"]
            if all(c in existing_df.columns for c in core_cols):
                combined = pd.concat([existing_df[core_cols], new_df[core_cols]], ignore_index=True)
            else:
                combined = new_df
        except Exception:
            combined = new_df
    else:
        combined = new_df

    df = combined.sort_values("timestamp", kind="stable").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    if len(df) > 10000:
        df = df.iloc[-10000:].reset_index(drop=True)

    df["total_liq_usd"] = df["long_liq_usd"] + df["short_liq_usd"]
    df["net_liq_usd"] = df["long_liq_usd"] - df["short_liq_usd"]
    df["long_ratio"] = np.where(df["total_liq_usd"] > 0, df["long_liq_usd"] / df["total_liq_usd"], 0.5)

    # Primary IST Columns
    df["time_ms"] = (df["timestamp"] * 1000).astype("int64")
    df["open_time_ms"] = df["time_ms"]
    df["close_time_ms"] = df["time_ms"] + 900000
    df["open_time_ist"] = df["time_ms"].apply(utc_ms_to_ist_str)
    df["close_time_ist"] = df["close_time_ms"].apply(utc_ms_to_ist_str)
    df["time_ist"] = df["open_time_ist"]
    df["candle_ist"] = df["open_time_ist"]
    df["datetime_ist"] = df["open_time_ist"]
    df["open_time_ist_ms"] = df["time_ms"] + 19800000
    df["close_time_ist_ms"] = df["close_time_ms"] + 19800000
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["bar_closed"] = df["close_time_ms"] <= int(time.time() * 1000)

    df["symbol"] = base_sym
    df["coinglass_symbol"] = tv_sym
    df["close_price"] = float(last_price) if last_price > 0 else np.nan

    # Rolling 50-bar 90th percentile threshold for spikes
    min_spike_floor = 50000.0 if base_sym in ["BTC", "ETH", "SOL"] else 15000.0
    rolling_p90 = df["total_liq_usd"].shift(1).rolling(50, min_periods=10).quantile(0.90).fillna(min_spike_floor)
    df["p90_threshold"] = np.maximum(rolling_p90, min_spike_floor)
    df["is_spike"] = (df["total_liq_usd"] >= df["p90_threshold"]) & (df["total_liq_usd"] > min_spike_floor)

    # Reversal bias logic
    df["reversal_bias"] = "NEUTRAL"
    df.loc[df["is_spike"] & (df["long_ratio"] >= 0.70), "reversal_bias"] = "BULLISH_REVERSAL"
    df.loc[df["is_spike"] & (df["long_ratio"] <= 0.30), "reversal_bias"] = "BEARISH_REVERSAL"

    atomic_parquet(df, out_file)

    closed_df = df[df["bar_closed"]]
    if closed_df.empty:
        return None
    latest = closed_df.iloc[-1]
    prev = closed_df.iloc[-2] if len(closed_df) > 1 else latest
    recent_spike = bool(latest["is_spike"] or prev["is_spike"])
    reversal_bias = latest["reversal_bias"] if latest["reversal_bias"] != "NEUTRAL" else prev["reversal_bias"]

    max_liq = max(latest["total_liq_usd"], prev["total_liq_usd"])
    p90 = latest["p90_threshold"]
    exhaustion_score = 0.0
    if recent_spike and p90 > 0:
        ratio = max_liq / p90
        exhaustion_score = min(100.0, float(50.0 + (ratio - 1.0) * 25.0))

    spike_bar = latest if latest["reversal_bias"] != "NEUTRAL" else prev
    return {
        "symbol": base_sym,
        "coinglass_symbol": tv_sym,
        "last_price": float(last_price) if last_price > 0 else None,
        "total_liq_usd": float(latest["total_liq_usd"]),
        "long_liq_usd": float(latest["long_liq_usd"]),
        "short_liq_usd": float(latest["short_liq_usd"]),
        "long_ratio": float(latest["long_ratio"]),
        "p90_threshold": float(latest["p90_threshold"]),
        "is_spike": recent_spike,
        "reversal_bias": reversal_bias,
        "exhaustion_score": exhaustion_score,
        "spike_bar_time_utc": str(spike_bar["datetime_utc"]),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "updated_at_ist": utc_ms_to_ist_str(int(time.time() * 1000)),
        "total_candles": int(len(df)),
        "closed_candles": int(len(closed_df)),
        "bar_closed": True,
        "candle_open_ms": int(latest["time_ms"]),
        "candle_close_ms": int(latest["close_time_ms"]),
        "candle_ist": str(latest["candle_ist"]),
        # Evaluator-facing aliases (previously read as missing/neutral).
        "recent_spike": recent_spike,
        "latest_total_liq_usd": float(latest["total_liq_usd"]),
        "latest_long_liq_usd": float(latest["long_liq_usd"]),
        "latest_short_liq_usd": float(latest["short_liq_usd"])
    }


async def run_sweep(symbols: Optional[Dict[str, str]] = None) -> bool:
    target_map = symbols or load_tv_symbols()
    acquire_singleton("coinglass_tv_scraper", DB_DIR / ".locks")

    with BrowserLock("TVLiquidationScraper", timeout_secs=15.0) as lock:
        if not lock.acquired:
            logger.info("Browser lock contention for TV sweep. Skipping.")
            return False

        tab_id, ws_url = open_tv_tab()
        if not tab_id or not ws_url:
            logger.error("Could not open TV tab via CDP.")
            return False

        try:
            async with websockets.connect(ws_url, open_timeout=8, close_timeout=3, max_size=50_000_000, ping_interval=None) as ws:
                ok = await setup_cdp_session(ws)
                if not ok:
                    logger.error("Failed to setup TradingView session.")
                    return False

                summary_records = []
                reversal_signals = {}

                t0 = time.time()
                for base_sym, tv_sym in target_map.items():
                    try:
                        raw_rows, last_price = await fetch_symbol_liquidations(ws, tv_sym)
                        summary = process_and_persist(base_sym, tv_sym, raw_rows, last_price)
                        if summary:
                            summary_records.append(summary)
                            spike_tag = f" [SPIKE: {summary['reversal_bias']}]" if summary.get("is_spike") and summary.get("reversal_bias") != "NEUTRAL" else ""
                            logger.info(f"Harvested {base_sym} ({tv_sym}): total {summary.get('total_candles', len(raw_rows))} bars, last price: {last_price}{spike_tag}")
                            if summary.get("is_spike") and summary.get("reversal_bias") != "NEUTRAL":
                                reversal_signals[base_sym] = summary
                    except Exception as e:
                        logger.error(f"[{base_sym}] Error: {repr(e)}")

                elapsed = time.time() - t0
                logger.info(f"TV sweep complete across {len(target_map)} assets in {elapsed:.1f}s.")

                # Persist summary parquet and reversal signals json
                if summary_records:
                    sum_df = pd.DataFrame(summary_records)
                    atomic_parquet(sum_df, SUMMARY_PARQUET_FILE)
                if reversal_signals:
                    atomic_json(SIGNALS_JSON_FILE, reversal_signals)
                    logger.info(f"🚨 Recorded {len(reversal_signals)} active liquidation reversal spikes!")
                return True
        except Exception as e:
            logger.error(f"WebSocket execution error: {type(e).__name__}: {e}")
            return False
        finally:
            close_tab(tab_id)


async def run_daemon(interval_seconds: int = 60):
    acquire_singleton("coinglass_tv_scraper", DB_DIR / ".locks")
    logger.info(f"CoinGlass TV Liquidation Scraper Daemon active (interval={interval_seconds}s)...")

    while True:
        now = datetime.now()
        cur_min = now.minute
        cur_sec = now.second
        # Yield on reserved Whale/Liquidation minutes
        if is_scheduled_for_other_collector(cur_min, "tv"):
            await asyncio.sleep(20.0)
            continue
        # TDMA Tier 2: TV Scraper strictly runs in second window :00 - :25
        if cur_sec > 25:
            await asyncio.sleep(max(1.0, 60.0 - cur_sec))
            continue

        start_time = time.time()
        try:
            await run_sweep()
        except Exception as e:
            logger.error(f"TV sweep error: {e}")

        elapsed = time.time() - start_time
        sleep_dur = max(5.0, interval_seconds - elapsed)
        await asyncio.sleep(sleep_dur)


def main():
    parser = argparse.ArgumentParser(description="CoinGlass TV Liquidation Scraper")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background (every 60s)")
    parser.add_argument("--once", action="store_true", help="Run single sweep and exit")
    parser.add_argument("--interval", type=int, default=60, help="Daemon cycle interval in seconds (default: 60)")
    args = parser.parse_args()

    if args.daemon:
        asyncio.run(run_daemon(args.interval))
    else:
        asyncio.run(run_sweep())


if __name__ == "__main__":
    main()
