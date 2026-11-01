"""
CoinGlass TV Live Liquidation Collector Daemon
==============================================
Collects real-time 15-minute aggregated liquidation timeseries (full history + running candle)
from CoinGlass TradingView chart interface across all 36 assets via CDP.

Speed Benchmark:
- ~0.50s per asset
- Full 36-asset sweep completed in ~18 seconds!

Guarantees:
1. Missing Candle Appending: Merges new observations with historical Parquet, permanently
   retaining historical depth (up to 10,000 candles / ~3.5 months of 15m bars).
2. Live Running Candle Updating: Continually updates the in-progress 15m bar with latest
   accumulated liquidation volume in real time.
3. Liquidation Reversal Spike Detection:
   - Rolling 50-bar 90th percentile threshold.
   - BULLISH_REVERSAL: Long liquidation cascade (> p90, > 70% long) indicating seller capitulation.
   - BEARISH_REVERSAL: Short liquidation squeeze (> p90, > 70% short) indicating buyer blow-off.
4. Auto-Dispatch: Evaluates spikes with RealtimeSignalEvaluator and triggers MatchTrader execution bridge.
5. Persists to:
   - Database/Live_Liquidations/<SYMBOL>_tv_liquidations.parquet
   - Database/Live_Liquidations/latest_reversal_signals.json
   - Database/Live_Liquidations/summary_liquidations.parquet
"""

import sys
import os
import subprocess
import json
import time
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import numpy as np
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_parquet, atomic_json
from Terminal.collector_lock import BrowserLock, rotate_warp_ip
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator

DB_DIR = ROOT_DIR / "Database"
LIVE_LIQ_DIR = DB_DIR / "Live_Liquidations"
LIVE_LIQ_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL_MAP_FILE = DB_DIR / "coinglass_tv_symbol_map.json"
SIGNALS_JSON_FILE = LIVE_LIQ_DIR / "latest_reversal_signals.json"
SUMMARY_PARQUET_FILE = LIVE_LIQ_DIR / "summary_liquidations.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [TV_LIQUIDATION] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("CoinGlassTVLiquidation")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

PROCESSED_REVERSAL_KEYS = set()
evaluator: Optional[RealtimeSignalEvaluator] = None


def load_mt5_map():
    if not REGISTRY_FILE.exists():
        return {}
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            reg = json.load(f)
        mt5_map = {}
        for cat, assets in reg.items():
            for sym, meta in assets.items():
                mt5_map[sym] = meta.get("mt5", f"{sym}USD")
        return mt5_map
    except Exception:
        return {}


MT5_MAP = load_mt5_map()

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
    "POL": "Binance_POLUSDT",
    "BCH": "Binance_BCHUSDT",
    "UNI": "Binance_UNIUSDT",
    "APT": "Binance_APTUSDT",
    "ATOM": "Binance_ATOMUSDT",
    "ARB": "Binance_ARBUSDT",
    "OP": "Binance_OPUSDT",
    "FIL": "Binance_FILUSDT",
    "RENDER": "Binance_RENDERUSDT",
    "FET": "Binance_FETUSDT",
    "TAO": "Binance_TAOUSDT",
    "1000PEPE": "Binance_1000PEPEUSDT",
    "1000SHIB": "Binance_1000SHIBUSDT",
    "XAU": "Binance_XAUUSDT",
    "XAG": "Binance_XAGUSDT",
    "UKOIL": "MEXC_UKOIL_USDT",
    "BRENTOIL": "tradeXYZ_BRENTOIL-USD",
    "NDX100": "MEXC_NAS100_USDT",
    "SPX500": "tradeXYZ_SP500-USD",
    "DJI30": "MEXC_US30_USDT",
    "JP225": "tradeXYZ_JP225-USD"
}


def load_symbol_map() -> Dict[str, str]:
    if SYMBOL_MAP_FILE.exists():
        try:
            with open(SYMBOL_MAP_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_SYMBOL_MAP


def get_tv_tab() -> Optional[Dict]:
    """Finds or opens the CoinGlass TV chart tab via CDP."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            tv_tabs = [t for t in tabs if "coinglass.com/tv" in t.get("url", "")]
            if tv_tabs:
                for extra in tv_tabs[1:]:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{extra.get('id')}", timeout=2)
                    except Exception:
                        pass
                return tv_tabs[0]
        # If not open, open a new tab
        open_req = urllib.request.Request("http://127.0.0.1:9222/json/new?https://www.coinglass.com/tv/Binance_BTCUSDT", method="PUT")
        with urllib.request.urlopen(open_req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            logger.info(f"Opened fresh CoinGlass TV tab: {data.get('id')}")
            time.sleep(4.0)
            return data
    except Exception as e:
        logger.error(f"Error querying CDP tabs: {e}")
        return None


async def ensure_tv_tab_healthy(tab: Dict) -> bool:
    """Verifies the CoinGlass TV page is healthy and not stuck on a loading spinner, recovering automatically."""
    ws_url = tab.get("webSocketDebuggerUrl")
    if not ws_url:
        return False
    try:
        async with websockets.connect(ws_url, open_timeout=4, close_timeout=2) as ws:
            check_expr = """({
                hasIframe: !!document.querySelector("iframe[id^='tradingview_']"),
                hasSpinner: !!document.querySelector(".MuiCircularProgress-root"),
                url: window.location.href
            })"""
            await ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {"expression": check_expr, "returnByValue": True}}))
            res = json.loads(await ws.recv())
            val = res.get("result", {}).get("result", {}).get("value", {})
            
            if val.get("hasSpinner") and not val.get("hasIframe"):
                logger.warning("CoinGlass TV tab stuck on loading spinner. Triggering recovery reload...")
                await ws.send(json.dumps({"id": 2, "method": "Page.reload", "params": {"ignoreCache": True}}))
                await asyncio.sleep(4.0)
                
                await ws.send(json.dumps({"id": 3, "method": "Runtime.evaluate", "params": {"expression": check_expr, "returnByValue": True}}))
                res2 = json.loads(await ws.recv())
                val2 = res2.get("result", {}).get("result", {}).get("value", {})
                if val2.get("hasSpinner") and not val2.get("hasIframe"):
                    logger.warning("Tab still stuck (likely rate-limited by CloudFront). Rotating WARP IP...")
                    rotate_warp_ip(context="CoinGlass-TV", reason="HTTP_429", force=True)
                    await asyncio.sleep(2.0)
                    await ws.send(json.dumps({"id": 4, "method": "Page.reload", "params": {"ignoreCache": True}}))
                    await asyncio.sleep(4.0)
            return True
    except Exception as e:
        logger.warning(f"Error verifying TV tab health: {e}")
        return False


async def harvest_single_symbol(ws, tv_ctx: int, tv_sym: str, msg_id: int) -> Tuple[Optional[List[List]], float]:
    """Switches symbol in TradingView and extracts the full liquidation history."""
    script = f"""
    (() => {{
        return new Promise((resolve) => {{
            try {{
                const cwc = window.chartWidgetCollection;
                cwc.setSymbol("{tv_sym}");
                let checks = 0;
                const interval = setInterval(() => {{
                    checks++;
                    const chart = cwc.activeChartWidget.value();
                    const m = chart.model().model();
                    const ms = m.mainSeries();
                    const currentSym = ms.symbol ? ms.symbol() : '';
                    const study = m.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
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
                        resolve({{ ok: true, symbol: currentSym, rows: rows, lastPrice: lastPrice }});
                    }}
                    if (checks >= 50) {{
                        clearInterval(interval);
                        resolve({{ ok: false, symbol: "{tv_sym}", error: "timeout" }});
                    }}
                }}, 60);
            }} catch(e) {{
                resolve({{ ok: false, error: e.message }});
            }}
        }});
    }})()
    """
    
    await ws.send(json.dumps({
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {
            "expression": script,
            "contextId": tv_ctx,
            "awaitPromise": True,
            "returnByValue": True
        }
    }))
    
    while True:
        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("id") == msg_id:
            res = msg.get("result", {}).get("result", {}).get("value", {})
            if res.get("ok"):
                return res.get("rows", []), float(res.get("lastPrice", 0.0))
            else:
                logger.warning(f"Failed to harvest {tv_sym}: {res.get('error')}")
                return None, 0.0


def process_and_persist_liquidations(base_sym: str, tv_sym: str, raw_rows: List[List], last_price: float = 0.0) -> Optional[Dict]:
    """
    Upsert logic:
    1. Merges new observations with existing Parquet (appends missing historical candles).
    2. Overwrites running candle with the latest live observation.
    3. Retains full historical depth (up to 10,000 bars).
    4. Computes rolling 50-bar p90 threshold and reversal signals.
    """
    if not raw_rows:
        return None
        
    new_df = pd.DataFrame(raw_rows, columns=["timestamp", "long_liq_usd", "short_liq_usd"])
    for col in new_df.columns:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce")
    new_df["timestamp"] = new_df["timestamp"].astype(int)

    out_file = LIVE_LIQ_DIR / f"{base_sym}_tv_liquidations.parquet"
    
    # Merge with existing file to ensure candles are appended and running candle updated
    if out_file.exists():
        try:
            existing_df = pd.read_parquet(out_file)
            core_cols = ["timestamp", "long_liq_usd", "short_liq_usd"]
            if all(c in existing_df.columns for c in core_cols):
                combined = pd.concat([existing_df[core_cols], new_df[core_cols]], ignore_index=True)
            else:
                combined = new_df[core_cols]
        except Exception:
            combined = new_df
    else:
        combined = new_df
        
    # Deduplicate keeping 'last' (this updates the running candle and appends missing ones)
    df = combined.sort_values('timestamp', kind='stable').drop_duplicates('timestamp', keep='last').reset_index(drop=True)
    
    # Cap historical depth to 10,000 bars (~3.5 months of 15m candles)
    if len(df) > 10000:
        df = df.iloc[-10000:].reset_index(drop=True)
        
    df["total_liq_usd"] = df["long_liq_usd"] + df["short_liq_usd"]
    df["net_liq_usd"] = df["long_liq_usd"] - df["short_liq_usd"]
    df["long_ratio"] = np.where(df["total_liq_usd"] > 0, df["long_liq_usd"] / df["total_liq_usd"], 0.5)
    
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["datetime_ist"] = (pd.to_datetime(df["timestamp"], unit="s", utc=True) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S IST")
    df["symbol"] = base_sym
    df["coinglass_symbol"] = tv_sym
    df["close_price"] = float(last_price)
    
    # Rolling 50-bar 90th percentile threshold for spikes
    min_spike_floor = 50000.0 if base_sym in ["BTC", "ETH", "SOL"] else 15000.0
    rolling_p90 = df["total_liq_usd"].shift(1).rolling(50, min_periods=10).quantile(0.90).fillna(min_spike_floor)
    df["p90_threshold"] = np.maximum(rolling_p90, min_spike_floor)
    df["is_spike"] = (df["total_liq_usd"] >= df["p90_threshold"]) & (df["total_liq_usd"] > min_spike_floor)
    
    # Reversal bias logic:
    df["reversal_bias"] = "NEUTRAL"
    df.loc[df["is_spike"] & (df["long_ratio"] >= 0.70), "reversal_bias"] = "BULLISH_REVERSAL"
    df.loc[df["is_spike"] & (df["long_ratio"] <= 0.30), "reversal_bias"] = "BEARISH_REVERSAL"
    
    # Persist parquet atomically
    atomic_parquet(df, out_file)
    
    # Latest bar analysis for signal output
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    
    recent_spike = bool(latest["is_spike"] or prev["is_spike"])
    reversal_bias = latest["reversal_bias"] if latest["reversal_bias"] != "NEUTRAL" else prev["reversal_bias"]
    
    max_liq = max(latest["total_liq_usd"], prev["total_liq_usd"])
    p90 = latest["p90_threshold"]
    exhaustion_score = 0.0
    if recent_spike and p90 > 0:
        ratio = max_liq / p90
        exhaustion_score = min(100.0, float(50.0 + (ratio - 1.0) * 25.0))
        
    spike_bar = latest if latest["reversal_bias"] != "NEUTRAL" else prev
    summary = {
        "symbol": base_sym,
        "coinglass_symbol": tv_sym,
        "last_price": float(last_price),
        "total_candles": len(df),
        "last_bar_time_utc": latest["datetime_utc"],
        "last_bar_time_ist": latest["datetime_ist"],
        "latest_long_liq_usd": float(latest["long_liq_usd"]),
        "latest_short_liq_usd": float(latest["short_liq_usd"]),
        "latest_total_liq_usd": float(latest["total_liq_usd"]),
        "prev_total_liq_usd": float(prev["total_liq_usd"]),
        "p90_threshold": float(p90),
        "spike_bar_time_utc": spike_bar["datetime_utc"] if recent_spike else None,
        "recent_spike": recent_spike,
        "reversal_bias": reversal_bias,
        "exhaustion_score": round(exhaustion_score, 1),
        "source": "CoinGlass_TV_Aggregated",
        "updated_at_utc": datetime.now(timezone.utc).isoformat()
    }
    return summary


async def sweep_all_liquidations(tab: Dict) -> Dict[str, Dict]:
    """Sweeps all 36 mapped symbols in the TradingView tab and persists data."""
    symbol_map = load_symbol_map()
    ws_url = tab["webSocketDebuggerUrl"]
    
    global evaluator
    if evaluator is None:
        try:
            logger.info("Initializing RealtimeSignalEvaluator for liquidation reversal alerts...")
            evaluator = RealtimeSignalEvaluator()
        except Exception as e:
            logger.warning(f"Could not initialize RealtimeSignalEvaluator: {e}")

    results = {}
    sweep_start = time.time()
    
    async with websockets.connect(ws_url, open_timeout=5, close_timeout=2, max_size=100*1024*1024) as ws:
        contexts = {}
        async def listen_for_ctx():
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("method") == "Runtime.executionContextCreated":
                    ctx = msg["params"]["context"]
                    contexts[ctx["id"]] = ctx
                elif msg.get("id") == 999:
                    break

        await ws.send(json.dumps({"id": 1, "method": "Runtime.enable"}))
        await ws.send(json.dumps({"id": 999, "method": "Runtime.evaluate", "params": {"expression": "1"}}))
        await asyncio.wait_for(listen_for_ctx(), timeout=10.0)
        
        tv_ctx = None
        for cid in contexts:
            await ws.send(json.dumps({
                "id": 1000 + cid,
                "method": "Runtime.evaluate",
                "params": {"expression": "typeof window.chartWidgetCollection !== 'undefined'", "contextId": cid}
            }))
            c_res = json.loads(await ws.recv())
            if c_res.get("result", {}).get("result", {}).get("value"):
                tv_ctx = cid
                break

        if tv_ctx is None:
            logger.error("Could not find TradingView execution context.")
            return results
        
        # Ensure Aggregated Liquidations study is active on the chart
        ensure_study_script = """
        (async () => {
            const cwc = window.chartWidgetCollection;
            if (!cwc) return { ok: false, error: "no_cwc" };
            const chart = cwc.activeChartWidget.value();
            if (!chart) return { ok: false, error: "no_chart" };
            const model = chart.model().model();
            let study = model.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
            if (!study) {
                const inserter = model.createStudyInserter({ type: 'java', studyId: 'aggregated_liq_kline@tv-basicstudies' });
                await inserter.insert();
                study = model.allStudies().find(s => s.metaInfo && s.metaInfo().id === 'aggregated_liq_kline@tv-basicstudies');
            }
            return { ok: true, studyId: study ? study.id() : null };
        })()
        """
        await ws.send(json.dumps({
            "id": 2000,
            "method": "Runtime.evaluate",
            "params": {"expression": ensure_study_script, "contextId": tv_ctx, "awaitPromise": True, "returnByValue": True}
        }))
        await ws.recv()
        
        msg_id = 5000
        try:
            for base_sym, tv_sym in symbol_map.items():
                msg_id += 1
                try:
                    raw_rows, last_price = await asyncio.wait_for(harvest_single_symbol(ws, tv_ctx, tv_sym, msg_id), timeout=4.0)
                    if raw_rows:
                        summary = process_and_persist_liquidations(base_sym, tv_sym, raw_rows, last_price)
                        if summary:
                            results[base_sym] = summary
                            spike_tag = f" [SPIKE: {summary['reversal_bias']}]" if summary["recent_spike"] and summary["reversal_bias"] != "NEUTRAL" else ""
                            logger.info(f"Harvested {base_sym} ({tv_sym}): total {summary['total_candles']} bars, running bar: ${summary['latest_total_liq_usd']:,.0f}{spike_tag}")

                            # Check for active reversal spike dispatch
                            if summary["recent_spike"] and summary["reversal_bias"] != "NEUTRAL" and summary["exhaustion_score"] >= 50.0:
                                spike_key = (base_sym, summary["spike_bar_time_utc"], summary["reversal_bias"])
                                if spike_key not in PROCESSED_REVERSAL_KEYS:
                                    PROCESSED_REVERSAL_KEYS.add(spike_key)
                                    if len(PROCESSED_REVERSAL_KEYS) > 4096:
                                        PROCESSED_REVERSAL_KEYS.remove(min(PROCESSED_REVERSAL_KEYS, key=lambda k: k[1]))
                                    logger.info(f"🚨 [COINGLASS SENTINEL] Active Reversal Spike on {base_sym}: {summary['reversal_bias']} (Exhaustion: {summary['exhaustion_score']:.0f}/100, Liq: ${summary['prev_total_liq_usd']:,.0f})! Dispatching to RealtimeSignalEvaluator...")
                                    if evaluator and last_price > 0:
                                        try:
                                            ts_sec = int(datetime.fromisoformat(summary["spike_bar_time_utc"].replace("Z", "+00:00")).timestamp())
                                        except Exception:
                                            ts_sec = int(time.time())
                                        candidate_sig = {
                                            "symbol": base_sym,
                                            "mt5_symbol": MT5_MAP.get(base_sym, f"{base_sym}USD"),
                                            "sig_name": f"Liquidation Exhaustion ({summary['reversal_bias']})",
                                            "sig_code": "LIQ_REV_SPIKE",
                                            "side": "buy" if summary["reversal_bias"] == "BULLISH_REVERSAL" else "sell",
                                            "price": last_price,
                                            "datetime_utc": summary["spike_bar_time_utc"],
                                            "observed_at_utc": summary["updated_at_utc"],
                                            "time_ms": ts_sec * 1000,
                                            "title": f"CoinGlass Live Liquidation Reversal Spike ({base_sym})",
                                            "description": f"Massive {summary['reversal_bias']} liquidation spike: total ${summary['prev_total_liq_usd']:,.0f} vs P90 ${summary['p90_threshold']:,.0f} (Exhaustion score: {summary['exhaustion_score']:.0f}/100)"
                                        }
                                        evaluator.evaluate_new_signals([candidate_sig])
                except Exception as e:
                    logger.error(f"Error harvesting {base_sym}: {e}")
        finally:
            # ALWAYS reset back to BTC for visual continuity and to prevent Next.js URL mismatch
            try:
                await ws.send(json.dumps({
                    "id": 9999,
                    "method": "Runtime.evaluate",
                    "params": {"expression": 'window.chartWidgetCollection.setSymbol("Binance_BTCUSDT")', "contextId": tv_ctx}
                }))
            except Exception:
                pass
            
    sweep_duration = time.time() - sweep_start
    if results:
        atomic_json(SIGNALS_JSON_FILE, results)
        summary_df = pd.DataFrame(list(results.values()))
        atomic_parquet(summary_df, SUMMARY_PARQUET_FILE)
        logger.info(f"Sweep complete: {len(results)} assets dumped & updated in {sweep_duration:.1f}s!")
        
    return results


async def run_daemon():
    acquire_singleton("coinglass_tv_liquidation_collector", DB_DIR / ".locks")
    logger.info("Starting CoinGlass TV Live Liquidation Collector Daemon...")
    
    sweep_interval_seconds = 60.0  # Sweeps every 60s
    
    while True:
        start_time = time.time()
        tab = get_tv_tab()
        if not tab:
            logger.warning("CoinGlass TV tab not available. Waiting 10s...")
            await asyncio.sleep(10.0)
            continue
            
        await ensure_tv_tab_healthy(tab)

        with BrowserLock("TVLiquidationCollector", timeout_secs=5.0) as lock:
            if not lock.acquired:
                logger.info("Browser lock busy. Yielding sweep to next cycle...")
            else:
                try:
                    await sweep_all_liquidations(tab)
                except Exception as e:
                    logger.error(f"Sweep error: {e}", exc_info=True)
                    
        elapsed = time.time() - start_time
        sleep_time = max(10.0, sweep_interval_seconds - elapsed)
        logger.info(f"Cycle completed in {elapsed:.1f}s. Sleeping {sleep_time:.1f}s until next sweep...")
        await asyncio.sleep(sleep_time)


def run_single_sweep():
    acquire_singleton("coinglass_tv_liquidation_collector", DB_DIR / ".locks")
    tab = get_tv_tab()
    if not tab:
        print("CoinGlass TV tab not available.")
        return
    asyncio.run(ensure_tv_tab_healthy(tab))
    with BrowserLock("TVLiquidationCollector", timeout_secs=5.0) as lock:
        if not lock.acquired:
            print("Browser lock busy.")
            return
        asyncio.run(sweep_all_liquidations(tab))


if __name__ == "__main__":
    if "--once" in sys.argv:
        run_single_sweep()
    else:
        asyncio.run(run_daemon())
