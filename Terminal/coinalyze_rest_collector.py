"""
Coinalyze REST API Live Liquidation Collector Daemon
===================================================
Collects real-time 15-minute aggregated liquidation timeseries across all 34 assets
(Crypto, Gold XAU, Silver XAG, Oil, Nasdaq NDX100, Dow DJI30, Nikkei JP225)
using the official Coinalyze REST API key.

Zero Chrome CDP dependency. Zero browser lock contention. Zero page crashes.

Computes:
1. Long/Short liquidation volume ($USD) per 15-minute bar.
2. 50-bar rolling 90th percentile thresholds for liquidation spike detection.
3. Live Liquidation Reversal signals:
   - BULLISH_REVERSAL: Large Long liquidation cascade (> p90) indicates seller exhaustion / capitulation bottom.
   - BEARISH_REVERSAL: Large Short liquidation squeeze (> p90) indicates buyer exhaustion / blow-off top.
4. Persists to:
   - Database/Live_Liquidations/<SYMBOL>_coinalyze_liquidations.parquet
   - Database/Live_Liquidations/latest_reversal_signals.json
   - Database/Live_Liquidations/summary_liquidations.parquet
5. Integrates with RealtimeSignalEvaluator to dispatch high-confluence reversal trade candidates.
"""

import sys
import os
import json
import time
import logging
import asyncio
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_parquet, atomic_json
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator

DB_DIR = ROOT_DIR / "Database"
LIVE_LIQ_DIR = DB_DIR / "Live_Liquidations"
LIVE_LIQ_DIR.mkdir(parents=True, exist_ok=True)
CANDLE_DIR = DB_DIR / "Candle_Data"

SYMBOL_MAP_FILE = DB_DIR / "coinalyze_symbol_map.json"
SIGNALS_JSON_FILE = LIVE_LIQ_DIR / "latest_reversal_signals.json"
SUMMARY_PARQUET_FILE = LIVE_LIQ_DIR / "summary_liquidations.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [COINALYZE] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("CoinalyzeLiquidation")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

API_KEY = os.getenv("COINALYZE_API_KEY", "e5a0b7b7-a0cb-4cf4-97dd-494db94be3c6")
BASE_URL = "https://api.coinalyze.net/v1"

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


def load_symbol_map() -> Dict[str, str]:
    if SYMBOL_MAP_FILE.exists():
        try:
            with open(SYMBOL_MAP_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "BTC": "BTCUSDT_PERP.A",
        "ETH": "ETHUSDT_PERP.A",
        "SOL": "SOLUSDT_PERP.A",
        "XRP": "XRPUSDT_PERP.A"
    }


def get_cached_candle_price(sym: str) -> float:
    """Reads latest close price from local parquet cache as fallback."""
    p = CANDLE_DIR / f"{sym}_candles.parquet"
    if p.exists():
        try:
            df = pd.read_parquet(p)
            if not df.empty and "close" in df.columns:
                return float(df["close"].iloc[-1])
        except Exception:
            pass
    return 0.0


def fetch_coinalyze_json(endpoint: str, params: Dict[str, str]) -> Optional[any]:
    """Sends authenticated HTTP GET request to Coinalyze API with 429 backoff."""
    params_with_key = dict(params)
    params_with_key["api_key"] = API_KEY
    query_str = "&".join(f"{k}={v}" for k, v in params_with_key.items())
    url = f"{BASE_URL}/{endpoint}?{query_str}"
    
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait_sec = float(retry_after) if retry_after else 5.0
                logger.warning(f"Coinalyze 429 rate limit hit. Backing off for {wait_sec:.1f}s...")
                time.sleep(min(wait_sec, 15.0))
                continue
            else:
                logger.warning(f"Coinalyze HTTP Error {e.code} for {endpoint}: {e.read().decode()[:150]}")
                break
        except Exception as e:
            logger.warning(f"Coinalyze Network Error for {endpoint}: {e}")
            break
    return None


def fetch_batch_data(cz_symbols: List[str], base_symbols: List[str], hours_back: int = 48) -> Tuple[Dict[str, List[Dict]], Dict[str, float]]:
    """
    Fetches liquidation history and prices for a batch of symbols.
    """
    now = int(time.time())
    from_time = now - (hours_back * 3600)
    syms_csv = ",".join(cz_symbols)
    
    # 1. Fetch liquidations
    liq_data = fetch_coinalyze_json("liquidation-history", {
        "symbols": syms_csv,
        "interval": "15min",
        "from": str(from_time),
        "to": str(now)
    })
    
    # 2. Fetch recent OHLCV
    ohlcv_data = fetch_coinalyze_json("ohlcv-history", {
        "symbols": syms_csv,
        "interval": "15min",
        "from": str(now - 3600 * 4),
        "to": str(now)
    })
    
    liq_map = {}
    if liq_data:
        for item in liq_data:
            liq_map[item.get("symbol")] = item.get("history", [])
            
    price_map = {}
    ohlcv_close_map = {}
    if ohlcv_data:
        for item in ohlcv_data:
            hist = item.get("history", [])
            if hist:
                sym = item.get("symbol")
                last_candle = hist[-1]
                price_map[sym] = float(last_candle.get("c", 0.0))
                ohlcv_close_map[sym] = {c.get("t"): float(c.get("c", 0.0)) for c in hist}
                
    # Fallback for prices from local parquet cache
    for b_sym, c_sym in zip(base_symbols, cz_symbols):
        if c_sym not in price_map or price_map[c_sym] <= 0:
            cached_p = get_cached_candle_price(b_sym)
            if cached_p > 0:
                price_map[c_sym] = cached_p
                
    return liq_map, price_map, ohlcv_close_map


def process_symbol_liquidations(base_sym: str, cz_sym: str, liq_bars: List[Dict], latest_price: float, ohlcv_closes: Dict[int, float]) -> Optional[Dict]:
    """Processes raw bars, computes rolling spike thresholds, reversal signals, and saves Parquet."""
    if not liq_bars:
        return None
        
    rows = []
    fallback_price = latest_price if latest_price > 0 else 1.0
    
    for bar in liq_bars:
        t = int(bar.get("t", 0))
        l_vol = float(bar.get("l", 0.0))
        s_vol = float(bar.get("s", 0.0))
        
        bar_price = ohlcv_closes.get(t, fallback_price)
        if bar_price <= 0:
            bar_price = fallback_price
            
        long_usd = l_vol * bar_price
        short_usd = s_vol * bar_price
        rows.append([t, long_usd, short_usd, bar_price])
        
    df = pd.DataFrame(rows, columns=["timestamp", "long_liq_usd", "short_liq_usd", "close_price"])
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp", kind="stable").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    
    df["total_liq_usd"] = df["long_liq_usd"] + df["short_liq_usd"]
    df["net_liq_usd"] = df["long_liq_usd"] - df["short_liq_usd"]
    df["long_ratio"] = np.where(df["total_liq_usd"] > 0, df["long_liq_usd"] / df["total_liq_usd"], 0.5)
    
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["datetime_ist"] = (pd.to_datetime(df["timestamp"], unit="s", utc=True) + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S IST")
    df["symbol"] = base_sym
    df["coinalyze_symbol"] = cz_sym
    
    # Rolling 50-bar 90th percentile threshold for spikes
    min_spike_floor = 50000.0 if base_sym in ["BTC", "ETH", "SOL"] else 15000.0
    rolling_p90 = df["total_liq_usd"].shift(1).rolling(50, min_periods=5).quantile(0.90).fillna(min_spike_floor)
    df["p90_threshold"] = np.maximum(rolling_p90, min_spike_floor)
    df["is_spike"] = (df["total_liq_usd"] >= df["p90_threshold"]) & (df["total_liq_usd"] > min_spike_floor)
    
    df["reversal_bias"] = "NEUTRAL"
    df.loc[df["is_spike"] & (df["long_ratio"] >= 0.70), "reversal_bias"] = "BULLISH_REVERSAL"
    df.loc[df["is_spike"] & (df["long_ratio"] <= 0.30), "reversal_bias"] = "BEARISH_REVERSAL"
    
    out_file = LIVE_LIQ_DIR / f"{base_sym}_coinalyze_liquidations.parquet"
    atomic_parquet(df, out_file)
    
    canonical_file = LIVE_LIQ_DIR / f"{base_sym}_tv_liquidations.parquet"
    atomic_parquet(df, canonical_file)
    
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
        "coinalyze_symbol": cz_sym,
        "last_price": float(latest_price),
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
        "source": "Coinalyze_REST_API",
        "updated_at_utc": datetime.now(timezone.utc).isoformat()
    }
    return summary


async def sweep_coinalyze():
    """Sweeps all symbols cleanly across batches with rate-limit pacing."""
    symbol_map = load_symbol_map()
    items = list(symbol_map.items())
    batch_size = 15
    
    global evaluator
    if evaluator is None:
        try:
            logger.info("Initializing RealtimeSignalEvaluator...")
            evaluator = RealtimeSignalEvaluator()
        except Exception as e:
            logger.warning(f"Could not initialize RealtimeSignalEvaluator: {e}")
            
    results = {}
    if SIGNALS_JSON_FILE.exists():
        try:
            with open(SIGNALS_JSON_FILE, "r", encoding="utf-8") as f:
                results = json.load(f)
        except Exception:
            results = {}

    for i in range(0, len(items), batch_size):
        chunk = items[i:i + batch_size]
        base_syms = [b for b, _ in chunk]
        cz_syms = [cz for _, cz in chunk]
        
        liq_map, price_map, ohlcv_map = await asyncio.to_thread(fetch_batch_data, cz_syms, base_syms)
        
        for base_sym, cz_sym in chunk:
            bars = liq_map.get(cz_sym, [])
            price = price_map.get(cz_sym, 0.0)
            closes = ohlcv_map.get(cz_sym, {})
            
            if bars:
                summary = process_symbol_liquidations(base_sym, cz_sym, bars, price, closes)
                if summary:
                    results[base_sym] = summary
                    spike_tag = f" [SPIKE: {summary['reversal_bias']}]" if summary["recent_spike"] and summary["reversal_bias"] != "NEUTRAL" else ""
                    logger.info(f"Harvested {base_sym} ({cz_sym}): {len(bars)} bars, Price: ${price:,.2f}, latest liq: ${summary['latest_total_liq_usd']:,.0f}{spike_tag}")
                    
                    if summary["recent_spike"] and summary["reversal_bias"] != "NEUTRAL" and summary["exhaustion_score"] >= 50.0:
                        spike_key = (base_sym, summary["spike_bar_time_utc"], summary["reversal_bias"])
                        if spike_key not in PROCESSED_REVERSAL_KEYS:
                            PROCESSED_REVERSAL_KEYS.add(spike_key)
                            if len(PROCESSED_REVERSAL_KEYS) > 4096:
                                PROCESSED_REVERSAL_KEYS.remove(min(PROCESSED_REVERSAL_KEYS, key=lambda k: k[1]))
                            logger.info(f"🚨 [COINALYZE SENTINEL] Active Reversal Spike on {base_sym}: {summary['reversal_bias']} (Exhaustion: {summary['exhaustion_score']:.0f}/100, Liq: ${summary['prev_total_liq_usd']:,.0f})! Dispatching to RealtimeSignalEvaluator...")
                            if evaluator and price > 0:
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
                                    "price": price,
                                    "datetime_utc": summary["spike_bar_time_utc"],
                                    "observed_at_utc": summary["updated_at_utc"],
                                    "time_ms": ts_sec * 1000,
                                    "title": f"Coinalyze Live Liquidation Reversal Spike ({base_sym})",
                                    "description": f"Massive {summary['reversal_bias']} liquidation spike: total ${summary['prev_total_liq_usd']:,.0f} vs P90 ${summary['p90_threshold']:,.0f} (Exhaustion score: {summary['exhaustion_score']:.0f}/100)"
                                }
                                evaluator.evaluate_new_signals([candidate_sig])
        await asyncio.sleep(2.0)
        
    if results:
        atomic_json(SIGNALS_JSON_FILE, results)
        summary_df = pd.DataFrame(list(results.values()))
        atomic_parquet(summary_df, SUMMARY_PARQUET_FILE)
        logger.info(f"Coinalyze sweep complete: {len(results)} assets updated.")


async def run_daemon():
    acquire_singleton("coinalyze_rest_collector", DB_DIR / ".locks")
    logger.info("Starting Coinalyze REST API Live Liquidation Collector Daemon...")
    while True:
        start_time = time.time()
        try:
            await sweep_coinalyze()
        except Exception as e:
            logger.error(f"Sweep error: {e}", exc_info=True)
            
        elapsed = time.time() - start_time
        sleep_time = max(20.0, 60.0 - elapsed)
        logger.info(f"Cycle completed in {elapsed:.1f}s. Sleeping {sleep_time:.1f}s until next sweep...")
        await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    if "--once" in sys.argv:
        asyncio.run(sweep_coinalyze())
    else:
        asyncio.run(run_daemon())
