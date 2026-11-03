"""
Forex & Metals CME Futures Footprint Collector Daemon
=====================================================
Acquires real-time institutional footprint and order flow from CME Currency Futures & COMEX Gold
via TradingView Web CDP bridge (Method 1).

Covers:
- EURUSD <- CME Euro FX (6E1!)
- GBPUSD <- CME British Pound (6B1!)
- USDJPY <- CME Japanese Yen (6J1! - Inverted)
- AUDUSD <- CME Australian Dollar (6A1!)
- USDCAD <- CME Canadian Dollar (6C1! - Inverted)
- XAUUSD <- COMEX Gold (GC1!)

Footprint Patterns Detected (100% Identical to CoinGlass Crypto Logic):
1. Absorption: Taker volume absorbed at bar extreme with sharp rejection
2. Divergence: Price bar closes contrary to taker volume flow
3. Exhaustion: Multi-bar price extreme made on depleted volume (<50% SMA20)
4. Trapped Traders: Breakout beyond 5-bar range that fails and snaps back
"""

import sys
import os
import json
import time
import logging
import asyncio
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

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

from Terminal.runtime_io import acquire_singleton, upsert_parquet, cdp_result
from Terminal.collector_lock import BrowserLock, rotate_warp_ip
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.auction_indicators import update_all_auction_profiles
from Terminal.universal_footprint_engine import UniversalFootprintEngine

DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

CANDLE_DIR = DB_DIR / "Candle_Data"
CANDLE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_PARQUET = SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [FOREX_CME] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("ForexCMEFootprint")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# In-memory tracking to ensure each signal is emitted and evaluated only once
KNOWN_SIGNAL_KEYS = set()
evaluator: Optional[RealtimeSignalEvaluator] = None

# CME Currency & Metals Futures Instruments
CME_CONTRACTS = {
    "EURUSD": {
        "cme_symbol": "CME:6E1!",
        "mt5": "EURUSD.pi",
        "name": "CME Euro FX Futures",
        "inverted": False,
        "pip_mult": 10000.0,
    },
    "GBPUSD": {
        "cme_symbol": "CME:6B1!",
        "mt5": "GBPUSD.pi",
        "name": "CME British Pound Futures",
        "inverted": False,
        "pip_mult": 10000.0,
    },
    "USDJPY": {
        "cme_symbol": "CME:6J1!",
        "mt5": "USDJPY.pi",
        "name": "CME Japanese Yen Futures",
        "inverted": True,
        "pip_mult": 100.0,
    },
    "AUDUSD": {
        "cme_symbol": "CME:6A1!",
        "mt5": "AUDUSD.pi",
        "name": "CME Australian Dollar Futures",
        "inverted": False,
        "pip_mult": 10000.0,
    },
    "USDCAD": {
        "cme_symbol": "CME:6C1!",
        "mt5": "USDCAD.pi",
        "name": "CME Canadian Dollar Futures",
        "inverted": True,
        "pip_mult": 10000.0,
    },
    "XAUUSD": {
        "cme_symbol": "COMEX:GC1!",
        "mt5": "XAUUSD.pi",
        "name": "COMEX Gold Futures",
        "inverted": False,
        "pip_mult": 10.0,
    },
    "XAGUSD": {
        "cme_symbol": "COMEX:SI1!",
        "mt5": "XAGUSD.pi",
        "name": "COMEX Silver Futures",
        "inverted": False,
        "pip_mult": 100.0,
    },
    "SPX500": {
        "cme_symbol": "CME_MINI:ES1!",
        "mt5": "SPX500",
        "name": "CME E-mini S&P 500 Futures",
        "inverted": False,
        "pip_mult": 1.0,
    },
    "NDX100": {
        "cme_symbol": "CME_MINI:NQ1!",
        "mt5": "NDX100",
        "name": "CME E-mini Nasdaq 100 Futures",
        "inverted": False,
        "pip_mult": 1.0,
    },
    "DJI30": {
        "cme_symbol": "CBOT_MINI:YM1!",
        "mt5": "DJI30",
        "name": "CBOT E-mini Dow Jones Futures",
        "inverted": False,
        "pip_mult": 1.0,
    },
}


def cleanup_tv_tabs():
    """Closes any lingering CME bot tabs to maintain zero tab footprint."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json")
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "")
                if "bot=forex_cme_collector" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Cleaned up orphan CME collector tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Tab cleanup error: {e}")


def open_cme_tv_tab(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """Opens a fresh ephemeral TradingView tab for the CME symbol."""
    cleanup_tv_tabs()
    encoded = urllib.parse.quote(symbol)
    url = f"https://www.tradingview.com/chart/?symbol={encoded}&bot=forex_cme_collector"
    try:
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.debug(f"Opened ephemeral TV tab ({tab_id}) for {symbol}")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open TV tab via CDP for {symbol}: {e}")
        return None, None


def close_tab(tab_id: Optional[str]):
    """Closes tab immediately after sweep to keep browser lean."""
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.debug(f"Closed CME TV tab ({tab_id}) - Tab footprint zero.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    cleanup_tv_tabs()


def classify_footprint_signals(bars: List[Dict[str, Any]], pair: str, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Computes institutional footprint pattern signals (Absorption, Divergence, Exhaustion, Trapped Traders)
    from 15m CME Futures bars with identical logic to CoinGlass Footprint.
    """
    if len(bars) < 25:
        return []

    signals = []
    inverted = meta.get("inverted", False)
    mt5_sym = meta.get("mt5", f"{pair}.pi")

    # Calculate 20-period moving average of volume
    vols = [b["vol"] for b in bars]
    closes = [b["close"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    opens = [b["open"] for b in bars]

    # Inspect the most recent completed bar (bar at index -2) and running bar (index -1)
    for idx in [len(bars) - 2, len(bars) - 1]:
        b = bars[idx]
        b_time_ms = int(b["time"] * 1000)
        o, h, l, c, v = b["open"], b["high"], b["low"], b["close"], b["vol"]
        bar_range = max(h - l, 1e-6)

        # Baseline volume (SMA 20)
        window_vols = vols[max(0, idx - 20):idx]
        avg_vol = np.mean(window_vols) if window_vols else 1.0

        # Wicks and body metrics
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body = abs(c - o)
        close_pos = (c - l) / bar_range  # 0.0 (at low) to 1.0 (at high)

        dt_utc = datetime.fromtimestamp(b["time"], timezone.utc)
        utc_iso = dt_utc.isoformat().replace("+00:00", "Z")
        ist_str = (dt_utc + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S IST")

        # Inversion handler: if inverted (6J, 6C), flip high/low and bullish/bearish
        def adjust_side(raw_side: str) -> str:
            if not inverted:
                return raw_side
            return "bearish" if raw_side == "bullish" else "bullish"

        # ---------------------------------------------------------------------
        # 1. ABSORPTION (abs)
        # High volume absorbed at extremes with rejection
        # ---------------------------------------------------------------------
        # Bearish Absorption: Buying absorbed at the high
        if upper_wick >= 0.35 * bar_range and close_pos <= 0.40 and v >= 1.2 * avg_vol:
            side = adjust_side("bearish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": side,
                "title": "Buying absorbed at the high" if side == "bearish" else "Selling absorbed at the low",
                "description": f"{int(v)} contracts traded with rejection at high, closed near low, {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })

        # Bullish Absorption: Selling absorbed at the low
        elif lower_wick >= 0.35 * bar_range and close_pos >= 0.60 and v >= 1.2 * avg_vol:
            side = adjust_side("bullish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": side,
                "title": "Selling absorbed at the low" if side == "bullish" else "Buying absorbed at the high",
                "description": f"{int(v)} contracts traded with absorption at low, closed near high, {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })

        # ---------------------------------------------------------------------
        # 2. DIVERGENCE (div)
        # Price movement without follow-through volume
        # ---------------------------------------------------------------------
        prev_c = closes[idx - 1]
        if c < prev_c and close_pos >= 0.65 and v >= 1.0 * avg_vol:
            # Bullish Divergence: Drop without aggressive selling
            side = adjust_side("bullish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": side,
                "title": "Drop without taker selling" if side == "bullish" else "Rally without taker buying",
                "description": f"Price down but closed in upper range (+{int(close_pos*100)}%), {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })
        elif c > prev_c and close_pos <= 0.35 and v >= 1.0 * avg_vol:
            # Bearish Divergence: Rally without aggressive buying
            side = adjust_side("bearish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": side,
                "title": "Rally without taker buying" if side == "bearish" else "Drop without taker selling",
                "description": f"Price up but closed in lower range ({int(close_pos*100)}%), {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })

        # ---------------------------------------------------------------------
        # 3. TRAPPED TRADERS / FAILED BREAKOUT (trap)
        # Breakout beyond 5-bar range that snaps back
        # ---------------------------------------------------------------------
        prior_5_high = max(highs[max(0, idx - 5):idx])
        prior_5_low = min(lows[max(0, idx - 5):idx])

        if h > prior_5_high and c < prior_5_high and close_pos <= 0.40:
            side = adjust_side("bearish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "trap",
                "sig_name": "Trapped longs",
                "side": side,
                "title": "Trapped breakout buyers" if side == "bearish" else "Trapped breakout sellers",
                "description": f"Broke 5-bar high ({round(prior_5_high, 5)}) but fell back inside bar range, {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })
        elif l < prior_5_low and c > prior_5_low and close_pos >= 0.60:
            side = adjust_side("bullish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "trap",
                "sig_name": "Trapped shorts",
                "side": side,
                "title": "Trapped breakout sellers" if side == "bullish" else "Trapped breakout buyers",
                "description": f"Broke 5-bar low ({round(prior_5_low, 5)}) but snapped back inside bar range, {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })

        # ---------------------------------------------------------------------
        # 4. EXHAUSTION (exh)
        # New 10-bar extreme on low volume (<50% SMA20)
        # ---------------------------------------------------------------------
        prior_10_high = max(highs[max(0, idx - 10):idx])
        prior_10_low = min(lows[max(0, idx - 10):idx])

        if h > prior_10_high and v < 0.50 * avg_vol:
            side = adjust_side("bearish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "exh",
                "sig_name": "Buyer exhaustion",
                "side": side,
                "title": "Buyer exhaustion at high",
                "description": f"New 10-bar high on depleted volume ({int(v)} contracts vs avg {int(avg_vol)}), {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })
        elif l < prior_10_low and v < 0.50 * avg_vol:
            side = adjust_side("bullish")
            signals.append({
                "symbol": pair,
                "mt5_symbol": mt5_sym,
                "sig_code": "exh",
                "sig_name": "Seller exhaustion",
                "side": side,
                "title": "Seller exhaustion at low",
                "description": f"New 10-bar low on depleted volume ({int(v)} contracts vs avg {int(avg_vol)}), {side}",
                "time_ms": b_time_ms,
                "candle_utc": utc_iso,
                "datetime_utc": utc_iso,
                "candle_ist": ist_str,
                "price": round(c, 5),
                "volume": v,
                "source": "cme_futures_footprint"
            })

    return signals


async def harvest_cme_symbol(ws, pair: str, meta: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
    """Switches resolution to 15m and extracts all 300 15m bars from TradingView."""
    try:
        # Set resolution to 15m
        await ws.send(json.dumps({'id': 1, 'method': 'Runtime.evaluate', 'params': {
            'expression': 'window._exposed_chartWidgetCollection.activeChartWidget.value().setResolution("15")',
            'returnByValue': True
        }}))
        await ws.recv()
        await asyncio.sleep(2.0)

        extract_js = '''
        (() => {
            try {
                const cwc = window._exposed_chartWidgetCollection;
                const chart = cwc.activeChartWidget.value();
                const ms = chart.model().model().mainSeries();
                const d = ms.data();
                const bars = [];
                d.each((idx, bar) => {
                    bars.push({
                        time: bar[0],
                        open: bar[1],
                        high: bar[2],
                        low: bar[3],
                        close: bar[4],
                        vol: bar[5]
                    });
                });
                return { ok: true, bars: bars };
            } catch(e) {
                return { ok: false, error: e.toString() };
            }
        })()
        '''
        await ws.send(json.dumps({'id': 2, 'method': 'Runtime.evaluate', 'params': {
            'expression': extract_js,
            'returnByValue': True
        }}))
        res = json.loads(await ws.recv())
        val = res.get('result', {}).get('result', {}).get('value', {})
        if not val.get('ok'):
            return [], []

        raw_bars = val.get('bars', [])
        if not raw_bars:
            return pd.DataFrame(), []

        engine = UniversalFootprintEngine()
        ac = "Metals" if pair in ("XAUUSD", "XAGUSD") else ("Indices" if pair in ("SPX500", "NDX100", "DJI30") else "Forex")
        rolling_df, signals = engine.analyze_bars(
            bars=raw_bars,
            symbol=pair,
            asset_class=ac,
            mt5_symbol=meta.get("mt5", f"{pair}.pi"),
            inverted=meta.get("inverted", False),
            rolling_window=300,
            inspect_last_n_bars=2
        )
        return rolling_df, signals
    except Exception as e:
        logger.error(f"Error harvesting CME data for {pair}: {e}")
        return pd.DataFrame(), []


def persist_cme_candles(pair: str, df: Any):
    if df is None:
        return
    if isinstance(df, list):
        if not df:
            return
        df = pd.DataFrame(df)
    if df.empty:
        return
    target_parquet = CANDLE_DIR / f"{pair}.parquet"
    upsert_parquet(df, target_parquet, ["open_time_ms"], sort_by="open_time_ms", limit=300)


def persist_cme_signals(signals: List[Dict]):
    if not signals:
        return
    df = pd.DataFrame(signals)
    upsert_parquet(df, SIGNALS_PARQUET, ["symbol", "sig_code", "time_ms"], sort_by="time_ms", limit=100000)


async def execute_cme_forex_sweep() -> int:
    """Executes a full sweep across all CME currency futures and gold contracts."""
    global evaluator
    if evaluator is None:
        evaluator = RealtimeSignalEvaluator()

    total_new_signals = 0
    t0 = time.time()
    logger.info(f"Starting CME Forex & Metals Footprint sweep across {len(CME_CONTRACTS)} contracts...")

    for pair, meta in CME_CONTRACTS.items():
        cme_sym = meta["cme_symbol"]
        tab_id, ws_url = open_cme_tv_tab(cme_sym)
        if not ws_url:
            continue

        try:
            async with websockets.connect(ws_url, open_timeout=6, close_timeout=2) as ws:
                await asyncio.sleep(4.0)
                candles, signals = await harvest_cme_symbol(ws, pair, meta)

                if candles is not None and len(candles) > 0:
                    persist_cme_candles(pair, candles)
                    logger.info(f"[{pair}] {len(candles)} 15m CME bars saved to Candle_Data.")

                if signals:
                    persist_cme_signals(signals)
                    # Evaluate new signals
                    for sig in signals:
                        sig_key = f"{sig['symbol']}_{sig['sig_code']}_{sig['time_ms']}"
                        if sig_key not in KNOWN_SIGNAL_KEYS:
                            KNOWN_SIGNAL_KEYS.add(sig_key)
                            total_new_signals += 1
                            try:
                                eval_result = evaluator.evaluate_signal(sig)
                                logger.info(f"[SIGNAL EVAL] {pair} {sig['side'].upper()} ({sig['sig_name']}) -> Score {eval_result['confluence_score']}/100")
                            except Exception as ex:
                                logger.warning(f"Evaluator error for {pair} signal: {ex}")
        finally:
            close_tab(tab_id)

    # Recalculate auction market profiles for all pairs
    try:
        update_all_auction_profiles()
    except Exception as e:
        logger.debug(f"Auction profile update error: {e}")

    logger.info(f"CME Forex & Metals sweep completed in {time.time() - t0:.2f}s | {total_new_signals} new signals evaluated.")
    return total_new_signals


async def run_cme_forex_daemon(sweep_interval_sec: int = 150):
    """Continuous daemon sweeping CME Forex & Metals footprint."""
    logger.info(f"=== Forex CME Footprint Daemon Active (Interval: {sweep_interval_sec}s) ===")
    while True:
        try:
            # Yield to other browser collectors during reserve windows
            now_min = datetime.now().minute
            if now_min in [0, 15, 30, 45]:
                # On candle close, wait 15s for candle completion then execute
                await asyncio.sleep(15)

            with BrowserLock(collector_name="ForexCMEFootprint", timeout_secs=45.0):
                await execute_cme_forex_sweep()
        except Exception as e:
            logger.error(f"Sweep error in Forex CME daemon: {e}")

        await asyncio.sleep(sweep_interval_sec)


if __name__ == "__main__":
    acquire_singleton("forex_cme_footprint_collector", DB_DIR / ".locks")
    if "--once" in sys.argv:
        asyncio.run(execute_cme_forex_sweep())
    else:
        asyncio.run(run_cme_forex_daemon(sweep_interval_sec=180))
