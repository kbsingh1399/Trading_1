"""
========================================================================================
Unified API Footprint & Order Flow Engine
========================================================================================
A single, institutional, 100% free API-driven footprint engine replacing all fragmented
web scrapers. Tracks every asset available at MetaTrader (Forex, Metals, Commodities, Indices)
plus all top Cryptos with zero web browser scraping.

Sources:
1. Crypto (32 assets): Binance Futures Public REST API (https://fapi.binance.com/fapi/v1/klines)
   - Real taker buy base asset volume, sell volume, delta, cumulative delta.
   - 100% free, high rate limit, zero API keys required.
2. Macro (Forex, Metals, Commodities, Indices): Local MetaTrader 5 Python API
   - Directly connected to FundedNext Account 35296418.
   - 100% free, local interbank tick volume and institutional rates.
   - Dynamic server offset compensation ensures exact IST (+05:30) and UTC alignment.

Guarantees:
- Maintains exact 300 rolling candle window per asset in Database/Candle_Data/<SYMBOL>.parquet.
- Primary Indian Standard Time (IST, UTC+5:30) mapping across all tables.
- Zero lock files in Database/Candle_Data (uses atomic OS file replacement).
- Generates all 5 CoinGlass footprint algorithms: Absorption, Divergence, Swing CVD Divergence,
  Exhaustion, and Trapped Traders.
- Appends newly triggered signals to Database/Signals/coinglass_aggregated_futures_signals.parquet.
- Automatically triggers RealtimeSignalEvaluator and MatchTrader Live Bridge for high-confluence
  order execution on MatchTrader (Account 101305360).
========================================================================================
"""

import sys
import os
import json
import time
import math
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Set  # Set kept for type hints elsewhere
import urllib.request
import urllib.error

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import numpy as np
import pandas as pd
import polars as pl

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    IST_OFFSET,
    IST_OFFSET_MS,
    utc_ms_to_ist_str,
    utc_ms_to_ist_iso,
    enrich_candle_df_with_ist
)
from Terminal.runtime_io import acquire_singleton, writer_lock, atomic_parquet
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.auction_indicators import update_all_auction_profiles

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_DIR = DB_DIR / "Signals"
CANDLE_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_PARQUET = SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet"
ALERTS_JSONL = SIGNALS_DIR / "realtime_trade_alerts.jsonl"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [UNIFIED_FOOTPRINT] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("UnifiedFootprintEngine")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# -----------------------------------------------------------------------------
# Asset Universe Configuration
# -----------------------------------------------------------------------------
CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "LTC",
    "DOT", "BCH", "NEAR", "TRX", "UNI", "AAVE", "FIL", "CRV", "ATOM", "ALGO",
    "AXS", "BAT", "CHZ", "AR", "COMP", "1INCH", "SUSHI", "SNX", "NEO", "ZEC",
    "HBAR", "ZRX"
]

FOREX_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "CADJPY", "CHFJPY", "EURAUD",
    "EURCAD", "EURCHF", "GBPAUD", "GBPCAD", "AUDCAD", "NZDCAD"
]

METALS_COMMODITIES = [
    ("XAUUSD", "Metals", ["XAU"]),
    ("XAGUSD", "Metals", ["XAG"]),
    ("USOUSD", "Commodities", ["USOIL"]),
    ("UKOUSD", "Commodities", ["UKOIL", "BRENTOIL"])
]

INDICES = [
    ("SPX500", "Indices", ["SP500"]),
    ("NDX100", "Indices", ["NAS100"]),
    ("US30", "Indices", ["DJI30"]),
    ("JP225", "Indices", []),
    ("GER30", "Indices", [])
]


class UnifiedAPIFootprintEngine:
    """
    Unified Engine executing 100% free API footprint collection and signal evaluation.
    """

    # TTL for signal deduplication cache: evict keys older than 24 hours
    _SIGNAL_KEY_TTL_MS: int = 86_400_000  # 24 h in milliseconds

    def __init__(self, rolling_window: int = 300, crypto_only: bool = False):
        self.rolling_window = rolling_window
        self.crypto_only = crypto_only
        self.engine = UniversalFootprintEngine(imbalance_ratio=3.0, min_delta_pct=5.0)
        self.evaluator: Optional[RealtimeSignalEvaluator] = None
        # Maps (symbol, sig_code, time_ms, price) → time_ms for TTL eviction
        self.known_signal_keys: Dict[Tuple[str, str, int, float], int] = {}
        self._mt5_initialized = False
        self._mt5_server_offset_sec = 0
        self._sweep_count: int = 0  # tracks sweeps for periodic TTL eviction

        self._load_known_signals()
        self._init_evaluator()
        if not self.crypto_only:
            self._init_mt5()

    def _init_evaluator(self):
        try:
            self.evaluator = RealtimeSignalEvaluator()
            logger.info("RealtimeSignalEvaluator initialized.")
        except Exception as e:
            logger.warning(f"Could not initialize RealtimeSignalEvaluator: {e}")
            self.evaluator = None

    def _evict_stale_signal_keys(self):
        """
        Evicts signal deduplication keys older than 24 hours.
        Called every sweep to bound memory growth.  A 300-bar × 5-signal × 100-asset
        worst case would otherwise accumulate ~150 k keys per day indefinitely.
        """
        cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - self._SIGNAL_KEY_TTL_MS
        stale = [k for k, t in self.known_signal_keys.items() if t < cutoff_ms]
        for k in stale:
            del self.known_signal_keys[k]
        if stale:
            logger.debug(f"TTL eviction: removed {len(stale)} stale signal keys (cache now {len(self.known_signal_keys)}).")

    def _load_known_signals(self):
        """Loads last-24h signal keys from parquet to prevent duplicate triggers on restart."""
        if SIGNALS_PARQUET.exists():
            try:
                df = pl.read_parquet(SIGNALS_PARQUET)
                cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - self._SIGNAL_KEY_TTL_MS
                for row in df.select(["symbol", "sig_code", "time_ms"]).iter_rows():
                    sym, code, t_ms = row
                    t_ms_int = int(t_ms or 0)
                    if t_ms_int >= cutoff_ms:  # only load recent keys — older ones can safely re-fire
                        key = (str(sym), str(code or ""), t_ms_int)
                        self.known_signal_keys[key] = t_ms_int
                logger.info(f"Loaded {len(self.known_signal_keys)} recent (24h) signal keys from historical parquet.")
            except Exception as e:
                logger.warning(f"Could not read existing signals parquet: {e}")

    def _init_mt5(self) -> bool:
        """Initializes MetaTrader 5 and computes dynamic server timezone offset with weekend guards."""
        try:
            import MetaTrader5 as mt5
            if not mt5.initialize():
                logger.warning("MetaTrader 5 initialization failed.")
                self._mt5_initialized = False
                return False

            acc = mt5.account_info()
            if acc:
                logger.info(f"Connected to MetaTrader 5 Account: {acc.login} ({acc.server})")
            
            # Default to Eastern European Time (EET UTC+2)
            offset_hrs = 2
            tick = mt5.symbol_info_tick("EURUSD")
            if tick and tick.time:
                server_tick_dt = datetime.fromtimestamp(tick.time, timezone.utc)
                utc_now = datetime.now(timezone.utc)
                diff_sec = (server_tick_dt - utc_now).total_seconds()
                # Weekend / market closure guard: only recompute if tick occurred within the past 4 hours
                if abs(diff_sec) < 14400:
                    calculated_offset = round(diff_sec / 3600.0)
                    if calculated_offset in (2, 3):
                        offset_hrs = calculated_offset
                    else:
                        logger.warning(f"Calculated offset {calculated_offset} out of bounds; falling back to EET (UTC+2)")
                else:
                    logger.info("Market closed or weekend detected (tick > 4h old); using deterministic EET offset (UTC+2).")
            
            self._mt5_server_offset_sec = offset_hrs * 3600
            logger.info(f"MT5 server time offset dynamically resolved: {offset_hrs:+d} hours ({self._mt5_server_offset_sec}s).")
            self._mt5_initialized = True
            return True
        except ImportError:
            logger.warning("MetaTrader5 python module not installed.")
            self._mt5_initialized = False
            return False
        except Exception as e:
            logger.warning(f"MT5 setup error: {e}")
            self._mt5_initialized = False
            return False

    def shutdown(self):
        if self._mt5_initialized:
            try:
                import MetaTrader5 as mt5
                mt5.shutdown()
                self._mt5_initialized = False
                logger.info("MetaTrader 5 shutdown complete.")
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # Ingestion Methods
    # -------------------------------------------------------------------------
    def fetch_binance_crypto_bars(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches 300 M15 klines from Binance Futures Public API with exact Taker Volume.
        URL: https://fapi.binance.com/fapi/v1/klines?symbol=<SYM>USDT&interval=15m&limit=300
        """
        api_symbol = f"{symbol}USDT"
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={api_symbol}&interval=15m&limit={self.rolling_window + 1}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                raw_data = json.loads(resp.read().decode("utf-8"))

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            bars = []
            for k in raw_data:
                open_t_ms = int(k[0])
                close_t_ms = int(k[6])
                # Drop uncompleted forming candle to ensure exact completed rolling window
                if close_t_ms > now_ms:
                    continue
                o = float(k[1])
                h = float(k[2])
                l = float(k[3])
                c = float(k[4])
                tot_vol = float(k[5])
                taker_buy_vol = float(k[9])
                sell_vol = max(tot_vol - taker_buy_vol, 0.0)
                delta = taker_buy_vol - sell_vol
                trades_count = float(k[8])

                bars.append({
                    "open_time_ms": open_t_ms,
                    "close_time_ms": close_t_ms,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "total_volume": tot_vol,
                    "buy_volume": taker_buy_vol,
                    "sell_volume": sell_vol,
                    "delta": delta,
                    "total_trades": trades_count
                })
            if len(bars) > self.rolling_window:
                bars = bars[-self.rolling_window:]
            return bars
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 10))
                logger.warning(f"Binance rate limit HTTP 429 on {symbol}; backing off for {retry_after}s.")
                time.sleep(retry_after)
            else:
                logger.debug(f"Binance HTTP error {e.code} for {symbol}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Binance fetch error for {symbol}: {e}")
            return None

    def fetch_mt5_bars(self, mt5_sym: str, asset_class: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches 300 M15 bars from local MetaTrader 5 API with server offset compensation.
        """
        if not self._mt5_initialized:
            return None
        try:
            import MetaTrader5 as mt5
            # Select symbol in Market Watch if not already active
            mt5.symbol_select(mt5_sym, True)
            rates = mt5.copy_rates_from_pos(mt5_sym, mt5.TIMEFRAME_M15, 0, self.rolling_window)
            if rates is None or len(rates) == 0:
                return None

            bars = []
            for r in rates:
                # Convert MT5 server epoch to true UTC epoch ms
                server_epoch_s = int(r["time"])
                utc_epoch_s = server_epoch_s - self._mt5_server_offset_sec
                open_t_ms = utc_epoch_s * 1000
                close_t_ms = open_t_ms + 900000

                o = float(r["open"])
                h = float(r["high"])
                l = float(r["low"])
                c = float(r["close"])
                tick_vol = float(r["tick_volume"])
                spread = float(r["spread"])

                bars.append({
                    "open_time_ms": open_t_ms,
                    "close_time_ms": close_t_ms,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "total_volume": tick_vol,
                    "spread": spread
                })
            return bars
        except Exception as e:
            logger.debug(f"MT5 fetch error for {mt5_sym}: {e}")
            return None

    # -------------------------------------------------------------------------
    # Atomic Lock-Free Parquet Persistence
    # -------------------------------------------------------------------------
    def save_rolling_parquet(self, df: pd.DataFrame, symbol: str, aliases: List[str] = []):
        """
        Atomically saves 300 rolling candle window to Database/Candle_Data/<symbol>.parquet.
        Uses atomic_parquet with retry backoff to survive Windows file-sharing locks.
        """
        target_path = CANDLE_DIR / f"{symbol}.parquet"
        try:
            rolling_df = df.tail(self.rolling_window).reset_index(drop=True)
            atomic_parquet(rolling_df, target_path)

            for alias in aliases:
                alias_target = CANDLE_DIR / f"{alias}.parquet"
                try:
                    atomic_parquet(rolling_df, alias_target)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed atomic parquet save for {symbol}: {e}")

    def append_signals(self, new_signals: List[Dict[str, Any]]):
        """Atomically appends new footprint signals to coinglass_aggregated_futures_signals.parquet."""
        if not new_signals:
            return

        with writer_lock(SIGNALS_PARQUET):
            try:
                new_df = pd.DataFrame(new_signals)
                for col in ["candle_ist", "open_time_ist", "close_time_ist", "time_ist"]:
                    if col not in new_df.columns and "open_time_ist" in new_df.columns:
                        new_df[col] = new_df["open_time_ist"]

                if SIGNALS_PARQUET.exists():
                    existing_df = pd.read_parquet(SIGNALS_PARQUET)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["symbol", "sig_code", "time_ms"]).reset_index(drop=True)
                else:
                    combined_df = new_df

                atomic_parquet(combined_df, SIGNALS_PARQUET)
                # Deduplication keys marked as known only after verified successful append
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                for s in new_signals:
                    key = (str(s["symbol"]), str(s.get("sig_code", "")), int(s.get("time_ms", 0)))
                    self.known_signal_keys[key] = int(s.get("time_ms", now_ms))
                logger.info(f"Appended {len(new_signals)} new footprint signals to {SIGNALS_PARQUET.name} (Total: {len(combined_df)})")
            except Exception as e:
                logger.error(f"Failed to append footprint signals: {e}")

    # -------------------------------------------------------------------------
    # Single Asset Processing Loop
    # -------------------------------------------------------------------------
    def process_asset(
        self,
        symbol: str,
        asset_class: str,
        mt5_symbol: Optional[str] = None,
        aliases: List[str] = []
    ) -> List[Dict[str, Any]]:
        """
        Pulls 300 bars via free API, computes footprint metrics, stores rolling parquet,
        and returns any newly triggered signals.
        """
        bars = None
        if asset_class == "Crypto":
            bars = self.fetch_binance_crypto_bars(symbol)
        else:
            fetch_sym = mt5_symbol or symbol
            bars = self.fetch_mt5_bars(fetch_sym, asset_class)

        if not bars or len(bars) < 10:
            return None

        # Run Universal Footprint Engine
        rolling_df, recent_sigs = self.engine.analyze_bars(
            bars=bars,
            symbol=symbol,
            asset_class=asset_class,
            mt5_symbol=mt5_symbol or symbol,
            rolling_window=self.rolling_window,
            inspect_last_n_bars=2
        )

        # Atomic Parquet save with IST mapping
        self.save_rolling_parquet(rolling_df, symbol, aliases)

        # Filter for genuine newly triggered signals on COMPLETED bars only
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        fresh_signals = []
        for s in recent_sigs:
            close_t = int(s.get("close_time_ms", 0))
            # Bar completion invariant: Never trigger execution on an unclosed candle that can repaint
            if close_t > now_ms:
                continue
            key = (str(s["symbol"]), str(s.get("sig_code", "")), int(s.get("time_ms", 0)))
            if key not in self.known_signal_keys:
                fresh_signals.append(s)

        return fresh_signals

    # -------------------------------------------------------------------------
    # Full Sweep Orchestrator
    # -------------------------------------------------------------------------
    def run_sweep(self, crypto_only: Optional[bool] = None) -> Dict[str, Any]:
        """
        Executes a single, lightning-fast sweep across Crypto (or all asset classes).
        """
        use_crypto_only = self.crypto_only if crypto_only is None else crypto_only
        t0 = time.time()
        all_new_signals: List[Dict[str, Any]] = []
        processed_count = 0

        target_str = "Crypto (Binance Futures)" if use_crypto_only else "Crypto, Forex, Metals, and Indices"
        logger.info(f"Starting unified API footprint sweep across {target_str}...")

        # Evict 24h-old deduplication keys every sweep to bound memory growth
        self._evict_stale_signal_keys()
        self._sweep_count += 1

        # 1. Sweep Crypto in parallel via Binance Futures API
        with ThreadPoolExecutor(max_workers=8) as executor:
            crypto_futures = {
                executor.submit(self.process_asset, sym, "Crypto", f"{sym}USDT"): sym
                for sym in CRYPTO_SYMBOLS
            }
            for fut in as_completed(crypto_futures):
                sym = crypto_futures[fut]
                try:
                    sigs = fut.result()
                    if sigs is not None:
                        if sigs:
                            all_new_signals.extend(sigs)
                        processed_count += 1
                except Exception as e:
                    logger.debug(f"Error processing crypto {sym}: {e}")

        if not use_crypto_only:
            # 2. Sweep Forex via MT5
            for sym in FOREX_PAIRS:
                try:
                    sigs = self.process_asset(sym, "Forex", sym)
                    if sigs:
                        all_new_signals.extend(sigs)
                    processed_count += 1
                except Exception as e:
                    logger.debug(f"Error processing Forex {sym}: {e}")

            # 3. Sweep Metals & Commodities via MT5
            for sym, asset_cls, aliases in METALS_COMMODITIES:
                try:
                    sigs = self.process_asset(sym, asset_cls, sym, aliases)
                    if sigs:
                        all_new_signals.extend(sigs)
                    processed_count += 1
                except Exception as e:
                    logger.debug(f"Error processing {sym}: {e}")

            # 4. Sweep Indices via MT5
            for sym, asset_cls, aliases in INDICES:
                try:
                    sigs = self.process_asset(sym, asset_cls, sym, aliases)
                    if sigs:
                        all_new_signals.extend(sigs)
                    processed_count += 1
                except Exception as e:
                    logger.debug(f"Error processing index {sym}: {e}")

        sweep_elapsed = time.time() - t0
        logger.info(
            f"Sweep complete: {processed_count} assets updated in {sweep_elapsed:.2f}s. "
            f"New signals triggered: {len(all_new_signals)}"
        )

        # Append new signals to global parquet
        if all_new_signals:
            self.append_signals(all_new_signals)

            # Evaluate through RealtimeSignalEvaluator for high-confluence execution
            if self.evaluator:
                try:
                    logger.info(f"Evaluating {len(all_new_signals)} fresh signals for MatchTrader / MT5 order execution...")
                    self.evaluator.evaluate_new_signals(all_new_signals)
                except Exception as e:
                    logger.error(f"Error in signal evaluation: {e}")

        # Update Auction Levels (VWAP, POC, Value Area)
        try:
            update_all_auction_profiles()
        except Exception as e:
            logger.debug(f"Auction profiles update error: {e}")

        return {
            "processed_assets": processed_count,
            "new_signals_count": len(all_new_signals),
            "elapsed_seconds": round(sweep_elapsed, 2)
        }

    # -------------------------------------------------------------------------
    # Continuous Daemon Mode
    # -------------------------------------------------------------------------
    def run_daemon(self, interval_seconds: int = 60):
        """Runs the unified footprint engine continuously in daemon mode."""
        acquire_singleton("unified_footprint_engine", DB_DIR / ".locks")
        logger.info(f"Unified API Footprint Daemon armed. Polling every {interval_seconds}s...")
        try:
            while True:
                try:
                    res = self.run_sweep()
                    logger.info(f"Next sweep in {interval_seconds}s (Updated: {res['processed_assets']} assets in {res['elapsed_seconds']}s)...")
                except Exception as e:
                    logger.error(f"Error during sweep loop: {e}", exc_info=True)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Daemon interrupted by user. Shutting down...")
        finally:
            self.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Unified API Footprint & Order Flow Engine")
    parser.add_argument("--once", action="store_true", help="Run a single sweep and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--crypto-only", action="store_true", help="Focus only on Crypto assets (Binance Futures)")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds (default: 60)")
    args = parser.parse_args()

    engine = UnifiedAPIFootprintEngine(crypto_only=args.crypto_only)

    if args.daemon:
        engine.run_daemon(interval_seconds=args.interval)
    else:
        res = engine.run_sweep()
        print(json.dumps(res, indent=2))
        engine.shutdown()


if __name__ == "__main__":
    main()
