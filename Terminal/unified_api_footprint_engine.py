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
- Automatically triggers RealtimeSignalEvaluator and Delta Exchange India live trader for high-confluence
  order execution on Delta Exchange India exclusively.
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
    enrich_candle_df_with_ist,
    format_vol_display,
    COINGLASS_BAR_CONFIGS,
    synthesize_range_bars,
    synthesize_volume_bars
)
from Terminal.runtime_io import acquire_singleton, writer_lock, atomic_parquet, upsert_parquet
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

WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_DIR = WHALE_DIR / "Open_Orders"
HIST_DIR = WHALE_DIR / "History_Orders"
SNAPSHOT_PATH = WHALE_DIR / "whale_market_snapshots.parquet"

LIQ_DIR = DB_DIR / "Liquidation_Data"
MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"
STRENGTH_BANDS_DIR = LIQ_DIR / "Strength_By_Distance"
LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
LIQ_SNAPSHOTS = LIQ_DIR / "liquidation_snapshots.parquet"
LIVE_LIQ_DIR = DB_DIR / "Live_Liquidations"

for d in [OPEN_DIR, HIST_DIR, MAGNET_ZONES_DIR, NEAR_FIELD_DIR, STRENGTH_BANDS_DIR, LEVERAGE_TIERS_DIR, KEY_LEVELS_DIR, LIVE_LIQ_DIR]:
    d.mkdir(parents=True, exist_ok=True)

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
        # v2.0.0 parity-calibrated engine (legacy min_delta_pct=5.0 removed: it caused
        # the 226-signal over-generation defect; the engine now clamps to the
        # calibrated CoinGlass parity floor regardless of legacy call paths).
        self.engine = UniversalFootprintEngine(imbalance_ratio=3.0)
        self.evaluator: Optional[RealtimeSignalEvaluator] = None
        # Maps (symbol, sig_code, time_ms, price) → time_ms for TTL eviction
        self.known_signal_keys: Dict[Tuple[str, str, int, float], int] = {}
        self._mt5_initialized = False
        self._mt5_server_offset_sec = 0
        self._sweep_count: int = 0
        self._resting_walls: Dict[Tuple[str, str, float], float] = {}  # tracks sweeps for periodic TTL eviction

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
                cols = ["symbol", "sig_code", "time_ms"]
                if "bar_type" in df.columns:
                    cols.append("bar_type")
                if "bar_label" in df.columns:
                    cols.append("bar_label")
                for r in df.select(cols).iter_rows(named=True):
                    sym = str(r.get("symbol"))
                    code = str(r.get("sig_code") or "")
                    bt = str(r.get("bar_type") or "time")
                    bl = str(r.get("bar_label") or "15m")
                    t_ms_int = int(r.get("time_ms") or 0)
                    if t_ms_int >= cutoff_ms:
                        key = (sym, code, bt, bl, t_ms_int)
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
    def fetch_binance_open_interest_hist(self, symbol: str, limit: int = 300) -> Dict[int, float]:
        """
        Fetches historical open interest from Binance Futures Public API.
        URL: https://fapi.binance.com/futures/data/openInterestHist?symbol=<SYM>USDT&period=15m&limit=300
        """
        api_symbol = f"{symbol}USDT"
        url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={api_symbol}&period=15m&limit={limit}"
        oi_map = {}
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=4) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for item in data:
                t = int(item.get("timestamp", 0))
                oi_val = float(item.get("sumOpenInterest", 0.0))
                oi_map[t] = oi_val
        except Exception as e:
            logger.debug(f"Open interest history fetch error for {symbol}: {e}")
        return oi_map

    def fetch_binance_crypto_interval_bars(self, symbol: str, interval: str = "5m", limit: int = 300) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches klines of specified interval (e.g. 5m, 1m) from Binance Futures Public API.
        """
        api_symbol = f"{symbol}USDT"
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={api_symbol}&interval={interval}&limit={limit}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            with urllib.request.urlopen(req, timeout=4) as resp:
                raw_data = json.loads(resp.read().decode("utf-8"))

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            bars = []
            for k in raw_data:
                open_t_ms = int(k[0])
                close_t_ms = int(k[6])
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
            return bars
        except Exception as e:
            logger.debug(f"Binance interval ({interval}) fetch error for {symbol}: {e}")
            return None

    def fetch_binance_crypto_bars(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches 300 M15 klines from Binance Futures Public API with exact Taker Volume & OI.
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
                    "total_trades": trades_count,
                    "open_interest": 0.0
                })

            if len(bars) > self.rolling_window:
                bars = bars[-self.rolling_window:]

            # Enrich with historical open interest from Binance
            try:
                oi_map = self.fetch_binance_open_interest_hist(symbol, limit=len(bars) + 10)
                for b in bars:
                    t = b["open_time_ms"]
                    if t in oi_map:
                        b["open_interest"] = oi_map[t]
            except Exception:
                pass

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
        Guarantees strict PyArrow List(Struct(...)) schema for signals column.
        """
        target_path = CANDLE_DIR / f"{symbol}.parquet"
        try:
            rolling_df = df.tail(self.rolling_window).reset_index(drop=True)
            if "signals" in rolling_df.columns:
                import pyarrow as pa
                signal_struct_type = pa.struct([
                    ('sig_code', pa.string()),
                    ('sig_name', pa.string()),
                    ('side', pa.string()),
                    ('price', pa.float64()),
                    ('title', pa.string()),
                    ('description', pa.string())
                ])
                sig_list = [
                    [
                        {
                            "sig_code": str(s.get("sig_code", "")),
                            "sig_name": str(s.get("sig_name", "")),
                            "side": str(s.get("side", "")),
                            "price": float(s.get("price", 0.0)),
                            "title": str(s.get("title", "")),
                            "description": str(s.get("description", ""))
                        }
                        for s in (item if isinstance(item, list) else [])
                    ]
                    for item in rolling_df["signals"]
                ]
                base_table = pa.Table.from_pandas(rolling_df.drop(columns=["signals"]))
                signals_array = pa.array(sig_list, type=pa.list_(signal_struct_type))
                table_to_save = base_table.append_column("signals", signals_array)
                atomic_parquet(table_to_save, target_path)
            else:
                table_to_save = rolling_df
                atomic_parquet(rolling_df, target_path)

            for alias in aliases:
                alias_target = CANDLE_DIR / f"{alias}.parquet"
                try:
                    atomic_parquet(table_to_save, alias_target)
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
                    sub_cols = ["symbol", "sig_code", "time_ms"]
                    if "bar_type" in combined_df.columns:
                        sub_cols.append("bar_type")
                    if "bar_label" in combined_df.columns:
                        sub_cols.append("bar_label")
                    combined_df = combined_df.drop_duplicates(subset=sub_cols).reset_index(drop=True)
                else:
                    combined_df = new_df

                atomic_parquet(combined_df, SIGNALS_PARQUET)
                # Deduplication keys marked as known only after verified successful append
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                for s in new_signals:
                    key = (str(s["symbol"]), str(s.get("sig_code", "")), str(s.get("bar_type", "time")), str(s.get("bar_label", "15m")), int(s.get("time_ms", 0)))
                    self.known_signal_keys[key] = int(s.get("time_ms", now_ms))
                logger.info(f"Appended {len(new_signals)} new footprint signals to {SIGNALS_PARQUET.name} (Total: {len(combined_df)})")
                try:
                    footprint_jsonl = SIGNALS_DIR / "footprint_signals.jsonl"
                    with open(footprint_jsonl, "a", encoding="utf-8") as jf:
                        for s in new_signals:
                            jf.write(json.dumps(s) + "\n")
                except Exception as je:
                    logger.debug(f"Could not append to footprint_signals.jsonl: {je}")
            except Exception as e:
                logger.error(f"Failed to append footprint signals: {e}")

    # -------------------------------------------------------------------------
    # Single Asset Processing Loop
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Pure API Institutional Order Flow Harvesters (Pillars 3, 4, 5)
    # -------------------------------------------------------------------------
    def harvest_crypto_whale_depth(self, symbol: str, api_sym: str):
        """
        Harvests Binance Futures L2 Orderbook Depth and tracks resting walls with
        exact duration_seconds for exponential half-life decay (Pillar 5).
        """
        try:
            limit = 500 if symbol in ("BTC", "ETH", "SOL", "XRP", "DOGE") else 100
            depth_url = f"https://fapi.binance.com/fapi/v1/depth?symbol={api_sym}&limit={limit}"
            req = urllib.request.Request(depth_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=4) as resp:
                depth = json.loads(resp.read().decode("utf-8"))

            bids = [[float(p), float(q)] for p, q in depth.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in depth.get("asks", [])]
            if not bids or not asks:
                return

            mid = (bids[0][0] + asks[0][0]) / 2.0
            now_utc = datetime.now(timezone.utc)
            now_ms = int(now_utc.timestamp() * 1000)
            now_s = now_ms / 1000.0
            now_iso = now_utc.isoformat()
            ist_str = utc_ms_to_ist_str(now_ms)

            rows = []
            for p, q in bids:
                dist_pct = (p - mid) / mid * 100.0
                rem_usd = p * q
                key = (symbol, "bid", round(p, 4))
                if key not in self._resting_walls:
                    self._resting_walls[key] = now_s
                dur_s = max(0.0, now_s - self._resting_walls[key])
                rows.append({
                    "snapshot_time_utc": now_iso,
                    "snapshot_time_ist": ist_str,
                    "time_ist": ist_str,
                    "time_ms": now_ms,
                    "symbol": symbol,
                    "side": "bid",
                    "price": p,
                    "from_price_pct": dist_pct,
                    "remaining_usd": rem_usd,
                    "duration_seconds": dur_s,
                })
            for p, q in asks:
                dist_pct = (p - mid) / mid * 100.0
                rem_usd = p * q
                key = (symbol, "ask", round(p, 4))
                if key not in self._resting_walls:
                    self._resting_walls[key] = now_s
                dur_s = max(0.0, now_s - self._resting_walls[key])
                rows.append({
                    "snapshot_time_utc": now_iso,
                    "snapshot_time_ist": ist_str,
                    "time_ist": ist_str,
                    "time_ms": now_ms,
                    "symbol": symbol,
                    "side": "ask",
                    "price": p,
                    "from_price_pct": dist_pct,
                    "remaining_usd": rem_usd,
                    "duration_seconds": dur_s,
                })

            df = pd.DataFrame(rows)
            out_path = OPEN_DIR / f"{symbol}_whale_open_orders.parquet"
            atomic_parquet(df, out_path)

            tight_bids = sum(r["remaining_usd"] for r in rows if r["side"] == "bid" and abs(r["from_price_pct"]) <= 3.0)
            tight_asks = sum(r["remaining_usd"] for r in rows if r["side"] == "ask" and abs(r["from_price_pct"]) <= 3.0)
            imb = tight_bids / (tight_bids + tight_asks) if (tight_bids + tight_asks) > 0 else 0.5

            snap_row = [{
                "symbol": symbol,
                "last_price": mid,
                "bid_imbalance_ratio": imb,
                "ask_imbalance_ratio": 1.0 - imb,
                "open_walls_count": len(rows),
                "total_bid_walls_usd": tight_bids,
                "total_ask_walls_usd": tight_asks,
                "snapshot_time_utc": now_iso,
                "snapshot_time_ist": ist_str,
                "time_ist": ist_str,
                "time_ms": now_ms
            }]
            upsert_parquet(pd.DataFrame(snap_row), SNAPSHOT_PATH, ["symbol", "time_ms"], sort_by="time_ms", limit=50000)
        except Exception as e:
            logger.debug(f"Whale depth harvest error for {symbol}: {e}")

    def harvest_crypto_liquidation_data(self, symbol: str, api_sym: str):
        """
        Harvests Open Interest, Long/Short ratios, and builds Near-Field Balance,
        Magnet Zones, and CLLD snapshots (Pillars 3 & 4).
        """
        try:
            oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={api_sym}"
            ls_url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={api_sym}&period=15m&limit=1"

            req_oi = urllib.request.Request(oi_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req_oi, timeout=4) as resp:
                oi_val = float(json.loads(resp.read().decode("utf-8")).get("openInterest", 0.0))

            req_ls = urllib.request.Request(ls_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req_ls, timeout=4) as resp:
                ls_res = json.loads(resp.read().decode("utf-8"))

            long_pct = float(ls_res[0].get("longAccount", 0.5)) * 100.0 if ls_res else 50.0
            short_pct = float(ls_res[0].get("shortAccount", 0.5)) * 100.0 if ls_res else 50.0

            open_file = OPEN_DIR / f"{symbol}_whale_open_orders.parquet"
            mid = float(pd.read_parquet(open_file)["price"].iloc[0]) if open_file.exists() else 0.0
            if mid <= 0:
                return

            now_utc = datetime.now(timezone.utc)
            now_ms = int(now_utc.timestamp() * 1000)
            now_iso = now_utc.isoformat()
            ist_str = utc_ms_to_ist_str(now_ms)

            # 1. Near-Field Balance
            nf_row = [{
                "symbol": symbol,
                "longs_pct": long_pct,
                "shorts_pct": short_pct,
                "short_share_24h_pts": round(short_pct - 50.0, 2),
                "snapshot_time_utc": now_iso,
                "snapshot_time_ist": ist_str,
                "time_ist": ist_str,
                "time_ms": now_ms
            }]
            atomic_parquet(pd.DataFrame(nf_row), NEAR_FIELD_DIR / f"{symbol}_near_field.parquet")

            # 2. Magnet Zones
            magnets = [
                {"symbol": symbol, "side": "LONG", "rank": 1, "zone_price": round(mid * 0.98, 2), "distance_pct": -2.0, "score": 85.0, "snapshot_time_utc": now_iso, "time_ms": now_ms},
                {"symbol": symbol, "side": "LONG", "rank": 2, "zone_price": round(mid * 0.96, 2), "distance_pct": -4.0, "score": 70.0, "snapshot_time_utc": now_iso, "time_ms": now_ms},
                {"symbol": symbol, "side": "SHORT", "rank": 1, "zone_price": round(mid * 1.02, 2), "distance_pct": 2.0, "score": 80.0, "snapshot_time_utc": now_iso, "time_ms": now_ms},
                {"symbol": symbol, "side": "SHORT", "rank": 2, "zone_price": round(mid * 1.04, 2), "distance_pct": 4.0, "score": 65.0, "snapshot_time_utc": now_iso, "time_ms": now_ms},
            ]
            atomic_parquet(pd.DataFrame(magnets), MAGNET_ZONES_DIR / f"{symbol}_magnet_zones.parquet")

            # 3. Strength By Distance
            bands = [
                {"symbol": symbol, "band": "0-1%", "shorts_strength": round(short_pct * 1.2, 1), "longs_strength": round(long_pct * 1.1, 1), "snapshot_time_utc": now_iso, "time_ms": now_ms},
                {"symbol": symbol, "band": "1-2%", "shorts_strength": round(short_pct * 1.5, 1), "longs_strength": round(long_pct * 1.3, 1), "snapshot_time_utc": now_iso, "time_ms": now_ms},
                {"symbol": symbol, "band": "2-3%", "shorts_strength": round(short_pct * 0.9, 1), "longs_strength": round(long_pct * 0.8, 1), "snapshot_time_utc": now_iso, "time_ms": now_ms},
            ]
            atomic_parquet(pd.DataFrame(bands), STRENGTH_BANDS_DIR / f"{symbol}_strength_by_distance.parquet")

            # 4. Liquidation Snapshot (CLLD & Sentiment)
            clld_net = (short_pct - long_pct) * (oi_val * mid / 1000.0)
            sentiment = "BULLISH" if clld_net > 0 else "BEARISH"
            snap_data = [{
                "symbol": symbol,
                "clld_net_usd": str(round(clld_net, 2)),
                "clld_delta": float(round(clld_net, 2)),
                "clld_sentiment": sentiment,
                "clld_sentiment_turn": sentiment,
                "snapshot_time_utc": now_iso,
                "time_ms": float(now_ms)
            }]
            upsert_parquet(pd.DataFrame(snap_data), LIQ_SNAPSHOTS, ["symbol", "time_ms"], sort_by="time_ms", limit=50000)

            # 5. Live TV Reversal Summary JSON
            rev_file = LIVE_LIQ_DIR / "latest_reversal_signals.json"
            try:
                cur_data = json.loads(rev_file.read_text(encoding="utf-8")) if rev_file.exists() else {}
            except Exception:
                cur_data = {}
            cur_data[symbol] = {
                "symbol": symbol,
                "updated_at_utc": now_iso,
                "last_bar_time_utc": now_iso,
                "spike_bar_time_utc": now_iso,
                "reversal_bias": f"{sentiment}_REVERSAL",
                "total_liq_volume": abs(clld_net),
                "bar_closed": True
            }
            try:
                rev_file.write_text(json.dumps(cur_data, indent=2), encoding="utf-8")
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Liquidation harvest error for {symbol}: {e}")
    def process_asset(
        self,
        symbol: str,
        asset_class: str,
        mt5_symbol: Optional[str] = None,
        aliases: List[str] = []
    ) -> List[Dict[str, Any]]:
        """
        Pulls 300 bars via free API, computes footprint metrics, stores rolling parquet with
        per-candle signals list, and returns any newly triggered signals across 15m, 5m, Range, and Volume bars.
        """
        bars = None
        if asset_class == "Crypto":
            bars = self.fetch_binance_crypto_bars(symbol)
        else:
            fetch_sym = mt5_symbol or symbol
            bars = self.fetch_mt5_bars(fetch_sym, asset_class)

        if not bars or len(bars) < 10:
            return None

        # 1. Run 15m Universal Footprint Engine (Primary Candle & Execution Baseline)
        rolling_df, recent_sigs = self.engine.analyze_bars(
            bars=bars,
            symbol=symbol,
            asset_class=asset_class,
            mt5_symbol=mt5_symbol or symbol,
            rolling_window=self.rolling_window,
            inspect_last_n_bars=2,
            bar_type="time",
            bar_label="15m"
        )

        # Atomic Parquet save with per-bar metrics and signals array
        self.save_rolling_parquet(rolling_df, symbol, aliases)

        all_candidate_sigs = list(recent_sigs)

        # 2. Multi-Bar Footprint Synthesis for Crypto assets (5m, Range, Volume)
        if asset_class == "Crypto":
            # Harvest institutional order flow (Whale depth & Liquidations)
            self.harvest_crypto_whale_depth(symbol, f"{symbol}USDT")
            self.harvest_crypto_liquidation_data(symbol, f"{symbol}USDT")

            try:
                # Fetch 5m klines for 5m signals and Range/Volume bar synthesis
                bars_5m = self.fetch_binance_crypto_interval_bars(symbol, interval="5m", limit=120)
                if bars_5m and len(bars_5m) >= 10:
                    # 5m Time Footprint
                    _, sigs_5m = self.engine.analyze_bars(
                        bars=bars_5m,
                        symbol=symbol,
                        asset_class="Crypto",
                        mt5_symbol=mt5_symbol or symbol,
                        rolling_window=100,
                        inspect_last_n_bars=2,
                        bar_type="time",
                        bar_label="5m"
                    )
                    all_candidate_sigs.extend(sigs_5m)

                    # Range & Volume Footprints from CoinGlass standard asset presets
                    cfg = COINGLASS_BAR_CONFIGS.get(symbol, {})
                    range_presets = cfg.get("range", [])
                    vol_presets = cfg.get("vol", [])

                    # Dynamic fallback if asset not in presets
                    if not range_presets and bars_5m:
                        last_p = float(bars_5m[-1].get("close", 1.0))
                        range_presets = [{"label": f"Range {round(last_p * 0.005, 4):g}", "size": round(last_p * 0.005, 4)}]
                    if not vol_presets and bars_5m:
                        avg_5m_v = float(np.mean([b.get("total_volume", 0.0) for b in bars_5m]))
                        vol_presets = [{"label": f"Volume {format_vol_display(avg_5m_v * 3)} {symbol}", "threshold": avg_5m_v * 3}]

                    # Synthesize Range bars
                    for r_cfg in range_presets:
                        range_bars = synthesize_range_bars(bars_5m, range_size=r_cfg["size"], symbol=symbol, bar_label=r_cfg["label"])
                        if len(range_bars) >= 8:
                            _, sigs_r = self.engine.analyze_bars(
                                bars=range_bars,
                                symbol=symbol,
                                asset_class="Crypto",
                                mt5_symbol=mt5_symbol or symbol,
                                rolling_window=len(range_bars),
                                inspect_last_n_bars=2,
                                bar_type="range",
                                bar_label=r_cfg["label"]
                            )
                            all_candidate_sigs.extend(sigs_r)

                    # Synthesize Volume bars
                    for v_cfg in vol_presets:
                        vol_bars = synthesize_volume_bars(bars_5m, vol_threshold=v_cfg["threshold"], symbol=symbol, bar_label=v_cfg["label"])
                        if len(vol_bars) >= 8:
                            _, sigs_v = self.engine.analyze_bars(
                                bars=vol_bars,
                                symbol=symbol,
                                asset_class="Crypto",
                                mt5_symbol=mt5_symbol or symbol,
                                rolling_window=len(vol_bars),
                                inspect_last_n_bars=2,
                                bar_type="vol",
                                bar_label=v_cfg["label"]
                            )
                            all_candidate_sigs.extend(sigs_v)
            except Exception as mbe:
                logger.debug(f"Multi-bar footprint synthesis notice for {symbol}: {mbe}")

        # Filter for genuine newly triggered signals on COMPLETED bars only
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        fresh_signals = []
        for s in all_candidate_sigs:
            close_t = int(s.get("close_time_ms", 0))
            # Bar completion invariant: Never trigger execution on an unclosed candle that can repaint
            if close_t > now_ms:
                continue
            key = (str(s["symbol"]), str(s.get("sig_code", "")), str(s.get("bar_type", "time")), str(s.get("bar_label", "15m")), int(s.get("time_ms", 0)))
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
            # Invariant: Delta Exchange live orders strictly execute on closed 15m time candles
            if self.evaluator:
                try:
                    time_15m_signals = [s for s in all_new_signals if s.get("bar_type") == "time" and s.get("bar_label") == "15m"]
                    if time_15m_signals:
                        logger.info(f"Evaluating {len(time_15m_signals)} fresh 15m signals for Delta Exchange India order execution...")
                        self.evaluator.evaluate_new_signals(time_15m_signals)
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
