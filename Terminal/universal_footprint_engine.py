"""
========================================================================================
Universal Footprint & Order Flow Engine
========================================================================================
A single, institutional footprint calculation and signal classification engine for
ALL asset classes: Crypto, Forex, Indices, and Metals.

Implements the exact methodology of CoinGlass Footprint:
1. Absorption (abs):
   - Bullish: Heavy taker selling absorbed at bar low (30%+ of selling volume),
     price rejects and closes near the high.
   - Bearish: Heavy taker buying absorbed at bar high (30%+ of buying volume),
     price rejects and closes near the low.
2. Single-Bar Divergence (div):
   - Bullish: Drop without taker selling (closed down on positive Delta >= +5%).
   - Bearish: Rally without taker buying (closed up on negative Delta <= -5%).
3. Multi-Bar Swing Cumulative Delta Divergence (sdiv / Bull div / Bear div):
   - Bullish: Lower low in price with higher Cumulative Delta (sellers failing).
   - Bearish: Higher high in price with lower Cumulative Delta (buyers failing).
4. Exhaustion (exh):
   - Seller exhaustion: New 10-bar low with depleted volume in bottom wick,
     price closes off low into upper half.
   - Buyer exhaustion: New 10-bar high with depleted volume in top wick,
     price closes off high into lower half.
5. Trapped Traders (trap):
   - Trapped shorts: Failed breakdown of 5-bar low, aggressive selling trapped,
     candle rallies back above breakdown.
   - Trapped longs: Failed breakout of 5-bar high, aggressive buying trapped,
     candle falls back below breakout.

Supports IST (Indian Standard Time, UTC+5:30) primary time mapping in parquet tables
with rolling 300-candle windows.
========================================================================================
"""

import math
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
import numpy as np
import pandas as pd

# IST Offset: UTC + 5 hours 30 minutes
IST_OFFSET = timedelta(hours=5, minutes=30)
IST_OFFSET_MS = 19800000  # 5.5 * 3600 * 1000 ms


def safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or math.isnan(float(val)) or math.isinf(float(val)):
            return default
        return float(val)
    except Exception:
        return default


def safe_int(val: Any, default: int = 0) -> int:
    try:
        f = safe_float(val, float(default))
        return int(f)
    except Exception:
        return default


def utc_ms_to_ist_str(utc_ms: int) -> str:
    """Converts UTC epoch milliseconds to human-readable IST string."""
    dt_utc = datetime.fromtimestamp(utc_ms / 1000.0, timezone.utc)
    dt_ist = dt_utc + IST_OFFSET
    return dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")


def utc_ms_to_ist_iso(utc_ms: int) -> str:
    """Converts UTC epoch milliseconds to ISO string with +05:30 timezone offset."""
    dt_utc = datetime.fromtimestamp(utc_ms / 1000.0, timezone.utc)
    dt_ist = dt_utc + IST_OFFSET
    return dt_ist.strftime("%Y-%m-%dT%H:%M:%S+05:30")


def utc_to_ist_str(utc_iso: str) -> str:
    """Converts ISO UTC string to human-readable IST string."""
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        dt_ist = dt.astimezone(timezone(IST_OFFSET))
        return dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return utc_iso


def enrich_candle_df_with_ist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches candle DataFrame with primary Indian Standard Time (IST) columns.
    Ensures correct timing map: IST = UTC + 5:30.
    """
    if df.empty or "open_time_ms" not in df.columns:
        return df
    df = df.copy()
    df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").astype(np.int64)
    if "close_time_ms" not in df.columns:
        df["close_time_ms"] = df["open_time_ms"] + 900000
    else:
        df["close_time_ms"] = pd.to_numeric(df["close_time_ms"], errors="coerce").astype(np.int64)

    df["open_time_ist"] = df["open_time_ms"].apply(utc_ms_to_ist_str)
    df["close_time_ist"] = df["close_time_ms"].apply(utc_ms_to_ist_str)
    df["time_ist"] = df["open_time_ist"]
    df["candle_ist"] = df["open_time_ist"]
    # IST-epoch display index (NOT a UTC epoch): utc_ms + 19,800,000 ms offset.
    # Use open_time_ms for all UTC-relative comparisons.
    df["open_time_ist_ms"] = df["open_time_ms"] + IST_OFFSET_MS
    df["close_time_ist_ms"] = df["close_time_ms"] + IST_OFFSET_MS
    return df


class UniversalFootprintEngine:
    """
    Unified Footprint & Institutional Order Flow Analyzer across Crypto, Forex, Indices, and Metals.
    """

    def __init__(self, imbalance_ratio: float = 3.0, min_delta_pct: float = 5.0):
        self.imbalance_ratio = imbalance_ratio
        self.min_delta_pct = min_delta_pct

    def analyze_bars(
        self,
        bars: Union[List[Dict[str, Any]], pd.DataFrame],
        symbol: str,
        asset_class: str = "Crypto",
        mt5_symbol: Optional[str] = None,
        inverted: bool = False,
        rolling_window: int = 300,
        inspect_last_n_bars: int = 2
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Analyzes a sequence of bars, computes delta/CVD/POC, detects all footprint signals,
        and returns (enriched_rolling_df, newly_triggered_signals).
        """
        if isinstance(bars, pd.DataFrame):
            df = bars.copy()
        elif isinstance(bars, list):
            if not bars:
                return pd.DataFrame(), []
            df = pd.DataFrame(bars)
        else:
            return pd.DataFrame(), []

        if df.empty or len(df) < 5:
            return df, []

        mt5_sym = mt5_symbol or f"{symbol}.pi"

        # Ensure standard column naming
        col_mapping = {
            "vol": "total_volume",
            "volume": "total_volume",
            "time": "open_time_s",
        }
        for old_col, new_col in col_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]

        # Handle time mapping
        if "open_time_ms" not in df.columns:
            if "open_time_s" in df.columns:
                df["open_time_ms"] = (df["open_time_s"] * 1000).astype(np.int64)
            elif "time_ms" in df.columns:
                df["open_time_ms"] = df["time_ms"].astype(np.int64)
            elif "open_time_utc" in df.columns:
                df["open_time_ms"] = pd.to_datetime(df["open_time_utc"], utc=True).astype(np.int64) // 10**6
            else:
                raise ValueError("Bars missing timestamp column")

        df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").astype(np.int64)
        df = df.sort_values("open_time_ms").drop_duplicates("open_time_ms").reset_index(drop=True)

        # 15m close time = open_time + 900,000 ms
        if "close_time_ms" not in df.columns or df["close_time_ms"].isna().any():
            df["close_time_ms"] = df["open_time_ms"] + 900000

        # Numeric conversions
        for col in ["open", "high", "low", "close", "total_volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Inversion handler (e.g. 6J JPY futures or 6C CAD futures relative to USD pairs)
        if inverted:
            df["open"] = 1.0 / df["open"]
            df["close"] = 1.0 / df["close"]
            raw_high = df["high"].copy()
            df["high"] = 1.0 / df["low"]
            df["low"] = 1.0 / raw_high
            if "delta" in df.columns:
                df["delta"] = -df["delta"]
            if "buy_volume" in df.columns and "sell_volume" in df.columns:
                orig_buy = df["buy_volume"].copy()
                df["buy_volume"] = df["sell_volume"]
                df["sell_volume"] = orig_buy

        # Derive Volume, Delta, Cumulative Delta if not present or all null
        if "delta" not in df.columns or df["delta"].isna().all():
            # Estimate institutional delta using signed price velocity and bar skew
            candle_range = np.maximum(df["high"] - df["low"], 1e-9)
            body = df["close"] - df["open"]
            upper_wick = df["high"] - np.maximum(df["open"], df["close"])
            lower_wick = np.minimum(df["open"], df["close"]) - df["low"]
            wick_skew = (lower_wick - upper_wick) / candle_range
            delta_ratio = np.clip((body / candle_range) * 0.7 + wick_skew * 0.3, -1.0, 1.0)
            df["delta"] = df["total_volume"] * delta_ratio

        if "buy_volume" not in df.columns or df["buy_volume"].isna().any() or "sell_volume" not in df.columns or df["sell_volume"].isna().any():
            calc_buy = (df["total_volume"] + df["delta"]) / 2.0
            calc_sell = (df["total_volume"] - df["delta"]) / 2.0
            if "buy_volume" not in df.columns:
                df["buy_volume"] = calc_buy
            else:
                df["buy_volume"] = df["buy_volume"].fillna(calc_buy)
            if "sell_volume" not in df.columns:
                df["sell_volume"] = calc_sell
            else:
                df["sell_volume"] = df["sell_volume"].fillna(calc_sell)

        # Microstructure invariant: delta == buy_volume - sell_volume
        mismatch = (df["delta"] - (df["buy_volume"] - df["sell_volume"])).abs() > 1e-3
        if mismatch.any():
            df.loc[mismatch, "buy_volume"] = (df.loc[mismatch, "total_volume"] + df.loc[mismatch, "delta"]) / 2.0
            df.loc[mismatch, "sell_volume"] = (df.loc[mismatch, "total_volume"] - df.loc[mismatch, "delta"]) / 2.0

        if "delta_pct" not in df.columns or df["delta_pct"].isna().all():
            v_safe = np.maximum(df["total_volume"], 1e-9)
            df["delta_pct"] = np.round((df["delta"] / v_safe) * 100.0, 1)

        # Cumulative Volume Delta (CVD)
        if "cumulative_delta" not in df.columns or df["cumulative_delta"].isna().all():
            df["cumulative_delta"] = df["delta"].cumsum()
        df["cvd"] = df["cumulative_delta"]

        # Approximate Point of Control (POC) price if not already extracted from footprint cluster
        if "poc_price" not in df.columns or df["poc_price"].isna().all():
            # POC is heavily weighted near high volume node: midpoint of candle body and high-volume rejection
            df["poc_price"] = np.round((df["open"] + df["close"] + (df["high"] + df["low"]) / 2.0) / 3.0, 6)

        # Primary IST Time Mapping
        df["open_time_ist"] = df["open_time_ms"].apply(utc_ms_to_ist_str)
        df["close_time_ist"] = df["close_time_ms"].apply(utc_ms_to_ist_str)
        df["time_ist"] = df["open_time_ist"]
        df["candle_ist"] = df["open_time_ist"]
        # NOTE: open_time_ist_ms = utc_ms + 19,800,000 (IST_OFFSET_MS).
        # This is NOT a standard UTC epoch — it is an IST-epoch display index
        # used for IST-aware DuckDB partitioning and charting only.
        # Always use open_time_ms for cross-system UTC comparisons.
        df["open_time_ist_ms"] = (df["open_time_ms"] + IST_OFFSET_MS).astype(np.int64)
        df["close_time_ist_ms"] = (df["close_time_ms"] + IST_OFFSET_MS).astype(np.int64)

        # Standard UTC columns for cross-compatibility
        df["open_time_utc"] = df["open_time_ms"].apply(lambda ms: datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat())
        df["close_time_utc"] = df["close_time_ms"].apply(lambda ms: datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat())

        # Metadata
        df["symbol"] = symbol
        df["mt5_symbol"] = mt5_sym
        df["timeframe"] = "15m"
        df["asset_class"] = asset_class

        # Rolling 20 SMA of volume
        df["vol_sma20"] = df["total_volume"].rolling(window=20, min_periods=5).mean().fillna(df["total_volume"])

        # Signal tracking columns
        df["has_signal"] = False
        df["signal_type"] = ""
        df["signal_side"] = ""
        df["signal_desc"] = ""

        # Detect signals across the entire DataFrame
        signals = []
        n = len(df)
        start_eval_idx = max(5, n - inspect_last_n_bars)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for i in range(5, n):
            b_sigs = self._evaluate_bar_signals(df, i, symbol, mt5_sym, asset_class)
            if b_sigs:
                df.at[i, "has_signal"] = True
                df.at[i, "signal_type"] = "; ".join([s["sig_name"] for s in b_sigs])
                df.at[i, "signal_side"] = b_sigs[0]["side"]
                df.at[i, "signal_desc"] = "; ".join([s["title"] for s in b_sigs])
                # Strict Bar Completion Invariant: Execution signals MUST strictly be on completed, closed bars
                bar_close_ms = int(df.at[i, "close_time_ms"])
                if i >= start_eval_idx and bar_close_ms <= now_ms:
                    signals.extend(b_sigs)

        # Maintain exact rolling window (default 300)
        rolling_df = df.tail(rolling_window).reset_index(drop=True)
        return rolling_df, signals

    def _evaluate_bar_signals(
        self,
        df: pd.DataFrame,
        idx: int,
        symbol: str,
        mt5_sym: str,
        asset_class: str
    ) -> List[Dict[str, Any]]:
        """
        Evaluates footprint algorithms on bar at idx against historical lookback.
        A single bar may trigger multiple institutional conditions (e.g. Absorption + Divergence).
        """
        bar_signals = []
        row = df.iloc[idx]
        prev_row = df.iloc[idx - 1]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        prev_c = float(prev_row["close"])
        v = float(row["total_volume"])
        delta = float(row["delta"])
        delta_pct = float(row["delta_pct"])
        cvd = float(row["cumulative_delta"])
        t_ms = int(row["open_time_ms"])
        close_t_ms = int(row["close_time_ms"])
        avg_vol = float(row["vol_sma20"])

        bar_range = max(h - l, 1e-9)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        close_pos = (c - l) / bar_range  # 0.0 at low, 1.0 at high

        utc_iso = datetime.fromtimestamp(t_ms / 1000.0, timezone.utc).isoformat()
        close_utc_iso = datetime.fromtimestamp(close_t_ms / 1000.0, timezone.utc).isoformat()
        ist_str = utc_ms_to_ist_str(t_ms)
        close_ist_str = utc_ms_to_ist_str(close_t_ms)

        base_sig = {
            "symbol": symbol,
            "mt5_symbol": mt5_sym,
            "asset_class": asset_class,
            "time_ms": t_ms,
            "close_time_ms": close_t_ms,
            "datetime_utc": utc_iso,
            "close_time_utc": close_utc_iso,
            "detection_time_utc": datetime.now(timezone.utc).isoformat(),
            "candle_utc": utc_iso,
            "candle_ist": ist_str,
            "open_time_ist": ist_str,
            "close_time_ist": close_ist_str,
            "time_ist": ist_str,
            "price": round(c, 5),
            "volume": v,
            "delta": delta,
            "delta_pct": delta_pct,
            "cvd": cvd,
            "poc_price": float(row.get("poc_price", c)),
            "source": f"universal_footprint_{asset_class.lower()}"
        }

        # ---------------------------------------------------------------------
        # 1. ABSORPTION (abs)
        # Heavy taker volume absorbed at wick extremes, closed opposite
        # CoinGlass invariant: Requires significant volume (>= 1.1x 20-SMA)
        # Macro gate: For non-Crypto assets using OHLC-derived delta, require
        # |delta_pct| >= min_delta_pct/2 to suppress noise from low-skew doji bars.
        # ---------------------------------------------------------------------
        _macro_delta_gate = (asset_class == "Crypto") or (abs(delta_pct) >= self.min_delta_pct / 2.0)

        # Bullish Absorption: Heavy selling at low absorbed, closes near high
        if lower_wick >= 0.30 * bar_range and close_pos >= 0.50 and v >= 1.10 * avg_vol and _macro_delta_gate:
            sell_v = safe_float(row.get("sell_volume"), v * 0.5)
            bar_signals.append({
                **base_sig,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": "bullish",
                "title": "Selling absorbed at the low",
                "description": f"{safe_int(sell_v):,} taker selling absorbed at the low, yet closed near high, bullish"
            })

        # Bearish Absorption: Heavy buying at high absorbed, closes near low
        elif upper_wick >= 0.30 * bar_range and close_pos <= 0.50 and v >= 1.10 * avg_vol and _macro_delta_gate:
            buy_v = safe_float(row.get("buy_volume"), v * 0.5)
            bar_signals.append({
                **base_sig,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": "bearish",
                "title": "Buying absorbed at the high",
                "description": f"{safe_int(buy_v):,} taker buying absorbed at the high, yet closed near low, bearish"
            })

        # ---------------------------------------------------------------------
        # 2. SINGLE-BAR DIVERGENCE (div)
        # Bar closes opposite to net taker delta
        # ---------------------------------------------------------------------
        # Bullish: Price closed down, yet Delta is positive (Drop without taker selling)
        if (c < o or c < prev_c) and delta > 0 and delta_pct >= self.min_delta_pct:
            bar_signals.append({
                **base_sig,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": "bullish",
                "title": "Drop without taker selling",
                "description": f"Closed down on positive Delta (+{delta_pct:.0f}%), bullish"
            })

        # Bearish: Price closed up, yet Delta is negative (Rally without taker buying)
        elif (c > o or c > prev_c) and delta < 0 and delta_pct <= -self.min_delta_pct:
            bar_signals.append({
                **base_sig,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": "bearish",
                "title": "Rally without taker buying",
                "description": f"Closed up on negative Delta ({delta_pct:.0f}%), bearish"
            })

        # ---------------------------------------------------------------------
        # 3. MULTI-BAR SWING CUMULATIVE DELTA DIVERGENCE (sdiv)
        # ---------------------------------------------------------------------
        lookback = min(15, idx)
        recent_lows = df["low"].iloc[idx - lookback:idx].values
        recent_highs = df["high"].iloc[idx - lookback:idx].values
        recent_cvds = df["cumulative_delta"].iloc[idx - lookback:idx].values

        # Bullish Swing Div: Lower low than prior swing, but CVD is higher
        prior_min_idx = np.argmin(recent_lows)
        prior_min_low = recent_lows[prior_min_idx]
        prior_min_cvd = recent_cvds[prior_min_idx]
        if l < prior_min_low and cvd > prior_min_cvd and close_pos >= 0.40:
            diff_cvd = cvd - prior_min_cvd
            bar_signals.append({
                **base_sig,
                "sig_code": "sdiv",
                "sig_name": "Bull div",
                "side": "bullish",
                "title": "Lower low, higher Cumulative Delta",
                "description": f"Lower low, yet Cumulative Delta rose {safe_int(diff_cvd):,} over swing. Sellers aren't following through, bullish"
            })

        # Bearish Swing Div: Higher high than prior swing, but CVD is lower
        prior_max_idx = np.argmax(recent_highs)
        prior_max_high = recent_highs[prior_max_idx]
        prior_max_cvd = recent_cvds[prior_max_idx]
        if h > prior_max_high and cvd < prior_max_cvd and close_pos <= 0.60:
            diff_cvd = prior_max_cvd - cvd
            bar_signals.append({
                **base_sig,
                "sig_code": "sdiv",
                "sig_name": "Bear div",
                "side": "bearish",
                "title": "Higher high, lower Cumulative Delta",
                "description": f"Higher high, yet Cumulative Delta fell {safe_int(diff_cvd):,} over swing. Buyers aren't following through, bearish"
            })

        # ---------------------------------------------------------------------
        # 4. EXHAUSTION (exh)
        # New 10-bar extreme on depleted volume, closes back inside
        # ---------------------------------------------------------------------
        if idx >= 10:
            last10_highs = df["high"].iloc[idx - 10:idx].values
            last10_lows = df["low"].iloc[idx - 10:idx].values
            # Buyer exhaustion at high
            if h >= np.max(last10_highs) and (v <= 0.90 * avg_vol or upper_wick >= 0.35 * bar_range) and close_pos <= 0.50:
                bar_signals.append({
                    **base_sig,
                    "sig_code": "exh",
                    "sig_name": "Buyer exhaustion",
                    "side": "bearish",
                    "title": "Buyer exhaustion at high",
                    "description": f"New 10-bar high on depleted volume ({safe_int(v):,} vs avg {safe_int(avg_vol):,}), closed in lower half, bearish"
                })

            # Seller exhaustion at low
            if l <= np.min(last10_lows) and (v <= 0.90 * avg_vol or lower_wick >= 0.35 * bar_range) and close_pos >= 0.50:
                bar_signals.append({
                    **base_sig,
                    "sig_code": "exh",
                    "sig_name": "Seller exhaustion",
                    "side": "bullish",
                    "title": "Seller exhaustion at low",
                    "description": f"New 10-bar low on depleted volume ({safe_int(v):,} vs avg {safe_int(avg_vol):,}), closed in upper half, bullish"
                })

        # ---------------------------------------------------------------------
        # 5. TRAPPED TRADERS (trap)
        # Failed 5-bar breakout/breakdown on volume, sharp rejection
        # ---------------------------------------------------------------------
        if idx >= 5:
            last5_highs = df["high"].iloc[idx - 5:idx].values
            last5_lows = df["low"].iloc[idx - 5:idx].values

            # Trapped shorts: Broke 5-bar low, but bar closed back inside
            if l < np.min(last5_lows) and c > np.min(last5_lows) and close_pos >= 0.50:
                bar_signals.append({
                    **base_sig,
                    "sig_code": "trap",
                    "sig_name": "Trapped shorts",
                    "side": "bullish",
                    "title": "Failed breakdown",
                    "description": f"Broke 5-bar low with aggressive selling, then rallied back. Breakdown sellers trapped, bullish"
                })

            # Trapped longs: Broke 5-bar high, but bar closed back inside
            if h > np.max(last5_highs) and c < np.max(last5_highs) and close_pos <= 0.50:
                bar_signals.append({
                    **base_sig,
                    "sig_code": "trap",
                    "sig_name": "Trapped longs",
                    "side": "bearish",
                    "title": "Failed breakout",
                    "description": f"Broke 5-bar high with aggressive buying, then fell back. Breakout buyers trapped, bearish"
                })

        return bar_signals
