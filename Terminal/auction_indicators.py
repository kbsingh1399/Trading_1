"""
Auction Market Theory, Institutional Levels & Multi-Timeframe Fibonacci-Prime Engine
=====================================================================================
Computes institutional auction levels, session profiles, Fib-Prime ribbons, and MTF alignment:

1. Prior Day Reference Levels:
   - Prior Day High (PDH), Prior Day Low (PDL), Prior Day Close (PDC)
   - Prior Day VWAP (pdVWAP), Prior Day POC (pdPOC), Prior Day VAH/VAL (pdVAH, pdVAL)
   - Prior Day Half-Back ((PDH + PDL) / 2)

2. Current Day Auction Profile & Initial Balance:
   - Current Day Open (CDO / 00:00 UTC), Day High (CDH), Day Low (CDL), Day Half-Back
   - Initial Balance High (IBH), Initial Balance Low (IBL) (First 60 min: 00:00-01:00 UTC)
   - Day VWAP & Standard Deviation Bands (±1σ, ±2σ)
   - 70% Value Area: Day POC, Day VAH, Day VAL, Value Area Status (INSIDE, ABOVE_VAH, BELOW_VAL)

3. Macro Timeframe Anchors:
   - Weekly Open (Monday 00:00 UTC Open)
   - Monthly Open (1st of month 00:00 UTC Open)

4. Session Anchored VWAPs:
   - Asia Session VWAP (00:00 - 08:00 UTC)
   - London Session VWAP (08:00 - 16:30 UTC)
   - New York Session VWAP (13:30 - 21:00 UTC)

5. Fibonacci-Prime EMAs (15m & 5m):
   - Prime & Fibonacci Periods: EMA 5, EMA 13, EMA 21, EMA 55, EMA 89, EMA 144, EMA 233
   - Ribbon Alignment: BULLISH_FIB_PRIME_STACK, BEARISH_FIB_PRIME_STACK, TREND_PULLBACK, CHOP

6. Higher Timeframe (HTF) Multi-Timeframe Alignment:
   - 1H Timeframe: 1H Trend, 1H Fib EMAs (13, 55, 89), 1H VWAP
   - 4H Timeframe: 4H Macro Trend, 4H Fib EMAs (21, 55, 233)
   - Triple Timeframe Confluence: HTF_BULLISH_CONFLUENCE, HTF_BEARISH_CONFLUENCE, HTF_DIVERGENCE

7. Microstructure Levels (Whale Walls & Liquidation Magnets):
   - Nearest Whale Bid/Ask Walls from Whale Orders database
   - Nearest Liquidation Magnet Zones from Liquidation database
   - Naked Point of Control (nVPOC) tracking

8. Continuous Local Persistence:
   - Persists Database/Auction_Levels/<SYMBOL>_auction_levels.parquet
   - Persists Database/Auction_Levels/all_symbols_auction_levels.parquet
   - Persists Database/Auction_Levels/all_symbols_auction_levels.json
"""

import sys
import os
import math
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import atomic_json, atomic_parquet, writer_lock

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
AUCTION_DIR = DB_DIR / "Auction_Levels"
WHALE_DIR = DB_DIR / "Whale_Orders" / "Open_Orders"
MAGNET_DIR = DB_DIR / "Liquidation_Data" / "Magnet_Zones"

AUCTION_DIR.mkdir(parents=True, exist_ok=True)


def get_tick_bin_size(symbol: str, price: float) -> float:
    """Adaptive price bin size for volume profile clustering."""
    if price >= 50000:
        return 50.0   # BTC
    elif price >= 1000:
        return 5.0    # ETH, Gold, Indices
    elif price >= 100:
        return 0.50   # SOL, Oil
    elif price >= 10:
        return 0.05   # LINK, AVAX, Silver
    elif price >= 1:
        return 0.005  # XRP, ADA, SUI
    else:
        return 0.0005 # DOGE, PEPE, SHIB


def compute_fib_prime_emas(series: pd.Series) -> Dict[str, float]:
    """Computes Fibonacci & Prime EMAs: 5, 13, 21, 34, 55, 59, 89, 144, 233."""
    spans = [5, 13, 21, 34, 55, 59, 89, 144, 233]
    emas = {}
    for s in spans:
        if len(series) >= s:
            emas[f"ema{s}"] = float(series.ewm(span=s, adjust=False).mean().iloc[-1])
        else:
            emas[f"ema{s}"] = float(series.ewm(span=min(len(series), s), adjust=False).mean().iloc[-1])
    return emas


def evaluate_fib_prime_stack(price: float, emas: Dict[str, float]) -> str:
    """Classifies the Fibonacci Prime Ribbon alignment."""
    e5 = emas.get("ema5", price)
    e13 = emas.get("ema13", price)
    e21 = emas.get("ema21", price)
    e34 = emas.get("ema34", price)
    e55 = emas.get("ema55", price)
    e59 = emas.get("ema59", price)
    e89 = emas.get("ema89", price)
    e144 = emas.get("ema144", price)
    e233 = emas.get("ema233", price)

    if price > e5 > e13 > e21 > e34 > e55 > e59 > e89 > e144 > e233:
        return "BULLISH_FIB_PRIME_STACK"
    elif price < e5 < e13 < e21 < e34 < e55 < e59 < e89 < e144 < e233:
        return "BEARISH_FIB_PRIME_STACK"
    elif price > e59 > e89 and e5 < e21:
        return "BULLISH_TREND_PULLBACK"
    elif price < e59 < e89 and e5 > e21:
        return "BEARISH_TREND_RALLY"
    elif price > e55 and price > e59:
        return "BULLISH_ABOVE_POCKET"
    elif price < e55 and price < e59:
        return "BEARISH_BELOW_POCKET"
    else:
        return "MIXED_COMPRESSION"


def compute_fib_prime_pullback_proximity(price: float, emas: Dict[str, float]) -> Dict[str, Any]:
    """Computes proximity to dynamic Fib-Prime pocket [EMA 34, EMA 55, EMA 59, EMA 89]."""
    e34 = emas.get("ema34", price)
    e55 = emas.get("ema55", price)
    e59 = emas.get("ema59", price)
    e89 = emas.get("ema89", price)
    e233 = emas.get("ema233", price)

    d34 = abs(price - e34) / price * 100.0 if price > 0 else 999.0
    d55 = abs(price - e55) / price * 100.0 if price > 0 else 999.0
    d59 = abs(price - e59) / price * 100.0 if price > 0 else 999.0
    d89 = abs(price - e89) / price * 100.0 if price > 0 else 999.0
    d233 = abs(price - e233) / price * 100.0 if price > 0 else 999.0

    dist_55_59 = min(d55, d59)
    in_pullback_pocket = dist_55_59 <= 0.60
    in_deep_pocket = min(d55, d59, d89) <= 0.60

    return {
        "dist_34_pct": round(d34, 3),
        "dist_55_pct": round(d55, 3),
        "dist_59_pct": round(d59, 3),
        "dist_89_pct": round(d89, 3),
        "dist_233_pct": round(d233, 3),
        "dist_55_59_pct": round(dist_55_59, 3),
        "in_pullback_pocket": in_pullback_pocket,
        "in_deep_pocket": in_deep_pocket
    }


def extract_prior_day_levels(df: pd.DataFrame, today_date) -> Dict[str, Any]:
    """Calculates PDH, PDL, PDC, pdVWAP, pdPOC, pdVAH, pdVAL, and pdHalfBack."""
    unique_days = sorted(df["day"].unique())
    if today_date in unique_days:
        idx = unique_days.index(today_date)
        if idx > 0:
            prior_date = unique_days[idx - 1]
            prior_df = df[df["day"] == prior_date].copy()
            if not prior_df.empty:
                pdh = float(prior_df["high"].max())
                pdl = float(prior_df["low"].min())
                pdc = float(prior_df["close"].iloc[-1])
                pd_half_back = float((pdh + pdl) / 2.0)
                
                # Prior day VWAP
                p_vol = prior_df["total_volume"].sum()
                pd_vwap = float(prior_df["pv"].sum() / p_vol) if p_vol > 0 else pdc
                
                # Prior day POC and Value Area
                bin_size = get_tick_bin_size(prior_df["symbol"].iloc[0] if "symbol" in prior_df else "BTC", pdc)
                prior_df["price_bin"] = np.round(prior_df["tp"] / bin_size) * bin_size
                vp = prior_df.groupby("price_bin")["total_volume"].sum().sort_index()
                pd_poc = float(vp.idxmax())
                
                target_vol = p_vol * 0.70
                sorted_vp = vp.sort_values(ascending=False)
                cum_vol = 0.0
                va_bins = []
                for p_bin, vol in sorted_vp.items():
                    cum_vol += vol
                    va_bins.append(float(p_bin))
                    if cum_vol >= target_vol:
                        break
                pd_vah = float(max(va_bins)) if va_bins else pdh
                pd_val = float(min(va_bins)) if va_bins else pdl
                
                return {
                    "pd_date": str(prior_date),
                    "pdh": round(pdh, 4),
                    "pdl": round(pdl, 4),
                    "pdc": round(pdc, 4),
                    "pd_half_back": round(pd_half_back, 4),
                    "pd_vwap": round(pd_vwap, 4),
                    "pd_poc": round(pd_poc, 4),
                    "pd_vah": round(pd_vah, 4),
                    "pd_val": round(pd_val, 4)
                }
    return {
        "pd_date": "", "pdh": 0.0, "pdl": 0.0, "pdc": 0.0,
        "pd_half_back": 0.0, "pd_vwap": 0.0, "pd_poc": 0.0, "pd_vah": 0.0, "pd_val": 0.0
    }


def compute_htf_alignment(df: pd.DataFrame) -> Dict[str, Any]:
    """Resamples 15m candles to 1H and 4H to establish institutional multi-timeframe bias."""
    try:
        df_indexed = df.set_index("dt").copy()
        
        # 1H Resample (Aggregated from 15m)
        df_1h = df_indexed.resample("1h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "total_volume": "sum"
        }).dropna()
        
        # 4H Resample (Aggregated from 15m)
        df_4h = df_indexed.resample("4h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "total_volume": "sum"
        }).dropna()
        
        # 1H Metrics (Direct 1H resample + 15m native span verification)
        h1_trend = "NEUTRAL"
        h1_ema13 = None
        h1_ema55 = None
        h1_ema89 = None
        if len(df_1h) >= 10:
            c_1h = df_1h["close"]
            h1_ema13 = float(c_1h.ewm(span=13, adjust=False).mean().iloc[-1])
            h1_ema55 = float(c_1h.ewm(span=55, adjust=False).mean().iloc[-1])
            h1_ema89 = float(c_1h.ewm(span=89, adjust=False).mean().iloc[-1])
            last_c1 = float(c_1h.iloc[-1])
            if last_c1 > h1_ema13 > h1_ema55:
                h1_trend = "BULLISH"
            elif last_c1 < h1_ema13 < h1_ema55:
                h1_trend = "BEARISH"
        elif len(df) >= 52:
            # 15m native equivalent: span 52 (1H EMA 13) and span 220 (1H EMA 55)
            c_15m = df["close"]
            h1_ema13 = float(c_15m.ewm(span=52, adjust=False).mean().iloc[-1])
            h1_ema55 = float(c_15m.ewm(span=min(len(c_15m), 220), adjust=False).mean().iloc[-1])
            last_c = float(c_15m.iloc[-1])
            if last_c > h1_ema13 > h1_ema55:
                h1_trend = "BULLISH"
            elif last_c < h1_ema13 < h1_ema55:
                h1_trend = "BEARISH"
                
        # 4H Metrics (Native 15m span verification to avoid sample starvation)
        h4_trend = "NEUTRAL"
        h4_ema21 = None
        h4_ema55 = None
        h4_ema233 = None
        if len(df_4h) >= 15:
            c_4h = df_4h["close"]
            h4_ema21 = float(c_4h.ewm(span=21, adjust=False).mean().iloc[-1])
            h4_ema55 = float(c_4h.ewm(span=55, adjust=False).mean().iloc[-1])
            h4_ema233 = float(c_4h.ewm(span=min(len(c_4h), 233), adjust=False).mean().iloc[-1])
            last_c4 = float(c_4h.iloc[-1])
            if last_c4 > h4_ema21 > h4_ema55:
                h4_trend = "BULLISH"
            elif last_c4 < h4_ema21 < h4_ema55:
                h4_trend = "BEARISH"
        elif len(df) >= 80:
            # 15m native equivalent: span 84 (4H EMA 21) and span 220 (4H EMA 55)
            c_15m = df["close"]
            h4_ema21 = float(c_15m.ewm(span=84, adjust=False).mean().iloc[-1])
            h4_ema55 = float(c_15m.ewm(span=min(len(c_15m), 220), adjust=False).mean().iloc[-1])
            last_c = float(c_15m.iloc[-1])
            if last_c > h4_ema21 > h4_ema55:
                h4_trend = "BULLISH"
            elif last_c < h4_ema21 < h4_ema55:
                h4_trend = "BEARISH"

        # Confluence classification
        if h1_trend == "BULLISH" and h4_trend == "BULLISH":
            mtf_bias = "HTF_BULLISH_CONFLUENCE"
        elif h1_trend == "BEARISH" and h4_trend == "BEARISH":
            mtf_bias = "HTF_BEARISH_CONFLUENCE"
        elif h1_trend != h4_trend and h4_trend != "NEUTRAL":
            mtf_bias = "HTF_DIVERGENCE"
        else:
            mtf_bias = "HTF_NEUTRAL"

        return {
            "htf_1h_trend": h1_trend,
            "htf_1h_ema13": round(h1_ema13, 4) if h1_ema13 else None,
            "htf_1h_ema55": round(h1_ema55, 4) if h1_ema55 else None,
            "htf_1h_ema89": round(h1_ema89, 4) if h1_ema89 else None,
            "htf_4h_trend": h4_trend,
            "htf_4h_ema21": round(h4_ema21, 4) if h4_ema21 else None,
            "htf_4h_ema55": round(h4_ema55, 4) if h4_ema55 else None,
            "htf_4h_ema233": round(h4_ema233, 4) if h4_ema233 else None,
            "mtf_bias": mtf_bias
        }
    except Exception as e:
        return {
            "htf_1h_trend": "NEUTRAL", "htf_4h_trend": "NEUTRAL", "mtf_bias": "HTF_UNAVAILABLE", "error": str(e)
        }


def extract_microstructure_levels(symbol: str, current_price: float) -> Dict[str, Any]:
    """Queries live Whale Order Walls & Liquidation Magnet Zones."""
    empty_level = {"price": 0.0, "amount_usd": 0.0, "score": 0.0}
    result = {
        "nearest_whale_bid": dict(empty_level), "nearest_whale_ask": dict(empty_level),
        "nearest_long_liq_magnet": dict(empty_level), "nearest_short_liq_magnet": dict(empty_level)
    }
    
    # 1. Whale Walls
    whale_file = WHALE_DIR / f"{symbol}_whale_open_orders.parquet"
    if whale_file.exists():
        try:
            wdf = pd.read_parquet(whale_file)
            if not wdf.empty and "price" in wdf.columns and "side" in wdf.columns:
                wdf["price"] = pd.to_numeric(wdf["price"], errors="coerce")
                bids = wdf[wdf["side"].str.upper().str.contains("BUY|BID")].dropna(subset=["price"])
                asks = wdf[wdf["side"].str.upper().str.contains("SELL|ASK")].dropna(subset=["price"])
                
                # Nearest bid wall below price
                bids_below = bids[bids["price"] <= current_price]
                if not bids_below.empty:
                    near_bid = bids_below.sort_values("price", ascending=False).iloc[0]
                    result["nearest_whale_bid"] = {
                        "price": float(near_bid["price"]),
                        "amount_usd": float(near_bid.get("amount_usd", near_bid.get("size", 0.0)))
                    }
                    
                # Nearest ask wall above price
                asks_above = asks[asks["price"] >= current_price]
                if not asks_above.empty:
                    near_ask = asks_above.sort_values("price", ascending=True).iloc[0]
                    result["nearest_whale_ask"] = {
                        "price": float(near_ask["price"]),
                        "amount_usd": float(near_ask.get("amount_usd", near_ask.get("size", 0.0)))
                    }
        except Exception:
            pass

    # 2. Liquidation Magnets
    magnet_file = MAGNET_DIR / f"{symbol}_magnet_zones.parquet"
    if magnet_file.exists():
        try:
            mdf = pd.read_parquet(magnet_file)
            price_col = "zone_price" if "zone_price" in mdf.columns else ("price" if "price" in mdf.columns else None)
            if not mdf.empty and "side" in mdf.columns:
                if price_col:
                    mdf["zone_price"] = pd.to_numeric(mdf[price_col], errors="coerce")
                elif "price_band" in mdf.columns:
                    import re
                    def parse_band_mid(val):
                        nums = re.findall(r"[\d,]+\.?\d*", str(val))
                        if len(nums) >= 2:
                            return (float(nums[0].replace(",", "")) + float(nums[1].replace(",", ""))) / 2.0
                        return None
                    mdf["zone_price"] = mdf["price_band"].apply(parse_band_mid)
                
                long_liq = mdf[mdf["side"].str.upper().str.contains("LONG")].dropna(subset=["zone_price"])
                short_liq = mdf[mdf["side"].str.upper().str.contains("SHORT")].dropna(subset=["zone_price"])
                
                if not long_liq.empty:
                    top_long = long_liq.sort_values("score", ascending=False).iloc[0] if "score" in long_liq.columns else long_liq.iloc[0]
                    result["nearest_long_liq_magnet"] = {
                        "price": float(top_long["zone_price"]),
                        "score": float(top_long.get("score", 0.0))
                    }
                if not short_liq.empty:
                    top_short = short_liq.sort_values("score", ascending=False).iloc[0] if "score" in short_liq.columns else short_liq.iloc[0]
                    result["nearest_short_liq_magnet"] = {
                        "price": float(top_short["zone_price"]),
                        "score": float(top_short.get("score", 0.0))
                    }
        except Exception:
            pass

    return result


def compute_auction_profile(symbol: str) -> Dict[str, Any]:
    """
    Computes complete Institutional Auction Levels, Fib-Prime EMAs,
    HTF Multi-Timeframe Alignment, and Microstructure Anchors.
    """
    candle_file = CANDLE_DIR / f"{symbol}.parquet"
    if not candle_file.exists():
        return {"data_valid": False, "error": f"No candle data for {symbol}"}

    try:
        df = pd.read_parquet(candle_file)
        if df.empty or len(df) < 10:
            return {"data_valid": False, "error": "Insufficient bars"}

        for c in ["open", "high", "low", "close", "total_volume"]:
            if c not in df.columns:
                return {"data_valid": False, "error": f"Missing column {c}"}
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["close", "total_volume"]).reset_index(drop=True)
        if len(df) < 10:
            return {"data_valid": False, "error": "Too few valid bars"}

        df["dt"] = pd.to_datetime(df["open_time_utc"], utc=True)
        df["tp"] = (df["high"] + df["low"] + df["close"]) / 3.0
        df["pv"] = df["tp"] * df["total_volume"]
        df["day"] = df["dt"].dt.date

        latest_close = float(df["close"].iloc[-1])
        today = df["day"].iloc[-1]
        today_df = df[df["day"] == today].copy()

        # Fallback to last 24h of bars if current day has too few bars (e.g. right after 00:00 UTC)
        if len(today_df) < 4:
            today_df = df.tail(96).copy()

        total_vol = float(today_df["total_volume"].sum())
        if total_vol <= 0:
            return {"data_valid": False, "error": "Zero volume"}

        # 1. Current Day Core Auction Levels
        day_open = float(today_df["open"].iloc[0])
        day_high = float(today_df["high"].max())
        day_low = float(today_df["low"].min())
        day_half_back = float((day_high + day_low) / 2.0)
        
        day_vwap = float(today_df["pv"].sum() / total_vol)
        today_df["dev_sq"] = ((today_df["tp"] - day_vwap) ** 2) * today_df["total_volume"]
        day_vwap_sd = float(np.sqrt(today_df["dev_sq"].sum() / total_vol))

        # 2. 70% Value Area & Point of Control (POC)
        bin_size = get_tick_bin_size(symbol, latest_close)
        today_df["price_bin"] = np.round(today_df["tp"] / bin_size) * bin_size
        vp = today_df.groupby("price_bin")["total_volume"].sum().sort_index()
        day_poc = float(vp.idxmax())

        target_vol = total_vol * 0.70
        sorted_vp = vp.sort_values(ascending=False)
        cum_vol = 0.0
        va_bins = []
        for p_bin, vol in sorted_vp.items():
            cum_vol += vol
            va_bins.append(float(p_bin))
            if cum_vol >= target_vol:
                break

        day_vah = float(max(va_bins))
        day_val = float(min(va_bins))

        if latest_close > day_vah:
            value_status = "ABOVE_VAH" # Premium
        elif latest_close < day_val:
            value_status = "BELOW_VAL" # Discount
        else:
            value_status = "INSIDE_VALUE"

        # 3. Initial Balance (First 60m of UTC Day: 00:00 to 01:00 UTC)
        ib_df = today_df[(today_df["dt"].dt.hour == 0)]
        if not ib_df.empty:
            ibh = float(ib_df["high"].max())
            ibl = float(ib_df["low"].min())
            ib_mid = float((ibh + ibl) / 2.0)
            if latest_close > ibh:
                ib_extension = "EXPANDING_UP"
            elif latest_close < ibl:
                ib_extension = "EXPANDING_DOWN"
            else:
                ib_extension = "INSIDE_IB"
        else:
            ibh = day_high
            ibl = day_low
            ib_mid = day_half_back
            ib_extension = "INSIDE_IB"

        # 4. Macro Open Anchors (Weekly Open & Monthly Open)
        now_dt = df["dt"].iloc[-1]
        monday_dt = now_dt - timedelta(days=now_dt.weekday())
        week_df = df[df["dt"] >= monday_dt.replace(hour=0, minute=0, second=0, microsecond=0)]
        weekly_open = float(week_df["open"].iloc[0]) if not week_df.empty else day_open
        
        month_df = df[df["dt"] >= now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)]
        monthly_open = float(month_df["open"].iloc[0]) if not month_df.empty else weekly_open

        # 5. Prior Day Levels
        pd_levels = extract_prior_day_levels(df, today)

        # 6. Session VWAPs
        cur_hour = today_df["dt"].iloc[-1].hour
        active_session = "ASIA" if cur_hour < 8 else ("LONDON" if cur_hour < 13 else ("NEW_YORK" if cur_hour < 21 else "ASIA"))

        asia_df = today_df[today_df["dt"].dt.hour < 8]
        asia_vwap = float(asia_df["pv"].sum() / asia_df["total_volume"].sum()) if not asia_df.empty and asia_df["total_volume"].sum() > 0 else day_vwap

        london_df = today_df[(today_df["dt"].dt.hour >= 8) & ((today_df["dt"].dt.hour < 16) | ((today_df["dt"].dt.hour == 16) & (today_df["dt"].dt.minute <= 30)))]
        london_vwap = float(london_df["pv"].sum() / london_df["total_volume"].sum()) if not london_df.empty and london_df["total_volume"].sum() > 0 else day_vwap

        ny_df = today_df[((today_df["dt"].dt.hour == 13) & (today_df["dt"].dt.minute >= 30)) | ((today_df["dt"].dt.hour > 13) & (today_df["dt"].dt.hour < 21))]
        ny_vwap = float(ny_df["pv"].sum() / ny_df["total_volume"].sum()) if not ny_df.empty and ny_df["total_volume"].sum() > 0 else day_vwap

        # 7. Fibonacci Prime EMAs (15m)
        fib_emas = compute_fib_prime_emas(df["close"])
        fib_prime_alignment = evaluate_fib_prime_stack(latest_close, fib_emas)
        pullback_prox = compute_fib_prime_pullback_proximity(latest_close, fib_emas)

        # 8. HTF Multi-Timeframe Alignment (1H and 4H)
        htf = compute_htf_alignment(df)

        # 9. Microstructure Levels (Whale Walls & Magnets)
        micro = extract_microstructure_levels(symbol, latest_close)

        # Recent Bar POC
        recent_bar_poc = float(df["poc_price"].iloc[-1]) if "poc_price" in df.columns and pd.notna(df["poc_price"].iloc[-1]) else None

        vwap_dist_pct = round(((latest_close - day_vwap) / day_vwap) * 100.0, 2)
        poc_dist_pct = round(((latest_close - day_poc) / day_poc) * 100.0, 2)

        profile = {
            "data_valid": True,
            "symbol": symbol,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "latest_close": latest_close,
            
            # Current Day Auction Levels
            "day_open": round(day_open, 4),
            "day_high": round(day_high, 4),
            "day_low": round(day_low, 4),
            "day_half_back": round(day_half_back, 4),
            "day_vwap": round(day_vwap, 4),
            "vwap_sd1_upper": round(day_vwap + day_vwap_sd, 4),
            "vwap_sd1_lower": round(day_vwap - day_vwap_sd, 4),
            "vwap_sd2_upper": round(day_vwap + 2 * day_vwap_sd, 4),
            "vwap_sd2_lower": round(day_vwap - 2 * day_vwap_sd, 4),
            "vwap_distance_pct": vwap_dist_pct,
            "price_vs_vwap": "ABOVE_VWAP" if latest_close >= day_vwap else "BELOW_VWAP",
            
            # Value Area & Volume Profile
            "day_poc": round(day_poc, 4),
            "day_vah": round(day_vah, 4),
            "day_val": round(day_val, 4),
            "value_status": value_status,
            "poc_distance_pct": poc_dist_pct,
            "recent_bar_poc": round(recent_bar_poc, 4) if recent_bar_poc else None,
            
            # Initial Balance
            "initial_balance_high": round(ibh, 4),
            "initial_balance_low": round(ibl, 4),
            "initial_balance_mid": round(ib_mid, 4),
            "ib_extension": ib_extension,
            
            # Macro Opens
            "weekly_open": round(weekly_open, 4),
            "weekly_open_dist_pct": round(((latest_close - weekly_open) / weekly_open) * 100.0, 2),
            "monthly_open": round(monthly_open, 4),
            
            # Prior Day Levels
            "pdh": pd_levels.get("pdh"),
            "pdl": pd_levels.get("pdl"),
            "pdc": pd_levels.get("pdc"),
            "pd_half_back": pd_levels.get("pd_half_back"),
            "pd_vwap": pd_levels.get("pd_vwap"),
            "pd_poc": pd_levels.get("pd_poc"),
            "pd_vah": pd_levels.get("pd_vah"),
            "pd_val": pd_levels.get("pd_val"),
            
            # Session VWAPs
            "active_session": active_session,
            "asia_vwap": round(asia_vwap, 4) if asia_vwap else round(day_vwap, 4),
            "london_vwap": round(london_vwap, 4) if london_vwap else round(day_vwap, 4),
            "ny_vwap": round(ny_vwap, 4) if ny_vwap else round(day_vwap, 4),
            
            # Fibonacci-Prime EMAs (15m)
            "ema5": round(fib_emas.get("ema5", 0.0), 4),
            "ema13": round(fib_emas.get("ema13", 0.0), 4),
            "ema21": round(fib_emas.get("ema21", 0.0), 4),
            "ema34": round(fib_emas.get("ema34", 0.0), 4),
            "ema55": round(fib_emas.get("ema55", 0.0), 4),
            "ema59": round(fib_emas.get("ema59", 0.0), 4),
            "ema89": round(fib_emas.get("ema89", 0.0), 4),
            "ema144": round(fib_emas.get("ema144", 0.0), 4),
            "ema233": round(fib_emas.get("ema233", 0.0), 4),
            "fib_prime_alignment": fib_prime_alignment,
            "dist_55_59_pct": pullback_prox.get("dist_55_59_pct", 999.0),
            "in_pullback_pocket": pullback_prox.get("in_pullback_pocket", False),
            "in_deep_pocket": pullback_prox.get("in_deep_pocket", False),
            "dist_34_pct": pullback_prox.get("dist_34_pct", 999.0),
            "dist_55_pct": pullback_prox.get("dist_55_pct", 999.0),
            "dist_59_pct": pullback_prox.get("dist_59_pct", 999.0),
            "dist_89_pct": pullback_prox.get("dist_89_pct", 999.0),
            "dist_233_pct": pullback_prox.get("dist_233_pct", 999.0),
            # Backward compatibility aliases
            "ema9": round(fib_emas.get("ema13", 0.0), 4),
            "ema20": round(fib_emas.get("ema21", 0.0), 4),
            "ema50": round(fib_emas.get("ema55", 0.0), 4),
            "ema200": round(fib_emas.get("ema233", 0.0), 4),
            "ema_alignment": "BULLISH_STACK" if "BULLISH" in fib_prime_alignment else ("BEARISH_STACK" if "BEARISH" in fib_prime_alignment else "MIXED"),
            
            # Higher Timeframe Alignment
            "htf_1h_trend": htf.get("htf_1h_trend"),
            "htf_1h_ema13": htf.get("htf_1h_ema13"),
            "htf_1h_ema55": htf.get("htf_1h_ema55"),
            "htf_4h_trend": htf.get("htf_4h_trend"),
            "htf_4h_ema21": htf.get("htf_4h_ema21"),
            "htf_4h_ema55": htf.get("htf_4h_ema55"),
            "mtf_bias": htf.get("mtf_bias"),
            
            # Microstructure Anchors
            "nearest_whale_bid": micro.get("nearest_whale_bid"),
            "nearest_whale_ask": micro.get("nearest_whale_ask"),
            "nearest_long_liq_magnet": micro.get("nearest_long_liq_magnet"),
            "nearest_short_liq_magnet": micro.get("nearest_short_liq_magnet")
        }

        # Atomically save individual symbol profile
        save_symbol_auction_level(symbol, profile)

        return profile

    except Exception as e:
        return {"data_valid": False, "error": str(e)}


def save_symbol_auction_level(symbol: str, profile: Dict[str, Any]):
    """Persists symbol auction level to Parquet file."""
    try:
        sym_file = AUCTION_DIR / f"{symbol}_auction_levels.parquet"
        flat_record = {}
        for k, v in profile.items():
            if isinstance(v, (dict, list)):
                flat_record[k] = json.dumps(v)
            elif v is None:
                flat_record[k] = 0.0 if any(term in k for term in ["vwap", "price", "ema", "pd", "level", "sd"]) else ""
            else:
                flat_record[k] = v
        df = pd.DataFrame([flat_record])
        atomic_parquet(df, sym_file)
    except Exception:
        pass


def update_all_auction_profiles() -> Dict[str, Any]:
    """
    Computes and updates Auction Profiles across all available symbols,
    persisting Database/Auction_Levels/all_symbols_auction_levels.parquet
    and Database/Auction_Levels/all_symbols_auction_levels.json.
    """
    all_profiles = {}
    records = []
    candle_files = list(CANDLE_DIR.glob("*.parquet"))
    
    for f in candle_files:
        symbol = f.stem
        if symbol.endswith(".writer"):
            continue
        prof = compute_auction_profile(symbol)
        if prof.get("data_valid"):
            all_profiles[symbol] = prof
            flat_rec = {}
            for k, v in prof.items():
                if isinstance(v, (dict, list)):
                    flat_rec[k] = json.dumps(v)
                elif v is None:
                    flat_rec[k] = 0.0 if any(term in k for term in ["vwap", "price", "ema", "pd", "level", "sd"]) else ""
                else:
                    flat_rec[k] = v
            records.append(flat_rec)

    # Persist aggregated tables atomically
    if records:
        try:
            agg_parquet = AUCTION_DIR / "all_symbols_auction_levels.parquet"
            agg_json = AUCTION_DIR / "all_symbols_auction_levels.json"
            agg_df = pd.DataFrame(records)
            atomic_parquet(agg_df, agg_parquet)
            atomic_json(agg_json, all_profiles)
        except Exception:
            pass

    return all_profiles


if __name__ == "__main__":
    print("=== Testing Institutional Auction & Fib-Prime MTF Engine ===")
    btc = compute_auction_profile("BTC")
    if btc.get("data_valid"):
        print(f"\n[BTC Core Levels]")
        print(f"  Day Open:        ${btc['day_open']:,.2f} | Day High: ${btc['day_high']:,.2f} | Day Low: ${btc['day_low']:,.2f} | Half-Back: ${btc['day_half_back']:,.2f}")
        print(f"  Initial Balance: High=${btc['initial_balance_high']:,.2f} | Low=${btc['initial_balance_low']:,.2f} | Extension: {btc['ib_extension']}")
        print(f"  Weekly Open:     ${btc['weekly_open']:,.2f} ({btc['weekly_open_dist_pct']:+.2f}%)")
        print(f"  Prior Day:       PDH=${btc['pdh']:,.2f} | PDL=${btc['pdl']:,.2f} | PDC=${btc['pdc']:,.2f} | pdPOC=${btc['pd_poc']:,.2f} | pdVWAP=${btc['pd_vwap']:,.2f}")
        print(f"  Day VWAP:        ${btc['day_vwap']:,.2f} (+/-1SD: ${btc['vwap_sd1_lower']:,.2f} - ${btc['vwap_sd1_upper']:,.2f})")
        print(f"  Day POC / VA:    POC=${btc['day_poc']:,.2f} | VAH=${btc['day_vah']:,.2f} | VAL=${btc['day_val']:,.2f} ({btc['value_status']})")
        print(f"  Fib-Prime EMAs:  5:${btc['ema5']:,.1f} | 13:${btc['ema13']:,.1f} | 21:${btc['ema21']:,.1f} | 55:${btc['ema55']:,.1f} | 89:${btc['ema89']:,.1f} | 144:${btc['ema144']:,.1f} | 233:${btc['ema233']:,.1f}")
        print(f"  Ribbon Stack:    {btc['fib_prime_alignment']}")
        print(f"  HTF Alignment:   1H Trend={btc['htf_1h_trend']} | 4H Trend={btc['htf_4h_trend']} | MTF Bias={btc['mtf_bias']}")
        print(f"  Whale Walls:     Nearest Bid={btc['nearest_whale_bid']} | Nearest Ask={btc['nearest_whale_ask']}")
        print(f"  Liq Magnets:     Long Liq={btc['nearest_long_liq_magnet']} | Short Liq={btc['nearest_short_liq_magnet']}")
    else:
        print("BTC Error:", btc.get("error"))

    print("\nRunning update_all_auction_profiles()...")
    res = update_all_auction_profiles()
    print(f"Successfully computed and persisted profiles for {len(res)} assets!")
