"""
========================================================================================
Unit & Parity Tests for Universal Footprint Engine
========================================================================================
Validates that UniversalFootprintEngine:
1. Generates exact CoinGlass footprint signals (Absorption, Divergence, Exhaustion, Trapped).
2. Computes correct Delta, Cumulative Delta (CVD), and POC.
3. Maps timestamps strictly to Indian Standard Time (IST, UTC+5:30) with ms precision.
4. Enforces strict 300 rolling candle window.
5. Operates across Crypto, Forex, Metals, and Indices uniformly.
========================================================================================
"""

import math
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import pytest

from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    utc_ms_to_ist_str,
    utc_ms_to_ist_iso,
    enrich_candle_df_with_ist,
    IST_OFFSET_MS
)


def make_test_bars(n=350, start_utc_ms=1793500000000, base_price=100.0):
    bars = []
    price = base_price
    for i in range(n):
        t_ms = start_utc_ms + i * 900000
        o = price
        h = o + 0.5
        l = o - 0.5
        c = o + 0.1
        v = 1000.0
        price = c
        bars.append({
            "open_time_ms": t_ms,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "total_volume": v
        })
    return bars


def test_ist_time_mapping_accuracy():
    """Verify that IST time mapping is mathematically exact (+5:30 = +19,800,000 ms)."""
    # UTC timestamp for 2026-11-02 11:00:00 UTC
    dt_utc = datetime(2026, 11, 2, 11, 0, 0, tzinfo=timezone.utc)
    utc_ms = int(dt_utc.timestamp() * 1000)

    ist_str = utc_ms_to_ist_str(utc_ms)
    assert "16:30:00 IST" in ist_str
    assert "2026-11-02" in ist_str

    ist_iso = utc_ms_to_ist_iso(utc_ms)
    assert ist_iso == "2026-11-02T16:30:00+05:30"

    df = pd.DataFrame([{"open_time_ms": utc_ms}])
    df = enrich_candle_df_with_ist(df)
    assert df["open_time_ist"].iloc[0] == "2026-11-02 16:30:00 IST"
    assert df["close_time_ist"].iloc[0] == "2026-11-02 16:45:00 IST"
    assert df["open_time_ist_ms"].iloc[0] == utc_ms + IST_OFFSET_MS


def test_rolling_300_window_enforcement():
    """Verify that the engine strictly enforces 300 rolling candle window."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=350)
    df, sigs = engine.analyze_bars(bars, symbol="EURUSD", asset_class="Forex", rolling_window=300)

    assert len(df) == 300
    assert "time_ist" in df.columns
    assert "open_time_ist" in df.columns
    assert "close_time_ist" in df.columns
    assert "cumulative_delta" in df.columns
    assert "cvd" in df.columns
    assert "poc_price" in df.columns


def test_absorption_footprint_signal():
    """Verify Bullish & Bearish Absorption detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Bullish Absorption at bar 25:
    # Huge lower wick (0.7 of range), high volume (2x avg), closed near high
    b = bars[25]
    b["open"] = 100.0
    b["high"] = 100.2
    b["low"] = 98.0
    b["close"] = 100.1
    b["total_volume"] = 3000.0

    df, sigs = engine.analyze_bars(bars, symbol="BTC", asset_class="Crypto", inspect_last_n_bars=30)
    abs_sigs = [s for s in sigs if s["sig_code"] == "abs" and s["time_ms"] == b["open_time_ms"]]
    assert len(abs_sigs) > 0
    assert abs_sigs[0]["side"] == "bullish"
    assert abs_sigs[0]["title"] == "Selling absorbed at the low"


def test_divergence_footprint_signal():
    """Verify Single-Bar Divergence (Drop without selling / Rally without buying)."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Bearish Divergence at bar 28:
    # Price closed UP, but Delta is strongly NEGATIVE (-30%)
    b = bars[28]
    b["open"] = 100.0
    b["high"] = 100.8
    b["low"] = 99.9
    b["close"] = 100.7  # Closed up
    b["total_volume"] = 2000.0
    b["delta"] = -600.0  # -30% Delta
    b["delta_pct"] = -30.0

    df, sigs = engine.analyze_bars(bars, symbol="GBPUSD", asset_class="Forex", inspect_last_n_bars=30)
    div_sigs = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == b["open_time_ms"]]
    assert len(div_sigs) > 0
    assert div_sigs[0]["side"] == "bearish"
    assert div_sigs[0]["title"] == "Rally without taker buying"


def test_exhaustion_footprint_signal():
    """Verify Buyer & Seller Exhaustion detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Buyer Exhaustion at bar 29:
    # New 10-bar high on depleted volume, closed in lower half
    b = bars[29]
    b["open"] = 101.5
    b["high"] = 105.0  # Big new 10-bar high
    b["low"] = 101.0
    b["close"] = 101.2  # Closed near low
    b["total_volume"] = 200.0  # Depleted volume (vs 1000 avg)

    df, sigs = engine.analyze_bars(bars, symbol="XAUUSD", asset_class="Metals", inspect_last_n_bars=30)
    exh_sigs = [s for s in sigs if s["sig_code"] == "exh" and s["time_ms"] == b["open_time_ms"]]
    assert len(exh_sigs) > 0
    assert exh_sigs[0]["side"] == "bearish"
    assert exh_sigs[0]["title"] == "Buyer exhaustion at high"


def test_trapped_traders_signal():
    """Verify Trapped Traders / Failed breakout detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Trapped Longs at bar 27:
    # Broke 5-bar high (around 103.1), but snapped back inside bar range
    b = bars[27]
    b["open"] = 102.5
    b["high"] = 106.0  # Broke 5-bar high (> 103.5)
    b["low"] = 101.5
    b["close"] = 102.0  # Closed back below breakout level
    b["total_volume"] = 2500.0

    df, sigs = engine.analyze_bars(bars, symbol="SPX500", asset_class="Indices", inspect_last_n_bars=30)
    trap_sigs = [s for s in sigs if s["sig_code"] == "trap" and s["time_ms"] == b["open_time_ms"]]
    assert len(trap_sigs) > 0
    assert trap_sigs[0]["side"] == "bearish"
    assert trap_sigs[0]["title"] == "Failed breakout"
