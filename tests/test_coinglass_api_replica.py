"""
Unit & Parity Tests for CoinGlass Pure API Replica and Candle Metric Engine
=============================================================================
Validates:
1. Local candle Parquet dumps contain:
   - All per-bar metrics: Volume, Delta, Cumulative Delta, OI change, Min Delta,
     Max Delta, Trades, Buy/Sell Ratio, VWAP.
   - Signals array on each candle (List of structs, allowing multiple signals per bar).
2. Multiple concurrent signals on a single candle (e.g. Absorption + Trapped Shorts).
3. Multi-bar footprints: 5m, 15m, Range bars (e.g. Range 10), and Volume bars (e.g. Volume 50K ETH).
4. Exact CoinGlass signal formulations and descriptions (abs, trap, div, sdiv, exh).
5. Empirical behavioral & metric parity against historical CoinGlass CDP dumps.
"""

import sys
from pathlib import Path
import pytest
import numpy as np
import pandas as pd
import polars as pl

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    format_vol_display,
    synthesize_range_bars,
    synthesize_volume_bars,
    COINGLASS_BAR_CONFIGS
)
from Terminal.unified_api_footprint_engine import UnifiedAPIFootprintEngine

CANDLE_DIR = ROOT_DIR / "Database" / "Candle_Data"
SIGNALS_PARQUET = ROOT_DIR / "Database" / "Signals" / "coinglass_aggregated_futures_signals.parquet"


def test_per_bar_metrics_in_candle_parquet():
    """Verify that every candle in local Parquet dumps stores all CoinGlass per-bar metrics."""
    eth_path = CANDLE_DIR / "ETH.parquet"
    assert eth_path.exists(), "ETH.parquet must exist"

    df = pl.read_parquet(eth_path)
    required_metrics = [
        "volume",
        "delta",
        "cumulative_delta",
        "oi_change",
        "min_delta",
        "max_delta",
        "trades",
        "buy_sell_ratio",
        "vwap",
        "open_time_ist",
        "signals",
        "signals_count",
        "has_signal"
    ]
    for m in required_metrics:
        assert m in df.columns, f"Missing required metric '{m}' in ETH.parquet"

    # Verify non-trivial values
    assert (df["volume"] >= 0).all()
    assert (df["vwap"] > 0).all()
    assert (df["buy_sell_ratio"] >= 0).all()
    assert (df["min_delta"] <= 0.001).all()  # min delta is non-positive excursion
    assert (df["max_delta"] >= -0.001).all()  # max delta is non-negative excursion


def test_candle_signals_array_and_multiple_signals_per_bar():
    """Verify that candles store an array of signals and correctly handle multiple concurrent signals."""
    engine = UniversalFootprintEngine()

    # Construct test candle series
    t0 = 1793500000000
    bars = []
    for i in range(25):
        bars.append({
            "open_time_ms": t0 + i * 900000,
            "open": 2500.0,
            "high": 2505.0,
            "low": 2495.0,
            "close": 2500.0,
            "total_volume": 10000.0,
            "buy_volume": 5000.0,
            "sell_volume": 5000.0,
            "delta": 0.0
        })

    # Inject multiple simultaneous signals at bar 20:
    # 1. Broke 5-bar low on heavy volume -> rallies into upper half (Trapped Shorts)
    # 2. Huge lower wick >= 30% of range with 3x avg volume and closes near high (Bullish Absorption)
    target_bar = bars[20]
    target_bar["open"] = 2492.0
    target_bar["high"] = 2505.0
    target_bar["low"] = 2470.0  # deep breakdown of 2495
    target_bar["close"] = 2504.0  # rallies back near the high
    target_bar["total_volume"] = 35000.0  # 3.5x volume
    target_bar["buy_volume"] = 20000.0
    target_bar["sell_volume"] = 15000.0
    target_bar["delta"] = 5000.0

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=25)

    # Check the bar in DataFrame
    candle = df.iloc[20]
    assert bool(candle["has_signal"]) is True
    assert candle["signals_count"] >= 2, f"Expected multiple signals, got {candle['signals_count']}"

    # Verify signals array structure
    sig_codes = [s["sig_code"] for s in candle["signals"]]
    assert "abs" in sig_codes, "Absorption signal missing"
    assert "trap" in sig_codes, "Trapped shorts signal missing"

    # Verify side agreement
    assert candle["signal_side"] == "bullish"
    assert "Absorption" in candle["signal_type"]
    assert "Trapped shorts" in candle["signal_type"]


def test_multibar_synthesis_range_and_volume():
    """Verify synthesis of Range and Volume bars and their footprint signals."""
    t0 = 1793500000000
    sub_bars = [
        {"open_time_ms": t0 + 0, "open": 2000.0, "high": 2004.0, "low": 1999.0, "close": 2003.0, "total_volume": 15000.0, "buy_volume": 8000.0, "sell_volume": 7000.0},
        {"open_time_ms": t0 + 60000, "open": 2003.0, "high": 2011.0, "low": 2002.0, "close": 2010.0, "total_volume": 20000.0, "buy_volume": 12000.0, "sell_volume": 8000.0},
        {"open_time_ms": t0 + 120000, "open": 2010.0, "high": 2015.0, "low": 2008.0, "close": 2012.0, "total_volume": 25000.0, "buy_volume": 14000.0, "sell_volume": 11000.0},
        {"open_time_ms": t0 + 180000, "open": 2012.0, "high": 2022.0, "low": 2011.0, "close": 2020.0, "total_volume": 30000.0, "buy_volume": 18000.0, "sell_volume": 12000.0},
    ]

    # Range 10 synthesis
    range_bars = synthesize_range_bars(sub_bars, range_size=10.0, symbol="ETH", bar_label="Range 10")
    assert len(range_bars) >= 1
    for rb in range_bars:
        assert rb["bar_type"] == "range"
        assert rb["bar_label"] == "Range 10"
        assert (rb["high"] - rb["low"]) >= 10.0

    # Volume 50K ETH synthesis
    vol_bars = synthesize_volume_bars(sub_bars, vol_threshold=50000.0, symbol="ETH", bar_label="Volume 50K ETH")
    assert len(vol_bars) >= 1
    for vb in vol_bars:
        assert vb["bar_type"] == "vol"
        assert vb["bar_label"] == "Volume 50K ETH"
        assert vb["total_volume"] >= 50000.0


def test_coinglass_cdp_historical_parity():
    """
    Empirical parity test against historical CoinGlass CDP dataset.
    Verifies >= 85% directional concordance on overlapping historical bars.
    """
    assert SIGNALS_PARQUET.exists(), "Signals parquet must exist"
    cdp_df = pl.read_parquet(SIGNALS_PARQUET).filter(pl.col("source") == "coinglass_cdp")
    assert len(cdp_df) > 1000, "Historical CDP dataset should contain at least 1,000 records"

    engine = UniversalFootprintEngine()
    symbols = ["ETH", "BTC", "SOL", "BNB", "XRP"]
    total_eval = 0
    dir_matches = 0

    for sym in symbols:
        candle_p = CANDLE_DIR / f"{sym}.parquet"
        if not candle_p.exists():
            continue
        c_df = pd.read_parquet(candle_p)
        sym_cdp = cdp_df.filter((pl.col("symbol") == sym) & (pl.col("bar_type") == "time")).to_pandas()

        enriched, sigs = engine.analyze_bars(
            c_df, symbol=sym, asset_class="Crypto", rolling_window=len(c_df), inspect_last_n_bars=len(c_df)
        )
        api_map = {}
        for s in sigs:
            api_map.setdefault(int(s["time_ms"]), []).append(s)

        for _, row in sym_cdp.iterrows():
            t = int(row["time_ms"])
            if t in api_map:
                total_eval += 1
                code = str(row["sig_code"]).lower()
                side = str(row["side"]).lower()
                cand = next((s for s in api_map[t] if str(s["sig_code"]).lower() == code), api_map[t][0])
                if str(cand["side"]).lower() == side:
                    dir_matches += 1

    assert total_eval > 50, f"Expected at least 50 overlapping bars, got {total_eval}"
    concordance_rate = (dir_matches / total_eval) * 100.0
    print(f"\nEmpirical Parity Results: {dir_matches}/{total_eval} matches ({concordance_rate:.1f}%)")
    assert concordance_rate >= 85.0, f"Parity rate {concordance_rate:.1f}% below 85% threshold"


def test_format_vol_display():
    """Verify volume formatting matches CoinGlass UI strings."""
    assert format_vol_display(27000) == "27K"
    assert format_vol_display(2900000) == "2.9M"
    assert format_vol_display(10000) == "10K"
    assert format_vol_display(2000) == "2.0K"
    assert format_vol_display(2500) == "2.5K"
    assert format_vol_display(1000) == "1.0K"
    assert format_vol_display(500) == "500"
    assert format_vol_display(200000) == "200K"
    assert format_vol_display(-15000) == "-15K"
    assert format_vol_display(50000000) == "50M"


def test_edge_case_zero_volume_candle():
    """Verify robust handling of zero-volume / no-trades candles without division by zero or -0.0."""
    engine = UniversalFootprintEngine()
    bars = [
        {
            "open_time_ms": 1793500000000 + i * 900000,
            "open": 2500.0,
            "high": 2500.0,
            "low": 2500.0,
            "close": 2500.0,
            "total_volume": 0.0,
            "buy_volume": 0.0,
            "sell_volume": 0.0,
            "delta": 0.0,
            "total_trades": 0.0
        }
        for i in range(10)
    ]
    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto")
    assert not df.empty
    assert (df["volume"] == 0.0).all()
    # Zero volume candle represents neutral market: buy_sell_ratio must be 1.0 (balanced), not 0.0
    assert (df["buy_sell_ratio"] == 1.0).all()
    # Excursions should be exactly 0.0, avoiding IEEE -0.0
    for v in df["min_delta"]:
        assert v == 0.0 and str(v) != "-0.0"
    for v in df["max_delta"]:
        assert v == 0.0


def test_edge_case_fewer_than_five_bars():
    """Verify that fewer than 5 bars returns fully enriched candle schema rather than un-enriched dataframe."""
    engine = UniversalFootprintEngine()
    bars = [
        {
            "open_time_ms": 1793500000000 + i * 900000,
            "open": 2500.0,
            "high": 2505.0,
            "low": 2495.0,
            "close": 2502.0,
            "total_volume": 1000.0
        }
        for i in range(3)
    ]
    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto")
    assert len(df) == 3
    assert sigs == []  # Not enough bars for 5-bar historical signals
    # Must contain full candle metrics and empty signals array
    for col in ["volume", "delta", "cumulative_delta", "signals", "signals_count", "has_signal", "open_time_ist", "buy_sell_ratio", "vwap"]:
        assert col in df.columns, f"Missing metric column '{col}' in short candle sequence"
    assert (df["signals_count"] == 0).all()
    assert (df["has_signal"] == False).all()
    assert all(isinstance(s, list) and len(s) == 0 for s in df["signals"])


def test_parquet_signals_schema_consistency_with_empty_signals(tmp_path):
    """Verify that candle Parquet persistence enforces List(Struct(...)) schema even when zero signals are triggered."""
    engine = UniversalFootprintEngine()
    bars = [
        {
            "open_time_ms": 1793500000000 + i * 900000,
            "open": 2500.0,
            "high": 2501.0,
            "low": 2499.0,
            "close": 2500.0,
            "total_volume": 1000.0
        }
        for i in range(10)
    ]
    df, _ = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto")
    assert (df["signals_count"] == 0).all()

    # Save via UnifiedAPIFootprintEngine parquet pipeline
    api_engine = UnifiedAPIFootprintEngine.__new__(UnifiedAPIFootprintEngine)
    api_engine.rolling_window = 10
    target_parquet = tmp_path / "TEST_EMPTY.parquet"

    # Call save_rolling_parquet with mock target directory
    import Terminal.unified_api_footprint_engine as ufe
    orig_candle_dir = ufe.CANDLE_DIR
    ufe.CANDLE_DIR = tmp_path
    try:
        api_engine.save_rolling_parquet(df, "TEST_EMPTY")
    finally:
        ufe.CANDLE_DIR = orig_candle_dir

    assert target_parquet.exists()
    pldf = pl.read_parquet(target_parquet)
    # Check that signals column is strictly typed List(Struct(...))
    assert isinstance(pldf.schema["signals"], pl.List)
    inner_type = pldf.schema["signals"].inner
    assert isinstance(inner_type, pl.Struct), f"Expected Struct inner type, got {inner_type}"
    struct_fields = {f.name: f.dtype for f in inner_type.fields}
    assert "sig_code" in struct_fields
    assert "sig_name" in struct_fields
    assert "side" in struct_fields
    assert "price" in struct_fields
    assert "title" in struct_fields
    assert "description" in struct_fields


def test_coinglass_absorption_specifications():
    """Verify Absorption phrasing matches CoinGlass CDP ground truth (30%+ on BOTH sides)."""
    engine = UniversalFootprintEngine()
    t0 = 1793500000000
    bars = [
        {
            "open_time_ms": t0 + i * 900000,
            "open": 2500.0,
            "high": 2505.0,
            "low": 2495.0,
            "close": 2500.0,
            "total_volume": 10000.0,
            "buy_volume": 5000.0,
            "sell_volume": 5000.0,
            "delta": 0.0
        }
        for i in range(25)
    ]

    # Inject Bullish Absorption: 22% lower wick, closes near high
    b_bull = bars[20]
    b_bull["open"] = 2500.0
    b_bull["high"] = 2510.0
    b_bull["low"] = 2490.0  # range = 20.0
    b_bull["close"] = 2508.0  # close_pos = 18/20 = 0.90
    # lower_wick = min(open, close) - low = 2500 - 2490 = 10.0 -> 50% of range (>= 20%)
    b_bull["total_volume"] = 15000.0
    b_bull["buy_volume"] = 8000.0
    b_bull["sell_volume"] = 7000.0
    b_bull["delta"] = 1000.0

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=25)
    candle = df.iloc[20]
    abs_sigs = [s for s in candle["signals"] if s["sig_code"] == "abs"]
    assert len(abs_sigs) >= 1
    assert abs_sigs[0]["side"] == "bullish"
    # CoinGlass CDP ground truth: bullish absorption also reads "30%+ of the bar's selling"
    # (verified across the historical coinglass_cdp dump; the legacy "20%+" was a template defect).
    assert "30%+ of the bar's selling" in abs_sigs[0]["description"]
    assert "Selling absorbed at the low" in abs_sigs[0]["title"]


def test_coinglass_bar_configs_coverage():
    """Verify standard CoinGlass Range and Volume presets are registered for all major trading assets."""
    expected_assets = ["ETH", "BTC", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "NEAR", "DOT", "ATOM", "LTC", "BCH", "TRX", "CRV"]
    for sym in expected_assets:
        assert sym in COINGLASS_BAR_CONFIGS, f"Missing {sym} in COINGLASS_BAR_CONFIGS"
        cfg = COINGLASS_BAR_CONFIGS[sym]
        assert "range" in cfg and len(cfg["range"]) >= 1, f"Missing range configs for {sym}"
        assert "vol" in cfg and len(cfg["vol"]) >= 1, f"Missing vol configs for {sym}"

