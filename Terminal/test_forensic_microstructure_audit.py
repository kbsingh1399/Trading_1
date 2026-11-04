import os
import glob
import math
import pytest
import numpy as np
import pandas as pd
from pathlib import Path

from Terminal.microstructure_vpin_engine import compute_vpin_and_toxicity
from Terminal.collector_lock import calculate_wall_decay_weight, TAU_30MIN_HALFLIFE
from Terminal.auction_indicators import extract_microstructure_levels
from Terminal.runtime_io import completed_candles
from Terminal.hyperdash_intel_engine import compute_underwater_entry_pockets
from Terminal.universal_footprint_engine import enrich_candle_df_with_ist

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"


def test_candle_data_price_and_volume_integrity():
    """Forensically checks all parquet files on disk for price and volume bounds."""
    files = list(CANDLE_DIR.glob("*.parquet"))
    assert len(files) >= 30, f"Expected at least 30 candle files, found {len(files)}"

    for f in files:
        df = pd.read_parquet(f)
        assert not df.empty, f"Empty file: {f}"
        
        # Prices validity
        assert (df["high"] >= df[["open", "close"]].max(axis=1) - 1e-8).all(), f"High violation in {f.name}"
        assert (df["low"] <= df[["open", "close"]].min(axis=1) + 1e-8).all(), f"Low violation in {f.name}"
        assert (df[["open", "high", "low", "close"]] > 0).all().all(), f"Non-positive price in {f.name}"
        assert (df["total_volume"] >= 0).all(), f"Negative volume in {f.name}"

        # Volume / Delta consistency
        if "buy_volume" in df.columns and "sell_volume" in df.columns:
            vol_diff = (df["total_volume"] - (df["buy_volume"] + df["sell_volume"])).abs()
            assert (vol_diff < 1e-3).all(), f"Total vol != buy + sell in {f.name}"

            delta_diff = (df["delta"] - (df["buy_volume"] - df["sell_volume"])).abs()
            assert (delta_diff < 1e-3).all(), f"Delta != buy - sell in {f.name}"

        # IST alignment
        if "open_time_ist_ms" in df.columns:
            assert (df["open_time_ist_ms"] == df["open_time_ms"] + 19800000).all(), f"IST ms drift in {f.name}"


def test_crypto_candle_exact_15m_continuity():
    """Verifies that all crypto candle files have 100% strictly uniform 900,000ms intervals."""
    files = list(CANDLE_DIR.glob("*.parquet"))
    crypto_files = []
    for f in files:
        df = pd.read_parquet(f)
        if "asset_class" in df.columns and str(df["asset_class"].iloc[0]).lower() == "crypto":
            crypto_files.append((f, df))

    assert len(crypto_files) >= 20, "Expected at least 20 crypto files"

    for f, df in crypto_files:
        assert df["open_time_ms"].is_monotonic_increasing, f"Not monotonic: {f.name}"
        diffs = df["open_time_ms"].diff().dropna()
        assert (diffs == 900000).all(), f"Non-uniform 15m intervals detected in crypto asset {f.name}"


def test_microstructure_vpin_recognizes_total_volume():
    """Verifies that VPIN engine correctly parses parquet files with 'total_volume'."""
    res = compute_vpin_and_toxicity("BTC")
    assert res["data_valid"] is True
    assert res["regime"] != "DATA_UNAVAILABLE"
    assert 0.0 <= res["vpin"] <= 1.0


def test_whale_decay_tau_and_auction_extraction():
    """Verifies 30-minute half-life exponential decay (tau ~ 2596.84s) and proper USD parsing."""
    w0 = calculate_wall_decay_weight(0.0)
    w1800 = calculate_wall_decay_weight(1800.0)
    w3600 = calculate_wall_decay_weight(3600.0)

    assert w0 == 1.0
    assert abs(w1800 - 0.5000) <= 0.001
    assert abs(w3600 - 0.2500) <= 0.001

    levels = extract_microstructure_levels("BTC", 76000.0)
    ask_level = levels.get("nearest_whale_ask", {})
    assert ask_level.get("amount_usd", 0.0) > 0.0, "Nearest whale ask USD should be > 0"


def test_completed_candles_tradfi_session_breaks():
    """Verifies that TradFi assets with legitimate session/weekend breaks pass completed_candles."""
    now = 1793782800000
    # Create TradFi frame with 1-day weekend gap
    tradfi_frame = pd.DataFrame({
        "open_time_ms": [now - 90000000, now - 900000],  # 89.1M ms gap (> 1h, multiple of 15m)
        "close_time_ms": [now - 89100000, now],
        "asset_class": ["forex", "forex"]
    })
    res = completed_candles(tradfi_frame, now_ms=now)
    assert len(res) == 2

    # Verify that a non-multiple random missing bar in TradFi fails
    bad_tradfi_frame = pd.DataFrame({
        "open_time_ms": [now - 1200000, now - 900000],  # 300,000ms gap (5m != 15m)
        "close_time_ms": [now - 900000, now],
        "asset_class": ["forex", "forex"]
    })
    with pytest.raises(ValueError):
        completed_candles(bad_tradfi_frame, now_ms=now)


def test_underwater_entry_pockets_clustering():
    """Verifies clustering of trapped positions into overhead supply and support floors."""
    whales = [
        {"size": 10.0, "notional_usd": 810000.0, "entry_price": 81000.0, "unrealized_pnl": -10000.0},
        {"size": 5.0, "notional_usd": 406000.0, "entry_price": 81200.0, "unrealized_pnl": -6000.0},
        {"size": 20.0, "notional_usd": 1500000.0, "entry_price": 75000.0, "unrealized_pnl": 100000.0}, # In profit: ignored
        {"size": -15.0, "notional_usd": 1177500.0, "entry_price": 78500.0, "unrealized_pnl": -22500.0},
        {"size": -5.0, "notional_usd": 393000.0, "entry_price": 78600.0, "unrealized_pnl": -7000.0},
    ]

    pockets = compute_underwater_entry_pockets(whales, mark_price=80000.0)
    assert len(pockets) == 2
    types = {p["pocket_type"] for p in pockets}
    assert "OVERHEAD_SUPPLY" in types
    assert "SUPPORT_FLOOR" in types
