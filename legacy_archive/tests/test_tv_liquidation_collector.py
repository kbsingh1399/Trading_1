import os
import json
import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from Terminal.coinglass_tv_liquidation_collector import (
    load_symbol_map,
    process_and_persist_liquidations,
    LIVE_LIQ_DIR
)
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator


def test_symbol_map_loading():
    s_map = load_symbol_map()
    assert isinstance(s_map, dict)
    assert "BTC" in s_map
    assert "ETH" in s_map
    assert "SOL" in s_map
    assert s_map["BTC"] == "Binance_BTCUSDT"
    assert s_map["HYPE"] == "Bybit_HYPEUSDT"
    assert len(s_map) >= 20


def test_liquidation_spike_and_reversal_detection(tmp_path):
    # Mock 60 bars of quiet data
    raw_rows = []
    base_time = 1793500000
    for i in range(58):
        raw_rows.append([base_time + i * 900, 1000.0, 500.0])
        
    # Bar 58: Quiet bar
    raw_rows.append([base_time + 58 * 900, 2000.0, 1000.0])
    
    # Bar 59: Massive Long Liquidation Cascade ($2.5M Longs vs $10k Shorts)
    raw_rows.append([base_time + 59 * 900, 2500000.0, 10000.0])
    
    summary = process_and_persist_liquidations("TEST_BTC", "Binance_TEST_BTC", raw_rows)
    assert summary is not None
    assert summary["symbol"] == "TEST_BTC"
    assert summary["recent_spike"] is True
    assert summary["reversal_bias"] == "BULLISH_REVERSAL"
    assert summary["latest_total_liq_usd"] == 2510000.0
    assert summary["exhaustion_score"] >= 80.0
    
    # Verify generated parquet
    parquet_path = LIVE_LIQ_DIR / "TEST_BTC_tv_liquidations.parquet"
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) == 60
    assert bool(df.iloc[-1]["is_spike"]) is True
    assert df.iloc[-1]["reversal_bias"] == "BULLISH_REVERSAL"
    
    # Clean up test artifact
    parquet_path.unlink(missing_ok=True)


def test_short_squeeze_bearish_reversal():
    raw_rows = []
    base_time = 1793500000
    for i in range(58):
        raw_rows.append([base_time + i * 900, 1000.0, 1000.0])
        
    # Massive Short Squeeze ($1.8M Shorts vs $5k Longs)
    raw_rows.append([base_time + 58 * 900, 5000.0, 1800000.0])
    raw_rows.append([base_time + 59 * 900, 2000.0, 1000.0])
    
    summary = process_and_persist_liquidations("TEST_ETH", "Binance_TEST_ETH", raw_rows)
    assert summary is not None
    assert summary["recent_spike"] is True
    assert summary["reversal_bias"] == "BEARISH_REVERSAL"
    
    parquet_path = LIVE_LIQ_DIR / "TEST_ETH_tv_liquidations.parquet"
    parquet_path.unlink(missing_ok=True)


def test_realtime_evaluator_tv_liquidation_integration():
    evaluator = RealtimeSignalEvaluator()
    # Check method existence
    assert hasattr(evaluator, "query_live_tv_liquidations")
    res = evaluator.query_live_tv_liquidations("BTC")
    assert isinstance(res, dict)
    if "reversal_bias" in res:
        assert res["reversal_bias"] in ["BULLISH_REVERSAL", "BEARISH_REVERSAL", "NEUTRAL"]
