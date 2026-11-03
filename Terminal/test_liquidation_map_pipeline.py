import os
import sys
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.olap_engine import OlapEngine
from Terminal.auction_indicators import extract_microstructure_levels
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
import Terminal.liquidation_map_collector as lmc

DB_DIR = ROOT_DIR / "Database"
LIQ_DIR = DB_DIR / "Liquidation_Data"


def test_liquidation_directories_exist():
    expected_dirs = [
        LIQ_DIR / "Key_Levels",
        LIQ_DIR / "Magnet_Zones",
        LIQ_DIR / "Near_Field_Balance",
        LIQ_DIR / "Strength_By_Distance",
        LIQ_DIR / "Leverage_Tiers"
    ]
    for d in expected_dirs:
        assert d.exists(), f"Directory missing: {d}"


def test_olap_liquidation_confluence_query(tmp_path):
    olap = OlapEngine()
    # Test on BTC (or any crypto)
    res = olap.query_liquidation_confluence("BTC")
    assert isinstance(res, dict)
    assert "data_valid" in res
    assert "near_field_shorts_pct" in res
    assert "top_short_magnets" in res
    assert "top_long_magnets" in res
    assert "strength_by_distance" in res
    assert "leverage_tiers" in res
    assert "key_levels" in res


def test_auction_indicators_liquidation_magnet_extraction():
    # Microstructure levels should safely parse zone_price
    micro = extract_microstructure_levels("BTC", 78000.0)
    assert isinstance(micro, dict)
    assert "nearest_whale_bid" in micro
    assert "nearest_whale_ask" in micro
    assert "nearest_long_liq_magnet" in micro
    assert "nearest_short_liq_magnet" in micro


def test_signal_evaluator_trade_formulation_with_magnets():
    evaluator = RealtimeSignalEvaluator()
    dummy_signal = {
        "symbol": "BTC",
        "mt5_symbol": "BTCUSD.pi",
        "sig_name": "Absorption",
        "sig_code": "abs",
        "side": "bullish",
        "price": 78000.0,
        "title": "Selling absorbed at the low",
        "description": "Aggressive selling absorbed by limit bids",
        "datetime_utc": datetime.now(timezone.utc).isoformat()
    }
    res = evaluator.evaluate_signal(dummy_signal)
    assert res is not None
    assert "trade_levels" in res
    assert "entry_price" in res["trade_levels"]
    assert "take_profit_1" in res["trade_levels"]
    assert "take_profit_2" in res["trade_levels"]
    assert "stop_loss" in res["trade_levels"]
    assert "setup_type" in res
    assert res["setup_type"] in ["TREND_FOLLOWING_PULLBACK", "MEAN_REVERSAL", "UNCLASSIFIED"]
    assert "liquidation_microstructure" in res
