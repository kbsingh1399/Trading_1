import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from Terminal.coinalyze_rest_collector import process_symbol_liquidations
from Terminal.matchtrader_live_trader import (
    calculate_lot_size, TARGET_ACCOUNT_NUMBER, INITIAL_BALANCE,
    EQUITY_FLOOR_TOTAL, EQUITY_FLOOR_DAILY, MAX_CONCURRENT_POSITIONS
)
from Terminal.venue_admission import competition_risk_check, matchtrader_symbol


def test_coinalyze_liquidation_processing(tmp_path, monkeypatch):
    monkeypatch.setattr("Terminal.coinalyze_rest_collector.LIVE_LIQ_DIR", tmp_path)
    
    # Generate 60 bars of dummy liquidation data
    now_ts = 1793500000
    bars = []
    closes = {}
    for i in range(60):
        t = now_ts + (i * 900)
        closes[t] = 75000.0
        # Normal quiet bars: 0.1 BTC
        l = 0.1
        s = 0.05
        bars.append({"t": t, "l": l, "s": s})
        
    # Inject large long liquidation cascade on the last bar
    bars[-1]["l"] = 25.0  # 25 BTC * 75000 = $1.875M
    bars[-1]["s"] = 0.1
    
    summary = process_symbol_liquidations("BTC", "BTCUSDT_PERP.A", bars, 75000.0, closes)
    assert summary is not None
    assert summary["symbol"] == "BTC"
    assert summary["coinalyze_symbol"] == "BTCUSDT_PERP.A"
    assert summary["recent_spike"] is True
    assert summary["reversal_bias"] == "BULLISH_REVERSAL"
    assert summary["exhaustion_score"] >= 50.0
    assert summary["latest_long_liq_usd"] > 1_000_000.0


def test_coinalyze_short_squeeze_bearish_reversal(tmp_path, monkeypatch):
    monkeypatch.setattr("Terminal.coinalyze_rest_collector.LIVE_LIQ_DIR", tmp_path)
    
    now_ts = 1793500000
    bars = []
    closes = {}
    for i in range(60):
        t = now_ts + (i * 900)
        closes[t] = 2500.0
        bars.append({"t": t, "l": 0.5, "s": 0.5})
        
    # Inject massive short squeeze liquidation on the last bar
    bars[-1]["s"] = 1500.0  # 1500 ETH * 2500 = $3.75M
    bars[-1]["l"] = 10.0
    
    summary = process_symbol_liquidations("ETH", "ETHUSDT_PERP.A", bars, 2500.0, closes)
    assert summary is not None
    assert summary["recent_spike"] is True
    assert summary["reversal_bias"] == "BEARISH_REVERSAL"


def test_matchtrader_lot_sizing():
    # BTC: $500 risk, entry 80,000, SL 79,000 (dist 1000) -> lots = 500 / 1000 = 0.50
    btc_lots = calculate_lot_size("BTC", 80000.0, 79000.0, risk_usd=500.0)
    assert btc_lots == 0.50
    
    # Gold: entry 2700, SL 2690 (dist 10) -> 1 lot is 100oz ($1000 risk per lot). For $500 risk -> 0.50 lots
    gold_lots = calculate_lot_size("XAU", 2700.0, 2690.0, risk_usd=500.0)
    assert gold_lots == 0.50
    
    # Minimum lot bounds
    tiny_lots = calculate_lot_size("BTC", 80000.0, 70000.0, risk_usd=10.0)
    assert tiny_lots == 0.01


def test_matchtrader_competition_risk_check():
    # Target account baseline check
    ok, reason = competition_risk_check(
        account_id=TARGET_ACCOUNT_NUMBER,
        balance=100000.0,
        equity=100000.0,
        initial_balance=100000.0,
        opening_balance=100000.0,
        opening_equity=100000.0,
        open_positions=0,
        pending_orders=0,
        committed_stress=0.0,
        candidate_stress=500.0,
        day_key="2026-11-02",
        platform_day_key="2026-11-02"
    )
    assert ok is True
    assert reason == "OK"
    
    # Rejects if wrong account
    bad_acc, _ = competition_risk_check(
        account_id="99999999",
        balance=100000.0,
        equity=100000.0,
        initial_balance=100000.0,
        opening_balance=100000.0,
        opening_equity=100000.0,
        open_positions=0,
        pending_orders=0,
        committed_stress=0.0,
        candidate_stress=500.0,
        day_key="2026-11-02",
        platform_day_key="2026-11-02"
    )
    assert bad_acc is False
    
    # Rejects if already 2 open positions
    full_cap, reason_cap = competition_risk_check(
        account_id=TARGET_ACCOUNT_NUMBER,
        balance=100000.0,
        equity=100000.0,
        initial_balance=100000.0,
        opening_balance=100000.0,
        opening_equity=100000.0,
        open_positions=2,
        pending_orders=0,
        committed_stress=0.0,
        candidate_stress=500.0,
        day_key="2026-11-02",
        platform_day_key="2026-11-02"
    )
    assert full_cap is False
    assert reason_cap == "CAPACITY_RESERVED"


def test_matchtrader_symbol_mapping():
    assert matchtrader_symbol("BTC") == "BTCUSD"
    assert matchtrader_symbol("ETH") == "ETHUSD"
    assert matchtrader_symbol("XAU") == "XAUUSD"
    assert matchtrader_symbol("DJI30") == "DJI30"
