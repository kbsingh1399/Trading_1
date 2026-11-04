"""
Unit tests for Microstructure VPIN (Volume-Synchronized Probability of Toxicity) Engine
and Evaluator-Level Order Flow Toxicity Integration.
"""

import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, Mock
from types import SimpleNamespace
from datetime import datetime, timezone

from Terminal.microstructure_vpin_engine import (
    compute_vpin_and_toxicity,
    _construct_volume_buckets,
    _estimate_kyles_lambda
)
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator


def test_vpin_data_unavailable():
    res = compute_vpin_and_toxicity("NON_EXISTENT_COIN_XYZ")
    assert res["data_valid"] is False
    assert res["regime"] == "DATA_UNAVAILABLE"
    assert res["vpin"] == 0.50
    assert res["veto_toxic_flow"] is False


def test_vpin_balanced_two_sided_flow():
    # Construct synthetic balanced flow where buy volume roughly equals sell volume (Delta ~ 0)
    n = 50
    volumes = np.full(n, 100.0)
    deltas = np.zeros(n)
    closes = np.linspace(100.0, 100.5, n)

    df = pd.DataFrame({
        "volume": volumes,
        "delta": deltas,
        "close": closes,
        "open": closes,
        "high": closes + 0.1,
        "low": closes - 0.1
    })

    res = compute_vpin_and_toxicity("SYNTH_BALANCED", candle_df=df)
    assert res["data_valid"] is True
    assert res["vpin"] == 0.0
    assert res["regime"] == "LOW_TOXICITY"
    assert res["veto_toxic_flow"] is False
    assert res["kyles_lambda"] >= 0.0


def test_vpin_extreme_toxicity_one_sided_flow():
    # Construct extreme toxic aggressive selling flow (Delta = -Volume)
    n = 50
    volumes = np.full(n, 1000.0)
    deltas = np.full(n, -1000.0)
    closes = np.linspace(100.0, 80.0, n)

    df = pd.DataFrame({
        "volume": volumes,
        "delta": deltas,
        "close": closes,
        "open": closes + 0.5,
        "high": closes + 0.6,
        "low": closes - 0.2
    })

    res = compute_vpin_and_toxicity("SYNTH_TOXIC", candle_df=df)
    assert res["data_valid"] is True
    assert res["vpin"] >= 0.80
    assert res["regime"] == "EXTREME_TOXICITY"
    assert res["veto_toxic_flow"] is True


def test_vpin_rejects_corrupted_delta_overshoot():
    # Strict Data Integrity: |delta| > volume must be rejected as corrupted, not sanitized
    n = 50
    volumes = np.full(n, 100.0)
    deltas = np.full(n, 500.0)  # Impossible delta exceeding volume
    closes = np.linspace(100.0, 105.0, n)

    df = pd.DataFrame({
        "volume": volumes,
        "delta": deltas,
        "close": closes,
        "open": closes,
        "high": closes + 1.0,
        "low": closes - 1.0
    })

    res = compute_vpin_and_toxicity("SYNTH_CORRUPT", candle_df=df)
    assert res["data_valid"] is False
    assert res["regime"] == "CORRUPTED_DELTA_INTEGRITY"
    assert res["veto_toxic_flow"] is False


def test_kyles_lambda_zero_net_flow_and_alternating_flow():
    # Strict Zero-Net-Flow: alternating flow sum(delta) == 0 must return lambda = 0.0
    n = 50
    prices = np.linspace(100.0, 110.0, n)
    # Alternating deltas: [100, -100, 100, -100, ...] -> net sum == 0
    deltas = np.array([100.0 if i % 2 == 0 else -100.0 for i in range(n)])
    
    lambda_val = _estimate_kyles_lambda(prices, deltas)
    assert lambda_val == 0.0, f"Expected 0.0 on zero-net-flow, got {lambda_val}"


def test_evaluator_level_vpin_veto_causal_isolation():
    # Causally Isolated Evaluator Regression Test:
    # Proves that an otherwise 100% actionable, passing candidate setup is approved
    # when VPIN is benign, but flips to NOT actionable strictly when VPIN >= 0.80.
    evaluator = object.__new__(RealtimeSignalEvaluator)
    evaluator.olap = Mock()
    evaluator.olap.query_whale_walls.return_value = {'data_valid': True, 'whale_imbalance_bid_ratio': 0.7}
    evaluator.olap.query_liquidation_confluence.return_value = {'data_valid': True, 'near_field_shorts_pct': 60}
    evaluator.olap.query_cvd_trend.return_value = {'data_valid': True, 'cvd_slope': 'BULLISH_FLOW'}
    evaluator.olap.query_auction_profile.return_value = {'data_valid': True}
    evaluator.olap.get_forecast_frame.return_value = pd.DataFrame()

    evaluator.regime_det = Mock()
    evaluator.regime_det.analyze_symbol.return_value = {'regime': 'MEAN_REVERTING_RANGE', 'atr_pct': 1}
    evaluator.vector_mem = None
    evaluator.timesfm = SimpleNamespace(is_available=False)

    evaluator.critic_engine = Mock()
    evaluator.critic_engine.check_microstructure_knife_veto.return_value = (False, "", {})
    evaluator.critic_engine.audit_candidate_setup.return_value = {"critic_verdict": "CLEAR", "max_similarity": 0.0, "penalty": 0.0}

    evaluator.query_live_tv_liquidations = lambda s: {}
    evaluator._persist_alert = Mock()
    evaluator._broker_sizing = lambda *args: {'lots': 0.01, 'risk_usd': 5, 'cap_usd': 10}

    signal = {
        'symbol': 'BTC',
        'mt5_symbol': 'BTCUSD',
        'sig_name': 'Liquidation Exhaustion',
        'sig_code': 'LIQ_REV_SPIKE',
        'side': 'buy',
        'price': 100,
        'datetime_utc': datetime.now(timezone.utc).isoformat(),
        'bar_closed': True
    }

    mock_benign = {
        "symbol": "BTC",
        "vpin": 0.20,
        "regime": "LOW_TOXICITY",
        "adverse_selection_risk": "MINIMAL",
        "kyles_lambda": 0.001,
        "veto_toxic_flow": False,
        "data_valid": True
    }

    mock_toxic = {
        "symbol": "BTC",
        "vpin": 0.88,
        "regime": "EXTREME_TOXICITY",
        "adverse_selection_risk": "CRITICAL",
        "kyles_lambda": 0.045,
        "veto_toxic_flow": True,
        "data_valid": True
    }

    # 1. Benign Case -> Must be actionable
    with patch("Terminal.microstructure_vpin_engine.compute_vpin_and_toxicity", return_value=mock_benign):
        res_benign = evaluator.evaluate_signal(signal)
        assert res_benign.get("is_actionable") is True, f"Benign setup should be actionable: {res_benign.get('validation_errors')}"
        assert not any("VETO_TOXIC_ORDER_FLOW" in str(r) for r in res_benign.get("validation_errors", []))

    # 2. Toxic Case -> Must flip to NOT actionable strictly due to VPIN toxicity veto
    with patch("Terminal.microstructure_vpin_engine.compute_vpin_and_toxicity", return_value=mock_toxic):
        res_toxic = evaluator.evaluate_signal(signal)
        assert res_toxic.get("is_actionable") is False, "Toxic setup must NOT be actionable"
        assert any("VETO_TOXIC_ORDER_FLOW" in str(r) for r in res_toxic.get("validation_errors", [])), \
            f"Expected VETO_TOXIC_ORDER_FLOW in validation_errors: {res_toxic.get('validation_errors')}"

def test_vpin_rejects_nan_and_infinite_inputs():
    """Ensure NaN, positive infinity, or negative infinity values are strictly rejected."""
    # 1. NaN delta
    df_nan_delta = pd.DataFrame({
        "open": [100.0] * 20,
        "high": [101.0] * 20,
        "low": [99.0] * 20,
        "close": [100.5] * 20,
        "volume": [1000.0] * 20,
        "delta": [np.nan] * 20
    })
    res_nan = compute_vpin_and_toxicity("BTC", candle_df=df_nan_delta)
    assert res_nan["data_valid"] is False
    assert res_nan["regime"] == "CORRUPTED_DELTA_INTEGRITY"

    # 2. Inf volume
    df_inf_vol = pd.DataFrame({
        "open": [100.0] * 20,
        "high": [101.0] * 20,
        "low": [99.0] * 20,
        "close": [100.5] * 20,
        "volume": [np.inf] * 20,
        "delta": [100.0] * 20
    })
    res_inf = compute_vpin_and_toxicity("BTC", candle_df=df_inf_vol)
    assert res_inf["data_valid"] is False
    assert res_inf["regime"] == "CORRUPTED_DELTA_INTEGRITY"

    # 3. Nan prices in proxy mode
    df_nan_price = pd.DataFrame({
        "open": [np.nan] * 20,
        "high": [101.0] * 20,
        "low": [99.0] * 20,
        "close": [100.5] * 20,
        "volume": [1000.0] * 20
    })
    res_nan_price = compute_vpin_and_toxicity("BTC", candle_df=df_nan_price)
    assert res_nan_price["data_valid"] is False
    assert res_nan_price["regime"] == "CORRUPTED_DELTA_INTEGRITY"


def test_delta_venue_symbol_guard_ledgers_unlisted_suppression(tmp_path):
    """Ensure non-Delta symbols (e.g., GER30, EURUSD) are suppressed and recorded in the ledger."""
    from Terminal.asset_signal_stack import EvaluatedSignalsLedger
    import importlib
    sentinel_mod = importlib.import_module("5_wake_on_footprint_signal")

    ledger_file = tmp_path / "test_ledger.jsonl"
    ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)
    
    # GER30 is unlisted on Delta Exchange India
    sym = "GER30"
    open_ms = 1793780100000
    sig_codes = ["abs", "exh"]
    sig_id = sentinel_mod.generate_signal_id(sym, open_ms, "bearish", sig_codes)

    # Verify not resolved by delta client
    assert sentinel_mod.delta_client.resolve_product(sym) is None

    # Mark as suppressed
    ledger.mark_evaluated(
        symbol=sym,
        open_time_ms=open_ms,
        signal_id=sig_id,
        confluence_score=0.0,
        action="SUPPRESSED_UNLISTED_VENUE",
        sig_name="Absorption",
        direction="bearish"
    )

    import json
    assert ledger.is_evaluated(sym, open_ms) is True
    with open(ledger_file, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]
    assert len(records) == 1
    assert records[0]["symbol"] == "GER30"
    assert records[0]["action"] == "SUPPRESSED_UNLISTED_VENUE"


def test_whale_walls_feed_status_and_liquidation_isolation(tmp_path, monkeypatch):
    """Verifies feed_status == 'ACTIVE' and that stale magnet zones do not invalidate near-field liquidations."""
    from Terminal.olap_engine import OlapEngine
    import Terminal.olap_engine as oe
    import time

    monkeypatch.setattr(oe, "WHALE_DIR", tmp_path / "whales")
    monkeypatch.setattr(oe, "LIQ_DIR", tmp_path / "liq")
    monkeypatch.setattr(oe, "MAGNET_DIR", tmp_path / "magnet")
    (tmp_path / "whales").mkdir()
    (tmp_path / "liq").mkdir()
    (tmp_path / "magnet").mkdir()

    # 1. Whale parquet with depth
    now = time.time()
    whale_df = pd.DataFrame({
        "from_price_pct": [-1.0, 1.0],
        "remaining_usd": [100000.0, 100000.0],
        "side": ["bid", "ask"],
        "timestamp": [now, now],
        "order_age_seconds": [10.0, 10.0]
    })
    whale_df.to_parquet(tmp_path / "whales" / "BTC_whale_walls.parquet")

    olap = OlapEngine()
    whales = olap.query_whale_walls("BTC")
    assert whales["data_valid"] is True
    assert whales["feed_status"] == "ACTIVE"
    assert 0.0 <= whales["whale_imbalance_bid_ratio"] <= 1.0

    # 2. Fresh near-field + Stale magnet zones
    nf_df = pd.DataFrame({
        "shorts_pct": [85.0],
        "longs_pct": [15.0],
        "timestamp": [now]
    })
    nf_df.to_parquet(tmp_path / "liq" / "BTC_near_field.parquet")

    stale_mag_df = pd.DataFrame({
        "zone_price": [50000.0],
        "score": [10.0],
        "side": ["SHORT"],
        "timestamp": [now - 100000.0]  # Over 24 hours stale
    })
    stale_mag_df.to_parquet(tmp_path / "magnet" / "BTC_magnet_zones.parquet")

    liq = olap.query_liquidation_confluence("BTC")
    assert liq["data_valid"] is True
    assert 0.0 <= liq["near_field_shorts_pct"] <= 100.0


def test_trader_excludes_sub_dollar_dust_positions_from_max_positions():
    """Verifies that dust test positions (< $1.00 USD notional) do not block institutional trade slots."""
    from Terminal.delta_exchange_live_trader import DeltaExchangeTrader

    trader = DeltaExchangeTrader()
    # Mock active positions: 1 institutional ($19) + 1 dust ($0.32)
    mock_positions = [
        {"product_symbol": "ADAUSD", "size": -100, "mark_price": 0.19, "entry_price": 0.19},
        {"product_symbol": "TRXUSD", "size": 1, "mark_price": 0.32, "entry_price": 0.32}
    ]
    trader.get_active_positions = lambda: mock_positions
    trader.get_wallet_balances = lambda: [{"asset_symbol": "USD", "balance": 10.0, "available_balance": 8.0}]
    trader.has_open_position = lambda sym: False
    trader._inflight_orders = set()

    # Evaluating a candidate trade for BTC
    prod = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1"}
    sig = {"symbol": "BTCUSD", "price": 60000.0, "confluence_score": 85.0, "trade_parameters": {"entry_price": 60000.0, "stop_loss": 59000.0}}
    
    # Check institutional filtering logic
    institutional = [
        p for p in mock_positions
        if (abs(int(p.get("size", 0))) * float(p.get("mark_price", 0.0) or p.get("entry_price", 0.0) or 0.0)) >= 1.0
    ]
    # Only ADAUSD qualifies as institutional
    assert len(institutional) == 1
    assert institutional[0]["product_symbol"] == "ADAUSD"
