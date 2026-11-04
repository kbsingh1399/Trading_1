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
