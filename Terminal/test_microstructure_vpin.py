"""
Unit tests for Microstructure VPIN (Volume-Synchronized Probability of Toxicity) Engine
and Evaluator-Level Order Flow Toxicity Integration.
"""

import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch

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


def test_vpin_invariant_delta_clipping():
    # Invariant: |delta| <= volume even if input data has anomalous overshoot
    volumes = np.array([100.0, 200.0])
    deltas = np.array([500.0, -1000.0])  # oversized deltas
    b_buys, b_sells, b_size = _construct_volume_buckets(volumes, deltas, num_buckets=2)
    
    total_b_vol = np.sum(b_buys + b_sells)
    # Total volume in buckets must equal input volume sum (300.0)
    assert np.isclose(total_b_vol, 300.0)


def test_kyles_lambda_zero_flow_fallback():
    # Zero net flow or zero variance must not divide by zero or blow up to 1,000,000.0
    prices = np.full(50, 100.0)
    deltas = np.zeros(50)
    lambda_val = _estimate_kyles_lambda(prices, deltas)
    assert lambda_val == 0.0


def test_evaluator_level_vpin_veto_regression():
    # Evaluator regression test:
    # A candidate signal with high confluence (>= 75) MUST be vetoed and rendered non-actionable
    # when VPIN >= 0.80 (extreme toxic order flow).
    evaluator = RealtimeSignalEvaluator()
    mock_toxic_metrics = {
        "symbol": "BTC",
        "vpin": 0.88,
        "regime": "EXTREME_TOXICITY",
        "adverse_selection_risk": "CRITICAL",
        "kyles_lambda": 0.045,
        "veto_toxic_flow": True,
        "data_valid": True
    }

    mock_signal = {
        "symbol": "BTC",
        "side": "bullish",
        "price": 60000.0,
        "time_ms": 1700000000000,
        "bar_type": "time"
    }

    with patch("Terminal.microstructure_vpin_engine.compute_vpin_and_toxicity", return_value=mock_toxic_metrics):
        decision = evaluator.evaluate_signal(mock_signal)
        # Verify veto presence in validation_errors or confluence_reasons
        errs = decision.get("validation_errors", [])
        reasons = decision.get("confluence_reasons", [])
        has_toxicity_veto = any("VETO_TOXIC_ORDER_FLOW" in str(r) for r in errs + reasons)
        assert has_toxicity_veto is True, f"Expected VETO_TOXIC_ORDER_FLOW in validation_errors: {errs}"
        assert decision.get("is_actionable") is False, "Toxic order flow must disqualify actionability"
