"""
Unit tests for Microstructure VPIN (Volume-Synchronized Probability of Toxicity) Engine
"""

import pytest
import numpy as np
import pandas as pd
from Terminal.microstructure_vpin_engine import compute_vpin_and_toxicity


def test_vpin_insufficient_or_missing_data():
    res = compute_vpin_and_toxicity("NON_EXISTENT_COIN_XYZ")
    assert res["data_valid"] is False
    assert res["regime"] == "INSUFFICIENT_DATA"
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
    # If deltas are 0, v_buy = 50, v_sell = 50, |v_buy - v_sell| = 0, VPIN = 0.0
    assert res["vpin"] == 0.0
    assert res["regime"] == "LOW_TOXICITY"
    assert res["veto_toxic_flow"] is False


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
    assert res["kyles_lambda"] > 0.0


def test_vpin_on_real_parquet_btc():
    res = compute_vpin_and_toxicity("BTC")
    if res["data_valid"]:
        assert 0.0 <= res["vpin"] <= 1.0
        assert res["regime"] in ("EXTREME_TOXICITY", "HIGH_TOXICITY", "MODERATE_TOXICITY", "LOW_TOXICITY")
        assert res["kyles_lambda"] >= 0.0
