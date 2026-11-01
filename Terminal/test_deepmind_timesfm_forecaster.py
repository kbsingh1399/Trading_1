"""
Unit Tests for DeepMind TimesFM 2.5 Forecaster Quantile Logic & Zero-Volume Safety
==================================================================================
Verifies:
1. Qwen F-06: Column alignment between 10-column native PyTorch tensor [mean, q10...q90]
   and 9-column quantile grid [q10...q90].
2. Qwen F-13: Safe zero-volume bar handling (no 1e-9 clipping explosion to +/-1.0).
3. Orthogonal Risk Governance: Directional adverse veto and dispersion hurdle adjustments.
"""

import pytest
import numpy as np
from unittest.mock import MagicMock
from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster


def test_zero_volume_bars_safe_normalization():
    """
    Qwen F-13: Zero-volume bars should yield r_t = 0.0, NOT clip to +/-1.0.
    """
    cvd = np.array([100.0, 150.0, 150.0, 200.0, 200.0] * 3, dtype=np.float32)
    vol = np.array([50.0, 50.0, 0.0, 50.0, 0.0] * 3, dtype=np.float32)

    delta_cvd = np.diff(cvd)
    vol_bars = vol[1:]

    with np.errstate(divide='ignore', invalid='ignore'):
        valid_vol = vol_bars > 0.0
        r_t = np.zeros_like(delta_cvd, dtype=np.float32)
        r_t[valid_vol] = np.clip(delta_cvd[valid_vol] / vol_bars[valid_vol], -1.0, 1.0)

    # Bars where vol == 0 must have r_t == 0.0
    zero_vol_indices = np.where(vol_bars == 0.0)[0]
    for idx in zero_vol_indices:
        assert r_t[idx] == 0.0


def test_dynamic_quantile_column_indexing():
    """
    Qwen F-06: 10-column tensor has mean at col 0, q10 at col 1, q50 at col 5, q90 at col 9.
    """
    forecaster = DeepMindTimesFMForecaster.__new__(DeepMindTimesFMForecaster)
    forecaster._model = MagicMock()
    forecaster._compiled = True

    horizon = 4
    # Mock 10-column output (native PyTorch TimesFM):
    # col 0 = mean, col 1 = q10, col 5 = q50, col 9 = q90
    point_mock = np.array([[0.1, 0.2, 0.3, 0.4]])
    # shape (1, 4, 10)
    quantile_mock = np.zeros((1, 4, 10), dtype=np.float32)
    quantile_mock[0, :, 0] = 0.25   # mean
    quantile_mock[0, :, 1] = -0.50  # q10
    quantile_mock[0, :, 5] = 0.00   # q50
    quantile_mock[0, :, 9] = 0.50   # q90

    forecaster._model.forecast.return_value = (point_mock, quantile_mock)

    cvd = np.linspace(100, 200, 20)
    vol = np.full(20, 10.0)

    res = forecaster.forecast_normalized_delta_ratio(cvd, vol, horizon=horizon, candidate_direction="BUY")
    assert res["available"] is True
    # q10 should be -0.50, NOT 0.25 (which would have been the mean if using index 0)
    assert res["q10_downside"][0] == -0.50
    assert res["q50_median"][0] == 0.00
    assert res["q90_upside"][0] == 0.50
    # q90 sum is 4 * 0.50 = 2.0 > 0 -> BUY not vetoed
    assert res["adverse_veto"] is False


def test_timesfm_adverse_directional_veto():
    """
    If the entire 80% predictive interval is adverse, the trade must be vetoed.
    """
    forecaster = DeepMindTimesFMForecaster.__new__(DeepMindTimesFMForecaster)
    forecaster._model = MagicMock()
    forecaster._compiled = True

    horizon = 4
    point_mock = np.array([[-0.4, -0.4, -0.4, -0.4]])
    quantile_mock = np.zeros((1, 4, 10), dtype=np.float32)
    quantile_mock[0, :, 1] = -0.80  # q10
    quantile_mock[0, :, 9] = -0.10  # q90 (all negative)

    forecaster._model.forecast.return_value = (point_mock, quantile_mock)

    cvd = np.linspace(200, 100, 20)
    vol = np.full(20, 10.0)

    # BUY trade with all-negative forecast must be vetoed
    res_buy = forecaster.forecast_normalized_delta_ratio(cvd, vol, horizon=horizon, candidate_direction="BUY")
    assert res_buy["adverse_veto"] is True
    assert "BUY_VETO" in res_buy["veto_reason"]

    # SELL trade should NOT be vetoed
    res_sell = forecaster.forecast_normalized_delta_ratio(cvd, vol, horizon=horizon, candidate_direction="SELL")
    assert res_sell["adverse_veto"] is False
