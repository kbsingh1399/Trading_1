"""
========================================================================================
Google DeepMind TimesFM 2.5 (200M) Local Foundation Model Forecaster
========================================================================================
Provides zero-shot probabilistic quantile forecasting for order-flow increments:
- Stationary Normalized Delta Ratio: r_t = (CVD_t - CVD_{t-1}) / Volume_t in [-1, 1]
- Strictly k=1 bar differencing with context terminating at last completed bar
- Orthogonal Risk Governance (Arena Quant Audit Round 2):
  * NO volatility double-counting: position sizing is purely ATR-governed.
  * Quantile interval [q10, q90] is used EXCLUSIVELY for:
    1. Directional Veto: Adverse 80% predictive interval blocks counter-trend entries.
    2. Confluence Hurdle Adjustment: Wide forecast dispersion raises required confluence (+5 to +10 pts).
  * Horizon alignment: 24 bars (~6h) matching position management half-life.
========================================================================================
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional, Tuple
import numpy as np

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [TIMESFM] %(message)s")
logger = logging.getLogger("TimesFMForecaster")

MODEL_DIR = os.path.join(ROOT_DIR, "models", "timesfm-2.5-200m-pytorch")


class DeepMindTimesFMForecaster:
    """
    Singleton wrapper for Google DeepMind TimesFM 2.5 (200M PyTorch).
    Performs zero-shot multi-step point and quantile time-series forecasting.
    """
    _instance = None
    _model = None
    _compiled = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DeepMindTimesFMForecaster, cls).__new__(cls)
        return cls._instance

    def __init__(self, model_dir: Optional[str] = None):
        if self._model is not None:
            return
            
        self.model_dir = os.path.abspath(model_dir or MODEL_DIR)
        self._load_model()

    def _load_model(self):
        safetensors_path = os.path.join(self.model_dir, "model.safetensors")
        if not os.path.exists(safetensors_path):
            logger.warning(f"Model weights not found at {safetensors_path}. DeepMind TimesFM forecaster disabled.")
            return

        try:
            import timesfm
            logger.info(f"Initializing Google DeepMind TimesFM 2.5 (200M) from {self.model_dir}...")
            self._model = timesfm.TimesFM_2p5_200M_torch(torch_compile=False)
            self._model.load_checkpoint(self.model_dir)

            # Compile with max_horizon=24 matching 6h trade management horizon
            config = timesfm.ForecastConfig(
                max_context=128,
                max_horizon=24,
                normalize_inputs=True,
                per_core_batch_size=1
            )
            self._model.compile(config)
            self._compiled = True
            logger.info("DeepMind TimesFM 2.5 successfully compiled and ready for inference!")
        except Exception as e:
            logger.error(f"Failed to load DeepMind TimesFM model: {e}")
            self._model = None
            self._compiled = False

    @property
    def is_available(self) -> bool:
        return self._model is not None and self._compiled

    def forecast_normalized_delta_ratio(
        self,
        cvd_series: np.ndarray,
        volume_series: np.ndarray,
        horizon: int = 24,
        candidate_direction: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Stationary Normalized Order-Flow Ratio Forecaster (Arena Round 2 Specification).
        
        Args:
            cvd_series: Cumulative Volume Delta across historical completed bars.
            volume_series: Total bar volume corresponding to each bar.
            horizon: Forecast horizon (default: 24 bars = 6 hours on 15m).
            candidate_direction: 'BUY' or 'SELL' to evaluate directional veto.

        Returns:
            Dict containing:
            - point_forecast, q10_downside, q50_median, q90_upside
            - adverse_veto: True if predictive interval lies entirely adverse to trade
            - hurdle_adjustment: Additive confluence score penalty (+0 to +10 pts)
            - stationary_check: Stationary audit results (mean, std, adf_stationary proxy)
        """
        if not self.is_available:
            return {
                "available": False,
                "error": "Model not loaded",
                "adverse_veto": False,
                "hurdle_adjustment": 0.0
            }

        cvd = np.asarray(cvd_series, dtype=np.float64).flatten()
        vol = np.asarray(volume_series, dtype=np.float64).flatten()

        if len(cvd) < 12 or len(vol) < 12:
            return {
                "available": False,
                "error": f"Insufficient completed bars ({len(cvd)}, need >= 12)",
                "adverse_veto": False,
                "hurdle_adjustment": 0.0
            }

        if len(cvd) != len(vol) or not np.isfinite(cvd).all() or not np.isfinite(vol).all() or (vol < 0).any():
            return {"available": False, "error": "Invalid or unaligned model inputs", "adverse_veto": False}
        if not isinstance(horizon, int) or not 1 <= horizon <= 24:
            return {"available": False, "error": "Invalid horizon", "adverse_veto": False}
        # Align length to match
        min_len = min(len(cvd), len(vol))
        cvd = cvd[-min_len:]
        vol = vol[-min_len:]

        # 1. First differencing (k=1 bar only)
        delta_cvd = np.diff(cvd)
        vol_bars = vol[1:]

        # 2. Volume-normalization -> Delta ratio r_t in [-1.0, 1.0]
        # Qwen F-13: Handle zero-volume bars safely to avoid 1e-9 clipping explosion to +/-1.0
        with np.errstate(divide='ignore', invalid='ignore'):
            valid_vol = vol_bars > 0.0
            r_t = np.zeros_like(delta_cvd, dtype=np.float32)
            r_t[valid_vol] = np.clip(delta_cvd[valid_vol] / vol_bars[valid_vol], -1.0, 1.0)

        # Context length: max 128 completed bars
        if len(r_t) > 128:
            r_t = r_t[-128:]

        # 3. Stationarity proxy check: bounded mean and variance
        r_mean = float(np.mean(r_t))
        r_std = float(np.std(r_t) + 1e-6)
        is_stationary_bounded = abs(r_mean) < 0.35 and r_std < 0.60

        try:
            point_fc, quantile_fc = self._model.forecast(horizon=horizon, inputs=[r_t])
            point_fc, quantile_fc = np.asarray(point_fc), np.asarray(quantile_fc)
            if point_fc.shape != (1, horizon) or quantile_fc.ndim != 3 or quantile_fc.shape[:2] != (1, horizon):
                raise ValueError('Malformed forecast shape')
            if not np.isfinite(point_fc).all() or not np.isfinite(quantile_fc).all():
                raise ValueError('Nonfinite forecast')
            pts = point_fc[0][:horizon]

            # Qwen F-06: Resolve quantile columns dynamically to support both:
            # 1. Native TimesFM 2.5 PyTorch API: 10 columns [mean, q10, q20, q30, q40, q50, q60, q70, q80, q90]
            # 2. Standard 9-column quantile grid: [q10 ... q90]
            q_cols = quantile_fc.shape[-1]
            if q_cols >= 10:
                q10_idx, q50_idx, q90_idx = 1, 5, 9
            elif q_cols == 9:
                q10_idx, q50_idx, q90_idx = 0, 4, 8
            else:
                raise ValueError('Unknown quantile grid')

            q10 = quantile_fc[0][:horizon, q10_idx]  # 10th percentile
            q50 = quantile_fc[0][:horizon, q50_idx]  # 50th percentile (median)
            q90 = quantile_fc[0][:horizon, q90_idx]  # 90th percentile

            if np.any(q10 > q90):
                raise ValueError('Crossed forecast bounds')

            # Marginal quantile sums are diagnostics, not a joint horizon interval.
            # Horizon-integrated delta ratio (projected cumulative order-flow bias)
            projected_bias = float(np.sum(pts))
            q10_sum = float(np.sum(q10))
            q90_sum = float(np.sum(q90))
            interval_width = float(np.mean(q90 - q10))

            # 4. Orthogonal Risk Governance (Arena Rule: Do NOT scale lot size!)
            # Rule A: Adverse Veto
            # If 80% interval for trade horizon lies completely adverse, veto the trade
            adverse_veto = False
            veto_reason = ""
            if candidate_direction:
                cdir = candidate_direction.upper()
                if cdir == "BUY" and np.all(q90 < 0.0):
                    adverse_veto = True
                    veto_reason = f"BUY_VETO: All marginal upper quantiles are negative (q90_sum={q90_sum:.2f})"
                elif cdir == "SELL" and np.all(q10 > 0.0):
                    adverse_veto = True
                    veto_reason = f"SELL_VETO: All marginal lower quantiles are positive (q10_sum={q10_sum:.2f})"

            # Rule B: Confluence Hurdle Adjustment
            # Wider interval -> Higher required confluence hurdle (+0 to +10 pts)
            hurdle_penalty = float(np.clip((interval_width - 0.40) * 15.0, 0.0, 10.0))

            return {
                "available": True,
                "joint_horizon_interval_calibrated": False,
                "horizon": horizon,
                "projected_delta_ratio_sum": round(projected_bias, 4),
                "q10_sum": round(q10_sum, 4),
                "q90_sum": round(q90_sum, 4),
                "mean_interval_width": round(interval_width, 4),
                "adverse_veto": adverse_veto,
                "veto_reason": veto_reason,
                "confluence_hurdle_adjustment": round(hurdle_penalty, 2),
                "is_stationary_bounded": is_stationary_bounded,
                "point_forecast": [round(float(x), 4) for x in pts],
                "q10_downside": [round(float(x), 4) for x in q10],
                "q50_median": [round(float(x), 4) for x in q50],
                "q90_upside": [round(float(x), 4) for x in q90]
            }

        except Exception as e:
            logger.error(f"Inference error in TimesFM normalized forecast: {e}")
            return {
                "available": False,
                "error": str(e),
                "adverse_veto": False,
                "confluence_hurdle_adjustment": 0.0
            }


if __name__ == "__main__":
    print("Testing Updated TimesFM Stationary Forecaster...")
    fc = DeepMindTimesFMForecaster()
    print(f"TimesFM available: {fc.is_available}")
