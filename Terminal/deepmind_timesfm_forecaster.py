"""
========================================================================================
Google DeepMind TimesFM 2.5 (200M) Local Foundation Model Forecaster
========================================================================================
Provides zero-shot point and probabilistic quantile forecasting for crypto market microstructure:
- Cumulative Volume Delta (CVD) trajectory prediction
- Order-flow momentum and price exhaustion estimation
- Probabilistic downside (10th percentile) and upside (90th percentile) risk boundaries
========================================================================================
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional
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

            # Compile with default horizon=12, max_context=128
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

    def forecast_trajectory(
        self,
        series: np.ndarray,
        horizon: int = 12
    ) -> Dict[str, Any]:
        """
        Generates point and quantile forecasts for a given 1D time series (CVD or price).
        
        Args:
            series: 1D array of historical values (e.g. 32 to 128 bars).
            horizon: Number of forward periods to predict (default: 12).
            
        Returns:
            Dict containing point forecast, quantiles (10, 50, 90), expected drift, and direction.
        """
        if not self.is_available:
            return {
                "available": False,
                "error": "Model not loaded or compiled",
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0
            }

        arr = np.asarray(series, dtype=np.float32).flatten()
        if len(arr) < 8:
            return {
                "available": False,
                "error": f"Series too short ({len(arr)} bars, need >= 8)",
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0
            }

        # Truncate to max 128 bars
        if len(arr) > 128:
            arr = arr[-128:]

        try:
            point_fc, quantile_fc = self._model.forecast(horizon=horizon, inputs=[arr])
            
            # point_fc shape: (1, horizon)
            # quantile_fc shape: (1, horizon, 10) -> quantiles: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, mean]
            pts = point_fc[0][:horizon]
            q10 = quantile_fc[0][:horizon, 0] # 10th percentile
            q50 = quantile_fc[0][:horizon, 4] # 50th percentile (median)
            q90 = quantile_fc[0][:horizon, 8] # 90th percentile

            last_val = arr[-1]
            end_val = pts[-1]
            drift = float(end_val - last_val)
            drift_pct = float(drift / (abs(last_val) + 1e-9))

            # Directional consensus
            if drift > 0 and q10[-1] > last_val * 0.995:
                direction = "STRONG_BULLISH"
            elif drift > 0:
                direction = "BULLISH"
            elif drift < 0 and q90[-1] < last_val * 1.005:
                direction = "STRONG_BEARISH"
            elif drift < 0:
                direction = "BEARISH"
            else:
                direction = "NEUTRAL"

            spread = float(np.mean(q90 - q10))
            confidence = float(np.clip(1.0 - (spread / (abs(drift) + spread + 1e-6)), 0.1, 0.99))

            return {
                "available": True,
                "horizon": horizon,
                "last_value": float(last_val),
                "predicted_end": float(end_val),
                "predicted_drift": drift,
                "predicted_drift_pct": drift_pct,
                "direction": direction,
                "confidence": round(confidence, 4),
                "point_forecast": [round(float(x), 4) for x in pts],
                "q10_downside": [round(float(x), 4) for x in q10],
                "q50_median": [round(float(x), 4) for x in q50],
                "q90_upside": [round(float(x), 4) for x in q90],
            }

        except Exception as e:
            logger.error(f"Inference error during forecast: {e}")
            return {
                "available": False,
                "error": str(e),
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0
            }


if __name__ == "__main__":
    print("Testing DeepMind TimesFM Forecaster...")
    forecaster = DeepMindTimesFMForecaster()
    if forecaster.is_available:
        # Test synthetic CVD trend
        cvd_data = np.cumsum(np.random.normal(0.8, 0.3, size=48)).astype(np.float32)
        res = forecaster.forecast_trajectory(cvd_data, horizon=8)
        print("Forecast Output:")
        for k, v in res.items():
            if k in ["point_forecast", "q10_downside", "q50_median", "q90_upside"]:
                print(f"  {k}: {v[:4]}... (total {len(v)})")
            else:
                print(f"  {k}: {v}")
    else:
        print("DeepMind TimesFM Forecaster unavailable.")
