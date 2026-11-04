"""
========================================================================================
Microstructure VPIN & Order Flow Toxicity Engine
========================================================================================
Implements true Volume-Synchronized Probability of Toxicity (VPIN) based on Easley, 
Lopez de Prado, and O'Hara (2012) and Kyle's Lambda price impact regression estimator.

Calculates real-time adverse selection risk from M15 volume and delta vectors in
Database/Candle_Data/<SYMBOL>.parquet.

Guarantees:
- True equal-volume bucket partitioning across cumulative trading volume (Easley et al. 2012).
- Proportional volume and buy/sell delta allocation across bucket transitions.
- Kyle's Lambda calculated via OLS price-impact regression slope: Cov(dP, dV, ddof=1) / Var(dV, ddof=1).
- Unbiased estimators with matched degrees of freedom (ddof=1) and strict zero-net-flow fallback.
- Strict input integrity: rejects bars where |delta| > volume, returning data_valid=False.
- Explicit data qualification: fails closed with regime="DATA_UNAVAILABLE" or "CORRUPTED_DELTA_INTEGRITY".
========================================================================================
"""

import sys
import math
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"

logger = logging.getLogger("MicrostructureVPIN")
logger.setLevel(logging.INFO)


def _construct_volume_buckets(
    volumes: np.ndarray,
    deltas: np.ndarray,
    num_buckets: int = 50
) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Constructs N equal-volume buckets per Easley, Lopez de Prado & O'Hara (2012).
    Partitions continuous volume into fixed-size buckets of size V = sum(volumes) / num_buckets,
    allocating buy/sell delta proportionally when a bar spans bucket boundaries.
    """
    total_vol = float(np.sum(volumes))
    if total_vol <= 0 or num_buckets <= 0:
        return np.zeros(0), np.zeros(0), 0.0

    bucket_size = total_vol / float(num_buckets)
    if bucket_size <= 0:
        return np.zeros(0), np.zeros(0), 0.0

    v_buys = (volumes + deltas) / 2.0
    v_sells = (volumes - deltas) / 2.0

    bucket_buys = []
    bucket_sells = []

    cur_b_buy = 0.0
    cur_b_sell = 0.0
    cur_b_filled = 0.0

    for v, vb, vs in zip(volumes, v_buys, v_sells):
        rem_bar_vol = v
        rem_bar_buy = vb
        rem_bar_sell = vs

        while rem_bar_vol > 1e-12:
            needed = bucket_size - cur_b_filled
            if rem_bar_vol >= needed:
                # Fill current bucket to exact capacity
                frac = needed / rem_bar_vol if rem_bar_vol > 0 else 1.0
                cur_b_buy += rem_bar_buy * frac
                cur_b_sell += rem_bar_sell * frac

                bucket_buys.append(cur_b_buy)
                bucket_sells.append(cur_b_sell)

                rem_bar_vol -= needed
                rem_bar_buy -= rem_bar_buy * frac
                rem_bar_sell -= rem_bar_sell * frac

                cur_b_buy = 0.0
                cur_b_sell = 0.0
                cur_b_filled = 0.0
            else:
                # Add remainder of bar to current bucket
                cur_b_buy += rem_bar_buy
                cur_b_sell += rem_bar_sell
                cur_b_filled += rem_bar_vol
                rem_bar_vol = 0.0
                rem_bar_buy = 0.0
                rem_bar_sell = 0.0

    # If final bucket has volume, append it
    if cur_b_filled > 1e-12:
        bucket_buys.append(cur_b_buy)
        bucket_sells.append(cur_b_sell)

    return np.array(bucket_buys, dtype=np.float64), np.array(bucket_sells, dtype=np.float64), bucket_size


def _estimate_kyles_lambda(prices: np.ndarray, deltas: np.ndarray) -> float:
    """
    Estimates Kyle's Lambda (price impact slope) via linear regression:
    dP_t = lambda * Delta_t + epsilon_t
    lambda = Cov(dP, Delta, ddof=1) / (Var(Delta, ddof=1) + eps)
    Strictly returns 0.0 when net flow is zero (sum(Delta) == 0) or Var(Delta) == 0.
    """
    if len(prices) < 2 or len(deltas) < 2:
        return 0.0

    if not np.all(np.isfinite(prices)) or not np.all(np.isfinite(deltas)):
        return 0.0

    dP = np.diff(prices)
    flow = deltas[1:]  # align with price differences

    if len(dP) < 2 or len(flow) < 2:
        return 0.0

    # Explicit Zero Net Flow check (across either input deltas or flow slice)
    if abs(float(np.sum(deltas))) < 1e-5 or abs(float(np.sum(flow))) < 1e-5:
        return 0.0

    var_flow = float(np.var(flow, ddof=1))
    if var_flow < 1e-12:
        return 0.0

    cov_matrix = np.cov(dP, flow, ddof=1)
    cov_p_flow = float(cov_matrix[0, 1])

    raw_lambda = cov_p_flow / var_flow
    if abs(raw_lambda) < 1e-8:
        return 0.0
    return float(np.clip(raw_lambda, 0.0, 100.0))


def compute_vpin_and_toxicity(
    symbol: str,
    candle_df: Optional[pd.DataFrame] = None,
    lookback_bars: int = 50,
    num_buckets: int = 50
) -> Dict[str, Any]:
    """
    Computes true Volume-Synchronized Probability of Toxicity (VPIN) and
    Kyle's Lambda statistical price impact coefficient.
    """
    try:
        clean_sym = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        if candle_df is None:
            p_file = CANDLE_DIR / f"{clean_sym}.parquet"
            if not p_file.exists():
                p_file = CANDLE_DIR / f"{symbol}.parquet"
            if not p_file.exists():
                return {
                    "symbol": symbol,
                    "vpin": 0.50,
                    "regime": "DATA_UNAVAILABLE",
                    "adverse_selection_risk": "UNKNOWN",
                    "kyles_lambda": 0.0,
                    "veto_toxic_flow": False,
                    "data_valid": False
                }
            df = pd.read_parquet(p_file)
        else:
            df = candle_df.copy()

        if df.empty or len(df) < 10:
            return {
                "symbol": symbol,
                "vpin": 0.50,
                "regime": "DATA_UNAVAILABLE",
                "adverse_selection_risk": "UNKNOWN",
                "kyles_lambda": 0.0,
                "veto_toxic_flow": False,
                "data_valid": False
            }

        # Window slice
        tail_df = df.tail(lookback_bars).copy()
        
        # Resolve volume and delta
        vol_col = "total_volume" if "total_volume" in tail_df.columns else "volume" if "volume" in tail_df.columns else "tick_volume" if "tick_volume" in tail_df.columns else None
        delta_col = "delta" if "delta" in tail_df.columns else None

        if not vol_col or tail_df[vol_col].sum() <= 0:
            return {
                "symbol": symbol,
                "vpin": 0.50,
                "regime": "DATA_UNAVAILABLE",
                "adverse_selection_risk": "UNKNOWN",
                "kyles_lambda": 0.0,
                "veto_toxic_flow": False,
                "data_valid": False
            }

        raw_volumes = tail_df[vol_col].values.astype(np.float64)
        raw_volumes = np.maximum(raw_volumes, 0.0)

        # STRICT NON-FINITE INTEGRITY: Reject NaN or infinite volumes
        if not np.all(np.isfinite(raw_volumes)):
            return {
                "symbol": symbol,
                "vpin": 0.50,
                "regime": "CORRUPTED_DELTA_INTEGRITY",
                "adverse_selection_risk": "UNKNOWN",
                "kyles_lambda": 0.0,
                "veto_toxic_flow": False,
                "data_valid": False,
                "error": "Non-finite volume values detected"
            }

        if delta_col and delta_col in tail_df.columns:
            raw_deltas = tail_df[delta_col].values.astype(np.float64)
            # STRICT NON-FINITE INTEGRITY: Reject NaN or infinite deltas
            if not np.all(np.isfinite(raw_deltas)):
                return {
                    "symbol": symbol,
                    "vpin": 0.50,
                    "regime": "CORRUPTED_DELTA_INTEGRITY",
                    "adverse_selection_risk": "UNKNOWN",
                    "kyles_lambda": 0.0,
                    "veto_toxic_flow": False,
                    "data_valid": False,
                    "error": "Non-finite delta values detected"
                }
            # STRICT DATA INTEGRITY: |delta| cannot exceed volume
            # If any bar has |delta| > volume + 1e-4, reject input rather than silently sanitize
            overshoot = np.abs(raw_deltas) - raw_volumes
            if np.any(overshoot > 1e-4):
                return {
                    "symbol": symbol,
                    "vpin": 0.50,
                    "regime": "CORRUPTED_DELTA_INTEGRITY",
                    "adverse_selection_risk": "UNKNOWN",
                    "kyles_lambda": 0.0,
                    "veto_toxic_flow": False,
                    "data_valid": False,
                    "error": "Input delta exceeds total bar volume"
                }
        else:
            # Proxy delta using candle range
            closes = tail_df["close"].values.astype(np.float64)
            opens = tail_df["open"].values.astype(np.float64)
            highs = tail_df["high"].values.astype(np.float64)
            lows = tail_df["low"].values.astype(np.float64)
            if not (np.all(np.isfinite(closes)) and np.all(np.isfinite(opens)) and np.all(np.isfinite(highs)) and np.all(np.isfinite(lows))):
                return {
                    "symbol": symbol,
                    "vpin": 0.50,
                    "regime": "CORRUPTED_DELTA_INTEGRITY",
                    "adverse_selection_risk": "UNKNOWN",
                    "kyles_lambda": 0.0,
                    "veto_toxic_flow": False,
                    "data_valid": False,
                    "error": "Non-finite candle price values detected"
                }
            ranges = np.maximum(highs - lows, 1e-8)
            raw_deltas = raw_volumes * ((closes - opens) / ranges)

        # True Volume-Synchronized Partitioning
        bucket_buys, bucket_sells, bucket_size = _construct_volume_buckets(
            raw_volumes, raw_deltas, num_buckets=num_buckets
        )

        if len(bucket_buys) > 0 and bucket_size > 0:
            # VPIN = sum(|V_buy - V_sell|) / (N * V)
            imbalances = np.abs(bucket_buys - bucket_sells)
            total_bucket_vol = float(np.sum(bucket_buys + bucket_sells))
            if total_bucket_vol > 0:
                vpin = float(np.clip(np.sum(imbalances) / total_bucket_vol, 0.0, 1.0))
            else:
                vpin = 0.50
        else:
            vpin = 0.50

        # Statistical Kyle's Lambda Regression
        if "close" in tail_df.columns:
            closes = tail_df["close"].values.astype(np.float64)
            kyles_lambda = _estimate_kyles_lambda(closes, raw_deltas)
        else:
            kyles_lambda = 0.0

        # Toxicity Regimes
        if vpin >= 0.80:
            regime = "EXTREME_TOXICITY"
            adverse_risk = "CRITICAL"
            veto = True
        elif vpin >= 0.65:
            regime = "HIGH_TOXICITY"
            adverse_risk = "ELEVATED"
            veto = False
        elif vpin >= 0.40:
            regime = "MODERATE_TOXICITY"
            adverse_risk = "BALANCED"
            veto = False
        else:
            regime = "LOW_TOXICITY"
            adverse_risk = "MINIMAL"
            veto = False

        return {
            "symbol": symbol,
            "vpin": round(vpin, 4),
            "regime": regime,
            "adverse_selection_risk": adverse_risk,
            "kyles_lambda": round(kyles_lambda, 8),
            "total_window_volume": round(float(np.sum(raw_volumes)), 2),
            "num_buckets_evaluated": len(bucket_buys),
            "veto_toxic_flow": veto,
            "data_valid": True
        }

    except Exception as e:
        logger.debug(f"Error computing VPIN for {symbol}: {e}")
        return {
            "symbol": symbol,
            "vpin": 0.50,
            "regime": "ERROR",
            "adverse_selection_risk": "UNKNOWN",
            "kyles_lambda": 0.0,
            "veto_toxic_flow": False,
            "data_valid": False,
            "error": str(e)
        }
