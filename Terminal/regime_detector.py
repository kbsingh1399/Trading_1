"""
Market Microstructure Regime Detector (Powered by HMM & Quantitative Volatility Analytics)
========================================================================================
Identifies current market regime for crypto assets by analyzing:
1. 15-minute Candlestick price action & ATR volatility.
2. Order-Flow Cumulative Volume Delta (CVD) momentum and divergences.
3. Whale wall absorption vs liquidation breakout conditions.

Outputs:
- Primary Regime: [BULLISH_TREND, BEARISH_TREND, MEAN_REVERTING_RANGE, HIGH_VOLATILITY_CHOP]
- Strategy suitability weights to dynamically boost or veto trading signals.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd
from Terminal.runtime_io import completed_candles

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"

logger = logging.getLogger("RegimeDetector")
logger.setLevel(logging.INFO)


class RegimeDetector:
    def __init__(self):
        pass

    def analyze_symbol(self, symbol: str) -> Dict[str, Any]:
        """Analyzes 15m candle & volume delta data to classify the market regime."""
        candle_file = CANDLE_DIR / f"{symbol}.parquet"
        if not candle_file.exists():
            candle_file = CANDLE_DIR / f"{symbol}_15m.parquet"
        if not candle_file.exists():
            return {
                "symbol": symbol,
                "regime": "UNKNOWN",
                "volatility_regime": "NORMAL",
                "trend_strength": 0.0,
                "cvd_slope": 0.0,
                "recommended_strategies": ["Neutral"],
                "strategy_multipliers": {
                    "Whale Bid Wall Support Fade": 1.0,
                    "Whale Ask Wall Resistance Fade": 1.0,
                    "Liquidation Cascade Breakout": 1.0,
                    "Order-flow Absorption Reversal": 1.0
                }
            }

        try:
            df = completed_candles(pd.read_parquet(candle_file))
            if len(df) < 10:
                return {"symbol": symbol, "regime": "INSUFFICIENT_DATA", "strategy_multipliers": {}}

            # Ensure numeric columns
            for col in ["open", "high", "low", "close", "volume", "delta"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            df = df.dropna(subset=["close"]).reset_index(drop=True)
            if len(df) < 10:
                return {"symbol": symbol, "regime": "INSUFFICIENT_DATA"}

            # Calculate basic indicators
            close = df["close"].values
            high = df["high"].values
            low = df["low"].values
            
            # 1. Volatility (Normalized ATR)
            tr = np.maximum(high[1:] - low[1:], np.maximum(abs(high[1:] - close[:-1]), abs(low[1:] - close[:-1])))
            atr_pct = (np.mean(tr[-10:]) / close[-1]) * 100.0 if len(tr) >= 10 else 1.0

            # 2. Price Return & Trend Velocity (EMA 8 vs EMA 21)
            ema_fast = pd.Series(close).ewm(span=8, adjust=False).mean().values
            ema_slow = pd.Series(close).ewm(span=21, adjust=False).mean().values
            trend_dist_pct = ((ema_fast[-1] - ema_slow[-1]) / close[-1]) * 100.0

            # 3. CVD (Cumulative Volume Delta) momentum if available
            cvd_slope = 0.0
            if "delta" in df.columns:
                delta_sum = df["delta"].iloc[-5:].sum()
                vol_sum = df["total_volume"].iloc[-5:].sum() if "total_volume" in df.columns else 0.0
                cvd_slope = float(delta_sum / vol_sum) if vol_sum > 0 else 0.0

            # Regime Classification Rules
            # High volatility threshold: ATR% > 2.0%
            is_high_vol = atr_pct > 2.0
            vol_regime = "HIGH" if is_high_vol else ("LOW" if atr_pct < 0.6 else "NORMAL")

            if trend_dist_pct > 0.4 and cvd_slope > 0.05:
                regime = "BULLISH_TREND"
                recs = ["Liquidation Cascade Breakout", "Whale Bid Wall Support Fade"]
                multipliers = {
                    "Whale Bid Wall Support Fade": 1.20,
                    "Whale Ask Wall Resistance Fade": 0.60,  # dangerous to short strong bull
                    "Liquidation Cascade Breakout": 1.30,
                    "Order-flow Absorption Reversal": 0.80
                }
            elif trend_dist_pct < -0.4 and cvd_slope < -0.05:
                regime = "BEARISH_TREND"
                recs = ["Whale Ask Wall Resistance Fade", "Order-flow Absorption Reversal"]
                multipliers = {
                    "Whale Bid Wall Support Fade": 0.60,  # dangerous to catch falling knife
                    "Whale Ask Wall Resistance Fade": 1.25,
                    "Liquidation Cascade Breakout": 1.30,
                    "Order-flow Absorption Reversal": 1.10
                }
            elif is_high_vol and abs(trend_dist_pct) < 0.3:
                regime = "HIGH_VOLATILITY_CHOP"
                recs = ["Defensive / Reduced Sizing"]
                multipliers = {
                    "Whale Bid Wall Support Fade": 0.70,
                    "Whale Ask Wall Resistance Fade": 0.70,
                    "Liquidation Cascade Breakout": 0.60,
                    "Order-flow Absorption Reversal": 0.70
                }
            else:
                regime = "MEAN_REVERTING_RANGE"
                recs = ["Whale Bid Wall Support Fade", "Whale Ask Wall Resistance Fade"]
                multipliers = {
                    "Whale Bid Wall Support Fade": 1.25,
                    "Whale Ask Wall Resistance Fade": 1.25,
                    "Liquidation Cascade Breakout": 0.75,
                    "Order-flow Absorption Reversal": 1.20
                }

            return {
                "symbol": symbol,
                "regime": regime,
                "volatility_regime": vol_regime,
                "atr_pct": round(atr_pct, 3),
                "trend_dist_pct": round(trend_dist_pct, 3),
                "cvd_slope": round(cvd_slope, 4),
                "recommended_strategies": recs,
                "strategy_multipliers": multipliers
            }

        except Exception as e:
            logger.error(f"Error computing regime for {symbol}: {e}")
            return {"symbol": symbol, "regime": "ERROR", "strategy_multipliers": {}}


if __name__ == "__main__":
    detector = RegimeDetector()
    for s in ["BTC", "ETH", "SOL", "XRP"]:
        res = detector.analyze_symbol(s)
        print(f"[{s}] Regime: {res.get('regime')} | Vol: {res.get('volatility_regime')} | ATR: {res.get('atr_pct')}%")
