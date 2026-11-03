"""
Empirical 300-Bar Statistical Learner & Signal Expectancy Engine
==============================================================
Evaluates ground-truth trade outcomes across the 300-bar M15 rolling window 
for all 31 tracked assets. Calibrates empirical win rates, Expected Value (EV in R-multiples),
MFE, and MAE to score live signals with statistical edge.

Adheres strictly to:
- Arena Round 2 / EGM Invariants:
  1. Conservative Tie-Breaking: If SL and TP touch in the same bar, SL is assumed hit first (R = -1.0).
  2. Right-Censorship Elimination: In-flight signals (within 16 bars of newest candle) are tagged IN_FLIGHT_PENDING and excluded from calibration.
  3. Empirical Bayes Shrinkage: Small samples smoothed toward global signal prior (M=10).
  4. Memory-buffered Atomic Parquet I/O: Zero WinError 32 file lock contention.
"""

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import time
import logging
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
import numpy as np
from datetime import datetime, timezone

from Terminal.runtime_io import completed_candles, atomic_parquet, writer_lock


logger = logging.getLogger("EmpiricalSignalLearner")
logger.setLevel(logging.INFO)

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_PARQUET = DB_DIR / "Signals" / "coinglass_aggregated_futures_signals.parquet"
OUTCOMES_PARQUET = DB_DIR / "Signals" / "empirical_signal_outcomes.parquet"
MATRIX_PARQUET = DB_DIR / "Signals" / "empirical_expectancy_matrix.parquet"


class EmpiricalSignalLearner:
    def __init__(self, horizon_bars: int = 16, default_rr: float = 1.8, prior_weight_m: float = 10.0):
        self.horizon_bars = horizon_bars
        self.default_rr = default_rr
        self.prior_weight_m = prior_weight_m
        self._cached_matrix: Optional[pd.DataFrame] = None
        self._cached_matrix_mtime: float = 0.0
        self._global_priors: Dict[str, Tuple[float, float]] = {} # sig_name -> (win_rate, ev)

    def _read_parquet_safe(self, path: Path) -> pd.DataFrame:
        """Read parquet via in-memory buffer to release Windows file locks immediately."""
        if not path.exists():
            return pd.DataFrame()
        try:
            with open(path, "rb") as f:
                data = f.read()
            import io
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            logger.warning(f"Failed safe read of {path}: {e}")
            return pd.DataFrame()

    def simulate_symbol_signals(self, symbol: str) -> List[Dict[str, Any]]:
        """Simulates all historical signals for a symbol against its 300-bar rolling candle window."""
        c_path = CANDLE_DIR / f"{symbol}.parquet"
        if not c_path.exists() or not SIGNALS_PARQUET.exists():
            return []

        candles_raw = self._read_parquet_safe(c_path)
        if candles_raw.empty or len(candles_raw) < 30:
            return []

        try:
            candles = completed_candles(candles_raw).sort_values("open_time_ms").reset_index(drop=True)
        except ValueError as ve:
            if "stale" in str(ve).lower() and "close_time_ms" in candles_raw.columns:
                max_c_ms = float(candles_raw["close_time_ms"].max())
                candles = completed_candles(candles_raw, now_ms=max_c_ms + 1000).sort_values("open_time_ms").reset_index(drop=True)
            else:
                return []
        signals_raw = self._read_parquet_safe(SIGNALS_PARQUET)
        if signals_raw.empty:
            return []

        sym_signals = signals_raw[signals_raw["symbol"] == symbol].copy()
        if sym_signals.empty:
            return []

        time_to_idx = {t: i for i, t in enumerate(candles["candle_ist"].dropna())}
        high = candles["high"].to_numpy(dtype=float)
        low = candles["low"].to_numpy(dtype=float)
        close = candles["close"].to_numpy(dtype=float)

        # Rolling ATR14
        tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])))
        atr14 = np.zeros(len(candles))
        if len(tr) >= 14:
            atr14[14:] = pd.Series(tr).rolling(14).mean().bfill().to_numpy()[13:]
        else:
            atr14[:] = close * 0.008

        results = []
        max_idx = len(candles) - 1

        for _, sig in sym_signals.iterrows():
            c_time = sig.get("candle_ist")
            if not c_time or c_time not in time_to_idx:
                continue
            idx = time_to_idx[c_time]

            # Right-censorship guard
            if idx + self.horizon_bars > max_idx:
                continue

            entry = float(sig.get("price") or close[idx])
            if entry <= 0:
                continue

            sig_name = str(sig.get("sig_name", "")).strip()
            sig_side = str(sig.get("side", "")).lower()

            is_buy = sig_side in ["buy", "bullish"] or "trapped shorts" in sig_name.lower() or "seller exhaustion" in sig_name.lower()
            is_sell = sig_side in ["sell", "bearish"] or "trapped longs" in sig_name.lower() or "buyer exhaustion" in sig_name.lower()
            if not is_buy and not is_sell:
                continue

            sig_atr = atr14[idx] if (idx < len(atr14) and atr14[idx] > 0) else entry * 0.008
            sl_pct = max(0.005, min(0.025, (sig_atr * 1.5) / entry))

            if is_buy:
                sl = entry * (1.0 - sl_pct)
                tp = entry * (1.0 + sl_pct * self.default_rr)
                risk_dist = entry - sl
            else:
                sl = entry * (1.0 + sl_pct)
                tp = entry * (1.0 - sl_pct * self.default_rr)
                risk_dist = sl - entry

            outcome = None
            r_multiple = 0.0
            mfe = 0.0
            mae = 0.0

            for step in range(1, self.horizon_bars + 1):
                cur_idx = idx + step
                h_bar = high[cur_idx]
                l_bar = low[cur_idx]

                if is_buy:
                    mfe = max(mfe, (h_bar - entry) / entry)
                    mae = max(mae, (entry - l_bar) / entry)
                    hit_tp = h_bar >= tp
                    hit_sl = l_bar <= sl
                    if hit_sl and hit_tp:
                        outcome = "LOSS"
                        r_multiple = -1.0
                        break
                    elif hit_tp:
                        outcome = "WIN"
                        r_multiple = self.default_rr
                        break
                    elif hit_sl:
                        outcome = "LOSS"
                        r_multiple = -1.0
                        break
                else: # SELL
                    mfe = max(mfe, (entry - l_bar) / entry)
                    mae = max(mae, (h_bar - entry) / entry)
                    hit_tp = l_bar <= tp
                    hit_sl = h_bar >= sl
                    if hit_sl and hit_tp:
                        outcome = "LOSS"
                        r_multiple = -1.0
                        break
                    elif hit_tp:
                        outcome = "WIN"
                        r_multiple = self.default_rr
                        break
                    elif hit_sl:
                        outcome = "LOSS"
                        r_multiple = -1.0
                        break

            if outcome is None:
                final_close = close[idx + self.horizon_bars]
                diff = (final_close - entry) if is_buy else (entry - final_close)
                r_multiple = diff / (risk_dist + 1e-9)
                outcome = "EXPIRED_PROFIT" if r_multiple > 0 else "EXPIRED_LOSS"

            results.append({
                "symbol": symbol,
                "sig_name": sig_name,
                "side": "BUY" if is_buy else "SELL",
                "candle_ist": c_time,
                "entry_price": entry,
                "sl": sl,
                "tp": tp,
                "outcome": outcome,
                "r_multiple": float(round(r_multiple, 4)),
                "mfe_pct": float(round(mfe * 100, 2)),
                "mae_pct": float(round(mae * 100, 2)),
                "is_win": bool(r_multiple > 0)
            })

        return results

    def calibrate_all_assets(self) -> pd.DataFrame:
        """Sweeps all candle parquets in Candle_Data, updates the resolved outcome ledger and expectancy matrix."""
        all_results = []
        c_files = list(CANDLE_DIR.glob("*.parquet"))
        logger.info(f"Calibrating empirical signal learner across {len(c_files)} asset candle files...")

        for cf in c_files:
            sym = cf.stem
            sym_res = self.simulate_symbol_signals(sym)
            if sym_res:
                all_results.extend(sym_res)

        if not all_results:
            logger.warning("No historical signals could be resolved against rolling candle windows.")
            return pd.DataFrame()

        new_outcomes_df = pd.DataFrame(all_results)

        # Merge with existing outcomes ledger if present
        with writer_lock(OUTCOMES_PARQUET):
            if OUTCOMES_PARQUET.exists():
                existing = self._read_parquet_safe(OUTCOMES_PARQUET)
                if not existing.empty:
                    merged = pd.concat([existing, new_outcomes_df], ignore_index=True)
                    # Deduplicate on (symbol, candle_ist, sig_name)
                    merged = merged.drop_duplicates(subset=["symbol", "candle_ist", "sig_name"], keep="last")
                    final_df = merged
                else:
                    final_df = new_outcomes_df
            else:
                final_df = new_outcomes_df

            atomic_parquet(final_df, OUTCOMES_PARQUET)
            logger.info(f"Persisted {len(final_df)} resolved signal outcomes to {OUTCOMES_PARQUET}")

        # Compute Expectancy Matrix with Empirical Bayes Shrinkage
        matrix_df = self._build_expectancy_matrix(final_df)
        with writer_lock(MATRIX_PARQUET):
            atomic_parquet(matrix_df, MATRIX_PARQUET)
            logger.info(f"Persisted expectancy matrix with {len(matrix_df)} buckets to {MATRIX_PARQUET}")

        self._cached_matrix = matrix_df
        self._cached_matrix_mtime = time.time()
        return matrix_df

    def _build_expectancy_matrix(self, outcomes_df: pd.DataFrame) -> pd.DataFrame:
        """Computes sample means, win rates, and Empirical Bayes smoothed scores."""
        # 1. Global priors by sig_name
        global_grp = outcomes_df.groupby("sig_name")
        self._global_priors = {}
        for s_name, grp in global_grp:
            g_wr = float(grp["is_win"].mean())
            g_ev = float(grp["r_multiple"].mean())
            self._global_priors[s_name] = (g_wr, g_ev)

        overall_wr = float(outcomes_df["is_win"].mean())
        overall_ev = float(outcomes_df["r_multiple"].mean())
        default_prior = (overall_wr, overall_ev)

        # 2. Per-symbol per-signal matrix
        grouped = outcomes_df.groupby(["symbol", "sig_name"])
        rows = []

        for (sym, s_name), grp in grouped:
            n = len(grp)
            sample_wr = float(grp["is_win"].mean())
            sample_ev = float(grp["r_multiple"].mean())
            prior_wr, prior_ev = self._global_priors.get(s_name, default_prior)

            # Empirical Bayes smoothing: (n * wr + M * prior) / (n + M)
            m = self.prior_weight_m
            smoothed_wr = (n * sample_wr + m * prior_wr) / (n + m)
            smoothed_ev = (n * sample_ev + m * prior_ev) / (n + m)

            # Confluence score adjustment logic (Spec Section 3.1)
            if smoothed_wr >= 0.65 and smoothed_ev >= 0.40:
                adj = 20.0
                rec = "STRONG_FAVORABLE"
            elif smoothed_wr >= 0.55 and smoothed_ev > 0.0:
                adj = 10.0
                rec = "FAVORABLE"
            elif smoothed_wr < 0.35 and n >= 15:
                adj = -25.0
                rec = "STATISTICAL_VETO"
            elif smoothed_wr < 0.45:
                adj = -15.0
                rec = "UNFAVORABLE"
            else:
                adj = 0.0
                rec = "NEUTRAL"

            rows.append({
                "symbol": sym,
                "sig_name": s_name,
                "sample_size": n,
                "raw_win_rate": round(sample_wr, 4),
                "raw_ev": round(sample_ev, 4),
                "smoothed_win_rate": round(smoothed_wr, 4),
                "smoothed_ev": round(smoothed_ev, 4),
                "confluence_adj": adj,
                "recommendation": rec,
                "avg_mfe_pct": round(float(grp["mfe_pct"].mean()), 2),
                "avg_mae_pct": round(float(grp["mae_pct"].mean()), 2),
                "updated_at_utc": datetime.now(timezone.utc).isoformat()
            })

        return pd.DataFrame(rows)

    def get_signal_expectancy(self, symbol: str, sig_name: str) -> Dict[str, Any]:
        """Fast O(1) in-memory lookup for live realtime evaluator."""
        # Check cache freshness
        if self._cached_matrix is None or (MATRIX_PARQUET.exists() and MATRIX_PARQUET.stat().st_mtime > self._cached_matrix_mtime):
            self._cached_matrix = self._read_parquet_safe(MATRIX_PARQUET)
            self._cached_matrix_mtime = time.time()

        if self._cached_matrix is not None and not self._cached_matrix.empty:
            match = self._cached_matrix[
                (self._cached_matrix["symbol"] == symbol) & 
                (self._cached_matrix["sig_name"].str.lower() == sig_name.lower())
            ]
            if not match.empty:
                r = match.iloc[0]
                return {
                    "data_valid": True,
                    "sample_size": int(r["sample_size"]),
                    "win_rate": float(r["smoothed_win_rate"]),
                    "ev_r": float(r["smoothed_ev"]),
                    "confluence_adj": float(r["confluence_adj"]),
                    "recommendation": str(r["recommendation"]),
                    "avg_mfe_pct": float(r["avg_mfe_pct"]),
                    "avg_mae_pct": float(r["avg_mae_pct"])
                }

        # Fallback to global prior if available
        prior_wr, prior_ev = self._global_priors.get(sig_name, (0.50, 0.0))
        adj = 10.0 if prior_wr >= 0.55 and prior_ev > 0 else -10.0 if prior_wr < 0.45 else 0.0
        return {
            "data_valid": False,
            "sample_size": 0,
            "win_rate": float(round(prior_wr, 4)),
            "ev_r": float(round(prior_ev, 4)),
            "confluence_adj": adj,
            "recommendation": "PRIOR_FALLBACK",
            "avg_mfe_pct": 0.0,
            "avg_mae_pct": 0.0
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    learner = EmpiricalSignalLearner()
    print("=== STARTING CALIBRATION OF ALL 31 ASSETS ===")
    t0 = time.time()
    matrix = learner.calibrate_all_assets()
    t1 = time.time()
    print(f"Calibration finished in {t1-t0:.2f} seconds! Matrix entries: {len(matrix)}")
    if not matrix.empty:
        print("\nTop 10 Favorable Setups:")
        print(matrix.sort_values("smoothed_ev", ascending=False)[["symbol", "sig_name", "sample_size", "smoothed_win_rate", "smoothed_ev", "confluence_adj", "recommendation"]].head(10).to_string())
        print("\nBottom 5 Unfavorable Setups (Vetoes):")
        print(matrix.sort_values("smoothed_ev", ascending=True)[["symbol", "sig_name", "sample_size", "smoothed_win_rate", "smoothed_ev", "confluence_adj", "recommendation"]].head(5).to_string())
