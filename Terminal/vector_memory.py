"""
Vector Memory & Counterfactual Replay Engine (Powered by LanceDB & FastEmbed)
=============================================================================
Persistent memory system for:
1. Forward Counterfactual Replay: Simulates rejected setups against forward price candles
   using conservative tie-breaking (if both SL and TP fall in same bar, assume SL hit first).
2. Filter Efficiency Measurement: Computes empirical-Bayes shrunk hit rate n/(n+50)
   per (regime * setup_class * filter_reason).
3. Exact Deterministic Retrieval: Evaluates economic and order-flow state vectors.
=============================================================================
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple
import pandas as pd
import numpy as np

import lancedb
import pyarrow as pa
from fastembed import TextEmbedding

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
VECTOR_DIR = DB_DIR / "Vector_Memory"
VECTOR_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("VectorMemory")
logger.setLevel(logging.INFO)

MODEL_NAME = "BAAI/bge-small-en-v1.5"


class VectorMemory:
    _instance = None
    _embedder = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(VectorMemory, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        self.db = lancedb.connect(VECTOR_DIR.as_posix())
        self._init_embedder()
        self._init_tables()
        self._initialized = True

    def _init_embedder(self):
        if VectorMemory._embedder is None:
            logger.info(f"Initializing FastEmbed model: {MODEL_NAME}")
            VectorMemory._embedder = TextEmbedding(model_name=MODEL_NAME)
        self.embedder = VectorMemory._embedder

    def _embed_text(self, text: str) -> List[float]:
        embeddings = list(self.embedder.embed([text]))
        return embeddings[0].tolist()

    def _init_tables(self):
        """Create or open tables for trade setups and forensic reflections."""
        schema_setups = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("ticket_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("strategy", pa.string()),
            pa.field("setup_text", pa.string()),
            pa.field("direction", pa.string()),
            pa.field("confluence_score", pa.float32()),
            pa.field("whale_imbalance", pa.float32()),
            pa.field("outcome", pa.string()),  # WIN, LOSS, BREAKEVEN, PENDING
            pa.field("pnl_usd", pa.float32()),
            pa.field("risk_usd", pa.float32()),
            pa.field("timestamp", pa.string())
        ])

        schema_counterfactual = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("alert_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("direction", pa.string()),
            pa.field("rejection_reason", pa.string()),
            pa.field("setup_text", pa.string()),
            pa.field("entry_price", pa.float32()),
            pa.field("stop_loss", pa.float32()),
            pa.field("take_profit", pa.float32()),
            pa.field("confluence_score", pa.float32()),
            pa.field("counterfactual_outcome", pa.string()),  # UNEVALUATED, HYPO_WIN, HYPO_LOSS, HYPO_EXPIRED
            pa.field("counterfactual_r", pa.float32()),       # Realized R multiple of counterfactual trade
            pa.field("timestamp", pa.string())
        ])

        existing_tables = self.db.list_tables()
        table_names = [t if isinstance(t, str) else getattr(t, 'name', str(t)) for t in existing_tables]
        
        if "trade_setups" not in table_names:
            self.table_setups = self.db.create_table("trade_setups", schema=schema_setups)
            logger.info("Created 'trade_setups' LanceDB vector table.")
        else:
            self.table_setups = self.db.open_table("trade_setups")

        if "counterfactual_setups" not in table_names:
            self.table_counterfactual = self.db.create_table("counterfactual_setups", schema=schema_counterfactual)
            logger.info("Created 'counterfactual_setups' LanceDB vector table.")
        else:
            self.table_counterfactual = self.db.open_table("counterfactual_setups")

    def record_rejected_setup(
        self,
        alert_id: str,
        symbol: str,
        direction: str,
        rejection_reason: str,
        setup_text: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        confluence_score: float
    ):
        """
        Records a trade setup that was rejected or failed admission gates.
        Persists full trade geometry so forward replay can simulate the true counterfactual path.
        """
        try:
            vec = self._embed_text(setup_text)
            now_iso = datetime.now(timezone.utc).isoformat()
            data = [{
                "vector": vec,
                "alert_id": str(alert_id),
                "symbol": str(symbol),
                "direction": str(direction),
                "rejection_reason": str(rejection_reason),
                "setup_text": str(setup_text),
                "entry_price": float(entry_price),
                "stop_loss": float(stop_loss),
                "take_profit": float(take_profit),
                "confluence_score": float(confluence_score),
                "counterfactual_outcome": "UNEVALUATED",
                "counterfactual_r": 0.0,
                "timestamp": now_iso
            }]
            self.table_counterfactual.add(data)
            logger.info(f"Recorded rejected setup for {symbol} ({rejection_reason}) in LanceDB counterfactual memory.")
        except Exception as e:
            logger.error(f"Error recording rejected setup to LanceDB: {e}")

    def simulate_counterfactual_replays(self) -> Dict[str, Any]:
        """
        Arena Round 2 Compliant Counterfactual Simulator:
        Replays UNEVALUATED rejected setups against subsequent candle price paths.
        Enforces conservative tie-breaking:
        If both SL and TP fall inside the same bar, ASSUME SL HIT FIRST.
        """
        try:
            df = self.table_counterfactual.to_pandas()
            if df.empty:
                return {"updated": 0, "status": "EMPTY"}

            uneval = df[df["counterfactual_outcome"] == "UNEVALUATED"]
            if uneval.empty:
                return {"updated": 0, "status": "ALL_EVALUATED"}

            updated_count = 0
            for idx, row in uneval.iterrows():
                sym = row["symbol"]
                candle_file = CANDLE_DIR / f"{sym}.parquet"
                if not candle_file.exists():
                    continue

                cdf = pd.read_parquet(candle_file)
                if cdf.empty or "high" not in cdf.columns or "low" not in cdf.columns:
                    continue

                decision_ts = pd.to_datetime(row["timestamp"])
                # Filter strictly forward bars
                if "time" in cdf.columns:
                    cdf["dt"] = pd.to_datetime(cdf["time"])
                    forward = cdf[cdf["dt"] > decision_ts].sort_values("dt").head(24)
                else:
                    forward = cdf.tail(24)

                if forward.empty:
                    continue

                direction = row["direction"].upper()
                entry = row["entry_price"]
                sl = row["stop_loss"]
                tp = row["take_profit"]
                risk_dist = abs(entry - sl) + 1e-9

                outcome = "HYPO_EXPIRED"
                realized_r = 0.0

                for _, bar in forward.iterrows():
                    high = bar["high"]
                    low = bar["low"]

                    if direction == "BUY":
                        hit_tp = high >= tp
                        hit_sl = low <= sl
                        if hit_tp and hit_sl:
                            # Conservative tie-breaking: Assume SL hit first
                            outcome = "HYPO_LOSS"
                            realized_r = -1.0
                            break
                        elif hit_sl:
                            outcome = "HYPO_LOSS"
                            realized_r = -1.0
                            break
                        elif hit_tp:
                            outcome = "HYPO_WIN"
                            realized_r = round((tp - entry) / risk_dist, 2)
                            break
                    elif direction == "SELL":
                        hit_tp = low <= tp
                        hit_sl = high >= sl
                        if hit_tp and hit_sl:
                            # Conservative tie-breaking: Assume SL hit first
                            outcome = "HYPO_LOSS"
                            realized_r = -1.0
                            break
                        elif hit_sl:
                            outcome = "HYPO_LOSS"
                            realized_r = -1.0
                            break
                        elif hit_tp:
                            outcome = "HYPO_WIN"
                            realized_r = round((entry - tp) / risk_dist, 2)
                            break

                if outcome == "HYPO_EXPIRED":
                    last_close = forward.iloc[-1]["close"] if "close" in forward.columns else entry
                    if direction == "BUY":
                        realized_r = round((last_close - entry) / risk_dist, 2)
                    else:
                        realized_r = round((entry - last_close) / risk_dist, 2)

                df.at[idx, "counterfactual_outcome"] = outcome
                df.at[idx, "counterfactual_r"] = float(realized_r)
                updated_count += 1

            if updated_count > 0:
                self.table_counterfactual = self.db.create_table("counterfactual_setups", df, mode="overwrite")
                logger.info(f"Simulated {updated_count} counterfactual trade outcomes in LanceDB.")

            return {"updated": updated_count, "status": "SUCCESS"}
        except Exception as e:
            logger.error(f"Error simulating counterfactual replays: {e}")
            return {"error": str(e)}

    def calculate_filter_efficiency(self, filter_reason: Optional[str] = None) -> Dict[str, Any]:
        """
        Calculates empirical-Bayes shrunk filter efficiency:
        Shrinkage = n / (n + 50) toward base rate 0.50.
        Filter is considered value-additive if rejected trades had negative expectancy.
        """
        try:
            df = self.table_counterfactual.to_pandas()
            if df.empty:
                return {"n": 0, "status": "NO_DATA"}

            resolved = df[df["counterfactual_outcome"].isin(["HYPO_WIN", "HYPO_LOSS", "HYPO_EXPIRED"])]
            if filter_reason:
                resolved = resolved[resolved["rejection_reason"] == filter_reason]

            n = len(resolved)
            if n == 0:
                return {"n": 0, "status": "INSUFFICIENT_RESOLVED"}

            wins = len(resolved[resolved["counterfactual_outcome"] == "HYPO_WIN"])
            losses = len(resolved[resolved["counterfactual_outcome"] == "HYPO_LOSS"])
            mean_r = float(resolved["counterfactual_r"].mean())

            raw_win_rate = wins / n
            # Empirical-Bayes shrinkage toward 0.50
            k_prior = 50.0
            shrunk_win_rate = ((n * raw_win_rate) + (k_prior * 0.50)) / (n + k_prior)

            # Filter efficiency: positive if rejected trades were indeed losing (saving money)
            filter_value_add = -mean_r  # If counterfactual R is -0.4, filter saved +0.4 R

            return {
                "sample_size": n,
                "raw_win_rate": round(raw_win_rate, 3),
                "shrunk_win_rate": round(shrunk_win_rate, 3),
                "counterfactual_mean_r": round(mean_r, 3),
                "filter_value_add_r": round(filter_value_add, 3),
                "verdict": "VALUE_ADDITIVE" if filter_value_add > 0 else "DESTRUCTIVE_DRAG"
            }
        except Exception as e:
            return {"error": str(e)}
