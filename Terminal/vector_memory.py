"""
Vector Memory & Semantic Knowledge Engine (Powered by LanceDB & FastEmbed)
=========================================================================
Persistent, embedded vector database for semantic retrieval of:
- Historical trade setups and order-flow market conditions.
- Macro/microstructure regimes and whale order book snapshots.
- Forensic lessons and post-trade reflections.

Enables Gemini Flash to recall historically analogous market setups in sub-millisecond
queries to verify mathematical expectancy before executing new trades.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

import lancedb
import pyarrow as pa
from fastembed import TextEmbedding

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
VECTOR_DIR = DB_DIR / "Vector_Memory"
VECTOR_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("VectorMemory")
logger.setLevel(logging.INFO)

# Embedding model: Fast, local 384-dimensional ONNX model
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

        existing_tables = self.db.list_tables()
        table_names = [t if isinstance(t, str) else getattr(t, 'name', str(t)) for t in existing_tables]
        if "trade_setups" not in table_names:
            self.table_setups = self.db.create_table("trade_setups", schema=schema_setups)
            logger.info("Created 'trade_setups' LanceDB vector table.")
        else:
            self.table_setups = self.db.open_table("trade_setups")

    def record_setup(
        self,
        ticket_id: str,
        symbol: str,
        strategy: str,
        setup_text: str,
        direction: str,
        confluence_score: float,
        whale_imbalance: float,
        outcome: str = "PENDING",
        pnl_usd: float = 0.0,
        risk_usd: float = 0.0
    ):
        """Vectorizes and indexes a trade setup for future semantic lookups."""
        try:
            vec = self._embed_text(setup_text)
            now_iso = datetime.now(timezone.utc).isoformat()
            data = [{
                "vector": vec,
                "ticket_id": str(ticket_id),
                "symbol": symbol,
                "strategy": strategy,
                "setup_text": setup_text,
                "direction": direction,
                "confluence_score": float(confluence_score),
                "whale_imbalance": float(whale_imbalance),
                "outcome": outcome,
                "pnl_usd": float(pnl_usd),
                "risk_usd": float(risk_usd),
                "timestamp": now_iso
            }]
            self.table_setups.add(data)
            logger.info(f"Indexed trade setup for {symbol} (#{ticket_id}) in LanceDB.")
        except Exception as e:
            logger.error(f"Error indexing setup to LanceDB: {e}")

    def update_setup_outcome(self, ticket_id: str, outcome: str, pnl_usd: float):
        """Updates outcome and PnL of a previously recorded setup."""
        try:
            # Recreate table with updated record if found
            df = self.table_setups.to_pandas()
            if df.empty or ticket_id not in df["ticket_id"].values:
                return
            
            df.loc[df["ticket_id"] == ticket_id, "outcome"] = outcome
            df.loc[df["ticket_id"] == ticket_id, "pnl_usd"] = float(pnl_usd)
            
            # Overwrite table
            self.table_setups = self.db.create_table("trade_setups", df, mode="overwrite")
            logger.info(f"Updated LanceDB outcome for Ticket #{ticket_id}: {outcome} (${pnl_usd:.2f})")
        except Exception as e:
            logger.error(f"Error updating LanceDB setup outcome: {e}")

    def query_similar_setups(self, query_text: str, top_k: int = 5) -> Dict[str, Any]:
        """
        Retrieves top_k most semantically and microstructurally similar setups.
        Calculates empirical win-rate and expectancy of similar historical setups.
        """
        try:
            if len(self.table_setups) == 0:
                return {
                    "count": 0,
                    "win_rate": 0.5,
                    "avg_pnl": 0.0,
                    "similar_setups": [],
                    "verdict": "NO_HISTORICAL_DATA"
                }

            query_vec = self._embed_text(query_text)
            results = self.table_setups.search(query_vec).limit(top_k).to_pandas()
            
            if results.empty:
                return {"count": 0, "win_rate": 0.5, "avg_pnl": 0.0, "similar_setups": [], "verdict": "NO_MATCH"}

            completed = results[results["outcome"].isin(["WIN", "LOSS", "BREAKEVEN"])]
            
            if completed.empty:
                return {
                    "count": len(results),
                    "completed_count": 0,
                    "win_rate": 0.5,
                    "avg_pnl": 0.0,
                    "similar_setups": results[["symbol", "strategy", "outcome", "pnl_usd", "_distance"]].to_dict(orient="records"),
                    "verdict": "INSUFFICIENT_COMPLETED_HISTORY"
                }

            wins = len(completed[completed["outcome"] == "WIN"])
            losses = len(completed[completed["outcome"] == "LOSS"])
            total_resolved = wins + losses
            win_rate = round(wins / total_resolved, 3) if total_resolved > 0 else 0.5
            avg_pnl = round(float(completed["pnl_usd"].mean()), 2)

            verdict = "FAVORABLE" if win_rate >= 0.55 else ("UNFAVORABLE" if win_rate < 0.40 else "NEUTRAL")

            return {
                "count": len(results),
                "resolved_count": total_resolved,
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "verdict": verdict,
                "similar_setups": results[["ticket_id", "symbol", "strategy", "outcome", "pnl_usd", "_distance"]].to_dict(orient="records")
            }
        except Exception as e:
            logger.error(f"Error querying similar setups from LanceDB: {e}")
            return {"error": str(e), "win_rate": 0.5, "verdict": "ERROR"}


if __name__ == "__main__":
    vm = VectorMemory()
    print("Testing VectorMemory indexing and semantic retrieval...")
    
    # Test index sample past trade
    sample_text = "BTC absorption buy with $12M whale bid wall support at 76,800 and 70% near-field short liquidation imbalance."
    vm.record_setup(
        ticket_id="TEST_001",
        symbol="BTC",
        strategy="Whale Bid Wall Support Fade",
        setup_text=sample_text,
        direction="BUY",
        confluence_score=85.0,
        whale_imbalance=0.72,
        outcome="WIN",
        pnl_usd=42.50,
        risk_usd=30.00
    )
    
    # Query similar
    query = "Bitcoin strong absorption at key support with massive whale bid orders and short liquidations."
    sim = vm.query_similar_setups(query, top_k=3)
    print("Query Results:")
    print(sim)
