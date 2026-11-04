"""
Multi-Agent Swarm Coordinator Engine
====================================
Coordinates the three specialized subagent personas:
1. Data Sentinel: Audits 100% pipeline freshness, file parity, and stream health.
2. Quant Sentinel: Evaluates multi-stream confluence via DuckDB OLAP, LanceDB Vector Memory, and HMM Regimes.
3. Risk Governor: Enforces institutional risk guardrails, prop-firm floor budgets, and broker execution gates.

Produces structured consensus logs in Database/swarm_decisions.jsonl.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.olap_engine import OlapEngine
from Terminal.regime_detector import RegimeDetector
from Terminal.vector_memory import VectorMemory
from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster
from Terminal.persistent_memory import get_rules, get_strategy_weight, get_lessons

DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
DECISIONS_LOG = (SIGNALS_DIR / "swarm_decisions.jsonl") if (SIGNALS_DIR / "swarm_decisions.jsonl").exists() else (DB_DIR / "swarm_decisions.jsonl")
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SWARM-COORDINATOR] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SwarmCoordinator")


class SwarmCoordinator:
    def __init__(self):
        self.olap = OlapEngine()
        self.regime_det = RegimeDetector()
        self.vector_mem = VectorMemory()
        self.timesfm = DeepMindTimesFMForecaster()
        self.symbols = ["BTC", "ETH", "SOL", "XRP", "AVAX", "DOGE", "LINK", "NEAR"]

    # -------------------------------------------------------------------------
    # Subagent 1: Data Sentinel
    # -------------------------------------------------------------------------
    def run_data_sentinel_audit(self) -> Dict[str, Any]:
        """Audits the health, freshness, and parity of all Parquet datasets."""
        now = datetime.now(timezone.utc)
        audit = {
            "agent": "data-sentinel",
            "timestamp": now.isoformat(),
            "status": "HEALTHY",
            "datasets": {}
        }

        # Check Footprint Signals
        signals_p = (SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet") if (SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet").exists() else (DB_DIR / "coinglass_aggregated_futures_signals.parquet")
        if signals_p.exists():
            mod_time = datetime.fromtimestamp(signals_p.stat().st_mtime, timezone.utc)
            age_s = (now - mod_time).total_seconds()
            audit["datasets"]["footprint_signals"] = {
                "exists": True,
                "size_kb": round(signals_p.stat().st_size / 1024, 1),
                "age_seconds": round(age_s, 1),
                "status": "OK" if age_s < 3600 else "STALE"
            }
        else:
            audit["datasets"]["footprint_signals"] = {"exists": False, "status": "MISSING"}
            audit["status"] = "DEGRADED"

        # Check Whale Orders
        open_orders_dir = DB_DIR / "Whale_Orders" / "Open_Orders"
        open_count = len(list(open_orders_dir.glob("*.parquet"))) if open_orders_dir.exists() else 0
        audit["datasets"]["whale_open_orders"] = {
            "tables_count": open_count,
            "status": "OK" if open_count >= len(self.symbols) else "PARTIAL"
        }

        # Check Liquidation Zones
        mag_dir = DB_DIR / "Liquidation_Data" / "Magnet_Zones"
        mag_count = len(list(mag_dir.glob("*.parquet"))) if mag_dir.exists() else 0
        audit["datasets"]["liquidation_magnet_zones"] = {
            "tables_count": mag_count,
            "status": "OK" if mag_count >= len(self.symbols) else "PARTIAL"
        }

        # Check Candle CVD Data
        candle_dir = DB_DIR / "Candle_Data"
        candle_count = len(list(candle_dir.glob("*.parquet"))) if candle_dir.exists() else 0
        audit["datasets"]["candle_cvd_series"] = {
            "tables_count": candle_count,
            "status": "OK" if candle_count >= len(self.symbols) else "PARTIAL"
        }

        return audit

    # -------------------------------------------------------------------------
    # Subagent 2: Quant Sentinel
    # -------------------------------------------------------------------------
    def run_quant_sentinel_analysis(self) -> Dict[str, Any]:
        """Performs multi-stream DuckDB OLAP confluence scoring and regime detection."""
        now = datetime.now(timezone.utc)
        olap_results = self.olap.scan_all_confluences(self.symbols)
        
        setups = []
        for r in olap_results:
            sym = r["symbol"]
            reg = self.regime_det.analyze_symbol(sym)
            
            # Combine into composite conviction
            setup_desc = f"{sym} {r['bias']} with whale imbalance {r['whale_imbalance']*100:.1f}%, CVD {r['cvd_slope']}, regime {reg.get('regime')}"
            analogs = self.vector_mem.query_similar_setups(setup_desc, top_k=3)
            
            # DeepMind TimesFM Probabilistic Forecaster
            tfm_info = {"available": False, "direction": "NEUTRAL", "confidence": 0.0}
            cvd_series = self.olap.get_series(sym, "cumulative_delta", limit=64)
            if len(cvd_series) >= 16 and self.timesfm.is_available:
                tfm_res = self.timesfm.forecast_trajectory(cvd_series, horizon=8)
                if tfm_res.get("available"):
                    tfm_info = {
                        "available": True,
                        "direction": tfm_res.get("direction"),
                        "confidence": tfm_res.get("confidence"),
                        "predicted_drift": round(tfm_res.get("predicted_drift", 0.0), 2)
                    }
                    if (r["bias"] == "LONG" and "BULLISH" in tfm_res.get("direction", "")) or \
                       (r["bias"] == "SHORT" and "BEARISH" in tfm_res.get("direction", "")):
                        r["confluence_score"] += 15
                        r["reasons"].append(f"DeepMind TimesFM {tfm_res.get('direction')} ({tfm_res.get('confidence'):.0%})")
            
            setups.append({
                "symbol": sym,
                "confluence_score": r["confluence_score"],
                "bias": r["bias"],
                "whale_imbalance_bid_pct": round(r["whale_imbalance"] * 100, 1),
                "cvd_slope": r["cvd_slope"],
                "near_field_shorts_pct": round(r["near_field_shorts_pct"], 1),
                "regime": reg.get("regime"),
                "volatility": reg.get("volatility_regime"),
                "vector_memory_verdict": analogs.get("verdict"),
                "vector_memory_win_rate": analogs.get("win_rate"),
                "timesfm_forecast": tfm_info,
                "reasons": r["reasons"]
            })

        return {
            "agent": "quant-sentinel",
            "timestamp": now.isoformat(),
            "total_scanned": len(setups),
            "setups": setups
        }

    # -------------------------------------------------------------------------
    # Subagent 3: Risk Governor
    # -------------------------------------------------------------------------
    def run_risk_governor_audit(
        self,
        candidate_setups: List[Dict[str, Any]],
        current_equity: float = 4714.57,
        floor_equity: float = 4700.00,
        active_positions_count: int = 0
    ) -> Dict[str, Any]:
        """Applies institutional risk constraints, prop-firm floor budgets, and vetoes."""
        now = datetime.now(timezone.utc)
        buffer = max(0.0, current_equity - floor_equity)
        
        # Guardrail 1: Floor halt
        trading_halted = buffer <= 10.00
        
        # Guardrail 2: Dollar risk budget (never exceed 50% buffer or $50 hard cap)
        max_risk_allowed = min(50.00, max(0.0, buffer * 0.50)) if not trading_halted else 0.0

        vetted_trades = []
        for s in candidate_setups:
            score = s.get("confluence_score", 50.0)
            bias = s.get("bias", "NEUTRAL")
            sym = s.get("symbol")

            # Must have minimum confluence score and directional bias
            if score < 65.0 or bias == "NEUTRAL":
                continue

            # Position slots check
            if active_positions_count >= 2:
                s["governor_verdict"] = "VETO_MAX_POSITIONS_REACHED"
                continue

            if trading_halted:
                s["governor_verdict"] = "VETO_FLOOR_BUFFER_EXHAUSTED"
                continue

            if s.get("vector_memory_verdict") == "UNFAVORABLE":
                s["governor_verdict"] = "VETO_VECTOR_MEMORY_NEGATIVE_EXPECTANCY"
                continue

            # Approve setup
            s["governor_verdict"] = "APPROVED"
            s["approved_max_risk_usd"] = round(max_risk_allowed, 2)
            vetted_trades.append(s)

        return {
            "agent": "risk-governor",
            "timestamp": now.isoformat(),
            "current_equity": current_equity,
            "floor_equity": floor_equity,
            "buffer_above_floor": round(buffer, 2),
            "trading_halted": trading_halted,
            "max_risk_per_trade_usd": round(max_risk_allowed, 2),
            "approved_trades_count": len(vetted_trades),
            "approved_trades": vetted_trades
        }

    # -------------------------------------------------------------------------
    # Coordinator Master Cycle
    # -------------------------------------------------------------------------
    def run_consensus_cycle(self, equity: float = 4714.57) -> Dict[str, Any]:
        """Runs the entire multi-agent coordination consensus cycle."""
        logger.info("Executing Swarm Coordinator Consensus Cycle...")
        
        # 1. Data Sentinel Audit
        data_audit = self.run_data_sentinel_audit()
        
        # 2. Quant Sentinel Analysis
        quant_audit = self.run_quant_sentinel_analysis()
        
        # 3. Risk Governor Pre-Trade Audit
        risk_audit = self.run_risk_governor_audit(
            candidate_setups=quant_audit["setups"],
            current_equity=equity
        )

        consensus = {
            "cycle_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_sentinel": data_audit,
            "quant_sentinel": quant_audit,
            "risk_governor": risk_audit,
            "actionable_orders": risk_audit["approved_trades"]
        }

        # Log consensus decision
        with open(DECISIONS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(consensus) + "\n")

        logger.info(f"Consensus Complete: Data={data_audit['status']} | Setups Scanned={quant_audit['total_scanned']} | Approved Trades={risk_audit['approved_trades_count']}")
        return consensus


if __name__ == "__main__":
    coordinator = SwarmCoordinator()
    result = coordinator.run_consensus_cycle(equity=4714.57)
    
    print("\n" + "=" * 90)
    print("SWARM COORDINATOR CONSENSUS SUMMARY")
    print("=" * 90)
    print(f"Data Pipeline Health: {result['data_sentinel']['status']}")
    print(f"Account Equity Buffer: ${result['risk_governor']['buffer_above_floor']:.2f} above $4,700 floor (Max Risk: ${result['risk_governor']['max_risk_per_trade_usd']:.2f})")
    print(f"Actionable Approved Trades: {len(result['actionable_orders'])}")
    for s in result['quant_sentinel']['setups'][:5]:
        print(f" - {s['symbol']:<5} | Bias: {s['bias']:<8} | Score: {s['confluence_score']:<5} | Regime: {s['regime']:<22} | Memory: {s['vector_memory_verdict']}")
    print("=" * 90)
