"""
Terminal/trade_critic_engine.py
========================================================================================
Institutional Trade Critic & Post-Mortem Memory Engine (LanceDB & FastEmbed Powered)
========================================================================================
Purpose:
1. Anti-Falling Knife & Blow-Off Squeeze Microstructure Defense:
   Mathematically prevents buying into aggressive liquidation cascades or shorting
   into parabolic short squeezes without verified footprint absorption.
2. Deep Vector Post-Mortem Learning:
   Indexes every losing trade into LanceDB ('losing_trades_postmortem') using 384-D
   dense semantic embeddings (BAAI/bge-small-en-v1.5).
3. Pre-Trade Adversarial Criticism:
   Before order execution, audits candidate signals against historical loss vectors.
   If cosine similarity >= 0.85 to a recorded failure pattern, triggers immediate VETO
   or penalty to guarantee non-repetition of past trading errors.
========================================================================================
"""

import os
import sys
import json
import time
import math
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Tuple

import lancedb
import pyarrow as pa
from fastembed import TextEmbedding

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
VECTOR_DIR = DB_DIR / "Vector_Memory"
VECTOR_DIR.mkdir(parents=True, exist_ok=True)
CRITIC_JSONL = VECTOR_DIR / "losing_trades_critic.jsonl"

logger = logging.getLogger("TradeCriticEngine")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [CriticEngine] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(h)

MODEL_NAME = "BAAI/bge-small-en-v1.5"


# Canonical Failure Modes
FAILURE_FALLING_KNIFE_LONG = "FALLING_KNIFE_LONG"
FAILURE_PARABOLIC_SQUEEZE_SHORT = "PARABOLIC_SQUEEZE_SHORT"
FAILURE_PREMATURE_REVERSAL = "PREMATURE_REVERSAL"
FAILURE_UNEXHAUSTED_CASCADE = "UNEXHAUSTED_CASCADE"
FAILURE_WHALE_WALL_TRAP = "WHALE_WALL_TRAP"
FAILURE_LOW_RR_CHOP = "LOW_RR_CHOP"
FAILURE_HTF_MOMENTUM_RUNOVER = "HTF_MOMENTUM_RUNOVER"


CANONICAL_SEEDS = [
    {
        "failure_id": "SEED_KNIFE_01",
        "ticket_id": "CANONICAL_01",
        "symbol": "BTC",
        "direction": "BUY",
        "failure_mode": FAILURE_FALLING_KNIFE_LONG,
        "narrative": "Attempted BUY long on BTC during aggressive liquidation cascade. 4H macro trend was BEARISH, price was 3.5% below EMA233, CVD slope was strongly negative -0.65 with consecutive long liquidation spikes. No footprint absorption or delta divergence at swing low.",
        "critic_verdict": "Catastrophic falling knife purchase. Market had unexhausted downside institutional momentum with continuous market sell order flow overwhelming passive bid liquidity. Entered before volume climactic absorption appeared.",
        "mitigation_guard": "Strictly veto long entries when price is below EMA233, 4H macro is bearish, and CVD slope is negative without verified footprint absorption.",
        "cvd_slope": -0.65,
        "price_vs_ema233": -3.5,
        "whale_imbalance": -0.45,
        "liq_intensity": 85.0,
        "realized_loss_usd": 25.0
    },
    {
        "failure_id": "SEED_SQUEEZE_01",
        "ticket_id": "CANONICAL_02",
        "symbol": "ETH",
        "direction": "SELL",
        "failure_mode": FAILURE_PARABOLIC_SQUEEZE_SHORT,
        "narrative": "Attempted SELL short on ETH during parabolic short squeeze. 4H macro trend was BULLISH, price was 4.2% above EMA233, CVD slope was strongly positive +0.72 with aggressive market buying and short liquidation cascade. No upper exhaustion wick or footprint delta stall.",
        "critic_verdict": "Blown off on parabolic expansion. Attempted to catch the absolute top of a momentum trend fueled by forced buy-ins of liquidated shorts without waiting for buyer exhaustion or trapped longs.",
        "mitigation_guard": "Strictly veto short entries when price is above EMA233, 4H macro is bullish, and CVD slope is positive without verified upper footprint absorption.",
        "cvd_slope": 0.72,
        "price_vs_ema233": 4.2,
        "whale_imbalance": 0.50,
        "liq_intensity": 90.0,
        "realized_loss_usd": 25.0
    },
    {
        "failure_id": "SEED_TRAP_01",
        "ticket_id": "CANONICAL_03",
        "symbol": "SOL",
        "direction": "BUY",
        "failure_mode": FAILURE_WHALE_WALL_TRAP,
        "narrative": "Attempted BUY long directly into heavy institutional whale ask wall (Whale asks 78% vs bids 22%) at Value Area High resistance. Price immediately rejected at POC/VAH boundary.",
        "critic_verdict": "Bought straight into iceberg passive sell liquidity. Order book showed overwhelming ask wall resistance that was never absorbed before entry.",
        "mitigation_guard": "Veto or penalize long entries when whale ask imbalance exceeds 65% near Value Area High.",
        "cvd_slope": -0.15,
        "price_vs_ema233": 0.8,
        "whale_imbalance": -0.56,
        "liq_intensity": 35.0,
        "realized_loss_usd": 20.0
    },
    {
        "failure_id": "SEED_CHOP_01",
        "ticket_id": "CANONICAL_04",
        "symbol": "DOGE",
        "direction": "BUY",
        "failure_mode": FAILURE_LOW_RR_CHOP,
        "narrative": "Entered long inside tight low-volatility consolidation range with reward to risk of only 1.2:1. Market whipsawed within normal ATR noise, stopping out before reaching target.",
        "critic_verdict": "Sub-optimal trade geometry in chop. Low R:R (< 2:1) provides negative statistical expectancy against spread, slippage, and random price drift.",
        "mitigation_guard": "Enforce minimum 2.0:1 reward-to-risk ratio. Never execute setups in mid-range chop with compressed targets.",
        "cvd_slope": 0.05,
        "price_vs_ema233": -0.2,
        "whale_imbalance": 0.02,
        "liq_intensity": 20.0,
        "realized_loss_usd": 25.0
    },
    {
        "failure_id": "SEED_CASCADE_01",
        "ticket_id": "CANONICAL_05",
        "symbol": "LINK",
        "direction": "BUY",
        "failure_mode": FAILURE_UNEXHAUSTED_CASCADE,
        "narrative": "Premature mean-reversion attempt during wave 1 of a multi-leg liquidation cascade. The initial liquidation spike was mistaken for full capitulation, but secondary liquidation wave drove price 2% lower.",
        "critic_verdict": "Entered too early on capitulation signal without waiting for confirmation bar. Secondary flush violated tight stop loss.",
        "mitigation_guard": "Require bar-close confirmation and footprint delta reversal before fading a liquidation spike.",
        "cvd_slope": -0.40,
        "price_vs_ema233": -2.1,
        "whale_imbalance": -0.30,
        "liq_intensity": 75.0,
        "realized_loss_usd": 25.0
    }
]


class DeterministicOfflineEmbedder:
    """Deterministic, zero-network 384-dimensional embedder for offline/isolated execution and testing."""
    def embed(self, texts: List[str]):
        import hashlib
        import numpy as np
        for t in texts:
            h = hashlib.sha256(t.encode("utf-8")).digest()
            seed = int.from_bytes(h[:4], "big")
            rng = np.random.RandomState(seed)
            vec = rng.randn(384).astype(np.float32)
            words = t.lower()
            if "falling knife" in words or "cascade" in words:
                vec[:60] += 3.0
            if "parabolic" in words or "squeeze" in words:
                vec[60:120] += 3.0
            if "whale" in words or "trap" in words:
                vec[120:180] += 3.0
            if "chop" in words or "r:r" in words:
                vec[180:240] += 3.0
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
            yield vec


class TradeCriticEngine:
    _instance = None
    _embedder = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TradeCriticEngine, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        self.db = lancedb.connect(VECTOR_DIR.as_posix())
        self._init_embedder()
        self._init_table()
        self._initialized = True

    def _init_embedder(self):
        if TradeCriticEngine._embedder is None:
            if hasattr(self, "_custom_embedder") and self._custom_embedder is not None:
                TradeCriticEngine._embedder = self._custom_embedder
            else:
                try:
                    logger.info(f"Initializing FastEmbed model for Trade Critic: {MODEL_NAME}")
                    TradeCriticEngine._embedder = TextEmbedding(model_name=MODEL_NAME)
                except Exception as e:
                    logger.warning(f"FastEmbed TextEmbedding unavailable ({e}). Falling back to deterministic offline embedder.")
                    TradeCriticEngine._embedder = DeterministicOfflineEmbedder()
        self.embedder = TradeCriticEngine._embedder

    def embed_text(self, text: str) -> List[float]:
        """Embeds narrative into 384-dimensional vector."""
        emb = list(self.embedder.embed([text]))
        return emb[0].tolist()

    def _init_table(self):
        """Initializes or opens losing_trades_postmortem table in LanceDB."""
        schema = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("failure_id", pa.string()),
            pa.field("ticket_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("direction", pa.string()),
            pa.field("failure_mode", pa.string()),
            pa.field("narrative", pa.string()),
            pa.field("critic_verdict", pa.string()),
            pa.field("mitigation_guard", pa.string()),
            pa.field("cvd_slope", pa.float32()),
            pa.field("price_vs_ema233", pa.float32()),
            pa.field("whale_imbalance", pa.float32()),
            pa.field("liq_intensity", pa.float32()),
            pa.field("realized_loss_usd", pa.float32()),
            pa.field("timestamp", pa.string())
        ])

        existing_tables = self.db.list_tables()
        table_names = [t if isinstance(t, str) else getattr(t, 'name', str(t)) for t in existing_tables]

        if "losing_trades_postmortem" not in table_names:
            self.table = self.db.create_table("losing_trades_postmortem", schema=schema)
            logger.info("Created 'losing_trades_postmortem' LanceDB vector table.")
            self._seed_canonical_failures()
        else:
            self.table = self.db.open_table("losing_trades_postmortem")
            if len(self.table) < len(CANONICAL_SEEDS):
                self._seed_canonical_failures()

    def _seed_canonical_failures(self):
        """Pre-populates LanceDB with institutional canonical failure cases."""
        logger.info(f"Seeding {len(CANONICAL_SEEDS)} canonical failure scenarios into LanceDB...")
        records = []
        now_str = datetime.now(timezone.utc).isoformat()
        for seed in CANONICAL_SEEDS:
            vec = self.embed_text(seed["narrative"])
            record = {
                "vector": vec,
                "failure_id": seed["failure_id"],
                "ticket_id": seed["ticket_id"],
                "symbol": seed["symbol"],
                "direction": seed["direction"],
                "failure_mode": seed["failure_mode"],
                "narrative": seed["narrative"],
                "critic_verdict": seed["critic_verdict"],
                "mitigation_guard": seed["mitigation_guard"],
                "cvd_slope": float(seed["cvd_slope"]),
                "price_vs_ema233": float(seed["price_vs_ema233"]),
                "whale_imbalance": float(seed["whale_imbalance"]),
                "liq_intensity": float(seed["liq_intensity"]),
                "realized_loss_usd": float(seed["realized_loss_usd"]),
                "timestamp": now_str
            }
            records.append(record)

        self.table.add(records)
        logger.info(f"Canonical failure seeds successfully indexed in LanceDB (Total records: {len(self.table)}).")

    # =========================================================================
    # Microstructure Defense: Hard Falling Knife & Blow-Off Squeeze Veto
    # =========================================================================
    def check_microstructure_knife_veto(
        self,
        symbol: str,
        direction: str,
        price: float,
        ema233: float,
        htf_bias: str,
        cvd_slope: float,
        has_liq_spike: bool,
        liq_bias: str,
        has_footprint_absorption: bool,
        rr_ratio: float = 2.0,
        ema89: Optional[float] = None
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Deterministic, zero-latency microstructural knife detection.
        Returns (is_vetoed, reason_string, audit_metadata).
        """
        dir_clean = direction.upper()
        audit = {
            "symbol": symbol,
            "direction": dir_clean,
            "price": price,
            "ema233": ema233,
            "pct_from_ema233": round(((price - ema233) / (ema233 + 1e-9)) * 100, 2) if ema233 > 0 else 0.0,
            "ema89": ema89 or 0.0,
            "pct_from_ema89": round(((price - (ema89 or ema233)) / ((ema89 or ema233) + 1e-9)) * 100, 2) if (ema89 or ema233) > 0 else 0.0,
            "htf_bias": htf_bias.upper(),
            "cvd_slope": cvd_slope,
            "has_liq_spike": has_liq_spike,
            "liq_bias": liq_bias,
            "has_footprint_absorption": has_footprint_absorption,
            "rr_ratio": rr_ratio
        }

        # Guard 1: Minimum Reward-to-Risk Invariant
        if rr_ratio < 2.0:
            return True, f"VETO_LOW_RR_CHOP: Planned R:R ({rr_ratio:.2f}:1) violates strict minimum 2.0:1 threshold.", audit

        # Guard 2: Falling Knife Veto (for BUY / LONG signals)
        if dir_clean in ("BUY", "LONG"):
            # Quantitative Multi-Factor Knife Hazard Model (KHI)
            khi = 0.0
            if ema233 > 0 and price < ema233:
                pct_under = ((ema233 - price) / ema233) * 100.0
                khi += min(25.0, pct_under * 10.0)
            if ema89 is not None and ema89 > 0 and price < ema89:
                pct_under_89 = ((ema89 - price) / ema89) * 100.0
                khi += min(15.0, pct_under_89 * 8.0)
            if cvd_slope < 0:
                khi += min(25.0, abs(cvd_slope) * 40.0)
            if has_liq_spike and liq_bias in ("BEARISH_REVERSAL", "LONG_LIQ_DOMINANT"):
                khi += 20.0
            elif liq_bias == "LONG_LIQ_DOMINANT":
                khi += 10.0
            if "BEAR" in htf_bias.upper():
                khi += 15.0
            if not has_footprint_absorption:
                khi += 10.0

            audit["knife_hazard_index"] = round(khi, 1)

            # Price well below EMA233, 4H trend bearish, CVD dumping, long liquidations active without absorption
            is_below_ema = ema233 > 0 and price < ema233
            is_htf_bear = htf_bias.upper() in ("BEARISH", "STRONG_BEARISH")
            is_cvd_dumping = cvd_slope < -0.25
            is_liq_cascade = (has_liq_spike and liq_bias == "BEARISH_REVERSAL") or (liq_bias == "LONG_LIQ_DOMINANT")

            if is_below_ema and is_htf_bear and is_cvd_dumping and not has_footprint_absorption:
                reason = (
                    f"VETO_FALLING_KNIFE: Price (${price:.4f}) is below EMA233 (${ema233:.4f}) with 4H {htf_bias} bias, "
                    f"negative CVD slope ({cvd_slope:+.2f}), and zero footprint absorption. Catching falling knife blocked."
                )
                return True, reason, audit

            if is_liq_cascade and not has_footprint_absorption and is_cvd_dumping:
                reason = (
                    f"VETO_UNEXHAUSTED_CASCADE: Active long liquidation cascade detected with negative CVD slope ({cvd_slope:+.2f}) "
                    f"prior to absorption confirmation. Fading premature bounce blocked."
                )
                return True, reason, audit

            if khi >= 50.0:
                reason = (
                    f"VETO_KNIFE_HAZARD_INDEX: High composite falling knife risk ({khi:.1f}/100 >= 50.0). "
                    f"Downside order flow momentum unexhausted; buying into liquidation flush blocked."
                )
                return True, reason, audit

        # Guard 3: Parabolic Blow-Off Squeeze Veto (for SELL / SHORT signals)
        elif dir_clean in ("SELL", "SHORT"):
            # Quantitative Multi-Factor Squeeze Hazard Model (SHI)
            shi = 0.0
            if ema233 > 0 and price > ema233:
                pct_over = ((price - ema233) / ema233) * 100.0
                shi += min(25.0, pct_over * 10.0)
            if ema89 is not None and ema89 > 0 and price > ema89:
                pct_over_89 = ((price - ema89) / ema89) * 100.0
                shi += min(15.0, pct_over_89 * 8.0)
            if cvd_slope > 0:
                shi += min(25.0, abs(cvd_slope) * 40.0)
            if has_liq_spike and liq_bias in ("BULLISH_REVERSAL", "SHORT_LIQ_DOMINANT"):
                shi += 20.0
            elif liq_bias == "SHORT_LIQ_DOMINANT":
                shi += 10.0
            if "BULL" in htf_bias.upper():
                shi += 15.0
            if not has_footprint_absorption:
                shi += 10.0

            audit["squeeze_hazard_index"] = round(shi, 1)

            is_above_ema = ema233 > 0 and price > ema233
            is_htf_bull = htf_bias.upper() in ("BULLISH", "STRONG_BULLISH")
            is_cvd_pumping = cvd_slope > 0.25
            is_short_squeeze = (has_liq_spike and liq_bias == "BULLISH_REVERSAL") or (liq_bias == "SHORT_LIQ_DOMINANT")

            if is_above_ema and is_htf_bull and is_cvd_pumping and not has_footprint_absorption:
                reason = (
                    f"VETO_PARABOLIC_SQUEEZE: Price (${price:.4f}) is above EMA233 (${ema233:.4f}) with 4H {htf_bias} bias, "
                    f"positive CVD slope ({cvd_slope:+.2f}), and zero footprint absorption. Shorting parabolic squeeze blocked."
                )
                return True, reason, audit

            if is_short_squeeze and not has_footprint_absorption and is_cvd_pumping:
                reason = (
                    f"VETO_UNEXHAUSTED_SQUEEZE: Active short liquidation squeeze detected with positive CVD slope ({cvd_slope:+.2f}) "
                    f"prior to exhaustion wick. Fading parabolic momentum blocked."
                )
                return True, reason, audit

            if shi >= 50.0:
                reason = (
                    f"VETO_SQUEEZE_HAZARD_INDEX: High composite squeeze risk ({shi:.1f}/100 >= 50.0). "
                    f"Upside buying delta unexhausted; shorting parabolic squeeze blocked."
                )
                return True, reason, audit

        return False, "CLEARED_MICROSTRUCTURE_DEFENSE", audit

    # =========================================================================
    # Adversarial Vector Critic: Query Similarity to Historical Failures
    # =========================================================================
    def audit_candidate_setup(
        self,
        symbol: str,
        direction: str,
        price: float,
        confluence_score: float,
        htf_bias: str,
        cvd_slope: float,
        whale_imbalance: float,
        liq_regime: str,
        value_area_relation: str,
        rr_ratio: float = 2.0
    ) -> Dict[str, Any]:
        """
        Embeds candidate setup into semantic space and performs vector search against
        losing trades postmortem table. Returns adversarial critic evaluation.
        """
        dir_clean = direction.upper()
        narrative = (
            f"Candidate {dir_clean} trade on {symbol} at ${price:.4f}. "
            f"4H Macro HTF is {htf_bias}. Confluence score is {confluence_score:.1f}/100. "
            f"5-bar CVD slope is {cvd_slope:+.2f}. Whale imbalance is {whale_imbalance:+.2f}. "
            f"Liquidation state: {liq_regime}. Value Area relation: {value_area_relation}. "
            f"Reward-to-Risk ratio: {rr_ratio:.2f}:1."
        )

        try:
            query_vec = self.embed_text(narrative)
            # Search top 2 most similar historical failure modes
            df_matches = self.table.search(query_vec).metric('cosine').limit(2).to_pandas()

            if df_matches.empty:
                return {
                    "critic_verdict": "CLEAR",
                    "max_similarity": 0.0,
                    "matched_failure": None,
                    "penalty": 0.0,
                    "narrative": narrative
                }

            # Filter for same direction to assess exact analog
            matched_row = None
            max_sim = 0.0

            for _, row in df_matches.iterrows():
                # LanceDB search returns '_distance' (L2 distance or 1 - cosine)
                dist = float(row.get("_distance", 1.0))
                # For normalized vectors: cosine_sim = 1.0 - (dist^2)/2 or 1.0 - dist
                # FastEmbed vectors are unit normalized, so dist is Euclidean sqrt(2*(1-cos))
                cos_sim = max(0.0, min(1.0, 1.0 - dist))
                
                if row.get("direction", "").upper() == dir_clean:
                    if cos_sim > max_sim:
                        max_sim = cos_sim
                        matched_row = row

            if matched_row is None and not df_matches.empty:
                # Fallback to closest regardless of direction
                first = df_matches.iloc[0]
                dist = float(first.get("_distance", 1.0))
                max_sim = max(0.0, min(1.0, 1.0 - (dist * dist) / 2.0))
                matched_row = first

            # Causal Grounding Verification:
            # Prevents template semantic false-positives where surface phrasing is similar
            # but the actual physical/order-flow pathology of the failure mode is NOT present.
            is_causally_grounded = True
            unmet_reason = ""
            matched_mode = matched_row["failure_mode"] if matched_row is not None else ""

            if matched_mode == FAILURE_WHALE_WALL_TRAP:
                if dir_clean in ("BUY", "LONG") and whale_imbalance > -0.30:
                    is_causally_grounded = False
                    unmet_reason = f"Whale imbalance ({whale_imbalance:+.2f}) does not indicate adverse ask wall resistance"
                elif dir_clean in ("SELL", "SHORT") and whale_imbalance < 0.30:
                    is_causally_grounded = False
                    unmet_reason = f"Whale imbalance ({whale_imbalance:+.2f}) does not indicate adverse bid wall support"

            elif matched_mode == FAILURE_FALLING_KNIFE_LONG:
                if dir_clean in ("BUY", "LONG") and cvd_slope >= 0.0:
                    is_causally_grounded = False
                    unmet_reason = f"CVD slope ({cvd_slope:+.2f}) is positive/bullish, not dumping"

            elif matched_mode == FAILURE_PARABOLIC_SQUEEZE_SHORT:
                if dir_clean in ("SELL", "SHORT") and cvd_slope <= 0.0:
                    is_causally_grounded = False
                    unmet_reason = f"CVD slope ({cvd_slope:+.2f}) is negative/bearish, not pumping"

            elif matched_mode == FAILURE_LOW_RR_CHOP:
                if rr_ratio >= 2.0:
                    is_causally_grounded = False
                    unmet_reason = f"Planned R:R ({rr_ratio:.2f}:1) satisfies strict 2:1 mandate"

            elif matched_mode == FAILURE_UNEXHAUSTED_CASCADE:
                if liq_regime not in ("BEARISH_REVERSAL", "LONG_LIQ_DOMINANT") and cvd_slope >= 0.0:
                    is_causally_grounded = False
                    unmet_reason = "No active long liquidation cascade or dumping CVD flow"

            # Decision Gates
            if not is_causally_grounded:
                verdict = "CLEAR"
                penalty = 0.0
                reason = (
                    f"Candidate shares semantic phrasing with [{matched_mode}] ({max_sim*100:.1f}%), "
                    f"but actual causal risk is absent: {unmet_reason}. Cleared for execution."
                )
            elif max_sim >= 0.88:
                verdict = "HARD_VETO"
                penalty = 100.0
                reason = (
                    f"CRITIC_VETO: Candidate setup has extreme similarity ({max_sim*100:.1f}%) to recorded failure "
                    f"[{matched_row['failure_mode']}]: {matched_row['mitigation_guard']}"
                )
            elif max_sim >= 0.80:
                verdict = "HIGH_PENALTY"
                penalty = 25.0
                reason = (
                    f"CRITIC_PENALTY: Candidate setup has strong similarity ({max_sim*100:.1f}%) to past failure "
                    f"[{matched_row['failure_mode']}]. Applied -25.0 pt penalty."
                )
            elif max_sim >= 0.72:
                verdict = "ADVISORY"
                penalty = 10.0
                reason = (
                    f"CRITIC_ADVISORY: Moderate similarity ({max_sim*100:.1f}%) to past loss "
                    f"[{matched_row['failure_mode']}]. Applied -10.0 pt penalty."
                )
            else:
                verdict = "CLEAR"
                penalty = 0.0
                reason = f"Candidate setup shows low similarity ({max_sim*100:.1f}%) to historical failure modes."

            return {
                "critic_verdict": verdict,
                "max_similarity": round(max_sim, 4),
                "penalty": penalty,
                "reason": reason,
                "matched_failure_mode": matched_row["failure_mode"] if matched_row is not None else None,
                "matched_lesson": matched_row["mitigation_guard"] if matched_row is not None else None,
                "narrative": narrative
            }

        except Exception as e:
            logger.error(f"Error querying critic vector memory: {e}")
            return {
                "critic_verdict": "HARD_VETO",
                "max_similarity": 1.0,
                "penalty": 100.0,
                "reason": f"CRITIC_FAIL_CLOSED_VETO: Vector memory query error: {e}",
                "narrative": narrative,
                "matched_failure_mode": "CORRUPT_OR_UNAVAILABLE_MEMORY",
                "matched_lesson": "Fail-closed safety invariant: never execute when critic memory is unavailable."
            }

    # =========================================================================
    # Post-Mortem Forensic Reflection & Dynamic Memory Indexing
    # =========================================================================
    def conduct_post_mortem_and_store(
        self,
        ticket_id: str,
        symbol: str,
        direction: str,
        entry_price: float,
        exit_price: float,
        realized_pnl_usd: float,
        market_snapshot: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Forensic post-mortem engine called upon position close at a loss.
        Performs automated causal diagnosis, classifies failure mode, derives mitigation
        rule, embeds into LanceDB, and appends to persistent critic JSONL.
        """
        # Idempotency check:
        if hasattr(self, "table") and self.table is not None:
            try:
                df = self.table.to_pandas()
                if not df.empty and str(ticket_id) in df["ticket_id"].astype(str).values:
                    existing = df[df["ticket_id"].astype(str) == str(ticket_id)].iloc[0]
                    logger.info(f"Post-mortem for ticket {ticket_id} already exists in LanceDB (idempotent no-op).")
                    return {
                        "status": "ALREADY_RECORDED",
                        "failure_id": str(existing["failure_id"]),
                        "failure_mode": str(existing["failure_mode"]),
                        "verdict": str(existing["critic_verdict"]),
                        "mitigation_guard": str(existing["mitigation_guard"])
                    }
            except Exception as e:
                logger.warning(f"Idempotency search check failed: {e}")
        if realized_pnl_usd >= 0:
            logger.info(f"[{symbol}] Trade {ticket_id} closed in PROFIT (${realized_pnl_usd:+.2f}). Skipping post-mortem.")
            return {"status": "SKIPPED_PROFITABLE"}

        dir_clean = direction.upper()
        loss_usd = abs(realized_pnl_usd)
        logger.warning(f"🚨 [POST-MORTEM] Conducting forensic audit on losing trade {ticket_id} ({symbol} {dir_clean}, Loss: -${loss_usd:.2f})...")

        # Extract market snapshot features
        ema233 = float(market_snapshot.get("ema233", entry_price))
        cvd_slope = float(market_snapshot.get("cvd_slope", 0.0))
        htf_bias = str(market_snapshot.get("htf_bias", "NEUTRAL")).upper()
        whale_imb = float(market_snapshot.get("whale_imbalance", 0.0))
        has_liq = bool(market_snapshot.get("has_liq_spike", False))
        liq_bias = str(market_snapshot.get("liq_bias", "NEUTRAL"))
        va_rel = str(market_snapshot.get("value_area_relation", "INSIDE_VA"))

        # Automated Failure Mode Classifier
        pct_ema = ((entry_price - ema233) / (ema233 + 1e-9)) * 100 if ema233 > 0 else 0.0

        if dir_clean in ("BUY", "LONG") and pct_ema < -2.0 and cvd_slope < -0.2:
            failure_mode = FAILURE_FALLING_KNIFE_LONG
            verdict = (
                f"Long trade entered while price was {pct_ema:.1f}% below EMA233 with heavy negative CVD slope ({cvd_slope:+.2f}). "
                f"Market sell orders overwhelmed bid support."
            )
            mitigation = f"Never buy when price is >2% below EMA233 with negative CVD velocity without verified footprint absorption."
        elif dir_clean in ("SELL", "SHORT") and pct_ema > 2.0 and cvd_slope > 0.2:
            failure_mode = FAILURE_PARABOLIC_SQUEEZE_SHORT
            verdict = (
                f"Short trade entered while price was {pct_ema:.1f}% above EMA233 with strong positive CVD slope ({cvd_slope:+.2f}). "
                f"Forced liquidation buy-ins squeezed the position."
            )
            mitigation = f"Never short when price is >2% above EMA233 with positive CVD velocity without verified exhaustion wick."
        elif dir_clean in ("BUY", "LONG") and whale_imb < -0.40:
            failure_mode = FAILURE_WHALE_WALL_TRAP
            verdict = f"Bought directly into overwhelming Whale Ask wall (Imbalance: {whale_imb:+.2f}) near {va_rel}."
            mitigation = f"Enforce Whale book clearance check: avoid buying when whale ask imbalance exceeds 60%."
        elif dir_clean in ("SELL", "SHORT") and whale_imb > 0.40:
            failure_mode = FAILURE_WHALE_WALL_TRAP
            verdict = f"Sold directly into massive Whale Bid wall (Imbalance: {whale_imb:+.2f}) near {va_rel}."
            mitigation = f"Enforce Whale book clearance check: avoid selling when whale bid imbalance exceeds 60%."
        elif has_liq:
            failure_mode = FAILURE_UNEXHAUSTED_CASCADE
            verdict = f"Attempted counter-trend reversal during ongoing liquidation wave ({liq_bias}) before flush completion."
            mitigation = f"Wait for closed-bar confirmation and footprint delta reversal before fading a liquidation cascade."
        elif htf_bias != "NEUTRAL" and ((dir_clean in ("BUY", "LONG") and "BEAR" in htf_bias) or (dir_clean in ("SELL", "SHORT") and "BULL" in htf_bias)):
            failure_mode = FAILURE_HTF_MOMENTUM_RUNOVER
            verdict = f"Counter-trend trade clashed with higher timeframe momentum ({htf_bias}). Macro trend resumed and hit SL."
            mitigation = f"Require higher confluence score threshold (>= 85) for any setup fighting 4H HTF directional bias."
        else:
            failure_mode = FAILURE_LOW_RR_CHOP
            verdict = f"Trade chopped out by random market volatility in consolidation."
            mitigation = f"Ensure stop loss is anchored behind structural market profile levels, not arbitrary ATR fractions."

        narrative = (
            f"Losing {dir_clean} trade on {symbol} (Ticket: {ticket_id}). Entry: ${entry_price:.4f}, Exit: ${exit_price:.4f}, Loss: -${loss_usd:.2f}. "
            f"4H HTF: {htf_bias}, Price vs EMA233: {pct_ema:.2f}%, CVD slope: {cvd_slope:+.2f}, Whale Imbalance: {whale_imb:+.2f}, "
            f"Liquidation State: {liq_bias}. Failure mode: {failure_mode}. Diagnosis: {verdict}"
        )

        now_str = datetime.now(timezone.utc).isoformat()
        failure_id = f"LOSS_{symbol}_{int(time.time())}"

        # Generate embedding
        vector = self.embed_text(narrative)

        record = {
            "vector": vector,
            "failure_id": failure_id,
            "ticket_id": str(ticket_id),
            "symbol": symbol,
            "direction": dir_clean,
            "failure_mode": failure_mode,
            "narrative": narrative,
            "critic_verdict": verdict,
            "mitigation_guard": mitigation,
            "cvd_slope": float(cvd_slope),
            "price_vs_ema233": float(pct_ema),
            "whale_imbalance": float(whale_imb),
            "liq_intensity": 80.0 if has_liq else 20.0,
            "realized_loss_usd": float(loss_usd),
            "timestamp": now_str
        }

        # Store in LanceDB
        try:
            self.table.add([record])
            logger.info(f"Indexed post-mortem failure in LanceDB: {failure_id} ({failure_mode})")
        except Exception as e:
            logger.error(f"Failed to index post-mortem in LanceDB: {e}")

        # Append to critic JSONL
        clean_rec = {k: v for k, v in record.items() if k != "vector"}
        try:
            with open(CRITIC_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(clean_rec) + "\n")
        except Exception as e:
            logger.error(f"Failed to append to {CRITIC_JSONL}: {e}")

        # Update critic learning summary state
        self._update_learning_state(clean_rec)

        return {
            "status": "RECORDED",
            "failure_id": failure_id,
            "failure_mode": failure_mode,
            "verdict": verdict,
            "mitigation_guard": mitigation
        }

    def _update_learning_state(self, record: Dict[str, Any]):
        """Persists learning state and failure mode distribution to JSON."""
        state_file = DB_DIR / "critic_learning_state.json"
        state = {
            "total_losses_audited": 0,
            "failure_mode_counts": {},
            "active_mitigations": [],
            "recent_failures": [],
            "last_audit_utc": None
        }
        if state_file.exists():
            try:
                state = json.loads(state_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        state["total_losses_audited"] = state.get("total_losses_audited", 0) + 1
        fm = record.get("failure_mode", "UNKNOWN")
        counts = state.get("failure_mode_counts", {})
        counts[fm] = counts.get(fm, 0) + 1
        state["failure_mode_counts"] = counts

        mitigation = record.get("mitigation_guard")
        if mitigation and mitigation not in state.get("active_mitigations", []):
            state.setdefault("active_mitigations", []).append(mitigation)

        recent = state.get("recent_failures", [])
        recent.insert(0, {
            "failure_id": record.get("failure_id"),
            "symbol": record.get("symbol"),
            "direction": record.get("direction"),
            "failure_mode": fm,
            "loss_usd": record.get("realized_loss_usd"),
            "timestamp": record.get("timestamp")
        })
        state["recent_failures"] = recent[:10]
        state["last_audit_utc"] = record.get("timestamp")

        try:
            from Terminal.runtime_io import atomic_json
            atomic_json(state_file, state)
        except Exception:
            state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def get_learning_summary(self) -> Dict[str, Any]:
        """Returns the current learning and memory profile of the Trade Critic."""
        state_file = DB_DIR / "critic_learning_state.json"
        if state_file.exists():
            try:
                return json.loads(state_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "total_losses_audited": 0,
            "total_vector_records": len(self.table) if hasattr(self, 'table') else 0,
            "failure_mode_counts": {},
            "active_mitigations": [s["mitigation_guard"] for s in CANONICAL_SEEDS]
        }
