import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass
"""
Quant Agent Strategy Arena (LMSYS Bradley-Terry Elo Rating Engine)
========================================================================================
Inspired by LMSYS Chatbot Arena (https://arena.ai/blog/arena) and FastChat (https://github.com/lm-sys/FastChat).
Applies the Bradley-Terry Maximum Likelihood Estimation (MLE) with Bootstrap Confidence
Intervals to pit quantitative agent strategies in head-to-head tournament battles
across crypto market microstructure data.
========================================================================================
"""

import math
import json
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import numpy as np
import pandas as pd
from scipy.optimize import minimize

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [ARENA] %(message)s")
logger = logging.getLogger("QuantArena")

WORKSPACE_DIR = Path(__file__).resolve().parent
QUEUES_DIR = WORKSPACE_DIR / "Database" / "Signals" / "Signal_Queues"
CANDLES_DIR = WORKSPACE_DIR / "Database" / "Candle_Data"


# ========================================================================================
# 1. Candidate Quantitative Agent Strategies
# ========================================================================================

class BaseStrategyAgent:
    def __init__(self, name: str):
        self.name = name

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        """Returns dict with 'decision' (LONG, SHORT, PASS), 'conf_score', 'sl_dist', 'tp_dist'."""
        raise NotImplementedError


class RDNLStrategyAgent(BaseStrategyAgent):
    """Agent 1: Regime-Dependent Non-Linear Confluence Scoring with Pillar Synergy."""
    def __init__(self):
        super().__init__("Agent-RDNL-CS (Non-Linear Synergy)")

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        direction = signal.get("direction", "NEUTRAL")
        if direction not in ("LONG", "SHORT"):
            return None

        # Normalized weights
        w1 = context.get("w1_footprint", 15.0) / 30.0
        w2 = context.get("w2_auction", 10.0) / 20.0
        w3 = context.get("w3_whales", 10.0) / 20.0
        w4 = context.get("w4_magnets", 7.0) / 15.0
        w5 = context.get("w5_tv_liq", 7.0) / 15.0

        regime = context.get("regime", "NORMAL")
        # Non-linear synergy between W1 and W3, and W1 and W2
        synergy = 0.0
        if w1 >= 0.5 and w3 >= 0.5:
            synergy += 0.22 * math.sqrt(w1 * w3)
        if w1 >= 0.5 and w2 >= 0.5:
            synergy += 0.15 * math.sqrt(w1 * w2)

        base_score = (w1 * 0.30 + w2 * 0.20 + w3 * 0.20 + w4 * 0.15 + w5 * 0.15) * 100.0
        final_score = min(100.0, base_score * (1.0 + synergy))

        if final_score >= 75.0:
            return {"decision": direction, "conf_score": final_score, "rr_target": 3.0}
        return {"decision": "PASS", "conf_score": final_score, "rr_target": 0.0}


class LinearAdditiveAgent(BaseStrategyAgent):
    """Legacy 5-Pillar Linear Additive Scoring."""
    def __init__(self):
        super().__init__("Agent-Linear-5Pillar (Legacy)")

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        direction = signal.get("direction", "NEUTRAL")
        if direction not in ("LONG", "SHORT"):
            return None

        w1 = context.get("w1_footprint", 15.0)
        w2 = context.get("w2_auction", 10.0)
        w3 = context.get("w3_whales", 10.0)
        w4 = context.get("w4_magnets", 7.0)
        w5 = context.get("w5_tv_liq", 7.0)

        total_score = min(w1, 30.0) + min(w2, 20.0) + min(w3, 20.0) + min(w4, 15.0) + min(w5, 15.0)
        if total_score >= 75.0:
            return {"decision": direction, "conf_score": total_score, "rr_target": 2.0}
        return {"decision": "PASS", "conf_score": total_score, "rr_target": 0.0}


class WhaleDominantAgent(BaseStrategyAgent):
    """Whale Blotter Defense Heavy Agent (Requires large wall protection)."""
    def __init__(self):
        super().__init__("Agent-Whale-Dominant")

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        direction = signal.get("direction", "NEUTRAL")
        if direction not in ("LONG", "SHORT"):
            return None

        w3 = context.get("w3_whales", 0.0)
        wall_usd = context.get("defending_wall_usd", 0.0)
        if wall_usd >= 1_000_000 and w3 >= 14.0:
            return {"decision": direction, "conf_score": 82.0, "rr_target": 2.5}
        return {"decision": "PASS", "conf_score": 40.0, "rr_target": 0.0}


class FootprintPureTrapAgent(BaseStrategyAgent):
    """Order Flow Trap & Absorption Pure Specialist."""
    def __init__(self):
        super().__init__("Agent-Footprint-Pure")

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        sig_code = signal.get("sig_code", "")
        direction = signal.get("direction", "NEUTRAL")
        if sig_code in ("trap", "abs"):
            return {"decision": direction, "conf_score": 85.0, "rr_target": 2.8}
        return {"decision": "PASS", "conf_score": 30.0, "rr_target": 0.0}


class LiquidationSweepAgent(BaseStrategyAgent):
    """Liquidation Cascade & Magnet Harvester."""
    def __init__(self):
        super().__init__("Agent-Liq-Magnet")

    def evaluate(self, signal: dict, context: dict) -> Optional[dict]:
        direction = signal.get("direction", "NEUTRAL")
        w4 = context.get("w4_magnets", 0.0)
        w5 = context.get("w5_tv_liq", 0.0)
        if w4 >= 11.0 or w5 >= 12.0:
            return {"decision": direction, "conf_score": 78.0, "rr_target": 2.2}
        return {"decision": "PASS", "conf_score": 45.0, "rr_target": 0.0}


# ========================================================================================
# 2. Bradley-Terry MLE & Elo Computation (Direct LMSYS FastChat Implementation)
# ========================================================================================

def compute_bt_elo(
    battles_df: pd.DataFrame,
    base: float = 10.0,
    scale: float = 400.0,
    init_rating: float = 1000.0,
    tol: float = 1e-6
) -> pd.Series:
    """
    Computes Bradley-Terry Maximum Likelihood Estimation ratings.
    Matches FastChat rating_systems.py:compute_bt().
    """
    models = sorted(list(set(battles_df["model_a"]).union(set(battles_df["model_b"]))))
    model_to_idx = {m: i for i, m in enumerate(models)}
    n_models = len(models)

    # Encode matchups and outcomes
    m_a = battles_df["model_a"].map(model_to_idx).values
    m_b = battles_df["model_b"].map(model_to_idx).values
    
    outcomes = np.full(len(battles_df), 0.5)
    outcomes[battles_df["winner"] == "model_a"] = 1.0
    outcomes[battles_df["winner"] == "model_b"] = 0.0

    def bt_loss(ratings):
        # ratings: log-scale parameters (gamma_i = exp(r_i))
        r_a = ratings[m_a]
        r_b = ratings[m_b]
        # P(A wins) = 1 / (1 + exp(r_b - r_a)) = sigmoid(r_a - r_b)
        diff = r_a - r_b
        # Numerical stability via softplus: log(1 + exp(x))
        # log-likelihood = y * diff - log(1 + exp(diff))
        loss = -(outcomes * diff - np.logaddexp(0.0, diff)).sum()
        # L2 Regularization centered at 0
        loss += 0.5 * (ratings ** 2).sum()
        return loss

    init_r = np.zeros(n_models)
    res = minimize(bt_loss, init_r, method="BFGS", tol=tol)
    final_r = res.x

    # Scale to standard Elo range (mean 1000, scale 400)
    # in standard Elo: log_10 base -> r_scaled = r * (400 / ln(10))
    scale_factor = scale / math.log(base)
    scaled_ratings = final_r * scale_factor
    scaled_ratings = scaled_ratings - np.mean(scaled_ratings) + init_rating

    return pd.Series(scaled_ratings, index=models).sort_values(ascending=False)


def compute_bootstrap_elo(
    battles_df: pd.DataFrame,
    num_rounds: int = 200
) -> pd.DataFrame:
    """Computes bootstrap distributions and 95% Confidence Intervals across tournament rounds."""
    records = []
    n_rows = len(battles_df)
    for _ in range(num_rounds):
        sample_df = battles_df.sample(n=n_rows, replace=True)
        ratings = compute_bt_elo(sample_df)
        records.append(ratings)
    
    boot_df = pd.DataFrame(records)
    summary = pd.DataFrame({
        "Elo Rating": boot_df.median(),
        "95% CI Lower": boot_df.quantile(0.025),
        "95% CI Upper": boot_df.quantile(0.975),
        "Std Dev": boot_df.std(),
    }).sort_values(by="Elo Rating", ascending=False)
    return summary


# ========================================================================================
# 3. Tournament Simulator: Pitting Agents Against Market Microstructure
# ========================================================================================

def run_strategy_tournament(n_rounds: int = 150) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Runs head-to-head battles between all strategy agents against randomized market conditions.
    """
    agents = [
        RDNLStrategyAgent(),
        LinearAdditiveAgent(),
        WhaleDominantAgent(),
        FootprintPureTrapAgent(),
        LiquidationSweepAgent(),
    ]

    battles = []
    rng = np.random.default_rng(42)

    logger.info(f"Initiating Strategy Arena: {len(agents)} agents, {n_rounds} tournament rounds...")

    for i in range(n_rounds):
        # Generate realistic microstructure signal snapshot
        is_trap = rng.random() < 0.25
        sig_code = "trap" if is_trap else rng.choice(["abs", "ratio_p", "div", "sdiv", "p_spike"])
        true_direction = rng.choice(["LONG", "SHORT"])

        # Market regime
        regime = rng.choice(["COMPRESSION", "EXPANSION", "WALL_DOMINATED", "NORMAL"])
        has_whale_wall = rng.random() < 0.40
        has_liq_spike = rng.random() < 0.35

        signal = {
            "sig_code": sig_code,
            "direction": true_direction,
            "time_ms": int(1700000000000 + i * 900000),
        }

        context = {
            "w1_footprint": 24.0 if sig_code in ("trap", "abs") else 14.0,
            "w2_auction": 16.0 if regime == "COMPRESSION" else 8.0,
            "w3_whales": 18.0 if has_whale_wall else 4.0,
            "w4_magnets": 12.0 if has_liq_spike else 4.0,
            "w5_tv_liq": 13.0 if has_liq_spike else 3.0,
            "defending_wall_usd": 2_500_000 if has_whale_wall else 150_000,
            "regime": regime,
        }

        # Market Ground Truth: Did price move in true_direction favorably?
        # Non-linear coupling: trap + wall has 85% win rate; trap alone 60%; noise 45%
        win_prob = 0.50
        if sig_code in ("trap", "abs") and has_whale_wall:
            win_prob = 0.88
        elif sig_code in ("trap", "abs"):
            win_prob = 0.68
        elif has_liq_spike and regime == "EXPANSION":
            win_prob = 0.75
        elif regime == "COMPRESSION" and not has_whale_wall:
            win_prob = 0.40

        market_outcome = true_direction if (rng.random() < win_prob) else ("SHORT" if true_direction == "LONG" else "LONG")

        # Evaluate all agents
        agent_decisions = {}
        for ag in agents:
            res = ag.evaluate(signal, context)
            agent_decisions[ag.name] = res

        # Pairwise battles between all agent pairs
        for a_idx in range(len(agents)):
            for b_idx in range(a_idx + 1, len(agents)):
                ag_a = agents[a_idx].name
                ag_b = agents[b_idx].name
                dec_a = agent_decisions[ag_a]
                dec_b = agent_decisions[ag_b]

                score_a = 0.0
                score_b = 0.0

                if dec_a and dec_a["decision"] == market_outcome:
                    score_a = dec_a["rr_target"]
                elif dec_a and dec_a["decision"] != "PASS":
                    score_a = -1.0  # Stop loss hit

                if dec_b and dec_b["decision"] == market_outcome:
                    score_b = dec_b["rr_target"]
                elif dec_b and dec_b["decision"] != "PASS":
                    score_b = -1.0

                if score_a > score_b:
                    winner = "model_a"
                elif score_b > score_a:
                    winner = "model_b"
                else:
                    winner = "tie"

                battles.append({
                    "model_a": ag_a,
                    "model_b": ag_b,
                    "winner": winner,
                    "round": i,
                })

    battles_df = pd.DataFrame(battles)
    leaderboard = compute_bootstrap_elo(battles_df, num_rounds=200)

    # Compute pairwise win fractions
    models = leaderboard.index.tolist()
    matrix = pd.DataFrame(index=models, columns=models, dtype=float)
    for m1 in models:
        for m2 in models:
            if m1 == m2:
                matrix.loc[m1, m2] = 0.50
            else:
                m1_matches = battles_df[((battles_df["model_a"] == m1) & (battles_df["model_b"] == m2)) |
                                        ((battles_df["model_a"] == m2) & (battles_df["model_b"] == m1))]
                if len(m1_matches) == 0:
                    matrix.loc[m1, m2] = 0.50
                else:
                    m1_wins = len(m1_matches[(m1_matches["model_a"] == m1) & (m1_matches["winner"] == "model_a")]) + \
                              len(m1_matches[(m1_matches["model_b"] == m1) & (m1_matches["winner"] == "model_b")])
                    ties = len(m1_matches[m1_matches["winner"] == "tie"])
                    win_frac = (m1_wins + 0.5 * ties) / len(m1_matches)
                    matrix.loc[m1, m2] = round(win_frac, 3)

    return leaderboard, matrix


if __name__ == "__main__":
    leaderboard, win_matrix = run_strategy_tournament(n_rounds=250)

    print("\n" + "=" * 80)
    print("🏆 LMSYS CHATBOT ARENA — QUANT AGENT STRATEGY LEADERBOARD")
    print("=" * 80)
    print(leaderboard.to_string())
    print("\n" + "=" * 80)
    print("📊 PAIRWISE WIN FRACTION MATRIX (Row beats Column)")
    print("=" * 80)
    print(win_matrix.to_string())
    print("=" * 80 + "\n")
