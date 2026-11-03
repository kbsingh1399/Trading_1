"""
Terminal/test_trade_critic_lifecycle.py
Tests the complete lifecycle of the Trade Critic & Anti-Falling Knife Engine:
1. Deterministic falling knife & squeeze microstructural vetoes
2. Quantitative Knife Hazard Index (KHI) enforcement
3. Vector similarity search against LanceDB failure memories
4. Forensic post-mortem execution, causal diagnosis, mitigation rule derivation
5. Immediate learning retention: candidate matching the newly recorded loss is vetoed
"""

import time
import pytest
from pathlib import Path
from Terminal.trade_critic_engine import (
    TradeCriticEngine,
    FAILURE_FALLING_KNIFE_LONG,
    FAILURE_PARABOLIC_SQUEEZE_SHORT,
    FAILURE_LOW_RR_CHOP
)

@pytest.fixture(scope="module")
def critic():
    return TradeCriticEngine()

def test_knife_hazard_index_veto(critic):
    # Setup with price below EMA233, dumping CVD, long liquidation spike, bearish HTF
    vetoed, reason, audit = critic.check_microstructure_knife_veto(
        symbol="BTC",
        direction="BUY",
        price=60000.0,
        ema233=65000.0,
        htf_bias="BEARISH",
        cvd_slope=-0.50,
        has_liq_spike=True,
        liq_bias="BEARISH_REVERSAL",
        has_footprint_absorption=False,
        rr_ratio=2.5
    )
    assert vetoed is True
    assert "KNIFE" in reason or "CASCADE" in reason
    assert audit.get("knife_hazard_index", 0.0) >= 50.0

def test_low_rr_chop_veto(critic):
    # Low R:R (< 2.0) must be rejected instantly
    vetoed, reason, audit = critic.check_microstructure_knife_veto(
        symbol="ETH",
        direction="BUY",
        price=3000.0,
        ema233=2900.0,
        htf_bias="BULLISH",
        cvd_slope=0.10,
        has_liq_spike=False,
        liq_bias="NEUTRAL",
        has_footprint_absorption=True,
        rr_ratio=1.5  # Sub-mandate R:R
    )
    assert vetoed is True
    assert "VETO_LOW_RR_CHOP" in reason

def test_clean_setup_clears_knife_defense(critic):
    # Clean pullback to EMA233 with bullish CVD and absorption
    vetoed, reason, audit = critic.check_microstructure_knife_veto(
        symbol="SOL",
        direction="BUY",
        price=150.0,
        ema233=148.0,
        htf_bias="BULLISH",
        cvd_slope=0.20,
        has_liq_spike=False,
        liq_bias="NEUTRAL",
        has_footprint_absorption=True,
        rr_ratio=2.5
    )
    assert vetoed is False
    assert reason == "CLEARED_MICROSTRUCTURE_DEFENSE"

def test_post_mortem_learning_and_retention(critic):
    # Conduct post-mortem on a synthetic losing trade
    ticket_id = f"TEST_MORTEM_{int(time.time())}"
    sym = "AVAX"
    res = critic.conduct_post_mortem_and_store(
        ticket_id=ticket_id,
        symbol=sym,
        direction="BUY",
        entry_price=25.0,
        exit_price=24.0,
        realized_pnl_usd=-25.0,
        market_snapshot={
            "ema233": 28.0,
            "cvd_slope": -0.60,
            "htf_bias": "BEARISH",
            "whale_imbalance": -0.40,
            "has_liq_spike": True,
            "liq_bias": "LONG_LIQ_DOMINANT",
            "value_area_relation": "BELOW_VAL"
        }
    )
    assert res["status"] == "RECORDED"
    assert res["failure_mode"] == FAILURE_FALLING_KNIFE_LONG
    assert "mitigation_guard" in res

    # Verify that a subsequent candidate setup matching this profile triggers Critic Veto/Penalty
    audit = critic.audit_candidate_setup(
        symbol=sym,
        direction="BUY",
        price=25.0,
        confluence_score=78.0,
        htf_bias="BEARISH",
        cvd_slope=-0.60,
        whale_imbalance=-0.40,
        liq_regime="LONG_LIQ_DOMINANT",
        value_area_relation="BELOW_VAL",
        rr_ratio=2.1
    )
    assert audit["critic_verdict"] in ("HARD_VETO", "HIGH_PENALTY")
    assert audit["max_similarity"] >= 0.80
    assert audit["matched_failure_mode"] is not None

def test_critic_learning_summary(critic):
    summary = critic.get_learning_summary()
    assert summary["total_losses_audited"] >= 1
    assert "failure_mode_counts" in summary
    assert FAILURE_FALLING_KNIFE_LONG in summary["failure_mode_counts"]
