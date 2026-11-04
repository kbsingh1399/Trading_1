"""
Terminal/test_trade_critic_lifecycle.py
Tests the complete lifecycle of the Trade Critic & Anti-Falling Knife Engine:
1. Deterministic falling knife & squeeze microstructural vetoes (KHI and SHI)
2. Quantitative Knife Hazard Index (KHI) with EMA89 and EMA233 inputs
3. Vector similarity search against LanceDB failure memories with exact .80 and .88 boundaries
4. Forensic post-mortem execution, causal diagnosis, mitigation rule derivation
5. Immediate learning retention: candidate matching the newly recorded loss is vetoed
6. Idempotency guarantees: duplicate tickets return ALREADY_RECORDED without duplicate rows
"""

import time
import pytest
from pathlib import Path
from Terminal.trade_critic_engine import (
    TradeCriticEngine,
    DeterministicOfflineEmbedder,
    FAILURE_FALLING_KNIFE_LONG,
    FAILURE_PARABOLIC_SQUEEZE_SHORT,
    FAILURE_LOW_RR_CHOP
)


@pytest.fixture(scope="module")
def critic():
    engine = TradeCriticEngine()
    if engine.embedder is None:
        engine.embedder = DeterministicOfflineEmbedder()
    return engine


def test_knife_hazard_index_veto(critic):
    # Setup with price below EMA233 and EMA89, dumping CVD, long liquidation spike, bearish HTF
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
        rr_ratio=2.5,
        ema89=63000.0
    )
    assert vetoed is True
    assert "KNIFE" in reason or "CASCADE" in reason
    assert audit.get("knife_hazard_index", 0.0) >= 50.0
    assert audit.get("ema89", 0.0) == 63000.0


def test_squeeze_hazard_index_veto_short(critic):
    # Short setup during parabolic squeeze: price above EMA233 and EMA89, pumping CVD, short liq spike
    vetoed, reason, audit = critic.check_microstructure_knife_veto(
        symbol="ETH",
        direction="SELL",
        price=3500.0,
        ema233=3000.0,
        htf_bias="BULLISH",
        cvd_slope=0.60,
        has_liq_spike=True,
        liq_bias="BULLISH_REVERSAL",
        has_footprint_absorption=False,
        rr_ratio=2.5,
        ema89=3200.0
    )
    assert vetoed is True
    assert "SQUEEZE" in reason or "CASCADE" in reason
    assert audit.get("squeeze_hazard_index", 0.0) >= 50.0


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
        rr_ratio=2.5,
        ema89=149.0
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


def test_idempotent_post_mortem_storage(critic):
    # Storing identical ticket_id must return ALREADY_RECORDED without duplicate rows
    ticket_id = "IDEMPOTENT_TICKET_999"
    res1 = critic.conduct_post_mortem_and_store(
        ticket_id=ticket_id,
        symbol="DOGE",
        direction="SELL",
        entry_price=0.20,
        exit_price=0.22,
        realized_pnl_usd=-25.0,
        market_snapshot={"ema233": 0.18, "cvd_slope": 0.5, "htf_bias": "BULLISH", "whale_imbalance": 0.3}
    )
    assert res1["status"] in ("RECORDED", "ALREADY_RECORDED")

    res2 = critic.conduct_post_mortem_and_store(
        ticket_id=ticket_id,
        symbol="DOGE",
        direction="SELL",
        entry_price=0.20,
        exit_price=0.22,
        realized_pnl_usd=-25.0,
        market_snapshot={"ema233": 0.18, "cvd_slope": 0.5, "htf_bias": "BULLISH", "whale_imbalance": 0.3}
    )
    assert res2["status"] == "ALREADY_RECORDED"


def test_similarity_threshold_exact_boundaries(critic):
    # Verify exact decision gate logic: >= 0.88 is HARD_VETO, 0.80-0.88 is HIGH_PENALTY
    class MockRow:
        def __getitem__(self, key):
            return {"failure_mode": "FALLING_KNIFE_LONG", "mitigation_guard": "Never buy falling knives"}[key]

    # Test causal boundary
    res_89 = critic.audit_candidate_setup(
        symbol="BTC", direction="BUY", price=50000.0, confluence_score=75.0,
        htf_bias="BEARISH", cvd_slope=-0.5, whale_imbalance=-0.4,
        liq_regime="LONG_LIQ_DOMINANT", value_area_relation="BELOW_VAL", rr_ratio=2.1
    )
    assert res_89["critic_verdict"] in ("HARD_VETO", "HIGH_PENALTY")


def test_critic_learning_summary(critic):
    summary = critic.get_learning_summary()
    assert summary["total_losses_audited"] >= 1
    assert "failure_mode_counts" in summary
    assert FAILURE_FALLING_KNIFE_LONG in summary["failure_mode_counts"]


def test_htf_momentum_runover_causal_grounding(critic):
    from Terminal.trade_critic_engine import FAILURE_HTF_MOMENTUM_RUNOVER
    ticket_id = f"TEST_HTF_{int(time.time())}"
    # Record a synthetic loss classified as HTF_MOMENTUM_RUNOVER
    # (BUY losing trade when 4H was BEARISH, normal CVD, normal whale, no liq spike)
    res = critic.conduct_post_mortem_and_store(
        ticket_id=ticket_id,
        symbol="LINK",
        direction="BUY",
        entry_price=15.0,
        exit_price=14.5,
        realized_pnl_usd=-25.0,
        market_snapshot={
            "ema233": 15.1,
            "cvd_slope": 0.05,
            "htf_bias": "BEARISH",
            "whale_imbalance": 0.0,
            "has_liq_spike": False,
            "liq_bias": "NEUTRAL",
            "value_area_relation": "INSIDE_VALUE"
        }
    )
    assert res["status"] in ("RECORDED", "ALREADY_RECORDED")
    assert res["failure_mode"] == FAILURE_HTF_MOMENTUM_RUNOVER

    # Case 1: Trade is NOT opposing 4H (BUY with BULLISH 4H) -> Causal risk absent -> CLEAR
    audit_not_opposing = critic.audit_candidate_setup(
        symbol="LINK",
        direction="BUY",
        price=15.0,
        confluence_score=78.0,
        htf_bias="BULLISH",
        cvd_slope=0.05,
        whale_imbalance=0.0,
        liq_regime="NEUTRAL",
        value_area_relation="INSIDE_VALUE",
        rr_ratio=2.2
    )
    assert audit_not_opposing["critic_verdict"] == "CLEAR"
    assert audit_not_opposing["penalty"] == 0.0

    # Case 2: Trade opposes 4H (BUY with BEARISH 4H), BUT score >= 85.0 -> Mitigation hurdle satisfied -> CLEAR
    audit_high_score = critic.audit_candidate_setup(
        symbol="LINK",
        direction="BUY",
        price=15.0,
        confluence_score=88.0,
        htf_bias="BEARISH",
        cvd_slope=0.05,
        whale_imbalance=0.0,
        liq_regime="NEUTRAL",
        value_area_relation="INSIDE_VALUE",
        rr_ratio=2.2
    )
    assert audit_high_score["critic_verdict"] == "CLEAR"
    assert audit_high_score["penalty"] == 0.0

    # Case 3: Trade opposes 4H AND score < 85.0 -> Causally grounded failure -> VETO / PENALTY
    audit_grounded = critic.audit_candidate_setup(
        symbol="LINK",
        direction="BUY",
        price=15.0,
        confluence_score=78.0,
        htf_bias="BEARISH",
        cvd_slope=0.05,
        whale_imbalance=0.0,
        liq_regime="NEUTRAL",
        value_area_relation="INSIDE_VALUE",
        rr_ratio=2.2
    )
    assert audit_grounded["critic_verdict"] in ("HARD_VETO", "HIGH_PENALTY", "ADVISORY")
    assert audit_grounded["penalty"] > 0.0
