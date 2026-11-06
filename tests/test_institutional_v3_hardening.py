import pytest
from multi_agent.council import (
    AgentCouncil,
    Ballot,
    BaseCouncilAgent,
    OrderExecutionAgent,
    RiskGovernor,
    Vote,
    compute_council_jsd,
)
from trade_evaluator import (
    CushionKellySizer,
    TrailingState,
    TrailingStopManager,
    compute_chance_corrected_concordance,
)


# =========================================================================
# 1. Test CushionKellySizer & Cap Dominance
# =========================================================================
def test_cushion_kelly_stand_down_floor():
    res = CushionKellySizer.calculate(
        symbol="ETHUSD",
        entry=2500.0,
        sl=2450.0,
        confluence_score=85.0,
        eff_rr=2.5,
        account_cushion=15.00  # Below 16.48 stand-down floor
    )
    assert not res["viable"]
    assert "STAND_DOWN_CUSHION" in res["reason"]


def test_cushion_kelly_unshrinkable_min_lot_veto():
    # BTC with 4,000 pt stop distance: min lot 0.01 * 1.0 * 4000 = $40.00
    res = CushionKellySizer.calculate(
        symbol="BTCUSD",
        entry=70000.0,
        sl=66000.0,
        confluence_score=85.0,
        eff_rr=2.5,
        account_cushion=79.37
    )
    assert not res["viable"]
    assert "MIN_LOT_RISK" in res["reason"]
    assert res["risk_usd"] == 40.0


def test_cushion_kelly_viable_eth_trade():
    # ETH with 25 pt stop distance: min lot 0.01 * 1.0 * 25 = $0.25 <= $15.00 cap
    res = CushionKellySizer.calculate(
        symbol="ETHUSD",
        entry=2500.0,
        sl=2475.0,
        confluence_score=85.0,
        eff_rr=2.5,
        account_cushion=79.37
    )
    assert res["viable"]
    assert 10.0 <= res["risk_usd"] <= 15.0 or res["risk_usd"] <= 15.0
    assert res["lots"] >= 0.01


# =========================================================================
# 2. Test Chance-Corrected Concordance (kappa_c)
# =========================================================================
def test_concordance_kappa():
    # 50/50 split -> kappa = 0.0
    queue_split = [
        {"direction": "LONG"},
        {"direction": "LONG"},
        {"direction": "SHORT"},
        {"direction": "SHORT"},
    ]
    kappa, po, pe = compute_chance_corrected_concordance(queue_split, "LONG")
    assert kappa == 0.0

    # Unanimous LONG -> kappa = 1.0
    queue_unanimous = [
        {"direction": "LONG"},
        {"direction": "LONG"},
        {"direction": "LONG"},
        {"direction": "LONG"},
    ]
    kappa, po, pe = compute_chance_corrected_concordance(queue_unanimous, "LONG")
    assert kappa == 1.0
    assert po == 1.0


# =========================================================================
# 3. Test TrailingStopManager Monotonic Ratchet & Chandelier Anchor
# =========================================================================
def test_trailing_stop_monotonic_long():
    mgr = TrailingStopManager()
    state = TrailingState(
        entry=2000.0,
        sl_current=1950.0,
        vwap_sigma1=2030.0,
        vwap_sigma2=2060.0,
        poc=2000.0,
        atr_15m=20.0,
        direction="LONG"
    )

    # Price moves to 2080 (High Water Mark = 2080)
    # Chandelier ratchet: 2080 - 2.5 * 20 = 2030.0
    s1 = mgr.update(state, price=2080.0)
    assert s1.sl_current >= 2030.0

    # Price retraces to 2040: SL must NEVER relax below previous SL
    prev_sl = s1.sl_current
    s2 = mgr.update(s1, price=2040.0)
    assert s2.sl_current >= prev_sl


def test_trailing_stop_monotonic_short():
    mgr = TrailingStopManager()
    state = TrailingState(
        entry=2000.0,
        sl_current=2050.0,
        vwap_sigma1=1970.0,
        vwap_sigma2=1940.0,
        poc=2000.0,
        atr_15m=20.0,
        direction="SHORT"
    )

    # Price drops to 1920 (Low Water Mark = 1920)
    # Chandelier ratchet: 1920 + 2.5 * 20 = 1970.0
    s1 = mgr.update(state, price=1920.0)
    assert s1.sl_current <= 1970.0

    # Price bounces to 1960: SL must NEVER increase above previous SL
    prev_sl = s1.sl_current
    s2 = mgr.update(s1, price=1960.0)
    assert s2.sl_current <= prev_sl


# =========================================================================
# 4. Test Multi-Agent Whitelist, Grounding, and JSD Dissent Escalation
# =========================================================================
def test_order_execution_positive_whitelist():
    agent = OrderExecutionAgent()
    # Unlisted crypto asset
    b_sol = agent.vote({"symbol": "SOLUSD"}, {})
    assert b_sol.veto
    assert b_sol.vote == Vote.NAY
    assert "whitelist" in b_sol.rationale

    # Listed crypto asset
    b_eth = agent.vote({"symbol": "ETHUSD"}, {})
    assert not b_eth.veto
    assert b_eth.vote == Vote.YEA


def test_council_jsd_dissent_escalation():
    class DummyAgent(BaseCouncilAgent):
        def __init__(self, name, weight=1.0):
            self.name = name
            self.weight = weight

    agents = [DummyAgent(f"A{i}") for i in range(5)]

    # Unanimous agreement
    ballots_unanimous = [
        Ballot(agent=f"A{i}", vote=Vote.YEA, confidence=0.90) for i in range(5)
    ]
    jsd_low = compute_council_jsd(ballots_unanimous, agents)
    assert jsd_low < 0.10

    # Fractured deliberation (3 weak YEA, 2 strong NAY)
    ballots_fractured = [
        Ballot(agent="A0", vote=Vote.YEA, confidence=0.55),
        Ballot(agent="A1", vote=Vote.YEA, confidence=0.55),
        Ballot(agent="A2", vote=Vote.YEA, confidence=0.55),
        Ballot(agent="A3", vote=Vote.NAY, confidence=0.95),
        Ballot(agent="A4", vote=Vote.NAY, confidence=0.95),
    ]
    jsd_high = compute_council_jsd(ballots_fractured, agents)
    assert jsd_high > 0.38
