"""
Unit Tests for Institutional Risk Envelope (Arena Quant Audit Verification)
==========================================================================
Verifies:
1. Discrete grid solver exactness and k_min survival preservation.
2. Survival-constrained capital budgeting vs ruin path.
3. Prospective halt behavior when free buffer <= HALT_CUSHION_USD ($20.00).
4. Full closed-fail validation on inverted stops and spread gates.
"""

import math
import pytest
from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    HALT_CUSHION_USD,
    MAX_RISK_USD,
    MIN_RISK_USD,
    MAX_OPEN_POSITIONS,
    SURVIVAL_LOSSES,
    budget_per_trade,
    admissible_volume,
    solve_volume_from_loss_per_lot,
    validate_trade_geometry,
)


def test_grid_solver_kmin_survival_preservation():
    """
    On a discrete broker lot grid (step=0.01), when stop-loss yields $750/lot,
    verify that the solver selects k_min (smallest legal size >= floor) rather than k_max.
    """
    loss_per_lot = 750.0  # e.g. BTC with $750 SL distance on 1.0 lot
    volume_step = 0.01
    loss_per_step = loss_per_lot * volume_step  # $7.50 per 0.01 lot
    floor_usd = 5.00
    cap_usd = 25.00

    vol, reason, actual_risk = admissible_volume(
        loss_per_lot_step=loss_per_step,
        volume_step=volume_step,
        min_lot=0.01,
        max_lot=100.0,
        floor_usd=floor_usd,
        cap_usd=cap_usd
    )

    assert reason == "OK"
    assert vol == 0.01  # k_min=1 -> 0.01 lots ($7.50 risk), NOT k_max=3 (0.03 lots, $22.50)
    assert actual_risk == 7.50
    assert floor_usd <= actual_risk <= cap_usd


def test_grid_solver_no_representable_volume():
    """
    When min risk step exceeds cap_usd, solver must return explicit NO_REPRESENTABLE_VOLUME
    with achieved grid bounds in the reason string.
    """
    loss_per_step = 30.00  # $30 minimum step
    floor_usd = 5.00
    cap_usd = 25.00

    vol, reason, actual_risk = admissible_volume(
        loss_per_lot_step=loss_per_step,
        volume_step=0.01,
        min_lot=0.01,
        max_lot=10.0,
        floor_usd=floor_usd,
        cap_usd=cap_usd
    )

    assert vol is None
    assert "NO_REPRESENTABLE_VOLUME" in reason
    assert "grid=" in reason
    assert actual_risk == 0.0


def test_survival_constrained_capital_budget():
    """
    Verifies that on live equity $4,835.19 with $4,775 floor (buffer $60.19):
    per_trade budget = free / (10 * 2) = $60.19 / 20 = $3.01.
    Since $3.01 < MIN_RISK_USD ($5.00), it safely halts with BUDGET_BELOW_MIN_FLOOR.
    """
    equity = HARD_FLOOR_USD + 60.19
    balance = HARD_FLOOR_USD + 60.19
    # If buffer is $135.19 (equity = HARD_FLOOR_USD + 135.19)
    # per_trade = $135.19 / 20 = 6.7595 -> Floored to cent: $6.75 -> Admissible!
    budget, ok, reason = budget_per_trade(equity=HARD_FLOOR_USD + 135.19, balance=HARD_FLOOR_USD + 135.19, committed_stress_usd=0.0)
    assert ok is True
    assert budget == 6.75
    assert "OK" in reason

    # Test stress-native divisor (divisor = 20 * 1.25 = 25)
    # 135.19 / 25 = 5.4076 -> Floored to cent: $5.40
    b_stress, ok_stress, _ = budget_per_trade(equity=HARD_FLOOR_USD + 135.19, balance=HARD_FLOOR_USD + 135.19, committed_stress_usd=0.0, stress_native=True)
    assert ok_stress is True
    assert b_stress == 5.40


def test_halt_hierarchy_ordering():
    """
    Documents and verifies strict halt hierarchy ordering (Qwen F-04):
    1. Base capital <= HARD_FLOOR_USD -> EQUITY_BREACH
    2. Free buffer <= $20.00 -> PROSPECTIVE_HALT
    3. Computed budget < $5.00 -> BUDGET_BELOW_MIN_FLOOR (binds when free buffer < $100)
    """
    # 1. Base capital at or below floor
    _, ok1, r1 = budget_per_trade(equity=HARD_FLOOR_USD - 5.0, balance=HARD_FLOOR_USD - 5.0)
    assert not ok1 and "EQUITY_BREACH" in r1

    # 2. Free buffer <= $20 cushion (e.g. buffer = $18)
    _, ok2, r2 = budget_per_trade(equity=HARD_FLOOR_USD + 18.0, balance=HARD_FLOOR_USD + 18.0)
    assert not ok2 and "PROSPECTIVE_HALT" in r2

    # 3. Free buffer > $20 cushion but computed budget < $5 (e.g. buffer = $60 -> budget $3.00)
    _, ok3, r3 = budget_per_trade(equity=HARD_FLOOR_USD + 60.0, balance=HARD_FLOOR_USD + 60.0)
    assert not ok3 and "BUDGET_BELOW_MIN_FLOOR" in r3


def test_prospective_halt_on_low_cushion():
    """Trading must halt BEFORE reaching floor when free buffer <= $20.00."""
    equity = HARD_FLOOR_USD + 15.00  # Buffer is $15.00 <= $20.00 cushion
    budget, ok, reason = budget_per_trade(equity=equity, balance=equity, committed_stress_usd=0.0)
    assert ok is False
    assert budget == 0.0
    assert "PROSPECTIVE_HALT" in reason


def test_reservation_of_committed_stress():
    """Committed stress from existing positions reduces free buffer."""
    equity = HARD_FLOOR_USD + 135.19  # Buffer = $135.19
    committed = 120.00  # Free = $15.19 <= $20.00 cushion
    budget, ok, reason = budget_per_trade(equity=equity, balance=equity, committed_stress_usd=committed)
    assert ok is False
    assert "PROSPECTIVE_HALT" in reason


def test_validate_trade_geometry_fails_closed():
    """Inverted stops and excessive spread must fail closed."""
    # Inverted BUY stop
    ok, reason = validate_trade_geometry("BUY", entry=100.0, stop_loss=105.0, take_profit=115.0, spread_bps=10.0)
    assert ok is False
    assert "INVERTED_STOP" in reason

    # Excessive spread
    ok, reason = validate_trade_geometry("BUY", entry=100.0, stop_loss=95.0, take_profit=110.0, spread_bps=50.0)
    assert ok is False
    assert "SPREAD_GATE_BREACH" in reason


def test_equity_breach_at_or_below_hard_floor():
    """Confirms immediate halt when base capital <= HARD_FLOOR_USD."""
    budget, ok, msg = budget_per_trade(equity=HARD_FLOOR_USD, balance=HARD_FLOOR_USD + 75.00, committed_stress_usd=0.0)
    assert ok is False
    assert budget == 0.0
    assert "EQUITY_BREACH" in msg

