"""
========================================================================================
Institutional Risk Envelope & Admissible Volume Solver (Single Source of Truth)
========================================================================================
Canonical risk governance and mathematical volume solver.
Every execution and signal evaluation module imports from here.

Governed by Arena Quant Audit (Round 2 & Sonnet 5.5 Institutional Verification):
1. Survival Constraint: max_risk = free_buffer / (SURVIVAL_LOSSES * MAX_OPEN_POSITIONS)
   Guarantees a 10-consecutive loss streak does NOT breach the USD 4,775 hard floor.
2. Prospective Halt: Halts trading BEFORE entering if free_buffer <= HALT_CUSHION_USD.
3. Reservation-Based: Subtracts worst-case stressed risk of all open positions + resting limits.
4. Exact Integer Grid Solver: Solves discrete lot grid [k_min, k_max].
   Returns k_min (smallest legal volume >= floor), preserving account survival.
5. Fails Closed: Inverted stops, zero distances, or spread breach reject instantly.
========================================================================================
"""

import math
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("RiskEnvelope")

# ---------------------------------------------------------------------------
# Canonical Risk Constants (SINGLE SOURCE OF TRUTH ACROSS THE ENTIRE SYSTEM)
# ---------------------------------------------------------------------------
HARD_FLOOR_USD = 4775.00       # Contractual floor (USD 4,700) + Self-imposed safety buffer (USD 75) = USD 4,775
HALT_CUSHION_USD = 20.00       # Prospective halt cushion >= worst-case single-trade execution reserve
OPERATING_BUFFER_MIN = 20.00   # Canonical alias for HALT_CUSHION_USD
MAX_RISK_USD = 25.00           # Strict nominal ceiling per trade (USD 25.00)
MIN_RISK_USD = 5.00            # Minimum risk threshold to prevent micro-lot commission drag
MAX_OPEN_POSITIONS = 2         # Hard mandate: strictly 2 concurrent positions max
MAX_RESTING_LIMITS = 2         # Resting pending orders count as future contingent liabilities
MAX_SPREAD_BPS = 45.0          # 45.0 bps maximum allowable spread (0.45%)
MAX_ALLOWED_SPREAD_PCT = 0.45  # Same in percentage unit
MIN_RR_RATIO = 1.50            # Minimum 1.5:1 reward-to-risk ratio
STRESS_MULT = 1.25             # Stress multiplier on stop-out for gap/slippage reserve
EXEC_RESERVE_USD = 2.00        # Per-trade execution reserve for broker friction
SURVIVAL_LOSSES = 10           # Required consecutive loss survival streak
# Standard survival divisor (10 loss episodes * 2 positions = 20)
SURVIVAL_DIVISOR = SURVIVAL_LOSSES * MAX_OPEN_POSITIONS


# ---------------------------------------------------------------------------
# Daily Loss Tracking (prop challenge trailing drawdown guard)
# ---------------------------------------------------------------------------
# FundingPips challenge: max daily loss = 5% of initial balance ($250 for $5k).
# This module tracks P&L booked today relative to the platform opening equity.
# Callers (realtime_signal_evaluator) must feed opening_equity each day.
MAX_DAILY_LOSS_PCT = 0.05          # 5 % of account balance per challenge rules
MAX_DAILY_LOSS_USD = MAX_DAILY_LOSS_PCT * 5000.0  # = $250.00 for $5k account


def daily_loss_remaining(opening_equity: float, current_equity: float) -> Tuple[float, bool, str]:
    """
    Returns how much further loss is allowed today.

    Args:
        opening_equity: Equity at the start of today's trading session (platform day).
        current_equity: Current live equity.

    Returns:
        (remaining_usd, is_tradeable, reason)
    """
    if not all(isinstance(x, (int, float)) and math.isfinite(x) and x > 0 for x in (opening_equity, current_equity)):
        return 0.0, False, "INVALID_EQUITY_STATE"

    daily_cap = opening_equity * MAX_DAILY_LOSS_PCT
    booked_loss = max(0.0, opening_equity - current_equity)
    remaining = daily_cap - booked_loss

    if remaining <= 0.0:
        return 0.0, False, f"DAILY_LOSS_LIMIT_REACHED: Lost ${booked_loss:.2f} of ${daily_cap:.2f} daily cap"

    if remaining < HALT_CUSHION_USD:
        return remaining, False, f"DAILY_LOSS_CUSHION_BREACH: Only ${remaining:.2f} remaining today"

    return remaining, True, f"OK (Daily loss: ${booked_loss:.2f} of ${daily_cap:.2f} cap; ${remaining:.2f} remaining)"


def budget_per_trade(
    equity: float,
    balance: float,
    committed_stress_usd: float = 0.0,
    stress_native: bool = False
) -> Tuple[float, bool, str]:
    """
    Survival-constrained, reservation-based capital budgeting.
    Replaces the flawed '50% of buffer' geometric decay rule.

    Halt Hierarchy:
        1. Base capital <= HARD_FLOOR_USD -> EQUITY_BREACH
        2. Free buffer <= HALT_CUSHION_USD -> PROSPECTIVE_HALT (cushion defense)
        3. Computed budget < MIN_RISK_USD -> BUDGET_BELOW_MIN_FLOOR (binds when free buffer < USD 100)

    Calculates:
        if not all(isinstance(x, (int, float)) and math.isfinite(x) for x in (balance, equity, committed_stress_usd)) or committed_stress_usd < 0:
        return 0.0, False, "INVALID_CAPITAL_STATE"
    base = min(balance, equity)
        buffer = base - HARD_FLOOR_USD
        free = buffer - committed_stress_usd
        if free <= HALT_CUSHION_USD -> HALT (prospective)
        divisor = SURVIVAL_DIVISOR * (STRESS_MULT if stress_native else 1.0)
        per_trade = free / divisor
        budget = min(MAX_RISK_USD, math.floor(per_trade * 100.0) / 100.0)

    Returns:
        (allowed_budget_usd, is_admissible, reason)
    """
    base = min(balance, equity)
    buffer = base - HARD_FLOOR_USD

    if buffer <= 0.0:
        return 0.0, False, f"EQUITY_BREACH: Base capital USD {base:.2f} <= Hard Floor USD {HARD_FLOOR_USD:.2f}"

    free = buffer - committed_stress_usd
    if free <= HALT_CUSHION_USD:
        return 0.0, False, f"PROSPECTIVE_HALT: Free buffer USD {free:.2f} <= Halt Cushion USD {HALT_CUSHION_USD:.2f}"

    divisor = SURVIVAL_DIVISOR * (STRESS_MULT if stress_native else 1.0)
    per_trade = free / divisor
    # Qwen F-15: Floor to cent (math.floor) to prevent upward rounding on survival constraints
    budget = min(MAX_RISK_USD, max(0.0, math.floor(per_trade * 100.0) / 100.0))

    if budget < MIN_RISK_USD:
        return 0.0, False, (
            f"BUDGET_BELOW_MIN_FLOOR: Computed budget USD {budget:.2f} < Min threshold USD {MIN_RISK_USD:.2f} "
            f"(Free buffer: USD {free:.2f})"
        )

    return budget, True, f"OK (Free Buffer: USD {free:.2f}, Budget: USD {budget:.2f})"


def compute_dynamic_capital_budget(
    current_equity: float,
    balance: Optional[float] = None,
    committed_stress_usd: float = 0.0
) -> Tuple[float, bool, str]:
    """
    Backward-compatible wrapper mapping to the canonical survival-constrained budget_per_trade.
    """
    bal = balance if balance is not None else current_equity
    return budget_per_trade(equity=current_equity, balance=bal, committed_stress_usd=committed_stress_usd)


def admissible_volume(
    loss_per_lot_step: Optional[float] = None,
    volume_step: Optional[float] = None,
    min_lot: float = 0.01,
    max_lot: float = 100.0,
    floor_usd: float = MIN_RISK_USD,
    cap_usd: float = MAX_RISK_USD,
    # Compatibility arguments for callers passing per-lot loss and step
    loss_per_lot: Optional[float] = None,
    step: Optional[float] = None
) -> Tuple[Optional[float], str, float]:
    """
    Exact discrete integer-step grid solver.
    Supports both loss_per_lot_step (Arena spec) and (loss_per_lot, step) signatures.
    Always returns k_min (smallest legal size >= floor), preserving account survival.
    """
    actual_step = volume_step if volume_step is not None else (step if step is not None else 0.01)

    if loss_per_lot_step is not None:
        actual_loss_step = loss_per_lot_step
    elif loss_per_lot is not None:
        actual_loss_step = loss_per_lot * actual_step
    else:
        return None, "NO_BROKER_VALUATION", 0.0

    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (actual_loss_step, actual_step, min_lot, max_lot, floor_usd, cap_usd)):
        return None, "INVALID_VOLUME_INPUT", 0.0
    if actual_loss_step <= 0.0 or actual_step <= 0.0:
        return None, "NO_BROKER_VALUATION", 0.0

    if cap_usd < floor_usd:
        return None, f"EMPTY_CAP_WINDOW: cap (USD {cap_usd:.2f}) < floor (USD {floor_usd:.2f})", 0.0

    # Work in integer lot-step indices. Never mix volume with loss per step.
    values = (actual_loss_step, actual_step, min_lot, max_lot, floor_usd, cap_usd)
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in values):
        return None, "INVALID_VOLUME_INPUT", 0.0
    if min_lot <= 0 or max_lot < min_lot or floor_usd < 0 or cap_usd <= 0:
        return None, "INVALID_VOLUME_INPUT", 0.0
    loss, grid, minimum, maximum, floor, cap = map(lambda v: Decimal(str(v)), values)
    ceil = lambda v: int(v.to_integral_value(rounding=ROUND_CEILING))
    down = lambda v: int(v.to_integral_value(rounding=ROUND_FLOOR))
    k_min = max(1, ceil(minimum / grid), ceil(floor / loss))
    k_max = min(down(maximum / grid), down(cap / loss))
    if k_min > k_max:
        return None, f"NO_REPRESENTABLE_VOLUME: grid=[{k_min},{k_max}]", 0.0
    volume = grid * k_min
    risk = loss * k_min
    # Preserve full precision: rounding risk to cents can hide a cap breach.
    return float(volume), "OK", float(risk)



def solve_volume_from_loss_per_lot(
    loss_per_lot: float,
    volume_step: float,
    min_lot: float,
    max_lot: float,
    floor_usd: float = MIN_RISK_USD,
    cap_usd: float = MAX_RISK_USD
) -> Tuple[Optional[float], str, float]:
    """Helper wrapper taking per-lot loss and explicitly converting to per-step loss."""
    return admissible_volume(
        loss_per_lot=loss_per_lot,
        step=volume_step,
        min_lot=min_lot,
        max_lot=max_lot,
        floor_usd=floor_usd,
        cap_usd=cap_usd
    )


def validate_trade_geometry(
    direction: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
    spread_bps: float
) -> Tuple[bool, str]:
    """
    Fails closed on any inverted stop, non-positive price, or spread breach.
    """
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (entry, stop_loss, take_profit, spread_bps)) or spread_bps < 0:
        return False, "INVALID_GEOMETRY_INPUT"
    if spread_bps > MAX_SPREAD_BPS:
        return False, f"SPREAD_GATE_BREACH: Spread {spread_bps:.1f} bps > Max allowed {MAX_SPREAD_BPS:.1f} bps"

    if entry <= 0.0 or stop_loss <= 0.0 or take_profit <= 0.0:
        return False, "INVALID_PRICE_LEVELS: Non-positive prices"

    dir_upper = str(direction).upper()
    if dir_upper == "BUY":
        if stop_loss >= entry:
            return False, f"INVERTED_STOP: BUY stop-loss {stop_loss} >= entry {entry}"
        if take_profit <= entry:
            return False, f"INVERTED_TARGET: BUY take-profit {take_profit} <= entry {entry}"
    elif dir_upper == "SELL":
        if stop_loss <= entry:
            return False, f"INVERTED_STOP: SELL stop-loss {stop_loss} <= entry {entry}"
        if take_profit >= entry:
            return False, f"INVERTED_TARGET: SELL take-profit {take_profit} >= entry {entry}"
    else:
        return False, f"INVALID_DIRECTION: {direction}"

    risk_dist = abs(entry - stop_loss)
    reward_dist = abs(take_profit - entry)
    rr = reward_dist / risk_dist

    if rr < MIN_RR_RATIO:
        return False, f"POOR_REWARD_TO_RISK: R:R {rr:.2f} < Min required {MIN_RR_RATIO:.2f}"

    return True, "OK"


def solve_volume_ortools(
    loss_per_lot_step: float,
    volume_step: float = 0.01,
    min_lot: float = 0.01,
    max_lot: float = 100.0,
    current_equity: float = 5000.0,
    current_balance: float = 5000.0,
    committed_risk_usd: float = 0.0,
    active_open_positions: int = 0,
    confluence_score: float = 80.0,
    reward_risk_ratio: float = 2.0,
    max_risk_cap_usd: float = MAX_RISK_USD,
    min_risk_floor_usd: float = MIN_RISK_USD
) -> Tuple[Optional[float], str, float]:
    """
    Google OR-Tools Mixed-Integer Programming solver for optimal discrete lot sizing.
    Combines exact MILP optimization with FundingPips survival constraints.
    """
    try:
        from Terminal.ortools_lot_solver import solve_optimal_lot_ortools
        vol, status, risk, _ = solve_optimal_lot_ortools(
            loss_per_lot_step=loss_per_lot_step,
            volume_step=volume_step,
            min_lot=min_lot,
            max_lot=max_lot,
            current_equity=current_equity,
            current_balance=current_balance,
            committed_risk_usd=committed_risk_usd,
            active_open_positions=active_open_positions,
            confluence_score=confluence_score,
            reward_risk_ratio=reward_risk_ratio,
            max_risk_cap_usd=max_risk_cap_usd,
            min_risk_floor_usd=min_risk_floor_usd
        )
        return vol, status, risk
    except Exception as e:
        return admissible_volume(
            loss_per_lot_step=loss_per_lot_step,
            volume_step=volume_step,
            min_lot=min_lot,
            max_lot=max_lot,
            floor_usd=min_risk_floor_usd,
            cap_usd=max_risk_cap_usd
        )
