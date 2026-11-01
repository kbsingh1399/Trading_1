"""
========================================================================================
Institutional Risk Envelope & Admissible Volume Solver (Single Source of Truth)
========================================================================================
Canonical risk governance and mathematical volume solver.
Every execution and signal evaluation module imports from here.

Governed by Arena Quant Audit (Round 2 & Sonnet 5.5 Institutional Verification):
1. Survival Constraint: max_risk = free_buffer / (SURVIVAL_LOSSES * MAX_OPEN_POSITIONS)
   Guarantees a 10-consecutive loss streak does NOT breach the $4,775 hard floor.
2. Prospective Halt: Halts trading BEFORE entering if free_buffer <= HALT_CUSHION_USD.
3. Reservation-Based: Subtracts worst-case stressed risk of all open positions + resting limits.
4. Exact Integer Grid Solver: Solves discrete lot grid [k_min, k_max].
   Returns k_min (smallest legal volume >= floor), preserving account survival.
5. Fails Closed: Inverted stops, zero distances, or spread breach reject instantly.
========================================================================================
"""

import math
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("RiskEnvelope")

# ---------------------------------------------------------------------------
# Canonical Risk Constants (SINGLE SOURCE OF TRUTH ACROSS THE ENTIRE SYSTEM)
# ---------------------------------------------------------------------------
HARD_FLOOR_USD = 4775.00       # Contractual floor ($4,700) + Self-imposed safety buffer ($75) = $4,775
HALT_CUSHION_USD = 20.00       # Prospective halt cushion >= worst-case single-trade execution reserve
OPERATING_BUFFER_MIN = 20.00   # Canonical alias for HALT_CUSHION_USD
MAX_RISK_USD = 25.00           # Strict nominal ceiling per trade ($25.00)
MIN_RISK_USD = 5.00            # Minimum risk threshold to prevent micro-lot commission drag
MAX_OPEN_POSITIONS = 2         # Hard mandate: strictly 2 concurrent positions max
MAX_RESTING_LIMITS = 2         # Resting pending orders count as future contingent liabilities
MAX_SPREAD_BPS = 45.0          # 45.0 bps maximum allowable spread (0.45%)
MAX_ALLOWED_SPREAD_PCT = 0.45  # Same in percentage unit
MIN_RR_RATIO = 1.50            # Minimum 1.5:1 reward-to-risk ratio
STRESS_MULT = 1.25             # Stress multiplier on stop-out for gap/slippage reserve
EXEC_RESERVE_USD = 2.00        # Per-trade execution reserve for broker friction
SURVIVAL_LOSSES = 10           # Required consecutive loss survival streak


def budget_per_trade(
    equity: float,
    balance: float,
    committed_stress_usd: float = 0.0
) -> Tuple[float, bool, str]:
    """
    Survival-constrained, reservation-based capital budgeting.
    Replaces the flawed '50% of buffer' geometric decay rule.

    Calculates:
        base = min(balance, equity)
        buffer = base - HARD_FLOOR_USD
        free = buffer - committed_stress_usd
        if free <= HALT_CUSHION_USD -> HALT (prospective)
        per_trade = free / (SURVIVAL_LOSSES * MAX_OPEN_POSITIONS)
        budget = min(MAX_RISK_USD, per_trade)

    Returns:
        (allowed_budget_usd, is_admissible, reason)
    """
    base = min(balance, equity)
    buffer = base - HARD_FLOOR_USD

    if buffer <= 0.0:
        return 0.0, False, f"EQUITY_BREACH: Base capital ${base:.2f} <= Hard Floor ${HARD_FLOOR_USD:.2f}"

    free = buffer - committed_stress_usd
    if free <= HALT_CUSHION_USD:
        return 0.0, False, f"PROSPECTIVE_HALT: Free buffer ${free:.2f} <= Halt Cushion ${HALT_CUSHION_USD:.2f}"

    per_trade = free / (SURVIVAL_LOSSES * MAX_OPEN_POSITIONS)
    budget = min(MAX_RISK_USD, max(0.0, round(per_trade, 2)))

    if budget < MIN_RISK_USD:
        return 0.0, False, (
            f"BUDGET_BELOW_MIN_FLOOR: Computed budget ${budget:.2f} < Min threshold ${MIN_RISK_USD:.2f} "
            f"(Free buffer: ${free:.2f})"
        )

    return budget, True, f"OK (Free Buffer: ${free:.2f}, Budget: ${budget:.2f})"


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

    if actual_loss_step <= 0.0 or actual_step <= 0.0:
        return None, "NO_BROKER_VALUATION", 0.0

    if cap_usd < floor_usd:
        return None, f"EMPTY_CAP_WINDOW: cap (${cap_usd:.2f}) < floor (${floor_usd:.2f})", 0.0

    # Discrete step boundaries with 1e-12 floating-point margin
    k_min = math.ceil((floor_usd / actual_loss_step) - 1e-12)
    max_step_bound = min(cap_usd, max_lot * actual_loss_step)
    k_max = math.floor((max_step_bound / actual_loss_step) + 1e-12)

    # Broker min/max constraints in step index space
    k_broker_min = math.ceil((min_lot / actual_step) - 1e-12)
    k_broker_max = math.floor((max_lot / actual_step) + 1e-12)

    effective_k_min = max(k_min, k_broker_min)
    effective_k_max = min(k_max, k_broker_max)

    if effective_k_max < effective_k_min or effective_k_min <= 0:
        actual_min_risk = (effective_k_min * actual_loss_step)
        actual_max_risk = (effective_k_max * actual_loss_step)
        return None, (
            f"NO_REPRESENTABLE_VOLUME: grid=[{actual_min_risk:.2f},{actual_max_risk:.2f}] "
            f"window=[{floor_usd:.2f},{cap_usd:.2f}]"
        ), 0.0

    # Pick k_min: smallest legal size >= floor, preserving capital survival
    final_lots = round(effective_k_min * actual_step, 6)
    actual_risk_usd = round(effective_k_min * actual_loss_step, 2)

    if actual_risk_usd > cap_usd + 0.01:
        return None, f"NUMERICAL_OVERFLOW: realized risk ${actual_risk_usd:.2f} > cap ${cap_usd:.2f}", 0.0

    return final_lots, "OK", actual_risk_usd


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
    if spread_bps > MAX_SPREAD_BPS:
        return False, f"SPREAD_GATE_BREACH: Spread {spread_bps:.1f} bps > Max allowed {MAX_SPREAD_BPS:.1f} bps"

    if entry <= 0.0 or stop_loss <= 0.0 or take_profit <= 0.0:
        return False, "INVALID_PRICE_LEVELS: Non-positive prices"

    dir_upper = direction.upper()
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
    rr = reward_dist / (risk_dist + 1e-9)

    if rr < MIN_RR_RATIO:
        return False, f"POOR_REWARD_TO_RISK: R:R {rr:.2f} < Min required {MIN_RR_RATIO:.2f}"

    return True, "OK"
