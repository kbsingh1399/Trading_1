"""
========================================================================================
Institutional Risk Envelope & Admissible Volume Solver (Single Source of Truth)
========================================================================================
Canonical risk governance and mathematical volume solver.
Every execution and signal evaluation module imports from here.

Design Principles (From Arena Quant Audit):
1. Fails Closed: Inverted stops, zero distances, or missing values reject the trade.
2. Exact Discrete Grid Solver: Solves for the largest legal volume on broker lot grids
   strictly within [MIN_RISK_USD, MAX_RISK_USD] without rounding traps.
3. Dual-Constraint Capital Budgeting: Position risk <= 50% of buffer above $4,700 floor.
4. Hard Invariants: Max 2 concurrent open positions, max 0.45% spread gate.
========================================================================================
"""

import math
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("RiskEnvelope")

# ---------------------------------------------------------------------------
# Canonical Risk Constants (SINGLE SOURCE OF TRUTH)
# ---------------------------------------------------------------------------
HARD_FLOOR_USD = 4700.00           # 6% Max Drawdown Floor on $5,000 Prop-Firm Challenge
OPERATING_BUFFER_MIN = 10.00       # Equity buffer threshold below which all trading halts
BUFFER_RISK_FRACTION = 0.50        # Max 50% of available buffer risked per trade
MAX_RISK_USD = 50.00               # Strict hard ceiling per trade ($50.00)
MIN_RISK_USD = 5.00                # Minimum risk threshold to prevent micro-lot commission drag
MAX_OPEN_POSITIONS = 2             # Hard mandate: 2 concurrent positions max
MAX_ALLOWED_SPREAD_PCT = 0.45      # 0.45% maximum allowable spread (45 bps)
MIN_RR_RATIO = 1.50                # Minimum 1.5:1 reward-to-risk ratio


def admissible_volume(
    loss_per_lot: float,
    step: float,
    min_lot: float,
    max_lot: float,
    floor_usd: float = MIN_RISK_USD,
    cap_usd: float = MAX_RISK_USD
) -> Tuple[Optional[float], str, float]:
    """
    Solves for the largest broker grid volume whose realized dollar risk
    lies strictly inside [floor_usd, cap_usd].

    Args:
        loss_per_lot: Dollar loss if 1.0 lot hits stop-loss (points_dist * tick_val / tick_size).
        step: Broker volume_step (e.g. 0.01 or 0.1).
        min_lot: Broker minimum lot size (e.g. 0.01).
        max_lot: Broker maximum lot size (e.g. 100.0).
        floor_usd: Lower dollar risk bound (e.g. $5.00).
        cap_usd: Upper dollar risk bound (e.g. $50.00 or buffer * 0.50).

    Returns:
        (volume, reason_code, realized_risk_usd)
        If admissible: (float_lots, "OK", dollar_risk)
        If inadmissible: (None, specific_rejection_reason, 0.0)
    """
    if loss_per_lot <= 0.0 or step <= 0.0:
        return None, "INVALID_LOSS_PER_LOT_OR_STEP", 0.0

    if cap_usd < floor_usd:
        return None, f"EMPTY_CAP_WINDOW: cap (${cap_usd:.2f}) < floor (${floor_usd:.2f})", 0.0

    # Risk per single step
    loss_per_step = loss_per_lot * step

    # Calculate discrete integer step bounds
    k_min = math.ceil((floor_usd / loss_per_step) - 1e-12)
    k_max = math.floor((cap_usd / loss_per_step) + 1e-12)

    # Convert broker min_lot/max_lot to step multipliers
    k_broker_min = math.ceil((min_lot / step) - 1e-12)
    k_broker_max = math.floor((max_lot / step) + 1e-12)

    # Intersect allowable risk steps with broker allowed range
    effective_k_min = max(k_min, k_broker_min)
    effective_k_max = min(k_max, k_broker_max)

    if effective_k_max < effective_k_min:
        actual_min_risk = (effective_k_min * step) * loss_per_lot
        actual_max_risk = (effective_k_max * step) * loss_per_lot
        return None, (
            f"NO_REPRESENTABLE_VOLUME: grid step {step} gives [{actual_min_risk:.2f}, {actual_max_risk:.2f}] "
            f"outside target risk window [${floor_usd:.2f}, ${cap_usd:.2f}]"
        ), 0.0

    # Select the largest legal volume inside the window
    final_lots = round(effective_k_max * step, 6)
    actual_risk_usd = round(final_lots * loss_per_lot, 2)

    # Final safety sanity assert
    if actual_risk_usd > cap_usd + 0.01:
        return None, f"NUMERICAL_OVERFLOW: realized risk ${actual_risk_usd:.2f} > cap ${cap_usd:.2f}", 0.0

    return final_lots, "OK", actual_risk_usd


def compute_dynamic_capital_budget(current_equity: float) -> Tuple[float, bool, str]:
    """
    Computes available dollar risk based on prop-firm floor defense.
    Risk ceiling = min(MAX_RISK_USD, buffer * 50%).
    """
    if current_equity <= HARD_FLOOR_USD:
        return 0.0, False, f"EQUITY_BREACH: Current equity ${current_equity:.2f} <= Floor ${HARD_FLOOR_USD:.2f}"

    buffer = current_equity - HARD_FLOOR_USD
    if buffer <= OPERATING_BUFFER_MIN:
        return 0.0, False, f"HALT_BUFFER: Buffer ${buffer:.2f} <= minimum required ${OPERATING_BUFFER_MIN:.2f}"

    # Capital budgeted: half of buffer or $50.00 max
    budget = min(MAX_RISK_USD, round(buffer * BUFFER_RISK_FRACTION, 2))
    budget = max(MIN_RISK_USD, budget)
    return budget, True, f"OK (Buffer: ${buffer:.2f}, Budget: ${budget:.2f})"


def validate_trade_geometry(
    direction: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
    spread_pct: float
) -> Tuple[bool, str]:
    """
    Fails closed on any invalid geometry or spread breach.
    """
    if spread_pct > MAX_ALLOWED_SPREAD_PCT:
        return False, f"SPREAD_GATE_BREACH: Spread {spread_pct:.3f}% > Max allowed {MAX_ALLOWED_SPREAD_PCT:.3f}%"

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
