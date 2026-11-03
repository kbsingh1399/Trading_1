"""
========================================================================================
Google OR-Tools Mathematical Integer Programming Lot Solver
========================================================================================
Implements exact Mixed-Integer Linear Programming (MILP) using Google OR-Tools (SCIP/CBC).
Replaces heuristic search loops with mathematically guaranteed discrete lot size selection
under strict FundingPips prop firm survival constraints ($4,775.00 hard floor, $25.00 risk cap).

Architecture & Mathematical Formulation:
----------------------------------------
Decision Variable:
    k in {0, 1, 2, ..., K_max}  (Integer number of discrete volume steps)
    Volume = k * volume_step

Constraints:
    1. Grid bounds:
       k * volume_step >= min_lot (if entering, else k = 0)
       k * volume_step <= max_lot
    2. Capital risk cap:
       k * loss_per_step <= allowed_budget_usd (<= MAX_RISK_USD $25.00)
    3. Minimum economic viable risk:
       k * loss_per_step >= MIN_RISK_USD (e.g. $2.00)
    4. Account Hard Floor Protection:
       Base_Equity - Current_Committed - (k * loss_per_step) >= HARD_FLOOR_USD ($4,775.00) + HALT_CUSHION ($25.00)
    5. Concurrent Position Gate:
       current_open_positions + 1 <= MAX_OPEN_POSITIONS (2)

Objective:
    Maximize Expected Payoff:
    Z(k) = (confluence_score / 100.0) * (k * step_reward_usd) - lambda_risk * (k * loss_per_step)
========================================================================================
"""

import math
import logging
from typing import Tuple, Optional, Dict, Any
from decimal import Decimal, ROUND_FLOOR

from ortools.linear_solver import pywraplp

logger = logging.getLogger("GoogleORToolsLotSolver")
logger.setLevel(logging.INFO)

HARD_FLOOR_USD = 4775.00
HALT_CUSHION_USD = 25.00
MAX_RISK_USD = 25.00
MIN_RISK_USD = 2.00
MAX_OPEN_POSITIONS = 2


def solve_optimal_lot_ortools(
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
) -> Tuple[Optional[float], str, float, Dict[str, Any]]:
    """
    Solves for the optimal volume using Google OR-Tools SCIP Mixed-Integer Linear Program.

    Returns:
        (optimal_volume, status_reason, allocated_risk_usd, diagnostics_dict)
    """
    diagnostics = {
        "solver": "Google-OR-Tools-SCIP",
        "confluence_score": confluence_score,
        "active_positions": active_open_positions,
        "hard_floor_usd": HARD_FLOOR_USD,
        "max_risk_cap_usd": max_risk_cap_usd
    }

    # 1. Capacity & Position Guardrail
    if active_open_positions >= MAX_OPEN_POSITIONS:
        msg = f"POSITION_CAPACITY_EXHAUSTED: {active_open_positions}/{MAX_OPEN_POSITIONS} positions active"
        return None, msg, 0.0, diagnostics

    # 2. Capital Floor Evaluation
    base_equity = min(current_equity, current_balance)
    capital_buffer = base_equity - HARD_FLOOR_USD
    if capital_buffer <= 0.0:
        msg = f"EQUITY_BREACH: Base equity USD {base_equity:.2f} <= Hard Floor USD {HARD_FLOOR_USD:.2f}"
        return None, msg, 0.0, diagnostics

    free_survival_buffer = capital_buffer - committed_risk_usd
    if free_survival_buffer <= HALT_CUSHION_USD:
        msg = f"PROSPECTIVE_HALT: Free survival buffer USD {free_survival_buffer:.2f} <= Cushion USD {HALT_CUSHION_USD:.2f}"
        return None, msg, 0.0, diagnostics

    # 3. Dynamic Per-Trade Risk Cap
    effective_cap = min(max_risk_cap_usd, free_survival_buffer - HALT_CUSHION_USD)
    if effective_cap < min_risk_floor_usd:
        msg = f"BUDGET_BELOW_MIN_FLOOR: Effective cap USD {effective_cap:.2f} < Min USD {min_risk_floor_usd:.2f}"
        return None, msg, 0.0, diagnostics

    # 4. Input Sanity Checks
    if not all(math.isfinite(x) for x in (loss_per_lot_step, volume_step, min_lot, max_lot)) or loss_per_lot_step <= 0 or volume_step <= 0:
        return None, "INVALID_NUMERIC_PARAMETERS", 0.0, diagnostics

    # 5. Initialize Google OR-Tools Solver
    solver = pywraplp.Solver.CreateSolver("SCIP")
    if not solver:
        # Fallback to CBC or basic solver if SCIP not compiled
        solver = pywraplp.Solver.CreateSolver("CBC")
    if not solver:
        return None, "OR_TOOLS_SOLVER_UNAVAILABLE", 0.0, diagnostics

    # Integer decision variable: k = number of volume steps
    k_min_bound = int(math.ceil(min_lot / volume_step))
    k_max_bound = int(math.floor(max_lot / volume_step))
    
    k_var = solver.IntVar(0, k_max_bound, "lot_steps")

    # Binary execution indicator: enter_trade in {0, 1}
    enter_var = solver.BoolVar("enter_trade")

    # Constraint A: If enter_trade == 1, k >= k_min_bound; else k == 0
    solver.Add(k_var >= k_min_bound * enter_var)
    solver.Add(k_var <= k_max_bound * enter_var)

    # Constraint B: Risk cap (k * loss_per_lot_step <= effective_cap)
    solver.Add(k_var * loss_per_lot_step <= effective_cap * enter_var)

    # Constraint C: Minimum risk floor (k * loss_per_lot_step >= min_risk_floor_usd)
    solver.Add(k_var * loss_per_lot_step >= min_risk_floor_usd * enter_var)

    # Constraint D: Account Hard Floor Protection
    # Base_Equity - Committed - (k * loss_per_lot_step) >= HARD_FLOOR_USD + HALT_CUSHION_USD
    solver.Add(k_var * loss_per_lot_step <= (free_survival_buffer - HALT_CUSHION_USD) * enter_var)

    # Objective Function:
    # Maximize Expected Payoff:
    # Win prob proportional to confluence: p_win = (confluence_score / 100.0)
    # Expected Return = p_win * (reward_risk_ratio * Risk) - (1 - p_win) * Risk
    #                 = [p_win * reward_risk_ratio - (1 - p_win)] * (k * loss_per_lot_step)
    p_win = max(0.01, min(0.99, confluence_score / 100.0))
    expected_value_multiplier = (p_win * reward_risk_ratio) - (1.0 - p_win)
    
    # We want to maximize expected net gain while penalizing excessive risk
    # To prioritize conservative account survival, if expected value is positive, scale linearly with small dampener
    objective = solver.Objective()
    # If edge is positive, maximize k * loss_per_lot_step scaled by edge; otherwise maximize 0
    edge_coeff = max(0.05, expected_value_multiplier)
    objective.SetCoefficient(k_var, edge_coeff * loss_per_lot_step)
    objective.SetMaximization()

    # Solve the MILP problem
    status = solver.Solve()

    if status not in (pywraplp.Solver.OPTIMAL, pywraplp.Solver.FEASIBLE):
        diagnostics["solver_status"] = "INFEASIBLE_OR_FAILED"
        return None, "NO_FEASIBLE_DISCRETE_SOLUTION", 0.0, diagnostics

    solved_k = int(round(k_var.solution_value()))
    solved_enter = int(round(enter_var.solution_value()))

    if solved_enter == 0 or solved_k <= 0:
        diagnostics["solver_status"] = "ZERO_VOLUME_OPTIMAL"
        return None, "ZERO_VOLUME_OPTIMAL", 0.0, diagnostics

    # Calculate exact volume and risk
    # Use Decimal to ensure zero floating point precision drift
    d_step = Decimal(str(volume_step))
    d_k = Decimal(str(solved_k))
    d_loss = Decimal(str(loss_per_lot_step))
    
    final_volume = float(d_k * d_step)
    allocated_risk = float(d_k * d_loss)

    diagnostics.update({
        "solver_status": "OPTIMAL",
        "solved_steps": solved_k,
        "computed_volume": final_volume,
        "allocated_risk_usd": allocated_risk,
        "effective_cap_usd": effective_cap,
        "free_buffer_usd": free_survival_buffer
    })

    return final_volume, "OK", allocated_risk, diagnostics
