"""
========================================================================================
Google OR-Tools Mathematical Integer Programming Lot Solver
========================================================================================
Implements exact Mixed-Integer Linear Programming (MILP) using Google OR-Tools (SCIP/CBC).
Replaces heuristic search loops with mathematically guaranteed discrete lot size selection
under strict FundingPips prop firm survival constraints ($4,775.00 hard floor, $25.00 risk cap).

Guarantees (EGM Architectural Compliant):
1. 10-consecutive-loss survival preservation: divides buffer by SURVIVAL_DIVISOR (20).
2. Exact integer-step compliance: lot = k * volume_step, k in Z+.
3. Absolute risk cap enforcement: Loss(k) <= MAX_RISK_USD ($25.00).
4. Account Hard Floor Protection: Equity - Committed - Loss(k) >= HARD_FLOOR_USD ($4,775.00) + Cushion ($20.00).
5. Concurrent Position Gate: active_open_positions + 1 <= MAX_OPEN_POSITIONS (2).
6. Conservative volume bias: Defaults to k_min for survival preservation unless high confluence edge.
========================================================================================
"""

import math
import logging
from typing import Tuple, Optional, Dict, Any
from decimal import Decimal

from ortools.linear_solver import pywraplp

from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    HALT_CUSHION_USD,
    MAX_RISK_USD,
    MIN_RISK_USD,
    MAX_OPEN_POSITIONS,
    SURVIVAL_DIVISOR,
    STRESS_MULT
)

logger = logging.getLogger("GoogleORToolsLotSolver")
logger.setLevel(logging.INFO)


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
    min_risk_floor_usd: float = MIN_RISK_USD,
    stress_native: bool = False
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
        "max_risk_cap_usd": max_risk_cap_usd,
        "survival_divisor": SURVIVAL_DIVISOR
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

    # 3. Dynamic Per-Trade Risk Cap with 10-Loss Streak Survival Divisor
    divisor = SURVIVAL_DIVISOR * (STRESS_MULT if stress_native else 1.0)
    per_trade_budget = free_survival_buffer / divisor
    effective_cap = min(max_risk_cap_usd, max(0.0, math.floor(per_trade_budget * 100.0) / 100.0))

    if effective_cap < min_risk_floor_usd:
        msg = f"BUDGET_BELOW_MIN_FLOOR: Effective cap USD {effective_cap:.2f} < Min USD {min_risk_floor_usd:.2f} (Free buffer: ${free_survival_buffer:.2f})"
        return None, msg, 0.0, diagnostics

    # 4. Input Sanity Checks
    if not all(math.isfinite(x) for x in (loss_per_lot_step, volume_step, min_lot, max_lot)) or loss_per_lot_step <= 0 or volume_step <= 0:
        return None, "INVALID_NUMERIC_PARAMETERS", 0.0, diagnostics

    # 5. Initialize Google OR-Tools Solver
    solver = pywraplp.Solver.CreateSolver("SCIP")
    if not solver:
        solver = pywraplp.Solver.CreateSolver("CBC")
    if not solver:
        return None, "OR_TOOLS_SOLVER_UNAVAILABLE", 0.0, diagnostics

    # Integer decision variable: k = number of volume steps
    k_min_bound = int(math.ceil(min_lot / volume_step))
    k_max_bound = int(math.floor(max_lot / volume_step))
    
    k_var = solver.IntVar(0, k_max_bound, "lot_steps")
    enter_var = solver.BoolVar("enter_trade")

    # Constraint A: If enter_trade == 1, k >= k_min_bound; else k == 0
    solver.Add(k_var >= k_min_bound * enter_var)
    solver.Add(k_var <= k_max_bound * enter_var)

    # Constraint B: Strict Survival Risk Cap
    solver.Add(k_var * loss_per_lot_step <= effective_cap * enter_var)

    # Constraint C: Minimum economic viable risk
    solver.Add(k_var * loss_per_lot_step >= min_risk_floor_usd * enter_var)

    # Objective Function: Conservative Account Survival
    # In prop-firm challenge, capital preservation dominates.
    # We solve for minimal admissible size (k_min) when edge is moderate (75-80),
    # and only scale up when confluence >= 85.
    objective = solver.Objective()
    if confluence_score >= 85.0:
        # Scale with edge towards effective_cap
        p_win = max(0.01, min(0.99, confluence_score / 100.0))
        edge_coeff = (p_win * reward_risk_ratio) - (1.0 - p_win)
        objective.SetCoefficient(k_var, max(0.01, edge_coeff) * loss_per_lot_step)
        objective.SetMaximization()
    else:
        # Strictly minimize risk (choose k_min)
        objective.SetCoefficient(k_var, 1.0)
        objective.SetMinimization()

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

    # Decimal arithmetic for zero floating-point precision drift
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
