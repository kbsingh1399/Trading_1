"""Venue-specific read-only qualification; this module never sends orders."""
import json
import math
import time
from pathlib import Path
from Terminal.risk_envelope import (
    HARD_FLOOR_USD, HALT_CUSHION_USD, STRESS_MULT, EXEC_RESERVE_USD,
    MAX_OPEN_POSITIONS, MIN_RISK_USD, MAX_RISK_USD, budget_per_trade,
    admissible_volume, solve_volume_ortools, validate_trade_geometry,
)

# Legacy proprietary bridges are strictly decommissioned and excised per repository invariants (AGENTS.md Section 3).
# Live execution targets Delta Exchange India REST API exclusively.

def competition_risk_check(*, account_id=None, balance, equity, initial_balance,
                           opening_balance, opening_equity, open_positions,
                           pending_orders, committed_stress, candidate_stress,
                           day_key, platform_day_key):
    """Platform trading day drawdown check; missing or previous-day state fails closed."""
    values = (balance, equity, initial_balance, opening_balance, opening_equity,
              committed_stress, candidate_stress)
    if not day_key or day_key != platform_day_key:
        return False, 'ACCOUNT_OR_DAILY_BASELINE_UNKNOWN'
    if not all(isinstance(x, (int, float)) and math.isfinite(x) and x >= 0 for x in values) or min(values[:5]) <= 0:
        return False, 'INVALID_CAPITAL_STATE'
    if any(type(x) is not int or x < 0 for x in (open_positions, pending_orders)):
        return False, 'UNKNOWN_INVENTORY'
    if open_positions + pending_orders >= 2:
        return False, 'CAPACITY_RESERVED'
    floor = max(0.95 * max(opening_balance, opening_equity), 0.90 * initial_balance)
    if min(balance, equity) - committed_stress - candidate_stress <= floor:
        return False, 'COMPETITION_DRAWDOWN_LIMIT'
    return True, 'OK'


def mt5_account_book(mt5):
    account = mt5.account_info()
    if account is None or getattr(account, 'currency', '') != 'USD':
        raise ValueError('ACCOUNT_IDENTITY_UNKNOWN_OR_MISMATCH')
    positions, orders = mt5.positions_get(), mt5.orders_get()
    if positions is None or orders is None:
        raise ValueError('UNKNOWN_INVENTORY')
    committed = 0.0
    for kind, items in [('position', positions), ('order', orders)]:
        for item in items:
            if item.sl <= 0 or not math.isfinite(item.sl):
                raise ValueError('UNPROTECTED_BOOK')
            side = mt5.ORDER_TYPE_BUY if item.type in (0, 2, 4, 6) else mt5.ORDER_TYPE_SELL
            volume = item.volume if kind == 'position' else item.volume_current
            # Existing positions reserve further loss from executable mark, avoiding
            # entry-loss double counting against already marked account equity.
            start = item.price_open
            if kind == 'position':
                tick = mt5.symbol_info_tick(item.symbol)
                if tick is None:
                    raise ValueError('BOOK_QUOTE_UNKNOWN')
                start = tick.bid if side == mt5.ORDER_TYPE_BUY else tick.ask
            loss = mt5.order_calc_profit(side, item.symbol, volume, start, item.sl)
            if loss is None or not math.isfinite(loss):
                raise ValueError('BOOK_VALUATION_UNKNOWN')
            committed += max(0.0, -loss) * STRESS_MULT + EXEC_RESERVE_USD
    return account, positions, orders, committed


def native_size(mt5, symbol, direction, entry, sl, tp, cap=MAX_RISK_USD):
    """Validate actual broker metadata, quotes and total contingent liabilities."""
    account, positions, orders, committed = mt5_account_book(mt5)
    if len(positions) + len(orders) >= MAX_OPEN_POSITIONS:
        raise ValueError('CAPACITY_RESERVED')
    info, tick = mt5.symbol_info(symbol), mt5.symbol_info_tick(symbol)
    if info is None or tick is None or not 0 < tick.bid <= tick.ask:
        raise ValueError('BROKER_QUOTE_UNKNOWN')
    # Compensate for broker server timezone (e.g., EET UTC+2 or EEST UTC+3) with clock drift tolerance
    raw_tick_s = tick.time_msc / 1000.0
    now_s = time.time()
    diff = raw_tick_s - now_s
    server_offset_sec = round(diff / 3600.0) * 3600 if abs(diff) > 300 else 0
    tick_utc_s = raw_tick_s - server_offset_sec
    age = now_s - tick_utc_s
    if not -30 <= age <= 60:
        raise ValueError('BROKER_CLOCK_OR_QUOTE_STALE')
    spread = (tick.ask - tick.bid) / ((tick.ask + tick.bid) / 2) * 10000
    valid, reason = validate_trade_geometry(direction, entry, sl, tp, spread)
    if not valid:
        raise ValueError(reason)
    budget, ok, reason = budget_per_trade(account.equity, account.balance, committed, stress_native=True)
    if not ok:
        raise ValueError(reason)
    cap = min(cap, budget, MAX_RISK_USD,
              (min(account.balance, account.equity) - HARD_FLOOR_USD - HALT_CUSHION_USD - committed - EXEC_RESERVE_USD) / STRESS_MULT)
    side = mt5.ORDER_TYPE_BUY if direction == 'BUY' else mt5.ORDER_TYPE_SELL
    loss = mt5.order_calc_profit(side, symbol, info.volume_step, entry, sl)
    if loss is None or not math.isfinite(loss) or loss >= 0:
        raise ValueError('BROKER_VALUATION_UNKNOWN')
    lots, reason, risk = solve_volume_ortools(
        loss_per_lot_step=-loss,
        volume_step=info.volume_step,
        min_lot=info.volume_min,
        max_lot=info.volume_max,
        current_equity=account.equity,
        current_balance=account.balance,
        committed_risk_usd=committed,
        active_open_positions=len(positions or []),
        max_risk_cap_usd=cap,
        min_risk_floor_usd=MIN_RISK_USD
    )
    if lots is None:
        raise ValueError(reason)
    margin = mt5.order_calc_margin(side, symbol, lots, entry)
    if margin is None or not math.isfinite(margin) or margin < 0 or margin > account.margin_free:
        raise ValueError('INSUFFICIENT_OR_UNKNOWN_MARGIN')
    return {'lots': lots, 'risk_usd': risk, 'cap_usd': cap, 'spread_bps': spread,
            'margin_usd': margin, 'committed_stress_usd': committed}
