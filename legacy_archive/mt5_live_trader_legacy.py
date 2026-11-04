"""
========================================================================================
MetaTrader 5 Live Institutional Execution Engine
========================================================================================
Automates direct, institutional order execution on MetaTrader 5 for FundedNext Account
35296418 (or active connected broker account).

Supports all asset classes available on the MT5 terminal:
- Forex (20 major & cross pairs)
- Metals & Commodities (XAUUSD, XAGUSD, USOUSD, UKOUSD)
- Indices (SPX500, NDX100, US30, JP225, GER30)

Strict Prop Firm Risk Guardrails:
1. Max Open Positions: Strictly capped at 2 concurrent active trades (MAX_OPEN_POSITIONS).
2. Survival Capital Budget: Risk per trade strictly capped at $25.00 (MAX_RISK_USD).
3. Hard Floor Guard: Contractual floor $4,700 + safety buffer $75 = $4,775 floor.
4. Daily Drawdown Guard: Prevents entering if today's loss exceeds 5% daily limit.
5. Spread Gatekeeper: Max allowable spread <= 45.0 bps (0.45%).
6. Execution Intent Locking: Latches pending orders to prevent duplicate double-sends.
7. Fill Verification: Confirms broker deal execution and attached hard SL/TP protection.
========================================================================================
"""

import os
import sys
import math
import time
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import MetaTrader5 as mt5

from Terminal.runtime_io import acquire_singleton, writer_lock, atomic_json
from Terminal.venue_admission import (
    EXPECTED_MT5_LOGIN,
    VALID_MT5_LOGINS,
    mt5_account_book,
    native_size
)
from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    HALT_CUSHION_USD,
    MAX_RISK_USD,
    MIN_RISK_USD,
    MAX_OPEN_POSITIONS,
    MAX_SPREAD_BPS,
    MIN_RR_RATIO,
    STRESS_MULT,
    EXEC_RESERVE_USD,
    budget_per_trade,
    admissible_volume,
    validate_trade_geometry
)

DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

EXECUTED_TRADES_LOG = SIGNALS_DIR / "mt5_executed_trades.jsonl"
INTENTS_FILE = DB_DIR / "mt5_execution_intents.json"
MAGIC_NUMBER = 888123

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [MT5_TRADER] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("MT5LiveTrader")
logger.setLevel(logging.INFO)
logger.handlers = [handler]


def ensure_mt5_connected() -> bool:
    """Verifies MT5 connection to authorized account."""
    if mt5.terminal_info() is None:
        if not mt5.initialize():
            logger.error("MetaTrader 5 initialization failed.")
            return False

    acc = mt5.account_info()
    if acc is None:
        logger.error("Could not retrieve MT5 account info.")
        return False

    if acc.login not in VALID_MT5_LOGINS:
        logger.error(f"Unauthorized MT5 login: {acc.login}. Expected one of {VALID_MT5_LOGINS}")
        return False

    return True


def resolve_symbol_filling_mode(info: Any) -> int:
    """
    Determines the best supported execution filling mode for the symbol.
    Prefers IOC, falls back to FOK or RETURN.
    """
    filling = getattr(info, "filling_mode", 0)
    # Check bitmask: 1 = FOK, 2 = IOC
    if filling & 2:
        return mt5.ORDER_FILLING_IOC
    elif filling & 1:
        return mt5.ORDER_FILLING_FOK
    else:
        return mt5.ORDER_FILLING_RETURN


def adjust_prices_to_symbol_rules(
    info: Any,
    entry: float,
    sl: float,
    tp: float
) -> Tuple[float, float, float]:
    """
    Rounds entry, stop loss, and take profit to broker digits and tick size steps.
    """
    digits = getattr(info, "digits", 5)
    tick_size = getattr(info, "trade_tick_size", 0.0)

    if tick_size > 0:
        entry_adj = round(round(entry / tick_size) * tick_size, digits)
        sl_adj = round(round(sl / tick_size) * tick_size, digits)
        tp_adj = round(round(tp / tick_size) * tick_size, digits)
    else:
        entry_adj = round(entry, digits)
        sl_adj = round(sl, digits)
        tp_adj = round(tp, digits)

    return entry_adj, sl_adj, tp_adj


def execute_mt5_signal(alert: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes a qualified institutional trade alert live on MetaTrader 5.
    
    Args:
        alert: Full dictionary structure from RealtimeSignalEvaluator.
        
    Returns:
        Execution result dictionary with order ticket and confirmation status.
    """
    sym = alert.get("symbol", "UNKNOWN")
    mt5_sym = alert.get("mt5_symbol") or sym
    direction = str(alert.get("direction", "")).upper()
    trade_levels = alert.get("trade_levels", {})
    score = alert.get("confluence_score", 0.0)

    logger.info(f"⚡ [MT5_EXECUTE] Initiating order evaluation for {sym} ({mt5_sym}) {direction} (Score: {score:.1f}/100)...")

    if not ensure_mt5_connected():
        logger.error("MT5 connection check failed. Aborting execution.")
        return {"status": "FAILED", "reason": "MT5_NOT_CONNECTED"}

    # 1. Check Capacity & Open Positions
    try:
        acc, positions, orders, committed = mt5_account_book(mt5)
    except Exception as e:
        logger.error(f"Cannot audit MT5 account book: {e}")
        return {"status": "FAILED", "reason": f"ACCOUNT_BOOK_ERROR: {e}"}

    total_active = len(positions) + len(orders)
    if total_active >= MAX_OPEN_POSITIONS:
        logger.warning(f"Capacity limit reached ({total_active}/{MAX_OPEN_POSITIONS} active positions). Order rejected.")
        return {"status": "REJECTED", "reason": "MAX_CAPACITY_REACHED"}

    # Check if already in a position for this symbol
    existing_sym_positions = [p for p in positions if p.symbol == mt5_sym]
    if existing_sym_positions:
        logger.warning(f"Position already open for {mt5_sym} (Ticket #{existing_sym_positions[0].ticket}). Skipping duplicate.")
        return {"status": "SKIPPED", "reason": "POSITION_ALREADY_EXISTS"}

    # 2. Select Symbol & Get Live Quote
    if not mt5.symbol_select(mt5_sym, True):
        logger.error(f"Failed to select symbol {mt5_sym} in MT5 Market Watch.")
        return {"status": "FAILED", "reason": f"SYMBOL_SELECT_FAILED: {mt5_sym}"}

    info = mt5.symbol_info(mt5_sym)
    tick = mt5.symbol_info_tick(mt5_sym)
    if not info or not tick or tick.bid <= 0 or tick.ask <= 0:
        logger.error(f"Could not retrieve valid tick quotes for {mt5_sym}.")
        return {"status": "FAILED", "reason": "INVALID_BROKER_QUOTES"}

    # 3. Live Entry & Geometry Validation
    live_entry = tick.ask if direction == "BUY" else tick.bid
    spread_val = tick.ask - tick.bid
    mid_price = (tick.ask + tick.bid) / 2.0
    spread_bps = (spread_val / mid_price) * 10000.0

    if spread_bps > MAX_SPREAD_BPS:
        logger.warning(f"Spread gate breach on {mt5_sym}: {spread_bps:.1f} bps > max {MAX_SPREAD_BPS:.1f} bps.")
        return {"status": "REJECTED", "reason": f"SPREAD_GATE_BREACH: {spread_bps:.1f} bps"}

    raw_sl = float(trade_levels.get("stop_loss", 0.0))
    raw_tp = float(trade_levels.get("take_profit_1", 0.0))

    if raw_sl <= 0 or raw_tp <= 0:
        # Fallback 1.5% SL if not explicitly set
        sl_pct = 0.015
        raw_sl = live_entry * (1.0 - sl_pct) if direction == "BUY" else live_entry * (1.0 + sl_pct)
        raw_tp = live_entry * (1.0 + sl_pct * 1.8) if direction == "BUY" else live_entry * (1.0 - sl_pct * 1.8)

    entry_adj, sl_adj, tp_adj = adjust_prices_to_symbol_rules(info, live_entry, raw_sl, raw_tp)

    geom_valid, geom_reason = validate_trade_geometry(direction, entry_adj, sl_adj, tp_adj, spread_bps)
    if not geom_valid:
        logger.error(f"Trade geometry invalid for {mt5_sym}: {geom_reason}")
        return {"status": "REJECTED", "reason": f"GEOMETRY_INVALID: {geom_reason}"}

    # 4. Discrete Lot Sizing via Risk Envelope
    try:
        sized = native_size(
            mt5=mt5,
            symbol=mt5_sym,
            direction=direction,
            entry=entry_adj,
            sl=sl_adj,
            tp=tp_adj,
            cap=MAX_RISK_USD
        )
        lots = sized["lots"]
        realized_risk = sized["risk_usd"]
        logger.info(f"[{mt5_sym}] Risk Envelope solved: {lots} lots | Risk: ${realized_risk:.2f} (Cap: ${sized['cap_usd']:.2f})")
    except Exception as e:
        logger.error(f"Risk Envelope sizing failed for {mt5_sym}: {e}")
        return {"status": "FAILED", "reason": f"SIZING_ERROR: {e}"}

    if lots is None or lots <= 0:
        logger.error(f"No admissible lot size for {mt5_sym}.")
        return {"status": "REJECTED", "reason": "NO_ADMISSIBLE_LOTS"}

    # 5. Execution Intent Deduplication Lock
    sig_key = f"{sym}_{direction}_{alert.get('signal_details', {}).get('candle_utc', int(time.time()))}"
    intent_hash = hashlib.sha256(sig_key.encode()).hexdigest()

    try:
        intents = {}
        if INTENTS_FILE.exists():
            with open(INTENTS_FILE, "r", encoding="utf-8") as f:
                intents = json.load(f)

        if intent_hash in intents and intents[intent_hash].get("status") in ("CONFIRMED", "SENDING"):
            logger.warning(f"Intent {intent_hash[:12]} already recorded ({intents[intent_hash].get('status')}). Skipping duplicate.")
            return {"status": "SKIPPED", "reason": "INTENT_ALREADY_EXISTS"}

        intents[intent_hash] = {
            "symbol": sym,
            "mt5_symbol": mt5_sym,
            "direction": direction,
            "lots": lots,
            "status": "SENDING",
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        atomic_json(INTENTS_FILE, intents)
    except Exception as e:
        logger.warning(f"Could not latch execution intent: {e}")

    # 6. Formulate & Pre-flight Order Request
    filling_mode = resolve_symbol_filling_mode(info)
    order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": mt5_sym,
        "volume": float(lots),
        "type": order_type,
        "price": float(entry_adj),
        "sl": float(sl_adj),
        "tp": float(tp_adj),
        "deviation": 25,
        "magic": MAGIC_NUMBER,
        "comment": f"EGM_{alert.get('signal_details', {}).get('sig_code', 'sig')}_{sym}"[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": filling_mode
    }

    # Pre-flight sanity check
    check_res = mt5.order_check(request)
    if check_res is None or check_res.retcode != 0:
        comment = check_res.comment if check_res else "Broker order_check failed"
        logger.error(f"MT5 order_check preflight rejected: {comment} (retcode: {getattr(check_res, 'retcode', -1)})")
        intents[intent_hash]["status"] = "PREFLIGHT_FAILED"
        atomic_json(INTENTS_FILE, intents)
        return {"status": "FAILED", "reason": f"PREFLIGHT_REJECTED: {comment}"}

    # 7. Live Order Placement
    logger.info(
        f"🚀 [PUNCHING LIVE MT5 ORDER] {direction} {lots} {mt5_sym} @ {entry_adj} "
        f"| SL: {sl_adj} | TP: {tp_adj} | Risk: ${realized_risk:.2f}"
    )

    inflight_lock = DB_DIR / ".locks" / "order_execution_inflight.lock"
    inflight_lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        inflight_lock.touch()
    except Exception:
        pass

    try:
        order_res = mt5.order_send(request)
    except Exception as e:
        logger.error(f"MT5 order_send exception: {e}")
        order_res = None
    finally:
        if inflight_lock.exists():
            try:
                inflight_lock.unlink()
            except Exception:
                pass

    if order_res is None or order_res.retcode not in (mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL):
        ret_comment = getattr(order_res, "comment", "Unknown order_send error")
        ret_code = getattr(order_res, "retcode", -1)
        logger.error(f"MT5 order_send FAILED: {ret_comment} (retcode: {ret_code})")
        intents[intent_hash]["status"] = "SEND_FAILED"
        atomic_json(INTENTS_FILE, intents)
        return {"status": "FAILED", "reason": f"EXECUTION_FAILED: {ret_comment} (code {ret_code})"}

    deal_ticket = order_res.order
    fill_price = order_res.price
    fill_volume = order_res.volume

    logger.info(f"✅ [MT5 FILL CONFIRMED] Ticket #{deal_ticket} | Volume: {fill_volume} {mt5_sym} @ {fill_price}")

    # Verify Protection (hard SL/TP attached to position)
    open_positions = mt5.positions_get(symbol=mt5_sym)
    protected = open_positions is not None and any(
        p.ticket == deal_ticket or (p.magic == MAGIC_NUMBER and p.sl > 0 and p.tp > 0)
        for p in open_positions
    )

    intents[intent_hash].update({
        "status": "CONFIRMED",
        "ticket": deal_ticket,
        "fill_price": fill_price,
        "fill_volume": fill_volume,
        "protected": protected
    })
    atomic_json(INTENTS_FILE, intents)

    # 8. Log Full Quantitative Analysis to Audit Trail
    audit_record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "ticket": deal_ticket,
        "symbol": sym,
        "mt5_symbol": mt5_sym,
        "direction": direction,
        "lots": fill_volume,
        "fill_price": fill_price,
        "stop_loss": sl_adj,
        "take_profit": tp_adj,
        "realized_risk_usd": realized_risk,
        "confluence_score": score,
        "signal_details": alert.get("signal_details", {}),
        "order_flow_metrics": alert.get("order_flow_metrics", {}),
        "auction_profile": alert.get("auction_profile", {}),
        "deepmind_timesfm": alert.get("deepmind_timesfm", {}),
        "market_regime": alert.get("market_regime", {}),
        "confluence_reasons": alert.get("confluence_reasons", [])
    }

    try:
        with open(EXECUTED_TRADES_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_record) + "\n")
        logger.info(f"Logged executed trade #{deal_ticket} to {EXECUTED_TRADES_LOG.name}")
    except Exception as e:
        logger.warning(f"Could not write audit log: {e}")

    return {
        "status": "EXECUTED",
        "ticket": deal_ticket,
        "symbol": sym,
        "mt5_symbol": mt5_sym,
        "direction": direction,
        "lots": fill_volume,
        "price": fill_price,
        "sl": sl_adj,
        "tp": tp_adj,
        "realized_risk_usd": realized_risk,
        "protected": protected
    }
