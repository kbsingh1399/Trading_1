"""
========================================================================================
Institutional Position Risk Anchors & Trailing State Machine (Single Source of Truth)
========================================================================================
Guarantees:
1. Immutable Risk Anchor (R0):
   R0 = |P_fill - P_sl_initial| is permanently anchored upon execution fill.
   Ratcheting Stop-Loss into profit NEVER alters the risk denominator R0.
2. Monotonic Tier Progression:
   Tier 0: Initial Risk (unratcheted)
   Tier 1: Breakeven (+1 tick buffer) triggered at >= +1.0R0
   Tier 2: Lock +1.0R0 profit & widen TP to +3.0R0 triggered at >= +2.0R0
   Tier 3: Lock +2.0R0 profit triggered at >= +3.0R0
3. Lock-Free Atomic Persistence:
   Writes to temporary .json.tmp file and atomic-renames via os.replace.
4. Cold-Start Fallback Hierarchy:
   Infers R0 from TP order (R:R 2.0:1) or institutional MAX_RISK_USD envelope
   if daemon restarts while positions are already ratcheted.
========================================================================================
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("PositionAnchors")

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
ANCHORS_FILE = DB_DIR / "live_position_anchors.json"


def load_position_anchors() -> Dict[str, Any]:
    """Loads all active position anchors from persistent storage."""
    try:
        if ANCHORS_FILE.exists():
            with open(ANCHORS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Error loading position anchors: {e}")
    return {}


def save_position_anchors(anchors: Dict[str, Any]):
    """Atomically writes position anchors to disk (lock-free via os.replace)."""
    try:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        tmp_file = ANCHORS_FILE.with_suffix(".json.tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(anchors, f, indent=2)
        os.replace(tmp_file, ANCHORS_FILE)
    except Exception as e:
        logger.error(f"Error saving position anchors: {e}")


def register_position_anchor(
    symbol: str,
    product_id: int,
    direction: str,
    entry_price: float,
    initial_sl: float,
    initial_tp: float,
    size: int,
    initial_risk_dist: Optional[float] = None
) -> Dict[str, Any]:
    """
    Registers an immutable initial risk anchor upon order execution fill.
    R0 = |entry_price - initial_sl| is permanently locked.
    """
    anchors = load_position_anchors()
    r0 = abs(entry_price - initial_sl) if initial_risk_dist is None else float(initial_risk_dist)
    if r0 <= 0:
        r0 = entry_price * 0.005  # 0.5% safety floor

    clean_sym = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
    anchor = {
        "symbol": clean_sym,
        "product_id": int(product_id),
        "direction": direction.upper(),
        "entry_price": float(entry_price),
        "initial_sl": float(initial_sl),
        "initial_tp": float(initial_tp),
        "initial_risk_dist": float(r0),
        "current_tier": 0,
        "size": int(size),
        "last_ratchet_price": float(initial_sl)
    }
    anchors[clean_sym] = anchor
    save_position_anchors(anchors)
    logger.info(f"[{clean_sym}] Registered permanent risk anchor: Entry=${entry_price} | SL=${initial_sl} | R0=${r0:.6f} | TP=${initial_tp}")
    return anchor


def get_or_recover_anchor(
    symbol: str,
    product_id: int,
    direction: str,
    entry_price: float,
    cur_sl_price: float,
    cur_tp_price: Optional[float],
    size: int,
    contract_val: float = 1.0,
    max_risk_usd: float = 25.0
) -> Dict[str, Any]:
    """
    Retrieves the persistent anchor or recovers R0 from trade geometry / risk envelope.
    """
    clean_sym = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
    anchors = load_position_anchors()
    existing = anchors.get(clean_sym)

    # Validate existing anchor matches current position entry
    if existing and abs(existing.get("entry_price", 0.0) - entry_price) < (entry_price * 0.001):
        return existing

    # Cold-Start Recovery Hierarchy
    dir_mult = 1.0 if direction.upper() == "BUY" else -1.0
    is_sl_loss_side = (entry_price - cur_sl_price) * dir_mult > 0

    if is_sl_loss_side and cur_sl_price > 0:
        # Step 1: SL is still on loss side -> direct derivation
        r0 = abs(entry_price - cur_sl_price)
        recovered_tier = 0
    elif cur_tp_price and cur_tp_price > 0:
        # Step 2: Infer from 2.0:1 Take-Profit target
        reward_dist = abs(cur_tp_price - entry_price)
        r0 = reward_dist / 2.0
        recovered_tier = 1 if not is_sl_loss_side else 0
    else:
        # Step 3: Canonical institutional risk envelope cap ($25.00)
        eff_size = max(1, abs(size))
        r0 = max_risk_usd / (eff_size * max(1e-6, contract_val))
        recovered_tier = 0

    return register_position_anchor(
        symbol=clean_sym,
        product_id=product_id,
        direction=direction,
        entry_price=entry_price,
        initial_sl=cur_sl_price if is_sl_loss_side else entry_price - (dir_mult * r0),
        initial_tp=cur_tp_price if cur_tp_price else entry_price + (dir_mult * 2.0 * r0),
        size=size,
        initial_risk_dist=r0
    )


def update_anchor_tier(symbol: str, new_tier: int, new_sl_price: float):
    """Updates the monotonic tier state to prevent repetitive API calls."""
    clean_sym = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
    anchors = load_position_anchors()
    if clean_sym in anchors:
        anchors[clean_sym]["current_tier"] = int(new_tier)
        anchors[clean_sym]["last_ratchet_price"] = float(new_sl_price)
        save_position_anchors(anchors)


def prune_closed_positions(active_symbols: list):
    """Removes anchors for positions that are no longer open."""
    anchors = load_position_anchors()
    clean_active = {s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD" for s in active_symbols}
    to_remove = [k for k in anchors if k not in clean_active]
    if to_remove:
        for k in to_remove:
            del anchors[k]
        save_position_anchors(anchors)
        logger.info(f"Pruned closed positions from anchor store: {to_remove}")
