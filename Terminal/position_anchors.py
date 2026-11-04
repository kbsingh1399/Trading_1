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
    """Atomically writes position anchors to disk (lock-free via os.replace with Windows retry)."""
    import time
    DB_DIR.mkdir(parents=True, exist_ok=True)
    tmp_file = ANCHORS_FILE.with_suffix(".json.tmp")
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(anchors, f, indent=2)
        for attempt in range(5):
            try:
                os.replace(tmp_file, ANCHORS_FILE)
                break
            except (PermissionError, OSError):
                if attempt == 4:
                    raise
                time.sleep(0.05)
    except Exception as e:
        logger.error(f"Error saving position anchors: {e}")
        if tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass


def register_position_anchor(
    symbol: str,
    product_id: int,
    direction: str,
    entry_price: float,
    initial_sl: float,
    initial_tp: float,
    size: int,
    initial_risk_dist: Optional[float] = None,
    current_tier: int = 0,
    contract_val: float = 1.0
) -> Dict[str, Any]:
    """
    Registers an immutable initial risk anchor upon order execution fill.
    R0 = |entry_price - initial_sl| is permanently locked.
    Idempotent: will not reset an existing anchor at a higher tier.

    PATCH P2 (Phase 2): persists contract_val + initial_risk_usd so the
    committed-risk aggregator (risk_envelope.aggregate_committed_risk) can
    compute exact outstanding stop-loss risk in USD without re-resolving
    product metadata. Legacy anchors without these fields are handled
    fail-closed by the aggregator.
    """
    anchors = load_position_anchors()
    clean_sym = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
    existing = anchors.get(clean_sym)

    # Protect existing active ratcheted positions from denominator collapse
    if existing and abs(existing.get("entry_price", 0.0) - float(entry_price)) < (float(entry_price) * 0.001):
        if existing.get("current_tier", 0) > 0 or existing.get("initial_risk_dist", 0.0) > 0:
            logger.info(f"[{clean_sym}] Preserving existing anchor (Tier {existing.get('current_tier', 0)}, R0={existing.get('initial_risk_dist', 0.0):.6f})")
            return existing

    r0 = abs(entry_price - initial_sl) if initial_risk_dist is None else float(initial_risk_dist)
    if r0 <= 0:
        r0 = entry_price * 0.005  # 0.5% safety floor

    anchor = {
        "symbol": clean_sym,
        "product_id": int(product_id),
        "direction": direction.upper(),
        "entry_price": float(entry_price),
        "initial_sl": float(initial_sl),
        "initial_tp": float(initial_tp),
        "initial_risk_dist": float(r0),
        "current_tier": int(current_tier),
        "size": int(size),
        "last_ratchet_price": float(initial_sl),
        # PATCH P2: committed-risk aggregation fields
        "contract_val": float(contract_val),
        "initial_risk_usd": round(float(r0) * abs(int(size)) * float(contract_val), 2)
    }
    anchors[clean_sym] = anchor
    save_position_anchors(anchors)
    logger.info(f"[{clean_sym}] Registered permanent risk anchor: Entry=${entry_price} | SL=${initial_sl} | R0=${r0:.6f} | TP=${initial_tp} | Tier={current_tier}")
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

    # PATCH A1: Cold-Start Recovery Hierarchy — tier-aware R0 inference.
    # The profit-side SL is itself tier information:
    #   Tier 1: SL = breakeven (+1 tick)  -> |SL-lock| / TP-dist ~ 0
    #   Tier 2: SL locks +1.0R0, TP widened to +3.0R0 -> ratio ~ 1/3
    #   Tier 3: SL locks +2.0R0, TP at +4.0R0           -> ratio ~ 1/2
    # The ratio discriminator is TICK-AGNOSTIC (a 1bp price tolerance missed the
    # +1-tick BE buffer on small-tick instruments like HBAR). The old TP/2
    # inference recovered r0 = 1.5x-true at Tier 2, corrupting every subsequent
    # trailing threshold after a mid-trade daemon restart.
    dir_mult = 1.0 if direction.upper() == "BUY" else -1.0
    sl_dist_signed = (entry_price - cur_sl_price) * dir_mult if cur_sl_price > 0 else 0.0
    is_sl_loss_side = cur_sl_price > 0 and sl_dist_signed > 0
    tp_dist = abs(cur_tp_price - entry_price) if (cur_tp_price and cur_tp_price > 0) else 0.0
    eff_size = max(1, abs(size))
    env_r0 = max_risk_usd / (eff_size * max(1e-6, contract_val))

    if cur_sl_price <= 0:
        # No stop present (naked recovery): r0 from the unwidened 2R0 TP, else envelope.
        if tp_dist > 0:
            r0 = tp_dist / 2.0
            recovered_tier = 1
        else:
            r0 = env_r0
            recovered_tier = 0
    elif is_sl_loss_side:
        # Step 1: SL still on the loss side -> direct derivation (Tier 0)
        r0 = abs(entry_price - cur_sl_price)
        recovered_tier = 0
    else:
        # SL on the profit side: s = locked distance in R0 units.
        s = abs(sl_dist_signed)
        ratio = (s / tp_dist) if tp_dist > 0 else 1.0   # no TP -> treat as conservative
        if ratio < 0.05:
            # Tier 1 signature: SL at ~breakeven (+1 tick); r0 from unwidened 2R0 TP
            r0 = (tp_dist / 2.0) if tp_dist > 0 else env_r0
            recovered_tier = 1
        elif tp_dist > 0 and abs(ratio - (1.0 / 3.0)) <= 0.15 * (1.0 / 3.0):
            # Tier 2 signature: SL locks +1R0 (ratio 1/3 of the +3R0 widened TP)
            r0 = s
            recovered_tier = 2
        elif tp_dist > 0 and abs(ratio - 0.5) <= 0.15 * 0.5:
            # Tier 3 signature: SL locks +2R0, TP at +4R0 (ratio 1/2)
            r0 = s / 2.0
            recovered_tier = 3
        else:
            # Ambiguous (TP edit may have failed mid-widen): conservative default.
            # r0 = s reproduces exact math for Tier 2 and only *understates* r_gain
            # for Tier 3 — whose SL is already locked at the maximum tier, so no
            # further ratchet is possible anyway. Never overstate r0.
            r0 = s
            recovered_tier = 2

    return register_position_anchor(
        symbol=clean_sym,
        product_id=product_id,
        direction=direction,
        entry_price=entry_price,
        initial_sl=cur_sl_price if cur_sl_price > 0 else entry_price - (dir_mult * r0),
        initial_tp=cur_tp_price if cur_tp_price else entry_price + (dir_mult * 2.0 * r0),
        size=size,
        initial_risk_dist=r0,
        current_tier=recovered_tier,
        contract_val=contract_val
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
    clean_active = {
        s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
        for s in active_symbols if s
    }
    to_remove = [k for k in anchors if k not in clean_active]
    if to_remove:
        for k in to_remove:
            anchors.pop(k, None)
        save_position_anchors(anchors)
        logger.info(f"Pruned closed positions from anchor store: {to_remove}")
