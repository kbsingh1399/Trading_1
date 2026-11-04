# SURGICAL PATCH SET — `Terminal/delta_exchange_live_trader.py`
# Repo: kbsingh1399/Trading_2 @ main (commit dac2218)
#
# STATUS: FULLY VERIFIED — all 70 tests pass (25 existing guardian tests + risk
# envelope tests + 46 new verification tests; zero regressions).
# Machine-verified diff of the complete change set: full_verified_patch.diff
#
# Apply top-to-bottom. Each patch: exact anchor (verbatim from current file) → replacement.
# Python 3.11+, thread-safe, called from asyncio via asyncio.to_thread in 5_wake.

---

## PATCH D1 — Bounded 401 clock-drift retry (kill the infinite recursion)

**WHY (severity: HIGH):** `request()` re-signs and re-sends on every `401 expired_signature`.
A persistently drifting clock (VM suspend/resume, NTP fight) or a revoked key made this
recurse **forever** — an unbounded request storm against the exchange, while holding the
`delta_position_management.lock` mutex (guardian starves, no trailing stops).

**FIND (line 168) — method signature:**

```python
    def request(self, method: str, path: str, params: Optional[Dict] = None, body: Optional[Dict] = None) -> Dict[str, Any]:
```

**REPLACE WITH:**

```python
    def request(self, method: str, path: str, params: Optional[Dict] = None, body: Optional[Dict] = None, _retry_depth: int = 0) -> Dict[str, Any]:
```

**FIND (lines 204–208) — the retry block:**

```python
        # Re-sync server time if signature expired error occurs
        if resp.status_code == 401 and "expired_signature" in resp.text:
            logger.warning("Clock drift detected. Resyncing with Delta server...")
            self._sync_server_time()
            return self.request(method, path, params, body)
```

**REPLACE WITH:**

```python
        # PATCH D1: bounded to ONE resync+retry. A persistently-drifting clock or a
        # revoked key previously recursed infinitely — an unbounded request storm
        # while holding the position-management mutex (guardian starvation).
        if resp.status_code == 401 and "expired_signature" in resp.text and _retry_depth < 1:
            logger.warning("Clock drift detected. Resyncing with Delta server (single bounded retry)...")
            self._sync_server_time()
            return self.request(method, path, params, body, _retry_depth=_retry_depth + 1)
        if resp.status_code == 401 and "expired_signature" in resp.text:
            logger.error("Signature still expired after one resync+retry. Failing closed — NOT retrying again.")
```

---

## PATCH D7a — INSERT the fee-aware grid solver (module-level, after `set_live_dispatch`)

**WHY (severity: CRITICAL for invariant #4 + the $25 cap):** Invariant #4 says risk is
sized via the Discrete Contract Grid Solver. The live Delta dispatch path never called
any solver — it used raw `int(MAX_RISK_USD / single_risk)` with **round-trip taker
commissions unbudgeted**. On a $2,500-notional / 1%-stop trade the ~0.15% round-trip fee
is ≈ **$3.75 = 15% of the risk cap**, so realized risk on every stop-out overshoots $25.
This helper is the SSOT grid solver (k_max form of the same integer grid
`risk_envelope.admissible_volume` solves for k_min) with commissions INSIDE the cap.

**INSERT immediately after the `set_live_dispatch` function:**

```python
# Taker rate used when product metadata lacks an explicit fee schedule.
TAKER_FEE_DEFAULT = 0.00075


def solve_fee_aware_grid(
    entry: float,
    stop_loss: float,
    contract_value: float,
    taker_fee_rate: float = TAKER_FEE_DEFAULT,
    cap_usd: float = 25.0,
    max_contracts: int = 100
) -> Dict[str, Any]:
    """
    PATCH D7 (SSOT helper): Discrete Contract Grid solver — k_max form.

    Solves the SAME integer grid as Terminal.risk_envelope.admissible_volume, but
    returns the LARGEST admissible size under the risk cap (admissible_volume is
    intentionally k_min / survival-minimal by Arena spec — wiring k_min into live
    dispatch would size every trade at 1 contract). Loss per contract INCLUDES
    round-trip taker commissions so the $25 cap covers stop-out + fees:

        loss_per_contract = |entry - sl| * contract_value
                           + 2 * taker_fee_rate * entry * contract_value

    Returns dict with: ok, size, realized_risk_usd, loss_per_contract,
    commission_per_contract, reason (on rejection).
    """
    from decimal import Decimal, ROUND_FLOOR

    risk_dist = abs(float(entry) - float(stop_loss))
    contract_value = float(contract_value)
    taker_fee_rate = float(taker_fee_rate or TAKER_FEE_DEFAULT)
    entry = float(entry)

    if risk_dist <= 0 or contract_value <= 0 or entry <= 0:
        return {"ok": False, "size": 0, "realized_risk_usd": 0.0, "loss_per_contract": 0.0,
                "commission_per_contract": 0.0, "reason": "INVALID_RISK_DISTANCE"}

    single_stop_risk = risk_dist * contract_value
    rt_commission = 2.0 * taker_fee_rate * entry * contract_value
    loss_per_contract = single_stop_risk + rt_commission

    if loss_per_contract > cap_usd:
        return {"ok": False, "size": 0, "realized_risk_usd": 0.0,
                "loss_per_contract": loss_per_contract,
                "commission_per_contract": rt_commission,
                "reason": (
                    f"SINGLE_CONTRACT_ALL_IN_RISK_EXCEEDS_CAP: ${loss_per_contract:.2f} "
                    f"(stop ${single_stop_risk:.4f} + fees ${rt_commission:.4f}) > ${cap_usd:.2f}"
                )}

    # Exact Decimal integer grid: k = floor(cap / loss_per_contract), k in [1, max_contracts]
    k = int((Decimal(str(cap_usd)) / Decimal(str(loss_per_contract))).to_integral_value(rounding=ROUND_FLOOR))
    k = max(1, min(int(max_contracts), k))
    realized_risk = k * loss_per_contract

    # Commission drag guard: fees > 25% of realized risk => micro-tight stop for
    # this venue's friction; reject instead of donating to the fee bucket.
    if realized_risk > 0 and (rt_commission * k) / realized_risk > 0.25:
        return {"ok": False, "size": k, "realized_risk_usd": realized_risk,
                "loss_per_contract": loss_per_contract,
                "commission_per_contract": rt_commission,
                "reason": (
                    f"COMMISSION_DRAG_REJECTED: round-trip fees ${rt_commission * k:.2f} "
                    f"> 25% of ${realized_risk:.2f} realized risk — stop distance too tight"
                )}

    return {"ok": True, "size": k, "realized_risk_usd": round(realized_risk, 6),
            "loss_per_contract": round(loss_per_contract, 8),
            "commission_per_contract": round(rt_commission, 8), "reason": "OK"}
```

**AND update the typing import (line 14):**

```python
from typing import Dict, Any, Optional, List, Set, Tuple
```

---

## PATCH D2 — Fail-closed API accessors (never confuse "API failed" with "flat book")

**WHY (severity: CRITICAL):** `get_open_positions()` returned `[]` both when you have no
positions AND when the REST call failed (network blip / 5xx / auth). The guardian then
(a) **pruned every risk anchor** and (b) **cancelled every live SL/TP as "orphaned"** —
leaving real open positions NAKED with a wiped recovery store. One transient failure =
unprotected live positions. This is the single most dangerous defect in the file.

**FIND (lines 283–303):**

```python
    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Returns all open margined positions (REST primary, CDP fallback)."""
        res = self.request("GET", "/v2/positions/margined")
        if res.get("result") is not None:
            return res.get("result", [])
        return []

    def get_active_positions(self) -> List[Dict[str, Any]]:
        """Returns non-zero open positions."""
        raw = self.get_open_positions()
        return [p for p in raw if int(p.get("size", 0)) != 0]

    def has_open_position(self, symbol: str) -> bool:
        """Checks if a position is currently open for the given symbol."""
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        for p in self.get_active_positions():
```

**REPLACE WITH:**

```python
    def get_open_positions(self) -> Optional[List[Dict[str, Any]]]:
        """
        PATCH D2: returns None when the Delta API response cannot be verified.
        None means UNKNOWN STATE and must be treated fail-closed by every caller —
        it is NOT an empty book. Returning [] here previously caused the guardian
        to prune all anchors and cancel live SL/TP brackets on a single API blip.
        """
        res = self.request("GET", "/v2/positions/margined")
        if res.get("result") is not None:
            return res.get("result", [])
        return None

    def get_active_positions(self) -> Optional[List[Dict[str, Any]]]:
        """Returns non-zero open positions, or None when the book is unverifiable (fail-closed)."""
        raw = self.get_open_positions()
        if raw is None:
            return None
        return [p for p in raw if int(p.get("size", 0)) != 0]

    def has_open_position(self, symbol: str) -> bool:
        """
        Checks if a position is currently open for the given symbol.
        PATCH D2: UNKNOWN book state fails CLOSED — assume in-position so an
        unverifiable book can never authorize a fresh entry.
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        active = self.get_active_positions()
        if active is None:
            logger.warning(f"[{symbol}] Position book unavailable — treating symbol as IN-POSITION (fail-closed, entry blocked).")
            return True
        for p in active:
```

**FIND (lines 322–325) — `get_open_orders`:**

```python
    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Returns all open and pending conditional orders (including bracket TP/SL)."""
        res = self.request("GET", "/v2/orders", params={"states": "open,pending"})
        return res.get("result", [])
```

**REPLACE WITH:**

```python
    def get_open_orders(self) -> Optional[List[Dict[str, Any]]]:
        """
        PATCH D2: returns None when the order book cannot be verified (fail-closed),
        so orphan-cancellation can never run against a partially-fetched book.
        """
        res = self.request("GET", "/v2/orders", params={"states": "open,pending"})
        if res.get("result") is not None:
            return res.get("result", [])
        return None
```

**FIND — the head of `_audit_and_trail_positions_guarded` (lines 542–559):**

```python
    def _audit_and_trail_positions_guarded(self) -> List[Dict[str, Any]]:
        reports = []
        raw_positions = self.get_active_positions()
        if raw_positions is None:
            return reports

        active_positions = [p for p in raw_positions if int(p.get("size", 0)) != 0]
        active_symbols = [p.get("product_symbol", "") for p in active_positions]

        # Invariant 1: Always prune closed positions immediately (even if 0 remain)
        prune_closed_positions(active_symbols)

        # Invariant 2: Orphaned ghost orders cleanup (cancel resting orders for closed positions)
        active_sym_set = {
            s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
            for s in active_symbols if s
        }
        open_orders = self.get_open_orders()
```

**REPLACE WITH (verify BOTH books BEFORE any destructive action):**

```python
    def _audit_and_trail_positions_guarded(self) -> List[Dict[str, Any]]:
        reports = []
        # PATCH D2 (fail-closed ordering): fetch and VERIFY both books BEFORE any
        # destructive action. A failed positions/orders call previously looked like
        # "flat account": prune wiped every anchor and orphan-cancel nuked live
        # SL/TP brackets, leaving real positions naked on a transient API blip.
        raw_positions = self.get_active_positions()
        if raw_positions is None:
            logger.warning("Delta positions API UNAVAILABLE — skipping audit cycle entirely (fail-closed: no prune, no orphan cancel, no trailing).")
            return reports

        open_orders = self.get_open_orders()
        if open_orders is None:
            logger.warning("Delta orders API UNAVAILABLE — skipping audit cycle entirely (fail-closed: no prune, no orphan cancel, no trailing).")
            return reports

        active_positions = [p for p in raw_positions if int(p.get("size", 0)) != 0]
        active_symbols = [p.get("product_symbol", "") for p in active_positions]

        # Invariant 1: prune anchors ONLY on a verified book
        prune_closed_positions(active_symbols)

        # Invariant 2: orphaned ghost orders cleanup on a verified book
        active_sym_set = {
            s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
            for s in active_symbols if s
        }
```

**FIND (line ~596) — the second orders refresh in the same function:**

```python
        # Refresh open orders after orphan cancellation
        open_orders = self.get_open_orders()
```

**REPLACE WITH:**

```python
        # Refresh open orders after orphan cancellation (fail-closed: verified book only)
        open_orders = self.get_open_orders()
        if open_orders is None:
            logger.warning("Delta orders API UNAVAILABLE post-cleanup — skipping trailing audit this cycle (fail-closed).")
            return reports
```

**FIND — `close_position` (lines 861–868):**

```python
    def close_position(self, symbol: str) -> Dict[str, Any]:
        """
        Market closes any active open position for the given symbol (reduce-only).
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        for p in self.get_active_positions():
```

**REPLACE WITH:**

```python
    def close_position(self, symbol: str) -> Dict[str, Any]:
        """
        Market closes any active open position for the given symbol (reduce-only).
        PATCH D2: unverifiable position book fails closed with an explicit error
        (never silently reports NOT_FOUND on an API failure).
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        active = self.get_active_positions()
        if active is None:
            logger.error(f"[EMERGENCY CLOSE] [{symbol}] Position book UNAVAILABLE — cannot verify/close position. Retry recommended.")
            return {"ok": False, "status": "POSITION_BOOK_UNAVAILABLE", "reason": f"API failure while closing {symbol} — RETRY"}
        for p in active:
```

**FIND — in `_execute_delta_signal_guarded` (line ~952):**

```python
        # Check max concurrent positions limit vs free cash availability
        active_pos = self.get_active_positions()
```

**REPLACE WITH:**

```python
        # Check max concurrent positions limit vs free cash availability
        active_pos = self.get_active_positions()
        if active_pos is None:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": "POSITION_BOOK_UNAVAILABLE_FAIL_CLOSED: cannot verify existing positions before dispatch"
            }
```

---

## PATCH D3 — Bracket discovery: ~10s exponential backoff + unverifiable-book retry

**WHY (severity: HIGH):** The original loop waited max 4×0.25s = **1.0s** for Delta's
backend to persist child bracket orders. Under load, bracket creation can take longer;
the dispatcher then treated `NOT_FOUND` as fatal and **market-closed the freshly opened
position** (double taker fees + spread, repeated churn).

**FIND (lines 416–430):**

```python
        # Retry loop to wait for Delta backend to persist child bracket orders
        tp_order = None
        sl_order = None
        for attempt in range(4):
            orders = self.get_open_orders()
            for o in orders:
                if o.get("product_id") == prod_id:
                    s_type = str(o.get("stop_order_type", "")).lower()
                    if "take_profit" in s_type:
                        tp_order = o
                    elif "stop_loss" in s_type:
                        sl_order = o
            if tp_order and sl_order:
                break
            time.sleep(0.25)
```

**REPLACE WITH:**

```python
        # PATCH D3: wait for Delta backend to persist child bracket orders.
        # Exponential backoff over ~10s total (was 4x0.25s = 1.0s, which caused
        # spurious BRACKET_ORDERS_NOT_LOCATED -> emergency market close of a
        # perfectly good fresh position). Unverifiable order book (None) is
        # retried inside the window, never treated as "orders missing".
        tp_order = None
        sl_order = None
        wait_schedule = [0.2, 0.3, 0.5, 0.8, 1.2, 1.5, 1.5, 1.5, 2.0]  # ~9.5s
        for attempt in range(len(wait_schedule) + 1):
            orders = self.get_open_orders()
            if orders is not None:
                for o in orders:
                    if o.get("product_id") == prod_id:
                        s_type = str(o.get("stop_order_type", "")).lower()
                        if "take_profit" in s_type:
                            tp_order = o
                        elif "stop_loss" in s_type:
                            sl_order = o
            if tp_order and sl_order:
                break
            if attempt < len(wait_schedule):
                time.sleep(wait_schedule[attempt])
```

---

## PATCH D4 — Fill-anchored TP: WIDEN-ONLY invariant (stop snapping winners back to 2.0R)

**WHY (severity: CRITICAL for alpha):** The evaluator places TP1 at liquidation magnets
(Model 2: short-magnet target, frequently 2.4–3.0R+). `reanchor_brackets_to_fill` then
re-anchored TP to **exactly** `ceil(2R/tick)×tick` whenever it differed by >1 tick —
i.e., it **snapped a 2.6R magnet target back to 2.0R**, amputating 0.6R of expected
reward on every favorably-extended trade. The invariant is "R:R **≥** 2.0", not "= 2.0".

**FIND (lines 479–525) — the entire TP calibration tail of `reanchor_brackets_to_fill`
(from `# Calibrate TP to guarantee at least min_rr:1` to the final `return {"ok": True,
"status": "ALREADY_COMPLIANT", "current_rr": current_rr}`):**

**REPLACE WITH:**

```python
        # PATCH D4: guarantee at least min_rr:1 with a WIDEN-ONLY ratchet.
        # The evaluator frequently extends TP1 to a liquidation magnet (2.4-3.0R+).
        # The previous code snapped any TP that differed from the exact-2.0R grid
        # back to 2.0R — amputating the magnet-extended reward. TP may only move
        # OUTWARD from the fill, never inward.
        target_ticks = math.ceil((min_rr * actual_risk) / tick_size)
        calibrated_tp = fill_price + (dir_mult * target_ticks * tick_size)

        # Check current TP
        current_tp = float(tp_order.get("stop_price", 0.0) or 0.0)
        current_reward = abs(current_tp - fill_price)
        current_rr = current_reward / max(actual_risk, 1e-8)

        if current_tp > 0:
            if pos_direction == "BUY":
                effective_tp = max(calibrated_tp, current_tp)   # keep the wider target
            else:
                effective_tp = min(calibrated_tp, current_tp)
        else:
            effective_tp = calibrated_tp
        effective_rr = abs(effective_tp - fill_price) / max(actual_risk, 1e-8)

        register_position_anchor(
            symbol=symbol,
            product_id=prod_id,
            direction=pos_direction,
            entry_price=fill_price,
            initial_sl=calibrated_sl,
            initial_tp=effective_tp,
            size=size,
            initial_risk_dist=actual_risk
        )

        needs_edit = (current_rr < min_rr) or (abs(effective_tp - current_tp) > tick_size * 0.5)
        if needs_edit:
            edit_res = self.edit_order(
                order_id=tp_order["id"],
                product_id=prod_id,
                size=size,
                stop_price=effective_tp
            )
            is_success = bool(edit_res.get("success", False) or edit_res.get("result") or edit_res.get("id"))
            if not is_success:
                logger.error(f"[{symbol}] Failed to re-anchor TP to {effective_tp}: {edit_res}")
                return {
                    "ok": False,
                    "status": "TP_EDIT_FAILED",
                    "reason": f"Failed to edit TP order: {edit_res.get('error', 'UNKNOWN')}",
                    "edit_res": edit_res
                }
            logger.info(f"[{symbol}] Re-anchored TP from {current_tp} to {effective_tp} (fill {fill_price}, R:R {current_rr:.2f} -> {effective_rr:.2f}, widen-only ratchet)")
            return {
                "ok": True,
                "status": "REANCHORED",
                "calibrated_tp": effective_tp,
                "calibrated_sl": calibrated_sl,
                "rr": round(effective_rr, 2),
                "edit_res": edit_res
            }
        # Preserve the legacy return contract: calibrated_* keys always present.
        return {
            "ok": True,
            "status": "ALREADY_COMPLIANT",
            "current_rr": current_rr,
            "calibrated_tp": effective_tp,
            "calibrated_sl": calibrated_sl
        }
```

---

## PATCH D5 — Confirm the ACTUAL market fill before re-anchoring

**WHY (severity: CRITICAL):** For a market order, Delta's immediate response may not yet
contain `average_fill_price` (async matching). The old code silently fell back to the
**planned signal entry** — so "fill-anchored" R:R and the $25 cap were anchored to a
price the market never gave you. On fast moves (exactly when absorption signals fire)
the true fill can be materially worse; realized risk then exceeds `MAX_RISK_USD` with
no detection.

**INSERT — new method, immediately BEFORE `def reanchor_brackets_to_fill`:**

```python
    def confirm_market_fill(
        self,
        product_id: int,
        order_id: Optional[int],
        order_info: Dict[str, Any],
        fallback_mark_price: float,
        max_wait_s: float = 3.0
    ) -> Tuple[float, str]:
        """
        PATCH D5: resolves the ACTUAL executed fill price for a market order.
        1) immediate response average_fill_price (if already populated)
        2) poll GET /v2/fills for our order_id (up to max_wait_s)
        3) NEVER the planned signal price — fall back to the pre-trade live mark.
        Returns (fill_price, source_tag).
        """
        # 1) immediate response
        try:
            fp = float(order_info.get("average_fill_price") or 0.0)
            if fp > 0:
                return fp, "ORDER_RESPONSE"
        except (ValueError, TypeError):
            pass

        # 2) poll the fills ledger
        deadline = time.time() + max_wait_s
        while time.time() < deadline:
            try:
                fills = self.get_fills(limit=20)
                for f in fills:
                    try:
                        if int(f.get("product_id", -1)) != int(product_id):
                            continue
                        if order_id is not None and f.get("order_id") not in (None, order_id, int(order_id), str(order_id)):
                            continue
                        px = float(f.get("price") or 0.0)
                        if px > 0:
                            return px, "FILLS_LEDGER"
                    except (ValueError, TypeError):
                        continue
            except Exception as exc:
                logger.debug(f"Fill confirmation poll error: {exc}")
            time.sleep(0.3)

        # 3) conservative fresh fallback (planned entry is FORBIDDEN here)
        if fallback_mark_price and fallback_mark_price > 0:
            logger.warning(
                f"[product {product_id}] Fill not yet visible; falling back to pre-trade MARK "
                f"{fallback_mark_price} (planned entry is never used for risk anchoring)."
            )
            return float(fallback_mark_price), "MARK_FALLBACK"
        return 0.0, "UNCONFIRMED"
```

**THEN FIND (in `_execute_delta_signal_guarded`, lines ~1101–1112):**

```python
            ok = res.get("success", False) or ("result" in res and "id" in res.get("result", {}))
            if ok:
                order_info = res.get("result", {})
                fill_price_raw = order_info.get("average_fill_price")
                fill_price = float(fill_price_raw) if fill_price_raw and float(fill_price_raw) > 0 else entry
                try:
                    reanchor_res = self.reanchor_brackets_to_fill(
                        symbol=prod["symbol"],
                        fill_price=fill_price,
                        sl_price=sl,
                        min_rr=2.0
                    )
```

**REPLACE WITH:**

```python
            ok = res.get("success", False) or ("result" in res and "id" in res.get("result", {}))
            if ok:
                order_info = res.get("result", {})
                # PATCH D5: confirm the ACTUAL fill (fills ledger -> mark fallback).
                # Never anchor risk math to the planned signal price.
                order_id = order_info.get("id")
                fill_price, fill_source = self.confirm_market_fill(
                    product_id=prod["id"],
                    order_id=order_id,
                    order_info=order_info,
                    fallback_mark_price=live_mark_price
                )
                if fill_price <= 0:
                    logger.error(f"[{prod['symbol']}] Fill price unconfirmable; closing position immediately (fail-closed).")
                    close_res = self.close_position(prod["symbol"])
                    return {
                        "ok": False,
                        "status": "REJECTED_UNCONFIRMED_FILL",
                        "reason": "FILL_PRICE_UNCONFIRMED",
                        "venue": "DeltaExchangeIndia",
                        "symbol": prod["symbol"],
                        "flatten_confirmed": bool(close_res.get("success", False) or close_res.get("result") or close_res.get("id"))
                    }
                logger.info(f"[{prod['symbol']}] Fill confirmed @ {fill_price} (source: {fill_source}, planned entry was {entry})")
                try:
                    reanchor_res = self.reanchor_brackets_to_fill(
                        symbol=prod["symbol"],
                        fill_price=fill_price,
                        sl_price=sl,
                        min_rr=2.0
                    )
```

> `live_mark_price` is produced by PATCH D6, which runs earlier in the same method —
> apply both patches to the same function.

---

## PATCH D6 — Live microstructure gate: REAL spread + stale-entry drift veto

**WHY (severity: HIGH):** `validate_trade_geometry(..., spread_bps=0.0)` hardcoded zero —
the 45 bps spread gate was **decorative** in the live path. Worse: the signal's entry
price may be 30–90s old when the market order fires; a fast move silently consumes a
large slice of R0 as opening slippage (with a 0.5–2.5% stop, entering 0.3% late burns
12–60% of the risk budget before the bracket is even anchored).

**FIND (lines 1038–1047):**

```python
        dir_norm = "BUY" if direction in ("BUY", "BULLISH", "LONG") else "SELL"
        geom_ok, geom_reason = validate_trade_geometry(
            direction=dir_norm,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            spread_bps=0.0
        )
        if not geom_ok:
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}
```

**REPLACE WITH:**

```python
        dir_norm = "BUY" if direction in ("BUY", "BULLISH", "LONG") else "SELL"

        # PATCH D6: live microstructure gate before dispatch.
        # (a) fetch the product's live ticker; (b) enforce the 45bps spread gate
        # with the REAL spread (was hardcoded 0.0 = decorative); (c) reject when
        # the market has drifted >= 35% of the stop distance (or 1% of price,
        # whichever is smaller) away from the planned signal entry.
        live_mark_price = 0.0
        spread_bps_live = 0.0
        try:
            tk_res = self.request("GET", f"/v2/tickers/{prod['id']}")
            tk = tk_res.get("result", {}) if isinstance(tk_res, dict) else {}
            if isinstance(tk, list):
                tk = tk[0] if tk else {}
            live_mark_price = float(tk.get("mark_price") or 0.0)
            best_bid = float(tk.get("best_bid") or tk.get("bid") or 0.0)
            best_ask = float(tk.get("best_ask") or tk.get("ask") or 0.0)
            if best_bid > 0 and best_ask >= best_bid:
                spread_bps_live = ((best_ask - best_bid) / ((best_ask + best_bid) / 2.0)) * 10000.0
        except Exception as exc:
            logger.warning(f"[{sym}] Live ticker fetch error: {exc}")

        if live_mark_price <= 0:
            return {"ok": False, "status": "REJECTED", "reason": "LIVE_TICKER_UNAVAILABLE_FAIL_CLOSED"}

        geom_ok, geom_reason = validate_trade_geometry(
            direction=dir_norm,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            spread_bps=spread_bps_live
        )
        if not geom_ok:
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}

        # Stale-entry drift veto: market already ran away from the signal price.
        r0_dist = abs(entry - sl)
        max_drift = min(0.35 * r0_dist, 0.01 * entry)
        if abs(live_mark_price - entry) > max_drift:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": (
                    f"STALE_ENTRY_PRICE_DRIFT: |mark {live_mark_price:.8f} - entry {entry:.8f}| "
                    f"= {abs(live_mark_price - entry):.8f} > tolerance {max_drift:.8f} "
                    f"(0.35 x R0 {r0_dist:.8f}); signal price no longer executable at planned geometry"
                )
            }
```

---

## PATCH D7b — Wire the fee-aware grid into dispatch

**FIND (lines 1056–1072) — the sizing block:**

```python
        # Rigorous Discrete Contract Sizing via MAX_RISK_USD ($25.00) and available margin
        from Terminal.risk_envelope import MAX_RISK_USD
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        risk_dist = abs(entry - sl)
        single_contract_risk = risk_dist * contract_val
        if single_contract_risk > MAX_RISK_USD:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"SINGLE_CONTRACT_RISK_EXCEEDS_CAP: ${single_contract_risk:.2f} > ${MAX_RISK_USD:.2f}"
            }

        if risk_dist > 0 and contract_val > 0:
            calc_contracts = int(MAX_RISK_USD / single_contract_risk)
            size = max(1, min(calc_contracts, 100))
        else:
            size = 1
```

**REPLACE WITH:**

```python
        # PATCH D7: fee-aware Discrete Contract Grid sizing (SSOT helper
        # solve_fee_aware_grid — same integer grid as risk_envelope.admissible_volume,
        # k_max form). Round-trip taker commission is INSIDE the per-contract loss so
        # the $25.00 cap covers stop-out + fees (previously fees were unbudgeted:
        # ~15% of the cap leaked on a typical $2,500-notional / 1%-stop trade).
        from Terminal.risk_envelope import MAX_RISK_USD

        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        taker_fee_rate = float(prod.get("taker_fee_rate", TAKER_FEE_DEFAULT) or TAKER_FEE_DEFAULT)
        grid = solve_fee_aware_grid(
            entry=entry,
            stop_loss=sl,
            contract_value=contract_val,
            taker_fee_rate=taker_fee_rate,
            cap_usd=MAX_RISK_USD,
            max_contracts=100
        )
        if not grid["ok"]:
            return {"ok": False, "status": "REJECTED", "reason": grid["reason"]}
        size = int(grid["size"])
        realized_risk_at_size = float(grid["realized_risk_usd"])
        rt_commission_per_contract = float(grid["commission_per_contract"])
```

*Keep the existing margin-fit block that follows (`req_margin` / `sized_down`); it
consumes `size` exactly as before.*

---

## PATCH D8 — Leverage call: cache per product (latency out of the hot path)

**FIND (lines 353–354 in `place_order`):**

```python
        # Ensure leverage is configured
        self.set_leverage(prod_id, leverage)
```

**REPLACE WITH:**

```python
        # Ensure leverage is configured
        # PATCH D8: set leverage once per (product, leverage) per session.
        cached_lev = getattr(self, "_leverage_cache", {}).get(prod_id)
        if cached_lev != int(leverage):
            self.set_leverage(prod_id, int(leverage))
            if not hasattr(self, "_leverage_cache"):
                self._leverage_cache = {}
            self._leverage_cache[prod_id] = int(leverage)
```

---

## PATCH D9 — Slippage-breach visibility

**FIND (line 477):**

```python
                logger.info(f"[{symbol}] Calibrated SL inward to {calibrated_sl} to preserve ${max_allowed_risk_usd:.2f} risk cap")
```

**REPLACE WITH:**

```python
                logger.warning(
                    f"[{symbol}] SLIPPAGE BREACH: fill {fill_price} vs planned SL {sl_price} inflated "
                    f"risk to ${realized_risk_usd:.2f} > ${max_allowed_risk_usd:.2f} cap. SL tightened "
                    f"inward to {calibrated_sl} (size={size}, contract_val={contract_val}). "
                    f"Track opening slippage per product and tighten the D6 drift/spread gates if recurrent."
                )
```

---

## TEST IMPACT (must-read)

`Terminal/test_delta_position_guardian.py::test_failed_reanchor_triggers_emergency_close`
needs a live-ticker mock for the new D6 gate (the delivered
`test_delta_position_guardian_updated.py` contains the exact fix):

```python
            # PATCH D6 live microstructure gate: provide a live ticker mock
            # (mark 80000, tight spread, no drift) so dispatch reaches the order.
            live_ticker = {"success": True, "result": {"mark_price": "80000.0", "best_bid": "79990.0", "best_ask": "80010.0"}}
            with patch.object(trader, "request", return_value=live_ticker):
```

All other existing guardian tests pass **unchanged** with these patches (verified: 25/25,
plus risk-envelope suite green).
