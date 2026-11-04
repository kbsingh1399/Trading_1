# EGM Specification: Daemon Fleet Cleanup, Signal Precision, & Delta Order Integrity

**Date**: 2026-11-04  
**Author**: Elephant Architect (EGM Phase 1)  
**Status**: Ready for Goldfish Swarm Audit  
**Target Venue**: Delta Exchange India authenticated REST API only (`Terminal/delta_exchange_live_trader.py`)

---

## 1. Executive Summary & Root Cause Analysis

### 1.1 Background & User Observations
1. **Scraper 4 Lock Contention (`SKIPPED_LOCK_CONTENTION`)**:
   - `4_coinglass_tv_scraper.py` repeatedly logged `SKIPPED_LOCK_CONTENTION` because a zombie background process was running the older codebase (where all scrapers competed for a single `cdp_browser.lock`) rather than the domain-isolated lock map (`COLLECTOR_LOCK_MAP`).
2. **Signal Pollution with Non-15m Bars (e.g. ALGO @ 00:56:00 IST)**:
   - Coinglass Footprint UI exposes signals across three bar models: `15m` time bars (60 signals), `Volume` bars (24 signals, e.g. 1.0M ALGO), and `Range` bars (32 signals, e.g. 0.00025 range).
   - In `1_coinglass_footprint_scraper.py`, `extract_symbol_footprint` looped over all `sigSets` without filtering by `set.bt === 'time'`.
   - Volume and range bar signals with arbitrary timestamps (e.g. `00:56:00 IST`) were emitted, treated as 15m candles, and bypassed staleness filters.
3. **Repeated Review / Re-evaluation of Old Signals**:
   - `KNOWN_SIGNAL_KEYS` in scraper 1 only loaded parquet signals up to 2 hours old; older signals were re-emitted if discovered again.
   - In `5_wake_on_footprint_signal.py`, `has_mock_flag = any(s.get("bar_closed") for s in cluster)` was always True because scraper 1 tagged `s["bar_closed"] = True`, disabling the 30-minute stale veto in production.
4. **Duplicate Stop & Take Profit Orders on Delta Exchange India**:
   - Delta Exchange India REST API does not automatically cancel replaced conditional stop orders if an edit fails or if recovery routines detect a position during network latency.
   - For `SUSHIUSD`, a stale SL order (`0.1920`) co-existed with the ratcheted SL (`0.1908`).
   - For `TRXUSD`, duplicate SL (`0.3201`) and duplicate TP (`0.32802`) existed simultaneously.
5. **MatchTrader Remnants**:
   - Background zombie process `17904` was still executing old bytecode mentioning MatchTrader.
   - All references to MatchTrader must be completely excised.

---

## 2. Architectural Design & Invariants

### 2.1 Fleet Termination & Clean Startup Architecture
- **Process Cleanup**: Deterministically terminate all orphaned Python processes running `1_coinglass_footprint_scraper.py`, `2_coinglass_whales.py`, `3_coinglass_liquidation_levels.py`, `4_coinglass_tv_scraper.py`, and `5_wake_on_footprint_signal.py`.
- **Lock Sanitization**: Unlink all stale `.lock` files (`Database/.locks/*.lock`) before starting the fresh fleet.
- **Dedicated Port/Tab Architecture**: Ensure each scraper uses its domain-isolated lock from `COLLECTOR_LOCK_MAP` and creates/destroys its own CDP tab via `/json/new` and `/json/close/{id}` in strict `try ... finally` blocks.

### 2.2 Strict 15m Time Candle Invariant
- **In-Browser JS Filtering (`1_coinglass_footprint_scraper.py`)**:
  ```javascript
  for (const set of sigSets) {
      if (set.bt !== 'time') continue; // Strictly 15m time candles
      ...
  }
  ```
- **Quantized Boundary Enforcement**:
  - `t_ms % 900_000 == 0`: Only candles strictly on 15-minute boundaries (00, 15, 30, 45) are accepted.
- **Persistent Signal Deduplication**:
  - `load_known_signals()` must read `footprint_signals.jsonl` directly to load ALL historical signal tuples `(symbol, sig_code, time_ms)`. Once emitted to `footprint_signals.jsonl`, a signal is permanently suppressed from re-emission.

### 2.3 Single-Evaluation Ledger & Strict Freshness in Sentinel 5
- **Ledger Gate**:
  - `EvaluatedSignalsLedger`: `key = f"{symbol.upper()}:{open_time_ms}"`. If `self.stack.ledger.is_evaluated(sym, open_ms)` is True, immediately skip.
- **Deterministic Staleness Veto**:
  - Replace `has_mock_flag = any(s.get("bar_closed") for s in cluster)` with:
    ```python
    is_test_mode = os.environ.get("MOCK_TEST_MODE") == "1"
    if not is_test_mode and (now_ms - close_ms) > 1_800_000:
        logger.info(f"⏳ [STALE VETO] {sym} candle closed >30m ago. Discarded.")
        continue
    ```
  - Eliminates the bypass caused by `s["bar_closed"] = True`.

### 2.4 Delta Exchange India Duplicate Order Auto-Purge & 1:2 R:R Guardrail
- **Active Duplicate Order Purge in `audit_and_trail_positions()`**:
  - For each active position symbol:
    - Collect all open conditional orders with `stop_order_type in ('stop_loss_order', 'take_profit_order')`.
    - If multiple `stop_loss_order` exist:
      - BUY position: Keep order with `max(stop_price)`, cancel all others.
      - SELL position: Keep order with `min(stop_price)`, cancel all others.
    - If multiple `take_profit_order` exist:
      - BUY position: Keep order with `max(stop_price)`, cancel all others.
      - SELL position: Keep order with `min(stop_price)`, cancel all others.
- **1:2 R:R Invariant**:
  - Every trade must satisfy $R:R \ge 2.0:1$ relative to actual fill price $P_{\text{fill}}$.
  - If mark price advances $\ge +1.0R_0$, SL ratchets to Breakeven (+1 tick).
  - If mark price advances $\ge +2.0R_0$, SL ratchets to $+1.0R_0$ locked profit and TP widens to $+3.0R_0$.
  - If mark price advances $\ge +3.0R_0$, SL ratchets to $+2.0R_0$ locked profit.

### 2.5 Cron Audit Job Instructions
- Updated schedule `0,30 * * * *`:
  1. Delta Position Audit: Query `get_active_positions()`, run `audit_and_trail_positions()`, purge duplicate orders, verify $R:R \ge 2.0$.
  2. Margin Check: Verify free margin buffer $\ge 20\%$ floor. Clamped positions $\le 2$.
  3. Fleet Health: Verify heartbeats of scrapers 1, 2, 3, 4, and sentinel 5. Zero lock contention, zero zombie PIDs.
