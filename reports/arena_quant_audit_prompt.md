# Comprehensive Institutional Quant Audit Prompt for LMSYS Arena.ai (Round 4)

**Target Commit**: `a44274f` / `056c041` on `main`  
**GitHub Remotes**: 
- `https://github.com/kbsingh1399/_Trading_1.git`
- `https://github.com/kbsingh1399/Trading_2.git`

---

## Instructions for Arena.ai:

Please perform a comprehensive Round 4 Institutional Quantitative and Production Safety Audit on repository `_Trading_1` at latest commit `a44274f` (or pull latest `origin/main`).

This audit directly evaluates the remediation of all findings from Round 3 (which scored commit `a7e60cf` at 58/100, FAIL). Evaluate the codebase against the following institutional criteria:

### 1. Venue Exclusivity & Bridge Decommissioning (Delta Exchange India)
- Verify that MatchTrader, browser DOM execution, and all legacy prop-firm bridges have been completely decommissioned and excised from:
  * `AGENTS.md` (Section 3)
  * `Terminal/collector_lock.py` (`COLLECTOR_LOCK_MAP`)
  * `Terminal/realtime_signal_evaluator.py`
  * `5_wake_on_footprint_signal.py`
  * `Terminal/delta_exchange_live_trader.py`
- Verify that Delta Exchange India authenticated REST API (`Terminal/delta_exchange_live_trader.py`) is the sole execution target.

### 2. Live Trade Risk Invariants & Discrete Contract Grid Sizing
- Verify that the discrete contract grid solver in `Terminal/delta_exchange_live_trader.py` guarantees:
  * Maximum dollar risk per trade strictly $\le \$25.00$ (`MAX_RISK_USD = 25.0`).
  * Universal free margin floor $\ge 20\%$ across multi-asset collateral (`USD`, `USDT`, `INR`) with fail-closed rejection.
  * Maximum concurrent open positions $\le 2$ (`MAX_OPEN_POSITIONS = 2`).

### 3. Dynamic R:R Invariant & Directional Tick Quantization
- Verify that all live trades enforce a minimum 1:2 Risk-to-Reward ratio ($R:R \ge 2.0$) calibrated against the **actual fill price** ($P_{\text{fill}}$):
  * Directional tick snapping: `math.ceil((2.0 * r0) / tick_size)` for TP, `math.floor` for SL risk.
  * Dynamic re-anchoring upon fill (`reanchor_bracket_upon_fill`).
  * Continuous automated audit (`audit_and_trail_positions`) which auto-calibrates any existing order where $R:R < 1.999$.

### 4. Monotonic Multi-Tier Trailing Stop Loss State Machine
- Verify the three-tier dynamic ratchet in `Terminal/delta_exchange_live_trader.py`:
  * **Tier 1 (+1.0R0)**: Ratchet SL to Breakeven (+1 tick buffer).
  * **Tier 2 (+2.0R0)**: Lock $+1.0R0$ profit in SL and widen TP to $+3.0R0$.
  * **Tier 3 (+3.0R0)**: Lock $+2.0R0$ profit in SL and widen TP to $+4.0R0$.
- Verify emergency market close (`immediate_execution_stop_order`) if mark price has already penetrated the stop level.
- Verify deterministic directional duplicate order purge (highest TP for BUY, lowest TP for SELL preserved).

### 5. Bar Completion Invariant & Time-Series Precision
- Verify that signals execute strictly on closed 15-minute time candles:
  * `t_ms % 900_000 == 0`
  * `bar_type == 'time'`
- Verify in `1_coinglass_footprint_scraper.py` that JavaScript injection ignores non-time bars and non-15m intervals (`if (set.bt !== 'time') continue;`).
- Verify that Volume and Range bar signals are strictly suppressed and recorded as `DISCARDED_STALE` in `Database/Signals/evaluated_signals_ledger.jsonl`.

### 6. DeepSeek Harness (`dsh`) Antigravity Integration & Windows Job Objects
- Verify `Terminal/deepseek_harness_bridge.py` and design spec `docs/designs/deepseek_harness_antigravity_integration_spec.md`:
  * Child processes bound to Win32 Job Object (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000`) with fallback to `taskkill.exe /PID <pid> /T /F`.
  * Strict $\le 5.0\text{s}$ SLA on LLM reasoning with fail-closed behavior.
  * Memory budget $\le 150\text{ MB}$ (OfficeKit and Electron excised from runtime).
  * Isolation in `.dsh_home/` outside `Database/`.

### 7. Verification Test Suite Execution
Run the complete offline test suite (zero external network calls required):
```bash
python -m pytest Terminal/test_delta_position_guardian.py Terminal/test_collector_coordination.py Terminal/test_risk_envelope.py Terminal/test_cross_asset_stack.py Terminal/test_dual_models.py Terminal/test_deepseek_harness_bridge.py -v
```
Verify that all 42 unit tests PASS cleanly.

Please provide an updated quantitative readiness score (Target: $\ge 95/100$) and itemized breakdown across:
1. Architectural Boundary & Venue Safety (20 pts)
2. Risk Invariants & Trade Geometry (20 pts)
3. Trailing Stop & Position Guardian Resilience (20 pts)
4. Data Pipeline & Rolling Window Invariants (20 pts)
5. Test Coverage & Process Lifecycle Safety (20 pts)
