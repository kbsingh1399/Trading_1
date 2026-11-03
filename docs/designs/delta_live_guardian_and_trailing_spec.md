# EGM Architectural Design Specification: Autonomous Delta Position Guardian, Dynamic Trailing SL & Daemon Fleet

**Author**: The Elephant (Deep Context Quant Systems Architect)  
**Evaluator**: The Goldfish Swarm (`egm-design`, `egm-codebase-details`)  
**Status**: DRAFT FOR GOLDFISH AUDIT  
**Target Repository**: `c:\Users\SIGMA\Documents\_Trading_1`  
**Date**: 2026-11-04  

---

## 1. System Invariants & Operational Constraints

1. **Live Execution Target**:
   - Sole execution venue: **Delta Exchange India authenticated REST API** (`Terminal/delta_exchange_live_trader.py`). MatchTrader is completely excised from all code, docs, and routing.
2. **Strict Minimum 1:2 Risk:Reward Invariant**:
   - For every trade, Take-Profit ($P_{\text{tp}}$) relative to Fill Price ($P_{\text{fill}}$) and initial Stop-Loss ($P_{\text{sl}}$) must satisfy:
     $$\text{R:R} = \frac{|P_{\text{tp}} - P_{\text{fill}}|}{|P_{\text{fill}} - P_{\text{sl}}|} \ge 2.0$$
3. **Immutable Unit of Risk ($R_0$) Anchor**:
   - Unit of risk $R_0 \equiv |P_{\text{fill}} - P_{\text{sl\_initial}}|$ is immutable and stored in `Database/live_position_anchors.json`.
   - Trailing stop-loss moves to Breakeven (+1 tick) MUST NOT shrink $R_0$, preventing denominator collapse.
4. **Monotonic Trailing SL State Machine**:
   - **Tier 0** (Unrealized gain $< +1.0R_0$): Initial Stop-Loss.
   - **Tier 1** (Unrealized gain $\ge +1.0R_0$): SL ratcheted to Breakeven $+ 1\Delta_{\text{tick}}$.
   - **Tier 2** (Unrealized gain $\ge +2.0R_0$): SL ratcheted to $+1.0R_0$ profit lock; TP widened to $+3.0R_0$.
   - **Tier 3** (Unrealized gain $\ge +3.0R_0$): SL ratcheted to $+2.0R_0$ profit lock.
   - **Monotonicity**: Tiers only advance forward ($0 \to 1 \to 2 \to 3$). SL never regresses backwards.
5. **Naked Position Invariant**:
   - Zero open positions may exist without an active Stop-Loss order on Delta Exchange.
   - If an active position has no active SL order, the Position Guardian synthesizes a 1% protective SL and a 2% TP ($R:R = 2.0:1$) immediately.
6. **Margin Buffer Guardrail**:
   - Free margin buffer $\ge 20\%$ required before entering new trades to eliminate `insufficient_commission` rejections.
7. **Clean Daemon Fleet**:
   - All 5 processes run autonomously without zombie locks or orphaned child processes.

---

## 2. Dynamic Trailing SL Algorithm & Edge-Case Defenses

### 2.1 Formulae for Trailing Stops
For a Long position ($\text{dir} = +1$):
- $P_{\text{be}} = P_{\text{fill}} + \Delta_{\text{tick}}$
- $P_{\text{sl, tier 2}} = P_{\text{fill}} + 1.0 \times R_0$
- $P_{\text{sl, tier 3}} = P_{\text{fill}} + 2.0 \times R_0$

For a Short position ($\text{dir} = -1$):
- $P_{\text{be}} = P_{\text{fill}} - \Delta_{\text{tick}}$
- $P_{\text{sl, tier 2}} = P_{\text{fill}} - 1.0 \times R_0$
- $P_{\text{sl, tier 3}} = P_{\text{fill}} - 2.0 \times R_0$

### 2.2 Denominator Invariance & Widen TP
When an asset surges past $+2.0R_0$, if Take-Profit was initially pegged at $+2.0R_0$, the position could exit right as momentum peaks. To capture trend runners:
- When Tier 2 triggers, TP is dynamically widened from $+2.0R_0$ to $+3.0R_0$.
- SL is locked at $+1.0R_0$, guaranteeing a net profit of at least $1.0R$ regardless of subsequent pullbacks.

---

## 3. Background Daemon Orchestration

1. **Scraper 1 (Footprint)**: `python 1_coinglass_footprint_scraper.py --daemon`
2. **Scraper 2 (Whales)**: `python 2_coinglass_whales.py --daemon`
3. **Scraper 3 (Liquidation Levels)**: `python 3_coinglass_liquidation_levels.py --daemon`
4. **Scraper 4 (TV Liquidation Scraper)**: `python 4_coinglass_tv_scraper.py --daemon`
5. **Sentinel 5 (Reactive Evaluator & Guardian)**: `python 5_wake_on_footprint_signal.py`
6. **Cron Auditor**: Schedule firing every 30 minutes (`0,30 * * * *`) running `Terminal.cron_audit`.

---

## 4. Verification Plan

1. Execute test suite: `pytest Terminal/test_delta_position_guardian.py` (verify 100% pass).
2. Execute `python -m Terminal.cron_audit` to confirm all 6 active positions are compliant with active anchors and brackets.
3. Relaunch daemons cleanly and inspect initial output.
