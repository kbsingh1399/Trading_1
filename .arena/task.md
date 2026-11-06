# TASK: Institutional Architecture & Real-Time Production Review of _Trading_1

## 1. System Context & Environment
- **Target Repository**: `_Trading_1` (Cryptocurrency Microstructure, Order Flow & Auction Profile Quantitative Trading System)
- **Live Account**: FundedNext Stellar Lite $5K Challenge P1 (Login: `11808165`)
- **Capital Bounds**:
  - Max Loss Breach Line: `$4,600.00`
  - Current Equity: `$4,679.37`
  - Active Cushion: `$79.37`
  - Target Risk: Strictly bounded by Cap Dominance $\le \min(0.091 C_t, 0.25 C_t, \$15.00)$
  - Open Positions: 0 (Flat)
- **Execution Stack**: Windows 11 Pro, Python 3.14 (AsyncIO, DuckDB, Polars), MetaTrader 5 Build 5150 (`FundedNext-Server`), Chrome DevTools Protocol (`127.0.0.1:9222`).

## 2. Review Scope & Requirements (Especially Real-Time Working)
1. **Real-Time Ingestion & Scraping Lifecycles (`run_pipeline.py`, `coinglass_common.py`)**:
   - Zero-overlap TDMA 15-minute candle alignment.
   - Chrome CDP WebSocket stability, heartbeat watchdog, stale lock reaping (`_reap_stale_lock`), and orphan tab reaper without killing Cloudflare Turnstile challenge tabs.
   - Microstructure staleness pricing multiplier ($m = 2^{-\Delta t / 120\text{s}}$).
2. **Quantitative Formulation & Sizing (`trade_evaluator.py`)**:
   - RDNL-CS correlation discount ($\rho_{3,4} = 0.10, \rho_{1,5} = 0.08$) and PWD-Entropy ($\lambda = \ln(2)/1800, H_d \le 0.95$).
   - Effective R:R gate enforcement ($R:R_{\text{eff}} \ge 2.0$ deducting 0.06% fees and 0.05% slippage).
   - Quarter-Kelly sizing fraction persistence and `CushionKellySizer` dynamic calculation.
   - Chandelier trailing stop anchor ($HH_{22} - 2.5\times ATR_{14}$) + 2-bar confirmed VWAP $-1\sigma$ ratchet with Ratchet Monotonicity Law ($SL_t \ge SL_{t-1}$).
   - Chance-corrected concordance ($\kappa_c > 0.20$).
3. **Execution Safety & MT5 Bridge (`mt5_gui_puncher.py`)**:
   - Pre-flight spread gate ($\kappa_s = 2.5 \times \text{median\_spread}$).
   - Post-fill SL/TP handshake: poll MT5 positions for $\le 3.0\text{s}$. If SL fails to attach after 3 attempts or price slips $> 0.4R$ against position, execute emergency market close (`EMERGENCY_FLATTEN`).
4. **Multi-Agent Consensus (`multi_agent/council.py`)**:
   - Unshrinkable minimum lot risk veto ($\le \text{Cap}$). Stand-down floor $C^* = \$16.48$.
   - Positive MT5 crypto symbol whitelist (`BTCUSD`, `ETHUSD`, `DOGEUSD`, `ADAUSD`, `XRPUSD`, `LTCUSD`, `LINKUSD`).
   - Grounding check invalidating ungrounded ballots.
   - Jensen-Shannon Divergence quorum escalation to 5/5 unanimous when $D_t > 0.12$ or CVD divergence $z < -1.5$.
5. **Operational Parity & Verification**:
   - Daemons, cron triggers, unit tests, and 100% Git synchronization across both `origin` and `trading1`.
