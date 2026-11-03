# Exhaustive Quantitative Trading Pipeline & Codebase Architecture Audit

Conduct an institutional-grade, multi-persona architectural review of the entire codebase in repository `https://github.com/kbsingh1399/Trading_2` (branch `main`). Evaluate the production reliability, statistical rigor, and latency characteristics across the following five critical pillars:

---

### Pillar 1: In-Browser CDP Scraping & DOM Resilience
Examine `1_coinglass_footprint_scraper.py`, `2_coinglass_whales.py`, `3_coinglass_liquidation_levels.py`, and `4_coinglass_tv_scraper.py`.
1. **Memory Hooking vs DOM Virtualization**: Audit how `window.__cg_term` is extracted via React Fiber internals versus DOM virtual table scraping (`.lo-bt-body` / `.lo-bt-row`). Are there edge-case memory leaks or DOM node detachment risks?
2. **Dynamic Challenge & Error Recovery**: Review the automated error handling in `3_coinglass_liquidation_levels.py` (clicking in-page `.ll-overlay button` retry handlers, followed by Cloudflare WARP IP rotation and `Page.reload`).
3. **Cooperative TDMA Scheduling**: Inspect `Terminal/collector_lock.py`. How effectively does the two-tier micro/macro Time-Division Multiple Access (TDMA) scheduler prevent port 9222 CDP session collisions between the 4 scrapers?

---

### Pillar 2: True Cloudflare WARP IP Rotation & Network Hardening
Inspect `Terminal/collector_lock.py` (`rotate_warp_ip`, `_rotate_warp_ip_sync`).
1. **State Machine Integrity**: Evaluate the two-stage IP rotation: Stage 1 (dropping WARP to direct ISP IP) and Stage 2 (clearing registration via `warp-cli registration delete` + `registration new` + `connect` to acquire a new edge IP).
2. **Order Execution Race Protection**: Audit the `order_execution_inflight.lock` guard (with 20s TTL auto-expiry). Does this adequately prevent network disconnections while live orders are being transmitted to the MatchTrader brokerage API?
3. **Network Stabilization Probing**: Review the connectivity verification probe against `api.ipify.org`. Is a 10s timeout sufficient across congested edge nodes?

---

### Pillar 3: Microstructure OLAP Engine & Confluence Math
Inspect `Terminal/universal_footprint_engine.py`, `Terminal/unified_api_footprint_engine.py`, and `Terminal/realtime_signal_evaluator.py`.
1. **Mathematical Footprint Signal Classification**: Evaluate the exact algorithms for:
   - Absorption (30%+ volume absorbed in bar wicks with opposing close).
   - Single-Bar Divergence (Delta >= +5% on down bar, or <= -5% on up bar).
   - Multi-Bar Swing Cumulative Delta Divergence (higher highs on declining CVD).
   - Exhaustion and Trapped Traders.
2. **Exponential Half-Life Order Book Decay**: Audit the DuckDB OLAP engine (`Terminal/olap_engine.py`). Does the $\tau \approx 2597\text{s}$ time-decay correctly de-weight stale whale limit orders?
3. **Confluence Gate**: Audit the >= 75.0 / 100 confluence scoring threshold. Are the weight allocations between Footprint signals (35%), Liquidation barriers (25%), Whale order walls (20%), and Macro session levels (20%) statistically sound?

---

### Pillar 4: Prop Firm Risk Envelope & Execution Guardrails
Inspect `Terminal/risk_envelope.py` and `Terminal/matchtrader_live_trader.py`.
1. **Hard Drawdown Floor**: FundingPips MatchTrader Competition Account `101305360` ($5,000 challenge account). Audit `HARD_FLOOR_USD = $4,775.00`. How does the system guarantee zero trades if account equity reaches this floor?
2. **Position Limits & Risk Sizing**: Verify `MAX_OPEN_POSITIONS = 2` and `MAX_RISK_USD = $25.00` per trade. Audit the discrete lot grid solver for crypto vs forex instruments.
3. **Bar Completion Invariant**: Verify that automated live orders execute strictly on completed, closed 15m candles to eliminate repainting wicks.

---

### Pillar 5: Zero-Null Storage Invariants & Temporal Mapping
Inspect `Database/Candle_Data/`, `Database/Signals/`, `Database/Auction_Levels/`, and `Database/Whale_Orders/`.
1. **Primary IST Mapping**: Verify that `open_time_ist`, `close_time_ist`, and `candle_ist` provide continuous temporal alignment across all 31 crypto assets and MT5 macro pairs.
2. **Lock-Free Atomic Persistence**: Audit the `atomic_parquet` helper in `Terminal/runtime_io.py`. Does writing to temporary `.parquet.tmp` files and atomic-renaming via `os.replace` completely prevent read-during-write corruption under Windows NTFS?

---

### Deliverable Format:
1. **Executive Summary & Production Readiness Score (0–100)**: Frank, unsparing quantitative assessment.
2. **Critical Vulnerabilities & Race Conditions**: Point out exact file paths, lines, and potential breakdown scenarios under network lag or site DOM updates.
3. **Drop-In Code Patches**: Provide exact, drop-in replacement snippets for any identified flaws.
