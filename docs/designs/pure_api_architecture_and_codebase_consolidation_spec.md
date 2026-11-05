# Pure API Architecture & Codebase Consolidation Specification

**Document Status:** Approved EGM Consolidated Specification  
**Author:** AI Pair Programming Agent (Antigravity Elephant) with Adversarial Goldfish Swarm  
**Target Codebase:** `C:\Users\SIGMA\Documents\_Trading_1`  
**Artifact Path:** `docs/designs/pure_api_architecture_and_codebase_consolidation_spec.md`  

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
The current codebase has two major architectural deficiencies:
1. **Directory Disorder & Test Mixing**: The repository root contains legacy numbered scripts (`1_coinglass_...`, `2_...`, `3_...`, `4_...`, `5_...`), and `Terminal/` jumbles 24 production engine files together with 23 unit test files (`test_*.py`) in a single flat directory.
2. **Fragile In-Browser Scraping Dependencies**: Pillars 1, 2, 3, and 4 historically relied on Chrome DevTools Protocol (`CDP Bridge` on `127.0.0.1:9222`) to scrape CoinGlass web DOM, React Fiber hooks, and TradingView charts. This caused browser memory bloat (>2 GB RAM), Cloudflare Turnstile rate limits, necessity for WARP VPN rotation, and intermittent tab connection failures.

### 1.2 The Solution: 100% Pure API Order Flow & Unified Consolidation
Per user directive, we are transitioning to **pure API-based signals** and a **structured, consolidated codebase**:
1. **Pure API Ingestion**: Replace all 4 browser scrapers with high-speed, direct, free programmatic APIs:
   - **Footprint & Bars (Pillar 1)**: Binance Futures Public REST API (`fapi.binance.com/fapi/v1/klines`) + MetaTrader 5 native Python API.
   - **Auction Market Profile (Pillar 2)**: Local memory & DuckDB calculation from 300-bar rolling Parquets (`Terminal/auction_indicators.py`).
   - **Live Liquidations (Pillar 3)**: Binance Futures Forced Liquidation Orders (`/fapi/v1/allForceOrders`) + Aggregated 15m liquidation buckets.
   - **Liquidation Heatmap & Magnet Zones (Pillar 4)**: Binance Open Interest + Global Long/Short Ratio + Hyperdash GraphQL Liquidation clusters.
   - **Whale Book & Sweeps (Pillar 5)**: Binance Futures L2 Orderbook Depth with in-memory birth timestamps for exponential wall decay ($\tau = 1800 / \ln(2)$) + Binance Aggregated Trades (`/fapi/v1/aggTrades`) for institutional sweep bias.
   - **Institutional Intelligence**: Hyperdash GraphQL API (`https://api.hyperdash.com/graphql`).
2. **Decoupled 2-Process Architecture**:
   - **Process 1: Pure API Harvester (`Terminal/unified_api_footprint_engine.py --daemon`)**:
     Runs data ingestion sweeps on completed 15m candles, writes atomic Parquets, updates auction profiles, and appends candidate footprint signals to `Database/Signals/footprint_signals.jsonl`.
   - **Process 2: Reactive Sentinel & Position Guardian (`5_wake_on_footprint_signal.py --daemon` / `run_pipeline.py --sentinel`)**:
     Event-driven reactive loop (500ms) with dedicated 1-second position trailing thread, evaluating multi-pillar confluences and executing live orders exclusively on Delta Exchange India. Trailing stops and risk guardrails are NEVER blocked by data ingestion latency.
3. **Structured Directory Layout**:
   - Move all 23 `test_*.py` files from `Terminal/` to a dedicated `tests/` directory with `tests/conftest.py`.
   - Update `pytest.ini` (`testpaths = tests`).
   - Archive legacy CDP scrapers (`1_...`, `2_...`, `3_...`, `4_...`) into `legacy_archive/cdp_scrapers/`.
   - Expose a single clean CLI entrypoint: `run_pipeline.py`.

---

## 2. Target Repository Directory Structure

```text
_Trading_1/
├── run_pipeline.py                     # Primary pipeline launcher (--daemon, --sweep, --audit, --test)
├── pytest.ini                          # Configured for tests/
├── AGENTS.md                           # Repository architecture policies & invariants
├── TRADE_FORMULATION_AND_DATA_PROCESSING_PIPELINE.md
├── QUANT_SYSTEM_CRITIC_AGENTS_SUITE.md
├── 5_wake_on_footprint_signal.py       # Backward-compatible sentinel launcher (delegates to Terminal/)
│
├── Terminal/                           # Core Quantitative Engine Package
│   ├── __init__.py
│   ├── unified_api_footprint_engine.py # Pure API Harvester (Binance + MT5 + Depth + Liq)
│   ├── universal_footprint_engine.py   # Footprint math: Absorption, Divergence, CVD, Traps
│   ├── realtime_signal_evaluator.py    # 5-Pillar confluence scoring & independent gates
│   ├── delta_exchange_live_trader.py   # Delta Exchange India execution & connection pooling
│   ├── position_anchors.py             # Multi-tier monotonic trailing SL state machine
│   ├── risk_envelope.py                # SSOT portfolio risk limits (max 2 pos, max $25 risk)
│   ├── hyperdash_intel_engine.py       # Hyperdash GraphQL institutional intelligence
│   ├── auction_indicators.py           # Day VWAP, Value Area (VAH/VAL), Profile volume
│   ├── microstructure_vpin_engine.py   # Order flow toxicity & VPIN metric
│   ├── deepmind_timesfm_forecaster.py  # TimesFM 2.5 200M zero-shot time-series forecasting
│   ├── deepseek_harness_bridge.py      # DeepSeek V3/R1 LLM reasoning & trade validation
│   ├── empirical_signal_learner.py     # 300-bar empirical win-rate feature weighting
│   ├── regime_detector.py              # Market regime classifier (Trend / Mean-Reversion)
│   ├── olap_engine.py                  # DuckDB columnar analytical query engine
│   ├── vector_memory.py                # LanceDB vector similarity & counterfactual memory
│   ├── trade_critic_engine.py          # Post-mortem trade analysis & lesson distillation
│   ├── asset_signal_stack.py           # EvaluatedSignalsLedger & deduplication
│   ├── ortools_lot_solver.py           # Discrete contract grid solver
│   ├── paper_trading_engine.py         # Shadow paper trading simulator
│   ├── runtime_io.py                   # Atomic lock-free persistence & singleton mutex
│   ├── collector_lock.py               # Egress IP & process coordination
│   ├── alert_stream.py                 # Realtime trade alert dispatching
│   ├── cron_audit.py                   # Half-hour system health & portfolio auditor
│   └── venue_admission.py              # Exchange admission & instrument verification
│
├── tests/                              # Dedicated Test Suite (197 Passing Tests)
│   ├── conftest.py                     # Root sys.path injection & socket isolation stubs
│   ├── test_alpha_monetization_enhancements.py
│   ├── test_audit_regressions_c064c303.py
│   ├── test_candle_cluster_and_five_pillars.py
│   ├── test_collector_coordination.py
│   ├── test_columnar_storage.py
│   ├── test_cross_asset_stack.py
│   ├── test_deepmind_timesfm_forecaster.py
│   ├── test_deepseek_harness_bridge.py
│   ├── test_delta_position_guardian.py
│   ├── test_dual_models.py
│   ├── test_empirical_signal_learner.py
│   ├── test_forensic_microstructure_audit.py
│   ├── test_microstructure_vpin.py
│   ├── test_multibar_signal_evaluation.py
│   ├── test_paper_trading_engine.py
│   ├── test_pipeline_hardening.py
│   ├── test_review_fixes.py
│   ├── test_risk_envelope.py
│   ├── test_sentinel_engine.py
│   ├── test_single_source_of_truth.py
│   ├── test_swarm_synthesis_verification.py
│   ├── test_trade_critic_lifecycle.py
│   └── test_universal_footprint_engine.py
│
├── Database/                           # Persistent Columnar Storage
│   ├── Candle_Data/                    # 300-bar rolling M15 parquet files with IST columns
│   ├── Signals/                        # Footprint signals parquet & evaluated signals ledger
│   ├── Live_Liquidations/              # Realtime liquidation series & reversal signals
│   ├── Liquidation_Data/               # Near-field balance & magnet zones
│   ├── Whale_Orders/                   # Depth-derived whale orders & market snapshots
│   ├── Hyperdash_Data/                 # Institutional smart money & crowd features
│   └── Vector_Memory/                  # LanceDB post-mortem vector embeddings
│
├── docs/                               # Documentation & Specifications
│   └── designs/                        # Architectural design specifications
│
└── legacy_archive/                     # Decommissioned CDP Web Scrapers
    └── cdp_scrapers/
        ├── 1_coinglass_footprint_scraper.py
        ├── 2_coinglass_whales.py
        ├── 3_coinglass_liquidation_levels.py
        └── 4_coinglass_tv_scraper.py
```

---

## 3. Pure API Ingestion Architecture & Data Flow

### 3.1 Pillar Ingestion Mapping
| Pillar | Previous CDP Method | New Pure API Method | Endpoint / Transport | Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Pillar 1: Footprint & Bars** | `window.__cg_term` DOM scrape via CDP | Binance Futures Public REST API + MT5 IPC | `GET /fapi/v1/klines?symbol=BTCUSDT&interval=15m&limit=300` | ~1.5s total (32 assets) |
| **Pillar 2: Auction Market Value** | Computed from CDP scraped candles | Computed from Binance/MT5 300-bar rolling Parquets | Local memory / `Terminal.auction_indicators` | <10ms |
| **Pillar 3: Live Liquidations** | TradingView embedded chart clicks | Binance Futures Forced Orders API | `GET /fapi/v1/allForceOrders?symbol=BTCUSDT&limit=100` | ~250ms |
| **Pillar 4: Liquidation Heatmap** | CoinGlass Heatmap canvas DOM scrape | Binance Open Interest + Global L/S Ratio + Hyperdash Clusters | `GET /fapi/v1/openInterest`, `GET /futures/data/globalLongShortAccountRatio` | ~300ms |
| **Pillar 5: Whales & Sweeps** | CoinGlass Table 2 virtual scrolling | Binance Futures L2 Depth API (limit=1000) with stateful wall decay + Binance Agg Trades | `GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000`, `GET /fapi/v1/aggTrades` | ~200ms |
| **Institutional Intel** | Hyperdash GraphQL | Hyperdash GraphQL (kept warm) | `POST https://api.hyperdash.com/graphql` | ~500ms |

### 3.2 Parquet Compatibility & Zero-Regression Invariant
All output Parquet schemas and file locations remain bitwise compatible with `OlapEngine`:
- `Database/Candle_Data/{SYMBOL}.parquet`: Identical columns (`open_time_ms`, `close_time_ms`, `open`, `high`, `low`, `close`, `total_volume`, `buy_volume`, `sell_volume`, `delta`, `cumulative_delta`, IST columns).
- `Database/Signals/coinglass_aggregated_futures_signals.parquet`: Identical footprint signal schema.
- `Database/Whale_Orders/Open_Orders/{SYMBOL}_whale_open_orders.parquet`: Columns `snapshot_time_utc`, `price`, `remaining_usd`, `from_price_pct`, `side`, `duration_seconds`.
- `Database/Whale_Orders/History_Orders/{SYMBOL}_whale_history_orders.parquet`: Columns `venue`, `side`, `price`, `initial_usd`, `filled_usd`, `time_ms`.
- `Database/Live_Liquidations/{SYMBOL}_tv_liquidations.parquet`: 15m liquidation series.
- `Database/Live_Liquidations/latest_reversal_signals.json`: Reversal signals dict keyed by symbol.
- `Database/Liquidation_Data/Near_Field_Balance/{SYMBOL}_near_field.parquet`: `shorts_pct`, `longs_pct`, `short_share_24h_pts`.
- `Database/Liquidation_Data/Magnet_Zones/{SYMBOL}_magnet_zones.parquet`: `side`, `zone_price`, `score`, `distance_pct`.

---

## 4. Architectural Verification Plan
1. **Module Import Integrity**: Verify all `Terminal.*` imports resolve cleanly with tests placed in `tests/`.
2. **Full Test Suite Execution**: Run `python -m pytest tests` and verify 100% of 197 tests pass.
3. **Pure API Ingestion Sweep Benchmark**: Execute `python Terminal/unified_api_footprint_engine.py --once --crypto-only` and confirm 32 assets update in <12 seconds.
4. **Daemon Consolidation**: Verify `run_pipeline.py` launches the unified pure API engine and sentinel without requiring Chrome, CDP, or browser automation.
5. **Dual Remote Git Synchronization**: Push all commits to both `origin` (`_Trading_1`) and `trading2` (`Trading_2`).
