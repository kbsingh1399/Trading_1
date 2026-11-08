# GEMINI.md — Institutional Quant Trading & Pipeline System Architecture
========================================================================================
Project: `_Trading_1` | High-Confluence Algorithmic Trading & Order Flow Telemetry
OS: Windows 11 | Execution: MetaTrader 5 (Universal Multi-Asset & Multi-Broker) | Python: 3.14
========================================================================================

## 1. Project Overview & Institutional Mandate
This repository houses an institutional-grade quantitative trading and order flow telemetry system executing via MetaTrader 5 (MT5). The architecture is account-agnostic and broker-independent, designed to operate seamlessly across personal, proprietary, hedge fund, or institutional capital bases. The system blends multi-pillar high-frequency order flow telemetry, liquidation cluster mechanics, and multi-agent consensus scoring into deterministic trade setups.

### Core Risk Mandates & Capital Preservation Engine:
1. **Universal Dynamic Capital Sizing**:
   - Risk per trade ($r_t$) is governed dynamically by Fractional Kelly / Volatility-Adjusted Equity Risk:
     $$r_t = \text{Equity} \times f^*_{\text{adj}}$$
     where $f^*_{\text{adj}} \in [0.5\%, 2.0\%]$ is calibrated against regime volatility, or configured as a user-defined fixed percentage/dollar amount.
   - Dynamic maximum drawdown safety ceiling: automated stand-down if trailing peak-to-trough drawdown exceeds user risk threshold.
2. **Multi-Asset Trading Universe**:
   - **Crypto Majors**: `["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "ADAUSD", "LTCUSD", "LINKUSD", "DOGEUSD"]`.
   - **Precious Metals & Commodities**: `["XAUUSD" (Gold), "XAGUSD" (Silver)]`.
   - **Equities & Indices**: Expanded MT5 CFDs and major liquid instruments.
3. **Execution Kill-Switch (Immediate Flatten)**:
   - If Stop Loss (SL) attachment is unverified within 3.0 seconds post-fill, trigger `emergency_market_close()` immediately (`ORDER_FILLING_IOC` with Win32 GUI fallback).
   - If fill slippage exceeds $0.4R$ relative to planned entry, immediately flatten.
4. **Effective Risk-Reward Gate**: Friction-adjusted $R:R_{\text{eff}} \ge 2.0$ mandatory before execution:
   $$R:R_{\text{eff}} = \frac{D_{\text{TP}} - 2 \cdot (\text{fee} + \text{slip}) - \text{spread}}{D_{\text{SL}} + 2 \cdot (\text{fee} + \text{slip}) + \text{spread}} \ge 2.0$$

---

## 2. Order Flow Telemetry & Multi-Pillar Data Pipeline
The trading engine makes decisions purely based on empirical order flow telemetry captured directly from CoinGlass and market feeds via headless Chrome DevTools Protocol (CDP port 9222):

### Pillar 1: Footprint Order Flow (`standalone_coinglass_footprint_api.py` / `1_coinglass_footprint_scraper.py`)
- **Route**: `https://www.coinglass.com/footprint/<SYMBOL>?bot=footprint_api`
- **Data Captured**: Imbalance clusters (bid/ask volume ratios), Delta, Cumulative Volume Delta (CVD), Point of Control (POC), Value Area High/Low (VAH/VAL), absorption volume, trapped traders at extremes across 3 bar engines (Time 15m, Dynamic Volume, Dynamic Range).
- **Target Storage**: `Database/Footprint_Dumps/<SYMBOL>_<BAR_TYPE>_footprint.parquet`, `Database/Auction_Levels/<SYMBOL>_auction_levels.parquet`.

### Pillar 2: Whale Limit Order Book (`standalone_coinglass_whales_api.py` / `2_coinglass_whales.py`)
- **Route**: `https://www.coinglass.com/large-orderbook-statistics/<SYMBOL>?bot=whales_api`
- **Data Captured**: Active limit bid/ask orders across Binance, OKX, Bybit, Coinbase, Hyperliquid, Bitget. Wall depth in USD, distance from mark price, and Bayesian Prior Age Decay ($t_{1/2} = 30$ min, $\tau \approx 2596.84$s).
- **Target Storage**: `Database/Whale_Dumps/<SYMBOL>_whale_open_orders.parquet`, `Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet`.

### Pillar 3: Liquidation Density & Magnet Zones (`3_coinglass_liquidation_levels.py`)
- **Route**: `https://www.coinglass.com/liquidation-levels/<SYMBOL>?bot=liq_levels_scraper`
- **Data Captured**: Liquidation clusters, cumulative long vs short liquidation volumes, Near-Field Balance (within $\pm 1.5\%$), Magnet Zones (high-density pool within $\pm 0.8\%$), strength by distance bands.
- **Target Storage**: `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet`, `Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet`.

### Pillar 4: TradingView 15m Liquidations (`4_coinglass_tv_scraper.py`)
- **Route**: `https://www.coinglass.com/tv/Binance_<SYMBOL>USDT`
- **Data Captured**: 15m candle-closed liquidation spikes, Rolling 90th-percentile liquidation thresholds ($P_{90}$), Long/Short liquidation ratio ($>70\%$ long liq = Bullish Reversal Exhaustion; $>70\%$ short liq = Bearish Reversal Exhaustion).
- **Target Storage**: `Database/Live_Liquidations/<SYMBOL>_tv_liquidations.parquet`, `Database/Live_Liquidations/summary_liquidations.parquet`.

### Pillar 5: Multi-Pillar Confluence Engine (`trade_evaluator.py`)
- **Confluence Threshold**: Weighted confluence score $S \ge 75.0$ / 100.
- **Chance-Corrected Concordance**: Fleiss' Kappa $\kappa_c \ge 0.40$.
- **Council Consensus**: 5-agent deliberation (`QuantRiskAgent`, `MicrostructureAgent`, `MacroLiquidityAgent`, `ExecutionAgent`, `AdversarialAgent`). If Jensen-Shannon Divergence $D_{\text{JS}} > 0.38$, escalate quorum to 5/5 unanimous supermajority.

### Pillar 6: Open Interest & Predicted Funding Rates (`6_coinglass_oi_funding.py`)
- **Data Captured**: Real-time aggregated Open Interest in USD, 1h/4h/24h delta %, predicted funding rate, squeeze bias (Bullish Short Squeeze vs Bearish Long Squeeze).
- **Target Storage**: `Database/Market_Data/oi_and_funding.parquet`.

### Pillar 7: Sentiment Divergence Telemetry (`7_coinglass_sentiment_divergence.py`)
- **Data Captured**: Top Trader Long/Short accounts & positions vs Global Accounts ratio, smart money institutional positioning vs retail crowd positioning, divergence spread.
- **Target Storage**: `Database/Market_Data/sentiment_divergence.parquet`.

### Pillar 8: Multi-Timeframe Heatmap Intensity (`8_coinglass_heatmap_density.py`)
- **Data Captured**: Multi-horizon liquidation heatmap density ($12\text{h}, 24\text{h}, 3\text{d}, 7\text{d}$), strike magnet pools, asymmetry ratio, proximity to high-density targets.
- **Target Storage**: `Database/Market_Data/heatmap_density.parquet`.

---

## 3. Storage Architecture & Locking Invariants
1. **Zero-Overlap Concurrency & TDMA Isolation**:
   - Dedicated tagged tabs per scraper (`?bot=footprint_api`, `?bot=whales_api`, `?bot=liq_levels_scraper`).
   - Active Tab Registry (`_ACTIVE_WHALE_TABS`) and background watchdog preventing tab collisions or orphan reaping interference.
   - Ephemeral tabs: Every scraper opens its isolated tab, extracts data, and closes the tab immediately in `finally` blocks.
2. **Re-entrant `writer_lock` Invariant**:
   - On Windows, `msvcrt.locking` byte-locks are per file handle. Recursive calls across nested functions (`upsert_parquet` calling `atomic_parquet`) use thread-local lock counting (`threading.local()`) to prevent `TimeoutError: Writer busy` deadlocks.
3. **Crash-Proof `atomic_parquet` Publication**:
   - Parquet dumps write to an isolated temporary file before atomic OS replacement, guaranteeing that readers never encounter half-written or corrupted files.

---

## 4. Local Cloudflare Anti-Bot & IP Rotation Protocol
- **Constraint**: Purely local execution without calling external third-party paid proxies or cloud APIs.
- **Mechanism**: Cloudflare WARP CLI integration (`warp-cli disconnect` -> `warp-cli connect`) with zero-external dependencies, rotating egress IPs natively when Cloudflare challenges (Turnstile / 403 / 429) are detected.

---

## 5. Active Workspace Files
- `API/active_assets.json`: Centralized dynamic active assets universe.
- `API/active_assets_manager.py`: Dynamic active assets CLI and manager.
- `API/standalone_coinglass_footprint_api.py`: Standalone Footprint API and multi-horizon dumper.
- `API/standalone_coinglass_whales_api.py`: Standalone Whale Orderbook API and blotter dumper.
- `API/standalone_coinglass_liquidation_levels_api.py`: Standalone Liquidation Levels API and multi-horizon dumper.
- `run_pipeline.py`: Master TDMA scheduler daemon.
- `coinglass_common.py`: Shared CDP primitives, mutexes, decay math, and `writer_lock`.
- `1_coinglass_footprint_scraper.py`: Scheduled Footprint and auction profile scraper.
- `2_coinglass_whales.py`: Scheduled Whale order book scraper.
- `3_coinglass_liquidation_levels.py`: Liquidation level cluster scraper.
- `4_coinglass_tv_scraper.py`: TradingView 15m liquidation scraper.
- `6_coinglass_oi_funding.py`: Open Interest & Predicted Funding Rate scraper.
- `7_coinglass_sentiment_divergence.py`: Top Trader Sentiment Divergence scraper.
- `8_coinglass_heatmap_density.py`: Multi-Timeframe Heatmap Intensity scraper.
- `trade_evaluator.py`: Multi-pillar scoring and dynamic risk calculator.
- `multi_agent/council.py`: 5-agent Council consensus deliberation.
- `mt5_gui_puncher.py`: MetaTrader 5 order execution & Immediate Flatten kill-switch.

---

## 6. Standard User Intent Protocol: 'list all'
Whenever the user specifies **"list all"**, the system strictly delivers the **Full Comprehensive Dashboard**:
1. **Asset Universe & Catalog**: Categorized inventory of all available non-meme assets on CoinGlass (Major Layer-1/2 Cryptos, DeFi, US Equities/CFDs, Tokenized Metals).
2. **Local Storage & Database Inventory**: Live status of [`Database/Footprint_Dumps`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Footprint_Dumps) and [`Database/Whale_Dumps`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Whale_Dumps), reporting file counts, unique assets, disk footprint, and row counts.
3. **Pipeline & Concurrency Telemetry**: Active background daemon tasks, Chrome CDP tab health (port 9222), and file lock invariants.

---

## 7. Karpathy Guidelines & Superpowers Operational Protocol
All code modifications, reviews, and autonomous agent executions are governed by the 4 pillars of the **Karpathy Guidelines** (Think Before Coding, Simplicity First, Surgical Changes, Goal-Driven Execution) and **Superpowers Disciplines** (Systematic Debugging, Dynamic Decoupling, Completion Guarantees). Refer to `AGENTS.md` for full behavioral directives.
