# GEMINI.md — Institutional Quant Trading & Pipeline System Architecture
========================================================================================
Project: `_Trading_1` | High-Confluence Algorithmic Trading & Order Flow Telemetry
OS: Windows 11 | Execution: MetaTrader 5 (FundedNext) | Python: 3.14
========================================================================================

## 1. Project Overview & Institutional Mandate
This repository houses an institutional-grade quantitative crypto trading system executing on FundedNext MT5. The system blends multi-pillar high-frequency order flow telemetry, liquidation cluster mechanics, and multi-agent consensus scoring into deterministic trade setups.

### Core Risk Mandates & Account Constraints:
1. **Hard Account Floor**: **`$4,600.00`** hard equity floor. Breaching this floor is strictly catastrophic. Stand-down cushion floor is $C^* = \$16.48$.
2. **Strict Per-Trade Risk**: Strictly **`$10.00 – $15.00`** per trade, governed by Cap Dominance:
   $$r_t \le \min(0.091 C_t, 0.25 C_t, \$15.00)$$
   where $C_t = \text{Equity} - \$4,600.00$. If minimum lot risk ($0.01 \text{ lot} \times \text{SL distance}$) exceeds $r_t$, the trade is strictly vetoed.
3. **Execution Kill-Switch (Immediate Flatten)**:
   - If Stop Loss (SL) attachment is unverified within 3.0 seconds post-fill, trigger `emergency_market_close()` immediately (`ORDER_FILLING_IOC` with Win32 GUI fallback).
   - If fill slippage exceeds $0.4R$ relative to planned entry, immediately flatten.
4. **Execution Whitelist**: Strictly positive MT5 symbols whitelist:
   `["BTCUSD", "ETHUSD", "DOGEUSD", "ADAUSD", "XRPUSD", "LTCUSD", "LINKUSD"]`.
5. **Effective Risk-Reward Gate**: Friction-adjusted $R:R_{\text{eff}} \ge 2.0$ mandatory before execution:
   $$R:R_{\text{eff}} = \frac{D_{\text{TP}} - 2 \cdot (\text{fee} + \text{slip}) - \text{spread}}{D_{\text{SL}} + 2 \cdot (\text{fee} + \text{slip}) + \text{spread}} \ge 2.0$$

---

## 2. Order Flow Telemetry & 5-Pillar Data Pipeline
The trading engine makes decisions purely based on empirical order flow telemetry captured directly from CoinGlass via headless Chrome DevTools Protocol (CDP port 9222):

### Pillar 1: Footprint Order Flow (`1_coinglass_footprint_scraper.py`)
- **Route**: `https://www.coinglass.com/footprint/<SYMBOL>?bot=footprint_scraper`
- **Data Captured**: Imbalance clusters (bid/ask volume ratios), Delta, Cumulative Volume Delta (CVD), Point of Control (POC), Value Area High/Low (VAH/VAL), absorption volume, trapped traders at extremes.
- **Cadence**: Strictly every 10 minutes (6 runs/hour: :03, :13, :23, :33, :43, :53).
- **Target Storage**: `Database/Auction_Levels/<SYMBOL>_auction_levels.parquet`, `Database/Signals/coinglass_aggregated_futures_signals.parquet`.

### Pillar 2: Whale Limit Order Book (`2_coinglass_whales.py`)
- **Route**: `https://www.coinglass.com/large-orderbook/<SYMBOL>?bot=whales_scraper`
- **Data Captured**: Large limit bid/ask orders across Binance, OKX, Bybit, Coinbase. Wall depth in USD, distance from mark price, and Bayesian Prior Age Decay ($t_{1/2} = 30$ min, $\tau \approx 2596.84$s).
- **Cadence**: 4 times/hour (:09, :19, :37, :51).
- **Target Storage**: `Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet`, `Database/Whale_Orders/History_Orders/<SYMBOL>_whale_history_orders.parquet`.

### Pillar 3: Liquidation Density & Magnet Zones (`3_coinglass_liquidation_levels.py`)
- **Route**: `https://www.coinglass.com/liquidation-levels/<SYMBOL>?bot=liq_levels_scraper`
- **Data Captured**: Liquidation clusters, cumulative long vs short liquidation volumes, Near-Field Balance (within $\pm 1.5\%$), Magnet Zones (high-density pool within $\pm 0.8\%$), strength by distance bands.
- **Cadence**: 3 times/hour (:06, :26, :48).
- **Target Storage**: `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet`, `Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet`.

### Pillar 4: TradingView 15m Liquidations (`4_coinglass_tv_scraper.py`)
- **Route**: `https://www.coinglass.com/tv/Binance_<SYMBOL>USDT`
- **Data Captured**: 15m candle-closed liquidation spikes, Rolling 90th-percentile liquidation thresholds ($P_{90}$), Long/Short liquidation ratio ($>70\%$ long liq = Bullish Reversal Exhaustion; $>70\%$ short liq = Bearish Reversal Exhaustion).
- **Cadence**: Strictly aligned to 15m candle closes (4 runs/hour: :00, :15, :30, :45).
- **Target Storage**: `Database/Live_Liquidations/<SYMBOL>_tv_liquidations.parquet`, `Database/Live_Liquidations/summary_liquidations.parquet`, `Database/Live_Liquidations/latest_reversal_signals.json`.

### Pillar 5: Multi-Pillar Confluence Engine (`5_trade_evaluator.py` / `trade_evaluator.py`)
- **Cadence**: Triggered immediately at each 15m candle close (:00, :15, :30, :45).
- **Confluence Threshold**: Weighted confluence score $S \ge 75.0$ / 100.
- **Chance-Corrected Concordance**: Fleiss' Kappa $\kappa_c \ge 0.40$.
- **Council Consensus**: 5-agent deliberation (`QuantRiskAgent`, `MicrostructureAgent`, `MacroLiquidityAgent`, `ExecutionAgent`, `AdversarialAgent`). If Jensen-Shannon Divergence $D_{\text{JS}} > 0.38$, escalate quorum to 5/5 unanimous supermajority.

### Pillar 6: Open Interest & Predicted Funding Rates (`6_coinglass_oi_funding.py`)
- **Route / Feed**: Real-time aggregated Open Interest and Predicted Funding Rates across Binance, OKX, Bybit.
- **Data Captured**: Open interest in USD, 1h/4h/24h delta %, predicted funding rate, squeeze bias (Bullish Short Squeeze vs Bearish Long Squeeze).
- **Target Storage**: `Database/Market_Data/oi_and_funding.parquet`.

### Pillar 7: Sentiment Divergence Telemetry (`7_coinglass_sentiment_divergence.py`)
- **Route / Feed**: Top Trader Long/Short accounts & positions vs Global Accounts ratio.
- **Data Captured**: Smart money institutional positioning vs retail crowd positioning, divergence spread.
- **Target Storage**: `Database/Market_Data/sentiment_divergence.parquet`.

### Pillar 8: Multi-Timeframe Heatmap Intensity (`8_coinglass_heatmap_density.py`)
- **Route / Feed**: Multi-horizon liquidation heatmap density ($12\text{h}, 24\text{h}, 3\text{d}, 7\text{d}$).
- **Data Captured**: Upper/lower strike magnet pools, asymmetry ratio, magnet bias, proximity to high-density targets ($>\$50\text{M}$ within $\pm 2.0\%$).
- **Target Storage**: `Database/Market_Data/heatmap_density.parquet`.

---

## 3. Storage Architecture & Locking Invariants
1. **Zero-Overlap TDMA Orchestrator (`run_pipeline.py`)**:
   - Manages 17 scraper dispatches per hour with 2.0 to 7.0 minutes buffer between runs.
   - Background `ChromeWatchdog` probes CDP port 9222 every 30.0s.
   - Ephemeral tabs: Every scraper opens its tab, completes CDP harvesting, and closes the tab immediately.
2. **Re-entrant `writer_lock` Invariant**:
   - On Windows, `msvcrt.locking` byte-locks are per file handle. Recursive calls across nested functions (`upsert_parquet` calling `atomic_parquet`) must use thread-local lock counting (`threading.local()`) to prevent `TimeoutError: Writer busy` deadlocks.
3. **Lock Reaper Invariant**:
   - Non-blocking lock verification (`msvcrt.LK_NBLCK`) determines if a lock file in `Database/.locks/` is truly active. If the file is older than TTL and no living process holds an OS file lock, it is automatically unlinked.

---

## 4. Local Cloudflare Anti-Bot & IP Rotation Protocol
- **Constraint**: Purely local execution without calling external third-party paid proxies or cloud APIs.
- **Mechanism**: Cloudflare WARP CLI integration (`warp-cli disconnect` -> `warp-cli connect`) with zero-external dependencies, rotating egress IPs natively when Cloudflare challenges (Turnstile / 403 / 429) are detected.

---

## 5. Active Workspace Files
- `run_pipeline.py`: Master TDMA scheduler daemon.
- `coinglass_common.py`: Shared CDP primitives, mutexes, and decay math.
- `1_coinglass_footprint_scraper.py`: Footprint and auction profile scraper.
- `2_coinglass_whales.py`: Whale order book scraper.
- `3_coinglass_liquidation_levels.py`: Liquidation level cluster scraper.
- `4_coinglass_tv_scraper.py`: TradingView 15m liquidation scraper.
- `6_coinglass_oi_funding.py`: Open Interest & Predicted Funding Rate scraper (Pillar 6).
- `7_coinglass_sentiment_divergence.py`: Top Trader Sentiment Divergence scraper (Pillar 7).
- `8_coinglass_heatmap_density.py`: Multi-Timeframe Heatmap Intensity scraper (Pillar 8).
- `trade_evaluator.py`: 8-pillar scoring and Kelly risk calculator.
- `multi_agent/council.py`: 5-agent Council consensus deliberation.
- `mt5_gui_puncher.py`: MetaTrader 5 order execution & Immediate Flatten kill-switch.
