# Multi-Pillar Trade Formulation & Execution Architecture

> **Repository:** `C:\Users\SIGMA\Documents\_Trading_1`  
> **Status:** LIVE Production  
> **Primary Time Reference:** Indian Standard Time (IST, UTC+5:30)  
> **Target Asset Universe:** 31 Monitored Crypto Futures Assets (Binance & Bybit Aggregated Perps)

---

## 1. Executive Summary & Core Philosophy

The purpose of this trading system is to eliminate subjective intuition, execution delays, and fragmented market analysis by combining five independent, non-correlated market microstructure pillars into a unified, mathematically rigorous **Confluence Trade Formulator**.

Trading edge in cryptocurrency derivatives cannot rely on single-indicator indicators (e.g., standard moving averages, RSI). Instead, institutional order flow requires monitoring the complete supply/demand continuum:
1. **Aggressive Taker Flow & Imbalance** (Footprint Order Flow)
2. **Structural Market Acceptance vs. Rejection** (Auction Market Theory & Volume Profiles)
3. **Passive Institutional Liquidity & Defending Walls** (Whale Blotters)
4. **Counterparty Vulnerability & Stop Clustering** (Liquidation Heatmaps & Magnets)
5. **Realized Liquidation Shocks & Cascades** (TradingView 15-Minute Liquidation Prints)

Signals are synthesized inside a **Per-Asset Rolling 5-Signal FIFO Queue**, evaluating incoming footprint bursts against broader market context, dynamically calculating support/resistance anchors, and strictly gatekeeping trades with an **$R:R \ge 2.0$** and **Confluence Score $\ge 75.0$** standard.

```
                          ┌────────────────────────────────────────────────────────┐
                          │               CoinGlass Data Ingestion                 │
                          │   (Chrome CDP 127.0.0.1:9222 Zero-Overlap TDMA)        │
                          └───────────────────────────┬────────────────────────────┘
                                                      │
         ┌───────────────────┬────────────────────────┼────────────────────────┬───────────────────┐
         ▼                   ▼                        ▼                        ▼                   ▼
┌─────────────────┐ ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐ ┌─────────────────┐
│ Pillar 1:       │ │ Pillar 2:       │      │ Pillar 3:       │      │ Pillar 4:       │ │ Pillar 5:       │
│ Footprint &     │ │ Auction Profile │      │ Whale Blotter   │      │ Liquidation     │ │ TV 15m Liquid.  │
│ Order Flow      │ │ & VWAP SD Bands │      │ Open Walls      │      │ Magnet Zones    │ │ Bursts & Cliques│
└────────┬────────┘ └────────┬────────┘      └────────┬────────┘      └────────┬────────┘ └────────┬────────┘
         │                   │                        │                        │                   │
         └───────────────────┴───────────┬────────────┴────────────────────────┴───────────────────┘
                                         ▼
                   ┌──────────────────────────────────────────┐
                   │    Per-Asset Rolling 5-Signal Queue      │
                   │ Database/Signals/Signal_Queues/<SYM>.json│
                   │      (Burst Handling, FIFO Eviction)     │
                   └─────────────────────┬────────────────────┘
                                         ▼
                   ┌──────────────────────────────────────────┐
                   │    Multi-Pillar Trade Evaluator Engine   │
                   │      (Direction Shannon Entropy Gate)    │
                   │      (Confluence Score S >= 75.0 Gate)   │
                   │      (Dynamic Risk/Reward R:R >= 2.0)    │
                   └─────────────────────┬────────────────────┘
                                         ▼
                   ┌──────────────────────────────────────────┐
                   │     Active Formulations & Ledgers        │
                   │  Database/Signals/Trade_Setups/<SYM>.json│
                   │     (Polars & DuckDB NDJSON Ready)       │
                   └──────────────────────────────────────────┘
```

---

## 2. Ingestion Infrastructure & Schedule (TDMA)

To guarantee zero browser lock contention, minimal memory consumption, and clean isolation from user browsing tabs, all standalone scrapers operate on a **strictly synchronized 60-minute Time Division Multiple Access (TDMA)** schedule aligned with 15-minute candle closes:

| Minute | Pipeline Component | Script | Function |
| :---: | :--- | :--- | :--- |
| **`:00`** | 📺 TV Liquidations | [`4_coinglass_tv_scraper.py`](file:///C:/Users/SIGMA/Documents/_Trading_1/4_coinglass_tv_scraper.py) | Harvests finalized 15m liquidation candle closes |
| **`:03`** | 👣 Footprint Sweep | [`1_coinglass_footprint_scraper.py`](file:///C:/Users/SIGMA/Documents/_Trading_1/1_coinglass_footprint_scraper.py) | Sweeps 31 assets, harvests order flow, updates queues |
| **`:06`** | 💧 Liquidation Levels | [`3_coinglass_liquidation_levels.py`](file:///C:/Users/SIGMA/Documents/_Trading_1/3_coinglass_liquidation_levels.py) | Updates high-leverage liquidation clusters & magnets |
| **`:09`** | 🐋 Whale Blotter | [`2_coinglass_whales.py`](file:///C:/Users/SIGMA/Documents/_Trading_1/2_coinglass_whales.py) | Captures open bid/ask walls, fills, and decay weights |
| **`:13`** | 👣 Footprint Sweep | `1_coinglass_footprint_scraper.py` | Order flow sweep (2/6) |
| **`:15`** | 📺 TV Liquidations | `4_coinglass_tv_scraper.py` | 15m candle close harvest |
| **`:19`** | 🐋 Whale Blotter | `2_coinglass_whales.py` | Whale orderbook update (2/4) |
| **`:23`** | 👣 Footprint Sweep | `1_coinglass_footprint_scraper.py` | Order flow sweep (3/6) |
| **`:26`** | 💧 Liquidation Levels | `3_coinglass_liquidation_levels.py` | Liquidation levels update (2/3) |
| **`:30`** | 📺 TV Liquidations | `4_coinglass_tv_scraper.py` | 15m candle close harvest |
| **`:33`** | 👣 Footprint Sweep | `1_coinglass_footprint_scraper.py` | Order flow sweep (4/6) |
| **`:37`** | 🐋 Whale Blotter | `2_coinglass_whales.py` | Whale orderbook update (3/4) |
| **`:43`** | 👣 Footprint Sweep | `1_coinglass_footprint_scraper.py` | Order flow sweep (5/6) |
| **`:45`** | 📺 TV Liquidations | `4_coinglass_tv_scraper.py` | 15m candle close harvest |
| **`:48`** | 💧 Liquidation Levels | `3_coinglass_liquidation_levels.py` | Liquidation levels update (3/3) |
| **`:51`** | 🐋 Whale Blotter | `2_coinglass_whales.py` | Whale orderbook update (4/4) |
| **`:53`** | 👣 Footprint Sweep | `1_coinglass_footprint_scraper.py` | Order flow sweep (6/6) |

---

## 3. The Five Microstructure Pillars

### Pillar 1: Footprint & Order Flow Dynamics (Max 30 Points)
- **Source:** [`Database/Candle_Data/<SYM>.parquet`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Candle_Data) & `footprint_signals.jsonl`
- **Signal Priority Hierarchy:**
  1. `trap` (Priority 1 — Bull/Bear Trap): Aggressive breakout orders trapped as price re-enters range.
  2. `abs` (Priority 2 — Absorption): Massive aggressive buying/selling absorbed by limit orders without price movement.
  3. `ratio_p` (Priority 3 — Imbalance Ratio Pass): Heavy diagonal bid-ask ratio imbalances ($\ge 300\%$).
  4. `sdiv` / `div` (Priority 4/5 — Swing & Delta Divergence): Price makes higher highs while Cumulative Volume Delta (CVD) makes lower highs (or vice versa).
  5. `p_spike` / `exh` (Priority 6/7 — Pressure Spikes & Exhaustion): Climax volume spikes with shrinking candle bodies.
- **Delta & CVD Invariants:**
  - True buying pressure requires positive candle Delta and upward sloping CVD.
  - Reversals require delta divergence (e.g. Price down, Delta positive $\implies$ passive absorption).

### Pillar 2: Auction Market Theory & Value Areas (Max 20 Points)
- **Source:** [`Database/Auction_Levels/<SYM>_auction_levels.parquet`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Auction_Levels)
- **Core Parameters:**
  - `day_val` (Value Area Low, 70% volume boundary): Prime long bounce zone.
  - `day_vah` (Value Area High, 70% volume boundary): Prime short rejection zone.
  - `day_poc` (Point of Control): Heaviest volume price of the session. Reclaiming POC confirms institutional acceptance.
  - `day_vwap` $\pm 1\sigma, 2\sigma$: Anchored session volume-weighted average price.
  - `mtf_bias`: Multi-timeframe trend confluence (`HTF_BULLISH_CONFLUENCE` vs. `BEARISH_TREND`).
- **Formulation Rules:**
  - **Long:** Price trading at/near VAL or reclaiming POC/VWAP with bullish MTF bias.
  - **Short:** Price trading at/near VAH or breaking below POC/VWAP with bearish MTF bias.

### Pillar 3: Whale Blotters & Defending Walls (Max 20 Points)
- **Source:** [`Database/Whale_Orders/Open_Orders/<SYM>_whale_open_orders.parquet`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Whale_Orders/Open_Orders)
- **Core Parameters:**
  - Large resting limit orders ($\ge \$100\text{k}$ to $\$10\text{M}+$) on Binance and Bybit Perps.
  - `decay_weight`: Exponential time-decay $e^{-\frac{\text{age}}{\tau}}$ with $\tau = \frac{1800}{\ln 2}$ (30-minute half-life). Orders older than 45 minutes are discounted.
  - Defending Bid Walls within $0.1\%$ to $2.5\%$ below price act as structural stop shields.
  - Defending Ask Walls within $0.1\%$ to $2.5\%$ above price act as short defense barriers.

### Pillar 4: Liquidation Heatmap Magnet Zones (Max 15 Points)
- **Source:** [`Database/Liquidation_Data/Magnet_Zones/<SYM>_magnet_zones.parquet`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Liquidation_Data/Magnet_Zones)
- **Mechanics:**
  - High-leverage positions accumulate liquidation price thresholds. Market makers and algorithmic momentum flow hunt these liquidity clusters.
  - **Upside Magnets (Short Liquidations):** Cluster of short liquidations above current price acts as a vacuum pulling price upward. Used as primary **Take Profit target for LONGs**.
  - **Downside Magnets (Long Liquidations):** Cluster of long liquidations below current price acts as a vacuum pulling price downward. Used as primary **Take Profit target for SHORTs**.

### Pillar 5: TradingView 15-Minute Live Liquidation Shocks (Max 15 Points)
- **Source:** [`Database/Live_Liquidations/<SYM>_tv_liquidations.parquet`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Live_Liquidations)
- **Mechanics:**
  - Tracks actual executed liquidations per 15-minute bar across long and short traders.
  - `is_spike`: Exceeds the 90th percentile liquidation volume threshold.
  - **Liquidation Cascade Exhaustion:** Heavy long liquidations ($> \$1\text{M}+$) that immediately produce a wick and positive delta signal a cascade flush and sharp mean reversion (`BULLISH_REVERSAL`).
  - **Breakout Acceleration:** Heavy short liquidations accompanying an upward range breakout indicate fuel for sustained continuation.

---

## 4. Per-Asset Rolling 5-Signal Queue Mechanics

Each asset maintains its own dedicated queue file at `Database/Signals/Signal_Queues/<SYM>_queue.json`.

### A. Window Rolling & FIFO Eviction
1. Maximum window size: **5 signals**.
2. **Burst Handling:** If a sweep discovers multiple signals simultaneously (e.g. 2 to 7 signals across time, range, or volume bars):
   - All valid signals are appended.
   - The queue slices `[-5:]`, discarding older entries.
   - The latest triggers are evaluated directly against the surviving queue context.

### B. Contextual History Analysis
- The evaluator examines not only the trigger signal, but also the preceding 2–4 signals:
  - **Concordance:** If 3 or 4 of the 5 signals agree with the trade direction (e.g., prior `div` and `abs` followed by a fresh `trap`), confluence points are awarded.
  - **Regime Shift:** If older signals were bearish but a high-priority Priority-1 `trap` arrives, the system recognizes an institutional reversal.

### C. Shannon Direction Entropy Gate
To prevent chop and whip-saw entries during non-directional consolidation, the system calculates binary Shannon Entropy:
$$p_L = \frac{N_{\text{LONG}}}{N}, \quad p_S = \frac{N_{\text{SHORT}}}{N}$$
$$H_d = -\left(p_L \log_2 p_L + p_S \log_2 p_S\right)$$
- If $H_d > 0.95$ (near 50/50 disagreement in the queue) and the trigger is NOT a top-tier Priority-1 `trap` or `abs` reversal, the setup is **VETOED** with status `PASS_CONTRADICTION`.

---

## 5. Mathematical Trade Formulation & Risk Model

```
Total Confluence Score S = W1 + W2 + W3 + W4 + W5  (Range: 0.0 - 100.0)
Active Setup Condition:  S >= 75.0  AND  R:R >= 2.0  AND  Hd <= 0.95
```

### Dynamic Stop Loss (SL) Anchoring
Stop losses are **never placed arbitrarily**. They are structurally anchored behind defending institutional barriers:
- **For LONG:**
  $$\text{SL} = \min\Big(\text{Defending Whale Bid Price} \times 0.998, \; \text{VAL} \times 0.998\Big)$$
  Safety floor: $\text{SL} \le \text{Entry} - \max(1.2 \times \text{ATR}_{15m}, \; 0.3\% \times \text{Price})$.
- **For SHORT:**
  $$\text{SL} = \max\Big(\text{Defending Whale Ask Price} \times 1.002, \; \text{VAH} \times 1.002\Big)$$
  Safety ceiling: $\text{SL} \ge \text{Entry} + \max(1.2 \times \text{ATR}_{15m}, \; 0.3\% \times \text{Price})$.

### Dynamic Take Profit (TP) Anchoring
- **For LONG:** Anchored to the highest-scoring **Short Liquidation Magnet** above price or session VAH, enforcing:
  $$\text{TP} \ge \text{Entry} + 2.0 \times (\text{Entry} - \text{SL})$$
- **For SHORT:** Anchored to the highest-scoring **Long Liquidation Magnet** below price or session VAL, enforcing:
  $$\text{TP} \le \text{Entry} - 2.0 \times (\text{SL} - \text{Entry})$$

### Risk-to-Reward Hard Gate
$$\text{R:R} = \frac{|\text{TP} - \text{Entry}|}{|\text{Entry} - \text{SL}|} \ge 2.0$$
If market structure does not offer a natural liquidation or auction target that provides at least a **2:1 Reward-to-Risk ratio**, the setup is marked `PASS_RR_BELOW_2` and dismissed.

---

## 6. Storage Schema & DuckDB Ingestion Standard

All formulated trade evaluations are saved into:
- Individual asset ledgers: [`Database/Signals/Trade_Setups/<SYM>_setups.jsonl`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Signals/Trade_Setups)
- Master active alerts: [`Database/Signals/Trade_Setups/master_active_setups.jsonl`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/Signals/Trade_Setups/master_active_setups.jsonl)

### Canonical Flat Record Schema
```json
{
  "setup_id": "BTC_1793898746795286",
  "symbol": "BTC",
  "status": "ACTIVE",
  "direction": "LONG",
  "signal_count": 5,
  "primary_signal": "trap",
  "confluence_score": 82.5,
  "w1_footprint": 27.0,
  "w2_auction": 18.0,
  "w3_whales": 16.0,
  "w4_magnets": 11.0,
  "w5_tv_liquidations": 10.5,
  "entry_price": 76740.1,
  "stop_loss": 76150.0,
  "take_profit": 78200.0,
  "risk_reward_ratio": 2.47,
  "risk_amount": 590.1,
  "reward_amount": 1459.9,
  "atr_15m": 312.4,
  "auction_bias": "INSIDE_VALUE",
  "htf_trend": "HTF_BULLISH_CONFLUENCE",
  "defending_wall_price": 76300.0,
  "magnet_target_price": 78200.0,
  "time_ms": 1793896200000,
  "time_ist": "2026-11-05 22:00:00 IST",
  "created_at_ms": 1793898746795,
  "created_at_us": 1793898746795286,
  "created_at_utc": "2026-11-05T17:12:26.795286+00:00",
  "created_at_ist": "2026-11-05 22:42:26.795286 IST",
  "queue_snapshot_json": "[...stringified JSON...]",
  "evaluation_metrics_json": "{...stringified JSON...}"
}
```

Any tool (Python, DuckDB, Polars, ClickHouse) can query setups without schema errors:
```sql
SELECT symbol, direction, status, confluence_score, entry_price, stop_loss, take_profit, risk_reward_ratio, time_ist
FROM read_ndjson('Database/Signals/Trade_Setups/*.jsonl')
WHERE status = 'ACTIVE'
ORDER BY created_at_us DESC;
```

---

## 7. Operational Verification

To inspect or verify any part of the live pipeline:
- **Run Standalone Evaluator on Demand:**
  ```bash
  python 5_trade_evaluator.py --symbol BTC
  python 5_trade_evaluator.py --all
  ```
- **Inspect Live Master Orchestrator:**
  ```bash
  python run_pipeline.py --schedule
  ```
- **Launch Background Pipeline:**
  Double-click [`start_pipeline.bat`](file:///C:/Users/SIGMA/Documents/_Trading_1/start_pipeline.bat) to launch the master scheduler in a dedicated terminal.
