# EGM Architectural Specification: Unified Microstructure Confluence Engine & Empirical 300-Bar Statistical Learner

**Specification ID**: `EGM-SPEC-20261103-UNIFIED-CONFLUENCE-LEARNER`  
**Status**: APPROVED & SYNTHESIZED (Elephant Synthesis + Goldfish Design & Codebase Audits)  
**Authors**: Elephant (Deep Context Architect) + Goldfish Subagents (`egm-design`, `egm-codebase-details`)  
**Target Subsystems**:
- Analytical OLAP Engine (`Terminal/olap_engine.py`)
- Realtime Signal Evaluator (`Terminal/realtime_signal_evaluator.py`)
- Empirical 300-Bar Statistical Learner (`Terminal/empirical_signal_learner.py`)
- Persistent Ledgers (`Database/Signals/empirical_signal_outcomes.parquet`, `empirical_expectancy_matrix.parquet`)
- Data Pipeline & Feeds (`Database/Candle_Data/`, `Database/Whale_Orders/`, `Database/Liquidation_Data/`, `Database/Signals/`, `Database/Live_Liquidations/`)
- Venue Admission & Execution Safety (`Terminal/venue_admission.py`, `Terminal/matchtrader_live_trader.py`)

---

## 1. Executive Summary & Problem Resolution

This specification resolves two architectural imperatives mandated by the system architect:
1. **Total Data Confluence**: Every dumped microstructure dataset (Whale open walls, Whale history sweeps, Liquidation magnets, Key strike levels, Leverage tiers, Strength distance bands, Live TV liquidations, 300-bar rolling candles, and Auction profiles) is actively synthesized into confluence scoring. No dataset remains an unread orphan.
2. **Empirical Statistical Learning from 300 Rolling Candles**: Across all 31 tracked assets, the repository maintains an exact 300-bar M15 rolling window (`Database/Candle_Data/<SYMBOL>.parquet`) alongside historical footprint signals (`Database/Signals/coinglass_aggregated_futures_signals.parquet`). The engine continuously evaluates ground-truth forward trade outcomes (Win Rate, Expected Value $EV$, MFE, MAE) and weights live signals according to their statistical edge.
3. **Goldfish Audit Remediation**:
   - **Right-Censorship Elimination**: Signals occurring within $H=16$ bars of the latest candle are marked `IN_FLIGHT_PENDING` and excluded from calibration until resolved.
   - **Persistent Ledger Separation**: Historical signal outcomes are permanently appended to `empirical_signal_outcomes.parquet` so learning is preserved when candles roll off the 300-bar window.
   - **Empirical Bayes Shrinkage**: Small sample sizes ($N_K < 15$) are smoothed toward global prior win rates $WR_{\text{smoothed}} = \frac{N_K \cdot WR_K + M \cdot WR_{\text{global}}}{N_K + M}$ ($M=10$) to eliminate binomial noise.
   - **Pessimistic Intra-Bar Invariant**: If a candle touches both SL and TP, SL is strictly assumed hit first ($R_i = -1.0$).
   - **Dynamic Asset Class Normalization**: Normalizes scores across Crypto vs Macro (Forex/Metals/Indices) so non-crypto assets can achieve the 75.0 hurdle.
   - **TimesFM Orthogonal Governance**: TimesFM acts as an execution hurdle escalator ($C \ge 75.0 + \text{hurdle}$) and adverse directional veto, not an additive score term.
   - **Windows OS File Lock Prevention**: Parquet reads use memory-buffered byte streams to release file handles before metadata parsing.

---

## 2. Exhaustive Data Source Inventory & Confluence Mapping

| Dataset | Storage Path | Key Columns / Keys | Confluence Role & Weight Allocation |
| :--- | :--- | :--- | :--- |
| **1. Candlestick Rolling Window** | `Database/Candle_Data/<SYM>.parquet` | `open`, `high`, `low`, `close`, `total_volume`, `delta`, `cumulative_delta`, `cvd`, `poc_price`, `open_time_ms`, `close_time_ms` | **Ground Truth & Momentum**: Computes rolling ATR, CVD slope, volume-weighted fair value, and training ground for empirical learning. |
| **2. Footprint Signals** | `Database/Signals/coinglass_aggregated_futures_signals.parquet` | `symbol`, `sig_name`, `sig_code`, `side`, `price`, `candle_ist`, `volume`, `delta` | **Primary Catalyst (Base +30 pts)**: Detects institutional order-flow anomalies (Absorption, Exhaustion, Trapped Traders, Delta Divergence). |
| **3. Whale Open Orders** | `Database/Whale_Orders/Open_Orders/<SYM>_whale_open_orders.parquet` | `side`, `price`, `from_price_pct`, `remaining_usd`, `snapshot_time_utc` | **Order-Book Liquidity Walls (+/- 10 pts)**: Detects limit bid/ask walls within 3% of market price; calculates bid/ask depth ratio with dynamic half-life decay. |
| **4. Whale History Orders** | `Database/Whale_Orders/History_Orders/<SYM>_whale_history_orders.parquet` | `side`, `price`, `filled_usd`, `fills_count`, `duration_seconds`, `status` | **Aggressive Institutional Sweeps (+/- 5 pts)**: Measures whether recent filled whale trades represent aggressive market buying or selling over the last 60 minutes. |
| **5. Near-Field Liquidation Balance** | `Database/Liquidation_Data/Near_Field_Balance/<SYM>_near_field.parquet` | `longs_pct`, `shorts_pct`, `short_share_24h_pts`, `longs_count`, `shorts_count` | **Squeeze Fuel vs Cascade Fuel (+/- 10 pts)**: Determines whether market has trapped longs (cascade fuel for shorts) or trapped shorts (squeeze fuel for longs). |
| **6. Liquidation Magnet Zones** | `Database/Liquidation_Data/Magnet_Zones/<SYM>_magnet_zones.parquet` | `side`, `rank`, `price_low`, `price_high`, `zone_price`, `distance_pct`, `score` | **Dynamic Target Snapping (+/- 10 pts)**: Snaps TP1 and TP2 to high-density liquidation magnet zones within viable risk/reward horizons (0.4% - 4.0%). |
| **7. Key Liquidation Levels** | `Database/Liquidation_Data/Key_Levels/<SYM>_key_levels.parquet` | `price`, `distance_pct`, `strength`, `leverage_tier` (all lowercase) | **Strike Barrier Confirmation (+/- 5 pts)**: Evaluates high-density strike barriers immediately preceding stop-loss or take-profit levels. |
| **8. Leverage Tiers** | `Database/Liquidation_Data/Leverage_Tiers/<SYM>_leverage_tiers.parquet` | `tier` (25x, 50x, 100x), `shorts_strength`, `longs_strength`, `total_strength` | **Volatility & Cascade Risk (+/- 5 pts)**: Differentiates hyper-fragile 100x liquidation wicks from structural 25x institutional positioning pools. |
| **9. Strength by Distance Bands** | `Database/Liquidation_Data/Strength_By_Distance/<SYM>_strength_by_distance.parquet` | `band` (0-1%, 1-2%, 2-3%, 3-5%), `net_delta`, `total_volume` | **Near-Field Density Dominance (+/- 5 pts)**: Verifies that liquidation density within 0-2% confirms the proposed direction. |
| **10. Live TV Liquidations** | `Database/Live_Liquidations/latest_reversal_signals.json`, `latest_coinalyze_reversal_signals.json` | `long_liq_usd`, `short_liq_usd`, `is_spike`, `reversal_bias`, `exhaustion_score` | **Real-Time Capitulation Blow-Offs (+/- 15 pts)**: Detects intra-bar liquidation spikes signaling trend exhaustion and high-probability mean reversion. |
| **11. Auction Profiles** | `Database/Auction_Levels/<SYM>_auction_levels.parquet` | `day_vwap`, `day_vah`, `day_val`, `session_vwaps`, `fib_prime_alignment` | **Structural Value Confluence (+/- 15 pts)**: Assesses location relative to Day VWAP, Value Area (70% Volume), Initial Balance extensions, and 1H/4H macro trend. |

---

## 3. The 300-Bar Empirical Signal Learner Engine (`Terminal/empirical_signal_learner.py`)

### 3.1 Mathematical Formulation of Empirical Expectancy

For every asset $A \in \{1, \dots, 31\}$, the engine pairs historical footprint signals $S_t \in \mathcal{S}$ with the rolling 300 M15 candles $C_t \in \mathcal{C}$:

1. **Trade Simulation Geometry**:
   At the close of completed signal candle $t$, simulate entry at $P_0 = \text{close}_t$:
   $$\text{Stop Loss } (SL) = P_0 \times (1 \mp \text{sl\_pct})$$
   $$\text{Take Profit } (TP) = P_0 \times (1 \pm \text{sl\_pct} \times 1.8)$$
   where $\text{sl\_pct} = \max(0.005, \min(0.025, 1.5 \times \text{ATR}_{14} / P_0))$.

2. **Conservative Forward Path Evaluation ($H=16$ bars)**:
   - If $t > 300 - H$: Tag as `IN_FLIGHT_PENDING` and exclude from calibration.
   - For resolved trades $\tau \in [t+1, \min(t+H, 300)]$:
     - **Pessimistic Invariant**: If $\text{low}_\tau \le SL$ and $\text{high}_\tau \ge TP$ on the same bar $\tau$, assume SL was hit first ($R_i = -1.0$).
     - If $\text{high}_\tau \ge TP$ (BUY) or $\text{low}_\tau \le SL$ (SELL) without hitting stop: $R_i = +1.8$.
     - If $\text{low}_\tau \le SL$ (BUY) or $\text{high}_\tau \ge TP$ (SELL): $R_i = -1.0$.
     - If horizon expires at $t+H$ without hitting either: $R_i = \frac{P_{t+H} - P_0}{\text{risk\_dist}}$.
   - Realized Return Sample Mean:
     $$EV_K = \frac{1}{N_K} \sum_{i=1}^{N_K} R_i$$

3. **Empirical Bayes Smoothing**:
   $$WR_{\text{smoothed}} = \frac{N_K \cdot WR_K + M \cdot WR_{\text{global}}}{N_K + M} \quad (M = 10)$$

4. **Empirical Confluence Score Contribution**:
   $$\Delta \text{Score}_{\text{empirical}} = \begin{cases} 
   +20 & \text{if } WR_{\text{smoothed}} \ge 65\% \text{ and } EV_K \ge +0.40R \\
   +10 & \text{if } 55\% \le WR_{\text{smoothed}} < 65\% \text{ and } EV_K > 0 \\
   0 & \text{if } 45\% \le WR_{\text{smoothed}} < 55\% \text{ or } N_K < 5 \\
   -15 & \text{if } 35\% \le WR_{\text{smoothed}} < 45\% \\
   -25 & \text{if } WR_{\text{smoothed}} < 35\% \text{ and } N_K \ge 15 \text{ (Statistical Veto)}
   \end{cases}$$

---

## 4. Dynamic Asset Class Normalization & Confluence Scoring

To prevent non-crypto assets (Forex, Metals, Commodities, Indices) from being locked out of trading due to missing crypto-only feeds:

$$\text{Available Weight } W_{\text{avail}} = \sum_{m \in \mathcal{M}_{\text{avail}}} W_m^{\text{max}}$$
$$\text{Raw Score } C_{\text{raw}} = \sum_{m \in \mathcal{M}_{\text{avail}}} S_m$$
$$\text{Normalized Score } C_{\text{final}} = \min\left(100.0, \max\left(0.0, \frac{C_{\text{raw}}}{W_{\text{avail}}} \times 100.0\right)\right)$$

### Execution Gate:
- Live trade execution strictly requires $C_{\text{final}} \ge 75.0 + \text{hurdle}_{\text{timesfm}}$.
- TimesFM adverse directional veto (`adverse_veto == True`) immediately vetoes execution.
- Zero validation errors (e.g. `CAPACITY_RESERVED`, `BAR_NOT_CLOSED_INTRA_BAR_WICK`).

---

## 5. Fail-Soft Microstructure Ingestion Policy

- **Tier 1 Feeds (Mandatory)**: `Candle_Data` (300 bars) and `Signals`. Without these, data validity fails.
- **Tier 2 Feeds (Graceful Fail-Soft)**: `Whale_Orders`, `Liquidation_Data`, `Live_Liquidations`.
  - For assets not tracked in the 12-symbol whale list (like LTC), `query_whale_walls` returns `whale_imbalance = 0.50`, `data_valid = True`, `feed_status = 'FAIL_SOFT_NEUTRAL'`.
  - Missing feeds contribute 0 raw points and are excluded from $W_{\text{avail}}$, preventing false `MICROSTRUCTURE_DATA_UNQUALIFIED` vetoes.
