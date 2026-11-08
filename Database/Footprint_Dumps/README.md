# Institutional Footprint Order Flow & Auction Profile Architecture
========================================================================================
**Repository Path**: `Database/Footprint_Dumps/<SYMBOL>_<BAR_TYPE>_footprint.parquet`  
**Pipeline Pillar**: Pillar 1 (Order Flow Footprint, Microstructure Signals & Auction Profiles)  
**Data Source**: CoinGlass Footprint Engine via Chrome DevTools Protocol (`window.__cg_term`)  
**Target Schema**: Strict 16-Column Specification across 3 Bar Engines  
========================================================================================

## 1. Overview & Institutional Mandate

Traditional candlestick charts plot only four price levels (Open, High, Low, Close) and aggregate volume, hiding the internal microstructure of who bought, who sold, and at what price. **Footprint order flow charts** dismantle every candlestick into a discrete bid/ask diagonal ladder, revealing the interaction between aggressive market orders and passive limit liquidity.

The data in this directory captures complete tick-level order flow telemetry directly from CoinGlass's in-memory React Fiber runtime (`window.__cg_term`) across three distinct bar aggregation engines. This telemetry powers the trading engine's auction market profile analysis, volume delta tracking, and trapped trader detection.

---

## 2. The 3 Distinct Bar Aggregation Engines

To avoid the temporal distortions inherent to time-based charts during periods of low activity or extreme volatility bursts, the system ingests data across three distinct engines:

1. **Time Bars (15-Minute / `15Min`)**:
   * *File Pattern*: `Database/Footprint_Dumps/<SYMBOL>_time_footprint.parquet`
   * *Application*: Aligned with standard 15-minute candle closes for synchronization with TradingView liquidations and multi-timeframe moving averages.
2. **Discrete Dynamic Volume Bars (`volume`)**:
   * *File Pattern*: `Database/Footprint_Dumps/<SYMBOL>_volume_footprint.parquet`
   * *Application*: Each bar closes strictly after a predetermined volume quota is transacted (e.g. 2,000 BTC, 50,000 ETH). Eliminates low-volume chop and highlights periods of heavy institutional turnover.
3. **Discrete Dynamic Price Range Bars (`range`)**:
   * *File Pattern*: `Database/Footprint_Dumps/<SYMBOL>_range_footprint.parquet`
   * *Application*: Each bar closes strictly after price traverses a fixed price distance (e.g. 200 USDT BTC, 10 USDT ETH). Completely independent of time, isolating volatility expansions and trend continuations.

---

## 3. Strict 16-Column Footprint Specification

Every Parquet file in `Database/Footprint_Dumps/` adheres strictly to the following 16-column schema:

| Column Name | Type | Description & Quantitative Meaning |
| :--- | :--- | :--- |
| **`Time`** | `str` | Candle start timestamp formatted in ISO 8601 (`YYYY-MM-DD HH:MM:SS`). |
| **`Open`** | `float` | Candle opening price level. |
| **`High`** | `float` | Candle maximum price excursion. |
| **`Low`** | `float` | Candle minimum price excursion. |
| **`Close`** | `float` | Candle closing price level. |
| **`Candle_Vwap_Price`** | `float` | Intrabar midpoint volume-weighted average price: $\frac{\sum P_i \cdot V_i}{\sum V_i}$. Eliminates the -0.5 tick downward bias of naive tick midpoints. |
| **`Candle_Volume`** | `float` | Total transacted volume within the candle ($V_{\text{buy}} + V_{\text{sell}}$). |
| **`Candle_Delta`** | `float` | Net aggressive volume ($V_{\text{buy}} - V_{\text{sell}}$). Positive indicates buyer dominance; negative indicates seller dominance. |
| **`Candle_Delta_Percent`**| `float` | Relative delta ratio: $\frac{\Delta}{V_{\text{total}}} \times 100\%$. Normalized indicator of aggressive conviction. |
| **`Candle_CVD`** | `float` | Continuous Cumulative Volume Delta across the active trading session. Eliminates viewport-boundary step jumps. |
| **`Candle_Max_Delta`** | `float` | Highest intraday delta recorded during the lifetime of the bar (peak buyer effort). |
| **`Candle_Min_Delta`** | `float` | Lowest intraday delta recorded during the lifetime of the bar (peak seller effort). |
| **`Candle_Trades`** | `int` | Total number of individual trade transactions comprising the bar. |
| **`Candle_Avg_Trade`** | `float` | Average trade size: $\frac{V_{\text{total}}}{\text{Trades}}$. High values indicate institutional block participation. |
| **`Candle_BuySell_Ratio`**| `float` | Ratio of aggressive buying volume to aggressive selling volume ($\frac{V_{\text{buy}}}{V_{\text{sell}}}$). |
| **`Candle_Signal`** | `str` | Microstructure signal classification identified by the footprint engine (or empty string). |

---

## 4. The 6 Core Microstructure Signals

1. **Absorption (`abs`)**:
   * *Microstructure*: Aggressive market orders fail to push price further. Large passive limit orders absorb all aggressor volume.
   * *Signal Interpretation*: Bullish Absorption occurs at candle lows (sellers absorbed); Bearish Absorption occurs at candle highs (buyers absorbed).
2. **Delta Divergence (`div`)**:
   * *Microstructure*: Price makes a higher high, but `Candle_Delta` is negative (buyers exhausted); or price makes a lower low, but `Candle_Delta` is positive (sellers exhausted).
   * *Signal Interpretation*: Early warning of trend exhaustion and impending mean-reversion.
3. **Cumulative Volume Delta Divergence (`sdiv` / Bull Div / Bear Div)**:
   * *Microstructure*: Structural divergence between price trend and multi-bar CVD trend over rolling 10-bar windows.
4. **Exhaustion (`exh`)**:
   * *Microstructure*: Aggressive market orders dry up completely at candle wicks (e.g. low volume and low delta at extremes), showing lack of continuation interest.
5. **Trapped Shorts (`trap`)**:
   * *Microstructure*: High negative delta concentrated at the absolute candle low, followed by price closing back above the cluster. Short sellers are trapped in underwater positions.
   * *Signal Interpretation*: High-confluence long entry setup; trapped shorts become forced buyers on stop outs.
6. **Trapped Longs (`trap`)**:
   * *Microstructure*: High positive delta concentrated at the absolute candle high, followed by price closing back below the cluster. Long buyers are trapped in underwater positions.
   * *Signal Interpretation*: High-confluence short entry setup; trapped longs become forced sellers on stop outs.

---

## 5. Auction Market Profile Telemetry (`Database/Auction_Levels/`)

For each asset, auction market profiles are persisted to `Database/Auction_Levels/<SYMBOL>_auction_levels.parquet`:

* **Point of Control (POC)**: The exact price strike where the highest concentration of volume was transacted during the session. Acts as the primary institutional fair value benchmark.
* **Value Area High (VAH)**: Upper boundary of the 70% volume distribution. Price above VAH is considered overbought in auction theory.
* **Value Area Low (VAL)**: Lower boundary of the 70% volume distribution. Price below VAL is considered oversold in auction theory.
* **Single Prints**: Low-volume price intervals representing rapid, uncontested price discovery. These zones act as strong support/resistance when revisited.

---

## 6. CoinGlass Footprint FAQ Insights

*(Sourced directly from CoinGlass Footprint Documentation)*

### FAQ 1: How does the diagonal bid/ask ladder work?
> In modern electronic markets, trades match aggressively across the bid-ask spread: buy market orders lift the ask, and sell market orders hit the bid. The footprint ladder displays volume diagonally: aggressive buys at price $P$ are compared diagonally against aggressive sells at price $P - 1\text{ tick}$.

### FAQ 2: What are stacked imbalances?
> A stacked imbalance occurs when multiple consecutive price levels show aggressive buying or selling exceeding opposing volume by a factor of 3:1 or 4:1 (e.g., 300% to 400% imbalance). Stacked buy imbalances establish institutional demand zones; stacked sell imbalances establish supply zones.
