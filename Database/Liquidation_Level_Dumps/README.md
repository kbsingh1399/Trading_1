# Institutional Liquidation Level Telemetry & Magnet Zone Architecture
========================================================================================
**Repository Path**: `Database/Liquidation_Level_Dumps/<SYMBOL>/`  
**Pipeline Pillar**: Pillar 3 (Liquidation Density, Magnet Zones & Near-Field Microstructure)  
**Data Source**: CoinGlass Liquidation Levels Engine via Headless Chrome CDP (`127.0.0.1:9222`)  
**Target Schema**: 15 Parquet Files per Asset across 3 Discrete Lookback Horizons (`3D`, `7D`, `30D`)  
========================================================================================

## 1. Overview & Institutional Mandate

In derivative markets, futures contracts are traded with high leverage. When market price moves aggressively against a leveraged position, the exchange margin engine initiates an automated forced close—a **liquidation**. Because forced liquidations are executed as **unconditional market orders** (`IOC` / Market), they consume available orderbook depth, causing sharp price slippage and initiating self-reinforcing cascading chain reactions ("long squeeze" or "short squeeze").

The data in this directory captures the complete empirical liquidation surface from CoinGlass across three distinct lookback horizons (`3D · 1m`, `7D · 3m`, and `30D · 15m`). Rather than treating liquidations as retrospective events, this system models them as forward-looking **liquidity density maps**, identifying high-probability magnet zones where market makers, algorithmic high-frequency participants, and informed institutional capital target resting liquidation pools.

---

## 2. Directory Structure & File Naming Conventions

For every mutually inclusive asset (`<SYMBOL>`), an isolated directory is maintained containing exactly 15 Parquet tables:

```
Database/Liquidation_Level_Dumps/<SYMBOL>/
├── 3D_<SYMBOL>_KeyLiquidationLevel.parquet
├── 3D_<SYMBOL>_StrengthByDistance.parquet
├── 3D_<SYMBOL>_MagnetZones.parquet
├── 3D_<SYMBOL>_OpenLevelLeverageStrength.parquet
├── 3D_<SYMBOL>_NearFieldBalance.parquet
├── 7D_<SYMBOL>_KeyLiquidationLevel.parquet
├── 7D_<SYMBOL>_StrengthByDistance.parquet
├── 7D_<SYMBOL>_MagnetZones.parquet
├── 7D_<SYMBOL>_OpenLevelLeverageStrength.parquet
├── 7D_<SYMBOL>_NearFieldBalance.parquet
├── 30D_<SYMBOL>_KeyLiquidationLevel.parquet
├── 30D_<SYMBOL>_StrengthByDistance.parquet
├── 30D_<SYMBOL>_MagnetZones.parquet
├── 30D_<SYMBOL>_OpenLevelLeverageStrength.parquet
└── 30D_<SYMBOL>_NearFieldBalance.parquet
```

---

## 3. Parameter Specifications & Microstructure Significance

### 1. Key Liquidation Levels (`<HORIZON>_<SYMBOL>_KeyLiquidationLevel.parquet`)
* **CoinGlass Scope**: *"Key liquidation levels: Within ±4% · sorted by strength"*
* **Columns**:
  * `Asset` (`str`): Ticker symbol (e.g. `BTC`, `ETH`, `SOL`).
  * `Time` (`str`): Lookback horizon identifier (`3D`, `7D`, `30D`).
  * `Side` (`str`): Position side subject to forced liquidation (`Short` or `Long`).
  * `Price` (`float`): Estimated price level where positions will be forcibly closed.
  * `Distance` (`str`): Percentage distance from the active mark price (e.g. `+1.59%`, `-2.04%`).
  * `Leverage` (`str`): Exchange leverage tier responsible for the cluster (`High leverage`, `Mid leverage`, `Low leverage`).
  * `Strength` (`float`): Normalized cumulative strength score ($[0.0, 100.0]$) representing liquidation density.
* **Microstructure Significance & Use in Quantitative Strategy**:
  * **Short Liquidation Levels**: Located *above* the current price. When price breaches these levels, short positions are bought back forcibly at market, adding sudden aggressive buying volume.
  * **Long Liquidation Levels**: Located *below* the current price. When price breaches these levels, long positions are sold forcibly at market, adding sudden aggressive selling volume.
  * **Algorithmic Take-Profit & Reversal Gate**: Institutional algorithms use $P_{\text{liq}}$ as natural exit targets (placing limit Take-Profit orders right before the cluster) or fade-entry levels (entering counter-trend reversals once the forced-exit volume is absorbed by footprint iceberg orders).

---

### 2. Strength By Distance (`<HORIZON>_<SYMBOL>_StrengthByDistance.parquet`)
* **CoinGlass Scope**: *"Strength by distance: Total strength in each band from the last price"*
* **Columns**:
  * `Asset` (`str`): Ticker symbol.
  * `Time` (`str`): Lookback horizon (`3D`, `7D`, `30D`).
  * `Band` (`str`): Distance proximity band (`0-1%`, `1-2%`, `2-3%`, `3-5%`, `5-10%`).
  * `Short_Strength` (`float`): Total short liquidation strength accumulated within the band.
  * `Long_Strength` (`float`): Total long liquidation strength accumulated within the band.
  * `Total_Strength` (`float`): Aggregate combined strength ($S_{\text{short}} + S_{\text{long}}$).
  * `Short_Share_Pct` (`str`): Percentage share of short liquidations within the band.
  * `Long_Share_Pct` (`str`): Percentage share of long liquidations within the band.
* **Microstructure Significance & Use in Quantitative Strategy**:
  * **Near-Field Liquidity Density**: Bands within $0-1\%$ and $1-2\%$ represent immediate friction and volatility amplifiers. If `Short_Share_Pct` exceeds $75\%$ in the $0-1\%$ band, even a modest upside tick can trigger a cascade.
  * **Asymmetry Imbalance Ratio**: Evaluated as $\mathcal{A}_{\text{band}} = \frac{S_{\text{short}} - S_{\text{long}}}{S_{\text{short}} + S_{\text{long}}}$. Positive values indicate heavy upside vulnerability (short squeeze fuel); negative values indicate heavy downside vulnerability (long waterfall fuel).

---

### 3. Magnet Zones (`<HORIZON>_<SYMBOL>_MagnetZones.parquet`)
* **CoinGlass Scope**: *"Magnet zones: Where leveraged positions and their forced exits are stacked · adjacent open levels merged into bands · top 3 per side within ±10% · a risk map, not a forecast"*
* **Columns**:
  * `Asset` (`str`): Ticker symbol.
  * `Time` (`str`): Lookback horizon (`3D`, `7D`, `30D`).
  * `Side` (`str`): `Shorts get liquidated` (above price) or `Longs get liquidated` (below price).
  * `Rank` (`int`): Cluster density rank ($1, 2, 3$).
  * `Price_Range` (`str`): Consolidated boundary interval (e.g. `76,375.8 - 77,329.8`).
  * `Price_Low` (`float`): Lower bound of the magnet pool.
  * `Price_High` (`float`): Upper bound of the magnet pool.
  * `Distance` (`str`): Percentage offset from current price to the center of the band (e.g. `+1.15%`, `-2.04%`).
  * `Strength` (`float`): Aggregate liquidation intensity accumulated across the merged band.
  * `Band_Width` (`str`): Percentage width of the price interval (e.g. `width 1.26%`).
  * `Status` (`str`): `Active` or `No significant zone on this side`.
* **Microstructure Significance & Use in Quantitative Strategy**:
  * **Gravitational Attraction (Magnet Effect)**: In liquid auction markets, price acts like a heat-seeking missile towards unhedged liquidity pools. High-density magnet zones (Strength $> 500$) exert strong gravitational pull on order flow because liquidity takers know substantial fill volume exists within that boundary.
  * **Cascading Risk**: Once price enters the lower threshold (`Price_Low` for shorts, `Price_High` for longs), forced exits trigger automatically until the band is fully swept or absorbed by opposing limit walls.

---

### 4. Open-Level Leverage Strength (`<HORIZON>_<SYMBOL>_OpenLevelLeverageStrength.parquet`)
* **CoinGlass Scope**: *"By leverage tier: Open-level strength per leverage tier"*
* **Columns**:
  * `Asset` (`str`): Ticker symbol.
  * `Time` (`str`): Lookback horizon (`3D`, `7D`, `30D`).
  * `Tier` (`str`): Leverage classification (`High leverage`, `Mid leverage`, `Low leverage`).
  * `Short_Strength` (`float`): Total short liquidation strength in this tier.
  * `Long_Strength` (`float`): Total long liquidation strength in this tier.
  * `Total_Strength` (`float`): Sum of short and long liquidation strength.
  * `Short_Share_Pct` (`str`): Percentage share of shorts in this leverage bracket.
  * `Long_Share_Pct` (`str`): Percentage share of longs in this leverage bracket.
* **Microstructure Significance & Use in Quantitative Strategy**:
  * **High Leverage (50x - 100x+)**: Tight liquidation tolerances ($\pm 0.5\%$ to $\pm 1.5\%$). Characterized by retail traders and high-frequency momentum algorithms. Easily triggered by normal intraday noise.
  * **Mid Leverage (10x - 25x)**: Medium liquidation tolerances ($\pm 2.0\%$ to $\pm 6.0\%$). Characterized by directional swing traders and mid-size funds.
  * **Low Leverage (2x - 5x)**: Wide liquidation tolerances ($\pm 10.0\%$ to $\pm 30.0\%$). Characterized by institutional hedging and treasury operations.

---

### 5. Near-Field Balance (`<HORIZON>_<SYMBOL>_NearFieldBalance.parquet`)
* **CoinGlass Scope**: *"Near-field balance: Open strength within ±3% of price · shorts vs longs · how it moved over the last 24h"*
* **Columns**:
  * `Asset` (`str`): Ticker symbol.
  * `Time` (`str`): Lookback horizon (`3D`, `7D`, `30D`).
  * `Long_Pct` (`str`): Percentage of near-field liquidity belonging to long positions (e.g. `36%`).
  * `Long_Strength` (`float`): Raw numerical strength of open long liquidations within $\pm 3\%$.
  * `Short_Pct` (`str`): Percentage of near-field liquidity belonging to short positions (e.g. `64%`).
  * `Short_Strength` (`float`): Raw numerical strength of open short liquidations within $\pm 3\%$.
  * `Total_Strength` (`float`): Total open strength within the immediate $\pm 3\%$ perimeter.
  * `Bias` (`str`): Immediate directional imbalance (`Short-Heavy`, `Long-Heavy`, or `Balanced`).
  * `Interpretation` (`str`): Market commentary on directional acceleration (e.g. *"Heavier on shorts: an up-move would be amplified by short liquidations"*).
* **Microstructure Significance & Use in Quantitative Strategy**:
  * **Directional Asymmetry Ratio ($\delta_{\text{NF}}$)**:
    $$\delta_{\text{NF}} = \frac{S_{\text{short}} - S_{\text{long}}}{S_{\text{short}} + S_{\text{long}}}$$
    When $\delta_{\text{NF}} > +0.25$ ($>62.5\%$ shorts), upward price momentum encounters very little long liquidation resistance and is mechanically accelerated by short stops.
  * **24-Hour Regime Shift Detection**: Tracking the delta of short share over 24 hours reveals whether smart money is accumulating positions against the crowd.

---

## 4. CoinGlass FAQ Insights & Architectural Foundations

*(Sourced directly from CoinGlass Liquidation Levels FAQ documentation)*

### FAQ 1: What is a liquidation levels chart?
> Futures are traded with leverage. When price moves far enough against a position, the exchange closes it by force and the trader loses their margin. The chart records these estimated prices candle by candle. Traders watch it because liquidations turn into **unconditional market orders**. Once price runs into a dense cluster of forced exits, that extra market volume accelerates the move, often triggering the next level in a cascade.

### FAQ 2: What is the difference between 3D, 7D, and 30D?
> * **3D · 1m**: High-frequency tactical resolution. 1-minute candle precision capturing freshly opened intraday leverage positions. Best for scalping, immediate trade entry timing, and trailing stops.
> * **7D · 3m**: Intermediate swing resolution. 3-minute candle precision capturing weekly positioning, weekend liquidity pools, and major leverage resets.
> * **30D · 15m**: Macro structural resolution. 15-minute candle precision tracking multi-week accumulation and major institutional liquidation cliffs.

### FAQ 3: What do the lines on a liquidation levels chart mean?
> Each line is the estimated liquidation price of a batch of positions. It starts at the candle where those positions opened, and extends to the right until price hits it. **A brighter line means a larger estimated liquidation size.** Only lines that are still running right are active risks; lines that stop at a wick were swept.

### FAQ 4: How are Magnet Zones formed and why are they not forecasts?
> Magnet zones merge adjacent open liquidation levels into continuous bands within $\pm 10\%$ of price. They highlight where forced exits are stacked in close price proximity. **They are risk maps, not price predictions:** price may move toward a magnet zone due to order flow gravitation, but once swept, the market often exhausts and reverses sharply.

---

## 5. Algorithmic Trade Evaluation Integration

The quantitative decision engine in `trade_evaluator.py` integrates Pillar 3 liquidation data using deterministic mathematical rules:

1. **Magnet Attraction Pull Factor ($M_{\text{pull}}$)**:
   $$M_{\text{pull}} = \frac{\text{Strength}_{\text{nearest}}}{\max(0.1, |\text{Distance}_{\text{nearest}}|)} \times \text{sign}(\text{Distance})$$
2. **Cascading Fuel Threshold**:
   $$\text{If } |\delta_{\text{NF}}| \ge 0.28 \text{ and } \text{Strength}_{\text{nearest}} \ge 500.0 \implies \text{Liquidation Squeeze Active}$$
3. **Take-Profit Clamping**:
   Stop Loss and Take Profit levels generated by MT5 punchers (`mt5_gui_puncher.py`) automatically align with the outer boundaries of the nearest high-strength Magnet Zones to maximize risk-reward ($R:R_{\text{eff}} \ge 2.0$).
