# Institutional Whale Orderbook Blotter & Deep Wall Architecture
========================================================================================
**Repository Path**: `Database/Whale_Dumps/<SYMBOL>_whale_open_orders.parquet`  
**Pipeline Pillar**: Pillar 2 (Large Limit Orderbook Depth & Institutional Blotters)  
**Data Source**: CoinGlass Large Orderbook Statistics via In-Browser Chrome CDP (`127.0.0.1:9222`)  
**Target Schema**: Strict 8-Column User Excel Specification  
========================================================================================

## 1. Overview & Institutional Mandate

Large institutional participants, algorithmic market makers, and proprietary trading desks rarely execute large block orders via aggressive market orders due to adverse price slippage. Instead, they position massive **resting limit orders** ("whale walls") across centralized and decentralized liquidity venues (Binance, OKX, Bybit, Coinbase, Hyperliquid, Bitget, Bitfinex, Deribit, Gate.io).

These defending limit orders act as institutional structural barriers:
* **Bid Walls (Support)**: Absorb incoming sell market orders, halting downward momentum.
* **Ask Walls (Resistance)**: Absorb incoming buy market orders, halting upward momentum.

The data in this directory captures real-time empirical whale orderbook depth directly from the CoinGlass Large Orderbook engine. All orders are sorted strictly in Order Book Depth order (Price descending: Asks down to Mark Price, then Bids down to Low Bids) and formatted to the exact 8-column user Excel specification.

---

## 2. Strict 8-Column User Schema Specification

Every Parquet file in `Database/Whale_Dumps/<SYMBOL>_whale_open_orders.parquet` adheres strictly to the following 8 columns:

| Column Name | Python Type | Example Value | Description & Invariant |
| :--- | :--- | :--- | :--- |
| **`Venue/Side`** | `str` | `Coinbase Spot Ask`, `Binance Perp Bid` | Combined exchange venue name and orderbook side (`Bid` = Buy Support, `Ask` = Sell Resistance). Formatted without UI stars (`★☆`). |
| **`Price`** | `str` | `76,096.10`, `0.0521` | Limit order strike price formatted with cent precision (`toLocaleString` with commas for $\ge 1,000$). |
| **`From Price`** | `str` | `+1.25%`, `-0.85%` | Relative percentage distance from live aggregated mark price: $\frac{P_{\text{order}} - P_{\text{mark}}}{P_{\text{mark}}} \times 100\%$. Always signed with `+` or `-`. |
| **`Initial_USD`** | `str` | `$4.04M`, `$147.23K`, `$950` | Original total dollar value allocated to the order when submitted. Abbreviated as `$B`, `$M`, `$K`, or integer `$`. |
| **`Remaining_USD`** | `str` | `$4.04M`, `$85K` | Current active unfilled dollar volume defending this price level. Key metric for support/resistance strength. |
| **`Filled_USD`** | `str` | `$0`, `$250K` | Dollar volume executed/absorbed by incoming market orders against this wall. |
| **`Duration`** | `str` | `3d 22h`, `4h 15m`, `35s` | Continuous time elapsed since order placement on the venue book: `Xs`, `Xm Ys`, `Xh Ym`, or `Xd Yh`. |
| **`Status`** | `str` | `Open`, `Partially Filled` | Execution state (`Partially Filled` if `Filled_USD > 0` or fills recorded; otherwise `Open`). |

---

## 3. Microstructure Significance & Quantitative Mechanics

### 1. Bayesian Age Decay Modeling ($30\text{m}$ Half-Life)
In electronic orderbooks, resting limit orders are subject to the **fleeting order paradox**: many large walls are transient (used for algorithmic spoofing or liquidity signaling) and are cancelled before execution. However, orders that remain static on the book over prolonged periods demonstrate genuine passive defending intent.

The quantitative pipeline models order persistence using an empirical Bayesian exponential decay kernel:
$$w(t) = \exp\left(-\frac{\Delta t}{\tau}\right)$$
where:
* Half-life $t_{1/2} = 30 \text{ minutes} = 1,800 \text{ seconds}$.
* Decay constant $\tau = \frac{1800}{\ln(2)} \approx 2,596.84 \text{ seconds}$.
* Downstream algorithms in `trade_evaluator.py` filter for orders with $w(t) \ge 0.25$ (active for $\le 45$ minutes) to ensure fresh defending depth.

### 2. The Universal Size Threshold Invariant ($0)
By default, CoinGlass initializes the Size Threshold dropdown to `≥$50K` or `≥$100K`. For large-cap Bitcoin (`BTC`), orders $\ge \$100\text{K}$ are abundant. However, for mid-caps, altcoins, and precious metals (`DOT`, `LTC`, `XAG`, `UNI`), setting a $\$50\text{K}$ ceiling filters out valid institutional liquidity, causing tables to appear completely empty ("No rows match the current filters").

**The Invariant**: All scrapers in `_Trading_1` strictly automate selecting the **`$0` (no limit)** threshold via CDP before reading React Fiber or DOM rows. This guarantees that:
1. Every resting wall down to $\$1$ is captured.
2. Complete cumulative depth is preserved.
3. 100% data parity is maintained across all 121 assets.

---

## 4. CoinGlass Large Orderbook FAQ Insights

*(Sourced directly from CoinGlass Large Orderbook Statistics FAQ documentation)*

### FAQ 1: What is the Large Orderbook Blotter?
> The Large Orderbook table aggregates active large limit orders across the world's premier derivatives and spot exchanges (Binance, OKX, Bybit, Coinbase, Hyperliquid, Bitget, Bitfinex). It provides a consolidated view of institutional depth, revealing where major players are resting bids and offers.

### FAQ 2: What is the difference between Spot and Perp orders?
> * **Spot Orders**: Unleveraged physical asset orders (e.g. `Coinbase Spot`, `Binance Spot`). Represent genuine physical accumulation or distribution with zero liquidation risk. Strongest fundamental support/resistance.
> * **Perpetual Orders**: Leveraged derivative contracts (e.g. `Binance Perp`, `Hyperliquid Perp`, `Bybit Perp`). Represent directional positioning, hedging, or market maker inventory. Subject to funding rate dynamics.

### FAQ 3: How do institutional traders detect spoofing?
> Spoofing occurs when a trader places a large wall to intimidate the market, only to cancel it as price approaches. By tracking **Duration** and **Filled_USD**, algorithmic models distinguish between genuine defending orders (which absorb incoming volume and remain static) and fake spoofing orders (which vanish within seconds).

---

## 5. Algorithmic Trade Evaluation Integration

In `trade_evaluator.py`, whale orderbook data is transformed into empirical support and resistance factors:

1. **Defending Wall Proximity Gate**:
   * Defending Bid Wall: Order located *below* current mark price within $0.1\%$ to $2.5\%$.
   * Defending Ask Wall: Order located *above* current mark price within $0.1\%$ to $2.5\%$.
2. **Defending Liquidity Ratio**:
   $$\mathcal{R}_{\text{whale}} = \frac{\sum \text{Remaining\_USD}_{\text{bids}}}{\sum \text{Remaining\_USD}_{\text{asks}}}$$
   When $\mathcal{R}_{\text{whale}} > 1.8$, institutional bids strongly outweigh asks, providing high-confluence backing for bullish mean-reversion setups.
