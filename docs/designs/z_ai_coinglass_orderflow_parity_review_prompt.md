# Z.AI INSTITUTIONAL QUANT REVIEW & CRITIQUE PROMPT
## Mission: Pure-API CoinGlass Order-Flow Signal Parity & Signal Frequency Normalization

```markdown
Role: Senior High-Frequency Quantitative Architect & Order Flow Microstructure Specialist (Citadel / Jump Trading / CoinGlass Engineering mindset)

Objective: 
Review, audit, critique, and mathematically re-calibrate the pure-API CoinGlass Footprint and Signal Generation Engine in repository `kbsingh1399/_Trading_1` (and `kbsingh1399/Trading_2`).
Specifically resolve the SIGNAL OVER-GENERATION DEFECT:
- Ground Truth: In historical CoinGlass CDP data (`Database/Signals/coinglass_aggregated_futures_signals.parquet`), 300 15m bars of ETH yield exactly 25 signals (~8.3% signal density, ~1 signal per 12 bars).
- Naive API Engine: The current implementation fires on 226 out of 300 bars (~75% signal density, nearly 10x over-generation!).
We require identical methodology, exact timeframe alignment, and strict signal density parity (~8-12% of bars) with high directional concordance.

---

### 1. REPOSITORY & DATA CONTEXT

- **Repository Root:** `c:\Users\SIGMA\Documents\_Trading_1`
- **Key Files Under Review:**
  1. `Terminal/universal_footprint_engine.py` (Core footprint aggregator, multi-bar synthesizer, and order-flow signal detector)
  2. `Terminal/unified_api_footprint_engine.py` (Pure API harvester querying Binance Futures klines, aggTrades, and open interest)
  3. `Database/Candle_Data/<SYMBOL>.parquet` (Stores 300-bar rolling M15 window with complete per-candle metrics and native `signals` struct array)
  4. `Database/Signals/coinglass_aggregated_futures_signals.parquet` (Historical benchmark: 23,800+ CoinGlass CDP records across BTC, ETH, SOL, etc.)
  5. `tools/compare_api_vs_coinglass.py` (Parity comparison harness)
  6. `run_pipeline.py` (Unified root launcher)

---

### 2. EMPIRICAL GROUND TRUTH FROM COINGLASS CDP PARQUET

Across the exact same 300-bar window on ETH 15m (`2026-11-02 01:30 UTC` to `2026-11-05 01:45 UTC`):
- **Total Historical CoinGlass Signals:** Exactly 25 signals.
- **Distribution of CoinGlass Signals:**
  - `abs` (Absorption): 6 signals (24%)
  - `div` (Single-Bar Delta Divergence): 6 signals (24%)
  - `exh` (Exhaustion): 7 signals (28%)
  - `sdiv` (Swing CVD Divergence): 5 signals (20%)
  - `trap` (Trapped Traders): 1 signal (4%)
- **CoinGlass Exact Phrasing & Mathematical Traces:**
  - *Absorption:* `"14K of taker buying at the high (30%+ of the bar's buying), yet closed near the low, bearish - 2,435.7"`
    *Rule:* Upper wick contains >= 30% of total bar volume, bar closes at the bottom third (`close_pos <= 0.35`), and total volume is elevated (`volume >= 1.25x 20-SMA`).
  - *Divergence:* `"Closed up on negative Delta (−17%), bearish"` / `"Closed down on positive Delta (+27%), bullish"`
    *Rule:* Quantile analysis reveals median `|delta_pct|` in CoinGlass is **17.3%** (with 25th percentile at 10.5%). Real divergence requires `|delta_pct| >= 12% - 15%` combined with a non-zero directional candle body (`|close - open| / range >= 0.30`).
  - *Exhaustion:* `"New 10-bar high with almost no volume in the top 10% of the bar, closed in the lower half, bearish"`
    *Rule:* Price makes a new 10-bar high/low, but volume in that extreme 10% wick is depleted, total candle volume is low (`volume <= 0.70x 20-SMA`), and the candle closes back in the opposite half.
  - *Trapped Traders:* `"Broke the 5-bar high on 7.0K of taker buying at the top, then fell back into the lower part of the bar 3 bars later. Breakout buyers trapped, bearish"`
    *Rule:* Break of 5-bar swing high/low with aggressive taker volume, followed by rejection back into the bar's lower/upper half within 1 to 3 bars.
  - *Swing CVD Divergence:* `"Higher high than 11-03 21:15, yet Cumulative Delta fell 25K over the swing. Buyers aren't following through, bearish"`
    *Rule:* Higher price high across a genuine swing (at least 3-5 bars apart) with strictly lower Cumulative Volume Delta (CVD).

---

### 3. AUDIT & CRITIQUE OBJECTIVES FOR Z.AI

Please deliver a thorough, rigorous quantitative review addressing the following:

1. **Signal Over-Generation Root Cause Analysis:**
   - Why did the naive implementation generate 226 signals across 300 bars?
   - Identify every threshold flaw: `min_delta_pct = 5.0` triggering on normal crypto delta noise; neutral midpoint closes `close_pos <= 0.50` flagging neutral dojis; lack of body size filters; volume multiplier gates set too low (`v >= 0.70 * avg_vol`).

2. **Mathematical Formulation & Threshold Calibration:**
   - Provide exact mathematical definitions and calibrated threshold values for:
     a) Single-Bar Divergence (`div`)
     b) Absorption (`abs`)
     c) Swing CVD Divergence (`sdiv`)
     d) Exhaustion (`exh`)
     e) Trapped Traders (`trap`)
   - Ensure the expected signal density on any 300-bar M15 window targets **20 to 35 signals (~7% to 12% density)**, matching CoinGlass ground truth.

3. **Multi-Bar Type Footprint Parity:**
   - How should Range Bars (e.g. `Range 10` ETH, `Range 200` BTC) and Volume Bars (e.g. `Volume 50K ETH`, `Volume 2.0K BTC`) be synthesized from 1m/5m klines and aggTrades to ensure signal timing parity with CoinGlass?
   - How should multiple concurrent signals on the same bar be stored and prioritized for execution scoring in `RealtimeSignalEvaluator`?

4. **Discrete Contract Sizing & 1:2 R:R Guardrail Integration:**
   - Verify that all signals emitted feed cleanly into `RealtimeSignalEvaluator` and `delta_exchange_live_trader.py` with fill-calibrated 1:2 R:R, fee-aware lot sizing, and dynamic trailing stop loss ratchets.

5. **Complete, Drop-In Replacement Patch:**
   - Provide the exact Python code modifications for `Terminal/universal_footprint_engine.py` and `tools/compare_api_vs_coinglass.py` that achieve:
     - Exact timeframe alignment
     - Reduced signal count from 226 down to ~25-35 on ETH 300 bars
     - >85% directional concordance with historical CoinGlass CDP signals
     - Complete test suite pass (207/207 tests green)
```
