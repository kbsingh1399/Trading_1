# Architectural Specification: 5 Standalone CoinGlass Crypto Trading & Execution Suite (v2.0)

**Document Path**: `docs/designs/crypto_five_standalone_pipeline_spec.md`  
**Date**: November 3, 2026  
**Architect**: EGM Elephant (Deep Context)  
**Evaluators**: EGM Goldfish Reviewers (Design Critic, Tech Lead, Codebase Details)  
**Status**: Approved with Goldfish Hardening Patches Applied  
**Target Accounts**: FundingPips MatchTrader (`101305360`, $100K Competition Account) & MT5 (`35296418`)  
**Scope**: 100% CoinGlass In-Browser CDP Scraping across Crypto Catalog  

---

## 1. Executive Summary & Problem Statement

Per user instruction:
The repository is cleaned and restructured so that daily operations are completely contained in **exactly 5 standalone files**:
1. **`1_coinglass_footprint_scraper.py`**: Scrapes Footprint chart order-flow signals (`Absorption`, `Exhaustion`, `Divergence`, `Bull div`, `Bear div`, `Trapped shorts/longs`) and 15m candle volume/delta data directly from CoinGlass via ephemeral CDP tabs.
2. **`2_coinglass_whales.py`**: Scrapes large orderbook statistics and resting whale bid/ask walls (> $1M / $500K) directly from CoinGlass Large Orderbook via ephemeral CDP tabs.
3. **`3_coinglass_liquidation_levels.py`**: Scrapes dual-resolution liquidation levels, magnet zones (top 3 short/long clusters), and near-field balance (±3% range) directly from CoinGlass Liquidation Levels via ephemeral CDP tabs.
4. **`4_coinglass_tv_scraper.py`**: Scrapes live liquidation timeseries, liquidation volume spikes, and reversal/exhaustion scores directly from CoinGlass TradingView interface via ephemeral CDP tabs.
5. **`5_wake_on_footprint_signal.py`**: Immediate reactive wakeup bridge. Continuously watches the footprint signals stream, executes DeepMind TimesFM + DuckDB OLAP confluence evaluation on completed bars, auto-dispatches live trades to MatchTrader when score $\ge 75.0$, sounds audible terminal bell, and prints live trade cards.

All non-essential, legacy, and ad-hoc scripts in root and `Terminal/` are safely moved into `archive/` to ensure absolute repository clarity.

---

## 2. Invariants & Strict Guardrails

### 2.1 CoinGlass Pure Browser Scraper Invariant
All market data (Footprint, Whales, Liquidation Levels, TV Liquidations) originates directly from CoinGlass in-browser scraping via Chrome CDP (`127.0.0.1:9222`). Zero external browser scraping dependencies.

### 2.2 Ephemeral Tab Hygiene & Orphan Reaper
Every scraper spawns an ephemeral tab tagged with its dedicated bot identifier (`bot=footprint_scraper`, `bot=whale_scraper`, `bot=liq_scraper`, `bot=tv_scraper`), harvests the required payload, and **closes the tab immediately** (`Page/close` via `/json/close/{tab_id}`).
- **`EphemeralCdpTab` Context Manager**: Guarantees tab closure even on unhandled exceptions or socket timeouts.
- **Active Orphan Reaper**: On startup and cycle completion, sweeps `http://127.0.0.1:9222/json` and terminates any orphaned tabs containing `bot=` that are older than 90 seconds. User tabs (`Fundingpips`, `Gmail`, personal tabs) are strictly preserved.

### 2.3 System-Wide WARP IP Rotation with Inter-Process Cooldown
To prevent the "thundering herd" network drops where multiple scrapers concurrently rotate Cloudflare WARP and sever the machine's connection:
- A shared file lock `Database/.locks/warp_rotation.lock` tracks the global rotation timestamp.
- A mandatory **120-second system-wide cooldown** is enforced across all 5 independent processes.
- WARP rotation is strictly blocked while an order execution is actively in-flight on MatchTrader.

### 2.4 Cooperative Scheduling & Zero Contention on Chrome CDP
- Rather than competing over a 2-second lock that causes skips:
  - **Script 1 (Footprint)**: Runs continuous 120s sweeps, yielding for 45s during reserved minutes (:00, :10, :15, :25, :35, :50, :55).
  - **Script 2 (Whales)**: Triggers on reserved minutes :00, :15, :35, :50.
  - **Script 3 (Liquidations)**: Triggers on reserved minutes :10, :25, :55.
  - **Script 4 (TV Spikes)**: Fast 60s sweep.
  - **Script 5 (MatchTrader Live Execution)**: Connects directly to the existing persistent `Fundingpips` tab WebSocket URL. It never calls `/json/new` or `/json/close`, guaranteeing **0ms contention** with scraping tabs.

### 2.5 Prop Firm Risk Envelope (FundingPips MatchTrader Account `101305360`)
- **Account Scale**: $100,000.00 challenge balance.
- **Total Drawdown Floor**: $90,000.00 ($10,000 max total loss).
- **Daily Drawdown Floor**: $95,000.00 ($5,000 max daily loss from opening equity).
- **Max Concurrent Open Positions**: Strictly capped at **2 open positions** across all instruments.
- **Risk Sizing**: 0.5% risk ($500 max risk, default $25–$50 on tight stops) per trade via discrete lot grid solver.
- **Execution Hurdle**: Quantitative confluence score $\ge 75.0 / 100$ on fully closed 15m candles (`bar_close_ms <= now_ms`).
- **Mandatory Stop Loss & Take Profit**: Hard stops and minimum 1.5:1 reward-to-risk required.

### 2.6 Primary IST Time Mapping & Lock-Free Atomic Parquet
All parquet files store Indian Standard Time (IST, UTC+5:30) as primary columns: `open_time_ist`, `close_time_ist`, `time_ist`, `candle_ist`, `open_time_ist_ms`, `close_time_ist_ms`. All file writes use `.tmp` atomic replace via `runtime_io.atomic_parquet` with zero `.lock` files in `Database/Candle_Data/`.

---

## 3. Five Standalone Scripts Specification

### 3.1 Script 1: `1_coinglass_footprint_scraper.py`
- **Target URL**: `https://www.coinglass.com/footprint?bot=footprint_scraper`
- **Payload Harvested**:
  - Hooks internal React Fiber `window.__cg_term`.
  - Sets timeframe strictly to 15m (`term.setItv('15m')`).
  - Iterates target crypto assets via `term.pickPair(sym, 'agg-perp')`.
  - Extracts all 6 Footprint signals: `Absorption`, `Exhaustion`, `Divergence`, `Bull div`, `Bear div`, `Trapped shorts`, `Trapped longs`.
  - Extracts 15m candle bars (Open, High, Low, Close, Total Volume, Delta, Buy Volume, Sell Volume, POC price).
- **Persistence**:
  - `Database/Signals/coinglass_aggregated_futures_signals.parquet` (DuckDB OLAP bound)
  - `Database/Signals/footprint_signals.jsonl` (streaming watcher)
  - `Database/Candle_Data/<SYMBOL>.parquet` (exact 300-bar rolling window with IST mapping)
  - `Database/Auction_Levels/all_symbols_auction_levels.parquet`
- **CLI Options**:
  - `--daemon`: Runs continuous background cycle with cooperative scheduling.
  - `--once`: Sweeps target batch once and exits.

### 3.2 Script 2: `2_coinglass_whales.py`
- **Target URL**: `https://www.coinglass.com/large-orderbook-statistics?symbol=<SYM>&bot=whale_scraper`
- **Payload Harvested**:
  - Open limit orders table (bids & asks > $500k/$1M).
  - Orderbook resting depth, bid wall USD, ask wall USD, whale imbalance ratio within ±3% of price.
  - Continuous Bayesian prior decay $\exp(-\text{age}/\tau)$ with $\tau = 1800/\ln(2)$ (30m half-life).
- **Persistence**:
  - `Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet`
  - `Database/Whale_Orders/History_Orders/<SYMBOL>_whale_history_orders.parquet`
  - `Database/Whale_Orders/whale_market_snapshots.parquet`
- **CLI Options**:
  - `--daemon`: Runs on scheduled cadences (minutes :00, :15, :35, :50).
  - `--once`: Sweeps and exits immediately.

### 3.3 Script 3: `3_coinglass_liquidation_levels.py`
- **Target URL**: `https://www.coinglass.com/liquidation-levels?symbol=<SYM>&bot=liq_scraper`
- **Payload Harvested**:
  - Key liquidation levels (dual resolution ±1% to ±8%).
  - Magnet zones (top 3 short and long clusters: `zone_price`, distance %, liquidation score).
  - Near-field balance (±3% range long/short ratio and 24h delta).
  - Strength by distance & leverage tiers (25x, 50x, 100x).
- **Persistence**:
  - `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet`
  - `Database/Liquidation_Data/Magnet_Zones/<SYMBOL>_magnet_zones.parquet`
  - `Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet`
  - `Database/Liquidation_Data/liquidation_snapshots.parquet`
- **CLI Options**:
  - `--daemon`: Runs on scheduled cadences (minutes :10, :25, :55).
  - `--once`: Sweeps and exits immediately.

### 3.4 Script 4: `4_coinglass_tv_scraper.py`
- **Target URL**: `https://www.coinglass.com/tv/Binance_<SYM>USDT?bot=tv_scraper`
- **Payload Harvested**:
  - TradingView liquidation chart series (15m bars).
  - Rolling 50-bar 90th percentile threshold spike detection.
  - Liquidation reversal bias (`BULLISH_REVERSAL`, `BEARISH_REVERSAL`, `NEUTRAL`) and exhaustion score.
- **Persistence**:
  - `Database/Live_Liquidations/<SYMBOL>_tv_liquidations.parquet`
  - `Database/Live_Liquidations/latest_reversal_signals.json`
- **CLI Options**:
  - `--daemon`: Sweeps fast (every 60s).
  - `--once`: Sweeps core assets and exits immediately.

### 3.5 Script 5: `5_wake_on_footprint_signal.py`
- **Continuous Reactive Daemon**:
  - Uses Watchdog OS file watching + 0.5s polling fallback on `Database/Signals/footprint_signals.jsonl`.
  - Zero polling lag, non-terminating daemon.
  - Instantly evaluates every newly harvested footprint signal against DuckDB OLAP (Whales, Liquidations, TV Reversals) + Google DeepMind TimesFM 2.5 zero-shot forecaster.
  - If Confluence Score $\ge 75.0 / 100$ on closed 15m candle:
    - Verifies account health & equity cushion on MatchTrader tab (`52153D87C3EBBC41E0284A2480AB56A1`).
    - Dispatches live market order with protective SL and TP.
    - Sounds audible bell (`winsound.Beep(1200, 400)`).
    - Emits prominent formatted Trade Confluence Card to stdout.
    - Persists record to `Database/Signals/matchtrader_executed_trades.jsonl` and `Database/realtime_trade_alerts.jsonl`.
- **CLI Options**:
  - `--daemon`: Default continuous monitoring mode.
  - `--once`: Evaluates latest unread alerts and exits.

---

## 4. Repository Cleanliness & Directory Layout

Post-cleanup file structure of `C:\Users\SIGMA\Documents\_Trading_1` (and `C:\Users\SIGMA\Documents\Crypto Trading Business`):

```
_Trading_1/
├── 1_coinglass_footprint_scraper.py         # [STANDALONE 1] Footprint Signals & 15m Candles
├── 2_coinglass_whales.py                    # [STANDALONE 2] Whale Orderbook & Walls
├── 3_coinglass_liquidation_levels.py        # [STANDALONE 3] Liquidation Levels & Magnet Zones
├── 4_coinglass_tv_scraper.py                # [STANDALONE 4] TV Liquidation Spikes & Reversals
├── 5_wake_on_footprint_signal.py            # [STANDALONE 5] Reactive Wakeup & Auto-Trader
├── AGENTS.md                                # Repository Agent Invariants & EGM Guidelines
├── .gitignore                               # Git ignore
├── Terminal/                                # Core Engine Modules (Imported by Standalone Files)
│   ├── runtime_io.py                        # Atomic persistence, FileMutex, cdp_result
│   ├── collector_lock.py                    # Cloudflare WARP IP rotation & browser mutex
│   ├── realtime_signal_evaluator.py         # Multi-factor confluence engine (TimesFM + OLAP)
│   ├── matchtrader_live_trader.py           # MatchTrader CDP execution bridge & account risk
│   ├── olap_engine.py                       # DuckDB zero-copy microstructure queries
│   ├── auction_indicators.py                # Day VWAP, VAH/VAL/POC, Fib-Prime EMAs
│   ├── regime_detector.py                   # Market regime & volatility classifier
│   ├── deepmind_timesfm_forecaster.py       # TimesFM 2.5 (200M) neural flow forecaster
│   ├── venue_admission.py                   # Venue qualification & symbol mapping
│   ├── risk_envelope.py                     # Strict prop-firm risk limits & lot grid solver
│   └── vector_memory.py                     # LanceDB historical trade setup analogs
├── Database/                                # Atomic Parquet / JSONL Lake
├── models/                                  # TimesFM 2.5 (200M) local PyTorch weights
├── docs/designs/                            # EGM Design Specifications
└── archive/                                 # Retired / Legacy scripts
    ├── legacy_root/                         # Archived root scripts
    └── legacy_terminal/                     # Archived redundant collectors
```

---

## 5. Verification & Goldfish Audit Checklist

- [x] Directory junction `C:\Users\SIGMA\Documents\Crypto Trading Business` created and verified.
- [ ] All 4 scrapers operate exclusively via CoinGlass in-browser CDP (`127.0.0.1:9222`).
- [ ] Ephemeral tab lifecycle: every scraper closes its tab after every cycle (`/json/close/{tab_id}`).
- [ ] Browser mutex with cooperative time-slicing prevents CDP collisions.
- [ ] Cross-process WARP IP rotation cooldown (`Database/.locks/warp_rotation.lock`) prevents thundering herd drops.
- [ ] Confluence hurdle strictly enforced at $\ge 75.0 / 100$ on completed 15m candle closes.
- [ ] MatchTrader account risk limits strictly enforced (0.5% max risk, max 2 positions, daily floor $95K).
- [ ] `5_wake_on_footprint_signal.py` fires and wakes Antigravity within < 1 second of new signal detection.
