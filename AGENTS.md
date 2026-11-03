# Repository AI Agent Guidelines & Architecture Policies

EGM artifact directory: docs/designs

## 1. Spec-Driven Development (EGM: Elephant-Goldfish Model)
This repository follows Google's **Elephant-Goldfish Model (EGM)** for architectural design and complex feature additions:
- **The Elephant (Deep Context)**: Operates with complete repo memory, system invariants, historical context, and user constraints. Drafts comprehensive design specs in `docs/designs/` before code implementation.
- **The Goldfish (Zero Context)**: Evaluates design specifications via independent subagents without conversation history. Focuses on finding unstated assumptions, YAGNI violations, race conditions, and implementation blind spots.
- **Human Invariant**: The human architect resolves design trade-offs and gives final implementation approval.

## 2. Quantitative Trading Pipeline Standards
- **Unified API Engine**: All data collection must flow through direct, high-speed, free APIs (`Terminal/unified_api_footprint_engine.py`) using Binance Futures REST API for Crypto and local MetaTrader 5 API for Forex, Metals, Commodities, and Indices. No web browser scraping or browser DOM dependencies.
- **Rolling Windows**: Maintain exact 300-bar M15 rolling windows in `Database/Candle_Data/<SYMBOL>.parquet`.
- **Primary IST Time Mapping**: All parquet files must store Indian Standard Time (IST, UTC+5:30) as primary columns: `open_time_ist`, `close_time_ist`, `time_ist`, `candle_ist`, `open_time_ist_ms`, `close_time_ist_ms`.
- **Lock-Free Atomic Persistence**: Write to temporary `.parquet.tmp` files and atomic-rename via `os.replace`. Never create orphaned `.lock` files inside `Database/Candle_Data`.

## 3. Strict Prop Firm Risk Guardrails
- **Target Account**: FundingPips MatchTrader Competition Account `101305360` ($5,000 challenge account).
- **Hard Drawdown Floor**: $4,775.00 (`HARD_FLOOR_USD` in `Terminal/risk_envelope.py`), ensuring account survival across 10-consecutive loss episodes.
- **Position Limits**: Max 2 concurrent open positions across all instruments (`MAX_OPEN_POSITIONS`).
- **Risk Sizing**: Max $25.00 (`MAX_RISK_USD`) risk per trade based on stop loss distance and discrete lot grid solver.
- **Execution Threshold**: Automated live order execution requires quantitative confluence score >= 75.0/100.
- **Bar Completion Invariant**: Orders must execute only on fully completed, closed 15m candles to prevent entering on repainting intra-bar wicks.
