# Repository AI Agent Guidelines & Architecture Policies

EGM artifact directory: docs/designs

## 1. Spec-Driven Development (EGM: Elephant-Goldfish Model)
This repository follows Google's **Elephant-Goldfish Model (EGM)** for architectural design and complex feature additions:
- **The Elephant (Deep Context)**: Operates with complete repo memory, system invariants, historical context, and user constraints. Drafts comprehensive design specs in `docs/designs/` before code implementation.
- **The Goldfish (Zero Context)**: Evaluates design specifications via independent subagents without conversation history. Focuses on finding unstated assumptions, YAGNI violations, race conditions, and implementation blind spots.
- **Human Invariant**: The human architect resolves design trade-offs and gives final implementation approval.

## 2. Quantitative Trading Pipeline Standards
- **Unified API Engine**: All data collection must flow through direct, high-speed, free APIs (`Terminal/unified_api_footprint_engine.py`) using Binance Futures REST API for Crypto and local MetaTrader 5 API for Forex, Metals, Commodities, and Indices. No web browser scraping or browser DOM dependencies for execution.
- **Rolling Windows**: Maintain exact 300-bar M15 rolling windows in `Database/Candle_Data/<SYMBOL>.parquet`.
- **Primary IST Time Mapping**: All parquet files must store Indian Standard Time (IST, UTC+5:30) as primary columns: `open_time_ist`, `close_time_ist`, `time_ist`, `candle_ist`, `open_time_ist_ms`, `close_time_ist_ms`.
- **Lock-Free Atomic Persistence**: Write to temporary `.parquet.tmp` files and atomic-rename via `os.replace`. Never create orphaned `.lock` files inside `Database/Candle_Data`.

## 3. Strict Live Execution & Risk Guardrails (Delta Exchange India Exclusive)
- **Sole Execution Target**: Delta Exchange India authenticated REST API (`Terminal/delta_exchange_live_trader.py`). MatchTrader, browser DOM automation, and all legacy prop-firm execution bridges are strictly decommissioned, excised, and prohibited from execution paths.
- **Capital & Margin Preservation**: Available free margin buffer >= 20% universal floor to cover taker exchange commissions and eliminate `insufficient_commission` rejections. Fail-closed if balance unavailable.
- **Position Limits**: Max 2 concurrent open positions across all instruments (`MAX_OPEN_POSITIONS = 2`).
- **Risk Sizing**: Max $25.00 (`MAX_RISK_USD`) risk per trade based on stop loss distance and discrete contract grid solver.
- **Minimum 1:2 Risk-Reward Invariant**: All executed trades must strictly enforce at least 1:2 Risk:Reward ($R:R \ge 2.0$) calibrated against the *actual market fill price* ($P_{\text{fill}}$), dynamically re-anchoring bracket TP/SL upon fill with directional tick quantization.
- **Dynamic Trailing Stop Loss**:
  - **Tier 1 (+1.0R0)**: Move SL to Breakeven (+1 tick buffer) once mark price reaches $+1.0R0$ profit.
  - **Tier 2 (+2.0R0)**: Move SL to lock $+1.0R0$ profit and widen TP to $+3.0R0$.
  - **Tier 3 (+3.0R0)**: Move SL to lock $+2.0R0$ profit.
- **Execution Threshold**: Automated live order execution requires quantitative confluence score >= 75.0/100.
- **Bar Completion Invariant**: Orders must execute only on fully completed, closed 15m time candles (`t_ms % 900_000 == 0`, `bar_type == 'time'`). Volume/Range bar signals and intra-bar repainting wicks are strictly rejected.
- **Signal Ledger & De-duplication**: All candidate signals are recorded in `EvaluatedSignalsLedger` (`Database/Signals/evaluated_signals_ledger.jsonl`). Previously reviewed signals for an asset are suppressed to eliminate duplicate evaluations.
- **Cross-Process Position Guardian Mutex**: Position audit, duplicate order cleanup, and trailing stops are synchronized across background workers using `delta_position_management.lock`.

## 4. Autonomous Intelligence, Skills & MCP Synthesis Policy
- **Mandatory Pre-Response Skill & MCP Review**: Before generating any architectural, algorithmic, debugging, or execution responses, the AI agent MUST systematically scan and evaluate the complete catalog of available skills (`/skill`), Model Context Protocol (MCP) servers, and slash command capabilities.
- **Full Capability Matrix Utilization**: Proactively select and utilize ALL relevant skills and MCP tools across system domains (e.g., `agent-memory`, `systematic-debugging`, `quant-analyst`, `backtesting-frameworks`, `database-optimizer`, `performance-engineer`, `test-driven-development`, `code-review-graph`, `delta-exchange-mcp`, `sqlite`, `chrome-devtools`) rather than offering isolated, shallow, or improvised suggestions.
- **Persistent Knowledge & State Synchronization**: Maintain and update the agent memory graph (`memory` MCP server & `agent-memory` pattern) across sessions with verified architectural invariants, execution guardrails, and audit benchmarks.
- **Deep Cognitive Reasoning**: Mirror the comprehensive depth of the complete slash-command suite (`/plan`, `/boost`, `/teamwork`, `/goal`, `/agent-memory`, `/learn`) to guarantee verified, production-grade output.
