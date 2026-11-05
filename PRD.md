# PRD: Autonomous Quantitative Microstructure & Signal Pipeline Engine

## Executive Summary
This document serves as the single source of truth for the autonomous quantitative trading pipeline, order flow microstructures, signal ledgers, and multi-agent execution council within Antigravity.

## System Invariants
1. **Append-Only Signal Ledgers**: Signals across all 31 assets are appended immutably to `Database/Signals/Signal_Queues/<SYM>_signals.jsonl`.
2. **15m Candle Close Synchronization**: Pipeline evaluation fires deterministically at `:00`, `:15`, `:30`, `:45` aligned with exchange 15m candle closes.
3. **Stream Tearing & Crash Resilience**: All writes enforce trailing-newline guards, fsync, and cross-process file locks.
4. **Sub-Cent Tick Precision**: Micro-assets (PEPE, SHIB) enforce dynamic precision (up to 8 decimals) with non-zero stop loss distance floors.

---

## Tasks & Execution Roadmap

- [x] Task 1: Migrate rolling 5-signal queue to append-only JSONL ledgers across all 31 assets.
- [x] Task 2: Configure 15m candle close cron wakeup loop (`0,15,30,45 * * * *`).
- [x] Task 3: Build adversarial QA stress test suite with 100% passing rate (`tests/test_adversarial_qa.py`).
- [x] Task 4: Install and integrate Get-Shit-Done (GSD) globally and locally for Antigravity.
- [x] Task 5: Install and integrate CodeRabbit AI skills (`code-review`, `autofix`) for Antigravity and Gemini.
- [x] Task 6: Install and integrate Roo-Code multi-mode engine (`.roomodes`, `.roo/`, `evals-context`, `roo-conflict-resolution`).
- [x] Task 7: Install and integrate Ralph Loop autonomous execution controller (`ralph_loop.py`, `PRD.md`, `progress.txt`).
- [x] Task 8: Integrate Agent 1's Non-Linear Regime-Dependent Confluence Scoring (`RDNL-CS`) and dynamic VWAP/POC trailing stops into `trade_evaluator.py`.
- [x] Task 9: Connect `Database/database_compactor.py` into `run_pipeline.py` maintenance loop.
- [x] Task 10: Verify 100% test pass rate and maintain strict git replica across both GitHub remotes (`origin`, `trading1`).
