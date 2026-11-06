---
type: reference
created: 2026-11-06
updated: 2026-11-06
---

# Memory Index

## User
- [user] FundedNext MT5 Account 11808165, hard loss floor $4600, risk $10-$15 per trade -> user-preferences.md
- [user] Prioritizes 100% data fidelity directly from CoinGlass CDP, zero simulation -> user-preferences.md

## Project
- [project] Master zero-overlap TDMA 15m candle-aligned scheduler with 17 sweeps/hr -> project-conventions.md
- [project] Thread-local re-entrant writer_lock to prevent Windows msvcrt locking deadlocks -> tech-decisions.md
- [project] Immediate Flatten kill switch on failed SL verification within 3s or >0.4R slippage -> tech-decisions.md
- [project] Positive MT5 symbol whitelist for execution: BTC, ETH, DOGE, ADA, XRP, LTC, LINK -> tech-decisions.md

## Feedback
- [feedback] User noted data not updating properly: traced to double writer_lock timeout -> feedback-history.md
- [feedback] User demands local Cloudflare auto IP rotation without invoking external cloud APIs -> feedback-history.md

## Reference
- [reference] Chrome DevTools Protocol listening on 127.0.0.1:9222 -> project-conventions.md
- [reference] Database storage structured in Database/ with parquet tables and .locks/ -> project-conventions.md
