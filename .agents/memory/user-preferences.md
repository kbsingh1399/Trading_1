---
type: user
created: 2026-11-06
updated: 2026-11-06
---

# User Preferences

## Account & Risk Constraints
- FundedNext MT5 Account: Login `11808165`
- Hard Loss Equity Floor: Strictly `$4,600.00`
- Target Risk per Trade: Strictly `$10.00 – $15.00`
- Cushion Kelly Cap Dominance: Never risk more than 25% of current cushion ($C_t = \text{Equity} - \$4600$)
- Immediate Flatten: If Stop Loss fails verification within 3.0 seconds post-fill, immediately market-close position

## Core Priorities
- Trading decisions MUST be purely based on empirical order flow and market microstructure data.
- Data captured from CoinGlass CDP must be 100% genuine, captured as-is, without flawed simulations or dropped frames.
- Cloudflare IP auto-rotation must be handled completely locally (e.g. via local Cloudflare WARP CLI) without third-party paid proxies or external cloud invocations.
