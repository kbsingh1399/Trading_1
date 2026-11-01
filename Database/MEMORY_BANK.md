# Agent Persistent Memory Bank
*Synchronized: 2026-11-01 13:32:45 UTC*

## 1. Non-Negotiable Operational Rules & Guardrails

### 🛡️ Broker Spread Gatekeeper
Reject any crypto trade where the broker ask-bid spread exceeds 0.45% of price. Assets like AXS (24% spread) and SUSHI (11% spread) cause massive immediate slippage loss and are strictly blacklisted.
- **MAX_SPREAD_PCT**: `0.45`
- **PREFERRED_TIGHT_PAIRS**: `['BTCUSD.pi', 'ETHUSD.pi', 'SOLUSD.p', 'XRPUSD.pi', 'AVXUSD.p']`

### 🛡️ Emergency Circuit Breaker
The live trader daemon must continuously check open positions every 10 seconds. If any position floating loss touches -$48.00 USD, an emergency market close order is sent immediately.
- **CIRCUIT_BREAKER_LOSS_USD**: `-48.0`

### 🛡️ Hard Max Dollar Loss per Trade
Under NO circumstances may any single trade risk more than $50.00 USD. If calculated risk exceeds $50.00, trade lot sizing MUST be clamped downward before order submission.
- **HARD_MAX_LOSS_USD**: `50.0`

### 🛡️ Prop Firm Floor Guardrail (Dual Capital Budget)
On prop firm challenge accounts (e.g. Blueberry Funded 6% max drawdown), position dollar risk must never exceed 50% of the remaining buffer above the liquidation floor: risk_usd = min($50.00, max(0, (current_equity - floor_equity) * 0.50)). If buffer <= $10.00, trading must immediately halt.
- **MAX_OVERALL_DD_PCT**: `6.0`
- **BUFFER_SAFETY_MULTIPLIER**: `0.5`
- **MIN_HALT_BUFFER_USD**: `10.0`

## 2. Institutional Lessons & Forensic Traps

### ⚠️ Lesson #1: Prop Firm Floor vs Trade Sizing Collision
On 2026-11-01, a trade on SOL was submitted with calculated risk $46.19 (compliant with the $50 cap). However, the account balance was $4,714.57 against the $4,700.00 Blueberry Funded 6% floor. The intraday fluctuation touched the $4,700 floor, triggering broker auto-liquidation. Sizing must always factor in remaining floor margin, not just starting account balance.

### ⚠️ Lesson #2: Native Direct Page CDP over Browser CDP
Playwright's default browser CDP connection hangs when Chrome/Edge has 20+ tabs open due to browser target contention with MCP servers. Using direct Page WebSocket endpoints ('ws://127.0.0.1:9222/devtools/page/<id>') achieves 10-millisecond execution with zero contention.

## 3. Dynamic Strategy Weights & Edge Scores

| Strategy | Weight | Trades | Win Rate | Net PnL ($) |
| :--- | :---: | :---: | :---: | :---: |
| **Whale Bid Wall Support Fade** | `1.10` | 1 | 100.0% | $1.00 |
| **Order-flow Absorption Reversal** | `0.85` | 1 | 0.0% | $-14.06 |

## 4. Trade Reflection History (Last 20)

- **Ticket #19549297** (SOLUSD.p BUY) -> `LOSS` ($-14.06)
  - *Strategy*: Order-flow Absorption Reversal
  - *Root Cause*: Account lacked sufficient buffer ($14.57) above the prop firm $4,700.00 drawdown floor.
  - *Lesson*: Never open a trade whose dollar risk exceeds remaining account floor margin.
- **Ticket #19548368** (XRPUSD.pi BUY) -> `WIN` ($1.00)
  - *Strategy*: Whale Bid Wall Support Fade
  - *Root Cause*: Clean entry above $2.05M whale bid wall; held comfortably through volatility.
  - *Lesson*: Whale bid order book walls provide solid structural defense when combined with low spread.
