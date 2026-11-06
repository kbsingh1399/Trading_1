---
type: tech-decisions
created: 2026-11-06
updated: 2026-11-06
---

# Technical Decisions

## 1. Friction-Adjusted Risk-Reward Ratio ($R:R_{\text{eff}} \ge 2.0$)
Trade setups require effective friction-adjusted R:R $\ge 2.0$ incorporating exchange fees ($0.06\%$), execution slippage ($0.05\%$), and MT5 live spread.

## 2. Cap-Dominant Cushion Kelly Sizing
Risk dollar allocation is bounded:
$$r_t = \min(f^* \cdot C_t, 0.25 C_t, \$15.00)$$
Stand-down cushion threshold is $C^* = \$16.48$. If $0.01 \text{ lot} \times \text{SL distance} > r_t$, execution is vetoed.

## 3. Immediate Flatten Kill-Switch
Implemented in `mt5_gui_puncher.py`: if `verify_stop_loss_attachment()` fails 3 attempts within 3.0s or fill price slips $>0.4R$, triggers `emergency_market_close()`.

## 4. Re-Entrant Lock Manager
`coinglass_common.py` `writer_lock()` uses `threading.local()` recursion tracking so functions like `upsert_parquet` calling `atomic_parquet` never self-deadlock on Windows.
