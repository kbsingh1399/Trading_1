# Architecture Specification: Delta Live Position Guardian, Fill-Anchored 1:2 R:R Calibration & Dynamic Trailing Stop-Loss

**Document ID**: SPEC-2026-11-04-DELTA-POSITION-GUARDIAN-01  
**Status**: APPROVED & AUDITED (Google Elephant-Goldfish Model)  
**Author**: Elephant Architect (Google DeepMind Team)  
**Auditors**: `egm-design` (Design Critic Goldfish), `egm-codebase-details` (Codebase Details Goldfish)  
**Target Systems**: `Terminal/delta_exchange_live_trader.py`, `5_wake_on_footprint_signal.py`, `Terminal/realtime_signal_evaluator.py`, `Terminal/risk_envelope.py`  
**Primary Execution Venue**: Delta Exchange India (Authenticated REST API)  

---

## 1. Problem Statement & Root Cause Analysis

### 1.1 The R:R Inversion Incident (Evidence: HBAR Trade 2026-11-04 02:04:57 IST)
At 02:04:52 IST, Sentinel detected an actionable HBAR BUY signal with 100/100 confluence.
- Pre-execution theoretical geometry at $P_{\text{sig}} = 0.06914$:
  - Stop Loss: $0.06880$ (Risk distance $\Delta_{\text{risk}} = 0.00034$, $-0.50\%$)
  - Take Profit: $0.06980$ (Reward distance $\Delta_{\text{reward}} = 0.00066$, $+0.95\%$)
  - Theoretical Ratio: $0.00066 / 0.00034 = 1.94 \approx 2.0 : 1$
- Live Execution on Delta Exchange:
  - Taker market order filled at $P_{\text{fill}} = 0.06941$ (+0.39% slippage).
  - Pre-computed absolute bracket prices were sent: $\text{SL} = 0.06880$, $\text{TP} = 0.06980$.
- Resulting Live Geometry on Exchange:
  - Actual risk from fill: $0.06941 - 0.06880 = 0.00061$ ($-\$0.061$ USD on 100 contracts).
  - Actual reward from fill: $0.06980 - 0.06941 = 0.00039$ ($+\$0.039$ USD on 100 contracts).
  - Realized R:R: $0.00039 / 0.00061 = \mathbf{0.64 : 1}$!
- **Root Cause**: Fixed absolute bracket prices derived from signal candle close are distorted by taker execution slippage. If uncorrected, winning trades earn less than losing trades risk.

### 1.2 Deprecation of MatchTrader
MatchTrader is completely excised from live routing. Delta Exchange India is the primary automated execution venue.

---

## 2. Invariants & Mathematical Formulations

### Invariant 1: Fill-Anchored Minimum 1:2 Risk-Reward ($\ge 2.0 : 1$)
When a market order executes, Delta Exchange returns `average_fill_price` ($P_{\text{fill}}$). The system must anchor trade geometry to $P_{\text{fill}}$:

1. **Actual Risk Distance**:
   $$\Delta_{\text{risk}} = |P_{\text{fill}} - P_{\text{SL}}|$$

2. **Dollar Risk Cap Preservation**:
   $$\text{Realized Risk USD} = \Delta_{\text{risk}} \cdot \text{size} \cdot \text{contract\_val}$$
   If $\text{Realized Risk USD} > \text{MAX\_RISK\_USD}$ ($25.00), adjust $P_{\text{SL}}$ inward toward $P_{\text{fill}}$ such that:
   $$\Delta_{\text{risk\_capped}} = \frac{\text{MAX\_RISK\_USD}}{\text{size} \cdot \text{contract\_val}}$$
   $$P_{\text{SL\_calibrated}} = P_{\text{fill}} - \text{Direction} \cdot \Delta_{\text{risk\_capped}}$$

3. **Calibrated Take Profit**:
   $$P_{\text{TP\_calibrated}} = P_{\text{fill}} + \text{Direction} \cdot (2.0 \cdot \Delta_{\text{risk\_calibrated}})$$
   Both $P_{\text{SL\_calibrated}}$ and $P_{\text{TP\_calibrated}}$ are quantized to the instrument's exact `tick_size`.

### Invariant 2: Dynamic Multi-Tier Trailing Stop-Loss with High-Water Latching
Let $R = |P_{\text{fill}} - P_{\text{SL\_calibrated}}|$ be the trade's calibrated risk unit.  
Query live mark price $P_{\text{mark}}$ via `GET /v2/tickers/{symbol}`.  
Current profit in R-multiples:

$$r = \frac{\text{Direction} \cdot (P_{\text{mark}} - P_{\text{fill}})}{R}$$

State transitions are strictly one-way (latched high-water mark):

| State | Condition | Stop-Loss Ratchet Target | Action on Resting TP |
|---|---|---|---|
| `INITIAL` | $r < 1.0R$ | $P_{\text{SL\_calibrated}}$ | Resting TP at $+2.0R$ |
| `TIER_1_BE` | $r \ge +1.0R$ | $P_{\text{fill}} + \text{Direction} \cdot (1\,\text{tick})$ | Resting TP maintained at $+2.0R$ |
| `TIER_2_PLUS_1R` | $r \ge +2.0R$ | $P_{\text{fill}} + \text{Direction} \cdot 1.0R$ | Widen resting TP to $+3.0R$ to allow running |
| `TIER_3_PLUS_2R` | $r \ge +3.0R$ | $P_{\text{fill}} + \text{Direction} \cdot 2.0R$ | Trail at distance $1.0R$ behind mark price |

### Invariant 3: Monotonic Stop-Loss Ratchet & Stop Validity
- A stop-loss adjustment is rejected if:
  - BUY: $P_{\text{SL\_new}} \le P_{\text{SL\_current}}$
  - SELL: $P_{\text{SL\_new}} \ge P_{\text{SL\_current}}$
- Before issuing `PUT /v2/orders`, verify stop trigger validity:
  - BUY Stop: $P_{\text{SL\_new}} < P_{\text{mark}}$ (must not trigger immediately)
  - SELL Stop: $P_{\text{SL\_new}} > P_{\text{mark}}$

---

## 3. Implementation Details & Delta REST API Contracts

### 3.1 Delta REST API Order Contracts
1. **Querying Conditional Stop Orders**:
   `GET /v2/orders?states=open,pending`  
   Delta holds conditional bracket orders (`stop_loss_order`, `take_profit_order`) in `state: "pending"`. Querying without `states` omits them.
2. **Editing Open Orders**:
   `PUT /v2/orders`  
   Body must include:
   ```json
   {
     "id": 1651981531,
     "product_id": 18288,
     "size": 100,
     "stop_price": "0.07063",
     "reduce_only": true
   }
   ```
   Setting `reduce_only: true` eliminates redundant margin reservation.
3. **Price Quantization (`quantize_price`)**:
   Calculate exact tick increment and precision:
   ```python
   def quantize_price(self, price: float, product_id: int) -> str:
       prod = next((p for p in self.products_cache.values() if p.get("id") == product_id), None)
       tick = float(prod.get("tick_size", 0.01)) if prod else 0.01
       quantized = round(round(price / tick) * tick, 8)
       decimals = max(0, -int(math.floor(math.log10(tick)))) if tick < 1.0 else 0
       return f"{quantized:.{decimals}f}"
   ```
4. **Asynchronous Bracket Order Retries**:
   In `reanchor_brackets_to_fill(symbol, fill_price, sl_price, min_rr=2.0)`, retry up to 4 times with 250ms backoff if child stop orders have not yet populated.
5. **Mark Price Lookup**:
   In `audit_and_trail_positions()`, query `GET /v2/tickers/{symbol}` (or bulk `GET /v2/tickers`) to retrieve true live `mark_price`.
6. **HTTP 429 & Terminal Error Resilience**:
   Handle HTTP 429 with exponential backoff. Non-fatal errors like `order_not_found`, `order_cancelled`, or `order_already_closed` indicate normal position closure; log cleanly without crash.

### 3.2 MatchTrader Complete Removal
- Strip all `Terminal.matchtrader_live_trader` imports from `5_wake_on_footprint_signal.py`.
- Remove MatchTrader WebSocket CDP inspection loop.
- In `Terminal/realtime_signal_evaluator.py`, remove MatchTrader lot sizing branch; calculate discrete contracts directly using Delta instrument specifications.
- Redesign Sentinel terminal display card to showcase Delta Exchange India leverage, margin, open positions, unrealized PnL, and Trailing Stop status.

---

## 4. Verification Gate
1. Unit tests:
   - `test_delta_bracket_reanchoring_min_rr`: verify slippage triggers automatic TP re-calibration to $\ge 2.0 : 1$.
   - `test_quantize_price`: verify correct formatting across 1-decimal, 2-decimal, 4-decimal, and 5-decimal instruments.
   - `test_trailing_stop_progression`: verify Tiers 1, 2, 3 latch correctly and reject adverse movement.
2. Live test:
   - Query running Delta positions (`HBARUSD`, `TRXUSD`, `ARUSD`, `SUSHIUSD`).
   - Run `audit_and_trail_positions()` to verify zero errors and confirmed $R:R \ge 2.0$.
