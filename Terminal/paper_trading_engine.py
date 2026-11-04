"""
========================================================================================
[TERMINAL] Local Live-Tape Shadow Paper Trading Engine (Delta Exchange India)
========================================================================================
Provides realistic, zero-capital paper trading using live Delta Exchange India order books,
real-time mark prices, spread-aware tick slippage, fee deductions, discrete contract grid sizing,
and dynamic 1:2+ R:R trailing stop management.

Key Invariants:
1. Max 2 concurrent open positions across all instruments (MAX_OPEN_POSITIONS = 2).
2. Max $25.00 risk per trade calculated via solve_fee_aware_grid (including round-trip fees).
3. Dynamic fill-anchored bracket: R:R >= 2.0:1 guaranteed from actual live fill price.
4. Monotonic Dynamic Trailing Stop Loss:
   - Tier 1 (+1.0R0): Move SL to Breakeven (+1 tick buffer).
   - Tier 2 (+2.0R0): Move SL to lock +1.0R0 profit, widen TP to +3.0R0.
   - Tier 3 (+3.0R0): Move SL to lock +2.0R0 profit.
5. Persistent JSON / JSONL state: Database/Paper_Trading/
========================================================================================
"""

import os
import sys
import json
import time
import math
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import writer_lock, atomic_json
from Terminal.risk_envelope import MAX_OPEN_POSITIONS, MAX_RISK_USD

logger = logging.getLogger("PaperTrader")
logger.setLevel(logging.INFO)

DB_DIR = ROOT_DIR / "Database"
PAPER_DIR = DB_DIR / "Paper_Trading"
PAPER_DIR.mkdir(parents=True, exist_ok=True)

ACTIVE_POSITIONS_FILE = PAPER_DIR / "paper_trades_active.json"
LEDGER_FILE = PAPER_DIR / "paper_trades_ledger.jsonl"
TAKER_FEE_RATE = 0.00075  # 0.075% taker commission on Delta India
USD_INR_RATE = 86.50      # Approximate conversion for reporting


def utc_to_ist_str(utc_dt: Optional[datetime] = None) -> str:
    if utc_dt is None:
        utc_dt = datetime.now(timezone.utc)
    ts = utc_dt.timestamp() + 19800  # +5h30m
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S IST")


class PaperTradingEngine:
    def __init__(self):
        self.active_positions: Dict[str, Dict[str, Any]] = self._load_active()

    def _load_active(self) -> Dict[str, Dict[str, Any]]:
        if ACTIVE_POSITIONS_FILE.exists():
            try:
                with open(ACTIVE_POSITIONS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading active paper positions: {e}")
        return {}

    def _save_active(self):
        atomic_json(ACTIVE_POSITIONS_FILE, self.active_positions)

    def _append_ledger(self, record: Dict[str, Any]):
        try:
            line = json.dumps(record, default=str) + "\n"
            with open(LEDGER_FILE, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            logger.error(f"Error appending to paper trades ledger: {e}")

    def get_open_positions_count(self) -> int:
        return len(self.active_positions)

    def has_open_position(self, symbol: str) -> bool:
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "")
        for p_sym in self.active_positions:
            if clean in p_sym.upper():
                return True
        return False

    def execute_paper_order(self, eval_res: Dict[str, Any], delta_client) -> Dict[str, Any]:
        """
        Executes a simulated paper order using live market data from Delta Exchange India.
        """
        sym = eval_res.get("symbol", "")
        direction = str(eval_res.get("direction", "")).upper()
        if direction not in ("BUY", "SELL"):
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_DIRECTION: {direction}"}

        # 1. Enforce max concurrent positions
        if self.get_open_positions_count() >= MAX_OPEN_POSITIONS:
            logger.warning(f"[{sym}] Paper trade rejected: Max concurrent positions ({MAX_OPEN_POSITIONS}) reached.")
            return {"ok": False, "status": "REJECTED", "reason": f"MAX_OPEN_POSITIONS_REACHED ({MAX_OPEN_POSITIONS})"}

        # 2. Check if already open
        if self.has_open_position(sym):
            logger.warning(f"[{sym}] Paper trade rejected: Position already open.")
            return {"ok": False, "status": "REJECTED", "reason": f"POSITION_ALREADY_OPEN: {sym}"}

        # 3. Resolve Delta India Product
        prod = delta_client.resolve_product(sym)
        if not prod:
            return {"ok": False, "status": "REJECTED", "reason": f"UNLISTED_ON_DELTA_INDIA: {sym}"}

        prod_symbol = prod["symbol"]
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        tick_size = float(prod.get("tick_size", 0.0001) or 0.0001)

        # 4. Fetch live ticker to get exact market fill price and spread
        ticker_res = delta_client.request("GET", f"/v2/tickers/{prod_symbol}")
        ticker = ticker_res.get("result", {}) if ticker_res else {}
        mark_price = float(ticker.get("mark_price") or eval_res.get("signal", {}).get("price") or 0.0)
        quotes = ticker.get("quotes", {})
        best_bid = float(quotes.get("best_bid") or mark_price)
        best_ask = float(quotes.get("best_ask") or mark_price)

        if mark_price <= 0:
            return {"ok": False, "status": "REJECTED", "reason": "INVALID_MARK_PRICE"}

        # Check spread gate (<= 45 bps)
        spread_bps = ((best_ask - best_bid) / mark_price * 10000.0) if mark_price > 0 else 0.0
        if spread_bps > 45.0:
            logger.warning(f"[{prod_symbol}] Paper trade rejected: Spread {spread_bps:.1f} bps > 45.0 bps gate.")
            return {"ok": False, "status": "REJECTED", "reason": f"SPREAD_GATE_EXCEEDED: {spread_bps:.1f} bps"}

        # 5. Realistic Simulated Fill Price with Tick Slippage
        # Market BUY hits Ask (+1 tick slippage), Market SELL hits Bid (-1 tick slippage)
        if direction == "BUY":
            fill_price = best_ask + tick_size
        else:
            fill_price = best_bid - tick_size

        # Quantize fill to tick grid
        fill_price = round(round(fill_price / tick_size) * tick_size, 8)

        # 6. Sizing via Fee-Aware Discrete Grid Solver
        planned_levels = eval_res.get("trade_levels", {})
        planned_sl = float(planned_levels.get("stop_loss") or fill_price * (0.99 if direction == "BUY" else 1.01))
        initial_sl_dist = abs(fill_price - planned_sl)

        from Terminal.delta_exchange_live_trader import solve_fee_aware_grid
        grid_res = solve_fee_aware_grid(
            entry=fill_price,
            stop_loss=planned_sl,
            contract_value=contract_val,
            taker_fee_rate=TAKER_FEE_RATE,
            cap_usd=MAX_RISK_USD
        )
        contracts = grid_res.get("contracts", 1)
        if contracts <= 0:
            contracts = 1

        # 7. Dynamic Fill-Anchored Brackets (R:R >= 2.0 guaranteed)
        r0 = initial_sl_dist
        if r0 <= 0:
            r0 = fill_price * 0.005  # 0.5% default fallback

        if direction == "BUY":
            sl_price = fill_price - r0
            # Target at least 2.0R0
            planned_tp = float(planned_levels.get("take_profit_1") or (fill_price + 2.0 * r0))
            tp_dist = max(planned_tp - fill_price, 2.0 * r0)
            tp_price = fill_price + tp_dist
        else:
            sl_price = fill_price + r0
            planned_tp = float(planned_levels.get("take_profit_1") or (fill_price - 2.0 * r0))
            tp_dist = max(fill_price - planned_tp, 2.0 * r0)
            tp_price = fill_price - tp_dist

        # Quantize SL and TP
        sl_price = round(round(sl_price / tick_size) * tick_size, 8)
        tp_price = round(round(tp_price / tick_size) * tick_size, 8)
        rr_actual = tp_dist / r0 if r0 > 0 else 2.0

        notional_usd = contracts * contract_val * fill_price
        entry_fee_usd = notional_usd * TAKER_FEE_RATE
        trade_id = f"PAPER_{prod_symbol}_{int(time.time())}"

        pos_record = {
            "trade_id": trade_id,
            "product_symbol": prod_symbol,
            "symbol": sym,
            "direction": direction,
            "contracts": contracts,
            "contract_value": contract_val,
            "tick_size": tick_size,
            "fill_price": fill_price,
            "mark_price": mark_price,
            "stop_loss": sl_price,
            "take_profit": tp_price,
            "initial_r0": r0,
            "initial_sl": sl_price,
            "rr_ratio": round(rr_actual, 2),
            "notional_usd": round(notional_usd, 2),
            "entry_fee_usd": round(entry_fee_usd, 4),
            "tier_reached": 0,
            "confluence_score": eval_res.get("confluence_score", 0.0),
            "confluence_reasons": eval_res.get("confluence_reasons", []),
            "opened_at_utc": datetime.now(timezone.utc).isoformat(),
            "opened_at_ist": utc_to_ist_str(),
            "status": "OPEN"
        }

        self.active_positions[prod_symbol] = pos_record
        self._save_active()
        self._append_ledger({**pos_record, "event": "ORDER_OPENED"})

        # Visual Paper Trade Execution Card
        print("\n" + "╔" + "═" * 78 + "╗")
        print(f"║ 📝 [PAPER TRADE EXECUTED] {prod_symbol} {direction} ({contracts} contracts) ║".center(80))
        print("╠" + "═" * 78 + "╣")
        print(f"║ Fill Price:    ${fill_price:,.5f} (Mark: ${mark_price:,.5f} | Slippage: {spread_bps:.1f} bps)".ljust(79) + "║")
        print(f"║ Stop Loss:     ${sl_price:,.5f} (Risk Distance R0: ${r0:,.5f})".ljust(79) + "║")
        print(f"║ Take Profit:   ${tp_price:,.5f} (Target R:R: {rr_actual:.2f}:1)".ljust(79) + "║")
        print(f"║ Position Size: {contracts} contracts (${notional_usd:,.2f} Notional | Fee: ${entry_fee_usd:.3f})".ljust(79) + "║")
        print(f"║ Confluence:    {eval_res.get('confluence_score')}/100 | Time: {pos_record['opened_at_ist']}".ljust(79) + "║")
        print("╚" + "═" * 78 + "╝\n")

        return {"ok": True, "status": "PAPER_EXECUTED", "trade": pos_record}

    def audit_and_trail_positions(self, delta_client) -> List[Dict[str, Any]]:
        """
        Monitors active paper positions against live mark prices from Delta India.
        Enforces 3-tier trailing stop ratchet and checks for SL/TP hits.
        """
        if not self.active_positions:
            return []

        reports = []
        closed_symbols = []

        for prod_sym, pos in list(self.active_positions.items()):
            ticker_res = delta_client.request("GET", f"/v2/tickers/{prod_sym}")
            ticker = ticker_res.get("result", {}) if ticker_res else {}
            mark_price = float(ticker.get("mark_price") or 0.0)
            if mark_price <= 0:
                continue

            pos["mark_price"] = mark_price
            direction = pos["direction"]
            fill_price = pos["fill_price"]
            r0 = pos["initial_r0"]
            tick_size = pos["tick_size"]
            sl = pos["stop_loss"]
            tp = pos["take_profit"]
            contracts = pos["contracts"]
            contract_val = pos["contract_value"]

            # Calculate unrealized profit and R multiple
            if direction == "BUY":
                profit_dist = mark_price - fill_price
                sl_hit = mark_price <= sl
                tp_hit = mark_price >= tp
            else:
                profit_dist = fill_price - mark_price
                sl_hit = mark_price >= sl
                tp_hit = mark_price <= tp

            r_mult = profit_dist / r0 if r0 > 0 else 0.0
            pos["unrealized_r"] = round(r_mult, 2)
            pos["unrealized_pnl_usd"] = round(profit_dist * contracts * contract_val, 2)

            # Check SL / TP Exit
            if sl_hit or tp_hit:
                exit_price = sl if sl_hit else tp
                exit_reason = "STOP_LOSS_HIT" if sl_hit else "TAKE_PROFIT_HIT"
                
                if direction == "BUY":
                    realized_gross = (exit_price - fill_price) * contracts * contract_val
                else:
                    realized_gross = (fill_price - exit_price) * contracts * contract_val

                exit_notional = contracts * contract_val * exit_price
                exit_fee = exit_notional * TAKER_FEE_RATE
                net_pnl_usd = realized_gross - pos["entry_fee_usd"] - exit_fee
                net_pnl_inr = net_pnl_usd * USD_INR_RATE

                close_record = {
                    **pos,
                    "event": "TRADE_CLOSED",
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "realized_gross_usd": round(realized_gross, 2),
                    "exit_fee_usd": round(exit_fee, 4),
                    "net_pnl_usd": round(net_pnl_usd, 2),
                    "net_pnl_inr": round(net_pnl_inr, 2),
                    "closed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "closed_at_ist": utc_to_ist_str(),
                    "status": "CLOSED"
                }

                self._append_ledger(close_record)
                closed_symbols.append(prod_sym)

                result_banner = "🎯 PROFIT HIT (+)" if tp_hit else "🛑 STOPPED OUT (-)"
                print("\n" + "╔" + "═" * 78 + "╗")
                print(f"║ {result_banner} [PAPER TRADE CLOSED] {prod_sym} {direction} ║".center(80))
                print("╠" + "═" * 78 + "╣")
                print(f"║ Exit Price:   ${exit_price:,.5f} ({exit_reason})".ljust(79) + "║")
                print(f"║ Net PnL USD:  ${net_pnl_usd:+,.2f} USD (Net INR: ₹{net_pnl_inr:+,.2f})".ljust(79) + "║")
                print(f"║ Return:       {r_mult:+.2f} R0 | Held from: {pos.get('opened_at_ist', 'N/A')}".ljust(79) + "║")
                print("╚" + "═" * 78 + "╝\n")
                reports.append(close_record)
                continue

            # Trailing Stop Ratchet Logic
            old_sl = sl
            old_tp = tp
            tier = pos.get("tier_reached", 0)

            # Tier 3: +3.0R0 reached -> lock +2.0R0
            if r_mult >= 3.0 and tier < 3:
                new_sl = fill_price + (2.0 * r0 if direction == "BUY" else -2.0 * r0)
                pos["stop_loss"] = round(round(new_sl / tick_size) * tick_size, 8)
                pos["tier_reached"] = 3
                logger.info(f"🚀 [PAPER TRAILING TIER 3] {prod_sym} reached +{r_mult:.2f}R! SL locked at +2.0R: ${pos['stop_loss']}")

            # Tier 2: +2.0R0 reached -> lock +1.0R0, widen TP to +3.0R0
            elif r_mult >= 2.0 and tier < 2:
                new_sl = fill_price + (1.0 * r0 if direction == "BUY" else -1.0 * r0)
                new_tp = fill_price + (3.0 * r0 if direction == "BUY" else -3.0 * r0)
                pos["stop_loss"] = round(round(new_sl / tick_size) * tick_size, 8)
                pos["take_profit"] = round(round(new_tp / tick_size) * tick_size, 8)
                pos["tier_reached"] = 2
                logger.info(f"✨ [PAPER TRAILING TIER 2] {prod_sym} reached +{r_mult:.2f}R! SL locked at +1.0R (${pos['stop_loss']}), TP widened to +3.0R (${pos['take_profit']})")

            # Tier 1: +1.0R0 reached -> move SL to Breakeven (+1 tick buffer)
            elif r_mult >= 1.0 and tier < 1:
                be_sl = fill_price + (1 * tick_size if direction == "BUY" else -1 * tick_size)
                pos["stop_loss"] = round(round(be_sl / tick_size) * tick_size, 8)
                pos["tier_reached"] = 1
                logger.info(f"🛡️ [PAPER TRAILING TIER 1] {prod_sym} reached +{r_mult:.2f}R! SL moved to Breakeven (+1 tick): ${pos['stop_loss']}")

        for sym in closed_symbols:
            del self.active_positions[sym]

        if closed_symbols or any(pos.get("tier_reached", 0) > 0 for pos in self.active_positions.values()):
            self._save_active()

        return reports


# Singleton instance
paper_engine = PaperTradingEngine()
