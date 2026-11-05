"""
================================================================================
MT5 Quantitative Order Puncher & Risk Execution Engine
================================================================================
Author: Antigravity Multi-Pillar Quant Execution Team
License: Proprietary / FundedNext EVA P1

Features:
- Dynamic risk-calibrated position sizing ($10 to $15 risk per trade).
- Microstructure-calibrated stop loss and take profit anchoring.
- Pre-trade validation with mt5.order_check() before deal dispatch.
- In-flight position idempotency (never punches duplicate trades for active symbols).
- Multi-tier symbol mapping (e.g. DOGE -> DOGUSD, BTC -> BTCUSD, ETH -> ETHUSD).
================================================================================
"""

import sys
import os
import json
import math
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

import MetaTrader5 as mt5

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [MT5_PUNCHER] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MT5Puncher")

SYMBOL_MAP = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "DOGE": "DOGUSD",
    "SOL": "SOLUSD", # Fallback if added
    "XRP": "XRPUSD",
    "ADA": "ADAUSD",
    "LTC": "LTCUSD",
    "LINK": "LNKUSD"
}

EXECUTED_LEDGER_PATH = os.path.join(
    os.path.dirname(__file__), "Database", "Signals", "Trade_Setups", "executed_trades.jsonl"
)
MASTER_SETUPS_PATH = os.path.join(
    os.path.dirname(__file__), "Database", "Signals", "Trade_Setups", "master_active_setups.jsonl"
)


class MT5OrderPuncher:
    def __init__(self, target_risk_usd: float = 12.50, min_score: float = 75.0):
        self.target_risk_usd = max(10.0, min(15.0, target_risk_usd))
        self.min_score = min_score
        self.initialized = False

    def connect(self) -> bool:
        if not mt5.initialize():
            logger.error(f"Failed to initialize MetaTrader 5: {mt5.last_error()}")
            return False
        
        self.initialized = True
        term = mt5.terminal_info()
        acc = mt5.account_info()
        
        logger.info(f"Connected to MT5 Terminal build {term.build} ({term.company})")
        logger.info(f"Account: {acc.login} ({acc.name}) | Server: {acc.server} | Balance: ${acc.balance:.2f} | Equity: ${acc.equity:.2f}")
        logger.info(f"Terminal Trade Allowed: {term.trade_allowed} | Account Trade Allowed: {acc.trade_allowed}")
        
        if not term.trade_allowed:
            logger.warning("⚠️ ALGO TRADING IS CURRENTLY DISABLED in MetaTrader 5!")
            logger.warning("👉 To enable: Click the 'Algo Trading' button in the MT5 top toolbar or press Ctrl + E.")
            
        return True

    def disconnect(self):
        if self.initialized:
            mt5.shutdown()
            self.initialized = False

    def get_open_positions(self) -> Dict[str, Any]:
        """Returns map of mt5_symbol -> position dict."""
        positions = mt5.positions_get()
        pos_map = {}
        if positions:
            for p in positions:
                pos_map[p.symbol] = p
        return pos_map

    def calculate_lot_size(self, symbol: str, entry: float, sl: float) -> Optional[float]:
        """Calculates volume (lot size) to strictly enforce $10 to $15 risk."""
        info = mt5.symbol_info(symbol)
        if not info:
            logger.error(f"Cannot find symbol info for {symbol}")
            return None

        distance = abs(entry - sl)
        if distance <= 1e-8:
            logger.error(f"Invalid SL distance for {symbol}: entry={entry}, sl={sl}")
            return None

        contract_size = info.trade_contract_size
        volume_min = info.volume_min
        volume_max = info.volume_max
        volume_step = info.volume_step

        # Raw volume needed for target_risk_usd
        # Dollar Risk = Lots * contract_size * distance
        raw_volume = self.target_risk_usd / (contract_size * distance)

        # Round to volume_step
        steps = round(raw_volume / volume_step)
        # Ensure actual risk doesn't exceed $15.00 ceiling
        if (steps * volume_step * contract_size * distance) > 15.0 and (steps - 1) * volume_step >= volume_min:
            steps -= 1

        volume = max(volume_min, min(volume_max, steps * volume_step))
        volume = round(volume, 4)

        # Check resulting risk
        actual_risk = volume * contract_size * distance
        logger.info(
            f"[{symbol}] Distance: {distance:.5f} | Contract: {contract_size} | "
            f"Calculated Lot: {volume} | Actual Risk: ${actual_risk:.2f} (Target: ${self.target_risk_usd:.2f})"
        )
        return volume

    def execute_setup(self, setup: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        raw_sym = setup.get("symbol", "").upper()
        score = float(setup.get("confluence_score", 0.0))
        direction = setup.get("direction", "LONG").upper()
        planned_entry = float(setup.get("entry_price", 0.0))
        planned_sl = float(setup.get("stop_loss", 0.0))
        planned_tp = float(setup.get("take_profit", 0.0))

        if score < self.min_score:
            return {"status": "SKIPPED", "reason": f"Score {score} < {self.min_score}"}

        mt5_symbol = SYMBOL_MAP.get(raw_sym)
        if not mt5_symbol:
            return {"status": "SKIPPED", "reason": f"No MT5 mapping for {raw_sym}"}

        # Check symbol selection
        if not mt5.symbol_select(mt5_symbol, True):
            return {"status": "ERROR", "reason": f"Could not select {mt5_symbol}"}

        # Check open positions
        open_pos = self.get_open_positions()
        if mt5_symbol in open_pos:
            return {
                "status": "EXISTING_POSITION",
                "reason": f"Active position ticket {open_pos[mt5_symbol].ticket} already open on {mt5_symbol}"
            }

        tick = mt5.symbol_info_tick(mt5_symbol)
        if not tick:
            return {"status": "ERROR", "reason": f"No market tick for {mt5_symbol}"}

        s_info = mt5.symbol_info(mt5_symbol)
        digits = s_info.digits

        point = s_info.point
        stops_level = s_info.trade_stops_level * point
        spread = tick.ask - tick.bid

        if direction == "LONG":
            order_type = mt5.ORDER_TYPE_BUY
            market_price = tick.ask
            # Use planned SL if below bid with margin; otherwise use planned entry - sl distance from bid
            if planned_sl < tick.bid - stops_level:
                sl_price = round(planned_sl, digits)
            else:
                sl_distance = max(abs(planned_entry - planned_sl), spread + stops_level + 20 * point)
                sl_price = round(tick.bid - sl_distance, digits)

            # Ensure SL is strictly below bid
            if sl_price >= tick.bid:
                sl_price = round(tick.bid - (spread + stops_level + 20 * point), digits)

            tp_distance = abs(planned_tp - planned_entry)
            tp_price = round(market_price + tp_distance, digits)
        else:
            order_type = mt5.ORDER_TYPE_SELL
            market_price = tick.bid
            if planned_sl > tick.ask + stops_level:
                sl_price = round(planned_sl, digits)
            else:
                sl_distance = max(abs(planned_sl - planned_entry), spread + stops_level + 20 * point)
                sl_price = round(tick.ask + sl_distance, digits)

            if sl_price <= tick.ask:
                sl_price = round(tick.ask + (spread + stops_level + 20 * point), digits)

            tp_distance = abs(planned_entry - planned_tp)
            tp_price = round(market_price - tp_distance, digits)

        lot_size = self.calculate_lot_size(mt5_symbol, market_price, sl_price)
        if not lot_size:
            return {"status": "ERROR", "reason": "Failed to compute valid lot size"}

        # Select filling type
        filling_mode = s_info.filling_mode
        if filling_mode & 2:
            type_filling = mt5.ORDER_FILLING_IOC
        elif filling_mode & 1:
            type_filling = mt5.ORDER_FILLING_FOK
        else:
            type_filling = mt5.ORDER_FILLING_RETURN

        trade_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mt5_symbol,
            "volume": lot_size,
            "type": order_type,
            "price": market_price,
            "sl": sl_price,
            "tp": tp_price,
            "deviation": 50,
            "magic": 1001,
            "comment": f"RDNL {score:.0f}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": type_filling,
        }

        # 1. Pre-trade check
        check_res = mt5.order_check(trade_request)
        if not check_res or check_res.retcode != 0:
            err_msg = check_res.comment if check_res else str(mt5.last_error())
            logger.error(f"Order check failed for {mt5_symbol}: {err_msg} (retcode: {getattr(check_res, 'retcode', 'None')})")
            return {"status": "CHECK_FAILED", "retcode": getattr(check_res, 'retcode', -1), "comment": err_msg}

        logger.info(f"✅ Order check passed for {mt5_symbol}: Required Margin: ${check_res.margin:.2f}")

        if dry_run:
            return {"status": "DRY_RUN_PASSED", "request": trade_request, "margin": check_res.margin}

        term = mt5.terminal_info()
        if not term.trade_allowed:
            logger.warning(f"Cannot dispatch deal for {mt5_symbol}: AutoTrading disabled in MT5 terminal.")
            return {
                "status": "AUTOTRADING_DISABLED",
                "retcode": 10027,
                "reason": "AutoTrading disabled by client in MT5 GUI (press Ctrl+E in MT5 to enable)",
                "request": trade_request
            }

        # 2. Dispatch Deal
        send_res = mt5.order_send(trade_request)
        if not send_res or send_res.retcode != mt5.TRADE_RETCODE_DONE:
            err_msg = send_res.comment if send_res else str(mt5.last_error())
            retcode = getattr(send_res, 'retcode', -1)
            logger.error(f"❌ Order send failed for {mt5_symbol}: {err_msg} (retcode: {retcode})")
            return {"status": "SEND_FAILED", "retcode": retcode, "comment": err_msg}

        logger.info(
            f"🎯 DEAL EXECUTED: {mt5_symbol} {direction} | Deal Ticket: {send_res.deal} | "
            f"Order: {send_res.order} | Price: {send_res.price} | Volume: {send_res.volume} | SL: {sl_price} | TP: {tp_price}"
        )

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": raw_sym,
            "mt5_symbol": mt5_symbol,
            "direction": direction,
            "deal_ticket": send_res.deal,
            "order_id": send_res.order,
            "executed_price": send_res.price,
            "volume": send_res.volume,
            "sl": sl_price,
            "tp": tp_price,
            "confluence_score": score,
            "risk_usd": self.target_risk_usd
        }
        self._record_execution(record)

        return {"status": "SUCCESS", "deal": send_res.deal, "order": send_res.order, "price": send_res.price}

    def _record_execution(self, record: Dict[str, Any]):
        os.makedirs(os.path.dirname(EXECUTED_LEDGER_PATH), exist_ok=True)
        with open(EXECUTED_LEDGER_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def punch_active_setups(self, dry_run: bool = False) -> List[Dict[str, Any]]:
        if not os.path.exists(MASTER_SETUPS_PATH):
            logger.error(f"Master setups file not found: {MASTER_SETUPS_PATH}")
            return []

        setups = []
        with open(MASTER_SETUPS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        setups.append(json.loads(line))
                    except Exception:
                        pass

        # Sort setups by confluence score descending
        setups.sort(key=lambda x: float(x.get("confluence_score", 0)), reverse=True)
        
        # Deduplicate to keep latest setup per symbol
        seen_syms = set()
        unique_setups = []
        for s in setups:
            sym = s.get("symbol")
            if sym not in seen_syms:
                seen_syms.add(sym)
                unique_setups.append(s)

        results = []
        for s in unique_setups:
            if float(s.get("confluence_score", 0)) >= self.min_score:
                logger.info(f"Evaluating candidate: {s.get('symbol')} {s.get('direction')} (Score: {s.get('confluence_score')})")
                res = self.execute_setup(s, dry_run=dry_run)
                results.append({"symbol": s.get("symbol"), "result": res})

        return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MT5 Quantitative Order Puncher")
    parser.add_argument("--risk", type=float, default=12.50, help="Target risk per trade in USD (10 to 15)")
    parser.add_argument("--min-score", type=float, default=75.0, help="Minimum confluence score to execute")
    parser.add_argument("--dry-run", action="store_true", help="Perform pre-trade validation without sending deal")
    args = parser.parse_args()

    puncher = MT5OrderPuncher(target_risk_usd=args.risk, min_score=args.min_score)
    if not puncher.connect():
        sys.exit(1)

    try:
        results = puncher.punch_active_setups(dry_run=args.dry_run)
        print("\n" + "=" * 80)
        print("📋 EXECUTION SUMMARY:")
        print("=" * 80)
        for r in results:
            print(f"[{r['symbol']}] -> {r['result'].get('status')}: {r['result']}")
    finally:
        puncher.disconnect()


if __name__ == "__main__":
    main()
