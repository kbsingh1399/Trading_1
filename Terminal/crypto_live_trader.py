"""
CoinGlass & Whale Orders Autonomous Crypto Live Trading Engine
==============================================================
Fully autonomous live trading engine executing on Blueberry Markets MT5.
Synthesizes four institutional data streams:
1. CoinGlass Footprint Order Flow Signals (Absorption, Trapped Traders, Divergence).
2. Whale Orders Blotter & Limit Walls (Table 2 institutional resting support/resistance).
3. Liquidation Map & Magnet Zones (Stacked forced exit cascade pools).
4. Live MT5 Quotes, Spreads, and Order Management.

Safety & Risk Constraints:
- Strict Spread Gatekeeper: Max spread <= 0.45% (prevents slippage loss).
- Fixed Account Risk: 1.0% equity risk per trade (~$47 on $4,714 balance).
- Exact Lot Sizing: Dynamic calculation using broker tick value, tick size, and contract size.
- Hard Stops & Targets: Every order placed with hard SL and TP.
- Risk-Free Break-Even Lock: Automatically trails SL to break-even once trade hits +1R profit.
- Max Concurrent Crypto Positions: 2 active trades max to avoid overexposure.
"""

import os
import sys
import time
import math
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import MetaTrader5 as mt5

# Reconfigure stdout for UTF-8 and non-buffered logging
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# ---------------------------------------------------------------------------
# Paths and Configuration
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
WHALE_DIR = DB_DIR / "Whale_Orders"

SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
WHALE_SNAPSHOTS_PARQUET = WHALE_DIR / "whale_market_snapshots.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

EXECUTION_LOG_JSONL = DB_DIR / "crypto_trade_execution_log.jsonl"
ACTIVE_POSITIONS_PARQUET = DB_DIR / "active_crypto_positions.parquet"

# Magic number identifying trades placed by this autonomous engine
MAGIC_NUMBER = 888123
MAX_CONCURRENT_POSITIONS = 2
MAX_ALLOWED_SPREAD_PCT = 0.45       # Max 0.45% spread
RISK_PERCENT = 0.01                 # 1.0% account risk
HARD_MAX_LOSS_USD = 50.00           # Strict Hard Max Loss Ceiling per trade ($50.00)
MIN_RR_RATIO = 1.5                  # Minimum 1.5:1 reward to risk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LIVE-TRADER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LiveTrader")


def load_crypto_registry() -> Dict[str, dict]:
    """Load metadata and MT5 symbol mappings for crypto assets."""
    if not REGISTRY_FILE.exists():
        return {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    return reg.get("Cryptos", {})


def calculate_lot_size(
    info: mt5.SymbolInfo,
    risk_dollars: float,
    entry_price: float,
    sl_price: float
) -> Tuple[float, float]:
    """
    Calculates exact broker lot size based on fixed dollar risk.
    Returns (final_lots, expected_loss_usd).
    """
    price_dist = abs(entry_price - sl_price)
    if price_dist <= 0 or info.point <= 0:
        return 0.0, 0.0

    points_dist = price_dist / info.point
    tick_val = info.trade_tick_value
    tick_size = info.trade_tick_size
    point = info.point

    cost_per_point_per_lot = tick_val * (point / tick_size)
    loss_per_lot = points_dist * cost_per_point_per_lot

    if loss_per_lot <= 0:
        return 0.0, 0.0

    calculated_lots = min(risk_dollars, HARD_MAX_LOSS_USD) / loss_per_lot

    # Round to volume_step
    step = info.volume_step if info.volume_step > 0 else 0.01
    rounded_lots = round(math.floor(calculated_lots / step) * step, 2)
    final_lots = max(info.volume_min, min(info.volume_max, rounded_lots))

    actual_loss_usd = round(final_lots * loss_per_lot, 2)

    # HARD CEILING: Strictly enforce $50.00 maximum dollar loss
    while actual_loss_usd > HARD_MAX_LOSS_USD and final_lots > info.volume_min:
        final_lots = round(final_lots - step, 2)
        actual_loss_usd = round(final_lots * loss_per_lot, 2)

    if actual_loss_usd > HARD_MAX_LOSS_USD:
        # Minimum lot size exceeds $50 hard limit
        return 0.0, 0.0

    return final_lots, actual_loss_usd


class CryptoLiveTrader:
    def __init__(self):
        self.crypto_map = load_crypto_registry()
        self.magic = MAGIC_NUMBER
        self.active_signals_processed = set()

    def connect_mt5(self) -> bool:
        """Initialize connection to MetaTrader 5."""
        if not mt5.initialize():
            logger.error(f"Failed to connect to MT5: {mt5.last_error()}")
            return False
        acct = mt5.account_info()
        if not acct:
            logger.error("Connected to MT5 but account_info is None.")
            return False
        logger.info(f"Connected to MT5 Account: {acct.login} ({acct.name}) | Equity: ${acct.equity:,.2f}")
        return True

    def get_open_crypto_positions(self) -> List[dict]:
        """Fetch all currently open crypto positions placed by this engine."""
        positions = mt5.positions_get()
        if not positions:
            return []
        crypto_mt5_symbols = {v["mt5"] for v in self.crypto_map.values()}
        bot_positions = []
        for p in positions:
            if p.symbol in crypto_mt5_symbols and p.magic == self.magic:
                bot_positions.append({
                    "ticket": p.ticket,
                    "symbol": p.symbol,
                    "type": "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL",
                    "volume": p.volume,
                    "price_open": p.price_open,
                    "price_current": p.price_current,
                    "sl": p.sl,
                    "tp": p.tp,
                    "profit": p.profit,
                    "comment": p.comment,
                    "time": datetime.fromtimestamp(p.time, timezone.utc).isoformat()
                })
        return bot_positions

    def manage_open_positions(self):
        """
        Manages active positions:
        - HARD CIRCUIT BREAKER: Closes position immediately if loss approaches -$50.00 hard limit.
        - If profit reaches +1R (halfway to TP1 or equal to initial risk distance),
          moves SL to break-even (entry + small buffer) to guarantee zero loss.
        """
        open_pos = self.get_open_crypto_positions()
        if not open_pos:
            return

        for pos in open_pos:
            mt5_sym = pos["symbol"]
            info = mt5.symbol_info(mt5_sym)
            tick = mt5.symbol_info_tick(mt5_sym)
            if not info or not tick:
                continue

            # 1. HARD LOSS CIRCUIT BREAKER ($50 limit)
            if pos["profit"] <= -48.00:
                logger.warning(f"[{mt5_sym}] [CIRCUIT-BREAKER] Position #{pos['ticket']} hit hard loss threshold (${pos['profit']:.2f} <= -$48.00). Executing emergency market close!")
                close_type = mt5.ORDER_TYPE_SELL if pos["type"] == "BUY" else mt5.ORDER_TYPE_BUY
                close_price = tick.bid if pos["type"] == "BUY" else tick.ask
                close_req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "position": pos["ticket"],
                    "symbol": mt5_sym,
                    "volume": pos["volume"],
                    "type": close_type,
                    "price": close_price,
                    "deviation": 30,
                    "magic": self.magic,
                    "comment": "CG_HARD_50USD_STOP",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC
                }
                close_res = mt5.order_send(close_req)
                if close_res and close_res.retcode == mt5.TRADE_RETCODE_DONE:
                    logger.info(f"[{mt5_sym}] Emergency market close completed for Ticket #{pos['ticket']}!")
                else:
                    logger.error(f"[{mt5_sym}] Emergency market close failed: {close_res.comment if close_res else 'Unknown'}")
                continue

            point = info.point
            digits = info.digits
            entry = pos["price_open"]
            current = pos["price_current"]
            current_sl = pos["sl"]
            pos_type = pos["type"]

            # Initial risk distance
            if current_sl and current_sl > 0:
                init_risk = abs(entry - current_sl)
            else:
                init_risk = entry * 0.005  # fallback 0.5%

            # Check if +1R reached and SL not yet at/beyond break-even
            if pos_type == "BUY":
                gain = current - entry
                be_price = round(entry + 2 * point, digits)
                if gain >= init_risk and current_sl < entry:
                    logger.info(f"[{mt5_sym}] Trade reached +1R gain (+${pos['profit']:.2f}). Moving SL to Break-Even ({be_price})!")
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": pos["ticket"],
                        "symbol": mt5_sym,
                        "sl": be_price,
                        "tp": pos["tp"]
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"[{mt5_sym}] SL successfully moved to Break-Even!")
                    else:
                        logger.warning(f"[{mt5_sym}] Failed to update SL to BE: {res.comment if res else 'Error'}")

            elif pos_type == "SELL":
                gain = entry - current
                be_price = round(entry - 2 * point, digits)
                if gain >= init_risk and (current_sl > entry or current_sl == 0):
                    logger.info(f"[{mt5_sym}] Trade reached +1R gain (+${pos['profit']:.2f}). Moving SL to Break-Even ({be_price})!")
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": pos["ticket"],
                        "symbol": mt5_sym,
                        "sl": be_price,
                        "tp": pos["tp"]
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"[{mt5_sym}] SL successfully moved to Break-Even!")
                    else:
                        logger.warning(f"[{mt5_sym}] Failed to update SL to BE: {res.comment if res else 'Error'}")

    def evaluate_asset_confluence(self, symbol: str) -> Optional[dict]:
        """
        Synthesizes Footprint Signals, Whale Orders, Liquidation Magnets, and MT5 live quotes
        to produce a high-confluence trade ticket.
        """
        meta = self.crypto_map.get(symbol)
        if not meta:
            return None

        mt5_sym = meta["mt5"]

        # 1. MT5 Quote & Spread Check
        mt5.symbol_select(mt5_sym, True)
        tick = mt5.symbol_info_tick(mt5_sym)
        info = mt5.symbol_info(mt5_sym)

        if not tick or not info or tick.bid <= 0 or tick.ask <= 0:
            return None

        mid = (tick.bid + tick.ask) / 2.0
        spread_val = tick.ask - tick.bid
        spread_pct = (spread_val / mid) * 100.0

        # Reject if spread exceeds gatekeeper threshold
        if spread_pct > MAX_ALLOWED_SPREAD_PCT:
            return None

        # 2. Check CoinGlass Footprint Signals
        if not SIGNALS_PARQUET.exists():
            return None

        sig_df = pd.read_parquet(SIGNALS_PARQUET)
        sym_sigs = sig_df[sig_df["symbol"] == symbol]
        if sym_sigs.empty:
            return None

        latest_sig = sym_sigs.iloc[-1].to_dict()
        sig_name = latest_sig.get("sig_name", "")
        sig_side = str(latest_sig.get("side", "")).lower()
        sig_time = latest_sig.get("datetime_utc", "")

        # Signal de-duplication key
        sig_key = f"{symbol}_{sig_name}_{sig_time}_{latest_sig.get('price')}"
        if sig_key in self.active_signals_processed:
            return None

        # 3. Pull Whale Orders Context (Table 2)
        whale_open_p = WHALE_DIR / "Open_Orders" / f"{symbol}_whale_open_orders.parquet"
        whale_bids = pd.DataFrame()
        whale_asks = pd.DataFrame()
        top_bid_wall_usd = 0.0
        top_ask_wall_usd = 0.0
        top_bid_price = 0.0
        top_ask_price = 0.0

        if whale_open_p.exists():
            w_df = pd.read_parquet(whale_open_p)
            if not w_df.empty:
                bids = w_df[w_df["side"].str.upper() == "BID"]
                asks = w_df[w_df["side"].str.upper() == "ASK"]
                if not bids.empty:
                    top_b = bids.sort_values(by="remaining_usd", ascending=False).iloc[0]
                    top_bid_wall_usd = top_b["remaining_usd"]
                    top_bid_price = top_b["price"]
                if not asks.empty:
                    top_a = asks.sort_values(by="remaining_usd", ascending=False).iloc[0]
                    top_ask_wall_usd = top_a["remaining_usd"]
                    top_ask_price = top_a["price"]

        # 4. Pull Liquidation Magnet Zones
        magnet_p = LIQ_DIR / "Magnet_Zones" / f"{symbol}_magnet_zones.parquet"
        nearest_long_liq = None
        nearest_short_liq = None

        if magnet_p.exists():
            m_df = pd.read_parquet(magnet_p)
            if not m_df.empty:
                long_bands = m_df[m_df["side"] == "long"]
                short_bands = m_df[m_df["side"] == "short"]
                if not long_bands.empty:
                    nearest_long_liq = long_bands.iloc[0].to_dict()
                if not short_bands.empty:
                    nearest_short_liq = short_bands.iloc[0].to_dict()

        # 5. Pull 15m Footprint POC
        candle_p = CANDLE_DIR / f"{symbol}.parquet"
        poc_price = mid
        if candle_p.exists():
            c_df = pd.read_parquet(candle_p)
            if not c_df.empty:
                poc_price = c_df.iloc[-1].get("poc_price", mid)

        # -------------------------------------------------------------
        # Confluence Formulation
        # -------------------------------------------------------------
        confluence_score = 50
        reasons = []
        direction = None
        strategy = ""

        # SCENARIO A: BULLISH CONFLUENCE (Absorption / Trapped Shorts / Bull Div)
        if ("absorption" in sig_name.lower() and sig_side in ["buy", "long"]) or \
           ("trapped shorts" in sig_name.lower()) or \
           ("bull div" in sig_name.lower()) or \
           ("divergence" in sig_name.lower() and sig_side in ["buy", "bullish"]):

            direction = "BUY"
            strategy = "Footprint Absorption & Whale Wall Support"
            entry_price = tick.ask
            reasons.append(f"Footprint {sig_name} ({sig_side})")

            # Check Whale Bid Wall Support
            if top_bid_wall_usd >= 500_000 and top_bid_price > 0 and top_bid_price < entry_price:
                dist_pct = (entry_price - top_bid_price) / entry_price * 100.0
                if dist_pct <= 2.5:
                    confluence_score += 20
                    reasons.append(f"Whale Bid Wall Support (${top_bid_wall_usd/1e6:.2f}M @ {top_bid_price})")
                    # Set SL just below the Whale Wall
                    sl_price = top_bid_price * 0.997
                else:
                    sl_price = entry_price * 0.995
            else:
                sl_price = entry_price * 0.995

            # Target POC or next resistance
            tp1_price = poc_price if poc_price > entry_price * 1.008 else entry_price * 1.015
            tp2_price = entry_price * 1.025

        # SCENARIO B: BEARISH CONFLUENCE (Absorption / Trapped Longs / Bear Div / Exhaustion)
        elif ("absorption" in sig_name.lower() and sig_side in ["sell", "short"]) or \
             ("trapped longs" in sig_name.lower()) or \
             ("bear div" in sig_name.lower()) or \
             ("exhaustion" in sig_name.lower() and sig_side in ["buy", "long"]) or \
             ("divergence" in sig_name.lower() and sig_side in ["sell", "bearish"]):

            direction = "SELL"
            strategy = "Footprint Absorption & Whale Wall Resistance"
            entry_price = tick.bid
            reasons.append(f"Footprint {sig_name} ({sig_side})")

            # Check Whale Ask Wall Resistance
            if top_ask_wall_usd >= 500_000 and top_ask_price > 0 and top_ask_price > entry_price:
                dist_pct = (top_ask_price - entry_price) / entry_price * 100.0
                if dist_pct <= 2.5:
                    confluence_score += 20
                    reasons.append(f"Whale Ask Wall Resistance (${top_ask_wall_usd/1e6:.2f}M @ {top_ask_price})")
                    # Set SL just above the Whale Wall
                    sl_price = top_ask_price * 1.003
                else:
                    sl_price = entry_price * 1.005
            else:
                sl_price = entry_price * 1.005

            # Target POC or next support
            tp1_price = poc_price if poc_price < entry_price * 0.992 else entry_price * 0.985
            tp2_price = entry_price * 0.975

        else:
            return None

        # Check Risk to Reward Ratio
        risk_dist = abs(entry_price - sl_price)
        reward_dist = abs(tp1_price - entry_price)
        rr_ratio = round(reward_dist / risk_dist, 2) if risk_dist > 0 else 0.0

        if rr_ratio < MIN_RR_RATIO:
            # Adjust TP1 to meet minimum R:R
            if direction == "BUY":
                tp1_price = entry_price + risk_dist * MIN_RR_RATIO
            else:
                tp1_price = entry_price - risk_dist * MIN_RR_RATIO
            reward_dist = abs(tp1_price - entry_price)
            rr_ratio = round(reward_dist / risk_dist, 2)

        # 6. Calculate Position Sizing
        acct = mt5.account_info()
        balance = acct.balance if acct else 4714.57
        risk_usd = balance * RISK_PERCENT

        final_lots, actual_risk = calculate_lot_size(info, risk_usd, entry_price, sl_price)
        if final_lots <= 0:
            return None

        digits = info.digits

        return {
            "symbol": symbol,
            "mt5_symbol": mt5_sym,
            "direction": direction,
            "strategy": strategy,
            "confluence_score": confluence_score,
            "reasons": " | ".join(reasons),
            "signal_key": sig_key,
            "entry_price": round(entry_price, digits),
            "stop_loss": round(sl_price, digits),
            "take_profit_1": round(tp1_price, digits),
            "take_profit_2": round(tp2_price, digits),
            "lots": final_lots,
            "risk_usd": actual_risk,
            "risk_reward_ratio": rr_ratio,
            "spread_pct": round(spread_pct, 4),
            "filling_mode": mt5.ORDER_FILLING_IOC
        }

    def validate_and_adjust_stops_for_ticket(self, ticket: dict, info: mt5.SymbolInfo) -> bool:
        """
        Validates stops against broker stop levels. If retcode is 10016 (Invalid stops),
        adaptively expands SL and TP until compliant, then recalculates lot size to maintain
        strict 1.0% risk.
        """
        mt5_sym = ticket["mt5_symbol"]
        order_type = mt5.ORDER_TYPE_BUY if ticket["direction"] == "BUY" else mt5.ORDER_TYPE_SELL
        digits = info.digits
        point = info.point

        sl = ticket["stop_loss"]
        tp = ticket["take_profit_1"]
        lots = ticket["lots"]

        for attempt in range(15):
            req = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": mt5_sym,
                "volume": lots,
                "type": order_type,
                "price": ticket["entry_price"],
                "sl": sl,
                "tp": tp,
                "deviation": 20,
                "magic": self.magic,
                "comment": f"CG_{ticket['direction']}_{ticket['symbol']}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": ticket["filling_mode"]
            }
            check = mt5.order_check(req)
            if check.retcode == 0:
                if ticket.get("risk_usd", 0) > HARD_MAX_LOSS_USD:
                    logger.warning(f"[{mt5_sym}] Ticket risk exceeds $50.00 cap (${ticket.get('risk_usd'):.2f}). Order rejected.")
                    return False
                ticket["stop_loss"] = sl
                ticket["take_profit_1"] = tp
                ticket["lots"] = lots
                return True
            elif check.retcode == 10016:  # Invalid stops
                # Expand SL and TP by 10 points further away
                step_points = max(10, info.trade_stops_level) * point
                if order_type == mt5.ORDER_TYPE_BUY:
                    sl = round(sl - step_points, digits)
                    tp = round(tp + step_points * 2, digits)
                else:
                    sl = round(sl + step_points, digits)
                    tp = round(tp - step_points * 2, digits)

                # Recompute lot size to maintain strict 1.0% dollar risk
                acct = mt5.account_info()
                balance = acct.balance if acct else 4714.57
                risk_usd = balance * RISK_PERCENT
                new_lots, actual_risk = calculate_lot_size(info, risk_usd, ticket["entry_price"], sl)
                if new_lots <= 0:
                    return False
                lots = new_lots
                ticket["risk_usd"] = actual_risk
            else:
                logger.warning(f"[{mt5_sym}] Order Check rejected ({check.retcode}): {check.comment}")
                return False

        return False

    def execute_trade(self, ticket: dict) -> bool:
        """Executes a formulated trade directly on MT5."""
        mt5_sym = ticket["mt5_symbol"]
        info = mt5.symbol_info(mt5_sym)
        if not info:
            logger.error(f"Cannot get symbol info for {mt5_sym}")
            return False

        # Validate and adaptively adjust stops to broker requirements
        if not self.validate_and_adjust_stops_for_ticket(ticket, info):
            logger.error(f"[{mt5_sym}] Could not find broker-compliant stops. Aborting.")
            return False

        order_type = mt5.ORDER_TYPE_BUY if ticket["direction"] == "BUY" else mt5.ORDER_TYPE_SELL

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mt5_sym,
            "volume": ticket["lots"],
            "type": order_type,
            "price": ticket["entry_price"],
            "sl": ticket["stop_loss"],
            "tp": ticket["take_profit_1"],
            "deviation": 20,
            "magic": self.magic,
            "comment": f"CG_{ticket['direction']}_{ticket['symbol']}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": ticket["filling_mode"]
        }

        logger.info(f"Submitting MT5 Order: {ticket['direction']} {ticket['lots']} {mt5_sym} @ {ticket['entry_price']} | SL: {ticket['stop_loss']} | TP: {ticket['take_profit_1']} | Risk: ${ticket['risk_usd']:.2f}")

        res = mt5.order_send(req)
        if not res or res.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"MT5 Order Send failed ({res.retcode if res else 'None'}): {res.comment if res else 'Unknown'}")
            return False

        deal_ticket = res.order
        logger.info(f"TRADE EXECUTED SUCCESSFULLY! Order Ticket #{deal_ticket} | Volume: {res.volume} @ {res.price}")

        # Mark signal processed
        self.active_signals_processed.add(ticket["signal_key"])

        # Persist audit record
        audit_record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "ticket": deal_ticket,
            "symbol": ticket["symbol"],
            "mt5_symbol": mt5_sym,
            "direction": ticket["direction"],
            "strategy": ticket["strategy"],
            "volume": res.volume,
            "entry_price": res.price,
            "sl": ticket["stop_loss"],
            "tp": ticket["take_profit_1"],
            "risk_usd": ticket["risk_usd"],
            "risk_reward_ratio": ticket["risk_reward_ratio"],
            "spread_pct": ticket["spread_pct"],
            "confluence": ticket["reasons"]
        }

        with open(EXECUTION_LOG_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_record) + "\n")

        return True

    def scan_and_trade_cycle(self):
        """Performs one full evaluation cycle across all crypto assets."""
        if not self.connect_mt5():
            return

        try:
            # Step 1: Manage open positions (Trailing & Break-Even)
            self.manage_open_positions()

            # Step 2: Check current open crypto positions count
            open_pos = self.get_open_crypto_positions()
            current_count = len(open_pos)
            open_symbols = {p["symbol"] for p in open_pos}

            if current_count >= MAX_CONCURRENT_POSITIONS:
                logger.info(f"Max concurrent crypto positions reached ({current_count}/{MAX_CONCURRENT_POSITIONS}). Managing active trades...")
                return

            # Step 3: Scan mutual crypto assets for high confluence setups
            candidate_tickets = []
            for sym in self.crypto_map.keys():
                meta = self.crypto_map[sym]
                if meta["mt5"] in open_symbols:
                    continue  # Already in position

                ticket = self.evaluate_asset_confluence(sym)
                if ticket and ticket["confluence_score"] >= 65:
                    candidate_tickets.append(ticket)

            # Sort candidate tickets by confluence score and R:R
            candidate_tickets.sort(key=lambda t: (t["confluence_score"], t["risk_reward_ratio"]), reverse=True)

            # Step 4: Execute top candidates up to position limit
            slots_available = MAX_CONCURRENT_POSITIONS - current_count
            for ticket in candidate_tickets[:slots_available]:
                logger.info(f"High-Confluence Opportunity Found: {ticket['symbol']} ({ticket['direction']}) - Score: {ticket['confluence_score']}%")
                logger.info(f"   -> Reasons: {ticket['reasons']}")
                self.execute_trade(ticket)

        finally:
            mt5.shutdown()

    def run_daemon(self, loop_interval: int = 10):
        """Runs continuous autonomous trading daemon."""
        logger.info(f"Starting CoinGlass Crypto Autonomous Live Trader Daemon (Cycle Interval: {loop_interval}s)")
        while True:
            try:
                self.scan_and_trade_cycle()
            except Exception as e:
                logger.error(f"Error during live trader cycle: {e}", exc_info=True)
            time.sleep(loop_interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Autonomous Crypto Live Trading Daemon")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation cycle and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=10, help="Seconds between scan cycles (default 10s)")
    args = parser.parse_args()

    trader = CryptoLiveTrader()
    if args.daemon:
        trader.run_daemon(args.interval)
    else:
        trader.scan_and_trade_cycle()
