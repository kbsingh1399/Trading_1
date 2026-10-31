"""
Reactive Trade Formulation Engine with MT5 Live Sync & Spread Gatekeeper
-----------------------------------------------------------------------
Listens to live CoinGlass signals (Footprint Absorption, Trapped Traders, Divergence).
Upon detecting a new signal, it immediately wakes up, queries live MT5 bids/asks/spreads,
enforces spread gatekeeping, calculates exact position sizing (Lots), and formulates
an actionable MT5 order ticket.
"""

import sys
import json
import time
import math
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import MetaTrader5 as mt5

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PARQUET = DB_DIR / "Liquidation_Data" / "liquidation_snapshots.parquet"
KEY_LEVELS_DIR = DB_DIR / "Liquidation_Data" / "Key_Levels"
CANDLE_DIR = DB_DIR / "Candle_Data"
TICKS_PARQUET = DB_DIR / "mt5_live_ticks.parquet"
ACTIVE_SETUPS_PARQUET = DB_DIR / "active_trade_setups.parquet"
ALERTS_JSONL = DB_DIR / "live_trade_alerts.jsonl"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

MAX_SPREAD_THRESHOLDS = {
    "Forex": 0.10,             # Max 0.10% spread
    "Equity_Indices": 0.05,    # Max 0.05% spread
    "Metals_Commodities": 0.15,# Max 0.15% spread
    "Cryptos": 0.50            # Max 0.50% spread
}

RISK_PERCENT = 0.01  # 1.0% account balance risk per trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WAKEUP-ENGINE] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ReactiveEngine")


def load_registry_meta():
    if not REGISTRY_FILE.exists():
        return {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    meta = {}
    for cat, assets in reg.items():
        for sym, d in assets.items():
            meta[sym] = {"mt5": d["mt5"], "category": cat}
    return meta


def round_lot(lots: float, step: float, min_lot: float, max_lot: float) -> float:
    if step <= 0:
        step = 0.01
    rounded = math.floor(lots / step) * step
    return max(min_lot, min(max_lot, round(rounded, 2)))


def evaluate_signal_and_formulate_trade(signal_row: dict, registry_meta: dict, mt5_connected: bool) -> dict:
    symbol = signal_row.get("symbol")
    sig_name = signal_row.get("sig_name")
    sig_side = str(signal_row.get("side", "")).lower()
    sig_price = signal_row.get("price")
    sig_desc = signal_row.get("description", "")

    asset_meta = registry_meta.get(symbol)
    if not asset_meta:
        return None

    mt5_sym = asset_meta["mt5"]
    category = asset_meta["category"]

    # 1. Query live MT5 tick & symbol specifications
    tick = None
    info = None
    if mt5_connected:
        mt5.symbol_select(mt5_sym, True)
        tick = mt5.symbol_info_tick(mt5_sym)
        info = mt5.symbol_info(mt5_sym)

    if not tick or not info:
        logger.warning(f"[{symbol}] Cannot fetch live MT5 quote for '{mt5_sym}'. Skipping.")
        return None

    bid = tick.bid
    ask = tick.ask
    mid = (bid + ask) / 2.0
    digits = info.digits
    point = info.point
    spread_pts = info.spread
    spread_val = ask - bid
    spread_pct = (spread_val / mid * 100.0) if mid > 0 else 0.0
    tick_val = info.trade_tick_value
    tick_size = info.trade_tick_size
    min_lot = info.volume_min
    max_lot = info.volume_max
    lot_step = info.volume_step

    # 2. Spread Gatekeeper Check
    max_allowed_spread = MAX_SPREAD_THRESHOLDS.get(category, 0.25)
    spread_ok = (spread_pct <= max_allowed_spread)
    if not spread_ok:
        logger.warning(f"[{symbol}] Spread gatekeeper REJECTED: Spread is {spread_pct:.4f}% (Max allowed: {max_allowed_spread:.4f}%). Slippage risk.")
        return None

    # 3. Pull Liquidation Context
    snaps = pd.read_parquet(SNAPSHOTS_PARQUET) if SNAPSHOTS_PARQUET.exists() else pd.DataFrame()
    sym_snaps = snaps[snaps["symbol"] == symbol] if not snaps.empty else pd.DataFrame()
    latest_snap = sym_snaps.iloc[-1].to_dict() if not sym_snaps.empty else {}

    long_mag_dist = latest_snap.get("long_magnet_dist_pct")
    short_mag_dist = latest_snap.get("short_magnet_dist_pct")
    clld_sent = str(latest_snap.get("clld_sentiment", ""))

    # 4. Pull 15m Footprint POC
    candle_file = CANDLE_DIR / f"{symbol}.parquet"
    poc_price = mid
    if candle_file.exists():
        try:
            cdf = pd.read_parquet(candle_file)
            if not cdf.empty:
                poc_price = cdf.iloc[-1].get("poc_price", mid)
        except Exception:
            pass

    # 5. Determine Trade Direction & Strategy
    # Case A: Bullish Absorption / Trapped Shorts -> BUY
    if ("absorption" in sig_name.lower() and sig_side in ["buy", "long"]) or ("trapped shorts" in sig_name.lower()) or ("bull div" in sig_name.lower()):
        direction = "BUY"
        strategy = "Mean Reversion (Liquidity Absorption)"
        entry_price = ask
        # SL below long magnet or 0.4% below entry
        if long_mag_dist is not None and abs(long_mag_dist) <= 2.5:
            sl_price = entry_price * (1 + (long_mag_dist / 100) - 0.003)
        else:
            sl_price = entry_price * 0.995

        tp1_price = poc_price if poc_price > entry_price else entry_price * 1.012
        tp2_price = entry_price * 1.025

    # Case B: Bearish Absorption / Trapped Longs -> SELL
    elif ("absorption" in sig_name.lower() and sig_side in ["sell", "short"]) or ("trapped longs" in sig_name.lower()) or ("bear div" in sig_name.lower()):
        direction = "SELL"
        strategy = "Mean Reversion (Liquidity Absorption)"
        entry_price = bid
        # SL above short magnet or 0.4% above entry
        if short_mag_dist is not None and abs(short_mag_dist) <= 2.5:
            sl_price = entry_price * (1 + (short_mag_dist / 100) + 0.003)
        else:
            sl_price = entry_price * 1.005

        tp1_price = poc_price if poc_price < entry_price else entry_price * 0.988
        tp2_price = entry_price * 0.975

    # Case C: Exhaustion Breakout -> Trend Following
    elif "exhaustion" in sig_name.lower():
        if sig_side in ["buy", "long"]:
            direction = "SELL"  # Exhaustion of buyers leads to counter-trend fade
            strategy = "Mean Reversion (Buyer Exhaustion Fade)"
            entry_price = bid
            sl_price = entry_price * 1.004
            tp1_price = poc_price if poc_price < entry_price else entry_price * 0.99
            tp2_price = entry_price * 0.98
        else:
            direction = "BUY"   # Exhaustion of sellers leads to bounce
            strategy = "Mean Reversion (Seller Exhaustion Fade)"
            entry_price = ask
            sl_price = entry_price * 0.996
            tp1_price = poc_price if poc_price > entry_price else entry_price * 1.01
            tp2_price = entry_price * 1.02
    else:
        # Generic divergence or unclassified
        return None

    # 6. Position Sizing Calculation
    acct = mt5.account_info()
    balance = acct.balance if acct else 4714.57
    risk_dollars = balance * RISK_PERCENT

    sl_dist_points = abs(entry_price - sl_price) / point if point > 0 else 1.0
    if sl_dist_points <= 0:
        return None

    # Value of 1 lot for sl_dist_points
    # cost_per_lot = sl_dist_points * tick_val * (point / tick_size)
    cost_per_lot = sl_dist_points * (tick_val if tick_val > 0 else 1.0)
    calculated_lots = (risk_dollars / cost_per_lot) if cost_per_lot > 0 else min_lot
    final_lots = round_lot(calculated_lots, lot_step, min_lot, max_lot)

    risk_amount = abs(entry_price - sl_price)
    reward_amount = abs(tp1_price - entry_price)
    rr_ratio = round(reward_amount / risk_amount, 2) if risk_amount > 0 else 1.0

    trade_ticket = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "mt5_symbol": mt5_sym,
        "category": category,
        "strategy": strategy,
        "triggering_signal": sig_name,
        "signal_side": sig_side,
        "direction": direction,
        "mt5_bid": round(bid, digits),
        "mt5_ask": round(ask, digits),
        "mt5_spread_points": spread_pts,
        "mt5_spread_pct": round(spread_pct, 4),
        "entry_price": round(entry_price, digits),
        "stop_loss": round(sl_price, digits),
        "take_profit_1": round(tp1_price, digits),
        "take_profit_2": round(tp2_price, digits),
        "calculated_lots": final_lots,
        "risk_dollars": round(risk_dollars, 2),
        "risk_reward_ratio": rr_ratio,
        "clld_sentiment": clld_sent,
        "signal_description": sig_desc
    }

    return trade_ticket


def scan_and_react_to_signals():
    """Wakes up, reads the latest CoinGlass signals, and formulates trades against MT5."""
    if not SIGNALS_PARQUET.exists():
        logger.warning(f"No signals file found at {SIGNALS_PARQUET}")
        return []

    signals_df = pd.read_parquet(SIGNALS_PARQUET)
    if signals_df.empty:
        return []

    registry_meta = load_registry_meta()
    mt5_ok = mt5.initialize()

    # Prioritize high-edge signals from the latest dump
    high_edge_names = ["Absorption", "Trapped shorts", "Trapped longs", "Exhaustion", "Bull div", "Bear div"]
    actionable_sigs = signals_df[signals_df["sig_name"].isin(high_edge_names)]

    # Take most recent signal per symbol
    latest_per_symbol = actionable_sigs.groupby("symbol").last().reset_index()
    logger.info(f"Scanning {len(latest_per_symbol)} recent high-edge signals against MT5 market conditions...")

    formulated_trades = []
    for _, row in latest_per_symbol.iterrows():
        ticket = evaluate_signal_and_formulate_trade(row.to_dict(), registry_meta, mt5_ok)
        if ticket:
            formulated_trades.append(ticket)
            logger.info(f"[WAKEUP-ALERT] Formulated Trade: {ticket['symbol']} ({ticket['mt5_symbol']}) -> {ticket['direction']} {ticket['calculated_lots']} lots @ {ticket['entry_price']} | SL: {ticket['stop_loss']} | TP1: {ticket['take_profit_1']} | R:R: {ticket['risk_reward_ratio']} | Spread: {ticket['mt5_spread_pct']}%")

    if mt5_ok:
        mt5.shutdown()

    # Persist active trade setups
    if formulated_trades:
        new_df = pd.DataFrame(formulated_trades)
        if ACTIVE_SETUPS_PARQUET.exists():
            try:
                old_df = pd.read_parquet(ACTIVE_SETUPS_PARQUET)
                combined = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(
                    subset=["symbol", "mt5_symbol", "direction", "strategy"], keep="last"
                )
            except Exception:
                combined = new_df
        else:
            combined = new_df

        combined.to_parquet(ACTIVE_SETUPS_PARQUET, index=False)

        # Append to live alerts jsonl
        with open(ALERTS_JSONL, "a", encoding="utf-8") as f:
            for t in formulated_trades:
                f.write(json.dumps(t, default=str) + "\n")

        logger.info(f"Saved {len(formulated_trades)} active MT5 trade tickets into {ACTIVE_SETUPS_PARQUET.name}")

    return formulated_trades


if __name__ == "__main__":
    trades = scan_and_react_to_signals()
    print("\n" + "="*115)
    print(f"{'SYM':<8} {'MT5':<12} {'DIR':<6} {'LOTS':<7} {'ENTRY':<12} {'SL':<12} {'TP1':<12} {'R:R':<6} {'SPREAD(%)':<10} {'TRIGGER':<20}")
    print("="*115)
    for t in trades[:12]:
        print(f"{t['symbol']:<8} {t['mt5_symbol']:<12} {t['direction']:<6} {t['calculated_lots']:<7} {str(t['entry_price']):<12} {str(t['stop_loss']):<12} {str(t['take_profit_1']):<12} {str(t['risk_reward_ratio']):<6} {t['mt5_spread_pct']:<10.4f} {t['triggering_signal']:<20}")
    print("="*115)
