"""
Quantitative Trade Formulation Engine
--------------------------------------
Synthesizes:
1. Structural Liquidity Map: Liquidation Snapshots, Key Levels, and Magnet Zones.
2. Order-Flow Microstructure: 15m Footprint Candles (Delta, CVD, POC, Volume).
3. Real-Time Derivatives Positioning: CLLD, Funding Rates, Trapped Longs/Shorts, Absorption.

Generates:
- Strategy A: Liquidity Sweep & Absorption (Mean Reversion)
- Strategy B: Cascade Liquidity Void Expansion (Trend Following)
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
OUTPUT_SETUPS_PARQUET = DB_DIR / "active_trade_setups.parquet"


def load_datasets():
    snaps = pd.read_parquet(SNAPSHOTS_PARQUET) if SNAPSHOTS_PARQUET.exists() else pd.DataFrame()
    signals = pd.read_parquet(SIGNALS_PARQUET) if SIGNALS_PARQUET.exists() else pd.DataFrame()
    return snaps, signals


def evaluate_asset(symbol, snaps, signals):
    # 1. Get latest liquidation snapshot
    sym_snaps = snaps[snaps["symbol"] == symbol] if not snaps.empty else pd.DataFrame()
    if sym_snaps.empty:
        return None
    latest_snap = sym_snaps.iloc[-1].to_dict()

    cur_price = latest_snap.get("last_price")
    if not cur_price or np.isnan(cur_price):
        return None

    mt5_symbol = latest_snap.get("mt5_symbol", symbol)
    clld = latest_snap.get("clld_current", 0)
    clld_sent = str(latest_snap.get("clld_sentiment", ""))
    
    short_mag_dist = latest_snap.get("short_magnet_dist_pct")
    long_mag_dist = latest_snap.get("long_magnet_dist_pct")
    short_mag_str = latest_snap.get("short_magnet_strength", 0)
    long_mag_str = latest_snap.get("long_magnet_strength", 0)

    # 2. Get latest Footprint candle data
    candle_file = CANDLE_DIR / f"{symbol}.parquet"
    if not candle_file.exists():
        candle_file = CANDLE_DIR / f"{symbol}_15m.parquet"
    
    candles = pd.read_parquet(candle_file) if candle_file.exists() else pd.DataFrame()
    last_candle = candles.iloc[-1].to_dict() if not candles.empty else {}
    prev_candle = candles.iloc[-2].to_dict() if len(candles) >= 2 else {}

    candle_close = last_candle.get("close", cur_price)
    poc_price = last_candle.get("poc_price", cur_price)
    delta = last_candle.get("delta", 0)
    cum_delta = last_candle.get("cumulative_delta", 0)
    tot_vol = last_candle.get("total_volume", 1)
    delta_ratio = delta / tot_vol if tot_vol > 0 else 0
    oi_change = last_candle.get("oi_change", 0)

    # 3. Get latest Order Flow Signals for this asset
    sym_sigs = signals[signals["symbol"] == symbol] if not signals.empty else pd.DataFrame()
    recent_sig_names = sym_sigs["sig_name"].tolist() if not sym_sigs.empty else []

    has_absorption = "Absorption" in recent_sig_names
    has_exhaustion = "Exhaustion" in recent_sig_names
    has_trapped_longs = "Trapped longs" in recent_sig_names
    has_trapped_shorts = "Trapped shorts" in recent_sig_names
    has_divergence = ("Divergence" in recent_sig_names) or ("Bull div" in recent_sig_names) or ("Bear div" in recent_sig_names)

    # 4. Load Key Barrier Levels
    kl_file = KEY_LEVELS_DIR / f"{symbol}_key_levels.parquet"
    kl_df = pd.read_parquet(kl_file) if kl_file.exists() else pd.DataFrame()

    setups = []

    # -------------------------------------------------------------
    # MODEL 1: LIQUIDITY SWEEP MEAN REVERSION (Absorption Model)
    # -------------------------------------------------------------
    # Condition LONG: Price is near long magnet (support stops), CLLD shows Bullish turn or negative extreme,
    # and Footprint shows absorption / trapped shorts / positive delta divergence.
    if long_mag_dist is not None and abs(long_mag_dist) <= 2.5:
        # Distance is within 2.5% of long liquidation pool
        confluence = 50
        reasons = []

        if abs(long_mag_dist) <= 1.2:
            confluence += 15
            reasons.append(f"In Long Magnet Zone ({round(long_mag_dist, 2)}%)")

        if long_mag_str and long_mag_str > 100:
            confluence += 10
            reasons.append(f"High Magnet Density ({int(long_mag_str)})")

        if "bullish" in clld_sent.lower() or (clld is not None and clld < -200):
            confluence += 15
            reasons.append(f"CLLD Bullish Alignment ({clld_sent})")

        if has_absorption or has_trapped_shorts:
            confluence += 15
            reasons.append("Footprint Absorption / Trapped Shorts Confirmed")

        if delta > 0 and candle_close > poc_price:
            confluence += 10
            reasons.append("Delta Positive & Price > POC")

        if confluence >= 65:
            # Entry at current price, SL below long magnet zone, TP at POC or POC + 1.5%
            entry = cur_price
            sl = entry * (1 + (long_mag_dist / 100) - 0.004) # 0.4% below magnet zone
            tp1 = poc_price if poc_price > entry else entry * 1.015
            tp2 = entry * (1 + abs(long_mag_dist / 100) * 1.5)
            risk = entry - sl
            reward = tp1 - entry
            rr = round(reward / risk, 2) if risk > 0 else 1.0

            setups.append({
                "symbol": symbol,
                "mt5_symbol": mt5_symbol,
                "strategy": "Mean Reversion (Liquidity Absorption)",
                "direction": "BUY",
                "confluence_score": min(100, confluence),
                "entry_price": round(entry, 4),
                "stop_loss": round(sl, 4),
                "take_profit_1": round(tp1, 4),
                "take_profit_2": round(tp2, 4),
                "risk_reward_ratio": rr,
                "confluence_factors": " | ".join(reasons)
            })

    # Condition SHORT: Price is near short magnet (resistance stops), CLLD shows Bearish turn,
    # and Footprint shows absorption / trapped longs / exhaustion.
    if short_mag_dist is not None and abs(short_mag_dist) <= 2.5:
        confluence = 50
        reasons = []

        if abs(short_mag_dist) <= 1.2:
            confluence += 15
            reasons.append(f"In Short Magnet Zone ({round(short_mag_dist, 2)}%)")

        if short_mag_str and short_mag_str > 100:
            confluence += 10
            reasons.append(f"High Short Magnet Density ({int(short_mag_str)})")

        if "bearish" in clld_sent.lower() or (clld is not None and clld > 200):
            confluence += 15
            reasons.append(f"CLLD Bearish Alignment ({clld_sent})")

        if has_absorption or has_trapped_longs or has_exhaustion:
            confluence += 15
            reasons.append("Footprint Absorption / Trapped Longs Confirmed")

        if delta < 0 and candle_close < poc_price:
            confluence += 10
            reasons.append("Delta Negative & Price < POC")

        if confluence >= 65:
            entry = cur_price
            sl = entry * (1 + (short_mag_dist / 100) + 0.004) # 0.4% above magnet zone
            tp1 = poc_price if poc_price < entry else entry * 0.985
            tp2 = entry * (1 - abs(short_mag_dist / 100) * 1.5)
            risk = sl - entry
            reward = entry - tp1
            rr = round(reward / risk, 2) if risk > 0 else 1.0

            setups.append({
                "symbol": symbol,
                "mt5_symbol": mt5_symbol,
                "strategy": "Mean Reversion (Liquidity Absorption)",
                "direction": "SELL",
                "confluence_score": min(100, confluence),
                "entry_price": round(entry, 4),
                "stop_loss": round(sl, 4),
                "take_profit_1": round(tp1, 4),
                "take_profit_2": round(tp2, 4),
                "risk_reward_ratio": rr,
                "confluence_factors": " | ".join(reasons)
            })

    # -------------------------------------------------------------
    # MODEL 2: CASCADE BREAKOUT EXPANSION (Trend Following Model)
    # -------------------------------------------------------------
    # Bullish Trend Continuation: Price breaks above Key Barrier with aggressive positive Delta,
    # increasing OI, and strong directional momentum.
    if delta_ratio > 0.15 and oi_change is not None and oi_change > 0:
        confluence = 55
        reasons = [f"Strong Buy Delta Imbalance ({round(delta_ratio * 100, 1)}%)", "Open Interest Expansion"]

        if candle_close > poc_price:
            confluence += 15
            reasons.append("Candle Closes Above POC")

        if clld is not None and clld > 100:
            confluence += 15
            reasons.append("CLLD Strongly Positive (Bullish Momentum)")

        if not kl_df.empty:
            upper_barriers = kl_df[(kl_df["side"] == "short") & (kl_df["price"] > cur_price)]
            if not upper_barriers.empty:
                next_barrier = upper_barriers.iloc[0]["price"]
                confluence += 10
                reasons.append(f"Clear Run to Next Strike Barrier ({round(next_barrier, 2)})")
            else:
                next_barrier = cur_price * 1.03
        else:
            next_barrier = cur_price * 1.03

        if confluence >= 70:
            entry = cur_price
            sl = poc_price * 0.996 # Just below current 15m POC
            tp1 = next_barrier
            tp2 = entry + (next_barrier - entry) * 1.6
            risk = entry - sl
            reward = tp1 - entry
            rr = round(reward / risk, 2) if risk > 0 else 1.0

            setups.append({
                "symbol": symbol,
                "mt5_symbol": mt5_symbol,
                "strategy": "Trend Following (Cascade Expansion)",
                "direction": "BUY",
                "confluence_score": min(100, confluence),
                "entry_price": round(entry, 4),
                "stop_loss": round(sl, 4),
                "take_profit_1": round(tp1, 4),
                "take_profit_2": round(tp2, 4),
                "risk_reward_ratio": rr,
                "confluence_factors": " | ".join(reasons)
            })

    return setups


def main():
    print("=======================================================")
    print(" EXECUTING QUANTITATIVE TRADE FORMULATION SCANNER      ")
    print("=======================================================")

    snaps, signals = load_datasets()
    if snaps.empty:
        print("[-] No liquidation snapshots found.")
        return

    available_symbols = snaps["symbol"].unique()
    print(f"[*] Scanning across {len(available_symbols)} mutual assets...")

    all_setups = []
    for sym in available_symbols:
        res = evaluate_asset(sym, snaps, signals)
        if res:
            all_setups.extend(res)

    if not all_setups:
        print("[-] No high-confluence setups identified at this timestamp.")
        return

    df_setups = pd.DataFrame(all_setups)
    df_setups = df_setups.sort_values(by="confluence_score", ascending=False).reset_index(drop=True)

    # Persist to Parquet
    df_setups["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    df_setups.to_parquet(OUTPUT_SETUPS_PARQUET, index=False)
    print(f"\n[+] Generated {len(df_setups)} High-Confluence Trade Setups!")
    print(f"[+] Persisted to: {OUTPUT_SETUPS_PARQUET.name}")

    print("\n" + "="*100)
    print(f"{'SYM':<8} {'MT5':<10} {'STRATEGY':<32} {'DIR':<6} {'SCORE':<7} {'ENTRY':<12} {'SL':<12} {'TP1':<12} {'R:R':<5}")
    print("="*100)
    for _, row in df_setups.head(10).iterrows():
        print(f"{row['symbol']:<8} {row['mt5_symbol']:<10} {row['strategy'][:30]:<32} {row['direction']:<6} {row['confluence_score']:<7} {str(row['entry_price']):<12} {str(row['stop_loss']):<12} {str(row['take_profit_1']):<12} {str(row['risk_reward_ratio']):<5}")
        print(f"   +-- Confluence: {row['confluence_factors']}")
    print("="*100)


if __name__ == "__main__":
    main()
