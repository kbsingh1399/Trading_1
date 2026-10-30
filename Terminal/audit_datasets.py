import os
import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
SIGNALS_PATH = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PATH = LIQ_DIR / "liquidation_snapshots.parquet"
LEVELS_PATH = LIQ_DIR / "liquidation_levels.parquet"

print("=====================================================================")
print("             COINGLASS MASTER DATASET AUDIT REPORT                  ")
print("=====================================================================")

# -------------------------------------------------------------------
# 1. Candle Datasets (31 MT5 Assets)
# -------------------------------------------------------------------
candle_files = sorted(list(CANDLE_DIR.glob("*.parquet")))
print(f"\n1. 15-MINUTE CANDLE DATASETS ({len(candle_files)} ASSETS)")
print("---------------------------------------------------------------------")
total_candles = 0
failed_candles = []

for f in candle_files:
    sym = f.stem
    df = pd.read_parquet(f)
    n = len(df)
    total_candles += n
    
    null_cols = df.isnull().sum()[df.isnull().sum() > 0]
    monotonic = df["open_time_ms"].is_monotonic_increasing
    ohlc_valid = ((df["high"] >= df["open"]) & 
                  (df["high"] >= df["close"]) & 
                  (df["high"] >= df["low"]) & 
                  (df["low"] <= df["open"]) & 
                  (df["low"] <= df["close"])).all()
    vol_math = (abs((df["buy_volume"] + df["sell_volume"]) - df["total_volume"]) < 1e-4).all()
    delta_math = (abs((df["buy_volume"] - df["sell_volume"]) - df["delta"]) < 1e-4).all()
    trades_math = ((df["buy_trades"] + df["sell_trades"]) == df["total_trades"]).all()
    non_neg = ((df["total_volume"] >= 0) & (df["total_trades"] >= 0) & (df["open"] > 0)).all()
    
    ok = monotonic and ohlc_valid and vol_math and delta_math and trades_math and non_neg and len(null_cols) == 0
    if not ok:
        failed_candles.append(sym)
    else:
        t0 = df["open_time_utc"].iloc[0][:10]
        t1 = df["open_time_utc"].iloc[-1][:10]
        print(f"[{sym:5s}] OK: {n} bars | Window: {t0} -> {t1} | Total Vol: {df['total_volume'].sum():,.0f} | Nulls: 0")

print(f"Candles Audited: {total_candles:,} | Failures: {len(failed_candles)}")

# -------------------------------------------------------------------
# 2. Footprint Signals Dataset
# -------------------------------------------------------------------
print("\n2. FOOTPRINT SIGNALS DATASET")
print("---------------------------------------------------------------------")
if SIGNALS_PATH.exists():
    sig_df = pd.read_parquet(SIGNALS_PATH)
    print(f"Total Signals: {len(sig_df):,} | Unique Assets: {sig_df['symbol'].nunique()} | Nulls: {sig_df.isnull().sum().sum()}")
    print(f"By Bar Type: {sig_df['bar_type'].value_counts().to_dict()}")
    print(f"By Side: {sig_df['side'].value_counts().to_dict()}")
else:
    print("Signals dataset not found.")

# -------------------------------------------------------------------
# 3. Liquidation Snapshots Dataset
# -------------------------------------------------------------------
print("\n3. LIQUIDATION MACRO SNAPSHOTS")
print("---------------------------------------------------------------------")
if SNAPSHOTS_PATH.exists():
    snap_df = pd.read_parquet(SNAPSHOTS_PATH)
    print(f"Total Snapshots: {len(snap_df)} | Symbols: {list(snap_df['symbol'].unique())} | Exchanges: {list(snap_df['exchange'].unique())}")
    last_snap = snap_df.iloc[-1]
    print(f"Last Price: {last_snap['last_price']:,.1f}")
    print(f"Nearest Short Magnet: {last_snap['short_magnet_zone']} ({last_snap['short_magnet_dist_pct']:+.2f}%, Str: {last_snap['short_magnet_strength']})")
    print(f"Nearest Long Magnet:  {last_snap['long_magnet_zone']} ({last_snap['long_magnet_dist_pct']:+.2f}%, Str: {last_snap['long_magnet_strength']})")
    print(f"CLLD: {last_snap['clld_current']:+,.0f} ({last_snap['clld_sentiment']}, Turn: {last_snap['clld_turn_time']})")
    print(f"Near-Field Bias: {last_snap['near_field_bias']} (Longs: {last_snap['near_field_long_pct']}%, Shorts: {last_snap['near_field_short_pct']}%)")
else:
    print("Liquidation Snapshots dataset not found.")

# -------------------------------------------------------------------
# 4. Liquidation Discrete Strike Levels Dataset
# -------------------------------------------------------------------
print("\n4. LIQUIDATION DISCRETE STRIKE LEVELS")
print("---------------------------------------------------------------------")
if LEVELS_PATH.exists():
    lvl_df = pd.read_parquet(LEVELS_PATH)
    print(f"Total Discrete Levels: {len(lvl_df):,} | Active: {lvl_df['is_active'].sum():,} | Swept: {(~lvl_df['is_active']).sum():,}")
    print(f"Side Split: {lvl_df['side'].value_counts().to_dict()}")
    print(f"Leverage Tiers: {lvl_df['leverage_tier'].value_counts().to_dict()}")
    print(f"Price Range: ${lvl_df['price'].min():,.1f} -> ${lvl_df['price'].max():,.1f}")
    print(f"Distance Range: {lvl_df['distance_pct'].min():+.2f}% -> {lvl_df['distance_pct'].max():+.2f}%")
else:
    print("Liquidation Levels dataset not found.")

print("\n=====================================================================")
print("                   AUDIT COMPLETED SUCCESSFULLY                      ")
print("=====================================================================")
