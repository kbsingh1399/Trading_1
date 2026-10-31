import os
import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"
LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"
SIGNALS_PATH = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PATH = LIQ_DIR / "liquidation_snapshots.parquet"

print("=====================================================================")
print("             COINGLASS MASTER DATASET AUDIT REPORT                  ")
print("=====================================================================")

# -------------------------------------------------------------------
# 1. Candle Datasets (Mutual Intersection Assets)
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
        print(f"[{sym:6s}] OK: {n} bars | Window: {t0} -> {t1} | Total Vol: {df['total_volume'].sum():,.0f} | Nulls: 0")

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
    print(f"Total Snapshots: {len(snap_df)} | Symbols: {list(snap_df['symbol'].unique())} | Lookbacks: {list(snap_df['lookback_tf'].unique())}")
    last_snap = snap_df.iloc[-1]
    print(f"Latest Snapshot ({last_snap['symbol']} - {last_snap['lookback_tf']}):")
    print(f"  Last Price: {last_snap['last_price']:,.1f}")
    print(f"  Nearest Short Magnet: {last_snap['short_magnet_zone']} ({last_snap['short_magnet_dist_pct']:+.2f}%, Str: {last_snap['short_magnet_strength']})")
    print(f"  Nearest Long Magnet:  {last_snap['long_magnet_zone']} ({last_snap['long_magnet_dist_pct']:+.2f}%, Str: {last_snap['long_magnet_strength']})")
    print(f"  CLLD: {last_snap['clld_current']} ({last_snap['clld_sentiment']}, Turn: {last_snap['clld_turn_time']})")
    print(f"  Near-Field Bias: {last_snap['near_field_bias']} (Longs: {last_snap['near_field_long_pct']}%, Shorts: {last_snap['near_field_short_pct']}%)")
else:
    print("Liquidation Snapshots dataset not found.")

# -------------------------------------------------------------------
# 4. Asset-Partitioned Key Liquidation Levels (Ranked Strike Barriers)
# -------------------------------------------------------------------
print("\n4. KEY LIQUIDATION LEVELS (ASSET PARTITIONED)")
print("---------------------------------------------------------------------")
key_level_files = sorted(list(KEY_LEVELS_DIR.glob("*.parquet")))
if key_level_files:
    total_key_levels = 0
    for kf in key_level_files:
        kdf = pd.read_parquet(kf)
        total_key_levels += len(kdf)
        exs = kdf['exchange'].value_counts().to_dict() if 'exchange' in kdf.columns else {}
        sides = kdf['side'].value_counts().to_dict() if 'side' in kdf.columns else {}
        levs = kdf['leverage_tier'].value_counts().to_dict() if 'leverage_tier' in kdf.columns else {}
        tfs = kdf['lookback_tf'].value_counts().to_dict() if 'lookback_tf' in kdf.columns else {}
        print(f"[{kf.stem:16s}] {len(kdf):,} barriers | Ex: {exs} | Sides: {sides} | Lev: {levs} | TFs: {tfs}")
    print(f"\nTotal Key Strike Barriers across all assets: {total_key_levels:,}")
else:
    print("No asset-partitioned key level files found in Database/Liquidation_Data/Key_Levels/")

# -------------------------------------------------------------------
# 5. Asset-Partitioned Magnet Zones (Top 3 Bands per Side)
# -------------------------------------------------------------------
print("\n5. MAGNET ZONES (ASSET PARTITIONED)")
print("---------------------------------------------------------------------")
magnet_files = sorted(list(MAGNET_ZONES_DIR.glob("*.parquet")))
if magnet_files:
    total_magnet_zones = 0
    for mf in magnet_files:
        mdf = pd.read_parquet(mf)
        total_magnet_zones += len(mdf)
        sides = mdf['side'].value_counts().to_dict() if 'side' in mdf.columns else {}
        exs = mdf['exchange'].value_counts().to_dict() if 'exchange' in mdf.columns else {}
        tfs = mdf['lookback_tf'].value_counts().to_dict() if 'lookback_tf' in mdf.columns else {}
        print(f"[{mf.stem:16s}] {len(mdf):,} bands | Ex: {exs} | Sides: {sides} | TFs: {tfs}")
    print(f"\nTotal Magnet Zones across all assets: {total_magnet_zones:,}")
else:
    print("No asset-partitioned magnet zone files found in Database/Liquidation_Data/Magnet_Zones/")

# -------------------------------------------------------------------
# 6. Asset-Partitioned Leverage Tiers & Near-Field Balance
# -------------------------------------------------------------------
print("\n6. LEVERAGE TIERS & NEAR-FIELD BALANCE")
print("---------------------------------------------------------------------")
lev_files = sorted(list(LEVERAGE_TIERS_DIR.glob("*.parquet")))
near_files = sorted(list(NEAR_FIELD_DIR.glob("*.parquet")))
print(f"Leverage Tier files: {len(lev_files)} | Near-Field Balance files: {len(near_files)}")

print("\n=====================================================================")
print("                   AUDIT COMPLETED SUCCESSFULLY                      ")
print("=====================================================================")
