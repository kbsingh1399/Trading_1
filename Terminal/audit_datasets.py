import os
import pandas as pd
from pathlib import Path

candle_dir = Path(r"c:\Users\SIGMA\Documents\_Trading_1\Database\Candle_Data")
signals_path = Path(r"c:\Users\SIGMA\Documents\_Trading_1\Database\coinglass_aggregated_futures_signals.parquet")

files = sorted(list(candle_dir.glob("*.parquet")))
print(f"=== CANDLE DATASET SANITY AUDIT ({len(files)} ASSETS) ===")

total_candles = 0
failed_assets = []

for f in files:
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
    
    all_ok = monotonic and ohlc_valid and vol_math and delta_math and trades_math and non_neg and len(null_cols) == 0
    if not all_ok:
        failed_assets.append((sym, {
            "nulls": list(null_cols.index),
            "monotonic": monotonic,
            "ohlc": ohlc_valid,
            "vol": vol_math,
            "delta": delta_math,
            "trades": trades_math,
            "non_neg": non_neg
        }))
    else:
        t0 = df["open_time_utc"].iloc[0][:10]
        t1 = df["open_time_utc"].iloc[-1][:10]
        tot_vol = df["total_volume"].sum()
        print(f"[{sym:5s}] OK | Bars: {n} | Range: {t0} -> {t1} | Vol: {tot_vol:,.0f} | Nulls: 0")

print(f"\nTotal candles audited: {total_candles:,}")
print(f"Sanity failures count: {len(failed_assets)}")
if failed_assets:
    print("Failed details:", failed_assets)

print("\n=== SIGNALS DATASET SANITY AUDIT ===")
sig_df = pd.read_parquet(signals_path)
print(f"Signals total rows: {len(sig_df):,}")
print(f"Unique assets: {sig_df['symbol'].nunique()}")
print(f"Null count: {sig_df.isnull().sum().sum()}")
print(f"By Bar Type: {sig_df['bar_type'].value_counts().to_dict()}")
print(f"By Side: {sig_df['side'].value_counts().to_dict()}")
