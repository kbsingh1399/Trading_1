"""
========================================================================================
Candle Data IST Standardization & Footprint Annotation Backfill
========================================================================================
Ensures all parquet files in Database/Candle_Data/ conform to:
1. Primary IST (Indian Standard Time, UTC+5:30) time mapping:
   - open_time_ist, close_time_ist, time_ist, candle_ist, open_time_ist_ms, close_time_ist_ms
2. Dual UTC compatibility:
   - open_time_ms, close_time_ms, open_time_utc, close_time_utc
3. Footprint order flow indicators:
   - delta, delta_pct, cumulative_delta, cvd, poc_price
4. Embedded footprint signal flags:
   - has_signal, signal_type, signal_side, signal_desc
========================================================================================
"""

import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import atomic_parquet, writer_lock
from Terminal.universal_footprint_engine import UniversalFootprintEngine, enrich_candle_df_with_ist

CANDLE_DIR = ROOT_DIR / "Database" / "Candle_Data"
engine = UniversalFootprintEngine()

FOREX_PAIRS = {"EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"}
METALS = {"XAUUSD", "XAGUSD"}
INDICES = {"SPX500", "NDX100", "DJI30"}


def process_all_candle_parquets():
    if not CANDLE_DIR.exists():
        return

    files = sorted(list(CANDLE_DIR.glob("*.parquet")))
    print(f"Standardizing {len(files)} parquet files in {CANDLE_DIR} with primary IST time mapping...")

    for f in files:
        if f.name.endswith(".tmp.parquet"):
            continue
        sym = f.stem.split(".")[0]
        try:
            with writer_lock(f):
                df = pd.read_parquet(f)
                if df.empty or len(df) < 5:
                    continue

                if sym in FOREX_PAIRS:
                    ac = "Forex"
                    window = 300
                elif sym in METALS:
                    ac = "Metals"
                    window = 300
                elif sym in INDICES:
                    ac = "Indices"
                    window = 300
                else:
                    ac = "Crypto"
                    window = 300  # Enforce 300 rolling candle window

                # Run Universal Engine to enrich with delta, cvd, poc, signals, and IST time mapping
                enriched_df, _ = engine.analyze_bars(
                    bars=df,
                    symbol=sym,
                    asset_class=ac,
                    rolling_window=window,
                    inspect_last_n_bars=len(df)
                )

                if not enriched_df.empty:
                    atomic_parquet(enriched_df, f)
                    print(f"  ✅ {sym:<8} [{ac:<6}]: {len(enriched_df)} bars standardized with IST timing (Latest: {enriched_df['time_ist'].iloc[-1]})")
        except Exception as e:
            print(f"  ❌ Error processing {f.name}: {e}")


if __name__ == "__main__":
    process_all_candle_parquets()
