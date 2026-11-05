import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import polars as pl
from Terminal.universal_footprint_engine import UniversalFootprintEngine
from Terminal.unified_api_footprint_engine import UnifiedAPIFootprintEngine

CANDLE_DIR = Path("Database/Candle_Data")
engine = UniversalFootprintEngine()
api_engine = UnifiedAPIFootprintEngine.__new__(UnifiedAPIFootprintEngine)
api_engine.rolling_window = 300

files = list(CANDLE_DIR.glob("*.parquet"))
print(f"Enriching {len(files)} candle parquet files...")

for f in files:
    sym = f.stem
    try:
        df = pd.read_parquet(f)
        if len(df) == 0:
            continue
        asset_class = str(df.get("asset_class", ["Crypto"])[0]) if "asset_class" in df.columns else "Crypto"
        rolling_df, sigs = engine.analyze_bars(
            df,
            symbol=sym,
            asset_class=asset_class,
            rolling_window=len(df),
            inspect_last_n_bars=len(df),
            bar_type="time",
            bar_label="15m"
        )
        api_engine.save_rolling_parquet(rolling_df, sym)
    except Exception as e:
        print(f"Error enriching {f.name}: {e}")

print("All candle parquet files successfully enriched with per-bar metrics and signals arrays!")
