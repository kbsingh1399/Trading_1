"""
Data Dumping Perfection & Integrity Auditor
===========================================
Validates the structural health, row counts, and freshness of all
dumped Parquet files across the 4 data pipelines:
1. CoinGlass Footprint Signals
2. Whale Orders (Open Walls, History Blotters, Snapshots)
3. Liquidation Maps (Magnet Zones, Key Levels, Near Field Balance)
4. 15m Footprint Candle Data
"""

import os
import sys
import time
import glob
from pathlib import Path
from datetime import datetime, timezone
import polars as pl

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"

def audit_all_dumps() -> dict:
    now = time.time()
    results = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PERFECT",
        "pipelines": {}
    }

    # 1. Signals Stream
    sig_p = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
    if sig_p.exists():
        df_sig = pl.read_parquet(sig_p)
        age = int(now - os.path.getmtime(sig_p))
        results["pipelines"]["footprint_signals"] = {
            "status": "HEALTHY" if age < 900 else "STALE",
            "rows": len(df_sig),
            "age_seconds": age,
            "size_kb": round(os.path.getsize(sig_p) / 1024, 1)
        }
    else:
        results["pipelines"]["footprint_signals"] = {"status": "MISSING"}
        results["status"] = "DEGRADED"

    # 2. Whale Orders Stream
    whale_dir = DB_DIR / "Whale_Orders"
    whale_open = list((whale_dir / "Open_Orders").glob("*.parquet"))
    whale_hist = list((whale_dir / "History_Orders").glob("*.parquet"))
    whale_snap = whale_dir / "whale_market_snapshots.parquet"

    snap_age = int(now - os.path.getmtime(whale_snap)) if whale_snap.exists() else 99999
    snap_rows = len(pl.read_parquet(whale_snap)) if whale_snap.exists() else 0

    results["pipelines"]["whale_orders"] = {
        "status": "HEALTHY" if snap_age < 600 and len(whale_open) >= 12 else "DEGRADED",
        "open_order_files": len(whale_open),
        "history_order_files": len(whale_hist),
        "snapshots_rows": snap_rows,
        "snapshots_age_sec": snap_age
    }

    # 3. Liquidation Maps Stream
    liq_dir = DB_DIR / "Liquidation_Data"
    mag_files = list((liq_dir / "Magnet_Zones").glob("*.parquet"))
    kl_files = list((liq_dir / "Key_Levels").glob("*.parquet"))
    nf_files = list((liq_dir / "Near_Field_Balance").glob("*.parquet"))
    liq_snap = liq_dir / "liquidation_snapshots.parquet"

    results["pipelines"]["liquidation_data"] = {
        "status": "HEALTHY" if len(mag_files) > 0 and len(kl_files) > 0 else "DEGRADED",
        "magnet_zones_count": len(mag_files),
        "key_levels_count": len(kl_files),
        "near_field_count": len(nf_files),
        "snapshots_present": liq_snap.exists()
    }

    # 4. 15m Footprint Candles
    candle_files = list((DB_DIR / "Candle_Data").glob("*.parquet"))
    results["pipelines"]["candle_data"] = {
        "status": "HEALTHY" if len(candle_files) >= 10 else "DEGRADED",
        "candle_files_count": len(candle_files)
    }

    # Overall Status determination
    for pipe, data in results["pipelines"].items():
        if data.get("status") != "HEALTHY":
            results["status"] = "ATTENTION_REQUIRED"

    return results

if __name__ == "__main__":
    res = audit_all_dumps()
    print("=== DATA DUMPING INTEGRITY AUDIT ===")
    print(f"Overall Status: {res['status']}")
    for pipe, d in res["pipelines"].items():
        print(f"[{pipe:<20}] Status: {d.get('status'):<8} | Details: {d}")
