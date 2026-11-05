"""
========================================================================================
STANDALONE CANDLE & SIGNAL DUMPER (TIME, RANGE & VOLUME BARS)
========================================================================================
Location: C:\\Users\\SIGMA\\Documents\\_Trading_1\\dump_candles_and_signals.py

Fetches 4-Exchange aggregated live order flow (Binance, OKX, Bybit, Hyperliquid),
synthesizes 15m Time bars, Range bars, and Volume bars, extracts CoinGlass-calibrated
footprint signals (Absorption, Exhaustion, Divergence), embeds signals directly
into each candle row, and atomically dumps them to standardized Parquet files.

Usage:
    python dump_candles_and_signals.py                 # Dump BTC (Time, Range & Volume)
    python dump_candles_and_signals.py --symbol ETH    # Dump ETH
    python dump_candles_and_signals.py --top10         # Dump top 10 liquid assets
    python dump_candles_and_signals.py --symbol SOL --inspect # Dump & print full candle JSON
    python dump_candles_and_signals.py --all           # Dump all 32 supported crypto assets
========================================================================================
"""

import sys
import os
import json
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    COINGLASS_BAR_CONFIGS,
    synthesize_range_bars,
    synthesize_volume_bars,
    format_vol_display,
    utc_ms_to_ist_str
)
from Terminal.aggregated_futures_feed import AggregatedFuturesFeed
from Terminal.runtime_io import atomic_parquet, writer_lock

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_DIR = DB_DIR / "Signals"
CANDLE_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_PARQUET = SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet"
TOP10_ASSETS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOGE", "NEAR"]


def save_candle_parquet_with_signals(df: pd.DataFrame, file_path: Path):
    """
    Atomically saves a candle DataFrame to Parquet with a strict PyArrow List(Struct) signals schema.
    """
    if df is None or df.empty:
        return
    df = df.copy().reset_index(drop=True)
    if "signals" in df.columns:
        signal_struct_type = pa.struct([
            ('sig_code', pa.string()),
            ('sig_name', pa.string()),
            ('side', pa.string()),
            ('price', pa.float64()),
            ('title', pa.string()),
            ('description', pa.string())
        ])
        sig_list = [
            [
                {
                    "sig_code": str(s.get("sig_code", "")),
                    "sig_name": str(s.get("sig_name", "")),
                    "side": str(s.get("side", "")),
                    "price": float(s.get("price", 0.0)),
                    "title": str(s.get("title", "")),
                    "description": str(s.get("description", ""))
                }
                for s in (item if isinstance(item, list) else [])
            ]
            for item in df["signals"]
        ]
        base_table = pa.Table.from_pandas(df.drop(columns=["signals"]))
        signals_array = pa.array(sig_list, type=pa.list_(signal_struct_type))
        table_to_save = base_table.append_column("signals", signals_array)
        atomic_parquet(table_to_save, file_path)
    else:
        atomic_parquet(df, file_path)


def dump_asset_bars_and_signals(
    symbol: str,
    feed: AggregatedFuturesFeed,
    engine: UniversalFootprintEngine,
    inspect: bool = False
) -> Dict[str, Any]:
    """
    Dumps Time (15m), Range, and Volume candles for a single asset with embedded signals.
    """
    sym = symbol.upper().replace("USDT", "").replace("USD", "").strip()
    print(f"\n{'='*75}")
    print(f"PROCESSING ORDER FLOW & FOOTPRINT FOR: {sym}")
    print(f"{'='*75}")

    out_summary = {"symbol": sym, "bars": {}}

    # 1. 15m Aggregated Time Bars
    print(f"[{sym}] Fetching 4-Exchange Aggregated 15m Tape (Binance, OKX, Bybit, Hyperliquid)...")
    bars_15m = feed.fetch_aggregated_bars(sym, interval="15m", limit=300)
    if bars_15m and len(bars_15m) >= 5:
        df_time, sigs_time = engine.analyze_bars(
            bars=bars_15m,
            symbol=sym,
            asset_class="Crypto",
            mt5_symbol=f"{sym}USD",
            rolling_window=len(bars_15m),
            bar_type="time",
            bar_label="15m"
        )
        time_file = CANDLE_DIR / f"{sym}.parquet"
        save_candle_parquet_with_signals(df_time, time_file)
        sigs_in_df = len(df_time[df_time["has_signal"] == True]) if "has_signal" in df_time.columns else len(sigs_time)
        print(f"  ✓ [Time 15m] Dumped {len(df_time)} bars -> {time_file.name} ({time_file.stat().st_size/1024:.1f} KB) | Signals: {sigs_in_df} candles")
        out_summary["bars"]["time"] = {
            "file": str(time_file),
            "bars_count": len(df_time),
            "signals_count": len(sigs_time),
            "candles_with_signals": sigs_in_df,
            "df": df_time
        }
    else:
        print(f"  ✗ [Time 15m] Failed to fetch sufficient 15m bars ({len(bars_15m) if bars_15m else 0}).")

    # 2. Fetch 1m sub-bars for Range and Volume synthesis
    print(f"[{sym}] Fetching fine-grained 1m execution tape for Range & Volume synthesis...")
    sub_dict = feed.fetch_binance(sym, interval="1m", limit=300)
    sub_bars = []
    if sub_dict:
        for k in sorted(sub_dict.keys()):
            bar = dict(sub_dict[k])
            bar["open_time_ms"] = k
            sub_bars.append(bar)
    if not sub_bars or len(sub_bars) < 10:
        print(f"  ! Warning: 1m sub-bars insufficient ({len(sub_bars) if sub_bars else 0}). Skipping Range/Volume.")
        return out_summary

    cfg = COINGLASS_BAR_CONFIGS.get(sym, {})
    range_presets = cfg.get("range", [])
    vol_presets = cfg.get("vol", [])

    if not range_presets:
        last_p = float(sub_bars[-1].get("close", 1.0))
        range_presets = [{"label": f"Range {round(last_p * 0.005, 4):g}", "size": round(last_p * 0.005, 4)}]
    if not vol_presets:
        avg_sub_v = float(np.mean([b.get("total_volume", 0.0) for b in sub_bars]))
        vol_presets = [{"label": f"Volume {format_vol_display(avg_sub_v * 10)} {sym}", "threshold": avg_sub_v * 10}]

    # 3. Range Bars Synthesis
    r_cfg = range_presets[0]
    range_bars = synthesize_range_bars(sub_bars, range_size=r_cfg["size"], symbol=sym, bar_label=r_cfg["label"])
    if len(range_bars) >= 4:
        df_range, sigs_range = engine.analyze_bars(
            bars=range_bars,
            symbol=sym,
            asset_class="Crypto",
            mt5_symbol=f"{sym}USD",
            rolling_window=len(range_bars),
            bar_type="range",
            bar_label=r_cfg["label"]
        )
        range_file = CANDLE_DIR / f"{sym}_range.parquet"
        save_candle_parquet_with_signals(df_range, range_file)
        sigs_in_df_r = len(df_range[df_range["has_signal"] == True]) if "has_signal" in df_range.columns else len(sigs_range)
        print(f"  ✓ [Range Bar] Synthesized {len(df_range)} bars ({r_cfg['label']}) -> {range_file.name} ({range_file.stat().st_size/1024:.1f} KB) | Signals: {sigs_in_df_r} candles")
        out_summary["bars"]["range"] = {
            "file": str(range_file),
            "label": r_cfg["label"],
            "bars_count": len(df_range),
            "signals_count": len(sigs_range),
            "candles_with_signals": sigs_in_df_r,
            "df": df_range
        }
    else:
        print(f"  ! [Range Bar] Insufficient range bars synthesized ({len(range_bars)} < 4).")

    # 4. Volume Bars Synthesis
    v_cfg = vol_presets[0]
    vol_bars = synthesize_volume_bars(sub_bars, vol_threshold=v_cfg["threshold"], symbol=sym, bar_label=v_cfg["label"])
    if len(vol_bars) >= 4:
        df_vol, sigs_vol = engine.analyze_bars(
            bars=vol_bars,
            symbol=sym,
            asset_class="Crypto",
            mt5_symbol=f"{sym}USD",
            rolling_window=len(vol_bars),
            bar_type="vol",
            bar_label=v_cfg["label"]
        )
        vol_file = CANDLE_DIR / f"{sym}_volume.parquet"
        save_candle_parquet_with_signals(df_vol, vol_file)
        sigs_in_df_v = len(df_vol[df_vol["has_signal"] == True]) if "has_signal" in df_vol.columns else len(sigs_vol)
        print(f"  ✓ [Volume Bar] Synthesized {len(df_vol)} bars ({v_cfg['label']}) -> {vol_file.name} ({vol_file.stat().st_size/1024:.1f} KB) | Signals: {sigs_in_df_v} candles")
        out_summary["bars"]["volume"] = {
            "file": str(vol_file),
            "label": v_cfg["label"],
            "bars_count": len(df_vol),
            "signals_count": len(sigs_vol),
            "candles_with_signals": sigs_in_df_v,
            "df": df_vol
        }
    else:
        print(f"  ! [Volume Bar] Insufficient volume bars synthesized ({len(vol_bars)} < 4).")

    # 5. Inspection detail if requested
    if inspect:
        print(f"\n--- DETAILED INSPECTION FOR {sym} ---")
        for btype, binfo in out_summary["bars"].items():
            df_curr = binfo.get("df")
            if df_curr is not None and not df_curr.empty:
                sig_rows = df_curr[df_curr["has_signal"] == True]
                print(f"\n[{btype.upper()} BARS] Total: {len(df_curr)} | With Signals: {len(sig_rows)}")
                if not sig_rows.empty:
                    last_sig_candle = sig_rows.iloc[-1]
                    display_sample = {
                        "candle_ist": last_sig_candle.get("candle_ist"),
                        "close": float(last_sig_candle.get("close", 0.0)),
                        "total_volume": round(float(last_sig_candle.get("total_volume", 0.0)), 2),
                        "delta": round(float(last_sig_candle.get("delta", 0.0)), 2),
                        "poc_price": round(float(last_sig_candle.get("poc_price", 0.0)), 4),
                        "bar_type": str(last_sig_candle.get("bar_type")),
                        "bar_label": str(last_sig_candle.get("bar_label")),
                        "signal_type": str(last_sig_candle.get("signal_type")),
                        "signal_desc": str(last_sig_candle.get("signal_desc")),
                        "signals": last_sig_candle.get("signals")
                    }
                    print(json.dumps(display_sample, indent=2, default=str))

    return out_summary


def main():
    parser = argparse.ArgumentParser(description="Standalone Pure API Candle & Signal Dumper (Time, Range & Volume)")
    parser.add_argument("--symbol", type=str, default="BTC", help="Symbol to dump (e.g. BTC, ETH, SOL, AVAX; default: BTC)")
    parser.add_argument("--top10", action="store_true", help="Dump top 10 crypto futures assets")
    parser.add_argument("--all", action="store_true", help="Dump all 32 supported crypto assets")
    parser.add_argument("--inspect", action="store_true", help="Print full candle and embedded signal JSON details")
    args = parser.parse_args()

    feed = AggregatedFuturesFeed()
    engine = UniversalFootprintEngine()

    symbols_to_process = []
    if args.all:
        from Terminal.aggregated_futures_feed import VENUE_SYMBOL_MAP
        symbols_to_process = sorted(list(VENUE_SYMBOL_MAP.keys()))
    elif args.top10:
        symbols_to_process = TOP10_ASSETS
    else:
        symbols_to_process = [args.symbol]

    print("=" * 80)
    print("STANDALONE CANDLE & SIGNAL DUMPER (TIME, RANGE & VOLUME)")
    print("=" * 80)
    print(f"Target Assets ({len(symbols_to_process)}): {', '.join(symbols_to_process)}")
    print(f"Output Directory: {CANDLE_DIR}")

    t0 = time.time()
    results = []
    for sym in symbols_to_process:
        try:
            res = dump_asset_bars_and_signals(sym, feed, engine, inspect=args.inspect)
            results.append(res)
        except Exception as e:
            print(f"Error processing {sym}: {e}")

    elapsed = time.time() - t0
    print("\n" + "=" * 80)
    print(f"DUMP COMPLETE: Processed {len(results)} asset(s) in {elapsed:.2f}s")
    print(f"Parquet files successfully persisted with embedded signals to {CANDLE_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()
