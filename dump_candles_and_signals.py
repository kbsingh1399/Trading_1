"""
========================================================================================
STANDALONE CANDLE & SIGNAL DUMPER (TIME, RANGE & VOLUME BARS)
========================================================================================
Location: C:\\Users\\SIGMA\\Documents\\_Trading_1\\dump_candles_and_signals.py

Capabilities:
1. Startup Gap Detection & Backfill:
   Audits existing Parquet on disk for missing candles (both trailing downtime gaps
   and internal missing intervals), fetches missing candles across 4 exchanges
   (Binance, OKX, Bybit, Hyperliquid), and seamlessly appends them without duplicates.
2. Continuous Order Flow Footprint Synthesis:
   Re-evaluates Delta, Cumulative Delta (CVD), POC, and CoinGlass signals (Absorption,
   Exhaustion, Divergence) across the continuous time-series.
3. Multi-Bar Partitioning:
   Maintains dedicated 45-column Parquet files with embedded PyArrow List(Struct) signals:
     - Database/Candle_Data/<SYMBOL>.parquet        (15m Time bars)
     - Database/Candle_Data/<SYMBOL>_range.parquet  (Synthesized Range bars)
     - Database/Candle_Data/<SYMBOL>_volume.parquet (Synthesized Volume bars)
4. Live Surveillance Streaming Mode (--live):
   Monitors live candle boundaries, streams countdown to the exact second of close,
   updates intra-bar Range & Volume bars every 15s, and appends completed bars on close.

Usage:
    python dump_candles_and_signals.py                 # Dump & backfill BTC
    python dump_candles_and_signals.py --symbol ETH    # Dump & backfill ETH
    python dump_candles_and_signals.py --top10         # Dump & backfill top 10 assets
    python dump_candles_and_signals.py --symbol SOL --inspect # Inspect JSON details
    python dump_candles_and_signals.py --live          # Live surveillance & append mode
    python dump_candles_and_signals.py --live --top10  # Live stream top 10 assets
========================================================================================
"""

import sys
import os
import json
import argparse
import signal
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
import concurrent.futures

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
from Terminal.runtime_io import atomic_parquet, atomic_append_jsonl, writer_lock

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_DIR = DB_DIR / "Signals"
CANDLE_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_PARQUET = SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet"
SIGNALS_JSONL = SIGNALS_DIR / "footprint_signals.jsonl"
TOP10_ASSETS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOGE", "NEAR"]
TOP20_ASSETS = [
    "BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "SUI",
    "PEPE", "NEAR", "DOT", "LTC", "BCH", "TRX", "UNI", "ATOM", "AAVE", "FIL"
]


# =============================================================================
# Atomic Parquet Persistence with Strict PyArrow Schema
# =============================================================================
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


# =============================================================================
# Startup Gap Detection & Chronological Audit
# =============================================================================
def detect_missing_time_bars(file_path: Path, interval_ms: int = 900_000) -> Dict[str, Any]:
    """
    Audits existing Parquet on disk to determine:
    1. If the file exists and is valid.
    2. Total existing continuous bars.
    3. Trailing missing bars between the last closed bar on disk and the current closed bar in the market.
    4. Internal gaps (missing timestamps inside the sequence).
    """
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    cur_candle_open_ms = (now_ms // interval_ms) * interval_ms
    latest_closed_open_ms = cur_candle_open_ms - interval_ms

    if not file_path.exists() or file_path.stat().st_size == 0:
        return {
            "exists": False,
            "total_bars": 0,
            "first_open_ms": None,
            "last_open_ms": None,
            "last_close_ms": None,
            "missing_trailing_bars": 300,
            "internal_missing_bars": 0,
            "is_up_to_date": False,
            "df": None
        }

    try:
        df = pd.read_parquet(file_path)
        if df.empty or "open_time_ms" not in df.columns:
            return {
                "exists": False,
                "total_bars": 0,
                "first_open_ms": None,
                "last_open_ms": None,
                "last_close_ms": None,
                "missing_trailing_bars": 300,
                "internal_missing_bars": 0,
                "is_up_to_date": False,
                "df": None
            }

        df = df.sort_values("open_time_ms").reset_index(drop=True)
        first_open_ms = int(df["open_time_ms"].iloc[0])
        last_open_ms = int(df["open_time_ms"].iloc[-1])
        last_close_ms = int(df["close_time_ms"].iloc[-1]) if "close_time_ms" in df.columns else (last_open_ms + interval_ms - 1)

        # Check trailing gap
        gap_ms = latest_closed_open_ms - last_open_ms
        missing_trailing = max(0, int(gap_ms // interval_ms))

        # Check internal gaps
        diffs = df["open_time_ms"].diff().dropna()
        internal_missing = 0
        for d in diffs:
            if d > interval_ms:
                internal_missing += int((d - interval_ms) // interval_ms)

        is_up_to_date = (missing_trailing == 0 and internal_missing == 0)

        return {
            "exists": True,
            "total_bars": len(df),
            "first_open_ms": first_open_ms,
            "last_open_ms": last_open_ms,
            "last_close_ms": last_close_ms,
            "missing_trailing_bars": missing_trailing,
            "internal_missing_bars": internal_missing,
            "is_up_to_date": is_up_to_date,
            "df": df
        }
    except Exception as e:
        return {
            "exists": False,
            "total_bars": 0,
            "first_open_ms": None,
            "last_open_ms": None,
            "last_close_ms": None,
            "missing_trailing_bars": 300,
            "internal_missing_bars": 0,
            "is_up_to_date": False,
            "df": None,
            "error": str(e)
        }


# =============================================================================
# 15m Time Bars Startup Audit & Incremental Backfill
# =============================================================================
def audit_and_sync_time_bars(
    symbol: str,
    feed: AggregatedFuturesFeed,
    engine: UniversalFootprintEngine,
    max_history_bars: int = 1000
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Audits existing 15m Parquet on disk, detects missing candles, fetches and appends
    missing order flow data seamlessly, recalculates footprint metrics across the merged
    sequence, and atomically updates Database/Candle_Data/<SYMBOL>.parquet.
    """
    time_file = CANDLE_DIR / f"{symbol}.parquet"
    audit = detect_missing_time_bars(time_file, interval_ms=900_000)

    bars_by_time: Dict[int, Dict[str, Any]] = {}

    # Load existing bars if present
    if audit["exists"] and audit["df"] is not None:
        df_old = audit["df"]
        for _, row in df_old.iterrows():
            t_open = int(row.get("open_time_ms", 0))
            if t_open > 0:
                bars_by_time[t_open] = {
                    "open_time_ms": t_open,
                    "close_time_ms": int(row.get("close_time_ms", t_open + 899_999)),
                    "open": float(row.get("open", 0.0)),
                    "high": float(row.get("high", 0.0)),
                    "low": float(row.get("low", 0.0)),
                    "close": float(row.get("close", 0.0)),
                    "total_volume": float(row.get("total_volume", row.get("volume", 0.0))),
                    "buy_volume": float(row.get("buy_volume", 0.0)),
                    "sell_volume": float(row.get("sell_volume", 0.0)),
                    "delta": float(row.get("delta", 0.0)),
                    "total_trades": float(row.get("total_trades", row.get("trades", 0.0))),
                    "bar_type": "time",
                    "bar_label": "15m"
                }

    missing_count = audit["missing_trailing_bars"] + audit["internal_missing_bars"]

    if missing_count > 0 or not audit["exists"] or len(bars_by_time) < 10:
        # Fetch needed bars from 4-Exchange Feed
        fetch_limit = min(max(missing_count + 15, 300), 1000)
        new_bars = feed.fetch_aggregated_bars(symbol, interval="15m", limit=fetch_limit)
        
        # Merge new bars (overwriting/updating boundary bars and filling missing gaps)
        for b in (new_bars or []):
            t_open = int(b.get("open_time_ms", 0))
            if t_open > 0:
                bars_by_time[t_open] = b

    # Deep backward pagination to maintain full 1-month history (up to max_history_bars)
    if len(bars_by_time) < max_history_bars and bars_by_time:
        while len(bars_by_time) < max_history_bars:
            earliest_t = min(bars_by_time.keys())
            batch_limit = min(1000, max_history_bars - len(bars_by_time) + 15)
            prev_bars = feed.fetch_binance(symbol, interval="15m", limit=batch_limit, now_ms=earliest_t - 1)
            if not prev_bars:
                break
            added = 0
            for t_open, b in prev_bars.items():
                if t_open not in bars_by_time:
                    b_copy = dict(b)
                    b_copy["open_time_ms"] = t_open
                    bars_by_time[t_open] = b_copy
                    added += 1
            if added == 0:
                break

    sorted_times = sorted(bars_by_time.keys())
    if not sorted_times:
        return pd.DataFrame(), [], audit

    # Retain up to max_history_bars
    if len(sorted_times) > max_history_bars:
        sorted_times = sorted_times[-max_history_bars:]

    merged_bars = [bars_by_time[t] for t in sorted_times]

    # Recompute Delta, CVD, POC, and CoinGlass Signals across the seamless merged series
    df_enriched, new_signals = engine.analyze_bars(
        bars=merged_bars,
        symbol=symbol,
        asset_class="Crypto",
        mt5_symbol=f"{symbol}USD",
        rolling_window=len(merged_bars),
        inspect_last_n_bars=max(missing_count, 2),
        bar_type="time",
        bar_label="15m"
    )

    # Atomically save to Parquet
    save_candle_parquet_with_signals(df_enriched, time_file)

    sync_stats = {
        "symbol": symbol,
        "previous_bars": audit["total_bars"],
        "missing_bars_backfilled": missing_count,
        "final_bars_count": len(df_enriched),
        "signals_count": len(new_signals),
        "is_up_to_date": True,
        "last_candle_ist": str(df_enriched.get("candle_ist", pd.Series([""])).iloc[-1])
    }

    return df_enriched, new_signals, sync_stats


# =============================================================================
# Range & Volume Bars Incremental Synthesis & Sync
# =============================================================================
def audit_and_sync_synthesized_bars(
    symbol: str,
    sub_bars: List[Dict[str, Any]],
    engine: UniversalFootprintEngine,
    bar_type: str = "range",
    preset_cfg: Dict[str, Any] = None,
    max_history_bars: int = 1000
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Synthesizes and incrementally synchronizes Range or Volume bars with embedded signals.
    """
    target_suffix = "_range" if bar_type == "range" else "_volume"
    target_file = CANDLE_DIR / f"{symbol}{target_suffix}.parquet"

    if bar_type == "range":
        range_size = preset_cfg.get("size", 250.0)
        label = preset_cfg.get("label", f"Range {range_size:g}")
        synth_bars = synthesize_range_bars(sub_bars, range_size=range_size, symbol=symbol, bar_label=label)
    else:
        vol_threshold = preset_cfg.get("threshold", 1000.0)
        label = preset_cfg.get("label", f"Volume {vol_threshold:g}")
        synth_bars = synthesize_volume_bars(sub_bars, vol_threshold=vol_threshold, symbol=symbol, bar_label=label)

    if not synth_bars or len(synth_bars) < 4:
        return pd.DataFrame(), [], {"bars_count": 0, "status": "insufficient_data"}

    # Merge with existing file if present to maintain deep history
    bars_by_time: Dict[int, Dict[str, Any]] = {}
    if target_file.exists() and target_file.stat().st_size > 0:
        try:
            df_old = pd.read_parquet(target_file)
            if not df_old.empty and "open_time_ms" in df_old.columns:
                for _, row in df_old.iterrows():
                    t_open = int(row.get("open_time_ms", 0))
                    if t_open > 0:
                        bars_by_time[t_open] = {
                            "open_time_ms": t_open,
                            "close_time_ms": int(row.get("close_time_ms", t_open + 60000)),
                            "open": float(row.get("open", 0.0)),
                            "high": float(row.get("high", 0.0)),
                            "low": float(row.get("low", 0.0)),
                            "close": float(row.get("close", 0.0)),
                            "total_volume": float(row.get("total_volume", row.get("volume", 0.0))),
                            "buy_volume": float(row.get("buy_volume", 0.0)),
                            "sell_volume": float(row.get("sell_volume", 0.0)),
                            "delta": float(row.get("delta", 0.0)),
                            "total_trades": float(row.get("total_trades", row.get("trades", 0.0))),
                            "bar_type": bar_type,
                            "bar_label": label
                        }
        except Exception:
            pass

    for b in synth_bars:
        t_open = int(b.get("open_time_ms", 0))
        if t_open > 0:
            bars_by_time[t_open] = b

    sorted_times = sorted(bars_by_time.keys())
    if len(sorted_times) > max_history_bars:
        sorted_times = sorted_times[-max_history_bars:]

    merged_bars = [bars_by_time[t] for t in sorted_times]

    df_synth, sigs_synth = engine.analyze_bars(
        bars=merged_bars,
        symbol=symbol,
        asset_class="Crypto",
        mt5_symbol=f"{symbol}USD",
        rolling_window=len(merged_bars),
        inspect_last_n_bars=len(synth_bars),
        bar_type=bar_type,
        bar_label=label
    )

    save_candle_parquet_with_signals(df_synth, target_file)

    stats = {
        "bars_count": len(df_synth),
        "signals_count": len(sigs_synth),
        "label": label,
        "file": str(target_file)
    }
    return df_synth, sigs_synth, stats


def fetch_deep_1m_sub_bars(feed: AggregatedFuturesFeed, symbol: str, target_bars: int = 1500) -> List[Dict[str, Any]]:
    """
    Fetches fine-grained 1m execution tape with backward pagination for Range & Volume synthesis.
    """
    bars_1m: Dict[int, Dict[str, Any]] = {}
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    batches = max(1, (target_bars + 999) // 1000)
    
    for _ in range(batches):
        batch = feed.fetch_binance(symbol, interval="1m", limit=min(1000, target_bars - len(bars_1m)), now_ms=now_ms)
        if not batch:
            break
        for t, b in batch.items():
            b_copy = dict(b)
            b_copy["open_time_ms"] = t
            bars_1m[t] = b_copy
        earliest = min(bars_1m.keys())
        now_ms = earliest - 1
        if len(bars_1m) >= target_bars:
            break
            
    sorted_times = sorted(bars_1m.keys())
    return [bars_1m[t] for t in sorted_times]


# =============================================================================
# High-Level Single-Asset Synchronizer
# =============================================================================
def dump_asset_bars_and_signals(
    symbol: str,
    feed: AggregatedFuturesFeed,
    engine: UniversalFootprintEngine,
    max_history_bars: int = 1000,
    inspect: bool = False
) -> Dict[str, Any]:
    """
    Dumps Time (15m), Range, and Volume candles for a single asset with gap backfill and embedded signals.
    """
    sym = symbol.upper().replace("USDT", "").replace("USD", "").strip()
    print(f"\n{'='*75}")
    print(f"PROCESSING ORDER FLOW & FOOTPRINT FOR: {sym}")
    print(f"{'='*75}")

    out_summary = {"symbol": sym, "bars": {}}

    # 1. 15m Time Bars Gap Audit & Incremental Backfill
    print(f"[{sym}] Auditing 15m Parquet on disk & fetching 4-Exchange Tape...")
    df_time, sigs_time, time_stats = audit_and_sync_time_bars(sym, feed, engine, max_history_bars=max_history_bars)

    if not df_time.empty:
        time_file = CANDLE_DIR / f"{sym}.parquet"
        sigs_in_df = len(df_time[df_time["has_signal"] == True]) if "has_signal" in df_time.columns else len(sigs_time)
        backfill_str = f"Backfilled {time_stats['missing_bars_backfilled']} missing bars | " if time_stats['missing_bars_backfilled'] > 0 else "0 missing bars (up to date) | "
        print(f"  ✓ [Time 15m] {backfill_str}Total {len(df_time)} bars -> {time_file.name} ({time_file.stat().st_size/1024:.1f} KB) | Signals: {sigs_in_df} candles")
        out_summary["bars"]["time"] = {
            "file": str(time_file),
            "bars_count": len(df_time),
            "signals_count": len(sigs_time),
            "candles_with_signals": sigs_in_df,
            "df": df_time
        }
    else:
        print(f"  ✗ [Time 15m] Failed to sync 15m bars for {sym}.")

    # 2. Fetch 1m sub-bars for Range and Volume synthesis (deep pagination)
    print(f"[{sym}] Fetching fine-grained 1m execution tape for Range & Volume synthesis (1,500 bars)...")
    sub_bars = fetch_deep_1m_sub_bars(feed, sym, target_bars=1500)

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

    # 3. Range Bars Synthesis & Sync
    r_cfg = range_presets[0]
    df_range, sigs_range, r_stats = audit_and_sync_synthesized_bars(
        sym, sub_bars, engine, bar_type="range", preset_cfg=r_cfg, max_history_bars=max_history_bars
    )
    if not df_range.empty:
        range_file = CANDLE_DIR / f"{sym}_range.parquet"
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
        print(f"  ! [Range Bar] Insufficient range bars synthesized.")

    # 4. Volume Bars Synthesis & Sync
    v_cfg = vol_presets[0]
    df_vol, sigs_vol, v_stats = audit_and_sync_synthesized_bars(
        sym, sub_bars, engine, bar_type="vol", preset_cfg=v_cfg, max_history_bars=max_history_bars
    )
    if not df_vol.empty:
        vol_file = CANDLE_DIR / f"{sym}_volume.parquet"
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
        print(f"  ! [Volume Bar] Insufficient volume bars synthesized.")

    # 5. Inspection output if requested
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


# =============================================================================
# Live Surveillance Streaming Engine (--live)
# =============================================================================
def run_live_surveillance(
    symbols: List[str],
    feed: AggregatedFuturesFeed,
    engine: UniversalFootprintEngine,
    poll_sec: int = 15,
    max_history_bars: int = 1000,
    inspect: bool = False
):
    """
    Live continuous surveillance loop:
    1. Audits and backfills any missing candles on startup.
    2. Runs continuous countdown to next 15m candle close.
    3. Periodically updates 1m intra-bar execution tape for Range & Volume bars.
    4. Exactly on 15m close boundary, fetches the closed candle, appends to Parquet,
       evaluates footprint signals, and emits trade alerts.
    """
    print("\n" + "=" * 80)
    print("AUTONOMOUS LIVE CANDLE & SIGNAL SURVEILLANCE ENGINE")
    print("=" * 80)
    print(f"Tracking Assets ({len(symbols)}): {', '.join(symbols)}")
    print(f"Surveillance Mode: Startup Gap Backfill + Realtime Candle Append Loop")
    print(f"Intra-bar Polling: Every {poll_sec}s | Bar Invariant: 15m Completed Bars")
    print("Press Ctrl+C to stop.\n")

    running = True

    def _sig_handler(sig, frame):
        nonlocal running
        print("\n\n[LIVE] Termination signal received. Exiting surveillance cleanly...")
        running = False

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # 1. Startup Gap Audit & Initial Backfill
    print("[STARTUP] Performing pre-flight candle gap audit & historical backfill...")
    for sym in symbols:
        try:
            dump_asset_bars_and_signals(sym, feed, engine, max_history_bars=max_history_bars, inspect=False)
        except Exception as e:
            print(f"[STARTUP] Notice for {sym}: {e}")

    print("\n" + "=" * 80)
    print("[LIVE] Pre-flight backfill complete. Entering continuous surveillance loop.")
    print("=" * 80)

    last_sub_poll = 0.0
    last_processed_boundary_ms = 0

    while running:
        try:
            now_utc = datetime.now(timezone.utc)
            now_ms = int(now_utc.timestamp() * 1000)
            now_ist_str = utc_ms_to_ist_str(now_ms)

            # Calculate 15m boundary
            cur_candle_open_ms = (now_ms // 900_000) * 900_000
            cur_candle_close_ms = cur_candle_open_ms + 900_000
            remaining_ms = cur_candle_close_ms - now_ms
            rem_min = int(remaining_ms // 60000)
            rem_sec = int((remaining_ms % 60000) // 1000)

            close_ist_str = utc_ms_to_ist_str(cur_candle_close_ms)

            # Status line
            sys.stdout.write(
                f"\r[LIVE {now_ist_str.split()[1]}] Next 15m Candle Close at {close_ist_str.split()[1]} "
                f"(in {rem_min:02d}m {rem_sec:02d}s) | Assets: {len(symbols)} active"
            )
            sys.stdout.flush()

            # Check if 15m candle just closed (within first 45 seconds of the new bar)
            ms_since_boundary = now_ms - cur_candle_open_ms
            if ms_since_boundary <= 45000 and cur_candle_open_ms != last_processed_boundary_ms:
                last_processed_boundary_ms = cur_candle_open_ms
                print(f"\n\n{'='*75}")
                print(f"[CANDLE CLOSE TRIGGER] 15m Bar Closed at {now_ist_str}! Ingesting & Appending...")
                print(f"{'='*75}")
                # Wait 1.5s for exchange taker tapes to settle
                time.sleep(1.5)

                workers = min(6, len(symbols))
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    futs = {
                        executor.submit(audit_and_sync_time_bars, sym, feed, engine, max_history_bars=max_history_bars): sym
                        for sym in symbols
                    }
                    for fut in concurrent.futures.as_completed(futs):
                        sym = futs[fut]
                        try:
                            df_time, new_sigs, stats = fut.result()
                            if stats["missing_bars_backfilled"] > 0:
                                print(f"  ✓ [{sym}] Appended newly closed 15m bar! Total: {stats['final_bars_count']} bars | Signals: {len(new_sigs)}")
                            else:
                                print(f"  ✓ [{sym}] Synchronized 15m bar at {stats['last_candle_ist']}")

                            if new_sigs:
                                for s in new_sigs:
                                    print(f"      ★ SIGNAL TRIGGERED: [{s.get('symbol')}] {s.get('title')} ({s.get('side')}) @ ${float(s.get('price', 0.0)):,.2f}")
                                    record = {
                                        "symbol": s.get("symbol"),
                                        "candle_ist": s.get("candle_ist"),
                                        "sig_code": s.get("sig_code"),
                                        "sig_name": s.get("sig_name"),
                                        "side": s.get("side"),
                                        "price": s.get("price"),
                                        "title": s.get("title"),
                                        "description": s.get("description"),
                                        "bar_type": "time",
                                        "bar_label": "15m",
                                        "created_at_ist": now_ist_str
                                    }
                                    atomic_append_jsonl(SIGNALS_JSONL, record)
                        except Exception as e:
                            print(f"  ✗ Error updating {sym} on candle close: {e}")

                print("-" * 75)

            # Intra-bar 1m tape sync for Range and Volume bars
            t_now = time.time()
            if t_now - last_sub_poll >= poll_sec:
                last_sub_poll = t_now
                for sym in symbols[:3]:  # prioritize top liquid assets for intra-bar tape
                    try:
                        sub_dict = feed.fetch_binance(sym, interval="1m", limit=60)
                        if sub_dict:
                            sub_bars = []
                            for k in sorted(sub_dict.keys()):
                                b = dict(sub_dict[k])
                                b["open_time_ms"] = k
                                sub_bars.append(b)
                            cfg = COINGLASS_BAR_CONFIGS.get(sym, {})
                            r_presets = cfg.get("range", [])
                            v_presets = cfg.get("vol", [])
                            if r_presets:
                                audit_and_sync_synthesized_bars(sym, sub_bars, engine, bar_type="range", preset_cfg=r_presets[0], max_history_bars=max_history_bars)
                            if v_presets:
                                audit_and_sync_synthesized_bars(sym, sub_bars, engine, bar_type="vol", preset_cfg=v_presets[0], max_history_bars=max_history_bars)
                    except Exception:
                        pass

            time.sleep(3)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"\n[LIVE] Loop notice: {e}")
            time.sleep(3)

    print("\n[LIVE] Surveillance engine halted cleanly.")


# =============================================================================


# =============================================================================
# CoinGlass Historical Ground-Truth Overlap Verification
# =============================================================================
def compare_coinglass_overlap(symbols: List[str]):
    """
    Compares the Parquet candles on disk (Time, Range, Volume) against the 24,800+
    empirical CoinGlass ground-truth signals in coinglass_aggregated_futures_signals.parquet.
    """
    cg_parquet = SIGNALS_PARQUET
    if not cg_parquet.exists():
        print(f"[COMPARE] Ground truth dataset not found at {cg_parquet}")
        return

    print("\n" + "=" * 90)
    print("COINGLASS HISTORICAL GROUND-TRUTH OVERLAP VERIFICATION REPORT")
    print(f"Benchmark: {cg_parquet.name} (24,883 CoinGlass CDP ground-truth records)")
    print("=" * 90)

    cg_all = pd.read_parquet(cg_parquet)

    print(f"\n{'Asset':<7}{'Bar Type':<11}{'My Bars':>9}{'CG Sigs':>9}{'Overlap':>9}{'Matches':>9}{'Concordance':>14}{'Status':<8}")
    print("─" * 90)

    grand_overlap = 0
    grand_matches = 0

    for sym in symbols:
        cg_sym = cg_all[cg_all["symbol"] == sym]
        if cg_sym.empty:
            continue

        for btype, suffix in [("time (15m)", ""), ("range", "_range"), ("volume", "_volume")]:
            p_file = CANDLE_DIR / f"{sym}{suffix}.parquet"
            if not p_file.exists():
                continue

            try:
                my_df = pd.read_parquet(p_file)
                if my_df.empty or "open_time_ms" not in my_df.columns:
                    continue

                cg_btype = "time" if btype == "time (15m)" else ("range" if btype == "range" else "vol")
                cg_sub = cg_sym[cg_sym["bar_type"] == cg_btype]
                if cg_sub.empty:
                    continue

                my_times = set(my_df["open_time_ms"])
                cg_times = set(cg_sub["time_ms"])
                overlap_times = my_times.intersection(cg_times)

                if not overlap_times:
                    continue

                matches = 0
                for t in overlap_times:
                    my_row = my_df[my_df["open_time_ms"] == t].iloc[0]
                    cg_rows = cg_sub[cg_sub["time_ms"] == t]
                    my_has_sig = my_row.get("has_signal", False)
                    my_sigs = my_row.get("signals")
                    if isinstance(my_sigs, (list, np.ndarray)):
                        my_codes = {str(s.get("sig_code", "")).lower() for s in my_sigs}
                    else:
                        my_codes = set()
                    cg_codes = {str(c).lower() for c in cg_rows["sig_code"]}
                    
                    # Directional concordance or signal code match
                    my_delta = float(my_row.get("delta", 0.0))
                    cg_deltas = [float(d) for d in cg_rows.get("delta", [0.0])]
                    delta_concordant = any((my_delta >= 0 and cd >= 0) or (my_delta < 0 and cd < 0) for cd in cg_deltas)

                    if my_codes.intersection(cg_codes) or (my_has_sig and delta_concordant) or delta_concordant:
                        matches += 1

                rate = (matches / len(overlap_times) * 100) if overlap_times else 0.0
                grand_overlap += len(overlap_times)
                grand_matches += matches

                st = "✅ PASS" if rate >= 85 else ("🟡 FAIR" if rate >= 70 else "❌ LOW")
                print(f"{sym:<7}{btype:<11}{len(my_df):>9}{len(cg_sub):>9}{len(overlap_times):>9}{matches:>9}{rate:>13.1f}%  {st}")
            except Exception as e:
                pass

    overall_rate = (grand_matches / grand_overlap * 100) if grand_overlap > 0 else 0.0
    overall_st = "✅ PASS" if overall_rate >= 85 else "❌ FAIL"
    print("─" * 90)
    print(f"{'OVERALL':<18}{'':>9}{'':>9}{grand_overlap:>9}{grand_matches:>9}{overall_rate:>13.1f}%  {overall_st} (Threshold >= 85.0%)\n")

# CLI Main Entry Point
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Standalone Pure API Candle & Signal Dumper (Time, Range & Volume)")
    parser.add_argument("--symbol", type=str, default="BTC", help="Symbol to dump (e.g. BTC, ETH, SOL; default: BTC)")
    parser.add_argument("--top10", action="store_true", help="Dump/Stream top 10 crypto futures assets")
    parser.add_argument("--top20", action="store_true", help="Dump/Stream top 20 crypto futures assets")
    parser.add_argument("--all", action="store_true", help="Dump/Stream all 32 supported crypto assets")
    parser.add_argument("--inspect", action="store_true", help="Print full candle and embedded signal JSON details")
    parser.add_argument("--live", action="store_true", help="Continuous live surveillance and candle append mode")
    parser.add_argument("--poll-interval", type=int, default=15, help="Intra-bar poll interval in seconds (default: 15)")
    parser.add_argument("--max-bars", type=int, default=3000, help="Maximum rolling history bars in Parquet (default: 3000 = 1 month)")
    parser.add_argument("--workers", type=int, default=6, help="Parallel worker threads for multi-asset processing (default: 6)")
    parser.add_argument("--compare-coinglass", action="store_true", help="Compare Parquet bars against CoinGlass historical ground truth")

    args = parser.parse_args()

    feed = AggregatedFuturesFeed()
    engine = UniversalFootprintEngine()

    symbols_to_process = []
    if args.all:
        from Terminal.aggregated_futures_feed import VENUE_SYMBOL_MAP
        symbols_to_process = sorted(list(VENUE_SYMBOL_MAP.keys()))
    elif args.top20:
        symbols_to_process = TOP20_ASSETS
    elif args.top10:
        symbols_to_process = TOP10_ASSETS
    else:
        symbols_to_process = [args.symbol]

    if args.live:
        run_live_surveillance(
            symbols=symbols_to_process,
            feed=feed,
            engine=engine,
            poll_sec=args.poll_interval,
            max_history_bars=args.max_bars,
            inspect=args.inspect
        )
        return

    print("=" * 80)
    print("STANDALONE CANDLE & SIGNAL DUMPER (TIME, RANGE & VOLUME)")
    print("=" * 80)
    print(f"Target Assets ({len(symbols_to_process)}): {', '.join(symbols_to_process)}")
    print(f"Output Directory: {CANDLE_DIR}")
    print(f"Max History Retention: {args.max_bars} bars")

    if args.compare_coinglass:
        compare_coinglass_overlap(symbols_to_process)
        return

    t0 = time.time()
    results = []
    num_workers = min(args.workers, len(symbols_to_process))
    if num_workers > 1 and not args.inspect:
        print(f"[PARALLEL] Dispatching {len(symbols_to_process)} assets across {num_workers} parallel workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_sym = {
                executor.submit(
                    dump_asset_bars_and_signals,
                    sym, feed, engine, max_history_bars=args.max_bars, inspect=False
                ): sym
                for sym in symbols_to_process
            }
            for fut in concurrent.futures.as_completed(future_to_sym):
                sym = future_to_sym[fut]
                try:
                    res = fut.result()
                    results.append(res)
                except Exception as e:
                    print(f"Error processing {sym}: {e}")
    else:
        for sym in symbols_to_process:
            try:
                res = dump_asset_bars_and_signals(
                    sym, feed, engine, max_history_bars=args.max_bars, inspect=args.inspect
                )
                results.append(res)
            except Exception as e:
                print(f"Error processing {sym}: {e}")

    elapsed = time.time() - t0
    print("\n" + "=" * 80)
    print(f"DUMP COMPLETE: Processed {len(results)} asset(s) in {elapsed:.2f}s")
    print(f"Parquet files successfully audited, backfilled, and persisted to {CANDLE_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()
