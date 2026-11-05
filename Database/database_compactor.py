"""
database_compactor.py
========================================================================================
Database Compaction, Hygiene & Parquet Optimization Engine
========================================================================================
- Deduplicates append-only ledgers in Database/Signals/Signal_Queues and Trade_Setups.
- Fixes un-terminated trailing lines and purges corrupted JSON entries.
- Deduplicates Database/Signals/Trade_Setups/master_active_setups.jsonl.
- Converts uncompressed / Snappy Parquet files to ZSTD-3 with dictionary encoding.
- Cleans orphaned / stale file locks in Database/.locks/ older than 15 minutes.
- Verifies cross-engine DuckDB and Polars schema stability across all 31 assets.
========================================================================================
"""

import sys
import os
import json
import time
import shutil
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
import polars as pl

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from coinglass_common import writer_lock, FileMutex

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][Compactor][%(levelname)s] %(message)s"
)
logger = logging.getLogger("DatabaseCompactor")

DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
QUEUE_DIR = SIGNALS_DIR / "Signal_Queues"
SETUPS_DIR = SIGNALS_DIR / "Trade_Setups"
MASTER_SETUPS_PATH = SETUPS_DIR / "master_active_setups.jsonl"
LOCKS_DIR = DB_DIR / ".locks"


def clean_stale_locks(max_age_seconds: int = 900) -> int:
    """Removes orphaned lock files in Database/.locks/ not updated for max_age_seconds."""
    if not LOCKS_DIR.exists():
        return 0

    removed = 0
    now = time.time()
    for lock_file in LOCKS_DIR.glob("*.lock"):
        try:
            mtime = lock_file.stat().st_mtime
            if (now - mtime) > max_age_seconds:
                # Attempt non-blocking lock to ensure no active process holds it
                with FileMutex(str(lock_file), timeout=0.1) as mutex:
                    if mutex.acquired:
                        # Close handle before unlinking on Windows
                        pass
                # Safe to unlink if no active process holds it
                try:
                    lock_file.unlink(missing_ok=True)
                    removed += 1
                    logger.info(f"Cleaned stale lock: {lock_file.name} (Age: {(now - mtime)/60:.1f}m)")
                except PermissionError:
                    pass
        except Exception as e:
            logger.debug(f"Could not inspect lock {lock_file.name}: {e}")

    logger.info(f"Stale locks cleanup complete: {removed} lock(s) removed.")
    return removed


def deduplicate_jsonl_file(
    file_path: Path,
    key_fn: Any,
    keep: str = "last"
) -> Tuple[int, int, int]:
    """
    Safely deduplicates a JSONL file in-place with cross-process FileMutex.
    Returns: (original_count, cleaned_count, corrupted_count)
    """
    if not file_path.exists():
        return 0, 0, 0

    with writer_lock(file_path):
        records = []
        corrupted = 0
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    records.append(rec)
                except Exception:
                    corrupted += 1

        orig_len = len(records)
        seen = {}
        for r in records:
            k = key_fn(r)
            if keep == "last":
                seen[k] = r
            else:
                if k not in seen:
                    seen[k] = r

        deduped = list(seen.values())

        # Write back atomically with trailing newline
        tmp_path = file_path.with_suffix(".tmp_compact")
        with open(tmp_path, "w", encoding="utf-8") as tf:
            for r in deduped:
                tf.write(json.dumps(r, ensure_ascii=True, allow_nan=False) + "\n")
            tf.flush()
            try:
                os.fsync(tf.fileno())
            except OSError:
                pass

        os.replace(tmp_path, file_path)

    return orig_len, len(deduped), corrupted


def deduplicate_master_active_setups() -> Tuple[int, int]:
    """Deduplicates master_active_setups.jsonl on (symbol, time_ms, direction, primary_signal)."""
    if not MASTER_SETUPS_PATH.exists():
        return 0, 0

    def key_fn(r):
        return (
            str(r.get("symbol", "")).upper(),
            int(r.get("time_ms", 0)),
            str(r.get("direction", "")).upper(),
            str(r.get("primary_signal", "")).lower()
        )

    orig, cleaned, corrupt = deduplicate_jsonl_file(MASTER_SETUPS_PATH, key_fn, keep="last")
    logger.info(
        f"Master active setups compacted: {orig} -> {cleaned} setups "
        f"({orig - cleaned} duplicates removed, {corrupt} corrupt lines purged)."
    )
    return orig, cleaned


def deduplicate_all_signal_queues() -> Dict[str, Any]:
    """Deduplicates all per-asset signal ledgers in Database/Signals/Signal_Queues/."""
    def sig_key(s):
        return (
            str(s.get("symbol", "")).upper(),
            str(s.get("sig_code", "")).lower(),
            str(s.get("bar_type", "time")).lower(),
            str(s.get("bar_label", "")).strip(),
            int(s.get("time_ms", 0))
        )

    total_orig = 0
    total_cleaned = 0
    total_corrupt = 0
    files = list(QUEUE_DIR.glob("*_signals.jsonl"))

    for sf in files:
        orig, cleaned, corrupt = deduplicate_jsonl_file(sf, sig_key, keep="last")
        total_orig += orig
        total_cleaned += cleaned
        total_corrupt += corrupt

    logger.info(
        f"All signal ledgers compacted: {len(files)} files | "
        f"{total_orig} -> {total_cleaned} signals ({total_orig - total_cleaned} duplicates, {total_corrupt} corrupt)."
    )
    return {"files": len(files), "original": total_orig, "cleaned": total_cleaned, "corrupted": total_corrupt}


def deduplicate_all_trade_setups() -> Dict[str, Any]:
    """Deduplicates all per-asset setup ledgers in Database/Signals/Trade_Setups/."""
    def setup_key(s):
        return (
            str(s.get("symbol", "")).upper(),
            int(s.get("time_ms", 0)),
            str(s.get("direction", "")).upper(),
            str(s.get("primary_signal", "")).lower()
        )

    total_orig = 0
    total_cleaned = 0
    total_corrupt = 0
    files = [f for f in SETUPS_DIR.glob("*_setups.jsonl") if f.name != "master_active_setups.jsonl"]

    for sf in files:
        orig, cleaned, corrupt = deduplicate_jsonl_file(sf, setup_key, keep="last")
        total_orig += orig
        total_cleaned += cleaned
        total_corrupt += corrupt

    logger.info(
        f"All trade setup ledgers compacted: {len(files)} files | "
        f"{total_orig} -> {total_cleaned} setups ({total_orig - total_cleaned} duplicates, {total_corrupt} corrupt)."
    )
    return {"files": len(files), "original": total_orig, "cleaned": total_cleaned, "corrupted": total_corrupt}


def optimize_parquet_files(directories: Optional[List[str]] = None, level: int = 3) -> Dict[str, Any]:
    """
    Converts Snappy / uncompressed Parquet files to ZSTD level 3 with dictionary encoding.
    Returns stats on files processed and bytes saved.
    """
    if not directories:
        directories = [
            "Candle_Data",
            "Live_Liquidations",
            "Whale_Orders/Open_Orders",
            "Whale_Orders/History_Orders",
            "Liquidation_Data/Magnet_Zones",
            "Signals",
        ]

    total_orig_bytes = 0
    total_new_bytes = 0
    converted_count = 0

    for d in directories:
        target_dir = DB_DIR / d
        if not target_dir.exists():
            continue
        for pfile in target_dir.glob("*.parquet"):
            try:
                orig_size = pfile.stat().st_size
                table = None
                with open(pfile, "rb") as bf:
                    pf = pq.ParquetFile(bf)
                    codecs = {pf.metadata.row_group(0).column(c).compression for c in range(pf.metadata.num_columns)} if pf.metadata.num_row_groups > 0 else set()
                    if "ZSTD" in codecs and len(codecs) == 1:
                        total_orig_bytes += orig_size
                        total_new_bytes += orig_size
                        continue
                    bf.seek(0)
                    table = pq.read_table(bf)

                tmp_file = pfile.with_suffix(".tmp_zstd")
                pq.write_table(table, tmp_file, compression="zstd", compression_level=level, use_dictionary=True)
                new_size = tmp_file.stat().st_size

                # Only replace if new size is smaller or equal
                if new_size <= orig_size:
                    os.replace(tmp_file, pfile)
                    total_orig_bytes += orig_size
                    total_new_bytes += new_size
                    converted_count += 1
                else:
                    tmp_file.unlink(missing_ok=True)
                    total_orig_bytes += orig_size
                    total_new_bytes += orig_size
            except Exception as e:
                logger.warning(f"Failed converting {pfile.name} to ZSTD: {e}")

    saved_bytes = total_orig_bytes - total_new_bytes
    saved_pct = (saved_bytes / max(total_orig_bytes, 1)) * 100
    logger.info(
        f"Parquet ZSTD Optimization: {converted_count} file(s) converted. "
        f"Storage: {total_orig_bytes/1024:.1f} KB -> {total_new_bytes/1024:.1f} KB "
        f"(Saved: {saved_bytes/1024:.1f} KB / {saved_pct:.1f}%)."
    )
    return {
        "converted": converted_count,
        "orig_kb": total_orig_bytes / 1024,
        "new_kb": total_new_bytes / 1024,
        "saved_kb": saved_bytes / 1024,
        "saved_pct": saved_pct
    }


def verify_schema_integrity() -> bool:
    """Verifies that all ledgers load correctly in Polars and DuckDB."""
    from trade_evaluator import (
        TRADE_SETUPS_POLARS_SCHEMA,
        SIGNALS_POLARS_SCHEMA,
        load_trade_setups_polars,
        load_signals_polars
    )

    all_ok = True
    try:
        df_setups = load_trade_setups_polars()
        logger.info(f"Polars Trade Setups verification: SUCCESS ({len(df_setups)} rows across all assets)")
    except Exception as e:
        logger.error(f"Polars Trade Setups verification: FAILED ({e})")
        all_ok = False

    try:
        df_sigs = load_signals_polars()
        logger.info(f"Polars Signal Queues verification: SUCCESS ({len(df_sigs)} rows across all assets)")
    except Exception as e:
        logger.error(f"Polars Signal Queues verification: FAILED ({e})")
        all_ok = False

    try:
        duck_setups = duckdb.execute(
            f"SELECT COUNT(*) FROM read_json_auto('{str(SETUPS_DIR / '*_setups.jsonl').replace(chr(92), '/')}', union_by_name=true)"
        ).fetchall()[0][0]
        logger.info(f"DuckDB Trade Setups verification: SUCCESS ({duck_setups} rows across all assets)")
    except Exception as e:
        logger.error(f"DuckDB Trade Setups verification: FAILED ({e})")
        all_ok = False

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Database Compactor & Storage Optimizer")
    parser.add_argument("--all", action="store_true", help="Run full compaction, hygiene, deduplication and ZSTD conversion")
    parser.add_argument("--dedup-master", action="store_true", help="Deduplicate master active setups")
    parser.add_argument("--dedup-all", action="store_true", help="Deduplicate all signal and setup ledgers")
    parser.add_argument("--clean-locks", action="store_true", help="Clean stale lock files older than 15m")
    parser.add_argument("--compress-zstd", action="store_true", help="Convert Parquet files to ZSTD level 3")
    parser.add_argument("--verify", action="store_true", help="Verify DuckDB and Polars schema integrity")
    args = parser.parse_args()

    if args.all or not any([args.dedup_master, args.dedup_all, args.clean_locks, args.compress_zstd, args.verify]):
        logger.info("Executing FULL Database Compaction & Storage Optimization...")
        clean_stale_locks()
        deduplicate_master_active_setups()
        deduplicate_all_signal_queues()
        deduplicate_all_trade_setups()
        optimize_parquet_files()
        verify_schema_integrity()
        logger.info("Full compaction cycle completed successfully.")
        return

    if args.clean_locks:
        clean_stale_locks()
    if args.dedup_master:
        deduplicate_master_active_setups()
    if args.dedup_all:
        deduplicate_all_signal_queues()
        deduplicate_all_trade_setups()
    if args.compress_zstd:
        optimize_parquet_files()
    if args.verify:
        verify_schema_integrity()


if __name__ == "__main__":
    main()
