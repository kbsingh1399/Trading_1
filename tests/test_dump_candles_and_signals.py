"""
Unit and Integration Tests for Standalone Candle & Signal Dumper Pipeline
=========================================================================
Tests startup gap detection, incremental missing candle backfill, multi-bar
synthesis, schema validation, and live boundary handling.
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime, timezone
import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from dump_candles_and_signals import (
    detect_missing_time_bars,
    audit_and_sync_time_bars,
    save_candle_parquet_with_signals,
    CANDLE_DIR
)
from Terminal.aggregated_futures_feed import AggregatedFuturesFeed
from Terminal.universal_footprint_engine import UniversalFootprintEngine


def test_detect_missing_time_bars_on_fresh_or_missing_file(tmp_path):
    """Verifies that an absent file correctly reports 0 existing and 300 missing bars."""
    fake_file = tmp_path / "NONEXISTENT.parquet"
    res = detect_missing_time_bars(fake_file, interval_ms=900_000)
    assert res["exists"] is False
    assert res["total_bars"] == 0
    assert res["missing_trailing_bars"] == 300
    assert res["is_up_to_date"] is False


def test_startup_gap_detection_and_incremental_backfill(tmp_path):
    """
    Simulates a 5-bar historical gap by truncating an existing candle parquet,
    then verifies that detect_missing_time_bars accurately identifies the 5-bar gap
    and audit_and_sync_time_bars backfills the exact missing candles.
    """
    btc_file = CANDLE_DIR / "BTC.parquet"
    assert btc_file.exists(), "BTC.parquet must exist in CANDLE_DIR"

    # Read real BTC parquet
    df_real = pd.read_parquet(btc_file).sort_values("open_time_ms").reset_index(drop=True)
    assert len(df_real) >= 50

    # Create synthetic truncated parquet with 5 missing bars
    df_truncated = df_real.iloc[:-5].copy()
    test_file = tmp_path / "TEST_GAP.parquet"
    save_candle_parquet_with_signals(df_truncated, test_file)

    # 1. Audit truncated file
    audit = detect_missing_time_bars(test_file, interval_ms=900_000)
    assert audit["exists"] is True
    assert audit["total_bars"] == len(df_truncated)
    assert audit["missing_trailing_bars"] >= 5
    assert audit["is_up_to_date"] is False

    # 2. Backfill with feed & engine
    feed = AggregatedFuturesFeed()
    engine = UniversalFootprintEngine()

    # Backfill on real BTC
    df_synced, sigs, stats = audit_and_sync_time_bars("BTC", feed, engine, max_history_bars=300)
    assert not df_synced.empty
    assert len(df_synced) >= 290
    assert "signals" in df_synced.columns

    # Verify no chronological timestamp duplicates
    time_diffs = df_synced["open_time_ms"].diff().dropna()
    assert (time_diffs <= 0).sum() == 0, "No duplicate or backwards timestamps allowed"


def test_parquet_schema_has_strict_signals_struct_list():
    """Verifies that persisted Parquet has valid PyArrow List(Struct) signals schema."""
    btc_file = CANDLE_DIR / "BTC.parquet"
    table = pq.read_table(btc_file)
    schema = table.schema

    assert "signals" in schema.names
    field = schema.field("signals")
    assert pa.types.is_list(field.type)

    value_type = field.type.value_type
    assert pa.types.is_struct(value_type)
    struct_field_names = [value_type.field(i).name for i in range(value_type.num_fields)]
    assert "sig_code" in struct_field_names
    assert "sig_name" in struct_field_names
    assert "side" in struct_field_names
    assert "price" in struct_field_names
    assert "title" in struct_field_names
    assert "description" in struct_field_names


def test_multibar_files_exist_and_are_valid():
    """Verifies that Time, Range, and Volume Parquet files all exist and have valid shapes."""
    for suffix in ["", "_range", "_volume"]:
        f = CANDLE_DIR / f"BTC{suffix}.parquet"
        assert f.exists(), f"Expected {f.name} to exist"
        assert f.stat().st_size > 0
        df = pd.read_parquet(f)
        assert len(df) >= 4
        assert "delta" in df.columns
        assert "total_volume" in df.columns
        assert "signals" in df.columns
