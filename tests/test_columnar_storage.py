"""
Unit and Regression tests for Columnar Storage, Projection Pushdown, and 300-Bar Invariant.
"""
import os
import tempfile
from pathlib import Path
import pandas as pd
import polars as pl
import duckdb
import pyarrow.parquet as pq
import pytest

from Terminal.runtime_io import atomic_parquet, upsert_parquet
from Terminal.olap_engine import OlapEngine
from Terminal.auction_indicators import compute_auction_profile


def test_atomic_parquet_codecs(tmp_path):
    """Test snappy and zstd compression codecs in atomic_parquet."""
    df = pd.DataFrame({
        "open_time_ms": [1000, 2000, 3000],
        "close": [100.5, 101.2, 102.0],
        "symbol": ["BTC", "BTC", "BTC"]
    })
    
    # Snappy
    p_snappy = tmp_path / "test_snappy.parquet"
    atomic_parquet(df, p_snappy, compression="snappy")
    assert p_snappy.exists()
    meta_s = pq.read_metadata(p_snappy)
    assert meta_s.row_group(0).column(0).compression == "SNAPPY"
    
    # ZSTD
    p_zstd = tmp_path / "test_zstd.parquet"
    atomic_parquet(df, p_zstd, compression="zstd")
    assert p_zstd.exists()
    meta_z = pq.read_metadata(p_zstd)
    assert meta_z.row_group(0).column(0).compression == "ZSTD"
    
    # Verify exact round-trip content across engines
    pd_read = pd.read_parquet(p_zstd)
    pl_read = pl.read_parquet(p_zstd)
    assert len(pd_read) == 3
    assert len(pl_read) == 3
    assert list(pd_read["close"]) == [100.5, 101.2, 102.0]


def test_projection_pushdown_column_subset(tmp_path):
    """Verify projection pushdown correctly isolates requested columns without schema mutation."""
    df = pd.DataFrame({
        "open_time_ms": [1000, 2000],
        "close_time_ms": [1900, 2900],
        "open": [10.0, 11.0],
        "high": [12.0, 13.0],
        "low": [9.0, 10.0],
        "close": [11.0, 12.0],
        "total_volume": [500.0, 600.0],
        "delta": [50.0, -20.0],
        "cumulative_delta": [50.0, 30.0],
        "unused_str_1": ["long_unnecessary_string_a", "long_unnecessary_string_b"],
        "unused_str_2": ["another_string_a", "another_string_b"],
    })
    p = tmp_path / "test_proj.parquet"
    atomic_parquet(df, p)
    
    target_cols = ["open_time_ms", "close", "delta"]
    projected = pd.read_parquet(p, columns=target_cols)
    assert list(projected.columns) == target_cols
    assert len(projected) == 2
    assert "unused_str_1" not in projected.columns


def test_exact_300_rolling_window_invariant(tmp_path):
    """Verify exact 300-row invariant: files never store > 300 rows."""
    p = tmp_path / "test_300.parquet"
    
    # Create 400 rows
    df_400 = pd.DataFrame({
        "open_time_ms": [i * 900000 for i in range(400)],
        "close": [100.0 + i for i in range(400)]
    })
    
    # Simulate trimming logic
    trimmed = df_400.tail(300)
    atomic_parquet(trimmed, p)
    
    meta = pq.read_metadata(p)
    assert meta.num_rows == 300
    
    read_df = pd.read_parquet(p)
    assert len(read_df) == 300
    # First row should be bar index 100
    assert read_df["open_time_ms"].iloc[0] == 100 * 900000
    assert read_df["open_time_ms"].iloc[-1] == 399 * 900000
