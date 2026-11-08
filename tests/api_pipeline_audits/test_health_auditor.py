import os, sys, time, json
from pathlib import Path
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

# Ensure API directory is on sys.path
API_DIR = Path(__file__).resolve().parent.parent.parent / "API"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

import pipeline_health_check as auditor


def test_audit_parquet_freshness_fresh(tmp_path):
    p = tmp_path / "test.parquet"
    table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["id"])
    pq.write_table(table, p)

    res = auditor.audit_parquet_freshness(p, max_age_secs=300, min_rows=1)
    assert res["exists"] is True
    assert res["status"] == "FRESH"
    assert res["row_count"] == 3
    assert res["age_seconds"] < 10.0


def test_audit_parquet_freshness_stale(tmp_path):
    p = tmp_path / "test.parquet"
    table = pa.Table.from_arrays([pa.array([1, 2])], names=["id"])
    pq.write_table(table, p)

    stale_time = time.time() - 3600.0
    os.utime(p, (stale_time, stale_time))

    res = auditor.audit_parquet_freshness(p, max_age_secs=900, min_rows=1)
    assert res["exists"] is True
    assert res["status"] == "STALE"
    assert res["age_seconds"] >= 3600.0


def test_audit_parquet_freshness_empty(tmp_path):
    p = tmp_path / "empty.parquet"
    table = pa.Table.from_arrays([pa.array([], type=pa.int64())], names=["id"])
    pq.write_table(table, p)

    res = auditor.audit_parquet_freshness(p, max_age_secs=900, min_rows=1)
    assert res["status"] == "EMPTY"
    assert res["row_count"] == 0


def test_audit_parquet_freshness_missing(tmp_path):
    p = tmp_path / "missing.parquet"
    res = auditor.audit_parquet_freshness(p, max_age_secs=900)
    assert res["exists"] is False
    assert res["status"] == "MISSING"


def test_audit_lock_files_detects_stale(tmp_path, monkeypatch):
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    stale_lock = lock_dir / "writer.lock"
    stale_lock.write_text("pid:1234")
    stale_time = time.time() - 500.0
    os.utime(stale_lock, (stale_time, stale_time))

    fresh_lock = lock_dir / "fresh.lock"
    fresh_lock.write_text("pid:5678")

    monkeypatch.setattr(auditor, "is_lock_held", lambda path: True)

    stale_list = auditor.audit_lock_files(lock_dir, stale_threshold_secs=180.0)
    assert len(stale_list) == 1
    assert stale_list[0]["name"] == "writer.lock"
    assert stale_list[0]["age_seconds"] >= 500.0


def test_audit_full_pipeline_healthy(tmp_path, monkeypatch):
    db_dir = tmp_path / "Database"
    fp_dir = db_dir / "Footprint_Dumps"
    whale_dir = db_dir / "Whale_Dumps"
    liq_dir = db_dir / "Liquidation_Level_Dumps"
    legend_dir = db_dir / "Live_Liquidation_Dump"
    locks_dir = db_dir / ".locks"

    for d in (fp_dir, whale_dir, liq_dir, legend_dir, locks_dir):
        d.mkdir(parents=True)

    table = pa.Table.from_arrays([pa.array([1, 2])], names=["id"])
    pq.write_table(table, fp_dir / "BTC_Time_15m_footprint.parquet")
    pq.write_table(table, whale_dir / "BTC_whale_open_orders.parquet")
    pq.write_table(table, liq_dir / "BTC_3d_liquidation_levels.parquet")
    pq.write_table(table, legend_dir / "BTC_Live_Liquidation.parquet")

    health_file = db_dir / "pipeline_health.json"
    health_payload = {
        "timestamp_ist": "2026-11-08 22:00:00 IST",
        "timestamp_epoch": time.time(),
        "cycle_number": 1,
        "current_ip": "1.2.3.4",
        "cycles_on_current_ip": 1,
        "ip_age_seconds": 10.0,
        "status": "HEALTHY",
        "pillars": {},
        "symbols": ["BTC"]
    }
    health_file.write_text(json.dumps(health_payload), encoding="utf-8")

    report = auditor.audit_full_pipeline(db_dir=db_dir, symbols=["BTC"], max_age_min=15.0)
    assert report["overall_status"] == "HEALTHY"
    assert report["healthy"] is True
