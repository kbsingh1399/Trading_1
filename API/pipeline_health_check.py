"""
pipeline_health_check.py
========================================================================================
Independent Telemetry Health Auditor for Multi-Pillar Order Flow Pipeline
========================================================================================
Audits Parquet dumps freshness, row counts, atomic heartbeat status, and stale locks.
Exits 0 on HEALTHY, 1 on DEGRADED / STALE / UNHEALTHY.
========================================================================================
"""

import sys
import os
import json
import time
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pyarrow.parquet as pq

API_DIR = Path(__file__).resolve().parent
ROOT_DIR = API_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

try:
    from active_assets_manager import load_active_assets
except ImportError:
    load_active_assets = lambda: ["BTC", "ETH", "SOL"]


def audit_parquet_freshness(path: Path, max_age_secs: float = 900.0, min_rows: int = 1) -> Dict[str, Any]:
    """Audits existence, staleness, row count, and corruption of a parquet file."""
    p = Path(path)
    if not p.exists():
        return {
            "path": str(p),
            "exists": False,
            "status": "MISSING",
            "age_seconds": None,
            "row_count": 0,
            "error": "File does not exist"
        }
    try:
        stat = p.stat()
        age_seconds = max(0.0, time.time() - stat.st_mtime)
        meta = pq.read_metadata(p)
        row_count = meta.num_rows

        if row_count < min_rows:
            status = "EMPTY"
            error = f"Row count {row_count} < minimum {min_rows}"
        elif age_seconds > max_age_secs:
            status = "STALE"
            error = f"Age {age_seconds:.1f}s > max {max_age_secs:.1f}s"
        else:
            status = "FRESH"
            error = None

        return {
            "path": str(p),
            "exists": True,
            "status": status,
            "age_seconds": round(age_seconds, 1),
            "row_count": row_count,
            "error": error
        }
    except Exception as exc:
        return {
            "path": str(p),
            "exists": True,
            "status": "CORRUPTED",
            "age_seconds": None,
            "row_count": 0,
            "error": str(exc)
        }


def audit_lock_files(lock_dir: Path, stale_threshold_secs: float = 180.0) -> List[Dict[str, Any]]:
    """Detects any orphaned lock files lingering longer than the stale threshold."""
    stale_locks = []
    p = Path(lock_dir)
    if not p.exists():
        return stale_locks

    now = time.time()
    for lock_path in p.glob("**/*.lock"):
        try:
            stat = lock_path.stat()
            age = max(0.0, now - stat.st_mtime)
            if age > stale_threshold_secs:
                stale_locks.append({
                    "path": str(lock_path),
                    "name": lock_path.name,
                    "age_seconds": round(age, 1)
                })
        except Exception:
            pass
    return stale_locks


def audit_heartbeat(health_file: Path, max_age_secs: float = 900.0) -> Dict[str, Any]:
    """Inspects Database/pipeline_health.json emitted by the runner daemon."""
    p = Path(health_file)
    if not p.exists():
        return {
            "exists": False,
            "status": "MISSING",
            "age_seconds": None,
            "payload": None,
            "error": "pipeline_health.json not found"
        }
    try:
        stat = p.stat()
        age = max(0.0, time.time() - stat.st_mtime)
        data = json.loads(p.read_text(encoding="utf-8"))
        is_stale = age > max_age_secs
        reported_status = data.get("status", "UNKNOWN")
        status = "STALE" if is_stale else reported_status
        return {
            "exists": True,
            "status": status,
            "age_seconds": round(age, 1),
            "payload": data,
            "error": f"Heartbeat stale ({age:.1f}s)" if is_stale else None
        }
    except Exception as exc:
        return {
            "exists": True,
            "status": "CORRUPTED",
            "age_seconds": None,
            "payload": None,
            "error": str(exc)
        }


def audit_universe_parquets(db_dir: Path, symbols: List[str], max_age_secs: float = 900.0) -> Dict[str, Any]:
    """Audits parquet freshness across all 4 pillars for the given symbols."""
    fp_dir = db_dir / "Footprint_Dumps"
    whale_dir = db_dir / "Whale_Dumps"
    liq_dir = db_dir / "Liquidation_Level_Dumps"
    legend_dump = db_dir / "Live_Liquidation_Dump"
    legend_alt = db_dir / "Live_Liquidations"

    per_asset = {}
    stale_count = 0
    missing_count = 0
    empty_count = 0

    for sym in symbols:
        asset_report = {}

        # 1. Footprint (Time 15m)
        fp_path = fp_dir / f"{sym}_Time_15m_footprint.parquet"
        asset_report["footprint"] = audit_parquet_freshness(fp_path, max_age_secs)

        # 2. Whales
        whale_path = whale_dir / f"{sym}_whale_open_orders.parquet"
        asset_report["whales"] = audit_parquet_freshness(whale_path, max_age_secs)

        # 3. Liquidation Levels (3d or key levels)
        liq_path = liq_dir / f"{sym}_3d_liquidation_levels.parquet"
        if not liq_path.exists():
            liq_path = db_dir / "Liquidation_Data" / "Key_Levels" / f"{sym}_key_levels.parquet"
        asset_report["liquidation_levels"] = audit_parquet_freshness(liq_path, max_age_secs)

        # 4. Live Legend Liquidations
        legend_path = legend_dump / f"{sym}_Live_Liquidation.parquet"
        if not legend_path.exists():
            legend_path = legend_alt / f"{sym}_tv_liquidations.parquet"
        asset_report["live_legend"] = audit_parquet_freshness(legend_path, max_age_secs)

        # Aggregate counts
        for pillar_res in asset_report.values():
            st = pillar_res["status"]
            if st == "STALE":
                stale_count += 1
            elif st == "MISSING":
                missing_count += 1
            elif st == "EMPTY":
                empty_count += 1

        per_asset[sym] = asset_report

    return {
        "per_asset": per_asset,
        "stale_count": stale_count,
        "missing_count": missing_count,
        "empty_count": empty_count
    }


def audit_full_pipeline(
    db_dir: Path,
    symbols: Optional[List[str]] = None,
    max_age_min: float = 15.0
) -> Dict[str, Any]:
    """Executes holistic 360-degree health audit of the live telemetry pipeline."""
    db_dir = Path(db_dir)
    max_age_secs = max_age_min * 60.0

    if not symbols:
        try:
            symbols = load_active_assets()
        except Exception:
            symbols = ["BTC", "ETH", "SOL"]

    heartbeat_res = audit_heartbeat(db_dir / "pipeline_health.json", max_age_secs)
    stale_locks = audit_lock_files(db_dir / ".locks", stale_threshold_secs=180.0)
    parquet_res = audit_universe_parquets(db_dir, symbols, max_age_secs)

    healthy = (
        heartbeat_res["status"] == "HEALTHY"
        and len(stale_locks) == 0
        and parquet_res["stale_count"] == 0
        and parquet_res["missing_count"] == 0
        and parquet_res["empty_count"] == 0
    )

    overall_status = "HEALTHY" if healthy else "DEGRADED"

    return {
        "timestamp_ist": time.strftime("%Y-%m-%d %H:%M:%S IST", time.localtime()),
        "overall_status": overall_status,
        "healthy": healthy,
        "max_age_minutes": max_age_min,
        "heartbeat": heartbeat_res,
        "stale_locks": stale_locks,
        "parquet_summary": {
            "audited_assets": len(symbols),
            "stale_count": parquet_res["stale_count"],
            "missing_count": parquet_res["missing_count"],
            "empty_count": parquet_res["empty_count"]
        },
        "per_asset": parquet_res["per_asset"]
    }


def main():
    parser = argparse.ArgumentParser(description="Multi-Pillar Telemetry Pipeline Health Auditor")
    parser.add_argument("--db-dir", type=str, default=str(ROOT_DIR / "Database"), help="Database root directory")
    parser.add_argument("--max-age-min", type=float, default=15.0, help="Max file age in minutes before marking STALE (default: 15.0)")
    parser.add_argument("--symbols", nargs="+", default=None, help="Specific symbols to audit")
    parser.add_argument("--json", action="store_true", help="Output audit report as JSON")
    parser.add_argument("--strict", action="store_true", help="Exit code 1 if any asset is missing or stale")
    args = parser.parse_args()

    report = audit_full_pipeline(
        db_dir=Path(args.db_dir),
        symbols=args.symbols,
        max_age_min=args.max_age_min
    )

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("=" * 80)
        print(f"PIPELINE HEALTH AUDIT REPORT | Status: [{report['overall_status']}]")
        print("=" * 80)
        hb = report["heartbeat"]
        print(f"Heartbeat: {hb['status']} (Age: {hb.get('age_seconds')}s)")
        if hb.get("payload"):
            p = hb["payload"]
            print(f"  Cycle #{p.get('cycle_number')} | IP: {p.get('current_ip')} | Cycles on IP: {p.get('cycles_on_current_ip')}")

        print(f"Stale Locks: {len(report['stale_locks'])}")
        for l in report["stale_locks"]:
            print(f"  [STALE LOCK] {l['name']} ({l['age_seconds']}s old)")

        summary = report["parquet_summary"]
        print(f"Parquet Health: {summary['audited_assets']} assets audited")
        print(f"  Fresh: {summary['audited_assets']*4 - summary['stale_count'] - summary['missing_count'] - summary['empty_count']}")
        print(f"  Stale: {summary['stale_count']} | Missing: {summary['missing_count']} | Empty: {summary['empty_count']}")
        print("=" * 80)

    is_ok = report["healthy"] if args.strict else (report["overall_status"] == "HEALTHY" or report["heartbeat"]["status"] == "HEALTHY")
    sys.exit(0 if is_ok else 1)


if __name__ == "__main__":
    main()
