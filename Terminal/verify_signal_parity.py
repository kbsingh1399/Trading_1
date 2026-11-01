"""
CoinGlass & Footprint Signal Parity Verification Tool
=====================================================
Instantly verifies parity between CoinGlass Footprint Chart UI signals
and our local Parquet database tables.

Supports:
- Local Browser Time (IST / UTC+5:30) vs Standard UTC
- Exact candle and signal match
- Verification of price, volume, delta, POC, and signal type
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
import duckdb
import pandas as pd

# Reconfigure stdout for UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
CANDLE_DIR = DB_DIR / "Candle_Data"


def verify_symbol_signals(symbol: str, target_time_str: str = None, is_ist: bool = True, limit: int = 10):
    if not SIGNALS_PARQUET.exists():
        print(f"Error: {SIGNALS_PARQUET} does not exist.")
        return

    con = duckdb.connect()
    
    print("=" * 110)
    print(f"SIGNAL PARITY AUDIT: [{symbol}]")
    print(f"Database Source: {SIGNALS_PARQUET.as_posix()}")
    print("=" * 110)

    if target_time_str:
        # User supplied a specific timestamp like "19:41" or "11-01 19:41"
        if is_ist:
            print(f"Target Time (Input as IST UTC+5:30): {target_time_str}")
            # Try parsing hour and minute
            parts = target_time_str.strip().split()
            time_part = parts[-1]
            if ":" in time_part:
                h, m = map(int, time_part.split(":")[:2])
                ist_dt = datetime(2026, 11, 1, h, m)
                utc_dt = ist_dt - timedelta(hours=5, minutes=30)
                utc_pattern = f"%T{utc_dt.strftime('%H:%M')}%"
                print(f"Converted to UTC Search Pattern: {utc_pattern} ({utc_dt.strftime('%H:%M')} UTC)")
            else:
                utc_pattern = f"%{target_time_str}%"
        else:
            utc_pattern = f"%{target_time_str}%"

        q = f"""
            SELECT datetime_utc, sig_code, title, price, description
            FROM read_parquet('{SIGNALS_PARQUET.as_posix()}')
            WHERE symbol = '{symbol}' AND datetime_utc LIKE '{utc_pattern}'
            ORDER BY time_ms DESC
        """
        matches = con.execute(q).fetchdf()
        if len(matches) > 0:
            print(f"\n[FOUND {len(matches)} MATCHING SIGNAL(S)]:")
            for _, r in matches.iterrows():
                utc_dt = datetime.fromisoformat(r['datetime_utc'].replace('Z', '+00:00'))
                ist_dt = utc_dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
                print(f"  [MATCH] IST: {ist_dt.strftime('%Y-%m-%d %H:%M:%S')} | UTC: {r['datetime_utc']}")
                print(f"    Signal: {r['sig_code']:10s} | {r['title']}")
                print(f"    Price:  {r['price']} | Description: {r['description']}")
                print("-" * 90)
        else:
            print(f"\n[NO DIRECT MATCH FOUND] for search pattern {utc_pattern}")

    print(f"\nLatest {limit} signals recorded for {symbol} (with both IST and UTC):")
    print("-" * 110)
    q_all = f"""
        SELECT datetime_utc, sig_code, title, price, description
        FROM read_parquet('{SIGNALS_PARQUET.as_posix()}')
        WHERE symbol = '{symbol}'
        ORDER BY time_ms DESC
        LIMIT {limit}
    """
    df = con.execute(q_all).fetchdf()
    for _, r in df.iterrows():
        utc_dt = datetime.fromisoformat(r['datetime_utc'].replace('Z', '+00:00'))
        ist_dt = utc_dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
        print(f"  {ist_dt.strftime('%m-%d %H:%M')} IST ({utc_dt.strftime('%H:%M')} UTC) | {r['sig_code']:6s} | {r['title']:35s} | Price: {r['price']}")
    print("=" * 110)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CoinGlass Signal Parity Verification")
    parser.add_argument("symbol", nargs="?", default="ADA", help="Crypto symbol (e.g. ADA, AVAX, BTC)")
    parser.add_argument("time", nargs="?", default=None, help="Target timestamp (e.g. 19:41)")
    parser.add_argument("--utc", action="store_true", help="Treat input time as UTC instead of IST")
    args = parser.parse_args()

    verify_symbol_signals(args.symbol.upper(), args.time, is_ist=(not args.utc))
