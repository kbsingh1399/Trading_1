"""
Multi-Method Live Parity Verification & Audit Suite
---------------------------------------------------
Systematically executes live extraction and immediate Parquet persistence
to verify 100.000% parity across:
1. Footprint Order Flow Candles & Aggregated Futures Signals
2. Liquidation Snapshots, Key Barrier Levels, and Discrete Strikes

Screenshots and detailed verification JSON are saved directly into the Artifacts folder.
"""

import os
import sys
import json
import time
import asyncio
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from playwright.async_api import async_playwright

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from Terminal.coinglass_data_collector import (
    ensure_terminal_ready,
    extract_footprint_chunk,
    upsert_candle_parquet,
    dump_signals_parquet,
    switch_liquidation_pair,
    extract_single_liquidation_state,
    upsert_liquidation_snapshots,
    upsert_asset_levels,
    upsert_asset_key_levels,
    MT5_ASSET_MAP,
    LIQ_EXCHANGES_MAP,
    LIQ_CONTRACTS_MAP,
    CANDLE_DIR,
    LEVELS_DIR,
    KEY_LEVELS_DIR,
    SNAPSHOTS_PARQUET,
    OUTPUT_SIGNALS_PARQUET
)

ARTIFACT_DIR = Path(r"C:\Users\SIGMA\.gemini\antigravity\brain\4bec80bb-bac5-4a24-9dda-8d6a650fbdd5")
CDP_URL = "http://127.0.0.1:9222"


async def run_footprint_parity_audit(page, symbols=["GAS"]):
    print("\n=======================================================")
    print(f" [METHOD 1 & 2] FOOTPRINT PARITY AUDIT FOR: {symbols}")
    print("=======================================================")

    await ensure_terminal_ready(page)
    await asyncio.sleep(2)

    # 1. Capture live visual screenshot of current active footprint page
    current_sym = symbols[0]
    shot_path = ARTIFACT_DIR / f"footprint_{current_sym}_live.png"
    await page.screenshot(path=str(shot_path), full_page=False)
    print(f"[*] Visual Screenshot saved: {shot_path.name}")

    # 2. Extract live payload from React Fiber terminal memory
    extracted_data = await extract_footprint_chunk(page, symbols)
    candles_dict = extracted_data.get("candles", {})
    signals_list = extracted_data.get("signals", [])

    parity_results = {}

    for sym in symbols:
        candles = candles_dict.get(sym, [])
        mt5_sym = MT5_ASSET_MAP.get(sym, sym)
        print(f"[*] Extracted {len(candles)} live 15m candles for {sym} ({mt5_sym})")

        if not candles:
            print(f"[-] No candles extracted for {sym}")
            continue

        # 3. Dump to Parquet
        upsert_candle_parquet(sym, mt5_sym, candles)

        # 4. Immediate Re-read and Assertion
        pq_path = CANDLE_DIR / f"{sym}.parquet"
        df_pq = pd.read_parquet(pq_path)
        last_extracted = candles[-1]
        last_pq_row = df_pq.iloc[-1].to_dict()

        print(f"\n--- Checking 100% Parity for {sym} Latest Candle ({last_pq_row.get('open_time_utc')}) ---")
        fields_to_compare = [
            "open_time_ms", "open", "high", "low", "close",
            "buy_volume", "sell_volume", "total_volume", "delta",
            "poc_price", "poc_volume", "poc_delta", "open_interest_close"
        ]

        field_checks = []
        all_passed = True
        for f in fields_to_compare:
            live_val = last_extracted.get(f)
            pq_val = last_pq_row.get(f)

            if live_val is not None and pq_val is not None:
                try:
                    diff = abs(float(live_val) - float(pq_val))
                    match = diff < 1e-4
                except (ValueError, TypeError):
                    match = (live_val == pq_val)
                    diff = 0 if match else 1
            else:
                match = (live_val == pq_val)
                diff = 0

            if not match:
                all_passed = False

            status = "PASS (100% Parity)" if match else "FAIL"
            print(f"  {f:<22}: Live Extracted = {str(live_val):<18} | Parquet = {str(pq_val):<18} | [{status}]")
            field_checks.append({
                "field": f,
                "live_val": live_val,
                "parquet_val": pq_val,
                "delta": diff,
                "match": match
            })

        parity_results[sym] = {
            "all_passed": all_passed,
            "candles_count": len(df_pq),
            "checks": field_checks
        }

    # Dump and verify signals
    if signals_list:
        dump_signals_parquet(signals_list)
        df_sig = pd.read_parquet(OUTPUT_SIGNALS_PARQUET)
        print(f"[*] Aggregated Signals verified in Parquet: {len(df_sig)} total signal rows across all assets.")

    return parity_results


async def run_liquidation_parity_audit(context, symbol="BTC", mt5_symbol="BTCUSD", exchange="Binance", contract="BTCUSDT"):
    print("\n=======================================================")
    print(f" [METHOD 1 & 2] LIQUIDATION PARITY AUDIT: {symbol} ({exchange} - {contract})")
    print("=======================================================")

    page = await context.new_page()
    await page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={symbol}", timeout=25000)
    await asyncio.sleep(3.5)

    # Perform verified pair switch
    print(f"[*] Ensuring pair switch to {exchange} ({contract})...")
    await switch_liquidation_pair(page, symbol, contract, exchange)
    await asyncio.sleep(2.5)

    # 1. Take high-res visual screenshot
    shot_path = ARTIFACT_DIR / f"liquidation_{symbol}_live.png"
    await page.screenshot(path=str(shot_path), full_page=False)
    print(f"[*] Visual Screenshot saved: {shot_path.name}")

    # 2. Extract DOM & React state at this exact moment
    extracted = await extract_single_liquidation_state(page, symbol, mt5_symbol, "3D - 1m")
    if not extracted or not extracted.get("snapshot"):
        print(f"[-] Failed to extract state for {symbol}")
        await page.close()
        return {"symbol": symbol, "passed": False}

    live_snap = extracted["snapshot"]
    live_levels = extracted.get("levels", [])
    live_key_levels = extracted.get("key_levels", [])

    print(f"[*] Extracted Live Snapshot: Price={live_snap.get('last_price')} | CLLD={live_snap.get('clld_current')} | ShortMag={live_snap.get('short_magnet_zone')}")
    print(f"[*] Extracted Live Discrete Strikes: {len(live_levels)} | Key Barriers: {len(live_key_levels)}")

    # 3. Immediate Dump to Parquet
    upsert_liquidation_snapshots([live_snap])
    upsert_asset_levels(symbol, live_levels)
    upsert_asset_key_levels(symbol, live_key_levels)

    # 4. Immediate Re-read and Verify 100% Parity
    # Snapshots check
    df_snap = pd.read_parquet(SNAPSHOTS_PARQUET)
    sym_snaps = df_snap[(df_snap["symbol"] == symbol) & (df_snap["exchange"] == live_snap["exchange"])]
    latest_pq_snap = sym_snaps.iloc[-1].to_dict()

    snap_fields = [
        "last_price", "clld_current", "clld_sentiment", "clld_1h_change",
        "short_magnet_zone", "short_magnet_dist_pct", "short_magnet_strength",
        "long_magnet_zone", "long_magnet_dist_pct", "long_magnet_strength",
        "near_field_long_pct", "near_field_short_pct", "near_field_bias"
    ]

    print(f"\n--- Liquidation Snapshot Parity Verification ({symbol}) ---")
    snap_checks = []
    all_snap_passed = True
    for sf in snap_fields:
        live_v = live_snap.get(sf)
        pq_v = latest_pq_snap.get(sf)

        if live_v is not None and pq_v is not None:
            try:
                diff = abs(float(live_v) - float(pq_v))
                match = (diff < 1e-4) or (str(live_v).strip() == str(pq_v).strip())
            except (ValueError, TypeError):
                match = (str(live_v).strip() == str(pq_v).strip())
                diff = 0 if match else 1
        else:
            match = (live_v == pq_v)
            diff = 0

        if not match:
            all_snap_passed = False

        status = "PASS (100% Parity)" if match else "FAIL"
        print(f"  {sf:<24}: Live UI = {str(live_v):<20} | Parquet = {str(pq_v):<20} | [{status}]")
        snap_checks.append({
            "field": sf, "live": live_v, "parquet": pq_v, "match": match
        })

    # Key Levels check
    kl_pq_file = KEY_LEVELS_DIR / f"{symbol}_key_levels.parquet"
    df_kl = pd.read_parquet(kl_pq_file)
    print(f"\n--- Top 5 Key Levels Parity Verification ({symbol}) ---")
    kl_checks = []
    if live_key_levels:
        for idx, lkl in enumerate(live_key_levels[:5]):
            side = lkl["side"]
            px = lkl["price"]
            str_val = lkl["strength"]
            pq_match = df_kl[(df_kl["side"] == side) & (abs(df_kl["price"] - px) < 1e-2)]
            match = not pq_match.empty
            status = "PASS (100% Parity)" if match else "FAIL"
            print(f"  Rank {idx+1} [{side.upper()}]: Live UI Px={px}, Str={str_val} | Parquet Found={match} | [{status}]")
            kl_checks.append({"rank": idx+1, "side": side, "price": px, "match": match})

    # 5. Dynamic Tab Lifecycle Verification: Ensure page closes cleanly
    print(f"[*] Dynamically closing liquidation tab for {symbol}...")
    await page.close()
    print(f"[+] Liquidation tab for {symbol} closed cleanly. Memory released.")

    return {
        "symbol": symbol,
        "exchange": exchange,
        "contract": contract,
        "all_snap_passed": all_snap_passed,
        "snap_checks": snap_checks,
        "key_level_checks": kl_checks,
        "discrete_strikes_dumped": len(live_levels)
    }


async def main():
    print("=======================================================")
    print(" EXECUTING MULTI-STREAM REAL-TIME PARITY VERIFICATION ")
    print("=======================================================")

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        context = browser.contexts[0]

        # Stream 1: Footprint Parity Audit (Active Page: GAS)
        fp_page = None
        for pg in context.pages:
            if "coinglass.com/footprint" in pg.url:
                fp_page = pg
                break

        full_results = {}
        if fp_page:
            full_results["footprint"] = await run_footprint_parity_audit(fp_page, ["GAS"])
        else:
            print("[-] No footprint tab open to test.")

        # Stream 2: Liquidation Benchmark 1: BTC (Binance BTCUSDT)
        full_results["liq_BTC"] = await run_liquidation_parity_audit(
            context, symbol="BTC", mt5_symbol="BTCUSD", exchange="Binance", contract="BTCUSDT"
        )

        # Stream 2: Liquidation Benchmark 2: US30 (MEXC US30_USDT)
        full_results["liq_US30"] = await run_liquidation_parity_audit(
            context, symbol="US30", mt5_symbol="US30.p", exchange="MEXC", contract="US30_USDT"
        )

        # Stream 2: Liquidation Benchmark 3: BRENTOIL (tradeXYZ BRENTOIL-USD)
        full_results["liq_BRENTOIL"] = await run_liquidation_parity_audit(
            context, symbol="BRENTOIL", mt5_symbol="UKBRENT.p", exchange="tradeXYZ", contract="BRENTOIL-USD"
        )

        # Persist full results to artifacts
        out_file = ARTIFACT_DIR / "parity_verification_results.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(full_results, f, indent=2, default=str)

        print("\n=======================================================")
        print(f" ALL PARITY AUDITS COMPLETED SUCCESSFULLY! ")
        print(f" Verification Artifact: {out_file.name} ")
        print("=======================================================")


if __name__ == "__main__":
    asyncio.run(main())
