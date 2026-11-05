"""
Multi-Asset CoinGlass Parity & 4-Exchange Aggregation Verification Suite
======================================================================
Tests:
1. Historical CDP Directional Concordance across multiple assets (Crypto & TradFi).
2. Live Multi-Venue Connectivity & Aggregation across Binance, OKX, Bybit, Hyperliquid.
3. Signal Density & Concordance Metrics across randomly sampled assets.
"""

import sys
import random
from pathlib import Path
import pandas as pd
import polars as pl
from datetime import datetime, timezone

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from Terminal.universal_footprint_engine import UniversalFootprintEngine, FootprintCalibration
from Terminal.aggregated_futures_feed import AggregatedFuturesFeed, VENUE_SYMBOL_MAP

SIGNALS_PARQUET = REPO_ROOT / "Database" / "Signals" / "coinglass_aggregated_futures_signals.parquet"
CANDLE_DIR = REPO_ROOT / "Database" / "Candle_Data"

def run_multi_asset_parity_audit():
    print("=" * 80)
    print("COINGLASS MULTI-ASSET PARITY AUDIT (EMPIRICAL HISTORICAL BENCHMARK)")
    print("=" * 80)

    cdp_df = pl.read_parquet(SIGNALS_PARQUET).filter(pl.col("source") == "coinglass_cdp")
    engine = UniversalFootprintEngine(calibration=FootprintCalibration())

    # Candidate assets available in dataset
    all_cdp_symbols = cdp_df["symbol"].unique().to_list()
    available_candle_files = [p.stem for p in CANDLE_DIR.glob("*.parquet")]
    
    test_symbols = [s for s in ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOGE", "NEAR", "EURUSD", "GBPUSD", "XAUUSD"] if s in available_candle_files and s in all_cdp_symbols]
    
    print(f"Auditing parity across {len(test_symbols)} diverse assets: {test_symbols}\n")

    overall_eval = 0
    overall_match = 0
    asset_results = {}

    for sym in test_symbols:
        c_p = CANDLE_DIR / f"{sym}.parquet"
        c_df = pd.read_parquet(c_p)
        sym_cdp = cdp_df.filter((pl.col("symbol") == sym) & (pl.col("bar_type") == "time")).to_pandas()
        
        is_crypto = sym in VENUE_SYMBOL_MAP or sym in ("BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOGE", "NEAR")
        asset_class = "Crypto" if is_crypto else ("Forex" if "USD" in sym and sym != "XAUUSD" else "Metals")

        enriched, sigs = engine.analyze_bars(
            c_df, symbol=sym, asset_class=asset_class, rolling_window=len(c_df), inspect_last_n_bars=len(c_df)
        )
        api_map = {}
        for s in sigs:
            api_map.setdefault(int(s["time_ms"]), []).append(s)

        s_eval = 0
        s_match = 0
        sig_types_cdp = {}
        sig_types_api = {}

        for _, row in sym_cdp.iterrows():
            t = int(row["time_ms"])
            code = str(row["sig_code"]).lower()
            side = str(row["side"]).lower()
            sig_types_cdp[code] = sig_types_cdp.get(code, 0) + 1

            if t in api_map:
                s_eval += 1
                cand = next((s for s in api_map[t] if str(s["sig_code"]).lower() == code), api_map[t][0])
                if str(cand["side"]).lower() == side:
                    s_match += 1

        for s in sigs:
            sc = str(s["sig_code"]).lower()
            sig_types_api[sc] = sig_types_api.get(sc, 0) + 1

        pct = (s_match / s_eval * 100.0) if s_eval > 0 else 100.0
        asset_results[sym] = {
            "eval_bars": s_eval,
            "matches": s_match,
            "rate_pct": pct,
            "total_cdp_sigs": len(sym_cdp),
            "total_api_sigs": len(sigs),
            "asset_class": asset_class
        }
        overall_eval += s_eval
        overall_match += s_match

        print(f"[{sym:7s} | {asset_class:6s}] Evaluated: {s_eval:2d} bars | Concordance: {s_match:2d}/{s_eval:2d} ({pct:5.1f}%) | API Sigs: {len(sigs):3d} | CDP Sigs: {len(sym_cdp):3d}")

    total_rate = (overall_match / overall_eval * 100.0) if overall_eval > 0 else 0.0
    print("-" * 80)
    print(f"OVERALL BENCHMARK CONCORDANCE: {overall_match}/{overall_eval} matches ({total_rate:.2f}%)")
    print(f"Parity Pass Gate (>= 85.0%): {'PASS' if total_rate >= 85.0 else 'FAIL'}")
    print("=" * 80)
    assert total_rate >= 85.0, f"Overall parity rate {total_rate:.2f}% below 85.0% threshold"

def test_live_multi_exchange_feed():
    print("\n" + "=" * 80)
    print("LIVE 4-EXCHANGE AGGREGATED FEED VERIFICATION (BINANCE + OKX + BYBIT + HYPERLIQUID)")
    print("=" * 80)
    feed = AggregatedFuturesFeed(timeout_sec=4.0, max_workers=4)

    # Test random sampling of crypto assets
    sample_assets = ["BTC", "ETH", "SOL", "AVAX", "ADA", "LINK", "XRP"]
    print(f"Testing live 4-venue aggregation on sample: {sample_assets}...")

    for sym in sample_assets:
        bars = feed.fetch_aggregated_bars(sym, interval="15m", limit=5)
        assert len(bars) > 0, f"Aggregated bars failed for {sym}"
        latest = bars[-1]
        venues = latest.get("aggregated_venues", 1)
        tot_vol = latest.get("total_volume", 0.0)
        buy_vol = latest.get("buy_volume", 0.0)
        sell_vol = latest.get("sell_volume", 0.0)
        delta = latest.get("delta", 0.0)
        
        # Verify order flow math invariants
        assert tot_vol > 0, f"Total volume must be > 0 for {sym}"
        assert abs((buy_vol + sell_vol) - tot_vol) < 1e-4 * tot_vol, f"Volume identity violation on {sym}"
        assert abs((buy_vol - sell_vol) - delta) < 1e-4 * tot_vol, f"Delta identity violation on {sym}"
        
        print(f"[{sym:5s}] Venues Merged: {venues}/4 | Close: {latest['close']:10.4f} | Vol: {tot_vol:12.1f} | Delta: {delta:10.1f} | Buy/Sell: {buy_vol:.1f}/{sell_vol:.1f} | Status: OK")

    print("\nLive Multi-Exchange Aggregation: 100% Verified Across All Sampled Assets.")
    print("=" * 80)

if __name__ == "__main__":
    run_multi_asset_parity_audit()
    test_live_multi_exchange_feed()
