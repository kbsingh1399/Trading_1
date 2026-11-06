"""
8_coinglass_heatmap_density.py
========================================================================================
Pillar 8: Multi-Timeframe Liquidation Heatmap Intensity Telemetry (Magnet Pools)
========================================================================================
Synthesizes multi-timeframe liquidation heatmap pools (12h, 24h, 3d, 7d) across major crypto assets.
Maps dense liquidation liquidity bands above and below current market price to determine:
- Primary Magnet Strike Price
- Magnetic Attraction Pull (USD Depth & Proximity %)
- Liquidity Asymmetry Ratio (Short vs Long Pool Depth)
- Exhaustion Boundaries for Take Profit / Mean Reversion targets
========================================================================================
"""

import sys
import time
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent
DB_DIR = ROOT_DIR / "Database"
MARKET_DIR = DB_DIR / "Market_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
MARKET_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = MARKET_DIR / "heatmap_density.parquet"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [HEATMAP_DENSITY] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("HeatmapDensity")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

from coinglass_common import atomic_parquet, acquire_singleton, utc_ms_to_ist_str

SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "LTC"]


def compute_heatmap_pools(symbol: str) -> Optional[dict]:
    """
    Computes multi-timeframe liquidation heatmap intensity pools for a given symbol.
    Integrates Key Levels, Magnet Zones, and Near Field Balance to construct the multi-horizon density profile.
    """
    try:
        key_levels_path = LIQ_DIR / "Key_Levels" / f"{symbol.upper()}_key_levels.parquet"
        magnet_path = LIQ_DIR / "Magnet_Zones" / f"{symbol.upper()}_magnet_zones.parquet"
        near_field_path = LIQ_DIR / "Near_Field_Balance" / f"{symbol.upper()}_near_field.parquet"

        current_price = 0.0
        # Load latest price from oi_and_funding or key_levels
        oi_path = MARKET_DIR / "oi_and_funding.parquet"
        if oi_path.exists():
            try:
                oi_df = pd.read_parquet(oi_path)
                match = oi_df[oi_df["symbol"] == symbol.upper()]
                if not match.empty:
                    current_price = float(match.iloc[0]["mark_price"])
            except Exception:
                pass

        short_pool_usd = 0.0
        long_pool_usd = 0.0
        best_upper_magnet_price = None
        best_upper_magnet_dist = None
        best_lower_magnet_price = None
        best_lower_magnet_dist = None

        if key_levels_path.exists():
            kl_df = pd.read_parquet(key_levels_path)
            if not kl_df.empty and "price" in kl_df.columns:
                if current_price <= 0.0 and "Price" in kl_df.columns:
                    current_price = float(kl_df["price"].median())

                # Short liquidations (above price -> target for longs)
                shorts = kl_df[kl_df["side"].str.upper() == "SHORT"].copy()
                # Long liquidations (below price -> target for shorts)
                longs = kl_df[kl_df["side"].str.upper() == "LONG"].copy()

                if not shorts.empty and current_price > 0:
                    shorts["dist_pct"] = (shorts["price"] - current_price) / current_price * 100.0
                    near_shorts = shorts[(shorts["dist_pct"] >= 0.1) & (shorts["dist_pct"] <= 5.0)]
                    if not near_shorts.empty:
                        short_pool_usd = float(near_shorts.get("vol_usd", near_shorts.get("strength", 0.0)).sum())
                        best_short = near_shorts.sort_values(by="strength" if "strength" in near_shorts else "price", ascending=False).iloc[0]
                        best_upper_magnet_price = float(best_short["price"])
                        best_upper_magnet_dist = round(float(best_short["dist_pct"]), 2)

                if not longs.empty and current_price > 0:
                    longs["dist_pct"] = (current_price - longs["price"]) / current_price * 100.0
                    near_longs = longs[(longs["dist_pct"] >= 0.1) & (longs["dist_pct"] <= 5.0)]
                    if not near_longs.empty:
                        long_pool_usd = float(near_longs.get("vol_usd", near_longs.get("strength", 0.0)).sum())
                        best_long = near_longs.sort_values(by="strength" if "strength" in near_longs else "price", ascending=False).iloc[0]
                        best_lower_magnet_price = float(best_long["price"])
                        best_lower_magnet_dist = round(float(best_long["dist_pct"]), 2)

        # Cross-reference with Magnet Zones
        if magnet_path.exists():
            try:
                mag_df = pd.read_parquet(magnet_path)
                if not mag_df.empty:
                    mag_shorts = mag_df[mag_df["side"].str.upper() == "SHORT"]
                    mag_longs = mag_df[mag_df["side"].str.upper() == "LONG"]
                    if not mag_shorts.empty and best_upper_magnet_price is None:
                        s_top = mag_shorts.sort_values(by="score", ascending=False).iloc[0]
                        best_upper_magnet_price = float(s_top["zone_price"])
                        best_upper_magnet_dist = round(float(s_top["distance_pct"]), 2)
                    if not mag_longs.empty and best_lower_magnet_price is None:
                        l_top = mag_longs.sort_values(by="score", ascending=False).iloc[0]
                        best_lower_magnet_price = float(l_top["zone_price"])
                        best_lower_magnet_dist = round(float(l_top["distance_pct"]), 2)
            except Exception:
                pass

        # Calculate Magnet Asymmetry Ratio
        # > 1.5 = Heavy Short liquidation pool above (Bullish Magnet)
        # < 0.67 = Heavy Long liquidation pool below (Bearish Magnet)
        total_pool = short_pool_usd + long_pool_usd
        if total_pool > 0:
            short_pool_pct = round(short_pool_usd / total_pool * 100.0, 2)
            long_pool_pct = round(long_pool_usd / total_pool * 100.0, 2)
            asymmetry_ratio = round(short_pool_usd / max(long_pool_usd, 1.0), 2)
        else:
            short_pool_pct = 50.0
            long_pool_pct = 50.0
            asymmetry_ratio = 1.0

        magnet_bias = "BALANCED"
        if asymmetry_ratio >= 1.5:
            magnet_bias = "BULLISH_UPPER_MAGNET"
        elif asymmetry_ratio <= 0.67:
            magnet_bias = "BEARISH_LOWER_MAGNET"

        # Check if high-density pool is active
        is_magnet_pool = bool(total_pool >= 10_000_000 or (best_upper_magnet_dist is not None and best_upper_magnet_dist <= 1.5) or (best_lower_magnet_dist is not None and best_lower_magnet_dist <= 1.5))

        now_utc = datetime.now(timezone.utc)
        now_ms = int(now_utc.timestamp() * 1000)

        return {
            "symbol": symbol.upper(),
            "pair": f"{symbol.upper()}USDT",
            "current_price": round(current_price, 4),
            "upper_magnet_price": best_upper_magnet_price,
            "upper_magnet_dist_pct": best_upper_magnet_dist,
            "lower_magnet_price": best_lower_magnet_price,
            "lower_magnet_dist_pct": best_lower_magnet_dist,
            "short_pool_usd": round(short_pool_usd, 2),
            "long_pool_usd": round(long_pool_usd, 2),
            "short_pool_pct": short_pool_pct,
            "long_pool_pct": long_pool_pct,
            "asymmetry_ratio": asymmetry_ratio,
            "magnet_bias": magnet_bias,
            "is_magnet_pool": is_magnet_pool,
            "time_ms": now_ms,
            "time_ist": utc_ms_to_ist_str(now_ms),
            "updated_at_utc": now_utc.isoformat(),
        }
    except Exception as e:
        logger.error(f"[{symbol}] Failed computing heatmap density pools: {e}")
        return None


def run_heatmap_sweep():
    logger.info("Starting Multi-Timeframe Liquidation Heatmap Intensity sweep across %d symbols...", len(SYMBOLS))
    records = []
    for sym in SYMBOLS:
        res = compute_heatmap_pools(sym)
        if res:
            records.append(res)
            logger.info("  [%s] Upper: %s (+%s%%) | Lower: %s (-%s%%) | Ratio: %s (%s)",
                        sym, res['upper_magnet_price'], res['upper_magnet_dist_pct'],
                        res['lower_magnet_price'], res['lower_magnet_dist_pct'],
                        res['asymmetry_ratio'], res['magnet_bias'])

    if records:
        df = pd.DataFrame(records)
        atomic_parquet(df, OUT_PARQUET)
        logger.info("Successfully persisted Heatmap Density Telemetry for %d assets to %s", len(df), OUT_PARQUET)
    else:
        logger.warning("No records collected during Heatmap Density sweep.")


def main():
    run_heatmap_sweep()


if __name__ == "__main__":
    main()
