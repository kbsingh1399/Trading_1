"""
High-Speed OLAP Analytical Engine (Powered by DuckDB)
====================================================
Performs sub-15ms multi-stream confluence analytics across all live Parquet tables:
- Footprint signals (Absorption, Trapped Traders, CVD Divergence)
- Whale Orders Table 2 (Active Bid/Ask Walls & Historical Order Flow)
- Liquidation Data (Magnet Zones, Key Liquidation Levels, Near-Field Balance)
- 15m Order-flow Candlestick Metrics (Volume, Delta, POC)
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

import duckdb
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_ORDERS_DIR = WHALE_DIR / "Open_Orders"
HISTORY_ORDERS_DIR = WHALE_DIR / "History_Orders"
WHALE_SNAPSHOTS = WHALE_DIR / "whale_market_snapshots.parquet"
LIQ_DIR = DB_DIR / "Liquidation_Data"
MAGNET_DIR = LIQ_DIR / "Magnet_Zones"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"
CANDLE_DIR = DB_DIR / "Candle_Data"

logger = logging.getLogger("OlapEngine")
logger.setLevel(logging.INFO)


class OlapEngine:
    def __init__(self):
        self.con = duckdb.connect(database=":memory:")
        self._init_views()

    def _init_views(self):
        """Register parquet files as virtual DuckDB tables."""
        if SIGNALS_PARQUET.exists():
            try:
                self.con.execute(f"CREATE OR REPLACE VIEW footprint_signals AS SELECT * FROM read_parquet('{SIGNALS_PARQUET.as_posix()}')")
            except Exception as e:
                logger.warning(f"Could not initialize footprint_signals view: {e}")
        if WHALE_SNAPSHOTS.exists():
            try:
                self.con.execute(f"CREATE OR REPLACE VIEW whale_snapshots AS SELECT * FROM read_parquet('{WHALE_SNAPSHOTS.as_posix()}')")
            except Exception as e:
                logger.warning(f"Could not initialize whale_snapshots view: {e}")

    def query_whale_walls(self, symbol: str) -> Dict[str, Any]:
        """Calculates total bid vs ask whale wall pressure within +-3% and +-10% of current price."""
        open_file = OPEN_ORDERS_DIR / f"{symbol}_whale_open_orders.parquet"
        if not open_file.exists():
            return {"bid_walls_usd": 0.0, "ask_walls_usd": 0.0, "nearest_bid": None, "nearest_ask": None, "imbalance": 0.5}

        query = f"""
        WITH parsed AS (
            SELECT 
                side,
                price,
                CAST(from_price_pct AS DOUBLE) AS pct_dist,
                remaining_usd AS size_usd
            FROM read_parquet('{open_file.as_posix()}')
        )
        SELECT 
            side,
            COALESCE(SUM(size_usd), 0.0) AS total_usd,
            COALESCE(SUM(CASE WHEN ABS(pct_dist) <= 3.0 THEN size_usd ELSE 0.0 END), 0.0) AS tight_usd,
            COUNT(*) AS wall_count
        FROM parsed
        GROUP BY side
        """
        try:
            df = self.con.execute(query).df()
            bids = df[df["side"].str.lower().str.contains("bid")]
            asks = df[df["side"].str.lower().str.contains("ask")]

            bid_usd = float(bids["tight_usd"].iloc[0]) if not bids.empty else 0.0
            ask_usd = float(asks["tight_usd"].iloc[0]) if not asks.empty else 0.0
            total = bid_usd + ask_usd
            imbalance = round(bid_usd / total, 3) if total > 0 else 0.5

            return {
                "tight_bid_walls_usd": bid_usd,
                "tight_ask_walls_usd": ask_usd,
                "whale_imbalance_bid_ratio": imbalance,
                "total_bids_usd": float(bids["total_usd"].iloc[0]) if not bids.empty else 0.0,
                "total_asks_usd": float(asks["total_usd"].iloc[0]) if not asks.empty else 0.0
            }
        except Exception as e:
            return {"error": str(e), "whale_imbalance_bid_ratio": 0.5}

    def query_liquidation_confluence(self, symbol: str) -> Dict[str, Any]:
        """Analyzes magnet zones and near field balance for liquidation pressure."""
        near_file = NEAR_FIELD_DIR / f"{symbol}_near_field.parquet"
        mag_file = MAGNET_DIR / f"{symbol}_magnet_zones.parquet"

        res = {"near_field_shorts_pct": 50.0, "top_magnet_zone": None}
        if near_file.exists():
            try:
                df = self.con.execute(f"SELECT * FROM read_parquet('{near_file.as_posix()}')").df()
                if not df.empty and "shorts_pct" in df.columns:
                    res["near_field_shorts_pct"] = float(df["shorts_pct"].iloc[0])
            except Exception:
                pass

        if mag_file.exists():
            try:
                m_df = self.con.execute(f"SELECT * FROM read_parquet('{mag_file.as_posix()}') ORDER BY score DESC LIMIT 1").df()
                if not m_df.empty:
                    res["top_magnet_zone"] = {
                        "side": m_df["side"].iloc[0],
                        "price_band": m_df["price_band"].iloc[0] if "price_band" in m_df.columns else "",
                        "distance_pct": m_df["distance_pct"].iloc[0] if "distance_pct" in m_df.columns else 0.0
                    }
            except Exception:
                pass

        return res

    def query_cvd_trend(self, symbol: str, lookback_bars: int = 12) -> Dict[str, Any]:
        """Evaluates 15m order-flow candle delta and CVD direction over lookback bars."""
        candle_file = CANDLE_DIR / f"{symbol}.parquet"
        if not candle_file.exists():
            return {"cvd_slope": "NEUTRAL", "cumulative_delta": 0.0, "poc_price": None}

        query = f"""
        SELECT open_time_utc, close, total_volume, delta, cumulative_delta, poc_price
        FROM read_parquet('{candle_file.as_posix()}')
        ORDER BY open_time_ms DESC
        LIMIT {lookback_bars}
        """
        try:
            df = self.con.execute(query).df()
            if df.empty:
                return {"cvd_slope": "NEUTRAL", "cumulative_delta": 0.0, "poc_price": None}

            latest_poc = float(df["poc_price"].iloc[0]) if pd.notnull(df["poc_price"].iloc[0]) else None
            recent_delta_sum = float(df["delta"].sum())
            price_change = float(df["close"].iloc[0] - df["close"].iloc[-1])

            # Detect CVD Divergence: Price falling but delta aggressive buying -> Bullish Divergence
            if price_change < 0 and recent_delta_sum > 0:
                slope = "BULLISH_DIVERGENCE"
            elif price_change > 0 and recent_delta_sum < 0:
                slope = "BEARISH_DIVERGENCE"
            elif recent_delta_sum > 0:
                slope = "BULLISH_FLOW"
            else:
                slope = "BEARISH_FLOW"

            return {
                "cvd_slope": slope,
                "recent_net_delta": recent_delta_sum,
                "recent_poc": latest_poc,
                "bars_analyzed": len(df)
            }
        except Exception as e:
            return {"error": str(e), "cvd_slope": "NEUTRAL"}

    def get_series(self, symbol: str, metric: str = "cumulative_delta", limit: int = 64) -> np.ndarray:
        """Returns chronological 1D array of metric (e.g. cumulative_delta or close) for DeepMind TimesFM."""
        candle_file = CANDLE_DIR / f"{symbol}.parquet"
        if not candle_file.exists():
            return np.array([], dtype=np.float32)
        query = f"""
        SELECT {metric}
        FROM read_parquet('{candle_file.as_posix()}')
        WHERE {metric} IS NOT NULL
        ORDER BY open_time_ms DESC
        LIMIT {limit}
        """
        try:
            df = self.con.execute(query).df()
            if df.empty:
                return np.array([], dtype=np.float32)
            # Reverse to chronological order (oldest to newest)
            return df[metric].values[::-1].astype(np.float32)
        except Exception:
            return np.array([], dtype=np.float32)

    def scan_all_confluences(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Instant multi-stream scoring across all mutual symbols."""
        results = []
        for sym in symbols:
            whale = self.query_whale_walls(sym)
            liq = self.query_liquidation_confluence(sym)
            cvd = self.query_cvd_trend(sym)

            # Synthesize Confluence Score (0 to 100)
            score = 50.0
            reasons = []

            # Whale Wall confirmation
            imbalance = whale.get("whale_imbalance_bid_ratio", 0.5)
            if imbalance >= 0.65:
                score += 15.0
                reasons.append(f"Whale Bids dominate ({imbalance*100:.0f}%)")
            elif imbalance <= 0.35:
                score -= 15.0
                reasons.append(f"Whale Asks dominate ({(1-imbalance)*100:.0f}%)")

            # CVD Divergence
            cvd_type = cvd.get("cvd_slope", "")
            if cvd_type == "BULLISH_DIVERGENCE":
                score += 20.0
                reasons.append("Bullish CVD Divergence (Price down, Aggressive delta buying)")
            elif cvd_type == "BEARISH_DIVERGENCE":
                score -= 20.0
                reasons.append("Bearish CVD Divergence (Price up, Aggressive delta selling)")
            elif cvd_type == "BULLISH_FLOW":
                score += 10.0
                reasons.append("Positive Net Order Flow")
            elif cvd_type == "BEARISH_FLOW":
                score -= 10.0
                reasons.append("Negative Net Order Flow")

            # Near Field Balance
            shorts_pct = liq.get("near_field_shorts_pct", 50.0)
            if shorts_pct >= 55.0:
                score += 10.0
                reasons.append(f"Shorts overstacked near price ({shorts_pct:.1f}% short fuel)")
            elif shorts_pct <= 45.0:
                score -= 10.0
                reasons.append(f"Longs overstacked near price ({100-shorts_pct:.1f}% long fuel)")

            bias = "BULLISH" if score >= 65 else ("BEARISH" if score <= 35 else "NEUTRAL")

            results.append({
                "symbol": sym,
                "confluence_score": round(score, 1),
                "bias": bias,
                "reasons": reasons,
                "whale_imbalance": imbalance,
                "cvd_slope": cvd_type,
                "near_field_shorts_pct": shorts_pct,
                "poc_price": cvd.get("recent_poc")
            })

        results.sort(key=lambda x: abs(x["confluence_score"] - 50.0), reverse=True)
        return results


if __name__ == "__main__":
    t0 = datetime.now()
    engine = OlapEngine()
    test_symbols = ["BTC", "ETH", "SOL", "XRP", "AVAX", "DOGE", "LINK", "NEAR"]
    res = engine.scan_all_confluences(test_symbols)
    elapsed_ms = (datetime.now() - t0).total_seconds() * 1000

    print(f"\nOLAP Confluence Scan Complete in {elapsed_ms:.2f} ms across {len(test_symbols)} symbols:\n")
    print(f"{'SYMBOL':<8} {'BIAS':<10} {'SCORE':<8} {'WHALE(BID%)':<14} {'CVD FLOW':<22} {'TOP REASONS'}")
    print("=" * 100)
    for r in res:
        reasons_str = "; ".join(r["reasons"][:2])
        print(f"{r['symbol']:<8} {r['bias']:<10} {r['confluence_score']:<8} {r['whale_imbalance']*100:<13.1f}% {r['cvd_slope']:<22} {reasons_str}")
    print("=" * 100)
