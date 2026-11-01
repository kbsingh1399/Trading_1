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
import re
from Terminal.runtime_io import completed_candles
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

    def query_whale_walls(self, symbol):
        try:
            self._validate_symbol(symbol)
            frame = pd.read_parquet(OPEN_ORDERS_DIR / f'{symbol}_whale_open_orders.parquet')
            frame = self._latest_snapshot(frame)
            age = (datetime.now(timezone.utc) - pd.to_datetime(frame.snapshot_time_utc.iloc[0], utc=True)).total_seconds()
            if not 0 <= age <= 1800:
                raise ValueError('Whale snapshot stale or future')
            for col in ['price', 'remaining_usd', 'from_price_pct']:
                frame[col] = pd.to_numeric(frame[col], errors='raise')
                if not np.isfinite(frame[col]).all():
                    raise ValueError('Nonfinite whale depth')
            if (frame.remaining_usd < 0).any():
                raise ValueError('Negative whale depth')
            side = frame.side.str.lower()
            bids = frame[side.str.contains('bid')]
            asks = frame[side.str.contains('ask')]
            bid = float(bids.loc[bids.from_price_pct.abs() <= 3, 'remaining_usd'].sum())
            ask = float(asks.loc[asks.from_price_pct.abs() <= 3, 'remaining_usd'].sum())
            if bid + ask <= 0:
                raise ValueError('No usable whale depth')
            return {'data_valid': True, 'tight_bid_walls_usd': bid, 'tight_ask_walls_usd': ask,
                    'whale_imbalance_bid_ratio': bid / (bid + ask),
                    'total_bids_usd': float(bids.remaining_usd.sum()), 'total_asks_usd': float(asks.remaining_usd.sum()),
                    'age_seconds': age}
        except Exception as exc:
            return {'data_valid': False, 'error': str(exc), 'whale_imbalance_bid_ratio': 0.5}

    def query_liquidation_confluence(self, symbol):
        result = {'data_valid': False, 'near_field_shorts_pct': 50.0, 'top_magnet_zone': None}
        try:
            self._validate_symbol(symbol)
            frame = self._latest_snapshot(pd.read_parquet(NEAR_FIELD_DIR / f'{symbol}_near_field.parquet'))
            value = float(frame.shorts_pct.iloc[-1])
            if not np.isfinite(value) or not 0 <= value <= 100:
                raise ValueError('Invalid near-field percentage')
            result.update(data_valid=True, near_field_shorts_pct=value)
            path = MAGNET_DIR / f'{symbol}_magnet_zones.parquet'
            if path.exists():
                magnets = self._latest_snapshot(pd.read_parquet(path)).sort_values('score', ascending=False)
                if not magnets.empty:
                    result['top_magnet_zone'] = magnets.iloc[0].to_dict()
        except Exception as exc:
            result.update(data_valid=False, error=str(exc))
        return result

    def query_cvd_trend(self, symbol, lookback_bars=12):
        try:
            frame = self.get_forecast_frame(symbol, lookback_bars)
            delta = float(frame.delta.sum())
            change = float(frame.close.iloc[-1] - frame.close.iloc[0])
            # Price/tape disagreement is unresolved absorption, not proof of reversal.
            if change * delta < 0:
                slope = 'PRICE_DELTA_DISAGREEMENT'
            else:
                slope = 'BULLISH_FLOW' if delta > 0 else 'BEARISH_FLOW' if delta < 0 else 'NEUTRAL'
            poc = float(frame.poc_price.iloc[-1])
            return {'data_valid': True, 'cvd_slope': slope, 'recent_net_delta': delta,
                    'recent_poc': poc if np.isfinite(poc) and poc > 0 else None, 'bars_analyzed': len(frame)}
        except Exception as exc:
            return {'data_valid': False, 'error': str(exc), 'cvd_slope': 'NEUTRAL'}

    def get_series(self, symbol, metric='cumulative_delta', limit=64):
        if metric not in {'cumulative_delta', 'close', 'total_volume', 'delta'}:
            raise ValueError('Unsupported forecast metric')
        try:
            return self.get_forecast_frame(symbol, limit)[metric].to_numpy(dtype=np.float64)
        except Exception:
            return np.array([], dtype=np.float64)

    @staticmethod
    def _validate_symbol(symbol):
        if not isinstance(symbol, str) or re.fullmatch(r'[A-Z0-9_]+', symbol) is None:
            raise ValueError('Invalid asset symbol')

    @staticmethod
    def _latest_snapshot(frame):
        if frame.empty or 'snapshot_time_utc' not in frame:
            raise ValueError('Missing snapshot timestamp')
        times = pd.to_datetime(frame.snapshot_time_utc, utc=True, errors='raise')
        age = (datetime.now(timezone.utc) - times.max()).total_seconds()
        if not 0 <= age <= 1800:
            raise ValueError('Snapshot stale or future')
        return frame.loc[times == times.max()].copy()

    def get_forecast_frame(self, symbol, limit=64):
        self._validate_symbol(symbol)
        if not isinstance(limit, int) or not 2 <= limit <= 10000:
            raise ValueError('Invalid context length')
        frame = completed_candles(pd.read_parquet(CANDLE_DIR / f'{symbol}.parquet')).tail(limit)
        for col in ['close', 'cumulative_delta', 'total_volume', 'delta']:
            frame[col] = pd.to_numeric(frame[col], errors='raise')
            if not np.isfinite(frame[col]).all():
                raise ValueError('Nonfinite candle input')
        if (frame.close <= 0).any() or (frame.total_volume < 0).any():
            raise ValueError('Invalid price or volume')
        return frame

    def close(self):
        self.con.close()

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
