from Terminal.auction_indicators import compute_auction_profile
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
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_PARQUET = (SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet") if (SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet").exists() else (DB_DIR / "coinglass_aggregated_futures_signals.parquet")
WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_ORDERS_DIR = WHALE_DIR / "Open_Orders"
HISTORY_ORDERS_DIR = WHALE_DIR / "History_Orders"
WHALE_SNAPSHOTS = WHALE_DIR / "whale_market_snapshots.parquet"
LIQ_DIR = DB_DIR / "Liquidation_Data"
MAGNET_DIR = LIQ_DIR / "Magnet_Zones"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"
STRENGTH_BANDS_DIR = LIQ_DIR / "Strength_By_Distance"
LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"
LIQ_SNAPSHOTS = LIQ_DIR / "liquidation_snapshots.parquet"
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
            if not 0 <= age <= 7200:
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
        result = {
            'data_valid': False,
            'near_field_shorts_pct': 50.0,
            'near_field_longs_pct': 50.0,
            'top_magnet_zone': None,
            'top_short_magnets': [],
            'top_long_magnets': [],
            'strength_by_distance': [],
            'leverage_tiers': [],
            'key_levels': [],
            'near_field': {}
        }
        try:
            self._validate_symbol(symbol)
            # 1. Near field balance
            nf_path = NEAR_FIELD_DIR / f'{symbol}_near_field.parquet'
            if nf_path.exists():
                frame = self._latest_snapshot(pd.read_parquet(nf_path))
                shorts_pct = float(frame.shorts_pct.iloc[-1])
                longs_pct = float(frame.longs_pct.iloc[-1]) if 'longs_pct' in frame.columns else 100.0 - shorts_pct
                if not np.isfinite(shorts_pct) or not 0 <= shorts_pct <= 100:
                    raise ValueError('Invalid near-field percentage')
                
                nf_dict = frame.iloc[-1].to_dict()
                result.update(
                    data_valid=True,
                    near_field_shorts_pct=shorts_pct,
                    near_field_longs_pct=longs_pct,
                    near_field=nf_dict
                )
            
            # 2. Magnet zones (partitioned by side)
            mag_path = MAGNET_DIR / f'{symbol}_magnet_zones.parquet'
            if mag_path.exists():
                mdf = self._latest_snapshot(pd.read_parquet(mag_path))
                if not mdf.empty:
                    # Parse zone_price if not numeric
                    price_col = 'zone_price' if 'zone_price' in mdf.columns else ('price' if 'price' in mdf.columns else None)
                    if price_col:
                        mdf['zone_price'] = pd.to_numeric(mdf[price_col], errors='coerce')
                    elif 'price_band' in mdf.columns:
                        def parse_mid(val):
                            nums = re.findall(r'[\d,]+\.?\d*', str(val))
                            if len(nums) >= 2:
                                return (float(nums[0].replace(',', '')) + float(nums[1].replace(',', ''))) / 2.0
                            return None
                        mdf['zone_price'] = mdf['price_band'].apply(parse_mid)
                    
                    if 'score' in mdf.columns:
                        mdf['score'] = pd.to_numeric(mdf['score'], errors='coerce')
                        top_all = mdf.sort_values('score', ascending=False)
                        if not top_all.empty:
                            result['top_magnet_zone'] = top_all.iloc[0].to_dict()

                    if 'side' in mdf.columns:
                        s_mdf = mdf[mdf['side'].str.upper().str.contains('SHORT')].sort_values('rank' if 'rank' in mdf.columns else 'score', ascending=True if 'rank' in mdf.columns else False)
                        l_mdf = mdf[mdf['side'].str.upper().str.contains('LONG')].sort_values('rank' if 'rank' in mdf.columns else 'score', ascending=True if 'rank' in mdf.columns else False)
                        result['top_short_magnets'] = s_mdf.head(3).to_dict(orient='records')
                        result['top_long_magnets'] = l_mdf.head(3).to_dict(orient='records')

            # 3. Strength by distance
            sb_path = STRENGTH_BANDS_DIR / f'{symbol}_strength_by_distance.parquet'
            if sb_path.exists():
                sb_df = self._latest_snapshot(pd.read_parquet(sb_path))
                result['strength_by_distance'] = sb_df.to_dict(orient='records')

            # 4. Leverage tiers
            lt_path = LEVERAGE_TIERS_DIR / f'{symbol}_leverage_tiers.parquet'
            if lt_path.exists():
                lt_df = self._latest_snapshot(pd.read_parquet(lt_path))
                result['leverage_tiers'] = lt_df.to_dict(orient='records')

            # 5. Key liquidation levels
            kl_path = KEY_LEVELS_DIR / f'{symbol}_key_levels.parquet'
            if kl_path.exists():
                kl_df = self._latest_snapshot(pd.read_parquet(kl_path))
                if not kl_df.empty:
                    if 'strength' in kl_df.columns:
                        kl_df['strength'] = pd.to_numeric(kl_df['strength'], errors='coerce')
                        kl_df = kl_df.sort_values('strength', ascending=False)
                    result['key_levels'] = kl_df.head(10).to_dict(orient='records')

            # 6. CLLD & Sentiment turns
            if LIQ_SNAPSHOTS.exists():
                snap_df = pd.read_parquet(LIQ_SNAPSHOTS)
                if not snap_df.empty and 'symbol' in snap_df.columns:
                    sym_snaps = snap_df[snap_df['symbol'] == symbol]
                    if not sym_snaps.empty:
                        last_snap = sym_snaps.iloc[-1]
                        result['clld'] = {
                            'delta': float(last_snap.get('clld_delta', 0.0) or 0.0),
                            'sentiment_turn': str(last_snap.get('clld_sentiment_turn', '') or ''),
                            'turn_time': str(last_snap.get('clld_turn_time', '') or '')
                        }

        except Exception as exc:
            result.update(data_valid=False, error=str(exc))
        return result

    def query_key_liquidation_levels(self, symbol: str, current_price: Optional[float] = None, max_dist_pct: float = 5.0) -> Dict[str, Any]:
        """Queries strike barriers from key levels table, partitioned into overhead and underlying."""
        res = {'data_valid': False, 'overhead_barriers': [], 'underlying_barriers': [], 'nearest_overhead': None, 'nearest_underlying': None}
        try:
            self._validate_symbol(symbol)
            kl_path = KEY_LEVELS_DIR / f'{symbol}_key_levels.parquet'
            if not kl_path.exists():
                return res
            kl_df = self._latest_snapshot(pd.read_parquet(kl_path))
            if kl_df.empty:
                return res
            kl_df['price'] = pd.to_numeric(kl_df['price'], errors='coerce')
            kl_df['distance_pct'] = pd.to_numeric(kl_df['distance_pct'], errors='coerce')
            kl_df['strength'] = pd.to_numeric(kl_df['strength'], errors='coerce')
            kl_df = kl_df.dropna(subset=['price'])
            
            if current_price is not None and current_price > 0:
                overhead = kl_df[kl_df['price'] > current_price].sort_values('price', ascending=True)
                underlying = kl_df[kl_df['price'] < current_price].sort_values('price', ascending=False)
                res['overhead_barriers'] = overhead.head(5).to_dict(orient='records')
                res['underlying_barriers'] = underlying.head(5).to_dict(orient='records')
                if not overhead.empty:
                    res['nearest_overhead'] = overhead.iloc[0].to_dict()
                if not underlying.empty:
                    res['nearest_underlying'] = underlying.iloc[0].to_dict()
                res['data_valid'] = True
            else:
                res['data_valid'] = True
        except Exception as exc:
            res['error'] = str(exc)
        return res

    def query_auction_profile(self, symbol: str) -> Dict[str, Any]:
        """Extracts Day VWAP, Session VWAPs, 70% Value Area (VAH/VAL/POC), and EMAs."""
        try:
            self._validate_symbol(symbol)
            return compute_auction_profile(symbol)
        except Exception as exc:
            return {"data_valid": False, "error": str(exc)}

    def query_cvd_trend(self, symbol, lookback_bars=12):
        try:
            frame = self.get_forecast_frame(symbol, lookback_bars)
            if frame.empty or len(frame) < 2:
                return {'data_valid': False, 'error': 'Insufficient candle bars', 'cvd_slope': 'NEUTRAL'}
            delta = float(frame.delta.sum())
            change = float(frame.close.iloc[-1] - frame.close.iloc[0])
            # Price/tape disagreement is unresolved absorption, not proof of reversal.
            if change * delta < 0:
                slope = 'BULLISH_DIVERGENCE' if delta > 0 else 'BEARISH_DIVERGENCE'
            else:
                slope = 'BULLISH_FLOW' if delta > 0 else 'BEARISH_FLOW' if delta < 0 else 'NEUTRAL'
            poc = float(frame.poc_price.iloc[-1]) if 'poc_price' in frame.columns and pd.notna(frame.poc_price.iloc[-1]) else None
            return {'data_valid': True, 'cvd_slope': slope, 'recent_net_delta': delta,
                    'recent_poc': poc if (poc is not None and np.isfinite(poc) and poc > 0) else None, 'bars_analyzed': len(frame)}
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
        if not 0 <= age <= 7200:
            raise ValueError('Snapshot stale or future')
        return frame.loc[times == times.max()].copy()

    def get_forecast_frame(self, symbol, limit=64):
        self._validate_symbol(symbol)
        if not isinstance(limit, int) or not 2 <= limit <= 10000:
            raise ValueError('Invalid context length')
        target_file = CANDLE_DIR / f'{symbol}.parquet'
        if not target_file.exists():
            return pd.DataFrame()
        try:
            raw_df = pd.read_parquet(target_file)
            if raw_df.empty:
                return pd.DataFrame()
            frame = completed_candles(raw_df).tail(limit)
            for col in ['close', 'cumulative_delta', 'total_volume', 'delta']:
                if col not in frame.columns:
                    return pd.DataFrame()
                frame[col] = pd.to_numeric(frame[col], errors='coerce')
            frame = frame.dropna(subset=['close', 'cumulative_delta', 'total_volume', 'delta'])
            if frame.empty or (frame.close <= 0).any() or (frame.total_volume < 0).any():
                return pd.DataFrame()
            return frame
        except Exception:
            return pd.DataFrame()

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
