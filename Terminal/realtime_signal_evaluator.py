"""

========================================================================================

Real-Time Signal Sentinel & Pipeline Evaluator

========================================================================================

Automatically triggered whenever the Footprint & Signals Collector discovers any new

signal during its 2-minute cycle sweeps.



Instantly executes an end-to-end multi-model quantitative evaluation:

1. DuckDB OLAP: Whale Table 2 walls, Liquidation magnet zones, CVD order-flow trends.

2. Market Regime: ATR Volatility & Trend classification.

3. LanceDB Vector Memory: Historical trade setup analogs & win-rate expectancy.

4. Google DeepMind TimesFM 2.5: Zero-shot CVD drift & probabilistic quantile forecasts.

5. Formulates precise Entry, SL, TP1, TP2, R:R, and enforces $50.00 max risk guardrails.

6. Persists qualified alerts to Database/realtime_trade_alerts.jsonl & parquet.

========================================================================================

"""



import os

import sys

import time

import json

import logging

import math

import hashlib

from pathlib import Path

from datetime import datetime, timezone, timedelta

from typing import List, Dict, Any, Optional

import pandas as pd

import numpy as np



try:

    import winsound

except ImportError:

    winsound = None



ROOT_DIR = Path(__file__).resolve().parent.parent

if str(ROOT_DIR) not in sys.path:

    sys.path.insert(0, str(ROOT_DIR))



from Terminal.runtime_io import writer_lock, atomic_parquet

# native_size excised per Delta Exchange India exclusivity

from Terminal.olap_engine import OlapEngine

from Terminal.regime_detector import RegimeDetector

from Terminal.vector_memory import VectorMemory

from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster

from Terminal.risk_envelope import (

    admissible_volume,

    validate_trade_geometry,

    HARD_FLOOR_USD,

    OPERATING_BUFFER_MIN,

    MAX_RISK_USD,

    MIN_RISK_USD,

    MAX_ALLOWED_SPREAD_PCT,

    MIN_RR_RATIO

)



logger = logging.getLogger("RealtimeSignalEvaluator")

DB_DIR = ROOT_DIR / "Database"

SIGNALS_DIR = DB_DIR / "Signals"

SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

ALERTS_JSONL = (SIGNALS_DIR / "realtime_trade_alerts.jsonl") if (SIGNALS_DIR / "realtime_trade_alerts.jsonl").exists() or SIGNALS_DIR.exists() else (DB_DIR / "realtime_trade_alerts.jsonl")

ALERTS_PARQUET = (SIGNALS_DIR / "realtime_trade_alerts.parquet") if (SIGNALS_DIR / "realtime_trade_alerts.parquet").exists() or SIGNALS_DIR.exists() else (DB_DIR / "realtime_trade_alerts.parquet")





def utc_to_ist_str(utc_str: Optional[str]) -> str:

    """Converts standard UTC ISO string to IST (UTC+5:30) display format."""

    if not utc_str:

        return ""

    try:

        clean = str(utc_str).replace("Z", "+00:00")

        dt = datetime.fromisoformat(clean)

        ist_dt = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))

        return ist_dt.strftime("%Y-%m-%d %H:%M:%S IST")

    except Exception:

        return str(utc_str)





class RealtimeSignalEvaluator:

    def __init__(self):

        self.olap = OlapEngine()

        self.regime_det = RegimeDetector()

        try:

            self.vector_mem = VectorMemory()

        except Exception as exc:

            logger.warning('Vector memory unavailable: %s', exc)

            self.vector_mem = None

        self.timesfm = DeepMindTimesFMForecaster()
        try:
            from Terminal.empirical_signal_learner import EmpiricalSignalLearner
            self.empirical_learner = EmpiricalSignalLearner()
        except Exception as exc:
            logger.warning('EmpiricalSignalLearner unavailable: %s', exc)
            self.empirical_learner = None
        try:
            from Terminal.trade_critic_engine import TradeCriticEngine
            self.critic_engine = TradeCriticEngine()
        except Exception as exc:
            logger.warning('TradeCriticEngine unavailable: %s', exc)
            self.critic_engine = None
        try:
            from Terminal.deepseek_harness_bridge import DeepSeekHarnessBridge
            self.dsh_bridge = DeepSeekHarnessBridge()
        except Exception as exc:
            logger.warning('DeepSeekHarnessBridge unavailable: %s', exc)
            self.dsh_bridge = None



    def query_live_tv_liquidations(self, sym: str) -> Dict[str, Any]:

        """Queries the latest live liquidation reversal data harvested from CoinGlass TV."""

        candidates = []

        for name in ['latest_reversal_signals.json', 'latest_coinalyze_reversal_signals.json']:

            try:

                item = json.loads((DB_DIR / 'Live_Liquidations' / name).read_text())[sym]

                now = datetime.now(timezone.utc)

                observed = datetime.fromisoformat(item['updated_at_utc'])

                bar = datetime.fromisoformat((item.get('spike_bar_time_utc') or item['last_bar_time_utc']).replace('Z', '+00:00'))

                if 0 <= (now-observed).total_seconds() <= 180 and 0 <= (now-bar).total_seconds() <= 1800:

                    candidates.append((observed, item))

            except (OSError, KeyError, ValueError, TypeError):

                continue

        return max(candidates, key=lambda x: x[0])[1] if candidates else {}



    def evaluate_signal(self, sig: Dict[str, Any]) -> Dict[str, Any]:

        """

        Deeply inspects a newly detected signal against the entire data pipeline.

        """

        sym = sig.get("symbol", "")

        mt5_sym = sig.get("mt5_symbol", "")

        sig_name = sig.get("sig_name", "")

        sig_side = str(sig.get("side", "")).lower()

        try:

            sig_price = float(sig.get("price", 0.0))

        except (ValueError, TypeError):

            sig_price = 0.0

        if not math.isfinite(sig_price):

            sig_price = 0.0

        sig_time = sig.get("datetime_utc", "")

        sig_desc = sig.get("description", "")

        sig_title = sig.get("title", "")



        # 1. OLAP Microstructure Queries

        whales = self.olap.query_whale_walls(sym)

        whale_history = self.olap.query_whale_history_orders(sym)

        liq = self.olap.query_liquidation_confluence(sym)

        key_barriers = self.olap.query_key_liquidation_levels(sym, sig_price)

        cvd = self.olap.query_cvd_trend(sym, lookback_bars=16)

        auction = self.olap.query_auction_profile(sym)

        lookup_sig_name = sig_name.split(" + ")[0].strip() if " + " in sig_name else sig_name
        emp_edge = self.empirical_learner.get_signal_expectancy(sym, lookup_sig_name) if getattr(self, 'empirical_learner', None) else {}



        whales = whales if isinstance(whales, dict) else {}
        whale_history = whale_history if isinstance(whale_history, dict) else {}
        key_barriers = key_barriers if isinstance(key_barriers, dict) else {}

        liq = liq if isinstance(liq, dict) else {}

        cvd = cvd if isinstance(cvd, dict) else {}

        auction = auction if isinstance(auction, dict) else {}



        whale_imbalance = self._finite(whales.get("whale_imbalance_bid_ratio"), 0.5)

        near_field_shorts = self._finite(liq.get("near_field_shorts_pct"), 50.0)

        nf_meta = liq.get("near_field", {}) if isinstance(liq.get("near_field"), dict) else {}

        short_share_trend = self._finite(nf_meta.get("short_share_24h_pts"), 0.0)

        short_magnets = liq.get("top_short_magnets", []) or []

        long_magnets = liq.get("top_long_magnets", []) or []

        key_levels_list = liq.get("key_levels", []) or []

        strength_bands = liq.get("strength_by_distance", []) or []

        leverage_tiers_list = liq.get("leverage_tiers", []) or []

        clld_data = liq.get("clld", {}) if isinstance(liq.get("clld"), dict) else {}

        clld_turn = str(clld_data.get("sentiment_turn", "") or "")

        cvd_slope = cvd.get("cvd_slope", "NEUTRAL")

        poc_price = cvd.get("recent_poc")



        # 2. Market Regime Detection

        reg = self.regime_det.analyze_symbol(sym)

        reg = reg if isinstance(reg, dict) else {}

        current_regime = reg.get("regime", "UNKNOWN")

        vol_regime = reg.get("volatility_regime", "NORMAL")



        confluence_score = 40.0

        reasons = [f"Signal: {sig_name} ({sig_side})"]
        validation_errors = []
        direction = "NEUTRAL"

        is_auction_valid = bool(auction.get("data_valid", False))
        fib_align = str(auction.get("fib_prime_alignment", "") or "")
        mtf_bias = str(auction.get("mtf_bias", "") or "")
        p_vs_vwap = str(auction.get("price_vs_vwap", "") or "")
        val_status = str(auction.get("value_status", "") or "")

        # Deterministic Dual-Model Routing State Machine (EGM-SPEC-20261103)
        day_vah = float(auction.get("day_vah", 0.0) or 0.0)
        day_val = float(auction.get("day_val", 0.0) or 0.0)
        day_vwap = float(auction.get("day_vwap", 0.0) or 0.0)
        day_vwap_sd = float(auction.get("vwap_sd1_upper", 0.0) - day_vwap) if auction.get("vwap_sd1_upper") else 0.0
        h1_trend = str(auction.get("htf_1h_trend", "NEUTRAL") or "NEUTRAL")
        h4_trend = str(auction.get("htf_4h_trend", "NEUTRAL") or "NEUTRAL")
        ema34 = float(auction.get("ema34", 0.0) or 0.0)
        ema55 = float(auction.get("ema55", 0.0) or 0.0)
        ema59 = float(auction.get("ema59", 0.0) or 0.0)
        ema89 = float(auction.get("ema89", 0.0) or 0.0)
        ema233 = float(auction.get("ema233", 0.0) or 0.0)
        in_pullback_pocket = bool(auction.get("in_pullback_pocket", False))
        in_deep_pocket = bool(auction.get("in_deep_pocket", False))
        dist_55_59 = float(auction.get("dist_55_59_pct", 999.0))

        is_bullish_sig = (
            ("absorption" in sig_name.lower() and sig_side in ["buy", "bullish"]) or
            ("trapped shorts" in sig_name.lower()) or 
            ("bull div" in sig_name.lower()) or 
            ("divergence" in sig_name.lower() and sig_side in ["buy", "bullish"]) or 
            ("seller exhaustion" in sig_name.lower()) or 
            ("exhaustion" in sig_name.lower() and "liquidation" not in sig_name.lower() and "seller" in sig_name.lower()) or 
            ("exhaustion" in sig_name.lower() and sig_side in ["buy", "bullish"]) or 
            ("liquidation exhaustion" in sig_name.lower() and sig_side in ["buy", "bullish"])
        )
        is_bearish_sig = (
            ("absorption" in sig_name.lower() and sig_side in ["sell", "bearish"]) or
            ("trapped longs" in sig_name.lower()) or 
            ("bear div" in sig_name.lower()) or 
            ("divergence" in sig_name.lower() and sig_side in ["sell", "bearish"]) or 
            ("buyer exhaustion" in sig_name.lower()) or 
            ("exhaustion" in sig_name.lower() and "liquidation" not in sig_name.lower() and "buyer" in sig_name.lower()) or 
            ("exhaustion" in sig_name.lower() and sig_side in ["sell", "bearish"]) or 
            ("liquidation exhaustion" in sig_name.lower() and sig_side in ["sell", "bearish"])
        )

        cand_dir = "BUY" if is_bullish_sig else ("SELL" if is_bearish_sig else "NEUTRAL")
        is_long_macro_aligned = (h1_trend == "BULLISH" and h4_trend == "BULLISH") or mtf_bias == "HTF_BULLISH_CONFLUENCE"
        is_short_macro_aligned = (h1_trend == "BEARISH" and h4_trend == "BEARISH") or mtf_bias == "HTF_BEARISH_CONFLUENCE"

        # Priority 1: Macro Trend Supremacy -> Model 2
        if cand_dir == "BUY" and is_long_macro_aligned:
            execution_model = "MODEL_2_TREND_PULLBACK"
        elif cand_dir == "SELL" and is_short_macro_aligned:
            execution_model = "MODEL_2_TREND_PULLBACK"
        # Priority 2: Range Confinement (Mean Reversion Fade) -> Model 1
        elif (
            (current_regime in ["MEAN_REVERTING_RANGE", "CHOP", "HIGH_VOLATILITY_COMPRESSION"] or mtf_bias in ["HTF_DIVERGENCE", "HTF_NEUTRAL"]) and
            (
                (cand_dir == "BUY" and (sig_price <= day_val * 1.002 or (day_vwap > 0 and sig_price <= day_vwap - day_vwap_sd) or val_status in ["BELOW_VAL", "OUTSIDE_VALUE"])) or
                (cand_dir == "SELL" and (sig_price >= day_vah * 0.998 or (day_vwap > 0 and sig_price >= day_vwap + day_vwap_sd) or val_status in ["ABOVE_VAH", "OUTSIDE_VALUE"]))
            )
        ):
            execution_model = "MODEL_1_MEAN_REVERSION"
        else:
            execution_model = "MODEL_0_FILTERED_BOUNDARY"

        # Bullish Setups
        if is_bullish_sig:
            direction = "BUY"
            if whale_imbalance >= 0.55:
                confluence_score += 15
                reasons.append(f"Whale Bids {whale_imbalance*100:.0f}%")

            if cvd_slope in ["BULLISH_DIVERGENCE", "BULLISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")

            if near_field_shorts >= 52.0:
                confluence_score += 10
                if short_share_trend > 0:
                    confluence_score += 5
                    reasons.append(f"Short Squeeze Fuel ({near_field_shorts:.0f}% shorts, piling in +{short_share_trend:.1f} pts 24h)")
                else:
                    reasons.append(f"Short Squeeze Fuel ({near_field_shorts:.0f}% shorts)")

            if short_magnets:
                nearest_sm = short_magnets[0]
                sm_dist = float(nearest_sm.get("distance_pct", 0.0))
                sm_score = float(nearest_sm.get("score", 0.0))
                if 0.4 <= sm_dist <= 4.0:
                    confluence_score += 10
                    reasons.append(f"Overhead Short Liq Target: ${nearest_sm.get('zone_price'):,.2f} (+{sm_dist:.1f}%, Score {sm_score:.0f})")

            if "bullish" in clld_turn.lower():
                confluence_score += 5
                reasons.append(f"CLLD Shift Confluence ({clld_turn})")

            if any(b.get("band") in ["0-1%", "1-2%"] and b.get("net_delta", 0) > 0 for b in strength_bands):
                confluence_score += 5
                reasons.append("Near-Field 0-2% Short Liquidation Density Dominance")

            # Model Specific Scoring Logic
            if execution_model == "MODEL_1_MEAN_REVERSION":
                confluence_score += 15
                reasons.append(f"Model 1: Mean Reversion Fade at Value Boundary (VAL ${day_val:,.2f})")
                if p_vs_vwap == "BELOW_VWAP":
                    confluence_score += 10
                    reasons.append(f"Fading Discount Below Day VWAP (${day_vwap:,.2f})")
                if mtf_bias == "HTF_DIVERGENCE":
                    reasons.append("HTF Divergence Counter-Macro Penalty Suppressed for Mean Reversion")
            elif execution_model == "MODEL_2_TREND_PULLBACK":
                confluence_score += 15
                reasons.append("Model 2: HTF Bullish Macro Trend Aligned (1H & 4H Bullish)")
                if in_pullback_pocket or dist_55_59 <= 0.60 or in_deep_pocket:
                    confluence_score += 15
                    reasons.append(f"Model 2: Dynamic Fib-Prime Pullback Pocket [55, 59] Proximity ({dist_55_59:.2f}%)")
                if sig_price >= ema59 * 0.998:
                    confluence_score += 10
                    reasons.append(f"Model 2: Directional Close Defending EMA 59 Support (${ema59:,.2f})")
            else: # MODEL_0_FILTERED_BOUNDARY
                if is_auction_valid:
                    if p_vs_vwap == "ABOVE_VWAP":
                        confluence_score += 10
                        reasons.append(f"Above Day VWAP (${auction.get('day_vwap', 0.0):,.2f})")
                    if "BULLISH" in fib_align:
                        confluence_score += 10
                        reasons.append(f"Bullish Fib-Prime Ribbon ({fib_align})")
                    if mtf_bias == "HTF_BULLISH_CONFLUENCE":
                        confluence_score += 15
                        reasons.append("HTF Bullish Confluence (1H and 4H Bullish)")
                    elif mtf_bias == "HTF_DIVERGENCE":
                        confluence_score -= 10
                        reasons.append("HTF Divergence Warning (Counter-trend to 4H Macro)")
                    if val_status == "INSIDE_VALUE":
                        reasons.append(f"Inside Value Area (VAL ${auction.get('day_val', 0.0):,.2f} - VAH ${auction.get('day_vah', 0.0):,.2f})")

        # Bearish Setups
        elif is_bearish_sig:
            direction = "SELL"
            if whale_imbalance <= 0.45:
                confluence_score += 15
                reasons.append(f"Whale Asks {(1-whale_imbalance)*100:.0f}%")

            if cvd_slope in ["BEARISH_DIVERGENCE", "BEARISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")

            if near_field_shorts <= 48.0:
                confluence_score += 10
                if short_share_trend < 0:
                    confluence_score += 5
                    reasons.append(f"Long Liq Cascade Fuel ({100-near_field_shorts:.0f}% longs, long share expanding)")
                else:
                    reasons.append(f"Long Liq Cascade Fuel ({100-near_field_shorts:.0f}% longs)")

            if long_magnets:
                nearest_lm = long_magnets[0]
                lm_dist = abs(float(nearest_lm.get("distance_pct", 0.0)))
                lm_score = float(nearest_lm.get("score", 0.0))
                if 0.4 <= lm_dist <= 4.0:
                    confluence_score += 10
                    reasons.append(f"Underlying Long Liq Target: ${nearest_lm.get('zone_price'):,.2f} (-{lm_dist:.1f}%, Score {lm_score:.0f})")

            if "bearish" in clld_turn.lower():
                confluence_score += 5
                reasons.append(f"CLLD Shift Confluence ({clld_turn})")

            if any(b.get("band") in ["0-1%", "1-2%"] and b.get("net_delta", 0) < 0 for b in strength_bands):
                confluence_score += 5
                reasons.append("Near-Field 0-2% Long Liquidation Cascade Dominance")

            # Model Specific Scoring Logic
            if execution_model == "MODEL_1_MEAN_REVERSION":
                confluence_score += 15
                reasons.append(f"Model 1: Mean Reversion Fade at Value Boundary (VAH ${day_vah:,.2f})")
                if p_vs_vwap == "ABOVE_VWAP":
                    confluence_score += 10
                    reasons.append(f"Fading Premium Above Day VWAP (${day_vwap:,.2f})")
                if mtf_bias == "HTF_DIVERGENCE":
                    reasons.append("HTF Divergence Counter-Macro Penalty Suppressed for Mean Reversion")
            elif execution_model == "MODEL_2_TREND_PULLBACK":
                confluence_score += 15
                reasons.append("Model 2: HTF Bearish Macro Trend Aligned (1H & 4H Bearish)")
                if in_pullback_pocket or dist_55_59 <= 0.60 or in_deep_pocket:
                    confluence_score += 15
                    reasons.append(f"Model 2: Dynamic Fib-Prime Pullback Pocket [55, 59] Proximity ({dist_55_59:.2f}%)")
                if sig_price <= ema59 * 1.002:
                    confluence_score += 10
                    reasons.append(f"Model 2: Directional Close Defending EMA 59 Resistance (${ema59:,.2f})")
            else: # MODEL_0_FILTERED_BOUNDARY
                if is_auction_valid:
                    if p_vs_vwap == "BELOW_VWAP":
                        confluence_score += 10
                        reasons.append(f"Below Day VWAP (${auction.get('day_vwap', 0.0):,.2f})")
                    if "BEARISH" in fib_align:
                        confluence_score += 10
                        reasons.append(f"Bearish Fib-Prime Ribbon ({fib_align})")
                    if mtf_bias == "HTF_BEARISH_CONFLUENCE":
                        confluence_score += 15
                        reasons.append("HTF Bearish Confluence (1H and 4H Bearish)")
                    elif mtf_bias == "HTF_DIVERGENCE":
                        confluence_score -= 10
                        reasons.append("HTF Divergence Warning (Counter-trend to 4H Macro)")
                    if val_status == "INSIDE_VALUE":
                        reasons.append(f"Inside Value Area (VAL ${auction.get('day_val', 0.0):,.2f} - VAH ${auction.get('day_vah', 0.0):,.2f})")

        
        # 3. Vector Memory Analogs

        setup_desc = f"{sym} {direction} {sig_name} with Whale Imbalance {whale_imbalance:.1%}, CVD {cvd_slope}, Regime {current_regime}"

        try:

            analogs = (self.vector_mem.query_similar_setups(setup_desc, top_k=3) if self.vector_mem else {}) or {}

        except Exception:

            analogs = {}

        vm_verdict = analogs.get("verdict", "INCONCLUSIVE")

        vm_win_rate = self._finite(analogs.get("win_rate"), 0.5)



        if vm_verdict == "FAVORABLE":

            confluence_score += 10

            reasons.append(f"Vector Memory Win Rate {vm_win_rate*100:.0f}%")

        elif vm_verdict == "UNFAVORABLE":

            confluence_score -= 15

            reasons.append("Vector Memory Unfavorable Veto")



        # 3.5 CoinGlass TV Live Liquidation Reversal Confluence (Big Liquidation Flush Reversals)
        tv_liq = self.query_live_tv_liquidations(sym)
        has_tv_liq_spike = bool(tv_liq.get("recent_spike", False) or tv_liq.get("is_spike", False))
        tv_liq_bias = tv_liq.get("reversal_bias", "NEUTRAL")
        tv_liq_exhaustion = self._finite(tv_liq.get("exhaustion_score"), 0.0)
        tv_liq_long_usd = self._finite(tv_liq.get("latest_long_liq_usd") or tv_liq.get("long_liq_usd"), 0.0)
        tv_liq_short_usd = self._finite(tv_liq.get("latest_short_liq_usd") or tv_liq.get("short_liq_usd"), 0.0)
        tv_liq_total_usd = self._finite(tv_liq.get("total_liq_usd"), 0.0)



        if direction == "BUY":

            if has_tv_liq_spike and tv_liq_bias == "BULLISH_REVERSAL":
                boost = 20 if (tv_liq_exhaustion >= 70.0 or tv_liq_long_usd >= 100_000.0) else 15
                confluence_score += boost
                reasons.append(f"CoinGlass TV Long Liq Capitulation Reversal (Exhaustion {tv_liq_exhaustion:.0f}/100, Long Liq ${tv_liq_long_usd:,.0f}, Boost +{boost}pts)")

            elif has_tv_liq_spike and tv_liq_bias == "BEARISH_REVERSAL":

                confluence_score -= 15

                reasons.append(f"Adverse Short Liq Squeeze Top (${tv_liq_short_usd:,.0f} short liq already exhausted)")

        elif direction == "SELL":

            if has_tv_liq_spike and tv_liq_bias == "BEARISH_REVERSAL":
                boost = 20 if (tv_liq_exhaustion >= 70.0 or tv_liq_short_usd >= 100_000.0) else 15
                confluence_score += boost
                reasons.append(f"CoinGlass TV Short Liq Blow-Off Reversal (Exhaustion {tv_liq_exhaustion:.0f}/100, Short Liq ${tv_liq_short_usd:,.0f}, Boost +{boost}pts)")

            elif has_tv_liq_spike and tv_liq_bias == "BULLISH_REVERSAL":

                confluence_score -= 15

                reasons.append(f"Adverse Long Liq Cascade Bottom (${tv_liq_long_usd:,.0f} long liq already exhausted)")



        # 3.55 Multi-Signal Candle Cluster Confluence Boost (Intra-Bar Stacking)
        cluster = sig.get("cluster_signals") or []
        cluster_agreement = sig.get("cluster_agreement", "")
        if cluster and len(cluster) >= 2:
            sig_names_str = " + ".join(s.get("sig_name", s.get("title", "Sig")) for s in cluster)
            if cluster_agreement in ("UNANIMOUS_BULLISH", "UNANIMOUS_BEARISH"):
                cluster_boost = 20.0 if len(cluster) >= 3 else 15.0
                confluence_score += cluster_boost
                reasons.append(f"Multi-Signal Candle Cluster ({len(cluster)} signals): {sig_names_str} aligned {direction} (+{cluster_boost:.0f}pts)")
            elif cluster_agreement == "DISCORDANT_CONFLICT":
                confluence_score -= 20.0
                reasons.append(f"Intra-Bar Signal Conflict: Opposing signals on same candle ({sig_names_str}) (-20pts)")
                validation_errors.append("INTRA_BAR_SIGNAL_CONFLICT")

        # 3.6 Empirical 300-Bar Statistical Edge
        emp_adj = float(emp_edge.get("confluence_adj", 0.0))
        if emp_adj != 0:
            confluence_score += emp_adj
            reasons.append(f"Empirical 300-Bar Edge ({emp_edge.get('recommendation')}: WR {emp_edge.get('win_rate', 0.5)*100:.0f}%, EV {emp_edge.get('ev_r', 0.0):+.2f}R, Adj {emp_adj:+.0f}pts)")

        # 3.7 Whale History Aggressive Sweep Confluence
        sweep_bias = str(whale_history.get("sweep_bias", "NEUTRAL"))
        net_whale_flow = self._finite(whale_history.get("net_whale_flow_usd"), 0.0)
        if direction == "BUY" and sweep_bias == "BULLISH_SWEEP":
            confluence_score += 5
            reasons.append(f"Whale Sweep Aggressive Buying (+${net_whale_flow/1e6:.1f}M net filled)")
        elif direction == "SELL" and sweep_bias == "BEARISH_SWEEP":
            confluence_score += 5
            reasons.append(f"Whale Sweep Aggressive Selling (-${abs(net_whale_flow)/1e6:.1f}M net filled)")

        # 3.8 Key Liquidation Cluster Barriers
        if direction == "BUY" and isinstance(key_barriers.get("nearest_overhead"), dict):
            p = self._finite(key_barriers["nearest_overhead"].get("price"), 0.0)
            if p > 0:
                confluence_score += 5
                reasons.append(f"Key Short Liq Strike Barrier: ${p:,.2f}")
        elif direction == "SELL" and isinstance(key_barriers.get("nearest_underlying"), dict):
            p = self._finite(key_barriers["nearest_underlying"].get("price"), 0.0)
            if p > 0:
                confluence_score += 5
                reasons.append(f"Key Long Liq Strike Barrier: ${p:,.2f}")

        # Forecast aligned completed-bar flow ratios through the implemented API.

        tfm_cvd, tfm_price = {}, {}

        try:

            frame = self.olap.get_forecast_frame(sym, 64)

            if self.timesfm.is_available and len(frame) >= 16:

                tfm_cvd = self.timesfm.forecast_normalized_delta_ratio(

                    frame.cumulative_delta.to_numpy(), frame.total_volume.to_numpy(),

                    horizon=24, candidate_direction=direction) or {}

        except Exception as exc:

            reasons.append(f'TimesFM unavailable: {exc}')

        forecast_veto = tfm_cvd.get('adverse_veto') is True

        hurdle = self._finite(tfm_cvd.get('confluence_hurdle_adjustment'), 0.0)

        if forecast_veto:

            reasons.append('TimesFM adverse-flow veto')

        if not tfm_cvd.get('available'):
            cvd_dir = 'UNAVAILABLE'
            cvd_conf = 0.0
        elif forecast_veto:
            cvd_dir = 'VETO'
            cvd_conf = 0.95
        else:
            proj_sum = float(tfm_cvd.get('projected_delta_ratio_sum', 0.0) or 0.0)
            cvd_dir = 'BULLISH' if proj_sum > 0 else ('BEARISH' if proj_sum < 0 else 'NEUTRAL')
            import numpy as _np
            _mean_unc = float(tfm_cvd.get("mean_interval_width", 0.40) or 0.40)
            cvd_conf = float(_np.clip(0.50 + 0.40 * _np.tanh(abs(proj_sum) / (12.0 * max(0.1, _mean_unc))), 0.50, 0.94))

        # Price Direction from completed sequence and Auction profile
        if not frame.empty and len(frame) >= 16:
            p_close = frame.close.to_numpy(dtype=float)
            price_change = p_close[-1] - p_close[0]
            price_dir = "BULLISH" if price_change > 0 else ("BEARISH" if price_change < 0 else "NEUTRAL")
            price_conf = min(0.95, max(0.50, 0.50 + (abs(price_change) / (p_close[-1] + 1e-9)) * 10.0))
        else:
            price_dir = "UNAVAILABLE"
            price_conf = 0.0

        cvd_unc_mult = 1.0

        effective_max_risk = MAX_RISK_USD



        # 5. Formulate Trade Risk Levels (Dual-Model Geometry & Target Extension)
        entry = sig_price
        atr_pct = self._finite(reg.get("atr_pct"), 0.0)
        sl_pct = max(0.005, min(0.025, (atr_pct * 1.5) / 100.0))

        if execution_model == "MODEL_1_MEAN_REVERSION":
            if direction == "BUY":
                sl = round(entry * (1.0 - sl_pct), 6)
                risk_dist_init = abs(entry - sl)
                vwap_target = day_vwap if day_vwap > (entry + 2.0 * risk_dist_init) else entry + 2.0 * risk_dist_init
                # Target Extension Invariant: Strict R:R >= 2.0:1 required
                if abs(vwap_target - entry) < 2.0 * risk_dist_init:
                    tp1_target = max(day_vah, entry + 2.0 * risk_dist_init) if day_vah > entry else entry + 2.0 * risk_dist_init
                    reasons.append(f"Model 1: Dynamic Target Extension to Opposite Value Area (VAH ${tp1_target:,.2f}) for R:R >= 2.0")
                else:
                    tp1_target = vwap_target
                    reasons.append(f"Model 1: Target Fair Value Day VWAP (${tp1_target:,.2f}) [R:R >= 2.0]")
                tp1 = round(max(tp1_target, entry + 2.0 * risk_dist_init), 6)
                tp2 = round(entry + 3.0 * risk_dist_init, 6)

            elif direction == "SELL":
                sl = round(entry * (1.0 + sl_pct), 6)
                risk_dist_init = abs(entry - sl)
                vwap_target = day_vwap if day_vwap < (entry - 2.0 * risk_dist_init) else entry - 2.0 * risk_dist_init
                # Target Extension Invariant: Strict R:R >= 2.0:1 required
                if abs(entry - vwap_target) < 2.0 * risk_dist_init:
                    tp1_target = min(day_val, entry - 2.0 * risk_dist_init) if 0 < day_val < entry else entry - 2.0 * risk_dist_init
                    reasons.append(f"Model 1: Dynamic Target Extension to Opposite Value Area (VAL ${tp1_target:,.2f}) for R:R >= 2.0")
                else:
                    tp1_target = vwap_target
                    reasons.append(f"Model 1: Target Fair Value Day VWAP (${tp1_target:,.2f}) [R:R >= 2.0]")
                tp1 = round(min(tp1_target, entry - 2.0 * risk_dist_init), 6)
                tp2 = round(entry - 3.0 * risk_dist_init, 6)
            else:
                sl, tp1, tp2 = entry, entry, entry

        elif execution_model == "MODEL_2_TREND_PULLBACK":
            if direction == "BUY":
                # Stop Loss anchored behind Fib-Prime Base EMA 89
                atr_val = (atr_pct * entry) / 100.0 if atr_pct > 0 else (entry * 0.01)
                ema_sl = ema89 - 0.2 * atr_val if ema89 > 0 and ema89 < entry else entry * (1.0 - sl_pct)
                min_sl = entry * 0.975
                max_sl = entry * 0.995
                sl = round(max(min_sl, min(max_sl, ema_sl)), 6)
                reasons.append(f"Model 2: SL anchored behind Fib-Prime Structural Base EMA 89 (${ema89:,.2f})")

                if short_magnets and len(short_magnets) >= 1:
                    sm_p1 = float(short_magnets[0].get("zone_price", 0.0))
                    if sm_p1 >= entry + 2.0 * abs(entry - sl):
                        tp1 = round(sm_p1, 6)
                    else:
                        tp1 = round(entry + 2.0 * abs(entry - sl), 6)
                else:
                    tp1 = round(entry + 2.0 * abs(entry - sl), 6)
                tp2 = round(entry + 3.0 * abs(entry - sl), 6)

            elif direction == "SELL":
                # Stop Loss anchored behind Fib-Prime Base EMA 89
                atr_val = (atr_pct * entry) / 100.0 if atr_pct > 0 else (entry * 0.01)
                ema_sl = ema89 + 0.2 * atr_val if ema89 > entry else entry * (1.0 + sl_pct)
                min_sl = entry * 1.005
                max_sl = entry * 1.025
                sl = round(min(max_sl, max(min_sl, ema_sl)), 6)
                reasons.append(f"Model 2: SL anchored behind Fib-Prime Structural Base EMA 89 (${ema89:,.2f})")

                if long_magnets and len(long_magnets) >= 1:
                    lm_p1 = float(long_magnets[0].get("zone_price", 0.0))
                    if 0 < lm_p1 <= entry - 2.0 * abs(entry - sl):
                        tp1 = round(lm_p1, 6)
                    else:
                        tp1 = round(entry - 2.0 * abs(entry - sl), 6)
                else:
                    tp1 = round(entry - 2.0 * abs(entry - sl), 6)
                tp2 = round(entry - 3.0 * abs(entry - sl), 6)
            else:
                sl, tp1, tp2 = entry, entry, entry

        else: # MODEL_0_FILTERED_BOUNDARY
            if direction == "BUY":
                sl = round(entry * (1.0 - sl_pct), 6)
                risk_dist_init = abs(entry - sl)
                tp1_target = entry + 2.0 * risk_dist_init
                if short_magnets and len(short_magnets) >= 1:
                    sm_p1 = float(short_magnets[0].get("zone_price", 0.0))
                    if sm_p1 >= entry + 2.0 * risk_dist_init:
                        tp1_target = sm_p1
                tp1 = round(tp1_target, 6)
                tp2 = round(entry + 3.0 * risk_dist_init, 6)
            elif direction == "SELL":
                sl = round(entry * (1.0 + sl_pct), 6)
                risk_dist_init = abs(entry - sl)
                tp1_target = entry - 2.0 * risk_dist_init
                if long_magnets and len(long_magnets) >= 1:
                    lm_p1 = float(long_magnets[0].get("zone_price", 0.0))
                    if 0 < lm_p1 <= entry - 2.0 * risk_dist_init:
                        tp1_target = lm_p1
                tp1 = round(tp1_target, 6)
                tp2 = round(entry - 3.0 * risk_dist_init, 6)
            else:
                sl, tp1, tp2 = entry, entry, entry

        
        risk_dist = abs(entry - sl)

        reward_dist = abs(tp1 - entry)

        rr = round(reward_dist / (risk_dist + 1e-9), 2) if risk_dist > 0 else 0.0



        # validation_errors already initialized at start

        if sig_price <= 0 or atr_pct <= 0:

            validation_errors.append('INVALID_PRICE_OR_ATR')

        try:

            sig_ts_str = sig.get('detection_time_utc') or sig.get('close_time_utc') or sig_time

            age = (datetime.now(timezone.utc) - datetime.fromisoformat(sig_ts_str.replace('Z', '+00:00'))).total_seconds()

            if not 0 <= age <= 3600:

                validation_errors.append('SIGNAL_STALE_OR_FUTURE')

        except (ValueError, TypeError):

            validation_errors.append('SIGNAL_TIMESTAMP_UNKNOWN')



        # Strict Bar Completion and Bar Type Support (Time 15m/5m, Range, and Volume)
        bar_type = str(sig.get("bar_type", "time")).lower()
        sig_time_ms = int(sig.get("time_ms") or 0)
        now_ms = time.time() * 1000

        if bar_type in ("range", "vol", "volume"):
            is_bar_closed = True
        else:
            bar_interval = 300_000 if (sig_time_ms % 300_000 == 0 and sig_time_ms % 900_000 != 0) else 900_000
            is_bar_closed = (sig_time_ms > 0 and (sig_time_ms + bar_interval) <= now_ms) or sig.get("bar_closed") is True

        if not is_bar_closed:
            validation_errors.append('BAR_NOT_CLOSED_INTRA_BAR_WICK')

        is_macro = sym in ('EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD', 'XAUUSD', 'XAGUSD', 'SPX500', 'NDX100', 'DJI30', 'US500', 'US100', 'US30') or sig.get('asset_class') in ('Forex', 'Metals', 'Indices', 'Metals_Commodities')

        # Mandatory Tier 1 Feeds (CVD and Auction from 300 completed bars)
        if not (cvd.get('data_valid') is True and auction.get('data_valid') is True):
            validation_errors.append('MICROSTRUCTURE_DATA_UNQUALIFIED')

        # Statistical Veto from Empirical 300-Bar Learner
        if emp_edge.get('recommendation') == 'STATISTICAL_VETO':
            validation_errors.append('STATISTICAL_VETO_NEGATIVE_EXPECTANCY')

        if current_regime in ('UNKNOWN', 'ERROR', 'INSUFFICIENT_DATA'):

            validation_errors.append('REGIME_UNQUALIFIED')

        solved_lot, realized_risk, vol_status = None, 0.0, 'BROKER_VALIDATION_REQUIRED'

        geom_valid = False

        # Primary Execution Venue: Delta Exchange India Discrete Contract Solver
        from Terminal.delta_exchange_live_trader import delta_client
        delta_prod = delta_client.resolve_product(sym)

        if delta_prod:
            contract_val = float(delta_prod.get('contract_value', 1.0) or 1.0)
            risk_dist = abs(entry - sl)
            single_risk = risk_dist * contract_val
            if single_risk > 0 and single_risk <= MAX_RISK_USD:
                solved_lot = max(1, int(MAX_RISK_USD / single_risk))
                realized_risk = min(MAX_RISK_USD, solved_lot * single_risk)
                effective_max_risk = MAX_RISK_USD
                geom_valid = True
                vol_status = 'DELTA_QUALIFIED'
            else:
                solved_lot = 1
                realized_risk = single_risk
                effective_max_risk = MAX_RISK_USD
                geom_valid = (single_risk <= MAX_RISK_USD)
                vol_status = 'DELTA_QUALIFIED' if geom_valid else 'RISK_CAP_EXCEEDED'
        else:

            try:
                sized = self._broker_sizing(mt5_sym, direction, entry, sl, tp1, effective_max_risk)
                solved_lot, realized_risk = sized['lots'], sized['risk_usd']
                effective_max_risk, geom_valid, vol_status = sized['cap_usd'], True, 'OK'
            except Exception as exc:
                risk_dist_val = abs(entry - sl)
                if risk_dist_val > 0:
                    solved_lot = round(MAX_RISK_USD / risk_dist_val, 4)
                    realized_risk = MAX_RISK_USD
                    geom_valid = True
                    vol_status = 'INFORMATIONAL_EVALUATION'
                else:
                    validation_errors.append(str(exc))

        # Trade Critic & Anti-Falling Knife Defense Check
        critic_audit = {"status": "SKIPPED"}
        if getattr(self, 'critic_engine', None) is None:
            validation_errors.append('CRITIC_ENGINE_UNAVAILABLE')
        elif direction in ('BUY', 'SELL') and entry > 0:
            ema233_val = float(auction.get("ema233", 0.0) or auction.get("day_vwap", 0.0) or 0.0)
            ema89_val = float(auction.get("ema89", 0.0) or 0.0)
            raw_slope = cvd.get("slope") if cvd.get("slope") is not None else cvd.get("cvd_slope")
            if isinstance(raw_slope, (int, float)):
                cvd_slope_val = float(raw_slope)
            elif isinstance(raw_slope, str):
                s_upper = raw_slope.upper()
                if "BEARISH" in s_upper or "SHORT" in s_upper:
                    cvd_slope_val = -1.0
                elif "BULLISH" in s_upper or "LONG" in s_upper:
                    cvd_slope_val = 1.0
                else:
                    cvd_slope_val = 0.0
            else:
                cvd_slope_val = 0.0

            # Physically validate absorption against footprint taker flow & wick dynamics
            is_named_absorption = bool("absorption" in sig_name.lower() or "delta_divergence" in sig_name.lower() or "trapped" in sig_name.lower())
            sig_delta = float(sig.get("delta") or 0.0)
            has_absorption_sig = is_named_absorption and (
                (direction == "BUY" and (sig_delta > 0 or cvd_slope_val >= 0.0 or sig.get("side") in ("buy", "bullish"))) or
                (direction == "SELL" and (sig_delta < 0 or cvd_slope_val <= 0.0 or sig.get("side") in ("sell", "bearish")))
            )

            knife_vetoed, knife_reason, _ = self.critic_engine.check_microstructure_knife_veto(
                symbol=sym,
                direction=direction,
                price=entry,
                ema233=ema233_val,
                htf_bias=h4_trend,
                cvd_slope=cvd_slope_val,
                has_liq_spike=has_tv_liq_spike,
                liq_bias=tv_liq_bias,
                has_footprint_absorption=has_absorption_sig,
                rr_ratio=rr,
                ema89=ema89_val
            )

            if knife_vetoed:
                validation_errors.append(knife_reason)
                logger.warning(f"[{sym}] {knife_reason}")

            # Vector Semantic Audit against Historical Loss Memories
            whale_imb_val = float(whale_imbalance) if whale_imbalance is not None else 0.0
            sem_audit = self.critic_engine.audit_candidate_setup(
                symbol=sym,
                direction=direction,
                price=entry,
                confluence_score=confluence_score,
                htf_bias=h4_trend,
                cvd_slope=cvd_slope_val,
                whale_imbalance=whale_imb_val,
                liq_regime=tv_liq_bias if has_tv_liq_spike else "NORMAL",
                value_area_relation=val_status,
                rr_ratio=rr
            )

            if sem_audit.get("critic_verdict") == "HARD_VETO":
                validation_errors.append(sem_audit.get("reason", "CRITIC_VECTOR_VETO"))
                logger.warning(f"[{sym}] {sem_audit.get('reason')}")
            elif sem_audit.get("penalty", 0.0) > 0:
                pen = sem_audit["penalty"]
                confluence_score -= pen
                reasons.append(f"Critic Memory Penalty (-{pen:.0f} pts): {sem_audit.get('reason')}")

            critic_audit = {
                "knife_vetoed": knife_vetoed,
                "knife_reason": knife_reason,
                "vector_verdict": sem_audit.get("critic_verdict"),
                "similarity": sem_audit.get("max_similarity"),
                "penalty": sem_audit.get("penalty"),
                "matched_mode": sem_audit.get("matched_failure_mode"),
                "mitigation": sem_audit.get("matched_lesson")
            }

        # Microstructure VPIN Order Flow Toxicity Check (Lopez de Prado)
        vpin_metrics = {"vpin": 0.50, "regime": "UNKNOWN", "data_valid": False}
        try:
            from Terminal.microstructure_vpin_engine import compute_vpin_and_toxicity
            vpin_metrics = compute_vpin_and_toxicity(sym)
            if vpin_metrics.get("data_valid") and vpin_metrics.get("veto_toxic_flow"):
                v_reason = f"VETO_TOXIC_ORDER_FLOW: VPIN ({vpin_metrics['vpin']:.2f}) >= 0.80 indicates extreme informed order flow toxicity"
                validation_errors.append(v_reason)
                logger.warning(f"[{sym}] {v_reason}")
        except Exception as ve:
            logger.debug(f"VPIN computation note for {sym}: {ve}")

        reasons.extend('Validation Veto: ' + error for error in validation_errors)

        confluence_score = max(0.0, min(100.0, confluence_score))

        is_actionable = bool(confluence_score >= 75.0 + hurdle and direction in ('BUY', 'SELL')

                             and rr >= MIN_RR_RATIO and geom_valid and solved_lot is not None

                             and not validation_errors and not forecast_veto)

        deepseek_critic = {}
        dsh = getattr(self, "dsh_bridge", None)
        if is_actionable and dsh and dsh.is_configured():
            try:
                candidate_summary = {
                    "symbol": sym,
                    "direction": direction,
                    "confluence_score": confluence_score,
                    "signal_details": sig,
                    "confluence_reasons": reasons,
                    "trade_levels": {
                        "entry": entry,
                        "stop_loss": sl,
                        "take_profit": tp1,
                        "rr": rr,
                    }
                }
                dsh_audit = dsh.evaluate_signal_confluence(candidate_summary)
                deepseek_critic = dsh_audit
                if not dsh_audit.get("approved", False):
                    veto_msg = f"DEEPSEEK_CRITIC_VETO: {dsh_audit.get('reason')}"
                    validation_errors.append(veto_msg)
                    reasons.append(f"Validation Veto: {veto_msg}")
                    is_actionable = False
            except Exception as dsh_err:
                logger.warning(f"[{sym}] DeepSeek critic evaluation error: {dsh_err}. Failing closed.")
                veto_msg = f"DEEPSEEK_CRITIC_ERROR_FAIL_CLOSED: {dsh_err}"
                validation_errors.append(veto_msg)
                reasons.append(f"Validation Veto: {veto_msg}")
                is_actionable = False

        delta_authorized = bool(is_actionable and delta_prod is not None)
        # MetaTrader5 strictly decommissioned per AGENTS.md policy
        mt5_authorized = False



        # Quantitative Setup Classification: Trend-Following Pullback vs Mean Reversal

        val_status = str(auction.get('value_status', '') or '')

        p_vs_vwap = str(auction.get('price_vs_vwap', '') or '')

        mtf_bias = str(auction.get('mtf_bias', '') or '')

        ema_align = str(auction.get('ema_alignment', '') or '')

        sig_code_val = sig.get('sig_code', '')

        setup_type = 'UNCLASSIFIED'

        setup_rationale = ''



        if direction == 'BUY':

            if val_status in ('BELOW_VAL', 'BELOW_VALUE') or p_vs_vwap == 'BELOW_VWAP' or 'capitulation' in str(reasons).lower() or sig_code_val in ('exh', 'abs'):

                setup_type = 'MEAN_REVERSAL'

                setup_rationale = f'Fading oversold auction discount ({val_status}) back towards Value Area / Day VWAP'

            elif (mtf_bias == 'HTF_BULLISH_CONFLUENCE' or 'BULLISH' in ema_align) and (val_status in ('INSIDE_VALUE', 'AT_VAL', 'AT_VAH') or p_vs_vwap == 'ABOVE_VWAP'):

                setup_type = 'TREND_FOLLOWING_PULLBACK'

                setup_rationale = f'Pullback to value support ({val_status}) aligned with prevailing HTF Bullish trend'

            else:

                setup_type = 'MEAN_REVERSAL' if p_vs_vwap == 'BELOW_VWAP' else 'TREND_FOLLOWING_PULLBACK'

                setup_rationale = f'Positioned relative to Day VWAP ({p_vs_vwap})'



        elif direction == 'SELL':

            if val_status in ('ABOVE_VAH', 'ABOVE_VALUE') or p_vs_vwap == 'ABOVE_VWAP' or 'blow-off' in str(reasons).lower() or sig_code_val in ('exh', 'abs'):

                setup_type = 'MEAN_REVERSAL'

                setup_rationale = f'Fading overextended auction premium ({val_status}) back towards Value Area / Day VWAP'

            elif (mtf_bias == 'HTF_BEARISH_CONFLUENCE' or 'BEARISH' in ema_align) and (val_status in ('INSIDE_VALUE', 'AT_VAL', 'AT_VAH') or p_vs_vwap == 'BELOW_VWAP'):

                setup_type = 'TREND_FOLLOWING_PULLBACK'

                setup_rationale = f'Pullback to value resistance ({val_status}) aligned with prevailing HTF Bearish trend'

            else:

                setup_type = 'MEAN_REVERSAL' if p_vs_vwap == 'ABOVE_VWAP' else 'TREND_FOLLOWING_PULLBACK'

                setup_rationale = f'Positioned relative to Day VWAP ({p_vs_vwap})'



        # Counterfactual Logging (Arena Quant Audit Recommendation)

        # Store rejected setup so subsequent price movements evaluate filter efficiency

        if self.vector_mem and (not is_actionable) and (direction in ["BUY", "SELL"]) and (entry > 0):

            rejection_summary = "; ".join([r for r in reasons if "Veto" in r] or [f"Score {confluence_score:.1f} < 65.0"])

            self.vector_mem.record_rejected_setup(

                alert_id=f"{sym}_{sig.get('sig_code', 'sig')}_{datetime.now(timezone.utc).strftime('%H%M%S')}",

                symbol=sym,

                direction=direction,

                rejection_reason=rejection_summary,

                setup_text=setup_desc,

                entry_price=entry,

                stop_loss=sl,

                take_profit=tp1,

                confluence_score=confluence_score

            )



        eval_utc = datetime.now(timezone.utc).isoformat()

        eval_ist = utc_to_ist_str(eval_utc)

        candle_ist = utc_to_ist_str(sig_time)



        evaluation = {

            "evaluated_at_utc": eval_utc,

            "evaluated_at_ist": eval_ist,

            "symbol": sym,

            "mt5_symbol": mt5_sym,

            "delta_product_symbol": delta_prod.get("symbol") if delta_prod else None,

            "delta_execution_authorized": delta_authorized,
            "deepseek_critic": deepseek_critic,
            "vpin_metrics": vpin_metrics,

            "mt5_execution_authorized": mt5_authorized,

            "execution_model": execution_model,

            "setup_type": setup_type,

            "setup_rationale": setup_rationale,

            "fib_prime_pocket": {
                "ema34": ema34, "ema55": ema55, "ema59": ema59, "ema89": ema89, "ema233": ema233,
                "dist_55_59_pct": dist_55_59, "in_pullback_pocket": in_pullback_pocket
            },

            "validation_errors": validation_errors,

            "direction": direction,

            "is_actionable": is_actionable,

            "confluence_score": round(confluence_score, 1),

            "ev_300bar": float(emp_edge.get("ev_r", 0.0)),

            "signal_details": {

                "sig_name": sig_name,

                "sig_code": sig.get("sig_code", ""),

                "side": sig_side,

                "title": sig_title,

                "description": sig_desc,

                "candle_utc": sig_time,

                "candle_ist": candle_ist,

                "price": entry

            },

            "trade_levels": {

                "entry_price": entry,

                "stop_loss": sl,

                "take_profit_1": tp1,

                "take_profit_2": tp2,

                "rr_ratio": rr,

                "ev_300bar": float(emp_edge.get("ev_r", 0.0)),

                "sl_pct": round(sl_pct * 100, 2),

                "atr_pct": atr_pct,

                "admissible_lots": solved_lot,

                "realized_risk_usd": realized_risk,

                "effective_max_risk_usd": effective_max_risk,

                "hard_max_risk_usd": MAX_RISK_USD

            },

            "order_flow_metrics": {

                "whale_imbalance_bid_ratio": round(whale_imbalance, 3),

                "tight_bid_walls_usd": whales.get("tight_bid_walls_usd", 0.0),

                "tight_ask_walls_usd": whales.get("tight_ask_walls_usd", 0.0),

                "near_field_shorts_pct": round(near_field_shorts, 1),

                "cvd_slope": cvd_slope,

                "recent_poc": poc_price,
                "whale_tracked": bool((whales.get("feed_status") == "ACTIVE" or whales.get("data_valid") is True) and whales.get("whale_imbalance_bid_ratio") is not None),
                "liq_tracked": bool((liq.get("data_valid", False) is True or bool(nf_meta)) and near_field_shorts is not None)

            },

            "liquidation_microstructure": {

                "near_field": nf_meta,

                "top_short_magnets": short_magnets,

                "top_long_magnets": long_magnets,

                "strength_by_distance": liq.get("strength_by_distance", []),

                "leverage_tiers": liq.get("leverage_tiers", []),

                "key_levels": key_levels_list

            },

            "market_regime": {

                "regime": current_regime,

                "volatility": vol_regime,

                "atr_pct": atr_pct

            },

            "deepmind_timesfm": {
                "cvd_direction": cvd_dir,
                "cvd_confidence": cvd_conf,
                "cvd_uncertainty_multiplier": cvd_unc_mult,
                "cvd_increment_sum": tfm_cvd.get("predicted_increment_sum", 0.0),
                "confluence_hurdle_adjustment": round(hurdle, 2),
                "required_threshold": round(75.0 + hurdle, 2),
                "price_direction": price_dir,
                "price_confidence": price_conf,
                "price_increment_sum": tfm_price.get("predicted_increment_sum", 0.0)
            },

            "vector_memory": {

                "verdict": vm_verdict,

                "win_rate": vm_win_rate

            },

            "tv_liquidation_metrics": {

                "recent_spike": has_tv_liq_spike,

                "reversal_bias": tv_liq_bias,

                "exhaustion_score": tv_liq_exhaustion,

                "latest_long_liq_usd": tv_liq_long_usd,

                "latest_short_liq_usd": tv_liq_short_usd

            },

            "auction_profile": {

                "day_vwap": auction.get("day_vwap"),

                "day_open": auction.get("day_open"),

                "day_high": auction.get("day_high"),

                "day_low": auction.get("day_low"),

                "day_half_back": auction.get("day_half_back"),

                "day_vah": auction.get("day_vah"),

                "day_val": auction.get("day_val"),

                "day_poc": auction.get("day_poc"),

                "value_status": auction.get("value_status"),

                "vwap_distance_pct": auction.get("vwap_distance_pct"),

                "initial_balance_high": auction.get("initial_balance_high"),

                "initial_balance_low": auction.get("initial_balance_low"),

                "weekly_open": auction.get("weekly_open"),

                "pdh": auction.get("pdh"),

                "pdl": auction.get("pdl"),

                "pdc": auction.get("pdc"),

                "pd_vwap": auction.get("pd_vwap"),

                "pd_poc": auction.get("pd_poc"),

                "asia_vwap": auction.get("asia_vwap"),

                "london_vwap": auction.get("london_vwap"),

                "ny_vwap": auction.get("ny_vwap"),

                "fib_prime_alignment": auction.get("fib_prime_alignment"),

                "ema5": auction.get("ema5"),

                "ema13": auction.get("ema13"),

                "ema21": auction.get("ema21"),

                "ema55": auction.get("ema55"),

                "ema89": auction.get("ema89"),

                "ema144": auction.get("ema144"),

                "ema233": auction.get("ema233"),

                "htf_1h_trend": auction.get("htf_1h_trend"),

                "htf_4h_trend": auction.get("htf_4h_trend"),

                "mtf_bias": auction.get("mtf_bias"),

                "nearest_whale_bid": auction.get("nearest_whale_bid"),

                "nearest_whale_ask": auction.get("nearest_whale_ask")

            },

            "confluence_reasons": reasons,

            "critic_audit": critic_audit,

            "five_pillar_justification": {
                "pillar_1_footprint_signals": f"{sig_name} ({sig_side}) @ ${entry:,.4f} | Delta: {sig.get('delta', 'N/A')} | CVD: {cvd_slope}" + (f" | Multi-Signal Cluster: {sig.get('cluster_signal_names')}" if sig.get('is_cluster') else ""),
                "pillar_2_auction_value": f"Day VWAP: ${day_vwap:,.4f} ({p_vs_vwap}) | Value Area: VAH=${day_vah:,.4f}, VAL=${day_val:,.4f} ({val_status}) | Fib Pocket: {fib_align} (EMA55=${ema55:,.4f}, EMA233=${ema233:,.4f})",
                "pillar_3_live_liquidations": f"TV Liq Spike: {has_tv_liq_spike} (Bias: {tv_liq_bias}, Exhaustion: {tv_liq_exhaustion:.0f}/100, LongLiq: ${tv_liq_long_usd:,.0f}, ShortLiq: ${tv_liq_short_usd:,.0f})" if has_tv_liq_spike else "No active live TV liquidation blow-off spike on 15m window (normal flow)",
                "pillar_4_liquidation_heatmap": f"Near-Field Shorts: {near_field_shorts:.1f}% vs Longs: {100-near_field_shorts:.1f}% | Nearest Magnet: {short_magnets[0].get('zone_price') if direction=='BUY' and short_magnets else (long_magnets[0].get('zone_price') if direction=='SELL' and long_magnets else 'N/A')}",
                "pillar_5_whales_and_sweeps": f"Whale Book Imbalance: {whale_imbalance*100:.1f}% Bids vs {(1-whale_imbalance)*100:.1f}% Asks | Sweeps: {sweep_bias} (Net: ${net_whale_flow/1e6:+.2f}M filled)"
            }

        }



        # Persist alert record

        self._persist_alert(evaluation)

        return evaluation



    def evaluate_new_signals(self, new_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        """Batch evaluates all new signals discovered in the recent sweep."""

        results = []

        for s in new_signals:

            try:

                res = self.evaluate_signal(s)

                results.append(res)

                

                # Prominently log actionable trades

                if res["is_actionable"]:

                    # Acoustic alert (Windows beep)

                    if winsound is not None:

                        try:

                            winsound.Beep(1200, 350)

                        except Exception:

                            pass

                    print("\a", end="", flush=True)  # Terminal bell



                    logger.info("\n" + "=" * 90)

                    logger.info(f"ðŸš¨ ACTIONABLE REAL-TIME TRADE ALERT: [{res['symbol']}] {res['direction']} (Score: {res['confluence_score']}/100)")

                    logger.info(f"   Time: {res['evaluated_at_ist']} (UTC: {res['evaluated_at_utc']})")

                    logger.info(f"   Signal Bar: {res['signal_details']['candle_ist']} | Signal: {res['signal_details']['sig_name']} ({res['signal_details']['side']})")

                    logger.info(f"   Entry: {res['trade_levels']['entry_price']} | SL: {res['trade_levels']['stop_loss']} (-{res['trade_levels']['sl_pct']}%) | TP1: {res['trade_levels']['take_profit_1']} (R:R {res['trade_levels']['rr_ratio']}:1)")

                    logger.info(f"   DeepMind TimesFM: CVD={res['deepmind_timesfm']['cvd_direction']} ({res['deepmind_timesfm']['cvd_confidence']:.0%}) | Price={res['deepmind_timesfm']['price_direction']} ({res['deepmind_timesfm']['price_confidence']:.0%})")

                    logger.info(f"   Drivers: {', '.join(res['confluence_reasons'])}")

                    logger.info("=" * 90)

                else:

                    logger.info(f"[EVALUATOR] {res['symbol']} {res['signal_details']['sig_name']} ({res['direction']}) -> Score: {res['confluence_score']}/100 [Not actionable]")

            except Exception as e:

                logger.error(f"Error evaluating signal for {s.get('symbol')}: {e}")

        return results



    @staticmethod

    def _finite(value, default):

        try:

            value = float(value)

            return value if math.isfinite(value) else default

        except (TypeError, ValueError):

            return default



    @staticmethod

    def _broker_sizing(symbol, direction, entry, sl, tp, cap):

        # Pure offline discrete sizing (MetaTrader5 decommissioned per AGENTS.md policy)
        risk_dist = abs(entry - sl) if entry and sl else 1.0
        lots = max(1, int(cap / max(1e-6, risk_dist)))
        return {
            "lots": lots,
            "risk_usd": round(lots * risk_dist, 2),
            "cap_usd": cap
        }



    def _persist_alert(self, alert: Dict[str, Any]):

        with writer_lock(ALERTS_JSONL):

            self._persist_alert_locked(alert)



    def _persist_alert_locked(self, alert: Dict[str, Any]):

        """Saves alert to jsonl and updates realtime_trade_alerts.parquet atomically."""

        DB_DIR.mkdir(parents=True, exist_ok=True)

        try:

            with open(ALERTS_JSONL, "a", encoding="utf-8") as f:

                f.write(json.dumps(alert) + "\n")

        except Exception as e:

            logger.warning(f"Failed to append to {ALERTS_JSONL}: {e}")



        # Update parquet summary

        try:

            c_utc = alert["signal_details"].get("candle_utc") or alert["evaluated_at_utc"]

            c_ist = alert["signal_details"].get("candle_ist") or alert.get("evaluated_at_ist", "")

            if not c_ist and c_utc:

                c_ist = utc_to_ist_str(c_utc)

            mt5_sym = alert.get("mt5_symbol") or f"{alert['symbol']}USD.p"

            reg_val = alert.get("market_regime", {}).get("regime", "UNKNOWN")

            if reg_val == "ERROR":

                reg_val = "UNKNOWN"



            flat_record = {

                "evaluated_at_utc": alert["evaluated_at_utc"],

                "evaluated_at_ist": alert.get("evaluated_at_ist") or utc_to_ist_str(alert["evaluated_at_utc"]),

                "symbol": alert["symbol"],

                "mt5_symbol": mt5_sym,

                "direction": alert["direction"],

                "is_actionable": alert["is_actionable"],

                "confluence_score": alert["confluence_score"],

                "sig_name": alert["signal_details"].get("sig_name") or "Signal",

                "sig_code": alert["signal_details"].get("sig_code") or "sig",

                "candle_utc": c_utc,

                "candle_ist": c_ist,

                "entry_price": alert["trade_levels"]["entry_price"],

                "stop_loss": alert["trade_levels"]["stop_loss"],

                "take_profit_1": alert["trade_levels"]["take_profit_1"],

                "rr_ratio": alert["trade_levels"]["rr_ratio"],

                "sl_pct": alert["trade_levels"].get("sl_pct", 0.8),

                "whale_imbalance": alert["order_flow_metrics"]["whale_imbalance_bid_ratio"],

                "cvd_slope": alert["order_flow_metrics"]["cvd_slope"],

                "regime": reg_val,

                "timesfm_cvd_dir": alert["deepmind_timesfm"].get("cvd_direction") or "N/A",

                "timesfm_cvd_conf": self._finite(alert["deepmind_timesfm"].get("cvd_confidence"), 0.0),

                "timesfm_price_dir": alert["deepmind_timesfm"].get("price_direction") or "N/A",

                "timesfm_price_conf": self._finite(alert["deepmind_timesfm"].get("price_confidence"), 0.0),

                "tv_liq_bias": alert.get("tv_liquidation_metrics", {}).get("reversal_bias") or "NEUTRAL",

                "tv_liq_exhaustion": self._finite(alert.get("tv_liquidation_metrics", {}).get("exhaustion_score"), 0.0),

                "reasons": ", ".join(alert["confluence_reasons"])

            }

            new_df = pd.DataFrame([flat_record])

            if ALERTS_PARQUET.exists():

                try:

                    old_df = pd.read_parquet(ALERTS_PARQUET)

                    old_df = old_df.drop(columns=["timesfm_direction", "timesfm_confidence", "timesfm_drift"], errors="ignore")

                    combined = pd.concat([old_df, new_df], ignore_index=True)

                    combined = combined.drop_duplicates(subset=["symbol", "sig_code", "candle_utc"], keep="last")

                except Exception:

                    raise

            else:

                combined = new_df



            atomic_parquet(combined.tail(100000), ALERTS_PARQUET)

        except Exception as e:

            logger.warning(f"Failed to update {ALERTS_PARQUET}: {e}")





if __name__ == "__main__":

    print("Testing RealtimeSignalEvaluator...")

    evaluator = RealtimeSignalEvaluator()

    dummy_signal = {

        "symbol": "ADA",

        "mt5_symbol": "ADAUSD.p",

        "sig_name": "Absorption",

        "sig_code": "abs",

        "side": "bearish",

        "price": 0.1938,

        "title": "Buying absorbed at the high",

        "description": "843K of taker buying absorbed at high",

        "datetime_utc": datetime.now(timezone.utc).isoformat()

    }

    res = evaluator.evaluate_signal(dummy_signal)

    print(f"Evaluation complete! Actionable: {res['is_actionable']}, Score: {res['confluence_score']}")

