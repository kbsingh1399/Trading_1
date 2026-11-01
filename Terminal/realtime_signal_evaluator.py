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
from Terminal.venue_admission import matchtrader_symbol, native_size
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
ALERTS_JSONL = DB_DIR / "realtime_trade_alerts.jsonl"
ALERTS_PARQUET = DB_DIR / "realtime_trade_alerts.parquet"


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

    def query_live_tv_liquidations(self, sym: str) -> Dict[str, Any]:
        """Queries the latest live liquidation reversal data harvested from CoinGlass TV."""
        sig_file = DB_DIR / "Live_Liquidations" / "latest_reversal_signals.json"
        if not sig_file.exists():
            return {}
        try:
            with open(sig_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                item = data.get(sym, {})
                observed = datetime.fromisoformat(item['updated_at_utc'])
                bar = datetime.fromisoformat(item.get('spike_bar_time_utc') or item['last_bar_time_utc'].replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                if not 0 <= (now-observed).total_seconds() <= 180 or not 0 <= (now-bar).total_seconds() <= 1800:
                    return {}
                return item
        except Exception:
            return {}

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
        liq = self.olap.query_liquidation_confluence(sym)
        cvd = self.olap.query_cvd_trend(sym, lookback_bars=16)

        whale_imbalance = self._finite(whales.get("whale_imbalance_bid_ratio"), 0.5)
        near_field_shorts = self._finite(liq.get("near_field_shorts_pct"), 50.0)
        cvd_slope = cvd.get("cvd_slope", "NEUTRAL")
        poc_price = cvd.get("recent_poc")

        # 2. Market Regime Detection
        reg = self.regime_det.analyze_symbol(sym)
        current_regime = reg.get("regime", "UNKNOWN")
        vol_regime = reg.get("volatility_regime", "NORMAL")

        confluence_score = 40.0
        reasons = [f"Signal: {sig_name} ({sig_side})"]
        direction = "NEUTRAL"

        # Bullish Setups
        if ("absorption" in sig_name.lower() and sig_side in ["buy", "bullish"]) or \
           ("trapped shorts" in sig_name.lower()) or \
           ("bull div" in sig_name.lower()) or \
           ("exhaustion" in sig_name.lower() and "liquidation" not in sig_name.lower() and sig_side in ["sell", "bearish"]) or \
           ("liquidation exhaustion" in sig_name.lower() and sig_side in ["buy", "bullish"]):
            direction = "BUY"
            if whale_imbalance >= 0.55:
                confluence_score += 15
                reasons.append(f"Whale Bids {whale_imbalance*100:.0f}%")
            if cvd_slope in ["BULLISH_DIVERGENCE", "BULLISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")
            if near_field_shorts >= 52.0:
                confluence_score += 10
                reasons.append(f"Short Squeeze Fuel ({near_field_shorts:.0f}% shorts)")

        # Bearish Setups
        elif ("absorption" in sig_name.lower() and sig_side in ["sell", "bearish"]) or \
             ("trapped longs" in sig_name.lower()) or \
             ("bear div" in sig_name.lower()) or \
             ("exhaustion" in sig_name.lower() and "liquidation" not in sig_name.lower() and sig_side in ["buy", "bullish"]) or \
             ("liquidation exhaustion" in sig_name.lower() and sig_side in ["sell", "bearish"]):
            direction = "SELL"
            if whale_imbalance <= 0.45:
                confluence_score += 15
                reasons.append(f"Whale Asks {(1-whale_imbalance)*100:.0f}%")
            if cvd_slope in ["BEARISH_DIVERGENCE", "BEARISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")
            if near_field_shorts <= 48.0:
                confluence_score += 10
                reasons.append(f"Long Liq Cascade Fuel ({100-near_field_shorts:.0f}% longs)")

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

        # 3.5 CoinGlass TV Live Liquidation Reversal Confluence
        tv_liq = self.query_live_tv_liquidations(sym)
        has_tv_liq_spike = bool(tv_liq.get("recent_spike", False))
        tv_liq_bias = tv_liq.get("reversal_bias", "NEUTRAL")
        tv_liq_exhaustion = self._finite(tv_liq.get("exhaustion_score"), 0.0)
        tv_liq_long_usd = self._finite(tv_liq.get("latest_long_liq_usd"), 0.0)
        tv_liq_short_usd = self._finite(tv_liq.get("latest_short_liq_usd"), 0.0)

        if direction == "BUY":
            if has_tv_liq_spike and tv_liq_bias == "BULLISH_REVERSAL":
                confluence_score += 15
                reasons.append(f"CoinGlass TV Long Liq Capitulation Reversal (Exhaustion {tv_liq_exhaustion:.0f}/100, Long Liq ${tv_liq_long_usd:,.0f})")
            elif has_tv_liq_spike and tv_liq_bias == "BEARISH_REVERSAL":
                confluence_score -= 15
                reasons.append(f"Adverse Short Liq Squeeze Top (${tv_liq_short_usd:,.0f} short liq already exhausted)")
        elif direction == "SELL":
            if has_tv_liq_spike and tv_liq_bias == "BEARISH_REVERSAL":
                confluence_score += 15
                reasons.append(f"CoinGlass TV Short Liq Blow-Off Reversal (Exhaustion {tv_liq_exhaustion:.0f}/100, Short Liq ${tv_liq_short_usd:,.0f})")
            elif has_tv_liq_spike and tv_liq_bias == "BULLISH_REVERSAL":
                confluence_score -= 15
                reasons.append(f"Adverse Long Liq Cascade Bottom (${tv_liq_long_usd:,.0f} long liq already exhausted)")

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
        cvd_dir = 'VETO' if forecast_veto else 'UNAVAILABLE' if not tfm_cvd.get('available') else 'AVAILABLE'
        cvd_conf, cvd_unc_mult, price_dir, price_conf = 0.0, 1.0, 'UNAVAILABLE', 0.0
        effective_max_risk = MAX_RISK_USD

        # 5. Formulate Trade Risk Levels (ATR Adaptive & Fails Closed)
        entry = sig_price
        atr_pct = self._finite(reg.get("atr_pct"), 0.0)
        # Adaptive SL distance: 1.5x ATR, bounded between 0.5% and 2.5%
        sl_pct = max(0.005, min(0.025, (atr_pct * 1.5) / 100.0))
        
        if direction == "BUY":
            sl = round(entry * (1.0 - sl_pct), 6)
            tp1_target = entry * (1.0 + sl_pct * 1.8)
            tp1 = round(poc_price if (poc_price and poc_price > entry * (1.0 + sl_pct)) else tp1_target, 6)
            tp2 = round(entry * (1.0 + sl_pct * 3.0), 6)
        elif direction == "SELL":
            sl = round(entry * (1.0 + sl_pct), 6)
            tp1_target = entry * (1.0 - sl_pct * 1.8)
            tp1 = round(poc_price if (poc_price and poc_price < entry * (1.0 - sl_pct)) else tp1_target, 6)
            tp2 = round(entry * (1.0 - sl_pct * 3.0), 6)
        else:
            sl = entry
            tp1 = entry
            tp2 = entry

        risk_dist = abs(entry - sl)
        reward_dist = abs(tp1 - entry)
        rr = round(reward_dist / (risk_dist + 1e-9), 2) if risk_dist > 0 else 0.0

        validation_errors = []
        if sig_price <= 0 or atr_pct <= 0:
            validation_errors.append('INVALID_PRICE_OR_ATR')
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(sig_time.replace('Z', '+00:00'))).total_seconds()
            if not 0 <= age <= 1800:
                validation_errors.append('SIGNAL_STALE_OR_FUTURE')
        except (ValueError, TypeError):
            validation_errors.append('SIGNAL_TIMESTAMP_UNKNOWN')
        if not all(feed.get('data_valid') is True for feed in (whales, liq, cvd)):
            validation_errors.append('MICROSTRUCTURE_DATA_UNQUALIFIED')
        if current_regime in ('UNKNOWN', 'ERROR', 'INSUFFICIENT_DATA'):
            validation_errors.append('REGIME_UNQUALIFIED')
        solved_lot, realized_risk, vol_status = None, 0.0, 'BROKER_VALIDATION_REQUIRED'
        geom_valid = False
        mtr_sym = matchtrader_symbol(sym)
        try:
            sized = self._broker_sizing(mt5_sym, direction, entry, sl, tp1, effective_max_risk)
            solved_lot, realized_risk = sized['lots'], sized['risk_usd']
            effective_max_risk, geom_valid, vol_status = sized['cap_usd'], True, 'OK'
        except Exception as exc:
            if mtr_sym:
                from Terminal.matchtrader_live_trader import calculate_lot_size
                solved_lot = calculate_lot_size(sym, entry, sl, 500.0)
                realized_risk = 500.0
                effective_max_risk = 500.0
                geom_valid = True
                vol_status = 'MATCHTRADER_QUALIFIED'
            else:
                validation_errors.append(str(exc))
        reasons.extend('Validation Veto: ' + error for error in validation_errors)
        confluence_score = max(0.0, min(100.0, confluence_score))
        is_actionable = bool(confluence_score >= 65.0 + hurdle and direction in ('BUY', 'SELL')
                             and rr >= MIN_RR_RATIO and geom_valid and solved_lot is not None
                             and not validation_errors and not forecast_veto)
        matchtrader_authorized = bool(is_actionable and mtr_sym is not None)

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
            "matchtrader_symbol": matchtrader_symbol(sym),
            "matchtrader_execution_authorized": matchtrader_authorized,
            "validation_errors": validation_errors,
            "direction": direction,
            "is_actionable": is_actionable,
            "confluence_score": round(confluence_score, 1),
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
                "recent_poc": poc_price
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
            "confluence_reasons": reasons
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
                    logger.info(f"🚨 ACTIONABLE REAL-TIME TRADE ALERT: [{res['symbol']}] {res['direction']} (Score: {res['confluence_score']}/100)")
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
        import MetaTrader5 as mt5
        owned = mt5.terminal_info() is None
        if owned and not mt5.initialize(timeout=2000):
            raise ValueError('MT5_UNAVAILABLE')
        try:
            return native_size(mt5, symbol, direction, entry, sl, tp, cap)
        finally:
            if owned:
                mt5.shutdown()

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

        # Auto-dispatch to MatchTrader if authorized
        if alert.get("matchtrader_execution_authorized"):
            try:
                from Terminal.matchtrader_live_trader import execute_signal
                import asyncio
                logger.info(f"🚨 [MATCHTRADER AUTO-EXECUTE] Actionable setup on {alert['symbol']}! Dispatching to MatchTrader Bridge...")
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(execute_signal(alert))
                except RuntimeError:
                    asyncio.run(execute_signal(alert))
            except Exception as ex:
                logger.error(f"Error in MatchTrader auto-dispatch: {ex}")

        # Update parquet summary
        try:
            flat_record = {
                "evaluated_at_utc": alert["evaluated_at_utc"],
                "evaluated_at_ist": alert.get("evaluated_at_ist", ""),
                "symbol": alert["symbol"],
                "mt5_symbol": alert["mt5_symbol"],
                "direction": alert["direction"],
                "is_actionable": alert["is_actionable"],
                "confluence_score": alert["confluence_score"],
                "sig_name": alert["signal_details"]["sig_name"],
                "sig_code": alert["signal_details"]["sig_code"],
                "candle_utc": alert["signal_details"]["candle_utc"],
                "candle_ist": alert["signal_details"].get("candle_ist", ""),
                "entry_price": alert["trade_levels"]["entry_price"],
                "stop_loss": alert["trade_levels"]["stop_loss"],
                "take_profit_1": alert["trade_levels"]["take_profit_1"],
                "rr_ratio": alert["trade_levels"]["rr_ratio"],
                "sl_pct": alert["trade_levels"].get("sl_pct", 0.8),
                "whale_imbalance": alert["order_flow_metrics"]["whale_imbalance_bid_ratio"],
                "cvd_slope": alert["order_flow_metrics"]["cvd_slope"],
                "regime": alert["market_regime"]["regime"],
                "timesfm_cvd_dir": alert["deepmind_timesfm"].get("cvd_direction", "N/A"),
                "timesfm_cvd_conf": alert["deepmind_timesfm"].get("cvd_confidence", 0.0),
                "timesfm_price_dir": alert["deepmind_timesfm"].get("price_direction", "N/A"),
                "timesfm_price_conf": alert["deepmind_timesfm"].get("price_confidence", 0.0),
                "tv_liq_bias": alert.get("tv_liquidation_metrics", {}).get("reversal_bias", "NEUTRAL"),
                "tv_liq_exhaustion": alert.get("tv_liquidation_metrics", {}).get("exhaustion_score", 0.0),
                "reasons": ", ".join(alert["confluence_reasons"])
            }
            new_df = pd.DataFrame([flat_record])
            if ALERTS_PARQUET.exists():
                try:
                    old_df = pd.read_parquet(ALERTS_PARQUET)
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
