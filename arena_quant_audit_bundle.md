# Institutional Quantitative Audit Submission: Order-Flow & Microstructure Execution Engine

You are acting as an Elite Principal Quantitative Trader & Execution Architect.
Please perform a rigorous, institutional critique of our algorithmic trading architecture and order-flow execution pipeline.

## Institutional Upgrades Implemented:
1. **Canonical Risk Envelope (`risk_envelope.py`)**:
   - Single source of truth. Fails closed on inverted stops, non-positive levels, and spread > 45 bps.
   - Exact discrete grid volume solver `admissible_volume()` using integer step bounds $k_{min} = \lceil floor / loss\_per\_step \rceil$ and $k_{max} = \lfloor cap / loss\_per\_step \rfloor$.
   - Hard $50.00 max risk ceiling per trade, $5.00 min risk floor, max 2 concurrent positions.
   - Prop-firm floor defense: capital budget $\le 50\%$ of available equity buffer above $4,700 floor; halt if buffer $\le $10.00.

2. **Google DeepMind TimesFM 2.5 Stationary Increment Forecaster (`deepmind_timesfm_forecaster.py`)**:
   - Replaced non-stationary CVD level forecasting with first differences: $\Delta CVD_t = CVD_t - CVD_{t-1}$.
   - Forecasts future stationary increments and sums them for directional consensus.
   - Quantifies epistemic uncertainty via $(q_{90} - q_{10})$ quantile spread relative to series standard deviation, generating an `uncertainty_multiplier` (0.40 to 1.0) that discounts max risk ceiling during high uncertainty.

3. **Counterfactual Memory in LanceDB (`vector_memory.py`)**:
   - Added `counterfactual_setups` vector table.
   - Records all rejected setups with geometry and rejection reason to track opportunity cost and quantify filter efficiency over time.

4. **Autonomous Realtime Evaluator (`realtime_signal_evaluator.py`)**:
   - Automatically triggered on footprint and order-flow signal discoveries.
   - Synthesizes DuckDB OLAP microstructure, regime classification, vector memory analogs, and TimesFM stationary increments.
   - Discretely solves volume and validates geometry before declaring a setup actionable.

## Full Source Code of Core Modules:

---
### File: `Terminal/risk_envelope.py`

```python
"""
========================================================================================
Institutional Risk Envelope & Admissible Volume Solver (Single Source of Truth)
========================================================================================
Canonical risk governance and mathematical volume solver.
Every execution and signal evaluation module imports from here.

Design Principles (From Arena Quant Audit):
1. Fails Closed: Inverted stops, zero distances, or missing values reject the trade.
2. Exact Discrete Grid Solver: Solves for the largest legal volume on broker lot grids
   strictly within [MIN_RISK_USD, MAX_RISK_USD] without rounding traps.
3. Dual-Constraint Capital Budgeting: Position risk <= 50% of buffer above $4,700 floor.
4. Hard Invariants: Max 2 concurrent open positions, max 0.45% spread gate.
========================================================================================
"""

import math
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger("RiskEnvelope")

# ---------------------------------------------------------------------------
# Canonical Risk Constants (SINGLE SOURCE OF TRUTH)
# ---------------------------------------------------------------------------
HARD_FLOOR_USD = 4700.00           # 6% Max Drawdown Floor on $5,000 Prop-Firm Challenge
OPERATING_BUFFER_MIN = 10.00       # Equity buffer threshold below which all trading halts
BUFFER_RISK_FRACTION = 0.50        # Max 50% of available buffer risked per trade
MAX_RISK_USD = 50.00               # Strict hard ceiling per trade ($50.00)
MIN_RISK_USD = 5.00                # Minimum risk threshold to prevent micro-lot commission drag
MAX_OPEN_POSITIONS = 2             # Hard mandate: 2 concurrent positions max
MAX_ALLOWED_SPREAD_PCT = 0.45      # 0.45% maximum allowable spread (45 bps)
MIN_RR_RATIO = 1.50                # Minimum 1.5:1 reward-to-risk ratio


def admissible_volume(
    loss_per_lot: float,
    step: float,
    min_lot: float,
    max_lot: float,
    floor_usd: float = MIN_RISK_USD,
    cap_usd: float = MAX_RISK_USD
) -> Tuple[Optional[float], str, float]:
    """
    Solves for the largest broker grid volume whose realized dollar risk
    lies strictly inside [floor_usd, cap_usd].

    Args:
        loss_per_lot: Dollar loss if 1.0 lot hits stop-loss (points_dist * tick_val / tick_size).
        step: Broker volume_step (e.g. 0.01 or 0.1).
        min_lot: Broker minimum lot size (e.g. 0.01).
        max_lot: Broker maximum lot size (e.g. 100.0).
        floor_usd: Lower dollar risk bound (e.g. $5.00).
        cap_usd: Upper dollar risk bound (e.g. $50.00 or buffer * 0.50).

    Returns:
        (volume, reason_code, realized_risk_usd)
        If admissible: (float_lots, "OK", dollar_risk)
        If inadmissible: (None, specific_rejection_reason, 0.0)
    """
    if loss_per_lot <= 0.0 or step <= 0.0:
        return None, "INVALID_LOSS_PER_LOT_OR_STEP", 0.0

    if cap_usd < floor_usd:
        return None, f"EMPTY_CAP_WINDOW: cap (${cap_usd:.2f}) < floor (${floor_usd:.2f})", 0.0

    # Risk per single step
    loss_per_step = loss_per_lot * step

    # Calculate discrete integer step bounds
    k_min = math.ceil((floor_usd / loss_per_step) - 1e-12)
    k_max = math.floor((cap_usd / loss_per_step) + 1e-12)

    # Convert broker min_lot/max_lot to step multipliers
    k_broker_min = math.ceil((min_lot / step) - 1e-12)
    k_broker_max = math.floor((max_lot / step) + 1e-12)

    # Intersect allowable risk steps with broker allowed range
    effective_k_min = max(k_min, k_broker_min)
    effective_k_max = min(k_max, k_broker_max)

    if effective_k_max < effective_k_min:
        actual_min_risk = (effective_k_min * step) * loss_per_lot
        actual_max_risk = (effective_k_max * step) * loss_per_lot
        return None, (
            f"NO_REPRESENTABLE_VOLUME: grid step {step} gives [{actual_min_risk:.2f}, {actual_max_risk:.2f}] "
            f"outside target risk window [${floor_usd:.2f}, ${cap_usd:.2f}]"
        ), 0.0

    # Select the largest legal volume inside the window
    final_lots = round(effective_k_max * step, 6)
    actual_risk_usd = round(final_lots * loss_per_lot, 2)

    # Final safety sanity assert
    if actual_risk_usd > cap_usd + 0.01:
        return None, f"NUMERICAL_OVERFLOW: realized risk ${actual_risk_usd:.2f} > cap ${cap_usd:.2f}", 0.0

    return final_lots, "OK", actual_risk_usd


def compute_dynamic_capital_budget(current_equity: float) -> Tuple[float, bool, str]:
    """
    Computes available dollar risk based on prop-firm floor defense.
    Risk ceiling = min(MAX_RISK_USD, buffer * 50%).
    """
    if current_equity <= HARD_FLOOR_USD:
        return 0.0, False, f"EQUITY_BREACH: Current equity ${current_equity:.2f} <= Floor ${HARD_FLOOR_USD:.2f}"

    buffer = current_equity - HARD_FLOOR_USD
    if buffer <= OPERATING_BUFFER_MIN:
        return 0.0, False, f"HALT_BUFFER: Buffer ${buffer:.2f} <= minimum required ${OPERATING_BUFFER_MIN:.2f}"

    # Capital budgeted: half of buffer or $50.00 max
    budget = min(MAX_RISK_USD, round(buffer * BUFFER_RISK_FRACTION, 2))
    budget = max(MIN_RISK_USD, budget)
    return budget, True, f"OK (Buffer: ${buffer:.2f}, Budget: ${budget:.2f})"


def validate_trade_geometry(
    direction: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
    spread_pct: float
) -> Tuple[bool, str]:
    """
    Fails closed on any invalid geometry or spread breach.
    """
    if spread_pct > MAX_ALLOWED_SPREAD_PCT:
        return False, f"SPREAD_GATE_BREACH: Spread {spread_pct:.3f}% > Max allowed {MAX_ALLOWED_SPREAD_PCT:.3f}%"

    if entry <= 0.0 or stop_loss <= 0.0 or take_profit <= 0.0:
        return False, "INVALID_PRICE_LEVELS: Non-positive prices"

    dir_upper = direction.upper()
    if dir_upper == "BUY":
        if stop_loss >= entry:
            return False, f"INVERTED_STOP: BUY stop-loss {stop_loss} >= entry {entry}"
        if take_profit <= entry:
            return False, f"INVERTED_TARGET: BUY take-profit {take_profit} <= entry {entry}"
    elif dir_upper == "SELL":
        if stop_loss <= entry:
            return False, f"INVERTED_STOP: SELL stop-loss {stop_loss} <= entry {entry}"
        if take_profit >= entry:
            return False, f"INVERTED_TARGET: SELL take-profit {take_profit} >= entry {entry}"
    else:
        return False, f"INVALID_DIRECTION: {direction}"

    risk_dist = abs(entry - stop_loss)
    reward_dist = abs(take_profit - entry)
    rr = reward_dist / (risk_dist + 1e-9)

    if rr < MIN_RR_RATIO:
        return False, f"POOR_REWARD_TO_RISK: R:R {rr:.2f} < Min required {MIN_RR_RATIO:.2f}"

    return True, "OK"

```

---
### File: `Terminal/realtime_signal_evaluator.py`

```python
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
        self.vector_mem = VectorMemory()
        self.timesfm = DeepMindTimesFMForecaster()

    def evaluate_signal(self, sig: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deeply inspects a newly detected signal against the entire data pipeline.
        """
        sym = sig.get("symbol", "")
        mt5_sym = sig.get("mt5_symbol", "")
        sig_name = sig.get("sig_name", "")
        sig_side = str(sig.get("side", "")).lower()
        sig_price = float(sig.get("price", 0.0))
        sig_time = sig.get("datetime_utc", "")
        sig_desc = sig.get("description", "")
        sig_title = sig.get("title", "")

        # 1. OLAP Microstructure Queries
        whales = self.olap.query_whale_walls(sym)
        liq = self.olap.query_liquidation_confluence(sym)
        cvd = self.olap.query_cvd_trend(sym, lookback_bars=16)

        whale_imbalance = whales.get("whale_imbalance_bid_ratio", 0.5)
        near_field_shorts = liq.get("near_field_shorts_pct", 50.0)
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
           ("exhaustion" in sig_name.lower() and sig_side in ["sell", "bearish"]):
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
             ("exhaustion" in sig_name.lower() and sig_side in ["buy", "bullish"]):
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
        analogs = self.vector_mem.query_similar_setups(setup_desc, top_k=3)
        vm_verdict = analogs.get("verdict", "INCONCLUSIVE")
        vm_win_rate = analogs.get("win_rate", 0.5)

        if vm_verdict == "FAVORABLE":
            confluence_score += 10
            reasons.append(f"Vector Memory Win Rate {vm_win_rate*100:.0f}%")
        elif vm_verdict == "UNFAVORABLE":
            confluence_score -= 15
            reasons.append("Vector Memory Unfavorable Veto")

        # 4. Google DeepMind TimesFM 2.5 Dual Stationary Increments (CVD + Price)
        cvd_series = self.olap.get_series(sym, "cumulative_delta", limit=64)
        price_series = self.olap.get_series(sym, "close", limit=64)
        
        tfm_cvd = {"available": False}
        tfm_price = {"available": False}
        
        if self.timesfm.is_available:
            if len(cvd_series) >= 16:
                # Stationary increments forecast: ΔCVD_t = CVD_t - CVD_{t-1}
                tfm_cvd = self.timesfm.forecast_increments(cvd_series, horizon=8)
            if len(price_series) >= 16:
                tfm_price = self.timesfm.forecast_increments(price_series, horizon=8)

        cvd_dir = tfm_cvd.get("increment_direction", "NEUTRAL") if tfm_cvd.get("available") else "N/A"
        cvd_conf = tfm_cvd.get("confidence", 0.0)
        cvd_unc_mult = tfm_cvd.get("uncertainty_multiplier", 1.0)

        price_dir = tfm_price.get("increment_direction", "NEUTRAL") if tfm_price.get("available") else "N/A"
        price_conf = tfm_price.get("confidence", 0.0)

        # Epistemic uncertainty discounts maximum risk budget
        effective_max_risk = round(MAX_RISK_USD * cvd_unc_mult, 2)
        effective_max_risk = max(MIN_RISK_USD, effective_max_risk)

        if direction == "BUY":
            if "BULLISH" in cvd_dir:
                confluence_score += 10
                reasons.append(f"TimesFM CVD {cvd_dir} (conf {cvd_conf:.0%}, unc_mult {cvd_unc_mult:.2f})")
            elif "STRONG_BEARISH" in cvd_dir and cvd_conf > 0.6:
                confluence_score -= 20
                reasons.append(f"TimesFM Adverse CVD Veto ({cvd_dir})")
                
            if "BULLISH" in price_dir:
                confluence_score += 10
                reasons.append(f"TimesFM Price {price_dir} ({price_conf:.0%})")
            elif "STRONG_BEARISH" in price_dir and price_conf > 0.6:
                confluence_score -= 20
                reasons.append(f"TimesFM Adverse Price Veto ({price_dir})")
                
        elif direction == "SELL":
            if "BEARISH" in cvd_dir:
                confluence_score += 10
                reasons.append(f"TimesFM CVD {cvd_dir} (conf {cvd_conf:.0%}, unc_mult {cvd_unc_mult:.2f})")
            elif "STRONG_BULLISH" in cvd_dir and cvd_conf > 0.6:
                confluence_score -= 20
                reasons.append(f"TimesFM Adverse CVD Veto ({cvd_dir})")
                
            if "BEARISH" in price_dir:
                confluence_score += 10
                reasons.append(f"TimesFM Price {price_dir} ({price_conf:.0%})")
            elif "STRONG_BULLISH" in price_dir and price_conf > 0.6:
                confluence_score -= 20
                reasons.append(f"TimesFM Adverse Price Veto ({price_dir})")

        # 5. Formulate Trade Risk Levels (ATR Adaptive & Fails Closed)
        entry = sig_price
        atr_pct = float(reg.get("atr_pct", 1.0))
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

        # Validate geometry via Canonical Risk Envelope
        geom_valid, geom_reason = validate_trade_geometry(
            direction=direction,
            entry=entry,
            stop_loss=sl,
            take_profit=tp1,
            spread_pct=0.05
        )
        if not geom_valid and direction != "NEUTRAL":
            reasons.append(f"Geometry Veto: {geom_reason}")

        # Exact discrete grid volume solver: adapts step and min_lot based on asset price scale
        loss_per_unit = risk_dist if risk_dist > 0 else 1.0
        if entry >= 100.0:
            grid_step = 0.01
            min_lot = 0.01
            max_lot = 50.0
        elif entry >= 1.0:
            grid_step = 0.1
            min_lot = 0.1
            max_lot = 5000.0
        else:
            grid_step = 1.0
            min_lot = 1.0
            max_lot = 50000.0

        solved_lot, vol_status, realized_risk = admissible_volume(
            loss_per_lot=loss_per_unit,
            step=grid_step,
            min_lot=min_lot,
            max_lot=max_lot,
            floor_usd=MIN_RISK_USD,
            cap_usd=effective_max_risk
        )
        if solved_lot is None and direction != "NEUTRAL":
            reasons.append(f"Volume Solver Veto: {vol_status}")

        is_actionable = (
            (confluence_score >= 65.0) and
            (direction != "NEUTRAL") and
            (rr >= MIN_RR_RATIO) and
            geom_valid and
            (solved_lot is not None)
        )

        # Counterfactual Logging (Arena Quant Audit Recommendation)
        # Store rejected setup so subsequent price movements evaluate filter efficiency
        if (not is_actionable) and (direction in ["BUY", "SELL"]) and (entry > 0):
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

    def _persist_alert(self, alert: Dict[str, Any]):
        """Saves alert to jsonl and updates realtime_trade_alerts.parquet atomically."""
        DB_DIR.mkdir(parents=True, exist_ok=True)
        try:
            with open(ALERTS_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert) + "\n")
        except Exception as e:
            logger.warning(f"Failed to append to {ALERTS_JSONL}: {e}")

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
                "reasons": ", ".join(alert["confluence_reasons"])
            }
            new_df = pd.DataFrame([flat_record])
            if ALERTS_PARQUET.exists():
                try:
                    old_df = pd.read_parquet(ALERTS_PARQUET)
                    combined = pd.concat([old_df, new_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["symbol", "sig_code", "candle_utc", "entry_price"], keep="last")
                except Exception:
                    combined = new_df
            else:
                combined = new_df

            tmp_file = ALERTS_PARQUET.with_suffix(".tmp")
            combined.to_parquet(tmp_file, index=False, engine="pyarrow")
            tmp_file.replace(ALERTS_PARQUET)
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

```

---
### File: `Terminal/deepmind_timesfm_forecaster.py`

```python
"""
========================================================================================
Google DeepMind TimesFM 2.5 (200M) Local Foundation Model Forecaster
========================================================================================
Provides zero-shot point and probabilistic quantile forecasting for crypto market microstructure:
- Cumulative Volume Delta (CVD) trajectory prediction
- Order-flow momentum and price exhaustion estimation
- Probabilistic downside (10th percentile) and upside (90th percentile) risk boundaries
========================================================================================
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional
import numpy as np

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [TIMESFM] %(message)s")
logger = logging.getLogger("TimesFMForecaster")

MODEL_DIR = os.path.join(ROOT_DIR, "models", "timesfm-2.5-200m-pytorch")

class DeepMindTimesFMForecaster:
    """
    Singleton wrapper for Google DeepMind TimesFM 2.5 (200M PyTorch).
    Performs zero-shot multi-step point and quantile time-series forecasting.
    """
    _instance = None
    _model = None
    _compiled = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DeepMindTimesFMForecaster, cls).__new__(cls)
        return cls._instance

    def __init__(self, model_dir: Optional[str] = None):
        if self._model is not None:
            return
            
        self.model_dir = os.path.abspath(model_dir or MODEL_DIR)
        self._load_model()

    def _load_model(self):
        safetensors_path = os.path.join(self.model_dir, "model.safetensors")
        if not os.path.exists(safetensors_path):
            logger.warning(f"Model weights not found at {safetensors_path}. DeepMind TimesFM forecaster disabled.")
            return

        try:
            import timesfm
            logger.info(f"Initializing Google DeepMind TimesFM 2.5 (200M) from {self.model_dir}...")
            self._model = timesfm.TimesFM_2p5_200M_torch(torch_compile=False)
            self._model.load_checkpoint(self.model_dir)

            # Compile with default horizon=12, max_context=128
            config = timesfm.ForecastConfig(
                max_context=128,
                max_horizon=24,
                normalize_inputs=True,
                per_core_batch_size=1
            )
            self._model.compile(config)
            self._compiled = True
            logger.info("DeepMind TimesFM 2.5 successfully compiled and ready for inference!")
        except Exception as e:
            logger.error(f"Failed to load DeepMind TimesFM model: {e}")
            self._model = None
            self._compiled = False

    @property
    def is_available(self) -> bool:
        return self._model is not None and self._compiled

    def forecast_trajectory(
        self,
        series: np.ndarray,
        horizon: int = 12
    ) -> Dict[str, Any]:
        """
        Generates point and quantile forecasts for a given 1D time series (CVD or price).
        
        Args:
            series: 1D array of historical values (e.g. 32 to 128 bars).
            horizon: Number of forward periods to predict (default: 12).
            
        Returns:
            Dict containing point forecast, quantiles (10, 50, 90), expected drift, and direction.
        """
        if not self.is_available:
            return {
                "available": False,
                "error": "Model not loaded or compiled",
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0
            }

        arr = np.asarray(series, dtype=np.float32).flatten()
        if len(arr) < 8:
            return {
                "available": False,
                "error": f"Series too short ({len(arr)} bars, need >= 8)",
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0
            }

        # Truncate to max 128 bars
        if len(arr) > 128:
            arr = arr[-128:]

        try:
            point_fc, quantile_fc = self._model.forecast(horizon=horizon, inputs=[arr])
            
            # point_fc shape: (1, horizon)
            # quantile_fc shape: (1, horizon, 10) -> quantiles: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, mean]
            pts = point_fc[0][:horizon]
            q10 = quantile_fc[0][:horizon, 0] # 10th percentile
            q50 = quantile_fc[0][:horizon, 4] # 50th percentile (median)
            q90 = quantile_fc[0][:horizon, 8] # 90th percentile

            last_val = arr[-1]
            end_val = pts[-1]
            drift = float(end_val - last_val)
            drift_pct = float(drift / (abs(last_val) + 1e-9))

            # Directional consensus
            if drift > 0 and q10[-1] > last_val * 0.995:
                direction = "STRONG_BULLISH"
            elif drift > 0:
                direction = "BULLISH"
            elif drift < 0 and q90[-1] < last_val * 1.005:
                direction = "STRONG_BEARISH"
            elif drift < 0:
                direction = "BEARISH"
            else:
                direction = "NEUTRAL"

            spread = float(np.mean(q90 - q10))
            confidence = float(np.clip(1.0 - (spread / (abs(drift) + spread + 1e-6)), 0.1, 0.99))
            # Epistemic uncertainty multiplier for position sizing (wider interval -> smaller multiplier)
            baseline_scale = float(np.std(arr) + 1e-6)
            rel_spread = spread / baseline_scale
            uncertainty_mult = float(np.clip(1.0 / (1.0 + 0.5 * rel_spread), 0.40, 1.0))

            return {
                "available": True,
                "horizon": horizon,
                "last_value": float(last_val),
                "predicted_end": float(end_val),
                "predicted_drift": drift,
                "predicted_drift_pct": drift_pct,
                "direction": direction,
                "confidence": round(confidence, 4),
                "uncertainty_multiplier": round(uncertainty_mult, 3),
                "point_forecast": [round(float(x), 4) for x in pts],
                "q10_downside": [round(float(x), 4) for x in q10],
                "q50_median": [round(float(x), 4) for x in q50],
                "q90_upside": [round(float(x), 4) for x in q90],
            }

        except Exception as e:
            logger.error(f"Inference error during forecast: {e}")
            return {
                "available": False,
                "error": str(e),
                "direction": "NEUTRAL",
                "predicted_drift": 0.0,
                "confidence": 0.0,
                "uncertainty_multiplier": 0.5
            }

    def forecast_increments(
        self,
        series: np.ndarray,
        horizon: int = 8
    ) -> Dict[str, Any]:
        """
        Stationary Increment Forecaster (Arena Recommendation).
        Transforms series to first differences: Δy_t = y_t - y_{t-1}.
        Forecasts future stationary increments and sums them for directional consensus.
        """
        arr = np.asarray(series, dtype=np.float32).flatten()
        if len(arr) < 10:
            return {
                "available": False,
                "error": f"Series too short for increments ({len(arr)} bars)",
                "direction": "NEUTRAL",
                "predicted_increment_sum": 0.0,
                "uncertainty_multiplier": 0.5
            }

        # First differences (stationary increments)
        diffs = np.diff(arr)
        res = self.forecast_trajectory(diffs, horizon=horizon)
        if not res.get("available"):
            return res

        # Sum of predicted increments gives net projected delta change
        pred_inc = sum(res.get("point_forecast", []))
        inc_dir = "BULLISH" if pred_inc > 0 else ("BEARISH" if pred_inc < 0 else "NEUTRAL")
        if abs(pred_inc) > np.std(diffs) * horizon * 0.5:
            inc_dir = f"STRONG_{inc_dir}"

        return {
            "available": True,
            "horizon": horizon,
            "predicted_increment_sum": round(float(pred_inc), 4),
            "increment_direction": inc_dir,
            "confidence": res.get("confidence", 0.5),
            "uncertainty_multiplier": res.get("uncertainty_multiplier", 1.0),
            "raw_trajectory": res
        }


if __name__ == "__main__":
    print("Testing DeepMind TimesFM Forecaster...")
    forecaster = DeepMindTimesFMForecaster()
    if forecaster.is_available:
        # Test synthetic CVD trend
        cvd_data = np.cumsum(np.random.normal(0.8, 0.3, size=48)).astype(np.float32)
        res = forecaster.forecast_trajectory(cvd_data, horizon=8)
        print("Forecast Output:")
        for k, v in res.items():
            if k in ["point_forecast", "q10_downside", "q50_median", "q90_upside"]:
                print(f"  {k}: {v[:4]}... (total {len(v)})")
            else:
                print(f"  {k}: {v}")
    else:
        print("DeepMind TimesFM Forecaster unavailable.")

```

---
### File: `Terminal/vector_memory.py`

```python
"""
Vector Memory & Semantic Knowledge Engine (Powered by LanceDB & FastEmbed)
=========================================================================
Persistent, embedded vector database for semantic retrieval of:
- Historical trade setups and order-flow market conditions.
- Macro/microstructure regimes and whale order book snapshots.
- Forensic lessons and post-trade reflections.

Enables Gemini Flash to recall historically analogous market setups in sub-millisecond
queries to verify mathematical expectancy before executing new trades.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

import lancedb
import pyarrow as pa
from fastembed import TextEmbedding

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
VECTOR_DIR = DB_DIR / "Vector_Memory"
VECTOR_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("VectorMemory")
logger.setLevel(logging.INFO)

# Embedding model: Fast, local 384-dimensional ONNX model
MODEL_NAME = "BAAI/bge-small-en-v1.5"


class VectorMemory:
    _instance = None
    _embedder = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(VectorMemory, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        
        self.db = lancedb.connect(VECTOR_DIR.as_posix())
        self._init_embedder()
        self._init_tables()
        self._initialized = True

    def _init_embedder(self):
        if VectorMemory._embedder is None:
            logger.info(f"Initializing FastEmbed model: {MODEL_NAME}")
            VectorMemory._embedder = TextEmbedding(model_name=MODEL_NAME)
        self.embedder = VectorMemory._embedder

    def _embed_text(self, text: str) -> List[float]:
        embeddings = list(self.embedder.embed([text]))
        return embeddings[0].tolist()

    def _init_tables(self):
        """Create or open tables for trade setups and forensic reflections."""
        schema_setups = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("ticket_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("strategy", pa.string()),
            pa.field("setup_text", pa.string()),
            pa.field("direction", pa.string()),
            pa.field("confluence_score", pa.float32()),
            pa.field("whale_imbalance", pa.float32()),
            pa.field("outcome", pa.string()),  # WIN, LOSS, BREAKEVEN, PENDING
            pa.field("pnl_usd", pa.float32()),
            pa.field("risk_usd", pa.float32()),
            pa.field("timestamp", pa.string())
        ])

        schema_counterfactual = pa.schema([
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("alert_id", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("direction", pa.string()),
            pa.field("rejection_reason", pa.string()),
            pa.field("setup_text", pa.string()),
            pa.field("entry_price", pa.float32()),
            pa.field("stop_loss", pa.float32()),
            pa.field("take_profit", pa.float32()),
            pa.field("confluence_score", pa.float32()),
            pa.field("counterfactual_outcome", pa.string()),  # UNEVALUATED, HYPOTHETICAL_WIN, HYPOTHETICAL_LOSS
            pa.field("hypothetical_pnl_usd", pa.float32()),
            pa.field("timestamp", pa.string())
        ])

        existing_tables = self.db.list_tables()
        table_names = [t if isinstance(t, str) else getattr(t, 'name', str(t)) for t in existing_tables]
        
        if "trade_setups" not in table_names:
            self.table_setups = self.db.create_table("trade_setups", schema=schema_setups)
            logger.info("Created 'trade_setups' LanceDB vector table.")
        else:
            self.table_setups = self.db.open_table("trade_setups")

        if "counterfactual_setups" not in table_names:
            self.table_counterfactual = self.db.create_table("counterfactual_setups", schema=schema_counterfactual)
            logger.info("Created 'counterfactual_setups' LanceDB vector table.")
        else:
            self.table_counterfactual = self.db.open_table("counterfactual_setups")

    def record_rejected_setup(
        self,
        alert_id: str,
        symbol: str,
        direction: str,
        rejection_reason: str,
        setup_text: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        confluence_score: float
    ):
        """
        Records a trade setup that was rejected or failed gates into LanceDB.
        Allows subsequent counterfactual replay: Did rejecting this setup save capital
        or forfeit a profitable move?
        """
        try:
            vec = self._embed_text(setup_text)
            now_iso = datetime.now(timezone.utc).isoformat()
            data = [{
                "vector": vec,
                "alert_id": str(alert_id),
                "symbol": str(symbol),
                "direction": str(direction),
                "rejection_reason": str(rejection_reason),
                "setup_text": str(setup_text),
                "entry_price": float(entry_price),
                "stop_loss": float(stop_loss),
                "take_profit": float(take_profit),
                "confluence_score": float(confluence_score),
                "counterfactual_outcome": "UNEVALUATED",
                "hypothetical_pnl_usd": 0.0,
                "timestamp": now_iso
            }]
            self.table_counterfactual.add(data)
            logger.info(f"Recorded rejected setup for {symbol} ({rejection_reason}) in LanceDB counterfactual memory.")
        except Exception as e:
            logger.error(f"Error recording rejected setup to LanceDB: {e}")

    def record_setup(
        self,
        ticket_id: str,
        symbol: str,
        strategy: str,
        setup_text: str,
        direction: str,
        confluence_score: float,
        whale_imbalance: float,
        outcome: str = "PENDING",
        pnl_usd: float = 0.0,
        risk_usd: float = 0.0
    ):
        """Vectorizes and indexes a trade setup for future semantic lookups."""
        try:
            vec = self._embed_text(setup_text)
            now_iso = datetime.now(timezone.utc).isoformat()
            data = [{
                "vector": vec,
                "ticket_id": str(ticket_id),
                "symbol": symbol,
                "strategy": strategy,
                "setup_text": setup_text,
                "direction": direction,
                "confluence_score": float(confluence_score),
                "whale_imbalance": float(whale_imbalance),
                "outcome": outcome,
                "pnl_usd": float(pnl_usd),
                "risk_usd": float(risk_usd),
                "timestamp": now_iso
            }]
            self.table_setups.add(data)
            logger.info(f"Indexed trade setup for {symbol} (#{ticket_id}) in LanceDB.")
        except Exception as e:
            logger.error(f"Error indexing setup to LanceDB: {e}")

    def update_setup_outcome(self, ticket_id: str, outcome: str, pnl_usd: float):
        """Updates outcome and PnL of a previously recorded setup."""
        try:
            # Recreate table with updated record if found
            df = self.table_setups.to_pandas()
            if df.empty or ticket_id not in df["ticket_id"].values:
                return
            
            df.loc[df["ticket_id"] == ticket_id, "outcome"] = outcome
            df.loc[df["ticket_id"] == ticket_id, "pnl_usd"] = float(pnl_usd)
            
            # Overwrite table
            self.table_setups = self.db.create_table("trade_setups", df, mode="overwrite")
            logger.info(f"Updated LanceDB outcome for Ticket #{ticket_id}: {outcome} (${pnl_usd:.2f})")
        except Exception as e:
            logger.error(f"Error updating LanceDB setup outcome: {e}")

    def query_counterfactual_expectancy(self, top_k: int = 50) -> Dict[str, Any]:
        """
        Analyzes the opportunity cost of past rejected setups:
        How many rejected trades would have hit TP vs SL?
        """
        try:
            if len(self.table_counterfactual) == 0:
                return {"count": 0, "status": "NO_REJECTED_SETUPS"}
            
            df = self.table_counterfactual.to_pandas()
            total = len(df)
            hypo_wins = len(df[df["counterfactual_outcome"] == "HYPOTHETICAL_WIN"])
            hypo_losses = len(df[df["counterfactual_outcome"] == "HYPOTHETICAL_LOSS"])
            return {
                "total_rejected": total,
                "hypothetical_wins": hypo_wins,
                "hypothetical_losses": hypo_losses,
                "filter_efficiency": round(hypo_losses / (hypo_wins + hypo_losses + 1e-6), 3) if (hypo_wins + hypo_losses) > 0 else 1.0
            }
        except Exception as e:
            return {"error": str(e)}

    def query_similar_setups(self, query_text: str, top_k: int = 5) -> Dict[str, Any]:
        """
        Retrieves top_k most semantically and microstructurally similar setups.
        Calculates empirical win-rate and expectancy of similar historical setups.
        """
        try:
            if len(self.table_setups) == 0:
                return {
                    "count": 0,
                    "win_rate": 0.5,
                    "avg_pnl": 0.0,
                    "similar_setups": [],
                    "verdict": "NO_HISTORICAL_DATA"
                }

            query_vec = self._embed_text(query_text)
            results = self.table_setups.search(query_vec).limit(top_k).to_pandas()
            
            if results.empty:
                return {"count": 0, "win_rate": 0.5, "avg_pnl": 0.0, "similar_setups": [], "verdict": "NO_MATCH"}

            completed = results[results["outcome"].isin(["WIN", "LOSS", "BREAKEVEN"])]
            
            if completed.empty:
                return {
                    "count": len(results),
                    "completed_count": 0,
                    "win_rate": 0.5,
                    "avg_pnl": 0.0,
                    "similar_setups": results[["symbol", "strategy", "outcome", "pnl_usd", "_distance"]].to_dict(orient="records"),
                    "verdict": "INSUFFICIENT_COMPLETED_HISTORY"
                }

            wins = len(completed[completed["outcome"] == "WIN"])
            losses = len(completed[completed["outcome"] == "LOSS"])
            total_resolved = wins + losses
            win_rate = round(wins / total_resolved, 3) if total_resolved > 0 else 0.5
            avg_pnl = round(float(completed["pnl_usd"].mean()), 2)

            verdict = "FAVORABLE" if win_rate >= 0.55 else ("UNFAVORABLE" if win_rate < 0.40 else "NEUTRAL")

            return {
                "count": len(results),
                "resolved_count": total_resolved,
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "verdict": verdict,
                "similar_setups": results[["ticket_id", "symbol", "strategy", "outcome", "pnl_usd", "_distance"]].to_dict(orient="records")
            }
        except Exception as e:
            logger.error(f"Error querying similar setups from LanceDB: {e}")
            return {"error": str(e), "win_rate": 0.5, "verdict": "ERROR"}


if __name__ == "__main__":
    vm = VectorMemory()
    print("Testing VectorMemory indexing and semantic retrieval...")
    
    # Test index sample past trade
    sample_text = "BTC absorption buy with $12M whale bid wall support at 76,800 and 70% near-field short liquidation imbalance."
    vm.record_setup(
        ticket_id="TEST_001",
        symbol="BTC",
        strategy="Whale Bid Wall Support Fade",
        setup_text=sample_text,
        direction="BUY",
        confluence_score=85.0,
        whale_imbalance=0.72,
        outcome="WIN",
        pnl_usd=42.50,
        risk_usd=30.00
    )
    
    # Query similar
    query = "Bitcoin strong absorption at key support with massive whale bid orders and short liquidations."
    sim = vm.query_similar_setups(query, top_k=3)
    print("Query Results:")
    print(sim)

```

---
### File: `Terminal/collector_lock.py`

```python
"""
========================================================================================
Shared Collector Coordination & Network Infrastructure
========================================================================================
Guarantees:
1. Zero Overlap: Shared browser mutex ensures only ONE collector interacts with Chrome CDP.
2. Cloudflare WARP IP Rotation: Executed both BEFORE and AFTER each data dump cycle.
3. Clean Tab Hygiene: Automatic recovery of stale locks and orphan tabs.
========================================================================================
"""

import os
import sys
import time
import json
import logging
import urllib.request
import subprocess
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LOCK_DIR = DB_DIR / ".locks"
LOCK_DIR.mkdir(parents=True, exist_ok=True)
BROWSER_LOCK_FILE = LOCK_DIR / "cdp_browser.lock"
WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")

logger = logging.getLogger("CollectorCoordination")


def get_current_ip() -> str:
    """Queries external IP via ipify."""
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as response:
            return json.loads(response.read().decode()).get("ip", "unknown")
    except Exception:
        return "unknown"


def rotate_warp_ip(context: str = "Collector") -> str:
    """
    Disconnects and reconnects Cloudflare WARP to rotate IP and re-key the tunnel.
    Called both BEFORE and AFTER every data collection sweep.
    """
    if not WARP_CLI.exists():
        logger.warning(f"[{context}] WARP CLI binary not found at: {WARP_CLI}")
        return "unknown"

    old_ip = get_current_ip()
    logger.info(f"[{context}] [Cloudflare WARP] Current IP: {old_ip}. Rotating tunnel...")
    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(1.5)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1.0)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(3.5)
        new_ip = get_current_ip()
        logger.info(f"[{context}] [Cloudflare WARP] Rotation complete. New IP: {new_ip} (Old: {old_ip})")
        return new_ip
    except Exception as e:
        logger.error(f"[{context}] [Cloudflare WARP] Error rotating IP: {e}")
        return old_ip


class BrowserLock:
    """
    Context manager ensuring strictly non-overlapping access to Chrome CDP.
    Prevents any two collectors from opening tabs or sending WebSocket frames simultaneously.
    """
    def __init__(self, collector_name: str, timeout_secs: int = 120):
        self.collector_name = collector_name
        self.timeout_secs = timeout_secs
        self.acquired = False

    def __enter__(self):
        start = time.time()
        while time.time() - start < self.timeout_secs:
            if not BROWSER_LOCK_FILE.exists():
                try:
                    BROWSER_LOCK_FILE.write_text(f"{self.collector_name}:{os.getpid()}:{time.time()}")
                    self.acquired = True
                    logger.info(f"[{self.collector_name}] Acquired shared browser lock.")
                    return self
                except Exception:
                    pass
            else:
                # Check for stale lock (> 180 seconds old)
                try:
                    content = BROWSER_LOCK_FILE.read_text().strip()
                    parts = content.split(":")
                    if len(parts) >= 3 and time.time() - float(parts[2]) > 180:
                        logger.warning(f"[{self.collector_name}] Clearing stale browser lock held by {parts[0]}")
                        BROWSER_LOCK_FILE.unlink(missing_ok=True)
                        continue
                except Exception:
                    pass
            time.sleep(1.0)
        logger.warning(f"[{self.collector_name}] Timed out waiting for browser lock after {self.timeout_secs}s.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            try:
                BROWSER_LOCK_FILE.unlink(missing_ok=True)
                logger.info(f"[{self.collector_name}] Released shared browser lock.")
            except Exception:
                pass


# Reserved minutes in the hour for non-overlapping execution
WHALE_MINUTES = [0, 15, 35, 50]
LIQUIDATION_MINUTES = [10, 25, 55]


def is_scheduled_for_other_collector(current_minute: int, collector_type: str) -> bool:
    """
    Checks if the current minute is exclusively reserved for another collector.
    Used by Footprint to yield and guarantee zero overlap.
    """
    if collector_type == "footprint":
        return (current_minute in WHALE_MINUTES) or (current_minute in LIQUIDATION_MINUTES)
    elif collector_type == "whale":
        return current_minute in LIQUIDATION_MINUTES
    elif collector_type == "liquidation":
        return current_minute in WHALE_MINUTES
    return False

```

---
### File: `Terminal/crypto_live_trader.py`

```python
"""
CoinGlass & Whale Orders Autonomous Crypto Live Trading Engine
==============================================================
Fully autonomous live trading engine executing on Blueberry Markets MT5.
Synthesizes four institutional data streams:
1. CoinGlass Footprint Order Flow Signals (Absorption, Trapped Traders, Divergence).
2. Whale Orders Blotter & Limit Walls (Table 2 institutional resting support/resistance).
3. Liquidation Map & Magnet Zones (Stacked forced exit cascade pools).
4. Live MT5 Quotes, Spreads, and Order Management.

Safety & Risk Constraints:
- Strict Spread Gatekeeper: Max spread <= 0.45% (prevents slippage loss).
- Fixed Account Risk: 1.0% equity risk per trade (~$47 on $4,714 balance).
- Exact Lot Sizing: Dynamic calculation using broker tick value, tick size, and contract size.
- Hard Stops & Targets: Every order placed with hard SL and TP.
- Risk-Free Break-Even Lock: Automatically trails SL to break-even once trade hits +1R profit.
- Max Concurrent Crypto Positions: 2 active trades max to avoid overexposure.
"""

import os
import sys
import time
import math
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import numpy as np
import MetaTrader5 as mt5

from Terminal.persistent_memory import (
    get_rules, get_strategy_weight, record_trade_reflection, sync_to_markdown
)
from Terminal.olap_engine import OlapEngine
from Terminal.vector_memory import VectorMemory
from Terminal.regime_detector import RegimeDetector
from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster
from Terminal.risk_envelope import (
    admissible_volume, compute_dynamic_capital_budget, validate_trade_geometry,
    HARD_FLOOR_USD, OPERATING_BUFFER_MIN, MAX_RISK_USD, MIN_RISK_USD,
    MAX_OPEN_POSITIONS, MAX_ALLOWED_SPREAD_PCT, MIN_RR_RATIO
)

# Reconfigure stdout for UTF-8 and non-buffered logging
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# ---------------------------------------------------------------------------
# Paths and Configuration
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
LIQ_DIR = DB_DIR / "Liquidation_Data"
WHALE_DIR = DB_DIR / "Whale_Orders"

SIGNALS_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
WHALE_SNAPSHOTS_PARQUET = WHALE_DIR / "whale_market_snapshots.parquet"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

EXECUTION_LOG_JSONL = DB_DIR / "crypto_trade_execution_log.jsonl"
ACTIVE_POSITIONS_PARQUET = DB_DIR / "active_crypto_positions.parquet"

# Magic number identifying trades placed by this autonomous engine
MAGIC_NUMBER = 888123
MAX_CONCURRENT_POSITIONS = MAX_OPEN_POSITIONS
HARD_MAX_LOSS_USD = MAX_RISK_USD
PROP_FIRM_FLOOR_EQUITY = HARD_FLOOR_USD
PROP_FIRM_HALT_BUFFER = OPERATING_BUFFER_MIN
RISK_PERCENT = 0.01                 # 1.0% account risk baseline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LIVE-TRADER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LiveTrader")


def load_crypto_registry() -> Dict[str, dict]:
    """Load metadata and MT5 symbol mappings for crypto assets."""
    if not REGISTRY_FILE.exists():
        return {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    return reg.get("Cryptos", {})


def calculate_lot_size(
    info: mt5.SymbolInfo,
    risk_dollars: float,
    entry_price: float,
    sl_price: float
) -> Tuple[float, float]:
    """
    Exact mathematical volume solver based on Terminal.risk_envelope.admissible_volume.
    Guarantees realized dollar loss lies strictly inside [MIN_RISK_USD, cap_usd].
    """
    price_dist = abs(entry_price - sl_price)
    if price_dist <= 0 or info.point <= 0:
        return 0.0, 0.0

    points_dist = price_dist / info.point
    tick_val = info.trade_tick_value
    tick_size = info.trade_tick_size
    point = info.point

    cost_per_point_per_lot = tick_val * (point / tick_size)
    loss_per_lot = points_dist * cost_per_point_per_lot

    step = info.volume_step if info.volume_step > 0 else 0.01
    cap_usd = min(risk_dollars, MAX_RISK_USD)

    lots, status, actual_loss = admissible_volume(
        loss_per_lot=loss_per_lot,
        step=step,
        min_lot=info.volume_min,
        max_lot=info.volume_max,
        floor_usd=MIN_RISK_USD,
        cap_usd=cap_usd
    )
    if lots is None or lots <= 0:
        logger.warning(f"Admissible volume solver rejected sizing: {status}")
        return 0.0, 0.0

    return lots, actual_loss


class CryptoLiveTrader:
    def __init__(self):
        self.crypto_map = load_crypto_registry()
        self.magic = MAGIC_NUMBER
        self.active_signals_processed = set()
        self.olap = OlapEngine()
        self.vector_mem = VectorMemory()
        self.regime_det = RegimeDetector()
        self.timesfm = DeepMindTimesFMForecaster()
        self.known_open_tickets = set()

    def connect_mt5(self) -> bool:
        """Initialize connection to MetaTrader 5."""
        if not mt5.initialize():
            logger.error(f"Failed to connect to MT5: {mt5.last_error()}")
            return False
        acct = mt5.account_info()
        if not acct:
            logger.error("Connected to MT5 but account_info is None.")
            return False
        logger.info(f"Connected to MT5 Account: {acct.login} ({acct.name}) | Equity: ${acct.equity:,.2f}")
        return True

    def get_open_crypto_positions(self) -> List[dict]:
        """Fetch all currently open crypto positions placed by this engine."""
        positions = mt5.positions_get()
        if not positions:
            return []
        crypto_mt5_symbols = {v["mt5"] for v in self.crypto_map.values()}
        bot_positions = []
        for p in positions:
            if p.symbol in crypto_mt5_symbols and p.magic == self.magic:
                bot_positions.append({
                    "ticket": p.ticket,
                    "symbol": p.symbol,
                    "type": "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL",
                    "volume": p.volume,
                    "price_open": p.price_open,
                    "price_current": p.price_current,
                    "sl": p.sl,
                    "tp": p.tp,
                    "profit": p.profit,
                    "comment": p.comment,
                    "time": datetime.fromtimestamp(p.time, timezone.utc).isoformat()
                })
        return bot_positions

    def manage_open_positions(self):
        """
        Manages active positions:
        - HARD CIRCUIT BREAKER: Closes position immediately if loss approaches -$50.00 hard limit.
        - If profit reaches +1R (halfway to TP1 or equal to initial risk distance),
          moves SL to break-even (entry + small buffer) to guarantee zero loss.
        """
        open_pos = self.get_open_crypto_positions()
        if not open_pos:
            return

        for pos in open_pos:
            mt5_sym = pos["symbol"]
            info = mt5.symbol_info(mt5_sym)
            tick = mt5.symbol_info_tick(mt5_sym)
            if not info or not tick:
                continue

            # 1. HARD LOSS CIRCUIT BREAKER ($50 limit)
            if pos["profit"] <= -48.00:
                logger.warning(f"[{mt5_sym}] [CIRCUIT-BREAKER] Position #{pos['ticket']} hit hard loss threshold (${pos['profit']:.2f} <= -$48.00). Executing emergency market close!")
                close_type = mt5.ORDER_TYPE_SELL if pos["type"] == "BUY" else mt5.ORDER_TYPE_BUY
                close_price = tick.bid if pos["type"] == "BUY" else tick.ask
                close_req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "position": pos["ticket"],
                    "symbol": mt5_sym,
                    "volume": pos["volume"],
                    "type": close_type,
                    "price": close_price,
                    "deviation": 30,
                    "magic": self.magic,
                    "comment": "CG_HARD_50USD_STOP",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC
                }
                close_res = mt5.order_send(close_req)
                if close_res and close_res.retcode == mt5.TRADE_RETCODE_DONE:
                    logger.info(f"[{mt5_sym}] Emergency market close completed for Ticket #{pos['ticket']}!")
                    try:
                        record_trade_reflection(
                            ticket_id=str(pos["ticket"]),
                            symbol=pos["symbol"],
                            direction=pos["type"],
                            strategy="Emergency Circuit Breaker Triggered",
                            outcome="LOSS",
                            pnl_usd=pos["profit"],
                            risk_usd=50.00,
                            root_cause="Adverse market momentum exceeded -$48.00 threshold",
                            lesson_learned="Avoid fading high-volatility momentum without confirmed multi-timeframe CVD divergence."
                        )
                        self.vector_mem.update_setup_outcome(str(pos["ticket"]), "LOSS", pos["profit"])
                        sync_to_markdown()
                    except Exception as ex:
                        logger.warning(f"Failed to record reflection: {ex}")
                else:
                    logger.error(f"[{mt5_sym}] Emergency market close failed: {close_res.comment if close_res else 'Unknown'}")
                continue

            point = info.point
            digits = info.digits
            entry = pos["price_open"]
            current = pos["price_current"]
            current_sl = pos["sl"]
            pos_type = pos["type"]

            # Initial risk distance
            if current_sl and current_sl > 0:
                init_risk = abs(entry - current_sl)
            else:
                init_risk = entry * 0.005  # fallback 0.5%

            # Check if +1R reached and SL not yet at/beyond break-even
            if pos_type == "BUY":
                gain = current - entry
                be_price = round(entry + 2 * point, digits)
                if gain >= init_risk and current_sl < entry:
                    logger.info(f"[{mt5_sym}] Trade reached +1R gain (+${pos['profit']:.2f}). Moving SL to Break-Even ({be_price})!")
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": pos["ticket"],
                        "symbol": mt5_sym,
                        "sl": be_price,
                        "tp": pos["tp"]
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"[{mt5_sym}] SL successfully moved to Break-Even!")
                    else:
                        logger.warning(f"[{mt5_sym}] Failed to update SL to BE: {res.comment if res else 'Error'}")

            elif pos_type == "SELL":
                gain = entry - current
                be_price = round(entry - 2 * point, digits)
                if gain >= init_risk and (current_sl > entry or current_sl == 0):
                    logger.info(f"[{mt5_sym}] Trade reached +1R gain (+${pos['profit']:.2f}). Moving SL to Break-Even ({be_price})!")
                    req = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": pos["ticket"],
                        "symbol": mt5_sym,
                        "sl": be_price,
                        "tp": pos["tp"]
                    }
                    res = mt5.order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"[{mt5_sym}] SL successfully moved to Break-Even!")
                    else:
                        logger.warning(f"[{mt5_sym}] Failed to update SL to BE: {res.comment if res else 'Error'}")

    def evaluate_asset_confluence(self, symbol: str) -> Optional[dict]:
        """
        Synthesizes Footprint Signals, Whale Orders, Liquidation Magnets, and MT5 live quotes
        to produce a high-confluence trade ticket.
        """
        meta = self.crypto_map.get(symbol)
        if not meta:
            return None

        mt5_sym = meta["mt5"]

        # 1. MT5 Quote & Spread Check
        mt5.symbol_select(mt5_sym, True)
        tick = mt5.symbol_info_tick(mt5_sym)
        info = mt5.symbol_info(mt5_sym)

        if not tick or not info or tick.bid <= 0 or tick.ask <= 0:
            return None

        mid = (tick.bid + tick.ask) / 2.0
        spread_val = tick.ask - tick.bid
        spread_pct = (spread_val / mid) * 100.0

        # Reject if spread exceeds gatekeeper threshold
        if spread_pct > MAX_ALLOWED_SPREAD_PCT:
            return None

        # 2. Check CoinGlass Footprint Signals
        if not SIGNALS_PARQUET.exists():
            return None

        sig_df = pd.read_parquet(SIGNALS_PARQUET)
        sym_sigs = sig_df[sig_df["symbol"] == symbol]
        if sym_sigs.empty:
            return None

        latest_sig = sym_sigs.iloc[-1].to_dict()
        sig_name = latest_sig.get("sig_name", "")
        sig_side = str(latest_sig.get("side", "")).lower()
        sig_time = latest_sig.get("datetime_utc", "")

        # Signal de-duplication key
        sig_key = f"{symbol}_{sig_name}_{sig_time}_{latest_sig.get('price')}"
        if sig_key in self.active_signals_processed:
            return None

        # 3. Pull High-Speed OLAP Confluence Analytics (DuckDB)
        whale_data = self.olap.query_whale_walls(symbol)
        liq_data = self.olap.query_liquidation_confluence(symbol)
        cvd_data = self.olap.query_cvd_trend(symbol)
        regime_data = self.regime_det.analyze_symbol(symbol)

        whale_imbalance = whale_data.get("whale_imbalance_bid_ratio", 0.5)
        top_bid_wall_usd = whale_data.get("tight_bid_walls_usd", 0.0)
        top_ask_wall_usd = whale_data.get("tight_ask_walls_usd", 0.0)
        near_field_shorts = liq_data.get("near_field_shorts_pct", 50.0)
        cvd_slope = cvd_data.get("cvd_slope", "NEUTRAL")
        poc_price = cvd_data.get("recent_poc") or mid
        current_regime = regime_data.get("regime", "UNKNOWN")

        # -------------------------------------------------------------
        # Confluence Formulation
        # -------------------------------------------------------------
        confluence_score = 50
        reasons = []
        direction = None
        strategy = ""

        # SCENARIO A: BULLISH CONFLUENCE (Absorption / Trapped Shorts / Bull Div)
        if ("absorption" in sig_name.lower() and sig_side in ["buy", "long"]) or \
           ("trapped shorts" in sig_name.lower()) or \
           ("bull div" in sig_name.lower()) or \
           ("divergence" in sig_name.lower() and sig_side in ["buy", "bullish"]):

            direction = "BUY"
            strategy = "Whale Bid Wall Support Fade"
            entry_price = tick.ask
            reasons.append(f"Footprint {sig_name} ({sig_side})")

            # Check Whale Bid Wall Support
            if top_bid_wall_usd >= 500_000:
                confluence_score += 15
                reasons.append(f"Whale Bid Wall (${top_bid_wall_usd/1e6:.1f}M, {whale_imbalance*100:.0f}% bid depth)")
                sl_price = entry_price * 0.995
            else:
                sl_price = entry_price * 0.995

            # CVD Alignment
            if cvd_slope in ["BULLISH_DIVERGENCE", "BULLISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")

            # Near Field Short Liquidation Fuel
            if near_field_shorts >= 55.0:
                confluence_score += 10
                reasons.append(f"Short Squeeze Fuel ({near_field_shorts:.0f}% shorts)")

            # Target POC or next resistance
            tp1_price = poc_price if poc_price > entry_price * 1.008 else entry_price * 1.015
            tp2_price = entry_price * 1.025

        # SCENARIO B: BEARISH CONFLUENCE (Absorption / Trapped Longs / Bear Div / Exhaustion)
        elif ("absorption" in sig_name.lower() and sig_side in ["sell", "short"]) or \
             ("trapped longs" in sig_name.lower()) or \
             ("bear div" in sig_name.lower()) or \
             ("exhaustion" in sig_name.lower() and sig_side in ["buy", "long"]) or \
             ("divergence" in sig_name.lower() and sig_side in ["sell", "bearish"]):

            direction = "SELL"
            strategy = "Whale Ask Wall Resistance Fade"
            entry_price = tick.bid
            reasons.append(f"Footprint {sig_name} ({sig_side})")

            # Check Whale Ask Wall Resistance
            if top_ask_wall_usd >= 500_000:
                confluence_score += 15
                reasons.append(f"Whale Ask Wall (${top_ask_wall_usd/1e6:.1f}M, {(1-whale_imbalance)*100:.0f}% ask depth)")
                sl_price = entry_price * 1.005
            else:
                sl_price = entry_price * 1.005

            # CVD Alignment
            if cvd_slope in ["BEARISH_DIVERGENCE", "BEARISH_FLOW"]:
                confluence_score += 15
                reasons.append(f"CVD {cvd_slope}")

            # Near Field Long Liquidation Fuel
            if near_field_shorts <= 45.0:
                confluence_score += 10
                reasons.append(f"Long Liquidation Cascade Fuel ({100-near_field_shorts:.0f}% longs)")

            # Target POC or next support
            tp1_price = poc_price if poc_price < entry_price * 0.992 else entry_price * 0.985
            tp2_price = entry_price * 0.975

        else:
            return None

        # 4. Check Semantic Vector Memory for Historical Analogs
        setup_desc = f"{symbol} {direction} {strategy} with Whale Imbalance {whale_imbalance:.1%}, CVD {cvd_slope}, Regime {current_regime}"
        past_analogs = self.vector_mem.query_similar_setups(setup_desc, top_k=5)
        if past_analogs.get("verdict") == "UNFAVORABLE":
            logger.warning(f"[{symbol}] Vetoed by Vector Memory: historical win rate is only {past_analogs.get('win_rate'):.1%} < 40%!")
            return None
        elif past_analogs.get("verdict") == "FAVORABLE":
            confluence_score += 10
            reasons.append(f"Vector Memory Historical Win Rate {past_analogs.get('win_rate')*100:.0f}%")

        # 5. Google DeepMind TimesFM 2.5 Zero-Shot CVD Forecaster
        cvd_series = self.olap.get_series(symbol, "cumulative_delta", limit=64)
        if len(cvd_series) >= 16 and self.timesfm.is_available:
            tfm_res = self.timesfm.forecast_trajectory(cvd_series, horizon=8)
            if tfm_res.get("available"):
                tfm_dir = tfm_res.get("direction", "NEUTRAL")
                tfm_conf = tfm_res.get("confidence", 0.0)
                if direction == "BUY":
                    if "BULLISH" in tfm_dir:
                        confluence_score += 15
                        reasons.append(f"DeepMind TimesFM {tfm_dir} (conf {tfm_conf:.0%})")
                    elif "STRONG_BEARISH" in tfm_dir and tfm_conf > 0.6:
                        logger.warning(f"[{symbol}] Vetoed by DeepMind TimesFM: Forecast predicts strong adverse bearish CVD drift!")
                        return None
                elif direction == "SELL":
                    if "BEARISH" in tfm_dir:
                        confluence_score += 15
                        reasons.append(f"DeepMind TimesFM {tfm_dir} (conf {tfm_conf:.0%})")
                    elif "STRONG_BULLISH" in tfm_dir and tfm_conf > 0.6:
                        logger.warning(f"[{symbol}] Vetoed by DeepMind TimesFM: Forecast predicts strong adverse bullish CVD drift!")
                        return None

        # Check Risk to Reward Ratio
        risk_dist = abs(entry_price - sl_price)
        reward_dist = abs(tp1_price - entry_price)
        rr_ratio = round(reward_dist / risk_dist, 2) if risk_dist > 0 else 0.0

        if rr_ratio < MIN_RR_RATIO:
            # Adjust TP1 to meet minimum R:R
            if direction == "BUY":
                tp1_price = entry_price + risk_dist * MIN_RR_RATIO
            else:
                tp1_price = entry_price - risk_dist * MIN_RR_RATIO
            reward_dist = abs(tp1_price - entry_price)
            rr_ratio = round(reward_dist / risk_dist, 2)

        # 5. Canonical Risk Envelope & Prop-Firm Floor Defense
        acct = mt5.account_info()
        balance = acct.balance if acct else 4714.57
        equity = acct.equity if acct else balance

        floor_budget_risk, ok, budget_msg = compute_dynamic_capital_budget(equity)
        if not ok:
            logger.warning(f"[{symbol}] TRADING HALTED: {budget_msg}")
            return None

        valid_geo, geo_msg = validate_trade_geometry(direction, entry_price, sl_price, tp1_price, spread_pct)
        if not valid_geo:
            logger.warning(f"[{symbol}] Trade geometry rejected: {geo_msg}")
            return None

        # Dynamic Strategy & Regime Multipliers
        strat_weight = get_strategy_weight(strategy)
        regime_mult = regime_data.get("strategy_multipliers", {}).get(strategy, 1.0)
        final_target_risk = floor_budget_risk * strat_weight * regime_mult
        final_target_risk = min(HARD_MAX_LOSS_USD, max(5.0, final_target_risk))

        final_lots, actual_risk = calculate_lot_size(info, final_target_risk, entry_price, sl_price)
        if final_lots <= 0:
            return None

        digits = info.digits

        return {
            "symbol": symbol,
            "mt5_symbol": mt5_sym,
            "direction": direction,
            "strategy": strategy,
            "confluence_score": confluence_score,
            "reasons": " | ".join(reasons),
            "signal_key": sig_key,
            "entry_price": round(entry_price, digits),
            "stop_loss": round(sl_price, digits),
            "take_profit_1": round(tp1_price, digits),
            "take_profit_2": round(tp2_price, digits),
            "lots": final_lots,
            "risk_usd": actual_risk,
            "risk_reward_ratio": rr_ratio,
            "spread_pct": round(spread_pct, 4),
            "filling_mode": mt5.ORDER_FILLING_IOC
        }

    def validate_and_adjust_stops_for_ticket(self, ticket: dict, info: mt5.SymbolInfo) -> bool:
        """
        Validates stops against broker stop levels. If retcode is 10016 (Invalid stops),
        adaptively expands SL and TP until compliant, then recalculates lot size to maintain
        strict 1.0% risk.
        """
        mt5_sym = ticket["mt5_symbol"]
        order_type = mt5.ORDER_TYPE_BUY if ticket["direction"] == "BUY" else mt5.ORDER_TYPE_SELL
        digits = info.digits
        point = info.point

        sl = ticket["stop_loss"]
        tp = ticket["take_profit_1"]
        lots = ticket["lots"]

        for attempt in range(15):
            req = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": mt5_sym,
                "volume": lots,
                "type": order_type,
                "price": ticket["entry_price"],
                "sl": sl,
                "tp": tp,
                "deviation": 20,
                "magic": self.magic,
                "comment": f"CG_{ticket['direction']}_{ticket['symbol']}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": ticket["filling_mode"]
            }
            check = mt5.order_check(req)
            if check.retcode == 0:
                if ticket.get("risk_usd", 0) > HARD_MAX_LOSS_USD:
                    logger.warning(f"[{mt5_sym}] Ticket risk exceeds $50.00 cap (${ticket.get('risk_usd'):.2f}). Order rejected.")
                    return False
                ticket["stop_loss"] = sl
                ticket["take_profit_1"] = tp
                ticket["lots"] = lots
                return True
            elif check.retcode == 10016:  # Invalid stops
                # Expand SL and TP by 10 points further away
                step_points = max(10, info.trade_stops_level) * point
                if order_type == mt5.ORDER_TYPE_BUY:
                    sl = round(sl - step_points, digits)
                    tp = round(tp + step_points * 2, digits)
                else:
                    sl = round(sl + step_points, digits)
                    tp = round(tp - step_points * 2, digits)

                # Recompute lot size to maintain strict 1.0% dollar risk
                acct = mt5.account_info()
                balance = acct.balance if acct else 4714.57
                risk_usd = balance * RISK_PERCENT
                new_lots, actual_risk = calculate_lot_size(info, risk_usd, ticket["entry_price"], sl)
                if new_lots <= 0:
                    return False
                lots = new_lots
                ticket["risk_usd"] = actual_risk
            else:
                logger.warning(f"[{mt5_sym}] Order Check rejected ({check.retcode}): {check.comment}")
                return False

        return False

    def execute_trade(self, ticket: dict) -> bool:
        """Executes a formulated trade directly on MT5."""
        mt5_sym = ticket["mt5_symbol"]
        info = mt5.symbol_info(mt5_sym)
        if not info:
            logger.error(f"Cannot get symbol info for {mt5_sym}")
            return False

        # Validate and adaptively adjust stops to broker requirements
        if not self.validate_and_adjust_stops_for_ticket(ticket, info):
            logger.error(f"[{mt5_sym}] Could not find broker-compliant stops. Aborting.")
            return False

        order_type = mt5.ORDER_TYPE_BUY if ticket["direction"] == "BUY" else mt5.ORDER_TYPE_SELL

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mt5_sym,
            "volume": ticket["lots"],
            "type": order_type,
            "price": ticket["entry_price"],
            "sl": ticket["stop_loss"],
            "tp": ticket["take_profit_1"],
            "deviation": 20,
            "magic": self.magic,
            "comment": f"CG_{ticket['direction']}_{ticket['symbol']}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": ticket["filling_mode"]
        }

        logger.info(f"Submitting MT5 Order: {ticket['direction']} {ticket['lots']} {mt5_sym} @ {ticket['entry_price']} | SL: {ticket['stop_loss']} | TP: {ticket['take_profit_1']} | Risk: ${ticket['risk_usd']:.2f}")

        res = mt5.order_send(req)
        if not res or res.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"MT5 Order Send failed ({res.retcode if res else 'None'}): {res.comment if res else 'Unknown'}")
            return False

        deal_ticket = res.order
        logger.info(f"TRADE EXECUTED SUCCESSFULLY! Order Ticket #{deal_ticket} | Volume: {res.volume} @ {res.price}")

        # Mark signal processed
        self.active_signals_processed.add(ticket["signal_key"])

        # Persist audit record
        audit_record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "ticket": deal_ticket,
            "symbol": ticket["symbol"],
            "mt5_symbol": mt5_sym,
            "direction": ticket["direction"],
            "strategy": ticket["strategy"],
            "volume": res.volume,
            "entry_price": res.price,
            "sl": ticket["stop_loss"],
            "tp": ticket["take_profit_1"],
            "risk_usd": ticket["risk_usd"],
            "risk_reward_ratio": ticket["risk_reward_ratio"],
            "spread_pct": ticket["spread_pct"],
            "confluence": ticket["reasons"]
        }

        with open(EXECUTION_LOG_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_record) + "\n")

        # Record setup into Vector Memory for pattern retrieval
        try:
            self.vector_mem.record_setup(
                ticket_id=str(deal_ticket),
                symbol=ticket["symbol"],
                strategy=ticket["strategy"],
                setup_text=ticket["reasons"],
                direction=ticket["direction"],
                confluence_score=ticket["confluence_score"],
                whale_imbalance=ticket.get("whale_imbalance", 0.5),
                outcome="PENDING",
                pnl_usd=0.0,
                risk_usd=ticket["risk_usd"]
            )
        except Exception as e:
            logger.warning(f"Could not index setup into VectorMemory: {e}")

        return True

    def scan_and_trade_cycle(self):
        """Performs one full evaluation cycle across all crypto assets."""
        if not self.connect_mt5():
            return

        try:
            # Step 1: Manage open positions (Trailing & Break-Even)
            self.manage_open_positions()

            # Step 2: Check current open crypto positions count
            open_pos = self.get_open_crypto_positions()
            current_count = len(open_pos)
            open_symbols = {p["symbol"] for p in open_pos}

            if current_count >= MAX_CONCURRENT_POSITIONS:
                logger.info(f"Max concurrent crypto positions reached ({current_count}/{MAX_CONCURRENT_POSITIONS}). Managing active trades...")
                return

            # Step 3: Scan mutual crypto assets for high confluence setups
            candidate_tickets = []
            for sym in self.crypto_map.keys():
                meta = self.crypto_map[sym]
                if meta["mt5"] in open_symbols:
                    continue  # Already in position

                ticket = self.evaluate_asset_confluence(sym)
                if ticket and ticket["confluence_score"] >= 65:
                    candidate_tickets.append(ticket)

            # Sort candidate tickets by confluence score and R:R
            candidate_tickets.sort(key=lambda t: (t["confluence_score"], t["risk_reward_ratio"]), reverse=True)

            # Step 4: Execute top candidates up to position limit
            slots_available = MAX_CONCURRENT_POSITIONS - current_count
            for ticket in candidate_tickets[:slots_available]:
                logger.info(f"High-Confluence Opportunity Found: {ticket['symbol']} ({ticket['direction']}) - Score: {ticket['confluence_score']}%")
                logger.info(f"   -> Reasons: {ticket['reasons']}")
                self.execute_trade(ticket)

        finally:
            mt5.shutdown()

    def run_daemon(self, loop_interval: int = 10):
        """Runs continuous autonomous trading daemon."""
        logger.info(f"Starting CoinGlass Crypto Autonomous Live Trader Daemon (Cycle Interval: {loop_interval}s)")
        while True:
            try:
                self.scan_and_trade_cycle()
            except Exception as e:
                logger.error(f"Error during live trader cycle: {e}", exc_info=True)
            time.sleep(loop_interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Autonomous Crypto Live Trading Daemon")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation cycle and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=10, help="Seconds between scan cycles (default 10s)")
    args = parser.parse_args()

    trader = CryptoLiveTrader()
    if args.daemon:
        trader.run_daemon(args.interval)
    else:
        trader.scan_and_trade_cycle()

```

