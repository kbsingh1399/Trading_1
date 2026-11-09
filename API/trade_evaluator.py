"""
trade_evaluator.py
========================================================================================
Pillar 5 of Standalone Crypto Pipeline: Per-Asset Rolling 5-Signal Queue & Evaluator
========================================================================================
- Maintains dedicated per-asset rolling 5-signal FIFO queues in:
    Database/Signals/Signal_Queues/<SYM>_queue.json
- Handles burst arrivals (e.g. 2 simultaneous signals in one sweep) without loss:
    appends all, slices [-5:] to keep latest 5, analyzes latest triggers in context.
- Synthesizes 5 Pillars of Market Intelligence into a 0-100 Confluence Score:
    Pillar 1: Footprint Signal Quality & Queue Momentum/Exhaustion (Max 30)
    Pillar 2: Auction Market Theory, VAH/VAL/POC & VWAP Alignment (Max 20)
    Pillar 3: Whale Defending Walls & Orderbook Imbalance (Max 20)
    Pillar 4: Liquidation Heatmap Magnet Zones (Max 15)
    Pillar 5: TV 15m Liquidations Bursts & Squeeze Exhaustion (Max 15)
- Dynamic Risk Management:
    Dynamic SL anchored to Defending Whale Wall / VAL / VAH with 0.2% safety buffer
    Take Profit anchored to Counter Liquidation Magnet enforcing R:R >= 2.0
- Deterministic Pass Gates:
    Direction Contradiction Entropy (Hd <= 0.95)
    Strict Confluence Gate (Score >= 75.0)
    Risk-to-Reward Hard Gate (R:R >= 2.0)
- Dual Timestamp & Microsecond Precision:
    Candle bar time: time_ms, time_ist
    Event formulation: created_at_ms, created_at_us, created_at_utc, created_at_ist
- Fully compatible with DuckDB / Polars read_ndjson('Database/Signals/Trade_Setups/*.jsonl'):
    Flat typed primary schema, serialized JSON string for nested metadata, allow_nan=False.
========================================================================================
"""

import sys
import os
import json
import math
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional, Any
from dataclasses import dataclass

import pandas as pd
import numpy as np

# Root and Database directory resolution
ROOT_DIR = Path(__file__).resolve().parent.parent if Path(__file__).resolve().parent.name == 'API' else Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from coinglass_common import (
    writer_lock,
    atomic_json,
    atomic_append_jsonl,
    utc_ms_to_ist_str,
    acquire_singleton,
)

logger = logging.getLogger("TradeEvaluator")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Canonical Storage Paths
DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
QUEUE_DIR = SIGNALS_DIR / "Signal_Queues"
SETUPS_DIR = SIGNALS_DIR / "Trade_Setups"
MASTER_SETUPS_PATH = SETUPS_DIR / "master_active_setups.jsonl"

CANDLE_DIR = DB_DIR / "Candle_Data"
AUCTION_DIR = DB_DIR / "Auction_Levels"
WHALE_DIR = DB_DIR / "Whale_Orders" / "Open_Orders"
MAGNET_DIR = DB_DIR / "Liquidation_Data" / "Magnet_Zones"
TV_LIQ_DIR = DB_DIR / "Live_Liquidations"

# Ensure all directories exist
QUEUE_DIR.mkdir(parents=True, exist_ok=True)
SETUPS_DIR.mkdir(parents=True, exist_ok=True)

# Signal Priority Hierarchy (Lower number = Higher Priority)
SIGNAL_PRIORITY_MAP = {
    "trap": 1,       # Bull/Bear Trap (reversal with trapped market orders)
    "abs": 2,        # Absorption (high volume limit orders soaking aggressive flow)
    "ratio_p": 3,    # Ratio Pass / Bid-Ask Imbalance Pass
    "sdiv": 4,       # Swing CVD Divergence
    "div": 5,        # Delta Divergence
    "p_spike": 6,    # Pressure / Delta Spike
    "exh": 7,        # Volume Exhaustion
}

def get_signal_priority(sig_code: Optional[str]) -> int:
    if not sig_code:
        return 8
    code = str(sig_code).lower().strip()
    return SIGNAL_PRIORITY_MAP.get(code, 8)

def calculate_shannon_entropy(labels: List[str]) -> float:
    """Computes Shannon entropy across binary direction labels (e.g. ['LONG', 'SHORT'])."""
    if not labels:
        return 0.0
    total = len(labels)
    counts: Dict[str, int] = {}
    for l in labels:
        counts[l] = counts.get(l, 0) + 1
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return float(entropy)

def safe_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    try:
        f = float(val)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return default


def calculate_weighted_direction_entropy(
    queue: List[dict],
    half_life_seconds: float = 1800.0,
    reference_ms: Optional[int] = None
) -> float:
    """
    Computes Shannon entropy across queue directions, weighted by:
    1. Signal priority (trap=1.0, abs=0.9, ratio_p=0.75, sdiv=0.65, div=0.6, p_spike=0.4, exh=0.35)
    2. Continuous exponential recency decay (30-minute half-life)
    """
    if not queue:
        return 0.0

    now_ms = reference_ms or int(time.time() * 1000)
    tau = half_life_seconds * 1000.0 / math.log(2)

    priority_weights = {
        "trap": 1.00,
        "abs": 0.90,
        "ratio_p": 0.75,
        "sdiv": 0.65,
        "div": 0.60,
        "p_spike": 0.40,
        "exh": 0.35
    }

    w_long = 0.0
    w_short = 0.0

    for s in queue:
        direction = s.get("direction")
        if direction not in ("LONG", "SHORT"):
            continue

        sig_code = str(s.get("sig_code", "")).lower()
        p_w = priority_weights.get(sig_code, 0.30)

        t_ms = int(s.get("time_ms") or s.get("arrived_at_ms") or now_ms)
        age_ms = max(0, now_ms - t_ms)
        recency_w = math.exp(-age_ms / tau)

        eff_w = p_w * recency_w
        if direction == "LONG":
            w_long += eff_w
        else:
            w_short += eff_w

    total_w = w_long + w_short
    if total_w <= 0:
        return 0.0

    p_l = w_long / total_w
    p_s = w_short / total_w

    if p_l <= 0.0 or p_s <= 0.0:
        return 0.0

    return float(-(p_l * math.log2(p_l) + p_s * math.log2(p_s)))


def classify_market_regime(
    candle_ctx: dict,
    auction_ctx: dict,
    whale_ctx: dict,
    tv_liq_ctx: dict
) -> str:
    """Classifies asset microstructure state into COMPRESSION, EXPANSION, WALL_DOMINATED, or NORMAL."""
    close_px = safe_float(candle_ctx.get("latest_close"), 1.0)
    atr = safe_float(candle_ctx.get("atr_14"), 0.0)
    atr_pct = (atr / close_px) * 100.0 if close_px > 0 else 1.0

    val = safe_float(auction_ctx.get("day_val"))
    vah = safe_float(auction_ctx.get("day_vah"))
    poc = safe_float(auction_ctx.get("day_poc"))

    va_width_pct = 2.0
    if val and vah and poc and poc > 0:
        va_width_pct = ((vah - val) / poc) * 100.0

    bid_wall = safe_float(whale_ctx.get("bid_wall_usd"), 0.0)
    ask_wall = safe_float(whale_ctx.get("ask_wall_usd"), 0.0)
    wall_diff = abs(bid_wall - ask_wall)
    total_wall = bid_wall + ask_wall
    wall_imbalance = (wall_diff / total_wall) if total_wall > 0 else 0.0

    is_tv_spike = bool(tv_liq_ctx.get("is_spike", False))
    ib_ext = str(auction_ctx.get("ib_extension", ""))

    if atr_pct < 0.8 and va_width_pct < 1.5:
        return "COMPRESSION"
    elif atr_pct > 2.0 or is_tv_spike or "EXPANDING" in ib_ext:
        return "EXPANSION"
    elif wall_imbalance > 0.60 and max(bid_wall, ask_wall) >= 1_500_000:
        return "WALL_DOMINATED"
    else:
        return "NORMAL"


def compute_rdnl_confluence(
    w_raw: Dict[str, float],
    regime: str
) -> Tuple[float, float, List[str]]:
    """
    Computes Regime-Dependent Non-Linear Confluence Score with Pillar Interaction Synergy.
    Returns: (final_score, interaction_multiplier, active_couplings)
    """
    base_caps = {"W1": 30.0, "W2": 20.0, "W3": 20.0, "W4": 15.0, "W5": 15.0}
    x = {k: min(max(w_raw.get(k, 0.0), 0.0) / base_caps[k], 1.0) for k in base_caps}

    regime_weights = {
        "COMPRESSION":    {"W1": 0.35, "W2": 0.25, "W3": 0.15, "W4": 0.15, "W5": 0.10},
        "EXPANSION":      {"W1": 0.20, "W2": 0.15, "W3": 0.22, "W4": 0.23, "W5": 0.20},
        "WALL_DOMINATED": {"W1": 0.30, "W2": 0.10, "W3": 0.35, "W4": 0.15, "W5": 0.10},
        "NORMAL":         {"W1": 0.30, "W2": 0.20, "W3": 0.20, "W4": 0.15, "W5": 0.15},
    }

    weights = regime_weights.get(regime, regime_weights["NORMAL"])
    s_base = sum(x[k] * weights[k] * 100.0 for k in base_caps)

    gamma = {
        ("W1", "W2"): 0.20 if regime == "COMPRESSION" else 0.12,
        ("W1", "W3"): 0.25 if regime == "WALL_DOMINATED" else 0.18,
        ("W2", "W4"): 0.15 if regime != "EXPANSION" else 0.08,
        ("W3", "W5"): 0.18,
        ("W1", "W5"): 0.12 if regime == "EXPANSION" else 0.08,
    }

    synergy_bonus = 0.0
    active_couplings = []

    for (p_a, p_b), g in gamma.items():
        if x[p_a] >= 0.50 and x[p_b] >= 0.50:
            phi = math.sqrt(x[p_a] * x[p_b])
            bonus = g * phi
            synergy_bonus += bonus
            active_couplings.append(f"{p_a}x{p_b}(+{round(bonus * 100, 1)}%)")

    # Z.ai Correlation discount rho_ij to prevent double-counting orderbook & flow overlap
    rho = {
        ("W3", "W4"): 0.10,  # Whale Blotter x Liq Heatmap correlation discount
    }
    if regime == "EXPANSION":
        rho[("W1", "W5")] = 0.08  # Footprint Delta x TV Liq Volume correlation discount

    correlation_discount = 0.0
    for (p_a, p_b), r_val in rho.items():
        discount_term = r_val * min(x[p_a], x[p_b])
        correlation_discount += discount_term
        if discount_term > 0.02:
            active_couplings.append(f"{p_a}x{p_b}_discount(-{round(discount_term * 100, 1)}%)")

    m_interact = 1.0 + min(synergy_bonus - correlation_discount, 0.40)
    m_interact = max(0.60, m_interact)
    s_final = min(max(round(s_base * m_interact, 2), 0.0), 100.0)

    return s_final, round(m_interact, 3), active_couplings

# ========================================================================================
# Canonical Polars & DuckDB Schema Definitions for Zero-Copy Analytic Queries
# ========================================================================================
import polars as pl
import duckdb

TRADE_SETUPS_POLARS_SCHEMA = {
    "setup_id": pl.Utf8,
    "symbol": pl.Utf8,
    "status": pl.Utf8,
    "direction": pl.Utf8,
    "signal_count": pl.Int64,
    "primary_signal": pl.Utf8,
    "confluence_score": pl.Float64,
    "w1_footprint": pl.Float64,
    "w2_auction": pl.Float64,
    "w3_whales": pl.Float64,
    "w4_magnets": pl.Float64,
    "w5_tv_liquidations": pl.Float64,
    "entry_price": pl.Float64,
    "stop_loss": pl.Float64,
    "take_profit": pl.Float64,
    "risk_reward_ratio": pl.Float64,
    "risk_amount": pl.Float64,
    "reward_amount": pl.Float64,
    "atr_15m": pl.Float64,
    "auction_bias": pl.Utf8,
    "htf_trend": pl.Utf8,
    "defending_wall_price": pl.Float64,
    "magnet_target_price": pl.Float64,
    "time_ms": pl.Int64,
    "time_ist": pl.Utf8,
    "created_at_ms": pl.Int64,
    "created_at_us": pl.Int64,
    "created_at_utc": pl.Utf8,
    "created_at_ist": pl.Utf8,
    "effective_risk_reward_ratio": pl.Float64,
    "calculated_lots": pl.Float64,
    "target_risk_usd": pl.Float64,
    "kelly_fraction": pl.Float64,
    "concordance_kappa": pl.Float64,
    "queue_snapshot_json": pl.Utf8,
    "evaluation_metrics_json": pl.Utf8,
}

SIGNALS_POLARS_SCHEMA = {
    "sig_id": pl.Utf8,
    "symbol": pl.Utf8,
    "sig_code": pl.Utf8,
    "sig_name": pl.Utf8,
    "direction": pl.Utf8,
    "side": pl.Utf8,
    "price": pl.Float64,
    "bar_type": pl.Utf8,
    "bar_label": pl.Utf8,
    "time_ms": pl.Int64,
    "time_ist": pl.Utf8,
    "priority": pl.Int64,
    "delta": pl.Float64,
    "delta_pct": pl.Float64,
    "cvd": pl.Float64,
    "poc_price": pl.Float64,
    "arrived_at_ms": pl.Int64,
    "arrived_at_ist": pl.Utf8,
}

def load_trade_setups_polars(pattern: str = "*_setups.jsonl", status: Optional[str] = None) -> pl.DataFrame:
    """Zero-copy multi-file loader using explicit schema to avoid Polars Null-inference collapse."""
    path = str(SETUPS_DIR / pattern)
    df = pl.read_ndjson(path, schema=TRADE_SETUPS_POLARS_SCHEMA)
    if status:
        df = df.filter(pl.col("status") == status)
    return df

def scan_trade_setups_polars(pattern: str = "*_setups.jsonl") -> pl.LazyFrame:
    """Lazy zero-copy scanner for streaming analytical queries across all assets."""
    path = str(SETUPS_DIR / pattern)
    return pl.scan_ndjson(path, schema=TRADE_SETUPS_POLARS_SCHEMA)

def query_trade_setups_duckdb(sql_query: str) -> duckdb.DuckDBPyRelation:
    """Executes SQL query over Trade_Setups with union_by_name=true schema stability."""
    pattern = str(SETUPS_DIR / "*_setups.jsonl").replace("\\", "/")
    table_view = f"read_json_auto('{pattern}', union_by_name=true)"
    query = sql_query.replace("trade_setups", table_view)
    return duckdb.sql(query)

def load_signals_polars(pattern: str = "*_signals.jsonl", symbol: Optional[str] = None) -> pl.DataFrame:
    """Zero-copy multi-file signal loader across all asset ledgers."""
    path = str(QUEUE_DIR / pattern)
    df = pl.read_ndjson(path, schema=SIGNALS_POLARS_SCHEMA)
    if symbol:
        df = df.filter(pl.col("symbol") == symbol.upper().strip())
    return df


# ========================================================================================
# 1. Per-Asset Append-Only Signal Store & Event Stream Manager
# ========================================================================================
class SignalQueueManager:
    """
    Append-Only Signal Store & Event Stream Manager.
    Instead of truncating to 5 signals, all harvested signals are permanently appended
    to per-asset JSONL files in Database/Signals/Signal_Queues/<SYM>_signals.jsonl.
    Maintains full historical sequence while deduplicating against identical signatures.
    """
    @staticmethod
    def get_signals_jsonl_path(symbol: str) -> Path:
        return QUEUE_DIR / f"{symbol.upper()}_signals.jsonl"

    @staticmethod
    def get_queue_path(symbol: str) -> Path:
        return QUEUE_DIR / f"{symbol.upper()}_signals.jsonl"

    @staticmethod
    def get_legacy_json_path(symbol: str) -> Path:
        return QUEUE_DIR / f"{symbol.upper()}_queue.json"

    @classmethod
    def load_queue(cls, symbol: str) -> List[dict]:
        return cls.load_all_signals(symbol)

    @classmethod
    def load_all_signals(cls, symbol: str, migrate: bool = True) -> List[dict]:
        """Loads all appended signals from the asset's append-only ledger."""
        p_jsonl = cls.get_signals_jsonl_path(symbol)
        p_legacy = cls.get_legacy_json_path(symbol)

        signals = []
        if p_jsonl.exists():
            try:
                with open(p_jsonl, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                signals.append(json.loads(line))
                            except Exception:
                                pass
                return signals
            except Exception as e:
                logger.warning(f"[{symbol}] Error reading {p_jsonl.name}: {e}")

        # If jsonl does not exist yet, check legacy queue.json and migrate
        if p_legacy.exists():
            try:
                content = p_legacy.read_text(encoding="utf-8", errors="replace").strip()
                if content:
                    parsed = json.loads(content)
                    if isinstance(parsed, list):
                        signals = parsed
                        # Auto-migrate into jsonl append format only if requested (avoids recursive self-lock)
                        if migrate and signals:
                            cls.append_signals(symbol, signals, _is_migration=True)
            except Exception as e:
                logger.warning(f"[{symbol}] Failed reading legacy queue {p_legacy.name}: {e}")

        return signals

    @classmethod
    def push_signals_and_roll(
        cls,
        symbol: str,
        new_signals: List[dict],
        max_len: int = 0
    ) -> Tuple[List[dict], List[dict]]:
        return cls.append_signals(symbol, new_signals)

    @classmethod
    def append_signals(
        cls,
        symbol: str,
        new_signals: List[dict],
        _is_migration: bool = False
    ) -> Tuple[List[dict], List[dict]]:
        """
        Permanently appends newly harvested signals into Database/Signals/Signal_Queues/<SYM>_signals.jsonl.
        Deduplicates against already appended signals using signature keys.
        Never drops or truncates historical signals.
        Returns: (all_signals, newly_added_signals)
        """
        if not new_signals:
            return cls.load_all_signals(symbol), []

        p_jsonl = cls.get_signals_jsonl_path(symbol)
        p_legacy = cls.get_legacy_json_path(symbol)

        with writer_lock(p_jsonl):
            existing = [] if _is_migration else cls.load_all_signals(symbol, migrate=False)
            existing_keys = {
                (
                    str(s.get("sig_code", "")).lower(),
                    str(s.get("bar_type", "time")).lower(),
                    str(s.get("bar_label", "")).strip(),
                    int(s.get("time_ms", 0)),
                )
                for s in existing
            }

            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc + timedelta(hours=5, minutes=30)
            now_ms = int(now_utc.timestamp() * 1000)
            now_us = int(now_utc.timestamp() * 1_000_000)
            now_ist_str = now_ist.strftime("%Y-%m-%d %H:%M:%S.%f IST")

            newly_added = []
            lines_to_append = []

            for s in new_signals:
                sig_code = str(s.get("sig_code", "")).lower()
                bar_type = str(s.get("bar_type", "time")).lower()
                bar_label = str(s.get("bar_label", "")).strip()
                t_ms = int(s.get("time_ms", 0))

                key = (sig_code, bar_type, bar_label, t_ms)
                if key in existing_keys:
                    continue

                raw_side = str(s.get("side", "")).lower()
                if "bull" in raw_side or "long" in raw_side or "buy" in raw_side:
                    direction = "LONG"
                elif "bear" in raw_side or "short" in raw_side or "sell" in raw_side:
                    direction = "SHORT"
                else:
                    direction = "NEUTRAL"

                price_val = safe_float(s.get("price") or s.get("poc_price") or s.get("close"), 0.0)

                pruned_sig = {
                    "sig_id": f"{symbol}_{sig_code}_{bar_type}_{t_ms}_{now_us % 1000000}",
                    "symbol": symbol.upper(),
                    "sig_code": sig_code,
                    "sig_name": str(s.get("sig_name") or s.get("title") or sig_code),
                    "direction": direction,
                    "side": raw_side,
                    "price": round(price_val, 6),
                    "bar_type": bar_type,
                    "bar_label": bar_label,
                    "time_ms": t_ms,
                    "time_ist": s.get("time_ist") or utc_ms_to_ist_str(t_ms),
                    "priority": get_signal_priority(sig_code),
                    "delta": safe_float(s.get("delta"), 0.0),
                    "delta_pct": safe_float(s.get("delta_pct"), 0.0),
                    "cvd": safe_float(s.get("cvd"), 0.0),
                    "poc_price": safe_float(s.get("poc_price"), price_val),
                    "arrived_at_ms": now_ms,
                    "arrived_at_ist": now_ist_str,
                }

                existing_keys.add(key)
                existing.append(pruned_sig)
                newly_added.append(pruned_sig)
                lines_to_append.append(json.dumps(pruned_sig, ensure_ascii=True, allow_nan=False))

            if lines_to_append:
                # Defensive check: ensure prior line was terminated with a newline to prevent line mangling
                if p_jsonl.exists() and p_jsonl.stat().st_size > 0:
                    try:
                        with open(p_jsonl, "rb+") as f_chk:
                            f_chk.seek(-1, os.SEEK_END)
                            if f_chk.read(1) != b"\n":
                                f_chk.write(b"\n")
                                f_chk.flush()
                    except Exception:
                        pass

                with open(p_jsonl, "a", encoding="utf-8") as f:
                    for l in lines_to_append:
                        f.write(l + "\n")
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except OSError:
                        pass

                # Also update legacy json with full history for seamless backward compatibility
                try:
                    atomic_json(p_legacy, existing[-5:] if len(existing) > 5 else existing)
                except Exception:
                    pass

                logger.info(
                    f"[{symbol}] Appended {len(newly_added)} new signal(s) to permanent ledger "
                    f"(Total stored: {len(existing)})."
                )

            return existing, newly_added


# ========================================================================================
# 2. Multi-Pillar Trade Evaluator
# ========================================================================================

def compute_chance_corrected_concordance(
    queue: List[dict],
    target_direction: str,
    baseline_pe: float = 0.50
) -> Tuple[float, float, float]:
    """
    Computes Fleiss/Cohen chance-corrected concordance kappa_c = (P_o - P_e) / (1 - P_e).
    Returns: (kappa_c, P_observed, P_expected)
    """
    valid_signals = [s for s in queue if s.get("direction") in ("LONG", "SHORT")]
    if len(valid_signals) < 2:
        return 0.0, 0.0, baseline_pe
    
    n_total = len(valid_signals)
    n_long = sum(1 for s in valid_signals if s.get("direction") == "LONG")
    p_long = n_long / n_total
    p_short = 1.0 - p_long
    p_e = max(0.40, min(0.60, p_long**2 + p_short**2))
    
    matching = sum(1 for s in valid_signals if s.get("direction") == target_direction)
    p_o = matching / n_total
    
    if p_o <= p_e:
        kappa_c = 0.0
    else:
        kappa_c = (p_o - p_e) / max(1e-6, 1.0 - p_e)
        
    return float(round(kappa_c, 4)), float(round(p_o, 4)), float(round(p_e, 4))


class CushionKellySizer:
    """
    Implements Dynamic Cushion Sizing with strict Cap Dominance:
    r_t <= min(0.091 * C_t, 0.25 * C_t, $15.00)
    Stand-down floor C* = $16.48. Clamping to min-lot is strictly forbidden if r_min > Cap.
    """
    SYMBOL_SPECS = {
        "BTC": {"contract": 1.0, "min_vol": 0.01, "max_vol": 5.0, "step": 0.01},
        "BTCUSD": {"contract": 1.0, "min_vol": 0.01, "max_vol": 5.0, "step": 0.01},
        "ETH": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
        "ETHUSD": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
        "DOGE": {"contract": 1000.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "DOGEUSD": {"contract": 1000.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "ADA": {"contract": 100.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "ADAUSD": {"contract": 100.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "XRP": {"contract": 100.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "XRPUSD": {"contract": 100.0, "min_vol": 0.01, "max_vol": 100.0, "step": 0.01},
        "LTC": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
        "LTCUSD": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
        "LINK": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
        "LINKUSD": {"contract": 1.0, "min_vol": 0.01, "max_vol": 20.0, "step": 0.01},
    }

    @classmethod
    def calculate(
        cls,
        symbol: str,
        entry: float,
        sl: float,
        confluence_score: float,
        eff_rr: float,
        account_cushion: float = 79.37
    ) -> Dict[str, Any]:
        distance = abs(entry - sl)
        if distance <= 1e-8:
            return {"viable": False, "lots": 0.0, "risk_usd": 0.0, "reason": "ZERO_STOP_DISTANCE"}
        
        # Stand-down floor: C* = $16.48 (at universal r_min = $1.50 / 0.091)
        if account_cushion < 16.48:
            return {
                "viable": False,
                "lots": 0.0,
                "risk_usd": 0.0,
                "reason": f"STAND_DOWN_CUSHION_{account_cushion:.2f}_BELOW_16.48"
            }

        # 1. Platt-scaled win probability from Confluence Score
        z = 4.0 * (confluence_score / 100.0) - 2.4
        p_win = 1.0 / (1.0 + math.exp(-z))
        b_eff = max(eff_rr, 1.0)
        f_star = (p_win * b_eff - (1.0 - p_win)) / b_eff
        f_quarter = 0.25 * max(0.0, f_star)

        # 2. Strict Cap Dominance Rule (Proposition 3):
        # 1% Ruin Limit: xi = 0.091. Contractual Cap: 0.25 * C_t. Absolute max: $15.00
        cap = min(15.00, account_cushion * 0.25)
        
        # Target risk bounded between $10.0 and cap (or cap if cushion < $40)
        min_risk_bound = min(10.0, cap)
        target_risk = max(min_risk_bound, min(cap, account_cushion * f_quarter * 2.0))

        # 3. Broker Lot Specs & Discretization
        sym_clean = symbol.upper().replace("USDT", "").replace("USD", "")
        spec = cls.SYMBOL_SPECS.get(symbol.upper()) or cls.SYMBOL_SPECS.get(sym_clean, {"contract": 1.0, "min_vol": 0.01, "max_vol": 1.0, "step": 0.01})
        contract = spec["contract"]
        min_vol = spec["min_vol"]
        max_vol = spec["max_vol"]
        step = spec["step"]

        min_lot_risk = min_vol * contract * distance
        
        # Rule of Cap Dominance with Absolute Abstention:
        # Never round up to min lot if min lot exceeds the risk cap
        if min_lot_risk > cap:
            return {
                "viable": False,
                "lots": 0.0,
                "risk_usd": round(min_lot_risk, 2),
                "cap_usd": round(cap, 2),
                "reason": f"MIN_LOT_RISK_{min_lot_risk:.2f}_EXCEEDS_CAP_{cap:.2f}"
            }

        raw_lots = target_risk / max(contract * distance, 1e-8)
        steps = math.floor(raw_lots / step)
        lots = max(min_vol, min(max_vol, round(steps * step, 4)))
        
        # If stepped lot pushes risk above cap, step down
        if (lots * contract * distance) > cap and (lots - step) >= min_vol:
            lots = round(lots - step, 4)
            
        actual_risk = round(lots * contract * distance, 2)
        
        if actual_risk > cap:
            return {
                "viable": False,
                "lots": 0.0,
                "risk_usd": actual_risk,
                "cap_usd": round(cap, 2),
                "reason": f"ACTUAL_RISK_{actual_risk:.2f}_EXCEEDS_CAP_{cap:.2f}"
            }

        return {
            "viable": True,
            "lots": lots,
            "risk_usd": actual_risk,
            "target_risk_usd": round(target_risk, 2),
            "cap_usd": round(cap, 2),
            "kelly_fraction": round(f_quarter, 4),
            "p_win": round(p_win, 4),
            "reason": "OK"
        }

def _parse_usd(v) -> float:
    if v is None or pd.isna(v):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace('$', '').replace(',', '').strip().upper()
    if not s:
        return 0.0
    mult = 1.0
    if s.endswith('K'):
        mult = 1e3
        s = s[:-1]
    elif s.endswith('M'):
        mult = 1e6
        s = s[:-1]
    elif s.endswith('B'):
        mult = 1e9
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return 0.0


class MultiPillarTradeEvaluator:
    @classmethod
    def load_candle_context(cls, symbol: str) -> Dict[str, Any]:
        """Loads latest 15m candle metrics and ATR from Database/Candle_Data/<SYM>.parquet."""
        p = CANDLE_DIR / f"{symbol.upper()}.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            if df.empty:
                return {}
            last_row = df.iloc[-1].to_dict()

            # Calculate 14-period ATR
            atr_val = 0.0
            if len(df) >= 5 and "high" in df.columns and "low" in df.columns and "close" in df.columns:
                highs = df["high"].astype(float)
                lows = df["low"].astype(float)
                closes = df["close"].astype(float)
                prev_closes = closes.shift(1).fillna(closes)
                tr1 = highs - lows
                tr2 = (highs - prev_closes).abs()
                tr3 = (lows - prev_closes).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr_val = float(tr.tail(14).mean())

            return {
                "latest_close": safe_float(last_row.get("close")),
                "latest_open": safe_float(last_row.get("open")),
                "latest_high": safe_float(last_row.get("high")),
                "latest_low": safe_float(last_row.get("low")),
                "latest_volume": safe_float(last_row.get("total_volume") or last_row.get("volume"), 0.0),
                "latest_delta": safe_float(last_row.get("delta"), 0.0),
                "latest_delta_pct": safe_float(last_row.get("delta_pct"), 0.0),
                "latest_cvd": safe_float(last_row.get("cumulative_delta") or last_row.get("cvd"), 0.0),
                "latest_poc": safe_float(last_row.get("poc_price")),
                "atr_14": round(atr_val, 6) if atr_val > 0 else None,
                "time_ms": int(last_row.get("open_time_ms") or 0),
                "time_ist": str(last_row.get("time_ist") or ""),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading candles: {e}")
            return {}

    @classmethod
    def load_auction_context(cls, symbol: str) -> Dict[str, Any]:
        """Loads auction profile context from Auction_Levels or fresh Footprint_Dumps."""
        sym = symbol.upper()
        p = AUCTION_DIR / f"{sym}_auction_levels.parquet"
        fp_path = DB_DIR / "Footprint_Dumps" / f"{sym}_15Min_time_footprint.parquet"
        
        # Prefer fresh Footprint_Dumps if Auction_Levels is missing or older
        use_fp = False
        if fp_path.exists():
            if not p.exists():
                use_fp = True
            else:
                try:
                    if fp_path.stat().st_mtime > p.stat().st_mtime + 3600:
                        use_fp = True
                except Exception:
                    pass

        if use_fp:
            try:
                df_fp = pd.read_parquet(fp_path)
                if not df_fp.empty:
                    last_row = df_fp.iloc[-1].to_dict()
                    close_p = safe_float(last_row.get("Close"), 0.0)
                    vwap_p = safe_float(last_row.get("Candle_Vwap_Price"), close_p)
                    poc_p = safe_float(last_row.get("Candle_POC"), close_p)
                    vah_p = safe_float(last_row.get("Candle_VAH"), close_p)
                    val_p = safe_float(last_row.get("Candle_VAL"), close_p)
                    delta_p = safe_float(last_row.get("Candle_Delta"), 0.0)
                    
                    price_vs_vwap = "ABOVE_VWAP" if close_p >= vwap_p else "BELOW_VWAP"
                    if close_p > vah_p:
                        val_stat = "ABOVE_VALUE"
                    elif close_p < val_p:
                        val_stat = "BELOW_VALUE"
                    else:
                        val_stat = "INSIDE_VALUE"
                    mtf = "BULLISH" if delta_p > 0 else "BEARISH"

                    return {
                        "day_val": val_p,
                        "day_vah": vah_p,
                        "day_poc": poc_p,
                        "day_vwap": vwap_p,
                        "value_status": val_stat,
                        "price_vs_vwap": price_vs_vwap,
                        "mtf_bias": mtf,
                        "htf_1h_trend": mtf,
                        "htf_4h_trend": "UNKNOWN",
                        "nearest_whale_bid": None,
                        "nearest_whale_ask": None,
                        "updated_at_utc": str(last_row.get("Time") or ""),
                    }
            except Exception as e:
                logger.debug(f"[{symbol}] Failed reading footprint auction levels: {e}")

        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            if df.empty:
                return {}
            row = df.iloc[-1].to_dict()
            return {
                "day_val": safe_float(row.get("day_val")),
                "day_vah": safe_float(row.get("day_vah")),
                "day_poc": safe_float(row.get("day_poc")),
                "day_vwap": safe_float(row.get("day_vwap")),
                "value_status": str(row.get("value_status") or "UNKNOWN"),
                "price_vs_vwap": str(row.get("price_vs_vwap") or "UNKNOWN"),
                "mtf_bias": str(row.get("mtf_bias") or "UNKNOWN"),
                "htf_1h_trend": str(row.get("htf_1h_trend") or "UNKNOWN"),
                "htf_4h_trend": str(row.get("htf_4h_trend") or "UNKNOWN"),
                "nearest_whale_bid": row.get("nearest_whale_bid"),
                "nearest_whale_ask": row.get("nearest_whale_ask"),
                "updated_at_utc": str(row.get("updated_at_utc") or ""),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading auction levels: {e}")
            return {}
    @classmethod
    def load_whale_context(cls, symbol: str, current_price: float) -> Dict[str, Any]:
        """Loads active open whale orders and identifies defending bid/ask walls."""
        sym = symbol.upper()
        p = WHALE_DIR / f"{sym}_whale_open_orders.parquet"
        if not p.exists():
            p = DB_DIR / "Whale_Dumps" / f"{sym}_whale_open_orders.parquet"
        if not p.exists() or current_price <= 0:
            return {"defending_bid": None, "defending_ask": None, "bid_wall_usd": 0.0, "ask_wall_usd": 0.0}
        try:
            df = pd.read_parquet(p)
            if df.empty:
                return {"defending_bid": None, "defending_ask": None, "bid_wall_usd": 0.0, "ask_wall_usd": 0.0}

            # Robust column normalization across 8-column display and legacy quant formats
            if "side" not in df.columns and "Venue/Side" in df.columns:
                df["side"] = df["Venue/Side"].astype(str).apply(lambda v: "bid" if "bid" in v.lower() else "ask")
            if "raw_price" in df.columns:
                df["price"] = df["raw_price"].astype(float)
            elif "Price" in df.columns and ("price" not in df.columns or df["price"].dtype == object):
                df["price"] = df["Price"].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip().apply(safe_float)
            if "raw_remaining_usd" in df.columns:
                df["remaining_usd"] = df["raw_remaining_usd"].astype(float)
            elif "Remaining_USD" in df.columns and ("remaining_usd" not in df.columns or df["remaining_usd"].dtype == object):
                df["remaining_usd"] = df["Remaining_USD"].apply(_parse_usd)

            # Filter for orders with decay_weight >= 0.25 (fresh within ~45 min)
            if "decay_weight" in df.columns:
                df = df[df["decay_weight"] >= 0.25]

            bids = df[df["side"].astype(str).str.lower() == "bid"].copy()
            asks = df[df["side"].str.lower() == "ask"].copy()

            # Defending bid: order below current price within 0.1% to 2.5%
            bids["dist_pct"] = (current_price - bids["price"]) / current_price * 100.0
            defending_bids = bids[(bids["dist_pct"] >= 0.1) & (bids["dist_pct"] <= 2.5)]
            best_bid_wall = None
            bid_wall_usd = 0.0
            if not defending_bids.empty:
                best_bid_row = defending_bids.sort_values(by="remaining_usd", ascending=False).iloc[0]
                best_bid_wall = {
                    "price": safe_float(best_bid_row["price"]),
                    "usd": safe_float(best_bid_row["remaining_usd"]),
                    "dist_pct": safe_float(best_bid_row["dist_pct"]),
                }
                bid_wall_usd = float(defending_bids["remaining_usd"].sum())

            # Defending ask: order above current price within 0.1% to 2.5%
            asks["dist_pct"] = (asks["price"] - current_price) / current_price * 100.0
            defending_asks = asks[(asks["dist_pct"] >= 0.1) & (asks["dist_pct"] <= 2.5)]
            best_ask_wall = None
            ask_wall_usd = 0.0
            if not defending_asks.empty:
                best_ask_row = defending_asks.sort_values(by="remaining_usd", ascending=False).iloc[0]
                best_ask_wall = {
                    "price": safe_float(best_ask_row["price"]),
                    "usd": safe_float(best_ask_row["remaining_usd"]),
                    "dist_pct": safe_float(best_ask_row["dist_pct"]),
                }
                ask_wall_usd = float(defending_asks["remaining_usd"].sum())

            return {
                "defending_bid": best_bid_wall,
                "defending_ask": best_ask_wall,
                "bid_wall_usd": round(bid_wall_usd, 2),
                "ask_wall_usd": round(ask_wall_usd, 2),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading whale orders: {e}")
            return {"defending_bid": None, "defending_ask": None, "bid_wall_usd": 0.0, "ask_wall_usd": 0.0}

    @classmethod
    def load_magnet_context(cls, symbol: str, current_price: float) -> Dict[str, Any]:
        """Loads nearest liquidation magnet zones from Liquidation_Level_Dumps or legacy Liquidation_Data."""
        sym = symbol.upper()
        # Primary: Standalone API multi-horizon MagnetZones
        p = DB_DIR / "Liquidation_Level_Dumps" / sym / f"3D_{sym}_MagnetZones.parquet"
        if not p.exists():
            p = DB_DIR / "Liquidation_Level_Dumps" / sym / f"7D_{sym}_MagnetZones.parquet"
        if not p.exists():
            p = MAGNET_DIR / f"{sym}_magnet_zones.parquet"
        if not p.exists() or current_price <= 0:
            return {"long_magnet": None, "short_magnet": None}
        try:
            df = pd.read_parquet(p)
            if df.empty:
                return {"long_magnet": None, "short_magnet": None}

            # Normalize columns from standalone 3D_<SYM>_MagnetZones schema
            if "Side" in df.columns:
                df["side"] = df["Side"].astype(str).str.upper()
            if "zone_price" not in df.columns:
                if "Price_Low" in df.columns and "Price_High" in df.columns:
                    p_low = pd.to_numeric(df["Price_Low"], errors="coerce").fillna(0.0)
                    p_high = pd.to_numeric(df["Price_High"], errors="coerce").fillna(0.0)
                    df["zone_price"] = (p_low + p_high) / 2.0
                else:
                    df["zone_price"] = 0.0

            # Robust fallback for zero zone_price parsing Price_Range strings
            if (df["zone_price"] == 0.0).any() and "Price_Range" in df.columns:
                def _parse_pr_center(val):
                    if not val or pd.isna(val) or str(val).lower() == "none":
                        return 0.0
                    subparts = re.findall(r'[\d.]+[KMB]?', str(val).replace("$", "").replace(",", ""))
                    parsed_nums = [_parse_usd(p) for p in subparts if _parse_usd(p) > 0]
                    return sum(parsed_nums) / len(parsed_nums) if parsed_nums else 0.0
                zero_mask = df["zone_price"] == 0.0
                df.loc[zero_mask, "zone_price"] = df.loc[zero_mask, "Price_Range"].apply(_parse_pr_center)
            if "score" not in df.columns and "Strength" in df.columns:
                df["score"] = pd.to_numeric(df["Strength"], errors="coerce").fillna(1.0)
            if "distance_pct" not in df.columns and "Distance" in df.columns:
                df["distance_pct"] = df["Distance"].astype(str).str.replace("%", "", regex=False).str.replace("+", "", regex=False).str.replace("-", "", regex=False).apply(safe_float)

            shorts = df[df["side"].str.upper().str.contains("SHORT|ABOVE")]
            longs = df[df["side"].str.upper().str.contains("LONG|BELOW")]

            target_short_liq = None
            if not shorts.empty and "score" in shorts.columns:
                s_sorted = shorts.sort_values(by="score", ascending=False).iloc[0]
                target_short_liq = {
                    "price": safe_float(s_sorted.get("zone_price")),
                    "score": safe_float(s_sorted.get("score")),
                    "dist_pct": safe_float(s_sorted.get("distance_pct")),
                }

            target_long_liq = None
            if not longs.empty and "score" in longs.columns:
                l_sorted = longs.sort_values(by="score", ascending=False).iloc[0]
                target_long_liq = {
                    "price": safe_float(l_sorted.get("zone_price")),
                    "score": safe_float(l_sorted.get("score")),
                    "dist_pct": safe_float(l_sorted.get("distance_pct")),
                }

            return {
                "short_magnet": target_short_liq,
                "long_magnet": target_long_liq,
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading magnet zones: {e}")
            return {"long_magnet": None, "short_magnet": None}
    @classmethod
    def load_tv_liquidation_context(cls, symbol: str) -> Dict[str, Any]:
        """Loads latest liquidation spikes from Live_Liquidations/<SYM>_Live_Liquidation.parquet or tv_liquidations.parquet."""
        sym = symbol.upper()
        p = TV_LIQ_DIR / f"{sym}_Live_Liquidation.parquet"
        if not p.exists():
            p = DB_DIR / "Live_Liquidation_Dump" / f"{sym}_Live_Liquidation.parquet"
        if not p.exists():
            p = TV_LIQ_DIR / f"{sym}_tv_liquidations.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            if df.empty:
                return {}
            
            # Map columns across 5m Live_Liquidation and legacy tv_liquidations format
            long_col = "Long_Liquidation" if "Long_Liquidation" in df.columns else "long_liq_usd"
            short_col = "Short_Liquidation" if "Short_Liquidation" in df.columns else "short_liq_usd"
            if long_col not in df.columns or short_col not in df.columns:
                return {}

            last_row = df.iloc[-1].to_dict()
            latest_long = safe_float(last_row.get(long_col), 0.0)
            latest_short = safe_float(last_row.get(short_col), 0.0)
            
            # 1-hour window: 12 bars for 5m, 4 bars for 15m
            window_size = 12 if "Candle_Type" in df.columns and "5" in str(df.iloc[-1].get("Candle_Type", "")) else 4
            recent = df.tail(window_size)
            recent_long_liq = float(pd.to_numeric(recent[long_col], errors="coerce").fillna(0.0).sum())
            recent_short_liq = float(pd.to_numeric(recent[short_col], errors="coerce").fillna(0.0).sum())

            # Evaluate spike and reversal exhaustion
            if "is_spike" in last_row:
                is_spike = bool(last_row.get("is_spike", False))
                reversal_bias = str(last_row.get("reversal_bias") or "NEUTRAL")
            else:
                rolling_avg = (recent_long_liq + recent_short_liq) / max(1, len(recent))
                latest_total = latest_long + latest_short
                is_spike = latest_total > max(150000.0, rolling_avg * 2.0)
                if recent_long_liq > 2.0 * max(1000.0, recent_short_liq) and recent_long_liq > 80000.0:
                    reversal_bias = "BULLISH_EXHAUSTION"
                elif recent_short_liq > 2.0 * max(1000.0, recent_long_liq) and recent_short_liq > 80000.0:
                    reversal_bias = "BEARISH_EXHAUSTION"
                else:
                    reversal_bias = "NEUTRAL"

            return {
                "latest_long_liq": latest_long,
                "latest_short_liq": latest_short,
                "recent_long_liq_1h": safe_float(recent_long_liq, 0.0),
                "recent_short_liq_1h": safe_float(recent_short_liq, 0.0),
                "is_spike": is_spike,
                "reversal_bias": reversal_bias,
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading TV liquidations: {e}")
            return {}
    @classmethod
    def load_oi_funding_context(cls, symbol: str) -> dict:
        """Pillar 6: Loads Open Interest and Predicted Funding Rate telemetry."""
        p = DB_DIR / "Market_Data" / "oi_and_funding.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "open_interest_usd": safe_float(row.get("open_interest_usd"), 0.0),
                "funding_rate": safe_float(row.get("funding_rate"), 0.0),
                "funding_rate_pct": safe_float(row.get("funding_rate_pct"), 0.0),
                "squeeze_bias": str(row.get("squeeze_bias") or "NEUTRAL"),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading OI/Funding data: {e}")
            return {}

    @classmethod
    def load_sentiment_context(cls, symbol: str) -> dict:
        """Pillar 7: Loads Top Trader vs Global Retail Sentiment Divergence."""
        p = DB_DIR / "Market_Data" / "sentiment_divergence.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "top_long_pct": safe_float(row.get("top_long_pct"), 50.0),
                "top_short_pct": safe_float(row.get("top_short_pct"), 50.0),
                "global_long_pct": safe_float(row.get("global_long_pct"), 50.0),
                "global_short_pct": safe_float(row.get("global_short_pct"), 50.0),
                "divergence_spread": safe_float(row.get("divergence_spread"), 0.0),
                "sentiment_bias": str(row.get("sentiment_bias") or "NEUTRAL"),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading Sentiment data: {e}")
            return {}

    @classmethod
    def load_heatmap_density_context(cls, symbol: str, current_price: float) -> dict:
        """Pillar 8: Loads Multi-Timeframe Liquidation Heatmap Intensity."""
        p = DB_DIR / "Market_Data" / "heatmap_density.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "upper_magnet_price": safe_float(row.get("upper_magnet_price")),
                "upper_magnet_dist_pct": safe_float(row.get("upper_magnet_dist_pct")),
                "lower_magnet_price": safe_float(row.get("lower_magnet_price")),
                "lower_magnet_dist_pct": safe_float(row.get("lower_magnet_dist_pct")),
                "asymmetry_ratio": safe_float(row.get("asymmetry_ratio"), 1.0),
                "magnet_bias": str(row.get("magnet_bias") or "BALANCED"),
                "is_magnet_pool": bool(row.get("is_magnet_pool", False)),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading Heatmap Density data: {e}")
            return {}


    @classmethod
    def evaluate_symbol(
        cls,
        symbol: str,
        trigger_signals: List[dict],
        queue: List[dict],
        current_price: Optional[float] = None
    ) -> Optional[dict]:
        """
        Synthesizes the 5 Pillars of Market Intelligence to formulate a structured trade setup.
        Returns a canonical setup dict or None.
        """
        if not queue:
            return None

        # Determine primary trigger: highest priority among newly arrived triggers
        sorted_triggers = sorted(trigger_signals if trigger_signals else queue, key=lambda s: s.get("priority", 8))
        primary_trigger = sorted_triggers[0]

        # Determine recent window for momentum & entropy context (latest up to 15 signals)
        recent_window = queue[-15:] if len(queue) > 15 else queue

        target_direction = primary_trigger.get("direction", "NEUTRAL")
        if target_direction not in ("LONG", "SHORT"):
            # If neutral, derive from recent consensus
            long_cnt = sum(1 for s in recent_window if s.get("direction") == "LONG")
            short_cnt = sum(1 for s in recent_window if s.get("direction") == "SHORT")
            if long_cnt > short_cnt:
                target_direction = "LONG"
            elif short_cnt > long_cnt:
                target_direction = "SHORT"
            else:
                return None  # Pure deadlock

        # Direction Contradiction Entropy Gate on recent window
        queue_dirs = [s.get("direction", "NEUTRAL") for s in recent_window if s.get("direction") in ("LONG", "SHORT")]
        entropy = calculate_shannon_entropy(queue_dirs)
        weighted_entropy = calculate_weighted_direction_entropy(recent_window)
        contradiction_veto = False
        # Z.ai Upgrade 3.2: Strictly enforce Priority-Weighted Recency-Decayed Entropy (PWD-Entropy)
        if weighted_entropy > 0.95:
            # High chop / split queue even after recency decay and priority weighting
            contradiction_veto = True

        # Load context across all 5 pillars
        candle_ctx = cls.load_candle_context(symbol)
        raw_candidate = (
            safe_float(current_price)
            or safe_float(candle_ctx.get("latest_close"))
            or safe_float(primary_trigger.get("price"))
            or 0.0
        )
        ref_price = safe_float(raw_candidate, 0.0)
        if not ref_price or ref_price <= 0.0 or math.isnan(ref_price) or math.isinf(ref_price):
            logger.warning(f"[{symbol}] Cannot evaluate: invalid reference price {ref_price}")
            return None

        auction_ctx = cls.load_auction_context(symbol)
        whale_ctx = cls.load_whale_context(symbol, ref_price)
        magnet_ctx = cls.load_magnet_context(symbol, ref_price)
        tv_liq_ctx = cls.load_tv_liquidation_context(symbol)
        oi_ctx = cls.load_oi_funding_context(symbol)
        sentiment_ctx = cls.load_sentiment_context(symbol)
        heatmap_ctx = cls.load_heatmap_density_context(symbol, ref_price)

        # -------------------------------------------------------------------------
        # Pillar 1: Footprint Signal Quality & Queue Alignment (Max 30)
        # -------------------------------------------------------------------------
        w1_score = 0.0
        p_code = primary_trigger.get("sig_code", "")
        # Base trigger priority
        if p_code == "trap":
            w1_score += 18.0
        elif p_code == "abs":
            w1_score += 15.0
        elif p_code == "ratio_p":
            w1_score += 13.0
        elif p_code in ("sdiv", "div"):
            w1_score += 11.0
        elif p_code in ("p_spike", "exh"):
            w1_score += 9.0
        else:
            w1_score += 7.0

        # Recent signal concordance with Cohen chance-correction (kappa_c)
        kappa_c, p_o, p_e = compute_chance_corrected_concordance(recent_window, target_direction)
        matching_in_queue = sum(1 for s in recent_window if s.get("direction") == target_direction)
        if len(recent_window) >= 2:
            if kappa_c >= 0.60:
                w1_score += 8.0
            elif kappa_c >= 0.35:
                w1_score += 5.0
            elif kappa_c >= 0.15:
                w1_score += 3.0
        elif len(recent_window) == 1:
            # Single isolated signal baseline: baseline points without multi-signal bonus
            w1_score += 3.0

        # Burst concordance bonus
        if len(trigger_signals) >= 2:
            agreeing_triggers = sum(1 for s in trigger_signals if s.get("direction") == target_direction)
            if agreeing_triggers >= 2:
                w1_score += 2.0

        # Delta confirmation
        latest_delta = candle_ctx.get("latest_delta", 0.0)
        if target_direction == "LONG" and latest_delta > 0:
            w1_score += 2.0
        elif target_direction == "SHORT" and latest_delta < 0:
            w1_score += 2.0

        w1_score = min(w1_score, 30.0)

        # -------------------------------------------------------------------------
        # Pillar 2: Auction Market Theory, VAH/VAL/POC & VWAP Alignment (Max 20)
        # -------------------------------------------------------------------------
        w2_score = 0.0
        val = safe_float(auction_ctx.get("day_val"))
        vah = safe_float(auction_ctx.get("day_vah"))
        poc = safe_float(auction_ctx.get("day_poc"))
        vwap = safe_float(auction_ctx.get("day_vwap"))
        mtf_bias = auction_ctx.get("mtf_bias", "")

        if target_direction == "LONG":
            # Bouncing off VAL or reclaiming POC
            if val and abs(ref_price - val) / ref_price <= 0.012:
                w2_score += 8.0
            if vwap and ref_price >= vwap:
                w2_score += 6.0
            elif poc and ref_price >= poc:
                w2_score += 4.0
            if "BULLISH" in mtf_bias:
                w2_score += 6.0
            elif "BALANCED" in mtf_bias:
                w2_score += 3.0
        else:  # SHORT
            # Rejecting VAH or losing POC
            if vah and abs(ref_price - vah) / ref_price <= 0.012:
                w2_score += 8.0
            if vwap and ref_price <= vwap:
                w2_score += 6.0
            elif poc and ref_price <= poc:
                w2_score += 4.0
            if "BEARISH" in mtf_bias:
                w2_score += 6.0
            elif "BALANCED" in mtf_bias:
                w2_score += 3.0

        w2_score = min(w2_score, 20.0)

        # -------------------------------------------------------------------------
        # Pillar 3: Whale Defending Walls & Orderbook Imbalance (Max 20)
        # -------------------------------------------------------------------------
        w3_score = 0.0
        defending_bid = whale_ctx.get("defending_bid")
        defending_ask = whale_ctx.get("defending_ask")
        bid_wall_usd = whale_ctx.get("bid_wall_usd", 0.0)
        ask_wall_usd = whale_ctx.get("ask_wall_usd", 0.0)

        if target_direction == "LONG":
            if defending_bid:
                usd = defending_bid.get("usd", 0.0)
                if usd >= 1_500_000:
                    w3_score += 16.0
                elif usd >= 500_000:
                    w3_score += 12.0
                elif usd >= 100_000:
                    w3_score += 8.0
                else:
                    w3_score += 5.0
            if bid_wall_usd > ask_wall_usd * 1.5:
                w3_score += 4.0
        else:  # SHORT
            if defending_ask:
                usd = defending_ask.get("usd", 0.0)
                if usd >= 1_500_000:
                    w3_score += 16.0
                elif usd >= 500_000:
                    w3_score += 12.0
                elif usd >= 100_000:
                    w3_score += 8.0
                else:
                    w3_score += 5.0
            if ask_wall_usd > bid_wall_usd * 1.5:
                w3_score += 4.0

        w3_score = min(w3_score, 20.0)

        # -------------------------------------------------------------------------
        # Pillar 4: Liquidation Heatmap Magnet Zones (Max 15)
        # -------------------------------------------------------------------------
        w4_score = 0.0
        short_magnet = magnet_ctx.get("short_magnet")  # upside target
        long_magnet = magnet_ctx.get("long_magnet")    # downside target

        if target_direction == "LONG" and short_magnet:
            score_m = short_magnet.get("score", 0.0)
            dist_m = short_magnet.get("dist_pct", 0.0)
            if 0.3 <= dist_m <= 4.0:
                if score_m >= 75.0:
                    w4_score += 15.0
                elif score_m >= 50.0:
                    w4_score += 11.0
                else:
                    w4_score += 7.0
        elif target_direction == "SHORT" and long_magnet:
            score_m = long_magnet.get("score", 0.0)
            dist_m = long_magnet.get("dist_pct", 0.0)
            if 0.3 <= dist_m <= 4.0:
                if score_m >= 75.0:
                    w4_score += 15.0
                elif score_m >= 50.0:
                    w4_score += 11.0
                else:
                    w4_score += 7.0

        w4_score = min(w4_score, 15.0)

        # -------------------------------------------------------------------------
        # Pillar 5: TV 15m Liquidations Spike (Max 15)
        # -------------------------------------------------------------------------
        w5_score = 0.0
        is_spike = tv_liq_ctx.get("is_spike", False)
        rev_bias = tv_liq_ctx.get("reversal_bias", "NEUTRAL")
        rec_long_liq = tv_liq_ctx.get("recent_long_liq_1h", 0.0)
        rec_short_liq = tv_liq_ctx.get("recent_short_liq_1h", 0.0)

        if target_direction == "LONG":
            if rev_bias == "BULLISH_REVERSAL":
                w5_score += 15.0
            elif is_spike and rec_short_liq > rec_long_liq:
                w5_score += 12.0
            elif rec_short_liq > 1_000_000:
                w5_score += 8.0
            elif rec_short_liq > 250_000:
                w5_score += 5.0
        else:  # SHORT
            if rev_bias == "BEARISH_REVERSAL":
                w5_score += 15.0
            elif is_spike and rec_long_liq > rec_short_liq:
                w5_score += 12.0
            elif rec_long_liq > 1_000_000:
                w5_score += 8.0
            elif rec_long_liq > 250_000:
                w5_score += 5.0

        w5_score = min(w5_score, 15.0)

        # -------------------------------------------------------------------------
        # Pillars 6, 7, 8: Microstructure Confluence Expansion Modifiers
        # -------------------------------------------------------------------------
        expansion_boost = 0.0

        # Pillar 6: Open Interest & Predicted Funding Rate Squeeze Acceleration
        squeeze_bias = oi_ctx.get("squeeze_bias", "NEUTRAL")
        if target_direction == "LONG" and squeeze_bias == "BULLISH_SHORT_SQUEEZE":
            expansion_boost += 8.0
        elif target_direction == "SHORT" and squeeze_bias == "BEARISH_LONG_SQUEEZE":
            expansion_boost += 8.0

        # Pillar 7: Top Trader Sentiment Divergence (Smart vs Retail Money)
        sentiment_bias = sentiment_ctx.get("sentiment_bias", "NEUTRAL")
        div_spread = sentiment_ctx.get("divergence_spread", 0.0)
        if target_direction == "LONG" and (sentiment_bias == "BULLISH_SMART_MONEY" or div_spread >= 5.0):
            expansion_boost += 6.0
        elif target_direction == "SHORT" and (sentiment_bias == "BEARISH_SMART_MONEY" or div_spread <= -5.0):
            expansion_boost += 6.0

        # Pillar 8: Multi-Timeframe Heatmap Magnet Intensity
        mag_bias = heatmap_ctx.get("magnet_bias", "BALANCED")
        if target_direction == "LONG" and mag_bias == "BULLISH_UPPER_MAGNET":
            expansion_boost += 6.0
        elif target_direction == "SHORT" and mag_bias == "BEARISH_LOWER_MAGNET":
            expansion_boost += 6.0

        # Total Confluence Score via Non-Linear Regime-Dependent Confluence Scoring (RDNL-CS)
        raw_confluence = round(w1_score + w2_score + w3_score + w4_score + w5_score + expansion_boost, 2)
        regime = classify_market_regime(candle_ctx, auction_ctx, whale_ctx, tv_liq_ctx)
        rdnl_score, m_interact, active_couplings = compute_rdnl_confluence(
            {"W1": w1_score, "W2": w2_score, "W3": w3_score, "W4": w4_score, "W5": w5_score},
            regime
        )
        confluence_score = min(100.0, round(rdnl_score + expansion_boost, 2))

        # -------------------------------------------------------------------------
        # Dynamic Risk Management & Levels Formulation
        # -------------------------------------------------------------------------
        atr_raw = safe_float(candle_ctx.get("atr_14"))
        atr = atr_raw if (atr_raw is not None and atr_raw > 0) else (ref_price * 0.008)

        # Dynamic precision based on price magnitude (e.g. BTC vs PEPE sub-cent meme tokens)
        if ref_price >= 1000.0:
            p_dec = 2
        elif ref_price >= 1.0:
            p_dec = 4
        elif ref_price >= 0.01:
            p_dec = 6
        else:
            p_dec = 8

        min_tick = 10.0 ** (-p_dec)
        min_risk_dist = max(atr * 1.2, ref_price * 0.004, min_tick * 10.0)

        defending_wall_price = None
        target_magnet_price = None

        if target_direction == "LONG":
            # SL anchored behind defending whale bid or VAL with buffer
            bid_px = safe_float(defending_bid.get("price")) if defending_bid else None
            val_px = safe_float(val)

            candidate_sls = []
            if bid_px and bid_px < ref_price:
                candidate_sls.append(bid_px * 0.998)
                defending_wall_price = bid_px
            if val_px and val_px < ref_price:
                candidate_sls.append(val_px * 0.998)

            if candidate_sls:
                sl_price = min(candidate_sls)  # anchor behind true structural support
                if (ref_price - sl_price) < min_risk_dist:
                    sl_price = ref_price - min_risk_dist
            else:
                sl_price = ref_price - min_risk_dist

            risk_dist = ref_price - sl_price
            if risk_dist < min_risk_dist:
                sl_price = ref_price - min_risk_dist
                risk_dist = min_risk_dist

            # TP anchored to short liquidation magnet or VAH, enforcing R:R >= 2.0
            target_short_px = safe_float(short_magnet.get("price")) if short_magnet else None
            target_vah = safe_float(vah)

            min_tp = ref_price + (2.0 * risk_dist)
            tp_price = min_tp

            if target_short_px and target_short_px > ref_price:
                target_magnet_price = target_short_px
                if target_short_px >= min_tp:
                    tp_price = target_short_px
            elif target_vah and target_vah > ref_price and target_vah >= min_tp:
                tp_price = target_vah

        else:  # SHORT
            # SL anchored behind defending whale ask or VAH with buffer
            ask_px = safe_float(defending_ask.get("price")) if defending_ask else None
            vah_px = safe_float(vah)

            candidate_sls = []
            if ask_px and ask_px > ref_price:
                candidate_sls.append(ask_px * 1.002)
                defending_wall_price = ask_px
            if vah_px and vah_px > ref_price:
                candidate_sls.append(vah_px * 1.002)

            if candidate_sls:
                sl_price = max(candidate_sls)  # anchor behind true structural resistance
                if (sl_price - ref_price) < min_risk_dist:
                    sl_price = ref_price + min_risk_dist
            else:
                sl_price = ref_price + min_risk_dist

            risk_dist = sl_price - ref_price
            if risk_dist < min_risk_dist:
                sl_price = ref_price + min_risk_dist
                risk_dist = min_risk_dist

            # TP anchored to long liquidation magnet or VAL, enforcing R:R >= 2.0
            target_long_px = safe_float(long_magnet.get("price")) if long_magnet else None
            target_val = safe_float(val)

            min_tp = ref_price - (2.0 * risk_dist)
            tp_price = min_tp

            if target_long_px and target_long_px < ref_price:
                target_magnet_price = target_long_px
                if target_long_px <= min_tp:
                    tp_price = target_long_px
            elif target_val and target_val < ref_price and target_val <= min_tp:
                tp_price = target_val

        # Quantize prices according to scale and enforce strictly non-zero spacing
        entry_final = round(ref_price, p_dec)
        sl_final = round(sl_price, p_dec)
        tp_final = round(tp_price, p_dec)

        if target_direction == "LONG":
            if sl_final >= entry_final:
                sl_final = round(entry_final - max(min_risk_dist, min_tick), p_dec)
            if tp_final <= entry_final:
                tp_final = round(entry_final + (2.0 * abs(entry_final - sl_final)), p_dec)
        else:  # SHORT
            if sl_final <= entry_final:
                sl_final = round(entry_final + max(min_risk_dist, min_tick), p_dec)
            if tp_final >= entry_final:
                tp_final = round(entry_final - (2.0 * abs(entry_final - sl_final)), p_dec)

        # Exact calculated Risk-Reward Ratio on quantized execution prices
        actual_risk = round(abs(entry_final - sl_final), p_dec)
        actual_reward = round(abs(tp_final - entry_final), p_dec)
        invalid_risk = (actual_risk <= 0.0)

        calc_rr = round(actual_reward / max(actual_risk, 1e-9), 2) if not invalid_risk else 0.0

        # Z.ai Proposed Upgrade 3.4: Effective R:R incorporating taker fees, spread, and slippage
        fee_taker = 0.0006  # 0.06% round-trip broker CFD commission
        spread_ratio = 0.0003  # 3 bps spread estimate
        slippage_est = 0.0005  # 5 bps slippage buffer
        cost_multi = (2 * fee_taker + 0.5 * spread_ratio + slippage_est) / max(actual_risk / ref_price, 1e-4)
        eff_rr = round(max(0.0, calc_rr - cost_multi), 2)

        # Z.ai Section 5.8: Quarter-Kelly position sizing fraction & Effective R:R gate
        z_platt = 4.0 * (confluence_score / 100.0) - 2.4
        p_win = 1.0 / (1.0 + math.exp(-z_platt))
        b_eff = max(eff_rr, 1.0)
        f_star = p_win - (1.0 - p_win) / b_eff
        kelly_fraction = round(0.25 * max(0.0, f_star), 4)

        # -------------------------------------------------------------------------
        # Pass Condition Checks (Enforcing Effective R:R >= 2.0)
        # -------------------------------------------------------------------------
        if invalid_risk:
            status = "PASS_INVALID_RISK"
        elif contradiction_veto:
            status = "PASS_CONTRADICTION"
        elif eff_rr < 2.0:
            status = "PASS_EFF_RR_BELOW_2"
        elif confluence_score < 75.0:
            status = "PASS_SUB_CONFLUENCE"
        else:
            status = "ACTIVE"

        # Timestamp generation with microsecond precision
        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc + timedelta(hours=5, minutes=30)
        now_ms = int(now_utc.timestamp() * 1000)
        now_us = int(now_utc.timestamp() * 1_000_000)

        candle_t_ms = int(candle_ctx.get("time_ms") or primary_trigger.get("time_ms") or now_ms)
        candle_t_ist = candle_ctx.get("time_ist") or primary_trigger.get("time_ist") or utc_ms_to_ist_str(candle_t_ms)

        # Serialized JSON strings for DuckDB / Polars schema stability (recent window snapshot)
        queue_snapshot_json = json.dumps(recent_window[-5:], ensure_ascii=True, allow_nan=False)
        metrics_payload = {
            "w1_footprint": w1_score,
            "w2_auction": w2_score,
            "w3_whales": w3_score,
            "w4_magnets": w4_score,
            "w5_tv_liquidations": w5_score,
            "raw_confluence": raw_confluence,
            "rdnl_confluence": rdnl_score,
            "market_regime": regime,
            "synergy_multiplier": m_interact,
            "active_couplings": active_couplings,
            "entropy": round(entropy, 4),
            "weighted_entropy": round(weighted_entropy, 4),
            "kappa_c": kappa_c,
            "effective_rr": eff_rr,
            "kelly_fraction": kelly_fraction,
            "matching_signals": matching_in_queue,
            "total_signals_in_queue": len(queue),
            "val": val,
            "vah": vah,
            "poc": poc,
            "vwap": vwap,
            "bid_wall_usd": bid_wall_usd,
            "ask_wall_usd": ask_wall_usd,
        }
        evaluation_metrics_json = json.dumps(metrics_payload, ensure_ascii=True, allow_nan=False)

        # Canonical Record conforming strictly to DuckDB read_ndjson invariants
        setup_record = {
            "setup_id": f"{symbol.upper()}_{now_us}",
            "symbol": symbol.upper(),
            "status": status,
            "direction": target_direction,
            "signal_count": len(queue),
            "primary_signal": str(p_code),
            "confluence_score": float(confluence_score),
            "w1_footprint": float(round(w1_score, 2)),
            "w2_auction": float(round(w2_score, 2)),
            "w3_whales": float(round(w3_score, 2)),
            "w4_magnets": float(round(w4_score, 2)),
            "w5_tv_liquidations": float(round(w5_score, 2)),
            "entry_price": float(entry_final),
            "stop_loss": float(sl_final),
            "take_profit": float(tp_final),
            "risk_reward_ratio": float(calc_rr),
            "risk_amount": float(round(abs(entry_final - sl_final), p_dec)),
            "reward_amount": float(round(abs(tp_final - entry_final), p_dec)),
            "atr_15m": float(round(atr, p_dec)),
            "auction_bias": str(auction_ctx.get("value_status") or "UNKNOWN"),
            "htf_trend": str(auction_ctx.get("mtf_bias") or "UNKNOWN"),
            "defending_wall_price": float(round(defending_wall_price, p_dec)) if defending_wall_price is not None else None,
            "magnet_target_price": float(round(target_magnet_price, p_dec)) if target_magnet_price is not None else None,
            "time_ms": candle_t_ms,
            "time_ist": candle_t_ist,
            "created_at_ms": now_ms,
            "created_at_us": now_us,
            "created_at_utc": now_utc.isoformat(),
            "created_at_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S.%f IST"),
            "effective_risk_reward_ratio": float(eff_rr),
            "calculated_lots": 0.0,
            "target_risk_usd": 0.0,
            "kelly_fraction": float(kelly_fraction),
            "concordance_kappa": float(kappa_c),
            "queue_snapshot_json": queue_snapshot_json,
            "evaluation_metrics_json": evaluation_metrics_json,
        }

        # Persist setup to dedicated per-asset ledger (deduplicated by symbol, direction, signal, and candle time)
        asset_ledger_path = SETUPS_DIR / f"{symbol.upper()}_setups.jsonl"
        is_duplicate = False
        if asset_ledger_path.exists():
            try:
                with open(asset_ledger_path, "r", encoding="utf-8") as f:
                    last_line = ""
                    for line in f:
                        line = line.strip()
                        if line:
                            last_line = line
                    if last_line:
                        last_rec = json.loads(last_line)
                        if (
                            last_rec.get("symbol") == symbol.upper()
                            and last_rec.get("direction") == target_direction
                            and last_rec.get("time_ms") == candle_t_ms
                            and last_rec.get("primary_signal") == str(p_code)
                        ):
                            is_duplicate = True
            except Exception:
                pass

        if not is_duplicate:
            atomic_append_jsonl(asset_ledger_path, setup_record)
            if status == "ACTIVE":
                master_is_dup = False
                if MASTER_SETUPS_PATH.exists():
                    try:
                        with open(MASTER_SETUPS_PATH, "r", encoding="utf-8", errors="replace") as mf:
                            for ml in mf:
                                ml = ml.strip()
                                if ml:
                                    mrec = json.loads(ml)
                                    if (
                                        mrec.get("symbol") == symbol.upper()
                                        and mrec.get("time_ms") == candle_t_ms
                                        and mrec.get("direction") == target_direction
                                        and mrec.get("primary_signal") == str(p_code)
                                    ):
                                        master_is_dup = True
                                        break
                    except Exception:
                        pass
                if not master_is_dup:
                    atomic_append_jsonl(MASTER_SETUPS_PATH, setup_record)
                logger.info(
                    f"🎯 [ACTIVE SETUP] {symbol} {target_direction} | Score: {confluence_score}/100 "
                    f"| Entry: {entry_final} | SL: {sl_final} | TP: {tp_final} | R:R: {calc_rr}"
                )
            else:
                logger.info(
                    f"[{symbol}] Setup evaluated: {status} | Score: {confluence_score}/100 | Direction: {target_direction}"
                )
        else:
            if status == "ACTIVE":
                logger.info(
                    f"🎯 [EXISTING ACTIVE SETUP] {symbol} {target_direction} | Score: {confluence_score}/100 "
                    f"| Entry: {entry_final} | SL: {sl_final} | TP: {tp_final} | R:R: {calc_rr}"
                )

        return setup_record


# ========================================================================================
# 3. High-Level Callback & Standalone Entrypoints
# ========================================================================================
def update_asset_queue_and_evaluate(
    symbol: str,
    new_signals: List[dict],
    current_price: Optional[float] = None
) -> Optional[dict]:
    """
    Main entrypoint called synchronously upon harvesting footprint signals.
    Permanently appends signals to the asset ledger, then runs Multi-Pillar Evaluator
    strictly if new signals were added.
    """
    signals, added = SignalQueueManager.append_signals(symbol, new_signals)
    if not signals or not added:
        return None

    return MultiPillarTradeEvaluator.evaluate_symbol(
        symbol=symbol,
        trigger_signals=added,
        queue=signals,
        current_price=current_price
    )


def evaluate_all_existing_queues(since_minutes: int = 30) -> List[dict]:
    """
    Scans all per-asset append ledgers in Database/Signals/Signal_Queues/ and evaluates new signals.
    Runs on the 0th, 15th, 30th, 45th minute candle close cron jobs.
    """
    results = []
    symbol_files = list(QUEUE_DIR.glob("*_signals.jsonl")) + list(QUEUE_DIR.glob("*_queue.json"))
    symbols = sorted({f.name.split("_")[0].upper() for f in symbol_files})
    if not symbols:
        logger.info("No signal ledgers found in Database/Signals/Signal_Queues/.")
        return []

    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(minutes=since_minutes)).timestamp() * 1000)
    logger.info(f"Evaluating {len(symbols)} asset ledgers for new signals (window: {since_minutes}m)...")

    active_count = 0
    for sym in symbols:
        try:
            signals = SignalQueueManager.load_all_signals(sym)
            if not signals:
                continue

            # Prioritize signals in the recent window, or fall back to latest signal
            recent_triggers = [s for s in signals if int(s.get("time_ms", 0)) >= cutoff_ms or int(s.get("arrived_at_ms", 0)) >= cutoff_ms]
            triggers = recent_triggers if recent_triggers else [signals[-1]]

            res = MultiPillarTradeEvaluator.evaluate_symbol(
                symbol=sym,
                trigger_signals=triggers,
                queue=signals
            )
            if res:
                results.append(res)
                if res.get("status") == "ACTIVE":
                    active_count += 1
                    logger.info(
                        f"🚨 ACTIVE TRADE SETUP: [{sym}] {res['direction']} | Score: {res['confluence_score']}/100 | "
                        f"Entry: {res['entry_price']} | SL: {res['stop_loss']} | TP: {res['take_profit']} | R:R: {res['risk_reward_ratio']}"
                    )
        except Exception as e:
            logger.error(f"[{sym}] Evaluation failed: {e}")

    logger.info(f"Evaluation complete: {len(results)} assets analyzed, {active_count} ACTIVE trade setup(s).")
    return results


@dataclass
class TrailingState:
    entry: float
    sl_current: float
    vwap_sigma1: float
    vwap_sigma2: float
    poc: float
    atr_15m: float
    direction: str = "LONG"
    high_water_mark: float = 0.0
    low_water_mark: float = float("inf")
    chandelier_mult: float = 2.5
    in_profit: bool = False


class TrailingStopManager:
    """
    Implements Hybrid Chandelier Volatility Anchor + Monotonic Ratchet.
    Hard anchor: HH_22 - 2.5 * ATR_14 (Long) / LL_22 + 2.5 * ATR_14 (Short).
    Monotonicity invariant: Long stops NEVER decrease; Short stops NEVER increase.
    """
    def update(self, state: TrailingState, price: float) -> TrailingState:
        is_long = str(state.direction).upper() in ("LONG", "BUY", "1")
        prev_sl = state.sl_current
        atr = max(state.atr_15m, 1e-6)

        if is_long:
            state.high_water_mark = max(state.high_water_mark, price, state.entry)
            candidates = [state.sl_current]

            # Tier 1: Breakeven Lock at +0.5 ATR
            if state.high_water_mark >= state.entry + 0.5 * atr:
                candidates.append(state.entry + 0.1 * atr)
                state.in_profit = True

            # Tier 2: Chandelier Volatility Ratchet (HWM - 2.5 * ATR) once in profit >= 1.0 ATR
            if state.high_water_mark >= state.entry + 1.0 * atr:
                candidates.append(state.high_water_mark - state.chandelier_mult * atr)
                state.in_profit = True

            # Tier 3: VWAP +1 sigma Lock
            if state.high_water_mark >= state.vwap_sigma1:
                candidates.append(state.vwap_sigma1)
                state.in_profit = True

            # Tier 4: Aggressive VWAP +2 sigma Lock
            if state.high_water_mark >= state.vwap_sigma2:
                candidates.append(max(state.vwap_sigma1, state.vwap_sigma2 - 0.5 * atr))
                state.in_profit = True

            # MONOTONIC RATCHET INVARIANT: SL must NEVER decrease for LONG
            new_sl = max(candidates)
            state.sl_current = max(prev_sl, round(new_sl, 5))
        else:
            state.low_water_mark = min(state.low_water_mark, price, state.entry)
            candidates = [state.sl_current]

            # Tier 1: Breakeven Lock at -0.5 ATR
            if state.low_water_mark <= state.entry - 0.5 * atr:
                candidates.append(state.entry - 0.1 * atr)
                state.in_profit = True

            # Tier 2: Chandelier Volatility Ratchet (LWM + 2.5 * ATR) once in profit >= 1.0 ATR
            if state.low_water_mark <= state.entry - 1.0 * atr:
                candidates.append(state.low_water_mark + state.chandelier_mult * atr)
                state.in_profit = True

            # Tier 3: VWAP -1 sigma Lock
            if state.low_water_mark <= state.vwap_sigma1:
                candidates.append(state.vwap_sigma1)
                state.in_profit = True

            # Tier 4: Aggressive VWAP -2 sigma Lock
            if state.low_water_mark <= state.vwap_sigma2:
                candidates.append(min(state.vwap_sigma1, state.vwap_sigma2 + 0.5 * atr))
                state.in_profit = True

            # MONOTONIC RATCHET INVARIANT: SL must NEVER increase for SHORT
            new_sl = min(candidates)
            state.sl_current = min(prev_sl, round(new_sl, 5))

        return state


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-Pillar Trade Evaluator & Append-Only Signal Store")
    parser.add_argument("--symbol", type=str, default="", help="Evaluate specific asset (e.g. BTC)")
    parser.add_argument("--all", action="store_true", help="Evaluate all existing asset ledgers")
    parser.add_argument("--window", type=int, default=30, help="Lookback window in minutes for new signals (default 30)")
    args = parser.parse_args()

    if args.symbol:
        sym = args.symbol.upper().strip()
        signals = SignalQueueManager.load_all_signals(sym)
        if not signals:
            print(f"No signals exist for {sym}.")
            return
        res = MultiPillarTradeEvaluator.evaluate_symbol(sym, [signals[-1]], signals)
        print(json.dumps(res, indent=2))
    elif args.all:
        evaluate_all_existing_queues(since_minutes=args.window)
    else:
        print("Usage: python trade_evaluator.py --symbol <SYM> OR --all")

if __name__ == "__main__":
    main()
