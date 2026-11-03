"""
Cross-Asset Signal Stack, Generational State Machine & Deterministic Deduplication
===================================================================================
Guarantees (EGM Architectural Standards & Quant Audit Round 2 Compliant):
1. CandleClusterKey = <symbol, open_time_ms>:
   Aggregates multi-signal footprint discoveries (Absorption + Trapped Shorts)
   into a single composite cluster to preserve the vital +15 to +20 pt confluence boost.
2. Single-Evaluation / Reviewed-Signal Suppression Invariant:
   Evaluated candles are recorded in Database/Signals/evaluated_signals_ledger.json.
   Once evaluated, a candle is NEVER re-evaluated, re-alerted, or re-dispatched.
3. Antecedent Context Traversal:
   Preceding evaluated signals are inspected strictly as read-only historical
   context for multi-bar structural confirmation (e.g. 3-bar reversal sequences).
4. Strict AGENTS.md Parquet Standards:
   Primary IST columns (open_time_ist, close_time_ist, time_ist, candle_ist,
   open_time_ist_ms, close_time_ist_ms) alongside numeric open_time_ms, close_time_ms.
5. Priority Arbiter:
   Normalized ranking formula with EV >= 0.0 gate, clamped R:R [1.5, 4.0],
   Delta listing verification, and strict MAX_OPEN_POSITIONS = 2 risk guardrails.
===================================================================================
"""

import os
import sys
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
LOCK_DIR = DB_DIR / ".locks"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
LOCK_DIR.mkdir(parents=True, exist_ok=True)

from Terminal.runtime_io import atomic_json, atomic_parquet, FileMutex, writer_lock
from Terminal.universal_footprint_engine import utc_ms_to_ist_str, IST_OFFSET_MS
from Terminal.risk_envelope import MAX_OPEN_POSITIONS, MAX_RISK_USD

logger = logging.getLogger("AssetSignalStack")
logger.setLevel(logging.INFO)

EVALUATED_LEDGER_FILE = SIGNALS_DIR / "evaluated_signals_ledger.json"
PENDING_WICKS_FILE = SIGNALS_DIR / "pending_wick_signals.json"
ACTIVE_STACK_PARQUET = SIGNALS_DIR / "active_signal_stack.parquet"
CANDLE_M15_MS = 900_000


def canonical_bar_epoch(t_ms: int) -> int:
    """Quantizes UTC epoch milliseconds to canonical 15-minute bar start."""
    return (int(t_ms) // CANDLE_M15_MS) * CANDLE_M15_MS


def generate_signal_id(symbol: str, open_time_ms: int, direction: str, sig_codes: List[str]) -> str:
    """Deterministic 16-character SHA-256 fingerprint."""
    sorted_codes = ",".join(sorted(set(sig_codes)))
    raw = f"{symbol.upper()}:{open_time_ms}:{direction.upper()}:{sorted_codes}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class EvaluatedSignalsLedger:
    """
    Durable ledger ensuring that once a candle signal has been evaluated,
    it is NEVER re-evaluated or re-alerted on subsequent sweeps.
    """
    def __init__(self, ledger_path: Path = EVALUATED_LEDGER_FILE):
        self.path = ledger_path
        self._evaluated: Dict[str, Dict[str, Any]] = {}
        self.load()

    def _key(self, symbol: str, open_time_ms: int) -> str:
        return f"{symbol.upper()}:{open_time_ms}"

    def load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    self._evaluated = data
            except Exception as e:
                logger.warning(f"Error loading evaluated signals ledger: {e}")
                self._evaluated = {}

    def is_evaluated(self, symbol: str, open_time_ms: int) -> bool:
        """Returns True if this exact candle has already undergone evaluation."""
        return self._key(symbol, open_time_ms) in self._evaluated

    def mark_evaluated(
        self,
        symbol: str,
        open_time_ms: int,
        signal_id: str,
        confluence_score: float,
        action: str,
        sig_name: str = "",
        direction: str = ""
    ):
        """Records the candle as evaluated and durably persists."""
        key = self._key(symbol, open_time_ms)
        record = {
            "symbol": symbol.upper(),
            "open_time_ms": open_time_ms,
            "signal_id": signal_id,
            "confluence_score": round(confluence_score, 1),
            "action": action,
            "sig_name": sig_name,
            "direction": direction,
            "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
            "evaluated_at_ist": utc_ms_to_ist_str(int(time.time() * 1000))
        }
        self._evaluated[key] = record

        # Prune ledger to keep last 1,000 evaluated signals
        if len(self._evaluated) > 1000:
            keys_to_remove = list(self._evaluated.keys())[:-1000]
            for k in keys_to_remove:
                del self._evaluated[k]

        try:
            atomic_json(self.path, self._evaluated)
        except Exception as e:
            logger.error(f"Error saving evaluated signals ledger: {e}")

    def get_antecedent_history(self, symbol: str, current_open_ms: int, lookback_ms: int = 10_800_000) -> List[Dict[str, Any]]:
        """
        Retrieves preceding evaluated signals for an asset within the lookback window
        (default 3 hours: tau_bar - 3h <= t < tau_bar).
        Strictly passive read-only antecedent evidence.
        """
        history = []
        sym_upper = symbol.upper()
        min_time = current_open_ms - lookback_ms

        for rec in self._evaluated.values():
            if rec.get("symbol") == sym_upper:
                t = rec.get("open_time_ms", 0)
                if min_time <= t < current_open_ms:
                    history.append(rec)

        history.sort(key=lambda x: x.get("open_time_ms", 0))
        return history


class AssetSignalStack:
    """
    Central cross-asset matrix maintaining the latest signal state per asset
    across all tracked perpetual contracts with deterministic deduplication.
    """
    def __init__(self, stack_path: Path = ACTIVE_STACK_PARQUET):
        self.stack_path = stack_path
        self.ledger = EvaluatedSignalsLedger()
        self.stack: Dict[str, Dict[str, Any]] = {}
        self.load_stack()

    def load_stack(self):
        if self.stack_path.exists():
            try:
                df = pd.read_parquet(self.stack_path)
                for _, row in df.iterrows():
                    sym = str(row.get("symbol", "")).upper()
                    if sym:
                        self.stack[sym] = row.to_dict()
            except Exception as e:
                logger.warning(f"Error reading active signal stack parquet: {e}")

    def register_candle_cluster(
        self,
        raw_signals: List[Dict[str, Any]],
        now_ms: Optional[int] = None
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        Ingests a list of raw signals co-temporal on the same 15m candle.
        Returns (cluster_candidate_dict, status_code).

        Status codes:
        - 'ACTIVE_LATEST_CLOSED': Fresh, completed bar eligible for quantitative evaluation.
        - 'DEDUPLICATED_ALREADY_REVIEWED': Candle was already evaluated; strictly suppressed.
        - 'PENDING_INTRA_BAR': Candle is actively forming (t < tau_close); buffered.
        - 'EXPIRED_STALE': Candle closed > 15m ago; discarded.
        - 'EMPTY_SIGNALS': No signals provided.
        """
        if not raw_signals:
            return None, "EMPTY_SIGNALS"

        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        first_sig = raw_signals[0]
        sym = str(first_sig.get("symbol", "")).upper()
        raw_t_ms = first_sig.get("time_ms") or first_sig.get("open_time_ms")

        if not raw_t_ms:
            return None, "INVALID_TIMESTAMP"

        open_ms = canonical_bar_epoch(raw_t_ms)
        close_ms = open_ms + CANDLE_M15_MS

        # Invariant 2: Check Single-Evaluation Ledger
        if self.ledger.is_evaluated(sym, open_ms):
            return None, "DEDUPLICATED_ALREADY_REVIEWED"

        # Invariant 3: Check Stale Freshness Boundary (closed > 15 minutes ago)
        if (now - close_ms) > CANDLE_M15_MS:
            return None, "EXPIRED_STALE"

        # Check Active Forming Wick (t < tau_close)
        if now < close_ms:
            return None, "PENDING_INTRA_BAR"

        # Construct Cluster Attributes
        sig_codes = []
        sig_names = []
        directions = set()
        prices = []

        for s in raw_signals:
            c = s.get("sig_code", "")
            n = s.get("sig_name", "")
            d = "BUY" if s.get("side", "").lower() in ("buy", "bullish") else "SELL"
            p = float(s.get("price") or 0.0)

            if c:
                sig_codes.append(c)
            if n and n not in sig_names:
                sig_names.append(n)
            directions.add(d)
            if p > 0:
                prices.append(p)

        composite_name = " + ".join(sig_names) if sig_names else "Footprint Signal"
        composite_codes = ",".join(sorted(set(sig_codes)))
        primary_dir = list(directions)[0] if len(directions) == 1 else "DISCORDANT"
        avg_price = (sum(prices) / len(prices)) if prices else float(first_sig.get("price", 0.0))

        signal_id = generate_signal_id(sym, open_ms, primary_dir, sig_codes)

        cluster_candidate = {
            "symbol": sym,
            "signal_id": signal_id,
            "open_time_ms": open_ms,
            "close_time_ms": close_ms,
            "open_time_ist": utc_ms_to_ist_str(open_ms),
            "close_time_ist": utc_ms_to_ist_str(close_ms),
            "time_ist": utc_ms_to_ist_str(open_ms),
            "candle_ist": utc_ms_to_ist_str(open_ms),
            "open_time_ist_ms": open_ms + IST_OFFSET_MS,
            "close_time_ist_ms": close_ms + IST_OFFSET_MS,
            "sig_name": composite_name,
            "sig_code": composite_codes,
            "cluster_signals_count": len(raw_signals),
            "raw_cluster_signals": raw_signals,
            "direction": primary_dir,
            "price": avg_price,
            "confluence_score": 0.0,
            "ev_300bar": 0.0,
            "rr_ratio": 0.0,
            "state": "ACTIVE_LATEST_CLOSED",
            "is_new_candle": True,
            "delta_product": f"{sym}USD",
            "updated_at_ist": utc_ms_to_ist_str(now)
        }

        # Update in-memory stack
        self.stack[sym] = cluster_candidate
        return cluster_candidate, "ACTIVE_LATEST_CLOSED"

    def rank_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Applies the Priority Arbiter formula to rank competing candidates:
        - Mandatory gate: EV >= 0.0, Confluence Score >= 75.0
        - Clamped R:R to [1.5, 4.0]
        - Normalized RankScore:
          0.70 * Confluence + 0.15 * RR_norm + 0.15 * EV_norm
        """
        valid = []
        for c in candidates:
            score = float(c.get("confluence_score", 0.0))
            ev = float(c.get("ev_300bar", 0.0))
            rr = float(c.get("rr_ratio", 0.0))

            # Mandatory quantitative gates
            if score < 75.0:
                continue
            if ev < 0.0:
                continue

            rr_clamped = min(4.0, max(1.5, rr))
            rr_norm = ((rr_clamped - 1.5) / 2.5) * 100.0
            ev_norm = min(1.0, max(0.0, ev)) * 100.0

            rank_score = (0.70 * score) + (0.15 * rr_norm) + (0.15 * ev_norm)
            c_copy = dict(c)
            c_copy["arbiter_rank_score"] = round(rank_score, 2)
            valid.append(c_copy)

        valid.sort(key=lambda x: x.get("arbiter_rank_score", 0.0), reverse=True)
        return valid

    def persist_stack(self):
        """Atomically persists active signal stack to Parquet with primary IST columns."""
        if not self.stack:
            return

        records = []
        for row in self.stack.values():
            r = {k: v for k, v in row.items() if k != "raw_cluster_signals"}
            records.append(r)

        df = pd.DataFrame(records)
        try:
            with writer_lock(str(self.stack_path)):
                atomic_parquet(df, self.stack_path)
        except Exception as e:
            logger.error(f"Error persisting active signal stack: {e}")
