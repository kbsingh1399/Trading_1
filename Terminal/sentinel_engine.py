"""
Real-Time Sentinel: Universe Intersection, Cold-Start Seeding & Dual Zone/Event Latch
=====================================================================================
Governed by Arena Quant Audit (Round 2, DeepSeek Flash 4.1 & Qwen Verification):
1. Strict Normalized Universe Intersection & Broker Alias Mapping (F-08, AUD-08):
   Normalizes CFD symbols across casing, whitespace, and broker suffixes (.m, c, pro).
   Emits an observable ALARM if the resulting executable universe is empty.
2. Dual-Layer Identity Latching (F-03, AUD-01):
   - Layer 1: Persistent Absorption Zone Latch:
     hash(symbol_norm, sig_code, price_bucket, time_bucket) with 30-minute cooldown
     and 0.25% price bands to prevent re-entering the same absorption zone repeatedly.
   - Layer 2: Exact-Event Replay Guard:
     hash(symbol_norm, sig_code, price, time_ms) to reject identical event duplicates.
3. Two-Phase Latch Lifecycle (F-09):
   - evaluate_incoming_signal / reserve_signal: Reserves the zone during evaluation.
   - confirm_signal: Marks the latch as active once MT5 execution succeeds.
   - release_signal: Frees the reservation if execution fails (e.g. spread gate breach).
4. Bounded Memory & Thread Synchronization (F-12, F-18):
   - TTL-based LRU pruning of stale latches older than 24h.
   - Thread-safe mutation guarded by threading.Lock().
=====================================================================================
"""

import hashlib
import logging
import threading
from typing import Set, List, Dict, Any, Optional

logger = logging.getLogger("RealTimeSentinel")


def normalize_symbol_string(sym: str) -> str:
    """Normalizes symbol by trimming, uppercasing, and stripping common broker suffixes."""
    s = sym.strip().upper()
    for suffix in [".M", ".PRO", "C", "PRO"]:
        if s.endswith(suffix) and len(s) > len(suffix) + 2:
            s = s[:-len(suffix)]
            break
    return s


class RealTimeSentinel:
    def __init__(
        self,
        footprint_feeds: List[str],
        blueberry_mt5_map: List[str],
        zone_pct: float = 0.0025,
        cooldown_ms: int = 1800_000,
        ttl_ms: int = 86_400_000
    ):
        self.lock = threading.Lock()
        self.zone_pct = zone_pct
        self.cooldown_ms = cooldown_ms
        self.ttl_ms = ttl_ms

        # Build broker alias map
        self.alias_to_broker_symbol: Dict[str, str] = {}
        for b_sym in blueberry_mt5_map:
            b_norm = normalize_symbol_string(b_sym)
            self.alias_to_broker_symbol[b_norm] = b_sym
            self.alias_to_broker_symbol[b_sym.upper()] = b_sym

        self.footprint_feeds_normalized = {normalize_symbol_string(s) for s in footprint_feeds}
        self.blueberry_normalized = set(self.alias_to_broker_symbol.keys())

        # Strict intersection on normalized keys
        matched_bases = self.footprint_feeds_normalized.intersection(
            {normalize_symbol_string(b) for b in blueberry_mt5_map}
        )
        self.executable_universe = sorted(list(matched_bases))

        if not self.executable_universe:
            logger.error("ALARM: Executable universe intersection is EMPTY! No symbols match between Footprint and MT5.")

        # Latch storage: key -> timestamp_ms
        self.known_event_hashes: Dict[str, int] = {}
        self.known_zone_latches: Dict[str, int] = {}
        self.pending_reservations: Dict[str, int] = {}

    @property
    def known_signal_hashes(self) -> Set[str]:
        """Backward-compatible property returning set of known event hashes."""
        with self.lock:
            return set(self.known_event_hashes.keys())

    @staticmethod
    def compute_signal_latch_hash(symbol: str, sig_code: str, price: float, time_ms: int) -> str:
        """Deterministic 16-character SHA-256 exact event identity latch."""
        sym_norm = normalize_symbol_string(symbol)
        payload = f"{sym_norm}|{sig_code.upper()}|{price:.6f}|{int(time_ms)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def compute_zone_latch_key(self, symbol: str, sig_code: str, price: float, time_ms: int) -> str:
        """
        Computes quantized persistent absorption zone key.
        Groups prices into +/-0.25% bands and time into 30m cooldown buckets.
        """
        sym_norm = normalize_symbol_string(symbol)
        bucket_size = max(1e-4, price * self.zone_pct)
        price_bucket = int(round(price / bucket_size))
        time_bucket = int(time_ms // self.cooldown_ms)
        payload = f"{sym_norm}|{sig_code.upper()}|{price_bucket}|{time_bucket}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def _prune_stale_latches(self, current_time_ms: int):
        """Bounded memory eviction of entries older than TTL."""
        cutoff = current_time_ms - self.ttl_ms
        self.known_event_hashes = {k: v for k, v in self.known_event_hashes.items() if v >= cutoff}
        self.known_zone_latches = {k: v for k, v in self.known_zone_latches.items() if v >= cutoff}
        self.pending_reservations = {k: v for k, v in self.pending_reservations.items() if v >= cutoff}

    def load_known_signals(self, historical_signals: List[Dict[str, Any]]) -> int:
        """Cold-start seeding of historical signals and zones."""
        with self.lock:
            seeded = 0
            for sig in historical_signals:
                t_ms = int(sig.get("time_ms", 0))
                p = float(sig.get("price", 0.0))
                ev_h = self.compute_signal_latch_hash(sig["symbol"], sig["sig_code"], p, t_ms)
                zn_k = self.compute_zone_latch_key(sig["symbol"], sig["sig_code"], p, t_ms)
                if ev_h not in self.known_event_hashes:
                    self.known_event_hashes[ev_h] = t_ms
                    self.known_zone_latches[zn_k] = t_ms
                    seeded += 1
            return seeded

    def evaluate_incoming_signal(
        self,
        symbol: str,
        sig_code: str,
        price: float,
        time_ms: int,
        auto_reserve: bool = True
    ) -> Dict[str, Any]:
        """
        Evaluates an incoming signal against universe gating and dual-layer novelty latches.
        """
        sym_norm = normalize_symbol_string(symbol)

        with self.lock:
            self._prune_stale_latches(time_ms)

            # 1. Universe Gating
            if sym_norm not in self.executable_universe:
                return {
                    "accepted": False,
                    "reason": f"UNIVERSE_MISMATCH: {symbol} (norm: {sym_norm}) not in (Footprint ∩ Blueberry MT5)",
                    "latch_hash": None,
                    "zone_key": None,
                    "broker_symbol": None
                }

            broker_sym = self.alias_to_broker_symbol.get(sym_norm, symbol)
            event_hash = self.compute_signal_latch_hash(sym_norm, sig_code, price, time_ms)
            zone_key = self.compute_zone_latch_key(sym_norm, sig_code, price, time_ms)

            # 2. Exact Event Duplicate Check
            if event_hash in self.known_event_hashes:
                return {
                    "accepted": False,
                    "reason": f"DUPLICATE_LATCH_BLOCKED: event_hash={event_hash} already latched",
                    "latch_hash": event_hash,
                    "zone_key": zone_key,
                    "broker_symbol": broker_sym
                }

            # 3. Persistent Absorption Zone Re-entry Check (F-03, AUD-01)
            if zone_key in self.known_zone_latches:
                return {
                    "accepted": False,
                    "reason": f"PERSISTENT_ZONE_BLOCKED: zone_key={zone_key} already active in cooldown window",
                    "latch_hash": event_hash,
                    "zone_key": zone_key,
                    "broker_symbol": broker_sym
                }

            if auto_reserve:
                self.known_event_hashes[event_hash] = time_ms
                self.known_zone_latches[zone_key] = time_ms
                self.pending_reservations[event_hash] = time_ms

            return {
                "accepted": True,
                "reason": "NOVEL_SIGNAL_ACCEPTED",
                "latch_hash": event_hash,
                "zone_key": zone_key,
                "broker_symbol": broker_sym
            }

    def confirm_signal(self, latch_hash: str, zone_key: Optional[str] = None):
        """Two-phase commit: confirms signal execution."""
        with self.lock:
            if latch_hash in self.pending_reservations:
                del self.pending_reservations[latch_hash]

    def release_signal(self, latch_hash: str, zone_key: Optional[str] = None):
        """Two-phase rollback: releases reservation on downstream execution failure."""
        with self.lock:
            if latch_hash in self.known_event_hashes:
                del self.known_event_hashes[latch_hash]
            if zone_key and zone_key in self.known_zone_latches:
                del self.known_zone_latches[zone_key]
            if latch_hash in self.pending_reservations:
                del self.pending_reservations[latch_hash]
