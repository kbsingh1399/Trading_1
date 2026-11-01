"""
Real-Time Sentinel: Universe Intersection, Cold-Start Seeding & Signal Identity Latch
=====================================================================================
Governed by Arena Quant Audit (Round 2 & Google Argon Specifications):
1. Strict Universe Intersection: Intersects live footprint feeds with Blueberry MT5 broker map.
   Prevents tracking assets with no valid broker market specifications.
2. Signal Identity Hashing: Stable hash(symbol, sig_code, price, time_ms) latch.
   Prevents re-entering the same persistent 30-minute absorption zone 15 times.
3. Cold-Start Seeding: Seeds active historical signals on startup without firing execution orders.
=====================================================================================
"""

import hashlib
from typing import Set, List, Dict, Any, Optional


class RealTimeSentinel:
    def __init__(self, footprint_feeds: List[str], blueberry_mt5_map: List[str]):
        self.footprint_feeds = set(footprint_feeds)
        self.blueberry_mt5_map = set(blueberry_mt5_map)
        # Strict intersection of live footprint feeds and Blueberry MT5 broker map
        self.executable_universe = sorted(list(self.footprint_feeds.intersection(self.blueberry_mt5_map)))
        self.known_signal_hashes: Set[str] = set()

    @staticmethod
    def compute_signal_latch_hash(symbol: str, sig_code: str, price: float, time_ms: int) -> str:
        """Deterministic 16-character SHA-256 identity latch."""
        payload = f"{symbol.upper()}|{sig_code.upper()}|{price:.6f}|{int(time_ms)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def load_known_signals(self, historical_signals: List[Dict[str, Any]]) -> int:
        """Cold-start seeding prevents execution burst on daemon restart."""
        seeded = 0
        for sig in historical_signals:
            h = self.compute_signal_latch_hash(
                sig["symbol"], sig["sig_code"], float(sig["price"]), int(sig["time_ms"])
            )
            if h not in self.known_signal_hashes:
                self.known_signal_hashes.add(h)
                seeded += 1
        return seeded

    def evaluate_incoming_signal(self, symbol: str, sig_code: str, price: float, time_ms: int) -> Dict[str, Any]:
        """
        Evaluates an incoming signal against universe gating and novelty latches.
        """
        sym_clean = symbol.upper()
        if sym_clean not in self.executable_universe:
            return {
                "accepted": False,
                "reason": f"UNIVERSE_MISMATCH: {sym_clean} not in (Footprint ∩ Blueberry MT5)",
                "latch_hash": None
            }

        latch_hash = self.compute_signal_latch_hash(sym_clean, sig_code, price, time_ms)
        if latch_hash in self.known_signal_hashes:
            return {
                "accepted": False,
                "reason": f"DUPLICATE_LATCH_BLOCKED: hash={latch_hash} already latched",
                "latch_hash": latch_hash
            }

        self.known_signal_hashes.add(latch_hash)
        return {
            "accepted": True,
            "reason": "NOVEL_SIGNAL_ACCEPTED",
            "latch_hash": latch_hash
        }
