"""
Unit Tests for Real-Time Sentinel Engine (Google Argon & Arena Verification)
============================================================================
Verifies:
1. Strict universe intersection: only symbols in both Footprint and Blueberry MT5 are admitted.
2. Cold-start seeding: historical signals are loaded into known hashes without triggering entries.
3. Signal identity latching: duplicate signals with identical (symbol, sig_code, price, time_ms) are blocked.
4. Novelty acceptance: genuinely new signals are accepted and immediately latched.
"""

import pytest
from Terminal.sentinel_engine import RealTimeSentinel


def test_sentinel_universe_intersection():
    footprint = ["BTC", "ETH", "SOL", "DOGE", "UNLISTED_TOKEN"]
    broker_map = ["BTC", "ETH", "SOL", "DOGE", "XAUUSD"]

    sentinel = RealTimeSentinel(footprint_feeds=footprint, blueberry_mt5_map=broker_map)
    # Expected intersection: BTC, DOGE, ETH, SOL
    assert sentinel.executable_universe == ["BTC", "DOGE", "ETH", "SOL"]

    # Candidate not in broker map must be rejected
    res = sentinel.evaluate_incoming_signal("UNLISTED_TOKEN", "abs", 1.25, 1700000000000)
    assert res["accepted"] is False
    assert "UNIVERSE_MISMATCH" in res["reason"]


def test_sentinel_cold_start_seeding_and_duplicate_latching():
    footprint = ["BTC", "ETH"]
    broker_map = ["BTC", "ETH"]
    sentinel = RealTimeSentinel(footprint_feeds=footprint, blueberry_mt5_map=broker_map)

    historical = [
        {"symbol": "BTC", "sig_code": "abs", "price": 68500.0, "time_ms": 1700000000000},
        {"symbol": "ETH", "sig_code": "trap", "price": 3450.0, "time_ms": 1700000005000}
    ]
    seeded = sentinel.load_known_signals(historical)
    assert seeded == 2

    # Re-evaluating an identical seeded signal must be blocked by duplicate latch
    res = sentinel.evaluate_incoming_signal("BTC", "abs", 68500.0, 1700000000000)
    assert res["accepted"] is False
    assert "DUPLICATE_LATCH_BLOCKED" in res["reason"]


def test_sentinel_novel_signal_acceptance():
    footprint = ["BTC", "ETH"]
    broker_map = ["BTC", "ETH"]
    sentinel = RealTimeSentinel(footprint_feeds=footprint, blueberry_mt5_map=broker_map)

    # Genuinely new signal
    res = sentinel.evaluate_incoming_signal("BTC", "div", 68600.0, 1700000100000)
    assert res["accepted"] is True
    assert res["reason"] == "NOVEL_SIGNAL_ACCEPTED"
    assert res["latch_hash"] is not None

    # Immediate second evaluation must now be blocked
    res2 = sentinel.evaluate_incoming_signal("BTC", "div", 68600.0, 1700000100000)
    assert res2["accepted"] is False
    assert "DUPLICATE_LATCH_BLOCKED" in res2["reason"]
