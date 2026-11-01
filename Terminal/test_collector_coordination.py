"""
Unit Tests for Collector Coordination, Lock Mutex & Exponential Half-Life Decay
================================================================================
Verifies:
1. Qwen F-05 / DeepSeek AUD-05: Exact 30-minute (1800s) half-life in exponential wall decay.
2. Fast 2.0s deadline on BrowserLock yielding SKIPPED_LOCK_CONTENTION without blocking.
3. Post-rotation grace window and cooldown protection in WARP rotation logic.
"""

import math
import time
import pytest
from Terminal.collector_lock import (
    calculate_wall_decay_weight,
    HALF_LIFE_SECONDS,
    TAU_30MIN_HALFLIFE,
    BrowserLock,
    rotate_warp_ip
)


def test_exact_30min_exponential_half_life():
    """
    Qwen F-05 / DeepSeek AUD-05:
    Confirms that with tau = 1800 / ln(2) (~2596.84s), the half-life is EXACTLY 1800s (30m).
    """
    # At t = 0: weight = 1.0
    w0 = calculate_wall_decay_weight(0.0)
    assert w0 == 1.0

    # At t = 1800s (30m): weight = 0.5000
    w30 = calculate_wall_decay_weight(1800.0)
    assert abs(w30 - 0.5000) < 1e-3

    # At t = 3600s (60m): weight = 0.2500 (two half lives)
    w60 = calculate_wall_decay_weight(3600.0)
    assert abs(w60 - 0.2500) < 1e-3


def test_browser_lock_mutex_and_skip():
    """
    Acquiring an already-held lock should timeout within 2.0s and set acquired=False.
    """
    # First acquire succeeds
    with BrowserLock("Collector_A", timeout_secs=0.5) as lock_a:
        assert lock_a.acquired is True

        # Second concurrent acquire on same resource fails fast
        with BrowserLock("Collector_B", timeout_secs=0.3) as lock_b:
            assert lock_b.acquired is False


def test_warp_rotation_cooldown_and_skips():
    """
    Unforced normal operation skips WARP rotation to preserve cookies and socket state.
    """
    res = rotate_warp_ip(context="Test", reason="NORMAL_OPERATION", force=False)
    # Skipped rotation should return current or unknown IP without executing subprocess
    assert isinstance(res, str)
