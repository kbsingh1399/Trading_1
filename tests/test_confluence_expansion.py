"""
tests/test_confluence_expansion.py
========================================================================================
Unit & Integration Tests for Pillars 6, 7, 8 and In-Flight Trade Lock Protection
========================================================================================
"""
import pytest
from pathlib import Path
import pandas as pd
import numpy as np

from trade_evaluator import MultiPillarTradeEvaluator, DB_DIR
from coinglass_common import rotate_warp_ip, FileMutex, LOCK_DIR


def test_load_oi_funding_context():
    """Verify Pillar 6 data loader parses oi_and_funding.parquet correctly."""
    ctx = MultiPillarTradeEvaluator.load_oi_funding_context("BTC")
    assert isinstance(ctx, dict)
    if ctx:  # If parquet file is populated
        assert "open_interest_usd" in ctx
        assert "funding_rate" in ctx
        assert "squeeze_bias" in ctx
        assert ctx["open_interest_usd"] >= 0


def test_load_sentiment_context():
    """Verify Pillar 7 data loader parses sentiment_divergence.parquet correctly."""
    ctx = MultiPillarTradeEvaluator.load_sentiment_context("BTC")
    assert isinstance(ctx, dict)
    if ctx:
        assert "top_long_pct" in ctx
        assert "global_long_pct" in ctx
        assert "divergence_spread" in ctx
        assert "sentiment_bias" in ctx


def test_load_heatmap_density_context():
    """Verify Pillar 8 data loader parses heatmap_density.parquet correctly."""
    ctx = MultiPillarTradeEvaluator.load_heatmap_density_context("BTC", 75000.0)
    assert isinstance(ctx, dict)
    if ctx:
        assert "asymmetry_ratio" in ctx
        assert "magnet_bias" in ctx
        assert "is_magnet_pool" in ctx


def test_inflight_trade_lock_prevents_ip_rotation():
    """Verify that active order execution strictly blocks Cloudflare WARP IP rotation."""
    inflight_lock_path = LOCK_DIR / "order_execution_inflight.lock"
    
    with FileMutex(str(inflight_lock_path), timeout=2.0) as guard:
        assert guard.acquired, "Could not acquire inflight trade lock"
        
        # Fail closed: callers must not interpret a blocked rotation as a successful IP change.
        import pytest
        with pytest.raises(RuntimeError, match="order execution in flight"):
            rotate_warp_ip(context="UnitTest", reason="CHALLENGE", force=True)
