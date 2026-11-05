"""
Unit tests for Alpha Monetization & Profit Maximization Enhancements:
1. Underwater Pocket Support Floor / Overhead Supply Defense (+8 pts in Pillar 5)
2. Whale Bid/Ask Fortress Dominance (+5 pts)
3. Wyckoff Spring / Upthrust boundary absorption (+6 pts)
4. Liquidation Squeeze Ignition sweet-spot (+6 pts)
5. Adaptive Bps-Bounded IOC limit buffer
6. Tier 1.5 Trailing Stop Ratchet (+0.50R0 lock at +1.50R0 gain)
"""

import math
import pytest
from unittest.mock import MagicMock, patch
from Terminal.realtime_signal_evaluator import (
    RealtimeSignalEvaluator,
    detect_market_pillar_hits,
)
from Terminal.delta_exchange_live_trader import DeltaExchangeTrader


@pytest.fixture(autouse=True)
def isolate_anchors_file(tmp_path):
    """Ensures unit tests never overwrite or unlink production live_position_anchors.json."""
    test_anchor_file = tmp_path / "live_position_anchors.json"
    with patch("Terminal.position_anchors.ANCHORS_FILE", test_anchor_file):
        yield test_anchor_file


def test_underwater_pocket_support_floor_defense_p5():
    """BUY setup with an underwater SUPPORT_FLOOR pocket within -2.5% to -0.15%
    earns a +8 pts boost and actively triggers P5_WHALE_SWEEP in the pillar gate."""
    pockets = [{
        "pocket_type": "SUPPORT_FLOOR",
        "side": "SHORT",
        "pocket_price": 99.0,
        "dist_pct": -1.0,
        "trapped_notional_usd": 1_500_000.0,
        "trapped_musd": 1.5,
        "whale_count": 3,
        "pocket_score": 1e6
    }]
    hits = detect_market_pillar_hits(
        direction="BUY",
        whale_imbalance=0.50,
        cvd_slope="NEUTRAL",
        entry_pockets=pockets,
    )
    assert "P5_WHALE_SWEEP" in hits
    assert hits["P5_WHALE_SWEEP"] == 8.0


def test_underwater_pocket_overhead_supply_defense_p5():
    """SELL setup with an underwater OVERHEAD_SUPPLY pocket within +0.15% to +2.5%
    earns a +8 pts boost and actively triggers P5_WHALE_SWEEP in the pillar gate."""
    pockets = [{
        "pocket_type": "OVERHEAD_SUPPLY",
        "side": "LONG",
        "pocket_price": 101.0,
        "dist_pct": 1.0,
        "trapped_notional_usd": 1_200_000.0,
        "trapped_musd": 1.2,
        "whale_count": 2,
        "pocket_score": 8e5
    }]
    hits = detect_market_pillar_hits(
        direction="SELL",
        whale_imbalance=0.50,
        cvd_slope="NEUTRAL",
        entry_pockets=pockets,
    )
    assert "P5_WHALE_SWEEP" in hits
    assert hits["P5_WHALE_SWEEP"] == 8.0


def test_wyckoff_spring_absorption_evaluator():
    """Bullish CVD Divergence occurring at Value Area Low (VAL) earns Wyckoff Spring boost."""
    ev = object.__new__(RealtimeSignalEvaluator)
    ev.olap = MagicMock()
    ev.olap.query_whale_walls.return_value = {"data_valid": True, "whale_imbalance_bid_ratio": 0.55}
    ev.olap.query_liquidation_confluence.return_value = {"data_valid": True, "near_field_shorts_pct": 55.0}
    ev.olap.query_cvd_trend.return_value = {"data_valid": True, "cvd_slope": "BULLISH_DIVERGENCE"}
    ev.olap.query_key_liquidation_levels.return_value = {}
    ev.olap.query_auction_profile.return_value = {
        "data_valid": True, "day_vah": 105.0, "day_val": 100.0, "day_vwap": 102.5,
        "mtf_bias": "HTF_NEUTRAL", "htf_1h_trend": "NEUTRAL", "htf_4h_trend": "NEUTRAL",
        "value_status": "BELOW_VAL", "price_vs_vwap": "BELOW_VWAP",
        "ema34": 0.0, "ema55": 0.0, "ema59": 0.0, "ema89": 0.0, "ema233": 0.0,
    }
    ev.olap.get_forecast_frame.return_value = MagicMock(empty=True)
    ev.regime_det = MagicMock()
    ev.regime_det.analyze_symbol.return_value = {"regime": "MEAN_REVERTING_RANGE", "atr_pct": 1.0}
    ev.vector_mem = None
    ev.timesfm = MagicMock(is_available=False)
    ev.critic_engine = MagicMock()
    ev.critic_engine.check_microstructure_knife_veto.return_value = (False, "", {})
    ev.critic_engine.audit_candidate_setup.return_value = {"critic_verdict": "CLEAR", "max_similarity": 0.0, "penalty": 0.0}
    ev.query_live_tv_liquidations = lambda s: {}
    ev._finite = lambda val, d: float(val) if val is not None and math.isfinite(float(val)) else d
    ev._persist_alert = MagicMock()
    ev.hyperdash = None

    sig = {
        "symbol": "BTC", "mt5_symbol": "BTCUSD", "sig_name": "Absorption Bids",
        "side": "buy", "price": 99.5, "bar_closed": True, "time_ms": 1700000000000,
        "datetime_utc": "2026-11-04T00:00:00Z"
    }

    res = ev.evaluate_signal(sig)
    reasons = " | ".join(res.get("confluence_reasons", []))
    assert "Wyckoff Spring: Bullish CVD Divergence Absorbed at Value Discount" in reasons


def test_tier_1_5_trailing_ratchet():
    """Position progressing to +1.55R0 locks in +0.50R0 guaranteed profit (Tier 1.5)."""
    trader = DeltaExchangeTrader()
    # HBAR: entry 0.06941, SL 0.06880 -> r0 = 0.00061.
    # At mark 0.07036, gain = (0.07036 - 0.06941) = +0.00095 -> r_gain = +1.557R0!
    pos = {
        "product_symbol": "HBARUSD",
        "size": 100,
        "entry_price": "0.06941",
        "mark_price": "0.07036"
    }
    # Current SL is already at BE (Tier 1, 0.06942)
    sl_order = {
        "id": 501,
        "product_id": 18288,
        "stop_order_type": "stop_loss_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.06942"
    }
    tp_order = {
        "id": 502,
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.07063"
    }

    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
            with patch.object(trader, "request", return_value={"result": [{"symbol": "HBARUSD", "mark_price": "0.07036"}]}):
                with patch("Terminal.delta_exchange_live_trader.get_or_recover_anchor", return_value={
                    "initial_risk_dist": 0.00061,
                    "current_tier": 1,
                    "last_ratchet_price": 0.06942,
                    "initial_sl": 0.06880
                }):
                    with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
                        reports = trader._audit_and_trail_positions_guarded()
                        assert len(reports) == 1
                        rep = reports[0]
                        assert rep["action"] == "RATCHET_SL"
                        assert rep["tier"] == "TIER_1_5_PLUS_HALF_R_LOCK"
                        # Target SL is entry (0.06941) + 0.5 * 0.00061 = 0.069715
                        assert rep["new_sl"] >= 0.06970
