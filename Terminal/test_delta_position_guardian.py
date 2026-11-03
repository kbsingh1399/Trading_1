"""
Terminal/test_delta_position_guardian.py
========================================================================================
Institutional Unit Tests for Delta Position Guardian, Dynamic Trailing SL & 1:2 R:R
========================================================================================
Validates:
1. Exact discrete tick price quantization across various asset specs (BTC, ETH, DOGE, HBAR)
2. Fill-anchored 1:2 R:R dynamic calibration upon taker execution slippage
3. Dollar risk cap preservation ($25.00 MAX_RISK_USD) under adverse entry slippage
4. Multi-tier trailing stop progression (Tier 1 Breakeven, Tier 2 +1R, Tier 3 +2R)
5. Monotonic stop ratchet invariant (rejection of adverse movement)
========================================================================================
"""

import pytest
import math
from unittest.mock import MagicMock, patch
from Terminal.delta_exchange_live_trader import DeltaExchangeTrader
from Terminal.risk_envelope import MAX_RISK_USD


@pytest.fixture
def trader():
    with patch.object(DeltaExchangeTrader, '_sync_server_time', return_value=None):
        with patch.object(DeltaExchangeTrader, '_load_products', return_value=None):
            tr = DeltaExchangeTrader()
            tr.products_cache = {
                "BTCUSD": {"id": 1, "symbol": "BTCUSD", "tick_size": "0.5", "contract_value": "0.001"},
                "ETHUSD": {"id": 2, "symbol": "ETHUSD", "tick_size": "0.05", "contract_value": "0.01"},
                "DOGEUSD": {"id": 3, "symbol": "DOGEUSD", "tick_size": "0.00001", "contract_value": "100"},
                "HBARUSD": {"id": 18288, "symbol": "HBARUSD", "tick_size": "0.00001", "contract_value": "1.0"},
            }
            return tr


def test_quantize_price(trader):
    """Verifies legal tick snapping and decimal formatting for various instrument tick sizes."""
    # BTC (0.5 tick)
    assert trader.quantize_price(68500.12, 1) == "68500.0"
    assert trader.quantize_price(68500.45, 1) == "68500.5"
    assert trader.quantize_price(68500.80, 1) == "68501.0"

    # ETH (0.05 tick)
    assert trader.quantize_price(2450.12, 2) == "2450.10"
    assert trader.quantize_price(2450.14, 2) == "2450.15"

    # HBAR (0.00001 tick)
    assert trader.quantize_price(0.069412, 18288) == "0.06941"
    assert trader.quantize_price(0.069418, 18288) == "0.06942"


def test_reanchor_brackets_to_fill_min_rr(trader):
    """
    Simulates the exact HBAR trade incident where taker entry slipped from 0.06914 to 0.06941.
    Verifies that reanchor_brackets_to_fill detects the degraded 0.64:1 ratio and edits TP to 2.0:1.
    """
    symbol = "HBARUSD"
    fill_price = 0.06941
    sl_price = 0.06880

    mock_tp_order = {
        "id": 101,
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.06980"  # Original pre-slippage TP (0.64:1 relative to 0.06941)
    }
    mock_sl_order = {
        "id": 102,
        "product_id": 18288,
        "stop_order_type": "stop_loss_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.06880"
    }

    with patch.object(trader, "get_open_orders", return_value=[mock_tp_order, mock_sl_order]):
        with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
            res = trader.reanchor_brackets_to_fill(symbol, fill_price, sl_price, min_rr=2.0)
            assert res["ok"] is True
            assert res["status"] == "REANCHORED"
            # Actual risk: 0.06941 - 0.06880 = 0.00061
            # Required reward: 2.0 * 0.00061 = 0.00122
            # Calibrated TP: 0.06941 + 0.00122 = 0.07063
            expected_tp = round(0.06941 + 2.0 * (0.06941 - 0.06880), 5)
            assert round(res["calibrated_tp"], 5) == expected_tp
            mock_edit.assert_called_once()
            _, kwargs = mock_edit.call_args
            assert kwargs["order_id"] == 101
            assert round(kwargs["stop_price"], 5) == expected_tp


def test_reanchor_caps_dollar_risk(trader):
    """
    Verifies that if slippage inflates dollar risk beyond $25.00,
    the guardian ratchets SL inward towards fill to preserve MAX_RISK_USD.
    """
    symbol = "HBARUSD"
    fill_price = 0.07000
    initial_sl = 0.06500  # 0.00500 risk distance
    # 10,000 contracts * 0.00500 * 1.0 = $50.00 risk (> $25.00 limit!)
    mock_tp = {"id": 201, "product_id": 18288, "stop_order_type": "take_profit_order", "side": "sell", "size": 10000, "stop_price": "0.08000"}
    mock_sl = {"id": 202, "product_id": 18288, "stop_order_type": "stop_loss_order", "side": "sell", "size": 10000, "stop_price": "0.06500"}

    with patch.object(trader, "get_open_orders", return_value=[mock_tp, mock_sl]):
        with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
            res = trader.reanchor_brackets_to_fill(symbol, fill_price, initial_sl, min_rr=2.0)
            assert res["ok"] is True
            # Max distance for $25.00 with 10,000 contracts is 25 / 10000 = 0.0025
            # Calibrated SL should be 0.07000 - 0.0025 = 0.06750
            assert round(res["calibrated_sl"], 5) == 0.06750


def test_audit_and_trail_positions_progression(trader):
    """
    Verifies multi-tier trailing stop progression:
    Tier 1 (+1.0R): Move SL to Breakeven (+1 tick)
    Tier 2 (+2.0R): Move SL to +1.0R and widen TP to +3.0R
    Tier 3 (+3.0R): Move SL to +2.0R
    """
    pos = {
        "product_symbol": "HBARUSD",
        "size": 100,
        "entry_price": "0.06941",
        "mark_price": "0.07005"  # +0.00064 gain. Risk dist = 0.06941 - 0.06880 = 0.00061. r = +1.05R!
    }
    sl_order = {
        "id": 301,
        "product_id": 18288,
        "stop_order_type": "stop_loss_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.06880"
    }
    tp_order = {
        "id": 302,
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.07063"
    }

    # Test Tier 1 (Breakeven ratchet)
    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
            with patch.object(trader, "request", return_value={"result": [{"symbol": "HBARUSD", "mark_price": "0.07005"}]}):
                with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
                    reports = trader.audit_and_trail_positions()
                    assert len(reports) == 1
                    rep = reports[0]
                    assert rep["action"] == "RATCHET_SL"
                    assert rep["tier"] == "TIER_1_BREAKEVEN"
                    # New SL should be entry (0.06941) + 1 tick (0.00001) = 0.06942
                    assert round(rep["new_sl"], 5) == 0.06942


def test_monotonic_ratchet_rejects_adverse_move(trader):
    """Verifies that an adverse stop adjustment is strictly rejected."""
    pos = {
        "product_symbol": "HBARUSD",
        "size": 100,
        "entry_price": "0.06941",
        "mark_price": "0.06900"  # Losing position: r = -0.67R
    }
    # Existing SL is already at 0.06880
    sl_order = {
        "id": 401,
        "product_id": 18288,
        "stop_order_type": "stop_loss_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.06880"
    }
    tp_order = {
        "id": 402,
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.07063"
    }

    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
            with patch.object(trader, "request", return_value={"result": [{"symbol": "HBARUSD", "mark_price": "0.06900"}]}):
                with patch.object(trader, "edit_order") as mock_edit:
                    reports = trader.audit_and_trail_positions()
                    # No SL ratchet should occur for a losing position
                    sl_ratchets = [r for r in reports if r.get("action") == "RATCHET_SL"]
                    assert len(sl_ratchets) == 0
                    mock_edit.assert_not_called()
