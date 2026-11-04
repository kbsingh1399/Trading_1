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
6. Naked position auto-recovery (synthesis of protective SL when unhedged)
7. Orphaned ghost order cancellation upon position closure
8. SHORT position trailing stop progression
========================================================================================
"""

import pytest
import math
from unittest.mock import MagicMock, patch
from Terminal.delta_exchange_live_trader import DeltaExchangeTrader
from Terminal.risk_envelope import MAX_RISK_USD


@pytest.fixture(autouse=True)
def isolate_anchors_file(tmp_path):
    """Ensures unit tests never overwrite production anchors or contend on live daemon locks."""
    test_anchor_file = tmp_path / "live_position_anchors.json"
    test_lock = tmp_path / "delta_position_management.lock"
    with patch("Terminal.position_anchors.ANCHORS_FILE", test_anchor_file):
        with patch("Terminal.delta_exchange_live_trader.DELTA_MGMT_LOCK_FILE", test_lock):
            yield test_anchor_file


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
                "SOLUSD": {"id": 4, "symbol": "SOLUSD", "tick_size": "0.25", "contract_value": "1.0"},
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

    # SOL (0.25 tick - fractional tick check)
    assert trader.quantize_price(185.25, 4) == "185.25"
    assert trader.quantize_price(185.10, 4) == "185.00"
    assert trader.quantize_price(185.40, 4) == "185.50"


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
            expected_tp = round(0.06941 + 2.0 * (0.06941 - 0.06880), 5)
            assert round(res["calibrated_tp"], 5) == expected_tp
            mock_edit.assert_called_once()
            _, kwargs = mock_edit.call_args
            assert kwargs["order_id"] == 101
            assert round(kwargs["stop_price"], 5) == expected_tp


def test_reanchor_caps_dollar_risk(trader):
    """
    Verifies that if slippage inflates dollar risk beyond $25.00,
    the guardian does NOT inward-compress SL into noise, but sheds excess contracts
    via reduce-only market order to fit MAX_RISK_USD.
    """
    symbol = "HBARUSD"
    fill_price = 0.07000
    initial_sl = 0.06500  # 0.00500 risk distance -> $50 risk for 10000 contracts
    mock_tp = {"id": 201, "product_id": 18288, "stop_order_type": "take_profit_order", "side": "sell", "size": 10000, "stop_price": "0.08000"}
    mock_sl = {"id": 202, "product_id": 18288, "stop_order_type": "stop_loss_order", "side": "sell", "size": 10000, "stop_price": "0.06500"}

    with patch.object(trader, "get_open_orders", return_value=[mock_tp, mock_sl]):
        with patch.object(trader, "request", return_value={"success": True, "result": {"id": 777}}) as mock_req:
            with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
                res = trader.reanchor_brackets_to_fill(symbol, fill_price, initial_sl, min_rr=2.0)
                assert res["ok"] is True
                # SL remains uncompressed at 0.06500
                assert round(res["calibrated_sl"], 5) == 0.06500
                # Excess 5000 contracts shed to fit $25 cap (10000 -> 5000)
                assert res["size"] == 5000
                mock_req.assert_called_once()
                _, req_kwargs = mock_req.call_args
                assert req_kwargs["body"]["size"] == 5000
                assert req_kwargs["body"]["reduce_only"] is True


def test_reanchor_fails_closed_when_min_lot_exceeds_cap(trader):
    """
    Verifies that if slippage causes even 1 contract to exceed the $25 cap,
    the guardian triggers close_position fail-closed rather than holding unhedged/fragile risk.
    """
    symbol = "HBARUSD"
    fill_price = 35.0
    initial_sl = 5.0  # 30.00 risk distance -> 1 contract = $30 risk > $25 cap
    mock_tp = {"id": 201, "product_id": 18288, "stop_order_type": "take_profit_order", "side": "sell", "size": 1, "stop_price": "95.0"}
    mock_sl = {"id": 202, "product_id": 18288, "stop_order_type": "stop_loss_order", "side": "sell", "size": 1, "stop_price": "5.0"}

    with patch.object(trader, "get_open_orders", return_value=[mock_tp, mock_sl]):
        with patch.object(trader, "close_position") as mock_close, patch.object(trader, "cancel_order") as mock_cancel:
            res = trader.reanchor_brackets_to_fill(symbol, fill_price, initial_sl, min_rr=2.0)
            assert res["ok"] is False
            assert res["status"] == "SLIPPAGE_EXCEEDS_SINGLE_CONTRACT_CAP"
            assert res.get("position_closed") is True
            mock_close.assert_called_once_with(symbol)
            mock_cancel.assert_any_call(202, 18288)
            mock_cancel.assert_any_call(201, 18288)


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
                    assert round(rep["new_sl"], 5) == 0.06942


def test_monotonic_ratchet_rejects_adverse_move(trader):
    """Verifies that an adverse stop adjustment is strictly rejected."""
    pos = {
        "product_symbol": "HBARUSD",
        "size": 100,
        "entry_price": "0.06941",
        "mark_price": "0.06900"  # Losing position: r = -0.67R
    }
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
                    sl_ratchets = [r for r in reports if r.get("action") == "RATCHET_SL"]
                    assert len(sl_ratchets) == 0
                    mock_edit.assert_not_called()


def test_r0_stability_after_breakeven(trader):
    """
    Verifies that once SL is moved to Breakeven (+1 tick), R0 remains anchored
    to the initial 0.00061 risk distance, preventing denominator collapse.
    """
    from Terminal.position_anchors import register_position_anchor
    register_position_anchor(
        symbol='HBARUSD',
        product_id=18288,
        direction='BUY',
        entry_price=0.06941,
        initial_sl=0.06880,
        initial_tp=0.07063,
        size=100,
        initial_risk_dist=0.00061
    )

    pos = {'product_symbol': 'HBARUSD', 'size': 100, 'entry_price': '0.06941', 'mark_price': '0.07005'}
    sl_order = {'id': 501, 'product_id': 18288, 'stop_order_type': 'stop_loss_order', 'side': 'sell', 'size': 100, 'stop_price': '0.06942'}
    tp_order = {'id': 502, 'product_id': 18288, 'stop_order_type': 'take_profit_order', 'side': 'sell', 'size': 100, 'stop_price': '0.07063'}

    with patch.object(trader, 'get_active_positions', return_value=[pos]):
        with patch.object(trader, 'get_open_orders', return_value=[sl_order, tp_order]):
            with patch.object(trader, 'request', return_value={'result': [{'symbol': 'HBARUSD', 'mark_price': '0.07005'}]}):
                with patch.object(trader, 'edit_order') as mock_edit:
                    reports = trader.audit_and_trail_positions()
                    mock_edit.assert_not_called()


def test_cold_start_recovery_from_tp(trader):
    """
    Verifies that on a cold start without prior anchor records,
    R0 is recovered via TP inverse calculation (|TP - Entry| / 2.0).
    """
    from Terminal.position_anchors import get_or_recover_anchor

    anchor = get_or_recover_anchor(
        symbol='HBARUSD',
        product_id=18288,
        direction='BUY',
        entry_price=0.06941,
        cur_sl_price=0.06942,  # Already at Breakeven
        cur_tp_price=0.07063,  # 2.0:1 TP
        size=100
    )
    # Recovered R0 should be (0.07063 - 0.06941) / 2 = 0.00061
    assert round(anchor['initial_risk_dist'], 5) == 0.00061


def test_naked_position_auto_recovery(trader):
    """
    Verifies that when an active position exists on Delta Exchange without
    an SL order, audit_and_trail_positions() synthesizes and posts a protective SL order.
    """
    pos = {
        "product_symbol": "HBARUSD",
        "size": 100,
        "entry_price": "0.06941",
        "mark_price": "0.06950"
    }
    tp_order = {
        "id": 601,
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.07063"
    }

    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[tp_order]):
            with patch.object(trader, "request") as mock_req:
                mock_req.side_effect = [
                    {"result": [{"symbol": "HBARUSD", "mark_price": "0.06950"}]},  # tickers
                    {"success": True, "result": {"id": 999123}}  # place order response
                ]
                reports = trader.audit_and_trail_positions()
                assert len(reports) >= 1
                rec_rep = next((r for r in reports if r.get("action") == "RECOVERED_NAKED_SL"), None)
                assert rec_rep is not None
                assert rec_rep["order_id"] == 999123


def test_orphaned_orders_cancelled_when_position_closed(trader):
    """
    Verifies that when a position is closed (size=0), any resting conditional
    orders for that symbol are cancelled to prevent reverse ghost fills.
    """
    active_positions = []
    orphaned_order = {
        "id": 701,
        "product_symbol": "HBARUSD",
        "product_id": 18288,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 100,
        "stop_price": "0.07063"
    }

    with patch.object(trader, "get_active_positions", return_value=active_positions):
        with patch.object(trader, "get_open_orders", return_value=[orphaned_order]):
            with patch.object(trader, "cancel_order", return_value={"success": True}) as mock_cancel:
                trader.audit_and_trail_positions()
                mock_cancel.assert_called_once_with(701, 18288)


def test_short_position_trailing_ratchet(trader):
    """
    Verifies that SHORT positions correctly ratchet SL downwards towards profit.
    """
    pos = {
        "product_symbol": "ARUSD",
        "size": -1,
        "entry_price": "3.1330",
        "mark_price": "3.0700"
    }
    trader.products_cache["ARUSD"] = {"id": 21523, "symbol": "ARUSD", "tick_size": "0.001", "contract_value": "1.0"}
    sl_order = {
        "id": 801,
        "product_id": 21523,
        "stop_order_type": "stop_loss_order",
        "side": "buy",
        "size": 1,
        "stop_price": "3.1914"
    }
    tp_order = {
        "id": 802,
        "product_id": 21523,
        "stop_order_type": "take_profit_order",
        "side": "buy",
        "size": 1,
        "stop_price": "3.0160"
    }

    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
            with patch.object(trader, "request", return_value={"result": [{"symbol": "ARUSD", "mark_price": "3.0700"}]}):
                with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
                    reports = trader.audit_and_trail_positions()
                    assert len(reports) >= 1
                    rep = next(r for r in reports if r.get("action") == "RATCHET_SL")
                    assert rep["tier"] == "TIER_1_BREAKEVEN"
                    assert round(rep["new_sl"], 4) == 3.1320


def test_exact_symbol_match_prevents_substring_collision(trader):
    """
    Verifies that 'AR' does NOT match 'NEARUSD' in has_open_position or close_position.
    """
    pos = {
        "product_symbol": "NEARUSD",
        "size": 10,
        "entry_price": "5.50",
        "mark_price": "5.60"
    }
    with patch.object(trader, "get_active_positions", return_value=[pos]):
        # "AR" must return False because only NEAR is open
        assert trader.has_open_position("AR") is False
        assert trader.has_open_position("ARUSD") is False
        # "NEAR" must return True
        assert trader.has_open_position("NEAR") is True
        assert trader.has_open_position("NEARUSD") is True

        # Closing "AR" must fail closed with NOT_FOUND and not touch NEARUSD
        with patch.object(trader, "request") as mock_req:
            res = trader.close_position("AR")
            assert res.get("status") == "NOT_FOUND"
            mock_req.assert_not_called()


def test_multi_collateral_margin_floor_rejection(trader):
    """
    Verifies that execute_delta_signal fails closed when available balance / balance < 20%.
    """
    signal = {
        "symbol": "BTCUSD",
        "direction": "BUY",
        "confluence_score": 85.0,
        "trade_parameters": {
            "entry_price": 80000.0,
            "stop_loss": 79000.0,
            "take_profit_1": 82000.0
        }
    }
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    # Mock wallet with 10% available buffer (< 20% universal floor)
    depleted_wallets = [
        {"asset_symbol": "USD", "balance": "100.0", "available_balance": "10.0"},
        {"asset_symbol": "INR", "balance": "1000.0", "available_balance": "800.0"}
    ]
    with patch("Terminal.delta_exchange_live_trader.LIVE_DISPATCH_ENABLED", True), patch.object(trader, "get_wallet_balances", return_value=depleted_wallets):
        with patch.object(trader, "get_active_positions", return_value=[]):
            res = trader.execute_delta_signal(signal)
            assert res.get("ok") is False
            assert res.get("status") == "REJECTED"
            assert "insufficient_margin_buffer" in res.get("reason", "").lower()


def test_immediate_stop_penetration_triggers_emergency_close(trader):
    """
    Verifies that if mark price breaches existing SL, an immediate emergency close is issued.
    """
    pos = {
        "product_symbol": "BTCUSD",
        "size": 1,
        "entry_price": "80000.0",
        "mark_price": "78500.0"  # Breached below SL of 79000.0 for BUY
    }
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    sl_order = {
        "id": 101,
        "product_id": 1,
        "stop_order_type": "stop_loss_order",
        "side": "sell",
        "size": 1,
        "stop_price": "79000.0"
    }
    tp_order = {
        "id": 102,
        "product_id": 1,
        "stop_order_type": "take_profit_order",
        "side": "sell",
        "size": 1,
        "stop_price": "82000.0"
    }

    with patch.object(trader, "get_active_positions", return_value=[pos]):
        with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
            with patch.object(trader, "request", return_value={"result": [{"symbol": "BTCUSD", "mark_price": "78500.0"}]}):
                with patch.object(trader, "close_position", return_value={"ok": True, "result": {"id": 999}}) as mock_close:
                    reports = trader.audit_and_trail_positions()
                    assert len(reports) >= 1
                    rep = next(r for r in reports if r.get("action") == "EMERGENCY_CLOSE_STOP_PENETRATED")
                    assert rep["symbol"] == "BTCUSD"
                    assert rep["mark_price"] == 78500.0
                    mock_close.assert_called_once_with("BTCUSD")


def test_failed_reanchor_triggers_emergency_close(trader):
    """
    Verifies that if post-fill bracket reanchoring fails, the position is immediately closed.
    """
    signal = {
        "symbol": "BTCUSD",
        "direction": "BUY",
        "confluence_score": 85.0,
        "trade_parameters": {
            "entry_price": 80000.0,
            "stop_loss": 79000.0,
            "take_profit_1": 82000.0
        }
    }
    healthy_wallets = [
        # PATCH P2 wallet upgraded to institutional scale: the dynamic survival
        # budget (hard floor $4,775) now correctly REJECTS a $100 mock account.
        # This test exercises the reanchor/emergency-close path, so the wallet
        # must be an admissible one ($5,000 balance, $4,800 available).
        {"asset_symbol": "USD", "balance": "5000.0", "available_balance": "4800.0"}
    ]
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}

    order_success_response = {
        "success": True,
        "result": {
            "id": 12345,
            "product_id": 1,
            "average_fill_price": "80050.0"
        }
    }

    live_ticker = {"success": True, "result": {"mark_price": "80000.0", "best_bid": "79990.0", "best_ask": "80010.0"}}
    with patch("Terminal.delta_exchange_live_trader.LIVE_DISPATCH_ENABLED", True), patch.object(trader, "get_wallet_balances", return_value=healthy_wallets), patch.object(trader, "request", return_value=live_ticker):
        with patch.object(trader, "get_active_positions", return_value=[]):
            with patch.object(trader, "place_order", return_value=order_success_response):
                # Re-anchor fails because bracket orders could not be located
                with patch.object(trader, "reanchor_brackets_to_fill", return_value={"ok": False, "status": "NOT_FOUND", "reason": "TP_ORDER_NOT_LOCATED"}):
                    with patch.object(trader, "close_position") as mock_close:
                        res = trader.execute_delta_signal(signal)
                        assert res.get("ok") is False
                        assert res.get("status") == "REJECTED_FAILED_BRACKET_ANCHOR"
                        mock_close.assert_called_once_with("BTCUSD")


def test_execute_delta_signal_handles_pre_closed_position_without_duplicate_close(trader):
    """
    Verifies that when reanchor_brackets_to_fill already closed the position (position_closed=True),
    execute_delta_signal does not invoke close_position a second time and confirms flattening.
    """
    signal = {
        "symbol": "BTCUSD",
        "direction": "BUY",
        "confluence_score": 85.0,
        "trade_parameters": {
            "entry_price": 80000.0,
            "stop_loss": 79000.0,
            "take_profit_1": 82000.0
        }
    }
    healthy_wallets = [{"asset_symbol": "USD", "balance": "5000.0", "available_balance": "4800.0"}]
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    order_success = {"success": True, "result": {"id": 12345, "product_id": 1, "average_fill_price": "80050.0"}}
    live_ticker = {"success": True, "result": {"mark_price": "80000.0", "best_bid": "79990.0", "best_ask": "80010.0"}}

    with patch("Terminal.delta_exchange_live_trader.LIVE_DISPATCH_ENABLED", True), \
         patch.object(trader, "get_wallet_balances", return_value=healthy_wallets), \
         patch.object(trader, "request", return_value=live_ticker), \
         patch.object(trader, "get_active_positions", return_value=[]), \
         patch.object(trader, "place_order", return_value=order_success):
        # reanchor_brackets_to_fill already emergency-closed position on single contract cap breach
        with patch.object(trader, "reanchor_brackets_to_fill", return_value={
            "ok": False,
            "status": "SLIPPAGE_EXCEEDS_SINGLE_CONTRACT_CAP",
            "position_closed": True,
            "close_res": {"ok": True, "result": {"id": 999}}
        }):
            with patch.object(trader, "close_position") as mock_close:
                res = trader.execute_delta_signal(signal)
                assert res.get("ok") is False
                assert res.get("status") == "REJECTED_FAILED_BRACKET_ANCHOR"
                assert res.get("flatten_confirmed") is True
                # Redundant second close must NOT have been called
                mock_close.assert_not_called()


def test_sl_edit_failure_returns_false_and_does_not_claim_reanchored(trader):
    """
    Verifies that if SL order edit fails during reanchoring, reanchor_brackets_to_fill
    fails closed (ok=False) and does NOT report REANCHORED.
    """
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    open_orders = [
        {"id": 101, "product_id": 1, "stop_order_type": "take_profit_order", "side": "sell", "size": 10, "stop_price": "82000.0"},
        {"id": 102, "product_id": 1, "stop_order_type": "stop_loss_order", "side": "sell", "size": 10, "stop_price": "70000.0"},
    ]
    with patch.object(trader, "get_open_orders", return_value=open_orders):
        with patch.object(trader, "request", return_value={"success": True, "result": {"id": 888}}):
            # Mock edit_order on SL to fail
            with patch.object(trader, "edit_order", return_value={"success": False, "error": "ORDER_LOCKED"}):
                res = trader.reanchor_brackets_to_fill(
                    symbol="BTCUSD",
                    fill_price=80000.0,
                    sl_price=70000.0,
                    min_rr=2.0
                )
                assert res.get("ok") is False
                assert res.get("status") == "SL_EDIT_FAILED"
                assert res.get("status") != "REANCHORED"


def test_fx_normalized_collateral_margin(trader):
    """
    Verifies that INR balances are not nominally summed with USD without FX conversion.
    """
    mixed_wallets = [
        {"asset_symbol": "INR", "balance": "87000.0", "available_balance": "87000.0"},
        {"asset_symbol": "USD", "balance": "100.0", "available_balance": "80.0"},
    ]
    with patch.object(trader, "get_wallet_balances", return_value=mixed_wallets):
        summary = trader.get_wallet_balances_normalized_usd()
        assert summary["total_balance_usd"] == pytest.approx(1100.0, rel=1e-2)
        assert summary["total_available_usd"] == pytest.approx(1080.0, rel=1e-2)
        assert trader.get_available_margin_usd() < 2000.0


def test_execute_delta_signal_handles_cancelled_ioc_without_emergency_close(trader):
    """
    Verifies that when an IOC limit order expires cancelled without execution,
    execute_delta_signal returns status='IOC_UNFILLED' and does not attempt emergency close.
    """
    signal = {
        "symbol": "BTCUSD",
        "direction": "BUY",
        "confluence_score": 85.0,
        "trade_parameters": {
            "entry_price": 80000.0,
            "stop_loss": 79000.0,
            "take_profit_1": 82000.0
        }
    }
    healthy_wallets = [{"asset_symbol": "USD", "balance": "5000.0", "available_balance": "4800.0"}]
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    cancelled_ioc_response = {
        "success": True,
        "result": {
            "id": 54321,
            "product_id": 1,
            "state": "cancelled",
            "size": 5,
            "unfilled_size": 5,
            "average_fill_price": None
        }
    }
    live_ticker = {"success": True, "result": {"mark_price": "80000.0", "best_bid": "79990.0", "best_ask": "80010.0"}}

    with patch("Terminal.delta_exchange_live_trader.LIVE_DISPATCH_ENABLED", True), \
         patch.object(trader, "get_wallet_balances", return_value=healthy_wallets), \
         patch.object(trader, "request", return_value=live_ticker), \
         patch.object(trader, "get_active_positions", return_value=[]), \
         patch.object(trader, "place_order", return_value=cancelled_ioc_response), \
         patch.object(trader, "get_fills", return_value=[]), \
         patch.object(trader, "close_position") as mock_close:
        res = trader.execute_delta_signal(signal)
        assert res.get("ok") is False
        assert res.get("status") == "IOC_UNFILLED"
        # Emergency close must NOT have been called
        mock_close.assert_not_called()


def test_reanchor_brackets_cancels_residual_orders_on_edit_failure(trader):
    """
    Verifies that if SL order edit fails during reanchoring, cancel_order is called
    to ensure zero orphaned orders remain on the exchange.
    """
    trader.products_cache["BTCUSD"] = {"id": 1, "symbol": "BTCUSD", "tick_size": "0.1", "contract_value": "0.001"}
    open_orders = [
        {"id": 101, "product_id": 1, "stop_order_type": "take_profit_order", "side": "sell", "size": 10, "stop_price": "82000.0"},
        {"id": 102, "product_id": 1, "stop_order_type": "stop_loss_order", "side": "sell", "size": 10, "stop_price": "70000.0"},
    ]
    with patch.object(trader, "get_open_orders", return_value=open_orders), \
         patch.object(trader, "request", return_value={"success": True, "result": {"id": 888}}), \
         patch.object(trader, "edit_order", return_value={"success": False, "error": "ORDER_LOCKED"}), \
         patch.object(trader, "cancel_order") as mock_cancel:
        res = trader.reanchor_brackets_to_fill(
            symbol="BTCUSD",
            fill_price=80000.0,
            sl_price=70000.0,
            min_rr=2.0
        )
        assert res.get("ok") is False
        assert res.get("status") == "SL_EDIT_FAILED"
        mock_cancel.assert_any_call(102, 1)
        mock_cancel.assert_any_call(101, 1)

def test_environment_aware_product_resolution_demo_vs_prod(trader):
    """
    Verifies that _load_products and resolve_product correctly handle demo vs production
    endpoints and allow resolving by both symbol and numeric product_id.
    """
    trader.products_cache = {
        "BTCUSD": {"id": 84, "symbol": "BTCUSD", "tick_size": 0.1, "contract_value": 0.001, "underlying": "BTC"}
    }
    # Resolve by symbol
    prod_sym = trader.resolve_product("BTCUSD")
    assert prod_sym is not None
    assert prod_sym["id"] == 84

    # Resolve by numeric product ID
    prod_id_int = trader.resolve_product(84)
    assert prod_id_int is not None
    assert prod_id_int["symbol"] == "BTCUSD"

    prod_id_str = trader.resolve_product("84")
    assert prod_id_str is not None
    assert prod_id_str["symbol"] == "BTCUSD"


def test_audit_prioritizes_pos_product_id_and_prevents_mismatched_anchor(trader):
    """
    Verifies that audit_and_trail_positions uses pos.get('product_id') directly
    and correctly matches resting bracket orders with identical product_id.
    """
    from Terminal.position_anchors import load_position_anchors

    active_pos = [{
        "product_symbol": "BTCUSD",
        "product_id": 84,
        "size": 1,
        "entry_price": 80000.0,
        "mark_price": 80200.0,
        "product": {"id": 84, "contract_value": "0.001", "tick_size": "0.1"}
    }]
    open_orders = [
        {"id": 201, "product_id": 84, "stop_order_type": "stop_loss_order", "stop_price": "79000.0", "side": "sell", "size": 1},
        {"id": 202, "product_id": 84, "stop_order_type": "take_profit_order", "stop_price": "82000.0", "side": "sell", "size": 1}
    ]

    with patch.object(trader, "get_active_positions", return_value=active_pos), \
         patch.object(trader, "get_open_orders", return_value=open_orders), \
         patch.object(trader, "request", return_value={"result": [{"symbol": "BTCUSD", "mark_price": 80200.0}]}):
        reports = trader.audit_and_trail_positions()
        # No emergency closes or missing bracket warnings
        assert not any("EMERGENCY_CLOSE" in r.get("action", "") for r in reports)
        anchors = load_position_anchors()
        assert "BTCUSD" in anchors
        assert anchors["BTCUSD"]["product_id"] == 84
        assert anchors["BTCUSD"]["initial_sl"] == 79000.0

