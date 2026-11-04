"""
Terminal/test_swarm_synthesis_verification.py
========================================================================================
Institutional Unit Tests for Adversarial Swarm Synthesis Patches:
1. Short-Side Exit Commission Risk Preservation (solve_fee_aware_grid)
2. Inverted Stop Loss Protection on Fill Re-anchoring (reanchor_brackets_to_fill)
3. Pre-emptive Trailing SL / TP Widening Ratchet (+1.8R / +2.8R)
4. Atomic Stream Appending & Torn-Line Resistance (atomic_append_jsonl & read_new_raw_signals)
5. Resilient Evaluated Signals Ledger against Corrupted Rows (EvaluatedSignalsLedger)
6. Non-Blocking FileMutex WARP Rotation Check (is_warp_rotating)
========================================================================================
"""

import json
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from Terminal.delta_exchange_live_trader import DeltaExchangeTrader, solve_fee_aware_grid
from Terminal.runtime_io import atomic_append_jsonl
from Terminal.asset_signal_stack import EvaluatedSignalsLedger
from Terminal.collector_lock import is_warp_rotating, LOCK_DIR
from Terminal.risk_envelope import MAX_RISK_USD


@pytest.fixture(autouse=True)
def isolate_anchors_file(tmp_path):
    test_anchor_file = tmp_path / "live_position_anchors.json"
    test_lock = tmp_path / "delta_position_management.lock"
    with patch("Terminal.position_anchors.ANCHORS_FILE", test_anchor_file):
        with patch("Terminal.delta_exchange_live_trader.DELTA_MGMT_LOCK_FILE", test_lock):
            yield test_anchor_file


def test_short_side_exit_commission_in_solve_fee_aware_grid():
    """
    On short positions, exit notional (at SL) > entry notional.
    Verifies that solve_fee_aware_grid accurately prices taker fees on BOTH entry and SL exit.
    """
    entry = 100.0
    sl_long = 90.0   # Long: risk dist = 10.0, exit at 90.0
    sl_short = 110.0 # Short: risk dist = 10.0, exit at 110.0
    cval = 1.0
    fee_rate = 0.0005 # 5 bps

    res_long = solve_fee_aware_grid(entry, sl_long, cval, taker_fee_rate=fee_rate, cap_usd=25.0)
    res_short = solve_fee_aware_grid(entry, sl_short, cval, taker_fee_rate=fee_rate, cap_usd=25.0)

    assert res_long["ok"] is True
    assert res_short["ok"] is True

    # Long commission per contract: 0.0005 * (100 + 90) = 0.095
    assert abs(res_long["commission_per_contract"] - (fee_rate * (100.0 + 90.0))) < 1e-6
    # Short commission per contract: 0.0005 * (100 + 110) = 0.105
    assert abs(res_short["commission_per_contract"] - (fee_rate * (100.0 + 110.0))) < 1e-6
    # Short commission is strictly higher because exit notional is higher
    assert res_short["commission_per_contract"] > res_long["commission_per_contract"]
    # Total risk per contract includes directional fees
    assert res_short["loss_per_contract"] > res_long["loss_per_contract"]


def test_inverted_stop_loss_fails_closed():
    """
    Verifies that if fill price slips past the stop loss, reanchor_brackets_to_fill
    detects the inverted stop loss and immediately fails closed with close_position.
    """
    with patch.object(DeltaExchangeTrader, '_sync_server_time', return_value=None):
        with patch.object(DeltaExchangeTrader, '_load_products', return_value=None):
            trader = DeltaExchangeTrader()
            trader.products_cache = {
                "BTCUSD": {"id": 1, "symbol": "BTCUSD", "tick_size": "0.5", "contract_value": "0.001"}
            }

            # BUY position: Fill price slipped below SL (e.g., fill at 79,000 but SL was 80,000)
            mock_tp = {"id": 101, "product_id": 1, "stop_order_type": "take_profit_order", "side": "sell", "size": 1, "stop_price": "85000"}
            mock_sl = {"id": 102, "product_id": 1, "stop_order_type": "stop_loss_order", "side": "sell", "size": 1, "stop_price": "80000"}

            with patch.object(trader, "get_open_orders", return_value=[mock_tp, mock_sl]):
                with patch.object(trader, "close_position", return_value={"success": True}) as mock_close:
                    with patch.object(trader, "cancel_order", return_value={"success": True}) as mock_cancel:
                        res = trader.reanchor_brackets_to_fill("BTCUSD", fill_price=79000.0, sl_price=80000.0)
                        assert res["ok"] is False
                        assert res["status"] == "INVERTED_STOP_LOSS"
                        assert res.get("position_closed") is True
                        mock_close.assert_called_once_with("BTCUSD")
                        mock_cancel.assert_any_call(102, 1)
                        mock_cancel.assert_any_call(101, 1)


def test_preemptive_trailing_ratchet_at_plus_1_8r():
    """
    Verifies that Tier 2 trailing ratchet triggers at +1.8R to pre-emptively widen TP to +3R
    before Delta's matching engine touches the resting +2R limit order.
    """
    with patch.object(DeltaExchangeTrader, '_sync_server_time', return_value=None):
        with patch.object(DeltaExchangeTrader, '_load_products', return_value=None):
            trader = DeltaExchangeTrader()
            trader.products_cache = {
                "BTCUSD": {"id": 1, "symbol": "BTCUSD", "tick_size": "0.5", "contract_value": "0.001"}
            }

            # Entry 80,000, Initial SL 79,000 (R0 = 1,000), Initial TP 82,000 (+2R)
            # Mark price at 81,850 (+1.85R gain)
            pos = {
                "product_symbol": "BTCUSD",
                "size": 1,
                "entry_price": "80000.0",
                "mark_price": "81850.0"
            }
            sl_order = {
                "id": 201,
                "product_id": 1,
                "stop_order_type": "stop_loss_order",
                "side": "sell",
                "size": 1,
                "stop_price": "80000.5" # currently at breakeven
            }
            tp_order = {
                "id": 202,
                "product_id": 1,
                "stop_order_type": "take_profit_order",
                "side": "sell",
                "size": 1,
                "stop_price": "82000.0"
            }

            from Terminal.position_anchors import register_position_anchor
            register_position_anchor(
                symbol="BTCUSD",
                product_id=1,
                direction="BUY",
                entry_price=80000.0,
                initial_sl=79000.0,
                initial_tp=82000.0,
                size=1,
                initial_risk_dist=1000.0,
                current_tier=1 # at Tier 1 (breakeven)
            )

            with patch.object(trader, "get_active_positions", return_value=[pos]):
                with patch.object(trader, "get_open_orders", return_value=[sl_order, tp_order]):
                    with patch.object(trader, "request", return_value={"result": [{"symbol": "BTCUSD", "mark_price": "81850.0"}]}):
                        with patch.object(trader, "edit_order", return_value={"success": True}) as mock_edit:
                            reports = trader.audit_and_trail_positions()
                            assert len(reports) >= 1
                            ratchet = next((r for r in reports if r.get("action") == "RATCHET_SL"), None)
                            assert ratchet is not None
                            assert ratchet["tier"] == "TIER_2_PLUS_1R_LOCK"
                            # SL ratcheted to +1.0R0 = 80,000 + 1,000 = 81,000
                            assert ratchet["new_sl"] == 81000.0
                            # TP widened to +3.0R0 = 80,000 + 3,000 = 83,000
                            mock_edit.assert_any_call(order_id=202, product_id=1, size=1, stop_price=83000.0)


def test_atomic_append_jsonl_and_torn_line_recovery(tmp_path):
    """
    Verifies that atomic_append_jsonl writes durable, valid JSONL records,
    and that reading tolerates incomplete/torn lines without desyncing.
    """
    jsonl_file = tmp_path / "test_stream.jsonl"
    
    # 1. Append valid records
    recs = [{"id": 1, "symbol": "BTC"}, {"id": 2, "symbol": "ETH"}]
    atomic_append_jsonl(jsonl_file, recs)
    
    assert jsonl_file.exists()
    lines = jsonl_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["symbol"] == "BTC"
    assert json.loads(lines[1])["symbol"] == "ETH"

    # 2. Simulate torn line (writer interrupted before newline)
    with open(jsonl_file, "ab") as f:
        f.write(b'{"id": 3, "symbol": "SO') # incomplete line without newline
        f.flush()

    # 3. Reading with torn-line guard should stop BEFORE the incomplete line
    import importlib
    wake_mod = importlib.import_module("5_wake_on_footprint_signal")
    FootprintSignalSentinel = wake_mod.FootprintSignalSentinel
    sentinel = FootprintSignalSentinel(from_current_end=False)
    with patch.object(wake_mod, "FOOTPRINT_SIGNALS_JSONL", jsonl_file):
        sentinel.state = {"offset": 0}
        signals = sentinel.read_new_raw_signals()
        assert len(signals) == 2
        assert signals[0]["symbol"] == "BTC"
        assert signals[1]["symbol"] == "ETH"
        # The offset must NOT advance past the torn line
        offset_before_torn = sentinel._pending_offset

        # 4. Now writer finishes writing the line
        with open(jsonl_file, "ab") as f:
            f.write(b'L", "price": 150.0}\n')
            f.flush()

        sentinel.state = {"offset": offset_before_torn}
        new_signals = sentinel.read_new_raw_signals()
        assert len(new_signals) == 1
        assert new_signals[0]["symbol"] == "SOL"


def test_evaluated_signals_ledger_corrupted_row_resilience(tmp_path):
    """
    Verifies that if evaluated_signals_ledger.jsonl contains a corrupted or broken line,
    the ledger recovers all other intact rows rather than wiping history.
    """
    ledger_file = tmp_path / "evaluated_signals_ledger.jsonl"
    rec1 = {"symbol": "BTCUSD", "open_time_ms": 1700000000000, "bar_type": "time", "action": "DISPATCH"}
    rec2 = {"symbol": "ETHUSD", "open_time_ms": 1700000900000, "bar_type": "time", "action": "FILTER"}
    
    with open(ledger_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(rec1) + "\n")
        f.write("{CORRUPTED_JSON_LINE\n") # Malformed JSON
        f.write(json.dumps(rec2) + "\n")

    ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)
    assert ledger.is_evaluated("BTCUSD", 1700000000000) is True
    assert ledger.is_evaluated("ETHUSD", 1700000900000) is True
    assert ledger.is_evaluated("SOLUSD", 1700000000000) is False


def test_is_warp_rotating_non_blocking(tmp_path):
    """
    Verifies that is_warp_rotating returns False when lock is idle,
    and returns True only when another process actively holds the mutex.
    """
    test_lock = tmp_path / "warp_execution.lock"
    with patch("Terminal.collector_lock.LOCK_DIR", tmp_path):
        assert is_warp_rotating() is False

        from Terminal.runtime_io import FileMutex
        with FileMutex(str(test_lock), timeout=0.1) as held_mutex:
            assert held_mutex.acquired is True
            # Inside the locked block, is_warp_rotating should detect the active lock
            assert is_warp_rotating() is True

        # After releasing, it should be False again
        assert is_warp_rotating() is False
