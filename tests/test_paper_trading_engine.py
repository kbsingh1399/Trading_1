"""
Unit tests for the Local Live-Tape Shadow Paper Trading Engine.
Verifies sizing, position limits, bracket geometry, trailing ratchet, and PnL calculation.
"""

import pytest
from unittest.mock import MagicMock, patch
from Terminal.paper_trading_engine import PaperTradingEngine, TAKER_FEE_RATE, USD_INR_RATE


@pytest.fixture
def mock_delta_client():
    client = MagicMock()
    client.resolve_product.return_value = {
        "id": 4,
        "symbol": "XRPUSD",
        "underlying": "XRP",
        "contract_value": 1.0,
        "tick_size": 0.0001
    }
    client.request.return_value = {
        "success": True,
        "result": {
            "mark_price": 1.2500,
            "quotes": {
                "best_bid": 1.2499,
                "best_ask": 1.2501
            }
        }
    }
    return client


def test_paper_trade_execution_and_geometry(mock_delta_client, tmp_path):
    with patch("Terminal.paper_trading_engine.ACTIVE_POSITIONS_FILE", tmp_path / "active.json"), \
         patch("Terminal.paper_trading_engine.LEDGER_FILE", tmp_path / "ledger.jsonl"):
        engine = PaperTradingEngine()

        eval_res = {
            "symbol": "XRP",
            "direction": "BUY",
            "confluence_score": 80.0,
            "trade_levels": {
                "entry_price": 1.2500,
                "stop_loss": 1.2400,
                "take_profit_1": 1.2750
            }
        }

        res = engine.execute_paper_order(eval_res, mock_delta_client)
        assert res["ok"] is True
        assert res["status"] == "PAPER_EXECUTED"

        trade = res["trade"]
        assert trade["product_symbol"] == "XRPUSD"
        assert trade["direction"] == "BUY"
        # Fill price is best_ask (1.2501) + 1 tick (0.0001) = 1.2502
        assert trade["fill_price"] == 1.2502
        assert trade["rr_ratio"] >= 2.0
        assert engine.get_open_positions_count() == 1


def test_paper_trade_enforces_max_concurrent_positions(mock_delta_client, tmp_path):
    with patch("Terminal.paper_trading_engine.ACTIVE_POSITIONS_FILE", tmp_path / "active.json"), \
         patch("Terminal.paper_trading_engine.LEDGER_FILE", tmp_path / "ledger.jsonl"):
        engine = PaperTradingEngine()
        engine.active_positions = {
            "BTCUSD": {"symbol": "BTC"},
            "ETHUSD": {"symbol": "ETH"}
        }

        eval_res = {"symbol": "SOL", "direction": "BUY"}
        res = engine.execute_paper_order(eval_res, mock_delta_client)
        assert res["ok"] is False
        assert "MAX_OPEN_POSITIONS_REACHED" in res["reason"]


def test_paper_trailing_stop_tiers_and_exit(mock_delta_client, tmp_path):
    with patch("Terminal.paper_trading_engine.ACTIVE_POSITIONS_FILE", tmp_path / "active.json"), \
         patch("Terminal.paper_trading_engine.LEDGER_FILE", tmp_path / "ledger.jsonl"):
        engine = PaperTradingEngine()

        # Seed an active BUY position with wide initial TP
        # Entry: 1.0000, initial R0 = 0.0500, SL = 0.9500, TP = 1.2500
        engine.active_positions["XRPUSD"] = {
            "trade_id": "TEST_XRP",
            "product_symbol": "XRPUSD",
            "symbol": "XRP",
            "direction": "BUY",
            "contracts": 100,
            "contract_value": 1.0,
            "tick_size": 0.0001,
            "fill_price": 1.0000,
            "mark_price": 1.0000,
            "stop_loss": 0.9500,
            "take_profit": 1.2500,
            "initial_r0": 0.0500,
            "entry_fee_usd": 0.075,
            "tier_reached": 0,
            "opened_at_ist": "2026-11-05 00:00:00 IST"
        }

        # 1. Price moves to +1.0R (1.0500) -> Tier 1 Breakeven (+1 tick = 1.0001)
        mock_delta_client.request.return_value = {"success": True, "result": {"mark_price": 1.0510}}
        engine.audit_and_trail_positions(mock_delta_client)
        pos = engine.active_positions["XRPUSD"]
        assert pos["tier_reached"] == 1
        assert pos["stop_loss"] == 1.0001

        # 2. Price moves to +2.0R (1.1000) -> Tier 2 Lock +1.0R (1.0500), Widen TP to +3.0R (1.1500)
        mock_delta_client.request.return_value = {"success": True, "result": {"mark_price": 1.1010}}
        engine.audit_and_trail_positions(mock_delta_client)
        pos = engine.active_positions["XRPUSD"]
        assert pos["tier_reached"] == 2
        assert pos["stop_loss"] == 1.0500
        assert pos["take_profit"] == 1.1500

        # 3. Price hits widened TP (1.1510) -> Closed with profit
        mock_delta_client.request.return_value = {"success": True, "result": {"mark_price": 1.1510}}
        reports = engine.audit_and_trail_positions(mock_delta_client)
        assert len(reports) == 1
        assert reports[0]["exit_reason"] == "TAKE_PROFIT_HIT"
        assert reports[0]["net_pnl_usd"] > 0
        assert "XRPUSD" not in engine.active_positions
