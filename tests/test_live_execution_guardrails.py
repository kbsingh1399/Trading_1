import math
import time
import pytest
from unittest.mock import MagicMock, patch

from Terminal.delta_exchange_live_trader import DeltaExchangeTrader, solve_fee_aware_grid
from Terminal.asset_signal_stack import EvaluatedSignalsLedger
from Terminal.risk_envelope import MAX_RISK_USD


class TestLiveExecutionGuardrails:

    def setup_method(self):
        self.trader = DeltaExchangeTrader()
        self.trader.api_key = "mock_key"
        self.trader.api_secret = "mock_secret"

    def test_fee_aware_contract_shedder(self):
        """Verify contract shedder accounts for round-trip taker fees and strictly enforces $25 risk cap."""
        # Setup: BTC entry 70,000, SL 68,000 (risk = 2,000 pts).
        # Contract val = 0.001. Pure price risk per contract = $2.00.
        # With 0.075% taker fee, entry fee = 0.075% * 70000 * 0.001 = $0.0525.
        # Exit fee = 0.075% * 68000 * 0.001 = $0.051. Total fee = $0.1035.
        # Total risk per contract = $2.1035.
        # Cap $25.00 -> max allowed size = floor(25 / 2.1035) = 11 contracts ($23.14).
        grid = solve_fee_aware_grid(
            entry=70000.0,
            stop_loss=68000.0,
            contract_value=0.001,
            taker_fee_rate=0.00075,
            cap_usd=25.0,
            max_contracts=100
        )
        assert grid["ok"] is True
        assert grid["size"] == 11
        assert grid["realized_risk_usd"] <= 25.0

    def test_non_time_bar_rejected_from_live_execution(self):
        """Range and Volume bars must be strictly prohibited from punching orders."""
        sig_range = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "bar_type": "range",
            "bar_label": "Range 250",
            "time_ms": 1730000000000,
            "confluence_score": 85.0,
            "is_actionable": True,
            "validation_errors": []
        }
        res = self.trader.execute_delta_signal(sig_range)
        assert res["ok"] is False
        assert "NON_TIME_BAR_EXECUTION_PROHIBITED" in res["reason"]

        sig_vol = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "bar_type": "vol",
            "bar_label": "Volume 1000 BTC",
            "time_ms": 1730000000000,
            "confluence_score": 85.0,
            "is_actionable": True,
            "validation_errors": []
        }
        res_vol = self.trader.execute_delta_signal(sig_vol)
        assert res_vol["ok"] is False
        assert "NON_TIME_BAR_EXECUTION_PROHIBITED" in res_vol["reason"]

    def test_5m_bar_rejected_even_on_15m_boundary(self):
        """A 5m bar timestamped on a 15m boundary (e.g. 15:00) must be rejected fail-closed."""
        sig_5m = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "bar_type": "time",
            "bar_label": "5m",
            "time_ms": 1730000000000 - (1730000000000 % 900_000), # Exactly on 15m boundary
            "confluence_score": 85.0,
            "is_actionable": True,
            "validation_errors": []
        }
        res = self.trader.execute_delta_signal(sig_5m)
        assert res["ok"] is False
        assert "NON_15M_INTERVAL_PROHIBITED" in res["reason"]

    def test_unaligned_15m_timestamp_rejected(self):
        """Any 15m bar with timestamp not cleanly divisible by 900,000 must be rejected."""
        sig_unaligned = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "bar_type": "time",
            "bar_label": "15m",
            "time_ms": 1730000000000 - (1730000000000 % 900_000) + 60_000, # 1 minute offset
            "confluence_score": 85.0,
            "is_actionable": True,
            "validation_errors": []
        }
        res = self.trader.execute_delta_signal(sig_unaligned)
        assert res["ok"] is False
        assert "NON_15M_INTERVAL_PROHIBITED" in res["reason"]

    def test_missing_bar_label_fails_closed(self):
        """Unlabeled candle must not default to 15m; must fail closed."""
        sig_no_label = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "bar_type": "time",
            "bar_label": None,
            "time_ms": 1730000000000 - (1730000000000 % 900_000),
            "confluence_score": 85.0,
            "is_actionable": True,
            "validation_errors": []
        }
        res = self.trader.execute_delta_signal(sig_no_label)
        assert res["ok"] is False
        assert "NON_15M_INTERVAL_PROHIBITED" in res["reason"]

    def test_dust_filter_uses_own_contract_value(self):
        """Dust position filter must resolve existing position's own product contract value, not incoming product."""
        # Mock active positions: 1 institutional DOGE position (notional > $1.00)
        # DOGE contract_value = 10.0, price = 0.20, size = 100 contracts -> notional = 100 * 10 * 0.20 = $200.00
        # If incorrectly using BTC contract value (0.001), notional would be $0.02 (falsely treated as dust).
        mock_positions = [{
            "product_symbol": "DOGEUSD",
            "product_id": 9999,
            "size": 100,
            "mark_price": 0.20,
            "contract_value": 10.0
        }]
        with patch.object(self.trader, "get_active_positions", return_value=mock_positions), \
             patch.object(self.trader, "resolve_product", return_value={"id": 1, "symbol": "BTCUSD", "contract_value": 0.001, "tick_size": 0.5}), \
             patch.object(self.trader, "get_wallet_balances", return_value=[{"asset_symbol": "USDT", "balance": 1000.0, "available_balance": 800.0}]), \
             patch.object(self.trader, "convert_to_usd", side_effect=lambda v, a: v):

            sig_btc = {
                "symbol": "BTCUSD",
                "direction": "BUY",
                "bar_type": "time",
                "bar_label": "15m",
                "time_ms": 1730000000000 - (1730000000000 % 900_000),
                "confluence_score": 85.0,
                "is_actionable": True,
                "trade_parameters": {"entry_price": 70000.0, "stop_loss": 68000.0, "take_profit_1": 74000.0}
            }
            # DOGE position is counted as 1 institutional position (1/2 positions)
            # Let's verify with 2 existing institutional positions that it blocks the 3rd
            mock_positions.append({
                "product_symbol": "ETHUSD",
                "product_id": 8888,
                "size": 10,
                "mark_price": 2500.0,
                "contract_value": 0.01
            })
            with patch("Terminal.delta_exchange_live_trader.LIVE_DISPATCH_ENABLED", True):
                res = self.trader.execute_delta_signal(sig_btc)
                assert res["ok"] is False
                assert "DELTA_MAX_POSITIONS_REACHED" in res["reason"]

    def test_ledger_cooldown_blocks_rapid_churn(self, tmp_path):
        """Symbol cooldown state machine prevents rapid consecutive churn in range-bound chop."""
        ledger_file = tmp_path / "test_ledger.jsonl"
        ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

        assert ledger.is_in_cooldown("BTC")[0] is False

        # Stop out occurs -> activate 60-minute cooldown
        ledger.set_cooldown("BTC", duration_sec=3600.0, reason="STOP_OUT_CHOP_PROTECTION")
        in_cd, rem = ledger.is_in_cooldown("BTC")
        assert in_cd is True
        assert 3500 <= rem <= 3600

        # Unrelated symbol unaffected
        assert ledger.is_in_cooldown("ETH")[0] is False
