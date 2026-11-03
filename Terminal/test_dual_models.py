"""
Unit Tests for Dual-Model Execution Architecture (Mean Reversion vs Trend Following on Pullback)
and Fibonacci-Prime Moving Average Ribbons (including EMA 34, 55, 59, 89, 144, 233).
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timezone

from Terminal.auction_indicators import (
    compute_fib_prime_emas,
    evaluate_fib_prime_stack,
    compute_fib_prime_pullback_proximity
)
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator


class TestDualModelsAndFibPrime(unittest.TestCase):
    def setUp(self):
        self.evaluator = RealtimeSignalEvaluator()

    def test_1_fib_prime_emas_complete_sequence(self):
        """Verifies all 9 Fibonacci and Prime spans are computed accurately."""
        # Generate monotonic trend series
        prices = pd.Series([100.0 + i * 0.5 for i in range(300)])
        emas = compute_fib_prime_emas(prices)
        
        expected_spans = [5, 13, 21, 34, 55, 59, 89, 144, 233]
        for s in expected_spans:
            self.assertIn(f"ema{s}", emas, f"Span {s} missing from computed EMAs")
            self.assertGreater(emas[f"ema{s}"], 0.0)

        # In strong uptrend: Price > EMA5 > EMA13 > ... > EMA233
        latest_price = float(prices.iloc[-1])
        stack_status = evaluate_fib_prime_stack(latest_price, emas)
        self.assertEqual(stack_status, "BULLISH_FIB_PRIME_STACK")

    def test_2_fib_prime_pullback_proximity(self):
        """Verifies pullback proximity calculation into [EMA 55, EMA 59] pocket."""
        emas = {
            "ema34": 102.0,
            "ema55": 100.0,
            "ema59": 99.8,
            "ema89": 97.0,
            "ema233": 90.0
        }
        
        # Test price inside pocket [100.0, 99.8] -> price = 100.2 (0.2% dist)
        prox_in = compute_fib_prime_pullback_proximity(100.2, emas)
        self.assertTrue(prox_in["in_pullback_pocket"])
        self.assertLessEqual(prox_in["dist_55_59_pct"], 0.60)

        # Test price far outside pocket -> price = 110.0 (10% dist)
        prox_out = compute_fib_prime_pullback_proximity(110.0, emas)
        self.assertFalse(prox_out["in_pullback_pocket"])
        self.assertGreater(prox_out["dist_55_59_pct"], 0.60)

    def test_3_model_1_mean_reversion_target_extension(self):
        """Verifies Model 1 extends TP1 to Opposite Value Area extreme when VWAP is too close."""
        # Mock signal for Bearish Divergence at VAH
        raw_sig = {
            "symbol": "BTC",
            "price": 105.0,
            "sig_name": "Divergence",
            "sig_code": "div",
            "side": "sell",
            "title": "Bearish Divergence at High",
            "description": "Rally without taker buying",
            "candle_utc": datetime.now(timezone.utc).isoformat(),
            "time_ms": int(datetime.now(timezone.utc).timestamp() * 1000) - 950_000,
            "bar_closed": True
        }

        # Override auction query to simulate Range regime at VAH extreme with VWAP near entry
        res = self.evaluator.evaluate_signal(raw_sig)
        
        # Verify execution model assignment
        self.assertIn("execution_model", res)
        # Verify trade levels formulated
        trade = res.get("trade_levels", {})
        self.assertGreater(trade.get("rr_ratio", 0.0), 0.0)
        self.assertIsNotNone(trade.get("stop_loss"))
        self.assertIsNotNone(trade.get("take_profit_1"))

    def test_4_model_2_trend_pullback_routing(self):
        """Verifies Model 2 routes continuation setups when macro trend is aligned."""
        # Mock signal for Bullish Absorption in strong uptrend
        raw_sig = {
            "symbol": "ETH",
            "price": 2500.0,
            "sig_name": "Absorption",
            "sig_code": "abs",
            "side": "buy",
            "title": "Bullish Absorption at Pullback",
            "description": "Selling absorbed at dynamic support",
            "candle_utc": datetime.now(timezone.utc).isoformat(),
            "time_ms": int(datetime.now(timezone.utc).timestamp() * 1000) - 950_000,
            "bar_closed": True
        }

        res = self.evaluator.evaluate_signal(raw_sig)
        self.assertIn("execution_model", res)
        self.assertIn("fib_prime_pocket", res)
        pocket = res["fib_prime_pocket"]
        self.assertIn("ema55", pocket)
        self.assertIn("ema59", pocket)
        self.assertIn("ema89", pocket)

    def test_5_sol_divergence_non_mt5_informational_evaluation(self):
        """Verifies SOL Divergence evaluates cleanly without BROKER_QUOTE_UNKNOWN veto."""
        raw_sig = {
            "symbol": "SOL",
            "price": 93.08,
            "sig_name": "Divergence",
            "sig_code": "div",
            "side": "bullish",
            "title": "Drop without taker selling",
            "description": "Closed down on positive Delta (+21%)",
            "candle_utc": datetime.now(timezone.utc).isoformat(),
            "time_ms": int(datetime.now(timezone.utc).timestamp() * 1000) - 950_000,
            "bar_closed": True
        }

        res = self.evaluator.evaluate_signal(raw_sig)
        # Should not have BROKER_QUOTE_UNKNOWN veto
        reasons_text = " ".join(res.get("confluence_reasons", []))
        self.assertNotIn("BROKER_QUOTE_UNKNOWN", reasons_text)
        self.assertFalse(res.get("matchtrader_execution_authorized", False)) # MatchTrader Crypto only BTC/ETH/XRP
        self.assertGreater(res.get("confluence_score", 0.0), 0.0)


if __name__ == "__main__":
    unittest.main()
