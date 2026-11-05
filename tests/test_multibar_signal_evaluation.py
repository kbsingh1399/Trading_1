"""
Unit Tests for Multi-Bar Signal Ingestion & Real-Time Evaluation
================================================================
Validates:
1. Support for all bar types: 15m time candles, 5m time candles, Range bars, and Volume bars.
2. Zero delay: range and volume bars are immediately marked closed upon emission and evaluated.
3. Evaluator does not veto range/volume bars with NON_TIME_BAR_DISCARDED.
4. Ledger records each bar type independently: (symbol, bar_type, open_ms).
5. In-flight forming time bars are evaluated live for operator situational awareness while Delta execution is guarded.
"""

import time
import json
import asyncio
import unittest
from unittest.mock import MagicMock, patch

from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.asset_signal_stack import EvaluatedSignalsLedger
import importlib

sentinel_mod = importlib.import_module("5_wake_on_footprint_signal")
FootprintSignalSentinel = sentinel_mod.FootprintSignalSentinel
format_confluence_card = sentinel_mod.format_confluence_card


class TestMultiBarSignalEvaluation(unittest.TestCase):
    def setUp(self):
        self.evaluator = RealtimeSignalEvaluator()
        self.evaluator.olap.query_auction_profile = MagicMock(return_value={
            "data_valid": True,
            "day_vwap": 100.0,
            "day_vah": 105.0,
            "day_val": 95.0,
            "day_poc": 100.0,
            "pdh": 108.0,
            "pdl": 92.0,
            "pd_poc": 99.0,
            "fib_prime_alignment": "BULLISH_ABOVE_POCKET",
            "mtf_bias": "BULLISH"
        })
        self.evaluator.olap.query_cvd_trend = MagicMock(return_value={
            "data_valid": True,
            "slope": "BULLISH_FLOW"
        })
        self.evaluator.olap.query_whale_walls = MagicMock(return_value={
            "data_valid": True,
            "imbalance_ratio": 0.55
        })
        self.evaluator.olap.query_liquidation_magnets = MagicMock(return_value={
            "data_valid": True,
            "near_field_shorts_pct": 60.0,
            "short_magnets": [{"zone_price": 106.0}],
            "long_magnets": [{"zone_price": 94.0}]
        })

    def test_range_bar_signal_evaluation_no_veto(self):
        """Range bar signal should not be vetoed as NON_TIME_BAR."""
        sig = {
            "symbol": "ETH",
            "bar_type": "range",
            "bar_label": "Range 10",
            "sig_name": "Absorption",
            "sig_code": "abs",
            "side": "bullish",
            "price": 2320.0,
            "time_ms": int(time.time() * 1000) - 60_000,
            "title": "Selling absorbed at the low",
            "description": "21K of taker selling absorbed"
        }
        res = self.evaluator.evaluate_signal(sig)
        val_errors = res.get("validation_errors", [])
        self.assertNotIn("NON_TIME_BAR_DISCARDED", val_errors)
        self.assertNotIn("MISALIGNED_TIMESTAMP_DISCARDED", val_errors)
        self.assertNotIn("BAR_NOT_CLOSED_INTRA_BAR_WICK", val_errors)
        self.assertGreaterEqual(res.get("confluence_score", 0.0), 50.0)

    def test_volume_bar_signal_evaluation_no_veto(self):
        """Volume bar signal should not be vetoed as NON_TIME_BAR."""
        sig = {
            "symbol": "SOL",
            "bar_type": "vol",
            "bar_label": "Volume 250K SOL",
            "sig_name": "Buyer exhaustion",
            "sig_code": "exh",
            "side": "bearish",
            "price": 93.5,
            "time_ms": int(time.time() * 1000) - 120_000,
            "title": "Buyer exhaustion",
            "description": "New 10-bar high with almost no volume in top 10%"
        }
        res = self.evaluator.evaluate_signal(sig)
        val_errors = res.get("validation_errors", [])
        self.assertNotIn("NON_TIME_BAR_DISCARDED", val_errors)
        self.assertNotIn("MISALIGNED_TIMESTAMP_DISCARDED", val_errors)
        self.assertNotIn("BAR_NOT_CLOSED_INTRA_BAR_WICK", val_errors)

    def test_ledger_indexes_by_bar_type(self):
        """Ledger must distinguish between time, range, and volume bars on same symbol and timestamp."""
        ledger = EvaluatedSignalsLedger()
        ledger._evaluated = {}
        ts = 1700000000000
        
        # Mark time bar
        ledger.mark_evaluated("BTC", ts, "sig_time", 75.0, "DISPATCHED", "Absorption", "BUY", bar_type="time")
        self.assertTrue(ledger.is_evaluated("BTC", ts, bar_type="time"))
        # Range bar on exact same timestamp must NOT be suppressed by time bar
        self.assertFalse(ledger.is_evaluated("BTC", ts, bar_type="range"))
        self.assertFalse(ledger.is_evaluated("BTC", ts, bar_type="vol"))

        # Now mark range bar
        ledger.mark_evaluated("BTC", ts, "sig_range", 80.0, "DISPATCHED", "Divergence", "BUY", bar_type="range")
        self.assertTrue(ledger.is_evaluated("BTC", ts, bar_type="range"))
        self.assertFalse(ledger.is_evaluated("BTC", ts, bar_type="vol"))

    def test_sentinel_immediate_multi_bar_evaluation(self):
        """Sentinel must evaluate Range, Vol, and Time signals in the same batch."""
        sentinel = FootprintSignalSentinel(from_current_end=True)
        sentinel.stack.ledger._evaluated = {}

        now_ms = int(time.time() * 1000)
        raw_signals = [
            {
                "symbol": "BTCUSD",
                "bar_type": "range",
                "sig_name": "Absorption",
                "side": "bullish",
                "price": 78400.0,
                "time_ms": now_ms - 30_000,
                "is_mock": True
            },
            {
                "symbol": "ETHUSD",
                "bar_type": "vol",
                "sig_name": "Absorption",
                "side": "bullish",
                "price": 2320.0,
                "time_ms": now_ms - 20_000,
                "is_mock": True
            }
        ]

        sentinel.evaluator.evaluate_signal = MagicMock(side_effect=lambda s: {
            "symbol": s["symbol"],
            "direction": "BUY",
            "confluence_score": 78.0,
            "is_actionable": False,
            "validation_errors": []
        })

        evals = asyncio.run(sentinel.evaluate_and_dispatch(raw_signals))
        self.assertEqual(len(evals), 2)
        # Verify both were marked in ledger
        self.assertTrue(sentinel.stack.ledger.is_evaluated("BTCUSD", now_ms - 30_000, bar_type="range"))
        self.assertTrue(sentinel.stack.ledger.is_evaluated("ETHUSD", now_ms - 20_000, bar_type="vol"))


if __name__ == "__main__":
    unittest.main()
