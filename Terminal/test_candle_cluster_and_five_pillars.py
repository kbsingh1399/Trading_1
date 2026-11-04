"""
Unit Tests for Multi-Signal Candle Clustering, 5-Pillar Justification, and Delta Exchange Focus
================================================================================================
Validates:
1. Multi-signal candle clustering:
   - Dual unanimous signals (e.g. Absorption + Trapped Shorts) receive +15 pts confluence boost.
   - Triple unanimous signals receive +20 pts confluence boost.
   - Discordant intra-bar signals (Bullish + Bearish on same candle) trigger -20 pts penalty and INTRA_BAR_SIGNAL_CONFLICT veto.
2. 5-Pillar quantitative formulation justification:
   - Pillar 1: Footprint & Cluster Signals
   - Pillar 2: Auction Profile & Value Areas
   - Pillar 3: Live TV Liquidations & Reversals
   - Pillar 4: Liquidation Heatmap & Strike Magnets
   - Pillar 5: Order Book Whales & Sweeps
3. Single trade per candle invariant:
   - Sentinel evaluate_and_dispatch groups signals with identical (symbol, time_ms) into a single evaluation.
4. Delta Exchange India authorization display in Trade Confluence Card.
"""

import asyncio
import importlib
import os
import time
import unittest
from unittest.mock import MagicMock, patch

from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator

sentinel_mod = importlib.import_module("5_wake_on_footprint_signal")
format_confluence_card = sentinel_mod.format_confluence_card
FootprintSignalSentinel = sentinel_mod.FootprintSignalSentinel


class TestCandleClusterAndFivePillars(unittest.TestCase):
    def setUp(self):
        self.evaluator = RealtimeSignalEvaluator()
        # Mock OLAP to provide deterministic valid responses
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
        self.evaluator.olap.query_whale_history = MagicMock(return_value={
            "data_valid": True,
            "sweep_bias": "BULLISH_SWEEP",
            "net_whale_flow_usd": 2500000.0
        })

    def test_dual_unanimous_bullish_cluster_boost(self):
        """Dual concordant signals (Absorption + Trapped Shorts) receive +15 pts confluence boost."""
        cluster_sigs = [
            {"sig_name": "Absorption", "side": "bullish", "price": 101.0},
            {"sig_name": "Trapped shorts", "side": "bullish", "price": 101.0}
        ]
        sig = {
            "symbol": "BTCUSD",
            "sig_name": "Absorption + Trapped shorts",
            "side": "bullish",
            "price": 101.0,
            "time_ms": 1700000000000,
            "candle_ist": "2026-11-03 22:45:00 IST",
            "is_cluster": True,
            "cluster_signals": cluster_sigs,
            "cluster_signal_names": ["Absorption", "Trapped shorts"],
            "cluster_agreement": "UNANIMOUS_BULLISH"
        }
        res = self.evaluator.evaluate_signal(sig)
        reasons_text = " ".join(res.get("confluence_reasons", []))
        self.assertIn("Multi-Signal Candle Cluster (2 signals)", reasons_text)
        self.assertIn("+15pts", reasons_text)

    def test_triple_unanimous_cluster_boost(self):
        """Triple concordant signals on same candle receive +20 pts confluence boost."""
        cluster_sigs = [
            {"sig_name": "Absorption", "side": "bullish", "price": 101.0},
            {"sig_name": "Trapped shorts", "side": "bullish", "price": 101.0},
            {"sig_name": "Divergence", "side": "bullish", "price": 101.0}
        ]
        sig = {
            "symbol": "BTCUSD",
            "sig_name": "Absorption + Trapped shorts + Divergence",
            "side": "bullish",
            "price": 101.0,
            "time_ms": 1700000000000,
            "candle_ist": "2026-11-03 22:45:00 IST",
            "is_cluster": True,
            "cluster_signals": cluster_sigs,
            "cluster_signal_names": ["Absorption", "Trapped shorts", "Divergence"],
            "cluster_agreement": "UNANIMOUS_BULLISH"
        }
        res = self.evaluator.evaluate_signal(sig)
        reasons_text = " ".join(res.get("confluence_reasons", []))
        self.assertIn("Multi-Signal Candle Cluster (3 signals)", reasons_text)
        self.assertIn("+20pts", reasons_text)

    def test_discordant_conflict_penalized_and_vetoed(self):
        """Opposing signals on same candle trigger -20 pts penalty and INTRA_BAR_SIGNAL_CONFLICT veto."""
        cluster_sigs = [
            {"sig_name": "Absorption", "side": "bullish", "price": 101.0},
            {"sig_name": "Buyer exhaustion", "side": "bearish", "price": 101.0}
        ]
        sig = {
            "symbol": "BTCUSD",
            "sig_name": "Absorption + Buyer exhaustion",
            "side": "bullish",
            "price": 101.0,
            "time_ms": 1700000000000,
            "candle_ist": "2026-11-03 22:45:00 IST",
            "is_cluster": True,
            "cluster_signals": cluster_sigs,
            "cluster_signal_names": ["Absorption", "Buyer exhaustion"],
            "cluster_agreement": "DISCORDANT_CONFLICT"
        }
        res = self.evaluator.evaluate_signal(sig)
        reasons_text = " ".join(res.get("confluence_reasons", []))
        self.assertIn("Intra-Bar Signal Conflict", reasons_text)
        self.assertIn("-20pts", reasons_text)
        self.assertIn("INTRA_BAR_SIGNAL_CONFLICT", res.get("validation_errors", []))
        self.assertFalse(res.get("is_actionable", False))

    def test_five_pillar_justification_structure(self):
        """Verifies all 5 pillars are populated with quantitative justification text."""
        sig = {
            "symbol": "BTCUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 101.0,
            "time_ms": 1700000000000,
            "candle_ist": "2026-11-03 22:45:00 IST"
        }
        res = self.evaluator.evaluate_signal(sig)
        five_p = res.get("five_pillar_justification")
        self.assertIsNotNone(five_p, "five_pillar_justification missing from evaluation")
        self.assertIn("pillar_1_footprint_signals", five_p)
        self.assertIn("pillar_2_auction_value", five_p)
        self.assertIn("pillar_3_live_liquidations", five_p)
        self.assertIn("pillar_4_liquidation_heatmap", five_p)
        self.assertIn("pillar_5_whales_and_sweeps", five_p)

        # Confirm non-empty content
        for k, v in five_p.items():
            self.assertTrue(len(v) > 5, f"{k} is too short or empty: {v}")

    def test_card_contains_delta_exchange_and_five_pillars(self):
        """Verifies Trade Confluence Card includes Delta Exchange branding and 5-Pillar section."""
        alert = {
            "symbol": "BTCUSD",
            "direction": "BUY",
            "confluence_score": 85.0,
            "is_actionable": True,
            "trade_parameters": {
                "entry_price": 100.0,
                "stop_loss": 98.0,
                "take_profit_1": 104.0,
                "take_profit_2": 106.0,
                "rr_ratio": 2.0,
                "admissible_lots": 1.5,
                "realized_risk_usd": 24.50
            },
            "five_pillar_justification": {
                "pillar_1_footprint_signals": "Absorption (bullish) @ $100.00",
                "pillar_2_auction_value": "Day VWAP: $99.50 (ABOVE_VWAP)",
                "pillar_3_live_liquidations": "No active live TV liquidation blow-off spike",
                "pillar_4_liquidation_heatmap": "Near-Field Shorts: 65.0% vs Longs: 35.0%",
                "pillar_5_whales_and_sweeps": "Whale Book Imbalance: 55.0% Bids vs 45.0% Asks"
            },
            "confluence_reasons": ["Absorption (bullish)", "Above Day VWAP"]
        }
        card = format_confluence_card(alert)
        self.assertIn("Delta Exchange:", card)
        self.assertIn("5-PILLAR FORMULATION JUSTIFICATION", card)
        self.assertIn("1. Footprint & Cluster:", card)
        self.assertIn("2. Auction & Value:", card)
        self.assertIn("3. Live TV Liquidations:", card)
        self.assertIn("4. Liquidation Heatmap:", card)
        self.assertIn("5. Whales & Sweeps:", card)

    def test_sentinel_groups_multi_signals_on_same_candle(self):
        """Sentinel evaluate_and_dispatch groups multiple signals on same (symbol, time_ms) into 1 cluster."""
        sentinel = FootprintSignalSentinel(from_current_end=True)
        if hasattr(sentinel, 'stack') and hasattr(sentinel.stack, 'ledger'):
            sentinel.stack.ledger._evaluated = {}
        # Mock evaluate_signal
        sentinel.evaluator.evaluate_signal = MagicMock(return_value={
            "symbol": "NEARUSD",
            "direction": "BUY",
            "confluence_score": 80.0,
            "is_actionable": False,
            "validation_errors": []
        })

        raw_signals = [
            {
                "symbol": "NEARUSD",
                "sig_name": "Absorption",
                "side": "bullish",
                "price": 3.823,
                "time_ms": 1700000000000,
                "bar_closed": True
            },
            {
                "symbol": "NEARUSD",
                "sig_name": "Trapped shorts",
                "side": "bullish",
                "price": 3.823,
                "time_ms": 1700000000000,
                "bar_closed": True
            }
        ]

        evals = asyncio.run(sentinel.evaluate_and_dispatch(raw_signals))
        # Should result in exactly ONE evaluation for the candle
        self.assertEqual(len(evals), 1)
        sentinel.evaluator.evaluate_signal.assert_called_once()
        called_raw = sentinel.evaluator.evaluate_signal.call_args[0][0]
        self.assertTrue(called_raw.get("is_cluster"))
        self.assertEqual(called_raw.get("cluster_agreement"), "UNANIMOUS_BULLISH")
        self.assertEqual(len(called_raw.get("cluster_signals")), 2)

    def test_hyperdash_smart_money_and_crowd_squeeze_boost(self):
        """Verifies Hyperdash smart money accumulation and crowd squeeze signals boost confluence score."""
        self.evaluator.hyperdash = MagicMock()
        self.evaluator.hyperdash.get_asset_intel.return_value = {
            "coin": "SOL",
            "symbol": "SOLUSD",
            "data_valid": True,
            "smart_bias": 28.5,
            "crowd_bias": -35.2,
            "intel_score": 88.0,
            "intel_direction": "BULLISH",
            "signals": ["SMART_MONEY_ACCUM", "CROWD_SQUEEZE_LONG"]
        }

        signal = {
            "symbol": "SOLUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 100.0,
            "time_ms": 1700000000000,
            "bar_closed": True
        }

        res = self.evaluator.evaluate_signal(signal)
        reasons = res.get("confluence_reasons", [])
        self.assertTrue(any("Hyperdash Smart Money Accumulation" in r for r in reasons))
        self.assertTrue(any("Hyperdash Retail Short Squeeze Fuel" in r for r in reasons))
        self.assertIn("hyperdash_intel", res)
        self.assertEqual(res["hyperdash_intel"]["smart_bias"], 28.5)

        # Verify card renders Hyperdash line
        card = format_confluence_card(res)
        self.assertIn("Hyperdash Intel:", card)
        self.assertIn("Smart Bias=+28.5%", card)

    def test_hyperdash_opposing_distribution_penalty(self):
        """Verifies opposing smart money distribution penalizes bullish signal (-15 pts)."""
        self.evaluator.hyperdash = MagicMock()
        self.evaluator.hyperdash.get_asset_intel.return_value = {
            "coin": "SOL",
            "symbol": "SOLUSD",
            "data_valid": True,
            "smart_bias": -26.0,
            "crowd_bias": 15.0,
            "intel_score": 40.0,
            "intel_direction": "BEARISH",
            "signals": ["SMART_MONEY_DIST"]
        }

        signal = {
            "symbol": "SOLUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 100.0,
            "time_ms": 1700000000000,
            "bar_closed": True
        }

        res = self.evaluator.evaluate_signal(signal)
        reasons = res.get("confluence_reasons", [])
        self.assertTrue(any("Hyperdash Opposing Smart Money Distribution" in r for r in reasons))

    def test_laplace_headcount_smoothing(self):
        """Verifies Laplace smoothing for Hyperdash crowd bias when headcount < 10."""
        from Terminal.hyperdash_intel_engine import compute_net_bias_count
        # Zero traders
        self.assertEqual(compute_net_bias_count(0, 0), 0.0)
        # 1 long trader, 0 short: smoothed = (1 - 0) / (1 + 2) * 100 = 33.33%
        bias_1 = compute_net_bias_count(1, 0)
        self.assertAlmostEqual(bias_1, 33.33, places=1)
        # 0 long trader, 2 short: smoothed = (0 - 2) / (2 + 2) * 100 = -50.0%
        bias_2 = compute_net_bias_count(0, 2)
        self.assertAlmostEqual(bias_2, -50.0, places=1)
        # 10 long traders, 0 short: headcount >= 10 -> full bias = 100.0%
        bias_10 = compute_net_bias_count(10, 0)
        self.assertEqual(bias_10, 100.0)
        # Equal traders
        self.assertEqual(compute_net_bias_count(4, 4), 0.0)

    def test_forming_bar_parking_and_no_ledger_burn(self):
        """Verifies forming bars are parked or marked unqualified without burning ledger entries."""
        sentinel = FootprintSignalSentinel(from_current_end=True)
        if hasattr(sentinel, 'stack') and hasattr(sentinel.stack, 'ledger'):
            sentinel.stack.ledger._evaluated = {}
        sentinel.pending_signals.clear()

        # Mock evaluate_signal for actionable forming bar
        # Note: realtime evaluator appends BAR_NOT_CLOSED_INTRA_BAR_WICK when bar is forming
        sentinel.evaluator.evaluate_signal = MagicMock(return_value={
            "symbol": "BTCUSD",
            "direction": "BUY",
            "confluence_score": 82.0,
            "is_actionable": False,
            "validation_errors": ["BAR_NOT_CLOSED_INTRA_BAR_WICK"],
            "deepmind_timesfm": {"required_threshold": 75.0, "adverse_veto": False}
        })

        # Future time_ms -> bar is forming (not closed)
        now_ms = time.time() * 1000
        future_ms = int((now_ms // 900_000) * 900_000)
        raw_signal = [{
            "symbol": "BTCUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 85000.0,
            "time_ms": future_ms,
            "bar_type": "time",
            "is_mock": False,
            "bar_closed": False
        }]

        # Set environment to allow live logic evaluation
        with patch.dict(os.environ, {"MOCK_TEST_MODE": "0"}, clear=False):
            evals = asyncio.run(sentinel.evaluate_and_dispatch(raw_signal))
            self.assertEqual(len(evals), 1)

            # Signal must be parked, NOT burned in ledger
            self.assertEqual(len(sentinel.pending_signals), 1)
            park_key = ("BTCUSD", future_ms, "time", "")
            self.assertIn(park_key, sentinel.pending_signals)
            # Ledger must NOT have evaluated this open_ms
            self.assertFalse(sentinel.stack.ledger.is_evaluated("BTCUSD", future_ms, bar_type="time"))

            # Test forming bar that is unqualified (score < 75.0)
            sentinel.evaluator.evaluate_signal = MagicMock(return_value={
                "symbol": "ETHUSD",
                "direction": "BUY",
                "confluence_score": 60.0,
                "is_actionable": False,
                "validation_errors": ["BAR_NOT_CLOSED_INTRA_BAR_WICK"],
                "deepmind_timesfm": {"required_threshold": 75.0, "adverse_veto": False}
            })
            unqualified_signal = [{
                "symbol": "ETHUSD",
                "sig_name": "Absorption",
                "side": "bullish",
                "price": 2500.0,
                "time_ms": future_ms,
                "bar_type": "time",
                "is_mock": False,
                "bar_closed": False
            }]
            evals_unq = asyncio.run(sentinel.evaluate_and_dispatch(unqualified_signal))
            # Must NOT be in pending signals and NOT burned in ledger
            unq_park_key = ("ETHUSD", future_ms, "time", "")
            self.assertNotIn(unq_park_key, sentinel.pending_signals)
            self.assertFalse(sentinel.stack.ledger.is_evaluated("ETHUSD", future_ms, bar_type="time"))

    def test_forming_bar_drain_upon_close_and_degradation_unpark(self):
        """Verifies parked forming bars drain upon close, do NOT re-park, and unpark on degradation."""
        sentinel = FootprintSignalSentinel(from_current_end=True)
        if hasattr(sentinel, 'stack') and hasattr(sentinel.stack, 'ledger'):
            sentinel.stack.ledger._evaluated = {}
        sentinel.pending_signals.clear()

        # 1. Park an actionable signal with close_ms in the past (already closed)
        past_ms = int(time.time() * 1000) - 1_800_000
        open_ms = (past_ms // 900_000) * 900_000
        close_ms = open_ms + 900_000
        park_key = ("SOLUSD", open_ms, "time", "")
        cluster = [{
            "symbol": "SOLUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 150.0,
            "time_ms": open_ms,
            "bar_type": "time",
            "is_mock": False,
            "bar_closed": False
        }]
        sentinel.pending_signals[park_key] = {
            "cluster": cluster,
            "close_ms": close_ms,
            "bar_type": "time",
            "bar_variant": "",
            "open_ms": open_ms,
            "parked_at_ms": past_ms - 300_000,
        }

        # Mock evaluate_signal to return actionable closed result
        sentinel.evaluator.evaluate_signal = MagicMock(return_value={
            "symbol": "SOLUSD",
            "direction": "BUY",
            "confluence_score": 88.0,
            "is_actionable": True,
            "validation_errors": [],
            "deepmind_timesfm": {"required_threshold": 75.0, "adverse_veto": False}
        })

        with patch("Terminal.delta_exchange_live_trader.delta_client.execute_delta_signal", return_value={"ok": True}):
            with patch.dict(os.environ, {"MOCK_TEST_MODE": "0"}, clear=False):
                asyncio.run(sentinel._drain_pending_forming_bars())

                # The cluster should be drained and NOT re-parked
                self.assertNotIn(park_key, sentinel.pending_signals)
                # The candle should be recorded in the ledger
                self.assertTrue(sentinel.stack.ledger.is_evaluated("SOLUSD", open_ms, bar_type="time"))

        # 2. Test degradation unparking: parked signal that subsequently degrades is unparked
        future_ms = int((int(time.time() * 1000) // 900_000) * 900_000)
        park_key_deg = ("ADAUSD", future_ms, "time", "")
        sentinel.pending_signals[park_key_deg] = {
            "cluster": [],
            "close_ms": future_ms + 900_000,
            "bar_type": "time",
            "bar_variant": "",
            "open_ms": future_ms,
            "parked_at_ms": future_ms,
        }
        # Later in-flight tick shows degraded score < 75.0
        sentinel.evaluator.evaluate_signal = MagicMock(return_value={
            "symbol": "ADAUSD",
            "direction": "BUY",
            "confluence_score": 55.0,
            "is_actionable": False,
            "validation_errors": ["BAR_NOT_CLOSED_INTRA_BAR_WICK"],
            "deepmind_timesfm": {"required_threshold": 75.0, "adverse_veto": False}
        })
        degraded_signal = [{
            "symbol": "ADAUSD",
            "sig_name": "Absorption",
            "side": "bullish",
            "price": 0.50,
            "time_ms": future_ms,
            "bar_type": "time",
            "is_mock": False,
            "bar_closed": False
        }]
        with patch.dict(os.environ, {"MOCK_TEST_MODE": "0"}, clear=False):
            asyncio.run(sentinel.evaluate_and_dispatch(degraded_signal))
            # Must be popped from pending_signals
            self.assertNotIn(park_key_deg, sentinel.pending_signals)


if __name__ == "__main__":
    unittest.main()

