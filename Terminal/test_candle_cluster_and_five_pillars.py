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


if __name__ == "__main__":
    unittest.main()
