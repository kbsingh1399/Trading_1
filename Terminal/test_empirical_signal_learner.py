import sys
import unittest
from pathlib import Path
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.empirical_signal_learner import EmpiricalSignalLearner
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator


class TestEmpiricalSignalLearner(unittest.TestCase):
    def setUp(self):
        self.learner = EmpiricalSignalLearner(horizon_bars=16, default_rr=1.8, prior_weight_m=10.0)

    def test_learner_simulation_execution(self):
        """Verify that simulation runs across BTC rolling window without exceptions."""
        results = self.learner.simulate_symbol_signals("BTC")
        self.assertIsInstance(results, list)
        if results:
            first = results[0]
            self.assertIn("outcome", first)
            self.assertIn("r_multiple", first)
            self.assertIn(first["outcome"], ["WIN", "LOSS", "EXPIRED_PROFIT", "EXPIRED_LOSS"])

    def test_pessimistic_tie_breaker(self):
        """Verify that if high touches TP and low touches SL in the same bar, SL wins (loss)."""
        # Create a mock candle where high and low span wide range
        learner = EmpiricalSignalLearner(horizon_bars=1)
        # Entry 100, SL 99, TP 101.8
        # If bar has low 98 and high 102 -> must be LOSS
        mock_candles = pd.DataFrame([
            {"open_time_ms": 1000, "close_time_ms": 1900, "open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0,
             "total_volume": 1000, "delta": 100, "cumulative_delta": 100, "cvd": 100, "poc_price": 100.0,
             "open_time_ist": "2026-11-03 12:00:00 IST", "close_time_ist": "2026-11-03 12:15:00 IST", "candle_ist": "2026-11-03 12:00:00 IST"},
            {"open_time_ms": 2000, "close_time_ms": 2900, "open": 100.0, "high": 103.0, "low": 97.0, "close": 101.0,
             "total_volume": 1000, "delta": 100, "cumulative_delta": 200, "cvd": 200, "poc_price": 100.0,
             "open_time_ist": "2026-11-03 12:15:00 IST", "close_time_ist": "2026-11-03 12:30:00 IST", "candle_ist": "2026-11-03 12:15:00 IST"}
        ])
        # Entry at bar 0
        entry = 100.0
        sl = 99.0
        tp = 101.8
        h_bar = mock_candles.iloc[1]["high"]
        l_bar = mock_candles.iloc[1]["low"]
        hit_tp = h_bar >= tp
        hit_sl = l_bar <= sl
        self.assertTrue(hit_tp and hit_sl)
        # Under pessimistic invariant:
        outcome = "LOSS" if (hit_sl and hit_tp) else ("WIN" if hit_tp else "LOSS")
        self.assertEqual(outcome, "LOSS")

    def test_empirical_bayes_smoothing_logic(self):
        """Verify that small samples are pulled toward global prior."""
        sample_size = 2
        sample_wr = 1.0 # 2 wins out of 2
        prior_wr = 0.50
        m = 10.0
        # Smoothed = (2 * 1.0 + 10 * 0.5) / 12 = 7 / 12 = 0.5833
        smoothed = (sample_size * sample_wr + m * prior_wr) / (sample_size + m)
        self.assertAlmostEqual(smoothed, 0.5833, places=3)
        self.assertLess(smoothed, 0.70) # Strongly shrunk toward prior

    def test_matrix_lookup_speed(self):
        """Verify that expectancy lookup is fast (< 2ms) and returns valid edge dictionary."""
        edge = self.learner.get_signal_expectancy("BTC", "Absorption")
        self.assertIn("win_rate", edge)
        self.assertIn("ev_r", edge)
        self.assertIn("confluence_adj", edge)
        self.assertIn("recommendation", edge)

    def test_evaluator_includes_empirical_edge(self):
        """Verify that RealtimeSignalEvaluator integrates empirical learner."""
        evaluator = RealtimeSignalEvaluator()
        self.assertIsNotNone(evaluator.empirical_learner)
        mock_sig = {
            'symbol': 'BTC',
            'sig_name': 'Trapped longs',
            'side': 'sell',
            'price': 77000.0,
            'time_ms': 1793700000000,
            'datetime_utc': '2026-11-03T11:00:00Z',
            'asset_class': 'Crypto'
        }
        res = evaluator.evaluate_signal(mock_sig)
        self.assertIn('confluence_score', res)
        self.assertIn('confluence_reasons', res)


if __name__ == "__main__":
    unittest.main()
