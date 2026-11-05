"""
tests/test_adversarial_qa.py
========================================================================================
Pillar 5 Adversarial Stress & QA Test Suite
========================================================================================
Covers:
1. Concurrent burst arrivals: Multi-threaded simultaneous append to signal ledgers
   verifying atomic append, zero line mangling, deduplication, and lock robustness.
2. 50/50 LONG/SHORT deadlock in entropy calculation and consensus gating.
3. Missing or empty candle files, zero ATR, zero/NaN reference prices.
4. Zero-distance stop loss causing ZeroDivisionError in R:R.
5. Corrupted JSON lines in signals.jsonl and recovery resilience.
6. run_pipeline.py TDMA buffer safety and orphaned tab cleanup under simulated crashes.
========================================================================================
"""

import sys
import os
import math
import json
import time
import threading
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import (
    SignalQueueManager,
    MultiPillarTradeEvaluator,
    calculate_shannon_entropy,
    safe_float,
    CANDLE_DIR,
    QUEUE_DIR,
    SETUPS_DIR,
)
from run_pipeline import (
    SCHEDULE,
    cleanup_scraper_orphaned_tabs,
)


class TestSignalLedgerConcurrency:
    """Stress tests concurrent bursts on append-only signal ledgers."""

    @pytest.fixture(autouse=True)
    def cleanup_test_ledgers(self):
        self.symbol = "TEST_CONCURRENT_BURST"
        self.p_jsonl = SignalQueueManager.get_signals_jsonl_path(self.symbol)
        self.p_legacy = SignalQueueManager.get_legacy_json_path(self.symbol)
        self.cleanup()
        yield
        self.cleanup()

    def cleanup(self):
        if self.p_jsonl.exists():
            self.p_jsonl.unlink(missing_ok=True)
        if self.p_legacy.exists():
            self.p_legacy.unlink(missing_ok=True)

    def test_concurrent_burst_arrivals_50_signals(self):
        """
        Simulate 50 synthetic signals arriving simultaneously across 10 worker threads.
        Assert:
        - All 50 signals appended without loss or timeout.
        - Zero line mangling: every line in .jsonl is valid JSON.
        - Full schema compliance and deduplication stability.
        """
        num_threads = 10
        signals_per_thread = 5
        total_expected = num_threads * signals_per_thread

        errors = []
        successes = []

        def worker(w_id):
            signals = []
            for i in range(signals_per_thread):
                signals.append({
                    "sig_code": f"trap_w{w_id}_s{i}",
                    "side": "bull",
                    "price": 60000.0 + (w_id * 10) + i,
                    "time_ms": 1700000000000 + (w_id * 100) + i,
                    "bar_type": "time",
                    "bar_label": "15m",
                    "delta": 150.0,
                    "cvd": 500.0,
                })
            try:
                SignalQueueManager.append_signals(self.symbol, signals)
                successes.append(w_id)
            except Exception as e:
                errors.append((w_id, type(e).__name__, str(e)))

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Encountered thread errors during burst: {errors}"
        assert len(successes) == num_threads

        # Verify line-by-line file integrity (Zero line mangling)
        assert self.p_jsonl.exists()
        raw_lines = self.p_jsonl.read_text(encoding="utf-8").strip().split("\n")
        assert len(raw_lines) == total_expected, f"Expected {total_expected} lines, got {len(raw_lines)}"

        parsed_signals = []
        for idx, line in enumerate(raw_lines):
            try:
                sig = json.loads(line)
                parsed_signals.append(sig)
            except Exception as e:
                pytest.fail(f"Line {idx + 1} mangled/unparseable JSON: {repr(line[:80])} - Error: {e}")

        assert len(parsed_signals) == total_expected

        # Load back via API
        loaded = SignalQueueManager.load_all_signals(self.symbol)
        assert len(loaded) == total_expected


class TestCorruptedJsonlResilience:
    """Stress tests ledger corruption recovery and line mangling prevention."""

    @pytest.fixture(autouse=True)
    def cleanup_test_files(self):
        self.symbol = "TEST_CORRUPT_RESILIENCE"
        self.p_jsonl = SignalQueueManager.get_signals_jsonl_path(self.symbol)
        self.p_legacy = SignalQueueManager.get_legacy_json_path(self.symbol)
        self.cleanup()
        yield
        self.cleanup()

    def cleanup(self):
        if self.p_jsonl.exists():
            self.p_jsonl.unlink(missing_ok=True)
        if self.p_legacy.exists():
            self.p_legacy.unlink(missing_ok=True)

    def test_unterminated_last_line_prevents_mangling(self):
        """
        Verify that if a prior write was interrupted without a trailing newline,
        subsequent appends do NOT get welded to the prior line.
        """
        # Write valid line 1, corrupt line 2, and valid un-terminated line 3 (no newline)
        with open(self.p_jsonl, "w", encoding="utf-8") as f:
            f.write('{"sig_code": "trap", "side": "bull", "price": 50000.0, "time_ms": 1000, "bar_type": "time", "bar_label": "15m"}\n')
            f.write('{"sig_code": "abs", MALFORMED_JSON_LINE\n')
            f.write('{"sig_code": "div", "side": "bear", "price": 51000.0, "time_ms": 2000, "bar_type": "time", "bar_label": "15m"}')

        loaded_before = SignalQueueManager.load_all_signals(self.symbol)
        assert len(loaded_before) == 2  # trap and div parsed successfully

        # Append new signal
        new_signal = [{
            "sig_code": "ratio_p",
            "side": "bull",
            "price": 52000.0,
            "time_ms": 3000,
            "bar_type": "time",
            "bar_label": "15m"
        }]
        SignalQueueManager.append_signals(self.symbol, new_signal)

        # Ensure newline guard inserted a newline, preventing line welding
        loaded_after = SignalQueueManager.load_all_signals(self.symbol)
        codes = [s["sig_code"] for s in loaded_after]
        assert "trap" in codes
        assert "div" in codes
        assert "ratio_p" in codes
        assert len(loaded_after) == 3

    def test_binary_garbage_and_invalid_utf8_recovery(self):
        """
        Verify that stray bytes/invalid UTF-8 in a ledger do not discard the entire file.
        """
        with open(self.p_jsonl, "wb") as f:
            f.write(b'{"sig_code": "trap", "side": "bull", "price": 50000.0, "time_ms": 1000, "bar_type": "time", "bar_label": "15m"}\n')
            f.write(b"\xff\xfe\x00\x12\n")  # invalid binary sequence
            f.write(b'{"sig_code": "div", "side": "bear", "price": 51000.0, "time_ms": 2000, "bar_type": "time", "bar_label": "15m"}\n')

        loaded = SignalQueueManager.load_all_signals(self.symbol)
        assert len(loaded) == 2
        assert loaded[0]["sig_code"] == "trap"
        assert loaded[1]["sig_code"] == "div"

    def test_legacy_queue_json_migration_no_self_lock(self):
        """
        Verify that migrating a legacy queue.json into signals.jsonl does not trigger
        recursive lock timeouts or Writer busy exceptions.
        """
        legacy_records = [
            {"sig_code": "trap", "side": "bull", "price": 60000.0, "time_ms": 1000, "bar_type": "time", "bar_label": "15m"},
            {"sig_code": "abs", "side": "bear", "price": 60500.0, "time_ms": 2000, "bar_type": "time", "bar_label": "15m"}
        ]
        self.p_legacy.write_text(json.dumps(legacy_records), encoding="utf-8")

        # Load should auto-migrate without timeout
        t0 = time.time()
        signals = SignalQueueManager.load_all_signals(self.symbol)
        dur = time.time() - t0

        assert dur < 1.0, f"Migration took too long ({dur:.2f}s), possible lock contention"
        assert len(signals) == 2
        assert self.p_jsonl.exists()


class TestTradeEvaluatorEdgeCases:
    """Stress tests edge cases and math failure modes in trade_evaluator.py."""

    def test_shannon_entropy_5050_deadlock(self):
        """Tests 50/50 LONG/SHORT split produces exact Shannon entropy of 1.0."""
        dirs_5050 = ["LONG"] * 5 + ["SHORT"] * 5
        e = calculate_shannon_entropy(dirs_5050)
        assert abs(e - 1.0) < 1e-6

        # Evaluator behavior on 50/50 deadlock with neutral trigger
        res = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_DEADLOCK",
            trigger_signals=[{"sig_code": "p_spike", "direction": "NEUTRAL", "price": 50000.0, "time_ms": 1000, "priority": 6}],
            queue=[
                {"direction": "LONG", "sig_code": "div", "price": 50000.0, "time_ms": 1000, "priority": 5},
                {"direction": "SHORT", "sig_code": "div", "price": 50000.0, "time_ms": 1000, "priority": 5},
            ],
            current_price=50000.0
        )
        assert res is None  # Pure deadlock cleanly yields None

    def test_shannon_entropy_contradiction_gate(self):
        """50/50 split with non-trap trigger should trigger PASS_CONTRADICTION."""
        res = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_CONTRADICTION",
            trigger_signals=[{"sig_code": "div", "direction": "LONG", "price": 50000.0, "time_ms": 1000, "priority": 5}],
            queue=[
                {"direction": "LONG", "sig_code": "div", "price": 50000.0, "time_ms": 1000, "priority": 5},
                {"direction": "SHORT", "sig_code": "div", "price": 50000.0, "time_ms": 1000, "priority": 5},
            ],
            current_price=50000.0
        )
        assert res is not None
        assert res["status"] == "PASS_CONTRADICTION"

    def test_invalid_reference_prices(self):
        """Ensure zero, negative, and NaN reference prices are safely rejected."""
        # Zero price
        res_zero = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_ZERO_PX",
            trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 0.0, "time_ms": 1000, "priority": 1}],
            queue=[{"sig_code": "trap", "direction": "LONG", "price": 0.0, "time_ms": 1000, "priority": 1}],
            current_price=0.0
        )
        assert res_zero is None

        # NaN price
        res_nan = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_NAN_PX",
            trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": float("nan"), "time_ms": 1000, "priority": 1}],
            queue=[{"sig_code": "trap", "direction": "LONG", "price": float("nan"), "time_ms": 1000, "priority": 1}],
            current_price=float("nan")
        )
        assert res_nan is None

        # Negative price
        res_neg = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_NEG_PX",
            trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": -100.0, "time_ms": 1000, "priority": 1}],
            queue=[{"sig_code": "trap", "direction": "LONG", "price": -100.0, "time_ms": 1000, "priority": 1}],
            current_price=-100.0
        )
        assert res_neg is None

    def test_missing_and_empty_candle_files(self):
        """Ensure evaluator handles missing or 0-row candle parquet files gracefully."""
        # Missing candle file
        res_missing = MultiPillarTradeEvaluator.evaluate_symbol(
            symbol="TEST_NO_CANDLE_FILE",
            trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
            queue=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
            current_price=100.0
        )
        assert res_missing is not None
        assert res_missing["atr_15m"] > 0

        # Empty candle file
        p_empty = CANDLE_DIR / "TEST_EMPTY_CANDLES.parquet"
        pd.DataFrame().to_parquet(p_empty)
        try:
            res_empty = MultiPillarTradeEvaluator.evaluate_symbol(
                symbol="TEST_EMPTY_CANDLES",
                trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 250.0, "time_ms": 1000, "priority": 1}],
                queue=[{"sig_code": "trap", "direction": "LONG", "price": 250.0, "time_ms": 1000, "priority": 1}],
                current_price=250.0
            )
            assert res_empty is not None
            assert res_empty["atr_15m"] > 0
        finally:
            p_empty.unlink(missing_ok=True)

    def test_zero_distance_stop_loss_prevention_across_all_price_scales(self):
        """
        Verify that sub-cent assets (e.g. PEPE $0.000001) never round to entry == sl,
        guaranteeing actual_risk > 0 and preventing ZeroDivisionError in R:R.
        """
        test_prices = [0.000001, 0.00005, 0.001, 0.5, 10.0, 1500.0, 65000.0]
        for px in test_prices:
            for direction in ["LONG", "SHORT"]:
                side = "bull" if direction == "LONG" else "bear"
                res = MultiPillarTradeEvaluator.evaluate_symbol(
                    symbol=f"TEST_RR_{direction}_{px}",
                    trigger_signals=[{"sig_code": "trap", "direction": direction, "side": side, "price": px, "time_ms": 1000, "priority": 1}],
                    queue=[{"sig_code": "trap", "direction": direction, "side": side, "price": px, "time_ms": 1000, "priority": 1}],
                    current_price=px
                )
                assert res is not None
                entry = res["entry_price"]
                sl = res["stop_loss"]
                tp = res["take_profit"]
                risk = res["risk_amount"]
                rr = res["risk_reward_ratio"]

                assert entry != sl, f"[{direction} @ {px}] Zero distance stop loss: entry={entry}, sl={sl}"
                assert risk > 0.0, f"[{direction} @ {px}] Risk amount is zero or negative: {risk}"
                assert rr > 0.0 or res["status"] == "PASS_INVALID_RISK"
                assert not math.isnan(rr)
                assert not math.isinf(rr)


class TestPipelineScheduleAndTabCleanup:
    """Verifies TDMA buffer intervals and Chrome tab cleanup under scraper failure."""

    def test_tdma_buffer_safety(self):
        """Verify that all tasks have >= 2.0 minutes buffer and TV liquidations align with 15m closes."""
        sorted_mins = sorted(SCHEDULE.keys())
        assert len(sorted_mins) == 17

        # Check 15m candle close alignment
        for m in (0, 15, 30, 45):
            assert m in SCHEDULE
            script, desc = SCHEDULE[m]
            assert script == "4_coinglass_tv_scraper.py"

        # Check all buffers
        for i in range(len(sorted_mins)):
            m_curr = sorted_mins[i]
            m_next = sorted_mins[(i + 1) % len(sorted_mins)]
            buf = (m_next - m_curr) if m_next > m_curr else (m_next + 60 - m_curr)
            assert buf >= 2, f"Buffer between :{m_curr:02d} and :{m_next:02d} is {buf} min (< 2 min)!"
            assert buf <= 7, f"Buffer between :{m_curr:02d} and :{m_next:02d} is {buf} min (> 7 min)!"

    @patch("urllib.request.urlopen")
    def test_orphaned_tab_cleanup_under_simulated_crash(self, mock_urlopen):
        """
        Simulate a scraper crash leaving both an active CoinGlass route tab
        and an uninitialized about:blank tab open.
        Assert that both are closed, while baseline pre-existing user tabs remain untouched.
        """
        baseline_tabs = {"user_tab_pinned_1", "user_tab_pinned_2"}

        mock_tabs = [
            {"id": "user_tab_pinned_1", "type": "page", "url": "https://tradingview.com", "title": "Chart"},
            {"id": "user_tab_pinned_2", "type": "page", "url": "https://google.com", "title": "Search"},
            {"id": "scraper_cg_tab", "type": "page", "url": "https://www.coinglass.com/tv/Binance_BTCUSDT", "title": "CoinGlass"},
            {"id": "scraper_blank_tab", "type": "page", "url": "about:blank", "title": ""},
        ]

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_tabs).encode()
        mock_resp.__enter__.return_value = mock_resp

        closed_tabs = []

        def side_effect(url_or_req, timeout=None):
            url_str = url_or_req if isinstance(url_or_req, str) else url_or_req.full_url
            if "/close/" in url_str:
                closed_tabs.append(url_str)
                ret = MagicMock()
                ret.__enter__.return_value = ret
                return ret
            return mock_resp

        mock_urlopen.side_effect = side_effect

        cleanup_scraper_orphaned_tabs(baseline_tabs)

        # Both orphaned tabs must be closed
        assert len(closed_tabs) == 2
        assert any("scraper_cg_tab" in u for u in closed_tabs)
        assert any("scraper_blank_tab" in u for u in closed_tabs)
        # Baseline user tabs must never be closed
        assert not any("user_tab_pinned_1" in u for u in closed_tabs)
        assert not any("user_tab_pinned_2" in u for u in closed_tabs)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
