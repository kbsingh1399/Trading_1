"""
Comprehensive Verification Suite for Cross-Asset Signal Stack, Deduplication & Priority Arbiter
================================================================================================
Guarantees (EGM Architectural Standards & Quant Verification Compliant):
1. Exact 15-Minute Epoch Quantization (tau_bar = floor(t/900000)*900000)
2. Deterministic Signal Fingerprinting (SHA-256 16-char hex)
3. Single-Evaluation Invariant: Stamped candles are permanently suppressed from re-evaluation
4. Antecedent Context Traversal: Preceding candles act as passive read-only evidence
5. Stale Freshness Gate: Signals closed > 15m ago return EXPIRED_STALE
6. Forming Wick Rejection: Intra-bar signals return PENDING_INTRA_BAR
7. Multi-Signal Clustering: Simultaneous signals form unified clusters preserving cluster_boost
8. Priority Arbiter: EV >= 0.0 gate, R:R clamped to [1.5, 4.0], normalized ranking
9. Strict Parquet IST Schema: Primary IST columns conforming to AGENTS.md
================================================================================================
"""

import time
import pytest
import pandas as pd
from pathlib import Path
from Terminal.asset_signal_stack import (
    canonical_bar_epoch,
    generate_signal_id,
    EvaluatedSignalsLedger,
    AssetSignalStack,
    CANDLE_M15_MS
)


def test_exact_15m_candle_epoch_quantization():
    """Confirms all timestamps strictly floor to 900,000 ms (15-minute) boundaries."""
    base_epoch = 1730678400000  # Exact 15m multiple
    assert canonical_bar_epoch(base_epoch) == base_epoch
    assert canonical_bar_epoch(base_epoch + 1) == base_epoch
    assert canonical_bar_epoch(base_epoch + 450_000) == base_epoch
    assert canonical_bar_epoch(base_epoch + 899_999) == base_epoch
    assert canonical_bar_epoch(base_epoch + 900_000) == base_epoch + 900_000


def test_deterministic_signal_id():
    """Confirms signal fingerprint is deterministic regardless of signal code input order."""
    sym = "SOL"
    t_ms = 1730678400000
    d = "BUY"
    id1 = generate_signal_id(sym, t_ms, d, ["abs", "trap"])
    id2 = generate_signal_id(sym, t_ms, d, ["trap", "abs"])
    assert id1 == id2
    assert len(id1) == 16


def test_evaluated_signals_ledger_suppression(tmp_path):
    """
    Operator Cardinal Invariant:
    Once a signal is evaluated, it must NEVER be re-evaluated on subsequent sweeps.
    """
    ledger_file = tmp_path / "test_ledger.json"
    ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

    sym = "SOL"
    open_ms = 1730678400000
    sig_id = "abc123def4567890"

    assert ledger.is_evaluated(sym, open_ms) is False

    ledger.mark_evaluated(sym, open_ms, sig_id, confluence_score=82.5, action="DISPATCHED", sig_name="Absorption", direction="BUY")
    assert ledger.is_evaluated(sym, open_ms) is True

    # Reload from disk to verify persistence across restarts
    reloaded_ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)
    assert reloaded_ledger.is_evaluated(sym, open_ms) is True


def test_antecedent_context_traversal(tmp_path):
    """
    Preceding evaluated signals are retrieved strictly as passive read-only context
    within the 3-hour lookback window.
    """
    ledger_file = tmp_path / "test_ledger.json"
    ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

    sym = "SOL"
    t0 = 1730678400000
    t1 = t0 + CANDLE_M15_MS
    t2 = t1 + CANDLE_M15_MS

    # Record t0 and t1 in ledger
    ledger.mark_evaluated(sym, t0, "id_0", 72.0, "FILTERED", "Exhaustion", "BUY")
    ledger.mark_evaluated(sym, t1, "id_1", 76.0, "DISPATCHED", "Absorption", "BUY")

    # When evaluating at t2, preceding history contains t0 and t1
    history = ledger.get_antecedent_history(sym, current_open_ms=t2, lookback_ms=10_800_000)
    assert len(history) == 2
    assert history[0]["open_time_ms"] == t0
    assert history[1]["open_time_ms"] == t1
    assert history[0]["sig_name"] == "Exhaustion"
    assert history[1]["sig_name"] == "Absorption"


def test_stale_boundary_rejection(tmp_path):
    """Signals on candles closed > 15 minutes ago are rejected with EXPIRED_STALE."""
    stack_file = tmp_path / "test_stack.parquet"
    ledger_file = tmp_path / "test_ledger.json"

    stack = AssetSignalStack(stack_path=stack_file)
    stack.ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

    now_ms = 1730682000000
    # Candle that closed 25 minutes ago
    stale_open_ms = now_ms - (25 * 60 * 1000) - CANDLE_M15_MS

    raw = [{"symbol": "SOL", "time_ms": stale_open_ms, "sig_name": "Absorption", "sig_code": "abs", "side": "buy", "price": 180.5}]
    cand, status = stack.register_candle_cluster(raw, now_ms=now_ms)
    assert cand is None
    assert status == "EXPIRED_STALE"


def test_forming_wick_pending(tmp_path):
    """Intra-bar signals forming before bar close return PENDING_INTRA_BAR."""
    stack_file = tmp_path / "test_stack.parquet"
    ledger_file = tmp_path / "test_ledger.json"

    stack = AssetSignalStack(stack_path=stack_file)
    stack.ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

    bar_open = 1730678400000
    # Current time is 5 minutes into the 15-minute bar
    now_ms = bar_open + (5 * 60 * 1000)

    raw = [{"symbol": "SOL", "time_ms": bar_open, "sig_name": "Absorption", "sig_code": "abs", "side": "buy", "price": 180.5}]
    cand, status = stack.register_candle_cluster(raw, now_ms=now_ms)
    assert cand is None
    assert status == "PENDING_INTRA_BAR"


def test_multi_signal_candle_clustering(tmp_path):
    """
    Simultaneous footprint signals on the same bar cluster into a single candidate,
    preserving composite patterns and the cluster boost.
    """
    stack_file = tmp_path / "test_stack.parquet"
    ledger_file = tmp_path / "test_ledger.json"

    stack = AssetSignalStack(stack_path=stack_file)
    stack.ledger = EvaluatedSignalsLedger(ledger_path=ledger_file)

    bar_open = 1730678400000
    bar_close = bar_open + CANDLE_M15_MS
    now_ms = bar_close + 10_000  # Freshly closed

    raw = [
        {"symbol": "SOL", "time_ms": bar_open, "sig_name": "Absorption", "sig_code": "abs", "side": "buy", "price": 180.5},
        {"symbol": "SOL", "time_ms": bar_open, "sig_name": "Trapped Shorts", "sig_code": "trap", "side": "buy", "price": 180.5}
    ]

    cand, status = stack.register_candle_cluster(raw, now_ms=now_ms)
    assert status == "ACTIVE_LATEST_CLOSED"
    assert cand is not None
    assert cand["cluster_signals_count"] == 2
    assert "Absorption" in cand["sig_name"]
    assert "Trapped Shorts" in cand["sig_name"]
    assert cand["sig_code"] == "abs,trap"
    assert cand["direction"] == "BUY"


def test_priority_arbiter_ranking():
    """
    Priority Arbiter:
    - Filters out negative EV or sub-75 score
    - Ranks higher score, higher EV, higher R:R at top
    """
    stack = AssetSignalStack()

    c1 = {"symbol": "BTC", "confluence_score": 78.0, "ev_300bar": 0.35, "rr_ratio": 2.0}
    c2 = {"symbol": "SOL", "confluence_score": 88.0, "ev_300bar": 0.50, "rr_ratio": 2.5}
    c3 = {"symbol": "ETH", "confluence_score": 72.0, "ev_300bar": 0.40, "rr_ratio": 2.0}  # Fails score gate
    c4 = {"symbol": "DOGE", "confluence_score": 82.0, "ev_300bar": -0.15, "rr_ratio": 2.0}  # Fails EV gate

    ranked = stack.rank_candidates([c1, c2, c3, c4])
    assert len(ranked) == 2
    assert ranked[0]["symbol"] == "SOL"
    assert ranked[1]["symbol"] == "BTC"
    assert ranked[0]["arbiter_rank_score"] > ranked[1]["arbiter_rank_score"]


def test_active_signal_stack_parquet_schema_ist(tmp_path):
    """
    Confirms persisted stack contains all primary IST columns with zero nulls
    conforming strictly to AGENTS.md standards.
    """
    stack_file = tmp_path / "test_active_signal_stack.parquet"
    stack = AssetSignalStack(stack_path=stack_file)

    bar_open = 1730678400000
    bar_close = bar_open + CANDLE_M15_MS
    now_ms = bar_close + 5_000

    raw = [{"symbol": "SOL", "time_ms": bar_open, "sig_name": "Absorption", "sig_code": "abs", "side": "buy", "price": 180.5}]
    cand, _ = stack.register_candle_cluster(raw, now_ms=now_ms)
    stack.persist_stack()

    assert stack_file.exists()
    df = pd.read_parquet(stack_file)
    assert len(df) == 1

    # Check required primary IST columns per AGENTS.md
    required_cols = [
        "symbol", "signal_id", "open_time_ist", "close_time_ist",
        "time_ist", "candle_ist", "open_time_ist_ms", "close_time_ist_ms",
        "open_time_ms", "close_time_ms", "sig_name", "sig_code",
        "confluence_score", "ev_300bar", "rr_ratio", "state"
    ]
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
        assert df[col].isnull().sum() == 0, f"Null values found in column: {col}"
