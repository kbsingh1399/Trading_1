"""
========================================================================================
Phase 2 Unit & Integration Tests — Portfolio Survival Budgeting & Alpha Expansion
========================================================================================
Covers the four Phase 2 patches (test ids P1..P4 to avoid collision with Round 1 A/W/D/E):

  P1  Dynamic Survival Risk Budgeting & Committed-Risk Aggregation
      (Terminal/risk_envelope.py, Terminal/position_anchors.py,
       Terminal/delta_exchange_live_trader.py)
  P2  Underwater Whale Entry Pockets & Inter-Refresh Whale Delta Tracking
      (Terminal/hyperdash_intel_engine.py, Terminal/realtime_signal_evaluator.py)
  P3  Independent Pillar-Minimum Gate (Terminal/realtime_signal_evaluator.py)
  P4  Asynchronous Bounded Fan-Out (5_wake_on_footprint_signal.py,
      Terminal/deepmind_timesfm_forecaster.py)

All tests are fully offline: no network, no exchange credentials, no live dispatch.
========================================================================================
"""

import asyncio
import os
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, AsyncMock

import numpy as np
import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    MAX_RISK_USD,
    MIN_RISK_USD,
    STRESS_MULT,
    MARGIN_BUFFER_FLOOR_PCT,
    DEFAULT_MARGIN_PER_RISK_USD,
    budget_per_trade,
    budget_per_trade_dynamic,
    aggregate_committed_risk,
)
from Terminal.hyperdash_intel_engine import (
    compute_underwater_entry_pockets,
    compute_whale_flow_delta,
    _empty_whale_flow,
    HyperdashIntelEngine,
)
from Terminal.realtime_signal_evaluator import (
    RealtimeSignalEvaluator,
    detect_market_pillar_hits,
    MIN_INDEPENDENT_MARKET_PILLARS,
)


# =====================================================================
# PATCH P1 — Dynamic Survival Risk Budgeting & Committed-Risk Aggregation
# =====================================================================

@pytest.fixture(autouse=True)
def _set_legacy_prop_floor(monkeypatch):
    """P1 tests were calibrated for FundingPips challenge with $4,775 hard floor."""
    import Terminal.risk_envelope as re
    monkeypatch.setattr(re, "HARD_FLOOR_USD", 4775.00)

def _tier0_anchor(risk_usd=20.0, sym="BTCUSD", cv=1.0, size=10):
    return {
        "symbol": sym, "current_tier": 0, "size": size, "contract_val": cv,
        "entry_price": 100.0, "initial_sl": 98.0, "last_ratchet_price": 98.0,
        "initial_risk_usd": risk_usd, "initial_risk_dist": 2.0, "direction": "BUY",
    }


def test_p1_budget_dynamic_full_envelope_no_anchors():
    """Fresh $5,000 account: buffer $225 / survival divisor 20 -> $11.25."""
    budget, ok, reason = budget_per_trade_dynamic(5000.0)
    assert ok is True
    assert budget == pytest.approx(11.25, abs=0.01)
    assert "Free Buffer: USD 225.00" in reason


def test_p1_budget_dynamic_anchor_subtracts_committed_risk():
    """One tier-0 anchor ($20 risk) reserves $25.00 stressed -> budget shrinks to $10.00."""
    anchors = {"BTCUSD": _tier0_anchor(risk_usd=20.0)}
    budget, ok, reason = budget_per_trade_dynamic(5000.0, anchors)
    assert ok is True
    # free = 225 - 25 = 200; 200/20 = 10.00
    assert budget == pytest.approx(10.00, abs=0.01)
    assert "AnchorCommitted=$25.00" in reason


def test_p1_budget_dynamic_ratcheted_anchor_frees_envelope():
    """A tier-2 anchor has SL locked beyond breakeven: worst case is locked profit,
    so its outstanding risk is 0 and the full envelope is restored."""
    anchor = _tier0_anchor(risk_usd=20.0)
    anchor["current_tier"] = 2
    anchor["last_ratchet_price"] = 101.0  # locked profit side
    budget, ok, _ = budget_per_trade_dynamic(5000.0, {"BTCUSD": anchor})
    assert ok is True
    assert budget == pytest.approx(11.25, abs=0.01)


def test_p1_budget_dynamic_margin_floor_fail_closed():
    """Free margin below the 20% floor -> fail-closed, zero budget."""
    budget, ok, reason = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=950.0, margin_per_risk_usd=4.0
    )
    assert ok is False and budget == 0.0
    assert "MARGIN_FLOOR_BREACH" in reason


def test_p1_budget_dynamic_margin_clamp_exact_geometry():
    """With exact geometry (margin_per_risk known), the budget is clamped so the
    implied margin lock-up never breaches the 20% free-margin floor."""
    # balance 5000, floor 1000, avail 1500 -> headroom 500; 4 margin-USD per risk-USD
    budget, ok, reason = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=1500.0, margin_per_risk_usd=4.0
    )
    assert ok is True
    assert budget == pytest.approx(11.25, abs=0.01)  # survival budget below margin cap (125)

    # Tight headroom: avail 1100 -> headroom 100 -> margin cap 25 -> still above 11.25
    budget, ok, reason = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=1100.0, margin_per_risk_usd=4.0
    )
    assert ok is True
    assert budget == pytest.approx(11.25, abs=0.01)

    # Headroom so small the cap drops below the survival budget:
    # avail 1045 -> headroom 45 -> cap 11.25 == budget (boundary)
    budget, ok, _ = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=1045.0, margin_per_risk_usd=4.0
    )
    assert ok is True and budget == pytest.approx(11.25, abs=0.01)

    # cap 10.00 (< 11.25) -> clamped; still >= MIN_RISK_USD
    budget, ok, reason = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=1040.0, margin_per_risk_usd=4.0
    )
    assert ok is True and budget == pytest.approx(10.00, abs=0.01)
    assert "MarginFloorClamp" in reason

    # cap below MIN_RISK_USD -> inadmissible
    budget, ok, reason = budget_per_trade_dynamic(
        5000.0, {}, free_margin_usd=1015.0, margin_per_risk_usd=4.0
    )
    assert ok is False and budget == 0.0
    assert "MARGIN_HEADROOM_BELOW_MIN_FLOOR" in reason


def test_p1_aggregate_committed_risk_legacy_anchor_fail_closed():
    """Legacy anchor without initial_risk_usd/contract_val must be booked at the
    mandate cap (never understate exposure)."""
    legacy = {"XRPUSD": {"current_tier": 0, "size": 5, "entry_price": 0.5}}
    committed, detail = aggregate_committed_risk(legacy)
    assert committed == pytest.approx(25.0 * STRESS_MULT)
    assert "XRPUSD" in detail["legacy_unknown"]


def test_p1_aggregate_committed_risk_audit_detail():
    """Mixed book: tier-0 anchors stressed, tier-2 locked off, totals audited."""
    book = {
        "BTCUSD": _tier0_anchor(risk_usd=20.0),
        "ETHUSD": {**_tier0_anchor(risk_usd=8.0, sym="ETHUSD"), "current_tier": 2},
    }
    committed, detail = aggregate_committed_risk(book)
    assert committed == pytest.approx(20.0 * STRESS_MULT)
    assert detail["anchors"]["BTCUSD"] == 20.0
    assert detail["locked_off_risk"] == ["ETHUSD"]
    assert detail["total_unstressed_usd"] == 20.0


def test_p1_budget_per_trade_scalar_signature_backward_compat():
    """Legacy 3-arg scalar signature must behave exactly as before."""
    budget, ok, _ = budget_per_trade(5000.0, 5000.0, 0.0)
    assert ok is True and budget == 11.25
    budget, ok, reason = budget_per_trade(4776.0, 4776.0, 0.0)
    assert ok is False and "PROSPECTIVE_HALT" in reason  # free $1 <= $20 cushion


def test_p1_anchor_registration_persists_risk_usd(tmp_path, monkeypatch):
    """register_position_anchor persists contract_val + initial_risk_usd for the
    committed-risk aggregator, and get_or_recover_anchor passes contract_val through."""
    import Terminal.position_anchors as pa
    anchors_file = tmp_path / "live_position_anchors.json"
    monkeypatch.setattr(pa, "ANCHORS_FILE", anchors_file)
    monkeypatch.setattr(pa, "DB_DIR", tmp_path)

    anchor = pa.register_position_anchor(
        symbol="XRPUSD", product_id=4, direction="BUY",
        entry_price=0.50, initial_sl=0.48, initial_tp=0.54,
        size=100, contract_val=1.0
    )
    assert anchor["contract_val"] == 1.0
    assert anchor["initial_risk_usd"] == pytest.approx(2.00)  # 0.02 * 100 * 1.0
    stored = pa.load_position_anchors()
    assert stored["XRPUSD"]["initial_risk_usd"] == 2.00

    # Recovery path passes contract_val through (existing param, now persisted)
    rec = pa.get_or_recover_anchor(
        symbol="BTCUSD", product_id=1, direction="BUY",
        entry_price=100.0, cur_sl_price=98.0, cur_tp_price=104.0,
        size=10, contract_val=0.001, max_risk_usd=25.0
    )
    assert rec["contract_val"] == 0.001
    assert rec["initial_risk_usd"] == pytest.approx(0.02, abs=1e-6)


def test_p1_dynamic_budget_blocks_low_equity_dispatch():
    """The Delta dispatch path now rejects accounts below the hard floor BEFORE
    sizing (fail-closed): simulate the guarded wiring contract."""
    budget, ok, reason = budget_per_trade_dynamic(100.0, {})
    assert ok is False and budget == 0.0
    assert "EQUITY_BREACH" in reason


# =====================================================================
# PATCH P2 — Underwater Whale Entry Pockets & Inter-Refresh Whale Flow
# =====================================================================

def _whale(addr, side, notional, entry, upnl):
    return {
        "address": addr, "label": f"W-{addr[:6]}", "size": (500 if side == "LONG" else -400),
        "side": side, "notional_usd": notional, "entry_price": entry,
        "liquidation_price": 80.0, "unrealized_pnl": upnl,
    }


def test_p2_pocket_overhead_supply_clustering():
    """Two underwater LONG whales with near-identical entries above mark cluster
    into a single OVERHEAD_SUPPLY pocket."""
    whales = [
        _whale("0xAAA111...", "LONG", 600_000.0, 102.0, -8_000.0),
        _whale("0xBBB222...", "LONG", 590_000.0, 102.3, -9_000.0),
    ]
    pockets = compute_underwater_entry_pockets(whales, 100.0)
    assert len(pockets) == 1
    p = pockets[0]
    assert p["pocket_type"] == "OVERHEAD_SUPPLY"
    assert p["side"] == "LONG"
    assert p["whale_count"] == 2
    assert p["trapped_notional_usd"] == pytest.approx(1_190_000.0)
    assert 2.0 < p["dist_pct"] < 2.3
    # weighted mean entry between the two entries
    assert 102.0 < p["pocket_price"] < 102.3


def test_p2_pocket_support_floor_below_mark():
    """Underwater SHORT whale (entry below mark) forms a SUPPORT_FLOOR pocket."""
    whales = [_whale("0xCCC333...", "SHORT", 550_000.0, 95.5, -7_000.0)]
    pockets = compute_underwater_entry_pockets(whales, 100.0)
    assert len(pockets) == 1
    assert pockets[0]["pocket_type"] == "SUPPORT_FLOOR"
    assert pockets[0]["side"] == "SHORT"
    assert pockets[0]["dist_pct"] == pytest.approx(-4.5)


def test_p2_pocket_excludes_profitable_and_far_entries():
    """Profitable whales are never trapped supply; entries beyond POCKET_FAR_PCT
    are unreachable for the trade horizon and excluded."""
    whales = [
        _whale("0xDDD444...", "LONG", 800_000.0, 99.5, +5_000.0),   # profitable
        _whale("0xEEE555...", "LONG", 700_000.0, 109.0, -30_000.0),  # 9% above: too far
    ]
    assert compute_underwater_entry_pockets(whales, 100.0) == []


def test_p2_flow_distribution_state_with_new_short_entry():
    """W1 accumulates +$300K long, W5 enters new $900K short -> net -$600K,
    score < -25 -> DISTRIBUTING with 1 accumulate + 1 new entry."""
    prev = [
        _whale("0xAAA111...", "LONG", 600_000.0, 100.0, 1_000.0),
        _whale("0xBBB222...", "LONG", 590_000.0, 101.0, 500.0),
    ]
    cur = [
        {**_whale("0xAAA111...", "LONG", 900_000.0, 100.0, 1_500.0)},  # +50% accumulate
        {**_whale("0xBBB222...", "LONG", 590_000.0, 101.0, 500.0)},    # holding
        _whale("0xFFF666...", "SHORT", 900_000.0, 100.5, -1_000.0),      # NEW short
    ]
    flow = compute_whale_flow_delta(prev, cur, time.time() - 95.0, time.time())
    assert flow["data_valid"] is True
    assert flow["flow_delta_usd"] == pytest.approx(-600_000.0)
    assert flow["flow_state"] == "DISTRIBUTING"
    assert flow["accumulating"] == 1
    assert flow["new_entries"] == 1
    assert -30.0 < flow["flow_score"] <= -25.0


def test_p2_flow_closed_exit_counts_full_removal():
    """A vanished long removes its full signed notional from the flow; the
    surviving whale holds (no net change from it)."""
    prev = [_whale("0xAAA111...", "LONG", 600_000.0, 100.0, 1_000.0),
            _whale("0xBBB222...", "LONG", 590_000.0, 101.0, 500.0)]
    cur = [_whale("0xBBB222...", "LONG", 590_000.0, 101.0, 500.0)]
    flow = compute_whale_flow_delta(prev, cur, time.time() - 90.0, time.time())
    assert flow["closed_exits"] == 1
    assert flow["flow_delta_usd"] == pytest.approx(-600_000.0)


def test_p2_flow_window_guards():
    """<30s window: same-tick noise -> invalid. >900s window: stale baseline ->
    low_confidence flag set."""
    prev = [_whale("0xAAA111...", "LONG", 600_000.0, 100.0, 0.0)]
    now = time.time()
    fast = compute_whale_flow_delta(prev, prev, now - 5.0, now)
    assert fast["data_valid"] is False
    assert fast["flow_state"] == "INVALID_WINDOW_TOO_FAST"

    stale = compute_whale_flow_delta(prev, prev, now - 1200.0, now)
    assert stale["low_confidence"] is True
    assert stale["data_valid"] is True


def test_p2_engine_cache_single_flight_and_flow_refresh(monkeypatch):
    """Per-coin lock + double-checked cache: N concurrent readers of the same coin
    trigger exactly ONE upstream fetch; second fresh fetch produces a flow delta."""
    eng = HyperdashIntelEngine()
    calls = []
    release = threading.Event()

    def fake_fetch(coin, limit=10):
        calls.append(coin)
        release.wait(1.0)  # hold the first fetcher inside the lock window
        return [_whale("0xAAA111...", "LONG", 600_000.0, 100.0, 500.0)], True

    monkeypatch.setattr(eng, "_fetch_top_whales", fake_fetch)
    eng._ticker_positions_cache.clear()
    eng._whale_flow_snapshots.clear()

    # First fetch establishes the baseline (NO_BASELINE flow)
    whales1, ok1, flow1 = eng._fetch_top_whales_cached("BTC")
    assert ok1 is True and len(whales1) == 1
    assert flow1["flow_state"] == "NO_BASELINE" and flow1["data_valid"] is False
    assert len(calls) == 1

    # Cache hit path: no new fetch, flow reused
    whales2, ok2, flow2 = eng._fetch_top_whales_cached("BTC")
    assert len(calls) == 1
    assert flow2["flow_state"] == "NO_BASELINE"

    # Expire the cache and fire 4 CONCURRENT readers: exactly one upstream fetch
    calls.clear()
    release.set()
    past = time.time() - (HyperdashIntelEngine and 91.0)
    eng._ticker_positions_cache["BTC"] = (past, [], False)  # force expiry via negative TTL
    eng._whale_flow_snapshots.clear()

    def fast_fetch(coin, limit=10):
        calls.append(coin)
        time.sleep(0.08)
        return [_whale("0xAAA111...", "LONG", 800_000.0, 100.0, 700.0)], True

    monkeypatch.setattr(eng, "_fetch_top_whales", fast_fetch)
    threads = [threading.Thread(target=lambda: eng._fetch_top_whales_cached("BTC")) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(calls) == 1, f"single-flight violated: {len(calls)} upstream fetches"


def test_p2_engine_get_asset_intel_exposes_new_features(monkeypatch):
    """get_asset_intel output must carry entry_pockets + whale_flow (live API surface)."""
    eng = HyperdashIntelEngine()
    eng._last_participation_ts = time.time()
    eng._coin_features = {
        "BTC": {"coin": "BTC", "data_valid": True, "smart_bias": 5.0, "crowd_bias": -2.0,
                "net_divergence": 7.0, "open_interest_usd": 1e9, "intel_score": 55.0,
                "intel_direction": "NEUTRAL", "signals": [], "updated_at": time.time()}
    }
    monkeypatch.setattr(eng, "_fetch_top_whales_cached",
                        lambda coin: ([_whale("0xAAA111...", "LONG", 600_000.0, 102.0, -5_000.0)], True, _empty_whale_flow()))
    res = eng.get_asset_intel("BTCUSD", mark_price_hint=100.0)
    assert res["data_valid"] is True
    assert "entry_pockets" in res and "whale_flow" in res
    assert len(res["entry_pockets"]) == 1
    assert res["entry_pockets"][0]["pocket_type"] == "OVERHEAD_SUPPLY"
    assert res["whale_flow"]["flow_state"] == "NO_BASELINE"


# =====================================================================
# PATCH P3 — Independent Pillar-Minimum Gate
# =====================================================================

def test_p3_pillar_detector_identifies_two_market_pillars():
    """CVD flow (P1) + whale book imbalance (P5) -> two independent hits."""
    hits = detect_market_pillar_hits(
        direction="BUY",
        whale_imbalance=0.70,
        cvd_slope="BULLISH_FLOW",
        near_field_shorts=50.0,
    )
    assert "P1_FOOTPRINT_FLOW" in hits and "P5_WHALE_SWEEP" in hits
    assert len(hits) >= 2


def test_p3_pillar_detector_neutral_market_yields_no_hits():
    """Neutral microstructure (no market confirmation) yields zero pillars —
    base+model+memory alone can never pass the gate."""
    hits = detect_market_pillar_hits(direction="BUY")
    assert hits == {}


def test_p3_pillar_detector_p4_sweep_targets_and_crowd_squeeze():
    hits = detect_market_pillar_hits(
        direction="BUY",
        sweep_targets=[{"side": "SHORT", "dist_pct": 1.2, "sweep_score": 5e5}],
        hd_signals=["CROWD_SQUEEZE_LONG"],
    )
    assert hits.get("P4_LIQUIDATION_MAGNETS") == 10.0


def _make_evaluator(whale_imb, cvd, near_field, auction_extra=None):
    ev = object.__new__(RealtimeSignalEvaluator)
    ev.olap = MagicMock()
    ev.olap.query_whale_walls.return_value = {"data_valid": True, "whale_imbalance_bid_ratio": whale_imb}
    ev.olap.query_liquidation_confluence.return_value = {"data_valid": True, "near_field_shorts_pct": near_field}
    ev.olap.query_cvd_trend.return_value = {"data_valid": True, "cvd_slope": cvd}
    ev.olap.query_key_liquidation_levels.return_value = {}
    base_auction = {
        "data_valid": True, "day_vah": 103.0, "day_val": 101.0, "day_vwap": 102.5,
        "mtf_bias": "HTF_NEUTRAL", "htf_1h_trend": "NEUTRAL", "htf_4h_trend": "NEUTRAL",
        "value_status": "BELOW_VAL", "price_vs_vwap": "BELOW_VWAP",
        "ema34": 0.0, "ema55": 0.0, "ema59": 0.0, "ema89": 0.0, "ema233": 0.0,
    }
    base_auction.update(auction_extra or {})
    ev.olap.query_auction_profile.return_value = base_auction
    ev.olap.get_forecast_frame.return_value = pd.DataFrame()
    ev.regime_det = MagicMock()
    ev.regime_det.analyze_symbol.return_value = {"regime": "MEAN_REVERTING_RANGE", "atr_pct": 1.0}
    ev.vector_mem = None
    ev.timesfm = SimpleNamespace(is_available=False)
    ev.critic_engine = MagicMock()
    ev.critic_engine.check_microstructure_knife_veto.return_value = (False, "", {})
    ev.critic_engine.audit_candidate_setup.return_value = {"critic_verdict": "CLEAR", "max_similarity": 0.0, "penalty": 0.0}
    ev.query_live_tv_liquidations = lambda s: {}
    ev._persist_alert = MagicMock()
    ev._broker_sizing = lambda *a, **k: {"lots": 0.01, "risk_usd": 5.0, "cap_usd": 10.0}
    ev.hyperdash = MagicMock()  # must never be called when prefetched intel is supplied
    return ev


def _signal(prefetched=None, side="buy"):
    sig = {
        "symbol": "BTC", "mt5_symbol": "BTCUSD", "sig_name": "Liquidation Exhaustion",
        "sig_code": "LIQ_REV_SPIKE", "side": side, "price": 100.0,
        "datetime_utc": datetime.now(timezone.utc).isoformat(),
        "detection_time_utc": datetime.now(timezone.utc).isoformat(),
        "bar_closed": True, "is_mock": True,
    }
    if prefetched is not None:
        sig["prefetched_hyperdash_intel"] = prefetched
    return sig


def _prefetched_hd(smart_bias=20.0, crowd_bias=0.0, pockets=None, flow=None, signals=None):
    return {
        "coin": "BTC", "symbol": "BTCUSD", "data_valid": True, "intel_age_sec": 10.0,
        "smart_bias": smart_bias, "crowd_bias": crowd_bias, "intel_score": 60.0,
        "intel_direction": "BULLISH", "signals": signals or [],
        "sweep_targets": [], "entry_pockets": pockets or [],
        "whale_flow": flow or _empty_whale_flow(),
    }


def test_p3_soft_clear_chain_blocked_by_pillar_gate():
    """REGRESSION FOR SECTION 4.5: base 40 + Model-1 bonus 15 + below-VWAP 10 +
    vector memory 10 + Hyperdash 10 = 85 >= 75 with ONLY ONE market pillar (P5
    via smart money). The pillar gate must block dispatch."""
    ev = _make_evaluator(whale_imb=0.50, cvd="NEUTRAL", near_field=50.0)
    ev.vector_mem = MagicMock()
    ev.vector_mem.query_similar_setups.return_value = {"verdict": "FAVORABLE", "win_rate": 0.80}

    res = ev.evaluate_signal(_signal(prefetched=_prefetched_hd(smart_bias=20.0)))
    assert res["confluence_score"] == pytest.approx(85.0, abs=1.0)
    assert res["pillar_gate"]["gate_passed"] is False
    assert res["pillar_gate"]["independent_pillars_fired"] == 1
    assert "P5_WHALE_SWEEP" in res["pillar_gate"]["pillar_hits"]
    assert res["is_actionable"] is False
    assert any("PILLAR_MINIMUM_GATE_FAILED" in r for r in res["confluence_reasons"])
    ev.hyperdash.get_asset_intel.assert_not_called()  # prefetched intel consumed


def test_p3_two_market_pillars_clear_the_gate():
    """Same setup but CVD BULLISH_FLOW (P1) + whale imbalance 0.70 (P5) +
    near-field shorts 60 (P4) -> gate passes, candidate actionable."""
    ev = _make_evaluator(whale_imb=0.70, cvd="BULLISH_FLOW", near_field=60.0)
    res = ev.evaluate_signal(_signal(prefetched=_prefetched_hd(smart_bias=20.0)))
    assert res["pillar_gate"]["gate_passed"] is True
    assert res["pillar_gate"]["independent_pillars_fired"] >= 2
    assert res["is_actionable"] is True


def test_p3_gate_killswitch_restores_legacy_behavior(monkeypatch):
    """PILLAR_MIN_MARKET_PILLARS=0 (or module constant 0) disables the gate:
    the 1-pillar soft-clear becomes actionable again (legacy behavior)."""
    import Terminal.realtime_signal_evaluator as rse
    monkeypatch.setattr(rse, "MIN_INDEPENDENT_MARKET_PILLARS", 0)
    ev = _make_evaluator(whale_imb=0.50, cvd="NEUTRAL", near_field=50.0)
    ev.vector_mem = MagicMock()
    ev.vector_mem.query_similar_setups.return_value = {"verdict": "FAVORABLE", "win_rate": 0.80}
    res = ev.evaluate_signal(_signal(prefetched=_prefetched_hd(smart_bias=20.0)))
    assert res["pillar_gate"]["gate_passed"] is True
    assert res["is_actionable"] is True


def test_p2_evaluator_pocket_penalty_and_flow_bonus_wiring():
    """Evaluator consumes entry_pockets (penalty) + whale_flow (tailwind) from the
    prefetched Hyperdash intel and reports both in the reason string.
    Deterministic score audit: base 40 + CVD 15 + whale-imbalance 15 + Model-1 15
    + HD smart money 10 + whale-flow 6 - overhead-pocket 8 = 93."""
    ev = _make_evaluator(whale_imb=0.70, cvd="BULLISH_FLOW", near_field=50.0,
                         auction_extra={"price_vs_vwap": "ABOVE_VWAP"})
    hd = _prefetched_hd(
        smart_bias=20.0,
        pockets=[{
            "pocket_type": "OVERHEAD_SUPPLY", "side": "LONG", "pocket_price": 100.8,
            "dist_pct": 0.8, "trapped_notional_usd": 1_200_000.0, "trapped_musd": 1.2,
            "whale_count": 2, "avg_underwater_pnl_pct": -2.0, "pocket_score": 9e5,
        }],
        flow={"data_valid": True, "flow_state": "ACCUMULATING", "flow_score": 30.0,
              "flow_delta_usd": 700_000.0, "window_sec": 90.0, "low_confidence": False},
    )
    res = ev.evaluate_signal(_signal(prefetched=hd))
    reasons = " | ".join(res["confluence_reasons"])
    assert "Overhead Supply Cap" in reasons          # pocket penalty applied
    assert "Whale Flow: ACCUMULATING" in reasons     # flow tailwind applied
    # exact score audit proves BOTH the -8 pocket penalty and +6 flow bonus landed
    assert res["confluence_score"] == pytest.approx(93.0, abs=0.001)


# =====================================================================
# PATCH P4 — Asynchronous Bounded Fan-Out
# =====================================================================

def _mock_range_signal(sym, t_offset, price=100.0):
    return {
        "symbol": sym, "bar_type": "range", "sig_name": "Absorption",
        "side": "bullish", "price": price, "time_ms": int(time.time() * 1000) - t_offset,
        "is_mock": True,
    }


def _build_sentinel():
    import importlib
    sentinel_mod = importlib.import_module("5_wake_on_footprint_signal") \
        if False else importlib.import_module("Terminal.test_review_fixes")._load_wake_module()
    return sentinel_mod


def _load_wake_module():
    """Loads 5_wake_on_footprint_signal.py as a module (digit-leading filename)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "wake_on_footprint_signal_mod",
        ROOT_DIR / "5_wake_on_footprint_signal.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["wake_on_footprint_signal_mod"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_p4_fanout_bounded_concurrency_and_order_preservation():
    """6 burst signals evaluated with max 4 parallel workers; all evaluated once;
    results returned in original deterministic order."""
    mod = _load_wake_module()
    sentinel = mod.FootprintSignalSentinel(from_current_end=True)
    sentinel.stack.ledger._evaluated = {}

    concurrency = {"cur": 0, "max": 0}
    lock = threading.Lock()

    def slow_eval(sig):
        with lock:
            concurrency["cur"] += 1
            concurrency["max"] = max(concurrency["max"], concurrency["cur"])
        time.sleep(0.12)
        with lock:
            concurrency["cur"] -= 1
        return {"symbol": sig["symbol"], "direction": "BUY", "confluence_score": 78.0,
                "is_actionable": False, "validation_errors": []}

    sentinel.evaluator.evaluate_signal = MagicMock(side_effect=slow_eval)

    signals = [_mock_range_signal(f"SYM{i}USD", 1_000 * (i + 1)) for i in range(6)]
    start = time.perf_counter()
    evals = asyncio.run(sentinel.evaluate_and_dispatch(signals))
    elapsed = time.perf_counter() - start

    assert len(evals) == 6
    assert [e["symbol"] for e in evals] == [s["symbol"] for s in signals]  # order preserved
    assert 2 <= concurrency["max"] <= mod.MAX_PARALLEL_EVALUATIONS  # parallel but bounded
    assert concurrency["max"] <= 4
    # 6 x 0.12s serial == 0.72s; bounded fan-out must finish clearly faster
    assert elapsed < 0.60, f"fan-out too slow: {elapsed:.2f}s (serial-equivalent 0.72s)"
    for s in signals:
        assert sentinel.stack.ledger.is_evaluated(s["symbol"], s["time_ms"], bar_type="range")


def test_p4_fanout_worker_exception_isolation():
    """One failing evaluation must not poison the batch: others still evaluate,
    the failed cluster is absent from the results and is not ledger-marked."""
    mod = _load_wake_module()
    sentinel = mod.FootprintSignalSentinel(from_current_end=True)
    sentinel.stack.ledger._evaluated = {}

    def flaky_eval(sig):
        if sig["symbol"] == "BOOMUSD":
            raise RuntimeError("simulated OLAP explosion")
        return {"symbol": sig["symbol"], "direction": "BUY", "confluence_score": 70.0,
                "is_actionable": False, "validation_errors": []}

    sentinel.evaluator.evaluate_signal = MagicMock(side_effect=flaky_eval)
    signals = [_mock_range_signal("OK1USD", 5_000), _mock_range_signal("BOOMUSD", 4_000),
               _mock_range_signal("OK2USD", 3_000)]
    evals = asyncio.run(sentinel.evaluate_and_dispatch(signals))
    assert [e["symbol"] for e in evals] == ["OK1USD", "OK2USD"]
    assert not sentinel.stack.ledger.is_evaluated("BOOMUSD", signals[1]["time_ms"], bar_type="range")
    assert sentinel.stack.ledger.is_evaluated("OK1USD", signals[0]["time_ms"], bar_type="range")


def test_p4_fanout_parallel_workers_config():
    """MAX_PARALLEL_EVALUATIONS defaults to 4 and is env-overridable."""
    mod = _load_wake_module()
    assert mod.MAX_PARALLEL_EVALUATIONS == 4


def test_p4_mock_signals_skip_hyperdash_prefetch(monkeypatch):
    """Mock/test signals must never touch the network: the async pre-fetch is
    skipped entirely (get_asset_intel_async not called)."""
    mod = _load_wake_module()
    sentinel = mod.FootprintSignalSentinel(from_current_end=True)
    sentinel.stack.ledger._evaluated = {}

    from Terminal.hyperdash_intel_engine import hyperdash_engine
    forbidden = AsyncMock(side_effect=AssertionError("network access from tests"))
    monkeypatch.setattr(hyperdash_engine, "get_asset_intel_async", forbidden)

    sentinel.evaluator.evaluate_signal = MagicMock(side_effect=lambda s: {
        "symbol": s["symbol"], "direction": "BUY", "confluence_score": 60.0,
        "is_actionable": False, "validation_errors": []})

    asyncio.run(sentinel.evaluate_and_dispatch([_mock_range_signal("BTCUSD", 9_000)]))
    forbidden.assert_not_called()
    assert "prefetched_hyperdash_intel" not in (sentinel.evaluator.evaluate_signal.call_args[0][0])


def test_p4_live_signals_prefetch_intel_concurrently(monkeypatch):
    """Non-mock clusters pre-fetch Hyperdash intel via get_asset_intel_async and
    hand it to the evaluator inside the signal dict."""
    mod = _load_wake_module()
    sentinel = mod.FootprintSignalSentinel(from_current_end=True)
    sentinel.stack.ledger._evaluated = {}

    # Force the live (non-mock) path: neutralize pytest detection
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("MOCK_TEST_MODE", "0")
    # Seed the product cache so resolve_product stays offline
    mod.delta_client.products_cache["BTCUSD"] = {
        "id": 1, "symbol": "BTCUSD", "underlying": "BTC", "contract_value": 0.001,
        "tick_size": 0.5, "default_leverage": 20.0, "status": "operational",
    }

    from Terminal.hyperdash_intel_engine import hyperdash_engine
    fake_intel = {"coin": "BTC", "data_valid": True, "smart_bias": 9.9, "crowd_bias": 0.0,
                  "signals": [], "sweep_targets": [], "entry_pockets": [],
                  "whale_flow": _empty_whale_flow()}
    prefetch = AsyncMock(return_value=fake_intel)
    monkeypatch.setattr(hyperdash_engine, "get_asset_intel_async", prefetch)

    captured = {}
    def capturing_eval(sig):
        captured.update(dict(sig))
        return {"symbol": sig["symbol"], "direction": "BUY", "confluence_score": 60.0,
                "is_actionable": False, "validation_errors": []}

    sentinel.evaluator.evaluate_signal = MagicMock(side_effect=capturing_eval)

    live_signal = {
        "symbol": "BTCUSD", "bar_type": "range", "sig_name": "Absorption",
        "side": "bullish", "price": 100000.0, "time_ms": int(time.time() * 1000) - 1_000,
    }
    asyncio.run(sentinel.evaluate_and_dispatch([live_signal]))
    prefetch.assert_awaited_once()
    assert captured.get("prefetched_hyperdash_intel") == fake_intel


def test_p4_timesfm_inference_is_mutex_serialized():
    """Two threads invoking forecast_normalized_delta_ratio serialize through the
    class-level inference lock (fan-out thread-safety)."""
    from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster
    model = object.__new__(DeepMindTimesFMForecaster)
    model._compiled = True

    inner_calls = {"cur": 0, "max": 0}
    lock = threading.Lock()

    class FakeForecast:
        def forecast(self, horizon, inputs):
            with lock:
                inner_calls["cur"] += 1
                inner_calls["max"] = max(inner_calls["max"], inner_calls["cur"])
            time.sleep(0.05)
            with lock:
                inner_calls["cur"] -= 1
            # shape: (batch=1, horizon), quantile grid (1, horizon, 10)
            return [np.zeros((1, horizon))], [np.zeros((1, horizon, 10))]

    model._model = FakeForecast()
    cvd = np.arange(30, dtype=float)
    vol = np.ones(30)

    errs = []
    def run():
        try:
            model.forecast_normalized_delta_ratio(cvd, vol, horizon=8)
        except Exception as e:  # noqa
            errs.append(e)

    ts = [threading.Thread(target=run) for _ in range(3)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errs
    assert inner_calls["max"] == 1, "TimesFM inference calls overlapped — mutex broken"


def test_p4_backward_compat_evaluate_and_dispatch_signature():
    """Public API shape preserved: same signature, returns list of evaluation dicts."""
    import inspect
    mod = _load_wake_module()
    sig = inspect.signature(mod.FootprintSignalSentinel.evaluate_and_dispatch)
    params = list(sig.parameters.keys())
    assert params == ["self", "raw_signals"]
