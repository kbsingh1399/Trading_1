"""
========================================================================================
Unit & Parity Tests for Universal Footprint Engine
========================================================================================
Validates that UniversalFootprintEngine:
1. Generates exact CoinGlass footprint signals (Absorption, Divergence, Exhaustion, Trapped).
2. Computes correct Delta, Cumulative Delta (CVD), and POC.
3. Maps timestamps strictly to Indian Standard Time (IST, UTC+5:30) with ms precision.
4. Enforces strict 300 rolling candle window.
5. Operates across Crypto, Forex, Metals, and Indices uniformly.
========================================================================================
"""

import math
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import pytest

from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    utc_ms_to_ist_str,
    utc_ms_to_ist_iso,
    enrich_candle_df_with_ist,
    IST_OFFSET_MS
)


def make_test_bars(n=350, start_utc_ms=1793500000000, base_price=100.0):
    bars = []
    price = base_price
    for i in range(n):
        t_ms = start_utc_ms + i * 900000
        o = price
        h = o + 0.5
        l = o - 0.5
        c = o + 0.1
        v = 1000.0
        price = c
        bars.append({
            "open_time_ms": t_ms,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "total_volume": v
        })
    return bars


def test_ist_time_mapping_accuracy():
    """Verify that IST time mapping is mathematically exact (+5:30 = +19,800,000 ms)."""
    # UTC timestamp for 2026-11-02 11:00:00 UTC
    dt_utc = datetime(2026, 11, 2, 11, 0, 0, tzinfo=timezone.utc)
    utc_ms = int(dt_utc.timestamp() * 1000)

    ist_str = utc_ms_to_ist_str(utc_ms)
    assert "16:30:00 IST" in ist_str
    assert "2026-11-02" in ist_str

    ist_iso = utc_ms_to_ist_iso(utc_ms)
    assert ist_iso == "2026-11-02T16:30:00+05:30"

    df = pd.DataFrame([{"open_time_ms": utc_ms}])
    df = enrich_candle_df_with_ist(df)
    assert df["open_time_ist"].iloc[0] == "2026-11-02 16:30:00 IST"
    assert df["close_time_ist"].iloc[0] == "2026-11-02 16:45:00 IST"
    assert df["open_time_ist_ms"].iloc[0] == utc_ms + IST_OFFSET_MS


def test_rolling_300_window_enforcement():
    """Verify that the engine strictly enforces 300 rolling candle window."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=350)
    df, sigs = engine.analyze_bars(bars, symbol="EURUSD", asset_class="Forex", rolling_window=300)

    assert len(df) == 300
    assert "time_ist" in df.columns
    assert "open_time_ist" in df.columns
    assert "close_time_ist" in df.columns
    assert "cumulative_delta" in df.columns
    assert "cvd" in df.columns
    assert "poc_price" in df.columns


def test_absorption_footprint_signal():
    """Verify Bullish & Bearish Absorption detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Bullish Absorption at bar 25:
    # Huge lower wick (0.7 of range), high volume (2x avg), closed near high
    b = bars[25]
    b["open"] = 100.0
    b["high"] = 100.2
    b["low"] = 98.0
    b["close"] = 100.1
    b["total_volume"] = 3000.0

    df, sigs = engine.analyze_bars(bars, symbol="BTC", asset_class="Crypto", inspect_last_n_bars=30)
    abs_sigs = [s for s in sigs if s["sig_code"] == "abs" and s["time_ms"] == b["open_time_ms"]]
    assert len(abs_sigs) > 0
    assert abs_sigs[0]["side"] == "bullish"
    assert abs_sigs[0]["title"] == "Selling absorbed at the low"


def test_divergence_footprint_signal():
    """Verify Single-Bar Divergence (Drop without selling / Rally without buying)."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Bearish Divergence at bar 28:
    # Price closed UP, but Delta is strongly NEGATIVE (-30%)
    b = bars[28]
    b["open"] = 100.0
    b["high"] = 100.8
    b["low"] = 99.9
    b["close"] = 100.7  # Closed up
    b["total_volume"] = 2000.0
    b["delta"] = -600.0  # -30% Delta
    b["delta_pct"] = -30.0

    df, sigs = engine.analyze_bars(bars, symbol="GBPUSD", asset_class="Forex", inspect_last_n_bars=30)
    div_sigs = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == b["open_time_ms"]]
    assert len(div_sigs) > 0
    assert div_sigs[0]["side"] == "bearish"
    assert div_sigs[0]["title"] == "Rally without taker buying"


def test_exhaustion_footprint_signal():
    """Verify Buyer & Seller Exhaustion detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Buyer Exhaustion at bar 29:
    # New 10-bar high on depleted volume, closed in lower half
    b = bars[29]
    b["open"] = 101.5
    b["high"] = 105.0  # Big new 10-bar high
    b["low"] = 101.0
    b["close"] = 101.2  # Closed near low
    b["total_volume"] = 200.0  # Depleted volume (vs 1000 avg)

    df, sigs = engine.analyze_bars(bars, symbol="XAUUSD", asset_class="Metals", inspect_last_n_bars=30)
    exh_sigs = [s for s in sigs if s["sig_code"] == "exh" and s["time_ms"] == b["open_time_ms"]]
    assert len(exh_sigs) > 0
    assert exh_sigs[0]["side"] == "bearish"
    assert exh_sigs[0]["title"] == "Buyer exhaustion at high"


def test_trapped_traders_signal():
    """Verify Trapped Traders / Failed breakout detection."""
    engine = UniversalFootprintEngine()
    bars = make_test_bars(n=30)

    # Inject Trapped Longs at bar 27:
    # Broke 5-bar high (around 103.1), but snapped back inside bar range
    b = bars[27]
    b["open"] = 102.5
    b["high"] = 106.0  # Broke 5-bar high (> 103.5)
    b["low"] = 101.5
    b["close"] = 102.0  # Closed back below breakout level
    b["total_volume"] = 2500.0

    df, sigs = engine.analyze_bars(bars, symbol="SPX500", asset_class="Indices", inspect_last_n_bars=30)
    trap_sigs = [s for s in sigs if s["sig_code"] == "trap" and s["time_ms"] == b["open_time_ms"]]
    assert len(trap_sigs) > 0
    assert trap_sigs[0]["side"] == "bearish"
    assert trap_sigs[0]["title"] == "Failed breakout"


# ========================================================================================
# v2.0.0 CoinGlass Parity Calibration Tests
# ========================================================================================
from Terminal.universal_footprint_engine import (  # noqa: E402
    FootprintCalibration,
    SIGNAL_PRIORITY_WEIGHTS,
    DEFAULT_CALIBRATION,
)

CANDLE_DIR = Path(__file__).resolve().parent.parent / "Database" / "Candle_Data"


def _flat_bars(n=30, start_utc_ms=1793500000000, base_price=2500.0, volume=10000.0):
    bars = []
    for i in range(n):
        t_ms = start_utc_ms + i * 900000
        bars.append({
            "open_time_ms": t_ms,
            "open": base_price,
            "high": base_price + 5.0,
            "low": base_price - 5.0,
            "close": base_price,
            "total_volume": volume,
            "buy_volume": volume / 2.0,
            "sell_volume": volume / 2.0,
            "delta": 0.0,
        })
    return bars


def test_legacy_min_delta_pct_is_clamped_to_parity_floor():
    """The historical min_delta_pct=5.0 call path must NOT regress the over-generation defect."""
    engine = UniversalFootprintEngine(imbalance_ratio=3.0, min_delta_pct=5.0)
    assert engine.min_delta_pct >= 20.0, "legacy min_delta_pct must be clamped to the calibrated parity floor"
    fresh = UniversalFootprintEngine()
    assert fresh.min_delta_pct == DEFAULT_CALIBRATION.div_delta_pct_floor


def test_divergence_respects_calibrated_delta_floor():
    """|delta_pct| below the calibrated floor must not fire (the naive 5% floor fired 41.7% of bars)."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30)

    # Weak divergence: closed up on -10% delta -> below the 22% parity floor
    b = bars[25]
    b["open"], b["high"], b["low"], b["close"] = 2500.0, 2510.0, 2495.0, 2508.0
    b["total_volume"] = 20000.0
    b["delta"] = -2000.0  # -10%
    b["buy_volume"] = 9000.0
    b["sell_volume"] = 11000.0

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    weak = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == b["open_time_ms"]]
    assert weak == [], "div must not fire below the calibrated delta floor"

    # Strong divergence: closed up on -30% delta -> fires
    b2 = bars[27]
    b2["open"], b2["high"], b2["low"], b2["close"] = 2500.0, 2510.0, 2495.0, 2508.0
    b2["total_volume"] = 20000.0
    b2["delta"] = -6000.0  # -30%
    b2["buy_volume"] = 7000.0
    b2["sell_volume"] = 13000.0

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    strong = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == b2["open_time_ms"]]
    assert len(strong) == 1
    assert strong[0]["side"] == "bearish"


def test_divergence_requires_close_vs_open_not_prev_close():
    """The naive `or c < prev_c` expansion fired on down-closing gap bars; must not."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30)
    # Bar 26 closes at 2504 (prev close 2500): closed UP normally
    bars[26].update({"open": 2500.0, "high": 2512.0, "low": 2498.0, "close": 2504.0,
                     "total_volume": 20000.0, "delta": -6000.0,
                     "buy_volume": 7000.0, "sell_volume": 13000.0})
    # Bar 27: opens at 2510 (gap up), closes 2506 -> closes DOWN vs open, but ABOVE prev close 2504.
    # delta stays strongly negative. The naive engine fired bearish div here via `c > prev_c`.
    bars[27].update({"open": 2510.0, "high": 2514.0, "low": 2502.0, "close": 2506.0,
                     "total_volume": 20000.0, "delta": -6000.0,
                     "buy_volume": 7000.0, "sell_volume": 13000.0})

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    div27 = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == bars[27]["open_time_ms"]]
    assert div27 == [], "div must evaluate close-vs-open only, never close-vs-prev-close"
    # sanity: bar 26 (closed up, -30% delta) still fires
    div26 = [s for s in sigs if s["sig_code"] == "div" and s["time_ms"] == bars[26]["open_time_ms"]]
    assert len(div26) == 1 and div26[0]["side"] == "bearish"


def test_trap_requires_future_confirmation():
    """A breakout bar whose follow-through CONTINUES must not be a trap."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30, base_price=2500.0)
    # Failed-breakout shape at bar 20 (would fire in the naive same-bar engine)...
    bars[20].update({"open": 2500.0, "high": 2540.0, "low": 2496.0, "close": 2502.0,
                     "total_volume": 40000.0, "delta": 12000.0,
                     "buy_volume": 26000.0, "sell_volume": 14000.0})
    # ...but bars 21-23 keep making new highs -> no confirmation, no trap
    for k, p in enumerate([2545.0, 2550.0, 2555.0]):
        bars[21 + k].update({"open": 2502.0 + k, "high": p, "low": 2500.0, "close": p - 1.0,
                             "total_volume": 20000.0, "delta": 2000.0,
                             "buy_volume": 11000.0, "sell_volume": 9000.0})

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    traps = [s for s in sigs if s["sig_code"] == "trap" and s["time_ms"] == bars[20]["open_time_ms"]]
    assert traps == [], "trap must require 1-3 bar failure confirmation, not fire on the break itself"


def test_trap_confirmation_lag_is_recorded():
    """Confirmed traps carry their CoinGlass confirmation lag (1-3 bars)."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30, base_price=2500.0)
    bars[20].update({"open": 2500.0, "high": 2540.0, "low": 2496.0, "close": 2510.0,
                     "total_volume": 40000.0, "delta": 12000.0,
                     "buy_volume": 26000.0, "sell_volume": 14000.0})
    # Bar 21 closes back inside the lower 35% band of bar 20's range (2496..2540)
    retest_level = 2496.0 + 0.35 * (2540.0 - 2496.0)  # 2511.4
    bars[21].update({"open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2505.0,
                     "total_volume": 20000.0, "delta": -2000.0,
                     "buy_volume": 9000.0, "sell_volume": 11000.0})

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    traps = [s for s in sigs if s["sig_code"] == "trap" and s["time_ms"] == bars[20]["open_time_ms"]]
    assert len(traps) == 1
    assert traps[0]["side"] == "bearish"
    assert traps[0]["confirm_bars"] == 1
    assert "1 bars later" in traps[0]["description"]  # CoinGlass literal template


def test_sdiv_requires_fractal_pivot_with_minimum_gap():
    """sdiv compares against a 3-left/3-right fractal pivot (minimum pivot->signal gap = 4 bars)."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30, base_price=2500.0)
    # Build a swing pivot high at bar 10 (unique max of bars 7..13)
    bars[10].update({"open": 2500.0, "high": 2560.0, "low": 2495.0, "close": 2550.0,
                     "total_volume": 20000.0, "delta": 3000.0,
                     "buy_volume": 11500.0, "sell_volume": 8500.0})
    # Divergent new high at bar 14 (gap = 4 bars from pivot) with heavy negative delta
    bars[14].update({"open": 2520.0, "high": 2570.0, "low": 2515.0, "close": 2565.0,
                     "total_volume": 30000.0, "delta": -9000.0,
                     "buy_volume": 10500.0, "sell_volume": 19500.0})

    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    sdivs = [s for s in sigs if s["sig_code"] == "sdiv" and s["time_ms"] == bars[14]["open_time_ms"]]
    assert len(sdivs) == 1
    assert sdivs[0]["side"] == "bearish"
    assert "Higher high" in sdivs[0]["title"]


def test_cooldown_suppresses_consecutive_same_type():
    """Two adjacent absorption-shaped bars: the second is suppressed by cooldown."""
    engine = UniversalFootprintEngine()
    bars = _flat_bars(n=30, base_price=2500.0)
    for i in (20, 21):
        bars[i].update({"open": 2500.0, "high": 2505.0, "low": 2470.0, "close": 2504.0,
                        "total_volume": 40000.0, "delta": 6000.0,
                        "buy_volume": 23000.0, "sell_volume": 17000.0})
    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=30)
    t20 = [s for s in sigs if s["sig_code"] == "abs" and s["time_ms"] == bars[20]["open_time_ms"]]
    t21 = [s for s in sigs if s["sig_code"] == "abs" and s["time_ms"] == bars[21]["open_time_ms"]]
    assert len(t20) == 1, "first absorption bar must fire"
    assert t21 == [], "second consecutive absorption bar must be cooldown-suppressed"


def test_density_governor_hard_ceiling():
    """Governor: even adversarial input cannot exceed the hard signal ceiling."""
    cal = FootprintCalibration(
        governor_enabled=True,
        governor_window=300,
        governor_max_total=3,
        governor_max_per_type=2,
        abs_cooldown=1,
    )
    engine = UniversalFootprintEngine(calibration=cal)
    bars = _flat_bars(n=40, base_price=2500.0)
    for i in range(10, 40):
        lo = 2470.0 if i % 2 == 0 else 2500.0
        bars[i].update({"open": 2500.0, "high": 2535.0, "low": lo, "close": 2530.0 if lo == 2470.0 else 2505.0,
                        "total_volume": 40000.0, "delta": 6000.0,
                        "buy_volume": 23000.0, "sell_volume": 17000.0})
    df, sigs = engine.analyze_bars(bars, symbol="ETH", asset_class="Crypto", inspect_last_n_bars=40)
    assert len(sigs) <= 3, "governor must enforce the hard total ceiling"


def test_pick_primary_priority_arbitration():
    """Concurrent signals: trap outranks div regardless of list order (execution scoring)."""
    engine = UniversalFootprintEngine()
    div_sig = {"sig_code": "div", "side": "bearish", "strength": 0.9}     # 0.60 x 0.9 = 0.54
    trap_sig = {"sig_code": "trap", "side": "bearish", "strength": 0.6}   # 1.00 x 0.6 = 0.60
    primary = engine.pick_primary([div_sig, trap_sig])
    assert primary["sig_code"] == "trap"
    assert SIGNAL_PRIORITY_WEIGHTS["trap"] > SIGNAL_PRIORITY_WEIGHTS["abs"] > SIGNAL_PRIORITY_WEIGHTS["exh"]


def test_execution_plan_fill_calibrated_rr_and_fees():
    """1:2 R:R order plan with fee-aware discrete sizing for the Delta Exchange trader."""
    engine = UniversalFootprintEngine()
    signal = {"side": "bearish", "price": 100.0}
    plan = engine.build_execution_plan(
        signal, equity_usd=10000.0, atr_pct=1.0, risk_fraction=0.01,
        rr_target=2.0, taker_fee_rate=0.0005, contract_value=1.0, min_size=1.0)
    assert plan["valid"] is True
    assert plan["direction"] == "SHORT"
    assert plan["rr"] == 2.0
    assert plan["stop_loss"] == pytest.approx(101.5, abs=1e-9)   # ATR band 1.5%
    assert plan["take_profit"] == pytest.approx(97.0, abs=1e-9)  # exactly 2R
    # Fee-aware discrete size: floor(100 / (1.5 + 0.0005*(100+97)))
    assert plan["size"] == math.floor(100.0 / (1.5 + 0.0005 * 197.0))
    assert plan["fees_reserved_usd"] > 0

    tiny = engine.build_execution_plan(signal, equity_usd=1.0, atr_pct=1.0)
    assert tiny["valid"] is False
    assert tiny["reason"] == "below_min_size"


def test_trailing_ratchet_never_widens():
    engine = UniversalFootprintEngine()
    stop = 98.5
    stop = engine.apply_trailing_ratchet(stop, mark_price=102.0, direction=1, atr=1.0)
    assert stop == pytest.approx(100.0)          # ratcheted up
    stop2 = engine.apply_trailing_ratchet(stop, mark_price=99.0, direction=1, atr=1.0)
    assert stop2 == pytest.approx(100.0)         # must NOT widen on pullback
    short_stop = engine.apply_trailing_ratchet(101.5, mark_price=98.0, direction=-1, atr=1.0)
    assert short_stop == pytest.approx(100.0)    # ratcheted down


@pytest.mark.skipif(not (CANDLE_DIR / "ETH.parquet").exists(), reason="ETH.parquet not present")
def test_signal_density_parity_on_eth_window():
    """The over-generation defect fix: ETH 300-bar window must stay in the CoinGlass density band."""
    engine = UniversalFootprintEngine()
    raw = pd.read_parquet(CANDLE_DIR / "ETH.parquet").tail(300)
    df, sigs = engine.analyze_bars(
        raw, symbol="ETH", asset_class="Crypto",
        rolling_window=300, inspect_last_n_bars=300)
    n = len(df)
    bars_with = int((df["signals_count"] > 0).sum())
    density = 100.0 * bars_with / n
    # CoinGlass ground truth on the same window: 25 signals / 8.3% density
    # (naive engine: 226 signals / 42.7% of bars). Parity band: 5%-20%.
    assert 5.0 <= density <= 20.0, f"signal-bar density {density:.1f}% outside parity band"
    assert 10 <= len(sigs) <= 60, f"{len(sigs)} signals outside parity range"
    stats = engine.last_run_stats
    assert stats["engine_version"] == "2.0.0"
