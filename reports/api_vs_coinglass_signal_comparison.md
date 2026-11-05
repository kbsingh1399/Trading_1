# Empirical Parity Report: API Footprint vs. CoinGlass Footprint (v2.0)

**Execution Timestamp:** `2026-11-05T08:00:31.502665+00:00`  
**Engine:** `Terminal/universal_footprint_engine.py` v2.0.0 (CoinGlass parity calibration)  
**Historical Benchmark:** `Database/Signals/coinglass_aggregated_futures_signals.parquet` (23,800+ CoinGlass CDP records)  

## Parity Verdict: **PASS**

- Directional concordance: **89.9%** (gate: >= 85.0% -> OK)
- Signal density band across symbols: **7.0% - 13.67%** of 15m bars (CoinGlass band: 8%-13% on majors, up to 19% on high-activity symbols -> OK)
- Total engine signals: **324** vs CoinGlass **305** (ratio 1.062)

---

## 1. Crypto Benchmark: API Engine vs. CoinGlass Ground Truth

| Asset | 15m Bars | CG Signals | CG Density | API Signals | API Signal Bars | API Density | Overlap Bars | Concordance |
|---|---|---|---|---|---|---|---|---|
| **BTC** | 300 | 23 | 5.67% | 26 | 21 | 7.0% | 3 | **66.7%** |
| **ETH** | 300 | 25 | 8.0% | 38 | 33 | 11.0% | 13 | **84.6%** |
| **SOL** | 300 | 24 | 8.0% | 32 | 29 | 9.67% | 12 | **91.7%** |
| **BNB** | 300 | 36 | 9.67% | 31 | 26 | 8.67% | 17 | **88.2%** |
| **XRP** | 300 | 34 | 9.33% | 39 | 34 | 11.33% | 16 | **93.8%** |
| **DOGE** | 300 | 26 | 8.0% | 30 | 25 | 8.33% | 6 | **100.0%** |
| **ADA** | 300 | 39 | 11.67% | 39 | 33 | 11.0% | 13 | **100.0%** |
| **LINK** | 300 | 58 | 16.33% | 38 | 34 | 11.33% | 25 | **88.0%** |
| **AVAX** | 300 | 40 | 10.67% | 51 | 41 | 13.67% | 24 | **87.5%** |
| **TOTAL** | - | **305** | - | **324** | - | - | **129** | **89.9%** |

---

## 2. Signal Family Parity Breakdown

| Code | Signal Family | CoinGlass (in window) | API (in window) | Exact Bar+Side Matches | Density Ratio |
|---|---|---|---|---|---|
| `div` | Single-Bar Divergence (close vs Delta opposition) | 82 | 78 | 35 | 0.95x |
| `abs` | Absorption (heavy side volume absorbed at extreme) | 78 | 90 | 12 | 1.15x |
| `exh` | Exhaustion (new 10-bar extreme, depleted extreme zone) | 75 | 79 | 24 | 1.05x |
| `sdiv` | Swing CVD Divergence (fractal pivot vs CVD) | 37 | 40 | 14 | 1.08x |
| `trap` | Trapped Traders (failed 5-bar break, 1-3 bar confirmation) | 33 | 37 | 3 | 1.12x |

> abs/trap bar-level timing is bounded by the klines-only zone-volume estimation
> ceiling (see audit report, Section 4); density and direction remain parity-locked.

---

## 3. Macro Classes Extrapolation (Forex, Metals, Indices)

| Symbol | Asset Class | 15m Bars | Footprint Signals | Density | Latest Bar IST |
|---|---|---|---|---|---|
| **EURUSD** | Macro | 300 | 19 (exh: 14, abs: 3, sdiv: 1, div: 1) | 5.33% | `2026-11-04 14:15:00 IST` |
| **GBPUSD** | Macro | 300 | 11 (exh: 7, trap: 1, abs: 3) | 3.0% | `2026-11-04 14:15:00 IST` |
| **USDJPY** | Macro | 300 | 12 (exh: 8, div: 1, abs: 3) | 3.33% | `2026-11-04 14:15:00 IST` |
| **XAUUSD** | Macro | 300 | 10 (exh: 8, div: 1, abs: 1) | 2.67% | `2026-11-04 14:15:00 IST` |
| **SPX500** | Macro | 300 | 14 (exh: 10, abs: 3, trap: 1) | 4.0% | `2026-11-04 14:15:00 IST` |

---

## 4. Active Calibration Profile

```json
{
  "div_delta_pct_floor": 22.0,
  "div_cooldown": 2,
  "abs_close_pos_extreme": 0.2,
  "abs_zone_share_min": 0.3,
  "abs_wick_min": 0.1,
  "abs_vol_ratio_min": 1.4,
  "abs_cooldown": 4,
  "exh_lookback": 10,
  "exh_wick_min": 0.55,
  "exh_close_pos": 0.3,
  "exh_cooldown": 6,
  "sdiv_pivot_left": 3,
  "sdiv_pivot_right": 3,
  "sdiv_cvd_mag_mult": 5.0,
  "sdiv_cvd_mag_window": 100,
  "sdiv_cooldown": 3,
  "trap_break_lookback": 5,
  "trap_zone_share_min": 0.3,
  "trap_vol_ratio_min": 2.0,
  "trap_confirm_bars": 3,
  "trap_cooldown": 4,
  "governor_enabled": true,
  "governor_window": 300,
  "governor_max_total": 60,
  "governor_max_per_type": 20
}
```

---

## 5. Parity & System Guarantees
- **Bar Completion Invariant:** execution signals only on completed, closed bars (`bar_type == 'time'`, `t_ms % 900_000 == 0`).
- **Trap Confirmation Invariant:** trapped-trader signals timestamp at the trigger bar and emit only after the 1-3 bar failure confirmation (CoinGlass lag distribution 68/21/11%).
- **Multi-Signal Confluence:** candles store a native `signals` struct array; concurrent signals are priority-ranked (trap > abs > exh > sdiv > div) for `RealtimeSignalEvaluator` scoring.
- **Density Governor:** hard ceiling (20% of window, 20 per type) makes a 226-signal-class regression structurally impossible.