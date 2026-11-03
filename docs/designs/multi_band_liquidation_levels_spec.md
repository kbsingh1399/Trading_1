# Architecture Design Specification: Multi-Horizon & Multi-Band Liquidation Levels Extraction

**Document ID**: `docs/designs/multi_band_liquidation_levels_spec.md`  
**Author**: Elephant (Deep Context Architectural Orchestrator)  
**Status**: APPROVED & REMEDIATED (Goldfish Audited by `egm-implementation-readiness`)  
**Target Component**: `3_coinglass_liquidation_levels.py` & `Database/Liquidation_Data/Key_Levels/`  
**Downstream Consumer**: `Terminal/olap_engine.py`  
**User Directives**:
1. *"don't you think you should structurally dump all percent till 8 percent max? make it structured manner."*
2. *"keep this format only"* (Excel template with `Asset`, `Side`, `Price`, `Distance`, `Leverage`, `Strength`, `Percent`)
3. *"We need welstrucutred at different time and percent combination...consider all combination"*

---

## 1. Problem Statement & Motivation
Currently, [`3_coinglass_liquidation_levels.py`](file:///c:/Users/SIGMA/Documents/_Trading_1/3_coinglass_liquidation_levels.py) extracts only a single static slice of Key Liquidation Levels (whatever tab happens to be active or default, typically ±5% on 3D).

CoinGlass organizes liquidation level barriers across two orthogonal dimensions:
1. **Time Lookback Horizons (3 total)**:
   - `3D · 1m` (High-frequency short-term order flow)
   - `7D · 3m` (Weekly swing order flow)
   - `30D · 15m` (Monthly macro liquidity clusters)
2. **Radial Percentage Horizons (8 total)**:
   - `±1%`, `±2%`, `±3%`, `±4%`, `±5%`, `±6%`, `±7%`, `±8%`
   - Short liquidations sit above price (`1%` to `8%`).
   - Long liquidations sit below price (`-1%` to `-8%`).

By harvesting **all 24 discrete Time × Percent combinations** in a single browser pass (~6.5 seconds per asset):
- **Scalping Precision (1m / ±1% to ±2%)**: Immediate high-leverage squeeze levels.
- **Intraday Confluence (3m / ±3% to ±5%)**: Session Value Area and intraday targets.
- **Macro Swing Anchors (15m / ±6% to ±8%)**: Major institutional liquidation walls.

---

## 2. Structured Data Schema & Format Requirements

The user explicitly mandated format:
```
Asset | Side | Price | Distance | Leverage | Strength | Percent
SOL   | Short| 96.70 | +5.02%   | High leverage | 59.4 | 1%
SOL   | Long | 90.75 | -1.45%   | Low leverage  | 48.3 | -1%
```

### 2.1 Standard Column Specifications
| Column Name | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `Asset` | `str` | Base cryptocurrency symbol | `SOL`, `BTC`, `ETH` |
| `Time` | `str` | Lookback horizon | `3D · 1m`, `7D · 3m`, `30D · 15m` |
| `Side` | `str` | Liquidation barrier side | `Short` (above price), `Long` (below price) |
| `Price` | `float64` | Liquidation barrier strike price | `96.70`, `90.75` |
| `Distance` | `str` | Distance from current market price | `+5.02%`, `-1.45%` |
| `Leverage` | `str` | Institutional leverage classification | `High leverage`, `Mid leverage`, `Low leverage` |
| `Strength` | `float64` | Relative liquidation intensity score (0–100) | `59.4`, `95.9`, `100.0` |
| `Percent` | `str` | Signed distance horizon band | `1%` to `8%` for Shorts; `-1%` to `-8%` for Longs |

### 2.2 System & Auxiliary Fields for Parquet (Cross-Asset Pipeline Integration)
To comply with quantitative trading standards (Primary IST time mapping and numeric indexing), the following columns are preserved in Parquet:
- `percent_val` (`int64`): Signed integer percentage (`1` to `8` for Shorts; `-1` to `-8` for Longs).
- `rank` (`int64`): Relative rank within the band (1 to 5).
- `snapshot_time_ist` (`str`): Primary Indian Standard Time (`YYYY-MM-DD HH:MM:SS IST`).
- `time_ist` (`str`): Join key matching all candle and signal tables.
- `time_ms` (`int64`): Epoch milliseconds.
- `snapshot_time_utc` (`str`): ISO 8601 UTC timestamp.

### 2.3 Formats Persisted
1. **Master Parquet**: `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet` containing all ~230+ records across all combinations with primary columns:
   `['Asset', 'Time', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent', ...]`
2. **Master Combined CSV**: `Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.csv` with columns:
   `['Asset', 'Time', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']`
3. **Discrete Horizon CSVs** (Exact User Column Format):
   - `Database/Liquidation_Data/Key_Levels/<SYMBOL>_3D.csv`
   - `Database/Liquidation_Data/Key_Levels/<SYMBOL>_7D.csv`
   - `Database/Liquidation_Data/Key_Levels/<SYMBOL>_30D.csv`
   Each containing strictly: `['Asset', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']`.

---

## 3. Implementation Plan & Goldfish Remediations

### 3.1 CDP `awaitPromise` Support in `3_coinglass_liquidation_levels.py`
Update `cdp_eval`:
```python
async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    return await cdp_result(ws, {
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {"expression": expr, "returnByValue": True, "awaitPromise": await_promise}
    })
```

### 3.2 Browser Extraction Script (`HARVEST_ALL_COMBINATIONS_JS`)
- Clean separation: Synchronous `EXTRACTION_JS` parses `last_price`, `clld`, `magnet_zones`, `near_field`, `strength_by_distance`, `leverage_tiers`. Section 8 (`key_levels`) is removed from `EXTRACTION_JS`.
- Asynchronous `HARVEST_ALL_COMBINATIONS_JS` sweeps 3 Timeframes × 8 Percent tabs:
  - 400ms delay after timeframe switch to allow network/render settle.
  - 150ms delay after tab click.
  - Defensive parsing: `replace(/[$,]/g, '')` for prices, `replace(/[−–—]/g, '-')` for distances.
  - Generates signed string `Percent` (`1%` vs `-1%`) and signed integer `percent_val`.

### 3.3 Downstream Deduplication in `Terminal/olap_engine.py`
In `Terminal/olap_engine.py`'s `query_key_liquidation_levels()`:
Add deduplication:
```python
kl_df = kl_df.drop_duplicates(subset=['Side', 'Price']) if 'Side' in kl_df.columns else kl_df.drop_duplicates(subset=['side', 'price'])
```
This guarantees that `overhead.head(5)` and `underlying.head(5)` return 5 distinct strike barrier prices rather than repeated rows across overlapping percentage bands.

---

## 4. Verification Protocol
1. **Schema Check**: Assert `['Asset', 'Side', 'Price', 'Distance', 'Leverage', 'Strength', 'Percent']` exist in parquet and CSV.
2. **Completeness Check**: Assert all 3 timeframes (`3D · 1m`, `7D · 3m`, `30D · 15m`) and all 8 percentage horizons are present (`nunique >= 8`).
3. **Consumer Check**: Verify `OlapEngine().query_key_liquidation_levels('SOL')` executes cleanly and returns 5 unique overhead/underlying barriers.