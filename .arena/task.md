# TASK: Flawless CoinGlass CDP Ingestion, Re-entrant Windows Locking, Local Cloudflare WARP Auto-Rotation, and High-Confluence Expansion

## 1. Context & Account Constraints
- **Target Repository**: `_Trading_1` (Cryptocurrency Microstructure, Order Flow & High-Confluence Quantitative Trading System)
- **Broker / Platform**: MetaTrader 5 (FundedNext Server), Login `11808165`.
- **Hard Account Loss Floor**: Strictly **`$4,600.00`** hard equity floor. Current equity ~$4,679.37. Active cushion ~$79.37.
- **Target Per-Trade Risk**: Strictly **`$10.00 – $15.00`**, governed by Cap Dominance: $r_t \le \min(0.091 C_t, 0.25 C_t, \$15.00)$. Stand-down floor $C^* = \$16.48$.
- **Kill-Switch**: Immediate Flatten if SL attachment fails verification within 3.0s or fill slips $> 0.4R$.
- **Execution Whitelist**: `["BTCUSD", "ETHUSD", "DOGEUSD", "ADAUSD", "XRPUSD", "LTCUSD", "LINKUSD"]`.

## 2. Core Requirements to Audit, Solve and Verify

### A. Eliminate the Double-Lock Deadlock & Data Stagnation
- Investigate and eliminate the root cause of the 9.7-hour data stagnation where `summary_liquidations.parquet`, `coinglass_aggregated_futures_signals.parquet`, and `liquidation_snapshots.parquet` were not updating.
- Implement thread-local re-entrant `writer_lock` in `coinglass_common.py` so nested calls (`upsert_parquet` calling `atomic_parquet`, or `process_and_persist` calling `atomic_parquet`) never deadlock on Windows `msvcrt` file handles.
- Implement non-blocking OS lock testing in `_reap_stale_lock()` and purge all abandoned `.lock` files in `Database/.locks/`.

### B. Local Cloudflare Auto-Rotation (Zero Outside Invocations)
- Integrate local Cloudflare WARP client (`warp-cli.exe` located at `C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe`).
- Strictly NO outside paid proxies or cloud API calls.
- Auto-rotate local egress IP on Cloudflare Turnstile challenge detection, HTTP 403, HTTP 429, or consecutive empty scrapes.
- Maintain persistent in-flight trade guard (`order_execution_inflight.lock`) so rotation NEVER occurs during an MT5 trade execution.

### C. Pipeline Scraper Timeout & Fault Tolerance
- In `run_pipeline.py`, ensure scraper timeouts safely accommodate all 31 crypto symbols without premature process termination.
- Ensure `1_coinglass_footprint_scraper.py` updates auction profiles in a `finally` block so harvested data is never lost.
- Ensure all 4 scrapers (`1_coinglass_footprint_scraper.py`, `2_coinglass_whales.py`, `3_coinglass_liquidation_levels.py`, `4_coinglass_tv_scraper.py`) dump genuine data as-is from CoinGlass CDP.

### D. High-Confluence Expansion (New Microstructure Pillars)
- Explore and specify high-confluence data additions from CoinGlass:
  - **Pillar 6**: Open Interest & Predicted Funding Rates (Squeeze acceleration).
  - **Pillar 7**: Top Trader Long/Short Sentiment Divergence (Smart money vs retail positioning).
  - **Pillar 8**: Multi-Timeframe Liquidation Heatmap Intensity (Magnet price targets).
- Integrate new data points into the 5-Pillar Confluence Engine (`trade_evaluator.py`).

## 3. Review Standard & Done Criteria
- Every file and line of code in the data ingestion pipeline must be checked.
- All files in `Database/` must update in real-time with verified recent timestamps.
- Zero `TimeoutError: Writer busy` exceptions.
- 100% test pass on institutional test suites.
- Git parity across local, `origin`, and `trading1`.
