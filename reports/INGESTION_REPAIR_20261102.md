# Ingestion repairs and live verification — 2 November 2026

Completed ingestion-only repairs and live data checks. No orders were sent or modified, no trading daemon was started, and no risk policy was loosened.

| Pipeline | Before | Verified after |
|---|---:|---:|
| Footprint candles | 1/36 | 36/36 refreshed |
| Whale books | 12/12 | 12/12 refreshed |
| Liquidation maps | 13/13 | 13/13 refreshed |
| TradingView liquidations | 0/36 | 36/36 refreshed; final pass 37.3 seconds |
| Coinalyze REST | 15/34 | 29/34 with observations; 5 explicit empty responses |

## Applied changes

- Terminal/direct_footprint_collector.py: replaced the obsolete exact-three-signal-sets requirement with populated time-set readiness. Waits for the selected asset's replacement data, handles an unchanged selection, cleans up polling on completion/timeout, and reports load failures rather than silently returning empty success.
- Terminal/coinglass_tv_liquidation_collector.py: resolves the actual chart inside the same-origin TradingView iframe, correlates CDP request IDs, bounds response waits, checks M15 resolution, and avoids deleting unrelated TV tabs. Removed speculative network rotation from chart-health checks. Rejects invalid liquidation numbers and corrupt historical reads; excludes legacy REST aliases from native TV history. Missing main-series prices remain null with price_available=False; liquidation observations remain usable as liquidation data. The emitter already requires a positive price before proposing a signal.
- Terminal/coinalyze_rest_collector.py: persists symbol-weighted API quota reservations across runs, waits the full Retry-After cooldown, validates configured markets against the provider catalogue, fetches historical conversion coverage, requests native USD notional conversion, and removes fallback conversion at an invented price. API keys are sent in headers rather than URLs. REST no longer overwrites TV history or TV summaries. A collection-health file distinguishes fetched data, empty provider responses, and unsupported markets.
- Terminal/realtime_signal_evaluator.py: reads the two separately identified summary sources and selects fresh eligible observations, rather than depending on one shared overwritten summary.
- Terminal/data_integrity_audit.py: checks actual row timestamps, readability, nonempty data and source identity. Older files outside current configured coverage remain flagged; a file count alone cannot certify freshness.
- Terminal/test_ingestion_recovery.py: regression tests for weighted quota, full retry cooldown, no USD double conversion, source separation, invalid input rejection, historical conversion requirements and missing TV prices.

## Verification and limits

Final pytest: 54 passed in 4.18 seconds. Compileall and git diff --check passed.

All configured footprint/TV assets refreshed in live browser passes. Native REST USD conversion and separate publication were checked in a subsequent live pass. No HTTP 429 retry loop occurred in the paced verification.

The REST API returned no liquidation-history observations for UKOIL, USOIL, NDX100, DJI30 and JP225. This is an upstream response, not repaired by fabricating zero-volume histories. TV provides an independent synthetic-market liquidation source for these symbols; it is not proof of broker CFD stops or interchangeable venue liquidity.

BTC's TV main-series OHLC was zero at the provider while liquidation-study history was populated. Missing prices are explicitly tagged and cannot be used for trade dispatch. The final JSON lists every symbol with a missing quote. Other verified price feeds can be considered by their own validation paths; this repair does not manufacture a live BTC quote.

The health auditor intentionally continues to flag stale legacy files and sparse old REST liquidation-event timestamps. Successful collection, file freshness and execution qualification are separate checks. These bounded verification scripts do not install or activate recurring services.

Machine-readable evidence: ingestion_repair_final.json. Logs: data_dump_repaired.log, ingestion_repair_verification.log and tv_final_verification.log. Reproduction scripts: run_data_dump_verification.py and verify_repaired_sources.py.

Provider rate and conversion reference: https://api.coinalyze.net/v1/doc/

Full Parquet decode: 312 files, 0 read failures.
