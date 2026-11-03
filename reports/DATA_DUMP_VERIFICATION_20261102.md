# Live data-dump verification — 2 November 2026

Completed 2026-11-02T07:17:56.245672+00:00 (12:47 IST). Overall: DEGRADED.

Ran five bounded ingestion passes sequentially. Signal evaluation was replaced with a data-only sink in this verification process. No trade dispatch or broker order modification was performed. Persistent daemons were not started.

| Pipeline | Refreshed assets | Files updated | Result |
|---|---:|---:|---|
| footprint | 1/36 | 2 | PARTIAL |
| whale_orders | 12/12 | 25 | REFRESHED |
| liquidation_maps | 13/13 | 40 | REFRESHED |
| tv_liquidations | 0/36 | 0 | FAILED |
| coinalyze_rest | 15/34 | 31 | PARTIAL |

All 311 Parquet files were decoded successfully; zero unreadable or empty files.

- Footprint: only ATOM candle data refreshed, with 79 footprint signals harvested. Most pages remained Loading with zero bars. Existing untouched candle files must not be considered refreshed.
- Whale orders: all 12 configured assets refreshed. BTC produced 706 open walls and 121 history observations.
- Liquidation maps: all 13 configured assets refreshed; magnets, key levels and near-field balances updated.
- TradingView liquidations: failed to find the TradingView execution context; no output files updated.
- Coinalyze REST: partial coverage, with HTTP 429 rate limits. Summary count includes retained prior assets and must not be interpreted as all newly fetched. Compatibility files ending _tv_liquidations are REST copies, not independent TV observations.
- Readability and recent write time alone do not establish causal freshness or correct volume units. Full per-file timestamps, counts, updated-path lists and failures are archived in data_dump_verification_latest.json.

Evidence log: data_dump_verification.log. Reproducible bounded ingestion runner: run_data_dump_verification.py.
