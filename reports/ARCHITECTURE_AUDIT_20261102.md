# Architecture audit and refactor â€” 2 November 2026

Workspace: C:/Users/SIGMA/Documents/_Trading_1. Verification snapshot: 2026-11-02 06:59:57 UTC (12:29:57 IST). No live orders were sent, positions modified, daemons started, or trading services restarted by this audit.

## Result

Code hardening and regression verification completed. Production execution is not certified: MT5 IPC timed out, no relevant Python collector/trader processes were visible, BTC inputs failed freshness checks, and the MatchTrader browser tab was absent. CDP itself responded. These are observed operational blockers; passing unit tests does not resolve them.

Initial suite: 25 passed. On continuation the repository contained five additional tests and new Coinalyze/MatchTrader modules added by concurrent development. Final suite: 48 passed. Python compileall and git diff --check succeeded. Existing generated database changes were preserved. Earlier audit changes are already present in the current committed tree; this audit did not create those commits or push them.

## Changes and rationale

| Files under Terminal | Defect and resulting behavior |
| --- | --- |
| runtime_io.py; collector_lock.py | Replaced file-existence mutex with OS byte locks, monotonic deadline and crash-released ownership. Added unique temporary publication, bounded Windows replacement retries, serialized read/modify/write and singleton ownership without killing reused PIDs. |
| direct_footprint_collector.py | Honors failed browser acquisition; bounded CDP requests; serialized atomic Parquet updates; latest revision deduplication excludes changing price from candle identity. |
| liquidation_map_collector.py | Honors browser contention; bounded CDP requests; current key levels, magnets and near-field state replace old snapshots instead of accumulating obsolete levels; snapshot history uses atomic upserts. |
| whale_orders_collector.py | Honors browser contention; atomic current-book publication and serialized historical upserts; bounded CDP requests and singleton ownership. |
| coinglass_tv_liquidation_collector.py | Atomic liquidation and summary publication; historical-only percentile baseline; stable spike-bar identity; bounded sweeps and two-second lock deadline. Account for concurrent revisions to this collector when deploying. |
| olap_engine.py | Completed contiguous candle filtering; current snapshot selection and freshness qualification; unknown inputs carry data_valid=False; price/delta disagreement no longer automatically earns reversal confirmation. |
| regime_detector.py | Uses completed bars and total_volume denominator instead of treating missing volume as one. |
| deepmind_timesfm_forecaster.py | Rejects nonfinite, unaligned inputs, unsupported output grids and malformed outputs. Preserves double precision before differencing. Marginal quantile sums are explicitly not a calibrated joint horizon confidence interval. |
| realtime_signal_evaluator.py | Calls the implemented normalized-flow forecasting API, corrects liquidation-exhaustion direction and spread units, uses broker-native sizing rather than guessing lots from asset price, tolerates unavailable optional vector memory, and rejects unqualified required data. Alerts include explicit MatchTrader mapping with execution authorization false. |
| vector_memory.py | Restricts historical replay to forward completed bars, requires entry touch before target credit, defers unresolved outcomes until sufficient forward data, updates individual rows rather than overwriting concurrent history, and avoids declaring profitability from three nearest text matches. Replay remains a hypothetical OHLC model, not proof of an executable counterfactual. |
| alert_stream.py; alert_watcher.py; signal_event_listener.py | Durable byte cursors handle partial lines, equal timestamps, truncation and rotation; only actionable records wake the listener. OS file events replace repeated full-file polling. Startup boundary is established before watcher subscription and drained afterward. Cursor persistence and user notification are not one atomic transaction. |
| risk_envelope.py | Decimal integer-step sizing fixes mixed volume/loss bounds and avoids rounded risk hiding cap breaches. Nonfinite or missing geometry and budget inputs fail closed. Existing MT5 policy constants were retained. |
| venue_admission.py | Separates venue/account checks, contingent book stress, native valuation, margin and quote freshness. MatchTrader daily floor derives from the higher opening balance/equity; overall floor derives from original capital. Missing daily identity or inventory rejects admission. |
| crypto_live_trader.py | Correct native forecast API, spread units and valuation; guards account identity and unknown inventory; serializes admission; persists execution intent before sending and blocks ambiguous retries; checks returned fill protection. Stop expansion no longer increases risk without independent requalification. |
| matchtrader_live_trader.py | Removed unverified order-button dispatch: old path ignored its calculated volume and did not attach the supplied SL/TP. Both dispatch entry points now explicitly reject. Sizing estimates return zero when broker minimum would exceed budget and for unknown contract units. Verified native instrument metadata and bracket/volume acknowledgments are required before implementing dispatch. |
| test_pipeline_hardening.py; test_coinalyze_and_matchtrader.py | Added 18 regression cases; corrected a legacy test that expected a minimum lot despite a risk-cap breach. All file/broker mutation tests use temporary files and doubles. |

## Verification

- python -m pytest Terminal -v: 48 passed in 3.70 seconds on the final source, including removal of unreachable rejected MatchTrader dispatch code.
- python -m compileall -q Terminal: successful.
- git diff --check -- Terminal: successful.
- Fifteen read-only BTC confluence queries: median 12.22 ms, maximum 54.38 ms. These measurements include rejection of stale inputs and do not establish qualified-signal or full-model latency.
- Native MT5 initialization: IPC timeout. Inventory and equity therefore remain unknown for this audit.
- CDP: two pages; MatchTrader absent. Collector/trader process inventory: none visible.

Machine-readable evidence: architecture_audit_20261102.json.

## Deployment limits and remaining qualification

Do not interpret these checks as a profitability backtest or a guarantee against gaps. Fixed risk budgets and stops constrain modeled losses; execution can exceed them. MT5 tick timestamps must be independently verified against UTC; an unknown server offset is rejected rather than inferred from the very tick under certification. The broker account must be verified before considering live activation. Other independent processes must use the same account lock to provide account-wide admission serialization.

Execution-intent ambiguity deliberately remains latched pending broker reconciliation; automatic reconciliation and full fill-to-position identity coverage require live broker evidence. The generic position supervisor still is not a certified immutable-initial-risk ratchet ledger. This audit did not activate trading or claim that overnight daemons loaded the patched source. The existing footprint ingestion cadence is distinct from the OS-event-driven desk wakeup.

FundingPips competition rules state 5 percent daily loss based on the higher opening balance/equity and 10 percent overall loss from starting capital. For a 100,000 USD start, 5,000/10,000 USD are loss allowances, not account floors. Current official rules also prohibit EA execution in the competition. The repository's MatchTrader execution remains blocked pending proper qualification and applicable trading authorization: https://help.fundingpips.com/hc/en-us/articles/48599924132241-Monthly-Competition
