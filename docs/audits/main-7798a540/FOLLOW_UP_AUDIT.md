# Follow-up Institutional Verification — `main@7798a540`

**Target:** `7798a540465b64298193c67eecb9479b4a7da574`<br>
**Subject:** `feat(critic): implement anti-falling knife defense, dynamic post-mortem learning, and integrate arena audit fixes`<br>
**Verification date:** 2026-11-03 (IST)<br>
**Verdict:** **FAIL — not ready for live or challenge-account execution**<br>
**Live/challenge readiness:** **18 / 100** (target: 90+; not achieved)

## Bottom line

The commit repairs several important items from the earlier `main-c064c303` audit: it compiles, the canonical hard floor/risk/R:R values are present, MatchTrader dispatch is disabled by default, the evaluator's persistence method has no broker call, exact M15 adjacency is enforced, closed-bar dedupe is improved, and the OR-Tools formulation constrains `enter_trade == 1`.

However, the new critic/memory system is **not operational in the committed artifact**, the required suite is not green, multiple prior pipeline protections have regressed or were never applied, and Sentinel 5 can bypass the disabled MatchTrader gate into an ungated Delta India live order path. The Delta module also contains committed fallback API credentials; their values are deliberately not reproduced in this report. **Revoke/rotate those credentials immediately.**

No CDP attachment, WARP rotation, Delta/MatchTrader API call, or order placement was performed during this review. Tests were run with external TCP sockets blocked.

## Verification scorecard

| Finding | Verdict | Evidence / conclusion |
|---|---|---|
| Evaluator syntax/import blocker from `c064` | **PASS** | `compileall` completed with exit 0; no evaluator syntax error remains. |
| `risk_envelope` canonical values and non-finite checks | **PARTIAL PASS** | `Terminal/risk_envelope.py:29,32,38` holds `$4,775`, `$25`, and `MIN_RR_RATIO=2.00`; `budget_per_trade` rejects non-finite/negative capital at `:112-114`. |
| Single source of truth for *all* risk controls | **FAIL** | `test_single_source_of_truth.py:17-40` only regexes six exact names. `matchtrader_live_trader.py:56-61` still creates local initial balance, daily drawdown/floor, and environment-overridable concurrent-position settings. In particular `$4,850` daily floor conflicts with the dynamic 5% daily policy described in `risk_envelope.py:49-53`. The narrow test passes but does not prove the broader property. |
| MatchTrader default fail-closed gate | **PASS** | `matchtrader_live_trader.py:63,285-289` defines `LIVE_DISPATCH_ENABLED=False` and rejects before touching a supplied socket. Targeted audit/SSOT tests: **11 passed**. |
| MatchTrader execution correctness if enabled | **FAIL** | `execute_signal` falls back to `.01` lots and zero SL/TP at `matchtrader_live_trader.py:537-541`; it does not call `validate_trade_geometry` or verify actual risk. `execute_market_order` still touch/unlinks a marker (`:291-296,504-509`) and returns `ok=val.get('ok', True)` at `:492-503`. It is unsafe to enable without a redesign. |
| Evaluator purity (`_persist_alert`) | **PASS** | AST inspection of `_persist_alert_locked` found no execution/dispatch call; it writes JSONL/Parquet only (`realtime_signal_evaluator.py:1392-1435`). The evaluator itself has no `execute_signal` call. |
| Orders only through Sentinel 5 | **PARTIAL PASS** | The evaluator is pure and Sentinel calls execution (`5_wake_on_footprint_signal.py:310-337`). But Sentinel's Delta failover is live, ungated, and not governed by the challenge risk envelope. |
| Exact contiguous M15 enforcement | **PASS** | `runtime_io.py:157-174` rejects any diff not exactly `900000`; audit regression passes. |
| Footprint dedupe consumes only closed bars | **PASS** | `1_coinglass_footprint_scraper.py:389-405` performs closure test before adding the dedupe key; importer similarly filters at `:104-117`. |
| OR-Tools prevents trivial zero lot | **PASS** | `ortools_lot_solver.py:111-124` has `enter_var == 1`, positive min-lot/risk constraints. Direct probes return an admissible positive `0.05` lot where feasible and `NO_FEASIBLE_DISCRETE_SOLUTION` otherwise. |
| Previously fixed wake offset / closed-candle / dispatch accounting | **FAIL — regression** | Wake advances its durable offset before evaluation (`5_wake_on_footprint_signal.py:265-268`), trusts producer `bar_closed=True` as an alternative to time (`:284`, also `:533`, `:605`), and increments dispatch count regardless of result (`:339-340`). |
| Previously fixed unified API closed-bar/persistence order | **FAIL — regression/unapplied patch** | `unified_api_footprint_engine.py:136-139` always initializes MT5, Binance fetches exactly `rolling_window` bars (`:233-272`), dedupe is marked before append success (`:420-423`), and `processed_count` increments on any nonexception result (`:455-458`). |
| Prior WARP/order serialization fix | **FAIL — incomplete** | `collector_lock.py:120-123` releases the order mutex immediately before the rotation begins; `_set_last_rotation_time` remains non-atomic (`:65-72`); stabilization computes `changed` but returns success without requiring it (`:79-94`). WARP is not an approved vendor-access recovery mechanism in any case. |
| KHI falling-knife veto | **PARTIAL PASS** | Deterministic code is present: KHI weights and `>=50` veto at `trade_critic_engine.py:269-314`; an offline pure-function probe produced KHI 95 and a veto. But it uses EMA233 only—no EMA89 input/reference—and evaluator labels absorption from a signal name (`realtime_signal_evaluator.py:865-879`), not validated wick/taker evidence. |
| SHI blow-off-squeeze veto | **PASS (static/pure logic only)** | SHI weights and `>=50` veto are present at `trade_critic_engine.py:316-360`; pure-function probe produced SHI 95 and a veto. No lifecycle test specifically covers this path. |
| Required LanceDB post-mortem table and embedding model | **FAIL** | `Database/Vector_Memory/` contains only `trade_setups` (3 rows) and `counterfactual_setups` (21 rows). `losing_trades_postmortem.lance` is absent. The committed code declares 384-D BGE schema at `trade_critic_engine.py:169-199`, but eager `TextEmbedding` construction at `:153-162` tries to download the model. |
| Similarity thresholds / adverse candidate gate | **PARTIAL FAIL** | Thresholds are correctly encoded: `>=.88` HARD_VETO and `.80-.88` 25-point penalty (`trade_critic_engine.py:430-444`). But query exception returns `CLEAR` at `:467-475`, so unavailable/corrupt memory fails **open**, and the evaluator silently sets `critic_engine=None` on initialization failure (`realtime_signal_evaluator.py:180-185`). No explicit normalized-vector or Lance metric contract is enforced. |
| Post-mortem storage on every losing closure | **FAIL** | `conduct_post_mortem_and_store` catches Lance add failure (`trade_critic_engine.py:580-585`) but still returns `RECORDED` (`:598-604`). Sentinel infers closure from disappearance and the last **unrealized** PnL (`5_wake_on_footprint_signal.py:488-519`), not broker trade history/final realized PnL. Keys collapse positions by symbol/side (`:415`) or product (`:462`); Delta assumes `int(size)` (`:461`). This cannot prove one accurate post-mortem for every losing close. |
| Required 70-test collectible suite | **FAIL** | **70 collected; 64 passed, 1 failed, 5 errors**. The five critic lifecycle tests cannot construct FastEmbed offline because the model is neither vendored nor pre-provisioned. The empirical learner test fails because the committed historical candles are stale relative to runtime now. |
| Required compilation command | **PASS** | `python -m compileall Terminal 1_coinglass_footprint_scraper.py 4_coinglass_tv_scraper.py 5_wake_on_footprint_signal.py`: exit 0. |

## P0 blockers

1. **Committed secret fallback and ungated live Delta route.** `Terminal/delta_exchange_live_trader.py:24-55` reads production API credentials and embeds fallback credentials. Import-time construction calls remote Delta endpoints (`:58-110`). Sentinel imports the client and, when MatchTrader rejects, automatically invokes `execute_delta_signal` (`5_wake_on_footprint_signal.py:329-336`). Delta uses five concurrent positions (`delta_exchange_live_trader.py:21,321-328`), hard-codes one contract and 25x leverage (`:330-345`), and does not import/enforce `risk_envelope`. This bypasses the default-disabled MatchTrader gate.
2. **The critic is optional/fail-open and the required table does not exist.** An unavailable model disables the entire defense and a failed similarity query returns `CLEAR`. The requested anti-falling-knife protection cannot be credited operationally.
3. **Loss classification is not based on closed-trade truth.** The post-mortem workflow can both miss losses and create false records because it substitutes last observed unrealized PnL for a final fill/history record.

## Test evidence

Executed from a fresh archive of the target commit with network sockets blocked:

```bash
python -m compileall Terminal 1_coinglass_footprint_scraper.py \
  4_coinglass_tv_scraper.py 5_wake_on_footprint_signal.py
# exit 0

python -m pytest Terminal/ -q --disable-socket --allow-unix-socket -p no:cacheprovider \
  --ignore=Terminal/test_ingestion_recovery.py \
  --ignore=Terminal/test_liquidation_map_pipeline.py \
  --ignore=Terminal/test_tv_liquidation_collector.py
# 70 collected; 64 passed, 1 failed, 5 errors; exit 1
```

The three omitted modules remain uncollectible because the production modules they import do not exist. The specific previous-audit regression tests plus the SSOT test pass in isolation: **11 passed**. Raw output and offline probes are retained in [`evidence/`](evidence/).

## Required remediation before another readiness review

1. **Immediate security response:** revoke the committed Delta credentials, remove all fallback secrets from Git history and runtime code, use a secret manager, and block all trading unless an explicit operator-controlled `LIVE_DISPATCH_ENABLED` gate is true. Delta must import and obey the same `risk_envelope`, account/position cap, geometry, idempotency, and broker-acknowledgement controls as MatchTrader.
2. **Make critic availability fail closed:** package and checksum/pin the BGE model for deployment, initialize it before accepting signals, create/migrate `losing_trades_postmortem` transactionally, and make model/table/query failures veto execution rather than return `CLEAR`.
3. **Correct the critic contract:** add EMA89 distance as specified; require independently measured absorption/taker-flow fields; set an explicit normalized cosine metric/index and test exact `.80`/`.88` boundaries, both directions, KHI and SHI.
4. **Use immutable broker identities and closure history:** track venue order/position IDs, reconcile final realized PnL, fill prices, fees, and close timestamps from broker history, and make post-mortem insertion idempotent. Return a failure—not `RECORDED`—unless both durable audit record and vector insert succeed.
5. **Restore earlier safeguards:** defer wake offset commit, derive bar closure only from timestamp, count only successful dispatch, fix unified forming-bar/persistence ordering, and keep order mutex held across the entire WARP rotation (or remove WARP/browser collection under the repository's API-only policy).
6. **Release gate:** make all 70 collectible tests deterministic and offline; add critic lifecycle fixtures with a local fake embedding backend plus a separately provisioned real-model integration test; make stale-data tests time-controlled. Do not enable live execution until the suite is green and a paper-account reconciliation test proves the complete order/close/post-mortem lifecycle.
