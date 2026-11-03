# Institutional Architecture Audit — `kbsingh1399/Trading_2`

**Audit target:** `main@c064c30336c3cfe44e6dfc8d0e1c2f1a303772c4`<br>
**Target commit subject:** `docs: add exhaustive Arena.ai quant audit round 3 bundle`<br>
**Audit date:** 2026-11-03 (IST)<br>
**Decision:** **NOT READY FOR LIVE OR CHALLENGE-ACCOUNT EXECUTION**<br>
**Readiness score:** **14 / 100**

> This is a frozen-commit audit. It assesses the exact `main` commit above, not an unpinned moving branch tip. The working-branch deliverable is a set of drop-in unified patches in [`patches/`](patches/) and reproducible evidence in [`evidence/`](evidence/).

## Executive decision

The repository contains useful building blocks—closed-bar intent, an OS-backed mutex primitive, a discrete volume solver, approximately 300 M15 crypto bars per asset, and many existing safety tests—but its **audit-target state must not be connected to the FundingPips account**.

Two independent P0s are decisive:

1. `Terminal/realtime_signal_evaluator.py` has 753 `CRCRLF` line endings and does not compile (`SyntaxError`, reported at line 328). This breaks evaluator import, the unified engine, and the wake path.
2. The executable MatchTrader path is configured for a **$100,000 account / $500 risk**, treats the order-inflight marker as a non-atomic touch/unlink file, and can declare `DISPATCHED` when DOM confirmation defaults truthy. It is incompatible with account `101305360`, the $5,000 challenge, the $4,775 floor, the two-position cap, and $25 maximum per-trade risk.

The un-gated weighted assessment is **24.5 / 100**. The score is reduced to **14 / 100** by the P0 build/import failure and the independent live-risk-envelope violation (a 15-point maximum readiness gate; one point retained only because the static risk-envelope design and offline test structure are recoverable). This is an operational prohibition, not a confidence interval.

### Required decision now

* Do **not** attach to a browser, rotate WARP, connect to MatchTrader/MT5, or transmit an order from the audited baseline.
* Apply `patches/ALL_CHANGES.patch` only to the exact target commit, validate in a non-production environment, and keep `LIVE_DISPATCH_ENABLED=False` until broker-side authorization and fill/SL/TP acknowledgement tests are signed off.
* Treat CoinGlass DOM/CDP collection as a separately approved integration. `AGENTS.md:12` says the supported production design is API-only and explicitly says no browser DOM dependencies; the CDP collectors are therefore both fragile **and** outside the repository's declared operating policy.

## Scoring model

| Pillar | Weight | Raw score | Weighted points | Why it is low |
|---|---:|---:|---:|---|
| P1 — production reliability and ingestion | 25% | 32 | 8.0 | Import failure, CDP single-port contention, non-durable write semantics, uncollectible test modules |
| P2 — statistical / signal rigor | 25% | 22 | 5.5 | OHLC-derived proxy delta, range-wick rather than wick-taker-volume absorption, non-composable additive score |
| P3 — latency and throughput | 20% | 28 | 5.6 | Local Parquet queries are fast, but multi-stream scan is ~225 ms and core feeds were often invalid/stale; no live CDP/broker E2E measurement |
| P4 — risk and execution control | 20% | 4 | 0.8 | Account/risk constants are wrong, DOM order confirmation is fail-open, lock can expire/unlink during an order |
| P5 — data, time, and auditability | 10% | 46 | 4.6 | Crypto M15 storage is generally contiguous, but macro inventory is absent, schema drift exists, and forming rows/null anchors are present |
| **Un-gated total** | **100%** |  | **24.5** |  |
| **P0 readiness gate** |  |  | **14 / 100** | Compile/import failure plus unsafe live risk/execution path cap readiness at 15 |

## Scope, method, and safety boundary

### Personas used

| Persona | Question asked | Principal conclusion |
|---|---|---|
| Production/SRE architect | Can processes, locks, files, and browser resources survive concurrency and restart? | No. The writer-lock inode race is reproduced; CDP is a shared port without a complete lifecycle/health contract. |
| Market microstructure/statistical reviewer | Are the claimed footprint signals causally supported by the stored observations? | No. Several features are useful proxies, but the data cannot support true taker-at-wick absorption or reliable CVD claims. |
| Low-latency/data engineer | Is the data path bounded, fresh, typed, and measurably fast? | Local scans are reasonable; freshness and availability make results unsuitable for automated decisions. |
| Prop-risk and execution-control reviewer | Can a challenge-account order be bounded and proven acknowledged? | No. The baseline carries $100k/$500 values and fail-open DOM result handling. |
| Security/vendor-governance reviewer | Are anti-bot/browser collection and IP rotation acceptable production dependencies? | No approval should be inferred. Browser/CDP plus WARP rotation is incompatible with the stated API-only architecture and requires vendor/terms approval. |

The review was source, static-data, and offline-test based. It **did not** perform live scraping, CDP attachment, WARP rotation, public-IP probing, broker/MT5 connection, or order submission. Network sockets were disabled for tests except Unix-domain test harness support.

## Evidence-led findings

Severity definitions: **P0** = immediate live-trading stop; **P1** = material failure/risk requiring remediation before commissioning; **P2** = correctness, model-validity, or resilience defect; **P3** = observability / maintainability defect.

### A. Production reliability, CDP, and storage

| ID | Severity | Exact evidence in audited `main` | Failure / race scenario | Required disposition |
|---|---|---|---|---|
| PR-01 | **P0** | `Terminal/realtime_signal_evaluator.py:164-171` uses backslash continuations followed by malformed `CRCRLF`; `compileall` and test collection report `SyntaxError` at line 328. | Any import of the evaluator prevents the unified/wake execution graph from starting; test coverage is partially masked by collection failure. | Normalize the file first; patch does this and adds compile/import regression coverage. |
| PR-02 | **P1** | `Terminal/runtime_io.py:63-84` acquires `FileMutex` then explicitly unlinks its lock path in `finally`. | A holds inode A, B blocks on inode A; A unlinks it; C creates/acquires inode B while B later acquires inode A. Concurrent read-modify-write can lose signal/data rows. The executable proof reports `BASELINE_RACE_REPRODUCED=True`. | Never unlink a held lock path; preserve lock inode. Patch proves `PATCHED_RACE_REPRODUCED=False`. |
| PR-03 | **P1** | `Terminal/runtime_io.py:87-109` catches and ignores fsync failure, has no directory fsync, and only a ~310 ms retry window for Windows `PermissionError`. | A power loss or NTFS reader/AV handle can yield a publish reported as complete without durable metadata, or surface a transient sharing violation. | Patch propagates write/fsync error, retries `os.replace` for 2 seconds, and fsyncs POSIX parent directories. Windows still needs an integration test on NTFS. |
| PR-04 | **P1** | `Terminal/runtime_io.py:161-178` accepts gaps up to three days because it checks `% 900000` and `<=259200000`, not exact M15 adjacency. | A missing 15-minute segment passes `completed_candles`, contaminating CVD/auction windows and closed-bar decisions. | Patch requires every tail difference to equal `900000` ms. |
| PR-05 | **P1** | `1_coinglass_footprint_scraper.py:118-155`, `2_coinglass_whales.py:76-111`, `3_coinglass_liquidation_levels.py:83-118`, and `4_coinglass_tv_scraper.py:107-145` operate on `127.0.0.1:9222`; shared coordination is only a best-effort `BrowserLock` in `Terminal/collector_lock.py:188-203`. | Two collectors can target different tabs during reuse/cleanup, a user tab can be closed by an orphan sweep, or a stale DevTools target can produce silent empty results. | Use a single brokered collector process with target IDs, leases, navigation epochs, and health/ownership checks—or retire CDP per `AGENTS.md:12`. |
| PR-06 | **P2** | `1_coinglass_footprint_scraper.py:171-203` walks private React Fiber to create `window.__cg_term`; `2_coinglass_whales.py:290,324` depends on virtualized `.lo-bt-body`; `4_coinglass_tv_scraper.py:156-217` reaches private widget/model APIs. | A React deployment changes Fiber fields, a virtualized viewport has only partial rows, an iframe detaches, or TradingView changes an internal model. Collection may return partial/empty but syntactically valid data. | No live reliability claim is possible without a versioned vendor API/schema contract and canary validation. Patches do not pretend to solve vendor DOM fragility. |
| PR-07 | **P2** | `3_coinglass_liquidation_levels.py:469-497` clicks `.ll-overlay button`, then rotates WARP and sends `Page.reload`. | Retry or reload can race navigation/context destruction; IP rotation cannot guarantee a vendor challenge is resolved and should not be used to evade access controls. | Fail closed with an explicit collector-unavailable state. Obtain vendor approval/API access rather than treating WARP as a service-recovery mechanism. |
| PR-08 | **P1** | `Terminal/unified_api_footprint_engine.py:136-139` initializes MT5 even in crypto-only use; `:239` fetches exactly the rolling window; `:400-423` saves before filtering/dedupe; `:455-458` increments `processed_count` after any nonexception result. | Crypto-only service couples availability to MT5; forming Binance candle is persisted, dedupe key is burned before durable append, and empty fetch is reported as processed. | Patch gates MT5, fetches one extra candle and drops the forming bar, returns `None` on insufficient data, and marks dedupe only after successful persistence. |
| PR-09 | **P1** | `5_wake_on_footprint_signal.py:241-243` advances and persists offset before evaluation; `:258-264` accepts producer-provided `bar_closed=True`; `:289-293` increments dispatch count regardless of broker result. | Crash/evaluator fault permanently skips unread signals; a forged/stale flag bypasses closed-bar policy; a rejected order is counted as dispatched. | Patch defers offset commit, derives closure from timestamp only, and increments only for a successful non-rejected result. |

### B. WARP and collector coordination

`Terminal/collector_lock.py:79-94` probes `api.ipify.org` repeatedly with individual 2.5-second requests. The function computes `changed` at line 89 but returns success at line 90 regardless of it. `is_warp_rotating()` treats the lock as active for only 15 seconds (`:97-107`), while an order lock is manually expired at 20 seconds before the WARP mutex is obtained (`:119-149`). Stage-2 registration commands ignore process return codes (`:165-175`); the `104.*` address heuristic (`:155-160`) is not a reliable WARP classifier; and cooldown state is read before mutual exclusion (`:116-140`). `_set_last_rotation_time` is a non-atomic `write_text` (`:65-72`).

This produces two important races:

1. **Transmission/rotation race:** a slow UI operation can exceed 20 seconds; a collector removes its marker and changes network identity during an order interaction.
2. **Dual-rotator race:** two collectors pass cooldown/order checks before either owns the WARP mutex; state writes/reloads then interleave.

The patch uses `FileMutex` for both rotation and the order-critical section, checks the order mutex while holding the rotation mutex, makes cooldown update atomic, expands stabilization to 10 seconds, requires a known *changed* IP, and fails closed on stage failures. This only improves local serialization; it does **not** make anti-bot/IP rotation an approved or reliable production strategy.

### C. Signal mathematics and statistical validity

| ID | Severity | Exact evidence | Finding and impact |
|---|---|---|---|
| SM-01 | **P1** | `Terminal/universal_footprint_engine.py:198-218` synthesizes delta from OHLC body/range/wick skew and derives buy/sell volume from it. | This is a model proxy, not taker-side trade flow. It cannot validate market-buy/sell imbalance, true CVD, or footprint absorption. Label it `ohlc_proxy`, do not market it as institutional order flow, and do not use it alone for execution. |
| SM-02 | **P1** | `Terminal/universal_footprint_engine.py:342-362` calls absorption when wick >= 30% of **bar range**, volume >= 1.1× SMA, and close position is opposite; `:375-396` applies ±5% delta only to an opposing-close condition. | The requested rule is “>=30% **wick volume** with opposing close.” No wick-level taker-volume field exists, so it cannot be implemented honestly from these bars. The patch leaves the proxy explicit rather than fabricating wick volume. |
| SM-03 | **P2** | `Terminal/universal_footprint_engine.py:217-218` calculates CVD as `cumsum()` for the supplied frame; divergence logic begins at `:405`. | CVD restarts when a 300-bar frame rolls/reloads; cross-window swing divergence is non-stationary. Persist a session/epoch CVD anchor and test divergence against independently computed signed trade data. |
| SM-04 | **P2** | Exhaustion at `:445-456` permits low volume **or** a 35% wick; trapped trader descriptions at `:482-493` lack a mandatory delta/taker confirmation. | These rules can label ordinary range reversals as trapped/exhausted. Require a failed auction plus signed-flow confirmation and holdout calibration. |
| SM-05 | **P1** | `Terminal/realtime_signal_evaluator.py:153` starts score at 40; `:174-303` adds/subtracts overlapping 5–15 point rules; `:415-416` clips to 0–100 and applies `75 + hurdle`. | This is not the requested 35/25/20/20 footprint/liquidation/whale/macro weighting. Correlated evidence can be counted multiple times; clipped scores have no probability or calibration meaning. Patch changes invalid feed handling to fail closed, but a calibrated scorecard is still required. |
| SM-06 | **P1** | `Terminal/olap_engine.py:65-91` reads `decay_weight`-bearing data but aggregates raw `remaining_usd`; `:300+` starts a score from 50 even where streams are invalid. | Stale walls can retain full influence and invalid sources can look neutral rather than unavailable. Patch applies `exp(-(order_age + snapshot_age)/tau)`, exposes raw/weighted fields, and returns a neutral non-actionable result with `data_valid=False` if any required feed is invalid. |
| SM-07 | **P2** | `Terminal/auction_indicators.py:338-355` uses all rows and UTC dates; `:385-396` selects global highest-volume bins, not contiguous bins around POC. | Forming bars may enter auction levels; value area can contain holes and have no auction interpretation. Patch filters completed candles and expands contiguously from POC to 70% volume. Session timezone must still be an explicit trading-policy parameter. |

### D. Risk, execution, and broker control

| ID | Severity | Exact evidence in audited `main` | Failure scenario | Patch posture |
|---|---|---|---|---|
| EX-01 | **P0** | `Terminal/matchtrader_live_trader.py:53-61`: `INITIAL_BALANCE=100000`, total floor 90000, daily floor 95000, default risk $500. | A production order can be sized/governed for an account 20× larger than the stated $5,000 challenge. | Replaces constants with $5,000, $4,775, $25, 2; hard-disables live dispatch pending commissioning. |
| EX-02 | **P0** | `Terminal/realtime_signal_evaluator.py:399-406` calls `calculate_lot_size(..., 500.0)`, forces `realized_risk=500.0`, `effective_max_risk=500.0`, and `geom_valid=True`. | A MatchTrader-mapped symbol becomes executable without broker geometry, stop-distance, lot-grid, or true risk validation. | Removes fabricated qualification; requires broker validation and provides no MT5 fallback for a MatchTrader symbol. |
| EX-03 | **P0** | `Terminal/matchtrader_live_trader.py:287-292` `touch()`es an inflight file; `:497-502` unlinks it; `:485-495` returns `DISPATCHED` and defaults `val.get('ok', True)`. | Two senders race; a UI selector failure or partial result may be presented as a successful order. | Uses `FileMutex`, validates authorization/geometry/equity/risk/floor, makes disabled dispatch reject, and fails closed on missing confirmation. |
| EX-04 | **P1** | `Terminal/matchtrader_live_trader.py:530-534` defaults missing lots to .01 and missing SL/TP to zero, then sends the order. | A malformed evaluation can send an unbracketed or wrong-size market order. | Patch rejects absent/invalid brackets and geometry; broker acknowledgement is still a commissioning requirement. |
| EX-05 | **P1** | `5_wake_on_footprint_signal.py:280-293` independently calls `execute_signal`; `Terminal/unified_api_footprint_engine.py:499-507` independently evaluates new signals; baseline evaluator persists alerts. | One observed signal can traverse multiple execution-adjacent paths (duplicate dispatch risk). | Patch removes evaluator storage-side dispatch, persistence-gates unified evaluation, and leaves one explicit path for future authorized dispatch. |
| EX-06 | **P2** | `Terminal/test_single_source_of_truth.py:17-40` only regexes selected exact constant names. | `$100000` and `$500` aliases in another module pass the test, masking drift. | The new regression asserts MatchTrader constants against `risk_envelope`; add an AST policy test before enabling live dispatch. |

`Terminal/risk_envelope.py:29-43` is the strongest baseline component: it already expresses the $4,775 hard floor, $25 cap, two-position maximum, stress reserve, and survival-loss principle. Its docstring example at `:99-107` is malformed explanatory pseudo-code, however, and baseline `budget_per_trade` did not validate non-finite/negative capital before calculations. The patch adds `INVALID_CAPITAL_STATE`, uses the explicit $5,000 initial balance, and rejects a cap above mandate.

### E. Data, time, and latency observations

The static profile in [`evidence/data-audit.txt`](evidence/data-audit.txt) and [`evidence/data_audit.json`](evidence/data_audit.json) covered **262 Parquet files**, **164,771 rows**, and **287,752 null values** at `2026-11-03T10:40:47Z`.

* `Database/Candle_Data/` had 31 crypto assets × 300 rows; tested M15 intervals had no gaps and IST columns matched UTC+05:30. This is useful evidence of a healthy *crypto* snapshot.
* There were **zero macro candle files** although the registry/claims include MT5 macro pairs. Forex, metals, commodities, and indices therefore do not meet the same historical-data claim.
* ADA, AVAX, BNB, ETH, and LTC each contained one forming row and 3,300 nulls in relevant fields. `ny_vwap` was null for all 31 assets; roughly 20/31 whale/liquidation anchors were null.
* Signal storage had 13,592 records (`coinglass_cdp` 12,602; `universal_footprint_crypto` 624; null source 366) and no duplicate storage keys, but whale legacy IST fields are heavily null (for example ETH 19,871/21,071) and `whale_market_snapshots.parquet` mixes two schemas.
* The stored order age decay is consistent with `tau≈2596.6s`, but baseline OLAP did not actually consume that decay in its wall aggregation.

### Latency measurements (not a live E2E SLO)

[`evidence/latency_benchmark_at_commit.json`](evidence/latency_benchmark_at_commit.json) is an offline benchmark over stored Parquet; time was frozen to the target snapshot time for validity checks. It is **not** a CDP/vendor/browser/broker latency measurement.

| Operation | Sample | p50 | p95 | Max | Valid at frozen snapshot |
|---|---:|---:|---:|---:|---:|
| Whale wall query | 12 symbols | 10.28 ms | 23.58 ms | 37.17 ms | 12/12 |
| Liquidation query | 12 symbols | 8.76 ms | 9.80 ms | 10.02 ms | 0/12 |
| CVD trend query | 12 symbols | 9.65 ms | 11.42 ms | 11.58 ms | 7/12 |
| Auction profile | 12 symbols | 60.18 ms | 65.93 ms | 66.57 ms | 10/12 |
| `scan_all_confluences` | 8 symbols | 224.62 ms total | — | — | results included invalid feeds |
| Universal footprint BTC / 300 bars | 10 runs | 166.93 ms | — | 193.68 ms | 3 signals last run |

The micro-latencies are adequate for a 15-minute decision horizon. They do **not** establish availability: a zero-valid liquidation sample and partial CVD sample mean a fast answer is often an invalid answer. The patch makes that state non-actionable rather than silently neutral.

## Drop-in patch bundle

All patches are ordinary unified diffs against the stated commit. Apply only to a clean checkout of that revision:

```bash
git apply --check --whitespace=error docs/audits/main-c064c303/patches/ALL_CHANGES.patch
git apply --whitespace=error docs/audits/main-c064c303/patches/ALL_CHANGES.patch
```

| Patch | Main effect |
|---|---|
| `patches/1_coinglass_footprint_scraper.py.patch` | Emits only closed-bar JSONL signals, adds IST/close metadata, uses stable 3-part dedupe and bounded recent-key reload. |
| `patches/4_coinglass_tv_scraper.py.patch` | Adds canonical ms/IST/closed state, derives summaries only from closed liquidation bars, adds evaluator aliases. |
| `patches/5_wake_on_footprint_signal.py.patch` | Commits read offset only after evaluation, computes closure from time, counts only genuine dispatch success, uses $5,000 guardian default. |
| `patches/Terminal__runtime_io.py.patch` | Preserves writer-lock inode, makes publish durable/retry-aware, requires exact M15 continuity. |
| `patches/Terminal__collector_lock.py.patch` | Serializes WARP/order critical regions, atomically records cooldown, validates IP change and command outcomes, fails closed. |
| `patches/Terminal__risk_envelope.py.patch` | Adds capital-state validation and explicit $5,000/$25 cap protections. |
| `patches/Terminal__matchtrader_live_trader.py.patch` | Corrects challenge constants, introduces an OS mutex and strict authorization/geometry/risk gates, sets `LIVE_DISPATCH_ENABLED=False`. |
| `patches/Terminal__realtime_signal_evaluator.py.patch` | Repairs line endings/syntax, rejects unknown/forming/stale bars, validates feeds and broker sizing fail-closed, removes automatic storage-side dispatch. |
| `patches/Terminal__olap_engine.py.patch` | Applies decay-weighted walls, exposes raw/weighted fields, handles absent duration safely, and makes invalid confluence fail closed. |
| `patches/Terminal__unified_api_footprint_engine.py.patch` | Avoids MT5 in crypto-only mode, removes forming bars, persistence-gates dedupe/evaluation, and avoids false processed counts. |
| `patches/Terminal__auction_indicators.py.patch` | Excludes forming candles and computes contiguous 70% value area around POC. |
| `patches/Terminal__test_pipeline_hardening.py.patch` | Gives liquidation test fixture a genuine closed timestamp. |
| `patches/Terminal__test_audit_regressions_c064c303.py.patch` | Adds 10 offline regression tests: import/dispatch, challenge constants, risk cap, lock inode, M15 gap, WARP/order mutex, decay including missing duration, and closed-bar persistence. |

The bundle deliberately does **not** invent unavailable wick-taker volume, macro price history, a vendor contract, or a live broker acknowledgement. Those require data and staged integration, not a source-code patch.

## Patch validation

The patch bundle is application-clean:

* `git apply --check --whitespace=error`: **PASS** against a freshly extracted `main@c064c303...`.
* Actual apply followed by byte comparison of all 13 changed paths with the validated patched tree: **PASS — 0 mismatches**.
* `python -m compileall -q .`: **PASS** (exit 0).
* Offline collectible suite (network sockets disabled, three unrelated missing-module test files excluded): **55 passed, 2 warnings**.
* Full offline suite: exit 2 due to three **pre-existing missing production modules**—`Terminal.coinalyze_rest_collector`, `Terminal.liquidation_map_collector`, and `Terminal.coinglass_tv_liquidation_collector`. This remains a release blocker; it is not hidden by the patch.
* Lock-race proof: baseline `BASELINE_RACE_REPRODUCED=True`; patched `PATCHED_RACE_REPRODUCED=False`.

Exact commands and raw outputs are retained under [`evidence/`](evidence/), particularly [`test-run-command.txt`](evidence/test-run-command.txt), [`patch-application-verification.txt`](evidence/patch-application-verification.txt), and the corresponding pytest/log files.

## Commissioning gates to reach 75 / 100

The score must not be raised by documentation alone. Require all of the following, in sequence:

1. **Build and test integrity** — implement or remove the three missing collectors/tests; make the unfiltered offline suite green; add CI `compileall`, `git diff --check`, and AST-based risk-constant policy checks.
2. **Single approved ingestion architecture** — choose vendor-approved APIs or a governed browser integration. If browser use is retained, introduce a one-owner CDP service, target lease/heartbeat, schema/version canaries, viewport-complete virtual-scroll proof, and explicit unavailable outcomes. Do not rely on IP rotation to defeat vendor controls.
3. **Data contract and observability** — define schema versions and freshness budgets; record source timestamp, capture timestamp, latency, row count, null rate, completeness, and a `data_valid` decision for every source. Supply 300 closed M15 bars and IST mappings for every claimed macro instrument.
4. **Statistical validation** — ingest true aggressor-side/wick-level trade data or relabel/decommission absorption/CVD claims. Replace additive score with a documented 35/25/20/20 feature contract, correlation controls, walk-forward calibration, PnL/slippage/turnover tests, and confidence intervals by asset/regime.
5. **Execution simulation** — run deterministic broker-DOM fixtures and a paper account against account `101305360` rules. Prove account identity, opening equity/day baseline, open positions, all lot constraints, bounded loss at stop including spread/slippage, placement acknowledgement, bracket attachment acknowledgement, idempotency key, and reconciliation after restart.
6. **Release control** — keep `LIVE_DISPATCH_ENABLED=False` until an accountable owner approves a change-controlled enablement. Enable only with an independent kill switch, reconciliation dashboard, immutable audit log, alerting on stale/invalid feeds, and a tested rollback.

## Residual risk after applying the patches

The patched state is safer and buildable, not production certified. It still has unresolved vendor DOM dependence, unimplemented modules, incomplete macro data, proxy microstructure features, no broker integration proof, no full-suite green result, and no live end-to-end latency/failure measurement. Accordingly, the patch bundle changes the system from **unsafe/broken** to **offline-testable and fail-closed by default**; it does not authorize trading.
