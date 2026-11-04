# Round 5 institutional-readiness audit — `327bde42d542ac3759c6df5babf9a2cb3cc375a8`

**Audit date:** 2026-11-04 (Asia/Kolkata)

**Decision:** **45 / 100 — FAIL. Not approved for production/live enablement.**

## Target identity and audit boundary

The requested repository URL was `https://github.com/kbsingh1399/_Trading_1.git`. It was not accessible to this Arena session: both `git ls-remote` and `gh repo view` returned HTTP/GraphQL authorization failures. The configured checkout remote is instead `https://github.com/kbsingh1399/Trading_2.git`.

To avoid claiming access to the inaccessible repository, this audit is explicitly of the immutable object available in the configured checkout's `origin/main`:

```text
327bde42d542ac3759c6df5babf9a2cb3cc375a8
fix(institutional): remediate all Round 4 audit findings for Delta live execution and DeepSeek bridge
```

A fresh `git archive` of that exact object was inspected at `.audit/327bde4/target`. All source review, compilation, tests, and probes used that archive. No production source was changed. No Delta/broker endpoint, order route, browser/CDP endpoint, collector, or external AI API was called; all behavioral probes used mocks.

The inaccessible `_Trading_1` repository is a material provenance limitation. If it is intended to be a different repository, its GitHub access must be reconnected in Arena and it must be audited separately.

## Executive result

Round 4's Sentinel syntax defect is repaired, the targeted Delta guardian remediation tests pass, JSONL ledger handling is now materially improved, and evaluator construction no longer has an uninitialized DeepSeek attribute path. These are real improvements.

They do **not** amount to strict remediation of the requested findings. The DeepSeek bridge still approves when its required key is absent, accepts an arbitrary timeout above five seconds, does not apply a 150 MB cap, and never assigns an actual spawned child to its Job Object. The legacy MT5/MatchTrader execution implementations and hardcoded proprietary account artifacts remain. The full Terminal suite cannot collect, and Python 3.14 is unavailable in the audit runtime.

### Scorecard

| Category | Weight | Score | Basis |
|---|---:|---:|---|
| Delta execution, collateral, brackets, and guardian | 30 | 17 | Per-active-collateral 20% checks, exact-symbol matching, post-fill rejection status, and stop-penetration close are present/tested; actual SL-edit and emergency-close failures are not confirmed or escalated, and sizing still treats the first eligible wallet as USD. |
| DeepSeek containment, SLA, fail-closed behavior, evaluator safety | 25 | 5 | Evaluator attribute safety is repaired, but the required containment, 150 MB cap, maximum 5 s limit, and missing-key fail-closed behavior are absent. |
| Cross-asset ledger and bar admissibility | 15 | 14 | JSONL is the primary ledger, the JSON mirror is atomically written, and Sentinel logs/rejects stale, non-time, and misaligned bars. |
| Legacy venue/browser/prop-firm removal | 15 | 1 | Active evaluator routing is Delta-only, but executable MT5 and MatchTrader/DOM bridges remain and `venue_admission.py` retains hardcoded MT5 identities and APIs. |
| Build and acceptance verification | 15 | 8 | Compilation and 38 focused Python 3.11 tests pass; the full Terminal suite has three import/collection errors and Python 3.14 is unavailable. |
| **Total** | **100** | **45** | **50 points below the requested >=95 readiness threshold.** |

## Verification performed

### Build and test evidence

| Check | Result | Notes |
|---|---|---|
| `python -m compileall -q Terminal 1_coinglass_footprint_scraper.py 5_wake_on_footprint_signal.py` | **PASS** | The Round 4 unterminated f-string/Sentinel compilation blocker is repaired. |
| Focused socket-blocked tests | **PASS: 38 passed in 0.58 s** | Python 3.11.2, `pytest-socket --disable-socket --allow-unix-socket`; files: Delta guardian, cross-asset stack, DeepSeek bridge, Sentinel, and SSOT. |
| Full `pytest Terminal/ -v --disable-socket --allow-unix-socket -p no:cacheprovider` | **FAIL at collection** | Pytest reported 110 collected items plus 3 import errors: missing `Terminal.coinalyze_rest_collector`, `Terminal.liquidation_map_collector`, and `Terminal.coinglass_tv_liquidation_collector`. |
| Claimed 92-test acceptance | **NOT VERIFIED / FAIL** | The archive has 18 `Terminal/test_*.py` files and 121 statically discoverable `test_*` functions, not a clean verifiable 92-test suite. |
| Python 3.14 verification | **NOT AVAILABLE** | The sandbox only supplies Python 3.11.2; `python3.14` is absent. Python 3.11 evidence is not a Python 3.14 certification. |

Raw audit-local output is retained under `.audit/327bde4/`, including `compileall-round5.txt`, `pytest-terminal-round5-py311.txt`, and `pytest-round5-remediation-focused.txt`.

## Strict remediation findings

Legend: **PASS** means the requested behavior is source- and offline-test-evidenced; **PARTIAL** means a material operational failure path remains; **FAIL** means source/probe evidence contradicts the requirement.

| Requested Round 5 criterion | Verdict | Evidence |
|---|---|---|
| Every active USD/USDT/INR wallet has a >=20% available/balance buffer, with aggregate check | **PARTIAL** | `_execute_delta_signal_guarded()` loops USD, USDT, and INR balances, rejects any positive-balance asset below 20%, and rejects aggregate below 20% (`Terminal/delta_exchange_live_trader.py:915-955`). Focused `test_multi_collateral_margin_floor_rejection` passes. However aggregate balances are summed across USD, USDT, and INR without normalization, and `get_available_margin_usd()` returns the first eligible wallet value while labeling it USD (`:225-234`). Margin sizing at `:1011-1023` therefore depends on API ordering and can use INR/USDT units as dollars. This is not institutional-grade aggregate collateral accounting. |
| Actual-fill, directional-tick >=2R bracket anchoring | **PARTIAL** | The re-anchor routine derives risk from `fill_price`, floors an inward risk cap, and ceilings TP ticks (`delta_exchange_live_trader.py:398-426`); existing targeted tests for fill re-anchor and cap pass. The caller invokes it after fill and returns `REJECTED_FAILED_BRACKET_ANCHOR` if it returns false or raises (`:1043-1069`). But the SL edit result at `:414-420` is ignored. An offline mock forced that SL edit to fail while TP edit succeeded; the function still returned `{'ok': True, 'status': 'REANCHORED'}` and reported the unpersisted tighter SL. This fails strict confirmation of the actual bracket/risk anchor. Evidence: `.audit/327bde4/delta-sl-edit-failure-round5-probe.txt`. |
| Failed post-fill anchor closes/rejects safely | **PARTIAL** | The caller now issues `close_position()` and returns `REJECTED_FAILED_BRACKET_ANCHOR` when `reanchor_brackets_to_fill()` fails/raises (`delta_exchange_live_trader.py:1050-1069`); `test_failed_reanchor_triggers_emergency_close` passes. The returned close result is not checked, retried, or recorded in the rejection. A rejected bracket anchor plus rejected emergency close can still leave exposure live without a distinct critical status. |
| Existing SL penetration performs immediate emergency close | **PASS** | Guardian checks long `mark <= SL` and short `mark >= SL`, calls `close_position`, and reports `EMERGENCY_CLOSE_STOP_PENETRATED` (`delta_exchange_live_trader.py:662-683`). The targeted emergency-close test passes. |
| Exact symbol match prevents `AR` versus `NEARUSD` collision | **PASS** | `has_open_position()` normalizes and compares equality, rather than substring membership (`delta_exchange_live_trader.py:248-256`). `test_exact_symbol_match_prevents_substring_collision` passes. |
| Win32 kill-on-close Job Object actually contains an dsh process | **FAIL** | The module defines Job Object helper functions (`deepseek_harness_bridge.py:47-145`) and sets the kill-on-close flag. It does not spawn a dsh child, call `assign_process_to_job`, or call `terminate_process_tree` in its production lifecycle. Outside the bridge/tests, the only integration found is evaluator construction at `realtime_signal_evaluator.py:188`. Helper definitions are not process containment. |
| DeepSeek bounded at <=150 MB and <=5.0 seconds, failing closed | **FAIL** | The 150 MB structures are declared but no memory limit flag/value is applied; there is no RSS monitoring or 150 MB enforcement. `__init__` saves caller-provided `timeout_seconds` unchanged (`deepseek_harness_bridge.py:154-165`) and passes it unchanged to `urlopen` (`:202-204`). Offline probe: a bridge configured with `timeout_seconds=60.0` invoked `urlopen(..., timeout=60.0)`. More seriously, no API key produces `{'approved': True, 'status': 'FALLBACK_ALGORITHMIC'}` (`:232-240`); the offline probe reproduced that approval. Both contradict fail-closed approval and five-second requirements. Evidence: `.audit/327bde4/deepseek-round5-probes.txt`. |
| DeepSeek evaluator integration avoids an uninitialized attribute error | **PASS** | The evaluator creates `self.dsh_bridge` in a guarded import and explicitly sets it to `None` on failure (`realtime_signal_evaluator.py:186-191`). The use site protects access with `getattr(self, 'dsh_bridge', None)` and turns raised critic errors into an actionability veto (`:1003-1031`). This is safe attribute initialization, but it cannot compensate for the bridge's missing-key approval. |
| `.jsonl` is primary ledger with atomic JSON mirror | **PASS** | `EvaluatedSignalsLedger` uses `evaluated_signals_ledger.jsonl` as primary (`asset_signal_stack.py:49-85`), atomically replaces a full JSONL snapshot (`:147-158`), then syncs the JSON mirror through `atomic_json` (`:160-164`). An isolated temp-ledger probe created both files and verified the JSONL record/mirror key. Evidence: `.audit/327bde4/ledger-round5-probe.txt`. |
| Stale >30m, non-time, and misaligned bars are rejected and logged | **PASS** | Sentinel logs and ledger-records stale signals as `DISCARDED_STALE` when close time exceeds 30m (`5_wake_on_footprint_signal.py:305-320`), and logs/records non-time or off-grid inputs as `DISCARDED_NON_TIME_BAR` (`:322-337`). The evaluator independently adds `NON_TIME_BAR_DISCARDED` and `MISALIGNED_TIMESTAMP_DISCARDED` validation errors (`realtime_signal_evaluator.py:845-857`). |
| MatchTrader, browser DOM, and prop-firm execution are excised from live execution; no proprietary hardcoding in admission | **FAIL** | `Terminal/matchtrader_live_trader.py` remains a MatchTrader/FundingPips CDP/DOM execution bridge with proprietary account IDs and executable order logic (`:1-14`, `:309-580`). Its current `LIVE_DISPATCH_ENABLED = False` guard is a safety flag, not excision. `Terminal/mt5_live_trader.py` retains `execute_mt5_signal()` and calls `mt5.order_send()` (`:141-308`) without an equivalent immutable dispatch gate. `Terminal/venue_admission.py` retains `EXPECTED_MT5_LOGIN`, `VALID_MT5_LOGINS`, `mt5_account_book(mt5)`, and `native_size(mt5, ...)` (`:12-114`). The evaluator has removed active MT5 dispatch, but the strict repository-removal criterion is unmet. |

## Production blockers and required remediation

1. Make fill re-anchoring transactional: obtain/validate actual child SL and TP prices, reject any failed SL or TP edit, cancel/close with confirmed result, and raise an unmistakable critical incident when flattening fails.
2. Replace first-wallet/nominal-sum logic with product-collateral-aware, FX-normalized balances and an explicit reserve model. Do not label INR/USDT as USD.
3. Make the bridge truly fail closed for missing keys, malformed answers, excessive latency, and resource-limit failures. Clamp timeout to `min(requested, 5.0)`.
4. Either remove dsh process-containment claims or implement a real owned child lifecycle: spawn, assign process to a Job Object, set/enforce the 150 MB limit, and close/kill in all exit/error paths.
5. Remove `mt5_live_trader.py`, `matchtrader_live_trader.py`, proprietary admission helpers/account identifiers, and browser-DOM order logic from the production distribution or place them in non-executable archived material outside all production paths.
6. Restore the three missing collector modules or correct/remove their checked-in tests, then run the complete suite under an actual Python 3.14 environment before claiming the 92-test acceptance gate.

## Audit artifacts

- `.audit/327bde4/compileall-round5.txt`
- `.audit/327bde4/pytest-terminal-round5-py311.txt`
- `.audit/327bde4/pytest-round5-remediation-focused.txt`
- `.audit/327bde4/deepseek-round5-probes.txt`
- `.audit/327bde4/ledger-round5-probe.txt`
- `.audit/327bde4/delta-sl-edit-failure-round5-probe.txt`
- `.audit/327bde4/test-inventory-round5.txt`

No production behavior was modified by this audit report.
