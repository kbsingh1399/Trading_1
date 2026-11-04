# Round 6 institutional quantitative and production-safety audit — `3eaeebc3a70c8b5d5d3c991963f97974c5f031ea`

**Audit date:** 2026-11-04 (Asia/Kolkata)

**Target:** exact `origin/main` object `3eaeebc3a70c8b5d5d3c991963f97974c5f031ea`:

```text
fix(institutional): remediate all Round 5 audit findings for transactional brackets, FX margin, DeepSeek containment, and legacy excision
```

**Decision:** **69 / 100 — FAIL. Not approved for production/live enablement.**

A fresh `git archive` of the exact object was tested under `.audit/3eaeebc/target`. No production source was modified. No live Delta/broker/order, browser/CDP, collector, or DeepSeek endpoint was called. Runtime checks use local subprocesses or patched/mocked transport/order functions only.

## Executive result

This commit substantially remediates Round 5: FX-normalized collateral accounting is implemented; SL edits now fail closed; the normal re-anchor failure path records flatten confirmation and raises a critical incident if close submission fails; DeepSeek key/timeout policy is materially safer; and executable MatchTrader/MT5 bridge code has been replaced by stubs.

It does **not** strictly satisfy all Round 6 acceptance criteria:

1. The `except` branch for a re-anchor exception still calls `close_position()` without checking its result, setting `flatten_confirmed`, or emitting the required CRITICAL incident on failed flattening.
2. The Windows Job assignment result is ignored, so a child can be reported/tracked as contained when `AssignProcessToJobObject` failed. Windows behavior and the memory limit cannot be exercised in this Linux audit.
3. The MatchTrader stub returns status `REJECTED`, not the explicitly required `REJECTED_DECOMMISSIONED_BRIDGE`.
4. The requested full suite collects all 114 tests with no import/collection errors, but finishes **2 failed, 112 passed**. A zero-failure test gate is therefore not met.

### Institutional scorecard

| Category | Weight | Score | Basis |
|---|---:|---:|---|
| Transactional brackets and emergency flattening | 25 | 15 | SL/TP edit result gates are correct and the ordinary failure path is fail-closed, but exception-path flatten failure is not confirmed/escalated. |
| FX-normalized product collateral and buffers | 20 | 19 | Explicit USD/USDT/INR normalization and per-wallet/aggregate gates are implemented and tested. |
| DeepSeek containment and fail-closed operation | 25 | 17 | Missing-key rejection, timeout clamp, memory-limit configuration, and local child cleanup are present; Windows Job assignment is not verified/fail-closed. |
| Legacy execution decommissioning | 15 | 10 | Browser/order logic is removed from the two named bridges and proprietary login constants are gone, but the MatchTrader status contract is not met and generic MT5 protocol helpers remain in admission. |
| Build and full-suite integrity | 15 | 8 | Compilation passes and pytest collects 114 cleanly; two trade-critic lifecycle tests fail. |
| **Total** | **100** | **69** | **Below the required >=95 institutional-production threshold.** |

## Test and build verification

| Check | Result | Evidence |
|---|---|---|
| `pytest.ini` discovery boundaries | **PASS** | The exact archive has `testpaths = Terminal`, `norecursedirs = legacy_archive archive .git .dsh_home .pytest_cache`, and `python_files = test_*.py`. The three formerly uncollectable collector tests are archived under `legacy_archive/tests/`. |
| Source compilation | **PASS** | `python -m compileall -q Terminal 1_coinglass_footprint_scraper.py 5_wake_on_footprint_signal.py` exited 0. |
| Required SL-edit regression | **PASS** | `Terminal/test_delta_position_guardian.py::test_sl_edit_failure_returns_false_and_does_not_claim_reanchored` passed. |
| Focused Round 6 Delta/DeepSeek tests | **PASS: 11 passed** | Socket-blocked run covered the required SL test, FX normalization test, and all nine `test_deepseek_harness_bridge.py` tests. |
| Full suite | **FAIL: 2 failed, 112 passed; 114 collected; 0 collection/import errors** | `python -m pytest Terminal/ -v` from the archive completed in 554.97 seconds under Python 3.11.2. `test_post_mortem_learning_and_retention` and `test_similarity_threshold_exact_boundaries` both received critic verdict `CLEAR` where the tests require `HARD_VETO` or `HIGH_PENALTY`. |

The 114-item collection result is a real improvement over Round 5's three missing-module collection errors, but it is not the requested clean 114/114 run. The available interpreter is Python 3.11.2; no Python 3.14 claim is made.

## Round 6 blocker-by-blocker assessment

Legend: **PASS** = implemented and offline-evidenced; **PARTIAL** = substantial remediation with a material safety/verification gap; **FAIL** = contradicts an explicit acceptance requirement.

| Requirement | Verdict | Evidence and finding |
|---|---|---|
| Re-anchor validates failed SL edits and never reports `REANCHORED` | **PASS** | When capped risk requires a new SL, `reanchor_brackets_to_fill()` evaluates `sl_success` and returns `ok=False, status=SL_EDIT_FAILED` before TP processing if it fails (`Terminal/delta_exchange_live_trader.py:461-476`). TP edits similarly check `is_success` and return `TP_EDIT_FAILED` (`:500-515`). The named required test passes. |
| Any failed re-anchor emergency-flattens, confirms the result, logs critical flatten failure, and returns `REJECTED_FAILED_BRACKET_ANCHOR` | **PARTIAL** | In the non-exception failure case, code calls `close_position()`, derives `is_flattened`, emits `logger.critical` on failure, returns `REJECTED_FAILED_BRACKET_ANCHOR`, and includes `flatten_confirmed` (`delta_exchange_live_trader.py:1108-1122`). `close_position()` constructs an opposing `market_order` with `reduce_only=True` (`:861-888`). But the `except Exception` path at `:1123-1132` only calls `close_position()` and returns: it neither checks the response, nor logs a critical incident, nor returns flatten confirmation. Offline probe forced a re-anchor exception and `{success: False}` close result; output contained `REJECTED_FAILED_BRACKET_ANCHOR`, `close_called`, and `critical_called=False`. Evidence: `.audit/3eaeebc/round6-offline-probes.txt`. |
| FX conversion is explicit and available margin is genuinely USD-normalized | **PASS** | `convert_to_usd()` maps USD/USDT 1:1 and INR via `/87.0` (`delta_exchange_live_trader.py:224-235`). `get_wallet_balances_normalized_usd()` retains raw and normalized values and sums only normalized figures (`:237-272`); `get_available_margin_usd()` returns normalized total availability (`:274-281`). Offline USD+USDT+INR probe produced balance `$250.00`, availability `$155.00`, rather than nominal INR summation. |
| Every collateral wallet has >=20% buffer and aggregate uses FX-normalized USD | **PASS** | Dispatch checks every positive USD, USDT, or INR balance's native ratio and rejects below 20%; it converts each balance/availability before calculating the aggregate 20% buffer (`delta_exchange_live_trader.py:971-1013`). `test_fx_normalized_collateral_margin` and the existing per-wallet rejection test pass. |
| Missing DeepSeek API key fails closed | **PASS** | `evaluate_signal_confluence()` returns `ok=False`, `approved=False`, and `REJECTED_MISSING_API_KEY` before API use (`deepseek_harness_bridge.py:245-253`). Unit test and offline probe pass. |
| DeepSeek timeout is clamped to <=5.0 seconds | **PASS** | Constructor uses `min(max(0.1, float(timeout_seconds)), 5.0)` (`deepseek_harness_bridge.py:164-179`) and transport uses `effective_timeout = min(self.timeout_seconds, 5.0)` (`:214-217`). An offline bridge constructed with 60 seconds passed exactly `5.0` to mocked `urlopen`. |
| Win32 Job Object 150 MB containment and active child lifecycle | **PARTIAL** | Job Object setup applies `KILL_ON_JOB_CLOSE`, process-memory and job-memory flags, with both limits `150 * 1024 * 1024` (`deepseek_harness_bridge.py:92-114`). `spawn_contained_process`, `terminate_children`, and `close` track/terminate local child processes (`:310-345`), and the Linux child-cleanup test passes. However, `spawn_contained_process()` ignores the boolean result of `assign_process_to_job()` (`:314-317`): a failed Windows assignment neither kills the child nor fails the operation. The Linux test cannot verify actual Win32 API assignment or enforced memory caps. This is not sufficient evidence of guaranteed Windows containment. |
| MatchTrader and MT5 files are non-executable legacy stubs with expected rejection | **FAIL** | Both named files contain no browser DOM/CDP automation or broker dispatch and set `LIVE_DISPATCH_ENABLED = False`. MT5 returns the required `status: REJECTED_DECOMMISSIONED_BRIDGE` (`mt5_live_trader.py:30-50`). MatchTrader is non-executable but returns `status: REJECTED` rather than the explicitly required `REJECTED_DECOMMISSIONED_BRIDGE` (`matchtrader_live_trader.py:33-51`). Offline calls verified this mismatch. |
| `venue_admission.py` contains no proprietary account credentials | **PASS (narrow criterion)** | `EXPECTED_MT5_LOGIN` and `VALID_MT5_LOGINS` are absent; offline `hasattr` checks are false. The module still provides generic `mt5_account_book()` / `native_size()` functions (`venue_admission.py:36-113`), which are not account credentials or order senders but are residual MT5-specific protocol code. |
| Full test suite executes 114 tests with zero errors/failures | **FAIL** | Collection is clean and exactly 114 tests run, but the two critic lifecycle tests fail. This is a hard acceptance failure independent of the fact that the failed tests are outside the five modified safety files. |

## Required remediation before re-audit

1. Route **both** false-return and exception re-anchor paths through one emergency-flatten helper. It must submit the reduce-only market close, evaluate its response, set `flatten_confirmed`, and log `CRITICAL` when the close is unconfirmed. Prefer a post-close position query to distinguish accepted close order from actual flat inventory.
2. In `spawn_contained_process()`, treat `assign_process_to_job(...) == False` as a safety failure on Windows: terminate the newly spawned process and raise/return a fail-closed result. Add a Windows-mocked unit test that verifies this behavior and the three Job limit flags/150 MB values.
3. Change both MatchTrader stub dispatch methods to return `status: REJECTED_DECOMMISSIONED_BRIDGE`, matching the stated acceptance contract and the MT5 stub.
4. Make critic lifecycle tests hermetic/deterministic and repair the semantic-veto path so a stored falling-knife loss is found by the subsequent matching candidate. Re-run the complete 114-test suite until it is 114 passed with no collection errors.

## Audit artifacts

- `.audit/3eaeebc/pytest-terminal-round6-py311.txt`
- `.audit/3eaeebc/pytest-round6-remediation-tests.txt`
- `.audit/3eaeebc/compileall-round6.txt`
- `.audit/3eaeebc/round6-offline-probes.txt`
- `.audit/3eaeebc/test-inventory-round6.txt`

No production trading behavior was changed by this audit report.
