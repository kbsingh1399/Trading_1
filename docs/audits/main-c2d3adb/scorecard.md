# Round 4 institutional quantitative and production-safety audit — `c2d3adb8b817639c61022f58ca50eb1c70f13ee4`

**Audit date:** 2026-11-04 (Asia/Kolkata)

**Target:** exact `origin/main` object `c2d3adb8b817639c61022f58ca50eb1c70f13ee4` (`docs(audit): add Round 4 institutional quant audit prompt for Arena.ai`). The architecture specification identifies its repository target as `_Trading_1`; the audited Git remote/workspace is `kbsingh1399/Trading_2` as required by the active Arena checkout.

**Method:** a fresh `git archive` extraction under `.audit/c2d3adb/target`, isolated dependencies, source review, offline monkeypatched probes, compilation, and the requested test suite. No live broker, Delta REST, browser/CDP endpoint, WARP, collector, or order action was invoked.

## Executive decision

**Readiness score: 43 / 100 — FAIL. Not approved for live enablement.**

The requested six-file suite reports all 42 tests passing, but this does not establish production safety. The Sentinel cannot compile, the stated Delta-only boundary is not fully decommissioned, universal collateral verification is bypassable, fill-time bracket remediation is non-blocking, strict time-bar validation is incomplete, and the DeepSeek bridge is an unintegrated direct-HTTP helper rather than a process-contained harness.

### Quantitative breakdown

| Category | Weight | Score | Reason |
|---|---:|---:|---|
| 1. Architectural Boundary & Venue Safety | 20 | 8 | Delta REST is the active dispatcher, but residual CDP/browser machinery and MT5/MatchTrader-admission symbols remain in explicitly in-scope files. |
| 2. Risk Invariants & Trade Geometry | 20 | 8 | Local discrete size and two-position checks are present; multi-collateral margin is only first-wallet-item checked and fill-time R:R enforcement can fail without rejecting the fill. |
| 3. Trailing Stop & Position Guardian Resilience | 20 | 12 | Tier state machine and duplicate purge are implemented/tested, but an already-penetrated existing stop is not market-closed. |
| 4. Data Pipeline & Rolling Window Invariants | 20 | 6 | Scraper injection filters non-time bars, but evaluator/Sentinel lack required `bar_type` and epoch-modulus gates; non-time discards are not written to the mandated JSONL ledger. Sentinel has a syntax error. |
| 5. Test Coverage & Process Lifecycle Safety | 20 | 9 | Requested 42 tests pass, but socket-blocked verification observes DNS attempts, compilation fails, and dsh has no actual child-process lifecycle integration or memory cap. |
| **Total** | **100** | **43** | **Far below the requested >=95 threshold.** |

## Executed verification

### Requested suite

The exact requested command was run from the isolated target with Delta credential environment variables unset:

```bash
python -m pytest \
  Terminal/test_delta_position_guardian.py \
  Terminal/test_collector_coordination.py \
  Terminal/test_risk_envelope.py \
  Terminal/test_cross_asset_stack.py \
  Terminal/test_dual_models.py \
  Terminal/test_deepseek_harness_bridge.py -v
```

**PASS: 42 passed in 237.43 seconds.** The raw output is retained at `.audit/c2d3adb/pytest-round4.txt`.

An additional socket-blocked run of the same 42 tests used `--disable-socket --allow-unix-socket -p no:cacheprovider`:

**PASS WITH ISOLATION WARNINGS: 42 passed, 6 warnings, in 3.00 seconds.** The warnings say tests attempted `socket.getaddrinfo`; pytest-socket blocked them. Thus external network success was not required, but the suite is not free of attempted network resolution. Evidence: `.audit/c2d3adb/pytest-round4-socket-blocked.txt`.

### Compilation safety check

**FAIL.**

```bash
python -m compileall -q Terminal 1_coinglass_footprint_scraper.py 5_wake_on_footprint_signal.py
```

exited `1`. `5_wake_on_footprint_signal.py:197` contains an unterminated nested f-string in `format_confluence_card`. This means the active Sentinel source cannot be imported or started, despite the selected 42-test suite being green. Evidence: `.audit/c2d3adb/compileall-round4.txt`.

## Itemized institutional findings

Legend: **PASS** = implemented and evidenced offline; **PARTIAL** = substantial implementation with a material defect or unproved operational path; **FAIL** = contradicted by source or an offline probe.

| Criterion | Verdict | Evidence and finding |
|---|---|---|
| AGENTS §3 declares Delta India as sole execution venue | **PASS** | `AGENTS.md:17-30` specifies Delta authenticated REST as sole execution target, the 20% margin floor, two positions, $25 cap, fill-anchored R:R, tiers, and time-bar rule. |
| Legacy venue/browser decommissioning in `Terminal/collector_lock.py` | **FAIL** | `COLLECTOR_LOCK_MAP` still contains four `cdp_*` locks and `BrowserLock` at `collector_lock.py:198-224`; comments and scheduling retain browser execution semantics at `:258-265`. This contradicts strict removal from the named map/file. |
| Legacy venue/prop bridge removal in `Terminal/realtime_signal_evaluator.py` | **FAIL** | It imports `matchtrader_symbol` at `:81`, preserves MatchTrader fields at `:993`, `:1115-1117`, and retains `MetaTrader5` sizing/terminal startup at `:1452-1470`. |
| Legacy browser DOM removal in Delta module | **FAIL** | `delta_exchange_live_trader.py:220-278` still defines `_get_cdp_positions`, opens `http://127.0.0.1:9222/json`, attaches a WebSocket debugger, and executes DOM JavaScript. `get_open_positions()` currently returns REST-or-empty (`:292-297`), but retaining the browser fallback code violates strict excision. |
| Sentinel direct Delta route | **PASS** | The Sentinel documents MatchTrader removal (`5_wake_on_footprint_signal.py:71,151`) and its dispatch path resolves a Delta product then calls `delta_client.execute_delta_signal` (`:402-422`). |
| Delta India is the active authenticated REST target | **PASS** | `BASE_URL` is `https://api.india.delta.exchange` (`delta_exchange_live_trader.py:38`); signed REST requests are centralized in `:168-213`, and the live result labels `DeltaExchangeIndia` (`:1040-1047`). |
| Discrete grid risk never exceeds $25 in the normal entry sizing path | **PASS** | `_execute_delta_signal_guarded()` rejects single-contract risk above `MAX_RISK_USD` (`:980-990`) and floors the integer number of contracts (`:992-996`). The SSOT solver independently fails non-representable volumes (`risk_envelope.py:151-202`); the selected risk tests pass. |
| Maximum two concurrent positions | **PASS** | `MAX_DELTA_CONCURRENT_POSITIONS = MAX_OPEN_POSITIONS` at `delta_exchange_live_trader.py:39`; live execution counts active plus in-flight positions and rejects at the limit (`:944-951`). |
| Universal 20% free-margin floor across USD, USDT, and INR collateral | **FAIL** | Both `get_available_margin_usd()` (`:280-290`) and entry admission (`:920-942`) select the **first** qualifying wallet entry instead of aggregating/validating every collateral asset or converting INR. An offline patched probe supplied USD at 100% free margin and USDT/INR each at 0%; the code selected USD and returned `EXECUTED`. Evidence: `.audit/c2d3adb/margin-universal-counterexample.txt`. |
| R:R >=2 against actual fill with TP ceiling quantization | **PARTIAL** | `reanchor_brackets_to_fill()` uses `ceil((min_rr * actual_risk) / tick_size)` at `delta_exchange_live_trader.py:473-501`, registers fill anchors, and live dispatch invokes it at `:1026-1038`. However the return result is ignored: a forced `NOT_FOUND` re-anchor failure still returned `{"ok": true, "status": "EXECUTED"}`. Evidence: `.audit/c2d3adb/fill-reanchor-failure-probe.txt`. A live fill is therefore not rejected/closed when post-fill compliance cannot be established. |
| Directional SL tick quantization | **PARTIAL** | Risk-cap contraction uses `floor` at `delta_exchange_live_trader.py:457-463`, but general bracket/stop writes use `quantize_price()` nearest-rounding (`:313-324`, `:374-375`, `:399-403`), not an explicit direction-aware floor/ceiling. Initial sizing is calculated from unquantized entry/SL. |
| Continuous audit calibrates existing R:R below 1.999 | **PARTIAL** | Existing TP orders below 1.999 are recalibrated using ceiling ticks (`delta_exchange_live_trader.py:720-739`). Positions with no TP are not given a recovery TP, so the invariant is not universal. |
| Tier 1 / 2 / 3 monotonic trailing ratchet | **PASS** | The state machine applies breakeven plus a tick at +1R (`:772-775`), locks +1R and widens TP to +3R at +2R (`:759-771`), and locks +2R/widens to +4R at +3R (`:746-758`); monotonicity is checked at `:777-815`. Guardian tier tests pass. |
| Deterministic duplicate bracket-order purge | **PASS** | It retains the tightest SL (`:613-631`) and the highest BUY / lowest SELL TP (`:633-652`) while cancelling the rest. |
| Emergency close when mark has penetrated an existing stop | **FAIL** | Emergency close occurs only after a rejected *new ratchet edit* contains `immediate_execution_stop_order` (`delta_exchange_live_trader.py:789-800`), or for a naked-position recovery breach (`:671-676`). An offline probe with a BUY position at mark 94 and an existing SL at 95 produced no close call and no report. Evidence: `.audit/c2d3adb/guardian-stop-penetration-probe.txt`. |
| Scraper rejects non-time and non-M15 source bars | **PASS** | It locks the terminal to 15m at `1_coinglass_footprint_scraper.py:215-217`; injected JavaScript explicitly executes `if (set.bt !== 'time') continue` and checks `timeMs % 900000` (`:248-271`). |
| All execution routes enforce `bar_type == 'time'` and `t_ms % 900_000 == 0` | **FAIL** | The evaluator only tests closed time **or producer `bar_closed`** at `realtime_signal_evaluator.py:839-845`, with neither bar type nor epoch modulus validation. Sentinel `run_once` and daemon similarly only calculate close time (`5_wake_on_footprint_signal.py:581-592`, `:661-671`). Thus a closed, misaligned/range-volume input can proceed after the scraper boundary. |
| Volume/range signals ledgered as `DISCARDED_STALE` in `Database/Signals/evaluated_signals_ledger.jsonl` | **FAIL** | The scraper silently `continue`s on non-time/misaligned signals (`1_coinglass_footprint_scraper.py:403-409`). The actual ledger path is `evaluated_signals_ledger.json`, not the required `.jsonl` (`asset_signal_stack.py:49,67-128`), and its stale result is `EXPIRED_STALE` (`:181-212`). No required `DISCARDED_STALE` JSONL record is produced. |
| dsh Win32 job object and taskkill fallback helpers | **PARTIAL** | `deepseek_harness_bridge.py:42-125` defines `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000`, creates/configures a Job Object, and defines assignment. `terminate_process_tree()` uses `taskkill.exe /PID <pid> /T /F` at `:128-145`. But no production module instantiates a child dsh process or calls `assign_process_to_job`/`terminate_process_tree`; repository search finds no non-test call site. This is helper code, not verified containment. |
| dsh strict <=5.0s SLA and fail-closed behavior | **FAIL** | `DeepSeekHarnessBridge.__init__` stores arbitrary caller input as `self.timeout_seconds` (`deepseek_harness_bridge.py:154-165`). An offline patched probe built it with 60 seconds and confirmed `urlopen(..., timeout=60.0)`. Also, when the API key is absent `evaluate_signal_confluence()` returns `approved: True` (`:232-240`), contrary to the design spec's missing-key fail-closed statement. Evidence: `.audit/c2d3adb/deepseek-safety-probes.txt`. |
| dsh 150 MB memory budget / OfficeKit and Electron removal | **FAIL** | The bridge and design state the budget, but no memory measurement, resource limit, Job Object memory limit, or rejection code exists. No Office/Electron directory was found in the audit archive, but absence alone is not runtime budget enforcement. |
| `.dsh_home` isolation outside `Database/` | **PARTIAL** | `DSH_HOME_DIR = ROOT_DIR / '.dsh_home'` (`deepseek_harness_bridge.py:36-40`) is outside `Database`, and the directory test passed. `.gitignore` covers `deepseek-harness/` and `Database/DSH_Home/` (`:50-53`) but not `.dsh_home/`, despite the design specification requiring it. |
| DeepSeek bridge integrated into live evaluator/Sentinel | **FAIL** | Search found `DeepSeekHarnessBridge` only in its own module and its tests; neither `Terminal/realtime_signal_evaluator.py` nor `5_wake_on_footprint_signal.py` invokes it. This avoids an unreviewed live network dependency, but does not meet the claimed integration. |

## Production blockers, in remediation order

1. Repair the syntax error in `5_wake_on_footprint_signal.py:197` and require repository compilation before accepting any test-only green result.
2. Fully remove or isolate legacy execution/CDP/browser/MT5 artifacts from the explicitly named production files; replace evaluator metadata and sizing fallbacks with Delta-only paths.
3. Replace first-wallet-item selection with a fail-closed, normalized multi-collateral margin calculation. Require every eligible collateral balance/floor policy to be satisfied before execution.
4. Make post-fill re-anchoring transactional: do not return `EXECUTED` until actual fill, SL, and directionally quantized >=2R TP are confirmed; close/reject safely if bracket persistence cannot be verified.
5. Add a first-class existing-stop penetration check before any tier transition and close immediately, not only on an edit-response substring.
6. Enforce `bar_type == 'time'` and exact 900,000-ms epoch alignment at scraper, evaluator, and Sentinel boundaries. Write every suppressed range/volume/misaligned item as `DISCARDED_STALE` to the required JSONL ledger.
7. Either integrate an actual controlled dsh child process or revise the claim. Bound timeout with `min(requested, 5.0)`, fail closed for unavailable reasoning where required, apply a real 150-MB limit, and call Job Object/process-tree cleanup on all lifecycle paths.

## Audit artifacts

All raw output and probes are audit-local rather than production artifacts:

- `.audit/c2d3adb/pytest-round4.txt`
- `.audit/c2d3adb/pytest-round4-socket-blocked.txt`
- `.audit/c2d3adb/compileall-round4.txt`
- `.audit/c2d3adb/margin-universal-counterexample.txt`
- `.audit/c2d3adb/fill-reanchor-failure-probe.txt`
- `.audit/c2d3adb/guardian-stop-penetration-probe.txt`
- `.audit/c2d3adb/deepseek-safety-probes.txt`

No production trading behavior was modified by this audit report.
