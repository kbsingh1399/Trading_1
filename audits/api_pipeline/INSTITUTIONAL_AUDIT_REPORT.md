# Institutional 360° Hostile Audit — API Telemetry Pipeline

**Repository:** `kbsingh1399/Trading_1` · **Baseline:** `cb3e5cc00725eadc0d4951b1c60927b89a2fe3cd`  
**Branch:** `arena/c25f39f0-trading-1` · **Audit date:** 2026-11-08  
**Verdict:** **NOT certified for unattended production yet.** The patch removes the demonstrated erase-on-empty, false-completeness, cross-worker tab-reaping, lock-inode, and scheduled-rotation failure paths. Live Chrome/Windows/WARP rehearsal and dashboard parity are still mandatory.

## 1. Scorecard

Scores are hostile-audit judgments, not benchmark percentages. “Patched” credits only code inspected and exercised by the local regression suite; it does not credit unrun live integrations.

| Category | Baseline /100 | Patched /100 | Principal reason patched score is not 100 |
|---|---:|---:|---|
| Architecture | 46 | 78 | Shared kernel/adapters and fail-closed universe added; dual-file whale publication is not transactional across both destinations. |
| Concurrency | 38 | 80 | Persistent lock inodes, re-entrant writer locks, owned-tab `finally`, post-gather rotation; Windows `msvcrt` and SIGKILL orphan recovery not executed here. |
| DOM resilience | 41 | 76 | Unified whale JS, exact `$0`, suffix-aware liquidation parsing, horizon/15m hydration gates; selectors and Fiber shape remain unverified against live CoinGlass. |
| Data authenticity | 32 | 74 | Empty/corrupt history refused, PyArrow read-back, POC/VAH/VAL added, CVD rebased; CVD is local-history anchored and 100% live dashboard parity is unproven. |

## 2. Empirical evidence

- `python -m py_compile coinglass_common.py API/*.py`: **PASS**.
- All three embedded extraction scripts compiled by Node `vm.Script`: **PASS**.
- New hostile suite: **25 passed** on patched tree (`patched_regression.txt`).
- Same suite against exported baseline: **23 failed, 2 passed** (`baseline_regression.txt`). This is deliberate baseline-failure proof, not a claim that every baseline test failure is an independent production incident.
- Existing selected suites after updating the inflight-lock contract to fail closed: **22 passed, 1 failed** (`existing_tests.txt`). The remaining failure is `test_shannon_entropy_contradiction_gate` in the trade evaluator, outside this API patch; it is not silently fixed or hidden.
- Read-only baseline disk inventory: footprint **64**, whale display **13**, liquidation **195** Parquet files; all readable and nonempty. DOGE was absent from all three outputs. Footprint history had fragmented labels and no POC/VAH/VAL columns. Disk readability is **not** dashboard parity.
- jsdom contracts exercised: mid-cap/$0 retention, empty rejection, DOM suffix parsing and partial flag, liquidation decimal/K/M/B parsing and horizon rejection, footprint 15m forcing/POC/value-area/CVD/stale-loading rejection.
- Mocked WARP contracts exercised: scheduled reasons never call CLI; reactive sequence `disconnect → registration delete → registration new → connect`; order-inflight block; Include-mode fail-closed; restore-on-failure; success-only cooldown.
- Mocked runner contracts exercised: rotation occurs only after all three pillar tasks close their tabs, in a worker thread; `--rotate-chunks` alone never rotates.

## 3. Findings and exact replacements

The complete, applyable replacement text is in **`drop_in_replacements.patch`** (unified diff with baseline hunk line numbers). The working tree already contains these replacements. Current line anchors below are to the patched files; the patch hunks carry the original baseline line references. No placeholder pseudo-code is used.

### CRITICAL

| ID | Finding | Baseline anchor | Drop-in replacement anchor |
|---|---|---|---|
| C1 | Stale-lock reaping/unlink could let two writers lock different inodes. | `coinglass_common.py` FileMutex/reapers | `coinglass_common.py:37-150,781-790` — persistent inode, no unlink; compatibility reapers no-op. |
| C2 | Liquidation eval timeout produced `{}` and 15 empty placeholders; size-only checks reported success. | liq API `317-327,384-447,488`; runner `523` | liq API `101-345,348-421`; runner `199-230` — horizon proof, nonempty structures, schema + PyArrow read-back. |
| C3 | Whale repository/batch/runner could replace valid history with zero rows or overwrite unreadable history. | whales `783-797,1213-1227`; runner equity path | whales `494-562,691-721`; runner `159-196` — shared extractor, empty/corrupt refusal, locked verified write. |
| C4 | Scheduled/chunk/post-cycle WARP rotation interrupted host networking without block evidence; cooldown/order guards were racy. | runner `--rotate-chunks` paths; liq `578-583`; common rotation | common `540-660`; runner `233-332` — reactive-only gate, order guard, cooldown under execution lock, post-gather `to_thread` rotation. |
| C5 | Delta split-tunnel hosts could be added to an Include list or lost across registration; CLI failures were ignored. | common WARP exclusion/rotation block | common `532-570,625-660` — Exclude-mode gate, disconnected configuration, verified host list before/after registration, checked return codes. |

### MAJOR

| ID | Finding | Baseline anchor | Drop-in replacement anchor |
|---|---|---|---|
| M1 | Tagged-tab adoption/global reaping could close another worker's tab; substring matching could touch user URLs. | footprint `501-517,978-984`; runner `119-135`; whales reaper | common `266-291`; footprint `548-603`; whales `421-461`; runner `113-116` — owner token, exact query match, explicit owned-ID closure, global reaper disabled. |
| M2 | `includes('$0')` matched `$0.5M`; orders were cached before threshold change; three divergent whale JS copies; batch ignored `display_only`. | runner `290`; whales `441,983`, batch | whales `81-264,494-537,691-721`; runner `159-196` — one shared threshold-first extractor, exact-zero regex, hydration wait, display flag honored. |
| M3 | Footprint did not prove 15m/mode hydration; schema lacked POC/VAH/VAL; merge reset CVD and dedup keys collided. | footprint JS/schema/merge | footprint `87-201,257-520,738-792` — generation waits, 19-column schema with legacy null migration, volume-bin POC/70% VA, full-row dedup, cumulative-delta rebase. |
| M4 | Active-universe fallback expanded to hardcoded majors/registry; fixed `.tmp` add/remove races; static API whitelist blocked hot additions. | manager `91-148`; API guards | manager `43-120`; footprint `818-821,851-916`; whales `267-283,638-688` — fail-closed validation, locked atomic JSON, locked read-modify-write, request-time active authorization. |
| M5 | Liquidation parsers truncated decimals/K/M/B, accepted out-of-range rows, and did not prove horizon selection. | liq JS | liq `101-345` — signed decimal suffix parser, ±4% key filter, ±10%/top-3 magnets, horizon selected-state proof. |
| M6 | Daemon loops harvested only missing assets and stopped refreshing existing snapshots; once-mode failures could exit successfully. | all three `run_cli_*` | footprint `1025-1088`; whales `724-772`; liq `444-518`; runner `319-325` — daemon refreshes all active, once-mode reports incomplete sweeps. |
| M7 | Singleton acquisition failure only warned; async rotation blocked the event loop and could run while sibling tabs were active. | runner `642-646`, chunk rotation | runner `233-352` — fatal singleton failure, signals collected per chunk, one coordinated rotation after gather. |
| M8 | Downstream authenticity fallbacks can still synthesize timestamps/delta/POC from OHLC when explicit fields are absent. | common `2247-2324` (baseline) | **Report-only residual** in `UniversalFootprintEngine`; not used by the new API POC path. Quarantine estimated rows before execution; see checklist. |

### MINOR

| ID | Finding | Replacement |
|---|---|---|
| m1 | Fixed CDP request IDs and per-recv timeouts could misassociate replies or extend total deadline. | common `253-291` — monotonic IDs, whole-request `asyncio.timeout`. |
| m2 | Blocking CDP/urllib/disk calls inside coroutines. | runner `92-110,124-230`; adapters use `asyncio.to_thread` for tab/CLI/disk boundaries. |
| m3 | CORS wildcard combined with credentials. | footprint/whales middleware: `allow_credentials=False`. |
| m4 | Symbol interpolation into JS/paths. | manager `43-54`; adapters validate `[A-Z0-9][A-Z0-9_.-]{0,31}` and JSON-encode JS arguments. |
| m5 | `fsync` failure was swallowed and temp cleanup could mask the original error. | common `153-183,190-205` — re-raise publication failure, safe cleanup. |

## 4. Residual risks — explicit, not waived

1. **No live Chrome/CDP, Windows `msvcrt`, or `warp-cli` in this sandbox.** Selectors, Fiber hook shape, split-tunnel dump/list formatting, and WinError 32 behavior must be rehearsed on the production host.
2. **SIGKILL tab orphans:** global reaping is intentionally disabled because a tag is not a cross-process lease. Use a supervised browser profile/session or an explicit ownership registry with crash recovery; do not reintroduce substring reaping.
3. **Whale dual destination:** display and quant repositories are individually atomic/verified, not a single transaction across both files. A crash between writes can leave different snapshot generations.
4. **DOM fallback:** a virtualized book without a provable scroll boundary is marked partial and refused for persistence. This favors authenticity over silently replacing a full snapshot with visible rows.
5. **CVD anchor:** merged CVD is recomputed from retained local history. It is continuous within that history, not guaranteed equal to CoinGlass's absolute/session CVD anchor.
6. **Liquidation capability conflict:** the registry marks all four active metals `liq_snapshots:false`/`liq_levels:false`, while the active universe includes them. The patched code fails closed rather than fabricating placeholders; resolve capability truth before claiming 14/14 liquidation coverage.
7. **Host networking:** reactive WARP reset still briefly changes host egress. Delta hosts are excluded when the verified Exclude-mode configuration succeeds; “zero host interruption” is only claimed for normal no-block operation, not during an actual reactive reset.
8. **Existing evaluator test:** Shannon-entropy contradiction gate remains failing outside the API patch. Do not treat this audit as a whole-repository production certification.

## 5. Production-readiness checklist — continuous 14-asset operation

### Gate A — configuration and capabilities
- [ ] Back up `Database/` and `API/active_assets.json`; verify the 14 symbols and category counts.
- [ ] Resolve metals liquidation capability flags against live CoinGlass availability; document unsupported structures instead of writing placeholders.
- [ ] On Windows, verify `warp-cli tunnel dump` reports **Exclude** mode and `tunnel host list` contains all four Delta hosts before enabling rotation.
- [ ] Confirm Delta order execution holds `order_execution_inflight.lock` for the entire order lifetime.

### Gate B — hostile rehearsal on the production host
- [ ] Run the 25-test API suite and the three embedded-JS syntax checks on the production Python/Node versions.
- [ ] Exercise two concurrent writers and a held reader on Windows; confirm bounded `TimeoutError`/retry, no lock-file deletion, and no WinError 32 history loss.
- [ ] Open a user tab whose URL merely contains `bot=footprint_api` as text; run all pillars; confirm it is never adopted or closed.
- [ ] Kill a worker mid-evaluation; confirm its `finally` path is not relied upon and recover only explicitly owned tabs via supervised session policy.
- [ ] Inject zero-row, malformed, stale-horizon, and stale-15m payloads; confirm old Parquet bytes remain unchanged.
- [ ] Trigger a real 429/Turnstile in a controlled window; confirm no scheduled rotation occurs, rotation waits for sibling tabs to close, registration sequence completes, Delta exclusions survive, and connectivity restores on failure.

### Gate C — 14-asset parity burn-in
- [ ] Run at least 24 hours covering crypto weekend/weekday and metals session boundaries.
- [ ] For every asset/cycle, compare dashboard rows/values against disk: whale 8 display columns and `$0` threshold; footprint 15m/volume/range with POC/VAH/VAL; liquidation 3D/7D/30D × 5 structures where capability is true.
- [ ] Require PyArrow rows > 0 and exact expected column order for every published file; alert on missing DOGE or any active asset.
- [ ] Track CVD anchor offset separately; do not execute trades from estimated delta/POC fallback rows.
- [ ] Alert on partial DOM fallback, horizon mismatch, stale Fiber generation, schema drift, rotation cooldown rejection, and incomplete cycles.
- [ ] Define RPO/RTO and backup restore drill. “Zero data loss” is a design target for transient scrape failures, not a guarantee against disk loss, process kill between dual writes, or upstream data gaps.

**Release rule:** promote only after Gates A–C pass with recorded evidence. Until then, run in shadow/paper mode with historical files preserved.
