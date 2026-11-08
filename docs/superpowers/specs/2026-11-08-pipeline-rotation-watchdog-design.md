# Design Document: Opportunistic IP Rotation Ceiling & Heartbeat Watchdog
========================================================================================
**Project**: `_Trading_1` | **Date**: 2026-11-08 | **Author**: Antigravity  
**Status**: Pending Review | **Selected Approach**: Approach 2 (User Approved)
========================================================================================

## 1. Intent & Why
To support 24/7/forever unattended multi-pillar order flow harvesting without incurring Cloudflare/CoinGlass IP bans or silent pipeline stalls:
1. Establish an opportunistic IP rotation policy that rotates egress IP when Cloudflare challenges/rate-limits occur, or proactively when an IP reaches an age ceiling ($\ge 3$ cycles / $\approx 15$ minutes) during the inter-cycle sleep interval.
2. Implement an in-process telemetry heartbeat auditor emitting [`Database/pipeline_health.json`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/pipeline_health.json).
3. Establish a recurring watchdog schedule (via the agent cron system) that independently audits parquet freshness, row counts, and lock health.

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
- **Proactive + Reactive Egress Rotation**: Maintain existing reactive triggers (`HTTP_429`, `CHALLENGE`, `RATE_LIMIT_BANNER`) while adding a controlled inter-cycle rotation ceiling (`CYCLE_AGE_CEILING`) after 3 cycles or 15 minutes of continuous scraping on one IP.
- **Inter-Cycle Execution Safety**: Rotations occur only when all scraper tabs are closed and during the inter-cycle delay (300s), fully satisfying the 120s WARP cooldown.
- **Atomic Telemetry Heartbeat**: Output [`Database/pipeline_health.json`](file:///C:/Users/SIGMA/Documents/_Trading_1/Database/pipeline_health.json) detailing each pillar's last harvest timestamp, row count, status (`HEALTHY`, `STALE`, `DEGRADED`), and active egress IP.
- **Recurring Watchdog Monitoring**: Implement a parallel health check script and schedule an agent cron to continuously inspect data dump integrity without human intervention.

### Non-Goals
- Mid-scrape blind rotation (rotations will never interrupt active tab evaluations).
- Altering the Delta split-tunnel bypass rules (Delta Exchange hosts remain permanently excluded).

### Anti-Goals
- Introducing race conditions or lock deadlocks between the health auditor and active parquet writers.
- Masking partial scraper failures (degraded harvests must be flagged, not silently ignored).

---

## 3. Approaches Considered & Trade-off Analysis

| Feature / Criteria | Approach 1: Dedicated Inter-Cycle Rotation + External Daemon | Approach 2: In-Process Heartbeat + Opportunistic Rotation (Selected) | Approach 3: OS Task Scheduler Watchdog |
| :--- | :--- | :--- | :--- |
| **Rotation Trigger** | Rotates every single cycle (every 5 min) | Rotates reactively on 429/Challenge OR after 3 cycles (~15 min) | Rotates every single cycle |
| **Rotation Overhead** | ~1 registration reset every 5 min | Rotates 1/3 as often; minimizes network churn while preventing IP burnout | ~1 registration reset every 5 min |
| **Watchdog Model** | Separate Python daemon process + Cron | In-process heartbeat emission + Antigravity Agent Cron schedule | Windows Task Scheduler + Powershell script |
| **Failure Isolation** | High | High (Heartbeat file decoupled from auditor) | Medium |
| **Complexity** | 2 separate running daemons | Single unified runner + lightweight agent cron | OS-level configuration dependent |

**Decision**: Approach 2 was selected by the user. It strikes an optimal institutional balance: avoids unnecessary WARP network churn while ensuring no IP stays active long enough for CoinGlass to flag, and couples in-process heartbeat generation with an Antigravity agent cron schedule.

---

## 4. Architectural Specification & Code Changes

### Component 1: `coinglass_common.py`
Add `"CYCLE_AGE_CEILING"` as an authorized rotation reason in [`rotate_warp_ip`](file:///C:/Users/SIGMA/Documents/_Trading_1/coinglass_common.py):
```python
valid_reasons = {"HTTP_429", "CHALLENGE", "RATE_LIMIT_BANNER", "CYCLE_AGE_CEILING"}
```
Ensure cooldown ($\ge 120$s) and Delta split-tunnel Exclude mode checks remain strictly enforced.

### Component 2: `API/run_parallel_standalone.py`
1. **IP Age & Cycle Tracking**:
   - Track `cycles_on_current_ip` and `time_on_current_ip`.
   - At the completion of each harvest cycle (during the inter-cycle delay), check:
     ```python
     if cycles_on_current_ip >= max_cycles_per_ip or (time.time() - ip_start_time) >= max_ip_age_sec:
         logger.info("IP age ceiling reached (%d cycles); rotating egress IP...", cycles_on_current_ip)
         new_ip = rotate_warp_ip(reason="CYCLE_AGE_CEILING")
     ```
2. **Heartbeat Telemetry Publication**:
   - Write `Database/pipeline_health.json` after each cycle, reporting:
     - `timestamp_ist`: Current IST time.
     - `current_ip`: Active egress IP.
     - `cycle_number`: Current cycle index.
     - `pillars`: Summary for Footprint, Whales, Liquidation Levels, and Live Legend Liquidations (files updated, rows added, status).

### Component 3: `API/pipeline_health_check.py` (New Script)
A standalone CLI tool and agent check that:
- Reads `Database/pipeline_health.json`.
- Audits Parquet modification times across `Database/Live_Liquidation_Dump`, `Database/Footprint_Dumps`, `Database/Whale_Dumps`, and `Database/Liquidation_Level_Dumps`.
- Raises an alert if any parquet is older than 15 minutes, has 0 rows, or if stale `.lock` files exist.

### Component 4: Agent Cron Schedule via `schedule`
Register a background cron (e.g. `*/15 * * * *`) that triggers `API/pipeline_health_check.py` and alerts the user directly if any pillar becomes stale.

---

## 5. Verification Plan

1. **Automated Unit & Audit Tests**:
   - Run `python -m pytest tests/api_pipeline_audits tests/test_confluence_expansion.py -q` (all 29 tests must remain 100% green).
   - Run `python C:\Users\SIGMA\Downloads\api_audit_harness_v5\run_all.py` (all 5 suites must pass).
2. **Multi-Cycle Rotation & Heartbeat Verification**:
   - Execute a multi-cycle test run with `--max-cycles-per-ip 2` across `BTC ETH`.
   - Verify that on cycle 2, the pipeline safely rotates egress IP during the rest window.
   - Verify `Database/pipeline_health.json` is generated and contains accurate row counts and timestamps.
3. **Watchdog Verification**:
   - Run `python API/pipeline_health_check.py` and confirm clean exit code 0.
   - Set up the agent cron timer.
