# Pipeline Opportunistic IP Rotation Ceiling & Heartbeat Watchdog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement proactive inter-cycle WARP IP rotation (ceiling of 3 cycles / 15 min), atomic pipeline health telemetry (`Database/pipeline_health.json`), a standalone health auditor CLI (`API/pipeline_health_check.py`), and a recurring agent cron watchdog to support 24/7 unattended multi-pillar order flow harvesting.

**Architecture:** Extend `coinglass_common.py`'s `valid_reasons` to authorize `"CYCLE_AGE_CEILING"`. In `API/run_parallel_standalone.py`, track cycle count and IP elapsed time to opportunistically rotate WARP egress IP during the inter-cycle rest window (when all tabs are closed), and atomically publish `Database/pipeline_health.json` after every cycle. Build `API/pipeline_health_check.py` to audit file freshness, non-zero row counts, and lock health, and schedule an agent cron to continuously verify pipeline health.

**Tech Stack:** Python 3.14, Cloudflare WARP CLI (`warp-cli`), PyArrow/Parquet, asyncio, pytest.

**Spec:** `docs/superpowers/specs/2026-11-08-pipeline-rotation-watchdog-design.md`

## Global Constraints
- Strict preservation of Delta Exchange split-tunnel exclusions (`cdn.india.delta.exchange`, `api.india.delta.exchange`, `india.delta.exchange`, `delta.exchange`) in Exclude mode.
- Minimum 120-second rotation cooldown (`MIN_ROTATION_INTERVAL_SECS = 120.0`) enforced by `rotate_warp_ip`.
- Re-entrant `writer_lock` and thread-local depth tracking (`threading.local()`) on Windows 11.
- Zero breaking changes to existing tests: all 29 pytest tests and 5 v5 hostile audit suites must remain 100% green.
- Ephemeral Chrome CDP tabs: no leaking or lingering browser tabs across cycles.

## Review Focus
1. `rotate_warp_ip` called with `"CYCLE_AGE_CEILING"` within 120s cooldown: must defer rotation cleanly and return current IP without raising unhandled exception.
2. Single-cycle execution (`--sample 2` without `--continuous`): must write `Database/pipeline_health.json` and exit 0 without triggering rotation or failing on cycle count.
3. Partial pillar failure in a cycle: `pipeline_health.json` must record `DEGRADED` status and accurately report which pillar failed rather than throwing an unhandled exception.
4. Parquet auditor with missing directory or zero-byte file: `pipeline_health_check.py` should catch file errors, report `UNHEALTHY`, and return exit code 1.
5. Inflight order lock active during inter-cycle boundary: `rotate_warp_ip` should block rotation safely (RuntimeError caught and logged) without crashing the pipeline runner.

---

### Task 1: Authorize `"CYCLE_AGE_CEILING"` in `coinglass_common.py`

**Files:**
- Modify: `coinglass_common.py:635`
- Test: `tests/api_pipeline_audits/test_warp_cdp.py`

**Interfaces:**
- Consumes: `rotate_warp_ip(context: str, reason: str, force: bool = False) -> str`
- Produces: `valid_reasons = {"HTTP_429", "CHALLENGE", "RATE_LIMIT_BANNER", "CYCLE_AGE_CEILING"}`

- [ ] **Step 1: Write the failing unit test in `tests/api_pipeline_audits/test_warp_cdp.py`**

```python
def test_cycle_age_ceiling_is_authorized_rotation_reason(tmp_path, monkeypatch):
    calls = warp_fixture(tmp_path, monkeypatch)
    assert cc.rotate_warp_ip(reason="CYCLE_AGE_CEILING", force=True) == "new"
    core = [c for c in calls if c[0] in {"disconnect", "registration", "connect"}]
    assert core == [("disconnect",), ("registration", "delete"), ("registration", "new"), ("connect",)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/api_pipeline_audits/test_warp_cdp.py::test_cycle_age_ceiling_is_authorized_rotation_reason -v`  
Expected: FAIL (because `CYCLE_AGE_CEILING` is currently skipped as a non-reactive reason and returns "old")

- [ ] **Step 3: Update `valid_reasons` in `coinglass_common.py`**

Add `"CYCLE_AGE_CEILING"` to `valid_reasons`:
```python
valid_reasons = {"HTTP_429", "CHALLENGE", "RATE_LIMIT_BANNER", "CYCLE_AGE_CEILING"}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/api_pipeline_audits/test_warp_cdp.py -v`  
Expected: PASS (all tests pass, including `test_scheduled_rotation_never_calls_cli` and `test_cycle_age_ceiling_is_authorized_rotation_reason`)

- [ ] **Step 5: Commit**

```bash
git add coinglass_common.py tests/api_pipeline_audits/test_warp_cdp.py
git commit -m "feat(warp): authorize CYCLE_AGE_CEILING reason for inter-cycle IP rotation"
```

---

### Task 2: Implement Cycle Age Tracking & Health Heartbeat in `API/run_parallel_standalone.py`

**Files:**
- Modify: `API/run_parallel_standalone.py`
- Test: `tests/api_pipeline_audits/test_pipeline_health.py`

**Interfaces:**
- Consumes: `rotate_warp_ip(context, reason)` from `coinglass_common.py`
- Produces:
  - CLI flags: `--max-cycles-per-ip` (int, default: 3), `--max-ip-age-min` (float, default: 15.0)
  - Function: `emit_pipeline_health(cycle_num, current_ip, cycles_on_ip, ip_age_sec, pillar_stats, symbols, is_degraded) -> Path`
  - Published file: `Database/pipeline_health.json`

- [ ] **Step 1: Write the failing test for heartbeat emission and rotation triggering**

Create `tests/api_pipeline_audits/test_pipeline_health.py`:
```python
import json, time
from pathlib import Path
import pytest
import run_parallel_standalone as runner

def test_emit_pipeline_health(tmp_path, monkeypatch):
    health_file = tmp_path / "pipeline_health.json"
    monkeypatch.setattr(runner, "HEALTH_FILE", health_file)
    stats = {
        "footprint": {"dumped_files": 42, "expected_files": 42, "status": "HEALTHY"},
        "whales": {"dumped_assets": 14, "expected_assets": 14, "status": "HEALTHY"},
        "liquidation_levels": {"dumped_files": 210, "expected_files": 210, "status": "HEALTHY"},
        "live_legend": {"dumped_assets": 14, "expected_assets": 14, "status": "HEALTHY"}
    }
    path = runner.emit_pipeline_health(
        cycle_num=1,
        current_ip="1.2.3.4",
        cycles_on_ip=1,
        ip_age_sec=120.5,
        pillar_stats=stats,
        symbols=["BTC", "ETH"],
        is_degraded=False
    )
    assert path == health_file
    data = json.loads(health_file.read_text(encoding="utf-8"))
    assert data["cycle_number"] == 1
    assert data["current_ip"] == "1.2.3.4"
    assert data["status"] == "HEALTHY"
    assert data["pillars"]["footprint"]["dumped_files"] == 42
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/api_pipeline_audits/test_pipeline_health.py -v`  
Expected: FAIL with `AttributeError: module 'run_parallel_standalone' has no attribute 'emit_pipeline_health'`

- [ ] **Step 3: Implement `emit_pipeline_health` and inter-cycle rotation in `API/run_parallel_standalone.py`**

1. Define `HEALTH_FILE = DB_DIR / "pipeline_health.json"` (default).
2. Implement `emit_pipeline_health(...)` using atomic write (`.tmp` + `os.replace`).
3. Add CLI arguments:
   - `--max-cycles-per-ip` (default: 3)
   - `--max-ip-age-min` (default: 15.0)
4. In `run_parallel_cycle`, maintain `cycles_on_current_ip = 0` and `ip_start_time = time.time()`.
5. At the end of each cycle, compute pillar stats, call `emit_pipeline_health`, and if `continuous`:
   - Increment `cycles_on_current_ip += 1`.
   - Check if `cycles_on_current_ip >= max_cycles_per_ip` or `time.time() - ip_start_time >= max_ip_age_sec`.
   - If reached, invoke `rotate_warp_ip(context=f"Cycle{cycle_num}_Ceiling", reason="CYCLE_AGE_CEILING")`, and reset `cycles_on_current_ip = 0` and `ip_start_time = time.time()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/api_pipeline_audits/test_pipeline_health.py -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add API/run_parallel_standalone.py tests/api_pipeline_audits/test_pipeline_health.py
git commit -m "feat(pipeline): add opportunistic inter-cycle IP rotation ceiling and health heartbeat"
```

---

### Task 3: Implement Standalone Health Auditor `API/pipeline_health_check.py`

**Files:**
- Create: `API/pipeline_health_check.py`
- Test: `tests/api_pipeline_audits/test_health_auditor.py`

**Interfaces:**
- Consumes:
  - `Database/pipeline_health.json`
  - Parquet files in `Database/Live_Liquidations/`, `Database/Footprint_Dumps/`, `Database/Whale_Dumps/`, `Database/Liquidation_Level_Dumps/`
  - Active assets list from `API/active_assets.json`
- Produces:
  - CLI: `python API/pipeline_health_check.py [--max-age-min 15] [--json]`
  - Exit code: `0` (HEALTHY), `1` (DEGRADED / STALE / ERROR)

- [ ] **Step 1: Write failing unit test for `pipeline_health_check`**

Create `tests/api_pipeline_audits/test_health_auditor.py`:
```python
import json, time
from pathlib import Path
import pytest
import pipeline_health_check as auditor

def test_auditor_flags_stale_parquet(tmp_path, monkeypatch):
    # Setup test file older than max_age
    old_file = tmp_path / "BTC_Time_15m_footprint.parquet"
    old_file.write_text("data")
    import os
    stale_time = time.time() - 3600  # 1 hour ago
    os.utime(old_file, (stale_time, stale_time))
    
    result = auditor.audit_parquet_freshness(old_file, max_age_secs=900)
    assert result["status"] == "STALE"
    assert result["age_seconds"] >= 3600

def test_auditor_passes_fresh_parquet(tmp_path):
    fresh_file = tmp_path / "BTC_Time_15m_footprint.parquet"
    fresh_file.write_text("data")
    result = auditor.audit_parquet_freshness(fresh_file, max_age_secs=900)
    assert result["status"] == "FRESH"
    assert result["age_seconds"] < 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/api_pipeline_audits/test_health_auditor.py -v`  
Expected: FAIL with `ModuleNotFoundError: No module named 'pipeline_health_check'`

- [ ] **Step 3: Implement `API/pipeline_health_check.py`**

Implement comprehensive audit functions:
- `audit_parquet_freshness(path: Path, max_age_secs: float, min_rows: int = 1) -> dict`
- `audit_lock_files(lock_dir: Path, stale_threshold_secs: float = 180.0) -> list[dict]`
- `audit_full_pipeline(db_dir: Path, symbols: list[str], max_age_min: float = 15.0) -> dict`
- Main CLI parser supporting `--max-age-min`, `--db-dir`, `--json`, and proper exit codes (`0` vs `1`).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/api_pipeline_audits/test_health_auditor.py -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add API/pipeline_health_check.py tests/api_pipeline_audits/test_health_auditor.py
git commit -m "feat(auditor): add standalone pipeline health check CLI and library"
```

---

### Task 4: Run Full Regression and External Audit Suites

**Files:**
- Test: `tests/api_pipeline_audits/`
- Test: `C:\Users\SIGMA\Downloads\api_audit_harness_v5\run_all.py`

- [ ] **Step 1: Run complete repository pytest suite**

Run: `python -m pytest tests/api_pipeline_audits tests/test_confluence_expansion.py -q`  
Expected: 31+ passed (100% GREEN)

- [ ] **Step 2: Run external v5 hostile audit harness**

Run: `python C:\Users\SIGMA\Downloads\api_audit_harness_v5\run_all.py`  
Expected: 5/5 SUITES PASSED (100% GREEN)

- [ ] **Step 3: Verify clean git status and commit any test adjustments**

```bash
git status
```

---

### Task 5: Live Pipeline Smoke Run & Agent Cron Setup

**Files:**
- CLI / Schedule

- [ ] **Step 1: Execute single-cycle parallel live harvest smoke test**

Run: `python API/run_parallel_standalone.py --sample 2 --with-legend`  
Expected: Completes all 4 pillars for 2 assets, outputs `Database/pipeline_health.json`, exits 0 cleanly.

- [ ] **Step 2: Execute `pipeline_health_check.py` against live dumps**

Run: `python API/pipeline_health_check.py --max-age-min 60`  
Expected: Reports status for sampled assets, exits 0.

- [ ] **Step 3: Register recurring Antigravity Cron via `schedule` tool**

Schedule recurring agent check every 15 minutes (`*/15 * * * *`) that executes `python API/pipeline_health_check.py --max-age-min 20 --json` to alert in chat if data dump stalls.

- [ ] **Step 4: Push to `origin/main`**

```bash
git push origin main
```
