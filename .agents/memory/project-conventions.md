---
type: project
created: 2026-11-06
updated: 2026-11-06
---

# Project Conventions & Standards

## Pipeline & TDMA Timing
- Master Orchestrator: `run_pipeline.py` enforces a 60-minute TDMA timetable with 17 non-overlapping sweeps per hour.
- Buffer: 2.0 to 7.0 minutes buffer between scraper sweeps to eliminate browser lock contention.
- Scraper Execution: Child process per scraper with `--once` flag; Chrome tabs opened ephemerally and closed immediately upon completion or timeout.
- Time Format: All parquet files maintain both UTC (`time_ms`, `datetime_utc`) and IST (`candle_ist`, `time_ist`) columns.

## Windows & Concurrency Standards
- OS: Windows 11 with PowerShell.
- File Locking: All file operations into `Database/` use `writer_lock(path)` with `.locks/<relative_path>.writer.lock`.
- Locks MUST be re-entrant per thread/process using thread-local recursion counters to prevent `msvcrt.locking` deadlocks.
- Stale locks older than TTL must be dynamically checked via non-blocking `msvcrt.locking` before being reaped.
