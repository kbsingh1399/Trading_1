---
type: feedback
created: 2026-11-06
updated: 2026-11-06
---

# User Feedback History

## 2026-11-06: Data Ingestion Stagnation Discovered
- **Feedback**: User was not convinced all data was updating properly ("Can you check if all data is updating properly...I am still not convinced").
- **Diagnosis**: Forensically uncovered that `Database/Signals/coinglass_aggregated_futures_signals.parquet`, `Live_Liquidations/summary_liquidations.parquet`, and `Liquidation_Data/liquidation_snapshots.parquet` had not updated in 9.7 hours.
- **Root Cause**: `upsert_parquet()` entered `writer_lock()` and immediately called `atomic_parquet()` which also entered `writer_lock()`. On Windows, `msvcrt.locking` on a separate file descriptor failed to acquire the second lock, causing a 5.0-second timeout and raising `TimeoutError: Writer busy`.
- **Action**: Implement thread-local re-entrant locking and clean up orphaned locks.

## 2026-11-06: Local Cloudflare Auto IP Rotation Mandate
- **Feedback**: User requested Cloudflare auto-rotation IP feature without invoking external cloud APIs or paid services.
- **Action**: Integrate local `warp-cli` (Cloudflare WARP client) to reconnect/rotate egress IP on demand when Cloudflare Turnstile/challenge pages appear.
