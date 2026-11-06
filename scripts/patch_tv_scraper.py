import os
import sys
from pathlib import Path

target_file = Path("4_coinglass_tv_scraper.py")
content = target_file.read_text(encoding="utf-8")

# 1. Replace double writer_lock in process_and_persist
old_persist_lock = """    from coinglass_common import writer_lock
    with writer_lock(out_file):
        atomic_parquet(df, out_file)"""

new_persist_lock = """    atomic_parquet(df, out_file)"""

if old_persist_lock in content:
    content = content.replace(old_persist_lock, new_persist_lock, 1)
    print("Replaced double writer_lock in process_and_persist.")

# 2. Add rotate_warp_ip trigger on setup failure
old_setup_fail = """    if not inserted or not inserted.get("ok"):
        logger.warning(f"Could not ensure liquidation study: {inserted}")
        return False"""

new_setup_fail = """    if not inserted or not inserted.get("ok"):
        logger.warning(f"Could not ensure liquidation study: {inserted}")
        try:
            rotate_warp_ip("TVScraper-Setup", reason="CHALLENGE")
        except Exception:
            pass
        return False"""

if old_setup_fail in content:
    content = content.replace(old_setup_fail, new_setup_fail, 1)
    print("Added rotate_warp_ip to TV setup failure.")

# 3. Add consecutive empty scrapes tracking in run_sweep
old_sweep_loop = """                t0 = time.time()
                for base_sym, tv_sym in target_map.items():
                    try:
                        raw_rows, last_price = await fetch_symbol_liquidations(ws, tv_sym)"""

new_sweep_loop = """                t0 = time.time()
                consecutive_empty = 0
                for base_sym, tv_sym in target_map.items():
                    try:
                        raw_rows, last_price = await fetch_symbol_liquidations(ws, tv_sym)
                        if not raw_rows:
                            consecutive_empty += 1
                            if consecutive_empty >= 4:
                                logger.warning(f"[Cloudflare WARP] Detected {consecutive_empty} consecutive empty scrapes. Auto-rotating IP...")
                                try:
                                    rotate_warp_ip("TVScraper-EmptyLoop", reason="CONSECUTIVE_EMPTY_SCRAPES")
                                except Exception:
                                    pass
                                consecutive_empty = 0
                        else:
                            consecutive_empty = 0"""

if old_sweep_loop in content:
    content = content.replace(old_sweep_loop, new_sweep_loop, 1)
    print("Added consecutive empty auto-rotation to run_sweep.")

target_file.write_text(content, encoding="utf-8")
print("Successfully patched 4_coinglass_tv_scraper.py!")
