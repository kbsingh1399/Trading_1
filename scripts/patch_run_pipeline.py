from pathlib import Path

target_file = Path("run_pipeline.py")
content = target_file.read_text(encoding="utf-8")

# 1. Update run_scraper timeout logic for footprint scraper
old_run_scraper = """def run_scraper(script_name: str, desc: str, flag: str = "--once", timeout_secs: float = 300.0) -> bool:"""
new_run_scraper = """def run_scraper(script_name: str, desc: str, flag: str = "--once", timeout_secs: float = 300.0) -> bool:
    # 31 symbols on footprint scraper require extended 420s window to avoid premature truncation
    if "1_coinglass_footprint" in script_name and timeout_secs <= 300.0:
        timeout_secs = 420.0"""

if old_run_scraper in content:
    content = content.replace(old_run_scraper, new_run_scraper, 1)
    print("Updated run_scraper timeout for footprint scraper.")

# 2. Add clean_all_abandoned_locks call to scheduler_daemon
old_daemon_init = """    logger.info("Pipeline scheduler daemon running. Waiting for next trigger slot...")"""
new_daemon_init = """    from coinglass_common import clean_all_abandoned_locks
    reaped = clean_all_abandoned_locks(ttl=120.0)
    if reaped > 0:
        logger.info(f"🧹 Initialized clean slate: purged {reaped} abandoned locks.")
    logger.info("Pipeline scheduler daemon running. Waiting for next trigger slot...")"""

if old_daemon_init in content:
    content = content.replace(old_daemon_init, new_daemon_init, 1)
    print("Added clean_all_abandoned_locks to scheduler_daemon.")

target_file.write_text(content, encoding="utf-8")
print("Successfully patched run_pipeline.py!")
