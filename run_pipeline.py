"""
run_pipeline.py
========================================================================================
Master Zero-Overlap Pipeline Orchestrator & TDMA Scheduler for CoinGlass Scrapers
========================================================================================

Schedule Specifications (Strictly Non-Overlapping & 15m Candle Aligned):
  - 4_coinglass_tv_scraper.py          : 4x/hr (:00, :15, :30, :45) -> 15m Candle Closes
  - 1_coinglass_footprint_scraper.py   : Every 10 min (6x/hr)       -> :03, :13, :23, :33, :43, :53
  - 2_coinglass_whales.py              : 4 times/hour               -> :09, :19, :37, :51
  - 3_coinglass_liquidation_levels.py  : 3 times/hour               -> :06, :26, :48

Guarantees:
  - Strict 15m candle close alignment for TV Liquidations (:00, :15, :30, :45).
  - 0% Browser Lock Contention: Every task has a 2.0 to 7.0 minute buffer before the next.
  - Ephemeral Chrome CDP tabs: Opens tab, scrapes, closes tab immediately.
  - Minimal RAM footprint: Only one scraper process runs at any moment.
========================================================================================
"""

import sys
import os
import time
import subprocess
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent
DB_DIR = ROOT_DIR / "Database"
LOCKS_DIR = DB_DIR / ".locks"
LOCKS_DIR.mkdir(parents=True, exist_ok=True)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [PIPELINE] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("PipelineScheduler")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# Minute-by-Minute Non-Overlapping TDMA Schedule (00 - 59)
SCHEDULE = {
    # CoinGlass TV Liquidations: Every 15m candle close (4 times/hr)
    0:  ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Liquidations (15m close :00)"),
    15: ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Liquidations (15m close :15)"),
    30: ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Liquidations (15m close :30)"),
    45: ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Liquidations (15m close :45)"),

    # Footprint: Strictly every 10 minutes (6 times/hr)
    3:  ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (1/6)"),
    13: ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (2/6)"),
    23: ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (3/6)"),
    33: ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (4/6)"),
    43: ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (5/6)"),
    53: ("1_coinglass_footprint_scraper.py", "👣 Footprint Sweep (6/6)"),

    # Liquidation Levels: 3 times/hour (~every 20 min)
    6:  ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels (1/3)"),
    26: ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels (2/3)"),
    48: ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels (3/3)"),

    # Whales: 4 times/hour (~every 15 min)
    9:  ("2_coinglass_whales.py", "🐋 Whales Sweep (1/4)"),
    19: ("2_coinglass_whales.py", "🐋 Whales Sweep (2/4)"),
    37: ("2_coinglass_whales.py", "🐋 Whales Sweep (3/4)"),
    51: ("2_coinglass_whales.py", "🐋 Whales Sweep (4/4)"),
}


import urllib.request
import json

def to_ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S IST")


def get_open_tabs_snapshot() -> set:
    """Snapshots the set of tab IDs currently open in Chrome."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            tabs = json.loads(resp.read().decode())
            return {t.get("id") for t in tabs if t.get("type") == "page"}
    except Exception:
        return set()


def cleanup_scraper_orphaned_tabs(baseline_tab_ids: set):
    """
    Closes any new tab that was created during a scraper's execution that was NOT
    present in baseline_tab_ids, ensuring no scraper leaves stranded tabs even upon crash or timeout.
    """
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=2) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                # If tab was created during this scraper run AND is a CoinGlass route OR blank/failed tab
                if tid and tid not in baseline_tab_ids and t.get("type") == "page":
                    is_cg = any(k in url for k in ["coinglass.com/large-orderbook", "coinglass.com/liquidation-levels", "coinglass.com/tv/", "coinglass.com/footprint", "bot="])
                    is_blank = url in ("about:blank", "", "chrome-error://chromewebdata/")
                    if is_cg or is_blank:
                        try:
                            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                            logger.info(f"🧹 Orchestrator cleaned up orphaned scraper tab ({tid}): {t.get('title') or url}")
                        except Exception:
                            pass
    except Exception as e:
        logger.debug(f"Orchestrator tab cleanup check: {e}")


def run_scraper(script_name: str, desc: str, flag: str = "--once") -> bool:
    script_path = ROOT_DIR / script_name
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False

    baseline_tabs = get_open_tabs_snapshot()
    logger.info(f"==> Launching {desc} [{script_name}] at {to_ist_now()}...")
    t0 = time.time()
    try:
        cmd = [sys.executable, str(script_path), flag]
        res = subprocess.run(cmd, cwd=str(ROOT_DIR), capture_output=True, text=True, timeout=300)
        dur = time.time() - t0
        if res.returncode == 0:
            logger.info(f"✅ Finished {desc} in {dur:.1f}s (Exit code 0).")
            return True
        else:
            logger.warning(f"⚠️ {desc} exited with code {res.returncode} in {dur:.1f}s.")
            if res.stderr:
                logger.warning(f"STDERR snippet: {res.stderr[-300:]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {desc} timed out after 300s.")
        return False
    except Exception as e:
        logger.error(f"❌ Error running {desc}: {e}")
        return False
    finally:
        cleanup_scraper_orphaned_tabs(baseline_tabs)


def run_all_sequentially():
    """Runs a single pass across all 5 pipeline components sequentially."""
    logger.info("Starting sequential execution of all 5 pipeline components...")
    components = [
        ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Scraper"),
        ("1_coinglass_footprint_scraper.py", "👣 Footprint Scraper & Signal Evaluator"),
        ("2_coinglass_whales.py", "🐋 Whales Scraper"),
        ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels Scraper"),
        ("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator"),
    ]
    for script, desc in components:
        flag = "--all" if "5_trade_evaluator" in script else "--once"
        run_scraper(script, desc, flag=flag)
        time.sleep(2.0)
    logger.info("All 5 pipeline components finished sequential execution pass.")


def print_schedule():
    print("\n" + "=" * 78)
    print("MASTER ZERO-OVERLAP 15M CANDLE-ALIGNED TDMA TIMETABLE (Every 60 Minutes)")
    print("=" * 78)
    sorted_mins = sorted(SCHEDULE.keys())
    for m in sorted_mins:
        script, desc = SCHEDULE[m]
        print(f"  Minute :{m:02d}  -->  {desc:50s} ({script})")
    print("=" * 78)
    print("Total Scheduled Sweeps per Hour: 17")
    print("  • TV Liquidations:    4 runs/hr  (Every 15m candle close: :00, :15, :30, :45)")
    print("  • Footprint:          6 runs/hr  (Strictly every 10 min: :03, :13, :23, :33, :43, :53)")
    print("  • Liquidation Levels: 3 runs/hr  (:06, :26, :48)")
    print("  • Whales:             4 runs/hr  (:09, :19, :37, :51)")
    print("Minimum buffer between any two tasks: 2.0 to 7.0 minutes. Zero overlap.")
    print("=" * 78 + "\n")


def scheduler_daemon():
    print_schedule()
    logger.info("Pipeline scheduler daemon running. Waiting for next trigger slot...")
    last_dispatched_minute = -1

    while True:
        now = datetime.now()
        cur_min = now.minute

        if cur_min in SCHEDULE and cur_min != last_dispatched_minute:
            script, desc = SCHEDULE[cur_min]
            last_dispatched_minute = cur_min
            success = run_scraper(script, desc)

            # At each 15m candle close (:00, :15, :30, :45), immediately execute trade evaluator across all new signals
            if cur_min in (0, 15, 30, 45):
                logger.info(f"🎯 Triggering 15m Candle Close Trade Evaluation pass at minute :{cur_min:02d}...")
                run_scraper("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator (15m Close)", flag="--all")

        # Sleep till next 5-second boundary
        time.sleep(5.0)


def main():
    parser = argparse.ArgumentParser(description="CoinGlass Pipeline Master Orchestrator")
    parser.add_argument("--daemon", action="store_true", help="Run continuously on the TDMA schedule")
    parser.add_argument("--run-all-now", action="store_true", help="Run all 4 scrapers once sequentially and exit")
    parser.add_argument("--schedule", action="store_true", help="Print the timetable and exit")
    args = parser.parse_args()

    if args.schedule:
        print_schedule()
    elif args.run_all_now:
        run_all_sequentially()
    else:
        scheduler_daemon()


if __name__ == "__main__":
    main()
