"""
========================================================================================
AUTONOMOUS TRADING PIPELINE ROOT LAUNCHER
========================================================================================
Unified management interface for pure-API signal ingestion, live Sentinel execution,
risk audit, automated tests, and CoinGlass parity validation.

Usage:
    python run_pipeline.py --sweep     # Run single pure API footprint sweep across assets
    python run_pipeline.py --daemon    # Run continuous 2-process pipeline (Harvester + Sentinel)
    python run_pipeline.py --sentinel  # Run Sentinel execution bridge (watches footprint_signals.jsonl)
    python run_pipeline.py --audit     # Run Delta Exchange India position trailing & margin audit
    python run_pipeline.py --test      # Run full repository test suite (207 tests)
    python run_pipeline.py --parity    # Run empirical CoinGlass footprint parity validation
"""

import sys
import os
import argparse
import subprocess
import signal
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def run_sweep(crypto_only: bool = False):
    """Executes a single pure API footprint and order flow sweep."""
    from Terminal.unified_api_footprint_engine import UnifiedAPIFootprintEngine
    print("\n[PIPELINE] Launching single pure API footprint sweep...")
    engine = UnifiedAPIFootprintEngine(crypto_only=crypto_only)
    try:
        res = engine.run_sweep()
        print(f"\n[PIPELINE] Sweep complete:")
        print(f"  • Processed assets: {res.get('processed_assets', 0)}")
        print(f"  • New signals:     {res.get('new_signals_count', 0)}")
        print(f"  • Elapsed time:    {res.get('elapsed_seconds', 0.0):.2f}s")
    finally:
        engine.shutdown()


def run_sentinel(reset_offset: bool = False, once: bool = False):
    """Runs the Sentinel live execution bridge."""
    cmd = [sys.executable, "5_wake_on_footprint_signal.py"]
    if reset_offset:
        cmd.append("--reset-offset")
    if once:
        cmd.append("--once")
    print(f"\n[PIPELINE] Launching Sentinel Live Execution Bridge ({' '.join(cmd)})...")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_audit():
    """Runs the Delta Exchange India position guardian and trailing ratchet audit."""
    print("\n[PIPELINE] Running Delta Exchange India Position & Risk Audit...")
    from Terminal.cron_audit import run_audit as exec_audit
    exec_audit()


def run_test():
    """Runs the full repository test suite via pytest."""
    print("\n[PIPELINE] Running complete test suite via pytest...")
    cmd = [sys.executable, "-m", "pytest", "tests", "-v"]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_parity():
    """Runs the empirical CoinGlass footprint parity validation report."""
    print("\n[PIPELINE] Running empirical CoinGlass footprint parity analysis...")
    cmd = [sys.executable, "tools/compare_api_vs_coinglass.py"]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_optimize_ram():
    """Trims stale working-set allocations across background processes and runs GC."""
    print("\n[PIPELINE] Running Win32 working-set RAM optimization...")
    from tools.optimize_memory import optimize_system_ram
    res = optimize_system_ram()
    print("=================================================================")
    print("WIN32 WORKING-SET RAM OPTIMIZATION REPORT")
    print("=================================================================")
    print(f"Trimmed processes:   {res.get('trimmed_processes')}")
    print(f"Free RAM before:     {res.get('free_before_gb')} GB")
    print(f"Free RAM after:      {res.get('free_after_gb')} GB (+{res.get('freed_gb')} GB freed)")
    print(f"Current Memory Load: {res.get('memory_load_pct')}%")
    print("=================================================================")



def run_daemon(interval: int = 60, crypto_only: bool = False):
    """
    Spawns and monitors the two decoupled processes:
      1. Harvester: Terminal/unified_api_footprint_engine.py --daemon
      2. Sentinel:  5_wake_on_footprint_signal.py
    """
    print("=" * 70)
    print("AUTONOMOUS TRADING 2-PROCESS DAEMON SUPERVISOR")
    print("=" * 70)
    print(f"Harvester Interval: {interval}s | Crypto Only: {crypto_only}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)

    # 1. Spawn Harvester
    harvester_cmd = [sys.executable, "-m", "Terminal.unified_api_footprint_engine", "--daemon", f"--interval={interval}"]
    if crypto_only:
        harvester_cmd.append("--crypto-only")

    # 2. Spawn Sentinel
    sentinel_cmd = [sys.executable, "5_wake_on_footprint_signal.py"]

    print(f"[SUPERVISOR] Spawning Harvester: {' '.join(harvester_cmd)}")
    p_harvester = subprocess.Popen(harvester_cmd, cwd=str(ROOT_DIR), env=env)

    print(f"[SUPERVISOR] Spawning Sentinel:  {' '.join(sentinel_cmd)}")
    p_sentinel = subprocess.Popen(sentinel_cmd, cwd=str(ROOT_DIR), env=env)

    procs = [p_harvester, p_sentinel]

    def _terminate_all(sig=None, frame=None):
        print("\n[SUPERVISOR] Termination signal received. Terminating child processes...")
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            if p.poll() is None:
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        print("[SUPERVISOR] All daemons halted cleanly.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _terminate_all)
    signal.signal(signal.SIGTERM, _terminate_all)

    print("[SUPERVISOR] All daemons running. Press Ctrl+C to halt supervisor.")
    try:
        while True:
            for name, p in [("Harvester", p_harvester), ("Sentinel", p_sentinel)]:
                ret = p.poll()
                if ret is not None:
                    print(f"[SUPERVISOR] WARNING: Process {name} exited with code {ret}!")
                    _terminate_all()
            time.sleep(2)
    except KeyboardInterrupt:
        _terminate_all()


def main():
    parser = argparse.ArgumentParser(description="Autonomous Trading Pipeline Root Launcher")
    parser.add_argument("--sweep", action="store_true", help="Run single pure API footprint sweep")
    parser.add_argument("--daemon", action="store_true", help="Run continuous 2-process pipeline (Harvester + Sentinel)")
    parser.add_argument("--sentinel", action="store_true", help="Run Sentinel live execution engine")
    parser.add_argument("--audit", action="store_true", help="Run Delta Exchange India position & risk audit")
    parser.add_argument("--test", action="store_true", help="Run complete pytest test suite")
    parser.add_argument("--parity", action="store_true", help="Run CoinGlass footprint parity validation report")
    parser.add_argument("--optimize-ram", action="store_true", help="Trim working sets and recover system RAM")
    parser.add_argument("--crypto-only", action="store_true", help="Restrict harvester sweep to crypto only")
    parser.add_argument("--interval", type=int, default=60, help="Harvester daemon sweep interval in seconds (default: 60)")
    parser.add_argument("--reset-offset", action="store_true", help="Start sentinel from current end of signal ledger")
    args = parser.parse_args()

    if args.sweep:
        run_sweep(crypto_only=args.crypto_only)
    elif args.sentinel:
        sys.exit(run_sentinel(reset_offset=args.reset_offset))
    elif args.audit:
        run_audit()
    elif args.test:
        sys.exit(run_test())
    elif args.parity:
        sys.exit(run_parity())
    elif args.optimize_ram:
        run_optimize_ram()
    elif args.daemon:
        run_daemon(interval=args.interval, crypto_only=args.crypto_only)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
