"""
========================================================================================
STANDALONE QUANT TRADING ENGINE - PRIMARY SYSTEM CONTROLLER
========================================================================================
Single standalone entry point for C:\\Users\\SIGMA\\Documents\\_Trading_1

Commands:
    python main.py --daemon       # Run autonomous 2-process pipeline (Harvester + Sentinel)
    python main.py --sweep        # Run on-demand 4-exchange pure API footprint sweep
    python main.py --status       # 360-degree operational reality & health audit
    python main.py --sentinel     # Run Sentinel live execution bridge alone
    python main.py --audit        # Run Delta Exchange India position trailing & risk audit
    python main.py --parity       # Run CoinGlass multi-asset empirical parity validation
    python main.py --test         # Run complete test suite (21 unit & integration tests)
    python main.py --optimize-ram # Trim working-set RAM allocations
========================================================================================
"""

import sys
import os
import argparse
import subprocess
import signal
import time
from pathlib import Path
from datetime import datetime, timezone

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def run_status():
    """Performs a 360-degree operational reality check across disk, memory, and exchanges."""
    print("=" * 80)
    print("360-DEGREE SYSTEM OPERATIONAL REALITY STATUS")
    print("=" * 80)

    # 1. Process table audit
    print("\n[1] ACTIVE BACKGROUND PROCESSES:")
    try:
        import psutil
        found_procs = []
        for p in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmd = ' '.join(p.info['cmdline'] or [])
                if any(k in cmd.lower() for k in ['unified_api_footprint_engine', '5_wake_on_footprint_signal', 'hyperdash_intel_engine']) and 'main.py' not in cmd:
                    found_procs.append((p.info['pid'], p.info['name'], cmd[:70]))
            except Exception:
                pass
        if found_procs:
            for pid, name, cmd in found_procs:
                print(f"  • PID {pid} ({name}): {cmd}...")
        else:
            print("  • No background trading daemons currently running (Standby mode).")
    except Exception as e:
        print(f"  • Process audit error: {e}")

    # 2. Disk persistence & candle data freshness
    print("\n[2] PHYSICAL DISK CANDLE STORAGE (Database/Candle_Data/):")
    candle_dir = ROOT_DIR / "Database" / "Candle_Data"
    if candle_dir.exists():
        pq_files = sorted(list(candle_dir.glob("*.parquet")), key=lambda p: p.stat().st_mtime, reverse=True)
        print(f"  • Total Asset Parquets: {len(pq_files)}")
        if pq_files:
            latest = pq_files[0]
            age_min = (time.time() - latest.stat().st_mtime) / 60.0
            print(f"  • Most recently updated: {latest.name} ({latest.stat().st_size / 1024:.1f} KB, {age_min:.1f} mins ago)")
            for f in pq_files[:5]:
                print(f"      - {f.name:<12} {f.stat().st_size / 1024:>6.1f} KB (Updated: {datetime.fromtimestamp(f.stat().st_mtime).strftime('%H:%M:%S')})")
    else:
        print("  • Candle_Data directory not found!")

    # 3. Recent evaluated signals
    print("\n[3] RECENT EVALUATED SIGNALS (Database/Signals/evaluated_signals_ledger.jsonl):")
    ledger_file = ROOT_DIR / "Database" / "Signals" / "evaluated_signals_ledger.jsonl"
    if ledger_file.exists() and ledger_file.stat().st_size > 0:
        import json
        with open(ledger_file, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        print(f"  • Total Evaluated Records: {len(lines)}")
        for l in lines[-5:]:
            try:
                rec = json.loads(l)
                print(f"      - [{rec.get('evaluated_at_ist', '')}] {rec.get('symbol'):<6} {rec.get('direction'):<4} Score: {rec.get('confluence_score'):>4.1f} | Action: {rec.get('action')}")
            except Exception:
                pass
    else:
        print("  • Evaluated signals ledger is currently empty.")

    # 4. Delta Exchange India live connectivity check
    print("\n[4] DELTA EXCHANGE INDIA CONNECTIVITY & RISK BUFFER:")
    try:
        from Terminal.delta_exchange_live_trader import delta_client
        if delta_client.api_key:
            wb = delta_client.get_wallet_balances() or []
            print(f"  • API Connection: Authenticated (Key: {delta_client.api_key[:6]}...)")
            for b in wb:
                if b.get("asset_symbol") in ("USD", "USDT", "INR"):
                    bal = float(b.get("balance", 0.0) or 0.0)
                    avail = float(b.get("available_balance", 0.0) or 0.0)
                    pct = (avail / bal * 100.0) if bal > 0 else 0.0
                    print(f"      - {b.get('asset_symbol')}: Bal ${bal:,.2f} | Avail ${avail:,.2f} ({pct:.1f}% free buffer)")
            positions = delta_client.get_active_positions() or []
            print(f"  • Open Positions: {len(positions)} / 2 allowed")
            for p in positions:
                print(f"      - {p.get('product_symbol')}: Size {p.get('size')} | Entry ${float(p.get('entry_price', 0.0)):,.2f} | Mark ${float(p.get('mark_price', 0.0)):,.2f}")
        else:
            print("  • API Key: Not configured (Shadow Paper Trading active).")
    except Exception as e:
        print(f"  • Delta Exchange check note: {e}")
    print("=" * 80)


def run_sweep(crypto_only: bool = False):
    """Executes a single pure API footprint and order flow sweep."""
    from Terminal.unified_api_footprint_engine import UnifiedAPIFootprintEngine
    print("\n[MAIN] Launching pure API footprint sweep across aggregated venues...")
    engine = UnifiedAPIFootprintEngine(crypto_only=crypto_only)
    try:
        res = engine.run_sweep()
        print("\n[MAIN] Sweep complete:")
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
    print(f"\n[MAIN] Launching Sentinel Live Execution Bridge ({' '.join(cmd)})...")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_audit():
    """Runs the Delta Exchange India position guardian and trailing ratchet audit."""
    print("\n[MAIN] Running Delta Exchange India Position & Risk Audit...")
    from Terminal.cron_audit import run_audit as exec_audit
    exec_audit()


def run_test():
    """Runs the full repository test suite via pytest."""
    print("\n[MAIN] Running complete test suite via pytest...")
    cmd = [sys.executable, "-m", "pytest", "tests/test_coinglass_api_replica.py", "tests/test_aggregated_futures_feed.py", "tests/test_live_execution_guardrails.py", "-v"]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_parity():
    """Runs the empirical CoinGlass footprint parity validation report."""
    print("\n[MAIN] Running empirical CoinGlass multi-asset footprint parity analysis...")
    cmd = [sys.executable, "tools/verify_multi_asset_parity.py"]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)
    p = subprocess.run(cmd, cwd=str(ROOT_DIR), env=env)
    return p.returncode


def run_optimize_ram():
    """Trims stale working-set allocations across background processes and runs GC."""
    print("\n[MAIN] Running Win32 working-set RAM optimization...")
    from tools.optimize_memory import optimize_system_ram
    res = optimize_system_ram()
    print("=" * 65)
    print("WIN32 WORKING-SET RAM OPTIMIZATION REPORT")
    print("=" * 65)
    print(f"Trimmed processes:   {res.get('trimmed_processes')}")
    print(f"Free RAM before:     {res.get('free_before_gb')} GB")
    print(f"Free RAM after:      {res.get('free_after_gb')} GB (+{res.get('freed_gb')} GB freed)")
    print(f"Current Memory Load: {res.get('memory_load_pct')}%")
    print("=" * 65)


def run_daemon(interval: int = 60, crypto_only: bool = False):
    """
    Spawns and monitors the two decoupled processes:
      1. Harvester: Terminal/unified_api_footprint_engine.py --daemon
      2. Sentinel:  5_wake_on_footprint_signal.py
    """
    print("=" * 80)
    print("AUTONOMOUS TRADING 2-PROCESS DAEMON SUPERVISOR")
    print("=" * 80)
    print(f"Harvester Interval: {interval}s | Crypto Only: {crypto_only}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_DIR)

    harvester_cmd = [sys.executable, "-m", "Terminal.unified_api_footprint_engine", "--daemon", f"--interval={interval}"]
    if crypto_only:
        harvester_cmd.append("--crypto-only")

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
    parser = argparse.ArgumentParser(description="Standalone Quant Trading Engine Primary Controller")
    parser.add_argument("--status", action="store_true", help="Display 360-degree operational reality & health status")
    parser.add_argument("--daemon", action="store_true", help="Run continuous 2-process pipeline (Harvester + Sentinel)")
    parser.add_argument("--sweep", action="store_true", help="Run single pure API footprint sweep across assets")
    parser.add_argument("--sentinel", action="store_true", help="Run Sentinel live execution engine alone")
    parser.add_argument("--audit", action="store_true", help="Run Delta Exchange India position & risk audit")
    parser.add_argument("--parity", action="store_true", help="Run CoinGlass multi-asset parity validation report")
    parser.add_argument("--test", action="store_true", help="Run repository pytest test suite")
    parser.add_argument("--optimize-ram", action="store_true", help="Trim working sets and recover system RAM")
    parser.add_argument("--crypto-only", action="store_true", help="Restrict harvester sweep to crypto only")
    parser.add_argument("--interval", type=int, default=60, help="Harvester daemon sweep interval in seconds (default: 60)")
    parser.add_argument("--reset-offset", action="store_true", help="Start sentinel from current end of signal ledger")

    args = parser.parse_args()

    if args.status:
        run_status()
    elif args.daemon:
        run_daemon(interval=args.interval, crypto_only=args.crypto_only)
    elif args.sweep:
        run_sweep(crypto_only=args.crypto_only)
    elif args.sentinel:
        sys.exit(run_sentinel(reset_offset=args.reset_offset))
    elif args.audit:
        run_audit()
    elif args.parity:
        sys.exit(run_parity())
    elif args.test:
        sys.exit(run_test())
    elif args.optimize_ram:
        run_optimize_ram()
    else:
        run_status()
        print("\nType 'python main.py --help' to view all available commands.")


if __name__ == "__main__":
    main()
