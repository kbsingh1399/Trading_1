"""
========================================================================================
AUTONOMOUS TRADING PIPELINE ROOT LAUNCHER (BACKWARD-COMPATIBILITY WRAPPER)
========================================================================================
Delegates directly to main.py, the primary standalone system controller.
"""

from main import (
    main,
    run_sweep,
    run_sentinel,
    run_audit,
    run_test,
    run_parity,
    run_optimize_ram,
    run_daemon,
    run_status
)

if __name__ == "__main__":
    main()
