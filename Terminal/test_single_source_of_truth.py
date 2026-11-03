"""
CI Verification: Single Source of Truth for Risk Constants
==========================================================
Verifies that canonical risk constants (HARD_FLOOR_USD, MAX_RISK_USD, etc.)
are exclusively defined in Terminal/risk_envelope.py and imported everywhere else.
Fails CI if any module defines hardcoded local risk numbers or diverges from SSOT.
"""

import os
import re
from pathlib import Path
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
TERMINAL_DIR = ROOT_DIR / "Terminal"

PROTECTED_DEFINITIONS = [
    r"^\s*HARD_FLOOR_USD\s*=\s*\d+",
    r"^\s*HALT_CUSHION_USD\s*=\s*\d+",
    r"^\s*MAX_RISK_USD\s*=\s*\d+",
    r"^\s*MIN_RISK_USD\s*=\s*\d+",
    r"^\s*SURVIVAL_LOSSES\s*=\s*\d+",
    r"^\s*MAX_OPEN_POSITIONS\s*=\s*\d+",
    r"^\s*MAX_DAILY_LOSS_USD\s*=\s*\d+",
    r"^\s*MIN_RR_RATIO\s*=\s*\d+"
]


def test_no_duplicate_risk_definitions():
    """Ensure no python file outside risk_envelope.py defines raw numerical risk constants."""
    violating_files = []

    for py_file in TERMINAL_DIR.glob("*.py"):
        if py_file.name in ["risk_envelope.py", "test_risk_envelope.py", "test_single_source_of_truth.py"]:
            continue

        content = py_file.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(content.splitlines(), start=1):
            for pattern in PROTECTED_DEFINITIONS:
                if re.search(pattern, line):
                    violating_files.append(f"{py_file.name}:{line_no} -> {line.strip()}")

    assert len(violating_files) == 0, f"Found rogue risk constant definitions outside risk_envelope.py:\n" + "\n".join(violating_files)


def test_venue_modules_import_risk_envelope_and_match_canonical_values():
    """Verifies that all broker venues adhere to SSOT risk envelope constants."""
    from Terminal import risk_envelope
    from Terminal import matchtrader_live_trader
    from Terminal import delta_exchange_live_trader

    # Canonical values in risk_envelope
    assert risk_envelope.HARD_FLOOR_USD == 4775.00
    assert risk_envelope.MAX_RISK_USD == 25.00
    assert risk_envelope.MAX_OPEN_POSITIONS == 2
    assert risk_envelope.MAX_DAILY_LOSS_USD == 250.00
    assert risk_envelope.MIN_RR_RATIO == 2.00

    # MatchTrader venue alignment
    assert matchtrader_live_trader.EQUITY_FLOOR_TOTAL == risk_envelope.HARD_FLOOR_USD
    assert matchtrader_live_trader.MAX_CONCURRENT_POSITIONS == risk_envelope.MAX_OPEN_POSITIONS
    assert matchtrader_live_trader.DEFAULT_RISK_PER_TRADE_USD == risk_envelope.MAX_RISK_USD
    assert matchtrader_live_trader.MAX_DAILY_DRAWDOWN_LIMIT == risk_envelope.MAX_DAILY_LOSS_USD
    assert matchtrader_live_trader.LIVE_DISPATCH_ENABLED is False

    # Delta Exchange India venue alignment
    assert delta_exchange_live_trader.MAX_DELTA_CONCURRENT_POSITIONS == risk_envelope.MAX_OPEN_POSITIONS
    assert delta_exchange_live_trader.LIVE_DISPATCH_ENABLED is False
