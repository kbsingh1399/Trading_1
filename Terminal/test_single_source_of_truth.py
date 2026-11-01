"""
CI Verification: Single Source of Truth for Risk Constants
==========================================================
Verifies that canonical risk constants (HARD_FLOOR_USD, MAX_RISK_USD, etc.)
are exclusively defined in Terminal/risk_envelope.py and imported everywhere else.
Fails CI if any module defines hardcoded local risk numbers.
"""

import os
import re
from pathlib import Path
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
TERMINAL_DIR = ROOT_DIR / "Terminal"

PROTECTED_DEFINITIONS = [
    r"^\s*HARD_FLOOR_USD\s*=",
    r"^\s*HALT_CUSHION_USD\s*=",
    r"^\s*MAX_RISK_USD\s*=",
    r"^\s*MIN_RISK_USD\s*=",
    r"^\s*SURVIVAL_LOSSES\s*=",
    r"^\s*MAX_OPEN_POSITIONS\s*="
]


def test_no_duplicate_risk_definitions():
    """Ensure no python file outside risk_envelope.py defines risk constants."""
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
