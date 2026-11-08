"""
standalone_coinglass_liquidation_levels_dumper.py
========================================================================================
Root Forwarding Wrapper -> API/standalone_coinglass_liquidation_levels_api.py
========================================================================================
Maintains 100% backwards compatibility for callers invoking the root script while
delegating all execution to the standardized API directory module.
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
API_DIR = ROOT_DIR / "API"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

from standalone_coinglass_liquidation_levels_api import main

if __name__ == "__main__":
    main()
