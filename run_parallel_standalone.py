"""
Forwarder Shim: points directly to API/run_parallel_standalone.py
Enables invoking either `python run_parallel_standalone.py` or `python API/run_parallel_standalone.py`.
"""
import sys
from pathlib import Path

API_DIR = Path(__file__).resolve().parent / "API"
if str(API_DIR) not in sys.path:
    sys.path.insert(0, str(API_DIR))

import run_parallel_standalone

if __name__ == "__main__":
    run_parallel_standalone.main()
