"""
5_trade_evaluator.py
========================================================================================
Pillar 5 Standalone CLI: Multi-Pillar Trade Evaluator & Rolling 5-Signal Queue
========================================================================================
Provides on-demand and scheduled execution for asset queue evaluation:
  python 5_trade_evaluator.py --all
  python 5_trade_evaluator.py --symbol BTC
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import main

if __name__ == "__main__":
    main()
