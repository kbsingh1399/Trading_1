"""
scripts/patch_pipeline_and_puncher.py
========================================================================================
Applies in-flight trade lock guard to mt5_gui_puncher.py and wires Pillars 6, 7, 8
into run_pipeline.py before trade evaluation.
========================================================================================
"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

# 1. Patch run_pipeline.py
RUN_PIPELINE = ROOT_DIR / "run_pipeline.py"
with open(RUN_PIPELINE, "r", encoding="utf-8") as f:
    pipe_code = f.read()

# Update run_all_sequentially
old_seq = '''    components = [
        ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Scraper"),
        ("1_coinglass_footprint_scraper.py", "👣 Footprint Scraper & Signal Evaluator"),
        ("2_coinglass_whales.py", "🐋 Whales Scraper"),
        ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels Scraper"),
        ("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator"),
    ]'''

new_seq = '''    components = [
        ("4_coinglass_tv_scraper.py", "📺 CoinGlass TV Scraper"),
        ("1_coinglass_footprint_scraper.py", "👣 Footprint Scraper & Signal Evaluator"),
        ("2_coinglass_whales.py", "🐋 Whales Scraper"),
        ("3_coinglass_liquidation_levels.py", "💧 Liquidation Levels Scraper"),
        ("6_coinglass_oi_funding.py", "📈 Pillar 6: Open Interest & Predicted Funding"),
        ("7_coinglass_sentiment_divergence.py", "📊 Pillar 7: Top Trader Sentiment Divergence"),
        ("8_coinglass_heatmap_density.py", "🔥 Pillar 8: Liquidation Heatmap Intensity"),
        ("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator"),
    ]'''

if old_seq in pipe_code:
    pipe_code = pipe_code.replace(old_seq, new_seq, 1)
    print("Updated run_all_sequentially in run_pipeline.py")
else:
    print("WARNING: old_seq not found in run_pipeline.py")

# Update 15m candle close triggers
old_close = '''            # At each 15m candle close (:00, :15, :30, :45), immediately execute trade evaluator across all new signals
            if cur_min in (0, 15, 30, 45):
                logger.info(f"🎯 Triggering 15m Candle Close Trade Evaluation pass at minute :{cur_min:02d}...")
                run_scraper("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator (15m Close)", flag="--all")'''

new_close = '''            # At each 15m candle close (:00, :15, :30, :45), immediately execute trade evaluator across all new signals
            if cur_min in (0, 15, 30, 45):
                logger.info(f"🎯 Triggering 15m Candle Close Trade Evaluation pass at minute :{cur_min:02d}...")
                run_scraper("6_coinglass_oi_funding.py", "📈 Pillar 6: Open Interest & Predicted Funding", flag="--once", timeout_secs=30.0)
                run_scraper("7_coinglass_sentiment_divergence.py", "📊 Pillar 7: Top Trader Sentiment Divergence", flag="--once", timeout_secs=30.0)
                run_scraper("8_coinglass_heatmap_density.py", "🔥 Pillar 8: Liquidation Heatmap Intensity", flag="--once", timeout_secs=30.0)
                run_scraper("5_trade_evaluator.py", "🎯 Multi-Pillar Trade Evaluator (15m Close)", flag="--all")'''

if old_close in pipe_code:
    pipe_code = pipe_code.replace(old_close, new_close, 1)
    print("Updated 15m candle close triggers in run_pipeline.py")
else:
    print("WARNING: old_close not found in run_pipeline.py")

with open(RUN_PIPELINE, "w", encoding="utf-8") as f:
    f.write(pipe_code)


# 2. Patch mt5_gui_puncher.py
PUNCHER = ROOT_DIR / "mt5_gui_puncher.py"
with open(PUNCHER, "r", encoding="utf-8") as f:
    punch_code = f.read()

# Add FileMutex and LOCK_DIR imports
old_imp = '''import pyautogui
import MetaTrader5 as mt5'''

new_imp = '''import pyautogui
import MetaTrader5 as mt5
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from coinglass_common import FileMutex, LOCK_DIR'''

if old_imp in punch_code and "from coinglass_common import FileMutex" not in punch_code:
    punch_code = punch_code.replace(old_imp, new_imp, 1)
    print("Added FileMutex import to mt5_gui_puncher.py")

# Wrap punch_market_order with in-flight lock
old_punch = '''def punch_market_order(symbol: str, volume: str, sl: str, tp: str, order_type: str = "BUY") -> bool:
    """
    Punches a market order via MT5 GUI Order Dialog (<F9>) and automatically
    attaches hard SL and TP via the Modify dialog.
    """'''

new_punch = '''def punch_market_order(symbol: str, volume: str, sl: str, tp: str, order_type: str = "BUY") -> bool:
    """
    Punches a market order via MT5 GUI Order Dialog (<F9>) and automatically
    attaches hard SL and TP via the Modify dialog.
    Guarded by order_execution_inflight.lock to prevent Cloudflare WARP IP rotation during execution.
    """
    with FileMutex(str(LOCK_DIR / "order_execution_inflight.lock"), timeout=5.0) as inflight_guard:
        return _punch_market_order_impl(symbol, volume, sl, tp, order_type)

def _punch_market_order_impl(symbol: str, volume: str, sl: str, tp: str, order_type: str = "BUY") -> bool:'''

if old_punch in punch_code:
    punch_code = punch_code.replace(old_punch, new_punch, 1)
    print("Wrapped punch_market_order with order_execution_inflight.lock")

with open(PUNCHER, "w", encoding="utf-8") as f:
    f.write(punch_code)

print("SUCCESS: Both run_pipeline.py and mt5_gui_puncher.py patched successfully!")
