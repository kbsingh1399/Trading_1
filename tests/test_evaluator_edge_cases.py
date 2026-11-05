import sys
import math
import numpy as np
import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import (
    MultiPillarTradeEvaluator,
    calculate_shannon_entropy,
    safe_float,
    CANDLE_DIR,
    SETUPS_DIR
)

print("=== 1. Entropy calculation with 50/50 split ===")
labels_5050 = ["LONG"] * 5 + ["SHORT"] * 5
e_5050 = calculate_shannon_entropy(labels_5050)
print(f"Entropy for 50/50 (5 L, 5 S): {e_5050} (Expected: 1.0)")
assert abs(e_5050 - 1.0) < 1e-6, f"Expected 1.0, got {e_5050}"

print("\n=== 2. Entropy calculation with empty / single label ===")
print("Empty list entropy:", calculate_shannon_entropy([]))
print("Single label entropy:", calculate_shannon_entropy(["LONG"]))
print("All same entropy:", calculate_shannon_entropy(["LONG"] * 10))

print("\n=== 3. Evaluator with 50/50 queue and NEUTRAL trigger ===")
neutral_trigger = [{
    "sig_code": "p_spike",
    "direction": "NEUTRAL",
    "side": "neutral",
    "price": 50000.0,
    "time_ms": 1000,
    "priority": 6
}]
queue_5050 = [
    {"direction": "LONG", "sig_code": "trap", "price": 50000.0, "time_ms": 1000, "priority": 1},
    {"direction": "SHORT", "sig_code": "trap", "price": 50000.0, "time_ms": 1000, "priority": 1},
]
res_deadlock = MultiPillarTradeEvaluator.evaluate_symbol(
    symbol="TEST_DEADLOCK",
    trigger_signals=neutral_trigger,
    queue=queue_5050,
    current_price=50000.0
)
print("Result for 50/50 neutral deadlock:", res_deadlock)

print("\n=== 4. Evaluator with zero reference price ===")
res_zero_px = MultiPillarTradeEvaluator.evaluate_symbol(
    symbol="TEST_ZEROPX",
    trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 0.0, "time_ms": 1000, "priority": 1}],
    queue=[{"sig_code": "trap", "direction": "LONG", "price": 0.0, "time_ms": 1000, "priority": 1}],
    current_price=0.0
)
print("Result for zero price:", res_zero_px)

print("\n=== 5. Evaluator with NaN reference price ===")
try:
    res_nan_px = MultiPillarTradeEvaluator.evaluate_symbol(
        symbol="TEST_NANPX",
        trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": float("nan"), "time_ms": 1000, "priority": 1}],
        queue=[{"sig_code": "trap", "direction": "LONG", "price": float("nan"), "time_ms": 1000, "priority": 1}],
        current_price=float("nan")
    )
    print("Result for NaN price:", res_nan_px)
except Exception as e:
    print(f"CRASH on NaN price: {type(e).__name__}: {e}")

print("\n=== 6. Evaluator with missing candle data ===")
res_no_candle = MultiPillarTradeEvaluator.evaluate_symbol(
    symbol="NONEXISTENT_SYMBOL_999",
    trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
    queue=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
    current_price=100.0
)
print("Result with missing candle file:", "Created setup" if res_no_candle else "None")
if res_no_candle:
    print("  ATR used:", res_no_candle.get("atr_15m"))
    print("  Status:", res_no_candle.get("status"))

print("\n=== 7. Evaluator with empty candle parquet ===")
empty_p = CANDLE_DIR / "TEST_EMPTY.parquet"
pd.DataFrame().to_parquet(empty_p)
try:
    res_empty_candle = MultiPillarTradeEvaluator.evaluate_symbol(
        symbol="TEST_EMPTY",
        trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
        queue=[{"sig_code": "trap", "direction": "LONG", "price": 100.0, "time_ms": 1000, "priority": 1}],
        current_price=100.0
    )
    print("Result with empty candle parquet:", "Created setup" if res_empty_candle else "None")
finally:
    if empty_p.exists():
        empty_p.unlink()

print("\n=== 8. Zero-distance stop loss causing ZeroDivisionError or entry == sl ===")
# What if defending bid is artificially constructed to test risk_dist <= 0 or zero risk?
# Let's inspect how sl_price is calculated when ref_price is small or bid wall is close
print("Done probing edge cases.")
