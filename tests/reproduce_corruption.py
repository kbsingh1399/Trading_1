import json
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import SignalQueueManager

sym = "TEST_CORRUPT"
p_jsonl = SignalQueueManager.get_signals_jsonl_path(sym)
p_legacy = SignalQueueManager.get_legacy_json_path(sym)
if p_jsonl.exists():
    p_jsonl.unlink()
if p_legacy.exists():
    p_legacy.unlink()

# Write 1 valid line, 1 corrupted line, and 1 un-terminated line (no trailing \n)
with open(p_jsonl, "w", encoding="utf-8") as f:
    f.write('{"sig_code": "trap", "side": "bull", "price": 50000.0, "time_ms": 1000, "bar_type": "time", "bar_label": "15m"}\n')
    f.write('{"sig_code": "abs", CORRUPTED_GARBAGE\n')
    f.write('{"sig_code": "div", "side": "bear", "price": 51000.0, "time_ms": 2000, "bar_type": "time", "bar_label": "15m"}')  # NO NEWLINE!

loaded = SignalQueueManager.load_all_signals(sym)
print(f"Loaded before append: {len(loaded)} signals")
for s in loaded:
    print("  Signal:", s.get("sig_code"))

# Now append a new signal
new_sig = [{
    "sig_code": "ratio_p",
    "side": "bull",
    "price": 52000.0,
    "time_ms": 3000,
    "bar_type": "time",
    "bar_label": "15m"
}]
SignalQueueManager.append_signals(sym, new_sig)

with open(p_jsonl, "r", encoding="utf-8") as f:
    raw_lines = f.readlines()
print(f"\nRaw lines count: {len(raw_lines)}")
for i, l in enumerate(raw_lines):
    print(f"Line {i+1}: {repr(l[:80])}")

loaded_after = SignalQueueManager.load_all_signals(sym)
print(f"\nLoaded after append: {len(loaded_after)} signals")
for s in loaded_after:
    print("  Signal:", s.get("sig_code"))

# Clean up
if p_jsonl.exists():
    p_jsonl.unlink()
if p_legacy.exists():
    p_legacy.unlink()
