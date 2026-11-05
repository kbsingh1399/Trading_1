import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import SignalQueueManager

sym = "TEST_UNICODE"
p_jsonl = SignalQueueManager.get_signals_jsonl_path(sym)
with open(p_jsonl, "wb") as f:
    f.write(b'{"sig_code": "trap", "side": "bull", "price": 50000.0, "time_ms": 1000, "bar_type": "time", "bar_label": "15m"}\n')
    f.write(b"\xff\xfe\x00\x12\n")
    f.write(b'{"sig_code": "div", "side": "bear", "price": 51000.0, "time_ms": 2000, "bar_type": "time", "bar_label": "15m"}\n')

res = SignalQueueManager.load_all_signals(sym)
print("Loaded signals:", len(res))
for s in res:
    print("  Signal:", s.get("sig_code"))
p_jsonl.unlink()
