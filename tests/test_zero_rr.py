import sys
import math
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from trade_evaluator import MultiPillarTradeEvaluator

# Case: Can risk_dist become 0 or cause ZeroDivisionError?
# In line 830:
# calc_rr = round(reward_dist / max(risk_dist, 1e-9), 2)
# But what if entry_final == sl_final?
# Suppose ref_price = 1000.0, and candle has atr_14 = 0.000001
# And min_risk_dist = max(atr * 1.2, ref_price * 0.003) -> ref_price * 0.003 is 3.0.
# BUT what if ref_price is very small? e.g. PEPE at 0.00000123
# Or what if ref_price is 1000.0, but candle_ctx gives atr_14 = 0.0, and ref_price * 0.008?
# Let's test various extreme prices:
for px in [0.000001, 0.00001, 0.0001, 1.0, 1000.0, 100000.0]:
    res = MultiPillarTradeEvaluator.evaluate_symbol(
        symbol=f"TEST_RR_{px}",
        trigger_signals=[{"sig_code": "trap", "direction": "LONG", "price": px, "time_ms": 1000, "priority": 1}],
        queue=[{"sig_code": "trap", "direction": "LONG", "price": px, "time_ms": 1000, "priority": 1}],
        current_price=px
    )
    if res:
        entry = res["entry_price"]
        sl = res["stop_loss"]
        tp = res["take_profit"]
        risk = res["risk_amount"]
        rr = res["risk_reward_ratio"]
        print(f"px={px}: entry={entry}, sl={sl}, tp={tp}, risk={risk}, rr={rr}, equal={entry == sl}")
        assert entry != sl, f"Zero distance stop loss! entry == sl == {entry}"
        assert risk > 0, f"Risk amount is not positive: {risk}"
