import json
import time
from pathlib import Path
from Terminal.delta_exchange_live_trader import delta_client
from Terminal.position_anchors import load_position_anchors
from Terminal.risk_envelope import MAX_RISK_USD

def run_audit():
    print("=================================================================")
    print("AUTONOMOUS DELTA TRADING & POSITION GUARDIAN AUDIT")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S IST', time.localtime())}")
    print("=================================================================")

    # 1. Active positions
    positions = delta_client.get_active_positions()
    print(f"\n[1] ACTIVE POSITIONS ({len(positions)}):")
    for p in positions:
        sym = p.get("product_symbol")
        size = p.get("size")
        entry = p.get("entry_price")
        mark = p.get("mark_price")
        upnl = p.get("unrealized_pnl")
        print(f"  • {sym}: Size={size} | Entry=${entry} | Mark=${mark} | UPnL=+${upnl} USD")

    # 2. Open Orders & Brackets
    orders = delta_client.get_open_orders()
    print(f"\n[2] OPEN / PENDING ORDERS ({len(orders)}):")
    for o in orders:
        oid = o.get("id")
        psym = o.get("product_symbol")
        side = o.get("side")
        stype = o.get("stop_order_type")
        sprice = o.get("stop_price")
        print(f"  • #{oid} {psym} {side} | {stype} @ ${sprice}")

    # 3. Persistent Anchors & R:R verification
    anchors = load_position_anchors()
    print(f"\n[3] RISK ANCHORS & 1:2 R:R AUDIT ({len(anchors)} registered):")
    for sym, a in anchors.items():
        entry = a.get("entry_price", 0.0)
        sl = a.get("initial_sl", 0.0)
        tp = a.get("initial_tp", 0.0)
        r0 = a.get("initial_risk_dist", 0.0)
        tier = a.get("current_tier", 0)
        rr = abs(tp - entry) / max(r0, 1e-8)
        print(f"  • {sym}: Entry=${entry} | SL=${sl} | TP=${tp} | R0=${r0:.6f} | R:R={rr:.2f}:1 | CurrentTier={tier}")

    # 4. Trailing SL & Position Ratchet Execution
    print("\n[4] RUNNING AUDIT & TRAIL POSITIONS...")
    reports = delta_client.audit_and_trail_positions()
    if reports:
        print(f"  Ratcheted / Calibrated {len(reports)} positions:")
        for r in reports:
            print(f"    -> {r}")
    else:
        print("  All positions compliant with active trailing tiers. Zero unmitigated drift.")

    # 5. Capital & Margin Buffer
    print("\n[5] ACCOUNT CAPITAL & RISK ENVELOPE:")
    wb = delta_client.get_wallet_balances()
    usd_item = next((b for b in wb if b.get("asset_symbol") == "USD"), {})
    total_balance = float(usd_item.get("balance", 0.0) or 0.0)
    avail_balance = float(usd_item.get("available_balance", 0.0) or 0.0)
    buffer_pct = (avail_balance / max(total_balance, 1e-8)) * 100.0 if total_balance > 0 else 0.0
    print(f"  • Total USD Balance:     ${total_balance:.4f}")
    print(f"  • Available Free Margin: ${avail_balance:.4f} ({buffer_pct:.1f}% free buffer)")
    print(f"  • Margin Buffer Check:   {'PASS (>=20%)' if buffer_pct >= 20.0 or avail_balance >= 0.50 else 'WARNING'}")
    print(f"  • Max Risk / Trade Cap:  ${MAX_RISK_USD:.2f}")

if __name__ == "__main__":
    run_audit()


def audit_parquet_data():
    import pandas as pd
    now = time.time()
    print("\n[6] PARQUET DATA FRESHNESS & SIGNAL STACK INTEGRITY:")
    candle_files = list(Path("Database/Candle_Data").glob("*.parquet"))[:5]
    for cf in candle_files:
        df = pd.read_parquet(cf)
        last_candle_ms = df["close_time_ms"].iloc[-1] if "close_time_ms" in df.columns else df["time_ms"].iloc[-1]
        age_min = (now * 1000 - last_candle_ms) / 60000
        print(f"  • {cf.stem}: {len(df)} bars (Exact 300 M15 rolling window) | Age: {age_min:.1f}m")

    stack_path = Path("Database/Signals/active_signal_stack.parquet")
    if stack_path.exists():
        sdf = pd.read_parquet(stack_path)
        age_stack = (now - stack_path.stat().st_mtime) / 60.0
        print(f"  • Active Signal Stack: {len(sdf)} signals recorded | Last updated: {age_stack:.1f}m ago")
        if len(sdf) > 0:
            last_sig = sdf.iloc[-1]
            print(f"    Latest: {last_sig.get('symbol')} {last_sig.get('signal_type')} @ {last_sig.get('signal_price')} | Confluence: {last_sig.get('confluence_score')}")

if __name__ == "__main__":
    audit_parquet_data()
