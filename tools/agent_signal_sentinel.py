"""
========================================================================================
[AGENT SIGNAL SENTINEL] Reactive Signal Wake-Up Sentinel for Antigravity AI Agent
========================================================================================
Monitors Database/Signals/footprint_signals.jsonl for newly received Footprint signals.
When ANY signal arrives:
1. Gathers complete 5-pillar quantitative telemetry:
   - Footprint Order Flow (delta, volume, CVD flow, absorption, exhaustion)
   - Auction Profile & Value Areas (Day VWAP, VAH/VAL/POC, Fib-Prime EMAs 55, 59, 233)
   - Live CoinGlass TV liquidations (spike reversals, capitulation exhaustion)
   - Liquidation Heatmap (near-field short/long asymmetry, magnet clusters)
   - Hyperdash v2 Whale Intelligence (smart bias, crowd bias, whale liquidation sweep targets)
   - DeepMind TimesFM 2.5 quantile forecasts & dynamic hurdle
   - LanceDB Vector Memory historical setup analogs
   - Trade Critic failure pattern similarity & failure modes
   - Delta Exchange India live ticker, bid/ask spread, listing resolution
2. Prints an exhaustive JSON/Markdown Wake-Up Packet to stdout.
3. Exits with code 0 to immediately trigger Antigravity Reactive Wakeup!
========================================================================================
"""

import sys
import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Reconfigure stdout for UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

FOOTPRINT_SIGNALS_JSONL = ROOT_DIR / "Database" / "Signals" / "footprint_signals.jsonl"
STATE_FILE = ROOT_DIR / "Database" / ".agent_signal_sentinel_state.json"

def load_offset() -> int:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return int(data.get("offset", 0))
        except Exception:
            pass
    # If no state file, start from the current end of file
    if FOOTPRINT_SIGNALS_JSONL.exists():
        return FOOTPRINT_SIGNALS_JSONL.stat().st_size
    return 0

def save_offset(offset: int):
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"offset": offset, "updated_at": time.time()}, f)
        tmp.replace(STATE_FILE)
    except Exception as e:
        print(f"Warning: could not save sentinel offset: {e}", file=sys.stderr)

def fetch_telemetry(sig: Dict[str, Any]) -> Dict[str, Any]:
    """Gathers all live data parameters across all 5 pillars + auxiliary AI models."""
    sym = sig.get("symbol", "N/A")
    side = sig.get("side", "N/A")
    direction = "BUY" if str(side).lower() in ("bullish", "buy", "long") else "SELL"
    price = float(sig.get("price") or 0.0)
    
    telemetry = {
        "signal": sig,
        "symbol": sym,
        "direction": direction,
        "price": price,
        "timestamp_utc": sig.get("datetime_utc") or sig.get("detection_time_utc"),
        "bar_type": sig.get("bar_type", "time"),
        "bar_label": sig.get("bar_label", "15m"),
        "bar_closed": sig.get("bar_closed", True)
    }

    # 1. Delta Exchange Live Microstructure
    try:
        from Terminal.delta_exchange_live_trader import delta_client
        product = delta_client.resolve_product(sym)
        telemetry["delta_product"] = product
        telemetry["is_listed_on_delta"] = bool(product)
        if product:
            ticker = delta_client.request("GET", f"/v2/tickers/{product}")
            if ticker and ticker.get("result"):
                t_res = ticker["result"]
                mark = float(t_res.get("mark_price") or 0.0)
                best_bid = float(t_res.get("quotes", {}).get("best_bid") or mark)
                best_ask = float(t_res.get("quotes", {}).get("best_ask") or mark)
                spread_bps = ((best_ask - best_bid) / mark * 10000.0) if mark > 0 else 0.0
                telemetry["delta_ticker"] = {
                    "mark_price": mark,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread_bps": round(spread_bps, 2),
                    "spread_gate_pass": spread_bps <= 45.0
                }
    except Exception as e:
        telemetry["delta_ticker_error"] = str(e)

    # 2. Complete Evaluator Telemetry (5 Pillars, TimesFM, Hyperdash, Critic)
    try:
        from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
        evaluator = RealtimeSignalEvaluator()
        sig_copy = dict(sig)
        if "bar_closed" not in sig_copy:
            sig_copy["bar_closed"] = True
        eval_res = evaluator.evaluate_signal(sig_copy)
        telemetry["confluence_evaluation"] = eval_res
    except Exception as e:
        telemetry["evaluator_error"] = str(e)

    return telemetry

def wait_for_signal(timeout_sec: float = 600.0) -> Optional[Dict[str, Any]]:
    """Polls footprint_signals.jsonl for newly appended signals."""
    if not FOOTPRINT_SIGNALS_JSONL.exists():
        print(f"Waiting for {FOOTPRINT_SIGNALS_JSONL} to exist...", file=sys.stderr)
        time.sleep(2)

    offset = load_offset()
    current_size = FOOTPRINT_SIGNALS_JSONL.stat().st_size if FOOTPRINT_SIGNALS_JSONL.exists() else 0
    
    # If file was truncated/recreated, reset offset
    if current_size < offset:
        offset = 0

    start_t = time.time()
    while time.time() - start_t < timeout_sec:
        if FOOTPRINT_SIGNALS_JSONL.exists():
            current_size = FOOTPRINT_SIGNALS_JSONL.stat().st_size
            if current_size > offset:
                # Read new signals
                with open(FOOTPRINT_SIGNALS_JSONL, "r", encoding="utf-8") as f:
                    f.seek(offset)
                    lines = f.readlines()
                    new_offset = f.tell()

                save_offset(new_offset)
                
                # Parse the last valid signal from the newly arrived lines
                for line in reversed(lines):
                    line_str = line.strip()
                    if line_str:
                        try:
                            sig = json.loads(line_str)
                            return sig
                        except Exception:
                            continue
        time.sleep(0.5)
    return None

def main():
    offset = load_offset()
    # Check if there's already an unread signal waiting
    current_size = FOOTPRINT_SIGNALS_JSONL.stat().st_size if FOOTPRINT_SIGNALS_JSONL.exists() else 0
    
    sig = None
    if current_size > offset:
        with open(FOOTPRINT_SIGNALS_JSONL, "r", encoding="utf-8") as f:
            f.seek(offset)
            lines = f.readlines()
            new_offset = f.tell()
        save_offset(new_offset)
        for line in reversed(lines):
            line_str = line.strip()
            if line_str:
                try:
                    sig = json.loads(line_str)
                    break
                except Exception:
                    continue
    
    if not sig:
        print("[AGENT_SENTINEL] Listening for next live market signal...", file=sys.stderr)
        sig = wait_for_signal(timeout_sec=300.0)

    if not sig:
        print("[AGENT_SENTINEL] No signal received within timeout window (300s). Exiting for cycle.")
        sys.exit(0)

    # Telemetry enrichment
    telemetry = fetch_telemetry(sig)

    print("=" * 80)
    print("🚀 [AGENTIC SIGNAL WAKE-UP PACKET DELIVERED]")
    print("=" * 80)
    print(f"Asset:          {telemetry.get('symbol')} ({telemetry.get('direction')})")
    print(f"Bar Type:       {telemetry.get('bar_type')} [{telemetry.get('bar_label')}] (Closed: {telemetry.get('bar_closed')})")
    print(f"Signal Code:    {sig.get('sig_code')} - {sig.get('sig_name')}")
    print(f"Signal Price:   ${telemetry.get('price')}")
    print(f"Headline:       {sig.get('title')} - {sig.get('description')}")
    print(f"Delta Listed:   {telemetry.get('is_listed_on_delta')} (Product: {telemetry.get('delta_product')})")
    
    ticker = telemetry.get("delta_ticker", {})
    if ticker:
        print(f"Live Mark Px:   ${ticker.get('mark_price')} | Spread: {ticker.get('spread_bps')} bps (Gate <= 45 bps: {ticker.get('spread_gate_pass')})")
    
    eval_res = telemetry.get("confluence_evaluation", {})
    if eval_res:
        print(f"Confluence:     {eval_res.get('confluence_score')}/100 | Hurdle: {eval_res.get('execution_hurdle')} | Actionable: {eval_res.get('is_actionable')}")
        print(f"Exec Model:     {eval_res.get('execution_model')}")
        levels = eval_res.get("trade_levels", {})
        if levels:
            print(f"Geometry:       Entry=${levels.get('entry_price')} | SL=${levels.get('stop_loss')} | TP1=${levels.get('take_profit_1')} | R:R={levels.get('rr_ratio')}:1")
        print("\nConfluence Reasons:")
        for r in eval_res.get("reasons", []):
            print(f"  • {r}")
    
    print("\n--- COMPLETE JSON TELEMETRY ---")
    print(json.dumps(telemetry, indent=2, default=str))
    print("=" * 80)
    print("ACTION REQUIRED: Perform agentic quantitative analysis and formulate trade execution.")
    print("=" * 80)

if __name__ == "__main__":
    main()
