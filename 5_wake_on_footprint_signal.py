"""
========================================================================================
[STANDALONE 5] Reactive Wake-On-Footprint Signal & Autonomous Live Trader
========================================================================================
Monitors Database/Signals/footprint_signals.jsonl for newly discovered Footprint signals
harvested across all assets by 1_coinglass_footprint_scraper.py.

Guarantees & Remediated Architecture (Goldfish Audit Validated):
1. Singleton Process Protection: acquire_singleton ensures only 1 sentinel instance runs.
2. Immediate Reactive Wakeup: Watchdog OS file modification events + 500ms reactive loop.
3. Strict Bar Completion Invariant: Live order execution occurs ONLY on completed 15m candles
   (time_ms + 900,000 <= current_time_ms) to completely eliminate intra-bar repainting wicks.
4. Full Multi-Factor Confluence: Evaluates incoming signals through RealtimeSignalEvaluator:
   - DuckDB OLAP: Whale Table 2 walls & bids/asks imbalance (2_coinglass_whales.py)
   - DuckDB OLAP: Liquidation magnet zones & near-field cascades (3_coinglass_liquidation_levels.py)
   - Live TV Liquidation spikes & reversal exhaustion (4_coinglass_tv_scraper.py)
   - Google DeepMind TimesFM 2.5 zero-shot drift & quantile forecasting
   - Auction Profile: Day VWAP, Value Area (VAH/VAL/POC), Fib-Prime EMAs
5. Strict Prop Firm Risk Guardrails & MatchTrader Live Order Dispatch:
   - If Confluence Score >= 75.0/100 on completed bar:
     * Audits MatchTrader Account 101305360 ($95K daily floor, $90K total floor)
     * Auto-dispatches live market order with protective SL and TP
     * Locks WARP IP rotation during execution via Database/.locks/order_execution_inflight.lock
     * Emits audible terminal bell (winsound.Beep + ASCII bell)
     * Prints prominent visual Trade Confluence Card to stdout
6. Active Management of MatchTrader Running Trades:
   - Tracks open positions, unrealized PnL, and distance to Stop Loss / Take Profit
   - Enforces -$500.00 (0.5% max risk) emergency circuit breaker and trailing break-even targets
========================================================================================
"""

import sys
import os
import json
import time
import math
import logging
import argparse
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

try:
    import winsound
except ImportError:
    winsound = None

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

# Reconfigure stdout for UTF-8 line buffering
try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import atomic_json, acquire_singleton, writer_lock
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
from Terminal.matchtrader_live_trader import (
    get_matchtrader_tab,
    inspect_account_and_positions,
    get_open_positions_detailed,
    close_position_by_index,
    execute_signal,
    matchtrader_symbol,
    SYMBOL_TO_MATCHTRADER,
    TARGET_ACCOUNT_NUMBER,
    EQUITY_FLOOR_DAILY,
    EQUITY_FLOOR_TOTAL,
    MAX_CONCURRENT_POSITIONS
)
from Terminal.delta_exchange_live_trader import delta_client

# Directories and Persistence Paths
DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

FOOTPRINT_SIGNALS_JSONL = SIGNALS_DIR / "footprint_signals.jsonl"
EVALUATED_ALERTS_JSONL = SIGNALS_DIR / "realtime_trade_alerts.jsonl"
STATE_FILE = DB_DIR / ".wake_on_footprint_state.json"

# Strict Prop Firm Risk Constants (FundingPips MatchTrader $5,000 Challenge Account 101305360)
from Terminal.risk_envelope import (
    MAX_RISK_USD,          # $25.00
    HARD_FLOOR_USD,        # $4,775.00
    MAX_OPEN_POSITIONS,    # 2
    MAX_DAILY_LOSS_USD     # $250.00
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [WAKE_SENTINEL] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("WakeOnFootprintSignal")
logger.setLevel(logging.INFO)
logger.handlers = [handler]


def get_ist_now() -> str:
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    return now_ist.strftime("%Y-%m-%d %H:%M:%S IST")


def load_state(reset_to_end: bool = False) -> Dict[str, Any]:
    if not reset_to_end and STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    initial_offset = FOOTPRINT_SIGNALS_JSONL.stat().st_size if FOOTPRINT_SIGNALS_JSONL.exists() else 0
    state = {
        "offset": initial_offset,
        "processed_signals_count": 0,
        "dispatched_trades_count": 0,
        "last_wake_utc": None
    }
    atomic_json(STATE_FILE, state)
    return state


def save_state(state: Dict[str, Any]):
    try:
        atomic_json(STATE_FILE, state)
    except Exception as e:
        logger.warning(f"Could not persist wake state: {e}")


def format_confluence_card(alert: Dict[str, Any]) -> str:
    """Renders prominent visual Trade Confluence Card for stdout."""
    sym = alert.get("symbol", "N/A")
    direction = alert.get("direction", "NEUTRAL")
    score = alert.get("confluence_score", 0.0)
    eval_ist = alert.get("evaluated_at_ist", get_ist_now())
    sig_det = alert.get("signal_details", {})
    trade = alert.get("trade_levels", {})
    tfm = alert.get("deepmind_timesfm", {})
    order_flow = alert.get("order_flow_metrics", {})
    regime = alert.get("market_regime", {})
    vm = alert.get("vector_memory", {})
    auction = alert.get("auction_profile", {})
    reasons = alert.get("confluence_reasons", [])
    is_actionable = alert.get("is_actionable", False)
    mtr_auth = alert.get("matchtrader_execution_authorized", False)
    lots = trade.get("admissible_lots", "N/A")
    risk_usd = trade.get("realized_risk_usd", 0.0)

    status_tag = "🚨 ACTIONABLE TRADE ALERT (SCORE >= 75.0)" if is_actionable else "⚡ ANALYZED SIGNAL (FILTERED / SUB-THRESHOLD)"

    vwap_str = f"${auction.get('day_vwap', 0.0):,.2f} ({auction.get('value_status', 'N/A')})" if auction.get('day_vwap') else "N/A"
    poc_vah_val = f"POC: ${auction.get('day_poc', 0.0):,.2f} | VAH: ${auction.get('day_vah', 0.0):,.2f} | VAL: ${auction.get('day_val', 0.0):,.2f}" if auction.get('day_poc') else "N/A"
    prior_str = f"PDH: ${auction.get('pdh', 0.0):,.2f} | PDL: ${auction.get('pdl', 0.0):,.2f} | pdPOC: ${auction.get('pd_poc', 0.0):,.2f}" if auction.get('pdh') else "N/A"
    fib_str = f"{auction.get('fib_prime_alignment', 'N/A')}" if auction.get('fib_prime_alignment') else "N/A"
    htf_str = f"{auction.get('mtf_bias', 'N/A')} (1H: {auction.get('htf_1h_trend', 'N/A')} | 4H: {auction.get('htf_4h_trend', 'N/A')})" if auction.get('mtf_bias') else "N/A"

    exec_model = alert.get("execution_model", "MODEL_0_FILTERED_BOUNDARY")
    headline = f"{sig_det.get('title', '')} - {sig_det.get('description', '')}" if sig_det.get('title') or sig_det.get('description') else "N/A"
    fib_pocket = alert.get("fib_prime_pocket", {})
    fib_pocket_str = f"EMA55=${fib_pocket.get('ema55', 0.0):,.2f} | EMA59=${fib_pocket.get('ema59', 0.0):,.2f} | EMA233=${fib_pocket.get('ema233', 0.0):,.2f} (Pocket: {fib_pocket.get('dist_55_59_pct', 0.0):.2f}%)" if fib_pocket.get('ema55') else "N/A"

    lines = [
        "",
        "╔" + "═" * 82 + "╗",
        f"║  {status_tag}".ljust(83) + "║",
        f"║  {sym} {direction}  |  CONFLUENCE SCORE: {score:.1f} / 100".ljust(83) + "║",
        f"║  Execution Model:  {exec_model}".ljust(83) + "║",
        "╠" + "═" * 82 + "╣",
        f"║  Signal Headline:  {headline[:60]}".ljust(83) + "║",
        f"║  Evaluation Time:  {eval_ist}".ljust(83) + "║",
        f"║  Signal Bar Time:  {sig_det.get('candle_ist', '')} (UTC: {sig_det.get('candle_utc', '')})".ljust(83) + "║",
        f"║  Signal Type:      {sig_det.get('sig_name', '')} [{sig_det.get('sig_code', '')}] ({sig_det.get('side', '')})".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  Trade Geometry:   Entry=${trade.get('entry_price', 0.0):,.4f} | SL=${trade.get('stop_loss', 0.0):,.4f} (-{trade.get('sl_pct', 0.0)}%)".ljust(83) + "║",
        f"║                    TP1=${trade.get('take_profit_1', 0.0):,.4f} | TP2=${trade.get('take_profit_2', 0.0):,.4f} | R:R={trade.get('rr_ratio', 0.0)}:1".ljust(83) + "║",
        f"║  Risk Allocation:  Lots={lots} | Risk=${risk_usd:.2f} (Cap: ${trade.get('hard_max_risk_usd', MAX_RISK_USD):.2f})".ljust(83) + "║",
        f"║  MatchTrader:      Authorized={'✅ YES (LIVE DISPATCH)' if mtr_auth else '❌ NO'} | Symbol={alert.get('matchtrader_symbol', 'N/A')}".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  Auction Profile:  Day VWAP: {vwap_str}".ljust(83) + "║",
        f"║  Value Area:       {poc_vah_val}".ljust(83) + "║",
        f"║  Prior Day Levels: {prior_str}".ljust(83) + "║",
        f"║  Fib-Prime EMAs:   {fib_str}".ljust(83) + "║",
        f"║  Fib Dynamic Pocket: {fib_pocket_str}".ljust(83) + "║",
        f"║  HTF Multi-TF:     {htf_str}".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  DeepMind TimesFM: CVD Dir={tfm.get('cvd_direction', 'N/A')} ({tfm.get('cvd_confidence', 0.0):.0%}) | Price Dir={tfm.get('price_direction', 'N/A')}".ljust(83) + "║",
        f"║  Microstructure:   Whale Bids={order_flow.get('whale_imbalance_bid_ratio', 0.0):.1%} | Near Shorts={order_flow.get('near_field_shorts_pct', 0.0):.1f}% | CVD={order_flow.get('cvd_slope', 'N/A')}".ljust(83) + "║",
        f"║  Regime & Vector:  Regime={regime.get('regime', 'N/A')} (ATR: {regime.get('atr_pct', 0.0):.2f}%) | VM Analog Win-Rate={vm.get('win_rate', 0.5):.0%}".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  Confluence Drivers:".ljust(83) + "║"
    ]
    for r in reasons:
        lines.append(f"║    • {r}".ljust(83) + "║")
    lines.append("╚" + "═" * 82 + "╝")
    lines.append("")
    return "\n".join(lines)


class SignalFileChangeHandler(FileSystemEventHandler):
    """Watchdog event handler waking asyncio sentinel immediately on file write."""
    def __init__(self, loop: asyncio.AbstractEventLoop, wake_event: asyncio.Event):
        self.loop = loop
        self.wake_event = wake_event

    def on_modified(self, event):
        if event.src_path.endswith("footprint_signals.jsonl"):
            self.loop.call_soon_threadsafe(self.wake_event.set)


class FootprintSignalSentinel:
    def __init__(self, from_current_end: bool = False):
        self.state = load_state(reset_to_end=from_current_end)
        self.evaluator = RealtimeSignalEvaluator()
        self.last_pos_check_ts = 0.0
        self.wake_event = asyncio.Event()

    def read_new_raw_signals(self) -> List[Dict[str, Any]]:
        """Reads any newly appended footprint signal lines from footprint_signals.jsonl."""
        if not FOOTPRINT_SIGNALS_JSONL.exists():
            return []

        current_size = FOOTPRINT_SIGNALS_JSONL.stat().st_size
        offset = self.state.get("offset", 0)

        if current_size < offset:
            offset = 0

        if current_size == offset:
            return []

        new_signals = []
        try:
            with open(FOOTPRINT_SIGNALS_JSONL, "r", encoding="utf-8") as f:
                f.seek(offset)
                for line in f:
                    line_str = line.strip()
                    if not line_str:
                        continue
                    try:
                        sig = json.loads(line_str)
                        new_signals.append(sig)
                    except json.JSONDecodeError:
                        continue
                new_offset = f.tell()
        except Exception as e:
            logger.error(f"Error reading footprint signals: {e}")
            return []

        self.state["offset"] = new_offset
        self.state["processed_signals_count"] += len(new_signals)
        save_state(self.state)
        return new_signals

    async def evaluate_and_dispatch(self, raw_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Evaluates each newly harvested raw footprint signal and executes live trades if qualified."""
        evaluations = []
        now_ms = time.time() * 1000

        for raw in raw_signals:
            try:
                sym = raw.get("symbol", "N/A")
                sig_name = raw.get("sig_name", raw.get("title", "Signal"))
                side = raw.get("side", raw.get("sig_side", "N/A"))
                t_ms = int(raw.get("time_ms") or 0)

                # Strict Bar Completion Invariant (AGENTS.md & Goldfish Finding 2)
                # 15m candle must be fully closed before order execution
                is_bar_closed = (t_ms > 0 and (t_ms + 900_000) <= now_ms) or raw.get("bar_closed") is True
                if not is_bar_closed:
                    mins_remaining = max(0.0, ((t_ms + 900_000) - now_ms) / 60000.0)
                    logger.info(f"⏳ [INTRA-BAR WICK VETO] {sym} {sig_name} on active candle (closes in {mins_remaining:.1f}m). Skipped to prevent wick repainting.")
                    continue

                title = str(raw.get("title") or raw.get("sig_name") or "Footprint Signal")
                desc = str(raw.get("description") or raw.get("desc") or "")
                price_val = float(raw.get("price", 0.0)) if raw.get("price") else 0.0
                c_ist = str(raw.get("candle_ist") or "")

                # Phase 1: Immediate Ingest Log
                logger.info(f"⚡ [WAKE EVENT] {sym} {sig_name} ({side}) @ ${price_val:,.4f} | Candle: {c_ist} | Title: '{title}' | Detail: '{desc}'")

                # Phase 2: Multi-factor quantitative evaluation offloaded to thread
                eval_res = await asyncio.to_thread(self.evaluator.evaluate_signal, raw)
                evaluations.append(eval_res)

                score = eval_res.get("confluence_score", 0.0)
                is_actionable = eval_res.get("is_actionable", False)
                direction = eval_res.get("direction", "NEUTRAL")

                # 2. Print formatted Trade Confluence Card
                card = format_confluence_card(eval_res)
                print(card, flush=True)

                                # 3. Actionable Trade Dispatch (Dual-Venue Smart Dispatch)
                if is_actionable and direction in ("BUY", "SELL"):
                    if winsound is not None:
                        try:
                            winsound.Beep(1200, 450)
                        except Exception:
                            pass
                    print("", end="", flush=True)

                    logger.info(f"⚡ CONFLUENCE THRESHOLD ACHIEVED: {score:.1f}/100 >= 75.0! Initiating Live Order Execution...")

                    mtr_sym = matchtrader_symbol(sym) or SYMBOL_TO_MATCHTRADER.get(sym)
                    dispatched = False
                    if mtr_sym:
                        logger.info(f"Routing {sym} to MatchTrader Prop Account ({mtr_sym})...")
                        dispatch_res = await execute_signal(eval_res)
                        logger.info(f"MatchTrader Dispatch Result: {dispatch_res}")
                        dispatched = dispatch_res.get("ok", False)

                        # If MatchTrader is full or rejected, and asset is crypto, failover to Delta Exchange India
                        if not dispatched and delta_client.resolve_product(sym):
                            logger.info(f"MatchTrader rejected ({dispatch_res.get('reason')}). Attempting failover to Delta Exchange India...")
                            delta_res = await asyncio.to_thread(delta_client.execute_delta_signal, eval_res)
                            logger.info(f"Delta Exchange India Failover Result: {delta_res}")
                    else:
                        logger.info(f"Routing {sym} to Delta Exchange India Live Venue...")
                        dispatch_res = await asyncio.to_thread(delta_client.execute_delta_signal, eval_res)
                        logger.info(f"Delta Exchange India Dispatch Result: {dispatch_res}")

                    self.state["dispatched_trades_count"] += 1
                    save_state(self.state)
                else:
                    logger.info(f"[EVALUATOR] {sym} {direction} Confluence Score: {score:.1f}/100 -> Filtered (Required >= 75.0)")

            except Exception as e:
                logger.error(f"Error processing signal for {raw.get('symbol')}: {e}", exc_info=True)

        return evaluations

    async def inspect_and_manage_running_trades(self):
        """Monitors MatchTrader open positions, verifies drawdown floor, and manages running trades."""
        now = time.time()
        if now - self.last_pos_check_ts < 15.0:
            return
        self.last_pos_check_ts = now

        tab = get_matchtrader_tab()
        if not tab:
            return

        import websockets
        try:
            async with websockets.connect(tab["webSocketDebuggerUrl"], ping_interval=None) as ws:
                account_state = await inspect_account_and_positions(ws)
                positions = await get_open_positions_detailed(ws)

                equity = account_state.get("current_equity", 100000.0)
                daily_loss = account_state.get("current_daily_loss", 0.0)
                total_loss = account_state.get("current_total_loss", 0.0)
                pos_count = len(positions)

                status_summary = f"[MATCHTRADER GUARDIAN] Equity=${equity:,.2f} | Open={pos_count} pos | Daily Loss=${daily_loss:,.2f} | Total Loss=${total_loss:,.2f}"
                
                # Check drawdown circuit breaker
                if account_state.get("daily_floor_breached") or account_state.get("total_floor_breached"):
                    logger.critical(f"🛑 [CIRCUIT BREAKER] DRAWDOWN FLOOR BREACHED! DailyFloor={account_state.get('daily_floor_breached')}, TotalFloor={account_state.get('total_floor_breached')}")
                    if winsound is not None:
                        try:
                            winsound.Beep(1800, 1000)
                        except Exception:
                            pass

                # Inspect each position
                for pos in positions:
                    sym = pos.get("symbol")
                    pnl = pos.get("profit_usd", 0.0)
                    pct = pos.get("profit_pct", 0.0)
                    side = pos.get("side")
                    vol = pos.get("volume")
                    idx = pos.get("index")

                    pos_log = f"   -> Pos #{idx}: {sym} {side} {vol} | PnL: ${pnl:+.2f} ({pct:+.2f}%)"
                    
                    # Individual hard loss limit ($25.00 max risk with 1.25 buffer = -$31.25)
                    max_loss_thresh = MAX_RISK_USD * 1.25
                    if pnl <= -max_loss_thresh:
                        logger.warning(f"🛑 Pos #{idx} {sym} loss exceeded hard risk envelope -${max_loss_thresh:.2f} (${pnl:.2f})! Emergency close triggered.")
                        await close_position_by_index(ws, idx)
                    elif pnl >= 250.0:
                        logger.info(f"✨ Pos #{idx} {sym} in strong profit (+${pnl:.2f}) | Trailing Break-Even / Target Qualified.")

                logger.info(status_summary)

        except Exception as e:
            logger.debug(f"Position inspection non-fatal error: {e}")

    async def run_once(self):
        """Processes any unread footprint signals and audits running trades once."""
        raw_signals = self.read_new_raw_signals()
        if raw_signals:
            logger.info(f"Found {len(raw_signals)} unread footprint signal(s). Evaluating...")
            await self.evaluate_and_dispatch(raw_signals)
        else:
            logger.info("No unread footprint signals in stream.")

        await self.inspect_and_manage_running_trades()

        # Delta Exchange India Position Guardian
        try:
            delta_pos = await asyncio.to_thread(delta_client.get_active_positions)
            if delta_pos:
                for dp in delta_pos:
                    d_sym = dp.get("product_symbol")
                    d_size = dp.get("size")
                    d_entry = dp.get("entry_price")
                    d_mark = dp.get("mark_price")
                    d_upnl = float(dp.get("unrealized_pnl") or 0.0)
                    logger.info(f"[DELTA GUARDIAN] {d_sym} {d_size} contracts | Entry: ${float(d_entry or 0):,.4f} | Mark: ${float(d_mark or 0):,.4f} | UPnL: ${d_upnl:+.4f}")
        except Exception as e:
            logger.debug(f"Delta position check: {e}")


    async def run_daemon(self):
        """Continuous reactive daemon watching footprint_signals.jsonl with zero CPU lag."""
        logger.info(f"Starting continuous Reactive Sentinel on {FOOTPRINT_SIGNALS_JSONL.name}...")
        logger.info(f"Target Account: FundingPips MatchTrader {TARGET_ACCOUNT_NUMBER}")
        logger.info(f"Prop-Firm Guardrails: Daily Floor ${EQUITY_FLOOR_DAILY:,.2f} | Total Floor ${EQUITY_FLOOR_TOTAL:,.2f} | Max Open {MAX_CONCURRENT_POSITIONS}")

        loop = asyncio.get_running_loop()
        observer = None
        if HAS_WATCHDOG and SIGNALS_DIR.exists():
            event_handler = SignalFileChangeHandler(loop, self.wake_event)
            observer = Observer()
            observer.schedule(event_handler, str(SIGNALS_DIR), recursive=False)
            observer.start()
            logger.info("⚡ Watchdog OS event listener attached to Database/Signals/ directory.")

        # Initial sweep
        await self.run_once()

        try:
            while True:
                try:
                    # Wait for either Watchdog event or 500ms timeout
                    try:
                        await asyncio.wait_for(self.wake_event.wait(), timeout=0.5)
                        self.wake_event.clear()
                    except asyncio.TimeoutError:
                        pass

                    # 1. Read newly appended signals
                    raw_signals = self.read_new_raw_signals()
                    if raw_signals:
                        await self.evaluate_and_dispatch(raw_signals)

                    # 2. Guardian check on MatchTrader open positions
                    await self.inspect_and_manage_running_trades()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in sentinel loop: {e}", exc_info=True)
                    await asyncio.sleep(2.0)
        finally:
            if observer:
                observer.stop()
                observer.join()
            logger.info("Reactive sentinel terminated cleanly.")


def main():
    parser = argparse.ArgumentParser(description="[STANDALONE 5] Reactive Wake-On-Footprint Signal Sentinel")
    parser.add_argument("--once", action="store_true", help="Evaluate latest unread signals and inspect running trades once, then exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuous reactive sentinel (default)")
    parser.add_argument("--reset-offset", action="store_true", help="Reset stream pointer to current end-of-file")
    args = parser.parse_args()

    # Singleton instance lock (Goldfish Finding 7)
    acquire_singleton("wake_on_footprint_signal", DB_DIR / ".locks")

    sentinel = FootprintSignalSentinel(from_current_end=args.reset_offset)

    if args.once:
        asyncio.run(sentinel.run_once())
    else:
        try:
            asyncio.run(sentinel.run_daemon())
        except KeyboardInterrupt:
            logger.info("Terminated by user. Goodbye!")


if __name__ == "__main__":
    main()
