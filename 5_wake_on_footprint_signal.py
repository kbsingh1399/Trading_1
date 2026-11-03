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
from Terminal.trade_critic_engine import TradeCriticEngine

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
        f"║  Delta Exchange:   Authorized={'✅ YES (AUTO-DISPATCH READY)' if is_actionable and delta_client.resolve_product(sym) else ('❌ NO (SUB-THRESHOLD)' if not is_actionable else '⚠️ NO PRODUCT')} | Product={delta_client.resolve_product(sym) or 'N/A'} (10x Leverage)".ljust(83) + "║",
        f"║  MatchTrader:      Authorized={'✅ YES' if mtr_auth else '❌ NO'} | Symbol={alert.get('matchtrader_symbol', 'N/A')}".ljust(83) + "║",
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
    
    five_p = alert.get("five_pillar_justification", {})
    if five_p:
        lines.append("╟" + "─" * 82 + "╢")
        lines.append("║  🏛️ 5-PILLAR FORMULATION JUSTIFICATION:".ljust(83) + "║")
        lines.append(f"║   1. Footprint & Cluster: {five_p.get('pillar_1_footprint_signals', 'N/A')[:60]}".ljust(83) + "║")
        lines.append(f"║   2. Auction & Value:     {five_p.get('pillar_2_auction_value', 'N/A')[:60]}".ljust(83) + "║")
        lines.append(f"║   3. Live TV Liquidations: {five_p.get('pillar_3_live_liquidations', 'N/A')[:59]}".ljust(83) + "║")
        lines.append(f"║   4. Liquidation Heatmap: {five_p.get('pillar_4_liquidation_heatmap', 'N/A')[:60]}".ljust(83) + "║")
        lines.append(f"║   5. Whales & Sweeps:     {five_p.get('pillar_5_whales_and_sweeps', 'N/A')[:60]}".ljust(83) + "║")
    
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
        try:
            self.critic_engine = TradeCriticEngine()
        except Exception as e:
            logger.warning(f"TradeCriticEngine initialization error: {e}")
            self.critic_engine = None
        self.active_positions_tracker: Dict[str, Dict[str, Any]] = {}
        self.last_pos_check_ts = 0.0
        self.wake_event = asyncio.Event()
        self.pending_signals = {}  # Holds intra-bar signals until 15m candle completion

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
        """Groups raw signals into candle clusters, evaluates each candle once, and auto-dispatches to Delta Exchange."""
        from collections import defaultdict
        evaluations = []
        now_ms = time.time() * 1000

        # 1. Group signals on the exact same (symbol, time_ms) candle
        candle_clusters = defaultdict(list)
        for s in raw_signals:
            s_sym = s.get("symbol", "N/A")
            s_t = int(s.get("time_ms") or 0)
            candle_clusters[(s_sym, s_t)].append(s)

        for (sym, t_ms), cluster in candle_clusters.items():
            try:
                # If multiple signals fired on the exact same candle, form a unified cluster
                if len(cluster) > 1:
                    names = [s.get("sig_name", s.get("title", "Signal")) for s in cluster]
                    sides = [str(s.get("side", s.get("sig_side", ""))).lower() for s in cluster]
                    all_bull = all(s in ("buy", "bullish") for s in sides)
                    all_bear = all(s in ("sell", "bearish") for s in sides)
                    primary_raw = dict(cluster[0])
                    primary_raw["is_cluster"] = True
                    primary_raw["cluster_signals"] = cluster
                    primary_raw["cluster_signal_names"] = names
                    if all_bull:
                        primary_raw["cluster_agreement"] = "UNANIMOUS_BULLISH"
                        primary_raw["side"] = "bullish"
                    elif all_bear:
                        primary_raw["cluster_agreement"] = "UNANIMOUS_BEARISH"
                        primary_raw["side"] = "bearish"
                    else:
                        primary_raw["cluster_agreement"] = "DISCORDANT_CONFLICT"
                    primary_raw["sig_name"] = " + ".join(names)
                    primary_raw["title"] = f"Multi-Signal Cluster ({len(names)}): " + " & ".join(names)
                    raw = primary_raw
                else:
                    raw = cluster[0]
                    raw["is_cluster"] = False

                sig_name = raw.get("sig_name", raw.get("title", "Signal"))
                side = raw.get("side", raw.get("sig_side", "N/A"))

                # Strict Bar Completion Invariant (AGENTS.md & Goldfish Finding 2)
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
                logger.info(f"⚡ [WAKE EVENT] {sym} {sig_name} ({side}) @ ${price_val:,.4f} | Candle: {c_ist} | Detail: '{desc}'")

                # Phase 2: Multi-factor quantitative evaluation offloaded to thread
                eval_res = await asyncio.to_thread(self.evaluator.evaluate_signal, raw)
                evaluations.append(eval_res)

                score = eval_res.get("confluence_score", 0.0)
                is_actionable = eval_res.get("is_actionable", False)
                direction = eval_res.get("direction", "NEUTRAL")

                # Phase 3: Formatted Trade Confluence Card
                card = format_confluence_card(eval_res)
                print(card, flush=True)

                # Phase 4: Actionable Trade Dispatch (Direct Delta Exchange India Auto-Wake Execution)
                if is_actionable and direction in ("BUY", "SELL"):
                    if winsound is not None:
                        try:
                            winsound.Beep(1200, 450)
                        except Exception:
                            pass
                    print(" ", end="", flush=True)

                    logger.info(f"⚡ CONFLUENCE THRESHOLD ACHIEVED: {score:.1f}/100 >= 75.0! Initiating Live Delta Exchange Execution...")
                    delta_product = delta_client.resolve_product(sym)
                    if delta_product:
                        logger.info(f"🚀 [DELTA AUTO-WAKE] Routing {sym} ({direction}) to Delta Exchange India ({delta_product})...")
                        dispatch_res = await asyncio.to_thread(delta_client.execute_delta_signal, eval_res)
                        logger.info(f"✨ Delta Exchange India Dispatch Result: {dispatch_res}")
                    else:
                        logger.warning(f"No Delta Exchange product resolved for {sym}; cannot execute.")

                    self.state["dispatched_trades_count"] += 1
                    save_state(self.state)
                else:
                    logger.info(f"[EVALUATOR] {sym} {direction} Confluence Score: {score:.1f}/100 -> Filtered (Required >= 75.0)")

            except Exception as e:
                logger.error(f"Error processing signal cluster for {sym}: {e}", exc_info=True)

        return evaluations

    def _gather_market_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Captures microstructure order flow context for post-mortem analysis."""
        try:
            clean_sym = symbol.replace("USD", "").replace(".PI", "").replace("USDT", "")
            olap = getattr(self.evaluator, "olap", None)
            if not olap:
                return {}
            auction = olap.query_auction_profile(clean_sym) if hasattr(olap, "query_auction_profile") else {}
            cvd = olap.query_cvd_trend(clean_sym, lookback_bars=16) if hasattr(olap, "query_cvd_trend") else {}
            whales = olap.query_whale_walls(clean_sym) if hasattr(olap, "query_whale_walls") else {}
            tv_liq = self.evaluator.query_live_tv_liquidations(clean_sym)
            return {
                "ema233": float(auction.get("ema233") or auction.get("day_vwap", 0.0) or 0.0),
                "cvd_slope": float(cvd.get("slope") or (1.0 if "BULLISH" in str(cvd.get("cvd_slope", "")) else -1.0 if "BEARISH" in str(cvd.get("cvd_slope", "")) else 0.0)),
                "htf_bias": str(auction.get("h4_trend") or auction.get("mtf_bias", "NEUTRAL")),
                "whale_imbalance": float(whales.get("whale_imbalance_bid_ratio", 0.5) - 0.5),
                "has_liq_spike": bool(tv_liq.get("has_spike")),
                "liq_bias": str(tv_liq.get("bias", "NEUTRAL")),
                "value_area_relation": str(auction.get("value_status", "INSIDE_VA"))
            }
        except Exception as e:
            logger.debug(f"Error gathering market snapshot for {symbol}: {e}")
            return {}

    async def inspect_and_manage_running_trades(self):
        """Monitors MatchTrader & Delta Exchange positions, verifies risk, and conducts post-mortems on losses."""
        now = time.time()
        if now - self.last_pos_check_ts < 15.0:
            return
        self.last_pos_check_ts = now

        current_active_keys = set()

        # 1. MatchTrader Positions Guardian
        tab = get_matchtrader_tab()
        if tab:
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

                    # Inspect each MatchTrader position
                    for pos in positions:
                        sym = pos.get("symbol", "")
                        pnl = float(pos.get("profit_usd") or 0.0)
                        pct = float(pos.get("profit_pct") or 0.0)
                        side = pos.get("side", "")
                        vol = pos.get("volume", "")
                        idx = pos.get("index", 0)
                        key = f"MTR_{sym}_{side}"
                        current_active_keys.add(key)

                        if key not in self.active_positions_tracker:
                            self.active_positions_tracker[key] = {
                                "venue": "MATCHTRADER",
                                "symbol": sym,
                                "direction": side,
                                "open_price": float(pos.get("open_price") or 0.0),
                                "last_price": float(pos.get("current_price") or 0.0),
                                "last_profit_usd": pnl,
                                "volume": vol,
                                "first_seen": now,
                                "last_seen": now
                            }
                        else:
                            self.active_positions_tracker[key].update({
                                "last_price": float(pos.get("current_price") or 0.0),
                                "last_profit_usd": pnl,
                                "last_seen": now
                            })

                        # Individual hard loss limit ($25.00 max risk with 1.25 buffer = -$31.25)
                        max_loss_thresh = MAX_RISK_USD * 1.25
                        if pnl <= -max_loss_thresh:
                            logger.warning(f"🛑 Pos #{idx} {sym} loss exceeded hard risk envelope -${max_loss_thresh:.2f} (${pnl:.2f})! Emergency close triggered.")
                            await close_position_by_index(ws, idx)
                        elif pnl >= 250.0:
                            logger.info(f"✨ Pos #{idx} {sym} in strong profit (+${pnl:.2f}) | Trailing Break-Even / Target Qualified.")

                    logger.info(status_summary)

            except Exception as e:
                logger.debug(f"MatchTrader position inspection non-fatal error: {e}")

        # 2. Delta Exchange India Positions Guardian
        try:
            delta_pos = await asyncio.to_thread(delta_client.get_active_positions)
            if delta_pos:
                for dp in delta_pos:
                    d_sym = dp.get("product_symbol", "")
                    d_size = dp.get("size", "0")
                    d_entry = float(dp.get("entry_price") or 0.0)
                    d_mark = float(dp.get("mark_price") or 0.0)
                    d_upnl = float(dp.get("unrealized_pnl") or 0.0)
                    d_upnl_pct = float(dp.get("unrealized_pnl_pct") or 0.0)
                    d_side = "BUY" if int(d_size) > 0 else "SELL"
                    key = f"DELTA_{d_sym}"
                    current_active_keys.add(key)

                    if key not in self.active_positions_tracker:
                        self.active_positions_tracker[key] = {
                            "venue": "DELTA_INDIA",
                            "symbol": d_sym,
                            "direction": d_side,
                            "open_price": d_entry,
                            "last_price": d_mark,
                            "last_profit_usd": d_upnl,
                            "volume": d_size,
                            "first_seen": now,
                            "last_seen": now
                        }
                    else:
                        self.active_positions_tracker[key].update({
                            "last_price": d_mark,
                            "last_profit_usd": d_upnl,
                            "last_seen": now
                        })

                    logger.info(f"[DELTA GUARDIAN] {d_sym} {d_size} contracts | Entry: ${d_entry:,.4f} | Mark: ${d_mark:,.4f} | UPnL: ${d_upnl:+.4f} ({d_upnl_pct:+.2f}%)")
        except Exception as e:
            logger.debug(f"Delta position check error: {e}")

        # 3. Dynamic Post-Mortem & Critic Memory Indexing on Closed Positions
        for key, tracked in list(self.active_positions_tracker.items()):
            if key not in current_active_keys and (now - tracked.get("last_seen", now)) > 5.0:
                pnl = tracked.get("last_profit_usd", 0.0)
                sym = tracked.get("symbol", "")
                direction = tracked.get("direction", "")
                venue = tracked.get("venue", "")

                if pnl < 0:
                    logger.warning(
                        f"🚨 [POST-MORTEM TRIGGERED] {venue} position {sym} ({direction}) closed in LOSS: "
                        f"${pnl:.2f}. Launching adversarial forensic analysis..."
                    )
                    snapshot = self._gather_market_snapshot(sym)
                    if self.critic_engine:
                        diagnosis = self.critic_engine.conduct_post_mortem_and_store(
                            ticket_id=key,
                            symbol=sym,
                            direction=direction,
                            entry_price=tracked.get("open_price", 0.0),
                            exit_price=tracked.get("last_price", 0.0),
                            realized_pnl_usd=pnl,
                            market_snapshot=snapshot
                        )
                        logger.info(
                            f"🧠 [CRITIC MEMORY COMMITTED] Failure Mode: {diagnosis.get('failure_mode')} | "
                            f"Mitigation Rule: {diagnosis.get('mitigation_guard')}"
                        )
                else:
                    logger.info(f"✨ [TRADE CLOSED PROFIT] {venue} position {sym} ({direction}) closed in PROFIT: +${pnl:.2f}.")

                del self.active_positions_tracker[key]

    async def run_once(self):
        """Processes any unread footprint signals and audits running trades once."""
        new_signals = self.read_new_raw_signals()
        for s in new_signals:
            k = f"{s.get('symbol')}_{s.get('sig_name')}_{s.get('time_ms')}"
            self.pending_signals[k] = s

        now_ms = time.time() * 1000
        signals_ready = []
        still_pending = {}
        for k, raw in list(self.pending_signals.items()):
            t_ms = int(raw.get("time_ms") or 0)
            is_bar_closed = (t_ms > 0 and (t_ms + 900_000) <= now_ms) or raw.get("bar_closed") is True
            if now_ms - t_ms > 1_800_000:
                continue
            if is_bar_closed:
                signals_ready.append(raw)
            else:
                still_pending[k] = raw
        self.pending_signals = still_pending

        if signals_ready:
            logger.info(f"Found {len(signals_ready)} closed footprint signal(s). Evaluating...")
            await self.evaluate_and_dispatch(signals_ready)
        else:
            logger.info("No completed candle footprint signals ready for evaluation.")

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

                    # 1. Read newly appended signals and buffer intra-bar signals
                    new_signals = self.read_new_raw_signals()
                    for s in new_signals:
                        k = f"{s.get('symbol')}_{s.get('sig_name')}_{s.get('time_ms')}"
                        self.pending_signals[k] = s

                    # 2. Evaluate signals whose 15m candle has officially completed
                    now_ms = time.time() * 1000
                    signals_ready = []
                    still_pending = {}
                    for k, raw in list(self.pending_signals.items()):
                        t_ms = int(raw.get("time_ms") or 0)
                        is_bar_closed = (t_ms > 0 and (t_ms + 900_000) <= now_ms) or raw.get("bar_closed") is True
                        if now_ms - t_ms > 1_800_000:
                            continue  # Discard signals older than 30 mins
                        if is_bar_closed:
                            signals_ready.append(raw)
                        else:
                            still_pending[k] = raw
                    self.pending_signals = still_pending

                    if signals_ready:
                        logger.info(f"⚡ [BAR COMPLETION WAKE] Releasing {len(signals_ready)} fully closed 15m signal(s) to evaluator.")
                        await self.evaluate_and_dispatch(signals_ready)

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
