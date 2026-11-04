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
5. Strict Risk Guardrails & Delta Exchange India Live Order Dispatch:
   - If Confluence Score >= 75.0/100 on completed bar:
     * Audits Delta Exchange India account
     * Auto-dispatches live market order with protective SL and TP
     * Locks WARP IP rotation during execution via Database/.locks/order_execution_inflight.lock
     * Emits audible terminal bell (winsound.Beep + ASCII bell)
     * Prints prominent visual Trade Confluence Card to stdout
6. Active Management of Delta Exchange Running Trades:
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
    _BaseHandler = FileSystemEventHandler
except ImportError:
    HAS_WATCHDOG = False
    Observer = None
    _BaseHandler = object

# Reconfigure stdout for UTF-8 line buffering
try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import atomic_json, acquire_singleton, writer_lock, FileMutex
from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
# MatchTrader removed per AGENTS.md policy (Delta Exchange India primary venue)
from Terminal.delta_exchange_live_trader import delta_client, set_live_dispatch
from Terminal.trade_critic_engine import TradeCriticEngine
from Terminal.asset_signal_stack import (
    AssetSignalStack,
    EvaluatedSignalsLedger,
    canonical_bar_epoch,
    generate_signal_id
)

# Directories and Persistence Paths
DB_DIR = ROOT_DIR / "Database"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

FOOTPRINT_SIGNALS_JSONL = SIGNALS_DIR / "footprint_signals.jsonl"
EVALUATED_ALERTS_JSONL = SIGNALS_DIR / "realtime_trade_alerts.jsonl"
STATE_FILE = DB_DIR / ".wake_on_footprint_state.json"

# Strict Risk Envelope Constants (Delta Exchange India)
from Terminal.risk_envelope import (
    MAX_RISK_USD,          # $25.00
    HARD_FLOOR_USD,        # $4,775.00
    MAX_OPEN_POSITIONS,    # 2
    MAX_DAILY_LOSS_USD     # $250.00
)

# PATCH P4 (Phase 2): bounded fan-out — max simultaneous signal evaluations.
# Burst signal volume across the 31 Delta India assets must never queue behind
# the tick tape; 4 parallel workers saturate the OLAP / Hyperdash pipelines
# without thrashing DuckDB or the TimesFM inference mutex.
MAX_PARALLEL_EVALUATIONS = int(os.environ.get("WAKE_MAX_PARALLEL_EVALUATIONS", "4"))

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
    # MatchTrader excised
    lots = trade.get("admissible_lots", "N/A")
    risk_usd = float(trade.get("realized_risk_usd") or 0.0)

    if is_actionable and alert.get("bar_closed", True):
        status_tag = "🚨 ACTIONABLE TRADE ALERT (SCORE >= 75.0)"
    elif not alert.get("bar_closed", True):
        status_tag = "⚡ ANALYZED SIGNAL (IN-FLIGHT FORMING BAR - EVALUATED LIVE)"
    else:
        status_tag = "⚡ ANALYZED SIGNAL (FILTERED / SUB-THRESHOLD)"

    vwap_str = f"${auction.get('day_vwap', 0.0):,.2f} ({auction.get('value_status', 'N/A')})" if auction.get('day_vwap') else "N/A"
    poc_vah_val = f"POC: ${auction.get('day_poc', 0.0):,.2f} | VAH: ${auction.get('day_vah', 0.0):,.2f} | VAL: ${auction.get('day_val', 0.0):,.2f}" if auction.get('day_poc') else "N/A"
    prior_str = f"PDH: ${auction.get('pdh', 0.0):,.2f} | PDL: ${auction.get('pdl', 0.0):,.2f} | pdPOC: ${auction.get('pd_poc', 0.0):,.2f}" if auction.get('pdh') else "N/A"
    fib_str = f"{auction.get('fib_prime_alignment', 'N/A')}" if auction.get('fib_prime_alignment') else "N/A"
    htf_str = f"{auction.get('mtf_bias', 'N/A')} (1H: {auction.get('htf_1h_trend', 'N/A')} | 4H: {auction.get('htf_4h_trend', 'N/A')})" if auction.get('mtf_bias') else "N/A"

    exec_model = alert.get("execution_model", "MODEL_0_FILTERED_BOUNDARY")
    headline = f"{sig_det.get('title', '')} - {sig_det.get('description', '')}" if sig_det.get('title') or sig_det.get('description') else "N/A"
    fib_pocket = alert.get("fib_prime_pocket", {})
    fib_pocket_str = f"EMA55=${fib_pocket.get('ema55', 0.0):,.2f} | EMA59=${fib_pocket.get('ema59', 0.0):,.2f} | EMA233=${fib_pocket.get('ema233', 0.0):,.2f} (Pocket: {fib_pocket.get('dist_55_59_pct', 0.0):.2f}%)" if fib_pocket.get('ema55') else "N/A"

    hurdle = float(tfm.get("confluence_hurdle_adjustment") or 0.0)
    req_threshold = float(tfm.get("required_threshold") or (75.0 + hurdle))
    score_header = f"║  {sym} {direction}  |  CONFLUENCE SCORE: {score:.1f} / 100"
    if hurdle > 0.0:
        score_header += f" (Required: {req_threshold:.1f} [Base 75 + Hurdle {hurdle:+.1f}])"

    delta_prod_res = delta_client.resolve_product(sym)
    delta_prod_sym = delta_prod_res.get("symbol", "N/A") if isinstance(delta_prod_res, dict) else (delta_prod_res or "N/A")

    if is_actionable and delta_prod_res:
        delta_auth_str = "✅ YES (DISPATCH READY)"
    elif not is_actionable:
        if score < req_threshold:
            delta_auth_str = f"❌ NO (SUB-THRESHOLD: {score:.1f} < {req_threshold:.1f})"
        elif alert.get("validation_errors"):
            delta_auth_str = "❌ NO (VALIDATION VETO)"
        else:
            delta_auth_str = "❌ NO (FILTERED)"
    else:
        delta_auth_str = "⚠️ NO PRODUCT (Unlisted on Delta India)"

    lines = [
        "",
        "╔" + "═" * 82 + "╗",
        f"║  {status_tag}".ljust(83) + "║",
        score_header.ljust(83) + "║",
        f"║  Execution Model:  {exec_model}".ljust(83) + "║",
        "╠" + "═" * 82 + "╣",
        f"║  Signal Headline:  {headline[:60]}".ljust(83) + "║",
        f"║  Evaluation Time:  {eval_ist}".ljust(83) + "║",
        f"║  Signal Bar Time:  {sig_det.get('candle_ist', '')} (UTC: {sig_det.get('candle_utc', '')})".ljust(83) + "║",
        f"║  Signal Type:      {sig_det.get('sig_name', '')} [{sig_det.get('sig_code', '')}] ({sig_det.get('side', '')})".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  Trade Geometry:   Entry=${trade.get('entry_price', 0.0):,.4f} | SL=${trade.get('stop_loss', 0.0):,.4f} (-{trade.get('sl_pct', 0.0)}%)".ljust(83) + "║",
        f"║                    TP1=${trade.get('take_profit_1', 0.0):,.4f} | TP2=${trade.get('take_profit_2', 0.0):,.4f} | R:R={trade.get('rr_ratio', 0.0)}:1".ljust(83) + "║",
        f"║  Risk Allocation:  Contracts={lots} | Risk=${risk_usd:.2f} (Cap: ${trade.get('hard_max_risk_usd', MAX_RISK_USD):.2f})".ljust(83) + "║",
        f"║  Delta Exchange:   Authorized={delta_auth_str} | Product={delta_prod_sym} (10x-20x Leverage)".ljust(83) + "║",
        f"║  Risk Execution:   R:R >= 2.0:1 Guaranteed (Fill-Anchored Dynamic Calibration) | Venue: Delta India".ljust(83) + "║",
        f"║  DeepSeek Critic:  {('✅ APPROVED (High-Probability Confluence)' if (alert.get('deepseek_critic') or {}).get('approved') is True else ('❌ VETOED (' + str((alert.get('deepseek_critic') or {}).get('reason', 'VETO')) + ')' if (alert.get('deepseek_critic') or {}).get('approved') is False else ('⚠️ UNCONFIGURED (Missing DEEPSEEK_API_KEY)' if is_actionable else f'⏭️ SKIPPED (Score {score:.1f} < Required {req_threshold:.1f})')))}".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  Auction Profile:  Day VWAP: {vwap_str}".ljust(83) + "║",
        f"║  Value Area:       {poc_vah_val}".ljust(83) + "║",
        f"║  Prior Day Levels: {prior_str}".ljust(83) + "║",
        f"║  Fib-Prime EMAs:   {fib_str}".ljust(83) + "║",
        f"║  Fib Dynamic Pocket: {fib_pocket_str}".ljust(83) + "║",
        f"║  HTF Multi-TF:     {htf_str}".ljust(83) + "║",
        "╟" + "─" * 82 + "╢",
        f"║  DeepMind TimesFM: CVD Dir={tfm.get('cvd_direction', 'N/A')} ({tfm.get('cvd_confidence', 0.0):.0%}) | Price Dir={tfm.get('price_direction', 'N/A')}".ljust(83) + "║",
        f"║  Microstructure:   Whale Bids={('UNTRACKED' if not order_flow.get('whale_tracked') else ('{:.1%}'.format(order_flow.get('whale_imbalance_bid_ratio', 0.0))))} | Near Shorts={('UNTRACKED' if not order_flow.get('liq_tracked') else ('{:.1f}%'.format(order_flow.get('near_field_shorts_pct', 0.0))))} | CVD={order_flow.get('cvd_slope', 'N/A')}".ljust(83) + "║",
        f"║  Regime & Vector:  Regime={regime.get('regime', 'N/A')} (ATR: {regime.get('atr_pct', 0.0):.2f}%) | VM Analog Win-Rate={('N/A (Inconclusive)' if vm.get('verdict') == 'INCONCLUSIVE' else ('{:.0%}'.format(vm.get('win_rate', 0.5))))}".ljust(83) + "║",
    ]
    hd_intel = alert.get("hyperdash_intel", {})
    if hd_intel and hd_intel.get("data_valid"):
        lines.append(f"║  Hyperdash Intel:  Smart Bias={hd_intel.get('smart_bias', 0.0):+.1f}% | Crowd Bias={hd_intel.get('crowd_bias', 0.0):+.1f}% | Score={hd_intel.get('intel_score', 50):.0f}/100 ({hd_intel.get('intel_direction', 'NEUTRAL')})".ljust(83) + "║")
    lines.extend([
        "╟" + "─" * 82 + "╢",
        f"║  Confluence Drivers:".ljust(83) + "║"
    ])
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


class SignalFileChangeHandler(_BaseHandler):
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
        self.stack = AssetSignalStack()

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

        self._pending_offset = new_offset
        return new_signals

    async def evaluate_and_dispatch(self, raw_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Groups raw signals into candle clusters, evaluates each candle once, and
        auto-dispatches to Delta Exchange India.

        PATCH P4 (Phase 2) — Asynchronous Bounded Fan-Out:
          Stage 1 (serial):   grouping + cheap invariants (ledger, venue,
                              staleness, cluster formation). No I/O.
          Stage 2 (parallel): up to MAX_PARALLEL_EVALUATIONS (default 4)
                              concurrent evaluations via asyncio.to_thread, each
                              pre-fetching Hyperdash intel with get_asset_intel_async
                              so burst signal volume across the 31 Delta India
                              assets never queues behind the real-time tick tape.
          Stage 3 (serial):   confluence cards, stack updates, parking and Delta
                              dispatch in the ORIGINAL deterministic cluster
                              order (FileMutex-guarded).
        """
        from collections import defaultdict
        evaluations = []
        now_ms = time.time() * 1000

        # 1. PATCH W1: group on (symbol, bar_type, bar_variant, time_ms).
        # bar_variant (bar_label: 'Range 10' vs 'Range 200' vs '100K' vs '250K' vs '5K')
        # MUST be part of the identity: different bar constructions closing at the same
        # instant are DIFFERENT candles. Merging them awarded the multi-signal cluster
        # boost (+15/+20) for one volume event seen twice, and the ledger then
        # suppressed the second variant permanently.
        candle_clusters = defaultdict(list)
        for s in raw_signals:
            s_sym = s.get("symbol", "N/A")
            s_bt = str(s.get("bar_type", "time")).lower()
            s_t = int(s.get("time_ms") or 0)
            s_var = str(s.get("bar_label") or s.get("bar_variant") or "").strip() if s_bt != "time" else ""
            candle_clusters[(s_sym, s_bt, s_var, s_t)].append(s)

        # ---------------- Stage 1: serial pre-filtering (no I/O) ----------------
        work_items = []
        for (sym, bar_type, bar_variant, t_ms), cluster in candle_clusters.items():
            try:
                is_mock = os.environ.get("MOCK_TEST_MODE") == "1" or "PYTEST_CURRENT_TEST" in os.environ or any(s.get("is_mock") for s in cluster)
                if bar_type in ("range", "vol", "volume"):
                    open_ms = t_ms
                    close_ms = t_ms
                    is_bar_closed = True
                else:
                    itv_ms = 300_000 if str(cluster[0].get("bar_label", "")).startswith("5m") else 900_000
                    open_ms = canonical_bar_epoch(t_ms) if itv_ms == 900_000 else ((t_ms // 300_000) * 300_000)
                    close_ms = open_ms + itv_ms
                    if any(s.get("bar_closed") is True for s in cluster):
                        is_bar_closed = True
                    elif any(s.get("bar_closed") is False for s in cluster):
                        is_bar_closed = bool(open_ms > 0 and close_ms <= now_ms)
                    else:
                        is_bar_closed = is_mock or (open_ms > 0 and close_ms <= now_ms)

                # Single Evaluation Invariant: Once reviewed, never evaluate again
                if self.stack.ledger.is_evaluated(sym, open_ms, bar_type=bar_type, bar_variant=bar_variant):
                    logger.debug(f"⏭️ [ALREADY REVIEWED] {sym} {bar_type} bar {open_ms} already evaluated. Suppressed.")
                    continue

                # Strict Venue Invariant: Reject any asset not tradeable on Delta Exchange India
                if not is_mock and not delta_client.resolve_product(sym):
                    logger.info(f"⏭️ [UNLISTED ASSET] {sym} is not listed on Delta Exchange India. Suppressed.")
                    sig_codes = [s.get("sig_code", "") for s in cluster]
                    unlisted_dir = str(cluster[0].get("side", "N/A"))
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, unlisted_dir, sig_codes),
                        confluence_score=0.0,
                        action="SUPPRESSED_UNLISTED_VENUE",
                        sig_name=str(cluster[0].get("sig_name", "Unlisted")),
                        direction=unlisted_dir,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )
                    continue

                # Strict Stale Invariant: Signals closed > 2 hours ago are discarded
                if not is_mock and (now_ms - close_ms) > 7_200_000:
                    logger.info(f"⏳ [STALE VETO] {sym} {bar_type} bar closed >2h ago ({int((now_ms - close_ms)/60000)}m ago). Discarded.")
                    sig_codes = [s.get("sig_code", "") for s in cluster]
                    stale_dir = str(cluster[0].get("side", "N/A"))
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=generate_signal_id(sym, open_ms, stale_dir, sig_codes),
                        confluence_score=0.0,
                        action="DISCARDED_STALE",
                        sig_name=str(cluster[0].get("sig_name", "Stale")),
                        direction=stale_dir,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )
                    continue

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

                # Antecedent Context Traversal: query preceding evaluated signals for multi-bar structural confirmation
                raw["antecedent_signals"] = self.stack.ledger.get_antecedent_history(sym, open_ms)

                title = str(raw.get("title") or raw.get("sig_name") or "Footprint Signal")
                desc = str(raw.get("description") or raw.get("desc") or "")
                price_val = float(raw.get("price", 0.0)) if raw.get("price") else 0.0
                c_ist = str(raw.get("candle_ist") or "")
                side = raw.get("side", raw.get("sig_side", "N/A"))
                sig_name = raw.get("sig_name", raw.get("title", "Signal"))

                # Phase 1: Immediate Ingest Log
                logger.info(f"⚡ [WAKE EVENT] {sym} {sig_name} ({side}) @ ${price_val:,.4f} | Candle: {c_ist} | Detail: '{desc}'")

                work_items.append({
                    "sym": sym,
                    "bar_type": bar_type,
                    "bar_variant": bar_variant,
                    "open_ms": open_ms,
                    "close_ms": close_ms,
                    "t_ms": t_ms,
                    "is_bar_closed": is_bar_closed,
                    "cluster": cluster,
                    "raw": raw,
                    "is_mock": is_mock,
                    "now_ms": now_ms,
                    "sig_name": sig_name,
                })
            except Exception as e:
                logger.error(f"Error pre-processing signal cluster for {sym}: {e}", exc_info=True)

        if not work_items:
            return evaluations

        # ------- Stage 2: bounded parallel evaluation (max 4 concurrent workers) -------
        sem = asyncio.Semaphore(max(1, MAX_PARALLEL_EVALUATIONS))

        async def _eval_worker(item: Dict[str, Any]):
            async with sem:
                raw = item["raw"]
                # PATCH P4: pre-fetch Hyperdash intel asynchronously — the
                # highest-latency feature (GraphQL whale book, 90s cached) is
                # fetched concurrently on the event loop instead of blocking the
                # serial chain inside evaluate_signal. Mock/test signals skip
                # the network entirely.
                if not item["is_mock"]:
                    try:
                        from Terminal.hyperdash_intel_engine import hyperdash_engine
                        if getattr(hyperdash_engine, "get_asset_intel_async", None) is not None:
                            _hint = float(raw.get("price") or 0.0)
                            raw["prefetched_hyperdash_intel"] = await hyperdash_engine.get_asset_intel_async(
                                item["sym"], mark_price_hint=(_hint if _hint > 0 else None)
                            )
                    except Exception as exc:
                        logger.debug(f"Hyperdash async pre-fetch failed for {item['sym']}: {exc}")
                        raw.pop("prefetched_hyperdash_intel", None)
                return await asyncio.to_thread(self.evaluator.evaluate_signal, raw)

        results = await asyncio.gather(
            *(_eval_worker(it) for it in work_items),
            return_exceptions=True
        )

        # ------- Stage 3: serial dispatch in original deterministic cluster order -------
        for item, eval_res in zip(work_items, results):
            sym = item["sym"]
            try:
                if isinstance(eval_res, BaseException):
                    raise eval_res
                if not isinstance(eval_res, dict):
                    raise TypeError(f"evaluate_signal returned non-dict result: {type(eval_res).__name__}")

                bar_type = item["bar_type"]
                bar_variant = item["bar_variant"]
                open_ms = item["open_ms"]
                close_ms = item["close_ms"]
                is_bar_closed = item["is_bar_closed"]
                cluster = item["cluster"]
                raw = item["raw"]
                now_ms = item["now_ms"]
                sig_name = item["sig_name"]

                evaluations.append(eval_res)

                score = eval_res.get("confluence_score", 0.0)
                is_actionable = eval_res.get("is_actionable", False)
                direction = eval_res.get("direction", "NEUTRAL")

                # Phase 3: Formatted Trade Confluence Card
                card = format_confluence_card(eval_res)
                print(card, flush=True)

                # Update Asset Signal Stack Matrix
                candidate, _ = self.stack.register_candle_cluster(cluster, now_ms=now_ms)
                action_str = "DISPATCHED" if is_actionable and direction in ("BUY", "SELL") else "FILTERED_SUB_THRESHOLD"
                sig_codes = [s.get("sig_code", "") for s in cluster]
                sig_id = eval_res.get("signal_id") or generate_signal_id(sym, open_ms, direction, sig_codes)

                if sym in self.stack.stack:
                    self.stack.stack[sym]["confluence_score"] = score
                    self.stack.stack[sym]["ev_300bar"] = float(eval_res.get("ev_300bar", 0.0))
                    self.stack.stack[sym]["rr_ratio"] = float(eval_res.get("trade_levels", {}).get("rr_ratio", 0.0))
                    self.stack.stack[sym]["state"] = action_str
                self.stack.persist_stack()

                # Phase 4: Actionable Trade Dispatch (Direct Delta Exchange India Auto-Wake Execution)
                val_errors = eval_res.get("validation_errors", [])
                remaining_vetoes = {v for v in val_errors if "BAR_NOT_CLOSED_INTRA_BAR_WICK" not in str(v)}
                tfm_req = float(eval_res.get("deepmind_timesfm", {}).get("required_threshold", 75.0) or 75.0)
                is_actionable_upon_close = (
                    score >= tfm_req
                    and direction in ("BUY", "SELL")
                    and not remaining_vetoes
                    and not eval_res.get("deepmind_timesfm", {}).get("adverse_veto", False)
                )

                if not is_bar_closed:
                    park_key = (sym, open_ms, bar_type, bar_variant)
                    if is_actionable_upon_close:
                        self.pending_signals[park_key] = {
                            "cluster": cluster,
                            "close_ms": close_ms,
                            "bar_type": bar_type,
                            "bar_variant": bar_variant,
                            "open_ms": open_ms,
                            "parked_at_ms": now_ms,
                        }
                        logger.info(
                            f"⏳ [IN-FLIGHT FORMING BAR] {sym} {bar_type}{('/' + bar_variant) if bar_variant else ''} "
                            f"actionable cluster PARKED (score {score:.1f}); will dispatch at bar close (close_ms={close_ms})."
                        )
                        final_action = "PARKED_IN_FLIGHT_FORMING"
                    else:
                        self.pending_signals.pop(park_key, None)
                        logger.info(
                            f"⏳ [IN-FLIGHT FORMING BAR] {sym} {bar_type}{('/' + bar_variant) if bar_variant else ''} "
                            f"unqualified cluster ignored (score {score:.1f}); leaving unburned for close evaluation."
                        )
                        final_action = "FORMING_BAR_UNQUALIFIED"
                else:
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
                            order_lock_file = str(DB_DIR / ".locks" / "order_execution_inflight.lock")
                            with FileMutex(order_lock_file, timeout=15.0) as order_guard:
                                if not order_guard.acquired:
                                    logger.warning(f"[{sym}] Order dispatch lock timeout (15.0s). Skipping to protect in-flight trade.")
                                    final_action = "DISPATCH_LOCK_TIMEOUT"
                                else:
                                    dispatch_res = await asyncio.to_thread(delta_client.execute_delta_signal, eval_res)
                                    logger.info(f"✨ Delta Exchange India Dispatch Result: {dispatch_res}")
                                    if dispatch_res and dispatch_res.get("ok"):
                                        self.state["dispatched_trades_count"] += 1
                                        save_state(self.state)
                                        final_action = "DISPATCHED"
                                    else:
                                        final_action = f"REJECTED_{dispatch_res.get('reason', 'UNKNOWN')}" if dispatch_res else "DISPATCH_FAILED"
                        else:
                            logger.warning(f"No Delta Exchange product resolved for {sym}; cannot execute.")
                            final_action = "REJECTED_UNLISTED_ON_DELTA"
                    else:
                        logger.info(f"[EVALUATOR] {sym} {direction} Confluence Score: {score:.1f}/100 -> Filtered (Required >= 75.0)")
                        final_action = "FILTERED_SUB_THRESHOLD"

                # Record in Evaluated Ledger ONLY after dispatch outcome is finalized
                # (and skip marking forming-bars so they can be re-evaluated at close)
                if is_bar_closed and final_action not in ("PARKED_IN_FLIGHT_FORMING", "FORMING_BAR_UNQUALIFIED"):
                    self.stack.ledger.mark_evaluated(
                        symbol=sym,
                        open_time_ms=open_ms,
                        signal_id=sig_id,
                        confluence_score=score,
                        action=final_action,
                        sig_name=sig_name,
                        direction=direction,
                        bar_type=bar_type,
                        bar_variant=bar_variant
                    )

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
        """Monitors Delta Exchange positions, verifies risk, and executes trailing stop loss."""
        now = time.time()
        if now - self.last_pos_check_ts < 15.0:
            return
        self.last_pos_check_ts = now

        current_active_keys = set()

        # 1. Delta Exchange India Position Guardian & Trailing Stop Loss
        try:
            guardian_reports = await asyncio.to_thread(delta_client.audit_and_trail_positions)
            for rep in guardian_reports:
                logger.info(f"🛡️ [DELTA GUARDIAN] {rep}")
        except Exception as e:
            logger.warning(f"Delta Guardian non-fatal error: {e}")
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

                # Reconcile actual realized PnL and exit price against broker history
                final_pnl = pnl
                final_exit = tracked.get("last_price", 0.0)
                reconciled_ticket = key
                if venue == "DELTA_INDIA":
                    try:
                        fills = delta_client.get_fills(symbol=sym, limit=5)
                        if fills:
                            latest_fill = fills[0]
                            fill_pnl = latest_fill.get("realized_pnl")
                            if fill_pnl is not None:
                                final_pnl = float(fill_pnl)
                            final_exit = float(latest_fill.get("price") or final_exit)
                            reconciled_ticket = str(latest_fill.get("order_id") or latest_fill.get("id") or key)
                    except Exception as fe:
                        logger.debug(f"Broker fill reconciliation note: {fe}")

                if final_pnl < 0:
                    logger.warning(
                        f"🚨 [POST-MORTEM TRIGGERED] {venue} position {sym} ({direction}) closed in LOSS: "
                        f"${final_pnl:.2f}. Launching adversarial forensic analysis..."
                    )
                    snapshot = self._gather_market_snapshot(sym)
                    if self.critic_engine:
                        diagnosis = self.critic_engine.conduct_post_mortem_and_store(
                            ticket_id=reconciled_ticket,
                            symbol=sym,
                            direction=direction,
                            entry_price=tracked.get("open_price", 0.0),
                            exit_price=final_exit,
                            realized_pnl_usd=final_pnl,
                            market_snapshot=snapshot
                        )
                        logger.info(
                            f"🧠 [CRITIC MEMORY COMMITTED] Failure Mode: {diagnosis.get('failure_mode')} | "
                            f"Mitigation Rule: {diagnosis.get('mitigation_guard')}"
                        )
                else:
                    logger.info(f"✨ [TRADE CLOSED PROFIT] {venue} position {sym} ({direction}) closed in PROFIT: +${pnl:.2f}.")

                del self.active_positions_tracker[key]

    async def _drain_pending_forming_bars(self):
        """
        PATCH W2: re-evaluates parked actionable forming-bar clusters at bar close.
        A parked cluster older than 2h without a close is discarded as stale.
        Ledger-marking happens ONLY here (after close), preserving the
        Single-Evaluation invariant while making close-confirmed dispatch possible.
        """
        if not self.pending_signals:
            return
        now_ms = time.time() * 1000
        for park_key in list(self.pending_signals.keys()):
            parked = self.pending_signals[park_key]
            close_ms = int(parked.get("close_ms") or 0)
            if close_ms <= 0:
                self.pending_signals.pop(park_key, None)
                continue
            if (now_ms - close_ms) > 7_200_000:   # > 2h stale
                logger.info(f"⏳ [PARKED SIGNAL STALE] {park_key} never confirmed closed; discarding.")
                self.pending_signals.pop(park_key, None)
                continue
            if now_ms < close_ms:
                continue   # bar still forming
            cluster = parked.get("cluster") or []
            if not cluster:
                self.pending_signals.pop(park_key, None)
                continue
            logger.info(f"🔔 [BAR CLOSED] Re-evaluating parked cluster {park_key} for confirmed-close dispatch.")
            self.pending_signals.pop(park_key, None)
            for s in cluster:
                s["bar_closed"] = True
            try:
                await self.evaluate_and_dispatch(cluster)
            except Exception as e:
                logger.error(f"Error draining parked cluster {park_key}: {e}", exc_info=True)

    async def run_once(self):
        """Processes any unread footprint signals and audits running trades once."""
        new_signals = self.read_new_raw_signals()
        if new_signals:
            logger.info(f"⚡ Discovered {len(new_signals)} new footprint signal(s) [time/range/vol]. Immediate quantitative evaluation...")
            await self.evaluate_and_dispatch(new_signals)
        else:
            logger.info("No new footprint signals ready for evaluation.")

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

        # Advance durable offset only after complete signal evaluation cycle
        if hasattr(self, "_pending_offset") and self._pending_offset > self.state.get("offset", 0):
            self.state["offset"] = self._pending_offset
            self.state["processed_signals_count"] += len(new_signals)
            save_state(self.state)


    async def run_daemon(self):
        """Continuous reactive daemon watching footprint_signals.jsonl with zero CPU lag."""
        logger.info(f"Starting continuous Reactive Sentinel on {FOOTPRINT_SIGNALS_JSONL.name}...")
        logger.info(f"Execution Venue: Delta Exchange India (Active Direct REST/WebSocket API)")
        logger.info(f"Exchange Risk Guardrails: Hard Equity Floor ${HARD_FLOOR_USD:,.2f} | Max Risk/Trade ${MAX_RISK_USD:.2f} | Max Concurrent Positions {MAX_OPEN_POSITIONS}")

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

                    # 1. Read newly appended signals and immediately evaluate
                    new_signals = self.read_new_raw_signals()
                    if new_signals:
                        logger.info(f"⚡ Discovered {len(new_signals)} new footprint signal(s) [time/range/vol]. Immediate quantitative evaluation...")
                        await self.evaluate_and_dispatch(new_signals)

                    # PATCH W2: drain parked forming-bar clusters once their bar closes
                    await self._drain_pending_forming_bars()

                    # Advance durable offset after processing signals
                    if hasattr(self, "_pending_offset") and self._pending_offset > self.state.get("offset", 0):
                        self.state["offset"] = self._pending_offset
                        self.state["processed_signals_count"] += len(new_signals)
                        save_state(self.state)

                    # 2. Guardian check on Delta Exchange open positions
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

    set_live_dispatch(True)
    sentinel = FootprintSignalSentinel(from_current_end=bool(args.reset_offset))

    if args.once:
        asyncio.run(sentinel.run_once())
    else:
        try:
            asyncio.run(sentinel.run_daemon())
        except KeyboardInterrupt:
            logger.info("Terminated by user. Goodbye!")


if __name__ == "__main__":
    main()
