"""
1_coinglass_footprint_scraper.py
========================================================================================
Standalone CoinGlass Footprint & Signals Scraper (100% In-Browser CDP Bridge)
========================================================================================
Pillar 1 of 5 Standalone Crypto Pipeline:
- Strictly operates via Chrome DevTools Protocol (CDP) on 127.0.0.1:9222.
- Ephemeral tab lifecycle: opens tab (?bot=footprint_scraper), harvests, closes immediately.
- Zero persistent bot tabs between sweeps; active orphan reaper preserves user tabs.
- Hooks window.__cg_term in memory via React Fiber; locks timeframe to 15m (m15).
- Extracts all 6 Footprint signals:
  1. Absorption (abs)
  2. Divergence (div)
  3. Cumulative Delta Divergence (sdiv / Bull div / Bear div)
  4. Exhaustion (exh)
  5. Trapped Shorts (trap)
  6. Trapped Longs (trap)
- Extracts completed 15m candle history (OHLCV, Delta, POC) directly from term.sigSets().
- Maintains exact 300-bar rolling window with primary IST columns in Database/Candle_Data/.
- Persists signals to:
  * Database/Signals/coinglass_aggregated_futures_signals.parquet (DuckDB OLAP bound)
  * Database/Signals/footprint_signals.jsonl (reactive stream watcher)
- Updates Database/Auction_Levels/ on every sweep cycle.
- Cooperative time-slicing: yields execution at reserved Whale/Liquidation minutes.
========================================================================================
"""

import sys
import os
import json
import time
import logging
import asyncio
import argparse
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Set

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_parquet, upsert_parquet, cdp_result
from Terminal.collector_lock import rotate_warp_ip, BrowserLock, is_scheduled_for_other_collector
from Terminal.auction_indicators import update_all_auction_profiles
from Terminal.universal_footprint_engine import enrich_candle_df_with_ist, utc_ms_to_ist_str, UniversalFootprintEngine

_footprint_engine = UniversalFootprintEngine()

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
SIGNALS_DIR = DB_DIR / "Signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
CANDLE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_PARQUET = SIGNALS_DIR / "coinglass_aggregated_futures_signals.parquet"
SIGNALS_JSONL = SIGNALS_DIR / "footprint_signals.jsonl"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [CG-FOOTPRINT] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("FootprintScraper")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# Pre-load known signals to avoid duplicate emission
KNOWN_SIGNAL_KEYS: set = set()

# Core Tier 1 Assets + Major Liquidity Cryptos
DEFAULT_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK",
    "AVAX", "SUI", "NEAR", "HYPE", "DOT", "BCH", "UNI", "LTC"
]


def load_target_symbols() -> Tuple[List[str], Dict[str, str]]:
    if not REGISTRY_FILE.exists():
        return DEFAULT_SYMBOLS, {}
    try:
        with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
            reg = json.load(f)
        symbols = []
        mt5_map = {}
        for cat, assets in reg.items():
            for sym, meta in assets.items():
                if meta.get("footprint", False):
                    symbols.append(sym)
                    mt5_map[sym] = meta.get("mt5", f"{sym}USD")
        return (symbols if symbols else DEFAULT_SYMBOLS), mt5_map
    except Exception as e:
        logger.warning(f"Error reading registry: {e}. Falling back to default symbols.")
        return DEFAULT_SYMBOLS, {}


def load_known_signals():
    global KNOWN_SIGNAL_KEYS
    KNOWN_SIGNAL_KEYS = set()
    if SIGNALS_JSONL.exists():
        try:
            with open(SIGNALS_JSONL, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        sym = data.get("symbol")
                        sig = str(data.get("sig_code") if data.get("sig_code") is not None else "")
                        t_ms = int(data.get("time_ms") or 0)
                        bt = str(data.get("bar_type") or "time").lower()
                        if sym and t_ms > 0:
                            var = str(data.get("bar_label") or "").strip() if bt != "time" else ""
                            KNOWN_SIGNAL_KEYS.add((sym, sig, bt, var, t_ms))
                    except Exception:
                        continue
            logger.info(f"Pre-loaded {len(KNOWN_SIGNAL_KEYS)} known signal keys from {SIGNALS_JSONL.name}.")
        except Exception as e:
            logger.warning(f"Could not pre-load known signals from JSONL: {e}")


def cleanup_orphan_tabs(keep_tab_id: Optional[str] = None):
    """Reaps any orphaned bot tabs in Chrome, preserving user & delta exchange tabs."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "bot=footprint_scraper" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Reaped orphaned footprint tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Tab reaper check: {e}")


def open_footprint_tab() -> Tuple[Optional[str], Optional[str]]:
    """Opens an ephemeral CoinGlass footprint tab and returns (tab_id, ws_url)."""
    cleanup_orphan_tabs()
    try:
        url = "https://www.coinglass.com/footprint?bot=footprint_scraper"
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=6)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened fresh ephemeral Footprint tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open footprint tab via CDP: {e}")
        return None, None


def close_tab(tab_id: Optional[str]):
    """Guaranteed tab closure."""
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed ephemeral tab ({tab_id}) - Tab footprint clean.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    cleanup_orphan_tabs()


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    return await cdp_result(ws, {
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {"expression": expr, "returnByValue": True, "awaitPromise": await_promise}
    })


async def ensure_terminal_ready(ws) -> bool:
    """Finds window.__cg_term from React Fiber root and locks 15m timeframe."""
    init_js = """(() => {
        if (window.__cg_term) return true;
        const nextEl = document.getElementById('__next');
        if (!nextEl) return false;
        const containerKey = Object.keys(nextEl).find(k => k.startsWith('__reactContainer$'));
        if (!containerKey) return false;
        const rootFiber = nextEl[containerKey];

        let term = null;
        function findTerm(fiber, depth = 0) {
            if (!fiber || depth > 80 || term) return;
            try {
                if (fiber.memoizedProps && fiber.memoizedProps.value && fiber.memoizedProps.value.term) {
                    term = fiber.memoizedProps.value.term;
                    return;
                }
            } catch(e) {}
            findTerm(fiber.child, depth + 1);
            findTerm(fiber.sibling, depth);
        }
        findTerm(rootFiber);
        if (term) {
            window.__cg_term = term;
            return true;
        }
        return false;
    })()"""
    for _ in range(12):
        ready = await cdp_eval(ws, init_js)
        if ready:
            # Strictly lock 15m interval
            await cdp_eval(ws, "if (window.__cg_term && window.__cg_term.prefs.itv !== '15m') window.__cg_term.setItv('15m')")
            return True
        await asyncio.sleep(0.7)
    return False


async def extract_symbol_footprint(ws, sym: str, mt5_sym: str) -> dict:
    """Hooks window.__cg_term, switches symbol, and harvests signals + 15m candle array."""
    js = f"""(async () => {{
        const term = window.__cg_term;
        if (!term) return {{ error: 'no __cg_term' }};
        const sym = '{sym}';
        const mt5 = '{mt5_sym}';

        return new Promise((resolve) => {{
            let unsub = () => {{}};
            const samePair = term.prefs.sym === sym && term.prefs.mkt === 'agg-perp' && term.prefs.itv === '15m';
            const priorX = term.sigSets()?.find(s => s.bt === 'time')?.X;
            const timeout = setTimeout(() => {{
                unsub();
                resolve({{ error: "footprint_load_timeout", signals: [], candles: [] }});
            }}, 4500);

            let checks = 0;
            const poll = setInterval(async () => {{
                checks++;
                const sigSets = term.sigSets ? term.sigSets() : null;
                const timeData = sigSets?.find(s => s.bt === 'time')?.X;
                if (term.prefs.sym === sym && timeData?.T?.length > 0 && (samePair || timeData !== priorX)) {{
                    clearTimeout(timeout);
                    unsub();

                    const signals = [];
                    for (const set of (sigSets || [])) {{
                        const X = set.X;
                        for (const s of (set.sigs || [])) {{
                            const timeMs = X && X.T ? X.T[s.i] : null;
                            if (!timeMs) continue;
                            const text = (term.sigText ? term.sigText(s, X) : null) || ['', ''];
                            signals.push({{
                                symbol: sym,
                                mt5_symbol: mt5,
                                market: 'agg-perp',
                                bar_type: set.bt || 'time',
                                bar_label: set.label || set.bt || '15m',
                                time_ms: timeMs,
                                datetime_utc: new Date(timeMs).toISOString(),
                                sig_code: s.t,
                                sig_name: term.sigName ? term.sigName(s) : (s.t || 'Signal'),
                                side: s.side === 'buy' ? 'bullish' : 'bearish',
                                price: s.p,
                                title: text[0] || '',
                                description: text[1] || ''
                            }});
                        }}
                    }}

                    const timeSet = sigSets.find(s => s.bt === 'time');
                    const bars = [];
                    if (timeSet && timeSet.X && timeSet.X.T) {{
                        const X = timeSet.X;
                        const n = X.T.length;
                        let cumDelta = 0;
                        for (let i = 0; i < n; i++) {{
                            const t = X.T[i];
                            const te = X.TE ? X.TE[i] : (t + 15 * 60 * 1000);
                            const o = X.O[i];
                            const h = X.H[i];
                            const l = X.L[i];
                            const c = X.C[i];
                            const F = X.F ? X.F[i] : null;

                            let buyVol = 0, sellVol = 0, maxVol = 0, pocIdx = -1;
                            if (F && F.b && F.s) {{
                                for (let j = 0; j < F.b.length; j++) {{
                                    const b = F.b[j] || 0;
                                    const s = F.s[j] || 0;
                                    buyVol += b;
                                    sellVol += s;
                                    if (b + s > maxVol) {{
                                        maxVol = b + s;
                                        pocIdx = j;
                                    }}
                                }}
                            }}
                            const delta = buyVol - sellVol;
                            cumDelta += delta;
                            let pocPrice = (F && pocIdx >= 0) ? (F.lo + pocIdx * F.tick) : null;

                            bars.push({{
                                symbol: sym,
                                mt5_symbol: mt5,
                                timeframe: '15m',
                                open_time_ms: t,
                                close_time_ms: te,
                                open_time_utc: new Date(t).toISOString(),
                                close_time_utc: new Date(te).toISOString(),
                                open: o, high: h, low: l, close: c,
                                total_volume: buyVol + sellVol,
                                delta: delta,
                                cumulative_delta: cumDelta,
                                poc_price: pocPrice
                            }});
                        }}
                    }}

                    resolve({{ signals, candles: bars }});
                }}
            }}, 25);
            unsub = () => clearInterval(poll);
            term.pickPair(sym, 'agg-perp');
            if (term.prefs.itv !== '15m') {{
                term.setItv('15m');
            }}
        }});
    }})()"""
    res = await cdp_eval(ws, js, msg_id=int(time.time() * 1000) % 1000000, await_promise=True)
    return res if isinstance(res, dict) else {"error": "cdp_eval_failed", "signals": [], "candles": []}


def dump_signals(signals: List[dict], candles: Optional[List[dict]] = None) -> List[dict]:
    """Persists signals to Parquet and appends brand-new signals to footprint_signals.jsonl."""
    if not signals:
        return []
    global KNOWN_SIGNAL_KEYS

    # Build candle lookup map for footprint order flow attributes
    candle_map = {}
    if candles:
        for c in candles:
            t = c.get("open_time_ms")
            if t:
                candle_map[t] = c

    now_utc_str = datetime.now(timezone.utc).isoformat()
    enriched = []
    for s in signals:
        s_copy = dict(s)
        t_ms = int(s_copy.get("time_ms") or 0)
        # PATCH C1: Range/Volume bars are discrete completed bars — their close time IS
        # their emission time; fabricating close = open + 15m corrupted close_time_utc
        # for every non-time bar in the parquet ledger.
        if str(s_copy.get("bar_type") or "time").lower() in ("range", "vol", "volume"):
            c_ms = t_ms
        else:
            itv_ms_probe = 300_000 if str(s_copy.get("bar_label", "")).lower().lstrip().startswith("5m") else 900_000
            c_ms = t_ms + int(s_copy.get("bar_interval_ms") or itv_ms_probe)
        s_copy["bar_interval_ms"] = int(c_ms - t_ms)

        t_ist = utc_ms_to_ist_str(t_ms)
        c_ist = utc_ms_to_ist_str(c_ms)
        t_utc = datetime.fromtimestamp(t_ms / 1000.0, timezone.utc).isoformat()
        c_utc = datetime.fromtimestamp(c_ms / 1000.0, timezone.utc).isoformat()

        s_copy["time_ms"] = t_ms
        s_copy["open_time_ist"] = t_ist
        s_copy["close_time_ist"] = c_ist
        s_copy["time_ist"] = t_ist
        s_copy["datetime_ist"] = t_ist
        s_copy["candle_ist"] = t_ist
        s_copy["time_ist_ms"] = t_ms + 19800000
        s_copy["close_time_ms"] = float(c_ms)
        s_copy["close_time_utc"] = c_utc
        s_copy["candle_utc"] = t_utc
        s_copy["detection_time_utc"] = now_utc_str
        s_copy["source"] = "coinglass_cdp"
        s_copy["asset_class"] = "Crypto"

        # Inherit footprint metrics from matching candle
        matching_candle = candle_map.get(t_ms)
        if matching_candle:
            s_copy["volume"] = float(matching_candle.get("total_volume", 0.0))
            s_copy["delta"] = float(matching_candle.get("delta", 0.0))
            s_copy["cvd"] = float(matching_candle.get("cumulative_delta", 0.0))
            s_copy["poc_price"] = float(matching_candle.get("poc_price") or s_copy.get("price", 0.0))
            v = s_copy["volume"]
            s_copy["delta_pct"] = round((s_copy["delta"] / max(v, 1e-9)) * 100.0, 1)
        else:
            s_copy["volume"] = float(s_copy.get("volume") or 0.0)
            s_copy["delta"] = float(s_copy.get("delta") or 0.0)
            s_copy["delta_pct"] = float(s_copy.get("delta_pct") or 0.0)
            s_copy["cvd"] = float(s_copy.get("cvd") or 0.0)
            s_copy["poc_price"] = float(s_copy.get("poc_price") or s_copy.get("price", 0.0))

        enriched.append(s_copy)

    df = pd.DataFrame(enriched)
    upsert_parquet(df, SIGNALS_PARQUET, ["symbol", "sig_code", "time_ms"], sort_by="time_ms", limit=100000)

    # Emit only closed, enriched signals to the reactive watcher. Never consume
    # a dedupe key on a forming bar: doing so permanently suppresses the same
    # signal after the candle closes.
    newly_discovered = []
    now_ms = time.time() * 1000
    for s in enriched:
        t_ms = int(s.get("time_ms") or 0)
        if t_ms <= 0:
            continue
        bar_type = str(s.get("bar_type", "time")).lower()
        if bar_type in ("range", "vol"):
            is_closed = True
            close_ms = t_ms
        else:
            itv_ms = 300_000 if str(s.get("bar_label", "")).startswith("5m") else 900_000
            close_ms = t_ms + itv_ms
            is_closed = (close_ms <= now_ms)

        if not is_closed:
            continue

        # Freshness invariant: ignore stale historical dumps older than 2 hours
        if (now_ms - t_ms) > 7_200_000:
            continue

        s["bar_closed"] = is_closed
        s["candle_close_ms"] = close_ms
        s["candle_close_ist"] = utc_ms_to_ist_str(close_ms)
        # PATCH C1: variant-aware key. Vol 100K and Vol 250K bars closing on the same
        # burst previously collided (same sig_code, same bar_type, same t_ms) and the
        # second variant was silently dropped — exactly during the volume spikes the
        # footprint stack is built to harvest.
        s_variant = str(s.get("bar_label") or "").strip() if bar_type != "time" else ""
        key = (s["symbol"], str(s.get("sig_code") if s.get("sig_code") is not None else ""), bar_type, s_variant, t_ms)
        if key not in KNOWN_SIGNAL_KEYS:
            KNOWN_SIGNAL_KEYS.add(key)
            newly_discovered.append(s)

    if newly_discovered:
        with open(SIGNALS_JSONL, "a", encoding="utf-8") as f:
            for s in newly_discovered:
                f.write(json.dumps(s) + "\n")
        logger.info(f"Discovered {len(newly_discovered)} footprint signal(s) [time/vol/range]; appended to {SIGNALS_JSONL.name}")
    return newly_discovered


def dump_candles(sym: str, candles: List[dict]):
    """Persists 15m candles with strict 300-bar rolling window, footprint metrics, and primary IST columns."""
    if not candles:
        return
    frame = pd.DataFrame(candles)
    frame = frame[frame["open_time_ms"] % 900000 == 0].drop_duplicates("open_time_ms").sort_values("open_time_ms")
    if frame.empty:
        return
    frame = enrich_candle_df_with_ist(frame)
    frame["dumped_at_utc"] = datetime.now(timezone.utc).isoformat()
    
    candle_file = CANDLE_DIR / f"{sym}.parquet"
    if candle_file.exists():
        try:
            existing = pd.read_parquet(candle_file)
            frame = pd.concat([existing, frame]).drop_duplicates("open_time_ms").sort_values("open_time_ms").tail(300)
        except Exception:
            pass

    try:
        analyzed_df, _ = _footprint_engine.analyze_bars(frame, symbol=sym, asset_class="crypto", rolling_window=300)
        if not analyzed_df.empty:
            frame = analyzed_df
    except Exception:
        pass

    for col in ["signal_type", "signal_side", "signal_desc"]:
        if col in frame.columns:
            frame[col] = frame[col].fillna("")
        else:
            frame[col] = ""
    if "has_signal" in frame.columns:
        frame["has_signal"] = frame["has_signal"].fillna(False)
    if "vol_sma20" in frame.columns:
        frame["vol_sma20"] = frame["vol_sma20"].fillna(frame["total_volume"])
    if "delta_pct" in frame.columns:
        frame["delta_pct"] = frame["delta_pct"].fillna(0.0)
    if "cvd" in frame.columns:
        frame["cvd"] = frame["cvd"].fillna(0.0)
    if "buy_volume" in frame.columns:
        frame["buy_volume"] = frame["buy_volume"].fillna(frame["total_volume"] / 2.0)
    if "sell_volume" in frame.columns:
        frame["sell_volume"] = frame["sell_volume"].fillna(frame["total_volume"] / 2.0)
    if "total_trades" in frame.columns:
        frame["total_trades"] = frame["total_trades"].fillna(0)
    if "asset_class" in frame.columns:
        frame["asset_class"] = frame["asset_class"].fillna("crypto")

    atomic_parquet(frame.tail(300), candle_file)


async def execute_footprint_sweep(ws) -> Tuple[int, int]:
    """Sweeps target assets, harvests footprint signals and candles, and updates auction profiles."""
    symbols, mt5_map = load_target_symbols()
    logger.info(f"Starting Footprint sweep across {len(symbols)} crypto assets...")
    total_signals = 0
    total_new = 0

    for idx, sym in enumerate(symbols):
        try:
            mt5_sym = mt5_map.get(sym, f"{sym}USD")
            res = await extract_symbol_footprint(ws, sym, mt5_sym)
            if res.get("error"):
                logger.debug(f"[{sym}] Extraction notice: {res.get('error')}")
                continue

            signals = res.get("signals", [])
            candles = res.get("candles", [])

            if candles:
                dump_candles(sym, candles)

            if signals:
                new_s = dump_signals(signals, candles=candles)
                total_signals += len(signals)
                total_new += len(new_s)

        except Exception as e:
            logger.error(f"[{sym}] Sweep error: {e}")

        await asyncio.sleep(0.05)

    # Update local auction levels atomically
    try:
        update_all_auction_profiles()
        logger.info("Updated all local Auction Profile tables (Database/Auction_Levels/).")
    except Exception as e:
        logger.warning(f"Error updating auction profiles: {e}")

    logger.info(f"Footprint sweep complete: {total_signals} total signals harvested ({total_new} new).")
    return total_signals, total_new


async def run_sweep_once() -> bool:
    acquire_singleton("coinglass_footprint_scraper", DB_DIR / ".locks")
    load_known_signals()

    with BrowserLock("FootprintScraper", timeout_secs=10.0) as lock:
        if not lock.acquired:
            logger.warning("Browser lock could not be acquired for Footprint sweep. Retrying next cycle.")
            return False

        tab_id, ws_url = open_footprint_tab()
        if not tab_id or not ws_url:
            logger.error("Could not open Footprint tab in Chrome.")
            return False

        try:
            async with websockets.connect(ws_url, open_timeout=8, close_timeout=3, max_size=50_000_000, ping_interval=None) as ws:
                ready = await ensure_terminal_ready(ws)
                if not ready:
                    logger.error("CoinGlass Footprint terminal failed to initialize.")
                    rotate_warp_ip("Footprint-Challenge", reason="CHALLENGE")
                    return False
                await execute_footprint_sweep(ws)
                return True
        except Exception as e:
            logger.error(f"WebSocket execution error: {e}")
            return False
        finally:
            close_tab(tab_id)


async def run_daemon(interval_seconds: int = 120):
    acquire_singleton("coinglass_footprint_scraper", DB_DIR / ".locks")
    load_known_signals()
    logger.info(f"CoinGlass Footprint Scraper Daemon running (interval={interval_seconds}s)...")

    while True:
        now = datetime.now()
        cur_min = now.minute
        cur_sec = now.second
        # Cooperative yielding on reserved Whale/Liquidation minutes
        if is_scheduled_for_other_collector(cur_min, "footprint"):
            logger.info(f"Minute :{cur_min:02d} is reserved for Whale/Liquidation sweep. Yielding 45s...")
            await asyncio.sleep(45.0)
            continue
        # TDMA Tier 2: Footprint runs on even minutes in second window :30 - :55
        if cur_min % 2 != 0:
            await asyncio.sleep(max(5.0, 60.0 - cur_sec))
            continue
        if cur_sec < 30:
            await asyncio.sleep(30.0 - cur_sec)
        elif cur_sec > 55:
            await asyncio.sleep(max(5.0, 60.0 - cur_sec))
            continue

        start_time = time.time()
        with BrowserLock("FootprintScraper", timeout_secs=15.0) as lock:
            if lock.acquired:
                tab_id, ws_url = open_footprint_tab()
                if tab_id and ws_url:
                    try:
                        async with websockets.connect(ws_url, open_timeout=8, close_timeout=3, max_size=50_000_000, ping_interval=None) as ws:
                            ready = await ensure_terminal_ready(ws)
                            if ready:
                                await execute_footprint_sweep(ws)
                            else:
                                logger.warning("Footprint terminal not ready. Rotating IP if needed...")
                                rotate_warp_ip("Footprint-NotReady", reason="CHALLENGE")
                    except Exception as e:
                        logger.error(f"Daemon cycle error: {e}")
                    finally:
                        close_tab(tab_id)
            else:
                logger.info("Browser lock contention. Skipping footprint cycle to let others pass.")

        elapsed = time.time() - start_time
        sleep_dur = max(10.0, interval_seconds - elapsed)
        await asyncio.sleep(sleep_dur)


def main():
    parser = argparse.ArgumentParser(description="CoinGlass Footprint Scraper")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--once", action="store_true", help="Run a single sweep and exit")
    parser.add_argument("--interval", type=int, default=120, help="Daemon cycle interval in seconds (default: 120)")
    args = parser.parse_args()

    if args.daemon:
        asyncio.run(run_daemon(args.interval))
    else:
        asyncio.run(run_sweep_once())


if __name__ == "__main__":
    main()
