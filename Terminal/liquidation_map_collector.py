"""
CoinGlass Liquidation Map Collector (Standalone Daemon)
=======================================================
Runs continuously in background:
- Triggers at :00:05 and :30:05 local wall-clock (0th and 30th minute of every hour).
- Dynamic tab lifecycle: Opens tab, harvests all available assets, dumps Parquet, and CLOSES the tab.
- Sweeps:
  1. Key Liquidation Levels (ranked strike barriers by side, price, leverage tier, distance %, strength)
  2. Magnet Zones (Top short & long liquidation cluster bands, distance %, strength)
  3. Near-Field Balance (±3% price proximity longs vs shorts % and 24h delta)
  4. Liquidation Snapshots (Last price, open strength, sentiment)

Persists to:
- Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet
- Database/Liquidation_Data/Magnet_Zones/<SYMBOL>_magnet_zones.parquet
- Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet
- Database/Liquidation_Data/liquidation_snapshots.parquet
"""

import sys
import os
import subprocess
import re
import json
import time
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import pandas as pd
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LIQ_DIR = DB_DIR / "Liquidation_Data"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"

REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [LIQUIDATION] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("LiquidationCollector")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# All available mutual crypto assets
CRYPTO_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "ADA", "LINK", "AVAX", "NEAR", "SUI", "TRX", "LTC"
]


def load_mt5_map():
    if not REGISTRY_FILE.exists():
        return {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    mt5_map = {}
    for cat, assets in reg.items():
        for sym, meta in assets.items():
            mt5_map[sym] = meta.get("mt5", "")
    return mt5_map


MT5_MAP = load_mt5_map()


def get_seconds_until_next_sweep() -> float:
    """Calculate exact seconds until next :00:05 or :30:05 local wall-clock time."""
    now = datetime.now()
    if now.minute < 30 or (now.minute == 30 and now.second < 5):
        target = now.replace(minute=30, second=5, microsecond=0)
    else:
        next_hour = now + timedelta(hours=1)
        target = next_hour.replace(minute=0, second=5, microsecond=0)
    diff = (target - now).total_seconds()
    return max(1.0, diff)


def cleanup_coinglass_tabs(keep_tab_id: Optional[str] = None):
    """Closes all orphaned CoinGlass liquidation tabs in Chrome to keep browser lean and free."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                tid = t.get("id")
                url = t.get("url", "").lower()
                if tid != keep_tab_id and "coinglass.com/liquidation" in url:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tid}", timeout=2)
                        logger.info(f"Cleaned up orphan Liquidation tab ({tid})")
                    except Exception:
                        pass
    except Exception as e:
        logger.debug(f"Tab cleanup error: {e}")


def acquire_pid_lock(name: str):
    """Ensures strictly one instance of this daemon runs at any time."""
    lock_dir = DB_DIR / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / f"{name}.pid"
    my_pid = os.getpid()
    if lock_file.exists():
        try:
            old_pid = int(lock_file.read_text().strip())
            if old_pid != my_pid:
                subprocess.run(
                    ["powershell", "-Command", f"Stop-Process -Id {old_pid} -Force -ErrorAction SilentlyContinue"],
                    capture_output=True, check=False
                )
                logger.info(f"Terminated previous instance PID {old_pid} to enforce single-instance guarantee.")
        except Exception:
            pass
    lock_file.write_text(str(my_pid))


def open_coinglass_liquidation_tab(initial_sym: str = "BTC") -> Tuple[Optional[str], Optional[str]]:
    """Opens a fresh CoinGlass liquidation tab and returns (tab_id, ws_url)."""
    cleanup_coinglass_tabs()
    try:
        url = f"https://www.coinglass.com/liquidation-levels?symbol={initial_sym}"
        req = urllib.request.Request(f"http://127.0.0.1:9222/json/new?{url}", method="PUT")
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read().decode())
        tab_id = data.get("id")
        ws_url = data.get("webSocketDebuggerUrl")
        logger.info(f"Opened fresh CoinGlass Liquidation tab ({tab_id})")
        return tab_id, ws_url
    except Exception as e:
        logger.error(f"Failed to open liquidation tab via CDP: {e}")
        return None, None


def close_tab(tab_id: str):
    """Closes tab immediately after sweep to keep browser lean."""
    if tab_id:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:9222/json/close/{tab_id}", timeout=3)
            logger.info(f"Closed liquidation tab ({tab_id}) - Browser lean & free.")
        except Exception as e:
            logger.warning(f"Could not close tab {tab_id}: {e}")
    cleanup_coinglass_tabs()


async def cdp_eval(ws, expr: str, msg_id: int = 1, await_promise: bool = False):
    payload = {
        "id": msg_id,
        "method": "Runtime.evaluate",
        "params": {
            "expression": expr,
            "returnByValue": True,
            "awaitPromise": await_promise
        }
    }
    await ws.send(json.dumps(payload))
    while True:
        raw = await ws.recv()
        resp = json.loads(raw)
        if resp.get("id") == msg_id:
            res = resp.get("result", {})
            return res.get("result", {}).get("value")


# ---------------------------------------------------------------------------
# Data Persistence Functions
# ---------------------------------------------------------------------------
def upsert_asset_key_levels(symbol: str, levels: List[dict]):
    if not levels:
        return
    KEY_LEVELS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = KEY_LEVELS_DIR / f"{symbol}_key_levels.parquet"
    new_df = pd.DataFrame(levels)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if out_path.exists():
        try:
            old_df = pd.read_parquet(out_path)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["range_filter", "side", "rank", "price"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(out_path, index=False, engine="pyarrow")


def upsert_asset_magnet_zones(symbol: str, magnet_zones: List[dict]):
    if not magnet_zones:
        return
    MAGNET_ZONES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MAGNET_ZONES_DIR / f"{symbol}_magnet_zones.parquet"
    new_df = pd.DataFrame(magnet_zones)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if out_path.exists():
        try:
            old_df = pd.read_parquet(out_path)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["side", "rank", "price_band"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(out_path, index=False, engine="pyarrow")


def upsert_asset_near_field(symbol: str, near_fields: List[dict]):
    if not near_fields:
        return
    NEAR_FIELD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NEAR_FIELD_DIR / f"{symbol}_near_field.parquet"
    new_df = pd.DataFrame(near_fields)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if out_path.exists():
        try:
            old_df = pd.read_parquet(out_path)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["symbol", "snapshot_time_utc"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(out_path, index=False, engine="pyarrow")


def upsert_liquidation_snapshots(snapshots: List[dict]):
    if not snapshots:
        return
    LIQ_DIR.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(snapshots)
    now_utc = datetime.now(timezone.utc).isoformat()
    new_df["snapshot_time_utc"] = now_utc
    new_df["dumped_at_utc"] = now_utc
    if SNAPSHOTS_PARQUET.exists():
        try:
            old_df = pd.read_parquet(SNAPSHOTS_PARQUET)
            combined = pd.concat([old_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["snapshot_time_utc", "symbol"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined.to_parquet(SNAPSHOTS_PARQUET, index=False, engine="pyarrow")


# ---------------------------------------------------------------------------
# Extraction Routine
# ---------------------------------------------------------------------------
EXTRACTION_JS = """(() => {
    const text = document.body.innerText;
    
    // 1. Last price
    let curPrice = null;
    const pMatch = text.match(/Last price\\s*[\\n\\r]*\\s*([\\d,]+\\.?\\d*)/);
    if (pMatch) {
        curPrice = parseFloat(pMatch[1].replace(/,/g, ''));
    }

    // 2. Magnet zones
    const magnetZones = [];
    const sMag = text.match(/Nearest short magnet zone\\s*[\\n\\r]*\\s*([\\d,]+\\.?\\d*\\s*[–\\-]\\s*[\\d,]+\\.?\\d*)\\s*[\\n\\r]*\\s*([+\\-\\d\\.]+%)\\s*·\\s*Strength\\s*(\\d+)/);
    if (sMag) {
        magnetZones.push({
            side: 'short',
            rank: 1,
            price_band: sMag[1].replace(/\\s+/g, ' '),
            distance_pct: parseFloat(sMag[2].replace('%', '').replace('+', '')),
            score: parseFloat(sMag[3])
        });
    }

    const lMag = text.match(/Nearest long magnet zone\\s*[\\n\\r]*\\s*([\\d,]+\\.?\\d*\\s*[–\\-]\\s*[\\d,]+\\.?\\d*)\\s*[\\n\\r]*\\s*([+\\-\\d\\.]+%)\\s*·\\s*Strength\\s*(\\d+)/);
    if (lMag) {
        magnetZones.push({
            side: 'long',
            rank: 1,
            price_band: lMag[1].replace(/\\s+/g, ' '),
            distance_pct: parseFloat(lMag[2].replace('%', '').replace('+', '')),
            score: parseFloat(lMag[3])
        });
    }

    // 3. Near-field balance
    let nearField = null;
    const nfShorts = text.match(/Shorts\\s*(\\d+)%\\s*·/);
    const nfLongs = text.match(/Longs\\s*(\\d+)%\\s*·/);
    if (nfShorts && nfLongs) {
        nearField = {
            shorts_pct: parseFloat(nfShorts[1]),
            longs_pct: parseFloat(nfLongs[1])
        };
    }

    // 4. Key levels table
    const table = document.querySelector('table');
    const keyLevels = [];
    if (table) {
        const trs = Array.from(table.querySelectorAll('tbody tr'));
        let rankShort = 1, rankLong = 1;
        for (const r of trs) {
            const cells = Array.from(r.querySelectorAll('td')).map(c => c.innerText.trim());
            if (cells.length >= 5) {
                const side = cells[0].toLowerCase();
                const rank = side === 'short' ? rankShort++ : rankLong++;
                keyLevels.push({
                    side: side,
                    rank: rank,
                    price: parseFloat(cells[1].replace(/,/g, '')),
                    distance_pct: parseFloat(cells[2].replace(/[%+]/g, '')),
                    leverage_tier: cells[3].toLowerCase(),
                    strength: parseFloat(cells[4].replace(/,/g, '')),
                    range_filter: 'all'
                });
            }
        }
    }

    return {
        curPrice,
        magnetZones,
        nearField,
        keyLevels
    };
})()"""


async def sweep_symbol_liquidation(ws, symbol: str) -> dict:
    mt5_sym = MT5_MAP.get(symbol, f"{symbol}USD.p")
    target_url = f"https://www.coinglass.com/liquidation-levels?symbol={symbol}"
    
    # Navigate
    await ws.send(json.dumps({
        "id": 1,
        "method": "Page.navigate",
        "params": {"url": target_url}
    }))
    await ws.recv()
    
    # Allow page to load & render
    await asyncio.sleep(3.5)
    
    # Extract
    state = await cdp_eval(ws, EXTRACTION_JS, msg_id=2)
    if not state:
        state = {}

    cur_price = state.get("curPrice")
    magnet_zones = state.get("magnetZones", [])
    near_field = state.get("nearField")
    key_levels = state.get("keyLevels", [])

    for mz in magnet_zones:
        mz["symbol"] = symbol
        mz["mt5_symbol"] = mt5_sym
    
    for kl in key_levels:
        kl["symbol"] = symbol
        kl["mt5_symbol"] = mt5_sym

    near_field_rows = []
    if near_field:
        near_field_rows.append({
            "symbol": symbol,
            "mt5_symbol": mt5_sym,
            "shorts_pct": near_field.get("shorts_pct", 50.0),
            "longs_pct": near_field.get("longs_pct", 50.0)
        })

    snap = {
        "symbol": symbol,
        "mt5_symbol": mt5_sym,
        "last_price": cur_price,
        "magnet_zones_count": len(magnet_zones),
        "key_levels_count": len(key_levels),
        "near_field_shorts_pct": near_field.get("shorts_pct") if near_field else 50.0,
        "near_field_longs_pct": near_field.get("longs_pct") if near_field else 50.0
    }

    if magnet_zones:
        upsert_asset_magnet_zones(symbol, magnet_zones)
    if key_levels:
        upsert_asset_key_levels(symbol, key_levels)
    if near_field_rows:
        upsert_asset_near_field(symbol, near_field_rows)
    upsert_liquidation_snapshots([snap])

    logger.info(f"[{symbol}] Harvested: Price={cur_price} | Magnets={len(magnet_zones)} | KeyLevels={len(key_levels)} | NearField={near_field}")
    return snap


async def execute_liquidation_sweep():
    """Executes a full sweep across all available assets, then immediately closes the tab."""
    logger.info(f"=== Initiating Liquidation Sweep across {len(CRYPTO_SYMBOLS)} assets ===")
    tab_id, ws_url = open_coinglass_liquidation_tab(CRYPTO_SYMBOLS[0])
    if not tab_id or not ws_url:
        logger.error("Failed to open liquidation tab. Aborting sweep.")
        return

    try:
        await asyncio.sleep(3.0)
        ws = None
        for attempt in range(4):
            try:
                ws = await websockets.connect(ws_url, max_size=50_000_000)
                break
            except Exception as conn_err:
                if attempt == 3:
                    raise conn_err
                await asyncio.sleep(2.0)

        async with ws:
            for i, sym in enumerate(CRYPTO_SYMBOLS, 1):
                try:
                    logger.info(f"[{i}/{len(CRYPTO_SYMBOLS)}] Sweeping liquidation for {sym}...")
                    await sweep_symbol_liquidation(ws, sym)
                except Exception as e:
                    logger.warning(f"[{sym}] Error during liquidation harvest: {e}")
    finally:
        # Close the tab immediately once done
        close_tab(tab_id)


async def run_collector_daemon():
    """Scheduled daemon triggering at :00:05 and :30:05 local wall-clock."""
    acquire_pid_lock("liquidation_map_collector")
    cleanup_coinglass_tabs()
    logger.info("=== Liquidation Map Daemon Active (Cadence: 0th & 30th minute of every hour) ===")
    
    # Run initial sweep immediately on startup
    await execute_liquidation_sweep()

    while True:
        wait_secs = get_seconds_until_next_sweep()
        next_dt = datetime.now() + timedelta(seconds=wait_secs)
        logger.info(f"Next liquidation sweep scheduled at {next_dt.strftime('%H:%M:%S')} (in {round(wait_secs / 60, 1)} mins). Sleeping...")
        await asyncio.sleep(wait_secs)

        # Trigger sweep at :00:05 or :30:05
        await execute_liquidation_sweep()


if __name__ == "__main__":
    asyncio.run(run_collector_daemon())
