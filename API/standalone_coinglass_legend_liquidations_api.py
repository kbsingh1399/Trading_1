"""
standalone_coinglass_legend_liquidations_api.py
========================================================================================
Institutional Standalone CoinGlass Legend Live Liquidation Harvester (5-Minute Bars)
========================================================================================

Architecture & Operational Invariants:
- Targets: https://legend.coinglass.com/chart/ (Template 34fb48203c544d2d82a76477ae00aa9a)
- Operates via Chrome DevTools Protocol (CDP port 9222).
- Zero DOM Fragility: Hooks into CoinGlass Legend React Canvas/DataFeed runtime 
  (`window.__chartInstance`) to programmatically switch symbols sub-second.
- Harvests 5-minute Aggregated Realized Liquidation K-Lines (Long & Short Liq in USD).
  Because the bottom indicator aggregates liquidations across all derivative exchanges
  (Binance, Bybit, OKX, etc.), numbers reflect total market liquidation volume.
- Schema Parity (Exact User Excel Specification):
    * Column A: Candle_Type      -> "5_Minute"
    * Column B: Time             -> "YYYY-MM-DD HH:MM:SS" (IST)
    * Column C: Long_Liquidation  -> float (USD)
    * Column D: Short_Liquidation -> float (USD)
    * Helper  : timestamp        -> int64 (UTC ms for chronological sort & deduplication)
- Persistence:
    * Primary: Database/Live_Liquidation_Dump/<ASSET>_Live_Liquidation.parquet
    * Mirror : Database/Live_Liquidations/<ASSET>_Live_Liquidation.parquet
    * Incremental History: Reads existing parquets, merges, deduplicates by Time, 
      and atomically writes back using Windows re-entrant writer_lock and atomic_parquet.
- Modes:
    * --once    : Execute a single complete sweep across the active asset universe.
    * --loop    : Continuous daemon running every N seconds (default: 300s / 5m).
    * --symbols : Comma-separated list of symbols (defaults to API/active_assets.json).
========================================================================================
"""

import sys
import os
import json
import time
import asyncio
import logging
import argparse
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple, Any

import pandas as pd
import websockets

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent.parent
API_DIR = Path(__file__).resolve().parent
DB_DIR = ROOT_DIR / "Database"
DUMP_DIR = DB_DIR / "Live_Liquidation_Dump"
MIRROR_DIR = DB_DIR / "Live_Liquidations"

DUMP_DIR.mkdir(parents=True, exist_ok=True)
MIRROR_DIR.mkdir(parents=True, exist_ok=True)

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from coinglass_common import writer_lock, atomic_parquet
except ImportError:
    writer_lock = None
    atomic_parquet = None

IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [LEGEND-LIQ] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LegendLiquidationAPI")

LEGEND_TEMPLATE_URL = "https://legend.coinglass.com/chart/34fb48203c544d2d82a76477ae00aa9a"

# Default specs for active assets
DEFAULT_ASSET_SPECS = {
    "BTC": {"ex": "Binance", "instId": "BTCUSDT", "name": "6:BTC:USDT:1", "prec": 1, "tick": 0.1, "coin": "Bitcoin", "ticks": ["0.1", "1", "10", "100", "1000"]},
    "ETH": {"ex": "Binance", "instId": "ETHUSDT", "name": "6:ETH:USDT:1", "prec": 2, "tick": 0.01, "coin": "Ethereum", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "SOL": {"ex": "Binance", "instId": "SOLUSDT", "name": "6:SOL:USDT:1", "prec": 2, "tick": 0.01, "coin": "Solana", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "XRP": {"ex": "Binance", "instId": "XRPUSDT", "name": "6:XRP:USDT:1", "prec": 4, "tick": 0.0001, "coin": "XRP", "ticks": ["0.0001", "0.001", "0.01", "0.1", "1"]},
    "BNB": {"ex": "Binance", "instId": "BNBUSDT", "name": "6:BNB:USDT:1", "prec": 2, "tick": 0.01, "coin": "BNB", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "DOGE": {"ex": "Binance", "instId": "DOGEUSDT", "name": "6:DOGE:USDT:1", "prec": 5, "tick": 0.00001, "coin": "Dogecoin", "ticks": ["0.00001", "0.0001", "0.001", "0.01", "0.1"]},
    "ADA": {"ex": "Binance", "instId": "ADAUSDT", "name": "6:ADA:USDT:1", "prec": 4, "tick": 0.0001, "coin": "Cardano", "ticks": ["0.0001", "0.001", "0.01", "0.1", "1"]},
    "LTC": {"ex": "Binance", "instId": "LTCUSDT", "name": "6:LTC:USDT:1", "prec": 2, "tick": 0.01, "coin": "Litecoin", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "LINK": {"ex": "Binance", "instId": "LINKUSDT", "name": "6:LINK:USDT:1", "prec": 3, "tick": 0.001, "coin": "Chainlink", "ticks": ["0.001", "0.01", "0.1", "1", "10"]},
    "AVAX": {"ex": "Binance", "instId": "AVAXUSDT", "name": "6:AVAX:USDT:1", "prec": 3, "tick": 0.001, "coin": "Avalanche", "ticks": ["0.001", "0.01", "0.1", "1", "10"]},
    "PAXG": {"ex": "Binance", "instId": "PAXGUSDT", "name": "6:PAXG:USDT:1", "prec": 2, "tick": 0.01, "coin": "PAX Gold", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "XAUT": {"ex": "Bybit", "instId": "XAUTUSDT", "name": "1:XAUT:USDT:1", "prec": 2, "tick": 0.01, "coin": "Tether Gold", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "XAU": {"ex": "Binance", "instId": "PAXGUSDT", "name": "6:PAXG:USDT:1", "prec": 2, "tick": 0.01, "coin": "PAX Gold", "ticks": ["0.01", "0.1", "1", "10", "100"]},
    "XAG": {"ex": "Bybit", "instId": "XAGUSD", "name": "1:XAG:USD:1", "prec": 3, "tick": 0.001, "coin": "Silver", "ticks": ["0.001", "0.01", "0.1", "1", "10"]}
}


def load_active_universe() -> List[str]:
    """Load active assets from dynamic active_assets.json registry."""
    reg_path = API_DIR / "active_assets.json"
    if reg_path.exists():
        try:
            with open(reg_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                active = data.get("active_assets", [])
                if active:
                    return active
        except Exception as e:
            logger.warning(f"Could not parse active_assets.json: {e}")
    # Fallback to institutional default
    return ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LTC", "LINK", "AVAX", "PAXG", "XAUT"]


def get_cdp_tab(cdp_port: int = 9222) -> Tuple[Optional[str], Optional[str]]:
    """Locate existing legend tab or create a new one."""
    try:
        url = f"http://127.0.0.1:{cdp_port}/json"
        with urllib.request.urlopen(url, timeout=5) as resp:
            tabs = json.loads(resp.read().decode())
    except Exception as e:
        logger.error(f"CDP connection error on port {cdp_port}: {e}")
        return None, None

    # Check for existing legend tab
    for t in tabs:
        t_url = t.get("url", "")
        if "legend.coinglass.com" in t_url:
            return t.get("webSocketDebuggerUrl"), t.get("id")

    # If not found, open a new tab
    try:
        create_url = f"http://127.0.0.1:{cdp_port}/json/new?{LEGEND_TEMPLATE_URL}"
        req = urllib.request.Request(create_url, method="PUT")
        with urllib.request.urlopen(req, timeout=10) as resp:
            new_tab = json.loads(resp.read().decode())
            logger.info(f"Opened new Legend tab (id: {new_tab.get('id')})")
            time.sleep(3.0)  # Allow page to bootstrap
            return new_tab.get("webSocketDebuggerUrl"), new_tab.get("id")
    except Exception as e:
        logger.error(f"Failed to open Legend tab via CDP: {e}")
        return None, None


class LegendLiquidationHarvester:
    """Orchestrates asset switching, indicator extraction, and incremental parquet dumps."""

    def __init__(self, cdp_port: int = 9222):
        self.cdp_port = cdp_port
        self.ws_url: Optional[str] = None
        self.tab_id: Optional[str] = None
        self.ws = None

    async def connect(self) -> bool:
        """Connects to CDP WebSocket."""
        self.ws_url, self.tab_id = get_cdp_tab(self.cdp_port)
        if not self.ws_url:
            logger.error("Could not obtain Legend WebSocket debugger URL.")
            return False

        try:
            self.ws = await websockets.connect(self.ws_url, max_size=30_000_000, ping_interval=20, ping_timeout=15)
            logger.info("Connected to Legend CDP WebSocket successfully.")
            await self._hook_chart_instance()
            return True
        except Exception as e:
            logger.error(f"WebSocket connection failure: {e}")
            return False

    async def _evaluate(self, expression: str, await_promise: bool = False) -> Any:
        """Executes JavaScript expression in target page."""
        if not self.ws:
            return None
        msg_id = int(time.time() * 1000) % 1_000_000
        payload = {
            "id": msg_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": await_promise
            }
        }
        await self.ws.send(json.dumps(payload))
        resp = await self.ws.recv()
        data = json.loads(resp)
        result = data.get("result", {}).get("result", {})
        return result.get("value")

    async def _hook_chart_instance(self) -> bool:
        """Locates and exposes the live Chart instance on window.__chartInstance."""
        script = """
        (() => {
            const chartDiv = document.getElementById("legend_chart_1") || document.querySelector('[id*="legend_chart"]');
            if (!chartDiv) return { success: false, reason: "No legend_chart_1 div found" };

            const reactKey = Object.keys(chartDiv).find(k => k.startsWith('__reactFiber'));
            if (!reactKey) return { success: false, reason: "No React Fiber on chartDiv" };

            let cur = chartDiv[reactKey];
            let classComp = null;
            while (cur) {
                if (cur.stateNode && cur.stateNode.props && cur.stateNode.props.symbol) {
                    classComp = cur.stateNode;
                    break;
                }
                cur = cur.return;
            }

            if (!classComp) return { success: false, reason: "No classComp stateNode found" };

            window.__chartInstance = classComp;
            
            // Ensure interval is set to 5m
            if (classComp.state && classComp.state.interval !== 'm5') {
                classComp.onChangeInterval('m5');
            }

            return {
                success: true,
                symbol: classComp.state ? classComp.state.symbol : null,
                interval: classComp.state ? classComp.state.interval : null
            };
        })()
        """
        for attempt in range(1, 6):
            res = await self._evaluate(script)
            if res and res.get("success"):
                logger.info(f"Hooked Chart instance: Symbol={res.get('symbol')}, Interval={res.get('interval')}")
                return True
            logger.info(f"Waiting for chart runtime to initialize (attempt {attempt}/5)...")
            await asyncio.sleep(2.0)

        logger.error(f"Failed to hook chart instance: {res}")
        return False

    async def switch_asset(self, asset: str) -> bool:
        """Programmatically switches asset via React instance or Search dialog fallback."""
        spec = DEFAULT_ASSET_SPECS.get(asset.upper(), {
            "ex": "Binance",
            "instId": f"{asset.upper()}USDT",
            "name": f"6:{asset.upper()}:USDT:1",
            "prec": 2,
            "tick": 0.01,
            "coin": asset.upper(),
            "ticks": ["0.01", "0.1", "1", "10", "100"]
        })

        ticker_payload = {
            "coinName": spec["coin"],
            "exchangeName": spec["ex"],
            "symbol": asset.upper(),
            "ticks": spec["ticks"],
            "instrument": {
                "baseAsset": asset.upper(),
                "exName": spec["ex"],
                "instrumentId": spec["instId"],
                "name": spec["name"],
                "pricePrecision": spec["prec"],
                "priceTick": spec["tick"],
                "quoteAsset": "USDT",
                "type": 1
            }
        }

        # 1. Direct JS call
        script_direct = f"""
        (() => {{
            const inst = window.__chartInstance;
            if (!inst || typeof inst.onChangeSymbol !== 'function') return {{ success: false, reason: "No onChangeSymbol" }};
            const ticker = {json.dumps(ticker_payload)};
            inst.onChangeSymbol(ticker);
            return {{ success: true }};
        }})()
        """
        res = await self._evaluate(script_direct)
        if res and res.get("success"):
            return True

        # 2. UI search dialog fallback
        script_ui = f"""
        (async () => {{
            let input = document.querySelector('input[placeholder="Search"]');
            if (!input) {{
                const all = Array.from(document.querySelectorAll('*'));
                const el = all.find(e => (e.title && e.title.includes('Search Trading Pairs')) || (e.getAttribute('aria-label') && e.getAttribute('aria-label').includes('Search Trading Pairs')));
                if (el) {{
                    const btn = el.closest('button') || el;
                    btn.click();
                    await new Promise(r => setTimeout(r, 400));
                    input = document.querySelector('input[placeholder="Search"]');
                }}
            }}
            if (!input) return {{ success: false, reason: "No input found" }};
            
            const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
            nativeSetter.call(input, "{spec['instId']}");
            input.dispatchEvent(new Event('input', {{ bubbles: true }}));
            await new Promise(r => setTimeout(r, 500));
            
            const items = Array.from(document.querySelectorAll('.dialog-list-item'));
            let target = items.find(it => it.innerText.includes("{spec['ex']}") && it.innerText.includes("{spec['instId']}"));
            if (!target && items.length > 0) target = items[0];
            
            if (target) {{
                target.click();
                return {{ success: true }};
            }}
            return {{ success: false, reason: "Item not found in search" }};
        }})()
        """
        res_ui = await self._evaluate(script_ui, await_promise=True)
        return bool(res_ui and res_ui.get("success"))

    async def wait_for_data(self, asset: str, timeout: float = 6.0) -> Optional[List[Dict[str, Any]]]:
        """Polls indicatorDatas until the target asset's aggregated_liq_kline is loaded."""
        spec = DEFAULT_ASSET_SPECS.get(asset.upper(), {})
        target_token = spec.get("instId", asset.upper()).split("USDT")[0].upper()

        start = time.time()
        while time.time() - start < timeout:
            script = f"""
            (() => {{
                const inst = window.__chartInstance;
                if (!inst || !inst.indicatorDatas) return null;
                const keys = Object.keys(inst.indicatorDatas);
                const liqKey = keys.find(k => k.includes('aggregated_liq_kline') && (k.toUpperCase().includes('{asset.upper()}') || k.toUpperCase().includes('{target_token}')));
                if (!liqKey) return null;
                const arr = inst.indicatorDatas[liqKey];
                if (!arr || arr.length === 0) return null;
                return arr;
            }})()
            """
            data = await self._evaluate(script)
            if data and isinstance(data, list) and len(data) > 0:
                return data
            await asyncio.sleep(0.5)
        return None

    def persist_asset_parquets(self, asset: str, raw_data: List[Dict[str, Any]]) -> Tuple[int, int, str]:
        """
        Processes raw bars into exact Excel format:
        [Candle_Type, Time, Long_Liquidation, Short_Liquidation, timestamp]
        Merges with existing parquet incrementally, deduplicating on timestamp.
        """
        rows = []
        for item in raw_data:
            ts_ms = item.get("timestamp")
            if not ts_ms:
                continue
            dt_ist = datetime.fromtimestamp(ts_ms / 1000.0, tz=IST)
            time_str = dt_ist.strftime("%Y-%m-%d %H:%M:%S")
            long_liq = float(item.get("open") or 0.0)
            short_liq = float(item.get("high") or 0.0)
            rows.append({
                "Candle_Type": "5_Minute",
                "Time": time_str,
                "Long_Liquidation": round(long_liq, 2),
                "Short_Liquidation": round(short_liq, 2),
                "timestamp": int(ts_ms)
            })

        new_df = pd.DataFrame(rows)
        if new_df.empty:
            return 0, 0, "Empty data"

        target_file = DUMP_DIR / f"{asset.upper()}_Live_Liquidation.parquet"
        mirror_file = MIRROR_DIR / f"{asset.upper()}_Live_Liquidation.parquet"

        # Incremental merge
        if target_file.exists():
            try:
                existing_df = pd.read_parquet(target_file)
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                combined.dropna(subset=["Time"], inplace=True)
                combined.drop_duplicates(subset=["Time"], keep="last", inplace=True)
                combined.sort_values(by=["Time"], ascending=True, inplace=True)
                combined.reset_index(drop=True, inplace=True)
                final_df = combined
            except Exception as e:
                logger.warning(f"Error reading existing parquet for {asset}: {e}. Overwriting.")
                final_df = new_df
        else:
            new_df.dropna(subset=["Time"], inplace=True)
            new_df.drop_duplicates(subset=["Time"], keep="last", inplace=True)
            new_df.sort_values(by=["Time"], ascending=True, inplace=True)
            new_df.reset_index(drop=True, inplace=True)
            final_df = new_df

        # Save atomically
        if atomic_parquet and writer_lock:
            with writer_lock(str(target_file)):
                atomic_parquet(final_df, target_file)
            with writer_lock(str(mirror_file)):
                atomic_parquet(final_df, mirror_file)
        else:
            final_df.to_parquet(target_file, index=False)
            final_df.to_parquet(mirror_file, index=False)

        non_zero = int(((final_df["Long_Liquidation"] > 0) | (final_df["Short_Liquidation"] > 0)).sum())
        total = len(final_df)
        return total, non_zero, str(target_file)

    async def harvest_universe(self, symbols: List[str]) -> Dict[str, Any]:
        """Harvests liquidation bars across all target symbols."""
        results = {}
        for idx, asset in enumerate(symbols, 1):
            logger.info(f"[{idx}/{len(symbols)}] Harvesting {asset}...")
            switched = await self.switch_asset(asset)
            if not switched:
                logger.warning(f"Could not switch to {asset}. Skipping.")
                results[asset] = {"status": "FAILED", "reason": "Switch failed"}
                continue

            raw_data = await self.wait_for_data(asset, timeout=5.0)
            if not raw_data:
                logger.warning(f"No aggregated liquidation data received for {asset}.")
                results[asset] = {"status": "NO_DATA"}
                continue

            total, non_zero, path = self.persist_asset_parquets(asset, raw_data)
            results[asset] = {
                "status": "SUCCESS",
                "total_candles": total,
                "non_zero_candles": non_zero,
                "path": path
            }
            logger.info(f"  -> {asset}: {total} bars ({non_zero} active liqs) dumped to {path}")
            await asyncio.sleep(0.3)

        return results

    async def close(self):
        """Closes CDP WebSocket."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


async def run_collector(symbols: Optional[List[str]] = None, once: bool = True, interval_sec: int = 300):
    """Main execution entrypoint."""
    target_symbols = symbols or load_active_universe()
    logger.info(f"Starting Legend Liquidation Scraper. Universe: {target_symbols}")

    harvester = LegendLiquidationHarvester()
    connected = await harvester.connect()
    if not connected:
        logger.error("Could not initialize Harvester. Terminating.")
        return

    try:
        while True:
            t0 = time.time()
            results = await harvester.harvest_universe(target_symbols)
            elapsed = time.time() - t0

            # Log summary table
            success_count = sum(1 for r in results.values() if r.get("status") == "SUCCESS")
            logger.info(f"Sweep complete in {elapsed:.2f}s | Success: {success_count}/{len(target_symbols)}")

            if once:
                break

            logger.info(f"Sleeping {interval_sec}s until next 5m candle cycle...")
            await asyncio.sleep(interval_sec)

    finally:
        await harvester.close()


def main():
    parser = argparse.ArgumentParser(description="CoinGlass Legend 5m Live Liquidation Scraper")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols (e.g. BTC,ETH,SOL)")
    parser.add_argument("--once", action="store_true", default=True, help="Run single sweep and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously in background daemon mode")
    parser.add_argument("--interval", type=int, default=300, help="Interval in seconds for daemon mode (default: 300)")

    args = parser.parse_args()
    sym_list = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    run_once = not args.loop

    asyncio.run(run_collector(symbols=sym_list, once=run_once, interval_sec=args.interval))


if __name__ == "__main__":
    main()
