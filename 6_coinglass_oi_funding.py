"""
6_coinglass_oi_funding.py
========================================================================================
Pillar 6: Aggregated Open Interest & Predicted Funding Rate Squeeze Telemetry
========================================================================================
Captures real-time Open Interest (USD) and Predicted Funding Rates across major crypto assets.
Identifies high-conviction short squeeze and long squeeze pressure dynamics.
"""

import sys
import time
import json
import logging
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent
DB_DIR = ROOT_DIR / "Database"
MARKET_DIR = DB_DIR / "Market_Data"
MARKET_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = MARKET_DIR / "oi_and_funding.parquet"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [OI_FUNDING] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("OIFundingTelemetry")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

from coinglass_common import atomic_parquet, acquire_singleton

SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "LTC", "AVAX", "DOT", "NEAR"]


def fetch_binance_funding_and_oi(symbol: str) -> Optional[dict]:
    """Fetches real-time Open Interest and Funding Rate via Binance public endpoints without API keys."""
    pair = f"{symbol}USDT"
    try:
        # 1. Fetch Open Interest
        oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={pair}"
        req_oi = urllib.request.Request(oi_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_oi, timeout=4) as resp:
            oi_data = json.loads(resp.read().decode())
        open_interest = float(oi_data.get("openInterest", 0.0))
        oi_time = int(oi_data.get("time", int(time.time() * 1000)))

        # 2. Fetch Premium Index / Predicted Funding Rate
        fr_url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={pair}"
        req_fr = urllib.request.Request(fr_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_fr, timeout=4) as resp:
            fr_data = json.loads(resp.read().decode())
        
        last_funding_rate = float(fr_data.get("lastFundingRate", 0.0))
        next_funding_time = int(fr_data.get("nextFundingTime", 0))
        mark_price = float(fr_data.get("markPrice", 0.0))
        index_price = float(fr_data.get("indexPrice", 0.0))

        oi_usd = open_interest * mark_price

        # Squeeze classification
        # Negative funding rate + high OI = Short Squeeze risk
        # Overheated positive funding (> +0.03%) = Long Squeeze / Flash crash risk
        squeeze_bias = "NEUTRAL"
        if last_funding_rate <= -0.0001:
            squeeze_bias = "BULLISH_SHORT_SQUEEZE"
        elif last_funding_rate >= 0.0003:
            squeeze_bias = "BEARISH_LONG_SQUEEZE"

        return {
            "symbol": symbol,
            "pair": pair,
            "open_interest": open_interest,
            "open_interest_usd": round(oi_usd, 2),
            "mark_price": mark_price,
            "index_price": index_price,
            "funding_rate": last_funding_rate,
            "funding_rate_pct": round(last_funding_rate * 100.0, 4),
            "next_funding_time": next_funding_time,
            "squeeze_bias": squeeze_bias,
            "timestamp_ms": oi_time,
            "updated_at_utc": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.debug(f"[{symbol}] Failed to fetch OI/Funding: {e}")
        return None


def run_sweep() -> bool:
    acquire_singleton("coinglass_oi_funding", DB_DIR / ".locks")
    logger.info(f"Starting Open Interest & Funding Rate sweep across {len(SYMBOLS)} crypto pairs...")
    records = []
    t0 = time.time()
    for sym in SYMBOLS:
        rec = fetch_binance_funding_and_oi(sym)
        if rec:
            records.append(rec)
            bias_tag = f" [{rec['squeeze_bias']}]" if rec['squeeze_bias'] != "NEUTRAL" else ""
            logger.info(f"Harvested {sym}: OI=${rec['open_interest_usd']:,.0f}, FR={rec['funding_rate_pct']:+.4f}%{bias_tag}")
        time.sleep(0.05)

    if records:
        df = pd.DataFrame(records)
        atomic_parquet(df, OUT_PARQUET)
        logger.info(f"Successfully persisted {len(df)} OI & Funding telemetry records to {OUT_PARQUET.name} in {time.time()-t0:.2f}s.")
        return True
    return False


if __name__ == "__main__":
    run_sweep()
