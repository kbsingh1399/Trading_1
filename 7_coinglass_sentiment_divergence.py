"""
7_coinglass_sentiment_divergence.py
========================================================================================
Pillar 7: Top Trader vs Global Sentiment Divergence Telemetry (Smart vs Retail)
========================================================================================
Measures divergence between elite institutional positioning (Top Trader Long/Short Ratio)
and retail crowds (Global Accounts Long/Short Ratio).
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
OUT_PARQUET = MARKET_DIR / "sentiment_divergence.parquet"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [SENTIMENT] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("SentimentDivergence")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

from coinglass_common import atomic_parquet, acquire_singleton

SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "LTC"]


def fetch_sentiment_ratios(symbol: str) -> Optional[dict]:
    pair = f"{symbol}USDT"
    try:
        # 1. Top Trader Long/Short Position Ratio
        top_url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={pair}&period=15m&limit=2"
        req_top = urllib.request.Request(top_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_top, timeout=4) as resp:
            top_data = json.loads(resp.read().decode())
        if not top_data:
            return None
        latest_top = top_data[-1]
        top_long_pct = float(latest_top.get("longAccount", latest_top.get("longPosition", 0.5))) * 100.0
        top_short_pct = float(latest_top.get("shortAccount", latest_top.get("shortPosition", 0.5))) * 100.0
        top_ls_ratio = float(latest_top.get("longShortRatio", 1.0))

        # 2. Global Retail Accounts Long/Short Ratio
        global_url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={pair}&period=15m&limit=2"
        req_global = urllib.request.Request(global_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_global, timeout=4) as resp:
            global_data = json.loads(resp.read().decode())
        if not global_data:
            return None
        latest_global = global_data[-1]
        global_long_pct = float(latest_global.get("longAccount", 0.5)) * 100.0
        global_short_pct = float(latest_global.get("shortAccount", 0.5)) * 100.0
        global_ls_ratio = float(latest_global.get("longShortRatio", 1.0))

        # Compute Smart Money Divergence Spread
        # Positive divergence: Top traders are significantly more long than retail
        # Negative divergence: Top traders are heavily short while retail is long (bull trap)
        divergence_spread = top_long_pct - global_long_pct

        sentiment_bias = "NEUTRAL"
        if divergence_spread >= 15.0:
            sentiment_bias = "SMART_MONEY_ACCUMULATION"
        elif divergence_spread <= -15.0:
            sentiment_bias = "INSTITUTIONAL_DISTRIBUTION"

        return {
            "symbol": symbol,
            "pair": pair,
            "top_long_pct": round(top_long_pct, 2),
            "top_short_pct": round(top_short_pct, 2),
            "top_ls_ratio": round(top_ls_ratio, 3),
            "global_long_pct": round(global_long_pct, 2),
            "global_short_pct": round(global_short_pct, 2),
            "global_ls_ratio": round(global_ls_ratio, 3),
            "divergence_spread": round(divergence_spread, 2),
            "sentiment_bias": sentiment_bias,
            "timestamp_ms": int(latest_top.get("timestamp", int(time.time() * 1000))),
            "updated_at_utc": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.debug(f"[{symbol}] Failed sentiment query: {e}")
        return None


def run_sweep() -> bool:
    acquire_singleton("coinglass_sentiment_divergence", DB_DIR / ".locks")
    logger.info(f"Starting Sentiment Divergence sweep across {len(SYMBOLS)} crypto pairs...")
    records = []
    t0 = time.time()
    for sym in SYMBOLS:
        rec = fetch_sentiment_ratios(sym)
        if rec:
            records.append(rec)
            tag = f" [{rec['sentiment_bias']}]" if rec['sentiment_bias'] != "NEUTRAL" else ""
            logger.info(f"Harvested {sym}: Top L/S={rec['top_ls_ratio']:.2f} ({rec['top_long_pct']:.1f}%L), Global L/S={rec['global_ls_ratio']:.2f} ({rec['global_long_pct']:.1f}%L) -> Div: {rec['divergence_spread']:+.1f}%{tag}")
        time.sleep(0.05)

    if records:
        df = pd.DataFrame(records)
        atomic_parquet(df, OUT_PARQUET)
        logger.info(f"Successfully persisted {len(df)} Sentiment Divergence records to {OUT_PARQUET.name} in {time.time()-t0:.2f}s.")
        return True
    return False


if __name__ == "__main__":
    run_sweep()
