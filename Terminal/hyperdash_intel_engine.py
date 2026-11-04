"""
========================================================================================
Hyperdash Institutional Intelligence Engine (Zero-Docker High-Speed GraphQL Client)
========================================================================================
Direct, high-speed, unauthenticated client querying Hyperdash's GraphQL API:
- GetPerpsMarketParticipation: 347 markets x 11 cohorts (extremely_profitable, apex, rekt)
- GetTickerPositions: Top whale position book per asset (notional, entry, UPnL, liquidation)

Computes quantitative smart money vs crowd positioning features:
- smart_bias: signed skew (-100 to +100) of profitable/apex cohorts
- crowd_bias: signed skew (-100 to +100) of unprofitable/rekt cohorts
- crowd_squeeze: crowd trapped counter to smart money (high probability short/long squeeze)
- composite intel_score: 0-100 multi-cohort conviction score
========================================================================================
"""

import os
import sys
import json
import time
import math
import logging
import urllib.request
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import writer_lock, atomic_json, atomic_parquet

logger = logging.getLogger("HyperdashIntel")
DB_DIR = ROOT_DIR / "Database"
HD_DIR = DB_DIR / "Hyperdash_Data"
HD_DIR.mkdir(parents=True, exist_ok=True)

PARTICIPATION_CACHE_FILE = HD_DIR / "market_participation.parquet"
FEATURES_CACHE_FILE = HD_DIR / "hyperdash_features.parquet"

GRAPHQL_URL = "https://api.hyperdash.com/graphql"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

Q_PARTICIPATION = """query GetPerpsMarketParticipation {
  analytics {
    perpsMarketParticipation {
      timestamp
      global { coin longTraderCount shortTraderCount profitableTraderCount losingTraderCount totalLongNotional totalShortNotional }
      sizeCohorts { cohortId cohortLabel markets { coin longTraderCount shortTraderCount profitableTraderCount losingTraderCount totalLongNotional totalShortNotional } }
      pnlCohorts { cohortId cohortLabel markets { coin longTraderCount shortTraderCount profitableTraderCount losingTraderCount totalLongNotional totalShortNotional } }
    }
  }
}"""

Q_TICKER_POSITIONS = """query GetTickerPositions($coin: String!, $limit: Int) {
  analytics {
    perpsTickerPositions(coin: $coin, limit: $limit) {
      coin
      totalLongNotional
      totalShortNotional
      totalNotional
      longCount
      shortCount
      positions {
        address
        displayName
        label
        size
        notionalSize
        entryPrice
        liquidationPrice
        unrealizedPnl
      }
    }
  }
}"""

# Cohort classification weights
SMART_WEIGHTS = {
    "extremely_profitable": 2.5,
    "Extremely Profitable": 2.5,
    "very_profitable": 2.0,
    "Very Profitable": 2.0,
    "apex": 3.0,
    "Apex": 3.0,
    "profitable": 1.0,
    "Profitable": 1.0,
    "whale": 2.0,
    "Whale": 2.0,
    "shark": 1.5,
    "Shark": 1.5
}

CROWD_WEIGHTS = {
    "unprofitable": 1.5,
    "Unprofitable": 1.5,
    "rekt": 2.5,
    "Rekt": 2.5,
    "dolphin": 0.8,
    "fish": 1.0,
    "Fish": 1.0
}


def normalize_coin_symbol(symbol: str) -> str:
    """Normalizes exchange symbol to base coin (e.g. BTCUSD -> BTC, XRPUSDT -> XRP)."""
    s = str(symbol or "").upper()
    for suffix in [".PII", ".PI", ".P", "USDT", "USDC", "USD"]:
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[:-len(suffix)]
            break
    return s


def compute_net_bias(long_usd: float, short_usd: float) -> float:
    """Signed skew percentage: +100 = 100% long, -100 = 100% short."""
    gross = float(long_usd) + float(short_usd)
    if gross <= 0.0:
        return 0.0
    return max(-100.0, min(100.0, 100.0 * (float(long_usd) - float(short_usd)) / gross))


class HyperdashIntelEngine:
    """Lightweight asynchronous / synchronous Hyperdash Intelligence Engine."""

    def __init__(self, cache_ttl_sec: float = 60.0):
        self.cache_ttl = cache_ttl_sec
        self._last_participation_ts = 0.0
        self._participation_cache: Dict[str, Any] = {}
        self._coin_features: Dict[str, Dict[str, Any]] = {}
        self._ticker_positions_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

    def _execute_query(self, query: str, variables: Optional[Dict[str, Any]] = None, timeout: float = 8.0) -> Optional[Dict[str, Any]]:
        """Executes a GraphQL query against Hyperdash with standard browser headers."""
        try:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            data_bytes = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                GRAPHQL_URL,
                data=data_bytes,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://hyperdash.com",
                    "Referer": "https://hyperdash.com/"
                }
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                res = json.loads(resp.read().decode("utf-8"))
                return res.get("data")
        except Exception as e:
            logger.debug(f"Hyperdash query error: {e}")
            return None

    def refresh_participation(self, force: bool = False) -> bool:
        """Pulls market participation across all 347 markets x 11 cohorts."""
        now = time.time()
        if not force and self._participation_cache and (now - self._last_participation_ts) < self.cache_ttl:
            return True

        data = self._execute_query(Q_PARTICIPATION)
        if not data:
            return False

        part = data.get("analytics", {}).get("perpsMarketParticipation", {})
        if not part:
            return False

        self._participation_cache = part
        self._last_participation_ts = now
        self._compute_all_features(part)
        return True

    def _compute_all_features(self, part: Dict[str, Any]):
        """Derives smart money bias, crowd bias, and squeeze signals per coin."""
        pnl_cohorts = part.get("pnlCohorts", [])
        size_cohorts = part.get("sizeCohorts", [])
        global_mkts = {m.get("coin"): m for m in part.get("global", []) if m.get("coin")}

        # Aggregate per coin across cohorts
        coin_smart: Dict[str, List[Tuple[float, float, float]]] = {}  # coin -> list of (bias, notional, weight)
        coin_crowd: Dict[str, List[Tuple[float, float, float]]] = {}

        all_cohorts = pnl_cohorts + size_cohorts
        for c in all_cohorts:
            label = str(c.get("cohortLabel") or "")
            is_smart = label in SMART_WEIGHTS
            is_crowd = label in CROWD_WEIGHTS
            if not is_smart and not is_crowd:
                continue

            weight = SMART_WEIGHTS.get(label, CROWD_WEIGHTS.get(label, 1.0))
            for m in c.get("markets", []):
                coin = str(m.get("coin") or "").upper()
                l_usd = float(m.get("totalLongNotional") or 0.0)
                s_usd = float(m.get("totalShortNotional") or 0.0)
                gross = l_usd + s_usd
                if gross <= 0:
                    continue
                bias = compute_net_bias(l_usd, s_usd)
                if is_smart:
                    coin_smart.setdefault(coin, []).append((bias, gross, weight))
                if is_crowd:
                    coin_crowd.setdefault(coin, []).append((bias, gross, weight))

        # Compute composite features for all active coins
        features_map = {}
        all_coins = set(global_mkts.keys()) | set(coin_smart.keys())

        for coin in all_coins:
            # Weighted smart money bias
            s_num = sum(b * g * w for b, g, w in coin_smart.get(coin, []))
            s_den = sum(g * w for b, g, w in coin_smart.get(coin, []))
            smart_bias = (s_num / s_den) if s_den > 0 else 0.0

            # Weighted crowd bias
            c_num = sum(b * g * w for b, g, w in coin_crowd.get(coin, []))
            c_den = sum(g * w for b, g, w in coin_crowd.get(coin, []))
            crowd_bias = (c_num / c_den) if c_den > 0 else 0.0

            glob = global_mkts.get(coin, {})
            tot_long = float(glob.get("totalLongNotional") or 0.0)
            tot_short = float(glob.get("totalShortNotional") or 0.0)
            open_interest = tot_long + tot_short

            # Signal Catalog
            signals = []
            if smart_bias >= 15.0:
                signals.append("SMART_MONEY_ACCUM")
            elif smart_bias <= -15.0:
                signals.append("SMART_MONEY_DIST")

            # Crowd Squeeze detection (Smart money opposing crowded retail)
            if smart_bias >= 10.0 and crowd_bias <= -20.0:
                signals.append("CROWD_SQUEEZE_LONG")
            elif smart_bias <= -10.0 and crowd_bias >= 20.0:
                signals.append("CROWD_SQUEEZE_SHORT")

            # Direction & Composite Score (0-100)
            net_divergence = smart_bias - crowd_bias
            if smart_bias > 5.0 and net_divergence > 10.0:
                intel_direction = "BULLISH"
                intel_score = min(100.0, 50.0 + (abs(smart_bias) * 0.35) + (max(0.0, net_divergence) * 0.25))
            elif smart_bias < -5.0 and net_divergence < -10.0:
                intel_direction = "BEARISH"
                intel_score = min(100.0, 50.0 + (abs(smart_bias) * 0.35) + (abs(min(0.0, net_divergence)) * 0.25))
            else:
                intel_direction = "NEUTRAL"
                intel_score = 50.0

            features_map[coin] = {
                "coin": coin,
                "data_valid": True,
                "smart_bias": round(smart_bias, 1),
                "crowd_bias": round(crowd_bias, 1),
                "net_divergence": round(net_divergence, 1),
                "open_interest_usd": open_interest,
                "intel_score": round(intel_score, 1),
                "intel_direction": intel_direction,
                "signals": signals,
                "updated_at": time.time()
            }

        self._coin_features = features_map

    def get_asset_intel(self, symbol: str) -> Dict[str, Any]:
        """Returns institutional intelligence features for a given trading symbol."""
        coin = normalize_coin_symbol(symbol)
        now = time.time()

        # Check or refresh cached participation
        if not self._coin_features or (now - self._last_participation_ts) >= self.cache_ttl:
            self.refresh_participation()

        feat = self._coin_features.get(coin)
        if not feat:
            # Fallback if coin not found in universe
            return {
                "coin": coin,
                "symbol": symbol,
                "data_valid": False,
                "smart_bias": 0.0,
                "crowd_bias": 0.0,
                "intel_score": 50.0,
                "intel_direction": "NEUTRAL",
                "signals": [],
                "top_whales": []
            }

        res = dict(feat)
        res["symbol"] = symbol

        # Fetch live top whale position book for this coin (cached 90s)
        whale_cached = self._ticker_positions_cache.get(coin)
        if whale_cached and (now - whale_cached[0]) < 90.0:
            res["top_whales"] = whale_cached[1]
        else:
            whale_data = self._fetch_top_whales(coin)
            self._ticker_positions_cache[coin] = (now, whale_data)
            res["top_whales"] = whale_data

        return res

    def _fetch_top_whales(self, coin: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Retrieves top whale positions for a given coin."""
        try:
            data = self._execute_query(Q_TICKER_POSITIONS, {"coin": coin, "limit": limit}, timeout=4.0)
            if not data:
                return []
            tp = data.get("analytics", {}).get("perpsTickerPositions", {})
            positions = tp.get("positions", [])
            out = []
            for p in positions:
                out.append({
                    "address": str(p.get("address", ""))[:10] + "...",
                    "label": p.get("label") or p.get("displayName") or "Whale",
                    "notional_usd": float(p.get("notionalSize") or 0.0),
                    "entry_price": float(p.get("entryPrice") or 0.0),
                    "unrealized_pnl": float(p.get("unrealizedPnl") or 0.0)
                })
            return out
        except Exception:
            return []


# Global singleton engine
hyperdash_engine = HyperdashIntelEngine()
