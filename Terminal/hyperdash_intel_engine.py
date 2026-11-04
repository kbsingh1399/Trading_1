"""
========================================================================================
Hyperdash Institutional Intelligence Engine v2 (Zero-Docker High-Speed GraphQL Client)
========================================================================================
Direct, high-speed, unauthenticated client querying Hyperdash's GraphQL API:
- GetPerpsMarketParticipation: 347 markets x 11 cohorts (extremely_profitable, apex, rekt)
- GetTickerPositions: Top whale position book per asset (notional, entry, UPnL, liquidation)


QUANT REVIEW v2 UPGRADES (this file replaces the previous engine wholesale):
1. WHALE LIQUIDATION SWEEP-TARGET ENGINE (new alpha):
   Retains `size` (signed) and `liquidationPrice` — previously fetched then DISCARDED —
   and clusters whale liquidation prices into notional-weighted sweep magnets with
   proximity-decayed sweep_score. Consumed by Pillar 4 (strike magnets).
2. DISJOINT COHORT WEIGHTING (fixes nested-cohort double counting):
   Hyperdash PnL cohorts are NESTED (apex C subset extremely_profitable C very_profitable
   C profitable). The old scheme weighted apex 3.0 + EP 2.5 + VP 2.0 + profitable 1.0,
   multiplying the same elite notional up to ~6.5x. New incremental scheme weights the
   marginal information of each nesting layer instead.
3. CROWD BIAS BY TRADER HEADCOUNT (herding = bodies, not dollars);
   SMART BIAS BY NOTIONAL (conviction = capital at risk).
4. STALENESS GUARD: features carry `intel_age_sec`; data_valid degrades to False after
   MAX_FEATURE_AGE_SEC so the evaluator can never trade on dead intel.
5. CASE/SPACE-INSENSITIVE cohort label matching + very_unprofitable / shrimp support.
6. NEGATIVE-CACHE on fetch failure (15s retry TTL instead of poisoning cache 90s).
7. THREAD-SAFE refresh (evaluations may fan out on worker threads via asyncio.to_thread).
8. ASYNC WRAPPER get_asset_intel_async() for direct await usage from event loops.


Backward-compatible public surface:
    hyperdash_engine.get_asset_intel(symbol) -> dict (superset of v1 fields)
    hyperdash_engine.refresh_participation(force=False) -> bool
========================================================================================
"""


import os
import sys
import json
import time
import math
import logging
import threading
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


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
MAX_FEATURE_AGE_SEC = 900.0      # 15 min: older participation features -> data_valid=False
WHALE_CACHE_TTL_SEC = 90.0       # positive whale-book cache
WHALE_NEG_CACHE_TTL_SEC = 15.0   # failed fetch: retry quickly instead of caching [] for 90s
SWEEP_NEAR_PCT = 0.25            # ignore clusters closer than this (already cascading)
SWEEP_FAR_PCT = 8.0              # ignore clusters further than this (not reachable fuel)
SWEEP_CLUSTER_TOL_PCT = 0.50     # single-linkage clustering tolerance (% of mark)
SWEEP_MAX_PER_SIDE = 3           # top clusters kept per side
SWEEP_MIN_NOTIONAL_USD = 250_000.0   # cluster smaller than this is noise at top-10 depth
TAKER_FEE_DEFAULT = 0.00075      # 0.075% default taker rate (used by sizing consumers)


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




def _norm_label(label: str) -> str:
    """Normalizes any cohortId/cohortLabel casing or spacing to a canonical key."""
    return "".join(ch for ch in str(label or "").lower() if ch.isalnum())




# ---------------------------------------------------------------------------
# Cohort weighting v2 — INCREMENTAL (nested-cohort aware)
# ---------------------------------------------------------------------------
# Hyperdash PnL cohorts nest: apex C extremely_profitable C very_profitable C profitable.
# A given elite wallet's notional appears in ALL enclosing layers. The v1 weights
# (apex 3.0 / EP 2.5 / VP 2.0 / profitable 1.0) therefore multiplied elite notional
# up to ~6.5x — the *ratio* between apex and a merely-profitable trader's notional
# was really 6.5 : 1.0 rather than the intended 3.0 : 1.0. v2 assigns each nesting
# layer only its MARGINAL information so the apex:profitable ratio is a true 3.0 : 0.1.
# Size cohorts (whale/shark) are capital-conviction proxies, disjoint from PnL tiers.
# ---------------------------------------------------------------------------
SMART_WEIGHTS_NORM: Dict[str, float] = {
    _norm_label(k): v for k, v in {
        "apex": 3.0,
        "extremely_profitable": 1.0,   # was 2.5 — apex already dominates this layer
        "very_profitable": 0.3,        # was 2.0 — nested, marginal info only
        "profitable": 0.1,              # near-baseline tier, mostly noise dilution
        "whale": 1.2,                  # size cohort: capital conviction
        "shark": 0.4,
    }.items()
}


# Crowd = herding headcount. Rekt carries the strongest contrarian signal (pain fuel),
# unprofitable/very_unprofitable are milder herding layers.
CROWD_WEIGHTS_NORM: Dict[str, float] = {
    _norm_label(k): v for k, v in {
        "rekt": 2.5,
        "very_unprofitable": 1.5,      # was MISSING in v1 — silently skipped
        "unprofitable": 0.7,           # was 1.5
        "fish": 0.5,
        "dolphin": 0.3,
        "shrimp": 0.3,                  # was MISSING in v1 — silently skipped
    }.items()
}


_seen_unknown_labels: set = set()




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




def compute_net_bias_count(long_n: float, short_n: float) -> float:
    """Signed skew percentage by TRADER COUNT (herding headcount)."""
    gross = float(long_n) + float(short_n)
    if gross <= 0.0:
        return 0.0
    return max(-100.0, min(100.0, 100.0 * (float(long_n) - float(short_n)) / gross))




def _estimate_mark_from_positions(positions: List[Dict[str, Any]]) -> float:
    """
    Derives an approximate mark price from the largest whale position:
    linear perp PnL identity  UPnL = size * (mark - entry)  =>  mark = entry + UPnL / size
    (works for both sides because `size` is signed).
    """
    best: Optional[Dict[str, Any]] = None
    for p in positions:
        if best is None or abs(float(p.get("notionalSize") or 0.0)) > abs(float(best.get("notionalSize") or 0.0)):
            best = p
    if best:
        try:
            sz = float(best.get("size") or 0.0)
            entry = float(best.get("entryPrice") or 0.0)
            upnl = float(best.get("unrealizedPnl") or 0.0)
            if sz != 0.0 and entry > 0.0:
                mark = entry + upnl / sz
                if mark > 0 and math.isfinite(mark):
                    return mark
        except (ValueError, TypeError):
            pass
    # Fallback: notional-weighted mean entry (worst case: stale by the entry-to-now drift)
    tot = 0.0
    wsum = 0.0
    for p in positions:
        n = abs(float(p.get("notionalSize") or 0.0))
        e = float(p.get("entryPrice") or 0.0)
        if e > 0:
            tot += n * e
            wsum += n
    return (tot / wsum) if wsum > 0 else 0.0




def compute_sweep_targets(
    positions: List[Dict[str, Any]],
    mark_price: float,
    near_pct: float = SWEEP_NEAR_PCT,
    far_pct: float = SWEEP_FAR_PCT,
    cluster_tol_pct: float = SWEEP_CLUSTER_TOL_PCT,
    max_per_side: int = SWEEP_MAX_PER_SIDE,
    min_cluster_notional_usd: float = SWEEP_MIN_NOTIONAL_USD
) -> List[Dict[str, Any]]:
    """
    WHALE LIQUIDATION SWEEP-TARGET ENGINE
    ----------------------------------------------------------------------------
    Clusters whale `liquidationPrice` levels into notional-weighted magnets:
      - SHORT whales liquidate ABOVE price  -> overhead squeeze fuel (BUY targets)
      - LONG  whales liquidate BELOW price  -> downside cascade fuel (SELL targets)
    Each cluster returns:
      side            'SHORT' (liq fuel above mark) or 'LONG' (liq fuel below mark)
      liq_price       notional-weighted mean liquidation level of the cluster
      dist_pct        signed distance from mark in percent
      notional_usd    total whale notional that dies at this level (sweep fuel)
      whale_count     whales in the cluster
      sweep_score     notional_usd * exp(-|dist_pct| / 3.0)  (proximity-decayed fuel)
    Clusters closer than near_pct are treated as already-cascading and ignored;
    clusters further than far_pct are not reachable trade-horizon fuel.
    """
    if not positions or mark_price <= 0:
        return []


    by_side: Dict[str, List[Dict[str, Any]]] = {"SHORT": [], "LONG": []}
    for p in positions:
        try:
            liq = float(p.get("liquidationPrice") or 0.0)
            sz = float(p.get("size") or 0.0)
            notion = abs(float(p.get("notionalSize") or 0.0))
        except (ValueError, TypeError):
            continue
        if liq <= 0.0 or sz == 0.0 or notion <= 0.0:
            continue
        side = "SHORT" if sz < 0 else "LONG"
        dist_pct = (liq - mark_price) / mark_price * 100.0  # signed
        # SHORT liqs must sit above mark; LONG liqs below; within reach window
        if side == "SHORT" and not (near_pct < dist_pct <= far_pct):
            continue
        if side == "LONG" and not (-far_pct <= dist_pct < -near_pct):
            continue
        by_side[side].append({
            "liq": liq, "dist_pct": dist_pct, "notional": notion,
            "label": p.get("label") or p.get("displayName") or "Whale",
        })


    targets: List[Dict[str, Any]] = []
    for side, items in by_side.items():
        if not items:
            continue
        # Single-linkage clustering by liquidation level proximity
        items.sort(key=lambda x: x["liq"])
        clusters: List[List[Dict[str, Any]]] = []
        cur: List[Dict[str, Any]] = []
        for it in items:
            if cur and (abs(it["liq"] - cur[-1]["liq"]) / mark_price * 100.0) > cluster_tol_pct:
                clusters.append(cur)
                cur = [it]
            else:
                cur.append(it)
        if cur:
            clusters.append(cur)


        for cl in clusters:
            total_notional = sum(x["notional"] for x in cl)
            if total_notional < min_cluster_notional_usd:
                continue
            wavg_liq = sum(x["liq"] * x["notional"] for x in cl) / max(total_notional, 1e-9)
            dist_pct = (wavg_liq - mark_price) / mark_price * 100.0
            targets.append({
                "side": side,                                   # side of the LIQUIDATED whales
                "liq_price": round(wavg_liq, 8),
                "dist_pct": round(dist_pct, 3),
                "notional_usd": round(total_notional, 2),
                "notional_musd": round(total_notional / 1e6, 3),
                "whale_count": len(cl),
                "sweep_score": round(total_notional * math.exp(-abs(dist_pct) / 3.0), 2),
            })


    targets.sort(key=lambda t: t["sweep_score"], reverse=True)
    # Keep top-N per side, preserving global ordering
    kept: List[Dict[str, Any]] = []
    per_side: Dict[str, int] = {"SHORT": 0, "LONG": 0}
    for t in targets:
        if per_side.get(t["side"], max_per_side) < max_per_side:
            kept.append(t)
            per_side[t["side"]] += 1
    return kept




class HyperdashIntelEngine:
    """Lightweight asynchronous / synchronous Hyperdash Intelligence Engine (v2)."""


    def __init__(self, cache_ttl_sec: float = 60.0):
        self.cache_ttl = cache_ttl_sec
        self._last_participation_ts = 0.0
        self._participation_cache: Dict[str, Any] = {}
        self._coin_features: Dict[str, Dict[str, Any]] = {}
        self._ticker_positions_cache: Dict[str, Tuple[float, List[Dict[str, Any]], bool]] = {}
        self._refresh_lock = threading.Lock()   # thread-safe: evaluate_signal fans out on worker threads


    # ------------------------------------------------------------------ transport
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


    # ------------------------------------------------------------------ participation
    def refresh_participation(self, force: bool = False) -> bool:
        """Pulls market participation across all 347 markets x 11 cohorts. Thread-safe."""
        with self._refresh_lock:
            now = time.time()
            if not force and self._participation_cache and (now - self._last_participation_ts) < self.cache_ttl:
                return True


            data = self._execute_query(Q_PARTICIPATION)
            if not data:
                return False


            part = data.get("analytics", {}).get("perpsMarketParticipation", {})
            if not part:
                return False


            # Source-freshness audit: if Hyperdash itself reports a stale snapshot,
            # degrade rather than blessing dead data as live intel.
            src_ts = part.get("timestamp")
            if src_ts:
                try:
                    src_age = now - float(src_ts)
                    if src_age > MAX_FEATURE_AGE_SEC:
                        logger.warning(f"Hyperdash participation snapshot is {src_age/60:.0f}m old (source-side lag).")
                except (ValueError, TypeError):
                    pass


            self._participation_cache = part
            self._last_participation_ts = now
            self._compute_all_features(part)
            return True


    def _compute_all_features(self, part: Dict[str, Any]):
        """
        Derives smart money bias (notional-weighted), crowd bias (headcount-weighted),
        squeeze signals, and the v2 disjoint-cohort weighting per coin.
        """
        pnl_cohorts = part.get("pnlCohorts", [])
        size_cohorts = part.get("sizeCohorts", [])
        global_mkts = {m.get("coin"): m for m in part.get("global", []) if m.get("coin")}


        # coin -> list of (bias_notional, weight)  |  crowd: (bias_headcount, weight)
        coin_smart: Dict[str, List[Tuple[float, float]]] = {}
        coin_crowd: Dict[str, List[Tuple[float, float]]] = {}


        all_cohorts = pnl_cohorts + size_cohorts
        for c in all_cohorts:
            raw_label = str(c.get("cohortLabel") or c.get("cohortId") or "")
            key = _norm_label(raw_label)
            weight_smart = SMART_WEIGHTS_NORM.get(key)
            weight_crowd = CROWD_WEIGHTS_NORM.get(key)
            if weight_smart is None and weight_crowd is None:
                if key and key not in _seen_unknown_labels:
                    _seen_unknown_labels.add(key)
                    logger.info(f"Hyperdash cohort '{raw_label}' not in weighting tables — skipped.")
                continue


            for m in c.get("markets", []):
                coin = str(m.get("coin") or "").upper()
                l_usd = float(m.get("totalLongNotional") or 0.0)
                s_usd = float(m.get("totalShortNotional") or 0.0)
                l_n = float(m.get("longTraderCount") or 0.0)
                s_n = float(m.get("shortTraderCount") or 0.0)
                gross = l_usd + s_usd
                if gross <= 0 and (l_n + s_n) <= 0:
                    continue
                if weight_smart is not None and gross > 0:
                    # SMART: conviction measured in capital — notional-weighted inside cohort
                    coin_smart.setdefault(coin, []).append((compute_net_bias(l_usd, s_usd), weight_smart))
                if weight_crowd is not None and (l_n + s_n) > 0:
                    # CROWD: herding measured in bodies — trader-count based
                    coin_crowd.setdefault(coin, []).append((compute_net_bias_count(l_n, s_n), weight_crowd))


        features_map = {}
        all_coins = set(global_mkts.keys()) | set(coin_smart.keys())


        for coin in all_coins:
            # Smart bias: cohort-weighted mean of cohort notional biases
            s_vals = coin_smart.get(coin, [])
            s_den = sum(w for _, w in s_vals)
            smart_bias = (sum(b * w for b, w in s_vals) / s_den) if s_den > 0 else 0.0


            # Crowd bias: cohort-weighted mean of cohort headcount biases
            c_vals = coin_crowd.get(coin, [])
            c_den = sum(w for _, w in c_vals)
            crowd_bias = (sum(b * w for b, w in c_vals) / c_den) if c_den > 0 else 0.0


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
                "updated_at": self._last_participation_ts or time.time()
            }


        self._coin_features = features_map


    # ------------------------------------------------------------------ whale book
    def _fetch_top_whales(self, coin: str, limit: int = 10) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Retrieves top whale positions for a given coin.
        Returns (whales, ok). ok=False means the fetch FAILED (used by the
        negative-cache so a network blip retries in 15s instead of caching
        a fake-empty book for 90s).
        """
        data = self._execute_query(Q_TICKER_POSITIONS, {"coin": coin, "limit": limit}, timeout=4.0)
        if not data:
            return [], False
        tp = data.get("analytics", {}).get("perpsTickerPositions", {})
        positions = tp.get("positions", [])
        out = []
        for p in positions:
            try:
                size = float(p.get("size") or 0.0)
                notional = abs(float(p.get("notionalSize") or 0.0))
                entry = float(p.get("entryPrice") or 0.0)
                liq = float(p.get("liquidationPrice") or 0.0)
                upnl = float(p.get("unrealizedPnl") or 0.0)
            except (ValueError, TypeError):
                continue
            out.append({
                "address": str(p.get("address", ""))[:10] + "...",
                "label": p.get("label") or p.get("displayName") or "Whale",
                "size": size,                              # SIGNED: >0 long, <0 short (v1 dropped this)
                "side": "LONG" if size > 0 else "SHORT",    # (v1 dropped this too)
                "notional_usd": notional,
                "entry_price": entry,
                "liquidation_price": liq,                   # THE SWEEP-TARGET FIELD (v1 dropped this)
                "unrealized_pnl": upnl
            })
        return out, True


    # ------------------------------------------------------------------ public API
    def get_asset_intel(self, symbol: str, mark_price_hint: Optional[float] = None) -> Dict[str, Any]:
        """
        Returns institutional intelligence features for a given trading symbol.
        Optional mark_price_hint (live signal price) anchors sweep-target distances;
        without it the mark is derived from the largest whale's PnL identity.
        """
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
                "intel_age_sec": None,
                "smart_bias": 0.0,
                "crowd_bias": 0.0,
                "intel_score": 50.0,
                "intel_direction": "NEUTRAL",
                "signals": [],
                "top_whales": [],
                "sweep_targets": [],
            }


        res = dict(feat)
        res["symbol"] = symbol


        # STALENESS GUARD: never present dead features as live intel.
        res["intel_age_sec"] = round(now - float(feat.get("updated_at", 0.0)), 1)
        if res["intel_age_sec"] > MAX_FEATURE_AGE_SEC:
            res["data_valid"] = False
            res["stale_intel_neutralized"] = True


        # Fetch live top whale position book for this coin (cached 90s, negative-cache 15s)
        whale_cached = self._ticker_positions_cache.get(coin)
        if whale_cached:
            cached_ts, whale_data, was_ok = whale_cached
            ttl = WHALE_CACHE_TTL_SEC if was_ok else WHALE_NEG_CACHE_TTL_SEC
            if (now - cached_ts) < ttl:
                whales = whale_data
            else:
                whales, ok = self._fetch_top_whales(coin)
                self._ticker_positions_cache[coin] = (now, whales, ok)
        else:
            whales, ok = self._fetch_top_whales(coin)
            self._ticker_positions_cache[coin] = (now, whales, ok)


        res["top_whales"] = whales


        # WHALE LIQUIDATION SWEEP-TARGET ENGINE (new alpha)
        mark = float(mark_price_hint or 0.0)
        if mark <= 0:
            mark = _estimate_mark_from_positions(whales)
        res["sweep_targets"] = compute_sweep_targets(whales, mark) if (whales and mark > 0) else []
        res["whale_mark_estimate"] = round(mark, 8) if mark > 0 else None


        return res


    async def get_asset_intel_async(self, symbol: str, mark_price_hint: Optional[float] = None) -> Dict[str, Any]:
        """Async wrapper — safe to await directly from an event loop (offloaded to a thread)."""
        import asyncio
        return await asyncio.to_thread(self.get_asset_intel, symbol, mark_price_hint)




# Global singleton engine
hyperdash_engine = HyperdashIntelEngine()

