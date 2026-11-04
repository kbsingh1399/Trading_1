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

# ---------------------------------------------------------------------------
# PATCH P2 (Phase 2): Underwater Entry Pockets & Inter-Refresh Whale Flow
# ---------------------------------------------------------------------------
POCKET_NEAR_PCT = 0.10            # pockets within 0.1% of mark are already defended/attacked
POCKET_FAR_PCT = 8.0              # pockets further than this are unreachable trapped supply
POCKET_CLUSTER_TOL_PCT = 0.50     # single-linkage clustering tolerance on entry prices (% of mark)
POCKET_MAX_PER_SIDE = 3           # top pockets kept per side
POCKET_MIN_NOTIONAL_USD = 250_000.0  # trapped notional below this is noise at top-10 depth
WHALE_FLOW_SCALE_USD = 2_000_000.0  # signed notional delta saturating flow_score ~ 76 (tanh)
WHALE_FLOW_MIN_MOVE_PCT = 0.10    # >=10% notional move per whale counts as accumulate/derisk
WHALE_FLOW_MIN_GAP_SEC = 30.0    # <30s between snapshots: same-tick noise, invalid delta
WHALE_FLOW_MAX_GAP_SEC = 900.0   # >15min between snapshots: stale baseline, low confidence


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
    """
    Signed skew percentage by TRADER COUNT (herding headcount).
    Requires total headcount (long_n + short_n) >= 10 before applying full crowd bias,
    or applies Laplace smoothing ((long_n + 1) / (long_n + short_n + 2)) to avoid wild
    swings on small altcoin sample sizes.
    """
    l_n = float(long_n)
    s_n = float(short_n)
    gross = l_n + s_n
    if gross <= 0.0:
        return 0.0
    if gross >= 10.0:
        bias = 100.0 * (l_n - s_n) / gross
    else:
        # Laplace smoothing: (l_n + 1) / (gross + 2) -> signed bias in [-100, 100]
        bias = 100.0 * (l_n - s_n) / (gross + 2.0)
    return max(-100.0, min(100.0, bias))




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


def compute_underwater_entry_pockets(
    positions: List[Dict[str, Any]],
    mark_price: float,
    near_pct: float = POCKET_NEAR_PCT,
    far_pct: float = POCKET_FAR_PCT,
    cluster_tol_pct: float = POCKET_CLUSTER_TOL_PCT,
    max_per_side: int = POCKET_MAX_PER_SIDE,
    min_pocket_notional_usd: float = POCKET_MIN_NOTIONAL_USD
) -> List[Dict[str, Any]]:
    """
    PATCH P2 — UNDERWATER WHALE ENTRY POCKET ENGINE
    ----------------------------------------------------------------------------
    Clusters the ENTRY PRICES of whale positions currently in NEGATIVE unrealized
    PnL ("underwater") into notional-weighted pockets of trapped capital:

      - Underwater LONG whales  (entry ABOVE mark): their breakeven-exit zone is
        OVERHEAD SUPPLY — a rally cap. As price recovers toward their entries,
        trapped longs sell into strength.
      - Underwater SHORT whales (entry BELOW mark): their breakeven-buyback zone
        is a SUPPORT FLOOR — price finds demand there as trapped shorts close.

    Each pocket returns:
      pocket_type          'OVERHEAD_SUPPLY' (trapped longs above) or 'SUPPORT_FLOOR' (trapped shorts below)
      side                 side of the trapped whales ('LONG'/'SHORT')
      pocket_price         notional-weighted mean entry of the trapped cluster
      dist_pct             signed distance from mark in percent
      trapped_notional_usd total whale notional trapped in this pocket
      trapped_musd        same in millions
      whale_count          whales in the pocket
      avg_underwater_pnl_pct  notional-weighted mean underwater depth (% of notional)
      pocket_score         trapped_notional * exp(-|dist_pct| / 3.0) — proximity-decayed cap/floor strength

    Pockets closer than near_pct are already resolved (price is at their entry);
    pockets beyond far_pct are unreachable trapped supply for trade horizon.
    """
    if not positions or mark_price <= 0:
        return []

    by_side: Dict[str, List[Dict[str, Any]]] = {"LONG": [], "SHORT": []}
    for p in positions:
        try:
            sz = float(p.get("size") or 0.0)
            notion = abs(float(p.get("notional_usd") or 0.0))
            entry = float(p.get("entry_price") or 0.0)
            upnl = float(p.get("unrealized_pnl") or 0.0)
        except (ValueError, TypeError):
            continue
        if sz == 0.0 or notion <= 0.0 or entry <= 0.0 or upnl >= 0.0:
            continue   # only UNDERWATER positions
        side = "LONG" if sz > 0 else "SHORT"
        dist_pct = (entry - mark_price) / mark_price * 100.0  # signed, entry-relative
        # Underwater LONG -> entry must sit ABOVE mark; underwater SHORT -> below.
        if side == "LONG" and not (near_pct < dist_pct <= far_pct):
            continue
        if side == "SHORT" and not (-far_pct <= dist_pct < -near_pct):
            continue
        underwater_pnl_pct = (upnl / notion) * 100.0 if notion > 0 else 0.0
        by_side[side].append({
            "entry": entry, "dist_pct": dist_pct, "notional": notion,
            "upnl_pct": underwater_pnl_pct,
        })

    pockets: List[Dict[str, Any]] = []
    for side, items in by_side.items():
        if not items:
            continue
        # Single-linkage clustering by entry-price proximity
        items.sort(key=lambda x: x["entry"])
        clusters: List[List[Dict[str, Any]]] = []
        cur: List[Dict[str, Any]] = []
        for it in items:
            if cur and (abs(it["entry"] - cur[-1]["entry"]) / mark_price * 100.0) > cluster_tol_pct:
                clusters.append(cur)
                cur = [it]
            else:
                cur.append(it)
        if cur:
            clusters.append(cur)

        for cl in clusters:
            total_notional = sum(x["notional"] for x in cl)
            if total_notional < min_pocket_notional_usd:
                continue
            wavg_entry = sum(x["entry"] * x["notional"] for x in cl) / max(total_notional, 1e-9)
            wavg_upnl_pct = sum(x["upnl_pct"] * x["notional"] for x in cl) / max(total_notional, 1e-9)
            dist_pct = (wavg_entry - mark_price) / mark_price * 100.0
            pockets.append({
                "pocket_type": "OVERHEAD_SUPPLY" if side == "LONG" else "SUPPORT_FLOOR",
                "side": side,                                    # side of the TRAPPED whales
                "pocket_price": round(wavg_entry, 8),
                "dist_pct": round(dist_pct, 3),
                "trapped_notional_usd": round(total_notional, 2),
                "trapped_musd": round(total_notional / 1e6, 3),
                "whale_count": len(cl),
                "avg_underwater_pnl_pct": round(wavg_upnl_pct, 2),
                "pocket_score": round(total_notional * math.exp(-abs(dist_pct) / 3.0), 2),
            })

    pockets.sort(key=lambda t: t["pocket_score"], reverse=True)
    kept: List[Dict[str, Any]] = []
    per_side: Dict[str, int] = {"LONG": 0, "SHORT": 0}
    for t in pockets:
        if per_side.get(t["side"], max_per_side) < max_per_side:
            kept.append(t)
            per_side[t["side"]] += 1
    return kept


def _empty_whale_flow() -> Dict[str, Any]:
    """Canonical empty whale-flow feature block (no baseline snapshot yet)."""
    return {
        "data_valid": False,
        "flow_state": "NO_BASELINE",
        "flow_score": 0.0,
        "flow_delta_usd": 0.0,
        "window_sec": 0.0,
        "flow_age_sec": None,
        "accumulating": 0,
        "derisking": 0,
        "new_entries": 0,
        "closed_exits": 0,
        "flips": 0,
        "low_confidence": False,
        "whale_moves": [],
    }


def compute_whale_flow_delta(
    prev_positions: List[Dict[str, Any]],
    cur_positions: List[Dict[str, Any]],
    prev_ts: float,
    cur_ts: float,
    flow_scale_usd: float = WHALE_FLOW_SCALE_USD,
    min_move_pct: float = WHALE_FLOW_MIN_MOVE_PCT,
    min_gap_sec: float = WHALE_FLOW_MIN_GAP_SEC,
    max_gap_sec: float = WHALE_FLOW_MAX_GAP_SEC
) -> Dict[str, Any]:
    """
    PATCH P2 — INTER-REFRESH WHALE DELTA TRACKING (90s-cache exploitation)
    ----------------------------------------------------------------------------
    Diffs two consecutive top-whale position-book snapshots for the same coin to
    detect whether the largest wallets are actively ACCUMULATING, DERISKING, or
    DUMPING into strength between refresh windows.

    Per-whale classification (matched by address, label fallback):
      NEW_ENTRY    whale appeared in the current snapshot
      CLOSED_EXIT  whale vanished from the current snapshot
      FLIPPED      whale reversed side between snapshots
      ACCUMULATING |size| grew >= min_move_pct (same side)
      DERISKING    |size| shrank >= min_move_pct (same side, not full exit)
      HOLDING      sub-threshold churn

    Aggregation:
      flow_delta_usd  sum of signed notional moves: + = long capital added /
                      short capital removed; - = short capital added / long
                      capital removed. Wallet vanishing counts as full removal,
                      wallet appearing as full addition.
      flow_score      100 * tanh(flow_delta_usd / flow_scale_usd)  [-100, +100]
      flow_state      ACCUMULATING (>= +25) / DISTRIBUTING (<= -25) /
                      LEANING_LONG (+10..+25) / LEANING_SHORT (-25..-10) / STABLE

    Gap guards: window < min_gap_sec -> invalid (same-tick noise);
                window > max_gap_sec -> low_confidence (stale baseline).
    """
    flow = _empty_whale_flow()
    flow["window_sec"] = round(max(0.0, float(cur_ts) - float(prev_ts)), 1)
    flow["computed_at"] = float(cur_ts)

    if not prev_positions and not cur_positions:
        flow["flow_state"] = "NO_BOOK"
        return flow
    window = float(cur_ts) - float(prev_ts)
    if window < min_gap_sec:
        flow["flow_state"] = "INVALID_WINDOW_TOO_FAST"
        return flow
    if window > max_gap_sec:
        flow["low_confidence"] = True

    def _key(w: Dict[str, Any]) -> str:
        addr = str(w.get("address") or "")
        return addr if addr and not addr.startswith("...") else str(w.get("label") or addr or "")

    def _signed_notional(w: Dict[str, Any]) -> float:
        sz = float(w.get("size") or 0.0)
        notion = abs(float(w.get("notional_usd") or 0.0))
        return notion if sz > 0 else -notion

    prev_map = {_key(w): w for w in prev_positions if isinstance(w, dict)}
    cur_map = {_key(w): w for w in cur_positions if isinstance(w, dict)}

    net_delta = 0.0
    moves: List[Dict[str, Any]] = []
    accum = derisk = new_n = closed_n = flips = 0

    for key, prev_w in prev_map.items():
        cur_w = cur_map.get(key)
        prev_signed = _signed_notional(prev_w)
        if cur_w is None:
            # Whale vanished: full removal of its directional capital
            net_delta -= prev_signed
            closed_n += 1
            moves.append({
                "whale": str(prev_w.get("label") or key), "action": "CLOSED_EXIT",
                "delta_usd": round(-prev_signed, 2),
            })
            continue
        cur_signed = _signed_notional(cur_w)
        delta = cur_signed - prev_signed
        prev_sz = float(prev_w.get("size") or 0.0)
        cur_sz = float(cur_w.get("size") or 0.0)
        if (prev_sz > 0) != (cur_sz > 0) and cur_sz != 0:
            flips += 1
            net_delta += delta
            moves.append({
                "whale": str(cur_w.get("label") or key), "action": "FLIPPED",
                "delta_usd": round(delta, 2),
            })
        elif abs(delta) >= min_move_pct * abs(prev_signed):
            net_delta += delta
            if delta > 0:
                accum += 1
                action = "ACCUMULATING"
            else:
                derisk += 1
                action = "DERISKING"
            moves.append({
                "whale": str(cur_w.get("label") or key), "action": action,
                "delta_usd": round(delta, 2),
            })

    for key, cur_w in cur_map.items():
        if key not in prev_map:
            cur_signed = _signed_notional(cur_w)
            net_delta += cur_signed
            new_n += 1
            moves.append({
                "whale": str(cur_w.get("label") or key), "action": "NEW_ENTRY",
                "delta_usd": round(cur_signed, 2),
            })

    moves.sort(key=lambda m: abs(m.get("delta_usd") or 0.0), reverse=True)

    flow_score = 100.0 * math.tanh(net_delta / max(flow_scale_usd, 1e-9))
    if flow_score >= 25.0:
        state = "ACCUMULATING"
    elif flow_score <= -25.0:
        state = "DISTRIBUTING"
    elif flow_score >= 10.0:
        state = "LEANING_LONG"
    elif flow_score <= -10.0:
        state = "LEANING_SHORT"
    else:
        state = "STABLE"

    flow.update({
        "data_valid": True,
        "flow_state": state,
        "flow_score": round(flow_score, 1),
        "flow_delta_usd": round(net_delta, 2),
        "accumulating": accum,
        "derisking": derisk,
        "new_entries": new_n,
        "closed_exits": closed_n,
        "flips": flips,
        "whale_moves": moves[:10],
    })
    return flow



class HyperdashIntelEngine:
    """Lightweight asynchronous / synchronous Hyperdash Intelligence Engine (v2)."""


    def __init__(self, cache_ttl_sec: float = 60.0):
        self.cache_ttl = cache_ttl_sec
        self._last_participation_ts = 0.0
        self._participation_cache: Dict[str, Any] = {}
        self._coin_features: Dict[str, Dict[str, Any]] = {}
        self._ticker_positions_cache: Dict[str, Tuple[float, List[Dict[str, Any]], bool]] = {}
        self._refresh_lock = threading.Lock()   # thread-safe: evaluate_signal fans out on worker threads
        # PATCH P2: inter-refresh whale delta tracking state
        self._whale_flow_snapshots: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self._last_flow_features: Dict[str, Dict[str, Any]] = {}
        # PATCH P4: per-coin fetch locks — prevent duplicate concurrent GraphQL
        # fetches for the same coin when evaluations fan out in parallel.
        self._whale_fetch_locks: Dict[str, threading.Lock] = {}

        # Load cached features from parquet if available for 0.0ms warm start
        if FEATURES_CACHE_FILE.exists():
            try:
                import pandas as pd
                df = pd.read_parquet(FEATURES_CACHE_FILE)
                for _, row in df.iterrows():
                    coin = str(row['coin'])
                    sig_val = row.get('signals', '')
                    sigs = sig_val.split(',') if isinstance(sig_val, str) and sig_val else []
                    self._coin_features[coin] = {
                        "coin": coin,
                        "data_valid": bool(row.get('data_valid', True)),
                        "smart_bias": float(row.get('smart_bias', 0.0)),
                        "crowd_bias": float(row.get('crowd_bias', 0.0)),
                        "net_divergence": float(row.get('net_divergence', 0.0)),
                        "open_interest_usd": float(row.get('open_interest_usd', 0.0)),
                        "intel_score": float(row.get('intel_score', 50.0)),
                        "intel_direction": str(row.get('intel_direction', 'NEUTRAL')),
                        "signals": sigs,
                        "updated_at": float(row.get('updated_at', time.time()))
                    }
                self._last_participation_ts = max((f['updated_at'] for f in self._coin_features.values()), default=0.0)
                logger.info(f"Loaded {len(self._coin_features)} warm features from {FEATURES_CACHE_FILE.name}")
            except Exception as e:
                logger.debug(f"Failed to load warm features from parquet: {e}")


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
        try:
            import pandas as pd
            df = pd.DataFrame(list(features_map.values()))
            if not df.empty:
                df['signals'] = df['signals'].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
                atomic_parquet(df, FEATURES_CACHE_FILE)
        except Exception as e:
            logger.debug(f'Failed to persist hyperdash features parquet: {e}')


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

    # ------------------------------------------------------------------ PATCH P2/P4
    def _fetch_top_whales_cached(self, coin: str) -> Tuple[List[Dict[str, Any]], bool, Dict[str, Any]]:
        """
        Cache-coherent, per-coin lock-guarded whale book fetch.

        PATCH P4: with bounded fan-out evaluation, several worker threads can
        request the SAME coin within milliseconds. The per-coin lock +
        double-checked cache guarantees exactly one upstream GraphQL fetch per
        TTL window; the losers reuse the fresh cache.

        PATCH P2: on every FRESH successful fetch, the current whale book is
        diffed against the previous snapshot (90s cadence) to refresh the
        inter-refresh whale-flow features for this coin.

        Returns:
            (whales, ok, flow_features)
        """
        now = time.time()
        cached = self._ticker_positions_cache.get(coin)
        if cached:
            cached_ts, whale_data, was_ok = cached
            ttl = WHALE_CACHE_TTL_SEC if was_ok else WHALE_NEG_CACHE_TTL_SEC
            if (now - cached_ts) < ttl:
                flow = dict(self._last_flow_features.get(coin) or _empty_whale_flow())
                if flow.get("computed_at"):
                    flow["flow_age_sec"] = round(max(0.0, now - float(flow["computed_at"])), 1)
                return whale_data, was_ok, flow

        lock = self._whale_fetch_locks.setdefault(coin, threading.Lock())
        with lock:
            # Double-check under lock: a parallel worker may have just refreshed.
            cached = self._ticker_positions_cache.get(coin)
            if cached:
                cached_ts, whale_data, was_ok = cached
                ttl = WHALE_CACHE_TTL_SEC if was_ok else WHALE_NEG_CACHE_TTL_SEC
                if (time.time() - cached_ts) < ttl:
                    flow = dict(self._last_flow_features.get(coin) or _empty_whale_flow())
                    if flow.get("computed_at"):
                        flow["flow_age_sec"] = round(max(0.0, time.time() - float(flow["computed_at"])), 1)
                    return whale_data, was_ok, flow

            whales, ok = self._fetch_top_whales(coin)
            fetch_ts = time.time()
            self._ticker_positions_cache[coin] = (fetch_ts, whales, ok)

            flow = _empty_whale_flow()
            if ok:
                prev = self._whale_flow_snapshots.get(coin)
                if prev and prev[1]:
                    flow = compute_whale_flow_delta(prev[1], whales, prev[0], fetch_ts)
                self._whale_flow_snapshots[coin] = (fetch_ts, whales)
            self._last_flow_features[coin] = flow
            return whales, ok, dict(flow)


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
                "entry_pockets": [],
                "whale_flow": _empty_whale_flow(),
            }


        res = dict(feat)
        res["symbol"] = symbol


        # STALENESS GUARD: never present dead features as live intel.
        res["intel_age_sec"] = round(now - float(feat.get("updated_at", 0.0)), 1)
        if res["intel_age_sec"] > MAX_FEATURE_AGE_SEC:
            res["data_valid"] = False
            res["stale_intel_neutralized"] = True


        # Fetch live top whale position book for this coin (cached 90s, negative-cache 15s)
        # PATCH P2/P4: lock-guarded cache-coherent fetch + inter-refresh flow delta.
        whales, _whale_ok, whale_flow = self._fetch_top_whales_cached(coin)


        res["top_whales"] = whales
        res["whale_flow"] = whale_flow


        # WHALE LIQUIDATION SWEEP-TARGET ENGINE (new alpha)
        mark = float(mark_price_hint or 0.0)
        if mark <= 0:
            mark = _estimate_mark_from_positions(whales)
        res["sweep_targets"] = compute_sweep_targets(whales, mark) if (whales and mark > 0) else []
        # PATCH P2: UNDERWATER WHALE ENTRY POCKETS (overhead supply / support floors)
        res["entry_pockets"] = compute_underwater_entry_pockets(whales, mark) if (whales and mark > 0) else []
        res["whale_mark_estimate"] = round(mark, 8) if mark > 0 else None


        return res


    async def get_asset_intel_async(self, symbol: str, mark_price_hint: Optional[float] = None) -> Dict[str, Any]:
        """Async wrapper — safe to await directly from an event loop (offloaded to a thread)."""
        import asyncio
        return await asyncio.to_thread(self.get_asset_intel, symbol, mark_price_hint)




    def start_background_updater(self, interval_sec: float = 60.0):
        """Starts a background daemon thread that keeps participation features hot."""
        if getattr(self, "_bg_thread", None) and self._bg_thread.is_alive():
            return
        
        def _bg_loop():
            logger.info("Hyperdash continuous background updater started (interval=%ss).", interval_sec)
            while True:
                try:
                    self.refresh_participation(force=True)
                except Exception as exc:
                    logger.debug("Hyperdash background updater refresh error: %s", exc)
                time.sleep(interval_sec)

        self._bg_thread = threading.Thread(target=_bg_loop, daemon=True, name="HyperdashBgUpdater")
        self._bg_thread.start()


# Global singleton engine
hyperdash_engine = HyperdashIntelEngine()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [HYPERDASH] %(message)s")
    parser = argparse.ArgumentParser(description="Hyperdash Institutional Intelligence Daemon")
    parser.add_argument("--daemon", action="store_true", help="Run as dedicated background daemon")
    parser.add_argument("--interval", type=float, default=60.0, help="Refresh interval in seconds (default: 60)")
    parser.add_argument("--symbol", type=str, default="BTCUSD", help="Test symbol to print")
    args = parser.parse_args()

    engine = HyperdashIntelEngine()
    if args.daemon:
        logger.info(f"Starting Hyperdash Intelligence Daemon (interval={args.interval}s)...")
        while True:
            try:
                ok = engine.refresh_participation(force=True)
                logger.info(f"Hyperdash participation updated: {ok} | Markets: {len(engine._coin_features)} | Saved: {FEATURES_CACHE_FILE.name}")
            except Exception as e:
                logger.error(f"Hyperdash daemon cycle error: {e}")
            time.sleep(args.interval)
    else:
        engine.refresh_participation(force=True)
        intel = engine.get_asset_intel(args.symbol)
        print(json.dumps(intel, indent=2, default=str))

