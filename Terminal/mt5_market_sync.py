"""
MetaTrader 5 Live Market Price & Spread Synchronizer
---------------------------------------------------
Synchronizes live bid, ask, spread, tick values, and lot constraints
from Blueberry Markets MT5 for all 44 mutual assets.

Calculates:
- Spread in points, dollars, and percentage.
- Basis differential between CoinGlass perpetual price and MT5 CFD quote.
- Lot size constraints (min lot, max lot, lot step, tick value).
Persists to: `Database/mt5_live_ticks.parquet`
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import MetaTrader5 as mt5

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
REGISTRY_FILE = DB_DIR / "mutual_asset_capability_registry.json"
OUTPUT_TICKS_PARQUET = DB_DIR / "mt5_live_ticks.parquet"
SNAPSHOTS_PARQUET = DB_DIR / "Liquidation_Data" / "liquidation_snapshots.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MT5-Sync] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("MT5Sync")


def load_registry_symbols():
    if not REGISTRY_FILE.exists():
        logger.error(f"Registry missing at {REGISTRY_FILE}")
        return {}
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        reg = json.load(f)
    
    mapping = {}
    for cat, assets in reg.items():
        for sym, meta in assets.items():
            mapping[sym] = {
                "mt5": meta["mt5"],
                "category": cat
            }
    return mapping


def sync_mt5_ticks():
    sym_map = load_registry_symbols()
    if not sym_map:
        return pd.DataFrame()

    if not mt5.initialize():
        logger.error(f"Failed to initialize MT5: {mt5.last_error()}")
        return pd.DataFrame()

    acct = mt5.account_info()
    if acct:
        logger.info(f"Connected to MT5: {acct.name} | Server: {acct.server} | Balance: ${acct.balance:,.2f}")
    else:
        logger.warning("MT5 connected but account_info returned None.")

    # Load latest coinglass prices for basis calculation
    cg_prices = {}
    if SNAPSHOTS_PARQUET.exists():
        try:
            snaps = pd.read_parquet(SNAPSHOTS_PARQUET)
            for _, r in snaps.groupby("symbol").last().iterrows():
                cg_prices[_] = r.get("last_price")
        except Exception as e:
            logger.warning(f"Failed to read snapshots for basis: {e}")

    records = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for sym, meta in sym_map.items():
        mt5_sym = meta["mt5"]
        selected = mt5.symbol_select(mt5_sym, True)
        if not selected:
            logger.warning(f"[{sym}] Could not select MT5 symbol '{mt5_sym}'")
            continue

        tick = mt5.symbol_info_tick(mt5_sym)
        info = mt5.symbol_info(mt5_sym)

        if not tick or not info:
            logger.warning(f"[{sym}] No tick/info for '{mt5_sym}'")
            continue

        bid = tick.bid
        ask = tick.ask
        last = tick.last if tick.last > 0 else (bid + ask) / 2.0
        mid = (bid + ask) / 2.0
        point = info.point
        spread_pts = info.spread
        spread_val = ask - bid
        spread_pct = (spread_val / mid) * 100.0 if mid > 0 else 0.0

        cg_px = cg_prices.get(sym)
        basis = (cg_px - mid) if (cg_px and mid > 0) else None
        basis_pct = (basis / mid * 100.0) if (basis is not None and mid > 0) else None

        records.append({
            "symbol": sym,
            "mt5_symbol": mt5_sym,
            "category": meta["category"],
            "bid": bid,
            "ask": ask,
            "last": last,
            "mid": round(mid, info.digits),
            "point": point,
            "digits": info.digits,
            "spread_points": spread_pts,
            "spread_val": round(spread_val, info.digits),
            "spread_pct": round(spread_pct, 4),
            "tick_value": info.trade_tick_value,
            "tick_size": info.trade_tick_size,
            "contract_size": info.trade_contract_size,
            "min_lot": info.volume_min,
            "max_lot": info.volume_max,
            "lot_step": info.volume_step,
            "coinglass_last": cg_px,
            "basis": round(basis, info.digits) if basis is not None else None,
            "basis_pct": round(basis_pct, 4) if basis_pct is not None else None,
            "server_time_ms": tick.time_msc,
            "updated_at_utc": now_utc
        })

    mt5.shutdown()

    if not records:
        logger.error("No MT5 records synchronized.")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    DB_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_TICKS_PARQUET, index=False)
    logger.info(f"Successfully synchronized {len(df)} MT5 symbols into {OUTPUT_TICKS_PARQUET.name}")
    return df


if __name__ == "__main__":
    df_result = sync_mt5_ticks()
    if not df_result.empty:
        print("\n" + "="*110)
        print(f"{'SYM':<8} {'MT5 SYMBOL':<12} {'CATEGORY':<12} {'BID':<12} {'ASK':<12} {'SPREAD(PTS)':<12} {'SPREAD(%)':<12} {'COINGLASS':<12} {'BASIS(%)':<10}")
        print("="*110)
        for _, r in df_result.head(15).iterrows():
            cg_str = f"{r['coinglass_last']:.2f}" if pd.notnull(r['coinglass_last']) else "N/A"
            basis_str = f"{r['basis_pct']:+.2f}%" if pd.notnull(r['basis_pct']) else "N/A"
            print(f"{r['symbol']:<8} {r['mt5_symbol']:<12} {r['category']:<12} {r['bid']:<12} {r['ask']:<12} {r['spread_points']:<12} {r['spread_pct']:<12.4f} {cg_str:<12} {basis_str:<10}")
        print("="*110)
