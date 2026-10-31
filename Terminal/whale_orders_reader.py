"""
Whale Orders Unified Reader & Analytics API
============================================
Provides fast query access to:
- Database/Whale_Orders/Open_Orders/<SYMBOL>_whale_open_orders.parquet
- Database/Whale_Orders/History_Orders/<SYMBOL>_whale_history_orders.parquet
- Database/Whale_Orders/whale_market_snapshots.parquet

Offers analytics on:
- Top bid/ask resistance walls
- Spoofing ratios (Cancelled / Initial)
- Absorption ratios (Filled / Initial)
- Volume concentration by venue (Coinbase Spot vs Binance Perp vs Hyperliquid)
"""

from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
WHALE_DIR = DB_DIR / "Whale_Orders"
OPEN_DIR = WHALE_DIR / "Open_Orders"
HIST_DIR = WHALE_DIR / "History_Orders"
SNAPSHOT_PATH = WHALE_DIR / "whale_market_snapshots.parquet"


class WhaleOrdersReader:
    def __init__(self, db_dir: Optional[Path] = None):
        self.whale_dir = (db_dir or DB_DIR) / "Whale_Orders"
        self.open_dir = self.whale_dir / "Open_Orders"
        self.hist_dir = self.whale_dir / "History_Orders"
        self.snapshot_path = self.whale_dir / "whale_market_snapshots.parquet"

    def list_available_assets(self) -> List[str]:
        """List all symbols with whale orders stored on disk."""
        if not self.open_dir.exists():
            return []
        return sorted([f.name.replace("_whale_open_orders.parquet", "") for f in self.open_dir.glob("*.parquet")])

    def get_open_orders(
        self,
        symbol: str,
        side: Optional[str] = None,
        min_usd: Optional[float] = None,
        venue: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch active open whale walls for an asset."""
        p = self.open_dir / f"{symbol}_whale_open_orders.parquet"
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_parquet(p)
        if side:
            df = df[df["side"].str.upper() == side.upper()]
        if min_usd:
            df = df[df["remaining_usd"] >= min_usd]
        if venue:
            df = df[df["venue"].str.contains(venue, case=False, na=False)]
        return df.sort_values(by="remaining_usd", ascending=False).reset_index(drop=True)

    def get_history_orders(
        self,
        symbol: str,
        status: Optional[str] = None,
        min_usd: Optional[float] = None
    ) -> pd.DataFrame:
        """Fetch historical whale orders (filled or cancelled)."""
        p = self.hist_dir / f"{symbol}_whale_history_orders.parquet"
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_parquet(p)
        if status:
            df = df[df["status"].str.contains(status, case=False, na=False)]
        if min_usd:
            df = df[df["initial_usd"] >= min_usd]
        return df.sort_values(by="initial_usd", ascending=False).reset_index(drop=True)

    def get_macro_snapshot(self, symbol: str) -> Dict:
        """Fetch latest macro whale market metrics for a symbol."""
        if not self.snapshot_path.exists():
            return {}
        df = pd.read_parquet(self.snapshot_path)
        sub = df[df["symbol"] == symbol]
        if sub.empty:
            return {}
        return sub.iloc[-1].to_dict()

    def get_spoofing_vs_absorption_metrics(self, symbol: str) -> Dict:
        """
        Calculates institutional spoofing ratio vs absorption ratio:
        - Spoofing: Orders pulled/cancelled with 0% fills
        - Absorption: Orders with >50% fills
        """
        df_hist = self.get_history_orders(symbol)
        if df_hist.empty:
            return {"symbol": symbol, "total_historical_orders": 0}

        total_orders = len(df_hist)
        total_initial_usd = df_hist["initial_usd"].sum()
        total_filled_usd = df_hist["filled_usd"].sum()

        zero_fill_cancelled = df_hist[df_hist["filled_usd"] == 0]
        spoofed_usd = zero_fill_cancelled["initial_usd"].sum()
        spoof_ratio_usd = (spoofed_usd / total_initial_usd * 100.0) if total_initial_usd > 0 else 0.0

        absorbed_orders = df_hist[df_hist["filled_usd"] >= 0.5 * df_hist["initial_usd"]]
        absorbed_usd = absorbed_orders["filled_usd"].sum()
        absorption_ratio_usd = (absorbed_usd / total_initial_usd * 100.0) if total_initial_usd > 0 else 0.0

        return {
            "symbol": symbol,
            "total_historical_orders": total_orders,
            "total_initial_usd": total_initial_usd,
            "total_filled_usd": total_filled_usd,
            "spoofed_usd": spoofed_usd,
            "spoof_ratio_pct": round(spoof_ratio_usd, 2),
            "absorbed_usd": absorbed_usd,
            "absorption_ratio_pct": round(absorption_ratio_usd, 2)
        }


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    reader = WhaleOrdersReader()
    assets = reader.list_available_assets()
    print(f"==================================================")
    print(f" WHALE ORDERS DATABASE SUMMARY ({len(assets)} ASSETS)")
    print(f"==================================================")
    for sym in assets:
        op = reader.get_open_orders(sym)
        hi = reader.get_history_orders(sym)
        metrics = reader.get_spoofing_vs_absorption_metrics(sym)
        top_ask = op[op["side"].str.upper() == "ASK"].head(1)
        top_bid = op[op["side"].str.upper() == "BID"].head(1)
        ask_str = f"${top_ask.iloc[0]['remaining_usd']/1e6:.2f}M @ {top_ask.iloc[0]['price']}" if not top_ask.empty else "N/A"
        bid_str = f"${top_bid.iloc[0]['remaining_usd']/1e6:.2f}M @ {top_bid.iloc[0]['price']}" if not top_bid.empty else "N/A"

        print(f"[{sym:5s}] Open: {len(op):4d} walls | Hist: {len(hi):5d} | Spoof: {metrics.get('spoof_ratio_pct', 0)}% | Absorb: {metrics.get('absorption_ratio_pct', 0)}%")
        print(f"        Top Bid Wall: {bid_str} | Top Ask Wall: {ask_str}")

