"""
Liquidation Intelligence System Reader
======================================
Unified, high-performance data access layer providing downstream algorithmic
trading bots, MT5 execution engines, and risk models with direct structured
access to CoinGlass liquidation intelligence:

1. Key Liquidation Levels (Ranked strike barriers by distance % filter: +-1% to +-8%)
2. Magnet Zones (Top 3 Short & Long liquidation cluster bands with distance % and score)
3. Leverage Tiers (High, Mid, and Low leverage open strength breakdown by side)
4. Near-Field Balance (+-3% price proximity longs vs shorts balance & 24h delta)
5. Macro Snapshots (CLLD, last price, 1h change, turn time, and sentiment)
"""

from pathlib import Path
from typing import Optional, Union, Dict, Any, List
import pandas as pd
import numpy as np

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LIQ_DIR = DB_DIR / "Liquidation_Data"
SNAPSHOTS_PARQUET = LIQ_DIR / "liquidation_snapshots.parquet"
KEY_LEVELS_DIR = LIQ_DIR / "Key_Levels"
MAGNET_ZONES_DIR = LIQ_DIR / "Magnet_Zones"
LEVERAGE_TIERS_DIR = LIQ_DIR / "Leverage_Tiers"
NEAR_FIELD_DIR = LIQ_DIR / "Near_Field_Balance"


class LiquidationReader:
    """Unified system reader for structured CoinGlass liquidation datasets."""

    def __init__(self, db_dir: Optional[Path] = None):
        self.db_dir = Path(db_dir) if db_dir else DB_DIR
        self.liq_dir = self.db_dir / "Liquidation_Data"
        self.snapshots_path = self.liq_dir / "liquidation_snapshots.parquet"
        self.key_levels_dir = self.liq_dir / "Key_Levels"
        self.magnet_zones_dir = self.liq_dir / "Magnet_Zones"
        self.leverage_tiers_dir = self.liq_dir / "Leverage_Tiers"
        self.near_field_dir = self.liq_dir / "Near_Field_Balance"

    def list_available_assets(self) -> List[str]:
        """Return list of all assets with available liquidation data."""
        if not self.key_levels_dir.exists():
            return []
        files = list(self.key_levels_dir.glob("*_key_levels.parquet"))
        return sorted([f.stem.replace("_key_levels", "") for f in files])

    def list_exchanges_for_asset(self, symbol: str) -> List[str]:
        """Return all distinct exchanges captured for a given asset."""
        df = self.get_key_levels(symbol)
        if df.empty or "exchange" not in df.columns:
            return []
        return sorted(df["exchange"].dropna().unique().tolist())

    def get_key_levels(
        self,
        symbol: str,
        range_filter: Optional[str] = None,
        exchange: Optional[str] = None,
        lookback_tf: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve ranked strike barriers for an asset.
        
        Args:
            symbol: Asset symbol (e.g. 'BTC', 'ETH', 'US30')
            range_filter: Specific tier, e.g. '±1%', '±2%', '±3%', '±4%', '±5%', '±6%', '±7%', '±8%'
            exchange: Specific exchange (e.g. 'Binance', 'Bybit', 'OKX')
            lookback_tf: Timeframe (e.g. '3D - 1m', '7D - 3m', '30D - 15m')
        """
        path = self.key_levels_dir / f"{symbol}_key_levels.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if df.empty:
            return df

        if exchange:
            df = df[df["exchange"].str.upper() == exchange.upper()]
        if lookback_tf:
            df = df[df["lookback_tf"].str.replace(" ", "") == lookback_tf.replace(" ", "")]
        if range_filter:
            norm_rf = range_filter.replace("+", "").replace("-", "").replace("±", "")
            df = df[df["range_filter"].str.contains(norm_rf, na=False)]
        return df.reset_index(drop=True)

    def get_magnet_zones(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        lookback_tf: Optional[str] = None,
        side: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve Top 3 Short and Long magnet zones for an asset.
        
        Args:
            symbol: Asset symbol (e.g. 'BTC', 'ETH')
            exchange: Filter by exchange
            lookback_tf: Filter by lookback timeframe
            side: Filter by 'short' or 'long'
        """
        path = self.magnet_zones_dir / f"{symbol}_magnet_zones.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if df.empty:
            return df

        if exchange:
            df = df[df["exchange"].str.upper() == exchange.upper()]
        if lookback_tf:
            df = df[df["lookback_tf"].str.replace(" ", "") == lookback_tf.replace(" ", "")]
        if side:
            df = df[df["side"].str.lower() == side.lower()]
        return df.reset_index(drop=True)

    def get_leverage_tiers(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        lookback_tf: Optional[str] = None
    ) -> pd.DataFrame:
        """Retrieve open strength breakdown across High, Mid, and Low leverage tiers."""
        path = self.leverage_tiers_dir / f"{symbol}_leverage_tiers.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if df.empty:
            return df

        if exchange:
            df = df[df["exchange"].str.upper() == exchange.upper()]
        if lookback_tf:
            df = df[df["lookback_tf"].str.replace(" ", "") == lookback_tf.replace(" ", "")]
        return df.reset_index(drop=True)

    def get_near_field_balance(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        lookback_tf: Optional[str] = None
    ) -> pd.DataFrame:
        """Retrieve open strength within +-3% of price (Shorts vs Longs share and 24h shift)."""
        path = self.near_field_dir / f"{symbol}_near_field.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if df.empty:
            return df

        if exchange:
            df = df[df["exchange"].str.upper() == exchange.upper()]
        if lookback_tf:
            df = df[df["lookback_tf"].str.replace(" ", "") == lookback_tf.replace(" ", "")]
        return df.reset_index(drop=True)

    def get_macro_snapshot(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        lookback_tf: Optional[str] = None
    ) -> Dict[str, Any]:
        """Retrieve latest consolidated macro snapshot for an asset."""
        if not self.snapshots_path.exists():
            return {}
        snaps = pd.read_parquet(self.snapshots_path)
        if snaps.empty:
            return {}

        match = snaps[snaps["symbol"] == symbol]
        if match.empty:
            return {}

        if exchange:
            match = match[match["exchange"].str.upper() == exchange.upper()]
        if lookback_tf:
            match = match[match["lookback_tf"].str.replace(" ", "") == lookback_tf.replace(" ", "")]

        if match.empty:
            return {}
        return match.iloc[-1].to_dict()

    def get_asset_intelligence(
        self,
        symbol: str,
        exchange: Optional[str] = None,
        lookback_tf: str = "3D - 1m"
    ) -> Dict[str, Any]:
        """
        Unified method returning full multi-dimensional intelligence package for an asset:
        - macro_snapshot
        - magnet_zones (Top 3 Shorts & Longs)
        - key_levels (Ranked barriers across ranges)
        - leverage_tiers (High/Mid/Low breakdown)
        - near_field_balance (+-3% bias & 24h drift)
        """
        snapshot = self.get_macro_snapshot(symbol, exchange=exchange, lookback_tf=lookback_tf)
        ex = exchange or (snapshot.get("exchange") if snapshot else None)

        mz = self.get_magnet_zones(symbol, exchange=ex, lookback_tf=lookback_tf)
        kl = self.get_key_levels(symbol, exchange=ex, lookback_tf=lookback_tf)
        lt = self.get_leverage_tiers(symbol, exchange=ex, lookback_tf=lookback_tf)
        nf = self.get_near_field_balance(symbol, exchange=ex, lookback_tf=lookback_tf)

        return {
            "symbol": symbol,
            "exchange": ex,
            "lookback_tf": lookback_tf,
            "last_price": snapshot.get("last_price") if snapshot else None,
            "macro_snapshot": snapshot,
            "magnet_zones": mz,
            "key_levels": kl,
            "leverage_tiers": lt,
            "near_field_balance": nf
        }

    def get_cross_exchange_matrix(self, symbol: str, lookback_tf: str = "3D - 1m") -> pd.DataFrame:
        """
        Compare liquidity zones and metrics across all exchanges for an asset.
        Produces a comparative summary table across Binance, Bybit, OKX, etc.
        """
        mz_df = self.get_magnet_zones(symbol, lookback_tf=lookback_tf)
        if mz_df.empty:
            return pd.DataFrame()

        rows = []
        for ex, group in mz_df.groupby("exchange"):
            s_top1 = group[(group["side"] == "short") & (group["rank"] == 1)]
            l_top1 = group[(group["side"] == "long") & (group["rank"] == 1)]

            rows.append({
                "symbol": symbol,
                "exchange": ex,
                "lookback_tf": lookback_tf,
                "short_magnet_top1": s_top1["zone_label"].values[0] if not s_top1.empty else None,
                "short_magnet_dist_pct": s_top1["distance_pct"].values[0] if not s_top1.empty else None,
                "short_magnet_str": s_top1["strength"].values[0] if not s_top1.empty else None,
                "long_magnet_top1": l_top1["zone_label"].values[0] if not l_top1.empty else None,
                "long_magnet_dist_pct": l_top1["distance_pct"].values[0] if not l_top1.empty else None,
                "long_magnet_str": l_top1["strength"].values[0] if not l_top1.empty else None,
            })
        return pd.DataFrame(rows)


# Quick CLI inspection
if __name__ == "__main__":
    import sys
    reader = LiquidationReader()
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTC"
    print(f"--- Liquidation Intelligence for {sym} ---")
    data = reader.get_asset_intelligence(sym)
    print(f"Price: {data['last_price']} on {data['exchange']}")
    print("\nMagnet Zones:")
    if not data["magnet_zones"].empty:
        print(data["magnet_zones"][["side", "rank", "zone_label", "distance_pct", "strength"]])
    else:
        print("  (None)")
    print("\nKey Levels Sample:")
    if not data["key_levels"].empty:
        print(data["key_levels"][["range_filter", "side", "rank", "price", "distance_pct", "strength"]].head(6))
    else:
        print("  (None)")
