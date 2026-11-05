"""
Aggregated Futures Multi-Exchange Order Flow Feed  —  v2.1.0
============================================================
Fetches, synchronizes, and aggregates real-time perpetual futures candle and order-flow
data across the 4 core Tier-1 cryptocurrency derivatives platforms used by CoinGlass:
  1. Binance Futures (BTCUSDT Perp)
  2. OKX Derivatives (BTC-USDT-SWAP Perp)
  3. Bybit Linear (BTCUSDT Perp)
  4. Hyperliquid L1/DEX (BTC-USD Perp)

Architecture:
- High-speed concurrent REST fetching via ThreadPoolExecutor (<600ms latency)
- Dynamic primary anchor failover: If Binance is unreachable, gracefully anchors to
  Bybit or OKX so the system never halts.
- Microstructure-Preserving Delta Scaling: Uses empirical Tier-1 taker volume ratios
  to scale aggregate volume, preventing synthetic delta corruption.
- Strict pure free APIs (No web browser scraping, No CDP dependencies).
"""

import json
import logging
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("AggregatedFuturesFeed")

# Venue Symbol Matrix
VENUE_SYMBOL_MAP: Dict[str, Dict[str, str]] = {
    "BTC": {"binance": "BTCUSDT", "okx": "BTC-USDT-SWAP", "bybit": "BTCUSDT", "hl": "BTC"},
    "ETH": {"binance": "ETHUSDT", "okx": "ETH-USDT-SWAP", "bybit": "ETHUSDT", "hl": "ETH"},
    "SOL": {"binance": "SOLUSDT", "okx": "SOL-USDT-SWAP", "bybit": "SOLUSDT", "hl": "SOL"},
    "DOGE": {"binance": "DOGEUSDT", "okx": "DOGE-USDT-SWAP", "bybit": "DOGEUSDT", "hl": "DOGE"},
    "XRP": {"binance": "XRPUSDT", "okx": "XRP-USDT-SWAP", "bybit": "XRPUSDT", "hl": "XRP"},
    "BNB": {"binance": "BNBUSDT", "okx": "BNB-USDT-SWAP", "bybit": "BNBUSDT", "hl": "BNB"},
    "ADA": {"binance": "ADAUSDT", "okx": "ADA-USDT-SWAP", "bybit": "ADAUSDT", "hl": "ADA"},
    "LINK": {"binance": "LINKUSDT", "okx": "LINK-USDT-SWAP", "bybit": "LINKUSDT", "hl": "LINK"},
    "AVAX": {"binance": "AVAXUSDT", "okx": "AVAX-USDT-SWAP", "bybit": "AVAXUSDT", "hl": "AVAX"},
    "NEAR": {"binance": "NEARUSDT", "okx": "NEAR-USDT-SWAP", "bybit": "NEARUSDT", "hl": "NEAR"},
    "SUI": {"binance": "SUIUSDT", "okx": "SUI-USDT-SWAP", "bybit": "SUIUSDT", "hl": "SUI"},
    "PEPE": {"binance": "1000PEPEUSDT", "okx": "PEPE-USDT-SWAP", "bybit": "1000PEPEUSDT", "hl": "kPEPE"},
    "DOT": {"binance": "DOTUSDT", "okx": "DOT-USDT-SWAP", "bybit": "DOTUSDT", "hl": "DOT"},
    "ATOM": {"binance": "ATOMUSDT", "okx": "ATOM-USDT-SWAP", "bybit": "ATOMUSDT", "hl": "ATOM"},
    "LTC": {"binance": "LTCUSDT", "okx": "LTC-USDT-SWAP", "bybit": "LTCUSDT", "hl": "LTC"},
    "BCH": {"binance": "BCHUSDT", "okx": "BCH-USDT-SWAP", "bybit": "BCHUSDT", "hl": "BCH"},
    "TRX": {"binance": "TRXUSDT", "okx": "TRX-USDT-SWAP", "bybit": "TRXUSDT", "hl": "TRX"},
    "UNI": {"binance": "UNIUSDT", "okx": "UNI-USDT-SWAP", "bybit": "UNIUSDT", "hl": "UNI"},
    "AAVE": {"binance": "AAVEUSDT", "okx": "AAVE-USDT-SWAP", "bybit": "AAVEUSDT", "hl": "AAVE"},
    "FIL": {"binance": "FILUSDT", "okx": "FIL-USDT-SWAP", "bybit": "FILUSDT", "hl": "FIL"},
    "CRV": {"binance": "CRVUSDT", "okx": "CRV-USDT-SWAP", "bybit": "CRVUSDT", "hl": "CRV"},
    "1INCH": {"binance": "1INCHUSDT", "okx": "1INCH-USDT-SWAP", "bybit": "1INCHUSDT"},
    "COMP": {"binance": "COMPUSDT", "okx": "COMP-USDT-SWAP", "bybit": "COMPUSDT"},
    "SUSHI": {"binance": "SUSHIUSDT", "okx": "SUSHI-USDT-SWAP", "bybit": "SUSHIUSDT"},
    "ZRX": {"binance": "ZRXUSDT", "okx": "ZRX-USDT-SWAP", "bybit": "ZRXUSDT"},
    "SNX": {"binance": "SNXUSDT", "okx": "SNX-USDT-SWAP", "bybit": "SNXUSDT"},
    "ALGO": {"binance": "ALGOUSDT", "okx": "ALGO-USDT-SWAP", "bybit": "ALGOUSDT"},
    "HBAR": {"binance": "HBARUSDT", "okx": "HBAR-USDT-SWAP", "bybit": "HBARUSDT"},
    "AXS": {"binance": "AXSUSDT", "okx": "AXS-USDT-SWAP", "bybit": "AXSUSDT"},
    "BAT": {"binance": "BATUSDT", "okx": "BAT-USDT-SWAP", "bybit": "BATUSDT"},
    "CHZ": {"binance": "CHZUSDT", "okx": "CHZ-USDT-SWAP", "bybit": "CHZUSDT"},
    "AR": {"binance": "ARUSDT", "okx": "AR-USDT-SWAP", "bybit": "ARUSDT"},
}

VENUE_PRICE_MULTIPLIERS: Dict[str, Dict[str, float]] = {
    "PEPE": {"okx": 1000.0}
}

# Contract Multiplier Normalization
# e.g., OKX PEPE-USDT-SWAP reports base coin in single PEPE tokens, while Binance/Bybit use 1000PEPE
VENUE_MULTIPLIERS: Dict[str, Dict[str, float]] = {
    "PEPE": {"okx": 0.001, "binance": 1.0, "bybit": 1.0, "hl": 1.0}
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


def _http_get_json(url: str, timeout: float = 3.5, headers: Optional[Dict[str, str]] = None) -> Any:
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, payload: Dict[str, Any], timeout: float = 3.5) -> Any:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


class AggregatedFuturesFeed:
    """
    Concurrent multi-exchange perpetual futures order flow aggregator.
    """

    def __init__(self, timeout_sec: float = 3.5, max_workers: int = 4):
        self.timeout_sec = timeout_sec
        self.max_workers = max_workers

    # -------------------------------------------------------------------------
    # Venue 1: Binance Futures (Deepest Liquidity & Genuine Taker Volume)
    # -------------------------------------------------------------------------
    def fetch_binance(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 300,
        now_ms: Optional[int] = None
    ) -> Dict[int, Dict[str, Any]]:
        api_sym = VENUE_SYMBOL_MAP.get(symbol, {}).get("binance", f"{symbol}USDT")
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={api_sym}&interval={interval}&limit={limit}"
        try:
            raw = _http_get_json(url, timeout=self.timeout_sec)
            now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
            mult = VENUE_MULTIPLIERS.get(symbol, {}).get("binance", 1.0)
            bars = {}
            for k in raw:
                t_open = int(k[0])
                t_close = int(k[6])
                if t_close > now_ms:
                    continue  # ignore currently forming bar
                tot_vol = float(k[5]) * mult
                taker_buy = float(k[9]) * mult
                sell_vol = max(tot_vol - taker_buy, 0.0)
                bars[t_open] = {
                    "close_time_ms": t_close,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "total_volume": tot_vol,
                    "buy_volume": taker_buy,
                    "sell_volume": sell_vol,
                    "delta": taker_buy - sell_vol,
                    "total_trades": float(k[8]),
                    "has_real_taker": True,
                }
            return bars
        except Exception as e:
            logger.debug(f"Binance fetch notice for {symbol}: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Venue 2: OKX Derivatives
    # -------------------------------------------------------------------------
    def fetch_okx(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 300,
        now_ms: Optional[int] = None
    ) -> Dict[int, Dict[str, Any]]:
        api_sym = VENUE_SYMBOL_MAP.get(symbol, {}).get("okx", f"{symbol}-USDT-SWAP")
        now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        mult = VENUE_MULTIPLIERS.get(symbol, {}).get("okx", 1.0)
        step_ms = 900000 if "15" in interval else (300000 if "5" in interval else 60000)
        needed = min(limit, 1000)
        pages = (needed + 99) // 100
        bars = {}
        after_param = ""

        try:
            for _ in range(pages):
                url = f"https://www.okx.com/api/v5/market/candles?instId={api_sym}&bar={interval}&limit=100{after_param}"
                res = _http_get_json(url, timeout=self.timeout_sec)
                data = res.get("data", []) if isinstance(res, dict) else []
                if not data:
                    break
                for k in data:
                    t_open = int(k[0])
                    t_close = t_open + step_ms - 1
                    if t_close > now_ms:
                        continue
                    if t_open not in bars:
                        o = float(k[1])
                        h = float(k[2])
                        l = float(k[3])
                        c = float(k[4])
                        tot_vol = float(k[6]) * mult
                        bars[t_open] = {
                            "close_time_ms": t_close,
                            "open": o,
                            "high": h,
                            "low": l,
                            "close": c,
                            "total_volume": tot_vol,
                            "buy_volume": tot_vol * 0.5,
                            "sell_volume": tot_vol * 0.5,
                            "delta": 0.0,
                            "total_trades": 0.0,
                            "has_real_taker": False,
                        }
                if len(bars) >= needed:
                    break
                oldest_t = data[-1][0]
                after_param = f"&after={oldest_t}"
            return bars
        except Exception as e:
            logger.debug(f"OKX fetch notice for {symbol}: {e}")
            return bars

    # -------------------------------------------------------------------------
    # Venue 3: Bybit Linear Perpetuals
    # -------------------------------------------------------------------------
    def fetch_bybit(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 300,
        now_ms: Optional[int] = None
    ) -> Dict[int, Dict[str, Any]]:
        api_sym = VENUE_SYMBOL_MAP.get(symbol, {}).get("bybit", f"{symbol}USDT")
        bybit_interval = interval.replace("m", "")
        fetch_limit = min(limit, 1000)
        url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={api_sym}&interval={bybit_interval}&limit={fetch_limit}"
        try:
            res = _http_get_json(url, timeout=self.timeout_sec)
            data = res.get("result", {}).get("list", []) if isinstance(res, dict) else []
            now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
            mult = VENUE_MULTIPLIERS.get(symbol, {}).get("bybit", 1.0)
            bars = {}
            for k in data:
                t_open = int(k[0])
                step_ms = 900000 if "15" in interval else (300000 if "5" in interval else 60000)
                t_close = t_open + step_ms - 1
                if t_close > now_ms:
                    continue
                o = float(k[1])
                h = float(k[2])
                l = float(k[3])
                c = float(k[4])
                tot_vol = float(k[5]) * mult
                bars[t_open] = {
                    "close_time_ms": t_close,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "total_volume": tot_vol,
                    "buy_volume": tot_vol * 0.5,
                    "sell_volume": tot_vol * 0.5,
                    "delta": 0.0,
                    "total_trades": 0.0,
                    "has_real_taker": False,
                }
            return bars
        except Exception as e:
            logger.debug(f"Bybit fetch notice for {symbol}: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Venue 4: Hyperliquid DEX Perpetuals
    # -------------------------------------------------------------------------
    def fetch_hyperliquid(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 300,
        now_ms: Optional[int] = None
    ) -> Dict[int, Dict[str, Any]]:
        # Skip HL for tokens not supported on Hyperliquid
        cfg = VENUE_SYMBOL_MAP.get(symbol)
        if cfg and "hl" in cfg:
            api_sym = cfg["hl"]
        elif symbol in ("BTC", "ETH", "SOL", "DOGE", "XRP", "BNB", "ADA", "LINK", "AVAX", "NEAR", "SUI", "PEPE"):
            api_sym = symbol
        else:
            return {}  # Non-supported token on Hyperliquid

        url = "https://api.hyperliquid.xyz/info"
        step_ms = 900000 if "15" in interval else (300000 if "5" in interval else 60000)
        start_time = int((time.time() - limit * (step_ms / 1000.0)) * 1000)
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": api_sym, "interval": interval, "startTime": start_time}
        }
        try:
            raw = _http_post_json(url, payload, timeout=self.timeout_sec)
            now_ms = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
            mult = VENUE_MULTIPLIERS.get(symbol, {}).get("hl", 1.0)
            bars = {}
            if isinstance(raw, list):
                for k in raw:
                    t_open = int(k.get("t", 0))
                    t_close = int(k.get("T", t_open + step_ms - 1))
                    if t_close > now_ms:
                        continue
                    o = float(k.get("o", 0.0))
                    h = float(k.get("h", 0.0))
                    l = float(k.get("l", 0.0))
                    c = float(k.get("c", 0.0))
                    tot_vol = float(k.get("v", 0.0)) * mult
                    trades = float(k.get("n", 0.0))
                    bars[t_open] = {
                        "close_time_ms": t_close,
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "total_volume": tot_vol,
                        "buy_volume": tot_vol * 0.5,
                        "sell_volume": tot_vol * 0.5,
                        "delta": 0.0,
                        "total_trades": trades,
                        "has_real_taker": False,
                    }
            return bars
        except Exception as e:
            logger.debug(f"Hyperliquid fetch notice for {symbol}: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Unified Aggregator (Binance + OKX + Bybit + Hyperliquid)
    # -------------------------------------------------------------------------
    def fetch_aggregated_bars(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 300
    ) -> List[Dict[str, Any]]:
        """
        Queries all 4 platforms concurrently, synchronizes timestamps, and merges
        volume and order flow metrics into CoinGlass-parity Aggregated Futures bars.
        """
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        venues: Dict[str, Dict[int, Dict[str, Any]]] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_venue = {
                executor.submit(self.fetch_binance, symbol, interval, limit, now_ms): "binance",
                executor.submit(self.fetch_okx, symbol, interval, limit, now_ms): "okx",
                executor.submit(self.fetch_bybit, symbol, interval, limit, now_ms): "bybit",
                executor.submit(self.fetch_hyperliquid, symbol, interval, limit, now_ms): "hl",
            }
            for fut in as_completed(future_to_venue):
                v_name = future_to_venue[fut]
                try:
                    venues[v_name] = fut.result()
                except Exception as e:
                    logger.debug(f"Venue {v_name} error: {e}")
                    venues[v_name] = {}

        # Select primary anchor venue (Binance preferred, fallback to Bybit or OKX)
        anchor_venue = None
        for cand in ("binance", "bybit", "okx", "hl"):
            if venues.get(cand):
                anchor_venue = cand
                break

        if not anchor_venue:
            logger.warning(f"All derivatives venues failed for {symbol}; unable to aggregate.")
            return []

        anchor_bars = venues[anchor_venue]
        secondary_venues = [venues[v] for v in ("binance", "okx", "bybit", "hl") if v != anchor_venue and v in venues]

        sorted_times = sorted(anchor_bars.keys())
        aggregated: List[Dict[str, Any]] = []

        for t in sorted_times:
            anc = anchor_bars[t]
            tot_vol = float(anc["total_volume"])
            trades = float(anc.get("total_trades", 0.0))

            matching_secondaries = []
            for sec in secondary_venues:
                b_sec = sec.get(t)
                if b_sec:
                    tot_vol += float(b_sec["total_volume"])
                    trades += float(b_sec.get("total_trades", 0.0))
                    matching_secondaries.append(b_sec)

            # Microstructure-Preserving Order Flow Scaling:
            # Anchor buy/sell ratio to genuine taker tape (Binance) when available
            binance_bar = venues.get("binance", {}).get(t)
            if binance_bar and binance_bar.get("has_real_taker") and binance_bar["total_volume"] > 0:
                b_tot = binance_bar["total_volume"]
                b_buy = binance_bar["buy_volume"]
                buy_ratio = b_buy / b_tot
                delta_ratio = binance_bar["delta"] / b_tot
                buy_vol = tot_vol * buy_ratio
                sell_vol = max(tot_vol - buy_vol, 0.0)
                delta = tot_vol * delta_ratio
            elif anc.get("has_real_taker") and anc["total_volume"] > 0:
                a_tot = anc["total_volume"]
                buy_ratio = anc["buy_volume"] / a_tot
                buy_vol = tot_vol * buy_ratio
                sell_vol = max(tot_vol - buy_vol, 0.0)
                delta = tot_vol * (anc["delta"] / a_tot)
            else:
                # Neutral 50/50 fallback if real taker tape is missing on this specific bar
                buy_vol = tot_vol * 0.5
                sell_vol = tot_vol * 0.5
                delta = 0.0

            # Price levels are anchored to the primary liquid market with price normalization
            p_mult = VENUE_PRICE_MULTIPLIERS.get(symbol, {}).get(anchor_venue, 1.0)
            aggregated.append({
                "open_time_ms": t,
                "close_time_ms": anc["close_time_ms"],
                "open": anc["open"] * p_mult,
                "high": anc["high"] * p_mult,
                "low": anc["low"] * p_mult,
                "close": anc["close"] * p_mult,
                "total_volume": tot_vol,
                "buy_volume": buy_vol,
                "sell_volume": sell_vol,
                "delta": delta,
                "total_trades": trades,
                "open_interest": 0.0,
                "aggregated_venues": 1 + len(matching_secondaries),
            })

        return aggregated
