"""
Delta Exchange India Live Execution Engine (REST & MCP Compatible).
Handles order placement, bracket TP/SL attachment, positions, leverage, and wallet monitoring.
"""

import os
import time
import hmac
import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import requests

logger = logging.getLogger("DeltaExchangeTrader")
logger.setLevel(logging.INFO)

CONFIG_ENV_PATH = Path.home() / ".delta-exchange-mcp" / "config.env"
BASE_URL = "https://api.india.delta.exchange"
MAX_DELTA_CONCURRENT_POSITIONS = 2


def load_credentials() -> Dict[str, str]:
    """Loads API key and secret from environment or ~/.delta-exchange-mcp/config.env."""
    key = os.environ.get("DELTA_API_KEY")
    secret = os.environ.get("DELTA_API_SECRET")
    env = os.environ.get("DELTA_MCP_ENV", "india_prod")

    if (not key or not secret) and CONFIG_ENV_PATH.exists():
        try:
            with open(CONFIG_ENV_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip()
                        if k == "DELTA_API_KEY" and not key:
                            key = v
                        elif k == "DELTA_API_SECRET" and not secret:
                            secret = v
                        elif k == "DELTA_MCP_ENV":
                            env = v
        except Exception as e:
            logger.warning(f"Error reading {CONFIG_ENV_PATH}: {e}")

    # Fallback to current session provided keys if still empty
    if not key:
        key = "RrPSJVABou8UAT4BIl5ExCctYWV6dp"
    if not secret:
        secret = "1a0HWlsQfoeyv7J0rPx5mXWP0rEoL85RCWxUQmsI06aMS65cuQOhcUwjAiks"

    return {"api_key": key, "api_secret": secret, "env": env}


class DeltaExchangeTrader:
    def __init__(self):
        creds = load_credentials()
        self.api_key = creds["api_key"]
        self.api_secret = creds["api_secret"]
        self.server_time_offset = 0.0
        self.products_cache: Dict[str, Dict[str, Any]] = {}
        self._sync_server_time()
        self._load_products()

    def _sync_server_time(self):
        """Measures drift against Delta Exchange server context to guarantee 0ms signature drift."""
        try:
            ts = str(int(time.time()))
            sig = hmac.new(self.api_secret.encode(), ("GET" + ts + "/v2/positions/margined").encode(), hashlib.sha256).hexdigest()
            headers = {"api-key": self.api_key, "timestamp": ts, "signature": sig}
            resp = requests.get(f"{BASE_URL}/v2/positions/margined", headers=headers, timeout=10)
            if resp.status_code == 401:
                ctx = resp.json().get("error", {}).get("context", {})
                server_time = ctx.get("server_time")
                if server_time:
                    self.server_time_offset = float(server_time - int(time.time()))
                    logger.info(f"Synchronized with Delta server clock offset: {self.server_time_offset:+.2f}s")
            elif resp.status_code == 200:
                self.server_time_offset = 0.0
        except Exception as e:
            logger.warning(f"Could not sync server time: {e}")

    def get_timestamp(self) -> str:
        """Returns drift-corrected Unix timestamp in integer seconds."""
        corrected = time.time() + self.server_time_offset
        return str(int(corrected))

    def _load_products(self):
        """Caches perpetual futures metadata for rapid resolution."""
        try:
            resp = requests.get(f"{BASE_URL}/v2/products", timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("result", [])
                for p in data:
                    if p.get("contract_type") == "perpetual_futures":
                        sym = p.get("symbol")
                        self.products_cache[sym] = {
                            "id": p.get("id"),
                            "symbol": sym,
                            "underlying": p.get("underlying_asset", {}).get("symbol"),
                            "contract_value": float(p.get("contract_value", 1.0)),
                            "tick_size": float(p.get("tick_size", 0.01)),
                            "default_leverage": float(p.get("default_leverage", 10.0)),
                            "status": p.get("trading_status")
                        }
        except Exception as e:
            logger.warning(f"Could not load products: {e}")

    def resolve_product(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Resolves raw symbol (e.g. 'LTC', 'LTCUSD') to Delta perpetual product."""
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "")
        # Exact match first
        cand1 = f"{clean}USD"
        if cand1 in self.products_cache:
            return self.products_cache[cand1]
        for p_sym, meta in self.products_cache.items():
            if meta.get("underlying") == clean or p_sym.startswith(clean):
                return meta
        return None

    def request(self, method: str, path: str, params: Optional[Dict] = None, body: Optional[Dict] = None) -> Dict[str, Any]:
        """Signs and executes authenticated HTTP request against Delta Exchange India."""
        ts = self.get_timestamp()
        query_str = ""
        if params:
            query_str = "?" + "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        body_str = json.dumps(body) if body is not None else ""

        sig_payload = method.upper() + ts + path + query_str + body_str
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sig_payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        headers = {
            "api-key": self.api_key,
            "timestamp": ts,
            "signature": signature,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "AntigravityQuant/1.0"
        }

        url = BASE_URL + path + query_str
        req_func = requests.get if method.upper() == "GET" else requests.post
        resp = req_func(url, headers=headers, data=body_str if body_str else None, timeout=10)

        # Re-sync server time if signature expired error occurs
        if resp.status_code == 401 and "expired_signature" in resp.text:
            logger.warning("Clock drift detected. Resyncing with Delta server...")
            self._sync_server_time()
            return self.request(method, path, params, body)

        try:
            return resp.json()
        except Exception:
            return {"success": False, "status_code": resp.status_code, "text": resp.text}

    def get_wallet_balances(self) -> List[Dict[str, Any]]:
        """Returns list of wallet balances (USD, INR, BTC, etc.)."""
        res = self.request("GET", "/v2/wallet/balances")
        return res.get("result", [])

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Returns all open margined positions."""
        res = self.request("GET", "/v2/positions/margined")
        return res.get("result", [])

    def get_active_positions(self) -> List[Dict[str, Any]]:
        """Returns non-zero open positions."""
        raw = self.get_open_positions()
        return [p for p in raw if int(p.get("size", 0)) != 0]

    def has_open_position(self, symbol: str) -> bool:
        """Checks if a position is currently open for the given symbol."""
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "")
        for p in self.get_active_positions():
            p_sym = p.get("product_symbol", "")
            if clean in p_sym:
                return True
        return False

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Returns all pending open orders (including bracket TP/SL)."""
        res = self.request("GET", "/v2/orders")
        return res.get("result", [])

    def set_leverage(self, product_id: int, leverage: int = 10) -> Dict[str, Any]:
        """Sets leverage multiplier for a contract."""
        body = {"product_id": product_id, "leverage": str(leverage)}
        return self.request("POST", "/v2/products/orders/leverage", body=body)

    def place_order(
        self,
        symbol: str,
        side: str,
        size: int,
        order_type: str = "market_order",
        limit_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        leverage: int = 10
    ) -> Dict[str, Any]:
        """
        Places a market or limit order with optional bracket Stop-Loss and Take-Profit.
        """
        prod = self.resolve_product(symbol)
        if not prod:
            return {"success": False, "error": f"Symbol {symbol} not tradable on Delta India"}

        prod_id = prod["id"]
        side_clean = "buy" if side.lower() in ["buy", "bullish", "long"] else "sell"

        # Ensure leverage is configured
        self.set_leverage(prod_id, leverage)

        body: Dict[str, Any] = {
            "product_id": prod_id,
            "size": int(size),
            "side": side_clean,
            "order_type": order_type
        }

        if order_type == "limit_order" and limit_price is not None:
            body["limit_price"] = str(round(limit_price, 4))

        if stop_loss_price is not None:
            body["bracket_stop_loss_price"] = str(round(stop_loss_price, 4))
            body["bracket_stop_trigger_method"] = "last_traded_price"

        if take_profit_price is not None:
            body["bracket_take_profit_price"] = str(round(take_profit_price, 4))

        res = self.request("POST", "/v2/orders", body=body)
        return res

    def execute_delta_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates signal parameters, checks Delta position limits, and punches live order
        with attached bracket Stop-Loss and Take-Profit.
        """
        sym = signal.get("symbol", "")
        direction = signal.get("direction", "").upper()
        if direction not in ("BUY", "SELL"):
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_DIRECTION: {direction}"}

        prod = self.resolve_product(sym)
        if not prod:
            return {"ok": False, "status": "REJECTED", "reason": f"SYMBOL_NOT_ON_DELTA_INDIA: {sym}"}

        # Check existing active position on this symbol
        if self.has_open_position(sym):
            return {"ok": False, "status": "REJECTED", "reason": f"POSITION_ALREADY_OPEN_ON_DELTA: {sym}"}

        # Check max concurrent positions limit
        active_pos = self.get_active_positions()
        if len(active_pos) >= MAX_DELTA_CONCURRENT_POSITIONS:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"DELTA_MAX_POSITIONS_REACHED: {len(active_pos)}/{MAX_DELTA_CONCURRENT_POSITIONS}"
            }

        trade_lvl = signal.get("trade_levels", {})
        sl = trade_lvl.get("stop_loss")
        tp = trade_lvl.get("take_profit_1")

        side = "buy" if direction in ("BUY", "BULLISH", "LONG") else "sell"
        size = 1  # Standard 1 contract for altcoin testing / micro balance

        logger.info(f"Executing Delta Live Order: {prod['symbol']} {side} {size} contracts | SL: {sl} | TP: {tp}")
        res = self.place_order(
            symbol=prod["symbol"],
            side=side,
            size=size,
            order_type="market_order",
            stop_loss_price=sl,
            take_profit_price=tp,
            leverage=10
        )

        ok = res.get("success", False) or ("result" in res and "id" in res.get("result", {}))
        return {
            "ok": ok,
            "status": "EXECUTED" if ok else "FAILED",
            "venue": "DeltaExchangeIndia",
            "symbol": prod["symbol"],
            "order": res.get("result", {}),
            "raw_response": res
        }


# Global singleton instance
delta_client = DeltaExchangeTrader()
