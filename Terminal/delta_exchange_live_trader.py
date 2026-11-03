"""
Delta Exchange India Live Execution Engine (REST & MCP Compatible).
Handles order placement, bracket TP/SL attachment, positions, leverage, and wallet monitoring.
"""

import os
import time
import hmac
import hashlib
import json
import math
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
import requests

from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    MAX_RISK_USD,
    MAX_OPEN_POSITIONS,
    MIN_RR_RATIO,
    validate_trade_geometry
)

logger = logging.getLogger("DeltaExchangeTrader")
logger.setLevel(logging.INFO)

CONFIG_ENV_PATH = Path.home() / ".delta-exchange-mcp" / "config.env"
BASE_URL = "https://api.india.delta.exchange"
MAX_DELTA_CONCURRENT_POSITIONS = MAX_OPEN_POSITIONS  # Strict SSOT from risk_envelope (2)
LIVE_DISPATCH_ENABLED = False  # Strict default fail-closed operator safety gate


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

    return {"api_key": key or "", "api_secret": secret or "", "env": env}


# Canonical pre-populated product metadata for zero-network resolution in tests
DEFAULT_PRODUCTS = {
    "BTCUSD": {"id": 1, "symbol": "BTCUSD", "underlying": "BTC", "contract_value": 0.001, "tick_size": 0.5, "default_leverage": 10.0, "status": "operational"},
    "ETHUSD": {"id": 2, "symbol": "ETHUSD", "underlying": "ETH", "contract_value": 0.01, "tick_size": 0.05, "default_leverage": 10.0, "status": "operational"},
    "SOLUSD": {"id": 3, "symbol": "SOLUSD", "underlying": "SOL", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 10.0, "status": "operational"},
    "XRPUSD": {"id": 4, "symbol": "XRPUSD", "underlying": "XRP", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 10.0, "status": "operational"},
    "LTCUSD": {"id": 5, "symbol": "LTCUSD", "underlying": "LTC", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 10.0, "status": "operational"},
    "DOGEUSD": {"id": 6, "symbol": "DOGEUSD", "underlying": "DOGE", "contract_value": 10.0, "tick_size": 0.00001, "default_leverage": 10.0, "status": "operational"},
    "BNBUSD": {"id": 7, "symbol": "BNBUSD", "underlying": "BNB", "contract_value": 0.01, "tick_size": 0.1, "default_leverage": 10.0, "status": "operational"},
    "AVAXUSD": {"id": 8, "symbol": "AVAXUSD", "underlying": "AVAX", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 10.0, "status": "operational"},
    "LINKUSD": {"id": 9, "symbol": "LINKUSD", "underlying": "LINK", "contract_value": 0.1, "tick_size": 0.001, "default_leverage": 10.0, "status": "operational"},
    "NEARUSD": {"id": 10, "symbol": "NEARUSD", "underlying": "NEAR", "contract_value": 1.0, "tick_size": 0.001, "default_leverage": 10.0, "status": "operational"},
    "SUIUSD": {"id": 11, "symbol": "SUIUSD", "underlying": "SUI", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 10.0, "status": "operational"},
    "TRXUSD": {"id": 12, "symbol": "TRXUSD", "underlying": "TRX", "contract_value": 10.0, "tick_size": 0.00001, "default_leverage": 10.0, "status": "operational"},
    "1INCHUSD": {"id": 13, "symbol": "1INCHUSD", "underlying": "1INCH", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 10.0, "status": "operational"},
    "ZECUSD": {"id": 98573, "symbol": "ZECUSD", "underlying": "ZEC", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 10.0, "status": "operational"},
    "SUSHIUSD": {"id": 14, "symbol": "SUSHIUSD", "underlying": "SUSHI", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 10.0, "status": "operational"},
    "ARUSD": {"id": 15, "symbol": "ARUSD", "underlying": "AR", "contract_value": 0.1, "tick_size": 0.001, "default_leverage": 10.0, "status": "operational"}
}


class DeltaExchangeTrader:
    def __init__(self):
        creds = load_credentials()
        self.api_key = creds.get("api_key", "")
        self.api_secret = creds.get("api_secret", "")
        self.server_time_offset = 0.0
        self.products_cache: Dict[str, Dict[str, Any]] = dict(DEFAULT_PRODUCTS)
        self._inflight_orders: Set[str] = set()
        if self.api_key and self.api_secret:
            try:
                self._sync_server_time()
                self._load_products()
            except Exception as e:
                logger.debug(f"Delta remote initialization deferred: {e}")

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

    def _get_cdp_positions(self) -> List[Dict[str, Any]]:
        """Fallback position reader directly querying open Delta Exchange Edge tab via CDP."""
        try:
            import urllib.request, websocket, re
            req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=2) as resp:
                tabs = json.loads(resp.read().decode())
            delta_tab = next((t for t in tabs if "delta.exchange" in t.get("url", "")), None)
            if not delta_tab:
                return []
            ws = websocket.create_connection(delta_tab["webSocketDebuggerUrl"], timeout=3)
            js = "Array.from(document.querySelectorAll('table')[3].querySelectorAll('tr')).map(r => Array.from(r.querySelectorAll('td, th')).map(c => c.innerText.trim().replace(/\\n+/g, ' ')))"
            ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {"expression": js, "returnByValue": True}}))
            res = json.loads(ws.recv())
            ws.close()
            rows = res.get("result", {}).get("result", {}).get("value", [])
            parsed = []
            for cols in rows[1:]:
                if len(cols) >= 8:
                    sym = cols[1].replace("Current ", "").strip()
                    size_str = cols[2]
                    entry_str = cols[4]
                    tpsl_str = cols[5]
                    mark_str = cols[7]
                    upnl_str = cols[11] if len(cols) > 11 else "0.0"
                    size_match = re.search(r'([-+]?\d+)', size_str)
                    size_val = int(size_match.group(1)) if size_match else 0
                    
                    upnl_val = 0.0
                    upnl_pct = 0.0
                    if upnl_str:
                        usd_match = re.search(r'([-+]?\d*\.?\d+)\s*USD', upnl_str)
                        if usd_match:
                            try:
                                upnl_val = float(usd_match.group(1))
                            except ValueError:
                                pass
                        pct_match = re.search(r'([-+]?\d*\.?\d+)\s*%', upnl_str)
                        if pct_match:
                            try:
                                upnl_pct = float(pct_match.group(1))
                            except ValueError:
                                pass

                    parsed.append({
                        "product_symbol": sym,
                        "size": str(size_val),
                        "entry_price": entry_str,
                        "mark_price": mark_str,
                        "tpsl_info": tpsl_str,
                        "unrealized_pnl": upnl_val,
                        "unrealized_pnl_str": upnl_str,
                        "unrealized_pnl_pct": upnl_pct,
                        "venue": "DELTA_CDP_ACTIVE"
                    })
            return parsed
        except Exception as e:
            logger.debug(f"CDP positions fallback error: {e}")
            return []

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Returns all open margined positions (REST primary, CDP fallback)."""
        res = self.request("GET", "/v2/positions/margined")
        if res.get("result") is not None:
            return res.get("result", [])
        return self._get_cdp_positions()

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

    def close_position(self, symbol: str) -> Dict[str, Any]:
        """
        Market closes any active open position for the given symbol (reduce-only).
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "")
        for p in self.get_active_positions():
            p_sym = p.get("product_symbol", "")
            if clean in p_sym:
                prod = self.resolve_product(p_sym)
                if not prod:
                    continue
                size = abs(int(p.get("size", 0)))
                if size <= 0:
                    continue
                # Opposing side to close
                cur_size = int(p.get("size", 0))
                close_side = "sell" if cur_size > 0 else "buy"
                body = {
                    "product_id": prod["id"],
                    "size": size,
                    "side": close_side,
                    "order_type": "market_order",
                    "reduce_only": True
                }
                logger.info(f"🚨 [EMERGENCY CLOSE] Closing Delta position for {p_sym}: {close_side} {size} contracts")
                return self.request("POST", "/v2/orders", body=body)
        return {"ok": False, "status": "NOT_FOUND", "reason": f"No open position found for {symbol}"}

    def get_fills(self, symbol: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieves fill history from broker for exact realized PnL post-mortem reconciliation."""
        if not self.api_key:
            return []
        try:
            params = {"limit": limit}
            if symbol:
                params["symbol"] = symbol
            res = self.request("GET", "/v2/fills", params=params)
            return res.get("result", []) if isinstance(res, dict) else []
        except Exception as e:
            logger.warning(f"Error fetching Delta fills: {e}")
            return []

    def get_orders_history(self, symbol: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieves closed/cancelled order history."""
        if not self.api_key:
            return []
        try:
            params = {"limit": limit}
            if symbol:
                params["symbol"] = symbol
            res = self.request("GET", "/v2/orders/history", params=params)
            return res.get("result", []) if isinstance(res, dict) else []
        except Exception as e:
            logger.warning(f"Error fetching Delta order history: {e}")
            return []

    def execute_delta_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates signal parameters, checks Delta position limits, and punches live order
        with attached bracket Stop-Loss and Take-Profit.
        """
        if not LIVE_DISPATCH_ENABLED:
            logger.warning("Delta live dispatch blocked: LIVE_DISPATCH_ENABLED is False (fail-closed operator safety gate).")
            return {"ok": False, "status": "BLOCKED_BY_SAFETY_GATE", "reason": "LIVE_DISPATCH_ENABLED_IS_FALSE"}

        sym = signal.get("symbol", "")
        direction = signal.get("direction", "").upper()
        if direction not in ("BUY", "SELL"):
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_DIRECTION: {direction}"}

        prod = self.resolve_product(sym)
        if not prod:
            return {"ok": False, "status": "REJECTED", "reason": f"SYMBOL_NOT_ON_DELTA_INDIA: {sym}"}

        # Check in-flight orders to prevent concurrent duplicate dispatch
        if prod["symbol"] in self._inflight_orders:
            return {"ok": False, "status": "REJECTED", "reason": f"ORDER_ALREADY_IN_FLIGHT: {prod['symbol']}"}

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

        trade_params = signal.get("trade_parameters") or signal.get("trade_levels") or {}
        entry = float(trade_params.get("entry_price") or signal.get("price") or 0.0)
        sl = trade_params.get("stop_loss")
        tp = trade_params.get("take_profit_1")

        # Fail-closed bracket invariant: SL and TP must be present and finite
        if not sl or not tp or not math.isfinite(sl) or not math.isfinite(tp):
            return {"ok": False, "status": "REJECTED", "reason": "MISSING_MANDATORY_BRACKET_SL_TP"}

        dir_norm = "BUY" if direction in ("BUY", "BULLISH", "LONG") else "SELL"
        geom_ok, geom_reason = validate_trade_geometry(
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            direction=dir_norm,
            min_rr=MIN_RR_RATIO
        )
        if not geom_ok:
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}

        side = "buy" if direction in ("BUY", "BULLISH", "LONG") else "sell"

        # Rigorous Discrete Contract Sizing via MAX_RISK_USD ($25.00)
        from Terminal.risk_envelope import MAX_RISK_USD
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        risk_dist = abs(entry - sl)
        if risk_dist > 0 and contract_val > 0:
            calc_contracts = int(MAX_RISK_USD / (risk_dist * contract_val))
            size = max(1, min(calc_contracts, 100))
        else:
            size = 1

        self._inflight_orders.add(prod["symbol"])
        try:
            logger.info(f"Executing Delta Live Order: {prod['symbol']} {side} {size} contracts | Entry: {entry} | SL: {sl} | TP: {tp} | Leverage: 10x")
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
                "size": size,
                "order": res.get("result", {}),
                "raw_response": res
            }
        finally:
            self._inflight_orders.discard(prod["symbol"])


# Global singleton instance
delta_client = DeltaExchangeTrader()
