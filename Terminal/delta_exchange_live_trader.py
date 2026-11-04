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
from Terminal.runtime_io import FileMutex
from Terminal.position_anchors import (
    register_position_anchor,
    get_or_recover_anchor,
    update_anchor_tier,
    prune_closed_positions,
    load_position_anchors,
    DB_DIR
)

logger = logging.getLogger("DeltaExchangeTrader")
logger.setLevel(logging.INFO)

CONFIG_ENV_PATH = Path.home() / ".delta-exchange-mcp" / "config.env"
BASE_URL = "https://api.india.delta.exchange"
MAX_DELTA_CONCURRENT_POSITIONS = MAX_OPEN_POSITIONS  # Strict SSOT from risk_envelope (2)
LIVE_DISPATCH_ENABLED = os.environ.get("DELTA_LIVE_DISPATCH", "false").lower() in ("true", "1", "yes")


def set_live_dispatch(enabled: bool):
    """Dynamically enables or disables live Delta Exchange dispatch."""
    global LIVE_DISPATCH_ENABLED
    LIVE_DISPATCH_ENABLED = enabled
    logger.info(f"Delta Live Dispatch state set to: {LIVE_DISPATCH_ENABLED}")


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
    "BTCUSD": {"id": 1, "symbol": "BTCUSD", "underlying": "BTC", "contract_value": 0.001, "tick_size": 0.5, "default_leverage": 100.0, "status": "operational"},
    "ETHUSD": {"id": 2, "symbol": "ETHUSD", "underlying": "ETH", "contract_value": 0.01, "tick_size": 0.05, "default_leverage": 100.0, "status": "operational"},
    "SOLUSD": {"id": 3, "symbol": "SOLUSD", "underlying": "SOL", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 50.0, "status": "operational"},
    "XRPUSD": {"id": 4, "symbol": "XRPUSD", "underlying": "XRP", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 50.0, "status": "operational"},
    "DOGEUSD": {"id": 5, "symbol": "DOGEUSD", "underlying": "DOGE", "contract_value": 10.0, "tick_size": 0.00001, "default_leverage": 50.0, "status": "operational"},
    "BNBUSD": {"id": 6, "symbol": "BNBUSD", "underlying": "BNB", "contract_value": 0.1, "tick_size": 0.05, "default_leverage": 50.0, "status": "operational"},
    "SUIUSD": {"id": 7, "symbol": "SUIUSD", "underlying": "SUI", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 20.0, "status": "operational"},
    "NEARUSD": {"id": 16615, "symbol": "NEARUSD", "underlying": "NEAR", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 20.0, "status": "operational"},
    "TRXUSD": {"id": 20193, "symbol": "TRXUSD", "underlying": "TRX", "contract_value": 1.0, "tick_size": 0.00001, "default_leverage": 50.0, "status": "operational"},
    "LTCUSD": {"id": 10, "symbol": "LTCUSD", "underlying": "LTC", "contract_value": 0.1, "tick_size": 0.01, "default_leverage": 50.0, "status": "operational"},
    "LINKUSD": {"id": 15041, "symbol": "LINKUSD", "underlying": "LINK", "contract_value": 1.0, "tick_size": 0.001, "default_leverage": 20.0, "status": "operational"},
    "ARUSD": {"id": 21523, "symbol": "ARUSD", "underlying": "AR", "contract_value": 1.0, "tick_size": 0.001, "default_leverage": 20.0, "status": "operational"},
    "SUSHIUSD": {"id": 21525, "symbol": "SUSHIUSD", "underlying": "SUSHI", "contract_value": 1.0, "tick_size": 0.0001, "default_leverage": 20.0, "status": "operational"},
    "HBARUSD": {"id": 18288, "symbol": "HBARUSD", "underlying": "HBAR", "contract_value": 1.0, "tick_size": 0.00001, "default_leverage": 10.0, "status": "operational"}
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
        method_upper = method.upper()
        if method_upper == 'GET':
            req_func = requests.get
        elif method_upper == 'PUT':
            req_func = requests.put
        elif method_upper == 'DELETE':
            req_func = requests.delete
        else:
            req_func = requests.post
        resp = req_func(url, headers=headers, data=body_str if body_str else None, timeout=10)

        # Re-sync server time if signature expired error occurs
        if resp.status_code == 401 and "expired_signature" in resp.text:
            logger.warning("Clock drift detected. Resyncing with Delta server...")
            self._sync_server_time()
            return self.request(method, path, params, body)

        try:
            data = resp.json()
            if not data.get("success") and data.get("error", {}).get("code") == "ip_not_whitelisted_for_api_key":
                client_ip = data.get("error", {}).get("context", {}).get("client_ip", "unknown")
                logger.error(f"🚨 [DELTA API] Current egress IP {client_ip} is NOT whitelisted on your Delta India API key! Please add {client_ip} to your API key whitelist on Delta Exchange.")
            return data
        except Exception:
            return {"success": False, "status_code": resp.status_code, "text": resp.text}

    def get_wallet_balances(self) -> List[Dict[str, Any]]:
        """Returns list of wallet balances (USD, INR, BTC, etc.)."""
        res = self.request("GET", "/v2/wallet/balances")
        return res.get("result", [])

    def get_available_margin_usd(self) -> float:
        """Returns current available collateral margin in USD directly from authenticated REST API."""
        try:
            wb = self.get_wallet_balances()
            if isinstance(wb, list):
                for b in wb:
                    if b.get("asset_symbol") in ("USD", "USDT", "INR"):
                        return float(b.get("available_balance", 0.0) or 0.0)
        except Exception as e:
            logger.warning(f"Error fetching wallet available margin: {e}")
        return 0.0

    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Returns all open margined positions (REST primary, CDP fallback)."""
        res = self.request("GET", "/v2/positions/margined")
        if res.get("result") is not None:
            return res.get("result", [])
        return []

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

    def quantize_price(self, price: float, product_id: int) -> str:
        """Quantizes price to the instrument's exact legal tick size and precision."""
        from decimal import Decimal
        p_val = float(price)
        prod = next((p for p in self.products_cache.values() if int(p.get("id", 0)) == int(product_id)), None)
        tick = float(prod.get("tick_size", 0.01)) if prod else 0.01
        if tick <= 0:
            tick = 0.01
        quantized = round(p_val / tick) * tick
        tick_dec = Decimal(str(tick)).normalize()
        decimals = max(0, -tick_dec.as_tuple().exponent)
        return f"{quantized:.{decimals}f}"

    def cancel_order(self, order_id: int, product_id: int) -> Dict[str, Any]:
        """Cancels an existing order on Delta Exchange India."""
        return self.request("DELETE", "/v2/orders", body={"id": int(order_id), "product_id": int(product_id)})

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Returns all open and pending conditional orders (including bracket TP/SL)."""
        res = self.request("GET", "/v2/orders", params={"states": "open,pending"})
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
            body["limit_price"] = self.quantize_price(limit_price, prod_id)

        if stop_loss_price is not None:
            body["bracket_stop_loss_price"] = self.quantize_price(stop_loss_price, prod_id)
            body["bracket_stop_trigger_method"] = "mark_price"

        if take_profit_price is not None:
            body["bracket_take_profit_price"] = self.quantize_price(take_profit_price, prod_id)

        res = self.request("POST", "/v2/orders", body=body)
        return res

    def edit_order(
        self,
        order_id: int,
        product_id: int,
        size: int,
        stop_price: Optional[float] = None,
        limit_price: Optional[float] = None,
        reduce_only: bool = True
    ) -> Dict[str, Any]:
        """Edits an existing open or pending conditional order on Delta Exchange India."""
        body: Dict[str, Any] = {
            "id": int(order_id),
            "product_id": int(product_id),
            "size": int(size)
        }
        if stop_price is not None:
            body["stop_price"] = self.quantize_price(stop_price, product_id)
        if limit_price is not None:
            body["limit_price"] = self.quantize_price(limit_price, product_id)
        return self.request("PUT", "/v2/orders", body=body)

    def reanchor_brackets_to_fill(
        self,
        symbol: str,
        fill_price: float,
        sl_price: float,
        min_rr: float = 2.0
    ) -> Dict[str, Any]:
        """
        Ensures that the Take-Profit bracket order strictly achieves >= min_rr (default 2.0:1)
        calibrated against the actual executed market fill price (P_fill) and caps dollar risk.
        """
        if not fill_price or fill_price <= 0:
            return {"ok": False, "status": "SKIPPED", "reason": "INVALID_FILL_PRICE"}
        prod = self.resolve_product(symbol)
        if not prod:
            return {"ok": False, "status": "SKIPPED", "reason": "PROD_NOT_FOUND"}
        prod_id = prod["id"]
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)

        # Retry loop to wait for Delta backend to persist child bracket orders
        tp_order = None
        sl_order = None
        for attempt in range(4):
            orders = self.get_open_orders()
            for o in orders:
                if o.get("product_id") == prod_id:
                    s_type = str(o.get("stop_order_type", "")).lower()
                    if "take_profit" in s_type:
                        tp_order = o
                    elif "stop_loss" in s_type:
                        sl_order = o
            if tp_order and sl_order:
                break
            time.sleep(0.25)

        if not tp_order:
            return {"ok": False, "status": "NOT_FOUND", "reason": "TP_ORDER_NOT_LOCATED"}

        # Determine position direction from TP side (TP side is opposing)
        pos_direction = "BUY" if tp_order.get("side", "").lower() == "sell" else "SELL"
        dir_mult = 1.0 if pos_direction == "BUY" else -1.0
        size = int(tp_order.get("size", 1))

        # Actual risk distance from fill
        actual_risk = abs(fill_price - sl_price)
        from Terminal.risk_envelope import MAX_RISK_USD
        max_allowed_risk_usd = MAX_RISK_USD

        # Check if slippage inflated dollar risk beyond MAX_RISK_USD
        realized_risk_usd = actual_risk * size * contract_val
        calibrated_sl = sl_price
        tick_size = float(prod.get("tick_size", 0.01) or 0.01)
        if realized_risk_usd > max_allowed_risk_usd:
            # Capping SL inward towards fill to preserve MAX_RISK_USD
            # Directional inward floor so realized risk <= max_allowed_risk_usd strictly
            max_loss_ticks = math.floor(max_allowed_risk_usd / max(1e-9, (size * contract_val * tick_size)))
            capped_dist = max_loss_ticks * tick_size
            calibrated_sl = fill_price - (dir_mult * capped_dist)
            actual_risk = capped_dist
            if sl_order:
                self.edit_order(
                    order_id=sl_order["id"],
                    product_id=prod_id,
                    size=size,
                    stop_price=calibrated_sl
                )
                logger.info(f"[{symbol}] Calibrated SL inward to {calibrated_sl} to preserve ${max_allowed_risk_usd:.2f} risk cap")

        # Calibrate TP to guarantee at least min_rr:1
        # Directional ceiling so R:R >= min_rr strictly after tick quantization
        target_ticks = math.ceil((min_rr * actual_risk) / tick_size)
        calibrated_tp = fill_price + (dir_mult * target_ticks * tick_size)

        # Check current TP
        current_tp = float(tp_order.get("stop_price", 0.0) or 0.0)
        current_reward = abs(current_tp - fill_price)
        current_rr = current_reward / max(actual_risk, 1e-8)

        register_position_anchor(
            symbol=symbol,
            product_id=prod_id,
            direction=pos_direction,
            entry_price=fill_price,
            initial_sl=calibrated_sl,
            initial_tp=calibrated_tp,
            size=size,
            initial_risk_dist=actual_risk
        )

        if current_rr < min_rr or abs(calibrated_tp - current_tp) > float(prod.get("tick_size", 0.01)):
            edit_res = self.edit_order(
                order_id=tp_order["id"],
                product_id=prod_id,
                size=size,
                stop_price=calibrated_tp
            )
            logger.info(f"[{symbol}] Re-anchored TP from {current_tp} to {calibrated_tp} (R:R calibrated from {current_rr:.2f}:1 to {min_rr:.1f}:1 relative to fill {fill_price})")
            return {
                "ok": True,
                "status": "REANCHORED",
                "calibrated_tp": calibrated_tp,
                "calibrated_sl": calibrated_sl,
                "rr": min_rr,
                "edit_res": edit_res
            }
        return {"ok": True, "status": "ALREADY_COMPLIANT", "current_rr": current_rr}

    def audit_and_trail_positions(self) -> List[Dict[str, Any]]:
        """
        Audits all active Delta Exchange positions, enforces R:R >= 2.0, ratchets
        trailing stop-loss monotonically, recovers naked positions, purges duplicate orders,
        and cancels orphaned ghost orders with cross-process mutual exclusion.
        """
        reports = []
        with FileMutex(str(DB_DIR / ".locks" / "delta_position_management.lock"), timeout=5.0) as mgmt_lock:
            if not mgmt_lock.acquired:
                logger.info("Another process is actively auditing Delta positions. Yielding.")
                return reports
            return self._audit_and_trail_positions_guarded()

    def _audit_and_trail_positions_guarded(self) -> List[Dict[str, Any]]:
        reports = []
        raw_positions = self.get_active_positions()
        if raw_positions is None:
            return reports

        active_positions = [p for p in raw_positions if int(p.get("size", 0)) != 0]
        active_symbols = [p.get("product_symbol", "") for p in active_positions]

        # Invariant 1: Always prune closed positions immediately (even if 0 remain)
        prune_closed_positions(active_symbols)

        # Invariant 2: Orphaned ghost orders cleanup (cancel resting orders for closed positions)
        active_sym_set = {
            s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
            for s in active_symbols if s
        }
        open_orders = self.get_open_orders()
        for o in open_orders:
            raw_sym = o.get("product_symbol") or ""
            if not raw_sym:
                prod = next((p for p in self.products_cache.values() if int(p.get("id", 0)) == int(o.get("product_id", -1))), None)
                if prod:
                    raw_sym = prod.get("symbol", "")
            if not raw_sym:
                continue
            osym = raw_sym.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
            if osym not in active_sym_set:
                s_type = str(o.get("stop_order_type") or "").lower()
                is_reduce = bool(o.get("reduce_only"))
                # Protect resting entry orders; only cancel bracket child orders (SL/TP) or reduce_only orders
                if s_type or is_reduce:
                    logger.warning(f"[{osym}] Detected orphaned resting bracket #{o.get('id')} ({s_type}) with position closed. Cancelling!")
                    try:
                        self.cancel_order(o.get("id"), o.get("product_id"))
                    except Exception as ex:
                        logger.debug(f"Error cancelling orphaned order: {ex}")

        if not active_positions:
            return reports

        # Live mark prices
        mark_prices = {}
        try:
            t_res = self.request("GET", "/v2/tickers")
            ticker_list = t_res.get("result", []) if isinstance(t_res, dict) else []
            for tk in ticker_list:
                sym = tk.get("symbol")
                if sym and "mark_price" in tk:
                    mark_prices[sym] = float(tk.get("mark_price") or 0.0)
        except Exception as e:
            logger.warning(f"Could not load live tickers for trailing SL: {e}")

        # Refresh open orders after orphan cancellation
        open_orders = self.get_open_orders()

        for pos in active_positions:
            p_sym = pos.get("product_symbol", "")
            size = int(pos.get("size", 0))
            if size == 0 or not p_sym:
                continue

            prod = self.resolve_product(p_sym)
            if not prod:
                continue
            prod_id = prod["id"]
            contract_val = float(prod.get("contract_value", 1.0) or 1.0)
            tick_size = float(prod.get("tick_size", 0.01) or 0.01)

            entry_price = float(pos.get("entry_price", 0.0) or 0.0)
            mark_price = mark_prices.get(p_sym) or float(pos.get("mark_price", 0.0) or 0.0)
            if entry_price <= 0 or mark_price <= 0:
                continue

            direction = "BUY" if size > 0 else "SELL"
            dir_mult = 1.0 if direction == "BUY" else -1.0

            # Match SL and TP orders with deterministic duplicate order purge
            prod_sl_orders = []
            prod_tp_orders = []
            for o in open_orders:
                if int(o.get("product_id", 0)) == prod_id:
                    s_type = str(o.get("stop_order_type", "")).lower()
                    if "stop_loss" in s_type:
                        prod_sl_orders.append(o)
                    elif "take_profit" in s_type:
                        prod_tp_orders.append(o)

            # 1. Deterministic Duplicate Stop Loss Purge
            sl_order = None
            if len(prod_sl_orders) > 1:
                # BUY: highest stop_price is tightest ratchet; SELL: lowest stop_price is tightest ratchet
                prod_sl_orders.sort(
                    key=lambda o: float(o.get("stop_price") or 0.0),
                    reverse=(direction == "BUY")
                )
                sl_order = prod_sl_orders[0]
                for dup in prod_sl_orders[1:]:
                    dup_id = dup.get("id")
                    logger.warning(f"[{p_sym}] Purging duplicate SL order #{dup_id} @ {dup.get('stop_price')}")
                    try:
                        self.cancel_order(dup_id, prod_id)
                        reports.append({"symbol": p_sym, "action": "PURGED_DUPLICATE_SL", "order_id": dup_id, "stop_price": dup.get("stop_price")})
                    except Exception as ce:
                        logger.error(f"[{p_sym}] Failed to cancel duplicate SL #{dup_id}: {ce}")
            elif prod_sl_orders:
                sl_order = prod_sl_orders[0]

            # 2. Deterministic Duplicate Take Profit Purge
            tp_order = None
            if len(prod_tp_orders) > 1:
                # Directional sort preserving widest profitable TP order across all ratchet tiers:
                # BUY: highest stop_price is widest; SELL: lowest stop_price is widest
                prod_tp_orders.sort(
                    key=lambda o: float(o.get("stop_price") or 0.0),
                    reverse=(direction == "BUY")
                )
                tp_order = prod_tp_orders[0]
                for dup in prod_tp_orders[1:]:
                    dup_id = dup.get("id")
                    logger.warning(f"[{p_sym}] Purging duplicate TP order #{dup_id} @ {dup.get('stop_price')}")
                    try:
                        self.cancel_order(dup_id, prod_id)
                        reports.append({"symbol": p_sym, "action": "PURGED_DUPLICATE_TP", "order_id": dup_id, "stop_price": dup.get("stop_price")})
                    except Exception as ce:
                        logger.error(f"[{p_sym}] Failed to cancel duplicate TP #{dup_id}: {ce}")
            elif prod_tp_orders:
                tp_order = prod_tp_orders[0]

            # --- NAKED POSITION RECOVERY ---
            if not sl_order:
                cur_tp_price = float(tp_order.get("stop_price", 0.0)) if tp_order else None
                anchor = get_or_recover_anchor(
                    symbol=p_sym,
                    product_id=prod_id,
                    direction=direction,
                    entry_price=entry_price,
                    cur_sl_price=0.0,
                    cur_tp_price=cur_tp_price,
                    size=abs(size),
                    contract_val=contract_val,
                    max_risk_usd=MAX_RISK_USD
                )
                r0 = float(anchor.get("initial_risk_dist", 0.0))
                rec_sl = float(anchor.get("last_ratchet_price") or anchor.get("initial_sl"))

                breached = (mark_price <= rec_sl) if direction == "BUY" else (mark_price >= rec_sl)
                if breached:
                    logger.error(f"🚨 [{p_sym}] NAKED POSITION BREACHED STOP LEVEL (${rec_sl:.5f} vs Mark ${mark_price:.5f}). Emergency closing!")
                    self.close_position(p_sym)
                    reports.append({"symbol": p_sym, "action": "EMERGENCY_CLOSE_NAKED_BREACH", "mark_price": mark_price, "target_sl": rec_sl})
                    continue

                sl_body = {
                    "product_id": prod_id,
                    "size": abs(size),
                    "side": "sell" if direction == "BUY" else "buy",
                    "order_type": "market_order",
                    "stop_order_type": "stop_loss_order",
                    "stop_price": self.quantize_price(rec_sl, prod_id),
                    "stop_trigger_method": "mark_price",
                    "reduce_only": True
                }
                res = self.request("POST", "/v2/orders", body=sl_body)
                order_res = res.get("result", {})
                if res.get("success") or ("id" in order_res):
                    logger.info(f"🛡️ [{p_sym}] Attached recovery SL order #{order_res.get('id')} @ {rec_sl}")
                    reports.append({"symbol": p_sym, "action": "RECOVERED_NAKED_SL", "sl_price": rec_sl, "order_id": order_res.get("id")})
                    sl_order = order_res
                else:
                    logger.error(f"[{p_sym}] Failed to attach recovery SL order: {res}")
                    continue

            cur_sl_price = float(sl_order.get("stop_price", 0.0) or 0.0)
            cur_tp_price = float(tp_order.get("stop_price", 0.0) or 0.0) if tp_order else None

            anchor = get_or_recover_anchor(
                symbol=p_sym,
                product_id=prod_id,
                direction=direction,
                entry_price=entry_price,
                cur_sl_price=cur_sl_price,
                cur_tp_price=cur_tp_price,
                size=abs(size),
                contract_val=contract_val,
                max_risk_usd=MAX_RISK_USD
            )
            r0 = float(anchor.get("initial_risk_dist", 0.0))
            cur_tier = int(anchor.get("current_tier", 0))
            if r0 <= 0:
                continue

            unrealized_dist = dir_mult * (mark_price - entry_price)
            r_gain = unrealized_dist / r0

            # TP Calibration check: strict R:R >= 2.0 invariant with directional tick snapping
            if tp_order and cur_tp_price:
                reward_dist = abs(cur_tp_price - entry_price)
                rr = reward_dist / r0
                if rr < 1.999:
                    target_ticks = math.ceil((2.0 * r0) / tick_size)
                    target_tp = entry_price + (dir_mult * target_ticks * tick_size)
                    self.edit_order(
                        order_id=tp_order["id"],
                        product_id=prod_id,
                        size=abs(size),
                        stop_price=target_tp
                    )
                    reports.append({
                        "symbol": p_sym,
                        "action": "CALIBRATED_TP",
                        "old_tp": cur_tp_price,
                        "new_tp": target_tp,
                        "rr": 2.0
                    })

            # Monotonic Multi-Tier Trailing SL State Machine
            target_sl = None
            target_tier = cur_tier
            tier_tag = None

            if r_gain >= 3.0 and cur_tier < 3:
                target_sl = entry_price + (dir_mult * 2.0 * r0)
                target_tier = 3
                tier_tag = "TIER_3_PLUS_2R_LOCK"
                if tp_order and cur_tp_price:
                    widen_tp = entry_price + (dir_mult * 4.0 * r0)
                    if (direction == "BUY" and widen_tp > cur_tp_price) or (direction == "SELL" and widen_tp < cur_tp_price):
                        self.edit_order(
                            order_id=tp_order["id"],
                            product_id=prod_id,
                            size=abs(size),
                            stop_price=widen_tp
                        )
            elif r_gain >= 2.0 and cur_tier < 2:
                target_sl = entry_price + (dir_mult * 1.0 * r0)
                target_tier = 2
                tier_tag = "TIER_2_PLUS_1R_LOCK"
                if tp_order and cur_tp_price:
                    widen_tp = entry_price + (dir_mult * 3.0 * r0)
                    if (direction == "BUY" and widen_tp > cur_tp_price) or (direction == "SELL" and widen_tp < cur_tp_price):
                        self.edit_order(
                            order_id=tp_order["id"],
                            product_id=prod_id,
                            size=abs(size),
                            stop_price=widen_tp
                        )
            elif r_gain >= 1.0 and cur_tier < 1:
                target_sl = entry_price + (dir_mult * 1.0 * tick_size)
                target_tier = 1
                tier_tag = "TIER_1_BREAKEVEN"

            if target_sl is not None and target_tier > cur_tier:
                is_better = (target_sl > cur_sl_price + tick_size * 0.5) if direction == "BUY" else (target_sl < cur_sl_price - tick_size * 0.5)
                is_valid_trigger = (target_sl < mark_price - tick_size) if direction == "BUY" else (target_sl > mark_price + tick_size)

                if is_better and is_valid_trigger:
                    try:
                        edit_res = self.edit_order(
                            order_id=sl_order["id"],
                            product_id=prod_id,
                            size=abs(size),
                            stop_price=target_sl
                        )
                        is_ok = edit_res.get("success") or ("result" in edit_res and "id" in edit_res.get("result", {}))
                        if not is_ok and "immediate_execution_stop_order" in str(edit_res):
                            logger.warning(f"[{p_sym}] Market price {mark_price} already penetrated ratchet stop {target_sl}. Executing emergency market close to lock in profits!")
                            close_res = self.close_position(p_sym)
                            reports.append({
                                "symbol": p_sym,
                                "action": "EMERGENCY_MARKET_CLOSE_ON_STOP_PENETRATION",
                                "stop_price": target_sl,
                                "mark_price": mark_price,
                                "close_result": close_res
                            })
                            continue

                        if is_ok:
                            update_anchor_tier(p_sym, target_tier, target_sl)
                            logger.info(f"🛡️ [{p_sym}] Ratcheted Stop Loss to {target_sl} ({tier_tag}, gain: +{r_gain:.2f}R0, mark: {mark_price})")
                            reports.append({
                                "symbol": p_sym,
                                "action": "RATCHET_SL",
                                "tier": tier_tag,
                                "old_sl": cur_sl_price,
                                "new_sl": target_sl,
                                "gain_r": round(r_gain, 2),
                                "mark_price": mark_price
                            })
                        else:
                            logger.warning(f"[{p_sym}] Failed to ratchet SL: {edit_res}")
                    except Exception as exc:
                        logger.warning(f"[{p_sym}] Error during SL ratchet: {exc}")

        return reports

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

        with FileMutex(str(DB_DIR / ".locks" / "delta_position_management.lock"), timeout=10.0) as mgmt_lock:
            if not mgmt_lock.acquired:
                logger.warning(f"[{sym}] Live dispatch lock contention on position management mutex. Yielding.")
                return {"ok": False, "status": "REJECTED", "reason": "POSITION_MANAGEMENT_MUTEX_CONTENTION"}
            return self._execute_delta_signal_guarded(signal, prod, sym, direction)

    def _execute_delta_signal_guarded(self, signal: Dict[str, Any], prod: Dict[str, Any], sym: str, direction: str) -> Dict[str, Any]:
        # Check in-flight orders to prevent concurrent duplicate dispatch
        if prod["symbol"] in self._inflight_orders:
            return {"ok": False, "status": "REJECTED", "reason": f"ORDER_ALREADY_IN_FLIGHT: {prod['symbol']}"}

        # Check existing active position on this symbol
        if self.has_open_position(sym):
            return {"ok": False, "status": "REJECTED", "reason": f"POSITION_ALREADY_OPEN_ON_DELTA: {sym}"}

        # Check max concurrent positions limit vs free cash availability
        active_pos = self.get_active_positions()
        free_cash_mode = os.environ.get("DELTA_FREE_CASH_MODE", "true").lower() in ("true", "1", "yes")
        avail_margin = self.get_available_margin_usd()

        # Reject mock signals in live dispatch
        if signal.get("is_mock") or os.environ.get("MOCK_TEST_MODE") == "1":
            logger.warning(f"[{sym}] Live dispatch rejected: mock signals prohibited from live exchange execution.")
            return {"ok": False, "status": "REJECTED", "reason": "MOCK_SIGNAL_LIVE_DISPATCH_PROHIBITED"}

        # Capital & Margin Preservation Invariant: available free margin buffer >= 20% universal floor (Fail-Closed)
        wb = self.get_wallet_balances()
        collateral_item = next(
            (b for b in wb if b.get("asset_symbol") in ("USD", "USDT", "INR")),
            {}
        ) if isinstance(wb, list) else {}
        total_balance = float(collateral_item.get("balance", 0.0) or 0.0)
        collateral_asset = collateral_item.get("asset_symbol", "USD")
        if total_balance <= 0 or not isinstance(wb, list):
            logger.warning(f"Live dispatch blocked: unable to verify account balance from Delta API (balance=${total_balance:.4f} {collateral_asset}).")
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": "UNABLE_TO_VERIFY_MARGIN_BUFFER: account balance unavailable or zero"
            }
        margin_buffer_pct = (avail_margin / total_balance) * 100.0
        if margin_buffer_pct < 20.0:
            logger.warning(f"Live dispatch blocked: free margin buffer {margin_buffer_pct:.1f}% is below 20.0% minimum guardrail ({avail_margin:.4f} of {total_balance:.4f} {collateral_asset}).")
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"INSUFFICIENT_MARGIN_BUFFER: {margin_buffer_pct:.1f}% < 20.0% floor (available={avail_margin:.4f} of {total_balance:.4f} {collateral_asset})"
            }

        # Strict Single Source of Truth for Max Open Positions (AGENTS.md Rule 3: Max 2 positions including in-flight)
        effective_positions = len(active_pos) + len(self._inflight_orders)
        if effective_positions >= MAX_DELTA_CONCURRENT_POSITIONS:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"DELTA_MAX_POSITIONS_REACHED: {effective_positions}/{MAX_DELTA_CONCURRENT_POSITIONS}"
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
            direction=dir_norm,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            spread_bps=0.0
        )
        if not geom_ok:
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}

        side = "buy" if direction in ("BUY", "BULLISH", "LONG") else "sell"

        # Determine leverage (use instrument default up to 20x or higher)
        leverage = int(prod.get("default_leverage", 20.0))
        if leverage < 1:
            leverage = 20

        # Rigorous Discrete Contract Sizing via MAX_RISK_USD ($25.00) and available margin
        from Terminal.risk_envelope import MAX_RISK_USD
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        risk_dist = abs(entry - sl)
        single_contract_risk = risk_dist * contract_val
        if single_contract_risk > MAX_RISK_USD:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"SINGLE_CONTRACT_RISK_EXCEEDS_CAP: ${single_contract_risk:.2f} > ${MAX_RISK_USD:.2f}"
            }

        if risk_dist > 0 and contract_val > 0:
            calc_contracts = int(MAX_RISK_USD / single_contract_risk)
            size = max(1, min(calc_contracts, 100))
        else:
            size = 1

        # Margin reservation: Ensure trade initial margin does not exceed available free cash
        if avail_margin > 0.02 and contract_val > 0:
            req_margin = (entry * contract_val * size) / float(leverage)
            if req_margin > avail_margin:
                sized_down = int((avail_margin * 0.80 * leverage) / (entry * contract_val))
                if sized_down < 1:
                    return {
                        "ok": False,
                        "status": "REJECTED",
                        "reason": f"INSUFFICIENT_MARGIN_FOR_MIN_LOT: required=${req_margin:.4f} > available=${avail_margin:.4f}"
                    }
                size = max(1, min(size, sized_down))
                logger.info(f"Scaled size to {size} contracts to fit available margin (${avail_margin:.2f} USD)")

        self._inflight_orders.add(prod["symbol"])
        try:
            logger.info(f"Executing Delta Live Order: {prod['symbol']} {side} {size} contracts | Entry: {entry} | SL: {sl} | TP: {tp} | Leverage: {leverage}x | AvailMargin: ${avail_margin:.2f}")
            res = self.place_order(
                symbol=prod["symbol"],
                side=side,
                size=size,
                order_type="market_order",
                stop_loss_price=sl,
                take_profit_price=tp,
                leverage=leverage
            )

            ok = res.get("success", False) or ("result" in res and "id" in res.get("result", {}))
            if ok:
                order_info = res.get("result", {})
                fill_price_raw = order_info.get("average_fill_price")
                fill_price = float(fill_price_raw) if fill_price_raw and float(fill_price_raw) > 0 else entry
                try:
                    self.reanchor_brackets_to_fill(
                        symbol=prod["symbol"],
                        fill_price=fill_price,
                        sl_price=sl,
                        min_rr=2.0
                    )
                except Exception as e:
                    logger.warning(f"[{prod['symbol']}] Post-fill bracket re-anchoring error: {e}")

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
