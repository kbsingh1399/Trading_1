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
from typing import Dict, Any, Optional, List, Set, Tuple, Union
import requests
from requests.adapters import HTTPAdapter

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
DELTA_MGMT_LOCK_FILE = DB_DIR / ".locks" / "delta_position_management.lock"
LIVE_DISPATCH_ENABLED = os.environ.get("DELTA_LIVE_DISPATCH", "false").lower() in ("true", "1", "yes")
PAPER_TRADING_ENABLED = os.environ.get("DELTA_PAPER_TRADING", "true").lower() in ("true", "1", "yes")


def set_live_dispatch(enabled: bool):
    """Dynamically enables or disables live Delta Exchange dispatch."""
    global LIVE_DISPATCH_ENABLED
    LIVE_DISPATCH_ENABLED = enabled
    logger.info(f"Delta Live Dispatch state set to: {LIVE_DISPATCH_ENABLED}")


def set_paper_trading(enabled: bool):
    """Dynamically enables or disables shadow paper trading."""
    global PAPER_TRADING_ENABLED
    PAPER_TRADING_ENABLED = enabled
    logger.info(f"Delta Paper Trading state set to: {PAPER_TRADING_ENABLED}")


# Taker rate used when product metadata lacks an explicit fee schedule.
TAKER_FEE_DEFAULT = 0.00075


def solve_fee_aware_grid(
    entry: float,
    stop_loss: float,
    contract_value: float,
    taker_fee_rate: float = TAKER_FEE_DEFAULT,
    cap_usd: float = 25.0,
    max_contracts: int = 100
) -> Dict[str, Any]:
    """
    PATCH D7 (SSOT helper): Discrete Contract Grid solver — k_max form.

    Solves the SAME integer grid as Terminal.risk_envelope.admissible_volume, but
    returns the LARGEST admissible size under the risk cap (admissible_volume is
    intentionally k_min / survival-minimal by Arena spec — wiring k_min into live
    dispatch would size every trade at 1 contract). Loss per contract INCLUDES
    round-trip taker commissions so the $25 cap covers stop-out + fees:

        loss_per_contract = |entry - sl| * contract_value
                           + 2 * taker_fee_rate * entry * contract_value

    Returns dict with: ok, size, realized_risk_usd, loss_per_contract,
    commission_per_contract, reason (on rejection).
    """
    from decimal import Decimal, ROUND_FLOOR

    risk_dist = abs(float(entry) - float(stop_loss))
    contract_value = float(contract_value)
    taker_fee_rate = float(taker_fee_rate)
    cap_usd = float(cap_usd)

    if risk_dist <= 0 or contract_value <= 0 or cap_usd <= 0:
        return {
            "ok": False,
            "size": 0,
            "realized_risk_usd": 0.0,
            "loss_per_contract": 0.0,
            "reason": f"INVALID_RISK_INPUTS: dist={risk_dist}, cval={contract_value}, cap={cap_usd}"
        }

    # Directional round-trip taker commission per contract (entry + SL exit)
    entry_notional = float(entry) * contract_value
    exit_notional = float(stop_loss) * contract_value
    commission_per_contract = taker_fee_rate * (entry_notional + exit_notional)
    loss_per_contract = (risk_dist * contract_value) + commission_per_contract

    if loss_per_contract <= 0:
        return {"ok": False, "size": 0, "realized_risk_usd": 0.0, "reason": "NON_POSITIVE_LOSS_PER_CONTRACT"}

    if loss_per_contract > cap_usd:
        return {
            "ok": False,
            "size": 0,
            "realized_risk_usd": 0.0,
            "loss_per_contract": loss_per_contract,
            "commission_per_contract": commission_per_contract,
            "reason": (
                f"SINGLE_CONTRACT_ALL_IN_RISK_EXCEEDS_CAP: "
                f"${loss_per_contract:.4f} (stop ${risk_dist * contract_value:.4f} + fee ${commission_per_contract:.4f}) > ${cap_usd:.2f}"
            )
        }

    # k_max form: largest integer k where k * loss_per_contract <= cap_usd
    d_cap = Decimal(str(cap_usd))
    d_loss = Decimal(str(loss_per_contract))
    k_max = int((d_cap / d_loss).to_integral_value(rounding=ROUND_FLOOR))
    size = max(1, min(k_max, int(max_contracts)))
    realized_risk = size * loss_per_contract

    return {
        "ok": True,
        "size": size,
        "realized_risk_usd": round(realized_risk, 4),
        "loss_per_contract": round(loss_per_contract, 4),
        "commission_per_contract": round(commission_per_contract, 4),
        "pure_risk_usd": round(size * risk_dist * contract_value, 4),
        "fees_reserved_usd": round(size * commission_per_contract, 4),
        "reason": "OK"
    }


def load_credentials() -> Dict[str, str]:
    """Loads API key, secret, and demo token from environment or ~/.delta-exchange-mcp/config.env."""
    key = os.environ.get("DELTA_API_KEY")
    secret = os.environ.get("DELTA_API_SECRET")
    token = os.environ.get("DELTA_DEMO_TOKEN")
    env = os.environ.get("DELTA_MCP_ENV", "demo")

    if CONFIG_ENV_PATH.exists():
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
                        elif k == "DELTA_DEMO_TOKEN" and not token:
                            token = v
                        elif k == "DELTA_MCP_ENV":
                            env = v
        except Exception as e:
            logger.warning(f"Error reading {CONFIG_ENV_PATH}: {e}")

    return {"api_key": key or "", "api_secret": secret or "", "token": token or "", "env": env}


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
        self.token = creds.get("token", "")
        self.env = creds.get("env", "demo")
        self.base_url = "https://cdn-ind.testnet.deltaex.org" if self.env in ("demo", "testnet") else BASE_URL
        self.server_time_offset = 0.0
        self.products_cache: Dict[str, Dict[str, Any]] = dict(DEFAULT_PRODUCTS)
        self._inflight_orders: Set[str] = set()
        # Persistent HTTP connection pooling for sub-50ms RTT
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=1)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        if (self.api_key and self.api_secret) or self.token:
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
            resp = self.session.get(f"{self.base_url}/v2/positions/margined", headers=headers, timeout=10)
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
            resp = self.session.get(f"{self.base_url}/v2/products", timeout=10)
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

    def resolve_product(self, symbol: Union[str, int]) -> Optional[Dict[str, Any]]:
        """Resolves raw symbol (e.g. 'LTC', 'LTCUSD') or product_id to Delta perpetual product."""
        if isinstance(symbol, int) or (isinstance(symbol, str) and symbol.isdigit()):
            target_id = int(symbol)
            for meta in self.products_cache.values():
                if int(meta.get("id", -1)) == target_id:
                    return meta
        clean = str(symbol).upper().replace(".PI", "").replace("USDT", "").replace("USD", "")
        # Exact match first
        cand1 = f"{clean}USD"
        if cand1 in self.products_cache:
            return self.products_cache[cand1]
        for p_sym, meta in self.products_cache.items():
            if meta.get("underlying") == clean or p_sym.startswith(clean):
                return meta
        return None

    def request(self, method: str, path: str, params: Optional[Dict] = None, body: Optional[Dict] = None, _retry_depth: int = 0) -> Dict[str, Any]:
        """Signs and executes authenticated HTTP request against Delta Exchange India."""
        ts = self.get_timestamp()
        query_str = ""
        if params:
            query_str = "?" + "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        body_str = json.dumps(body) if body is not None else ""

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "AntigravityQuant/1.0"
        }
        if getattr(self, "token", None):
            headers["Authorization"] = self.token
        elif self.api_key and self.api_secret:
            sig_payload = method.upper() + ts + path + query_str + body_str
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                sig_payload.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            headers["api-key"] = self.api_key
            headers["timestamp"] = ts
            headers["signature"] = signature

        base = getattr(self, "base_url", BASE_URL)
        url = base + path + query_str
        method_upper = method.upper()
        resp = self.session.request(
            method=method_upper,
            url=url,
            headers=headers,
            data=body_str if body_str else None,
            timeout=10
        )

        # PATCH D1: bounded to ONE resync+retry. A persistently-drifting clock or a
        # revoked key previously recursed infinitely — an unbounded request storm
        # while holding the position-management mutex (guardian starvation).
        if resp.status_code == 401 and "expired_signature" in resp.text and _retry_depth < 1:
            logger.warning("Clock drift detected. Resyncing with Delta server (single bounded retry)...")
            self._sync_server_time()
            return self.request(method, path, params, body, _retry_depth=_retry_depth + 1)
        if resp.status_code == 401 and "expired_signature" in resp.text:
            logger.error("Signature still expired after one resync+retry. Failing closed — NOT retrying again.")

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

    @staticmethod
    def convert_to_usd(amount: float, asset_symbol: str) -> float:
        """
        Converts collateral currency to USD using explicit, conservative FX conversion.
        USD and USDT are mapped 1:1. INR is converted at conservative 87.0 INR/USD rate.
        """
        sym = (asset_symbol or "").upper().strip()
        if sym in ("USD", "USDT"):
            return float(amount)
        if sym == "INR":
            return float(amount) / 87.0
        return 0.0

    def get_wallet_balances_normalized_usd(self) -> Dict[str, Any]:
        """
        Returns FX-normalized wallet balances across active collateral assets (USD, USDT, INR).
        Prevents nominal summation of mismatched currencies (e.g. INR + USD).
        """
        wb = self.get_wallet_balances()
        if not isinstance(wb, list):
            return {"total_balance_usd": 0.0, "total_available_usd": 0.0, "wallets": []}

        wallets_summary = []
        total_bal_usd = 0.0
        total_avail_usd = 0.0

        for b in wb:
            sym = b.get("asset_symbol", "")
            if sym in ("USD", "USDT", "INR"):
                raw_bal = float(b.get("balance", 0.0) or 0.0)
                raw_avail = float(b.get("available_balance", 0.0) or 0.0)
                bal_usd = self.convert_to_usd(raw_bal, sym)
                avail_usd = self.convert_to_usd(raw_avail, sym)
                total_bal_usd += bal_usd
                total_avail_usd += avail_usd
                wallets_summary.append({
                    "asset_symbol": sym,
                    "raw_balance": raw_bal,
                    "raw_available": raw_avail,
                    "balance_usd": bal_usd,
                    "available_usd": avail_usd,
                    "buffer_pct": (raw_avail / raw_bal * 100.0) if raw_bal > 0 else 0.0
                })

        return {
            "total_balance_usd": total_bal_usd,
            "total_available_usd": total_avail_usd,
            "wallets": wallets_summary
        }

    def get_available_margin_usd(self) -> float:
        """Returns total available collateral margin in FX-normalized USD."""
        try:
            summary = self.get_wallet_balances_normalized_usd()
            return summary.get("total_available_usd", 0.0)
        except Exception as e:
            logger.warning(f"Error fetching normalized wallet available margin: {e}")
            return 0.0

    def get_open_positions(self) -> Optional[List[Dict[str, Any]]]:
        """
        PATCH D2: returns None when the Delta API response cannot be verified.
        None means UNKNOWN STATE and must be treated fail-closed by every caller —
        it is NOT an empty book. Returning [] here previously caused the guardian
        to prune all anchors and cancel live SL/TP brackets on a single API blip.
        """
        res = self.request("GET", "/v2/positions/margined")
        if res.get("result") is not None:
            return res.get("result", [])
        return None

    def get_active_positions(self) -> Optional[List[Dict[str, Any]]]:
        """Returns non-zero open positions, or None when the book is unverifiable (fail-closed)."""
        raw = self.get_open_positions()
        if raw is None:
            return None
        return [p for p in raw if int(p.get("size", 0)) != 0]

    def has_open_position(self, symbol: str) -> bool:
        """
        Checks if a position is currently open for the given symbol.
        PATCH D2: UNKNOWN book state fails CLOSED — assume in-position so an
        unverifiable book can never authorize a fresh entry.
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        active = self.get_active_positions()
        if active is None:
            logger.warning(f"[{symbol}] Position book unavailable — treating symbol as IN-POSITION (fail-closed, entry blocked).")
            return True
        for p in active:
            p_sym = p.get("product_symbol", "")
            p_clean = p_sym.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
            if clean == p_clean or p_sym.upper() == symbol.upper():
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

    def get_open_orders(self) -> Optional[List[Dict[str, Any]]]:
        """
        PATCH D2: returns None when the order book cannot be verified (fail-closed),
        so orphan-cancellation can never run against a partially-fetched book.
        """
        res = self.request("GET", "/v2/orders", params={"states": "open,pending"})
        if res.get("result") is not None:
            return res.get("result", [])
        return None

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
        leverage: int = 10,
        time_in_force: Optional[str] = None
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
        # PATCH D8: set leverage once per (product, leverage) per session.
        cached_lev = getattr(self, "_leverage_cache", {}).get(prod_id)
        if cached_lev != int(leverage):
            self.set_leverage(prod_id, int(leverage))
            if not hasattr(self, "_leverage_cache"):
                self._leverage_cache = {}
            self._leverage_cache[prod_id] = int(leverage)

        body: Dict[str, Any] = {
            "product_id": prod_id,
            "size": int(size),
            "side": side_clean,
            "order_type": order_type
        }

        if order_type == "limit_order" and limit_price is not None:
            body["limit_price"] = self.quantize_price(limit_price, prod_id)

        if time_in_force:
            body["time_in_force"] = time_in_force

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

    def confirm_market_fill(
        self,
        product_id: int,
        order_id: Optional[int],
        order_info: Dict[str, Any],
        fallback_mark_price: float,
        max_wait_s: float = 3.0
    ) -> Tuple[float, str]:
        """
        PATCH D5: resolves the ACTUAL executed fill price for a market order.
        1) immediate response average_fill_price (if already populated)
        2) poll GET /v2/fills for our order_id (up to max_wait_s)
        3) NEVER the planned signal price — fall back to the pre-trade live mark.
        Returns (fill_price, source_tag).
        """
        # 1) immediate response
        try:
            fp = float(order_info.get("average_fill_price") or 0.0)
            if fp > 0:
                return fp, "ORDER_RESPONSE"
        except (ValueError, TypeError):
            pass

        # 2) poll the fills ledger
        deadline = time.time() + max_wait_s
        while time.time() < deadline:
            try:
                fills = self.get_fills(limit=20)
                for f in fills:
                    try:
                        if int(f.get("product_id", -1)) != int(product_id):
                            continue
                        if order_id is not None and f.get("order_id") not in (None, order_id, int(order_id), str(order_id)):
                            continue
                        px = float(f.get("price") or 0.0)
                        if px > 0:
                            return px, "FILLS_LEDGER"
                    except (ValueError, TypeError):
                        continue
            except Exception as exc:
                logger.debug(f"Fill confirmation poll error: {exc}")
            time.sleep(0.3)

        # 3) If order was explicitly cancelled and no fill was recorded, do NOT fall back to mark
        if str(order_info.get("state", "")).lower() == "cancelled":
            return 0.0, "ORDER_CANCELLED_NO_FILL"

        # 4) conservative fresh fallback (planned entry is FORBIDDEN here)
        if fallback_mark_price and fallback_mark_price > 0:
            logger.warning(
                f"[product {product_id}] Fill not yet visible; falling back to pre-trade MARK "
                f"{fallback_mark_price} (planned entry is never used for risk anchoring)."
            )
            return float(fallback_mark_price), "MARK_FALLBACK"
        return 0.0, "UNCONFIRMED"

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

        # PATCH D3: wait for Delta backend to persist child bracket orders.
        # Uses ~10s exponential backoff + fails closed if orders cannot be read.
        tp_order = None
        sl_order = None
        backoff_delays = [0.15, 0.35, 0.75, 1.5, 2.5, 4.0]
        for attempt, delay in enumerate(backoff_delays, start=1):
            orders = self.get_open_orders()
            if orders is None:
                # API read error: retry rather than immediately declaring bracket lost
                time.sleep(delay)
                continue
            for o in orders:
                if o.get("product_id") == prod_id:
                    s_type = str(o.get("stop_order_type", "")).lower()
                    if "take_profit" in s_type:
                        tp_order = o
                    elif "stop_loss" in s_type:
                        sl_order = o
            if tp_order and sl_order:
                break
            time.sleep(delay)

        def _cancel_brackets():
            if sl_order and sl_order.get("id"):
                try:
                    self.cancel_order(sl_order["id"], prod_id)
                except Exception as ce:
                    logger.debug(f"[{symbol}] Failed to cancel SL bracket #{sl_order.get('id')}: {ce}")
            if tp_order and tp_order.get("id"):
                try:
                    self.cancel_order(tp_order["id"], prod_id)
                except Exception as ce:
                    logger.debug(f"[{symbol}] Failed to cancel TP bracket #{tp_order.get('id')}: {ce}")

        if not tp_order or not sl_order:
            _cancel_brackets()
            missing = []
            if not tp_order:
                missing.append("TP")
            if not sl_order:
                missing.append("SL")
            return {"ok": False, "status": "NOT_FOUND", "reason": f"BRACKET_ORDERS_NOT_LOCATED: missing {','.join(missing)}"}

        # Determine position direction from TP side (TP side is opposing)
        pos_direction = "BUY" if tp_order.get("side", "").lower() == "sell" else "SELL"
        dir_mult = 1.0 if pos_direction == "BUY" else -1.0
        size = int(tp_order.get("size", 1))

        # Check for catastrophic adverse slippage beyond stop loss
        directional_stop_dist = (fill_price - sl_price) * dir_mult
        if directional_stop_dist <= 0:
            logger.error(
                f"[{symbol}] INVERTED STOP LOSS DETECTED: fill {fill_price} breached planned SL {sl_price} "
                f"for {pos_direction} position (dist={directional_stop_dist}). Closing immediately fail-closed."
            )
            close_res = self.close_position(symbol)
            _cancel_brackets()
            return {
                "ok": False,
                "status": "INVERTED_STOP_LOSS",
                "reason": f"Fill price {fill_price} slipped past SL {sl_price} in direction {pos_direction}",
                "position_closed": True,
                "close_res": close_res
            }

        # Actual risk distance from fill
        actual_risk = abs(fill_price - sl_price)
        from Terminal.risk_envelope import MAX_RISK_USD
        max_allowed_risk_usd = MAX_RISK_USD

        # Check if slippage inflated dollar risk beyond MAX_RISK_USD
        realized_risk_usd = actual_risk * size * contract_val
        calibrated_sl = sl_price
        tick_size = float(prod.get("tick_size", 0.01) or 0.01)
        if realized_risk_usd > max_allowed_risk_usd:
            # DO NOT tighten stop loss distance closer to entry (never compress SL into market noise).
            # Instead, shed excess contracts via a reduce-only market order to fit the $25 cap.
            loss_per_contract = actual_risk * contract_val
            allowed_size = math.floor(round(max_allowed_risk_usd / max(1e-9, loss_per_contract), 8))
            shed_contracts = size - allowed_size

            if allowed_size >= 1 and shed_contracts > 0:
                shed_side = "sell" if pos_direction == "BUY" else "buy"
                shed_body = {
                    "product_id": prod_id,
                    "size": int(shed_contracts),
                    "side": shed_side,
                    "order_type": "market_order",
                    "reduce_only": True
                }
                logger.warning(
                    f"[{symbol}] SLIPPAGE RISK BREACH: fill {fill_price} vs planned SL {sl_price} inflated "
                    f"risk to ${realized_risk_usd:.2f} > ${max_allowed_risk_usd:.2f} cap. Preserving structural SL distance "
                    f"({actual_risk:.6f}) and shedding {shed_contracts} excess contracts (size {size} -> {allowed_size})."
                )
                shed_res = self.request("POST", "/v2/orders", body=shed_body)
                shed_ok = bool(shed_res.get("success", False) or shed_res.get("result") or shed_res.get("id"))
                if not shed_ok:
                    logger.error(f"[{symbol}] Failed to shed {shed_contracts} excess contracts: {shed_res}. Closing position immediately (fail-closed).")
                    close_res = self.close_position(symbol)
                    _cancel_brackets()
                    return {
                        "ok": False,
                        "status": "SHED_EXCESS_CONTRACTS_FAILED",
                        "reason": f"Failed to shed excess contracts on slippage: {shed_res.get('error', 'UNKNOWN')}",
                        "shed_res": shed_res,
                        "position_closed": True,
                        "close_res": close_res
                    }
                size = int(allowed_size)
                realized_risk_usd = actual_risk * size * contract_val
                if sl_order:
                    sl_edit_res = self.edit_order(
                        order_id=sl_order["id"],
                        product_id=prod_id,
                        size=size,
                        stop_price=calibrated_sl
                    )
                    sl_success = bool(sl_edit_res.get("success", False) or sl_edit_res.get("result") or sl_edit_res.get("id"))
                    if not sl_success:
                        logger.error(f"[{symbol}] Failed to update SL order size to {size}: {sl_edit_res}")
                        _cancel_brackets()
                        return {
                            "ok": False,
                            "status": "SL_EDIT_FAILED",
                            "reason": f"Failed to edit SL order size: {sl_edit_res.get('error', 'UNKNOWN')}",
                            "edit_res": sl_edit_res
                        }
            elif allowed_size < 1:
                logger.error(
                    f"[{symbol}] SLIPPAGE EXCEEDS MIN LOT RISK: 1 contract risk ${loss_per_contract:.2f} > ${max_allowed_risk_usd:.2f} cap. "
                    f"Cannot shed to fit cap. Triggering close_position fail-closed."
                )
                close_res = self.close_position(symbol)
                _cancel_brackets()
                return {
                    "ok": False,
                    "status": "SLIPPAGE_EXCEEDS_SINGLE_CONTRACT_CAP",
                    "reason": f"1 contract risk ${loss_per_contract:.2f} exceeds ${max_allowed_risk_usd:.2f} cap; position closed fail-closed.",
                    "position_closed": True,
                    "close_res": close_res
                }

        # PATCH D4: guarantee at least min_rr:1 with a WIDEN-ONLY ratchet.
        # The evaluator frequently extends TP1 to a liquidation magnet (2.4-3.0R+).
        # The previous code snapped any TP that differed from the exact-2.0R grid
        # back to 2.0R — amputating the magnet-extended reward. TP may only move
        # OUTWARD from the fill, never inward.
        target_ticks = math.ceil((min_rr * actual_risk) / tick_size)
        calibrated_tp = fill_price + (dir_mult * target_ticks * tick_size)

        # Check current TP
        current_tp = float(tp_order.get("stop_price", 0.0) or 0.0)
        current_reward = abs(current_tp - fill_price)
        current_rr = current_reward / max(actual_risk, 1e-8)

        if current_tp > 0:
            if pos_direction == "BUY":
                effective_tp = max(calibrated_tp, current_tp)   # keep the wider target
            else:
                effective_tp = min(calibrated_tp, current_tp)
        else:
            effective_tp = calibrated_tp
        effective_rr = abs(effective_tp - fill_price) / max(actual_risk, 1e-8)

        register_position_anchor(
            symbol=symbol,
            product_id=prod_id,
            direction=pos_direction,
            entry_price=fill_price,
            initial_sl=calibrated_sl,
            initial_tp=effective_tp,
            size=size,
            initial_risk_dist=actual_risk,
            contract_val=contract_val
        )

        needs_edit = (current_rr < min_rr) or (abs(effective_tp - current_tp) > tick_size * 0.5) or (int(tp_order.get("size", 1)) != size)
        if needs_edit:
            edit_res = self.edit_order(
                order_id=tp_order["id"],
                product_id=prod_id,
                size=size,
                stop_price=effective_tp
            )
            is_success = bool(edit_res.get("success", False) or edit_res.get("result") or edit_res.get("id"))
            if not is_success:
                logger.error(f"[{symbol}] Failed to re-anchor TP to {effective_tp}: {edit_res}")
                _cancel_brackets()
                return {
                    "ok": False,
                    "status": "TP_EDIT_FAILED",
                    "reason": f"Failed to edit TP order: {edit_res.get('error', 'UNKNOWN')}",
                    "edit_res": edit_res
                }
            logger.info(f"[{symbol}] Re-anchored TP from {current_tp} to {effective_tp} (R:R {current_rr:.2f}:1 -> {effective_rr:.2f}:1 relative to fill {fill_price}, widen-only)")
            return {
                "ok": True,
                "status": "REANCHORED",
                "calibrated_tp": effective_tp,
                "calibrated_sl": calibrated_sl,
                "size": size,
                "rr": effective_rr,
                "edit_res": edit_res
            }
        return {
            "ok": True,
            "status": "ALREADY_COMPLIANT",
            "current_rr": current_rr,
            "calibrated_sl": calibrated_sl,
            "calibrated_tp": effective_tp,
            "size": size
        }

    reanchor_bracket_upon_fill = reanchor_brackets_to_fill

    def audit_and_trail_positions(self) -> List[Dict[str, Any]]:
        """
        Audits all active Delta Exchange positions, enforces R:R >= 2.0, ratchets
        trailing stop-loss monotonically, recovers naked positions, purges duplicate orders,
        and cancels orphaned ghost orders with cross-process mutual exclusion.
        """
        reports = []
        if PAPER_TRADING_ENABLED:
            try:
                from Terminal.paper_trading_engine import paper_engine
                paper_reports = paper_engine.audit_and_trail_positions(self)
                reports.extend(paper_reports)
            except Exception as e:
                logger.warning(f"Error auditing paper positions: {e}")

        with FileMutex(str(DELTA_MGMT_LOCK_FILE), timeout=5.0) as mgmt_lock:
            if not mgmt_lock.acquired:
                logger.info("Another process is actively auditing Delta positions. Yielding.")
                return reports
            real_reports = self._audit_and_trail_positions_guarded()
            reports.extend(real_reports)
            return reports

    def _audit_and_trail_positions_guarded(self) -> List[Dict[str, Any]]:
        reports = []
        # PATCH D2 (fail-closed ordering): fetch and VERIFY both books BEFORE any
        # destructive action. A failed positions/orders call previously looked like
        # "flat account": prune wiped every anchor and orphan-cancel nuked live
        # SL/TP brackets, leaving real positions naked on a transient API blip.
        raw_positions = self.get_active_positions()
        if raw_positions is None:
            logger.warning("Delta positions API UNAVAILABLE — skipping audit cycle entirely (fail-closed: no prune, no orphan cancel, no trailing).")
            return reports

        open_orders = self.get_open_orders()
        if open_orders is None:
            logger.warning("Delta orders API UNAVAILABLE — skipping audit cycle entirely (fail-closed: no prune, no orphan cancel, no trailing).")
            return reports

        active_positions = [p for p in raw_positions if int(p.get("size", 0)) != 0]
        active_symbols = [p.get("product_symbol", "") for p in active_positions]

        # Invariant 1: prune anchors ONLY on a verified book
        prune_closed_positions(active_symbols)

        # Invariant 2: orphaned ghost orders cleanup on a verified book
        active_sym_set = {
            s.upper().replace(".PI", "").replace("USDT", "").replace("USD", "") + "USD"
            for s in active_symbols if s
        }
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

        # Refresh open orders after orphan cancellation (fail-closed: verified book only)
        open_orders = self.get_open_orders()
        if open_orders is None:
            logger.warning("Delta orders API UNAVAILABLE post-cleanup — skipping trailing audit this cycle (fail-closed).")
            return reports

        for pos in active_positions:
            p_sym = pos.get("product_symbol", "")
            size = int(pos.get("size", 0))
            if size == 0 or not p_sym:
                continue

            prod = self.resolve_product(p_sym)
            prod_id = int(pos.get("product_id") or (prod["id"] if prod else 0))
            if not prod_id:
                continue
            contract_val = float(pos.get("product", {}).get("contract_value") or (prod.get("contract_value") if prod else 1.0) or 1.0)
            tick_size = float(pos.get("product", {}).get("tick_size") or (prod.get("tick_size") if prod else 0.01) or 0.01)

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

            # Immediate Stop Penetration Emergency Close Invariant (Arena Finding 5)
            # If mark price has already penetrated the existing SL, immediately emergency close
            is_stop_penetrated = (
                (direction == "BUY" and cur_sl_price > 0 and mark_price <= cur_sl_price) or
                (direction == "SELL" and cur_sl_price > 0 and mark_price >= cur_sl_price)
            )
            if is_stop_penetrated:
                logger.error(
                    f"🚨 [{p_sym}] Immediate Stop Penetration detected! "
                    f"Mark price {mark_price} has breached existing SL {cur_sl_price} ({direction}). "
                    f"Executing immediate emergency market close to truncate loss."
                )
                close_res = self.close_position(p_sym)
                reports.append({
                    "symbol": p_sym,
                    "action": "EMERGENCY_CLOSE_STOP_PENETRATED",
                    "mark_price": mark_price,
                    "sl_price": cur_sl_price,
                    "direction": direction,
                    "close_result": close_res
                })
                continue

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

            # Pre-emptive ratchet triggers at +2.8R / +1.8R to widen TP before Delta's resting limit fills
            if r_gain >= 2.8 and cur_tier < 3:
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
            elif r_gain >= 1.8 and cur_tier < 2:
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
            elif r_gain >= 1.5 and cur_tier == 1:
                # Intermediate Tier 1.5: Lock +0.5R0 profit once price reaches +1.5R0 (prevents giveback to BE)
                half_r_sl = entry_price + (dir_mult * 0.5 * r0)
                is_advancing = (half_r_sl > cur_sl_price + tick_size * 0.5) if direction == "BUY" else (half_r_sl < cur_sl_price - tick_size * 0.5)
                if is_advancing:
                    target_sl = half_r_sl
                    target_tier = 1
                    tier_tag = "TIER_1_5_PLUS_HALF_R_LOCK"
            elif r_gain >= 1.0 and cur_tier < 1:
                target_sl = entry_price + (dir_mult * 1.0 * tick_size)
                target_tier = 1
                tier_tag = "TIER_1_BREAKEVEN"

            if target_sl is not None and (target_tier > cur_tier or (target_tier == cur_tier and tier_tag == "TIER_1_5_PLUS_HALF_R_LOCK")):
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
        PATCH D2: unverifiable position book fails closed with an explicit error
        (never silently reports NOT_FOUND on an API failure).
        """
        clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
        active = self.get_active_positions()
        if active is None:
            logger.error(f"[EMERGENCY CLOSE] [{symbol}] Position book UNAVAILABLE — cannot verify/close position. Retry recommended.")
            return {"ok": False, "status": "POSITION_BOOK_UNAVAILABLE", "reason": f"API failure while closing {symbol} — RETRY"}
        for p in active:
            p_sym = p.get("product_symbol", "")
            p_clean = p_sym.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
            if clean == p_clean or p_sym.upper() == symbol.upper():
                prod = self.resolve_product(p_sym)
                prod_id = int(p.get("product_id") or (prod["id"] if prod else 0))
                if not prod_id:
                    continue
                size = abs(int(p.get("size", 0)))
                if size <= 0:
                    continue
                # Opposing side to close
                cur_size = int(p.get("size", 0))
                close_side = "sell" if cur_size > 0 else "buy"
                body = {
                    "product_id": prod_id,
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
            if PAPER_TRADING_ENABLED:
                logger.info("Delta Paper Trading active: routing order to Shadow Paper Trading Engine (Option 1)...")
                try:
                    from Terminal.paper_trading_engine import paper_engine
                    return paper_engine.execute_paper_order(signal, self)
                except Exception as e:
                    logger.error(f"Paper trading execution failed: {e}")
                    return {"ok": False, "status": "PAPER_EXECUTION_ERROR", "reason": str(e)}
            logger.warning("Delta live dispatch blocked: LIVE_DISPATCH_ENABLED is False (fail-closed operator safety gate).")
            return {"ok": False, "status": "BLOCKED_BY_SAFETY_GATE", "reason": "LIVE_DISPATCH_ENABLED_IS_FALSE"}

        sym = signal.get("symbol", "")
        direction = signal.get("direction", "").upper()
        if direction not in ("BUY", "SELL"):
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_DIRECTION: {direction}"}

        prod = self.resolve_product(sym)
        if not prod:
            return {"ok": False, "status": "REJECTED", "reason": f"SYMBOL_NOT_ON_DELTA_INDIA: {sym}"}

        with FileMutex(str(DELTA_MGMT_LOCK_FILE), timeout=10.0) as mgmt_lock:
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
        if active_pos is None:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": "POSITION_BOOK_UNAVAILABLE_FAIL_CLOSED: cannot verify existing positions before dispatch"
            }
        free_cash_mode = os.environ.get("DELTA_FREE_CASH_MODE", "true").lower() in ("true", "1", "yes")
        avail_margin = self.get_available_margin_usd()

        # Reject mock signals in live dispatch
        if signal.get("is_mock") or os.environ.get("MOCK_TEST_MODE") == "1":
            logger.warning(f"[{sym}] Live dispatch rejected: mock signals prohibited from live exchange execution.")
            return {"ok": False, "status": "REJECTED", "reason": "MOCK_SIGNAL_LIVE_DISPATCH_PROHIBITED"}

        # Capital & Margin Preservation Invariant: available free margin buffer >= 20% universal floor (Fail-Closed)
        wb = self.get_wallet_balances()
        if not isinstance(wb, list) or len(wb) == 0:
            logger.warning("Live dispatch blocked: unable to verify account balance from Delta API (empty wallet balances).")
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": "UNABLE_TO_VERIFY_MARGIN_BUFFER: account balance unavailable or zero"
            }

        # Multi-collateral universal floor: Every active collateral currency (USD, USDT, INR) MUST maintain >= 20% buffer
        collateral_found = False
        total_balance_agg = 0.0
        total_avail_agg = 0.0
        for b in wb:
            asset = b.get("asset_symbol")
            if asset in ("USD", "USDT", "INR"):
                bal = float(b.get("balance", 0.0) or 0.0)
                avail = float(b.get("available_balance", 0.0) or 0.0)
                if bal > 0:
                    collateral_found = True
                    bal_usd = self.convert_to_usd(bal, asset)
                    avail_usd = self.convert_to_usd(avail, asset)
                    total_balance_agg += bal_usd
                    total_avail_agg += avail_usd
                    buf_pct = (avail / bal) * 100.0
                    if buf_pct < 20.0:
                        logger.warning(
                            f"Live dispatch blocked: {asset} free margin buffer {buf_pct:.1f}% is below 20.0% minimum guardrail "
                            f"({avail:.4f} of {bal:.4f} {asset})."
                        )
                        return {
                            "ok": False,
                            "status": "REJECTED",
                            "reason": f"INSUFFICIENT_MARGIN_BUFFER: {asset} free margin buffer {buf_pct:.1f}% < 20.0% floor ({avail:.4f} of {bal:.4f} {asset})"
                        }

        if not collateral_found or total_balance_agg <= 0:
            logger.warning("Live dispatch blocked: zero collateral balance found in active wallets.")
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": "UNABLE_TO_VERIFY_MARGIN_BUFFER: zero collateral balance"
            }

        agg_buf_pct = (total_avail_agg / total_balance_agg) * 100.0
        if agg_buf_pct < 20.0:
            logger.warning(f"Live dispatch blocked: aggregate collateral free margin {agg_buf_pct:.1f}% < 20.0% floor.")
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"INSUFFICIENT_MARGIN_BUFFER: aggregate collateral margin {agg_buf_pct:.1f}% < 20.0% floor"
            }

        # Strict Single Source of Truth for Max Open Positions (AGENTS.md Rule 3: Max 2 positions including in-flight)
        # Exclude sub-dollar dust positions (< $1.00 notional) from blocking institutional risk slots
        institutional_pos = [
            p for p in active_pos
            if (abs(int(p.get("size", 0))) * float(p.get("mark_price", 0.0) or p.get("entry_price", 0.0) or 0.0)) >= 1.0
        ]
        effective_positions = len(institutional_pos) + len(self._inflight_orders)
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

        # PATCH D6: live microstructure gate before dispatch.
        # (a) fetch the product's live ticker; (b) enforce the 45bps spread gate
        # with the REAL spread (was hardcoded 0.0 = decorative); (c) reject when
        # the market has drifted >= 35% of the stop distance (or 1% of price,
        # whichever is smaller) away from the planned signal entry.
        live_mark_price = 0.0
        spread_bps_live = 0.0
        try:
            tk_res = self.request("GET", f"/v2/tickers/{prod['id']}")
            tk = tk_res.get("result", {}) if isinstance(tk_res, dict) else {}
            if isinstance(tk, list):
                tk = tk[0] if tk else {}
            live_mark_price = float(tk.get("mark_price") or 0.0)
            best_bid = float(tk.get("best_bid") or tk.get("bid") or 0.0)
            best_ask = float(tk.get("best_ask") or tk.get("ask") or 0.0)
            if best_bid > 0 and best_ask >= best_bid:
                spread_bps_live = ((best_ask - best_bid) / ((best_ask + best_bid) / 2.0)) * 10000.0
        except Exception as exc:
            logger.warning(f"[{sym}] Live ticker fetch error: {exc}")

        if live_mark_price <= 0:
            return {"ok": False, "status": "REJECTED", "reason": "LIVE_TICKER_UNAVAILABLE_FAIL_CLOSED"}

        geom_ok, geom_reason = validate_trade_geometry(
            direction=dir_norm,
            entry=entry,
            stop_loss=sl,
            take_profit=tp,
            spread_bps=spread_bps_live
        )
        if not geom_ok:
            return {"ok": False, "status": "REJECTED", "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}

        # Stale-entry drift veto: market already ran away from the signal price.
        r0_dist = abs(entry - sl)
        max_drift = min(0.35 * r0_dist, 0.01 * entry)
        if abs(live_mark_price - entry) > max_drift:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": (
                    f"STALE_ENTRY_PRICE_DRIFT: |mark {live_mark_price:.8f} - entry {entry:.8f}| "
                    f"= {abs(live_mark_price - entry):.8f} > tolerance {max_drift:.8f} "
                    f"(0.35 x R0 {r0_dist:.8f}); signal price no longer executable at planned geometry"
                )
            }

        side = "buy" if direction in ("BUY", "BULLISH", "LONG") else "sell"

        # Determine leverage (use instrument default up to 20x or higher)
        leverage = int(prod.get("default_leverage", 20.0))
        if leverage < 1:
            leverage = 20

                # PATCH P2 (Phase 2): DYNAMIC SURVIVAL RISK BUDGETING - replaces the static
        # MAX_RISK_USD cap. Fail-closed: an inadmissible budget blocks dispatch BEFORE sizing.
        # Walks live anchors, reserves committed risk under 1.25x stress, enforces
        # the survival divisor (20), and clamps to available margin headroom above
        # the universal 20% floor.
        from Terminal.risk_envelope import (
            MAX_RISK_USD,
            DEFAULT_MARGIN_PER_RISK_USD,
            MARGIN_BUFFER_FLOOR_PCT,
            budget_per_trade_dynamic,
        )
        anchors_snapshot = load_position_anchors()
        margin_per_risk = (entry / (r0_dist * float(leverage))) if (r0_dist > 0 and leverage > 0) else None
        dyn_budget, budget_ok, budget_reason = budget_per_trade_dynamic(
            portfolio_equity=total_balance_agg,
            open_anchors=anchors_snapshot,
            balance=total_balance_agg,
            free_margin_usd=total_avail_agg,
            margin_per_risk_usd=margin_per_risk if margin_per_risk else DEFAULT_MARGIN_PER_RISK_USD,
        )
        if not budget_ok:
            return {
                "ok": False,
                "status": "REJECTED",
                "reason": f"DYNAMIC_RISK_BUDGET_BLOCKED: {budget_reason}",
            }

        effective_cap_usd = min(MAX_RISK_USD, dyn_budget)
        contract_val = float(prod.get("contract_value", 1.0) or 1.0)
        taker_fee_rate = float(prod.get("taker_fee_rate", TAKER_FEE_DEFAULT) or TAKER_FEE_DEFAULT)
        grid = solve_fee_aware_grid(
            entry=entry,
            stop_loss=sl,
            contract_value=contract_val,
            taker_fee_rate=taker_fee_rate,
            cap_usd=effective_cap_usd,
            max_contracts=100
        )
        if not grid["ok"]:
            return {"ok": False, "status": "REJECTED", "reason": grid["reason"]}
        size = int(grid["size"])
        realized_risk_at_size = float(grid["realized_risk_usd"])
        logger.info(
            f"[{sym}] Discrete Contract Grid Sizing: {size} contracts @ entry {entry} (SL {sl}) -> "
            f"Realized risk ${realized_risk_at_size:.2f} <= ${effective_cap_usd:.2f} (budget ${dyn_budget:.2f}) "
            f"(Loss/contract: ${grid['loss_per_contract']:.4f} including fee ${grid['commission_per_contract']:.4f})"
        )

        # PATCH P2: harmonized with the 20.0% universal free-margin floor - the
        # sizedown target is now the headroom ABOVE the floor (avail - 20% of
        # balance), not a flat 80% of available margin.
        if avail_margin > 0.02 and contract_val > 0:
            req_margin = (entry * contract_val * size) / float(leverage)
            margin_headroom = max(0.0, avail_margin - MARGIN_BUFFER_FLOOR_PCT * total_balance_agg)
            if req_margin > margin_headroom:
                sized_down = int((margin_headroom * float(leverage)) / (entry * contract_val))
                if sized_down < 1:
                    return {
                        "ok": False,
                        "status": "REJECTED",
                        "reason": (
                            f"INSUFFICIENT_MARGIN_ABOVE_FLOOR: required=${req_margin:.4f} > "
                            f"headroom=${margin_headroom:.4f} (avail=${avail_margin:.4f} less "
                            f"{MARGIN_BUFFER_FLOOR_PCT*100:.1f}% floor of ${total_balance_agg:.2f})"
                        )
                    }
                size = max(1, min(size, sized_down))
                req_margin = (size * entry * contract_val) / float(leverage)
                logger.info(f"Scaled size to {size} contracts to fit margin headroom above 20% floor (${margin_headroom:.2f} USD)")

        self._inflight_orders.add(prod["symbol"])
        try:
            tick_size = float(prod.get("tick_size", 0.01) or 0.01)
            if live_mark_price > 0:
                if dir_norm == "BUY":
                    ref_px = best_ask if best_ask > 0 else live_mark_price
                    limit_px = ref_px + (5.0 * tick_size)
                else:
                    ref_px = best_bid if best_bid > 0 else live_mark_price
                    limit_px = ref_px - (5.0 * tick_size)
                order_type = "limit_order"
                time_in_force = "ioc"
            else:
                limit_px = None
                order_type = "market_order"
                time_in_force = None

            logger.info(
                f"Executing Delta Live Order: {prod['symbol']} {side} {size} contracts | "
                f"Entry: {entry} | SL: {sl} | TP: {tp} | OrderType: {order_type} "
                f"(LimitPx: {limit_px}, TIF: {time_in_force}) | Leverage: {leverage}x | AvailMargin: ${avail_margin:.2f}"
            )
            res = self.place_order(
                symbol=prod["symbol"],
                side=side,
                size=size,
                order_type=order_type,
                limit_price=limit_px,
                stop_loss_price=sl,
                take_profit_price=tp,
                leverage=leverage,
                time_in_force=time_in_force
            )

            ok = res.get("success", False) or ("result" in res and "id" in res.get("result", {}))
            if ok:
                order_info = res.get("result", {})
                # PATCH D5: confirm the ACTUAL fill (fills ledger -> mark fallback).
                # Never anchor risk math to the planned signal price.
                order_id = order_info.get("id")
                fill_price, fill_source = self.confirm_market_fill(
                    product_id=prod["id"],
                    order_id=order_id,
                    order_info=order_info,
                    fallback_mark_price=live_mark_price
                )
                if fill_price <= 0:
                    if fill_source == "ORDER_CANCELLED_NO_FILL" or str(order_info.get("state", "")).lower() == "cancelled":
                        logger.info(f"[{prod['symbol']}] IOC limit order cancelled without execution (5-tick slippage buffer protected against adverse fill).")
                        return {
                            "ok": False,
                            "status": "IOC_UNFILLED",
                            "reason": "IOC_ORDER_CANCELLED_UNFILLED",
                            "venue": "DeltaExchangeIndia",
                            "symbol": prod["symbol"]
                        }
                    logger.error(f"[{prod['symbol']}] Fill price unconfirmable; closing position immediately (fail-closed).")
                    close_res = self.close_position(prod["symbol"])
                    return {
                        "ok": False,
                        "status": "REJECTED_UNCONFIRMED_FILL",
                        "reason": "FILL_PRICE_UNCONFIRMED",
                        "venue": "DeltaExchangeIndia",
                        "symbol": prod["symbol"],
                        "flatten_confirmed": bool(close_res.get("success", False) or close_res.get("result") or close_res.get("id"))
                    }
                logger.info(f"[{prod['symbol']}] Fill confirmed @ {fill_price} (source: {fill_source}, planned entry was {entry})")
                try:
                    reanchor_res = self.reanchor_brackets_to_fill(
                        symbol=prod["symbol"],
                        fill_price=fill_price,
                        sl_price=sl,
                        min_rr=2.0
                    )
                    if not reanchor_res.get("ok", False):
                        logger.error(f"[{prod['symbol']}] Post-fill bracket re-anchoring failed: {reanchor_res}. Closing position immediately to prevent uncalibrated risk exposure.")
                        if reanchor_res.get("position_closed"):
                            close_res = reanchor_res.get("close_res", {})
                            is_flattened = True
                        else:
                            close_res = self.close_position(prod["symbol"])
                            is_flattened = bool(close_res.get("success", False) or close_res.get("result") or close_res.get("id"))
                            try:
                                open_orders = self.get_open_orders() or []
                                for o in open_orders:
                                    if o.get("product_id") == prod["id"]:
                                        self.cancel_order(o.get("id"), prod["id"])
                            except Exception as ce:
                                logger.debug(f"[{prod['symbol']}] Residual order cancellation error: {ce}")
                        if not is_flattened:
                            logger.critical(f"🚨 CRITICAL INCIDENT: EMERGENCY FLATTENING FAILED FOR {prod['symbol']}: {close_res}")
                        return {
                            "ok": False,
                            "status": "REJECTED_FAILED_BRACKET_ANCHOR",
                            "reason": f"BRACKET_ANCHOR_FAILURE: {reanchor_res.get('reason', 'UNKNOWN')}",
                            "venue": "DeltaExchangeIndia",
                            "symbol": prod["symbol"],
                            "details": reanchor_res,
                            "flatten_confirmed": is_flattened
                        }
                except Exception as e:
                    logger.error(f"[{prod['symbol']}] Post-fill bracket re-anchoring error: {e}. Closing position immediately.")
                    close_res = self.close_position(prod["symbol"])
                    is_flattened = bool(close_res.get("success", False) or close_res.get("result") or close_res.get("id"))
                    if not is_flattened:
                        logger.critical(f"🚨 CRITICAL INCIDENT: EMERGENCY FLATTENING FAILED FOR {prod['symbol']}: {close_res}")
                    return {
                        "ok": False,
                        "status": "REJECTED_FAILED_BRACKET_ANCHOR",
                        "reason": f"BRACKET_ANCHOR_EXCEPTION: {str(e)}",
                        "venue": "DeltaExchangeIndia",
                        "symbol": prod["symbol"],
                        "flatten_confirmed": is_flattened
                    }

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
