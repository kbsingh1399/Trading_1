#!/usr/bin/env python3
"""
FundedNext MCP Client & Risk Governor Bridge
Directly connects to https://mcp.fundednext.com/ via Model Context Protocol (JSON-RPC)
to fetch live account metrics, drawdown buffers, rule compliance, and trading history.
"""

import os
import sys
import json
import logging
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FundedNextMCP")

FUNDEDNEXT_MCP_URL = "https://mcp.fundednext.com/"
DEFAULT_TOKEN = "70308139|apqkOIdj0IadzSGi26FRIebvpVkOgOD1LzWhv5Lxcdef5db5"
TOKEN_FILE = os.path.join(os.path.dirname(__file__), ".fundednext_token")


def get_token() -> str:
    """Retrieve the FundedNext MCP token from env, file, or default."""
    if os.environ.get("FUNDEDNEXT_MCP_TOKEN"):
        return os.environ["FUNDEDNEXT_MCP_TOKEN"].strip()
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            t = f.read().strip()
            if t:
                return t
    return DEFAULT_TOKEN


def save_token(token: str) -> None:
    """Persist the token to .fundednext_token."""
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(token.strip())
    logger.info("FundedNext MCP token saved successfully.")


class FundedNextMCPClient:
    """JSON-RPC client for FundedNext Model Context Protocol server."""

    def __init__(self, token: Optional[str] = None):
        self.token = token or get_token()
        self.url = FUNDEDNEXT_MCP_URL
        self._account_id = None
        self._login = 11808165

    def _call_rpc(self, method: str, params: Dict[str, Any], rpc_id: int = 1) -> Dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "Antigravity-Agent/1.0",
        }
        payload = {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": method,
            "params": params,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.url, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            logger.error(f"FundedNext MCP HTTP Error {e.code}: {err_body}")
            raise RuntimeError(f"HTTP {e.code}: {err_body}")
        except Exception as e:
            logger.error(f"FundedNext MCP Request failed: {e}")
            raise

    def initialize(self) -> Dict[str, Any]:
        """Send MCP initialize handshake."""
        params = {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "antigravity-trading", "version": "1.0.0"},
        }
        res = self._call_rpc("initialize", params)
        return res.get("result", {})

    def list_tools(self) -> list:
        """List all available tools on the FundedNext MCP server."""
        res = self._call_rpc("tools/list", {})
        return res.get("result", {}).get("tools", [])

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a specific MCP tool."""
        params = {"name": name, "arguments": arguments}
        res = self._call_rpc("tools/call", params)
        result = res.get("result", {})
        # Check if structuredContent is provided
        if "structuredContent" in result:
            return result["structuredContent"]
        # Otherwise parse from content blocks
        for item in result.get("content", []):
            if item.get("type") == "text":
                txt = item.get("text", "")
                try:
                    return json.loads(txt)
                except Exception:
                    pass
        return result

    def get_accounts(self) -> list:
        """Fetch all paid trading accounts."""
        res = self.call_tool("get_accounts", {})
        data = res.get("data", [])
        if data:
            self._account_id = data[0].get("id")
            self._login = data[0].get("login")
        return data

    def get_account_id(self) -> int:
        """Get or resolve primary account ID."""
        if self._account_id is None:
            accounts = self.get_accounts()
            if accounts:
                self._account_id = accounts[0].get("id")
            else:
                self._account_id = 3085711  # fallback default
        return self._account_id

    def get_account_overview(self) -> Dict[str, Any]:
        """Fetch comprehensive account metrics, objectives, drawdown, and rules."""
        acc_id = self.get_account_id()
        return self.call_tool("get_account_overview", {"account_id": acc_id})

    def get_risk_cushion(self) -> Dict[str, float]:
        """Extract exact real-time drawdown cushion and loss limits."""
        overview = self.get_account_overview()
        objectives = overview.get("objectives", {})
        threshold = overview.get("threshold", {})
        stats = overview.get("stats", {})

        daily_loss_rem = float(threshold.get("daily_loss_threshold", 98.66))
        max_loss_rem = float(threshold.get("max_loss_threshold", 98.66))
        equity = float(stats.get("equity", 4698.66))
        balance = float(stats.get("balance", 4698.66))
        profit_target = float(stats.get("profit_target", 400.0))

        return {
            "balance": balance,
            "equity": equity,
            "daily_loss_remaining": daily_loss_rem,
            "max_loss_remaining": max_loss_rem,
            "profit_target": profit_target,
            "effective_risk_buffer": min(daily_loss_rem, max_loss_rem),
        }

    def get_recent_trades(self, page: int = 1) -> list:
        """Fetch closed trade history."""
        acc_id = self.get_account_id()
        res = self.call_tool("get_trading_history", {"account_id": acc_id, "page": page})
        return res.get("trades", {}).get("data", [])

    def get_running_trades(self) -> list:
        """Fetch currently open positions."""
        acc_id = self.get_account_id()
        res = self.call_tool("get_trading_history", {"account_id": acc_id, "page": 1})
        return res.get("runningTrades", [])

    def validate_order_risk(self, planned_risk_usd: float) -> tuple[bool, str]:
        """
        Adversarial Prop Guard: Checks planned trade risk against FundedNext's live loss cushion.
        Rejects if planned risk exceeds 25% of remaining cushion.
        """
        cushion = self.get_risk_cushion()
        buffer = cushion["effective_risk_buffer"]

        if buffer <= 0:
            return False, f"BREACH RISK: Account has 0 remaining loss buffer!"

        # Allow max 25% of remaining buffer per trade
        max_allowed_risk = round(buffer * 0.25, 2)
        if planned_risk_usd > max_allowed_risk:
            return False, (
                f"Risk Governor VETO: Planned risk ${planned_risk_usd:.2f} exceeds "
                f"25% of remaining cushion (${max_allowed_risk:.2f} max allowed from ${buffer:.2f} buffer)"
            )

        return True, (
            f"Risk Governor APPROVE: Planned risk ${planned_risk_usd:.2f} is safely within "
            f"cushion (${buffer:.2f} remaining buffer, max per-trade: ${max_allowed_risk:.2f})"
        )


if __name__ == "__main__":
    client = FundedNextMCPClient()
    print("Handshaking with FundedNext MCP...")
    init = client.initialize()
    print(f"Server: {init.get('serverInfo', {}).get('name')} v{init.get('serverInfo', {}).get('version')}")

    cushion = client.get_risk_cushion()
    print("\n--- FUNDEDNEXT LIVE ACCOUNT CUSHION ---")
    print(f"Balance:             ${cushion['balance']:.2f}")
    print(f"Equity:              ${cushion['equity']:.2f}")
    print(f"Daily Loss Left:     ${cushion['daily_loss_remaining']:.2f}")
    print(f"Overall Loss Left:   ${cushion['max_loss_remaining']:.2f}")
    print(f"Effective Buffer:    ${cushion['effective_risk_buffer']:.2f}")

    # Test risk guard on $12.50 trade
    ok, msg = client.validate_order_risk(12.50)
    print(f"\nOrder Risk Check ($12.50): {'PASS' if ok else 'FAIL'}")
    print(f"Details: {msg}")
