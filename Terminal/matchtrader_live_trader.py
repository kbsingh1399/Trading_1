"""
========================================================================================
MatchTrader Execution Bridge - DECOMMISSIONED AND EXCISED
========================================================================================
Per repository architecture invariants (AGENTS.md Section 3), MatchTrader, browser DOM
automation, and legacy prop-firm execution bridges are strictly decommissioned, excised,
and prohibited from execution paths.

Live execution exclusively targets the Delta Exchange India authenticated REST API
(Terminal/delta_exchange_live_trader.py).

This non-executable stub maintains contract safety invariants and rejects all dispatch calls.
========================================================================================
"""

import asyncio
from typing import Dict, Any, Optional
from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    MAX_OPEN_POSITIONS,
    MAX_RISK_USD,
    MAX_DAILY_LOSS_USD
)

INITIAL_BALANCE: float = 5000.0
LIVE_DISPATCH_ENABLED: bool = False
EQUITY_FLOOR_TOTAL: float = HARD_FLOOR_USD
MAX_CONCURRENT_POSITIONS: int = MAX_OPEN_POSITIONS
DEFAULT_RISK_PER_TRADE_USD: float = MAX_RISK_USD
MAX_DAILY_DRAWDOWN_LIMIT: float = MAX_DAILY_LOSS_USD


async def execute_market_order(*args, **kwargs) -> Dict[str, Any]:
    """Strictly rejects order dispatch: MatchTrader execution is permanently decommissioned."""
    return {
        "ok": False,
        "status": "REJECTED_DECOMMISSIONED_BRIDGE",
        "reason": "MATCHTRADER_STRICTLY_DECOMMISSIONED_AND_EXCISED",
        "venue": "MatchTrader"
    }


async def execute_signal(signal: Dict[str, Any] = None, *args, **kwargs) -> Dict[str, Any]:
    """Strictly rejects signal dispatch: MatchTrader execution is permanently decommissioned."""
    return {
        "ok": False,
        "status": "REJECTED_DECOMMISSIONED_BRIDGE",
        "reason": "MATCHTRADER_STRICTLY_DECOMMISSIONED_AND_EXCISED",
        "venue": "MatchTrader"
    }


def calculate_lot_size(symbol: str = "BTCUSD", risk_usd: float = DEFAULT_RISK_PER_TRADE_USD, **kwargs) -> float:
    """Returns 0.0 lot size for decommissioned bridge."""
    return 0.0


def matchtrader_symbol(symbol: str) -> str:
    """Utility formatter for symbol normalization without proprietary binding."""
    clean = symbol.upper().replace(".PI", "").replace("USDT", "").replace("USD", "").strip()
    return f"{clean}USD"
