"""
========================================================================================
MetaTrader 5 Live Execution Bridge - DECOMMISSIONED AND EXCISED
========================================================================================
Per repository architecture invariants (AGENTS.md Section 3), MetaTrader 5 live order
execution is strictly decommissioned and excised from live trading dispatch.

Live execution exclusively targets the Delta Exchange India authenticated REST API
(Terminal/delta_exchange_live_trader.py).

This non-executable stub maintains contract safety invariants and rejects all dispatch calls.
========================================================================================
"""

from typing import Dict, Any, Optional
from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    MAX_OPEN_POSITIONS,
    MAX_RISK_USD,
    MAX_DAILY_LOSS_USD
)

LIVE_DISPATCH_ENABLED: bool = False
EQUITY_FLOOR_TOTAL: float = HARD_FLOOR_USD
MAX_CONCURRENT_POSITIONS: int = MAX_OPEN_POSITIONS
DEFAULT_RISK_PER_TRADE_USD: float = MAX_RISK_USD
MAX_DAILY_DRAWDOWN_LIMIT: float = MAX_DAILY_LOSS_USD


def execute_mt5_signal(signal: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """Strictly rejects order dispatch: MT5 live execution is permanently decommissioned."""
    return {
        "ok": False,
        "status": "REJECTED_DECOMMISSIONED_BRIDGE",
        "reason": "MT5_LIVE_EXECUTION_STRICTLY_DECOMMISSIONED_AND_EXCISED",
        "venue": "MetaTrader5"
    }


def execute_market_order(*args, **kwargs) -> Dict[str, Any]:
    """Strictly rejects market order dispatch."""
    return {
        "ok": False,
        "status": "REJECTED_DECOMMISSIONED_BRIDGE",
        "reason": "MT5_LIVE_EXECUTION_STRICTLY_DECOMMISSIONED_AND_EXCISED",
        "venue": "MetaTrader5"
    }
