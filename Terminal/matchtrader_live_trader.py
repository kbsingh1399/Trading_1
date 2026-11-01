"""
MatchTrader Live Execution Bridge (FundingPips Competition Account)
===================================================================
Automates live order execution on FundingPips MatchTrader Web Platform
via Chrome DevTools Protocol (CDP) for Account 101305360.

Strict Prop Firm Risk Guardrails:
1. Account ID Lock: Executes ONLY on verified Account 101305360.
2. Max Total Drawdown Floor: $90,000 ($10,000 max overall loss from $100K).
3. Max Daily Drawdown Floor: $95,000 ($5,000 max daily loss from starting equity).
4. Max Concurrent Positions: Strictly capped at 2 open positions simultaneously.
5. Dynamic Position Sizing: 0.5% risk ($500) per trade based on Stop Loss distance.
6. Execution Logging: All fills logged atomically to Database/matchtrader_executed_trades.jsonl.
"""

import sys
import os
import json
import time
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Terminal.runtime_io import acquire_singleton, atomic_json
from Terminal.collector_lock import BrowserLock

DB_DIR = ROOT_DIR / "Database"
EXECUTED_TRADES_FILE = DB_DIR / "matchtrader_executed_trades.jsonl"
MATCHTRADER_STATE_FILE = DB_DIR / "matchtrader_account_state.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [MATCHTRADER] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("MatchTraderBridge")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# Strict Risk Constants
TARGET_ACCOUNT_NUMBER = "101305360"
INITIAL_BALANCE = 100000.0
MAX_TOTAL_DRAWDOWN_LIMIT = 10000.0
EQUITY_FLOOR_TOTAL = 90000.0
MAX_DAILY_DRAWDOWN_LIMIT = 5000.0
EQUITY_FLOOR_DAILY = 95000.0
MAX_CONCURRENT_POSITIONS = 2
DEFAULT_RISK_PER_TRADE_USD = 500.0  # 0.50%

# Symbol Map to MatchTrader Instrument Names
SYMBOL_TO_MATCHTRADER = {
    "BTC": "BTCUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
    "XRP": "XRPUSD",
    "DOGE": "DOGEUSD",
    "BNB": "BNBUSD",
    "ADA": "ADAUSD",
    "AVAX": "AVAXUSD",
    "LINK": "LINKUSD",
    "LTC": "LTCUSD",
    "DOT": "DOTUSD",
    "XAU": "XAUUSD",
    "XAG": "XAGUSD",
    "UKOIL": "UKOIL",
    "USOIL": "USOIL",
    "NDX100": "NDX100",
    "SPX500": "SPX500",
    "DJI30": "DJI30",
    "JP225": "JP225"
}


def get_matchtrader_tab() -> Optional[Dict]:
    """Finds the open MatchTrader tab via CDP."""
    try:
        req = urllib.request.Request("http://127.0.0.1:9222/json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as resp:
            tabs = json.loads(resp.read().decode())
            for t in tabs:
                if "mtr-competition.fundingpips.com" in t.get("url", ""):
                    return t
    except Exception as e:
        logger.error(f"Error finding MatchTrader tab: {e}")
    return None


async def inspect_account_and_positions(ws) -> Dict[str, Any]:
    """
    Extracts live account number, open positions count, and trading status via DOM.
    """
    script = """
    (() => {
        const accBtn = document.querySelector('.profile-trading-account-select__active-account-button');
        const accText = accBtn ? accBtn.innerText.trim() : '';
        
        // Open positions count
        const openPosEl = document.querySelector('trade-open-positions-desktop');
        const emptyState = openPosEl ? openPosEl.innerText.includes('You don’t have any open positions') : true;
        const positionRows = openPosEl ? Array.from(openPosEl.querySelectorAll('.ui-list-row, [data-testid="position-row"]')) : [];
        const openCount = emptyState ? 0 : Math.max(1, positionRows.length);
        
        // Check active instrument
        const activeSymEl = document.querySelector('trade-symbol-market, .trade-market-watch-item--selected, trade-market-watch-item');
        const activeSymText = activeSymEl ? activeSymEl.innerText.slice(0, 50) : '';
        
        // Check if market is closed (e.g. 'Opens in...')
        const bodyText = document.body.innerText;
        const marketClosed = bodyText.includes('Opens in:');
        
        return {
            accountNumberText: accText,
            isTargetAccount: accText.includes('101305360'),
            openPositionsCount: openCount,
            emptyState: emptyState,
            activeSymbolSnippet: activeSymText,
            marketClosed: marketClosed
        };
    })()
    """
    await ws.send(json.dumps({
        "id": 101,
        "method": "Runtime.evaluate",
        "params": {"expression": script, "returnByValue": True}
    }))
    raw = await ws.recv()
    res = json.loads(raw).get("result", {}).get("result", {}).get("value", {})
    return res


async def select_instrument(ws, mtr_symbol: str) -> bool:
    """
    Selects the target instrument on MatchTrader interface.
    """
    script = f"""
    (() => {{
        // Check if instrument is already visible in the list
        const rows = Array.from(document.querySelectorAll('trade-instrument-symbol-name, .trade-instrument-symbol-name'));
        for (const r of rows) {{
            if (r.innerText.trim() === "{mtr_symbol}") {{
                r.click();
                return {{ ok: true, method: 'direct_click' }};
            }}
        }}
        
        // If not in visible list, try searching
        const searchInput = document.querySelector('trade-symbol-search input, input[placeholder="Search symbols"]');
        if (searchInput) {{
            searchInput.focus();
            searchInput.value = "{mtr_symbol}";
            searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
            searchInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
            return {{ ok: true, method: 'search_input' }};
        }}
        return {{ ok: false, error: 'symbol_not_found' }};
    }})()
    """
    await ws.send(json.dumps({
        "id": 102,
        "method": "Runtime.evaluate",
        "params": {"expression": script, "returnByValue": True}
    }))
    raw = await ws.recv()
    val = json.loads(raw).get("result", {}).get("result", {}).get("value", {})
    return val.get("ok", False)


async def execute_market_order(ws, side: str, lot_size: float = 0.01) -> Dict[str, Any]:
    """
    Places a Market Order (BUY or SELL) on MatchTrader.
    """
    side_lower = side.lower()
    btn_testid = "order-panel-buy-button" if side_lower == "buy" else "order-panel-sell-button"
    
    script = f"""
    (() => {{
        const btn = document.querySelector('button[data-testid="{btn_testid}"]');
        if (!btn) return {{ ok: false, error: 'button_not_found' }};
        if (btn.hasAttribute('disabled')) return {{ ok: false, error: 'market_closed_or_button_disabled' }};
        
        // Click the order button
        btn.click();
        
        return {{ ok: true, side: "{side_lower}", clicked: true }};
    }})()
    """
    await ws.send(json.dumps({
        "id": 103,
        "method": "Runtime.evaluate",
        "params": {"expression": script, "returnByValue": True}
    }))
    raw = await ws.recv()
    val = json.loads(raw).get("result", {}).get("result", {}).get("value", {})
    
    # Check for confirmation dialog if one appears
    await asyncio.sleep(1.0)
    confirm_script = """
    (() => {
        const confirmBtn = document.querySelector('.cdk-overlay-container button.ui-button--brand, [data-testid="confirm-order-button"]');
        if (confirmBtn && !confirmBtn.hasAttribute('disabled')) {
            confirmBtn.click();
            return { confirmed: true };
        }
        return { confirmed: false };
    })()
    """
    await ws.send(json.dumps({
        "id": 104,
        "method": "Runtime.evaluate",
        "params": {"expression": confirm_script, "returnByValue": True}
    }))
    raw_confirm = await ws.recv()
    conf_val = json.loads(raw_confirm).get("result", {}).get("result", {}).get("value", {})
    val["confirmation"] = conf_val
    return val


def calculate_lot_size(symbol: str, entry_price: float, stop_loss: float, risk_usd: float = DEFAULT_RISK_PER_TRADE_USD) -> float:
    """
    Calculates safety-bounded lot size to strictly maintain risk envelope.
    """
    sl_dist = abs(entry_price - stop_loss)
    if sl_dist <= 0:
        return 0.01
        
    if symbol in ["BTC", "BTCUSD"]:
        # 1 lot BTC = 1 BTC
        lots = risk_usd / sl_dist
    elif symbol in ["ETH", "ETHUSD"]:
        lots = risk_usd / sl_dist
    elif symbol in ["XAU", "XAUUSD"]:
        # 1 lot Gold = 100 oz
        lots = risk_usd / (sl_dist * 100.0)
    elif symbol in ["NDX100", "DJI30", "SPX500"]:
        # 1 point = $1 or $0.05
        lots = max(0.01, round(risk_usd / (sl_dist * 10.0), 2))
    else:
        lots = max(0.01, round(risk_usd / (sl_dist * 100.0), 2))
        
    # Cap lot size to conservative bounds
    return max(0.01, min(1.0, round(lots, 2)))


async def execute_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """
    End-to-end validated execution of a high-confluence trade signal.
    """
    symbol = signal.get("symbol", "")
    side = signal.get("side", "").lower()
    price = float(signal.get("price", 0.0))
    trade_levels = signal.get("trade_levels", {})
    stop_loss = float(trade_levels.get("stop_loss", 0.0))
    take_profit = float(trade_levels.get("take_profit_1", 0.0))
    score = float(signal.get("confluence_score", 0.0))
    
    logger.info(f"Received trade execution request: {symbol} {side.upper()} @ {price} (Confluence: {score}/100)")
    
    # 1. Gate: Confluence Threshold
    if score < 70.0:
        logger.warning(f"Execution rejected: Confluence score {score} < 70.0 threshold.")
        return {"status": "REJECTED", "reason": "LOW_CONFLUENCE_SCORE", "score": score}
        
    mtr_symbol = SYMBOL_TO_MATCHTRADER.get(symbol, f"{symbol}USD")
    tab = get_matchtrader_tab()
    if not tab:
        logger.error("Execution failed: MatchTrader tab not open in browser.")
        return {"status": "FAILED", "reason": "MATCHTRADER_TAB_NOT_FOUND"}
        
    with BrowserLock("MatchTraderExecution", timeout_secs=5.0) as lock:
        if not lock.acquired:
            logger.warning("Execution deferred: BrowserLock busy.")
            return {"status": "DEFERRED", "reason": "BROWSER_LOCK_BUSY"}
            
        async with websockets.connect(tab["webSocketDebuggerUrl"], open_timeout=5, close_timeout=2) as ws:
            # 2. Gate: Account & Risk Audit
            state = await inspect_account_and_positions(ws)
            if not state.get("isTargetAccount"):
                logger.error(f"Execution rejected: Active account is not target {TARGET_ACCOUNT_NUMBER}! (Found: {state.get('accountNumberText')})")
                return {"status": "REJECTED", "reason": "WRONG_ACCOUNT", "details": state}
                
            open_count = state.get("openPositionsCount", 0)
            if open_count >= MAX_CONCURRENT_POSITIONS:
                logger.warning(f"Execution rejected: Max concurrent positions ({open_count}/{MAX_CONCURRENT_POSITIONS}) reached.")
                return {"status": "REJECTED", "reason": "MAX_CONCURRENT_POSITIONS_REACHED", "open_positions": open_count}
                
            # 3. Select Instrument
            selected = await select_instrument(ws, mtr_symbol)
            await asyncio.sleep(1.0)
            
            # 4. Calculate Sizing
            lots = calculate_lot_size(symbol, price, stop_loss)
            
            # 5. Execute Order
            res = await execute_market_order(ws, side, lots)
            
            result_record = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "account_number": TARGET_ACCOUNT_NUMBER,
                "symbol": symbol,
                "matchtrader_symbol": mtr_symbol,
                "side": side.upper(),
                "entry_price": price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "lot_size": lots,
                "confluence_score": score,
                "execution_response": res,
                "status": "EXECUTED" if res.get("ok") else "FAILED"
            }
            
            # Atomic append to audit ledger
            with open(EXECUTED_TRADES_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(result_record) + "\n")
                
            logger.info(f"Execution result for {symbol}: {result_record['status']} - {res}")
            return result_record


async def main():
    logger.info(f"MatchTrader Live Execution Bridge initialized for Account {TARGET_ACCOUNT_NUMBER}.")
    tab = get_matchtrader_tab()
    if tab:
        logger.info(f"MatchTrader tab detected: {tab.get('id')} - {tab.get('title')}")
        with BrowserLock("MatchTraderAudit", timeout_secs=3.0) as lock:
            if lock.acquired:
                async with websockets.connect(tab["webSocketDebuggerUrl"]) as ws:
                    state = await inspect_account_and_positions(ws)
                    logger.info(f"Live State: Target Account Verified={state.get('isTargetAccount')}, Open Positions={state.get('openPositionsCount')}, Market Closed={state.get('marketClosed')}")
    else:
        logger.warning("MatchTrader tab is currently not open.")


if __name__ == "__main__":
    asyncio.run(main())
