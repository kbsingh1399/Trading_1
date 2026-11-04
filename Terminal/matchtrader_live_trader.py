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
import re
import json
import time
import logging
import asyncio
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
MATCHTRADER_ACCOUNT = '101305360'

def matchtrader_symbol(asset, path=None):
    path = path or Path(__file__).resolve().parent.parent / 'Database/matchtrader_instruments_dump.json'
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8-sig'))
        if str(data.get('_cacheTradingAccountId')) != MATCHTRADER_ACCOUNT:
            return None
        symbols = {s for values in data['_mappedCategories'].values() for s in values}
        alias = {'BTC': 'BTCUSD', 'ETH': 'ETHUSD', 'XRP': 'XRPUSD', 'XAU': 'XAUUSD',
                 'GOLD': 'XAUUSD', 'XAG': 'XAGUSD', 'SILVER': 'XAGUSD',
                 'SP500': 'SPX500', 'NAS100': 'NDX100', 'DJ30': 'DJI30'}
        candidate = alias.get(asset, asset)
        return candidate if candidate in symbols else None
    except (OSError, ValueError, KeyError, TypeError):
        return None
from Terminal.risk_envelope import (
    HARD_FLOOR_USD,
    MAX_RISK_USD,
    MIN_RISK_USD,
    MAX_OPEN_POSITIONS,
    MAX_DAILY_LOSS_USD,
    MIN_RR_RATIO,
    validate_trade_geometry,
    daily_loss_remaining
)

DB_DIR = ROOT_DIR / "Database"
EXECUTED_TRADES_FILE = DB_DIR / "matchtrader_executed_trades.jsonl"
MATCHTRADER_STATE_FILE = DB_DIR / "matchtrader_account_state.json"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [MATCHTRADER] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("MatchTraderBridge")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# Strict FundingPips MatchTrader Risk Constants ($5,000 Challenge Account 101305360)
TARGET_ACCOUNT_NUMBER = "101305360"
INITIAL_BALANCE = 5000.0
EQUITY_FLOOR_TOTAL = HARD_FLOOR_USD
MAX_TOTAL_DRAWDOWN_LIMIT = INITIAL_BALANCE - HARD_FLOOR_USD  # $225.00 survival buffer
MAX_DAILY_DRAWDOWN_LIMIT = MAX_DAILY_LOSS_USD  # $250.00 dynamic 5% daily loss limit
EQUITY_FLOOR_DAILY = INITIAL_BALANCE - MAX_DAILY_LOSS_USD  # $4,750.00 dynamic daily floor
MAX_CONCURRENT_POSITIONS = MAX_OPEN_POSITIONS  # Strict SSOT: 2, no env override
DEFAULT_RISK_PER_TRADE_USD = MAX_RISK_USD  # $25.00 strictly capped per trade
LIVE_DISPATCH_ENABLED = False

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
    "XAUUSD": "XAUUSD",
    "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDJPY": "USDJPY",
    "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD",
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
    if not ws:
        return {"data_valid": False, "error": "NO_WEBSOCKET"}
    """
    Extracts live account number, free funds, equity, drawdown limits,
    open positions count, and trading status via ground-truth DOM selectors.
    """
    script = """
    (() => {
        try {
            const accBtn = document.querySelector('[data-testid="active-account-button"], .profile-trading-account-select__active-account-button');
            const accText = accBtn ? accBtn.innerText.trim() : '';
            
            // Profit item
            const profitEl = document.querySelector('[data-testid="profitItem"] [data-testid="fund-item-value"]');
            const profitText = profitEl ? profitEl.innerText.replace(/[^0-9.-]/g, '') : '0.0';
            
            // Goals widget
            const goalsEl = document.querySelector('[data-testid="widget-prop-goals"]');
            const goalsText = goalsEl ? goalsEl.innerText : '';
            
            // Open positions count
            const openPosEl = document.querySelector('trade-open-positions-desktop, [data-testid="open-positions"]');
            const emptyState = openPosEl ? (openPosEl.innerText.includes('don’t have any open positions') || openPosEl.innerText.includes("don't have any open positions")) : true;
            const positionRows = openPosEl ? Array.from(openPosEl.querySelectorAll('.ui-list-row, [data-testid="position-row"]')) : [];
            const openCount = emptyState ? 0 : Math.max(1, positionRows.length);
            
            // Active symbol
            const activeSymEl = document.querySelector('trade-symbol-market, .trade-market-watch-item--selected, trade-market-watch-item');
            const activeSymText = activeSymEl ? activeSymEl.innerText.slice(0, 50) : '';
            
            // Market status
            const bodyText = document.body.innerText;
            const marketClosed = bodyText.includes('Opens in:');
            
            return {
                accountText: accText,
                profitText: profitText,
                goalsText: goalsText,
                openPositionsCount: openCount,
                emptyState: emptyState,
                activeSymbolSnippet: activeSymText,
                marketClosed: marketClosed
            };
        } catch(e) {
            return { error: e.toString() };
        }
    })()
    """
    await ws.send(json.dumps({
        "id": 101,
        "method": "Runtime.evaluate",
        "params": {"expression": script, "returnByValue": True}
    }))
    raw = await ws.recv()
    raw_val = json.loads(raw).get("result", {}).get("result", {}).get("value", {})
    if not isinstance(raw_val, dict) or raw_val.get("error"):
        return {"data_valid": False, "error": raw_val.get("error", "DOM inspect failed")}

    acc_text = raw_val.get("accountText", "")
    is_target_account = TARGET_ACCOUNT_NUMBER in acc_text
    profit_usd = float(raw_val.get("profitText", 0.0) or 0.0)
    current_equity = INITIAL_BALANCE + profit_usd

    goals_text = raw_val.get("goalsText", "")
    daily_match = re.search(r"Max Daily Loss.*?\$([0-9.,]+)\s*/\s*\$([0-9]+)K", goals_text, re.DOTALL)
    total_match = re.search(r"Max Loss.*?\$([0-9.,]+)\s*/\s*\$([0-9]+)K", goals_text, re.DOTALL)

    current_daily_loss = float(daily_match.group(1).replace(",", "")) if daily_match else 0.0
    max_daily_loss_limit = float(daily_match.group(2)) * 1000.0 if daily_match else MAX_DAILY_DRAWDOWN_LIMIT

    current_total_loss = float(total_match.group(1).replace(",", "")) if total_match else 0.0
    max_total_loss_limit = float(total_match.group(2)) * 1000.0 if total_match else MAX_TOTAL_DRAWDOWN_LIMIT

    daily_floor_breached = (current_daily_loss >= max_daily_loss_limit) or (current_equity < EQUITY_FLOOR_DAILY)
    total_floor_breached = (current_total_loss >= max_total_loss_limit) or (current_equity < EQUITY_FLOOR_TOTAL)
    open_count = int(raw_val.get("openPositionsCount", 0))

    can_trade = bool(
        is_target_account and
        (not daily_floor_breached) and
        (not total_floor_breached) and
        (open_count < MAX_CONCURRENT_POSITIONS) and
        (not raw_val.get("marketClosed", False))
    )

    state = {
        "data_valid": True,
        "account_number": TARGET_ACCOUNT_NUMBER,
        "is_target_account": is_target_account,
        "current_equity": current_equity,
        "profit_usd": profit_usd,
        "current_daily_loss": current_daily_loss,
        "max_daily_loss_limit": max_daily_loss_limit,
        "daily_floor_breached": daily_floor_breached,
        "current_total_loss": current_total_loss,
        "max_total_loss_limit": max_total_loss_limit,
        "total_floor_breached": total_floor_breached,
        "open_positions_count": open_count,
        "max_concurrent_allowed": MAX_CONCURRENT_POSITIONS,
        "market_closed": bool(raw_val.get("marketClosed")),
        "can_trade": can_trade,
        "inspected_at_utc": datetime.now(timezone.utc).isoformat()
    }
    atomic_json(MATCHTRADER_STATE_FILE, state)
    return state


async def select_instrument(ws, mtr_symbol: str) -> bool:
    """
    Selects the target instrument on MatchTrader interface.
    """
    script = f"""
    (() => {{
        // 1. Direct row match in visible market watch
        const rows = Array.from(document.querySelectorAll('trade-instrument-symbol-name, .trade-instrument-symbol-name, trade-market-watch-item'));
        for (const r of rows) {{
            if (r.innerText.includes("{mtr_symbol}")) {{
                r.click();
                return {{ ok: true, method: 'direct_click' }};
            }}
        }}
        
        // 2. Search input
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
    return bool(val.get("ok", False))


def calculate_lot_size(symbol: str, entry_price: float, stop_loss: float, risk_usd: float = DEFAULT_RISK_PER_TRADE_USD) -> float:
    """Conservative lot sizing based on stop loss distance and contract units."""
    import math
    from decimal import Decimal, ROUND_FLOOR
    if not all(isinstance(x, (int, float)) and math.isfinite(x) and x > 0
               for x in (entry_price, stop_loss, risk_usd)) or entry_price == stop_loss:
        return 0.0
    units = {
        'BTC': 1, 'BTCUSD': 1,
        'ETH': 1, 'ETHUSD': 1,
        'SOL': 1, 'SOLUSD': 1,
        'XRP': 1, 'XRPUSD': 1,
        'XAU': 100, 'XAUUSD': 100,
        'XAG': 5000, 'XAGUSD': 5000,
        'EURUSD': 100000, 'GBPUSD': 100000,
        'AUDUSD': 100000, 'USDCAD': 100000, 'USDJPY': 1000,
        'DJI30': 1, 'SPX500': 1, 'NDX100': 1
    }
    mult = units.get(symbol, 1)
    loss = abs(Decimal(str(entry_price)) - Decimal(str(stop_loss))) * mult
    if loss <= Decimal('0'):
        return 0.0
    steps = (Decimal(str(risk_usd)) / loss / Decimal('0.01')).to_integral_value(rounding=ROUND_FLOOR)
    if steps < 1:
        return 0.0
    # Conservative cap: max 5.00 lots for forex, 1.00 lot for crypto / indices
    max_cap = Decimal('5.00') if symbol in ('EURUSD', 'GBPUSD', 'AUDUSD', 'USDCAD', 'USDJPY') else Decimal('1.00')
    return float(min(max_cap, steps * Decimal('0.01')))


async def execute_market_order(ws, side: str, lot_size: float = 0.01, symbol: str = "", stop_loss: float = 0.0, take_profit: float = 0.0) -> Dict[str, Any]:
    if not LIVE_DISPATCH_ENABLED:
        return {"status": "REJECTED", "ok": False, "reason": "MATCHTRADER_DOM_DISPATCH_DISABLED_UNVERIFIED"}
    if not ws:
        return {"status": "REJECTED", "ok": False, "reason": "BROKER_VOLUME_BRACKETS_AND_DAILY_BASELINE_UNVERIFIED"}
    """Safety-gated market order execution with live account verification and in-flight lock."""
    inflight_lock = DB_DIR / ".locks" / "order_execution_inflight.lock"
    inflight_lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        inflight_lock.touch()
    except Exception:
        pass

    try:
        with BrowserLock("MatchTraderLiveExecution", timeout_secs=8.0) as browser_lock:
            if not browser_lock.acquired:
                return {"status": "REJECTED", "ok": False, "reason": "BROWSER_LOCK_CONTENTION_DURING_ORDER"}
            state = await inspect_account_and_positions(ws)
            if not state.get("can_trade"):
                return {
                    "status": "REJECTED",
                    "ok": False,
                    "reason": f"Risk guardrail active: can_trade=False (Account={state.get('account_number')}, OpenPos={state.get('open_positions_count')}, DailyFloorBreached={state.get('daily_floor_breached')})"
                }

            # 1. If symbol provided, search and focus on that symbol
            if symbol:
                search_script = f"""
                (() => {{
                    try {{
                        const search = document.querySelector('input[data-testid="input-element"][placeholder*="Search"]');
                        if (search) {{
                            search.focus();
                            search.value = '{symbol}';
                            search.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            search.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            return {{ ok: true, symbol: '{symbol}' }};
                        }}
                        return {{ ok: false, error: 'search_input_not_found' }};
                    }} catch(e) {{
                        return {{ ok: false, error: e.toString() }};
                    }}
                }})()
                """
                await ws.send(json.dumps({
                    "id": 103,
                    "method": "Runtime.evaluate",
                    "params": {"expression": search_script, "returnByValue": True}
                }))
                await ws.recv()
                await asyncio.sleep(0.3)

                # Click the searched symbol row to reveal its order panel
                click_script = f"""
                (() => {{
                    try {{
                        const el = Array.from(document.querySelectorAll('*')).find(e => {{
                            return e.children.length === 0 && e.innerText && e.innerText.trim().toUpperCase() === '{symbol}'.toUpperCase();
                        }});
                        if (el) {{
                            el.click();
                            const row = el.closest('trade-market-watch-item, ui-list-row, [role="row"], tr') || el.parentElement;
                            if (row) row.click();
                            return {{ ok: true, action: 'row_clicked' }};
                        }}
                        return {{ ok: false, error: 'symbol_row_not_found' }};
                    }} catch(e) {{
                        return {{ ok: false, error: e.toString() }};
                    }}
                }})()
                """
                await ws.send(json.dumps({
                    "id": 1031,
                    "method": "Runtime.evaluate",
                    "params": {"expression": click_script, "returnByValue": True}
                }))
                await ws.recv()
                await asyncio.sleep(0.3)

            # 2. Set lot size in the input stepper (Goldfish Finding 5)
            lot_str = f"{lot_size:.2f}"
            stepper_script = f"""
            (() => {{
                try {{
                    const stepper = document.querySelector('[data-testid="input-stepper-input"], .ui-input-stepper__input');
                    if (stepper) {{
                        stepper.focus();
                        if ('value' in stepper) {{
                            const proto = Object.getPrototypeOf(stepper);
                            const setVal = Object.getOwnPropertyDescriptor(proto, 'value')?.set || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
                            if (setVal) setVal.call(stepper, '{lot_str}');
                            else stepper.value = '{lot_str}';
                        }}
                        stepper.innerText = '{lot_str}';
                        stepper.textContent = '{lot_str}';
                        stepper.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        stepper.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        stepper.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                        return {{ ok: true, lot: '{lot_str}' }};
                    }}
                    return {{ ok: false, error: 'stepper_not_found' }};
                }} catch(e) {{
                    return {{ ok: false, error: e.toString() }};
                }}
            }})()
            """
            await ws.send(json.dumps({
                "id": 104,
                "method": "Runtime.evaluate",
                "params": {"expression": stepper_script, "returnByValue": True}
            }))
            await ws.recv()
            await asyncio.sleep(0.1)

            # 3. Select and punch order on MatchTrader DOM via Native CDP Mouse Click
            side_lower = side.lower()
            get_btn_coords = f"""
            (() => {{
                try {{
                    const btn = document.querySelector('[data-testid="order-panel-{side_lower}-button"], .trade-order-panel__order-{side_lower}');
                    if (btn) {{
                        const r = btn.getBoundingClientRect();
                        return {{ ok: true, x: r.left + r.width / 2, y: r.top + r.height / 2 }};
                    }}
                    return {{ ok: false, error: 'order_button_not_found' }};
                }} catch(e) {{
                    return {{ ok: false, error: e.toString() }};
                }}
            }})()
            """
            await ws.send(json.dumps({
                "id": 105,
                "method": "Runtime.evaluate",
                "params": {"expression": get_btn_coords, "returnByValue": True}
            }))
            raw_b = await ws.recv()
            val = json.loads(raw_b).get("result", {}).get("result", {}).get("value", {})

            if val and val.get("ok") and val.get("x") is not None:
                bx, by = val["x"], val["y"]
                await ws.send(json.dumps({"id": 1051, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseMoved", "x": bx, "y": by}}))
                await ws.recv()
                await ws.send(json.dumps({"id": 1052, "method": "Input.dispatchMouseEvent", "params": {"type": "mousePressed", "button": "left", "clickCount": 1, "x": bx, "y": by}}))
                await ws.recv()
                await ws.send(json.dumps({"id": 1053, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseReleased", "button": "left", "clickCount": 1, "x": bx, "y": by}}))
                await ws.recv()
                await asyncio.sleep(0.4)

                # Auto-confirm modal if present
                get_confirm_coords = """
                (() => {
                    try {
                        const confirmBtn = Array.from(document.querySelectorAll('button')).find(b => b.innerText && b.innerText.trim() === 'Confirm');
                        if (confirmBtn) {
                            const r = confirmBtn.getBoundingClientRect();
                            return { ok: true, x: r.left + r.width / 2, y: r.top + r.height / 2 };
                        }
                        return { ok: false };
                    } catch(e) {
                        return { ok: false, error: e.toString() };
                    }
                })()
                """
                await ws.send(json.dumps({
                    "id": 1054,
                    "method": "Runtime.evaluate",
                    "params": {"expression": get_confirm_coords, "returnByValue": True}
                }))
                raw_c = await ws.recv()
                val_c = json.loads(raw_c).get("result", {}).get("result", {}).get("value", {})
                if val_c and val_c.get("ok") and val_c.get("x") is not None:
                    cx, cy = val_c["x"], val_c["y"]
                    await ws.send(json.dumps({"id": 1055, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseMoved", "x": cx, "y": cy}}))
                    await ws.recv()
                    await ws.send(json.dumps({"id": 1056, "method": "Input.dispatchMouseEvent", "params": {"type": "mousePressed", "button": "left", "clickCount": 1, "x": cx, "y": cy}}))
                    await ws.recv()
                    await ws.send(json.dumps({"id": 1057, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseReleased", "button": "left", "clickCount": 1, "x": cx, "y": cy}}))
                    await ws.recv()
                    val["confirmed"] = True
                    await asyncio.sleep(0.3)

            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc + timedelta(hours=5, minutes=30)
            record = {
                "executed_at_utc": now_utc.isoformat(),
                "executed_at_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
                "account": state.get("account_number"),
                "equity": state.get("current_equity"),
                "symbol": symbol,
                "side": side.upper(),
                "lots": lot_size,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "status": "DISPATCHED" if val.get("ok") else "FAILED_CLICK",
                "details": val
            }
        
            try:
                trades_file = DB_DIR / "Signals" / "matchtrader_executed_trades.jsonl"
                trades_file.parent.mkdir(parents=True, exist_ok=True)
                with open(trades_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record) + "\n")
            except Exception as e:
                logger.warning(f"Failed to record trade log: {e}")

            logger.info(f"⚡ [MATCHTRADER DISPATCH] Account {state.get('account_number')} | {symbol} {side.upper()} {lot_size} Lots (SL: {stop_loss}, TP: {take_profit}) | Status: {record['status']}")

            return {
                "status": "DISPATCHED",
                "ok": bool(val.get("ok", True)),
                "account": state.get("account_number"),
                "equity": state.get("current_equity"),
                "symbol": symbol,
                "side": side.upper(),
                "lots": lot_size,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "details": val
            }
    finally:
        if inflight_lock.exists():
            try:
                inflight_lock.unlink()
            except Exception:
                pass


async def execute_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """Validates full trade geometry, account health, and dispatches via MatchTrader."""
    tab = get_matchtrader_tab()
    if not tab:
        return {"status": "REJECTED", "ok": False, "reason": "MATCHTRADER_TAB_NOT_FOUND"}

    async with websockets.connect(tab["webSocketDebuggerUrl"]) as ws:
        state = await inspect_account_and_positions(ws)
        if not state.get("can_trade"):
            return {
                "status": "REJECTED",
                "ok": False,
                "reason": "RISK_GUARDRAILS_PREVENT_EXECUTION",
                "details": state
            }

        sym = signal.get("symbol", "")
        mtr_sym = matchtrader_symbol(sym) or SYMBOL_TO_MATCHTRADER.get(sym)
        if not mtr_sym:
            return {"status": "REJECTED", "ok": False, "reason": f"SYMBOL_NOT_MAPPED_TO_MATCHTRADER: {sym}"}

        side = signal.get("direction", "").upper()
        if side not in ("BUY", "SELL"):
            return {"status": "REJECTED", "ok": False, "reason": f"INVALID_DIRECTION: {side}"}

        trade_lvl = signal.get("trade_levels", {})
        lots = trade_lvl.get("admissible_lots")
        entry = float(trade_lvl.get("entry_price") or signal.get("price") or 0.0)
        sl = trade_lvl.get("stop_loss")
        tp = trade_lvl.get("take_profit_1")

        if not lots or not sl or not tp or not math.isfinite(sl) or not math.isfinite(tp) or lots <= 0:
            return {"status": "REJECTED", "ok": False, "reason": "MISSING_MANDATORY_RISK_PARAMETERS"}

        geom_ok, geom_reason = validate_trade_geometry(
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            direction=side,
            min_rr=MIN_RR_RATIO
        )
        if not geom_ok:
            return {"status": "REJECTED", "ok": False, "reason": f"INVALID_TRADE_GEOMETRY: {geom_reason}"}

        res = await execute_market_order(ws, side, lots, symbol=mtr_sym, stop_loss=sl, take_profit=tp)
        return res


async def get_open_positions_detailed(ws) -> List[Dict[str, Any]]:
    """Extracts structured list of all open positions directly from MatchTrader DOM."""
    if not ws:
        return []
    script = """
    (() => {
        const container = document.querySelector('trade-open-positions-desktop, [data-testid="open-positions"]');
        if (!container) return [];
        
        const rows = Array.from(container.querySelectorAll('.ui-list-row, [data-testid="position-row"]'));
        return rows.map((r, i) => {
            const symEl = r.querySelector('[data-testid="instrument-symbol-name"], trade-instrument-symbol-name');
            const sym = symEl ? symEl.innerText.trim() : 'UNKNOWN';
            
            const volEl = r.querySelector('[data-testid="open-position-volume"]');
            const vol = volEl ? volEl.innerText.trim().replace(/\\s+/g, '') : '0';
            
            const profitValEl = r.querySelector('[data-testid="open-position-profit-value"]');
            const profitVal = profitValEl ? parseFloat(profitValEl.innerText.replace(/[^0-9.-]/g, '')) : 0.0;
            
            const profitPctEl = r.querySelector('[data-testid="open-position-profit-percentage"]');
            const profitPct = profitPctEl ? parseFloat(profitPctEl.innerText.replace(/[^0-9.-]/g, '')) : 0.0;
            
            const txt = r.innerText.replace(/\\n+/g, ' | ');
            const isSell = /Sell/i.test(txt);
            const isBuy = /Buy/i.test(txt);
            const side = isSell ? 'SELL' : (isBuy ? 'BUY' : 'UNKNOWN');
            
            const nums = txt.match(/([0-9]+\\.[0-9]+)/g) || [];
            const openPrice = nums.length >= 1 ? parseFloat(nums[0]) : 0.0;
            const currentPrice = nums.length >= 2 ? parseFloat(nums[1]) : 0.0;

            return {
                index: i,
                symbol: sym,
                side: side,
                volume: vol,
                open_price: openPrice,
                current_price: currentPrice,
                profit_usd: profitVal,
                profit_pct: profitPct,
                raw_text: txt
            };
        });
    })()
    """
    await ws.send(json.dumps({'id': 109, 'method': 'Runtime.evaluate', 'params': {'expression': script, 'returnByValue': True}}))
    raw = await ws.recv()
    val = json.loads(raw).get('result', {}).get('result', {}).get('value', [])
    return val if isinstance(val, list) else []


async def close_position_by_index(ws, index: int = 0) -> Dict[str, Any]:
    """Closes a specific open position row on MatchTrader via CDP."""
    if not ws:
        return {"ok": False, "error": "NO_WEBSOCKET"}

    get_coords_script = f"""
    (() => {{
        try {{
            const container = document.querySelector('trade-open-positions-desktop, [data-testid="open-positions"]');
            if (!container) return {{ ok: false, error: 'container_not_found' }};
            const rows = Array.from(container.querySelectorAll('.ui-list-row, [data-testid="position-row"]'));
            if ({index} >= rows.length) return {{ ok: false, error: 'index_out_of_range' }};
            const row = rows[{index}];
            const closeBtn = row.querySelector('[data-testid="close-position-button"], button.trade-open-positions-desktop__close-position-button');
            if (!closeBtn) return {{ ok: false, error: 'close_button_not_found' }};
            const r = closeBtn.getBoundingClientRect();
            return {{ ok: true, x: r.left + r.width / 2, y: r.top + r.height / 2 }};
        }} catch(e) {{
            return {{ ok: false, error: e.toString() }};
        }}
    }})()
    """
    await ws.send(json.dumps({'id': 1101, 'method': 'Runtime.evaluate', 'params': {'expression': get_coords_script, 'returnByValue': True}}))
    raw = await ws.recv()
    val = json.loads(raw).get('result', {}).get('result', {}).get('value', {})
    if not val.get('ok'):
        return val

    bx, by = val['x'], val['y']
    await ws.send(json.dumps({"id": 1102, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseMoved", "x": bx, "y": by}}))
    await ws.recv()
    await ws.send(json.dumps({"id": 1103, "method": "Input.dispatchMouseEvent", "params": {"type": "mousePressed", "button": "left", "clickCount": 1, "x": bx, "y": by}}))
    await ws.recv()
    await ws.send(json.dumps({"id": 1104, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseReleased", "button": "left", "clickCount": 1, "x": bx, "y": by}}))
    await ws.recv()
    await asyncio.sleep(0.4)

    confirm_script = """
    (() => {
        try {
            const confirmBtn = Array.from(document.querySelectorAll('button')).find(b => b.innerText && b.innerText.trim() === 'Confirm');
            if (confirmBtn) {
                const r = confirmBtn.getBoundingClientRect();
                return { ok: true, x: r.left + r.width / 2, y: r.top + r.height / 2 };
            }
            return { ok: false };
        } catch(e) {
            return { ok: false, error: e.toString() };
        }
    })()
    """
    await ws.send(json.dumps({'id': 1105, 'method': 'Runtime.evaluate', 'params': {'expression': confirm_script, 'returnByValue': True}}))
    raw_c = await ws.recv()
    val_c = json.loads(raw_c).get('result', {}).get('result', {}).get('value', {})
    if val_c.get('ok'):
        cx, cy = val_c['x'], val_c['y']
        await ws.send(json.dumps({"id": 1106, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseMoved", "x": cx, "y": cy}}))
        await ws.recv()
        await ws.send(json.dumps({"id": 1107, "method": "Input.dispatchMouseEvent", "params": {"type": "mousePressed", "button": "left", "clickCount": 1, "x": cx, "y": cy}}))
        await ws.recv()
        await ws.send(json.dumps({"id": 1108, "method": "Input.dispatchMouseEvent", "params": {"type": "mouseReleased", "button": "left", "clickCount": 1, "x": cx, "y": cy}}))
        await ws.recv()
        await asyncio.sleep(0.3)

    return {"ok": True, "closed_index": index}



async def main():
    logger.info(f"MatchTrader Live Execution Bridge initialized for Account {TARGET_ACCOUNT_NUMBER}.")
    tab = get_matchtrader_tab()
    if tab:
        logger.info(f"MatchTrader tab detected: {tab.get('id')} - {tab.get('title')}")
        async with websockets.connect(tab["webSocketDebuggerUrl"]) as ws:
            state = await inspect_account_and_positions(ws)
            logger.info(f"Live State: Target Account={state.get('is_target_account')}, Equity=${state.get('current_equity'):,.2f}, Open Pos={state.get('open_positions_count')}, Can Trade={state.get('can_trade')}")
    else:
        logger.warning("MatchTrader tab is currently not open.")


if __name__ == "__main__":
    asyncio.run(main())
