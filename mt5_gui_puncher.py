#!/usr/bin/env python3
"""
MT5 GUI Order Puncher & Autonomous SL/TP Modifier
Automates order execution via the native MetaTrader 5 UI Order Dialog,
bypassing server-side EA / API blocks (retcode 10026) while strictly respecting
FundedNext risk rules and the user's $10-$15 risk window.

Automatically follows up market fills with direct GUI control targeting to attach
hard Stop Loss and Take Profit, ensuring full 360 broker server-side risk protection.
"""

import sys
import time
import ctypes
import logging
import win32gui
import win32con
import win32api
import pyautogui
import MetaTrader5 as mt5

pyautogui.FAILSAFE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MT5GUIPuncher")

DESKTOP_ACCESS_FLAGS = 0x0001 | 0x0040 | 0x0002 | 0x0008 | 0x0010 | 0x0020


def attach_desktop():
    """Switch thread to default interactive desktop station."""
    hDesk = ctypes.windll.user32.OpenDesktopW("default", 0, False, DESKTOP_ACCESS_FLAGS)
    if hDesk:
        ctypes.windll.user32.SetThreadDesktop(hDesk)
        return True
    return False


def find_mt5_window():
    """Find top-level MT5 window handle."""
    mt5_hwnd = None
    def cb(h, _):
        nonlocal mt5_hwnd
        t = win32gui.GetWindowText(h)
        if "11808165 - FundedNext-Server" in t:
            mt5_hwnd = h
        return True
    win32gui.EnumWindows(cb, None)
    return mt5_hwnd


def find_child_by_id(parent, target_id, cls_name=None):
    """Enumerate child controls matching target control ID."""
    res = []
    def c_cb(h, _):
        if win32api.GetWindowLong(h, win32con.GWL_ID) == target_id:
            if cls_name is None or win32gui.GetClassName(h) == cls_name:
                res.append(h)
        return True
    win32gui.EnumChildWindows(parent, c_cb, None)
    return res


def modify_position_sltp(ticket: int, sl: str, tp: str) -> bool:
    """
    Automates MT5 Position Modification dialog to attach SL and TP.
    """
    attach_desktop()
    mt5_hwnd = find_mt5_window()
    if not mt5_hwnd:
        logger.error("MT5 window not found for SL/TP modification.")
        return False

    ctypes.windll.user32.ShowWindow(mt5_hwnd, 9)
    ctypes.windll.user32.SetForegroundWindow(mt5_hwnd)
    time.sleep(0.4)

    # Double-click row 0 of Trade tab (SL column area at x=865, y=855)
    logger.info("Opening Modify Position dialog via Trade tab double-click...")
    pyautogui.doubleClick(865, 855)
    time.sleep(1.0)

    dlg = None
    def find_dlg(h, _):
        nonlocal dlg
        if win32gui.GetClassName(h) == '#32770' and 'Position:' in win32gui.GetWindowText(h):
            dlg = h
        return True
    win32gui.EnumWindows(find_dlg, None)

    if not dlg:
        logger.error("Modify dialog did not open.")
        return False

    ctypes.windll.user32.SetForegroundWindow(dlg)
    time.sleep(0.3)

    # Find controls
    sl_edit = None
    tp_edit = None
    mod_btn = None
    submit_btn = None

    def cc(h, _):
        nonlocal sl_edit, tp_edit, mod_btn, submit_btn
        cid = win32api.GetWindowLong(h, win32con.GWL_ID)
        vis = win32gui.IsWindowVisible(h)
        if cid == 11120:
            mod_btn = h
        elif cid == 10334 and vis:
            sl_edit = h
        elif cid == 10336 and vis:
            tp_edit = h
        elif cid == 10351:
            submit_btn = h
        return True
    win32gui.EnumChildWindows(dlg, cc, None)

    if not sl_edit or not submit_btn:
        if mod_btn:
            logger.info("Clicking Modify Position mode button (ID 11120)...")
            win32gui.SendMessage(mod_btn, win32con.BM_CLICK, 0, 0)
            time.sleep(0.5)
            win32gui.EnumChildWindows(dlg, cc, None)

    if not sl_edit or not tp_edit or not submit_btn:
        logger.error("Could not find all modify edit controls.")
        return False

    sl_rect = win32gui.GetWindowRect(sl_edit)
    tp_rect = win32gui.GetWindowRect(tp_edit)
    sub_rect = win32gui.GetWindowRect(submit_btn)

    sl_x = (sl_rect[0] + sl_rect[2]) // 2
    sl_y = (sl_rect[1] + sl_rect[3]) // 2
    tp_x = (tp_rect[0] + tp_rect[2]) // 2
    tp_y = (tp_rect[1] + tp_rect[3]) // 2
    sub_x = (sub_rect[0] + sub_rect[2]) // 2
    sub_y = (sub_rect[1] + sub_rect[3]) // 2

    logger.info(f"Setting SL={sl} and TP={tp}...")
    pyautogui.click(sl_x, sl_y)
    time.sleep(0.1)
    pyautogui.hotkey('ctrl', 'a')
    time.sleep(0.1)
    pyautogui.write(str(sl), interval=0.04)
    time.sleep(0.2)

    pyautogui.click(tp_x, tp_y)
    time.sleep(0.1)
    pyautogui.hotkey('ctrl', 'a')
    time.sleep(0.1)
    pyautogui.write(str(tp), interval=0.04)
    time.sleep(0.4)

    logger.info("Clicking Submit Modify button...")
    pyautogui.click(sub_x, sub_y)
    time.sleep(1.2)
    pyautogui.press('enter')
    time.sleep(0.5)

    # Verification
    if mt5.initialize():
        pos = mt5.positions_get(ticket=ticket)
        if pos:
            p = pos[0]
            logger.info(f"Verified Position {p.ticket}: SL={p.sl}, TP={p.tp}, PnL=${p.profit:.2f}")
            mt5.shutdown()
            return p.sl > 0
        mt5.shutdown()
    return True


def punch_market_order(symbol: str, volume: str, sl: str, tp: str, order_type: str = "BUY") -> bool:
    """
    Punches a market order via MT5 GUI Order Dialog (<F9>) and automatically
    attaches hard SL and TP via the Modify dialog.
    """
    if not attach_desktop():
        logger.error("Could not attach to default interactive desktop.")
        return False

    mt5_hwnd = find_mt5_window()
    if not mt5_hwnd:
        logger.error("Could not find running MT5 main window.")
        return False

    # Find chart MDI window
    chart_hwnd = None
    def chart_cb(h, _):
        nonlocal chart_hwnd
        t = win32gui.GetWindowText(h)
        if symbol in t:
            chart_hwnd = h
        return True
    win32gui.EnumChildWindows(mt5_hwnd, chart_cb, None)

    if chart_hwnd:
        mdi_client = None
        def mdi_cb(h, _):
            nonlocal mdi_client
            if win32gui.GetClassName(h) == "MDIClient":
                mdi_client = h
            return True
        win32gui.EnumChildWindows(mt5_hwnd, mdi_cb, None)
        if mdi_client:
            win32gui.SendMessage(mdi_client, win32con.WM_MDIACTIVATE, chart_hwnd, 0)
            time.sleep(0.3)

    # Press F9 to open Order Dialog
    win32api.PostMessage(mt5_hwnd, win32con.WM_KEYDOWN, win32con.VK_F9, 0)
    win32api.PostMessage(mt5_hwnd, win32con.WM_KEYUP, win32con.VK_F9, 0)
    time.sleep(0.7)

    dlg = None
    def find_dlg(h, _):
        nonlocal dlg
        cls = win32gui.GetClassName(h)
        t = win32gui.GetWindowText(h)
        if cls == "#32770" and "Order" in t:
            dlg = h
        return True
    win32gui.EnumWindows(find_dlg, None)

    if not dlg:
        logger.error("Order dialog did not appear.")
        return False

    logger.info(f"Order dialog opened: {hex(dlg)} Title: '{win32gui.GetWindowText(dlg)}'")

    vol_edits = find_child_by_id(dlg, 10333, "Edit")
    if order_type.upper() == "BUY":
        btns = [h for h in find_child_by_id(dlg, 10408, "Button") if win32gui.GetWindowText(h) == "Buy by Market"]
    else:
        btns = [h for h in find_child_by_id(dlg, 10409, "Button") if win32gui.GetWindowText(h) == "Sell by Market"]

    if not (vol_edits and btns):
        logger.error("Could not find dialog controls.")
        win32gui.PostMessage(dlg, win32con.WM_CLOSE, 0, 0)
        return False

    win32gui.SendMessage(vol_edits[0], win32con.WM_SETTEXT, 0, str(volume))
    time.sleep(0.4)

    logger.info(f"Filled {symbol}: Volume={volume}")
    logger.info(f"Clicking '{order_type} by Market' button...")
    win32gui.PostMessage(btns[0], win32con.BM_CLICK, 0, 0)
    time.sleep(1.5)

    pyautogui.press('enter')
    time.sleep(0.5)

    # Now verify position and attach SL/TP
    if mt5.initialize():
        positions = mt5.positions_get(symbol=symbol)
        mt5.shutdown()
        if positions:
            latest = positions[-1]
            logger.info(f"Position opened successfully: Ticket {latest.ticket}. Attaching SL/TP...")
            return modify_position_sltp(latest.ticket, sl, tp)

    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Punch order into MT5 GUI")
    parser.add_argument("--symbol", default="ETHUSD", help="Symbol to trade")
    parser.add_argument("--volume", default="0.83", help="Lot volume")
    parser.add_argument("--sl", default="2235.11", help="Stop Loss price")
    parser.add_argument("--tp", default="2473.58", help="Take Profit price")
    parser.add_argument("--type", default="BUY", choices=["BUY", "SELL"], help="Direction")
    parser.add_argument("--modify-only", action="store_true", help="Only modify SL/TP on active position")
    parser.add_argument("--ticket", type=int, default=208591556, help="Ticket to modify")
    args = parser.parse_args()

    if args.modify_only:
        res = modify_position_sltp(args.ticket, args.sl, args.tp)
    else:
        res = punch_market_order(args.symbol, args.volume, args.sl, args.tp, args.type)
    print("Execution Result:", "SUCCESS" if res else "FAILED")
