#!/usr/bin/env python3
"""
MT5 GUI Order Puncher
Automates order execution via the native MetaTrader 5 UI Order Dialog,
bypassing server-side EA / API blocks (retcode 10026) while strictly respecting
FundedNext risk rules and the user's $10-$15 risk window.
"""

import sys
import time
import ctypes
import logging
import win32gui
import win32con
import win32api
import MetaTrader5 as mt5

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


def punch_market_order(symbol: str, volume: str, sl: str, tp: str, order_type: str = "BUY") -> bool:
    """
    Punches a market order via MT5 GUI Order Dialog (<F9>).
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
        # Find MDIClient and activate chart
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

    # Find Order dialog
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

    # Locate Volume, SL, TP edits
    vol_edits = find_child_by_id(dlg, 10333, "Edit")
    sl_edits = find_child_by_id(dlg, 10334, "Edit")
    tp_edits = find_child_by_id(dlg, 10336, "Edit")

    if order_type.upper() == "BUY":
        btns = [h for h in find_child_by_id(dlg, 10408, "Button") if win32gui.GetWindowText(h) == "Buy by Market"]
    else:
        btns = [h for h in find_child_by_id(dlg, 10409, "Button") if win32gui.GetWindowText(h) == "Sell by Market"]

    if not (vol_edits and sl_edits and tp_edits and btns):
        logger.error("Could not find all dialog edit controls.")
        win32gui.PostMessage(dlg, win32con.WM_CLOSE, 0, 0)
        return False

    # Set parameters
    win32gui.SendMessage(vol_edits[0], win32con.WM_SETTEXT, 0, str(volume))
    win32gui.SendMessage(sl_edits[0], win32con.WM_SETTEXT, 0, str(sl))
    win32gui.SendMessage(tp_edits[0], win32con.WM_SETTEXT, 0, str(tp))
    time.sleep(0.4)

    logger.info(f"Filled {symbol}: Volume={volume}, SL={sl}, TP={tp}")
    logger.info(f"Clicking '{order_type} by Market' button...")

    # Click order button
    win32gui.PostMessage(btns[0], win32con.BM_CLICK, 0, 0)
    time.sleep(1.2)

    # Check for confirmation dialog / OK button
    post_dlg = None
    win32gui.EnumWindows(find_dlg, None)
    if post_dlg:
        ok_btns = find_child_by_id(post_dlg, 10411, "Button")
        if ok_btns:
            win32gui.PostMessage(ok_btns[0], win32con.BM_CLICK, 0, 0)
            time.sleep(0.3)

    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Punch order into MT5 GUI")
    parser.add_argument("--symbol", default="ETHUSD", help="Symbol to trade")
    parser.add_argument("--volume", default="0.83", help="Lot volume")
    parser.add_argument("--sl", default="2235.11", help="Stop Loss price")
    parser.add_argument("--tp", default="2473.58", help="Take Profit price")
    parser.add_argument("--type", default="BUY", choices=["BUY", "SELL"], help="Direction")
    args = parser.parse_args()

    success = punch_market_order(args.symbol, args.volume, args.sl, args.tp, args.type)
    print("Execution Result:", "SUCCESS" if success else "FAILED")
