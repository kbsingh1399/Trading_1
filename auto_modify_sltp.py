import sys
import time
import ctypes
import win32gui
import win32con
import win32api
import pyautogui
import MetaTrader5 as mt5

sys.stdout.reconfigure(encoding='utf-8')
pyautogui.FAILSAFE = False

print("=== AUTOMATIC STOP LOSS & TAKE PROFIT ATTACHER ===")

# Attach to desktop
flags = 0x0001 | 0x0040 | 0x0002 | 0x0008 | 0x0010 | 0x0020
hDesk = ctypes.windll.user32.OpenDesktopW("default", 0, False, flags)
ctypes.windll.user32.SetThreadDesktop(hDesk)

# Find MT5
mt5_hwnd = None
def cb(h, _):
    global mt5_hwnd
    t = win32gui.GetWindowText(h)
    if '11808165 - FundedNext-Server' in t: mt5_hwnd = h
    return True
win32gui.EnumWindows(cb, None)

if not mt5_hwnd:
    print("MT5 window not found!")
    sys.exit(1)

print(f"MT5 Window: {hex(mt5_hwnd)}")

# Bring MT5 to foreground
ctypes.windll.user32.ShowWindow(mt5_hwnd, 9) # SW_RESTORE
ctypes.windll.user32.SetForegroundWindow(mt5_hwnd)
time.sleep(0.5)

# Find Trade ListView
trade_lv = None
def c_cb(h, _):
    global trade_lv
    if win32gui.GetClassName(h) == "SysListView32":
        r = win32gui.GetWindowRect(h)
        cnt = win32gui.SendMessage(h, 4100, 0, 0)
        if r[1] > 600 and cnt >= 1:
            trade_lv = h
    return True
win32gui.EnumChildWindows(mt5_hwnd, c_cb, None)

print(f"Trade ListView: {hex(trade_lv) if trade_lv else 'None'}")
if not trade_lv:
    print("Trade ListView not found!")
    sys.exit(1)

lv_rect = win32gui.GetWindowRect(trade_lv)
print("ListView Rect:", lv_rect)

# Position row 0 center
row0_x = lv_rect[0] + 250
row0_y = lv_rect[1] + 32

print(f"Right-clicking position at ({row0_x}, {row0_y})...")
pyautogui.click(row0_x, row0_y, button='right')
time.sleep(0.5)

# Press 'm' to select 'Modify or Delete'
print("Pressing 'm' for Modify or Delete...")
pyautogui.press('m')
time.sleep(0.8)

# Find Modify Dialog
dlg = None
def find_dlg(h, _):
    global dlg
    cls = win32gui.GetClassName(h)
    t = win32gui.GetWindowText(h)
    if cls == "#32770" and ("Position:" in t or "Modify" in t or "Order" in t):
        dlg = h
    return True
win32gui.EnumWindows(find_dlg, None)

print(f"Modify Dialog: {hex(dlg) if dlg else 'None'}")
if not dlg:
    print("Modify dialog did not open, retrying...")
    sys.exit(1)

dlg_rect = win32gui.GetWindowRect(dlg)
print("Dialog Title:", win32gui.GetWindowText(dlg))
print("Dialog Rect:", dlg_rect)

# Check if 'Modify Position' button needs clicking (ID 11120)
def find_child(p, cid):
    res = []
    def cc(h, _):
        if win32api.GetWindowLong(h, win32con.GWL_ID) == cid: res.append(h)
        return True
    win32gui.EnumChildWindows(p, cc, None)
    return res

mod_btns = find_child(dlg, 11120)
if mod_btns:
    print("Clicking 'Modify Position' type button...")
    win32gui.PostMessage(mod_btns[0], win32con.BM_CLICK, 0, 0)
    time.sleep(0.4)

# Locate Stop Loss & Take Profit edit controls in dialog
# From our mapping:
# Stop Loss edit is at roughly x = dlg_rect[0] + 320, y = dlg_rect[1] + 85
# Take Profit edit is at roughly x = dlg_rect[0] + 320, y = dlg_rect[1] + 115
sl_x = dlg_rect[0] + 320
sl_y = dlg_rect[1] + 88
tp_x = dlg_rect[0] + 320
tp_y = dlg_rect[1] + 116

print(f"Clicking Stop Loss field at ({sl_x}, {sl_y})...")
pyautogui.doubleClick(sl_x, sl_y)
time.sleep(0.2)
pyautogui.hotkey('ctrl', 'a')
time.sleep(0.1)
pyautogui.write('2235.11', interval=0.03)
time.sleep(0.3)

print(f"Clicking Take Profit field at ({tp_x}, {tp_y})...")
pyautogui.doubleClick(tp_x, tp_y)
time.sleep(0.2)
pyautogui.hotkey('ctrl', 'a')
time.sleep(0.1)
pyautogui.write('2473.58', interval=0.03)
time.sleep(0.3)

# Find the big blue Modify button (ID 10351)
big_btn = find_child(dlg, 10351)
if big_btn:
    btn_hwnd = big_btn[0]
    btn_rect = win32gui.GetWindowRect(btn_hwnd)
    btn_x = (btn_rect[0] + btn_rect[2]) // 2
    btn_y = (btn_rect[1] + btn_rect[3]) // 2
    print(f"Clicking Modify button at ({btn_x}, {btn_y}) HWND: {hex(btn_hwnd)} Enabled: {win32gui.IsWindowEnabled(btn_hwnd)}...")
    pyautogui.click(btn_x, btn_y)
    time.sleep(1.0)
    # If OK confirmation appears
    pyautogui.press('enter')
    time.sleep(0.5)

# Verify position in MT5
mt5.initialize()
pos = mt5.positions_get(ticket=208591556)
if pos:
    p = pos[0]
    print("\n=== UPDATED POSITION STATUS ===")
    print(f"Ticket: {p.ticket}")
    print(f"Symbol: {p.symbol}")
    print(f"SL:     {p.sl}")
    print(f"TP:     {p.tp}")
    if p.sl > 0:
        print("SUCCESS! SL and TP are now active on broker server!")
    else:
        print("SL is still 0.0, will check coordinates.")
mt5.shutdown()
