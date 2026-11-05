import ctypes
import time
import win32gui
import win32con
import win32api
import pyautogui
import MetaTrader5 as mt5

pyautogui.FAILSAFE = False

print("=== EXECUTING SL & TP MODIFICATION VIA DIRECT CONTROL TARGETING ===")

# Attach to desktop
flags = 0x0001 | 0x0040 | 0x0002 | 0x0008 | 0x0010 | 0x0020
hDesk = ctypes.windll.user32.OpenDesktopW("default", 0, False, flags)
ctypes.windll.user32.SetThreadDesktop(hDesk)

# Find Dialog
dlg = None
def find_dlg(h, _):
    global dlg
    if win32gui.GetClassName(h) == '#32770' and 'Position:' in win32gui.GetWindowText(h):
        dlg = h
    return True
win32gui.EnumWindows(find_dlg, None)

if not dlg:
    print("Dialog not found, opening from Trade tab...")
    # Find MT5
    mt5_hwnd = None
    def cb(h, _):
        global mt5_hwnd
        if '11808165 - FundedNext-Server' in win32gui.GetWindowText(h): mt5_hwnd = h
        return True
    win32gui.EnumWindows(cb, None)
    ctypes.windll.user32.ShowWindow(mt5_hwnd, 9)
    ctypes.windll.user32.SetForegroundWindow(mt5_hwnd)
    time.sleep(0.5)
    # Double click at SL column (865, 855)
    pyautogui.doubleClick(865, 855)
    time.sleep(1.0)
    win32gui.EnumWindows(find_dlg, None)

if not dlg:
    print("ERROR: Could not find or open dialog!")
    exit(1)

print("Found Dialog:", hex(dlg))
ctypes.windll.user32.SetForegroundWindow(dlg)
time.sleep(0.3)

# Find controls by ID
sl_edit = None
tp_edit = None
mod_btn = None
submit_btn = None

def cc(h, _):
    global sl_edit, tp_edit, mod_btn, submit_btn
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
    print("Modify Position mode not active, clicking Modify Position tab (11120)...")
    if mod_btn:
        win32gui.SendMessage(mod_btn, win32con.BM_CLICK, 0, 0)
        time.sleep(0.5)
        win32gui.EnumChildWindows(dlg, cc, None)

print(f"SL Edit: {hex(sl_edit) if sl_edit else 'None'}")
print(f"TP Edit: {hex(tp_edit) if tp_edit else 'None'}")
print(f"Submit Button: {hex(submit_btn) if submit_btn else 'None'}")

if not sl_edit or not tp_edit:
    print("Could not find SL/TP edit fields!")
    exit(1)

sl_rect = win32gui.GetWindowRect(sl_edit)
tp_rect = win32gui.GetWindowRect(tp_edit)

sl_x = (sl_rect[0] + sl_rect[2]) // 2
sl_y = (sl_rect[1] + sl_rect[3]) // 2

tp_x = (tp_rect[0] + tp_rect[2]) // 2
tp_y = (tp_rect[1] + tp_rect[3]) // 2

print(f"Clicking Stop Loss edit box at ({sl_x}, {sl_y})...")
pyautogui.click(sl_x, sl_y)
time.sleep(0.1)
pyautogui.hotkey('ctrl', 'a')
time.sleep(0.1)
pyautogui.write('2235.11', interval=0.04)
time.sleep(0.3)

print(f"Clicking Take Profit edit box at ({tp_x}, {tp_y})...")
pyautogui.click(tp_x, tp_y)
time.sleep(0.1)
pyautogui.hotkey('ctrl', 'a')
time.sleep(0.1)
pyautogui.write('2473.58', interval=0.04)
time.sleep(0.4)

# Check Submit button status
is_en = win32gui.IsWindowEnabled(submit_btn)
print(f"Submit Button Enabled: {is_en}")

sub_rect = win32gui.GetWindowRect(submit_btn)
sub_x = (sub_rect[0] + sub_rect[2]) // 2
sub_y = (sub_rect[1] + sub_rect[3]) // 2

print(f"Clicking Submit Button at ({sub_x}, {sub_y})...")
pyautogui.click(sub_x, sub_y)
time.sleep(1.2)

# If confirmation or OK is needed, press Enter
pyautogui.press('enter')
time.sleep(0.5)

# Verify with MT5
if not mt5.initialize():
    print("MT5 init failed")
    exit(1)

pos = mt5.positions_get(ticket=208591556)
if pos:
    p = pos[0]
    print("\n================ VERIFICATION RESULT ================")
    print(f"Ticket:      {p.ticket}")
    print(f"Symbol:      {p.symbol}")
    print(f"Type:        {'BUY' if p.type == 0 else 'SELL'}")
    print(f"Volume:      {p.volume}")
    print(f"Price Open:  {p.price_open}")
    print(f"Price Curr:  {p.price_current}")
    print(f"Stop Loss:   {p.sl}")
    print(f"Take Profit: {p.tp}")
    print(f"Profit:      ${p.profit:.2f}")
    print("=====================================================")
    if p.sl > 0 and p.tp > 0:
        print(">>> SUCCESS: Stop Loss and Take Profit successfully attached to live order! <<<")
    else:
        print(">>> WARNING: SL or TP still 0, investigating... <<<")

mt5.shutdown()
