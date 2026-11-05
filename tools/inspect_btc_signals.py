import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import urllib.request
import json
import os
import copy
from datetime import datetime, timezone, timedelta
from Terminal.universal_footprint_engine import (
    UniversalFootprintEngine,
    FootprintCalibration,
    synthesize_volume_bars,
    synthesize_range_bars
)

def main():
    print("Fetching 1m klines from Binance Futures...")
    url_1m = "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=1000"
    req = urllib.request.Request(url_1m, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw_1m = json.loads(resp.read().decode("utf-8"))

    sub_bars = []
    for k in raw_1m:
        tot_vol = float(k[5])
        buy_vol = float(k[9])
        sub_bars.append({
            "open_time_ms": int(k[0]),
            "close_time_ms": int(k[6]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "total_volume": tot_vol,
            "buy_volume": buy_vol,
            "sell_volume": max(tot_vol - buy_vol, 0.0),
            "total_trades": float(k[8])
        })

    vol_bars = synthesize_volume_bars(sub_bars, vol_threshold=1000.0, symbol="BTC", bar_label="Volume 1.0K BTC")
    range_bars = synthesize_range_bars(sub_bars, range_size=250.0, symbol="BTC", bar_label="Range 250")

    print("Fetching 15m klines from Binance Futures...")
    url_15m = "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=15m&limit=300"
    req_15 = urllib.request.Request(url_15m, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req_15, timeout=10) as resp:
        raw_15m = json.loads(resp.read().decode("utf-8"))

    bars_15m = []
    for k in raw_15m:
        tot_vol = float(k[5])
        buy_vol = float(k[9])
        bars_15m.append({
            "open_time_ms": int(k[0]),
            "close_time_ms": int(k[6]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "total_volume": tot_vol,
            "buy_volume": buy_vol,
            "sell_volume": max(tot_vol - buy_vol, 0.0),
            "total_trades": float(k[8])
        })

    # Test with standard calibration
    cal = FootprintCalibration()
    engine = UniversalFootprintEngine(calibration=cal)
    
    df_vol, sigs_vol = engine.analyze_bars(vol_bars, symbol="BTC", asset_class="Crypto", bar_type="vol", bar_label="Volume 1.0K BTC", rolling_window=len(vol_bars), inspect_last_n_bars=len(vol_bars))
    df_range, sigs_range = engine.analyze_bars(range_bars, symbol="BTC", asset_class="Crypto", bar_type="range", bar_label="Range 250", rolling_window=len(range_bars), inspect_last_n_bars=len(range_bars))
    df_15, sigs_15 = engine.analyze_bars(bars_15m, symbol="BTC", asset_class="Crypto", bar_type="time", bar_label="15m", rolling_window=len(bars_15m), inspect_last_n_bars=len(bars_15m))

    print(f"\n--- STANDARD CALIBRATION ---")
    print(f"15m: {len(sigs_15)}, Volume 1.0K: {len(sigs_vol)}, Range 250: {len(sigs_range)}, Total: {len(sigs_15) + len(sigs_vol) + len(sigs_range)}")

    ist_tz = timezone(timedelta(hours=5, minutes=30))
    all_sigs = sorted(sigs_vol + sigs_range + sigs_15, key=lambda s: s["time_ms"])
    for s in all_sigs[-15:]:
        t_ist = datetime.fromtimestamp(s["time_ms"] / 1000, tz=ist_tz).strftime("%m-%d %H:%M")
        bl = s.get("bar_label", "")
        sn = s.get("sig_name", "")
        sd = s.get("side", "")
        pr = s.get("price", 0.0)
        desc = s.get("title", "")
        print(f"  {t_ist} | {bl:<15} | {sn:<12} | {sd:<7} | {pr:>9.1f} | {desc}")

if __name__ == "__main__":
    main()
