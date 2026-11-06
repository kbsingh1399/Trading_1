import os, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

now_ts = time.time()
ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
print(f"=== REAL-TIME DATA FRESHNESS AUDIT AT {ist_now.strftime('%Y-%m-%d %H:%M:%S IST')} ===\n")

db = Path("Database")
subdirs = [
    "Auction_Levels",
    "Whale_Orders/Open_Orders",
    "Liquidation_Data/Key_Levels",
    "Liquidation_Data/Near_Field_Balance",
    "TV_Liquidation_Spikes",
    "Signals/Signal_Queues",
    "Signals/Trade_Setups",
    ".locks"
]

for sd in subdirs:
    p = db / sd
    if not p.exists():
        print(f"[-] {sd}: DIRECTORY NOT FOUND")
        continue
    files = [f for f in p.iterdir() if f.is_file()]
    if not files:
        print(f"[-] {sd}: EMPTY DIRECTORY")
        continue
    
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    freshest = files[0]
    mtime = freshest.stat().st_mtime
    age_sec = now_ts - mtime
    age_min = age_sec / 60.0
    
    status = "[FRESH <15m]" if age_min <= 15.0 else ("[RECENT <60m]" if age_min <= 60.0 else "[STALE >60m]")
    print(f"{status} {sd}:")
    print(f"    Total files: {len(files)}")
    print(f"    Freshest file: {freshest.name}")
    print(f"    Last Modified: {datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')} ({age_min:.1f} min ago)")
    sample = [f"{f.name} ({int((now_ts - f.stat().st_mtime)/60)}m ago)" for f in files[:3]]
    print(f"    Top samples: {', '.join(sample)}\n")
