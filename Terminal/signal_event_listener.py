"""
Real-time Signal Event Listener (Reactive Antigravity Wakeup Trigger)
====================================================================
Watches Database/realtime_trade_alerts.jsonl for new incoming trade alerts.
Sleeps silently with zero overhead.
The instant a new signal is written by direct_footprint_collector:
- Parses the newly appended alert
- Prints the alert card to stdout
- Exits with code 0 to trigger an INSTANT reactive wakeup in Antigravity chat!
"""

import sys
import threading
import time
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from Terminal.alert_stream import consume_alerts

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
ALERTS_JSONL = DB_DIR / "realtime_trade_alerts.jsonl"
STATE_FILE = DB_DIR / ".signal_listener_state.json"


def get_current_line_count():
    if not ALERTS_JSONL.exists():
        return 0
    try:
        with open(ALERTS_JSONL, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


def main():
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    wake = threading.Event()

    class Handler(FileSystemEventHandler):
        def on_any_event(self, event):
            if event.event_type not in {'modified', 'created', 'moved'}:
                return
            paths = [event.src_path, getattr(event, 'dest_path', '')]
            if any(p and Path(p).resolve() == ALERTS_JSONL.resolve() for p in paths):
                wake.set()

    DB_DIR.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(Handler(), str(DB_DIR), recursive=False)
    observer.start()  # Subscribe before reading the cursor, preventing a lost wakeup.
    try:
        first = True
        while True:
            wake.clear()
            alerts = consume_alerts(ALERTS_JSONL, STATE_FILE, start_at_end=first)
            first = False
            if alerts:
                print(f'=== NEW_SIGNAL_EVENT_DETECTED: {len(alerts)} NEW ALERT(S) ===')
                for alert in alerts:
                    print(json.dumps(alert), flush=True)
                return 0
            wake.wait()
    finally:
        observer.stop()
        observer.join(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
