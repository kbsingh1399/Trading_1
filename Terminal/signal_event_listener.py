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
import time
import json
from pathlib import Path

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
    initial_count = get_current_line_count()
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            initial_count = max(initial_count, state.get("last_line_count", initial_count))
        except Exception:
            pass

    STATE_FILE.write_text(json.dumps({"last_line_count": initial_count}))
    # Wait until a new line is appended
    while True:
        time.sleep(1.0)
        current_count = get_current_line_count()
        if current_count > initial_count:
            # New signal detected! Read new lines
            new_lines = []
            try:
                with open(ALERTS_JSONL, "r", encoding="utf-8") as f:
                    all_lines = [l.strip() for l in f if l.strip()]
                    new_lines = all_lines[initial_count:]
            except Exception as e:
                print(f"Error reading new lines: {e}")
                time.sleep(1.0)
                continue

            STATE_FILE.write_text(json.dumps({"last_line_count": current_count}))
            print(f"=== NEW_SIGNAL_EVENT_DETECTED: {len(new_lines)} NEW ALERT(S) ===")
            for line in new_lines:
                print(line)
            # Exit to trigger instant reactive Antigravity wakeup
            sys.exit(0)


if __name__ == "__main__":
    main()
