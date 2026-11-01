"""
Real-time Alert Watcher & Notifier
==================================
Reads Database/realtime_trade_alerts.jsonl and extracts newly arrived actionable trade alerts.
Maintains state in Database/.alert_watcher_state.json to prevent duplicate notifications.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from Terminal.alert_stream import consume_alerts

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
ALERTS_JSONL = DB_DIR / "realtime_trade_alerts.jsonl"
STATE_FILE = DB_DIR / ".alert_watcher_state.json"


def check_for_new_actionable_alerts() -> list:
    return consume_alerts(ALERTS_JSONL, STATE_FILE)


if __name__ == "__main__":
    alerts = check_for_new_actionable_alerts()
    if alerts:
        print(f"FOUND_{len(alerts)}_NEW_ALERTS")
        for a in alerts:
            print(json.dumps(a))
    else:
        print("NO_NEW_ALERTS")
