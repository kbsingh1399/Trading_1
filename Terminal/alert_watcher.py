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

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
ALERTS_JSONL = DB_DIR / "realtime_trade_alerts.jsonl"
STATE_FILE = DB_DIR / ".alert_watcher_state.json"


def check_for_new_actionable_alerts() -> list:
    if not ALERTS_JSONL.exists():
        return []

    last_seen_ts = ""
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            last_seen_ts = state.get("last_seen_ts", "")
        except Exception:
            last_seen_ts = ""

    new_actionable = []
    latest_ts = last_seen_ts

    try:
        with open(ALERTS_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    eval_ts = record.get("evaluated_at_utc", "")
                    if eval_ts > last_seen_ts:
                        if record.get("is_actionable") is True:
                            new_actionable.append(record)
                        if eval_ts > latest_ts:
                            latest_ts = eval_ts
                except Exception:
                    continue
    except Exception as e:
        print(f"Error reading alerts: {e}")
        return []

    if latest_ts and latest_ts != last_seen_ts:
        STATE_FILE.write_text(json.dumps({"last_seen_ts": latest_ts, "updated_at": datetime.now(timezone.utc).isoformat()}))

    return new_actionable


if __name__ == "__main__":
    alerts = check_for_new_actionable_alerts()
    if alerts:
        print(f"FOUND_{len(alerts)}_NEW_ALERTS")
        for a in alerts:
            print(json.dumps(a))
    else:
        print("NO_NEW_ALERTS")
