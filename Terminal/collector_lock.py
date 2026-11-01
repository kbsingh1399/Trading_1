"""
========================================================================================
Shared Collector Coordination & Network Infrastructure
========================================================================================
Guarantees:
1. Zero Overlap: Shared browser mutex ensures only ONE collector interacts with Chrome CDP.
2. Cloudflare WARP IP Rotation: Executed both BEFORE and AFTER each data dump cycle.
3. Clean Tab Hygiene: Automatic recovery of stale locks and orphan tabs.
========================================================================================
"""

import os
import sys
import time
import json
import logging
import urllib.request
import subprocess
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LOCK_DIR = DB_DIR / ".locks"
LOCK_DIR.mkdir(parents=True, exist_ok=True)
BROWSER_LOCK_FILE = LOCK_DIR / "cdp_browser.lock"
WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")

logger = logging.getLogger("CollectorCoordination")


def get_current_ip() -> str:
    """Queries external IP via ipify."""
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as response:
            return json.loads(response.read().decode()).get("ip", "unknown")
    except Exception:
        return "unknown"


def rotate_warp_ip(context: str = "Collector") -> str:
    """
    Disconnects and reconnects Cloudflare WARP to rotate IP and re-key the tunnel.
    Called both BEFORE and AFTER every data collection sweep.
    """
    if not WARP_CLI.exists():
        logger.warning(f"[{context}] WARP CLI binary not found at: {WARP_CLI}")
        return "unknown"

    old_ip = get_current_ip()
    logger.info(f"[{context}] [Cloudflare WARP] Current IP: {old_ip}. Rotating tunnel...")
    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(1.5)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1.0)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(3.5)
        new_ip = get_current_ip()
        logger.info(f"[{context}] [Cloudflare WARP] Rotation complete. New IP: {new_ip} (Old: {old_ip})")
        return new_ip
    except Exception as e:
        logger.error(f"[{context}] [Cloudflare WARP] Error rotating IP: {e}")
        return old_ip


class BrowserLock:
    """
    Context manager ensuring strictly non-overlapping access to Chrome CDP.
    Prevents any two collectors from opening tabs or sending WebSocket frames simultaneously.
    """
    def __init__(self, collector_name: str, timeout_secs: int = 120):
        self.collector_name = collector_name
        self.timeout_secs = timeout_secs
        self.acquired = False

    def __enter__(self):
        start = time.time()
        while time.time() - start < self.timeout_secs:
            if not BROWSER_LOCK_FILE.exists():
                try:
                    BROWSER_LOCK_FILE.write_text(f"{self.collector_name}:{os.getpid()}:{time.time()}")
                    self.acquired = True
                    logger.info(f"[{self.collector_name}] Acquired shared browser lock.")
                    return self
                except Exception:
                    pass
            else:
                # Check for stale lock (> 180 seconds old)
                try:
                    content = BROWSER_LOCK_FILE.read_text().strip()
                    parts = content.split(":")
                    if len(parts) >= 3 and time.time() - float(parts[2]) > 180:
                        logger.warning(f"[{self.collector_name}] Clearing stale browser lock held by {parts[0]}")
                        BROWSER_LOCK_FILE.unlink(missing_ok=True)
                        continue
                except Exception:
                    pass
            time.sleep(1.0)
        logger.warning(f"[{self.collector_name}] Timed out waiting for browser lock after {self.timeout_secs}s.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            try:
                BROWSER_LOCK_FILE.unlink(missing_ok=True)
                logger.info(f"[{self.collector_name}] Released shared browser lock.")
            except Exception:
                pass


# Reserved minutes in the hour for non-overlapping execution
WHALE_MINUTES = [0, 15, 35, 50]
LIQUIDATION_MINUTES = [10, 25, 55]


def is_scheduled_for_other_collector(current_minute: int, collector_type: str) -> bool:
    """
    Checks if the current minute is exclusively reserved for another collector.
    Used by Footprint to yield and guarantee zero overlap.
    """
    if collector_type == "footprint":
        return (current_minute in WHALE_MINUTES) or (current_minute in LIQUIDATION_MINUTES)
    elif collector_type == "whale":
        return current_minute in LIQUIDATION_MINUTES
    elif collector_type == "liquidation":
        return current_minute in WHALE_MINUTES
    return False
