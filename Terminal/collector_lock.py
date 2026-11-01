"""
========================================================================================
Shared Collector Coordination, Event-Driven Network & Structural Decay Infrastructure
========================================================================================
Guarantees (Arena Quant Audit Round 2 Compliant):
1. Deadlock-Free Browser Mutex: Fast 2.0s acquire timeout. Skips on contention
   (SKIPPED_LOCK_CONTENTION) rather than creating stale latency queues.
2. Event-Driven WARP Rotation: Rotates ONLY on detection signals (403/429/Challenge/Empty)
   rather than blind scheduled churn, preserving session cookies and socket streams.
3. Structural Wall Exponential Decay: Continuous age decay exp(-age/tau) with tau=1800s (30m)
   replacing the fragile 30-second binary gate for 15-minute whale book cadences.
========================================================================================
"""

import os
import sys
import time
import json
import math
import logging
import urllib.request
import subprocess
from pathlib import Path
from typing import Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LOCK_DIR = DB_DIR / ".locks"
LOCK_DIR.mkdir(parents=True, exist_ok=True)
BROWSER_LOCK_FILE = LOCK_DIR / "cdp_browser.lock"
WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [Coordination] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("CollectorCoordination")
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(handler)


def get_current_ip() -> str:
    """Queries external IP via ipify."""
    try:
        req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=4) as response:
            return json.loads(response.read().decode()).get("ip", "unknown")
    except Exception:
        return "unknown"


_LAST_ROTATION_TIME = 0.0
MIN_ROTATION_INTERVAL_SECS = 180.0  # 3-minute cooldown between full tunnel re-keys
POST_ROTATION_GRACE_SECS = 45.0     # Qwen F-16: Grace window post-rotation before empty-scrape trigger can re-arm


def rotate_warp_ip(context: str = "Collector", reason: str = "SCHEDULED", force: bool = False) -> str:
    """
    Event-driven Cloudflare WARP IP rotation.
    Rotates tunnel keys and re-connects only when a detection signal occurs (403, 429, challenge)
    or when forced. Protected by a 3-minute cooldown and a post-rotation grace window.
    """
    global _LAST_ROTATION_TIME
    now = time.time()

    valid_reasons = ["CHALLENGE", "HTTP_403", "HTTP_429", "CONSECUTIVE_EMPTY_SCRAPES", "MANUAL_OVERRIDE"]
    if not force and reason not in valid_reasons:
        logger.info(f"[{context}] [Cloudflare WARP] Normal operation ({reason}). Rotation skipped to preserve session cookies.")
        return get_current_ip()

    if not force and (now - _LAST_ROTATION_TIME < MIN_ROTATION_INTERVAL_SECS):
        logger.info(f"[{context}] [Cloudflare WARP] Cooldown active ({int(now - _LAST_ROTATION_TIME)}s / {int(MIN_ROTATION_INTERVAL_SECS)}s). Skipped.")
        return get_current_ip()

    # Qwen F-16: Grace window prevents newly rotated IP cookie refresh from re-triggering CONSECUTIVE_EMPTY_SCRAPES
    if reason == "CONSECUTIVE_EMPTY_SCRAPES" and (now - _LAST_ROTATION_TIME < (MIN_ROTATION_INTERVAL_SECS + POST_ROTATION_GRACE_SECS)):
        logger.info(f"[{context}] [Cloudflare WARP] Post-rotation grace active. Skipped empty-scrape re-trigger.")
        return get_current_ip()

    if not WARP_CLI.exists():
        logger.warning(f"[{context}] WARP CLI binary not found at: {WARP_CLI}")
        return "unknown"

    old_ip = get_current_ip()
    logger.info(f"[{context}] [Cloudflare WARP] Rotation triggered ({reason}). Old IP: {old_ip}")
    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(1.0)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1.0)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(3.0)
        new_ip = get_current_ip()
        _LAST_ROTATION_TIME = time.time()
        logger.info(f"[{context}] [Cloudflare WARP] Rotation complete. New IP: {new_ip} (Changed: {new_ip != old_ip})")
        return new_ip
    except Exception as e:
        logger.error(f"[{context}] [Cloudflare WARP] Error rotating IP: {e}")
        return old_ip


# Global lock contention tracking (DeepSeek AUD-07)
LOCK_CONTENTION_SKIPS = 0


class BrowserLock:
    """
    Fast, deadline-enforcing browser mutex.
    Acquires CDP lock with a 2.0s deadline. If contested, sets acquired=False
    so callers can cleanly SKIP rather than blocking in a stale latency queue.
    """
    def __init__(self, collector_name: str, timeout_secs: float = 2.0):
        self.collector_name = collector_name
        self.timeout_secs = timeout_secs
        self.acquired = False

    def __enter__(self):
        global LOCK_CONTENTION_SKIPS
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
            time.sleep(0.2)

        LOCK_CONTENTION_SKIPS += 1
        logger.info(f"[{self.collector_name}] SKIPPED_LOCK_CONTENTION (total skips: {LOCK_CONTENTION_SKIPS}): Browser busy. Yielding immediately without blocking.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            try:
                BROWSER_LOCK_FILE.unlink(missing_ok=True)
                logger.info(f"[{self.collector_name}] Released shared browser lock.")
            except Exception:
                pass


# Canonical 30-minute half-life constant (Qwen F-05, DeepSeek AUD-05)
# Half-life t_half = tau * ln(2) = 1800 s => tau = 1800 / ln(2) ~= 2596.8437 s
HALF_LIFE_SECONDS = 1800.0
TAU_30MIN_HALFLIFE = HALF_LIFE_SECONDS / math.log(2)  # ~2596.8437 s


def calculate_wall_decay_weight(age_seconds: float, tau: float = TAU_30MIN_HALFLIFE) -> float:
    """
    Continuous Bayesian Prior Age Decay for Order Book Walls.
    weight = exp(-age / tau)
    With tau = 1800 / ln(2) (~2596.84s), achieves an exact 30-minute half-life:
    - At age 0s: weight = 1.00
    - At age 15m (900s): weight = 0.7071
    - At age 30m (1800s): weight = 0.5000 (Exact 30-minute half-life)
    - At age 60m (3600s): weight = 0.2500 (Two half-lives)
    Eliminates binary 30s cut-off mismatches on 15m collection cadences.
    """
    if age_seconds < 0.0:
        return 0.0
    return float(round(math.exp(-age_seconds / tau), 4))


# Reserved minutes in the hour for non-overlapping execution
WHALE_MINUTES = [0, 15, 35, 50]
LIQUIDATION_MINUTES = [10, 25, 55]


def is_scheduled_for_other_collector(current_minute: int, collector_type: str) -> bool:
    """Checks if current minute is exclusively reserved for another collector."""
    if collector_type == "footprint":
        return (current_minute in WHALE_MINUTES) or (current_minute in LIQUIDATION_MINUTES)
    elif collector_type == "whale":
        return current_minute in LIQUIDATION_MINUTES
    elif collector_type == "liquidation":
        return current_minute in WHALE_MINUTES
    return False
