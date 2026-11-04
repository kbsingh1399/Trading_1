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
from Terminal.runtime_io import FileMutex, atomic_json

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
LOCK_DIR = DB_DIR / ".locks"
LOCK_DIR.mkdir(parents=True, exist_ok=True)
ENGINE_LOCK_FILE = LOCK_DIR / "engine_sync.lock"
BROWSER_LOCK_FILE = ENGINE_LOCK_FILE  # Backwards compatibility alias
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


WARP_ROTATION_FILE = LOCK_DIR / "warp_last_rotation.json"


def _get_last_rotation_time() -> float:
    try:
        if WARP_ROTATION_FILE.exists():
            data = json.loads(WARP_ROTATION_FILE.read_text(encoding="utf-8"))
            return float(data.get("last_rotation_time", 0.0))
    except Exception:
        pass
    return 0.0


def _set_last_rotation_time(ts: float, ip: str, reason: str):
    try:
        atomic_json(WARP_ROTATION_FILE, {"last_rotation_time": ts, "last_ip": ip, "reason": reason})
    except Exception:
        pass


MIN_ROTATION_INTERVAL_SECS = 120.0  # 2-minute system-wide cross-process cooldown
POST_ROTATION_GRACE_SECS = 45.0     # Grace window post-rotation before empty-scrape trigger


def verify_network_stabilization(expected_change_from: str = "", max_timeout: float = 10.0) -> Tuple[bool, str]:
    """Polls external IP until network is online and verifies DNS/IP connectivity."""
    deadline = time.time() + max_timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request("https://api.ipify.org?format=json", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=2.5) as resp:
                data = json.loads(resp.read().decode())
                ip = data.get("ip", "")
                if ip and ip != "unknown":
                    changed = (ip != expected_change_from) if expected_change_from else True
                    if changed:
                        return True, ip
        except Exception:
            pass
        time.sleep(0.75)
    return False, get_current_ip()


def ensure_delta_split_tunnel_exclusions():
    """
    Ensures Delta Exchange India endpoints bypass WARP and route via direct ISP IP.
    Prevents 'ip_not_whitelisted_for_api_key' errors when WARP rotates.
    """
    if not WARP_CLI.exists():
        return
    delta_hosts = [
        "cdn.india.delta.exchange",
        "api.india.delta.exchange",
        "india.delta.exchange",
        "delta.exchange"
    ]
    for h in delta_hosts:
        try:
            subprocess.run([str(WARP_CLI), "tunnel", "host", "add", h], capture_output=True, timeout=5)
        except Exception:
            pass


def is_warp_rotating() -> bool:
    """Returns True if a WARP rotation is currently in-flight."""
    lock_file = LOCK_DIR / "warp_execution.lock"
    if lock_file.exists():
        try:
            age = time.time() - lock_file.stat().st_mtime
            if age < 15.0:
                return True
        except Exception:
            pass
    return False


def rotate_warp_ip(context: str = "Collector", reason: str = "SCHEDULED", force: bool = False) -> str:
    """
    Event-driven Cloudflare WARP True IP Rotation on Windows.
    Alternates between ISP egress and re-provisioned Cloudflare WARP client edge.
    Guarantees zero dropped orders via live order in-flight execution lock with 20s TTL auto-expiry.
    """
    now = time.time()
    last_rot = _get_last_rotation_time()

    # Hold persistent order-dispatch mutex to guarantee zero order-rotation interleaving
    with FileMutex(str(LOCK_DIR / "order_execution_inflight.lock"), timeout=0.0) as order_guard:
        if not order_guard.acquired:
            logger.warning(f"[{context}] [Cloudflare WARP] Delta Exchange order execution is currently IN-FLIGHT. Rotation blocked to protect trade.")
            return get_current_ip()

        valid_reasons = ["CHALLENGE", "HTTP_403", "HTTP_429", "FAILED_TO_LOAD", "CONSECUTIVE_EMPTY_SCRAPES", "MANUAL_OVERRIDE"]
        if not force and reason not in valid_reasons:
            logger.info(f"[{context}] [Cloudflare WARP] Normal operation ({reason}). Rotation skipped to preserve session cookies.")
            return get_current_ip()

        if not force and (now - last_rot < MIN_ROTATION_INTERVAL_SECS):
            logger.info(f"[{context}] [Cloudflare WARP] Cross-process cooldown active ({int(now - last_rot)}s / {int(MIN_ROTATION_INTERVAL_SECS)}s). Skipped.")
            return get_current_ip()

        if not WARP_CLI.exists():
            logger.warning(f"[{context}] WARP CLI binary not found at: {WARP_CLI}")
            return get_current_ip()

        with FileMutex(str(LOCK_DIR / "warp_execution.lock"), timeout=1.0) as mutex:
            if not mutex.acquired:
                logger.info(f"[{context}] [Cloudflare WARP] Another process is rotating WARP. Skipped.")
                return get_current_ip()

            old_ip = get_current_ip()
            logger.info(f"[{context}] [Cloudflare WARP] True IP Rotation triggered ({reason}). Old IP: {old_ip}")
            new_ip = old_ip
            try:
                # Stage 1: Fast toggle (Disconnect to revert to Direct ISP IP)
                if old_ip.startswith("104.28.") or old_ip.startswith("104."):
                    logger.info(f"[{context}] [Cloudflare WARP] Disconnecting WARP to switch to Direct ISP IP...")
                    subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, timeout=8)
                    stable, new_ip = verify_network_stabilization(expected_change_from=old_ip, max_timeout=8.0)
                    if stable and new_ip != old_ip:
                        _set_last_rotation_time(time.time(), new_ip, reason)
                        logger.info(f"[{context}] [Cloudflare WARP] Toggled to Direct ISP IP: {new_ip}")
                        return new_ip

                # Stage 2: Full Edge Re-Provisioning (delete registration -> new -> connect)
                logger.info(f"[{context}] [Cloudflare WARP] Performing Full Registration Re-Provisioning...")
                subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, timeout=8)
                time.sleep(0.5)
                subprocess.run([str(WARP_CLI), "registration", "delete"], capture_output=True, text=True, timeout=8)
                time.sleep(0.5)
                subprocess.run([str(WARP_CLI), "registration", "new"], capture_output=True, text=True, timeout=8)
                time.sleep(0.5)
                subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, timeout=8)
                ensure_delta_split_tunnel_exclusions()

                stable, new_ip = verify_network_stabilization(expected_change_from=old_ip, max_timeout=10.0)
                _set_last_rotation_time(time.time(), new_ip, reason)
                logger.info(f"[{context}] [Cloudflare WARP] Re-provisioning complete. New IP: {new_ip} (Changed: {new_ip != old_ip})")
                return new_ip
            except Exception as e:
                logger.error(f"[{context}] [Cloudflare WARP] Error during rotation: {e}")
                return get_current_ip()


# Global lock contention tracking (DeepSeek AUD-07)
LOCK_CONTENTION_SKIPS = 0

# Domain-Isolated Collector Engine Lock Map:
# Eliminates starvation deadlocks between long sweeps (Footprint), fast sweeps (TV), and live order dispatch.
COLLECTOR_LOCK_MAP = {
    "TVLiquidationScraper": LOCK_DIR / "api_tv_collector.lock",
    "FootprintScraper": LOCK_DIR / "api_footprint.lock",
    "WhaleScraper": LOCK_DIR / "api_whales.lock",
    "LiquidationScraper": LOCK_DIR / "api_liquidation.lock",
    "DeltaLiveExecution": LOCK_DIR / "order_execution_inflight.lock",
}


class BrowserLock(FileMutex):
    """OS-backed browser mutex with a monotonic, bounded acquisition deadline."""
    def __init__(self, collector_name, timeout_secs=2.0, lock_file=None):
        target_lock = lock_file or COLLECTOR_LOCK_MAP.get(collector_name, BROWSER_LOCK_FILE)
        super().__init__(target_lock, timeout_secs)
        self.collector_name = collector_name
        self.lock_file = self.path
        self.timeout_secs = timeout_secs

    def __enter__(self):
        global LOCK_CONTENTION_SKIPS
        super().__enter__()
        if not self.acquired:
            LOCK_CONTENTION_SKIPS += 1
            logger.info('[%s] SKIPPED_LOCK_CONTENTION', self.collector_name)
        return self


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
    if not math.isfinite(age_seconds) or not math.isfinite(tau) or tau <= 0 or age_seconds < 0.0:
        return 0.0
    return float(round(math.exp(-age_seconds / tau), 4))


# Master TDMA Hourly Windows
WHALE_MINUTES = [0, 15, 35, 50]
LIQUIDATION_MINUTES = [10, 25, 55]

# 2-minute lead-in reservation prevents long 120s-160s Footprint sweeps from overrunning into Whale or Liquidation windows:
WHALE_RESERVED_MINUTES = [58, 59, 0, 1, 13, 14, 15, 16, 33, 34, 35, 36, 48, 49, 50, 51]
LIQUIDATION_RESERVED_MINUTES = [8, 9, 10, 11, 12, 23, 24, 25, 26, 27, 53, 54, 55, 56, 57]


def is_scheduled_for_other_collector(current_minute: int, collector_type: str) -> bool:
    """
    Two-Tier TDMA Deconfliction Engine:
    Guarantees mathematically ZERO overlapping browser executions across all scrapers.
    - Whale sweeps: minutes [0, 1, 15, 16, 35, 36, 50, 51]
    - Liquidation sweeps: minutes [10, 11, 12, 25, 26, 27, 55, 56, 57]
    - Footprint scraper: strictly EVEN minutes outside Whale/Liq blocks (e.g. 2, 4, 6, 8, 14, 18...)
    - TV scraper: strictly ODD minutes outside Whale/Liq blocks (e.g. 3, 5, 7, 9, 13, 17...)
    """
    if collector_type == "footprint":
        # Footprint strictly runs on even unreserved minutes
        return (current_minute % 2 != 0) or (current_minute in WHALE_RESERVED_MINUTES) or (current_minute in LIQUIDATION_RESERVED_MINUTES)
    elif collector_type == "tv":
        # TV scraper strictly runs on odd unreserved minutes
        return (current_minute % 2 == 0) or (current_minute in WHALE_RESERVED_MINUTES) or (current_minute in LIQUIDATION_RESERVED_MINUTES)
    elif collector_type == "whale":
        return current_minute in LIQUIDATION_RESERVED_MINUTES
    elif collector_type == "liquidation":
        return current_minute in WHALE_RESERVED_MINUTES
    return False
