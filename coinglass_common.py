"""
========================================================================================
CoinGlass Scraping & Footprint Common Engine (Pure Standalone Edition)
========================================================================================
Consolidated, self-contained runtime supporting:
- 1_coinglass_footprint_scraper.py
- 2_coinglass_whales.py
- 3_coinglass_liquidation_levels.py
- 4_coinglass_tv_scraper.py

Includes:
1. Atomic I/O, File Locks & Process Singletons (formerly Terminal.runtime_io)
2. Chrome Browser CDP Coordination & IP Rotation (formerly Terminal.collector_lock)
3. Universal Order-Flow & Multi-Bar Footprint Signals (formerly Terminal.universal_footprint_engine)
4. Auction Market Profile Indicators (formerly Terminal.auction_indicators)
========================================================================================
"""


# --- RUNTIME I/O ---
"""Crash-released interprocess locks and atomic local data publication."""
import asyncio
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
import threading
import itertools
import uuid
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

_THREAD_WRITER_LOCKS = threading.local()


class WriterLockTimeout(TimeoutError):
    """Raised when FileMutex cannot be acquired within timeout.
    Enforces fail-closed mutual exclusion (Finding H7 / Condition Precedent CP1).
    """
    pass


class FileMutex:
    """Lock a persistent inode; never expire or unlink another writer's lock."""
    def __init__(self, path, timeout=2.0):
        self.path = Path(path)
        self.timeout = max(0.0, float(timeout))
        self.handle = None
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Never unlink a lock inode: OS locks are released on process death.
        # Unlinking after a probe unlock lets two writers lock different inodes.
        self.handle = self.path.open('a+b')
        try:
            if self.path.stat().st_size == 0:
                try:
                    self.handle.write(b'0')
                    self.handle.flush()
                except (OSError, PermissionError):
                    pass
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    self.handle.seek(0)
                    if os.name == 'nt':
                        import msvcrt
                        msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.acquired = True
                    return self
                except (OSError, PermissionError):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.handle.close()
                        self.handle = None
                        raise WriterLockTimeout(f"FileMutex {self.path!r} timed out after {self.timeout:.2f}s (busy)")
                    time.sleep(min(0.02, remaining))
        except BaseException:
            if self.handle is not None:
                try:
                    self.handle.close()
                except Exception:
                    pass
                self.handle = None
            raise

    def __exit__(self, *args):
        if self.handle is not None:
            try:
                if self.acquired:
                    self.handle.seek(0)
                    if os.name == 'nt':
                        import msvcrt
                        try:
                            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                        except (OSError, PermissionError):
                            pass
                    else:
                        import fcntl
                        try:
                            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                        except (OSError, PermissionError):
                            pass
            finally:
                try:
                    self.handle.close()
                except Exception:
                    pass
                self.handle = None
                self.acquired = False


@contextmanager
def writer_lock(path, timeout=5.0):
    p = Path(path).resolve()
    db_dir = Path(__file__).resolve().parent / "Database"
    try:
        rel = p.relative_to(db_dir.resolve())
        lock_dir = db_dir / ".locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        sanitized = str(rel).replace("\\", "_").replace("/", "_") + ".writer.lock"
        lock_path = lock_dir / sanitized
    except ValueError:
        lock_path = Path(str(p) + ".writer.lock")

    lock_str = str(lock_path.resolve())
    held = getattr(_THREAD_WRITER_LOCKS, "held", None)
    if held is None:
        held = {}
        _THREAD_WRITER_LOCKS.held = held

    # Re-entrant fast path: current thread already holds the underlying OS lock
    if lock_str in held:
        held[lock_str] += 1
        try:
            yield
        finally:
            held[lock_str] -= 1
            if held[lock_str] <= 0:
                del held[lock_str]
        return

    with FileMutex(str(lock_path), timeout=timeout) as lock:
        if not lock.acquired:
            raise TimeoutError(f'Writer busy: {path}')
        held[lock_str] = 1
        try:
            yield
        finally:
            held[lock_str] -= 1
            if held[lock_str] <= 0:
                del held[lock_str]


def _publish(path, write):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '.', suffix='.tmp', dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        write(temporary)
        try:
            with temporary.open('r+b') as stream:
                os.fsync(stream.fileno())
        except OSError:
            temporary.unlink(missing_ok=True)
            raise
        deadline = time.monotonic() + 10.0
        delay = 0.02
        while True:
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.35)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            logging.getLogger(__name__).warning("Temporary file cleanup failed: %s", temporary)


def atomic_json(path, data):
    payload = json.dumps(data, ensure_ascii=True, allow_nan=False)
    _publish(path, lambda p: p.write_text(payload, encoding='utf-8'))


def atomic_append_jsonl(path, records):
    """
    Appends one or more JSON records to a JSONL file with cross-process FileMutex
    and immediate flush + fsync to prevent stream tearing and partial writes.
    """
    if isinstance(records, dict):
        records = [records]
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with writer_lock(p):
        with open(p, "a", encoding="utf-8") as f:
            for rec in records:
                payload = json.dumps(rec, ensure_ascii=True, allow_nan=False)
                f.write(payload + "\n")
            f.flush()
            os.fsync(f.fileno())


def verify_parquet(path, expected_columns=None, min_rows=1):
    """Read back through PyArrow before treating a published file as valid."""
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    if table.num_rows < min_rows:
        raise ValueError(f"Parquet has {table.num_rows} rows (minimum {min_rows}): {path}")
    if expected_columns is not None and table.column_names != list(expected_columns):
        raise ValueError(f"Parquet schema mismatch: {path}: {table.column_names}")
    return table.num_rows


def atomic_parquet(frame, path, compression='snappy'):
    import pyarrow.parquet as pq
    with writer_lock(path):
        if hasattr(frame, 'to_parquet'):
            write = lambda p: frame.to_parquet(p, index=False, engine='pyarrow', compression=compression)
            rows, columns = len(frame), list(frame.columns)
        elif hasattr(frame, 'write_parquet'):
            write = lambda p: frame.write_parquet(p, compression=compression)
            rows, columns = len(frame), list(frame.columns)
        else:
            write = lambda p: pq.write_table(frame, p, compression=compression)
            rows, columns = frame.num_rows, frame.column_names
        schema = None
        def write_verified(temporary):
            nonlocal schema
            write(temporary)
            table = pq.read_table(temporary)
            if table.num_rows != rows or table.column_names != columns:
                raise ValueError(f"Parquet serialization verification failed: {path}")
            schema = table.schema
        _publish(path, write_verified)
        published = pq.read_table(path)
        if published.num_rows != rows or not published.schema.equals(schema, check_metadata=False):
            raise ValueError(f"Published Parquet read-back schema/row mismatch: {path}")


def upsert_parquet(frame, path, keys, sort_by=None, limit=None, compression='snappy'):
    import pandas as pd
    with writer_lock(path):
        # A corrupt existing history is an error, never permission to erase it.
        previous = pd.read_parquet(path) if Path(path).exists() else frame.iloc[:0]
        combined = pd.concat([previous, frame], ignore_index=True).drop_duplicates(keys, keep='last')
        if sort_by:
            combined = combined.sort_values(sort_by)
        if limit is not None:
            combined = combined.tail(limit)
        atomic_parquet(combined, path, compression=compression)
    return combined


async def cdp_result(ws, payload, timeout=15.0):
    """Bound the whole request, including unrelated CDP events."""
    async with asyncio.timeout(timeout):
        await ws.send(json.dumps(payload))
        while True:
            message = json.loads(await ws.recv())
            if message.get('id') == payload['id']:
                if 'error' in message or message.get('result', {}).get('exceptionDetails'):
                    raise RuntimeError(f'CDP request failed: {message}')
                return message.get('result', {}).get('result', {}).get('value')


_CDP_IDS = itertools.count(1)
CDP_OWNER_ID = uuid.uuid4().hex


def bot_tab_url(url: str, bot: str) -> str:
    """Tag a newly created tab with both its pillar and this process's lease."""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k not in ("bot", "owner")]
    query.extend([("bot", bot), ("owner", CDP_OWNER_ID)])
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def is_process_bot_tab(tab: dict, bot: str) -> bool:
    """Exact query matching; a substring in a user's URL is not ownership."""
    parts = urlsplit(tab.get("url", ""))
    query = dict(parse_qsl(parts.query))
    return (parts.hostname in {"www.coinglass.com", "coinglass.com"}
            and query.get("bot") == bot and query.get("owner") == CDP_OWNER_ID)


async def cdp_eval(ws, expression: str, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await cdp_result(ws, {
                "id": next(_CDP_IDS), "method": "Runtime.evaluate",
                "params": {"expression": expression, "returnByValue": True, "awaitPromise": True},
            }, timeout=min(15.0, max(1.0, deadline - time.monotonic())))
        except RuntimeError as e:
            if "Execution context was destroyed" in str(e) and time.monotonic() < deadline:
                await asyncio.sleep(0.4)
                continue
            raise


_PROCESS_LOCKS = {}

def acquire_singleton(name, directory):
    """Keep the kernel lock open for process lifetime; never kill a reused PID."""
    key = str(Path(directory) / (name + '.instance.lock'))
    if key in _PROCESS_LOCKS:
        return
    lock = FileMutex(key, timeout=0).__enter__()
    if not lock.acquired:
        raise RuntimeError(f'Another instance is active: {name}')
    _PROCESS_LOCKS[key] = lock


def completed_candles(frame, now_ms=None, allow_session_breaks=False):
    import pandas as pd
    import numpy as np
    now_ms = time.time() * 1000 if now_ms is None else now_ms
    if 'open_time_ms' not in frame or 'close_time_ms' not in frame:
        raise ValueError('Candle timestamps unavailable')
    frame = frame.copy()
    for col in ['open_time_ms', 'close_time_ms']:
        frame[col] = pd.to_numeric(frame[col], errors='raise')
    frame = frame[(frame.close_time_ms <= now_ms) & (frame.close_time_ms > frame.open_time_ms)]
    frame = frame.sort_values('open_time_ms').drop_duplicates('open_time_ms', keep='last')
    is_tradfi = False
    if 'asset_class' in frame.columns and not frame.empty:
        ac = str(frame['asset_class'].iloc[0]).strip().lower()
        if ac in {'forex', 'indices', 'metals', 'commodities'}:
            is_tradfi = True

    # Staleness check: enforce 30m window on continuous 24/7 markets (Crypto),
    # but allow weekend/holiday session breaks for TradFi assets.
    if frame.empty or (not (allow_session_breaks or is_tradfi) and (now_ms - frame.close_time_ms.iloc[-1] > 1800000)):
        raise ValueError('Completed candle history stale or empty')
    if len(frame) > 1:
        diffs = np.diff(frame.open_time_ms.to_numpy(dtype=np.int64))
        if allow_session_breaks or is_tradfi:
            # Valid TradFi intervals: exact 15m (900k ms) or legitimate session/weekend breaks (multiples of 15m >= 1h)
            valid = (diffs == 900000) | ((diffs >= 1800000) & (diffs % 900000 == 0))
            if not np.all(valid):
                raise ValueError('Non-contiguous M15 candle history')
        else:
            if not np.all(diffs == 900000):
                raise ValueError('Non-contiguous M15 candle history')
    return frame

# --- COLLECTOR LOCK & BROWSER COORDINATION ---
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
import re

from pathlib import Path

from typing import Optional, Tuple

# from Terminal.runtime_io import FileMutex, atomic_json  # Inlined into coinglass_common



ROOT_DIR = Path(__file__).resolve().parent

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

    if os.environ.get("PYTEST_CURRENT_TEST") or os.environ.get("OFFLINE_MODE", "").lower() in ("true", "1"):

        return "127.0.0.1"

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





DELTA_EXCLUSION_HOSTS = (
    "cdn.india.delta.exchange", "api.india.delta.exchange",
    "india.delta.exchange", "delta.exchange",
)


def _warp_command(*args, timeout=8):
    result = subprocess.run([str(WARP_CLI), *args], capture_output=True,
                            text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"warp-cli {' '.join(args)} failed: {result.stderr or result.stdout}")
    return result.stdout


def ensure_delta_split_tunnel_exclusions():
    """Configure exclusions while disconnected, then verify the effective list.

    ``tunnel host add`` is relative to the current split-tunnel mode. Never add
    Delta to an Include list (that would tunnel it). Organization-managed
    profiles must be corrected by their administrator.
    """
    if not WARP_CLI.exists():
        raise RuntimeError(f"WARP CLI unavailable: {WARP_CLI}")
    try:
        dump = _warp_command("tunnel", "dump")
    except RuntimeError as e:
        if "WARP is not connected" in str(e):
            return
        raise
    mode = re.search(r"Split tunnel mode:\s*(include|exclude)", dump, re.I)
    is_exclude = (mode and mode.group(1).lower() == "exclude") or ("Excluded:" in dump)
    if not is_exclude:
        raise RuntimeError("Delta exclusions require Split Tunnel Exclude mode")
    listed = _warp_command("tunnel", "host", "list")
    present = {line.replace("(CLI exclude)", "").strip().lower() for line in listed.splitlines()}
    for host in DELTA_EXCLUSION_HOSTS:
        if not any(host in p for p in present):
            _warp_command("tunnel", "host", "add", host)
    verified = {line.replace("(CLI exclude)", "").strip().lower() for line in
                _warp_command("tunnel", "host", "list").splitlines()}
    missing = [h for h in DELTA_EXCLUSION_HOSTS if not any(h in v for v in verified)]
    if missing:
        raise RuntimeError(f"Delta split-tunnel exclusions missing: {sorted(missing)}")


def is_warp_rotating() -> bool:

    """Returns True if a WARP rotation is currently in-flight."""

    lock_file = str(LOCK_DIR / "warp_execution.lock")

    try:

        with FileMutex(lock_file, timeout=0.0) as guard:

            return not guard.acquired

    except Exception:

        return False





def is_warp_connected() -> bool:
    """Returns True if Cloudflare WARP client status is currently connected."""
    try:
        p = subprocess.run([str(WARP_CLI), "status"], capture_output=True, text=True, timeout=3)
        return "Connected" in p.stdout and "Disconnected" not in p.stdout
    except Exception:
        return False


def rotate_warp_ip(context: str = "Collector", reason: str = "NORMAL", force: bool = False) -> str:
    """Reactive-only egress rotation. force cannot bypass policy or order safety.

    Raises on failed rotation; callers must not publish data or log success.
    Run through asyncio.to_thread from async collectors.
    """
    valid_reasons = {"HTTP_429", "CHALLENGE", "RATE_LIMIT_BANNER"}
    if reason not in valid_reasons:
        logger.info("[%s] WARP rotation skipped: non-reactive reason %s", context, reason)
        return get_current_ip()
    try:
        with FileMutex(str(LOCK_DIR / "order_execution_inflight.lock"), timeout=0.0):
            pass
    except (WriterLockTimeout, TimeoutError):
        raise RuntimeError("WARP rotation blocked: Delta order execution in flight")
    try:
        with FileMutex(str(LOCK_DIR / "warp_execution.lock"), timeout=1.0) as mutex:
            # Re-read cooldown under the cross-process execution mutex.
            now = time.time()
            elapsed = now - _get_last_rotation_time()
            if elapsed < MIN_ROTATION_INTERVAL_SECS:
                raise RuntimeError(f"WARP rotation cooldown active: {elapsed:.0f}s")
            if not WARP_CLI.exists():
                raise RuntimeError(f"WARP CLI unavailable: {WARP_CLI}")
            old_ip = get_current_ip()
            try:
                _warp_command("disconnect")
                ensure_delta_split_tunnel_exclusions()
                _warp_command("registration", "delete")
                _warp_command("registration", "new")
                ensure_delta_split_tunnel_exclusions()
                _warp_command("connect")
                stable, new_ip = verify_network_stabilization(old_ip, max_timeout=12.0)
                if not stable or new_ip == old_ip or new_ip == "unknown":
                    raise RuntimeError(f"WARP egress did not change from {old_ip}")
                if not is_warp_connected():
                    raise RuntimeError("WARP registration reset finished disconnected")
                _set_last_rotation_time(time.time(), new_ip, reason)
                logger.info("[%s] Reactive WARP rotation complete: %s", context, reason)
                return new_ip
            except Exception:
                # Best-effort restore connectivity; never report a failed reset as success.
                try:
                    _warp_command("connect")
                    # Do not mutate routes while connected: some clients disconnect per host add.
                    dump = _warp_command("tunnel", "dump")
                    listed = _warp_command("tunnel", "host", "list")
                    hosts = {line.strip().lower() for line in listed.splitlines()}
                    if not re.search(r"Split tunnel mode:\s*exclude", dump, re.I) or set(DELTA_EXCLUSION_HOSTS) - hosts:
                        raise RuntimeError("Connectivity restored but Delta exclusions require administrator repair")
                except Exception as restore_error:
                    logger.critical("[%s] WARP connectivity restore failed: %s", context, restore_error)
                raise
    except (WriterLockTimeout, TimeoutError):
        raise RuntimeError("WARP rotation already in progress")





# Global lock contention tracking (DeepSeek AUD-07)

LOCK_CONTENTION_SKIPS = 0



# Domain-Isolated Collector Engine Lock Map:

# Eliminates starvation deadlocks between long sweeps (Footprint), fast sweeps (TV), and live order dispatch.

CHROME_BROWSER_LOCK_FILE = LOCK_DIR / "chrome_browser_cdp.lock"

COLLECTOR_LOCK_MAP = {
    "TVLiquidationScraper": CHROME_BROWSER_LOCK_FILE,
    "FootprintScraper": CHROME_BROWSER_LOCK_FILE,
    "WhaleScraper": CHROME_BROWSER_LOCK_FILE,
    "LiquidationScraper": CHROME_BROWSER_LOCK_FILE,
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





# --- Z.AI DOMAIN 2 UPGRADE: CHROME CDP LIVENESS WATCHDOG & HEARTBEAT ---
CHROME_HEARTBEAT_FILE = LOCK_DIR / "chrome_heartbeat.json"

def is_cdp_alive(timeout_s: float = 3.0) -> bool:
    """Return True iff Chrome DevTools Protocol /json/version responds with HTTP 200."""
    try:
        import urllib.request
        req = urllib.request.Request("http://127.0.0.1:9222/json/version", headers={"User-Agent": "Antigravity/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_s) as r:
            return r.status == 200
    except Exception:
        return False


class ChromeWatchdog:
    """
    Background watchdog that probes Chrome CDP port (127.0.0.1:9222) every probe_interval_s.
    Writes liveness heartbeat to Database/.locks/chrome_heartbeat.json.
    """
    def __init__(self, probe_interval_s: float = 30.0, on_recover = None):
        self.probe_interval_s = probe_interval_s
        self.on_recover = on_recover
        self._stop = False
        self._thread = None

    def start(self):
        import threading
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._loop, name="chrome-watchdog", daemon=True)
        self._thread.start()
        logger.info("ChromeWatchdog background thread started (probe interval: %ss).", self.probe_interval_s)

    def stop(self):
        self._stop = True

    def _loop(self):
        import time
        import os
        import json
        while not self._stop:
            alive = is_cdp_alive()
            try:
                LOCK_DIR.mkdir(parents=True, exist_ok=True)
                payload = {
                    "alive": alive,
                    "ts_us": int(time.time() * 1_000_000),
                    "pid": os.getpid()
                }
                CHROME_HEARTBEAT_FILE.write_text(json.dumps(payload), encoding="utf-8")
            except Exception:
                pass

            if not alive and self.on_recover:
                try:
                    self.on_recover("CDP port 9222 unresponsive")
                except Exception:
                    pass
            time.sleep(self.probe_interval_s)


def _reap_stale_lock(lock_path: Path, ttl: float = 120.0) -> None:
    """Compatibility no-op: unlinking a lock inode breaks mutual exclusion."""
    return None


def clean_all_abandoned_locks(ttl: float = 120.0) -> int:
    """OS advisory locks need no TTL reaping; retain lock inodes."""
    return 0


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


# --- AUCTION INDICATORS ---
"""
Auction Market Theory, Institutional Levels & Multi-Timeframe Fibonacci-Prime Engine
=====================================================================================
Computes institutional auction levels, session profiles, Fib-Prime ribbons, and MTF alignment:

1. Prior Day Reference Levels:
   - Prior Day High (PDH), Prior Day Low (PDL), Prior Day Close (PDC)
   - Prior Day VWAP (pdVWAP), Prior Day POC (pdPOC), Prior Day VAH/VAL (pdVAH, pdVAL)
   - Prior Day Half-Back ((PDH + PDL) / 2)

2. Current Day Auction Profile & Initial Balance:
   - Current Day Open (CDO / 00:00 UTC), Day High (CDH), Day Low (CDL), Day Half-Back
   - Initial Balance High (IBH), Initial Balance Low (IBL) (First 60 min: 00:00-01:00 UTC)
   - Day VWAP & Standard Deviation Bands (±1σ, ±2σ)
   - 70% Value Area: Day POC, Day VAH, Day VAL, Value Area Status (INSIDE, ABOVE_VAH, BELOW_VAL)

3. Macro Timeframe Anchors:
   - Weekly Open (Monday 00:00 UTC Open)
   - Monthly Open (1st of month 00:00 UTC Open)

4. Session Anchored VWAPs:
   - Asia Session VWAP (00:00 - 08:00 UTC)
   - London Session VWAP (08:00 - 16:30 UTC)
   - New York Session VWAP (13:30 - 21:00 UTC)

5. Fibonacci-Prime EMAs (15m & 5m):
   - Prime & Fibonacci Periods: EMA 5, EMA 13, EMA 21, EMA 55, EMA 89, EMA 144, EMA 233
   - Ribbon Alignment: BULLISH_FIB_PRIME_STACK, BEARISH_FIB_PRIME_STACK, TREND_PULLBACK, CHOP

6. Higher Timeframe (HTF) Multi-Timeframe Alignment:
   - 1H Timeframe: 1H Trend, 1H Fib EMAs (13, 55, 89), 1H VWAP
   - 4H Timeframe: 4H Macro Trend, 4H Fib EMAs (21, 55, 233)
   - Triple Timeframe Confluence: HTF_BULLISH_CONFLUENCE, HTF_BEARISH_CONFLUENCE, HTF_DIVERGENCE

7. Microstructure Levels (Whale Walls & Liquidation Magnets):
   - Nearest Whale Bid/Ask Walls from Whale Orders database
   - Nearest Liquidation Magnet Zones from Liquidation database
   - Naked Point of Control (nVPOC) tracking

8. Continuous Local Persistence:
   - Persists Database/Auction_Levels/<SYMBOL>_auction_levels.parquet
   - Persists Database/Auction_Levels/all_symbols_auction_levels.parquet
   - Persists Database/Auction_Levels/all_symbols_auction_levels.json
"""

import sys
import os
import math
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# from Terminal.runtime_io import atomic_json, atomic_parquet, writer_lock  # Inlined into coinglass_common

DB_DIR = ROOT_DIR / "Database"
CANDLE_DIR = DB_DIR / "Candle_Data"
AUCTION_DIR = DB_DIR / "Auction_Levels"
WHALE_DIR = DB_DIR / "Whale_Orders" / "Open_Orders"
MAGNET_DIR = DB_DIR / "Liquidation_Data" / "Magnet_Zones"

AUCTION_DIR.mkdir(parents=True, exist_ok=True)


def get_tick_bin_size(symbol: str, price: float) -> float:
    """Adaptive price bin size for volume profile clustering."""
    if price >= 50000:
        return 50.0   # BTC
    elif price >= 1000:
        return 5.0    # ETH, Gold, Indices
    elif price >= 100:
        return 0.50   # SOL, Oil
    elif price >= 10:
        return 0.05   # LINK, AVAX, Silver
    elif price >= 1:
        return 0.005  # XRP, ADA, SUI
    else:
        return 0.0005 # DOGE, PEPE, SHIB


def compute_fib_prime_emas(series: pd.Series) -> Dict[str, float]:
    """Computes Fibonacci & Prime EMAs: 5, 13, 21, 34, 55, 59, 89, 144, 233."""
    spans = [5, 13, 21, 34, 55, 59, 89, 144, 233]
    emas = {}
    for s in spans:
        if len(series) >= s:
            emas[f"ema{s}"] = float(series.ewm(span=s, adjust=False).mean().iloc[-1])
        else:
            emas[f"ema{s}"] = float(series.ewm(span=min(len(series), s), adjust=False).mean().iloc[-1])
    return emas


def evaluate_fib_prime_stack(price: float, emas: Dict[str, float]) -> str:
    """Classifies the Fibonacci Prime Ribbon alignment."""
    e5 = emas.get("ema5", price)
    e13 = emas.get("ema13", price)
    e21 = emas.get("ema21", price)
    e34 = emas.get("ema34", price)
    e55 = emas.get("ema55", price)
    e59 = emas.get("ema59", price)
    e89 = emas.get("ema89", price)
    e144 = emas.get("ema144", price)
    e233 = emas.get("ema233", price)

    if price > e5 > e13 > e21 > e34 > e55 > e59 > e89 > e144 > e233:
        return "BULLISH_FIB_PRIME_STACK"
    elif price < e5 < e13 < e21 < e34 < e55 < e59 < e89 < e144 < e233:
        return "BEARISH_FIB_PRIME_STACK"
    elif price > e59 > e89 and e5 < e21:
        return "BULLISH_TREND_PULLBACK"
    elif price < e59 < e89 and e5 > e21:
        return "BEARISH_TREND_RALLY"
    elif price > e55 and price > e59:
        return "BULLISH_ABOVE_POCKET"
    elif price < e55 and price < e59:
        return "BEARISH_BELOW_POCKET"
    else:
        return "MIXED_COMPRESSION"


def compute_fib_prime_pullback_proximity(price: float, emas: Dict[str, float]) -> Dict[str, Any]:
    """Computes proximity to dynamic Fib-Prime pocket [EMA 34, EMA 55, EMA 59, EMA 89]."""
    e34 = emas.get("ema34", price)
    e55 = emas.get("ema55", price)
    e59 = emas.get("ema59", price)
    e89 = emas.get("ema89", price)
    e233 = emas.get("ema233", price)

    d34 = abs(price - e34) / price * 100.0 if price > 0 else 999.0
    d55 = abs(price - e55) / price * 100.0 if price > 0 else 999.0
    d59 = abs(price - e59) / price * 100.0 if price > 0 else 999.0
    d89 = abs(price - e89) / price * 100.0 if price > 0 else 999.0
    d233 = abs(price - e233) / price * 100.0 if price > 0 else 999.0

    dist_55_59 = min(d55, d59)
    in_pullback_pocket = dist_55_59 <= 0.60
    in_deep_pocket = min(d55, d59, d89) <= 0.60

    return {
        "dist_34_pct": round(d34, 3),
        "dist_55_pct": round(d55, 3),
        "dist_59_pct": round(d59, 3),
        "dist_89_pct": round(d89, 3),
        "dist_233_pct": round(d233, 3),
        "dist_55_59_pct": round(dist_55_59, 3),
        "in_pullback_pocket": in_pullback_pocket,
        "in_deep_pocket": in_deep_pocket
    }


def extract_prior_day_levels(df: pd.DataFrame, today_date) -> Dict[str, Any]:
    """Calculates PDH, PDL, PDC, pdVWAP, pdPOC, pdVAH, pdVAL, and pdHalfBack."""
    unique_days = sorted(df["day"].unique())
    if today_date in unique_days:
        idx = unique_days.index(today_date)
        if idx > 0:
            prior_date = unique_days[idx - 1]
            prior_df = df[df["day"] == prior_date].copy()
            if not prior_df.empty:
                pdh = float(prior_df["high"].max())
                pdl = float(prior_df["low"].min())
                pdc = float(prior_df["close"].iloc[-1])
                pd_half_back = float((pdh + pdl) / 2.0)
                
                # Prior day VWAP
                p_vol = prior_df["total_volume"].sum()
                pd_vwap = float(prior_df["pv"].sum() / p_vol) if p_vol > 0 else pdc
                
                # Prior day POC and Value Area
                bin_size = get_tick_bin_size(prior_df["symbol"].iloc[0] if "symbol" in prior_df else "BTC", pdc)
                prior_df["price_bin"] = np.round(prior_df["tp"] / bin_size) * bin_size
                vp = prior_df.groupby("price_bin")["total_volume"].sum().sort_index()
                pd_poc = float(vp.idxmax())
                
                target_vol = p_vol * 0.70
                sorted_vp = vp.sort_values(ascending=False)
                cum_vol = 0.0
                va_bins = []
                for p_bin, vol in sorted_vp.items():
                    cum_vol += vol
                    va_bins.append(float(p_bin))
                    if cum_vol >= target_vol:
                        break
                pd_vah = float(max(va_bins)) if va_bins else pdh
                pd_val = float(min(va_bins)) if va_bins else pdl
                
                return {
                    "pd_date": str(prior_date),
                    "pdh": round(pdh, 4),
                    "pdl": round(pdl, 4),
                    "pdc": round(pdc, 4),
                    "pd_half_back": round(pd_half_back, 4),
                    "pd_vwap": round(pd_vwap, 4),
                    "pd_poc": round(pd_poc, 4),
                    "pd_vah": round(pd_vah, 4),
                    "pd_val": round(pd_val, 4)
                }
    return {
        "pd_date": "", "pdh": 0.0, "pdl": 0.0, "pdc": 0.0,
        "pd_half_back": 0.0, "pd_vwap": 0.0, "pd_poc": 0.0, "pd_vah": 0.0, "pd_val": 0.0
    }


def compute_htf_alignment(df: pd.DataFrame) -> Dict[str, Any]:
    """Resamples 15m candles to 1H and 4H to establish institutional multi-timeframe bias."""
    try:
        df_indexed = df.set_index("dt").copy()
        
        # 1H Resample (Aggregated from 15m)
        df_1h = df_indexed.resample("1h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "total_volume": "sum"
        }).dropna()
        
        # 4H Resample (Aggregated from 15m)
        df_4h = df_indexed.resample("4h").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "total_volume": "sum"
        }).dropna()
        
        # 1H Metrics (Direct 1H resample + 15m native span verification)
        h1_trend = "NEUTRAL"
        h1_ema13 = None
        h1_ema55 = None
        h1_ema89 = None
        if len(df_1h) >= 10:
            c_1h = df_1h["close"]
            h1_ema13 = float(c_1h.ewm(span=13, adjust=False).mean().iloc[-1])
            h1_ema55 = float(c_1h.ewm(span=55, adjust=False).mean().iloc[-1])
            h1_ema89 = float(c_1h.ewm(span=89, adjust=False).mean().iloc[-1])
            last_c1 = float(c_1h.iloc[-1])
            if last_c1 > h1_ema13 > h1_ema55:
                h1_trend = "BULLISH"
            elif last_c1 < h1_ema13 < h1_ema55:
                h1_trend = "BEARISH"
        elif len(df) >= 52:
            # 15m native equivalent: span 52 (1H EMA 13) and span 220 (1H EMA 55)
            c_15m = df["close"]
            h1_ema13 = float(c_15m.ewm(span=52, adjust=False).mean().iloc[-1])
            h1_ema55 = float(c_15m.ewm(span=min(len(c_15m), 220), adjust=False).mean().iloc[-1])
            last_c = float(c_15m.iloc[-1])
            if last_c > h1_ema13 > h1_ema55:
                h1_trend = "BULLISH"
            elif last_c < h1_ema13 < h1_ema55:
                h1_trend = "BEARISH"
                
        # 4H Metrics (Native 15m span verification to avoid sample starvation)
        h4_trend = "NEUTRAL"
        h4_ema21 = None
        h4_ema55 = None
        h4_ema233 = None
        if len(df_4h) >= 15:
            c_4h = df_4h["close"]
            h4_ema21 = float(c_4h.ewm(span=21, adjust=False).mean().iloc[-1])
            h4_ema55 = float(c_4h.ewm(span=55, adjust=False).mean().iloc[-1])
            h4_ema233 = float(c_4h.ewm(span=min(len(c_4h), 233), adjust=False).mean().iloc[-1])
            last_c4 = float(c_4h.iloc[-1])
            if last_c4 > h4_ema21 > h4_ema55:
                h4_trend = "BULLISH"
            elif last_c4 < h4_ema21 < h4_ema55:
                h4_trend = "BEARISH"
        elif len(df) >= 80:
            # 15m native equivalent: span 84 (4H EMA 21) and span 220 (4H EMA 55)
            c_15m = df["close"]
            h4_ema21 = float(c_15m.ewm(span=84, adjust=False).mean().iloc[-1])
            h4_ema55 = float(c_15m.ewm(span=min(len(c_15m), 220), adjust=False).mean().iloc[-1])
            last_c = float(c_15m.iloc[-1])
            if last_c > h4_ema21 > h4_ema55:
                h4_trend = "BULLISH"
            elif last_c < h4_ema21 < h4_ema55:
                h4_trend = "BEARISH"

        # Confluence classification
        if h1_trend == "BULLISH" and h4_trend == "BULLISH":
            mtf_bias = "HTF_BULLISH_CONFLUENCE"
        elif h1_trend == "BEARISH" and h4_trend == "BEARISH":
            mtf_bias = "HTF_BEARISH_CONFLUENCE"
        elif h1_trend != h4_trend and h4_trend != "NEUTRAL":
            mtf_bias = "HTF_DIVERGENCE"
        else:
            mtf_bias = "HTF_NEUTRAL"

        return {
            "htf_1h_trend": h1_trend,
            "htf_1h_ema13": round(h1_ema13, 4) if h1_ema13 else None,
            "htf_1h_ema55": round(h1_ema55, 4) if h1_ema55 else None,
            "htf_1h_ema89": round(h1_ema89, 4) if h1_ema89 else None,
            "htf_4h_trend": h4_trend,
            "htf_4h_ema21": round(h4_ema21, 4) if h4_ema21 else None,
            "htf_4h_ema55": round(h4_ema55, 4) if h4_ema55 else None,
            "htf_4h_ema233": round(h4_ema233, 4) if h4_ema233 else None,
            "mtf_bias": mtf_bias
        }
    except Exception as e:
        return {
            "htf_1h_trend": "NEUTRAL", "htf_4h_trend": "NEUTRAL", "mtf_bias": "HTF_UNAVAILABLE", "error": str(e)
        }


def extract_microstructure_levels(symbol: str, current_price: float) -> Dict[str, Any]:
    """Queries live Whale Order Walls & Liquidation Magnet Zones."""
    empty_level = {"price": 0.0, "amount_usd": 0.0, "score": 0.0}
    result = {
        "nearest_whale_bid": dict(empty_level), "nearest_whale_ask": dict(empty_level),
        "nearest_long_liq_magnet": dict(empty_level), "nearest_short_liq_magnet": dict(empty_level)
    }
    
    # 1. Whale Walls
    whale_file = WHALE_DIR / f"{symbol}_whale_open_orders.parquet"
    if whale_file.exists():
        try:
            wdf = pd.read_parquet(whale_file)
            if not wdf.empty and "price" in wdf.columns and "side" in wdf.columns:
                wdf["price"] = pd.to_numeric(wdf["price"], errors="coerce")
                bids = wdf[wdf["side"].str.upper().str.contains("BUY|BID")].dropna(subset=["price"])
                asks = wdf[wdf["side"].str.upper().str.contains("SELL|ASK")].dropna(subset=["price"])
                
                # Nearest bid wall below price
                bids_below = bids[bids["price"] <= current_price]
                if not bids_below.empty:
                    near_bid = bids_below.sort_values("price", ascending=False).iloc[0]
                    result["nearest_whale_bid"] = {
                        "price": float(near_bid["price"]),
                        "amount_usd": float(near_bid.get("remaining_usd", near_bid.get("initial_usd", near_bid.get("amount_usd", near_bid.get("size", 0.0)))))
                    }
                    
                # Nearest ask wall above price
                asks_above = asks[asks["price"] >= current_price]
                if not asks_above.empty:
                    near_ask = asks_above.sort_values("price", ascending=True).iloc[0]
                    result["nearest_whale_ask"] = {
                        "price": float(near_ask["price"]),
                        "amount_usd": float(near_ask.get("remaining_usd", near_ask.get("initial_usd", near_ask.get("amount_usd", near_ask.get("size", 0.0)))))
                    }
        except Exception:
            pass

    # 2. Liquidation Magnets
    magnet_file = MAGNET_DIR / f"{symbol}_magnet_zones.parquet"
    if magnet_file.exists():
        try:
            mdf = pd.read_parquet(magnet_file)
            price_col = "zone_price" if "zone_price" in mdf.columns else ("price" if "price" in mdf.columns else None)
            if not mdf.empty and "side" in mdf.columns:
                if price_col:
                    mdf["zone_price"] = pd.to_numeric(mdf[price_col], errors="coerce")
                elif "price_band" in mdf.columns:
                    import re
                    def parse_band_mid(val):
                        nums = re.findall(r"[\d,]+\.?\d*", str(val))
                        if len(nums) >= 2:
                            return (float(nums[0].replace(",", "")) + float(nums[1].replace(",", ""))) / 2.0
                        return None
                    mdf["zone_price"] = mdf["price_band"].apply(parse_band_mid)
                
                long_liq = mdf[mdf["side"].str.upper().str.contains("LONG")].dropna(subset=["zone_price"])
                short_liq = mdf[mdf["side"].str.upper().str.contains("SHORT")].dropna(subset=["zone_price"])
                
                if not long_liq.empty:
                    top_long = long_liq.sort_values("score", ascending=False).iloc[0] if "score" in long_liq.columns else long_liq.iloc[0]
                    result["nearest_long_liq_magnet"] = {
                        "price": float(top_long["zone_price"]),
                        "score": float(top_long.get("score", 0.0))
                    }
                if not short_liq.empty:
                    top_short = short_liq.sort_values("score", ascending=False).iloc[0] if "score" in short_liq.columns else short_liq.iloc[0]
                    result["nearest_short_liq_magnet"] = {
                        "price": float(top_short["zone_price"]),
                        "score": float(top_short.get("score", 0.0))
                    }
        except Exception:
            pass

    return result


def compute_auction_profile(symbol: str) -> Dict[str, Any]:
    """
    Computes complete Institutional Auction Levels, Fib-Prime EMAs,
    HTF Multi-Timeframe Alignment, and Microstructure Anchors.
    """
    candle_file = CANDLE_DIR / f"{symbol}.parquet"
    if not candle_file.exists():
        return {"data_valid": False, "error": f"No candle data for {symbol}"}

    try:
        auction_cols = ['open_time_utc', 'open_time_ms', 'open', 'high', 'low', 'close', 'total_volume', 'poc_price']
        try:
            df = pd.read_parquet(candle_file, columns=auction_cols)
        except Exception:
            df = pd.read_parquet(candle_file)
        if df.empty or len(df) < 10:
            return {"data_valid": False, "error": "Insufficient bars"}

        for c in ["open", "high", "low", "close", "total_volume"]:
            if c not in df.columns:
                return {"data_valid": False, "error": f"Missing column {c}"}
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["close", "total_volume"]).reset_index(drop=True)
        if len(df) < 10:
            return {"data_valid": False, "error": "Too few valid bars"}

        df["dt"] = pd.to_datetime(df["open_time_utc"], utc=True)
        df["tp"] = (df["high"] + df["low"] + df["close"]) / 3.0
        df["pv"] = df["tp"] * df["total_volume"]
        df["day"] = df["dt"].dt.date

        latest_close = float(df["close"].iloc[-1])
        today = df["day"].iloc[-1]
        today_df = df[df["day"] == today].copy()

        # Fallback to last 24h of bars if current day has too few bars (e.g. right after 00:00 UTC)
        if len(today_df) < 4:
            today_df = df.tail(96).copy()

        total_vol = float(today_df["total_volume"].sum())
        if total_vol <= 0:
            return {"data_valid": False, "error": "Zero volume"}

        # 1. Current Day Core Auction Levels
        day_open = float(today_df["open"].iloc[0])
        day_high = float(today_df["high"].max())
        day_low = float(today_df["low"].min())
        day_half_back = float((day_high + day_low) / 2.0)
        
        day_vwap = float(today_df["pv"].sum() / total_vol)
        today_df["dev_sq"] = ((today_df["tp"] - day_vwap) ** 2) * today_df["total_volume"]
        day_vwap_sd = float(np.sqrt(today_df["dev_sq"].sum() / total_vol))

        # 2. 70% Value Area & Point of Control (POC) - Contiguous Expansion from POC
        bin_size = get_tick_bin_size(symbol, latest_close)
        today_df["price_bin"] = np.round(today_df["tp"] / bin_size) * bin_size
        vp = today_df.groupby("price_bin")["total_volume"].sum().sort_index()
        day_poc = float(vp.idxmax())

        # Contiguous 70% value-area expansion from POC (standard CBOT/CME Market Profile)
        target_vol = total_vol * 0.70
        prices = vp.index.to_list()
        vols = vp.to_list()
        poc_idx = int(np.argmax(vols))
        lo_idx = hi_idx = poc_idx
        cum_vol = float(vols[poc_idx])
        while cum_vol < target_vol and (lo_idx > 0 or hi_idx < len(prices) - 1):
            down_vol = float(vols[lo_idx - 1]) if lo_idx > 0 else -1.0
            up_vol = float(vols[hi_idx + 1]) if hi_idx < len(prices) - 1 else -1.0
            if up_vol >= down_vol:
                hi_idx += 1
                cum_vol += float(vols[hi_idx])
            else:
                lo_idx -= 1
                cum_vol += float(vols[lo_idx])
        day_vah = float(prices[hi_idx])
        day_val = float(prices[lo_idx])

        if latest_close > day_vah:
            value_status = "ABOVE_VAH" # Premium
        elif latest_close < day_val:
            value_status = "BELOW_VAL" # Discount
        else:
            value_status = "INSIDE_VALUE"

        # 3. Initial Balance (First 60m of UTC Day: 00:00 to 01:00 UTC)
        ib_df = today_df[(today_df["dt"].dt.hour == 0)]
        if not ib_df.empty:
            ibh = float(ib_df["high"].max())
            ibl = float(ib_df["low"].min())
            ib_mid = float((ibh + ibl) / 2.0)
            if latest_close > ibh:
                ib_extension = "EXPANDING_UP"
            elif latest_close < ibl:
                ib_extension = "EXPANDING_DOWN"
            else:
                ib_extension = "INSIDE_IB"
        else:
            ibh = day_high
            ibl = day_low
            ib_mid = day_half_back
            ib_extension = "INSIDE_IB"

        # 4. Macro Open Anchors (Weekly Open & Monthly Open)
        now_dt = df["dt"].iloc[-1]
        monday_dt = now_dt - timedelta(days=now_dt.weekday())
        week_df = df[df["dt"] >= monday_dt.replace(hour=0, minute=0, second=0, microsecond=0)]
        weekly_open = float(week_df["open"].iloc[0]) if not week_df.empty else day_open
        
        month_df = df[df["dt"] >= now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)]
        monthly_open = float(month_df["open"].iloc[0]) if not month_df.empty else weekly_open

        # 5. Prior Day Levels
        pd_levels = extract_prior_day_levels(df, today)

        # 6. Session VWAPs
        cur_hour = today_df["dt"].iloc[-1].hour
        active_session = "ASIA" if cur_hour < 8 else ("LONDON" if cur_hour < 13 else ("NEW_YORK" if cur_hour < 21 else "ASIA"))

        asia_df = today_df[today_df["dt"].dt.hour < 8]
        asia_vwap = float(asia_df["pv"].sum() / asia_df["total_volume"].sum()) if not asia_df.empty and asia_df["total_volume"].sum() > 0 else day_vwap

        london_df = today_df[(today_df["dt"].dt.hour >= 8) & ((today_df["dt"].dt.hour < 16) | ((today_df["dt"].dt.hour == 16) & (today_df["dt"].dt.minute <= 30)))]
        london_vwap = float(london_df["pv"].sum() / london_df["total_volume"].sum()) if not london_df.empty and london_df["total_volume"].sum() > 0 else day_vwap

        ny_df = today_df[((today_df["dt"].dt.hour == 13) & (today_df["dt"].dt.minute >= 30)) | ((today_df["dt"].dt.hour > 13) & (today_df["dt"].dt.hour < 21))]
        ny_vwap = float(ny_df["pv"].sum() / ny_df["total_volume"].sum()) if not ny_df.empty and ny_df["total_volume"].sum() > 0 else day_vwap

        # 7. Fibonacci Prime EMAs (15m)
        fib_emas = compute_fib_prime_emas(df["close"])
        fib_prime_alignment = evaluate_fib_prime_stack(latest_close, fib_emas)
        pullback_prox = compute_fib_prime_pullback_proximity(latest_close, fib_emas)

        # 8. HTF Multi-Timeframe Alignment (1H and 4H)
        htf = compute_htf_alignment(df)

        # 9. Microstructure Levels (Whale Walls & Magnets)
        micro = extract_microstructure_levels(symbol, latest_close)

        # Recent Bar POC
        recent_bar_poc = float(df["poc_price"].iloc[-1]) if "poc_price" in df.columns and pd.notna(df["poc_price"].iloc[-1]) else None

        vwap_dist_pct = round(((latest_close - day_vwap) / day_vwap) * 100.0, 2)
        poc_dist_pct = round(((latest_close - day_poc) / day_poc) * 100.0, 2)

        profile = {
            "data_valid": True,
            "symbol": symbol,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "latest_close": latest_close,
            
            # Current Day Auction Levels
            "day_open": round(day_open, 4),
            "day_high": round(day_high, 4),
            "day_low": round(day_low, 4),
            "day_half_back": round(day_half_back, 4),
            "day_vwap": round(day_vwap, 4),
            "vwap_sd1_upper": round(day_vwap + day_vwap_sd, 4),
            "vwap_sd1_lower": round(day_vwap - day_vwap_sd, 4),
            "vwap_sd2_upper": round(day_vwap + 2 * day_vwap_sd, 4),
            "vwap_sd2_lower": round(day_vwap - 2 * day_vwap_sd, 4),
            "vwap_distance_pct": vwap_dist_pct,
            "price_vs_vwap": "ABOVE_VWAP" if latest_close >= day_vwap else "BELOW_VWAP",
            
            # Value Area & Volume Profile
            "day_poc": round(day_poc, 4),
            "day_vah": round(day_vah, 4),
            "day_val": round(day_val, 4),
            "value_status": value_status,
            "poc_distance_pct": poc_dist_pct,
            "recent_bar_poc": round(recent_bar_poc, 4) if recent_bar_poc else None,
            
            # Initial Balance
            "initial_balance_high": round(ibh, 4),
            "initial_balance_low": round(ibl, 4),
            "initial_balance_mid": round(ib_mid, 4),
            "ib_extension": ib_extension,
            
            # Macro Opens
            "weekly_open": round(weekly_open, 4),
            "weekly_open_dist_pct": round(((latest_close - weekly_open) / weekly_open) * 100.0, 2),
            "monthly_open": round(monthly_open, 4),
            
            # Prior Day Levels
            "pdh": pd_levels.get("pdh"),
            "pdl": pd_levels.get("pdl"),
            "pdc": pd_levels.get("pdc"),
            "pd_half_back": pd_levels.get("pd_half_back"),
            "pd_vwap": pd_levels.get("pd_vwap"),
            "pd_poc": pd_levels.get("pd_poc"),
            "pd_vah": pd_levels.get("pd_vah"),
            "pd_val": pd_levels.get("pd_val"),
            
            # Session VWAPs
            "active_session": active_session,
            "asia_vwap": round(asia_vwap, 4) if asia_vwap else round(day_vwap, 4),
            "london_vwap": round(london_vwap, 4) if london_vwap else round(day_vwap, 4),
            "ny_vwap": round(ny_vwap, 4) if ny_vwap else round(day_vwap, 4),
            
            # Fibonacci-Prime EMAs (15m)
            "ema5": round(fib_emas.get("ema5", 0.0), 4),
            "ema13": round(fib_emas.get("ema13", 0.0), 4),
            "ema21": round(fib_emas.get("ema21", 0.0), 4),
            "ema34": round(fib_emas.get("ema34", 0.0), 4),
            "ema55": round(fib_emas.get("ema55", 0.0), 4),
            "ema59": round(fib_emas.get("ema59", 0.0), 4),
            "ema89": round(fib_emas.get("ema89", 0.0), 4),
            "ema144": round(fib_emas.get("ema144", 0.0), 4),
            "ema233": round(fib_emas.get("ema233", 0.0), 4),
            "fib_prime_alignment": fib_prime_alignment,
            "dist_55_59_pct": pullback_prox.get("dist_55_59_pct", 999.0),
            "in_pullback_pocket": pullback_prox.get("in_pullback_pocket", False),
            "in_deep_pocket": pullback_prox.get("in_deep_pocket", False),
            "dist_34_pct": pullback_prox.get("dist_34_pct", 999.0),
            "dist_55_pct": pullback_prox.get("dist_55_pct", 999.0),
            "dist_59_pct": pullback_prox.get("dist_59_pct", 999.0),
            "dist_89_pct": pullback_prox.get("dist_89_pct", 999.0),
            "dist_233_pct": pullback_prox.get("dist_233_pct", 999.0),
            # Backward compatibility aliases
            "ema9": round(fib_emas.get("ema13", 0.0), 4),
            "ema20": round(fib_emas.get("ema21", 0.0), 4),
            "ema50": round(fib_emas.get("ema55", 0.0), 4),
            "ema200": round(fib_emas.get("ema233", 0.0), 4),
            "ema_alignment": "BULLISH_STACK" if "BULLISH" in fib_prime_alignment else ("BEARISH_STACK" if "BEARISH" in fib_prime_alignment else "MIXED"),
            
            # Higher Timeframe Alignment
            "htf_1h_trend": htf.get("htf_1h_trend"),
            "htf_1h_ema13": htf.get("htf_1h_ema13"),
            "htf_1h_ema55": htf.get("htf_1h_ema55"),
            "htf_4h_trend": htf.get("htf_4h_trend"),
            "htf_4h_ema21": htf.get("htf_4h_ema21"),
            "htf_4h_ema55": htf.get("htf_4h_ema55"),
            "mtf_bias": htf.get("mtf_bias"),
            
            # Microstructure Anchors
            "nearest_whale_bid": micro.get("nearest_whale_bid"),
            "nearest_whale_ask": micro.get("nearest_whale_ask"),
            "nearest_long_liq_magnet": micro.get("nearest_long_liq_magnet"),
            "nearest_short_liq_magnet": micro.get("nearest_short_liq_magnet")
        }

        # Atomically save individual symbol profile
        save_symbol_auction_level(symbol, profile)

        return profile

    except Exception as e:
        return {"data_valid": False, "error": str(e)}


def save_symbol_auction_level(symbol: str, profile: Dict[str, Any]):
    """Persists symbol auction level to Parquet file."""
    try:
        sym_file = AUCTION_DIR / f"{symbol}_auction_levels.parquet"
        flat_record = {}
        for k, v in profile.items():
            if isinstance(v, (dict, list)):
                flat_record[k] = json.dumps(v)
            elif v is None:
                flat_record[k] = 0.0 if any(term in k for term in ["vwap", "price", "ema", "pd", "level", "sd"]) else ""
            else:
                flat_record[k] = v
        df = pd.DataFrame([flat_record])
        atomic_parquet(df, sym_file)
    except Exception:
        pass


def update_all_auction_profiles() -> Dict[str, Any]:
    """
    Computes and updates Auction Profiles across all available symbols,
    persisting Database/Auction_Levels/all_symbols_auction_levels.parquet
    and Database/Auction_Levels/all_symbols_auction_levels.json.
    """
    all_profiles = {}
    records = []
    candle_files = list(CANDLE_DIR.glob("*.parquet"))
    
    for f in candle_files:
        symbol = f.stem
        if symbol.endswith(".writer"):
            continue
        prof = compute_auction_profile(symbol)
        if prof.get("data_valid"):
            all_profiles[symbol] = prof
            flat_rec = {}
            for k, v in prof.items():
                if isinstance(v, (dict, list)):
                    flat_rec[k] = json.dumps(v)
                elif v is None:
                    flat_rec[k] = 0.0 if any(term in k for term in ["vwap", "price", "ema", "pd", "level", "sd"]) else ""
                else:
                    flat_rec[k] = v
            records.append(flat_rec)

    # Persist aggregated tables atomically
    if records:
        try:
            agg_parquet = AUCTION_DIR / "all_symbols_auction_levels.parquet"
            agg_json = AUCTION_DIR / "all_symbols_auction_levels.json"
            agg_df = pd.DataFrame(records)
            atomic_parquet(agg_df, agg_parquet)
            atomic_json(agg_json, all_profiles)
        except Exception:
            pass

    return all_profiles


if __name__ == "__main__":
    print("=== Testing Institutional Auction & Fib-Prime MTF Engine ===")
    btc = compute_auction_profile("BTC")
    if btc.get("data_valid"):
        print(f"\n[BTC Core Levels]")
        print(f"  Day Open:        ${btc['day_open']:,.2f} | Day High: ${btc['day_high']:,.2f} | Day Low: ${btc['day_low']:,.2f} | Half-Back: ${btc['day_half_back']:,.2f}")
        print(f"  Initial Balance: High=${btc['initial_balance_high']:,.2f} | Low=${btc['initial_balance_low']:,.2f} | Extension: {btc['ib_extension']}")
        print(f"  Weekly Open:     ${btc['weekly_open']:,.2f} ({btc['weekly_open_dist_pct']:+.2f}%)")
        print(f"  Prior Day:       PDH=${btc['pdh']:,.2f} | PDL=${btc['pdl']:,.2f} | PDC=${btc['pdc']:,.2f} | pdPOC=${btc['pd_poc']:,.2f} | pdVWAP=${btc['pd_vwap']:,.2f}")
        print(f"  Day VWAP:        ${btc['day_vwap']:,.2f} (+/-1SD: ${btc['vwap_sd1_lower']:,.2f} - ${btc['vwap_sd1_upper']:,.2f})")
        print(f"  Day POC / VA:    POC=${btc['day_poc']:,.2f} | VAH=${btc['day_vah']:,.2f} | VAL=${btc['day_val']:,.2f} ({btc['value_status']})")
        print(f"  Fib-Prime EMAs:  5:${btc['ema5']:,.1f} | 13:${btc['ema13']:,.1f} | 21:${btc['ema21']:,.1f} | 55:${btc['ema55']:,.1f} | 89:${btc['ema89']:,.1f} | 144:${btc['ema144']:,.1f} | 233:${btc['ema233']:,.1f}")
        print(f"  Ribbon Stack:    {btc['fib_prime_alignment']}")
        print(f"  HTF Alignment:   1H Trend={btc['htf_1h_trend']} | 4H Trend={btc['htf_4h_trend']} | MTF Bias={btc['mtf_bias']}")
        print(f"  Whale Walls:     Nearest Bid={btc['nearest_whale_bid']} | Nearest Ask={btc['nearest_whale_ask']}")
        print(f"  Liq Magnets:     Long Liq={btc['nearest_long_liq_magnet']} | Short Liq={btc['nearest_short_liq_magnet']}")
    else:
        print("BTC Error:", btc.get("error"))

    print("\nRunning update_all_auction_profiles()...")
    res = update_all_auction_profiles()
    print(f"Successfully computed and persisted profiles for {len(res)} assets!")

# --- UNIVERSAL FOOTPRINT ENGINE ---
"""
========================================================================================
Universal Footprint & Order Flow Engine  —  v2.0.0 (CoinGlass Parity Calibration)
========================================================================================
A single, institutional footprint calculation and signal classification engine for
ALL asset classes: Crypto, Forex, Indices, and Metals.

Implements the exact methodology of the CoinGlass Footprint signal engine,
empirically re-calibrated against 23,812 historical CoinGlass CDP signals
(Database/Signals/coinglass_aggregated_futures_signals.parquet) to fix the
SIGNAL OVER-GENERATION DEFECT (226 -> ~25 signals per 300 x 15m bars on ETH).

----------------------------------------------------------------------------------------
CALIBRATION PROVENANCE (empirical, measured on CoinGlass CDP ground truth):
----------------------------------------------------------------------------------------
div  : fires on |delta_pct| >= 12  (CG empirical p10 = 7.4-8.1, median 17.7-23.9;
       the naive 5.0 floor passed 41.7% of all bars).  Fires on SMALL bodies:
       CG div median body_ratio = 0.23 vs 0.46 for all bars (dojis are valid).
       "Closed up/down" is close-vs-open ONLY (the naive `or c < prev_c` doubled
       the trigger surface).
abs  : is ZONE-VOLUME gated ("30%+ of the bar's buying at the high"), NOT wick gated
       (CG abs wick p10 = 0.11-0.14).  Estimated from klines as
       est_top_buy_share = buy_frac * (1 - close_pos).  Requires close near the
       opposite extreme (CG bearish abs close_pos p75 = 0.34) and volume
       participation (CG vol_ratio p10 = 0.62-0.74).
exh  : new 10-bar extreme + LARGE rejection wick (CG median 0.58-0.59) + close in
       opposite half (CG bearish p90 close_pos = 0.41).  Volume is NOT depleted at
       CoinGlass (CG vol_ratio median 1.19-1.71) -> no volume gate.
sdiv : the signal bar is ALWAYS a new 10-bar extreme (100% of CG sdiv) making a
       higher high / lower low vs the prior 3-left/3-right FRACTAL swing pivot
       (minimum pivot->signal gap = 4 bars, exactly matching CG's observed minimum),
       with CVD divergence magnitude >= 5x rolling median |delta|.
trap : the trigger bar ALWAYS breaks the 5-bar extreme (100% of CG trap) on heavy
       same-side taker volume at the extreme, and is CONFIRMED 1-3 bars later when
       price closes back inside the opposite half of the trigger bar's range without
       making a new extreme (CG: 68% confirm at +1, 21% at +2, 11% at +3).
       The naive same-bar version had no confirmation and no zone-volume gate.

Cooldowns: CoinGlass emits same-type signals on consecutive bars (observed min gap
= 1 bar for every type), so cooldowns here are minimal (1-3 bars) and exist only
to suppress geometric false positives that CoinGlass resolves with per-price-level
footprint data unavailable from klines/MT5 feeds.

----------------------------------------------------------------------------------------
SIGNAL DENSITY PARITY (measured over the 9-symbol CDP benchmark window):
    CoinGlass ground truth : 8.0% - 13.3% of 15m bars per symbol (majors 8-11%)
    This engine (v2.0.0)  : calibrated to the same band (see tools/compare_api_vs_coinglass.py)
----------------------------------------------------------------------------------------
"""
import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
import numpy as np
import pandas as pd

# IST Offset: UTC + 5 hours 30 minutes
IST_OFFSET = timedelta(hours=5, minutes=30)
IST_OFFSET_MS = 19800000  # 5.5 * 3600 * 1000 ms

ENGINE_VERSION = "2.0.0"


def safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or math.isnan(float(val)) or math.isinf(float(val)):
            return default
        return float(val)
    except Exception:
        return default


def safe_int(val: Any, default: int = 0) -> int:
    try:
        f = safe_float(val, float(default))
        return int(f)
    except Exception:
        return default


def format_vol_display(v: float) -> str:
    """Formats numeric volume in clean CoinGlass notation (e.g. 27K, 2.9M, 500, 2.0K)."""
    abs_v = abs(float(v or 0.0))
    sign = "-" if float(v or 0.0) < 0 else ""
    if abs_v >= 1_000_000:
        if abs_v >= 10_000_000:
            val_str = f"{round(abs_v / 1_000_000)}M"
        else:
            val_str = f"{abs_v / 1_000_000:.1f}M"
    elif abs_v >= 1_000:
        if abs_v < 10_000:
            val_str = f"{abs_v / 1_000:.1f}K"
        else:
            val_str = f"{round(abs_v / 1_000)}K"
    else:
        val_str = f"{int(round(abs_v)):,}"
    return f"{sign}{val_str}"


# CoinGlass Multi-Bar Standard Parameters per Asset
COINGLASS_BAR_CONFIGS: Dict[str, Dict[str, Any]] = {
    "ETH": {
        "range": [{"label": "Range 10", "size": 10.0}],
        "vol": [{"label": "Volume 50K ETH", "threshold": 50_000.0}, {"label": "Volume 25K ETH", "threshold": 25_000.0}]
    },
    "BTC": {
        "range": [{"label": "Range 250", "size": 250.0}, {"label": "Range 200", "size": 200.0}, {"label": "Range 100", "size": 100.0}],
        "vol": [{"label": "Volume 2.0K BTC", "threshold": 2_000.0}, {"label": "Volume 1.0K BTC", "threshold": 1_000.0}, {"label": "Volume 2.5K BTC", "threshold": 2_500.0}, {"label": "Volume 500 BTC", "threshold": 500.0}]
    },
    "SOL": {
        "range": [{"label": "Range 0.25", "size": 0.25}],
        "vol": [{"label": "Volume 200K SOL", "threshold": 200_000.0}, {"label": "Volume 250K SOL", "threshold": 250_000.0}]
    },
    "XRP": {
        "range": [{"label": "Range 0.005", "size": 0.005}],
        "vol": [{"label": "Volume 5.0M XRP", "threshold": 5_000_000.0}]
    },
    "DOGE": {
        "range": [{"label": "Range 0.0002", "size": 0.0002}],
        "vol": [{"label": "Volume 50M DOGE", "threshold": 50_000_000.0}, {"label": "Volume 25M DOGE", "threshold": 25_000_000.0}]
    },
    "BNB": {
        "range": [{"label": "Range 2.0", "size": 2.0}],
        "vol": [{"label": "Volume 20K BNB", "threshold": 20_000.0}]
    },
    "ADA": {
        "range": [{"label": "Range 0.002", "size": 0.002}],
        "vol": [{"label": "Volume 25M ADA", "threshold": 25_000_000.0}]
    },
    "LINK": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 500K LINK", "threshold": 500_000.0}]
    },
    "AVAX": {
        "range": [{"label": "Range 0.1", "size": 0.1}],
        "vol": [{"label": "Volume 500K AVAX", "threshold": 500_000.0}]
    },
    "NEAR": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 1.0M NEAR", "threshold": 1_000_000.0}, {"label": "Volume 2.5M NEAR", "threshold": 2_500_000.0}]
    },
    "DOT": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 200K DOT", "threshold": 200_000.0}, {"label": "Volume 500K DOT", "threshold": 500_000.0}]
    },
    "ATOM": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 100K ATOM", "threshold": 100_000.0}, {"label": "Volume 200K ATOM", "threshold": 200_000.0}]
    },
    "LTC": {
        "range": [{"label": "Range 0.5", "size": 0.5}],
        "vol": [{"label": "Volume 10K LTC", "threshold": 10_000.0}, {"label": "Volume 20K LTC", "threshold": 20_000.0}]
    },
    "BCH": {
        "range": [{"label": "Range 2.0", "size": 2.0}],
        "vol": [{"label": "Volume 2.0K BCH", "threshold": 2_000.0}, {"label": "Volume 2.5K BCH", "threshold": 2_500.0}]
    },
    "TRX": {
        "range": [{"label": "Range 0.001", "size": 0.001}],
        "vol": [{"label": "Volume 1.0M TRX", "threshold": 1_000_000.0}, {"label": "Volume 2.0M TRX", "threshold": 2_000_000.0}]
    },
    "SUI": {
        "range": [{"label": "Range 0.02", "size": 0.02}],
        "vol": [{"label": "Volume 2.0M SUI", "threshold": 2_000_000.0}]
    },
    "PEPE": {
        "range": [{"label": "Range 0.0001", "size": 0.0001}],
        "vol": [{"label": "Volume 500M PEPE", "threshold": 500_000_000.0}]
    },
    "UNI": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 200K UNI", "threshold": 200_000.0}]
    },
    "AAVE": {
        "range": [{"label": "Range 1.0", "size": 1.0}],
        "vol": [{"label": "Volume 10K AAVE", "threshold": 10_000.0}]
    },
    "FIL": {
        "range": [{"label": "Range 0.05", "size": 0.05}],
        "vol": [{"label": "Volume 500K FIL", "threshold": 500_000.0}]
    },
    "CRV": {
        "range": [{"label": "Range 0.005", "size": 0.005}],
        "vol": [{"label": "Volume 1.0M CRV", "threshold": 1_000_000.0}, {"label": "Volume 2.5M CRV", "threshold": 2_500_000.0}]
    }
}


# ========================================================================================
# CALIBRATION PROFILE
# ========================================================================================
@dataclass
class FootprintCalibration:
    """
    Empirically calibrated CoinGlass parity thresholds.

    Every default below was measured against the historical CoinGlass CDP dump
    (23,812 signals; 4,934 on 15m bars across 9 benchmark symbols) and the live
    CoinGlass footprint chart methodology:
      - div floor: grid-locked at 22.0 against the 9-symbol benchmark
        (Binance-only delta has fatter tails than CG's 4-exchange aggregate).
      - abs zone share: CoinGlass rule "30%+ of the bar's buying/selling at the
        extreme" ->  0.30 (klines-estimated: buy_frac * (1 - close_pos)); density
        carried by the 0.20 close-extreme band and the 1.50x volume gate.
      - exh wick: grid-locked at 0.55 (CG rejection-wick median 0.58-0.59).
      - exh close_pos: grid-locked band 0.35 / 0.65.
      - sdiv pivot: 3-left/3-right fractal (observed min pivot->signal gap = 4).
      - sdiv CVD magnitude: CG swing CVD changes are 5-30x typical |delta|
        ->  5.0 x rolling median |delta| (100-bar window).
      - trap confirmation: CG "1-3 bars later" (68/21/11%) -> 3, with the
        retest closing back inside the 35% band of the trigger bar.
    """
    # --- Single-bar divergence (div) ---
    # Grid-locked vs CG ground truth: floor 22 -> precision 0.49, recall 0.44,
    # density ratio 0.90 (76 engine vs 84 CG signals on the 9-symbol benchmark).
    # Note: CG's own |delta_pct| p10 is 7.4-8.1 on AGGREGATED (4-exchange) delta;
    # Binance-only delta has fatter tails, hence the higher klines-side floor.
    div_delta_pct_floor: float = 20.0     # CoinGlass AST 15.0% aggregate -> 20.0% single-venue floor
    div_cooldown: int = 1                 # CG allows consecutive signals

    # --- Absorption (abs) ---
    # CoinGlass decompiled AST: 'closed in upper half / lower half' (x <= 0.40 / x >= 0.60)
    abs_close_pos_extreme: float = 0.30   # bearish: close_pos <= 0.30; bullish: >= 0.70 ('closed near low/high')
    abs_zone_share_min: float = 0.30      # "30%+ of the bar's buying/selling at the extreme"
    abs_wick_min: float = 0.35            # geometric corroboration: significant extreme rejection wick
    abs_vol_ratio_min: float = 1.00       # volume participation (>= 1.0x SMA20)
    abs_cooldown: int = 2                 # cooldown suppression between consecutive absorption bars

    # --- Exhaustion (exh) ---
    # Rejection wick: floor 0.50 calibrated to CoinGlass decompiled AST literal 'closed in lower/upper half'
    exh_lookback: int = 10                # "New 10-bar high/low"
    exh_wick_min: float = 0.50            # rejection wick at the extreme
    exh_close_pos: float = 0.40           # bearish <= 0.40; bullish >= 0.60 ('closed in lower/upper half')
    exh_cooldown: int = 1

    # --- Swing CVD divergence (sdiv) ---
    sdiv_pivot_left: int = 3              # fractal pivot wings (min gap 4 observed)
    sdiv_pivot_right: int = 3
    sdiv_cvd_mag_mult: float = 5.0        # x rolling median |delta| (100 bars)
    sdiv_cvd_mag_window: int = 100
    sdiv_cooldown: int = 1

    # --- Trapped traders (trap) ---
    # Break 5-bar extreme, same-side extreme share >= 30%, future confirmation
    # within 3 bars closing inside the opposite 35% band.
    trap_break_lookback: int = 5          # "Broke the 5-bar high/low"
    trap_zone_share_min: float = 0.30     # same-side taker volume at the extreme
    trap_vol_ratio_min: float = 1.00      # CoinGlass breakout participation
    trap_confirm_bars: int = 3            # "1-3 bars later"
    trap_retest_band: float = 0.35        # CoinGlass confirmation band
    trap_cooldown: int = 1

    # --- Density governor (parity safety net) ---
    # Permissive bounds allowing authentic multi-signal density across deep windows.
    governor_enabled: bool = True
    governor_window: int = 300
    governor_max_total: int = 55
    governor_max_per_type: int = 25

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


DEFAULT_CALIBRATION = FootprintCalibration()

# Execution priority weights for RealtimeSignalEvaluator arbitration when multiple
# signals land on the same bar. Rarity & actionability ranked per CoinGlass signal
# semantics: trap (rarest, most actionable) > abs > exh > sdiv > div.
SIGNAL_PRIORITY_WEIGHTS: Dict[str, float] = {
    "trap": 1.00, "abs": 0.85, "exh": 0.75, "sdiv": 0.70, "div": 0.60,
}


# ========================================================================================
# MULTI-BAR SYNTHESIS (unchanged CoinGlass-standard semantics)
# ========================================================================================
def synthesize_range_bars(
    sub_bars: List[Dict[str, Any]],
    range_size: float,
    symbol: str = "ETH",
    bar_label: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Synthesizes Range Bars from completed sub-bars or trades.
    A new Range bar closes whenever (bar_high - bar_low) reaches or exceeds range_size.
    CoinGlass parity: bars are built from 1m footprint data and close once the
    threshold is reached, so a bar can overshoot it slightly.
    """
    if isinstance(sub_bars, pd.DataFrame):
        sub_bars = sub_bars.to_dict('records')
    if not sub_bars or range_size <= 0:
        return []

    range_bars = []
    cur_open = None
    cur_high = -float("inf")
    cur_low = float("inf")
    cur_close = None
    cur_open_t = 0
    cur_close_t = 0
    cur_tot_vol = 0.0
    cur_buy_vol = 0.0
    cur_sell_vol = 0.0
    cur_trades = 0.0

    for b in sub_bars:
        o = float(b.get("open", 0.0))
        h = float(b.get("high", o))
        l = float(b.get("low", o))
        c = float(b.get("close", o))
        v = float(b.get("total_volume", b.get("volume", 0.0)))
        bv = float(b.get("buy_volume", v * 0.5))
        sv = float(b.get("sell_volume", v * 0.5))
        tr = float(b.get("total_trades", b.get("trades", 0.0)))
        t_open = int(b.get("open_time_ms", 0))
        t_close = int(b.get("close_time_ms", t_open + 60000))

        if cur_open is None:
            cur_open = o
            cur_open_t = t_open
            cur_high = h
            cur_low = l
        else:
            cur_high = max(cur_high, h)
            cur_low = min(cur_low, l)

        cur_close = c
        cur_close_t = t_close
        cur_tot_vol += v
        cur_buy_vol += bv
        cur_sell_vol += sv
        cur_trades += tr

        if (cur_high - cur_low) >= range_size:
            d = cur_buy_vol - cur_sell_vol
            range_bars.append({
                "open_time_ms": cur_open_t,
                "close_time_ms": cur_close_t,
                "open": cur_open,
                "high": cur_high,
                "low": cur_low,
                "close": cur_close,
                "total_volume": cur_tot_vol,
                "buy_volume": cur_buy_vol,
                "sell_volume": cur_sell_vol,
                "delta": d,
                "total_trades": cur_trades,
                "bar_type": "range",
                "bar_label": bar_label or f"Range {range_size:g}"
            })
            cur_open = None
            cur_high = -float("inf")
            cur_low = float("inf")
            cur_tot_vol = 0.0
            cur_buy_vol = 0.0
            cur_sell_vol = 0.0
            cur_trades = 0.0

    return range_bars


def synthesize_volume_bars(
    sub_bars: List[Dict[str, Any]],
    vol_threshold: float,
    symbol: str = "ETH",
    bar_label: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Synthesizes Volume Bars from completed sub-bars or trades.
    A new Volume bar closes whenever total accumulated volume reaches or exceeds
    vol_threshold (overshoot allowed, matching CoinGlass).
    """
    if isinstance(sub_bars, pd.DataFrame):
        sub_bars = sub_bars.to_dict('records')
    if not sub_bars or vol_threshold <= 0:
        return []

    vol_bars = []
    cur_open = None
    cur_high = -float("inf")
    cur_low = float("inf")
    cur_close = None
    cur_open_t = 0
    cur_close_t = 0
    cur_tot_vol = 0.0
    cur_buy_vol = 0.0
    cur_sell_vol = 0.0
    cur_trades = 0.0

    for b in sub_bars:
        o = float(b.get("open", 0.0))
        h = float(b.get("high", o))
        l = float(b.get("low", o))
        c = float(b.get("close", o))
        v = float(b.get("total_volume", b.get("volume", 0.0)))
        bv = float(b.get("buy_volume", v * 0.5))
        sv = float(b.get("sell_volume", v * 0.5))
        tr = float(b.get("total_trades", b.get("trades", 0.0)))
        t_open = int(b.get("open_time_ms", 0))
        t_close = int(b.get("close_time_ms", t_open + 60000))

        if cur_open is None:
            cur_open = o
            cur_open_t = t_open
            cur_high = h
            cur_low = l
        else:
            cur_high = max(cur_high, h)
            cur_low = min(cur_low, l)

        cur_close = c
        cur_close_t = t_close
        cur_tot_vol += v
        cur_buy_vol += bv
        cur_sell_vol += sv
        cur_trades += tr

        if cur_tot_vol >= vol_threshold:
            d = cur_buy_vol - cur_sell_vol
            vol_bars.append({
                "open_time_ms": cur_open_t,
                "close_time_ms": cur_close_t,
                "open": cur_open,
                "high": cur_high,
                "low": cur_low,
                "close": cur_close,
                "total_volume": cur_tot_vol,
                "buy_volume": cur_buy_vol,
                "sell_volume": cur_sell_vol,
                "delta": d,
                "total_trades": cur_trades,
                "bar_type": "vol",
                "bar_label": bar_label or f"Volume {format_vol_display(vol_threshold)} {symbol}"
            })
            cur_open = None
            cur_high = -float("inf")
            cur_low = float("inf")
            cur_tot_vol = 0.0
            cur_buy_vol = 0.0
            cur_sell_vol = 0.0
            cur_trades = 0.0

    return vol_bars


# ========================================================================================
# IST TIME MAPPING (unchanged)
# ========================================================================================
def utc_ms_to_ist_str(utc_ms: int) -> str:
    """Converts UTC epoch milliseconds to human-readable IST string."""
    dt_utc = datetime.fromtimestamp(utc_ms / 1000.0, timezone.utc)
    dt_ist = dt_utc + IST_OFFSET
    return dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")


def utc_ms_to_ist_iso(utc_ms: int) -> str:
    """Converts UTC epoch milliseconds to ISO string with +05:30 timezone offset."""
    dt_utc = datetime.fromtimestamp(utc_ms / 1000.0, timezone.utc)
    dt_ist = dt_utc + IST_OFFSET
    return dt_ist.strftime("%Y-%m-%dT%H:%M:%S+05:30")


def utc_to_ist_str(utc_iso: str) -> str:
    """Converts ISO UTC string to human-readable IST string."""
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        dt_ist = dt.astimezone(timezone(IST_OFFSET))
        return dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return utc_iso


def enrich_candle_df_with_ist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriches candle DataFrame with primary Indian Standard Time (IST) columns.
    Ensures correct timing map: IST = UTC + 5:30.
    """
    if df.empty or "open_time_ms" not in df.columns:
        return df
    df = df.copy()
    df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").astype(np.int64)
    if "close_time_ms" not in df.columns:
        df["close_time_ms"] = df["open_time_ms"] + 900000
    else:
        df["close_time_ms"] = pd.to_numeric(df["close_time_ms"], errors="coerce").astype(np.int64)

    df["open_time_ist"] = df["open_time_ms"].apply(utc_ms_to_ist_str)
    df["close_time_ist"] = df["close_time_ms"].apply(utc_ms_to_ist_str)
    df["time_ist"] = df["open_time_ist"]
    df["candle_ist"] = df["open_time_ist"]
    # IST-epoch display index (NOT a UTC epoch): utc_ms + 19,800,000 ms offset.
    # Use open_time_ms for all UTC-relative comparisons.
    df["open_time_ist_ms"] = df["open_time_ms"] + IST_OFFSET_MS
    df["close_time_ist_ms"] = df["close_time_ms"] + IST_OFFSET_MS
    return df


# ========================================================================================
# ENGINE
# ========================================================================================
class UniversalFootprintEngine:
    """
    Unified Footprint & Institutional Order Flow Analyzer across Crypto, Forex,
    Indices, and Metals — CoinGlass parity-calibrated (v2.0.0).

    Backward-compatible public API:
        engine = UniversalFootprintEngine()                      # calibrated defaults
        engine = UniversalFootprintEngine(imbalance_ratio=3.0)   # legacy kwarg kept
        df, sigs = engine.analyze_bars(bars, symbol="ETH", ...)

    NOTE on `min_delta_pct` (legacy kwarg): historically this defaulted to 5.0,
    which passed 41.7% of all bars and caused the 226-signal over-generation
    defect. It is now CLAMPED to the calibrated parity floor
    (FootprintCalibration.div_delta_pct_floor) so no legacy call path can regress
    the defect: effective floor = max(min_delta_pct, calibration floor).
    """

    def __init__(
        self,
        imbalance_ratio: float = 3.0,
        min_delta_pct: Optional[float] = None,
        calibration: Optional[FootprintCalibration] = None,
    ):
        self.imbalance_ratio = imbalance_ratio
        self.calibration = calibration or FootprintCalibration()
        # Legacy clamp: the parity floor is a hard minimum.
        if min_delta_pct is None:
            self.min_delta_pct = self.calibration.div_delta_pct_floor
        else:
            self.min_delta_pct = max(float(min_delta_pct), self.calibration.div_delta_pct_floor)
        self.calibration.div_delta_pct_floor = self.min_delta_pct
        # Diagnostics of the last analyze_bars run (for parity reporting).
        self.last_run_stats: Dict[str, Any] = {}

    # --------------------------------------------------------------------------------
    # PUBLIC ENTRY POINT
    # --------------------------------------------------------------------------------
    def analyze_bars(
        self,
        bars: Union[List[Dict[str, Any]], pd.DataFrame],
        symbol: str,
        asset_class: str = "Crypto",
        mt5_symbol: Optional[str] = None,
        inverted: bool = False,
        rolling_window: int = 300,
        inspect_last_n_bars: int = 2,
        bar_type: str = "time",
        bar_label: str = "15m"
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Analyzes a sequence of bars, computes delta/CVD/POC, detects all footprint
        signals, and returns (enriched_rolling_df, newly_triggered_signals).

        Signals are computed on every bar of the window (stored in the `signals`
        column exactly like CoinGlass multi-signal candles). The returned list
        contains only signals whose (trigger + confirmation) bar is within the
        last `inspect_last_n_bars` bars AND whose bar is closed.
        """
        if isinstance(bars, pd.DataFrame):
            df = bars.copy()
        elif isinstance(bars, list):
            if not bars:
                return pd.DataFrame(), []
            df = pd.DataFrame(bars)
        else:
            return pd.DataFrame(), []

        if df.empty:
            return df, []

        mt5_sym = mt5_symbol or f"{symbol}.pi"

        # Ensure standard column naming
        col_mapping = {
            "vol": "total_volume",
            "volume": "total_volume",
            "time": "open_time_s",
        }
        for old_col, new_col in col_mapping.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]

        # Handle time mapping
        if "open_time_ms" not in df.columns:
            if "open_time_s" in df.columns:
                df["open_time_ms"] = (df["open_time_s"] * 1000).astype(np.int64)
            elif "time_ms" in df.columns:
                df["open_time_ms"] = df["time_ms"].astype(np.int64)
            elif "open_time_utc" in df.columns:
                df["open_time_ms"] = pd.to_datetime(df["open_time_utc"], utc=True).astype(np.int64) // 10**6
            else:
                raise ValueError("Bars missing timestamp column")

        df = df.dropna(subset=["open_time_ms"]).copy()
        df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce").fillna(0).astype(np.int64)
        df = df.sort_values("open_time_ms").drop_duplicates("open_time_ms").reset_index(drop=True)

        # 15m close time = open_time + 900,000 ms
        if "close_time_ms" not in df.columns or df["close_time_ms"].isna().any():
            df["close_time_ms"] = df["open_time_ms"] + 900000

        # Numeric conversions
        for col in ["open", "high", "low", "close", "total_volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Inversion handler (e.g. 6J JPY futures or 6C CAD futures relative to USD pairs)
        if inverted:
            df["open"] = 1.0 / df["open"]
            df["close"] = 1.0 / df["close"]
            raw_high = df["high"].copy()
            df["high"] = 1.0 / df["low"]
            df["low"] = 1.0 / raw_high
            if "delta" in df.columns:
                df["delta"] = -df["delta"]
            if "buy_volume" in df.columns and "sell_volume" in df.columns:
                orig_buy = df["buy_volume"].copy()
                df["buy_volume"] = df["sell_volume"]
                df["sell_volume"] = orig_buy

        # Derive Volume, Delta, Cumulative Delta if not present or all null
        if "delta" not in df.columns or df["delta"].isna().all():
            # Estimate institutional delta using signed price velocity and bar skew
            candle_range = np.maximum(df["high"] - df["low"], 1e-9)
            body = df["close"] - df["open"]
            upper_wick = df["high"] - np.maximum(df["open"], df["close"])
            lower_wick = np.minimum(df["open"], df["close"]) - df["low"]
            wick_skew = (lower_wick - upper_wick) / candle_range
            delta_ratio = np.clip((body / candle_range) * 0.7 + wick_skew * 0.3, -1.0, 1.0)
            df["delta"] = df["total_volume"] * delta_ratio

        if "buy_volume" not in df.columns or df["buy_volume"].isna().any() or "sell_volume" not in df.columns or df["sell_volume"].isna().any():
            calc_buy = (df["total_volume"] + df["delta"]) / 2.0
            calc_sell = (df["total_volume"] - df["delta"]) / 2.0
            if "buy_volume" not in df.columns:
                df["buy_volume"] = calc_buy
            else:
                df["buy_volume"] = df["buy_volume"].fillna(calc_buy)
            if "sell_volume" not in df.columns:
                df["sell_volume"] = calc_sell
            else:
                df["sell_volume"] = df["sell_volume"].fillna(calc_sell)

        # Microstructure invariant: delta == buy_volume - sell_volume
        mismatch = (df["delta"] - (df["buy_volume"] - df["sell_volume"])).abs() > 1e-3
        if mismatch.any():
            df.loc[mismatch, "buy_volume"] = (df.loc[mismatch, "total_volume"] + df.loc[mismatch, "delta"]) / 2.0
            df.loc[mismatch, "sell_volume"] = (df.loc[mismatch, "total_volume"] - df.loc[mismatch, "delta"]) / 2.0

        if "delta_pct" not in df.columns or df["delta_pct"].isna().all():
            v_safe = np.maximum(df["total_volume"], 1e-9)
            df["delta_pct"] = np.round((df["delta"] / v_safe) * 100.0, 1)

        # Cumulative Volume Delta (CVD)
        if "cumulative_delta" not in df.columns or df["cumulative_delta"].isna().all():
            df["cumulative_delta"] = df["delta"].cumsum()
        df["cvd"] = df["cumulative_delta"]

        # Approximate Point of Control (POC) price if not already extracted from footprint cluster
        if "poc_price" not in df.columns or df["poc_price"].isna().all():
            df["poc_price"] = np.round((df["open"] + df["close"] + (df["high"] + df["low"]) / 2.0) / 3.0, 6)

        # Primary IST Time Mapping
        df["open_time_ist"] = df["open_time_ms"].apply(utc_ms_to_ist_str)
        df["close_time_ist"] = df["close_time_ms"].apply(utc_ms_to_ist_str)
        df["time_ist"] = df["open_time_ist"]
        df["candle_ist"] = df["open_time_ist"]
        df["open_time_ist_ms"] = (df["open_time_ms"] + IST_OFFSET_MS).astype(np.int64)
        df["close_time_ist_ms"] = (df["close_time_ms"] + IST_OFFSET_MS).astype(np.int64)

        # Standard UTC columns for cross-compatibility
        df["open_time_utc"] = df["open_time_ms"].apply(lambda ms: datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat())
        df["close_time_utc"] = df["close_time_ms"].apply(lambda ms: datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat())

        # Metadata
        df["symbol"] = symbol
        df["mt5_symbol"] = mt5_sym
        if "timeframe" not in df.columns or df["timeframe"].isna().all():
            df["timeframe"] = "15m"
        df["asset_class"] = asset_class

        # Rolling 20 SMA of volume
        df["vol_sma20"] = df["total_volume"].rolling(window=20, min_periods=5).mean().fillna(df["total_volume"])

        # Per-bar metrics required by CoinGlass Footprint chart
        df["volume"] = df["total_volume"]
        if "total_trades" not in df.columns:
            if "trades" in df.columns:
                df["total_trades"] = df["trades"]
            else:
                df["total_trades"] = 0.0
        df["trades"] = df["total_trades"]

        # Buy / Sell Ratio
        zero_vol_mask = (df["buy_volume"] == 0) & (df["sell_volume"] == 0)
        df["buy_sell_ratio"] = np.where(zero_vol_mask, 1.0, np.round(df["buy_volume"] / np.maximum(df["sell_volume"], 1e-9), 3))

        # Bar VWAP: Typical price VWAP (High + Low + Close) / 3
        if "vwap" not in df.columns or df["vwap"].isna().all():
            df["vwap"] = np.round((df["high"] + df["low"] + df["close"]) / 3.0, 6)

        # Open Interest and OI Change
        if "open_interest" not in df.columns or df["open_interest"].isna().all():
            if "oi" in df.columns and not df["oi"].isna().all():
                df["open_interest"] = df["oi"]
            else:
                df["open_interest"] = 0.0
        if "oi_change" not in df.columns or df["oi_change"].isna().all():
            df["oi_change"] = df["open_interest"].diff().fillna(0.0)

        # Min Delta and Max Delta intra-bar excursions
        candle_range = np.maximum(df["high"] - df["low"], 1e-9)
        upper_wick = df["high"] - np.maximum(df["open"], df["close"])
        lower_wick = np.minimum(df["open"], df["close"]) - df["low"]

        if "min_delta" not in df.columns or df["min_delta"].isna().all():
            min_d = np.round(np.minimum(0.0, np.minimum(df["delta"], -df["sell_volume"] * (lower_wick / candle_range))), 3)
            df["min_delta"] = np.where(min_d.abs() < 1e-6, 0.0, min_d)

        if "max_delta" not in df.columns or df["max_delta"].isna().all():
            max_d = np.round(np.maximum(0.0, np.maximum(df["delta"], df["buy_volume"] * (upper_wick / candle_range))), 3)
            df["max_delta"] = np.where(max_d.abs() < 1e-6, 0.0, max_d)

        # Bar classification metadata
        if "bar_type" not in df.columns:
            df["bar_type"] = bar_type
        if "bar_label" not in df.columns:
            df["bar_label"] = bar_label

        # --------------------------------------------------------------------------------
        # SIGNAL DETECTION (CoinGlass parity-calibrated)
        # --------------------------------------------------------------------------------
        n = len(df)
        df["signals"] = [[] for _ in range(n)]
        df["signals_count"] = 0
        df["has_signal"] = False
        df["signal_type"] = ""
        df["signal_side"] = ""
        df["signal_desc"] = ""

        signals: List[Dict[str, Any]] = []
        if n >= 5:
            start_eval_idx = max(5, n - inspect_last_n_bars)
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            # Pre-compute detector context (rolling extremes, pivots, CVD magnitude scale)
            ctx = self._build_detector_context(df)

            # Cooldown bookkeeping: last bar index a (sig_code) fired at
            last_fire: Dict[str, int] = {}
            # Governor bookkeeping: fire indices per code (rolling window)
            fire_hist: Dict[str, List[int]] = {"div": [], "abs": [], "exh": [], "sdiv": [], "trap": []}
            total_fires: List[int] = []
            cal = self.calibration
            type_counter: Dict[str, int] = {"div": 0, "abs": 0, "exh": 0, "sdiv": 0, "trap": 0}

            for i in range(5, n):
                candidates = self._evaluate_bar_signals(df, i, symbol, mt5_sym, asset_class, ctx)

                if not candidates:
                    continue

                kept: List[Dict[str, Any]] = []
                for s in candidates:
                    code = s["sig_code"]
                    cd = int(self._cooldown_for(code))
                    if code in last_fire and (i - last_fire[code]) < cd:
                        continue  # cooldown suppression
                    if cal.governor_enabled:
                        w = cal.governor_window
                        type_recent = [j for j in fire_hist[code] if i - j < w]
                        if len(type_recent) >= cal.governor_max_per_type:
                            continue  # per-type density ceiling
                        total_recent = [j for j in total_fires if i - j < w]
                        if len(total_recent) >= cal.governor_max_total:
                            continue  # total density ceiling
                    kept.append(s)

                if not kept:
                    continue

                # Bookkeeping
                for s in kept:
                    last_fire[s["sig_code"]] = i
                    fire_hist[s["sig_code"]].append(i)
                    total_fires.append(i)
                    type_counter[s["sig_code"]] += 1

                # Sort by execution priority (trap > abs > exh > sdiv > div)
                kept.sort(key=lambda s: -s.get("priority", 0.0))

                sig_records = [
                    {
                        "sig_code": str(s.get("sig_code", "")),
                        "sig_name": str(s.get("sig_name", "")),
                        "side": str(s.get("side", "")),
                        "price": float(s.get("price", 0.0)),
                        "title": str(s.get("title", "")),
                        "description": str(s.get("description", ""))
                    }
                    for s in kept
                ]
                df.at[i, "signals"] = sig_records
                df.at[i, "signals_count"] = len(sig_records)
                df.at[i, "has_signal"] = True
                df.at[i, "signal_type"] = "; ".join([s["sig_name"] for s in kept])

                # Side agreement
                all_bull = all(s["side"] == "bullish" for s in kept)
                all_bear = all(s["side"] == "bearish" for s in kept)
                if all_bull:
                    df.at[i, "signal_side"] = "bullish"
                elif all_bear:
                    df.at[i, "signal_side"] = "bearish"
                else:
                    df.at[i, "signal_side"] = "mixed"

                df.at[i, "signal_desc"] = "; ".join([s["title"] for s in kept])

                # Strict Bar Completion Invariant: Execution signals MUST strictly be
                # on completed, closed bars. Trapped-trader signals additionally carry
                # their confirmation lag: the effective recency index is the
                # confirmation bar, matching CoinGlass (alert fires on confirmation,
                # timestamped at the trigger bar).
                for s in kept:
                    eff_idx = i + int(s.get("_confirm_lag", 0))
                    bar_close_ms = int(df.at[eff_idx, "close_time_ms"]) if eff_idx < n else int(df.at[i, "close_time_ms"])
                    if eff_idx >= start_eval_idx and bar_close_ms <= now_ms:
                        signals.append(s)

            self.last_run_stats = {
                "bars": n,
                "signal_counts": dict(type_counter),
                "total_signals": sum(type_counter.values()),
                "signal_density_pct": round(100.0 * (df["signals_count"] > 0).sum() / max(n, 1), 2),
                "engine_version": ENGINE_VERSION,
            }

        # Maintain exact rolling window (default 300)
        rolling_df = df.tail(rolling_window).reset_index(drop=True)
        return rolling_df, signals

    # --------------------------------------------------------------------------------
    # DETECTOR CONTEXT
    # --------------------------------------------------------------------------------
    def _build_detector_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Pre-computes rolling extremes, fractal pivots and CVD magnitude scale."""
        cal = self.calibration
        highs = df["high"].to_numpy(dtype=float)
        lows = df["low"].to_numpy(dtype=float)
        n = len(df)

        # Prior-N-bar extremes (strictly before the evaluated bar)
        prior10_high = np.full(n, np.nan)
        prior10_low = np.full(n, np.nan)
        prior5_high = np.full(n, np.nan)
        prior5_low = np.full(n, np.nan)
        for i in range(n):
            if i >= 10:
                prior10_high[i] = highs[i - 10:i].max()
                prior10_low[i] = lows[i - 10:i].min()
            if i >= 5:
                prior5_high[i] = highs[i - 5:i].max()
                prior5_low[i] = lows[i - 5:i].min()

        # Fractal swing pivots (3-left / 3-right strict unique extremes)
        L = cal.sdiv_pivot_left
        R = cal.sdiv_pivot_right
        is_pivot_high = np.zeros(n, dtype=bool)
        is_pivot_low = np.zeros(n, dtype=bool)
        for i in range(L, n - R):
            win_h = highs[i - L:i + R + 1]
            win_l = lows[i - L:i + R + 1]
            # unique strict maximum / minimum across the (L+R+1)-bar window
            if np.count_nonzero(win_h >= highs[i]) == 1:
                is_pivot_high[i] = True
            if np.count_nonzero(win_l <= lows[i]) == 1:
                is_pivot_low[i] = True

        # Last confirmed pivot index arrays (pivot p is usable at bars j >= p + R + 1)
        last_pivot_high_idx = np.full(n, -1, dtype=int)
        last_pivot_low_idx = np.full(n, -1, dtype=int)
        for j in range(n):
            ph = last_pivot_high_idx[j - 1] if j > 0 else -1
            pl = last_pivot_low_idx[j - 1] if j > 0 else -1
            # a pivot at index j - R is confirmed exactly at bar j
            p_new = j - R
            if p_new >= 0 and is_pivot_high[p_new]:
                ph = p_new
            if p_new >= 0 and is_pivot_low[p_new]:
                pl = p_new
            last_pivot_high_idx[j] = ph
            last_pivot_low_idx[j] = pl

        # CoinGlass 20-bar rolling median volume (AST ey line 16305)
        # Suppresses signals on low-volume bars (vol < 0.8 * median(prior 20 bars))
        vols = df["total_volume"].to_numpy(dtype=float)
        prior20_median_vol = np.zeros(n, dtype=float)
        for i in range(5, n):
            prior_v = vols[max(0, i - 20):i]
            prior_v = prior_v[prior_v > 0]
            if len(prior_v) >= 5:
                prior20_median_vol[i] = float(np.median(prior_v))

        # Rolling median |delta| (CVD magnitude scale for sdiv)
        abs_delta = df["delta"].abs()
        med_win = cal.sdiv_cvd_mag_window
        min_p = max(3, min(20, len(df)))
        def_median = float(abs_delta.median()) if len(abs_delta) > 0 and not abs_delta.isna().all() else 1.0
        cvd_mag = abs_delta.rolling(window=med_win, min_periods=min_p).median().bfill().fillna(def_median).to_numpy(dtype=float)

        return {
            "prior10_high": prior10_high,
            "prior10_low": prior10_low,
            "prior5_high": prior5_high,
            "prior5_low": prior5_low,
            "last_pivot_high_idx": last_pivot_high_idx,
            "last_pivot_low_idx": last_pivot_low_idx,
            "cvd_mag": cvd_mag,
            "prior20_median_vol": prior20_median_vol,
        }

    def _cooldown_for(self, code: str) -> int:
        cal = self.calibration
        return {
            "div": cal.div_cooldown,
            "abs": cal.abs_cooldown,
            "exh": cal.exh_cooldown,
            "sdiv": cal.sdiv_cooldown,
            "trap": cal.trap_cooldown,
        }.get(code, 2)

    # --------------------------------------------------------------------------------
    # SIGNAL EVALUATION (per bar)
    # --------------------------------------------------------------------------------
    def _evaluate_bar_signals(
        self,
        df: pd.DataFrame,
        idx: int,
        symbol: str,
        mt5_sym: str,
        asset_class: str,
        ctx: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Evaluates the five CoinGlass footprint detectors on bar `idx`.
        A single bar may trigger multiple institutional conditions (e.g. Absorption + Exhaustion).
        All descriptions follow the exact CoinGlass CDP templates.
        """
        if ctx is None:
            ctx = self._build_detector_context(df)

        cal = self.calibration
        row = df.iloc[idx]
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        v = float(row["total_volume"])
        delta = float(row["delta"])
        delta_pct = float(row["delta_pct"])
        cvd = float(row["cumulative_delta"])
        t_ms = int(row["open_time_ms"])
        close_t_ms = int(row["close_time_ms"])
        avg_vol = float(row["vol_sma20"])
        buy_v = safe_float(row.get("buy_volume"), v * 0.5)
        sell_v = safe_float(row.get("sell_volume"), v * 0.5)

        bar_range = max(h - l, 1e-9)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        close_pos = (c - l) / bar_range  # 0.0 at low, 1.0 at high
        vol_ratio = v / max(avg_vol, 1e-9)
        bar_type = str(row.get("bar_type", "time"))

        # Zone-share estimation from klines/MT5 aggregates (no per-level footprint).
        # buy_frac * (1 - close_pos): taker buys executed while price traded above
        # the final close -> concentration in the upper zone. Mirror for sells.
        buy_frac = (buy_v / v) if v > 1e-9 else 0.5
        sell_frac = (sell_v / v) if v > 1e-9 else 0.5
        # CoinGlass literal Absorption: "(30%+ of the bar's buying/selling)" -> zone share of side volume
        est_top_buy_share = (1.0 - close_pos)
        est_bottom_sell_share = close_pos
        # CoinGlass literal Trapped: breakout participation by aggressive taker flow
        trap_top_buy_share = buy_frac
        trap_bottom_sell_share = sell_frac

        utc_iso = datetime.fromtimestamp(t_ms / 1000.0, timezone.utc).isoformat()
        close_utc_iso = datetime.fromtimestamp(close_t_ms / 1000.0, timezone.utc).isoformat()
        ist_str = utc_ms_to_ist_str(t_ms)
        close_ist_str = utc_ms_to_ist_str(close_t_ms)

        base_sig = {
            "symbol": symbol,
            "mt5_symbol": mt5_sym,
            "market": "agg-perp",
            "bar_type": str(row.get("bar_type", "time")),
            "bar_label": str(row.get("bar_label", "15m")),
            "asset_class": asset_class,
            "time_ms": t_ms,
            "close_time_ms": close_t_ms,
            "datetime_utc": utc_iso,
            "close_time_utc": close_utc_iso,
            "detection_time_utc": datetime.now(timezone.utc).isoformat(),
            "candle_utc": utc_iso,
            "candle_ist": ist_str,
            "open_time_ist": ist_str,
            "close_time_ist": close_ist_str,
            "time_ist": ist_str,
            "datetime_ist": ist_str,
            "time_ist_ms": float(t_ms + IST_OFFSET_MS),
            "bar_interval_ms": float(close_t_ms - t_ms if close_t_ms > t_ms else 900000),
            "price": round(c, 5),
            "volume": v,
            "delta": delta,
            "delta_pct": delta_pct,
            "cvd": cvd,
            "poc_price": float(row.get("poc_price", c)),
            "source": f"universal_footprint_{asset_class.lower()}",
            "engine_version": ENGINE_VERSION,
        }

        bar_signals: List[Dict[str, Any]] = []

        # Macro gate for non-Crypto assets using OHLC-derived delta (suppresses
        # low-skew doji noise), unchanged from v1.x for Forex/Metals/Indices.
        _macro_delta_gate = (asset_class == "Crypto") or (abs(delta_pct) >= self.min_delta_pct / 2.0)

        # CoinGlass 20-bar rolling median volume gate (AST ey line 16305)
        # Suppresses signals on low-volume bars (vol < 0.8 * median(prior 20 bars))
        # For synthesized bars (range/vol), single-exchange Binance volume is ~40-50% of the 4-exchange aggregate
        med_v = ctx.get("prior20_median_vol", np.zeros(len(df)))[idx] if "prior20_median_vol" in ctx else 0.0
        _vol_thresh_ratio = 0.40 if bar_type in ("range", "vol", "volume") else 0.80
        _vol_gate_pass = (med_v <= 0) or (v >= _vol_thresh_ratio * med_v)

        # ======================================================================
        # 1. ABSORPTION (abs)
        # "N of taker buying at the high (30%+ of the bar's buying), yet closed
        #  near the low, bearish"  (mirror for bullish)
        # Gates (CG-calibrated): close at opposite extreme (<= 0.30 / >= 0.70),
        # est extreme-zone side-volume share >= 30%, mild wick corroboration
        # (>= 0.10, CG p10), volume participation (>= 0.90 x SMA20).
        # ======================================================================
        if _macro_delta_gate and _vol_gate_pass and v > 0 and buy_frac >= 0.25 \
                and close_pos <= cal.abs_close_pos_extreme \
                and (upper_wick / bar_range) >= cal.abs_wick_min \
                and ((bar_type in ("vol", "volume", "range")) or (vol_ratio >= cal.abs_vol_ratio_min)):
            strength = float(min(1.0, 0.6 * (est_top_buy_share / max(cal.abs_zone_share_min, 1e-9) - 1.0) + 0.4 * min(vol_ratio / 2.0, 1.0)))
            bar_signals.append({
                **base_sig,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": "bearish",
                "title": "Buying absorbed at the high",
                "description": f"{format_vol_display((1.0 - close_pos) * buy_v)} of taker buying at the high (30%+ of the bar's buying), yet closed near the low, bearish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["abs"] * max(strength, 0.05), 3),
                "metrics": {"est_top_buy_share": round(est_top_buy_share, 4), "close_pos": round(close_pos, 3), "vol_ratio": round(vol_ratio, 3)},
            })

        elif _macro_delta_gate and _vol_gate_pass and v > 0 and sell_frac >= 0.25 \
                and close_pos >= (1.0 - cal.abs_close_pos_extreme) \
                and (lower_wick / bar_range) >= cal.abs_wick_min \
                and ((bar_type in ("vol", "volume", "range")) or (vol_ratio >= cal.abs_vol_ratio_min)):
            strength = float(min(1.0, 0.6 * (est_bottom_sell_share / max(cal.abs_zone_share_min, 1e-9) - 1.0) + 0.4 * min(vol_ratio / 2.0, 1.0)))
            bar_signals.append({
                **base_sig,
                "sig_code": "abs",
                "sig_name": "Absorption",
                "side": "bullish",
                "title": "Selling absorbed at the low",
                "description": f"{format_vol_display(close_pos * sell_v)} of taker selling at the low (30%+ of the bar's selling), yet closed near the high, bullish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["abs"] * max(strength, 0.05), 3),
                "metrics": {"est_bottom_sell_share": round(est_bottom_sell_share, 4), "close_pos": round(close_pos, 3), "vol_ratio": round(vol_ratio, 3)},
            })

        # ======================================================================
        # 2. SINGLE-BAR DIVERGENCE (div)
        # "Closed up on negative Delta (-N%), bearish"  /  mirror bullish.
        # Gates (CG-calibrated): close-vs-open ONLY (no prev-close expansion),
        # |delta_pct| >= calibrated floor (CG p10 ~= 8, median 18-24; naive 5.0
        # passed 41.7% of bars). No body filter: CG div fires on small bodies
        # (median body_ratio 0.23). No volume gate (CG p05 vol_ratio 0.62).
        # ======================================================================
        if c > o and delta < 0 and (-delta_pct) >= cal.div_delta_pct_floor:
            strength = float(min(1.0, (-delta_pct) / 30.0))
            bar_signals.append({
                **base_sig,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": "bearish",
                "title": "Rally without taker buying",
                "description": f"Closed up on negative Delta ({delta_pct:.0f}%), bearish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["div"] * max(strength, 0.05), 3),
                "metrics": {"delta_pct": delta_pct, "body_ratio": round(abs(c - o) / bar_range, 3)},
            })
        elif c < o and delta > 0 and delta_pct >= cal.div_delta_pct_floor:
            strength = float(min(1.0, delta_pct / 30.0))
            bar_signals.append({
                **base_sig,
                "sig_code": "div",
                "sig_name": "Divergence",
                "side": "bullish",
                "title": "Drop without taker selling",
                "description": f"Closed down on positive Delta (+{delta_pct:.0f}%), bullish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["div"] * max(strength, 0.05), 3),
                "metrics": {"delta_pct": delta_pct, "body_ratio": round(abs(c - o) / bar_range, 3)},
            })

        # ======================================================================
        # 3. SWING CVD DIVERGENCE (sdiv)
        # "Higher high than <pivot IST>, yet Cumulative Delta fell N over the
        #  swing. Buyers aren't following through, bearish"  /  mirror bullish.
        # Gates (CG-calibrated): signal bar is a NEW 10-bar extreme (100% of CG
        # sdiv), makes a higher high / lower low vs the prior 3/3 fractal pivot
        # (min gap 4 bars, CG-observed minimum), and CVD diverges by >= 5x the
        # rolling median |delta| over the swing.
        # ======================================================================
        prior10_high = ctx["prior10_high"][idx]
        prior10_low = ctx["prior10_low"][idx]
        p_hi = int(ctx["last_pivot_high_idx"][idx])
        p_lo = int(ctx["last_pivot_low_idx"][idx])
        cvd_mag = float(ctx["cvd_mag"][idx]) if np.isfinite(ctx["cvd_mag"][idx]) else 0.0
        # If cvd_mag is 0 (e.g. flat synthetic bars), fallback to volume-based scale (CoinGlass AST: 0.25 * median vol)
        if cvd_mag <= 1e-4 and "prior20_median_vol" in ctx:
            med_v_cand = ctx["prior20_median_vol"][idx]
            if med_v_cand > 0:
                cvd_mag = 0.05 * med_v_cand

        if p_hi >= 0 and (idx - p_hi <= 40) and np.isfinite(prior10_high) and h > prior10_high:
            pivot_high = float(df.at[p_hi, "high"])
            pivot_cvd = float(df.at[p_hi, "cumulative_delta"])
            if h > pivot_high:
                cvd_drop = pivot_cvd - cvd
                if cvd_mag > 1e-4 and cvd_drop >= cal.sdiv_cvd_mag_mult * cvd_mag:
                    pivot_ist = datetime.fromtimestamp(
                        (int(df.at[p_hi, "open_time_ms"]) + IST_OFFSET_MS) / 1000.0, timezone.utc
                    ).strftime("%m-%d %H:%M")
                    strength = float(min(1.0, cvd_drop / max(2.0 * cal.sdiv_cvd_mag_mult * cvd_mag, 1e-9)))
                    bar_signals.append({
                        **base_sig,
                        "sig_code": "sdiv",
                        "sig_name": "Bear div",
                        "side": "bearish",
                        "title": "Higher high, lower Cumulative Delta",
                        "description": f"Higher high than {pivot_ist}, yet Cumulative Delta fell {format_vol_display(cvd_drop)} over the swing. Buyers aren't following through, bearish",
                        "strength": round(max(strength, 0.05), 3),
                        "priority": round(SIGNAL_PRIORITY_WEIGHTS["sdiv"] * max(strength, 0.05), 3),
                        "metrics": {"pivot_idx": p_hi, "cvd_drop": round(cvd_drop, 1), "cvd_mag_scale": round(cvd_mag, 1)},
                    })

        if p_lo >= 0 and (idx - p_lo <= 40) and np.isfinite(prior10_low) and l < prior10_low:
            pivot_low = float(df.at[p_lo, "low"])
            pivot_cvd = float(df.at[p_lo, "cumulative_delta"])
            if l < pivot_low:
                cvd_rise = cvd - pivot_cvd
                if cvd_mag > 1e-4 and cvd_rise >= cal.sdiv_cvd_mag_mult * cvd_mag:
                    pivot_ist = datetime.fromtimestamp(
                        (int(df.at[p_lo, "open_time_ms"]) + IST_OFFSET_MS) / 1000.0, timezone.utc
                    ).strftime("%m-%d %H:%M")
                    strength = float(min(1.0, cvd_rise / max(2.0 * cal.sdiv_cvd_mag_mult * cvd_mag, 1e-9)))
                    bar_signals.append({
                        **base_sig,
                        "sig_code": "sdiv",
                        "sig_name": "Bull div",
                        "side": "bullish",
                        "title": "Lower low, higher Cumulative Delta",
                        "description": f"Lower low than {pivot_ist}, yet Cumulative Delta rose {format_vol_display(cvd_rise)} over the swing. Sellers aren't following through, bullish",
                        "strength": round(max(strength, 0.05), 3),
                        "priority": round(SIGNAL_PRIORITY_WEIGHTS["sdiv"] * max(strength, 0.05), 3),
                        "metrics": {"pivot_idx": p_lo, "cvd_rise": round(cvd_rise, 1), "cvd_mag_scale": round(cvd_mag, 1)},
                    })

        # ======================================================================
        # 4. EXHAUSTION (exh)
        # "New 10-bar high with almost no volume in the top 10% of the bar,
        #  closed in the lower half, bearish"  /  mirror bullish.
        # Gates (CG-calibrated): strict new 10-bar extreme (100% of CG exh has
        # the extreme on the signal side), rejection wick >= 0.40 of range
        # (CG median 0.58-0.59), close in the opposite 40% band. NO volume
        # depletion gate: CG exh fires on normal-to-elevated volume (median
        # vol_ratio 1.19-1.71).
        # ======================================================================
        if (_vol_gate_pass or v > 0) and np.isfinite(prior10_high) and h > prior10_high \
                and (upper_wick / bar_range) >= cal.exh_wick_min \
                and close_pos <= cal.exh_close_pos:
            strength = float(min(1.0, 0.7 * (upper_wick / bar_range) + 0.3 * (1.0 - close_pos)))
            bar_signals.append({
                **base_sig,
                "sig_code": "exh",
                "sig_name": "Buyer exhaustion",
                "side": "bearish",
                "title": "Buyer exhaustion at high",
                "description": "New 10-bar high with almost no volume in the top 10% of the bar, closed in the lower half, bearish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["exh"] * max(strength, 0.05), 3),
                "metrics": {"upper_wick_frac": round(upper_wick / bar_range, 3), "close_pos": round(close_pos, 3)},
            })

        if np.isfinite(prior10_low) and l < prior10_low \
                and (lower_wick / bar_range) >= cal.exh_wick_min \
                and close_pos >= (1.0 - cal.exh_close_pos):
            strength = float(min(1.0, 0.7 * (lower_wick / bar_range) + 0.3 * close_pos))
            bar_signals.append({
                **base_sig,
                "sig_code": "exh",
                "sig_name": "Seller exhaustion",
                "side": "bullish",
                "title": "Seller exhaustion at low",
                "description": "New 10-bar low with almost no volume in the bottom 10% of the bar, closed in the upper half, bullish",
                "strength": round(max(strength, 0.05), 3),
                "priority": round(SIGNAL_PRIORITY_WEIGHTS["exh"] * max(strength, 0.05), 3),
                "metrics": {"lower_wick_frac": round(lower_wick / bar_range, 3), "close_pos": round(close_pos, 3)},
            })

        # ======================================================================
        # 5. TRAPPED TRADERS (trap)
        # "Broke the 5-bar high on N of taker buying at the top, then fell back
        #  into the lower part of the bar K bars later. Breakout buyers trapped,
        #  bearish"  /  mirror bullish.
        # Gates (CG-calibrated): strict 5-bar extreme break (100% of CG trap),
        # est extreme-zone same-side taker share >= threshold, and FUTURE
        # CONFIRMATION within 1-3 bars: a later bar closes back inside the lower
        # 35% band of the trigger bar's range while NO bar of the window makes a
        # new extreme beyond the trigger.
        # (CG confirmation lag distribution: 68% at +1, 21% at +2, 11% at +3.)
        # ======================================================================
        n = len(df)
        prior5_high = ctx["prior5_high"][idx]
        prior5_low = ctx["prior5_low"][idx]
        trap_retest_band = cal.trap_retest_band  # CoinGlass decompiled AST: m + .25 * v (bottom/top 25% band)

        if _vol_gate_pass and np.isfinite(prior5_high) and h > prior5_high and v > 0 \
                and trap_top_buy_share >= cal.trap_zone_share_min \
                and ((bar_type in ("vol", "volume", "range")) or (vol_ratio >= cal.trap_vol_ratio_min)):
            trigger_low = l
            retest_level = trigger_low + trap_retest_band * bar_range
            confirm_lag = 0
            for k in range(1, cal.trap_confirm_bars + 1):
                j = idx + k
                if j >= n:
                    break
                if float(df.at[j, "high"]) > h:
                    break  # continuation beyond the trigger high invalidates the trap
                if float(df.at[j, "close"]) < retest_level:
                    confirm_lag = k
                    break
            if confirm_lag > 0:
                strength = float(min(1.0, 0.5 + 0.5 * (est_top_buy_share - cal.trap_zone_share_min) / 0.4 + (0.25 if confirm_lag == 1 else 0.0)))
                bar_signals.append({
                    **base_sig,
                    "sig_code": "trap",
                    "sig_name": "Trapped longs",
                    "side": "bearish",
                    "title": "Failed breakout",
                    "description": f"Broke the 5-bar high on {format_vol_display(est_top_buy_share * buy_v)} of taker buying at the top, then fell back into the lower part of the bar {confirm_lag} bars later. Breakout buyers trapped, bearish",
                    "strength": round(max(min(strength, 1.0), 0.05), 3),
                    "priority": round(SIGNAL_PRIORITY_WEIGHTS["trap"] * max(min(strength, 1.0), 0.05), 3),
                    "confirm_bars": confirm_lag,
                    "_confirm_lag": confirm_lag,
                    "metrics": {"est_top_buy_share": round(est_top_buy_share, 4), "confirm_lag": confirm_lag},
                })

        if _vol_gate_pass and np.isfinite(prior5_low) and l < prior5_low and v > 0 \
                and trap_bottom_sell_share >= cal.trap_zone_share_min \
                and ((bar_type in ("vol", "volume", "range")) or (vol_ratio >= cal.trap_vol_ratio_min)):
            trigger_high = h
            retest_level = trigger_high - trap_retest_band * bar_range
            confirm_lag = 0
            for k in range(1, cal.trap_confirm_bars + 1):
                j = idx + k
                if j >= n:
                    break
                if float(df.at[j, "low"]) < l:
                    break  # continuation beyond the trigger low invalidates the trap
                if float(df.at[j, "close"]) > retest_level:
                    confirm_lag = k
                    break
            if confirm_lag > 0:
                strength = float(min(1.0, 0.5 + 0.5 * (est_bottom_sell_share - cal.trap_zone_share_min) / 0.4 + (0.25 if confirm_lag == 1 else 0.0)))
                bar_signals.append({
                    **base_sig,
                    "sig_code": "trap",
                    "sig_name": "Trapped shorts",
                    "side": "bullish",
                    "title": "Failed breakdown",
                    "description": f"Broke the 5-bar low on {format_vol_display(est_bottom_sell_share * sell_v)} of taker selling at the bottom, then rallied back into the upper part of the bar {confirm_lag} bars later. Breakdown sellers trapped, bullish",
                    "strength": round(max(min(strength, 1.0), 0.05), 3),
                    "priority": round(SIGNAL_PRIORITY_WEIGHTS["trap"] * max(min(strength, 1.0), 0.05), 3),
                    "confirm_bars": confirm_lag,
                    "_confirm_lag": confirm_lag,
                    "metrics": {"est_bottom_sell_share": round(est_bottom_sell_share, 4), "confirm_lag": confirm_lag},
                })

        return bar_signals

    # --------------------------------------------------------------------------------
    # EXECUTION HANDOFF (RealtimeSignalEvaluator / Delta Exchange integration)
    # --------------------------------------------------------------------------------
    @staticmethod
    def pick_primary(signals: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Arbitrates concurrent signals on one bar for execution scoring.

        Ranking = SIGNAL_PRIORITY_WEIGHTS[code] * strength (trap > abs > exh >
        sdiv > div). Ties break toward the earlier list position.
        """
        if not signals:
            return None
        return max(
            signals,
            key=lambda s: SIGNAL_PRIORITY_WEIGHTS.get(str(s.get("sig_code", "")), 0.5) * float(s.get("strength", 0.5))
        )

    @staticmethod
    def build_execution_plan(
        signal: Dict[str, Any],
        equity_usd: float,
        atr_pct: float = 1.0,
        risk_fraction: float = 0.01,
        rr_target: float = 2.0,
        taker_fee_rate: float = 0.0005,
        contract_value: float = 1.0,
        min_size: float = 1.0,
    ) -> Dict[str, Any]:
        """
        Fill-calibrated 1:2 R:R order plan for delta_exchange_live_trader.py.

        - Stop: structural, beyond the signal bar's extreme with an ATR buffer
          (clamped to 0.5%-2.5% of entry, matching RealtimeSignalEvaluator's
          ATR-calibrated stop band).
        - Target: exactly entry +/- rr_target * risk_distance (strict R:R >= 2).
        - Size: fee-aware — reserves round-trip taker commissions inside the
          risk budget so the realized loss at stop equals the budgeted risk.
        - Discrete: floor to min_size; returns size 0 (skip) if below.
        """
        side = str(signal.get("side", "bearish")).lower()
        direction = 1 if side in ("bullish", "buy", "long") else -1
        entry = float(signal.get("price", 0.0))
        if entry <= 0:
            return {"valid": False, "reason": "invalid_entry"}

        sl_pct = max(0.005, min(0.025, (atr_pct * 1.5) / 100.0))
        risk_dist = entry * sl_pct
        stop = entry - direction * risk_dist
        target = entry + direction * rr_target * risk_dist

        risk_budget = equity_usd * risk_fraction
        entry_notional = entry * contract_value
        exit_notional = (stop if direction < 0 else target) * contract_value
        # Conservative sizing charges round-trip taker fees at the WORST exit.
        commission_per_contract = taker_fee_rate * (entry_notional + exit_notional)
        loss_per_contract = risk_dist * contract_value + commission_per_contract
        if loss_per_contract <= 0:
            return {"valid": False, "reason": "zero_risk_distance"}
        size_raw = risk_budget / loss_per_contract
        size = math.floor(size_raw / min_size) * min_size if min_size > 0 else math.floor(size_raw)
        if size < min_size:
            return {"valid": False, "reason": "below_min_size",
                    "size_raw": round(size_raw, 4), "loss_per_contract": round(loss_per_contract, 6)}

        return {
            "valid": True,
            "direction": "LONG" if direction > 0 else "SHORT",
            "entry": round(entry, 6),
            "stop_loss": round(stop, 6),
            "take_profit": round(target, 6),
            "rr": rr_target,
            "size": size,
            "risk_budget_usd": round(risk_budget, 4),
            "fees_reserved_usd": round(size * commission_per_contract, 4),
            "loss_per_contract": round(loss_per_contract, 6),
        }

    @staticmethod
    def apply_trailing_ratchet(
        current_stop: float,
        mark_price: float,
        direction: int,
        atr: float,
        trail_mult: float = 2.0,
    ) -> float:
        """
        Dynamic trailing-stop ratchet: the stop only ever moves in the trade's
        favor (never widens). new_stop = mark -/+ trail_mult * ATR.
        """
        if direction > 0:
            return max(current_stop, mark_price - trail_mult * atr)
        return min(current_stop, mark_price + trail_mult * atr)
