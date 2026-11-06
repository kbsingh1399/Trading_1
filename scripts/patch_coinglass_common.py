import os
import sys
from pathlib import Path

target_file = Path("coinglass_common.py")
content = target_file.read_text(encoding="utf-8")

# 1. Add threading import
old_import = "from pathlib import Path\n"
new_import = "from pathlib import Path\nimport threading\n\n_THREAD_WRITER_LOCKS = threading.local()\n"
if old_import in content:
    content = content.replace(old_import, new_import, 1)

# 2. Replace writer_lock
old_writer_lock = """@contextmanager
def writer_lock(path, timeout=5.0):
    p = Path(path).resolve()
    db_dir = Path(__file__).resolve().parent / "Database"
    try:
        rel = p.relative_to(db_dir.resolve())
        lock_dir = db_dir / ".locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        sanitized = str(rel).replace("\\\\", "_").replace("/", "_") + ".writer.lock"
        lock_path = lock_dir / sanitized
    except ValueError:
        lock_path = Path(str(p) + ".writer.lock")

    with FileMutex(str(lock_path), timeout=timeout) as lock:
        if not lock.acquired:
            raise TimeoutError(f'Writer busy: {path}')
        yield"""

new_writer_lock = """@contextmanager
def writer_lock(path, timeout=5.0):
    p = Path(path).resolve()
    db_dir = Path(__file__).resolve().parent / "Database"
    try:
        rel = p.relative_to(db_dir.resolve())
        lock_dir = db_dir / ".locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        sanitized = str(rel).replace("\\\\", "_").replace("/", "_") + ".writer.lock"
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
                del held[lock_str]"""

if old_writer_lock in content:
    content = content.replace(old_writer_lock, new_writer_lock, 1)

# 3. Replace _reap_stale_lock and add clean_all_abandoned_locks
old_reap = """def _reap_stale_lock(lock_path: Path, ttl: float = 300.0) -> None:
    \"\"\"Z.ai Domain 1 Upgrade: Remove stale PID lock if its holder has been dead >= ttl.\"\"\"
    if not lock_path.exists():
        return
    try:
        mtime = lock_path.stat().st_mtime
        if time.time() - mtime < ttl:
            return
        raw = lock_path.read_text().strip()
        holder_pid = int(raw) if raw.isdigit() else -1
        if holder_pid > 0:
            import ctypes
            # Check if process is alive on Windows
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, holder_pid)
            if h == 0:
                # Process is dead, reap stale lock
                lock_path.unlink(missing_ok=True)
                logger.info("Reaped stale lock file: %s (PID %s dead)", lock_path.name, holder_pid)
            else:
                ctypes.windll.kernel32.CloseHandle(h)
    except Exception:
        pass"""

new_reap = """def _reap_stale_lock(lock_path: Path, ttl: float = 120.0) -> None:
    \"\"\"Remove stale lock if older than ttl and not held by any living process.\"\"\"
    if not lock_path.exists():
        return
    try:
        mtime = lock_path.stat().st_mtime
        if time.time() - mtime < ttl:
            return
        if os.name == "nt":
            import msvcrt
            try:
                with open(lock_path, "r+b") as f:
                    msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                # Success proves no live process holds this lock
                lock_path.unlink(missing_ok=True)
                logger.debug("Reaped abandoned lock: %s (age > %ss)", lock_path.name, ttl)
            except (OSError, PermissionError):
                pass
        else:
            import fcntl
            try:
                with open(lock_path, "r+b") as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                lock_path.unlink(missing_ok=True)
            except (OSError, PermissionError):
                pass
    except Exception:
        pass


def clean_all_abandoned_locks(ttl: float = 120.0) -> int:
    \"\"\"Sweeps Database/.locks/ and unlinks any abandoned locks older than ttl.\"\"\"
    locks_dir = Path(__file__).resolve().parent / "Database" / ".locks"
    if not locks_dir.exists():
        return 0
    reaped = 0
    for p in locks_dir.glob("*.lock"):
        if not p.is_file():
            continue
        try:
            if time.time() - p.stat().st_mtime >= ttl:
                _reap_stale_lock(p, ttl=ttl)
                if not p.exists():
                    reaped += 1
        except Exception:
            pass
    return reaped"""

if old_reap in content:
    content = content.replace(old_reap, new_reap, 1)

target_file.write_text(content, encoding="utf-8")
print("Successfully patched coinglass_common.py!")
