"""Crash-released interprocess locks and atomic local data publication."""
import asyncio
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


class FileMutex:
    """Lock a persistent inode; never expire or unlink another writer's lock."""
    def __init__(self, path, timeout=2.0):
        self.path = Path(path)
        self.timeout = max(0.0, float(timeout))
        self.handle = None
        self.acquired = False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
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
                        return self
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
                        msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None
                self.acquired = False


@contextmanager
def writer_lock(path):
    p = Path(path).resolve()
    db_dir = Path(__file__).resolve().parent.parent / "Database"
    try:
        rel = p.relative_to(db_dir.resolve())
        lock_dir = db_dir / ".locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        sanitized = str(rel).replace("\\", "_").replace("/", "_") + ".writer.lock"
        lock_path = lock_dir / sanitized
    except ValueError:
        lock_path = Path(str(p) + ".writer.lock")

    with FileMutex(str(lock_path)) as lock:
        if not lock.acquired:
            raise TimeoutError(f'Writer busy: {path}')
        yield


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
        except Exception:
            pass
        deadline = time.monotonic() + 2.0
        delay = 0.01
        while True:
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.25)
    finally:
        temporary.unlink(missing_ok=True)


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
            try:
                os.fsync(f.fileno())
            except OSError:
                pass


def atomic_parquet(frame, path, compression='snappy'):
    if hasattr(frame, 'to_parquet'):
        _publish(path, lambda p: frame.to_parquet(p, index=False, engine='pyarrow', compression=compression))
    elif hasattr(frame, 'write_parquet'):
        _publish(path, lambda p: frame.write_parquet(p, compression=compression))
    else:
        import pyarrow.parquet as pq
        _publish(path, lambda p: pq.write_table(frame, p, compression=compression))


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
