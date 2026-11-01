"""Incremental JSONL consumption across restarts, partial writes and rotation."""
import hashlib
import json
import os
from pathlib import Path
from Terminal.runtime_io import atomic_json, writer_lock


def _anchor(stream, offset):
    stream.seek(max(0, offset - 64))
    return hashlib.sha256(stream.read(min(offset, 64))).hexdigest()


def consume_alerts(path, state_path, *, start_at_end=False):
    path, state_path = Path(path), Path(state_path)
    with writer_lock(state_path):
        state = json.loads(state_path.read_text(encoding='utf-8')) if state_path.exists() else {}
        if not path.exists():
            # Persist an empty cursor so creation events consume the first record.
            if not state:
                atomic_json(state_path, {'offset': 0, 'seen': []})
            return []
        with path.open('rb') as stream:
            stat = os.fstat(stream.fileno())
            identity = [stat.st_dev, stat.st_ino]
            offset = state.get('offset', stat.st_size if start_at_end and not state else 0)
            if 'offset' not in state and 'last_line_count' in state:
                for _ in range(state['last_line_count']):
                    if not stream.readline():
                        break
                offset = stream.tell()
            if (state.get('identity', identity) != identity or offset > stat.st_size
                    or ('anchor' in state and _anchor(stream, offset) != state['anchor'])):
                offset = 0
            stream.seek(offset)
            seen = list(state.get('seen', []))
            seen_set = set(seen)
            result = []
            while True:
                line = stream.readline()
                if not line or not line.endswith(b'\n'):
                    break
                offset = stream.tell()
                try:
                    record = json.loads(line)
                except (UnicodeDecodeError, ValueError):
                    continue
                if not isinstance(record, dict) or record.get('is_actionable') is not True:
                    continue
                # Stable signal identity prevents a replayed sweep waking the desk again.
                signal = record.get('signal_details') or {}
                key = record.get('alert_id') or hashlib.sha256(json.dumps([
                    record.get('symbol'), record.get('direction'), signal.get('sig_code'),
                    signal.get('candle_utc'), record.get('evaluated_at_utc') if not signal else None,
                ], sort_keys=True).encode()).hexdigest()
                if key not in seen_set:
                    result.append(record)
                    seen.append(key)
                    seen_set.add(key)
            atomic_json(state_path, {'offset': offset, 'identity': identity,
                                     'anchor': _anchor(stream, offset), 'seen': seen[-4096:]})
            return result
