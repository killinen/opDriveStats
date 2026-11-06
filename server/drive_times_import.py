"""Import trusted drive start times from OP-NAS into engagement_db.json.

Devices without network at boot record drives with a wrong clock, so the
website either drops them from the timeline or estimates their date from the
upload. OP-NAS reads the start from GPS time in the archived rlogs (or accepts
the device clock when it passes its checks) and lists the results at
``/api/drive-times``. This command stores them as ``trusted_start`` on matching
records; the synchronizer keeps that field when a drive is uploaded again.

Usage (the JSON is the list returned by OP-NAS's /api/drive-times)::

    python -m server.drive_times_import drive_times.json [--device ID] [--dry-run]
    ssh pi4 '...' | python -m server.drive_times_import - --device ff528bde139e99de
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable, Optional

from .drive_dates import parse_timestamp, resolve_drive_date
from .drive_stats_sync import PROJECT_ROOT, _atomic_write_json


def _default_db() -> Path:
    return Path(os.environ.get('ENGAGEMENT_DB_PATH', PROJECT_ROOT / 'engagement_db.json'))


def _default_lock() -> Path:
    return Path(os.environ.get('DRIVE_STATS_LOCK_PATH', PROJECT_ROOT / '.drive_stats_sync.lock'))


def apply_drive_times(
    entries: list[dict[str, Any]],
    drive_times: Iterable[dict[str, Any]],
    *,
    devices: Optional[set[str]] = None,
    now: Optional[datetime] = None,
) -> Counter:
    """Set ``trusted_start`` in place; return counts describing what changed."""
    now = now or datetime.now(timezone.utc)
    counts: Counter = Counter()
    times = {}
    for item in drive_times:
        if not isinstance(item, dict) or item.get('estimated'):
            counts['skipped_estimated'] += 1
            continue
        device, route = item.get('device_id'), item.get('route')
        if not device or not route or (devices and device not in devices):
            continue
        if parse_timestamp(item.get('value')) is None:
            counts['skipped_invalid'] += 1
            continue
        times[(str(device), str(route))] = item
    imported_at = now.isoformat()
    for row in entries:
        item = times.pop((str(row.get('device_id') or ''), str(row.get('drive') or '')), None)
        if item is None:
            continue
        before = resolve_drive_date(row, now=now)
        start = {
            'value': parse_timestamp(item['value']).isoformat(),
            'source': f"opnas_{item.get('source') or 'unknown'}",
        }
        previous = row.get('trusted_start') or {}
        if {k: previous.get(k) for k in start} == start:
            counts['unchanged'] += 1
            continue
        candidate = {**row, 'trusted_start': {**start, 'imported_at': imported_at}}
        after = resolve_drive_date(candidate, now=now)
        if after.source != 'trusted_start':
            # It fails the same software-date and upload checks as a device clock.
            counts['rejected_implausible'] += 1
            continue
        row['trusted_start'] = candidate['trusted_start']
        if before.timestamp is None:
            counts['dated_previously_undated'] += 1
        elif before.is_estimated:
            counts['replaced_upload_estimate'] += 1
        else:
            counts['replaced_recording_date'] += 1
    counts['not_in_database'] = len(times)
    return counts


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('source', help="OP-NAS /api/drive-times JSON file, or '-' for stdin")
    parser.add_argument('--device', action='append', help='Only import this device (repeatable)')
    parser.add_argument('--db', type=Path, default=_default_db())
    parser.add_argument('--lock', type=Path, default=_default_lock())
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    raw = sys.stdin.read() if args.source == '-' else Path(args.source).read_text(encoding='utf-8')
    drive_times = json.loads(raw)
    if not isinstance(drive_times, list):
        parser.error('source must be a JSON list')

    args.lock.parent.mkdir(parents=True, exist_ok=True)
    # Same lock as the synchronizer, so neither overwrites the other's write.
    with args.lock.open('a+', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        entries = json.loads(args.db.read_text(encoding='utf-8'))
        counts = apply_drive_times(entries, drive_times, devices=set(args.device or []) or None)
        changed = sum(counts[k] for k in (
            'dated_previously_undated', 'replaced_upload_estimate', 'replaced_recording_date'))
        if changed and not args.dry_run:
            _atomic_write_json(args.db, entries)
    print(json.dumps({'db': str(args.db), 'dry_run': args.dry_run, 'changed': changed, **counts}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
