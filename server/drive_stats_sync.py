from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
import fcntl
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DriveStatsSyncError(RuntimeError):
    pass


def _positive_int(value: Optional[str], default: int) -> int:
    try:
        return max(1, int(value or default))
    except (TypeError, ValueError):
        return default


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f'.{path.name}.tmp-{os.getpid()}')
    try:
        with temporary_path.open('w', encoding='utf-8') as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path, default: Any) -> Any:
    try:
        with path.open('r', encoding='utf-8') as source:
            return json.load(source)
    except FileNotFoundError:
        return deepcopy(default)
    except (OSError, json.JSONDecodeError) as exc:
        raise DriveStatsSyncError(f'Failed to read {path}: {exc}') from exc


def _normalize_stats(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Add opDriveStats legacy aliases while retaining the complete v3 payload."""
    stats = deepcopy(payload)
    if not stats.get('device_id') or not stats.get('drive'):
        raise DriveStatsSyncError('Source item is missing device_id or drive')

    stats.setdefault('intervention_count', stats.get('disengagement_count', 0))
    stats.setdefault(
        'interventions_per_100km',
        stats.get('disengagements_per_100km'),
    )
    speed_buckets = stats.get('speed_buckets')
    if isinstance(speed_buckets, dict):
        for bucket in speed_buckets.values():
            if not isinstance(bucket, dict):
                continue
            bucket.setdefault(
                'intervention_count',
                bucket.get('disengagement_count', 0),
            )
            bucket.setdefault(
                'interventions_per_100km',
                bucket.get('disengagements_per_100km'),
            )
    return stats


@dataclass(frozen=True)
class DriveStatsSyncConfig:
    source_url: str
    token: str
    db_path: Path
    cursor_path: Path
    lock_path: Path
    poll_interval_seconds: int = 60
    page_size: int = 100
    timeout_seconds: int = 30

    @classmethod
    def from_environment(cls) -> Optional['DriveStatsSyncConfig']:
        source_url = os.environ.get('DRIVE_STATS_SOURCE_URL', '').strip()
        if not source_url:
            return None

        token = os.environ.get('DRIVE_STATS_SOURCE_TOKEN', '').strip()
        if not token:
            token_file_value = os.environ.get(
                'DRIVE_STATS_SOURCE_TOKEN_FILE',
                str(PROJECT_ROOT / '.drive_stats_source_token'),
            )
            token_file = Path(token_file_value).expanduser().resolve()
            try:
                token = token_file.read_text(encoding='utf-8').strip()
            except FileNotFoundError:
                LOGGER.warning(
                    'Drive stats sync disabled: token file %s does not exist',
                    token_file,
                )
                return None
            except OSError as exc:
                LOGGER.warning('Drive stats sync disabled: cannot read token: %s', exc)
                return None
        if not token:
            LOGGER.warning('Drive stats sync disabled: source token is empty')
            return None

        db_path_value = os.environ.get(
            'ENGAGEMENT_DB_PATH',
            str(PROJECT_ROOT / 'engagement_db.json'),
        )
        cursor_path_value = os.environ.get(
            'DRIVE_STATS_CURSOR_PATH',
            str(PROJECT_ROOT / '.drive_stats_cursor.json'),
        )
        lock_path_value = os.environ.get(
            'DRIVE_STATS_LOCK_PATH',
            str(PROJECT_ROOT / '.drive_stats_sync.lock'),
        )
        return cls(
            source_url=source_url,
            token=token,
            db_path=Path(db_path_value).expanduser().resolve(),
            cursor_path=Path(cursor_path_value).expanduser().resolve(),
            lock_path=Path(lock_path_value).expanduser().resolve(),
            poll_interval_seconds=_positive_int(
                os.environ.get('DRIVE_STATS_POLL_INTERVAL'), 60
            ),
            page_size=min(
                500,
                _positive_int(os.environ.get('DRIVE_STATS_PAGE_SIZE'), 100),
            ),
            timeout_seconds=_positive_int(
                os.environ.get('DRIVE_STATS_SOURCE_TIMEOUT'), 30
            ),
        )


class DriveStatsSynchronizer:
    def __init__(self, config: DriveStatsSyncConfig) -> None:
        self.config = config

    def _load_cursor(self) -> int:
        state = _read_json(self.config.cursor_path, {'cursor': 0})
        if not isinstance(state, dict):
            raise DriveStatsSyncError('Drive stats cursor document must be an object')
        try:
            return max(0, int(state.get('cursor', 0)))
        except (TypeError, ValueError) as exc:
            raise DriveStatsSyncError('Drive stats cursor is invalid') from exc

    def _load_entries(self) -> List[Dict[str, Any]]:
        entries = _read_json(self.config.db_path, [])
        if not isinstance(entries, list) or not all(
            isinstance(entry, dict) for entry in entries
        ):
            raise DriveStatsSyncError('Engagement database must contain a JSON list')
        return entries

    def _fetch_page(self, cursor: int) -> Dict[str, Any]:
        query = urlencode({'after': cursor, 'limit': self.config.page_size})
        separator = '&' if '?' in self.config.source_url else '?'
        request = Request(
            f'{self.config.source_url}{separator}{query}',
            headers={
                'Accept': 'application/json',
                'Authorization': f'Bearer {self.config.token}',
                'User-Agent': 'opDriveStats-sync/1',
            },
        )
        try:
            with urlopen(request, timeout=self.config.timeout_seconds) as response:
                result = json.load(response)
        except Exception as exc:
            raise DriveStatsSyncError(f'Drive stats source request failed: {exc}') from exc
        if not isinstance(result, dict) or not isinstance(result.get('items'), list):
            raise DriveStatsSyncError('Drive stats source returned an invalid page')
        return result

    def _merge_page(
        self,
        entries: List[Dict[str, Any]],
        page: Dict[str, Any],
        current_cursor: int,
    ) -> tuple[List[Dict[str, Any]], int, int]:
        keyed_entries = {
            (str(entry.get('device_id') or ''), str(entry.get('drive') or '')): entry
            for entry in entries
            if entry.get('device_id') and entry.get('drive')
        }
        imported = 0
        next_cursor = current_cursor
        for item in page['items']:
            if not isinstance(item, dict) or not isinstance(item.get('stats'), dict):
                raise DriveStatsSyncError('Drive stats source item is invalid')
            try:
                item_cursor = int(item['cursor'])
            except (KeyError, TypeError, ValueError) as exc:
                raise DriveStatsSyncError('Drive stats source cursor is invalid') from exc
            if item_cursor <= next_cursor:
                raise DriveStatsSyncError('Drive stats source cursors are not increasing')
            stats = _normalize_stats(item['stats'])
            key = (str(stats['device_id']), str(stats['drive']))
            previous = keyed_entries.get(key)
            if previous != stats:
                keyed_entries[key] = stats
                imported += 1
            next_cursor = item_cursor

        unkeyed_entries = [
            entry
            for entry in entries
            if not entry.get('device_id') or not entry.get('drive')
        ]
        merged = unkeyed_entries + list(keyed_entries.values())
        merged.sort(
            key=lambda entry: (
                str(entry.get('device_id') or ''),
                str(entry.get('drive') or ''),
            )
        )
        return merged, next_cursor, imported

    def sync_once(self) -> int:
        self.config.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.lock_path.open('a+', encoding='utf-8') as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0

            cursor = self._load_cursor()
            entries = self._load_entries()
            imported_total = 0
            while True:
                page = self._fetch_page(cursor)
                merged, next_cursor, imported = self._merge_page(
                    entries,
                    page,
                    cursor,
                )
                if imported:
                    _atomic_write_json(self.config.db_path, merged)
                    entries = merged
                if next_cursor != cursor:
                    _atomic_write_json(
                        self.config.cursor_path,
                        {'cursor': next_cursor},
                    )
                    cursor = next_cursor
                imported_total += imported
                if not page.get('has_more'):
                    break
                if not page['items']:
                    raise DriveStatsSyncError(
                        'Drive stats source reported more data without any items'
                    )
            return imported_total

    async def run_forever(self) -> None:
        while True:
            try:
                imported = await asyncio.to_thread(self.sync_once)
                if imported:
                    LOGGER.info('Imported %d drive stats record(s)', imported)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('Drive stats synchronization failed')
            await asyncio.sleep(self.config.poll_interval_seconds)
