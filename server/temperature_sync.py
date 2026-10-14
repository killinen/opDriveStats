"""Authenticated cursor replication into the compressed temperature library."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import fcntl
import gzip
import json
import logging
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .drive_stats_sync import DriveStatsSyncConfig, PROJECT_ROOT, _positive_int
from .temperature_data import TemperatureRepository, configured_db_path

LOGGER = logging.getLogger(__name__)
MAX_PAGE_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class TemperatureSyncConfig:
    source_url: str
    token: str
    db_path: Path
    lock_path: Path
    poll_interval_seconds: int = 60
    page_size: int = 10
    timeout_seconds: int = 30

    @classmethod
    def from_environment(cls, drive_config: DriveStatsSyncConfig | None = None):
        if os.getenv('DRIVE_TELEMETRY_SYNC', '1').strip().lower() in {
            '0',
            'false',
            'no',
            'off',
        }:
            return None
        source = os.getenv('DRIVE_TELEMETRY_SOURCE_URL', '').strip()
        if (
            not source
            and drive_config
            and drive_config.source_url.endswith('/drive-stats/export')
        ):
            source = (
                drive_config.source_url.removesuffix('/drive-stats/export')
                + '/drive-telemetry/export'
            )
        if not source:
            return None
        token = os.getenv('DRIVE_TELEMETRY_SOURCE_TOKEN', '').strip() or (
            drive_config.token if drive_config else ''
        )
        if not token:
            filename = os.getenv(
                'DRIVE_TELEMETRY_SOURCE_TOKEN_FILE',
                os.getenv(
                    'DRIVE_STATS_SOURCE_TOKEN_FILE',
                    str(PROJECT_ROOT / '.drive_stats_source_token'),
                ),
            )
            try:
                token = Path(filename).expanduser().read_text().strip()
            except OSError:
                LOGGER.warning('Temperature sync disabled: source token is unavailable')
                return None
        if not token:
            return None
        return cls(
            source,
            token,
            configured_db_path(),
            Path(
                os.getenv(
                    'DRIVE_TELEMETRY_LOCK_PATH',
                    str(PROJECT_ROOT / '.temperature_sync.lock'),
                )
            )
            .expanduser()
            .resolve(),
            _positive_int(os.getenv('DRIVE_TELEMETRY_POLL_INTERVAL'), 60),
            min(100, _positive_int(os.getenv('DRIVE_TELEMETRY_PAGE_SIZE'), 10)),
            _positive_int(os.getenv('DRIVE_TELEMETRY_SOURCE_TIMEOUT'), 30),
        )


class TemperatureSynchronizer:
    def __init__(self, config: TemperatureSyncConfig):
        self.config = config
        self.repository = TemperatureRepository(config.db_path)

    def _fetch_page(self, cursor: int) -> dict:
        separator = '&' if '?' in self.config.source_url else '?'
        request = Request(
            self.config.source_url
            + separator
            + urlencode({'after': cursor, 'limit': self.config.page_size}),
            headers={
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip',
                'Authorization': f'Bearer {self.config.token}',
                'User-Agent': 'opDriveStats-telemetry/1',
            },
        )
        with urlopen(request, timeout=self.config.timeout_seconds) as response:
            encoding = response.headers.get('Content-Encoding', 'identity').lower()
            if encoding == 'gzip':
                raw = gzip.GzipFile(fileobj=response).read(MAX_PAGE_BYTES + 1)
            elif encoding == 'identity':
                raw = response.read(MAX_PAGE_BYTES + 1)
            else:
                raise ValueError('Unsupported telemetry response encoding')
        if len(raw) > MAX_PAGE_BYTES:
            raise ValueError('Telemetry export page is too large')
        result = json.loads(raw)
        if not isinstance(result, dict) or not isinstance(result.get('items'), list):
            raise ValueError('Invalid telemetry export page')
        return result

    def sync_once(self) -> int:
        self.config.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config.lock_path.open('a+') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            imported = 0
            while True:
                cursor = self.repository.state()['cursor']
                page = self._fetch_page(cursor)
                if page.get('has_more') and not page['items']:
                    raise ValueError(
                        'Telemetry source reported more data without items'
                    )
                imported += self.repository.ingest(page)
                if not page.get('has_more'):
                    return imported

    async def run_forever(self):
        while True:
            try:
                imported = await asyncio.to_thread(self.sync_once)
                if imported:
                    LOGGER.info('Imported %d temperature batch(es)', imported)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                try:
                    self.repository.record_error(error)
                except Exception:
                    LOGGER.exception('Unable to save temperature sync diagnostics')
                LOGGER.exception('Temperature synchronization failed')
            await asyncio.sleep(self.config.poll_interval_seconds)
