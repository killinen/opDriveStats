"""Compressed telemetry library and time-weighted operating-temperature statistics."""

from __future__ import annotations

from bisect import bisect_right
from collections import deque
from contextlib import closing
from datetime import datetime, timezone, timedelta
from functools import lru_cache
import gzip
import json
import math
import os
from pathlib import Path
import sqlite3
from typing import Any

SECOND = 1_000_000_000
SPEED_BANDS = ('0–5', '5–30', '30–70', '70–100', '100+')
ENGINE_BANDS = ('All', 'Below 50 °C', '50–80 °C', '80+ °C')
DRIVE_PHASES = ('All', 'First 5 min', '5–15 min', '15+ min', 'Unknown route start')
MAX_AGE = {'hrr': 1.5, 'ambient': 60, 'engine': 5, 'speed': 2}


def packed(value: Any) -> bytes:
    return gzip.compress(
        json.dumps(
            value, separators=(',', ':'), sort_keys=True, allow_nan=False
        ).encode(),
        compresslevel=6,
        mtime=0,
    )


def unpacked(value: bytes) -> Any:
    return json.loads(gzip.decompress(value))


def configured_db_path() -> Path:
    return (
        Path(
            os.getenv(
                'TEMPERATURE_DB_PATH',
                str(Path(__file__).resolve().parents[1] / 'temperature.sqlite3'),
            )
        )
        .expanduser()
        .resolve()
    )


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path), timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript("""
        CREATE TABLE IF NOT EXISTS batches (
            cursor INTEGER PRIMARY KEY, device TEXT NOT NULL, drive TEXT NOT NULL,
            collector TEXT NOT NULL, schema_version INTEGER NOT NULL, source TEXT NOT NULL,
            batch_index INTEGER NOT NULL, batch_count INTEGER NOT NULL, received_at TEXT NOT NULL,
            payload_gzip BLOB NOT NULL,
            UNIQUE(device, drive, collector, schema_version, source, batch_index)
        );
        CREATE INDEX IF NOT EXISTS batches_snapshot ON batches(device,drive,collector,schema_version,source);
        CREATE TABLE IF NOT EXISTS published (
            device TEXT NOT NULL, drive TEXT NOT NULL, schema_version INTEGER NOT NULL,
            source TEXT NOT NULL, PRIMARY KEY(device,drive,schema_version,source)
        );
        CREATE TABLE IF NOT EXISTS segments (
            device TEXT NOT NULL, drive TEXT NOT NULL, schema_version INTEGER NOT NULL,
            segment INTEGER NOT NULL, source TEXT NOT NULL, cursor INTEGER NOT NULL,
            coverage_json TEXT NOT NULL, samples_gzip BLOB NOT NULL,
            PRIMARY KEY(device,drive,schema_version,segment)
        );
        CREATE TABLE IF NOT EXISTS routes (
            device TEXT NOT NULL, drive TEXT NOT NULL, schema_version INTEGER NOT NULL,
            cursor INTEGER NOT NULL, metadata_json TEXT NOT NULL, coverage_json TEXT NOT NULL,
            grid_gzip BLOB NOT NULL, raw_max REAL,
            observed_seconds INTEGER NOT NULL, available_seconds INTEGER NOT NULL,
            PRIMARY KEY(device,drive,schema_version)
        );
        CREATE TABLE IF NOT EXISTS sync_state (
            name TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0,
            last_sync_at TEXT, last_error TEXT
        );
        INSERT OR IGNORE INTO sync_state(name) VALUES ('telemetry');
        INSERT OR IGNORE INTO sync_state(name) VALUES ('desktop_import');
        CREATE TABLE IF NOT EXISTS desktop_profiles (
            source_profile TEXT PRIMARY KEY, stats_profile INTEGER NOT NULL,
            config_json TEXT NOT NULL, label TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS desktop_archive (
            source_profile TEXT NOT NULL, source_segment_id INTEGER NOT NULL,
            fingerprint TEXT NOT NULL, metadata_json TEXT NOT NULL,
            samples_gzip BLOB NOT NULL, imported_at TEXT NOT NULL,
            PRIMARY KEY(source_profile,source_segment_id,fingerprint)
        );
    """)
    return db


def _streams(payloads: list[dict]) -> tuple[list[dict], list[dict]]:
    """v1 snapshots remain usable; v2 carries exact monotonic timestamps and coverage."""
    head = payloads[0]
    streams: list[dict] = []
    coverage: dict[int, dict] = {}
    for payload in payloads:
        if payload['schema_version'] == 2:
            streams.extend(payload['data']['samples'])
            for item in payload['data'].get('coverage', []):
                previous = coverage.setdefault(item['segment'], item)
                if previous != item:
                    raise ValueError('Coverage changed within a snapshot')
        else:
            for row in payload['data']['samples']:
                timestamp = head['start_mono_time_ns'] + round(row['time_s'] * SECOND)
                streams.append(
                    {
                        'metric': 'hrr',
                        'time_ns': timestamp,
                        'segment': row['segment'],
                        'value': row['hrr_temp_c'],
                        'counter': row['temperature_counter'],
                    }
                )
                for metric, value_key, age_key in [
                    ('speed', 'speed_kph', 'speed_age_s'),
                    ('ambient', 'ambient_temp_c', 'ambient_age_s'),
                ]:
                    if row[value_key] is not None:
                        sample = {
                            'metric': metric,
                            'time_ns': timestamp - round(row[age_key] * SECOND),
                            'segment': row['segment'],
                            'value': row[value_key],
                        }
                        if metric == 'ambient':
                            sample['raw'] = row['ambient_raw']
                        streams.append(sample)
    if head['schema_version'] == 1:
        for segment in head['segments']:
            reports = [
                row
                for row in streams
                if row['segment'] == segment and row['metric'] == 'hrr'
            ]
            if not reports:
                continue
            coverage[segment] = {
                'segment': segment,
                'first_ns': min(row['time_ns'] for row in reports),
                'last_ns': max(row['time_ns'] for row in reports),
                'can_events': 0,
                'hrr_frames': len(reports),
                'hrr_valid': sum(row['value'] is not None for row in reports),
                'hrr_invalid': sum(row['value'] is None for row in reports),
                'hrr_malformed': 0,
                'hrr_repeated': 0,
                'estimated': True,
            }
    if head['schema_version'] == 2:
        if not coverage:
            raise ValueError('Complete snapshot has no segment coverage')
        for sample in streams:
            interval = coverage.get(sample['segment'])
            if (
                interval is None
                or not interval['first_ns'] <= sample['time_ns'] <= interval['last_ns']
            ):
                raise ValueError('Sample lies outside complete snapshot coverage')
    return streams, list(coverage.values())


def _grid(
    segment_rows: list[sqlite3.Row],
) -> tuple[list[dict], list[dict], float | None]:
    coverage = [json.loads(row['coverage_json']) for row in segment_rows]
    samples = [
        sample for row in segment_rows for sample in unpacked(row['samples_gzip'])
    ]
    by_metric: dict[str, dict[int, float | None]] = {key: {} for key in MAX_AGE}
    for sample in sorted(samples, key=lambda item: item['time_ns']):
        by_metric[sample['metric']][sample['time_ns']] = sample['value']
    series = {
        key: (list(values), list(values.values())) for key, values in by_metric.items()
    }
    origin = min(
        (item['first_ns'] for item in coverage if item['segment'] == 0), default=None
    )
    slots: dict[int, int] = {}
    for interval in coverage:
        for timestamp in range(
            (interval['first_ns'] + SECOND - 1) // SECOND * SECOND,
            interval['last_ns'] // SECOND * SECOND + SECOND,
            SECOND,
        ):
            slots[timestamp] = interval['segment']
    rows = []
    for timestamp, segment in sorted(slots.items()):
        row = {
            'time_ns': timestamp,
            'segment': segment,
            'elapsed': (timestamp - origin) / SECOND if origin is not None else None,
        }
        for metric, (times, values) in series.items():
            index = bisect_right(times, timestamp) - 1
            row[metric] = (
                values[index]
                if index >= 0 and timestamp - times[index] <= MAX_AGE[metric] * SECOND
                else None
            )
        rows.append(row)
    counts = {item['segment']: [0, 0] for item in coverage}
    for row in rows:
        counts[row['segment']][0] += 1
        counts[row['segment']][1] += row['hrr'] is not None
    for interval in coverage:
        interval['observed_seconds'], interval['available_seconds'] = counts[
            interval['segment']
        ]
    raw = [
        sample['value']
        for sample in samples
        if sample['metric'] == 'hrr' and sample['value'] is not None
    ]
    return rows, coverage, max(raw, default=None)


def _publish(db: sqlite3.Connection, identity: tuple) -> None:
    device, drive, collector, version, source = identity
    if collector != 'hrr_temperature' or version not in (1, 2):
        return
    if db.execute(
        'SELECT 1 FROM published WHERE device=? AND drive=? AND schema_version=? AND source=?',
        (device, drive, version, source),
    ).fetchone():
        return
    batches = db.execute(
        """SELECT * FROM batches WHERE device=? AND drive=? AND collector=? AND schema_version=? AND source=? ORDER BY batch_index""",
        identity,
    ).fetchall()
    if not batches or len(batches) != batches[0]['batch_count']:
        return
    if [row['batch_index'] for row in batches] != list(range(len(batches))):
        raise ValueError('Snapshot has invalid batch indices')
    payloads = [unpacked(row['payload_gzip']) for row in batches]
    head = payloads[0]
    invariant = (
        'device_id',
        'drive',
        'schema_version',
        'collector',
        'source_fingerprint',
        'batch_count',
        'start_mono_time_ns',
        'segments',
        'car_name',
        'car_fingerprint',
    )
    if any(any(item[key] != head[key] for key in invariant) for item in payloads):
        raise ValueError('Snapshot metadata differs between batches')
    samples, coverage = _streams(payloads)
    latest_cursor = max(row['cursor'] for row in batches)
    records_by_segment = {}
    for sample in samples:
        records_by_segment.setdefault(sample['segment'], []).append(sample)
    for item in coverage:
        records = records_by_segment.get(item['segment'], [])
        db.execute(
            """INSERT INTO segments VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(device,drive,schema_version,segment) DO UPDATE SET
            source=excluded.source,cursor=excluded.cursor,coverage_json=excluded.coverage_json,samples_gzip=excluded.samples_gzip
            WHERE excluded.cursor > segments.cursor""",
            (
                device,
                drive,
                version,
                item['segment'],
                source,
                latest_cursor,
                json.dumps(item),
                packed(records),
            ),
        )
    metadata = {key: value for key, value in head.items() if key != 'data'}
    metadata['signals'] = head['data']['signals']
    materialize_route(db, device, drive, version, metadata, latest_cursor)
    db.execute(
        'INSERT INTO published VALUES(?,?,?,?)', (device, drive, version, source)
    )


def materialize_route(
    db: sqlite3.Connection,
    device: str,
    drive: str,
    version: int,
    metadata: dict,
    latest_cursor: int,
) -> None:
    segment_rows = db.execute(
        'SELECT * FROM segments WHERE device=? AND drive=? AND schema_version=? ORDER BY segment',
        (device, drive, version),
    ).fetchall()
    if not segment_rows:
        return
    grid, coverage, raw_max = _grid(segment_rows)
    metadata = dict(metadata)
    previous_origin = metadata.get(
        'display_origin_ns', metadata.get('start_mono_time_ns')
    )
    metadata['display_origin_ns'] = min(item['first_ns'] for item in coverage)
    metadata['segments'] = [item['segment'] for item in coverage]
    metadata['segment_count'] = len(coverage)
    if (
        metadata.get('recorded_at')
        and not metadata.get('recorded_at_is_reference')
        and previous_origin is not None
    ):
        utc = datetime.fromisoformat(metadata['recorded_at'])
        metadata['recorded_at'] = (
            utc
            + timedelta(
                seconds=(metadata['display_origin_ns'] - previous_origin) / SECOND
            )
        ).isoformat()
    db.execute(
        """INSERT INTO routes VALUES(?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(device,drive,schema_version) DO UPDATE SET cursor=excluded.cursor,metadata_json=excluded.metadata_json,
        coverage_json=excluded.coverage_json,grid_gzip=excluded.grid_gzip,raw_max=excluded.raw_max,
        observed_seconds=excluded.observed_seconds,available_seconds=excluded.available_seconds""",
        (
            device,
            drive,
            version,
            latest_cursor,
            json.dumps(metadata),
            json.dumps(coverage),
            packed(grid),
            raw_max,
            len(grid),
            sum(row['hrr'] is not None for row in grid),
        ),
    )


class Summary:
    def __init__(self):
        self.values: list[float] = []
        self.delta: list[float] = []
        self.drives: set[tuple] = set()
        self.window: deque[float] = deque(maxlen=30)
        self.previous: tuple | None = None
        self.sustained: float | None = None

    def add(self, row: dict, drive: tuple):
        self.values.append(row['hrr'])
        self.drives.add(drive)
        if row['ambient'] is not None:
            self.delta.append(row['hrr'] - row['ambient'])
        if self.previous != (drive, row['time_ns'] - SECOND):
            self.window.clear()
        self.window.append(row['hrr'])
        self.previous = (drive, row['time_ns'])
        if len(self.window) == 30:
            peak = sum(self.window) / 30
            self.sustained = (
                peak if self.sustained is None else max(self.sustained, peak)
            )

    def result(self) -> dict:
        values, delta = sorted(self.values), sorted(self.delta)

        def percentile(data, fraction):
            if not data:
                return None
            position = (len(data) - 1) * fraction
            lo, hi = math.floor(position), math.ceil(position)
            return data[lo] + (data[hi] - data[lo]) * (position - lo)

        return {
            'seconds': len(values),
            'drives': len(self.drives),
            'min': min(values, default=None),
            'median': percentile(values, 0.5),
            'p95': percentile(values, 0.95),
            'max': max(values, default=None),
            'sustained': self.sustained,
            'delta_median': percentile(delta, 0.5),
            'delta_p95': percentile(delta, 0.95),
            'delta_seconds': len(delta),
        }


def selected(row: dict, engine: str, phase: str) -> bool:
    if row['hrr'] is None:
        return False
    value = row['engine']
    if engine != 'All' and (
        value is None
        or not (
            (engine == ENGINE_BANDS[1] and value < 50)
            or (engine == ENGINE_BANDS[2] and 50 <= value < 80)
            or (engine == ENGINE_BANDS[3] and value >= 80)
        )
    ):
        return False
    elapsed = row['elapsed']
    return (
        phase == 'All'
        or (phase == DRIVE_PHASES[4] and elapsed is None)
        or (
            elapsed is not None
            and (
                (phase == DRIVE_PHASES[1] and 0 <= elapsed < 300)
                or (phase == DRIVE_PHASES[2] and 300 <= elapsed < 900)
                or (phase == DRIVE_PHASES[3] and elapsed >= 900)
            )
        )
    )


class TemperatureRepository:
    def __init__(self, path: Path | None = None):
        self.path = path or configured_db_path()
        self._cached_report = lru_cache(maxsize=24)(self._report)

    def state(self) -> dict:
        with closing(connect(self.path)) as db:
            return dict(
                db.execute("SELECT * FROM sync_state WHERE name='telemetry'").fetchone()
            )

    def import_state(self) -> dict:
        with closing(connect(self.path)) as db:
            return dict(
                db.execute(
                    "SELECT * FROM sync_state WHERE name='desktop_import'"
                ).fetchone()
            )

    def record_error(self, error: Exception):
        with closing(connect(self.path)) as db, db:
            db.execute(
                "UPDATE sync_state SET last_error=? WHERE name='telemetry'",
                (str(error)[:300],),
            )

    def ingest(self, page: dict) -> int:
        with closing(connect(self.path)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            cursor = db.execute(
                "SELECT cursor FROM sync_state WHERE name='telemetry'"
            ).fetchone()[0]
            imported, identities = 0, {}
            for item in page['items']:
                item_cursor = item['cursor']
                payload = item['telemetry']
                if not isinstance(item_cursor, int) or item_cursor <= cursor:
                    raise ValueError('Telemetry cursors must increase')
                identity = (
                    payload['device_id'],
                    payload['drive'],
                    payload['collector'],
                    payload['schema_version'],
                    payload['source_fingerprint'],
                )
                if not 0 <= payload['batch_index'] < payload['batch_count']:
                    raise ValueError('Invalid telemetry batch index')
                db.execute(
                    'INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (
                        item_cursor,
                        *identity,
                        payload['batch_index'],
                        payload['batch_count'],
                        item['received_at'],
                        packed(payload),
                    ),
                )
                identities[identity] = item_cursor
                cursor = item_cursor
                imported += 1
            if page.get('next_cursor', cursor) != cursor:
                raise ValueError('Source cursor does not agree with its items')
            for identity in sorted(identities, key=identities.__getitem__):
                _publish(db, identity)
            db.execute(
                "UPDATE sync_state SET cursor=?,last_sync_at=?,last_error=NULL WHERE name='telemetry'",
                (cursor, datetime.now(timezone.utc).isoformat()),
            )
            return imported

    def report(
        self,
        device: str | None = None,
        profile: int | None = None,
        engine: str = 'All',
        phase: str = 'All',
    ) -> dict:
        if engine not in ENGINE_BANDS or phase not in DRIVE_PHASES:
            raise ValueError('Unknown engine or drive-phase filter')
        state = self.state()
        # Synchronization and plot reads do not mutate cached reports.
        return self._cached_report(
            (state['cursor'], self.import_state()['cursor']),
            device,
            profile,
            engine,
            phase,
        )

    def _report(
        self,
        cursor: tuple[int, int],
        device: str | None,
        profile: int | None,
        engine: str,
        phase: str,
    ) -> dict:
        with closing(connect(self.path)) as db:
            profiles = [
                row[0]
                for row in db.execute(
                    'SELECT DISTINCT schema_version FROM routes ORDER BY CASE schema_version WHEN 2 THEN 0 WHEN 1 THEN 1 ELSE 2 END, schema_version'
                )
            ]
            profile = (
                profile if profile is not None else (profiles[0] if profiles else 2)
            )
            devices = [
                row[0]
                for row in db.execute(
                    'SELECT DISTINCT device FROM routes WHERE schema_version=? ORDER BY device',
                    (profile,),
                )
            ]
            routes = db.execute(
                'SELECT * FROM routes WHERE schema_version=? AND (? IS NULL OR device=?) ORDER BY device,drive',
                (profile, device, device),
            ).fetchall()
            total, grouped, segments, drives = Summary(), {}, [], []
            observed = available = 0
            raw_max = None
            for route in routes:
                key = (route['device'], route['drive'])
                grid = unpacked(route['grid_gzip'])
                metadata = json.loads(route['metadata_json'])
                coverage = json.loads(route['coverage_json'])
                observed += route['observed_seconds']
                available += route['available_seconds']
                if route['raw_max'] is not None:
                    raw_max = (
                        route['raw_max']
                        if raw_max is None
                        else max(raw_max, route['raw_max'])
                    )
                drives.append(
                    {
                        'device': key[0],
                        'drive': key[1],
                        'revision': f"{route['cursor']}:{cursor[1]}",
                        'recorded_at': metadata.get('recorded_at'),
                        'git_branch': metadata.get('git_branch'),
                        'segments': len(coverage),
                        'raw_max': route['raw_max'],
                        'observed_seconds': route['observed_seconds'],
                        'available_seconds': route['available_seconds'],
                    }
                )
                segments.extend(
                    {
                        **item,
                        'device': key[0],
                        'drive': key[1],
                        'revision': f"{route['cursor']}:{cursor[1]}",
                        'recorded_at': metadata.get('recorded_at'),
                    }
                    for item in coverage
                )
                for row in grid:
                    if not selected(row, engine, phase):
                        continue
                    total.add(row, key)
                    if row['ambient'] is not None and row['speed'] is not None:
                        cell = (
                            math.floor(row['ambient'] / 5) * 5,
                            bisect_right([5, 30, 70, 100], row['speed']),
                        )
                        grouped.setdefault(cell, Summary()).add(row, key)
            pending = db.execute("""SELECT COUNT(*) FROM (
                SELECT device,drive,schema_version,source,MAX(batch_count) AS expected,COUNT(*) AS actual FROM batches
                WHERE collector='hrr_temperature' GROUP BY device,drive,schema_version,source HAVING actual < expected
            )""").fetchone()[0]
            return {
                'profiles': profiles,
                'profile_details': [
                    {
                        'id': version,
                        'label': (
                            'LS600h Conditions · v2'
                            if version == 2
                            else 'LS600h legacy reports · v1'
                            if version == 1
                            else next(
                                (
                                    row['label']
                                    for row in db.execute(
                                        'SELECT label FROM desktop_profiles WHERE stats_profile=?',
                                        (version,),
                                    )
                                ),
                                f'Desktop profile {version}',
                            )
                        ),
                    }
                    for version in profiles
                ],
                'desktop_import': self.import_state(),
                'profile': profile,
                'devices': devices,
                'device': device,
                'engine_filter': engine,
                'phase_filter': phase,
                'summary': total.result(),
                'raw_max': raw_max,
                'observed_seconds': observed,
                'available_seconds': available,
                'segments': segments,
                'drives': drives,
                'cells': [
                    {'ambient_low': key[0], 'speed_band': key[1], **summary.result()}
                    for key, summary in sorted(grouped.items())
                ],
                'pending_snapshots': pending,
                'speed_bands': SPEED_BANDS,
                'ambient_provisional': True,
                'engine_tentative': True,
                'engine_bands': ENGINE_BANDS,
                'drive_phases': DRIVE_PHASES,
            }

    def drive(self, device: str, drive: str, profile: int = 2) -> dict | None:
        with closing(connect(self.path)) as db:
            route = db.execute(
                'SELECT * FROM routes WHERE device=? AND drive=? AND schema_version=?',
                (device, drive, profile),
            ).fetchone()
            if route is None:
                return None
            metadata = json.loads(route['metadata_json'])
            segments = db.execute(
                'SELECT samples_gzip FROM segments WHERE device=? AND drive=? AND schema_version=? ORDER BY segment',
                (device, drive, profile),
            ).fetchall()
            samples = [
                sample for item in segments for sample in unpacked(item['samples_gzip'])
            ]
            samples.sort(key=lambda item: (item['time_ns'], item['metric']))
            return {
                'device': device,
                'drive': drive,
                'profile': profile,
                'revision': f"{route['cursor']}:{self.import_state()['cursor']}",
                'metadata': metadata,
                'coverage': json.loads(route['coverage_json']),
                'samples': samples,
            }


repository = TemperatureRepository()
