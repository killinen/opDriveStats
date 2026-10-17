"""Publish complete TRQI snapshots and calculate cabin-oriented conditions.

Own tables avoid conflating HRR schema numbers with TRQI profiles. The shared
replica owns immutable batches and its cursor; this module owns derived data.
"""

from __future__ import annotations

import json
import math
import sqlite3
from bisect import bisect_right
from collections import deque
from contextlib import closing
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from ..temperature_data import SECOND, configured_db_path, connect, packed, unpacked

COLLECTOR = 'trqi_temperature'
MAX_AGE = {
    'trqi': 1.5,
    'speed': 2,
    'outside': 60,
    'outside_sensor': 60,
    'blower_on': 5,
    'ac_request': 5,
    'compressor_load': 5,
    'lateral_active': 2,
    'relay_active': 2,
    'steering_demand': 2,
}
PHASES = ('0–5 min', '5–15 min', '15–30 min', '30+ min', 'Unknown start')


def initialize(db):
    # execute(), not executescript(): publishing is part of the replica transaction.
    for statement in (
        """CREATE TABLE IF NOT EXISTS trqi_segments (
            device TEXT, drive TEXT, version INTEGER, segment INTEGER,
            cursor INTEGER NOT NULL, coverage_json TEXT NOT NULL, samples_gzip BLOB NOT NULL,
            PRIMARY KEY(device,drive,version,segment))""",
        """CREATE TABLE IF NOT EXISTS trqi_routes (
            device TEXT, drive TEXT, version INTEGER, cursor INTEGER NOT NULL,
            metadata_json TEXT NOT NULL, coverage_json TEXT NOT NULL, grid_gzip BLOB NOT NULL,
            summary_json TEXT NOT NULL, PRIMARY KEY(device,drive,version))""",
        """CREATE TABLE IF NOT EXISTS trqi_published (
            device TEXT, drive TEXT, version INTEGER, source TEXT,
            PRIMARY KEY(device,drive,version,source))""",
    ):
        db.execute(statement)


def publish(db: sqlite3.Connection, identity: tuple) -> None:
    device, drive, collector, version, source = identity
    if collector != COLLECTOR or version != 2:
        return
    initialize(db)
    if db.execute(
        'SELECT 1 FROM trqi_published WHERE device=? AND drive=? AND version=? AND source=?',
        (device, drive, version, source),
    ).fetchone():
        return
    batches = db.execute(
        """SELECT * FROM batches WHERE device=? AND drive=? AND collector=?
           AND schema_version=? AND source=? ORDER BY batch_index""",
        identity,
    ).fetchall()
    if not batches or len(batches) != batches[0]['batch_count']:
        return
    if [batch['batch_index'] for batch in batches] != list(range(len(batches))):
        raise ValueError('Invalid TRQI batch indices')
    payloads = [unpacked(batch['payload_gzip']) for batch in batches]
    head = payloads[0]
    invariant = (
        'device_id',
        'drive',
        'collector',
        'schema_version',
        'source_fingerprint',
        'batch_count',
        'start_mono_time_ns',
        'segments',
        'car_name',
        'car_fingerprint',
    )
    if any(
        any(payload[key] != head[key] for key in invariant)
        or payload['data']['signals'] != head['data']['signals']
        for payload in payloads
    ):
        raise ValueError('TRQI snapshot metadata changed across batches')
    coverage, samples = {}, []
    for payload in payloads:
        for interval in payload['data'].get('coverage', []):
            if coverage.setdefault(interval['segment'], interval) != interval:
                raise ValueError('TRQI snapshot coverage changed across batches')
        samples.extend(payload['data']['samples'])
    if set(coverage) != set(head['segments']):
        raise ValueError('TRQI snapshot requires coverage for every listed segment')
    grouped = {segment: [] for segment in coverage}
    for sample in samples:
        interval = coverage.get(sample['segment'])
        if (
            interval is None
            or not interval['first_ns'] <= sample['time_ns'] <= interval['last_ns']
        ):
            raise ValueError('TRQI sample lies outside observed log coverage')
        grouped[sample['segment']].append(sample)
    cursor = max(batch['cursor'] for batch in batches)
    for segment, interval in coverage.items():
        db.execute(
            """INSERT INTO trqi_segments VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(device,drive,version,segment) DO UPDATE SET
               cursor=excluded.cursor,coverage_json=excluded.coverage_json,samples_gzip=excluded.samples_gzip
               WHERE excluded.cursor > trqi_segments.cursor""",
            (
                device,
                drive,
                version,
                segment,
                cursor,
                json.dumps(interval),
                packed(grouped[segment]),
            ),
        )
    retained = db.execute(
        'SELECT * FROM trqi_segments WHERE device=? AND drive=? AND version=? ORDER BY segment',
        (device, drive, version),
    ).fetchall()
    coverage = [json.loads(row['coverage_json']) for row in retained]
    samples = [sample for row in retained for sample in unpacked(row['samples_gzip'])]
    grid = make_grid(samples, coverage)
    metadata = {key: value for key, value in head.items() if key != 'data'}
    metadata['signals'] = head['data']['signals']
    metadata['display_origin_ns'] = min(item['first_ns'] for item in coverage)
    metadata['segments'] = sorted(item['segment'] for item in coverage)
    metadata['segment_count'] = len(coverage)
    if metadata.get('recorded_at'):
        metadata['recorded_at'] = (
            datetime.fromisoformat(metadata['recorded_at'])
            + timedelta(
                seconds=(metadata['display_origin_ns'] - metadata['start_mono_time_ns'])
                / SECOND,
            )
        ).isoformat()
    old = db.execute(
        'SELECT metadata_json FROM trqi_routes WHERE device=? AND drive=? AND version=?',
        (device, drive, version),
    ).fetchone()
    firmware = (
        json.loads(old['metadata_json']).get('firmware_frames', []) if old else []
    )
    firmware.extend(
        frame
        for payload in payloads
        for frame in payload['data'].get('firmware_frames', [])
    )
    metadata['firmware_frames'] = list(
        {
            (frame['time_ns'], frame['address'], frame['data']): frame
            for frame in firmware
        }.values()
    )
    metadata['firmware'] = firmware_identity(metadata['firmware_frames'])
    summary = summarize(grid, samples)
    db.execute(
        """INSERT INTO trqi_routes VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(device,drive,version)
           DO UPDATE SET cursor=excluded.cursor,metadata_json=excluded.metadata_json,
           coverage_json=excluded.coverage_json,grid_gzip=excluded.grid_gzip,summary_json=excluded.summary_json""",
        (
            device,
            drive,
            version,
            cursor,
            json.dumps(metadata),
            json.dumps(coverage),
            packed(grid),
            json.dumps(summary),
        ),
    )
    db.execute(
        'INSERT INTO trqi_published VALUES(?,?,?,?)', (device, drive, version, source)
    )


def firmware_identity(frames: list[dict]) -> str:
    # Passive protocol v1 identity; mixed identities stay explicit, never silently pooled.
    info0 = [bytes.fromhex(row['data']) for row in frames if row['address'] == 0x639]
    versions = {
        '.'.join(str(value) for value in data[5:8])
        for data in info0
        if data[0] == 1 and int.from_bytes(data[1:5], 'little') == 0x54525101
    }
    builds = {
        f'{int.from_bytes(bytes.fromhex(row["data"])[4:8], "little"):08x}'
        for row in frames
        if row['address'] == 0x63A
    }
    if len(versions) == len(builds) == 1:
        return f'{next(iter(versions))} / {next(iter(builds))}'
    return 'Mixed identities' if len(versions) > 1 or len(builds) > 1 else 'Unknown'


def make_grid(samples: list[dict], coverage: list[dict]) -> list[dict]:
    origin = next((row['first_ns'] for row in coverage if row['segment'] == 0), None)
    # Freshness may cross contiguous segment boundaries, but never a missing segment.
    block_for, block, previous = {}, 0, None
    for interval in sorted(coverage, key=lambda row: row['first_ns']):
        if previous and (
            interval['segment'] != previous['segment'] + 1
            or interval['first_ns'] - previous['last_ns'] > 1.5 * SECOND
        ):
            block += 1
        block_for[interval['segment']] = block
        previous = interval
    streams = {}
    for sample in sorted(samples, key=lambda row: row['time_ns']):
        key = block_for[sample['segment']], sample['metric']
        streams.setdefault(key, {})[sample['time_ns']] = sample
    streams = {
        key: (list(values), list(values.values())) for key, values in streams.items()
    }
    slots = {}
    for interval in coverage:
        for timestamp in range(
            (interval['first_ns'] + SECOND - 1) // SECOND * SECOND,
            interval['last_ns'] // SECOND * SECOND + SECOND,
            SECOND,
        ):
            slots[timestamp] = interval['segment']
    grid = []
    for timestamp, segment in sorted(slots.items()):
        elapsed = (timestamp - origin) / SECOND if origin is not None else None
        row = {
            'time_ns': timestamp,
            'segment': segment,
            'block': block_for[segment],
            'elapsed': elapsed,
            'phase': PHASES[4]
            if elapsed is None
            else PHASES[bisect_right([300, 900, 1800], elapsed)],
        }
        for metric, max_age in MAX_AGE.items():
            times, reports = streams.get((block_for[segment], metric), ([], []))
            index = bisect_right(times, timestamp) - 1
            report = (
                reports[index]
                if index >= 0 and timestamp - times[index] <= max_age * SECOND
                else None
            )
            row[metric] = report['value'] if report else None
            if metric == 'trqi':
                # Bad CRCs terminate temperature availability and cannot assert faults.
                status = report['status'] if report and report['crc_valid'] else None
                row.update(
                    high_temp_error=bool(status & 2) if status is not None else None,
                    disengage_pending=bool(status & 4) if status is not None else None,
                    vdda_warn=bool(status & 16) if status is not None else None,
                    reset_cause=(status >> 5) if status is not None else None,
                )
        grid.append(row)
    return grid


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    pos = (len(values) - 1) * fraction
    low, high = math.floor(pos), math.ceil(pos)
    return values[low] + (values[high] - values[low]) * (pos - low)


def summarize(grid: list[dict], samples: list[dict] | None = None) -> dict:
    valid = [row for row in grid if row['trqi'] is not None]
    values = [row['trqi'] for row in valid]
    window, previous, sustained = deque(maxlen=30), None, None
    for row in valid:
        if previous != (row.get('block'), row['time_ns'] - SECOND):
            window.clear()
        window.append(row['trqi'])
        previous = (row.get('block'), row['time_ns'])
        if len(window) == 30:
            mean = sum(window) / 30
            sustained = mean if sustained is None else max(sustained, mean)
    startup = next(
        (
            row
            for row in valid
            if row['elapsed'] is not None and 0 <= row['elapsed'] <= 30
        ),
        None,
    )
    initial = startup['trqi'] if startup else None
    known = next((row for row in grid if row['elapsed'] is not None), None)
    if samples and known:
        origin = known['time_ns'] - round(known['elapsed'] * SECOND)
        first = next(
            (
                row
                for row in sorted(samples, key=lambda item: item['time_ns'])
                if row['metric'] == 'trqi'
                and row['value'] is not None
                and 0 <= row['time_ns'] - origin <= 30 * SECOND
            ),
            None,
        )
        initial = first['value'] if first else None
    raw = [
        row['value']
        for row in samples or []
        if row['metric'] == 'trqi' and row['value'] is not None
    ]
    return {
        'observed_seconds': len(grid),
        'valid_seconds': len(values),
        'initial': initial,
        'median': percentile(values, 0.5),
        'p95': percentile(values, 0.95),
        'maximum': max(values, default=None),
        'raw_maximum': max(raw, default=None),
        'sustained_30s': sustained,
        'seconds_ge_82': sum(value >= 82 for value in values),
        'seconds_ge_85': sum(value >= 85 for value in values),
        'fault_seconds': sum(row['high_temp_error'] is True for row in grid),
        'pending_seconds': sum(row['disengage_pending'] is True for row in grid),
        'vdda_warn_seconds': sum(row['vdda_warn'] is True for row in grid),
        **{
            f'delta_{minute}m': next(
                (
                    row['trqi'] - initial
                    for row in valid
                    if initial is not None
                    and row['elapsed'] is not None
                    and minute * 60 <= row['elapsed'] < minute * 60 + 2
                ),
                None,
            )
            for minute in (5, 15, 30)
        },
    }


def condition_cells(routes: list[dict], phase: str, hvac: str) -> list[dict]:
    groups = {}
    for route in routes:
        initial = route['summary']['initial']
        for row in route['grid']:
            if row['trqi'] is None or (phase != 'All' and row['phase'] != phase):
                continue
            blower = (
                'Unknown'
                if row['blower_on'] is None
                else 'On'
                if row['blower_on']
                else 'Off'
            )
            if hvac != 'All' and blower != hvac:
                continue
            startup_band = None if initial is None else math.floor(initial / 10) * 10
            outside_band = (
                None if row['outside'] is None else math.floor(row['outside'] / 5) * 5
            )
            speed_band = (
                None
                if row['speed'] is None
                else (
                    '0–5'
                    if row['speed'] < 5
                    else '5–30'
                    if row['speed'] < 30
                    else '30–70'
                    if row['speed'] < 70
                    else '70+'
                )
            )
            # Independent groupings: absent outside/HVAC/speed never hides core temperature evidence.
            for kind, band in (
                ('startup', startup_band),
                ('outside', outside_band),
                ('blower', blower),
                ('speed', speed_band),
            ):
                key = kind, band, row['phase']
                group = groups.setdefault(key, {'rows': [], 'drives': set()})
                group['rows'].append(row)
                group['drives'].add((route['device'], route['drive']))
    result = []
    for (kind, band, row_phase), group in groups.items():
        values = [row['trqi'] for row in group['rows']]
        result.append(
            {
                'kind': kind,
                'band': band,
                'phase': row_phase,
                'seconds': len(values),
                'drives': len(group['drives']),
                'median': percentile(values, 0.5),
                'p95': percentile(values, 0.95),
                'maximum': max(values),
                'seconds_ge_82': sum(value >= 82 for value in values),
                'seconds_ge_85': sum(value >= 85 for value in values),
            }
        )
    return sorted(
        result,
        key=lambda row: (row['kind'], str(row['band']), PHASES.index(row['phase'])),
    )


class Repository:
    def __init__(self, path: Path | None = None):
        self.path = path or configured_db_path()
        self._cached_report = lru_cache(maxsize=8)(self._report)

    def _routes(self, device=None, firmware=None, drive=None):
        with closing(connect(self.path)) as db, db:
            initialize(db)
            rows = db.execute(
                """SELECT * FROM trqi_routes WHERE (? IS NULL OR device=?)
                   AND (? IS NULL OR drive=?)
                   AND (? IS NULL OR json_extract(metadata_json,'$.firmware')=?) ORDER BY device,drive""",
                (device, device, drive, drive, firmware, firmware),
            ).fetchall()
            routes = [
                {
                    **dict(row),
                    'metadata': json.loads(row['metadata_json']),
                    'coverage': json.loads(row['coverage_json']),
                    'grid': unpacked(row['grid_gzip']),
                    'summary': json.loads(row['summary_json']),
                }
                for row in rows
            ]
        return routes

    def report(self, device=None, firmware=None, phase='All', hvac='All'):
        if phase not in ('All', *PHASES) or hvac not in ('All', 'On', 'Off', 'Unknown'):
            raise ValueError('Unknown TRQI condition filter')
        with closing(connect(self.path)) as db, db:
            initialize(db)
            state = dict(
                db.execute("SELECT * FROM sync_state WHERE name='telemetry'").fetchone()
            )
        result = self._cached_report(device, firmware, phase, hvac, state['cursor'])
        return {**result, 'sync': state}

    def _report(self, device, firmware, phase, hvac, revision):
        with closing(connect(self.path)) as db:
            options = db.execute(
                'SELECT device,metadata_json FROM trqi_routes'
            ).fetchall()
        routes = self._routes(device, firmware)
        selected = []
        for route in routes:
            selected.extend(
                row
                for row in route['grid']
                if (phase == 'All' or row['phase'] == phase)
                and (
                    hvac == 'All'
                    or hvac
                    == (
                        'Unknown'
                        if row['blower_on'] is None
                        else 'On'
                        if row['blower_on']
                        else 'Off'
                    )
                )
            )
        values = [row['trqi'] for row in selected if row['trqi'] is not None]
        # Avoid rolling means crossing drives, and distinguish global raw peaks from filtered grid peaks.
        summary = {
            'observed_seconds': len(selected),
            'valid_seconds': len(values),
            'median': percentile(values, 0.5),
            'p95': percentile(values, 0.95),
            'maximum': max(values, default=None),
            'seconds_ge_82': sum(value >= 82 for value in values),
            'seconds_ge_85': sum(value >= 85 for value in values),
        }
        with closing(connect(self.path)) as db:
            pending = db.execute(
                """SELECT COUNT(*) FROM (
                SELECT device,drive,schema_version,source,COUNT(*) AS actual,MAX(batch_count) AS expected
                FROM batches WHERE collector=? GROUP BY device,drive,schema_version,source
                HAVING actual < expected)""",
                (COLLECTOR,),
            ).fetchone()[0]
        return {
            'collector': COLLECTOR,
            'phases': PHASES,
            'devices': sorted({row['device'] for row in options}),
            'firmwares': sorted(
                {json.loads(row['metadata_json'])['firmware'] for row in options}
            ),
            'filters': {
                'device': device,
                'firmware': firmware,
                'phase': phase,
                'hvac': hvac,
            },
            'summary': summary,
            'cells': condition_cells(routes, phase, hvac),
            'drives': [
                {
                    key: row[key]
                    for key in (
                        'device',
                        'drive',
                        'version',
                        'cursor',
                        'metadata',
                        'coverage',
                        'summary',
                    )
                }
                for row in routes
            ],
            'pending_snapshots': pending,
        }

    def drive(self, device: str, drive: str):
        route = next(iter(self._routes(device, drive=drive)), None)
        if route is None:
            return None
        with closing(connect(self.path)) as db:
            records = db.execute(
                'SELECT samples_gzip FROM trqi_segments WHERE device=? AND drive=? AND version=? ORDER BY segment',
                (device, drive, route['version']),
            ).fetchall()
        samples = [
            sample for record in records for sample in unpacked(record['samples_gzip'])
        ]
        samples.sort(key=lambda row: (row['time_ns'], row['metric']))
        return {
            key: route[key]
            for key in ('device', 'drive', 'cursor', 'metadata', 'coverage', 'grid', 'summary')
        } | {'samples': samples}


repository = Repository()
