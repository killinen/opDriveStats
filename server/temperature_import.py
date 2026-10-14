"""Import a desktop HRR library without consuming remote telemetry cursors.

Run from the checkout: python -m server.temperature_import /path/library.sqlite
"""

from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

from server.temperature_data import (
    MAX_AGE,
    TemperatureRepository,
    connect,
    materialize_route,
    packed,
)

# Definition signatures for the current LS600h collector. Schema-file hashes do
# not affect these signal definitions; a different decoder stays a separate view.
_DEFINITIONS = {
    'ambient': (1, 0x3B0, [('AMBIENT_TEMP', 24, 8, 'little_endian', False, 1, -49)]),
    'engine': (1, 0x39, [('ENGINE_TEMP', 0, 8, 'little_endian', False, 1, 0)]),
    'hrr': (
        2,
        0x638,
        [
            ('MCU_Temperature', 0, 16, 'little_endian', True, 1, 0),
            ('MCU_Temperature_Valid', 16, 1, 'little_endian', False, 1, 0),
            ('Temperature_Counter', 24, 8, 'little_endian', False, 1, 0),
        ],
    ),
    'speed': (0, 0xB4, [('SPEED', 47, 16, 'big_endian', False, 0.01, 0)]),
}


def compatible(config: dict) -> bool:
    try:
        if config['decoder_version'] != 1 or set(config['bindings']) != set(MAX_AGE):
            return False
        for metric, (bus, address, expected) in _DEFINITIONS.items():
            binding, definition = (
                config['bindings'][metric],
                config['definitions'][metric],
            )
            actual = [
                tuple(
                    signal[key]
                    for key in (
                        'name',
                        'start',
                        'length',
                        'order',
                        'signed',
                        'scale',
                        'offset',
                    )
                )
                for signal in definition['signals']
            ]
            if (
                binding['bus'] != bus
                or definition['address'] != address
                or actual != expected
            ):
                return False
            if binding['signal'] != expected[0][0]:
                return False
            if binding.get('valid_signal', '') != (
                'MCU_Temperature_Valid' if metric == 'hrr' else ''
            ):
                return False
        return True
    except (KeyError, TypeError):
        return False


@contextmanager
def library_snapshot(source: Path):
    """Copy a quiescent SQLite file set, including committed WAL data.

    SQLite may create sidecars even for read-only connections. Work on a private
    copy so importing never writes beside the original desktop library.
    """
    source = source.expanduser().resolve()
    if not source.is_file():
        raise ValueError(f'Desktop library does not exist: {source}')
    with tempfile.TemporaryDirectory(prefix='hrr-library-') as directory:
        target = Path(directory) / 'library.sqlite'
        files = [source, Path(str(source) + '-wal'), Path(str(source) + '-journal')]

        def signatures():
            return [
                (path.stat().st_size, path.stat().st_mtime_ns)
                if path.exists()
                else None
                for path in files
            ]

        before = signatures()
        for path, signature in zip(files, before):
            if signature is not None:
                shutil.copyfile(path, Path(str(target) + str(path)[len(str(source)) :]))
        if before != signatures():
            raise ValueError(
                'Desktop library changed during snapshot; wait for its import to finish and retry'
            )
        with closing(sqlite3.connect(target)) as copied:
            copied.row_factory = sqlite3.Row
            if copied.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('Desktop library failed SQLite integrity check')
            yield copied


def _profile(db, row):
    saved = db.execute(
        'SELECT * FROM desktop_profiles WHERE source_profile=?', (row['id'],)
    ).fetchone()
    if saved:
        if json.loads(saved['config_json']) != json.loads(row['config']):
            raise ValueError(
                'Desktop profile configuration changed without a new profile hash'
            )
        return saved['stats_profile']
    config = json.loads(row['config'])
    version = (
        2
        if compatible(config)
        else max(
            999,
            db.execute(
                'SELECT COALESCE(MAX(stats_profile),999) FROM desktop_profiles'
            ).fetchone()[0],
        )
        + 1
    )
    label = f'Desktop library · {row["id"][:12]}'
    db.execute(
        'INSERT INTO desktop_profiles VALUES(?,?,?,?)',
        (row['id'], version, row['config'], label),
    )
    return version


def import_library(
    source: Path,
    repository: TemperatureRepository,
    profile: str | None = None,
    device: str | None = None,
) -> dict:
    result = {
        'routes': 0,
        'segments': 0,
        'samples': 0,
        'cached_segments': 0,
        'telemetry_segments_kept': 0,
        'profiles': [],
    }
    if source.expanduser().resolve() == repository.path.expanduser().resolve():
        raise ValueError(
            'Source library and destination database must be different files'
        )
    with library_snapshot(source) as library, closing(
        connect(repository.path)
    ) as db, db:
        required = {'profiles', 'segments', 'samples'}
        tables = {
            row[0]
            for row in library.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not required <= tables:
            raise ValueError('Not an HRR desktop library')
        profiles = library.execute(
            'SELECT * FROM profiles WHERE (? IS NULL OR id=?) ORDER BY id',
            (profile, profile),
        ).fetchall()
        if not profiles:
            raise ValueError('No matching desktop profile')
        db.execute('BEGIN IMMEDIATE')
        generation = (
            db.execute(
                "SELECT cursor FROM sync_state WHERE name='desktop_import'"
            ).fetchone()[0]
            + 1
        )
        imported_at = datetime.now(timezone.utc).isoformat()
        changed_routes = {}
        for source_profile in profiles:
            version = _profile(db, source_profile)
            config = json.loads(source_profile['config'])
            result['profiles'].append(
                {'source': source_profile['id'], 'profile': version}
            )
            segments = library.execute(
                'SELECT * FROM segments WHERE profile=? AND (? IS NULL OR device=?) ORDER BY device,route,segment_index,id',
                (source_profile['id'], device, device),
            ).fetchall()
            seen = set()
            for segment_row in segments:
                original = dict(segment_row)
                route_device = original['device']
                drive = original['route'].removeprefix(route_device + '/')
                index = original['segment_index']
                # Unknown indices cannot establish route start; negative IDs are
                # internal identifiers, while the original null stays archived.
                segment = index if index is not None else -original['id']
                identity = (route_device, drive, version, segment)
                if identity in seen:
                    raise ValueError(
                        f'Desktop library has conflicting segment identities: {identity}'
                    )
                seen.add(identity)
                samples = [
                    dict(row)
                    for row in library.execute(
                        'SELECT metric,time_ns,value,bus FROM samples WHERE segment_id=? ORDER BY time_ns,metric',
                        (original['id'],),
                    )
                ]
                for sample in samples:
                    if sample['metric'] not in MAX_AGE:
                        raise ValueError(f'Unknown desktop metric: {sample["metric"]}')
                    if (
                        original['first_ns'] is None
                        or original['last_ns'] is None
                        or not original['first_ns']
                        <= sample['time_ns']
                        <= original['last_ns']
                    ):
                        raise ValueError('Desktop sample outside segment coverage')
                archive = packed(samples)
                fingerprint = hashlib.sha256(packed(original) + archive).hexdigest()
                if db.execute(
                    'SELECT 1 FROM desktop_archive WHERE source_profile=? AND source_segment_id=? AND fingerprint=?',
                    (source_profile['id'], original['id'], fingerprint),
                ).fetchone():
                    result['cached_segments'] += 1
                    continue
                db.execute(
                    'INSERT INTO desktop_archive VALUES(?,?,?,?,?,?)',
                    (
                        source_profile['id'],
                        original['id'],
                        fingerprint,
                        json.dumps(original),
                        archive,
                        imported_at,
                    ),
                )
                result['segments'] += 1
                result['samples'] += len(samples)
                current = db.execute(
                    'SELECT cursor FROM segments WHERE device=? AND drive=? AND schema_version=? AND segment=?',
                    identity,
                ).fetchone()
                if current and current['cursor'] > 0:
                    result['telemetry_segments_kept'] += 1
                    continue
                if original['first_ns'] is None or original['last_ns'] is None:
                    # Archive undecodable segments without inventing a CAN interval.
                    continue
                coverage = {
                    'segment': segment,
                    'first_ns': original['first_ns'],
                    'last_ns': original['last_ns'],
                    'source_kind': 'desktop_library',
                    'source_profile': source_profile['id'],
                    'source_segment_index': index,
                    'hrr_status': original['hrr_status'],
                    'hrr_reports': original['hrr_reports'],
                    'hrr_valid': original['hrr_valid'],
                    'hrr_invalid': original['hrr_reports'] - original['hrr_valid'],
                    'hrr_malformed': original['hrr_malformed'],
                    'warnings': original['warnings'],
                    'utc_epoch': original['utc_epoch'],
                    'can_events': None,
                    'hrr_frames': None,
                    'hrr_repeated': None,
                    'estimated': False,
                }
                records = [
                    {**sample, 'segment': segment, 'source_segment_index': index}
                    for sample in samples
                ]
                db.execute(
                    'INSERT INTO segments VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(device,drive,schema_version,segment) DO UPDATE SET source=excluded.source,cursor=excluded.cursor,coverage_json=excluded.coverage_json,samples_gzip=excluded.samples_gzip',
                    (
                        *identity,
                        'desktop:' + fingerprint,
                        -generation,
                        json.dumps(coverage),
                        packed(records),
                    ),
                )
                metadata = {
                    'source_kind': 'desktop_library',
                    'source_profile': source_profile['id'],
                    'source_config': config,
                    'source_route': original['route'],
                    'device_id': route_device,
                    'drive': drive,
                    'schema_version': version,
                    'signals': config['bindings'],
                    'recorded_at_is_reference': True,
                    'recorded_at': datetime.fromtimestamp(
                        original['utc_epoch'], timezone.utc
                    ).isoformat()
                    if original['utc_epoch'] is not None
                    else None,
                }
                changed_routes.setdefault(identity[:3], metadata)
        for (route_device, drive, version), fallback in changed_routes.items():
            previous = db.execute(
                'SELECT * FROM routes WHERE device=? AND drive=? AND schema_version=?',
                (route_device, drive, version),
            ).fetchone()
            metadata = json.loads(previous['metadata_json']) if previous else fallback
            cursor = (
                max(previous['cursor'], -generation)
                if previous and previous['cursor'] > 0
                else -generation
            )
            materialize_route(db, route_device, drive, version, metadata, cursor)
        result['routes'] = len(changed_routes)
        if result['segments']:
            db.execute(
                "UPDATE sync_state SET cursor=?,last_sync_at=?,last_error=NULL WHERE name='desktop_import'",
                (generation, imported_at),
            )
    repository._cached_report.cache_clear()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('library', type=Path)
    parser.add_argument('--database', type=Path)
    parser.add_argument('--profile', help='Original desktop decoder-profile hash')
    parser.add_argument('--device', help='Import only this device')
    args = parser.parse_args()
    print(
        json.dumps(
            import_library(
                args.library,
                TemperatureRepository(args.database),
                args.profile,
                args.device,
            ),
            indent=2,
        )
    )


if __name__ == '__main__':
    main()
