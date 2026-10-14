from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from server.temperature_data import TemperatureRepository, connect, unpacked
from server.temperature_import import (
    _DEFINITIONS,
    compatible,
    import_library,
    library_snapshot,
)
from server.test_temperatures import BASE, SECOND, fixture, page


def config():
    bindings, definitions = {}, {}
    for metric, (bus, address, signatures) in _DEFINITIONS.items():
        bindings[metric] = {
            'bus': bus,
            'message': metric,
            'signal': signatures[0][0],
            'valid_signal': 'MCU_Temperature_Valid' if metric == 'hrr' else '',
        }
        definitions[metric] = {
            'address': address,
            'signals': [
                dict(
                    zip(
                        (
                            'name',
                            'start',
                            'length',
                            'order',
                            'signed',
                            'scale',
                            'offset',
                        ),
                        signature,
                    )
                )
                for signature in signatures
            ],
        }
    return {
        'decoder_version': 1,
        'bindings': bindings,
        'definitions': definitions,
        'schema': {'log.capnp': 'original-hash'},
    }


class DesktopImportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.source, self.repo = (
            root / 'library.sqlite',
            TemperatureRepository(root / 'temperatures.sqlite'),
        )
        with sqlite3.connect(self.source) as db:
            db.executescript("""
                CREATE TABLE profiles(id TEXT PRIMARY KEY,config TEXT,created TEXT);
                CREATE TABLE segments(id INTEGER PRIMARY KEY,profile TEXT,digest TEXT,route TEXT,device TEXT,
                  first_ns INTEGER,last_ns INTEGER,segment_index INTEGER,utc_epoch REAL,hrr_status TEXT,
                  hrr_reports INTEGER,hrr_valid INTEGER,hrr_malformed INTEGER,warnings TEXT);
                CREATE TABLE samples(segment_id INTEGER,metric TEXT,time_ns INTEGER,value REAL,bus INTEGER);
            """)
            db.execute(
                'INSERT INTO profiles VALUES(?,?,?)',
                ('original-profile', json.dumps(config()), '2026-01-01'),
            )
        self.add_segment(1, 0)

    def add_segment(self, identity, index, offset=0):
        with sqlite3.connect(self.source) as db:
            db.execute(
                'INSERT INTO segments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (
                    identity,
                    'original-profile',
                    'digest' + str(identity),
                    'device_123/route',
                    'device_123',
                    BASE + offset * SECOND,
                    BASE + (offset + 3) * SECOND,
                    index,
                    None,
                    'valid',
                    3,
                    2,
                    0,
                    'Provisional ambient',
                ),
            )
            db.executemany(
                'INSERT INTO samples VALUES(?,?,?,?,?)',
                [
                    (identity, 'hrr', BASE + (offset + 0.1) * SECOND, 0, 2),
                    (identity, 'hrr', BASE + (offset + 0.2) * SECOND, 99, 2),
                    (identity, 'hrr', BASE + (offset + 1.2) * SECOND, None, 2),
                    (identity, 'ambient', BASE + (offset + 0.7) * SECOND, 20, 1),
                    (identity, 'speed', BASE + (offset + 0.8) * SECOND, 90, 0),
                    (identity, 'engine', BASE + (offset + 0.9) * SECOND, 85, 1),
                ],
            )

    def test_lossless_idempotent_import_preserves_source_and_cursor(self):
        before = hashlib.sha256(self.source.read_bytes()).digest()
        result = import_library(self.source, self.repo)
        self.assertEqual(
            (result['routes'], result['segments'], result['samples']), (1, 1, 6)
        )
        self.assertEqual(self.repo.state()['cursor'], 0)
        drive = self.repo.drive('device_123', 'route')
        self.assertEqual(
            [s['value'] for s in drive['samples'] if s['metric'] == 'hrr'],
            [0, 99, None],
        )
        self.assertEqual(drive['samples'][0]['time_ns'], BASE + 100_000_000)
        self.assertEqual(drive['samples'][0]['bus'], 2)
        self.assertIsNone(drive['coverage'][0]['hrr_frames'])
        self.assertEqual(drive['coverage'][0]['hrr_reports'], 3)
        self.assertEqual(
            drive['metadata']['source_config']['schema']['log.capnp'], 'original-hash'
        )
        second = import_library(self.source, self.repo)
        self.assertEqual((second['segments'], second['cached_segments']), (0, 1))
        self.assertEqual(self.repo.import_state()['cursor'], 1)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).digest(), before)
        self.assertFalse(Path(str(self.source) + '-wal').exists())
        self.assertEqual(self.repo.report()['raw_max'], 99)

    def test_native_telemetry_wins_in_both_import_orders(self):
        for native_first in (True, False):
            repo = TemperatureRepository(
                Path(self.directory.name) / f'order{native_first}.sqlite'
            )
            native = page(fixture(seconds=3, spike=1))
            if native_first:
                repo.ingest(native)
            result = import_library(self.source, repo)
            if not native_first:
                repo.ingest(native)
            actual = repo.drive('device_123', 'route')
            self.assertTrue(
                all(
                    'counter' in row
                    for row in actual['samples']
                    if row['metric'] == 'hrr'
                )
            )
            self.assertEqual(repo.state()['cursor'], 1)
            self.assertEqual(result['telemetry_segments_kept'], int(native_first))
            with closing(connect(repo.path)) as db:
                self.assertEqual(
                    db.execute('SELECT COUNT(*) FROM desktop_archive').fetchone()[0], 1
                )

    def test_import_invalidates_other_process_cache_and_keeps_missing_segments(self):
        import_library(self.source, self.repo)
        other = TemperatureRepository(self.repo.path)
        original = other.report()
        self.add_segment(2, 1, offset=4)
        import_library(self.source, self.repo)
        self.assertEqual(len(other.report()['segments']), 2)
        self.assertNotEqual(
            original['drives'][0]['revision'], other.report()['drives'][0]['revision']
        )
        self.repo.ingest(page(fixture(seconds=3)))
        self.assertEqual(len(self.repo.drive('device_123', 'route')['coverage']), 2)

    def test_custom_decoder_separate_and_native_default_preferred(self):
        custom = config()
        custom['definitions']['ambient']['signals'][0]['offset'] = -50
        self.assertFalse(compatible(custom))
        with sqlite3.connect(self.source) as db:
            db.execute('UPDATE profiles SET config=?', (json.dumps(custom),))
        imported = import_library(self.source, self.repo)
        self.assertEqual(imported['profiles'][0]['profile'], 1000)
        self.repo.ingest(page(fixture()))
        self.assertEqual(self.repo.report()['profile'], 2)
        self.assertEqual(len(self.repo.report(profile=1000)['drives']), 1)

    def test_unknown_index_does_not_invent_route_start_or_utc_anchor(self):
        with sqlite3.connect(self.source) as db:
            db.execute('UPDATE segments SET segment_index=NULL,utc_epoch=1234')
        import_library(self.source, self.repo)
        drive = self.repo.drive('device_123', 'route')
        self.assertIsNone(drive['coverage'][0]['source_segment_index'])
        self.assertTrue(drive['metadata']['recorded_at_is_reference'])
        with closing(connect(self.repo.path)) as db:
            rows = unpacked(db.execute('SELECT grid_gzip FROM routes').fetchone()[0])
        self.assertTrue(all(row['elapsed'] is None for row in rows))

    def test_snapshot_reads_committed_wal_without_touching_source(self):
        with closing(sqlite3.connect(self.source)) as live:
            live.execute('PRAGMA journal_mode=WAL')
            live.execute("INSERT INTO profiles VALUES('second','{}','today')")
            live.commit()
            paths = [
                self.source,
                Path(str(self.source) + '-wal'),
                Path(str(self.source) + '-shm'),
            ]
            before = [path.read_bytes() for path in paths]
            with library_snapshot(self.source) as copied:
                self.assertEqual(
                    copied.execute('SELECT COUNT(*) FROM profiles').fetchone()[0], 2
                )
            self.assertEqual(before, [path.read_bytes() for path in paths])

    def test_unknown_metric_rolls_back_and_source_destination_must_differ(self):
        with sqlite3.connect(self.source) as db:
            db.execute("INSERT INTO samples VALUES(1,'bad',?,1,0)", (BASE,))
        with self.assertRaisesRegex(ValueError, 'Unknown desktop metric'):
            import_library(self.source, self.repo)
        self.assertEqual(self.repo.import_state()['cursor'], 0)
        with closing(connect(self.repo.path)) as db:
            self.assertEqual(
                db.execute('SELECT COUNT(*) FROM desktop_archive').fetchone()[0], 0
            )
        with self.assertRaisesRegex(ValueError, 'different files'):
            import_library(self.source, TemperatureRepository(self.source))


if __name__ == '__main__':
    unittest.main()
