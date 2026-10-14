from __future__ import annotations

from copy import deepcopy
import gzip
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server import temperature_data, temperature_routes, temperature_sync
from server.temperature_data import SECOND, TemperatureRepository, unpacked
from server.temperature_sync import TemperatureSyncConfig, TemperatureSynchronizer
from server.web_auth import AuthConfig, install_auth
from server.app import templates

BASE = 100 * SECOND


def fixture(
    device='device_123',
    drive='route',
    seconds=61,
    segments=None,
    invalid=(),
    spike=None,
    source='a',
):
    segments = segments or [(0, 0, seconds)]
    samples, coverage = [], []
    for segment, first, last in segments:
        values = range(first, last)
        coverage.append(
            {
                'segment': segment,
                'first_ns': BASE + first * SECOND,
                'last_ns': BASE + last * SECOND,
                'can_events': (last - first) * 20,
                'hrr_frames': len(values),
                'hrr_valid': sum(i not in invalid for i in values),
                'hrr_invalid': sum(i in invalid for i in values),
                'hrr_repeated': 0,
                'hrr_malformed': 0,
            }
        )
        for second in values:
            for metric, fraction, value in [
                (
                    'hrr',
                    0.2,
                    None
                    if second in invalid
                    else (99 if second == spike else 50 + second % 7),
                ),
                ('ambient', 0.7, 20),
                ('speed', 0.8, 90),
                ('engine', 0.9, 45 if second < 10 else 85),
            ]:
                row = {
                    'time_s': second + fraction,
                    'time_ns': BASE + second * SECOND + round(fraction * SECOND),
                    'segment': segment,
                    'metric': metric,
                    'value': value,
                }
                if metric == 'hrr':
                    row['counter'] = second % 256
                elif metric == 'ambient':
                    row['raw'] = 69
                samples.append(row)
    samples.sort(key=lambda item: item['time_ns'])
    return {
        'schema_version': 2,
        'collector': 'hrr_temperature',
        'device_id': device,
        'drive': drive,
        'source_fingerprint': source * 64,
        'car_name': 'toyota',
        'car_fingerprint': 'LEXUS_LS600h',
        'recorded_at': '2026-10-14T12:00:00+00:00',
        'start_mono_time_ns': BASE,
        'segment_count': len(segments),
        'segments': [item[0] for item in segments],
        'batch_index': 0,
        'batch_count': 1,
        'generated_at': '2026-10-14T13:00:00+00:00',
        'version': 'test',
        'git_commit': 'abc',
        'git_branch': 'test',
        'data': {
            'samples': samples,
            'coverage': coverage,
            'signals': {},
            'malformed_frames': 0,
            'repeated_temperature_reports': 0,
        },
    }


def page(payload, cursor=1):
    return {
        'items': [
            {
                'cursor': cursor,
                'received_at': '2026-10-14T13:00:00+00:00',
                'telemetry': payload,
            }
        ],
        'next_cursor': cursor,
        'has_more': False,
    }


class TemperatureLibraryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = TemperatureRepository(Path(self.directory.name) / 'temperature.db')

    def test_time_weighting_percentiles_and_raw_peak_match_reference(self):
        self.repo.ingest(page(fixture(invalid=(20,), spike=22)))
        report = self.repo.report()
        expected = [99 if i == 22 else 50 + i % 7 for i in range(61) if i != 20]
        self.assertEqual(report['observed_seconds'], 62)
        self.assertEqual(report['available_seconds'], 60)
        self.assertEqual(report['summary']['seconds'], len(expected))
        self.assertAlmostEqual(report['summary']['median'], 53.0)
        self.assertAlmostEqual(report['summary']['p95'], 56.0)
        self.assertEqual(report['raw_max'], 99)
        self.assertAlmostEqual(report['summary']['delta_p95'], 36.0)
        with sqlite3.connect(self.repo.path) as db:
            grid = unpacked(db.execute('SELECT grid_gzip FROM routes').fetchone()[0])
        # A report at 22.2 seconds cannot contribute to the 22-second observation.
        self.assertEqual(grid[22]['hrr'], 50 + 21 % 7)
        self.assertEqual(grid[23]['hrr'], 99)
        self.assertIsNone(grid[21]['hrr'])
        self.assertIsNone(grid[0]['speed'])

    def test_engine_phase_filters_and_unknown_start(self):
        self.repo.ingest(page(fixture()))
        self.assertEqual(
            self.repo.report(engine='Below 50 °C')['summary']['seconds'], 10
        )
        self.assertEqual(self.repo.report(engine='80+ °C')['summary']['seconds'], 51)
        self.assertEqual(
            self.repo.report(phase='First 5 min')['summary']['seconds'], 61
        )
        other = fixture(drive='no_start', segments=[(2, 40, 61)], source='b')
        self.repo.ingest(page(other, 2))
        self.assertEqual(
            self.repo.report(phase='Unknown route start')['summary']['seconds'], 21
        )
        self.assertEqual(
            self.repo.report(phase='First 5 min')['summary']['seconds'], 61
        )

    def test_missing_intervals_invalid_reports_and_condition_changes_break_30s_windows(
        self,
    ):
        self.repo.ingest(page(fixture(segments=[(0, 0, 20), (2, 40, 60)])))
        report = self.repo.report()
        self.assertIsNone(report['summary']['sustained'])
        self.assertIsNone(report['cells'][0]['sustained'])
        self.repo.ingest(page(fixture(drive='complete', seconds=61), 2))
        self.assertIsNotNone(self.repo.report()['summary']['sustained'])
        original = fixture(drive='different_conditions', seconds=40)
        for sample in original['data']['samples']:
            if sample['metric'] == 'speed' and sample['time_s'] >= 20:
                sample['value'] = 10
        self.repo.ingest(page(original, 3))
        for cell in self.repo.report(device='device_123')['cells']:
            if cell['speed_band'] == 1:
                self.assertIsNone(cell['sustained'])

    def test_coverage_without_hrr_and_stale_companions(self):
        payload = fixture(seconds=61)
        payload['data']['samples'] = []
        payload['data']['coverage'][0].update(hrr_frames=0, hrr_valid=0)
        self.repo.ingest(page(payload))
        self.assertEqual(self.repo.report()['observed_seconds'], 62)
        self.assertEqual(self.repo.report()['available_seconds'], 0)
        self.assertEqual(len(self.repo.report()['segments']), 1)
        payload = fixture(drive='stale', seconds=80)
        payload['data']['samples'] = [
            s
            for s in payload['data']['samples']
            if s['metric'] == 'hrr' or s['time_s'] < 1
        ]
        self.repo.ingest(page(payload, 2))
        self.assertEqual(
            self.repo.report()['cells'][0]['seconds'], 2
        )  # Speed expires first.
        with sqlite3.connect(self.repo.path) as db:
            rows = unpacked(
                db.execute(
                    "SELECT grid_gzip FROM routes WHERE drive='stale'"
                ).fetchone()[0]
            )
        self.assertIsNone(rows[3]['speed'])
        self.assertIsNone(rows[6]['engine'])
        self.assertIsNone(rows[61]['ambient'])

    def test_incomplete_snapshots_are_hidden_and_logs_deleted_on_device_do_not_remove_history(
        self,
    ):
        original = fixture(segments=[(0, 0, 30), (1, 30, 61)])
        first, second = deepcopy(original), deepcopy(original)
        first['batch_count'] = second['batch_count'] = 2
        second['batch_index'] = 1
        first['data']['samples'] = original['data']['samples'][:100]
        second['data']['samples'] = original['data']['samples'][100:]
        second['data'].pop('coverage')
        self.repo.ingest(page(first))
        self.assertEqual(self.repo.report()['drives'], [])
        self.assertEqual(self.repo.report()['pending_snapshots'], 1)
        self.repo.ingest(page(second, 2))
        self.assertEqual(self.repo.report()['available_seconds'], 61)
        later = fixture(segments=[(1, 30, 61)], source='b')
        self.repo.ingest(page(later, 3))
        self.assertEqual(self.repo.report()['available_seconds'], 61)
        self.assertEqual(len(self.repo.report()['segments']), 2)
        self.assertEqual(
            self.repo.drive('device_123', 'route')['metadata']['segments'], [0, 1]
        )

    def test_bad_snapshot_rolls_back_data_and_cursor(self):
        original = fixture()
        self.repo.ingest(page(original))
        bad = fixture(drive='bad')
        bad['data']['samples'][0]['value'] = float('nan')
        with self.assertRaises(ValueError):
            self.repo.ingest(page(bad, 2))
        self.assertEqual(self.repo.state()['cursor'], 1)
        with sqlite3.connect(self.repo.path) as db:
            self.assertEqual(
                db.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1
            )

    def test_compressed_records_restore_every_original_sample_and_device_counts_are_distinct(
        self,
    ):
        original = fixture()
        self.repo.ingest(page(original))
        self.repo.ingest(page(fixture(device='second_device'), 2))
        with sqlite3.connect(self.repo.path) as db:
            blob = db.execute(
                'SELECT payload_gzip FROM batches WHERE cursor=1'
            ).fetchone()[0]
        self.assertTrue(blob.startswith(b'\x1f\x8b'))
        self.assertEqual(unpacked(blob), original)
        self.assertEqual(self.repo.report()['summary']['drives'], 2)
        self.assertEqual(len(self.repo.report(device='device_123')['drives']), 1)

    def test_same_timestamp_report_peak_is_preserved_and_revisions_publish_in_cursor_order(
        self,
    ):
        original = fixture()
        duplicate = deepcopy(original['data']['samples'][0])
        duplicate['value'] = 100
        duplicate['counter'] = 99
        original['data']['samples'].insert(0, duplicate)
        original['data']['coverage'][0]['hrr_frames'] += 1
        original['data']['coverage'][0]['hrr_valid'] += 1
        self.repo.ingest(page(original))
        self.assertEqual(self.repo.report()['raw_max'], 100)
        self.assertEqual(self.repo.report()['summary']['max'], 56)
        old, new = fixture(drive='revisions'), fixture(drive='revisions', source='b')
        old['git_branch'] = 'old-branch'
        new['git_branch'] = 'new-branch'
        self.repo.ingest(
            {'items': page(old, 2)['items'] + page(new, 3)['items'], 'next_cursor': 3}
        )
        self.assertEqual(
            self.repo.drive('device_123', 'revisions')['metadata']['git_branch'],
            'new-branch',
        )

    def test_legacy_v1_is_read_without_engine_or_exact_coverage(self):
        payload = fixture()
        payload['schema_version'] = 1
        payload['data'] = {
            'signals': {},
            'samples': [
                {
                    'time_s': float(i),
                    'segment': 0,
                    'hrr_temp_c': 50,
                    'hrr_temp_valid': True,
                    'temperature_counter': i,
                    'speed_kph': 90.0,
                    'speed_age_s': 0.0,
                    'ambient_temp_c': 20.0,
                    'ambient_raw': 69,
                    'ambient_age_s': 0.0,
                }
                for i in range(1, 41)
            ],
        }
        self.repo.ingest(page(payload))
        report = self.repo.report()
        self.assertEqual(report['profile'], 1)
        self.assertEqual(report['summary']['seconds'], 40)
        self.assertTrue(report['segments'][0]['estimated'])
        self.assertEqual(self.repo.report(engine='80+ °C')['summary']['seconds'], 0)


class TemperatureWebsiteTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = TemperatureRepository(Path(self.directory.name) / 'data.db')
        self.repo.ingest(page(fixture(spike=22)))
        replacement = patch.object(temperature_data, 'repository', self.repo)
        replacement.start()
        self.addCleanup(replacement.stop)
        app = FastAPI()
        app.include_router(temperature_routes.router)
        config = AuthConfig(
            False,
            'https://stats.example.com',
            'owner@example.com',
            's' * 48,
            'id',
            'secret',
            Path(self.directory.name) / 'auth.db',
        )
        install_auth(app, templates, config)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def test_page_report_drive_and_csv(self):
        response = self.client.get('/temperatures')
        self.assertEqual(response.status_code, 200)
        for text in [
            'Ambient × speed',
            'Condition ranges',
            'Segment coverage',
            'Drive detail',
            'Ambient provisional',
        ]:
            self.assertIn(text, response.text)
        report = self.client.get('/api/temperatures').json()
        self.assertEqual(report['summary']['drives'], 1)
        self.assertEqual(
            self.client.get('/api/temperatures?engine=invalid').status_code, 422
        )
        response = self.client.get(
            '/api/temperatures/drive', params={'device': 'device_123', 'drive': 'route'}
        )
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json()['samples'][0]['time_ns'], str)
        self.assertEqual(
            self.client.get('/api/temperatures/drive?device=x&drive=none').status_code,
            404,
        )
        response = self.client.get('/api/temperatures/conditions.csv')
        self.assertIn('ambient_confidence', response.text)
        self.assertIn('provisional', response.text)
        response = self.client.get(
            '/api/temperatures/samples.csv?device=device_123&drive=route'
        )
        self.assertIn('122200000000', response.text)
        self.assertIn('99', response.text)
        self.assertEqual(
            len(response.text.splitlines()), len(fixture()['data']['samples']) + 1
        )

    def test_temperature_routes_follow_existing_auth_gate(self):
        app = FastAPI()
        app.include_router(temperature_routes.router)
        config = AuthConfig(
            True,
            'https://stats.example.com',
            'owner@example.com',
            's' * 48,
            'id',
            'secret',
            Path(self.directory.name) / 'private.db',
        )
        install_auth(app, templates, config)
        with TestClient(app, follow_redirects=False) as client:
            self.assertEqual(client.get('/temperatures').status_code, 303)
            for path in [
                '/api/temperatures',
                '/api/temperatures/drive?device=device_123&drive=route',
                '/api/temperatures/conditions.csv',
                '/api/temperatures/samples.csv?device=device_123&drive=route',
            ]:
                self.assertEqual(client.get(path).status_code, 401)


class TemperatureSyncTests(unittest.TestCase):
    def test_plain_and_gzip_response_cursor_restart_and_automatic_config(self):
        with tempfile.TemporaryDirectory() as directory:
            config = TemperatureSyncConfig(
                'https://example.com/api/drive-telemetry/export',
                'scoped-token',
                Path(directory) / 'data.db',
                Path(directory) / 'lock',
            )
            sync = TemperatureSynchronizer(config)
            body = page(fixture())

            class Reply(io.BytesIO):
                def __init__(self, compressed):
                    raw = json.dumps(body).encode()
                    super().__init__(gzip.compress(raw) if compressed else raw)
                    self.headers = {
                        'Content-Encoding': 'gzip' if compressed else 'identity'
                    }

            for compressed in (False, True):
                with patch.object(
                    temperature_sync, 'urlopen', return_value=Reply(compressed)
                ) as fetch:
                    result = sync._fetch_page(0)
                self.assertEqual(result, body)
                self.assertEqual(
                    fetch.call_args.args[0].get_header('Accept-encoding'), 'gzip'
                )
            with patch.object(sync, '_fetch_page', return_value=body):
                self.assertEqual(sync.sync_once(), 1)
            restarted = TemperatureSynchronizer(config)
            with patch.object(
                restarted,
                '_fetch_page',
                return_value={'items': [], 'next_cursor': 1, 'has_more': False},
            ) as fetch:
                self.assertEqual(restarted.sync_once(), 0)
            fetch.assert_called_once_with(1)
            drive = temperature_sync.DriveStatsSyncConfig(
                'https://example.com/api/drive-stats/export',
                'same-token',
                Path(directory) / 'legacy.json',
                Path(directory) / 'cursor',
                Path(directory) / 'lock2',
            )
            with patch.dict('os.environ', {}, clear=True):
                derived = TemperatureSyncConfig.from_environment(drive)
            self.assertEqual(
                derived.source_url, 'https://example.com/api/drive-telemetry/export'
            )
            self.assertEqual(derived.token, 'same-token')
            with patch.dict('os.environ', {'DRIVE_TELEMETRY_SYNC': '0'}, clear=True):
                self.assertIsNone(TemperatureSyncConfig.from_environment(drive))

    def test_failed_page_does_not_advance_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            config = TemperatureSyncConfig(
                'https://example.com/export',
                'token',
                Path(directory) / 'data.db',
                Path(directory) / 'lock',
            )
            sync = TemperatureSynchronizer(config)
            with patch.object(
                sync, '_fetch_page', return_value={'items': [], 'has_more': True}
            ):
                with self.assertRaises(ValueError):
                    sync.sync_once()
            self.assertEqual(sync.repository.state()['cursor'], 0)
