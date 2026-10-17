import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.temperature_data import TemperatureRepository, connect
from server.test_temperatures import BASE, SECOND, page
from server.test_temperatures import fixture as hrr_fixture
from server.trqi_temperature import data, routes
from server.web_auth import AuthConfig, install_auth


def fixture(seconds=65, segments=None, invalid=(), source='a'):
    body = hrr_fixture(seconds=seconds, source=source)
    body.update(
        collector='trqi_temperature',
        car_name='i30',
        car_fingerprint='HYUNDAI I30 GD 2014',
    )
    segments = segments or [(0, 0, seconds)]
    body['segments'] = [row[0] for row in segments]
    body['segment_count'] = len(segments)
    samples, coverage = [], []
    for segment, first, last in segments:
        for second in range(first, last):
            samples.append(
                {
                    'metric': 'trqi',
                    'value': None if second in invalid else 86.0,
                    'time_ns': BASE + second * SECOND,
                    'time_s': second,
                    'segment': segment,
                    'crc_valid': second not in invalid,
                    'sensor_valid': True,
                    'status': 15,
                    'raw': 1234,
                    'counter': second % 256,
                    'loop_max_ms': 3,
                    'frame_hex': '5c03d2040f030100',
                }
            )
            if second < first + 2:
                samples.extend(
                    {
                        'metric': metric,
                        'value': value,
                        'time_ns': BASE + second * SECOND,
                        'time_s': second,
                        'segment': segment,
                    }
                    for metric, value in [
                        ('outside', 20),
                        ('speed', 90),
                        ('blower_on', 1),
                    ]
                )
        coverage.append(
            {
                'segment': segment,
                'first_ns': BASE + first * SECOND,
                'last_ns': BASE + (last - 1) * SECOND,
                'can_events': last - first,
                'frames': last - first,
                'valid': sum(second not in invalid for second in range(first, last)),
                'invalid': sum(second in invalid for second in range(first, last)),
                'crc_errors': sum(second in invalid for second in range(first, last)),
                'malformed': 0,
                'repeated': 0,
                'counter_discontinuities': 0,
            }
        )
    samples.sort(key=lambda row: (row['time_ns'], row['metric']))
    body['data'] = {
        'samples': samples,
        'coverage': coverage,
        'signals': {},
        'firmware_frames': [],
    }
    return body


class TrqiLibraryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'telemetry.sqlite'
        self.replica = TemperatureRepository(self.path)
        self.repo = data.Repository(self.path)

    def ingest(self, body, cursor=1):
        self.replica.ingest(page(body, cursor))

    def test_thresholds_constant_readings_and_companion_expiry(self):
        self.ingest(fixture())
        report = self.repo.report()
        summary = report['drives'][0]['summary']
        self.assertEqual(summary['seconds_ge_82'], 65)
        self.assertEqual(summary['seconds_ge_85'], 65)
        self.assertEqual(summary['sustained_30s'], 86)
        self.assertEqual(summary['initial'], 86)
        self.assertEqual(summary['raw_maximum'], 86)
        grid = self.repo.drive('device_123', 'route')['grid']
        self.assertIsNone(grid[4]['speed'])
        self.assertIsNone(grid[7]['blower_on'])
        self.assertIsNone(grid[62]['outside'])
        self.assertEqual(report['summary']['valid_seconds'], 65)
        self.assertTrue(
            any(
                cell['kind'] == 'outside' and cell['band'] is None
                for cell in report['cells']
            )
        )

    def test_invalid_crc_interrupts_fault_and_sustained_windows(self):
        self.ingest(fixture(seconds=45, invalid=(20,)))
        summary = self.repo.report()['drives'][0]['summary']
        self.assertEqual(summary['valid_seconds'], 44)
        self.assertEqual(summary['fault_seconds'], 44)
        self.assertIsNone(summary['sustained_30s'])
        grid = self.repo.drive('device_123', 'route')['grid']
        self.assertIsNone(grid[20]['trqi'])
        self.assertIsNone(grid[20]['high_temp_error'])

    def test_missing_start_and_segments_do_not_create_startup_or_bridge_gaps(self):
        self.ingest(fixture(segments=[(1, 60, 61), (3, 62, 65)]))
        drive = self.repo.drive('device_123', 'route')
        self.assertIsNone(drive['summary']['initial'])
        self.assertTrue(all(row['elapsed'] is None for row in drive['grid']))
        self.assertTrue(all(row['phase'] == 'Unknown start' for row in drive['grid']))
        self.assertEqual(drive['summary']['observed_seconds'], 4)

    def test_missing_segment_cannot_forward_fill_even_when_timestamp_is_close(self):
        body = fixture(segments=[(0, 0, 1), (2, 1, 3)])
        body['data']['samples'] = [
            row for row in body['data']['samples'] if row['segment'] == 0
        ]
        body['data']['coverage'][1].update(frames=0, valid=0)
        self.ingest(body)
        grid = self.repo.drive('device_123', 'route')['grid']
        self.assertIsNone(grid[1]['trqi'])
        self.assertIsNone(grid[1]['outside'])

    def test_partial_batches_then_source_revision_retains_deleted_segments(self):
        body = fixture(segments=[(0, 0, 3), (1, 60, 63)])
        first = deepcopy(body)
        first['batch_count'] = 2
        first['data']['samples'] = body['data']['samples'][:4]
        self.ingest(first)
        self.assertEqual(self.repo.report()['drives'], [])
        self.assertEqual(self.repo.report()['pending_snapshots'], 1)
        second = deepcopy(first)
        second['batch_index'] = 1
        second['data'].pop('coverage')
        second['data']['samples'] = body['data']['samples'][4:]
        self.ingest(second, 2)
        self.assertEqual(self.repo.report()['summary']['valid_seconds'], 6)
        self.ingest(fixture(segments=[(1, 60, 63)], source='b'), 3)
        drive = self.repo.drive('device_123', 'route')
        self.assertEqual(drive['summary']['valid_seconds'], 6)
        self.assertEqual(drive['summary']['initial'], 86)
        self.assertEqual(drive['metadata']['segments'], [0, 1])

    def test_hr_r_same_schema_and_route_cannot_overwrite_trqi(self):
        self.ingest(fixture())
        self.ingest(hrr_fixture(), 2)
        self.assertEqual(self.repo.report()['summary']['maximum'], 86)
        self.assertLess(self.replica.report()['summary']['max'], 86)

    def test_outside_phase_and_blower_filters_and_unknown_firmware(self):
        self.ingest(fixture())
        report = self.repo.report(hvac='On')
        self.assertEqual(report['summary']['valid_seconds'], 7)
        self.assertEqual(report['firmwares'], ['Unknown'])
        self.assertEqual(
            self.repo.report(phase='15–30 min')['summary']['valid_seconds'], 0
        )
        self.assertEqual(self.repo.report(firmware='nonexistent')['drives'], [])
        with self.assertRaises(ValueError):
            self.repo.report(phase='invalid')

    def test_publish_error_rolls_back_batch_and_cursor(self):
        body = fixture()
        body['data']['samples'][0]['time_ns'] = BASE - SECOND
        with self.assertRaises(ValueError):
            self.ingest(body)
        with connect(self.path) as db:
            self.assertEqual(
                db.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 0
            )
            self.assertEqual(
                db.execute(
                    "SELECT cursor FROM sync_state WHERE name='telemetry'"
                ).fetchone()[0],
                0,
            )


class TrqiWebsiteTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'data.sqlite'
        TemperatureRepository(self.path).ingest(page(fixture()))
        repository = patch.object(data, 'repository', data.Repository(self.path))
        repository.start()
        self.addCleanup(repository.stop)
        app = FastAPI()
        app.include_router(routes.router)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def test_dashboard_filters_plotting_and_lossless_exports(self):
        response = self.client.get('/trqi-temperatures')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Startup temperature × phase', response.text)
        self.assertIn('provisional', response.text)
        self.assertEqual(
            self.client.get('/api/trqi-temperatures').json()['summary'][
                'seconds_ge_85'
            ],
            65,
        )
        self.assertEqual(
            self.client.get('/api/trqi-temperatures?phase=invalid').status_code, 422
        )
        response = self.client.get(
            '/api/trqi-temperatures/drive',
            params={'device': 'device_123', 'drive': 'route'},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json()['grid'][0]['time_ns'], str)
        self.assertEqual(
            self.client.get(
                '/api/trqi-temperatures/drive?device=x&drive=x'
            ).status_code,
            404,
        )
        self.assertIn(
            'seconds_ge_85',
            self.client.get('/api/trqi-temperatures/conditions.csv').text,
        )
        csv = self.client.get(
            '/api/trqi-temperatures/samples.csv?device=device_123&drive=route'
        ).text
        self.assertIn('frame_hex', csv)
        self.assertIn(str(BASE), csv)

    def test_all_new_routes_follow_existing_auth_gate(self):
        from fastapi.templating import Jinja2Templates

        app = FastAPI()
        app.include_router(routes.router)
        config = AuthConfig(True, '', '', '', '', '', self.path.parent / 'auth.sqlite')
        with patch.object(AuthConfig, 'from_environment', return_value=config):
            install_auth(
                app, Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))
            )
        with TestClient(app, follow_redirects=False) as client:
            self.assertEqual(client.get('/trqi-temperatures').status_code, 303)
            for path in (
                '/api/trqi-temperatures',
                '/api/trqi-temperatures/drive?device=x&drive=x',
                '/api/trqi-temperatures/conditions.csv',
                '/api/trqi-temperatures/samples.csv?device=x&drive=x',
            ):
                self.assertEqual(client.get(path).status_code, 401)
