import json
from html.parser import HTMLParser
from pathlib import Path
import tempfile
import unittest

from starlette.requests import Request

from server.app import templates
from server.data import EngagementRepository


SECOND_NS = 1_000_000_000


class TableRows(HTMLParser):
    def __init__(self, html: str) -> None:
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            self.row = []
        elif tag in ('td', 'th') and self.row is not None:
            self.cell = ''

    def handle_data(self, data):
        if self.cell is not None:
            self.cell += data

    def handle_endtag(self, tag):
        if tag in ('td', 'th') and self.cell is not None:
            self.row.append(' '.join(self.cell.split()))
            self.cell = None
        elif tag == 'tr' and self.row is not None:
            self.rows.append(self.row)
            self.row = None


class BucketAverageSpeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_path = Path(self.directory.name) / 'engagement_db.json'
        self.request = Request(
            {'type': 'http', 'method': 'GET', 'path': '/', 'headers': []}
        )

    def repository(self, drives):
        self.db_path.write_text(json.dumps(drives), encoding='utf-8')
        return EngagementRepository(self.db_path)

    def drive(self, buckets, device='one', branch='main'):
        return {
            'device_id': device,
            'drive': '2024-01-01--10-00-00',
            'git_branch': branch,
            'odo_distance': 200.0,
            'engaged_distance': 100.0,
            'total_time': 3600 * SECOND_NS,
            'drive_time_active': 1800 * SECOND_NS,
            'speed_buckets': buckets,
        }

    def bucket(self, distance, seconds, moving_seconds=None):
        return {
            'distance_km_raw': distance,
            'time_ns': seconds * SECOND_NS,
            'drive_time_ns': (seconds if moving_seconds is None else moving_seconds)
            * SECOND_NS,
            'engaged_distance_km_raw': distance / 2,
            'engaged_time_ns': seconds * SECOND_NS / 2,
        }

    def render(self, template_name, **context):
        return templates.env.get_template(template_name).render(
            request=self.request, **context
        )

    def test_unequal_drive_durations_are_weighted_separately_in_each_bucket(self):
        first = {'city': 3.1234, 'road': 15.1234, 'highway': 25.1234}
        second = {'city': 12.9876, 'road': 54.9876, 'highway': 90.9876}
        repository = self.repository(
            [
                self.drive(
                    {key: self.bucket(distance, 900) for key, distance in first.items()}
                ),
                self.drive(
                    {
                        key: self.bucket(distance, 2700)
                        for key, distance in second.items()
                    }
                ),
            ]
        )
        summary = repository.device_summary('one')
        comparison = repository.comparison_summary(device_ids=['one'])
        comparison_buckets = {
            row['speed_bucket']: row for row in comparison['speed_bucket_rows']
        }
        for key in first:
            with self.subTest(bucket=key):
                expected = first[key] + second[key]
                self.assertAlmostEqual(
                    summary['speed_bucket_summary'][key]['average_speed_kmh'], expected
                )
                self.assertAlmostEqual(
                    comparison_buckets[key]['average_speed_kmh'], expected
                )
                unweighted = (first[key] * 4 + second[key] * 4 / 3) / 2
                self.assertNotAlmostEqual(expected, unweighted)
                self.assertEqual(
                    summary['speed_bucket_summary'][key]['engagement_pct'], 50.0
                )
                self.assertEqual(
                    summary['speed_bucket_summary'][key]['dist_engagement_pct'], 50.0
                )

    def test_stopped_time_is_included_and_stopped_only_bucket_has_zero_speed(self):
        repository = self.repository(
            [
                self.drive({'city': self.bucket(5, 600)}),
                self.drive(
                    {
                        'city': self.bucket(0, 600, moving_seconds=0),
                        'road': self.bucket(0, 300, moving_seconds=0),
                    }
                ),
            ]
        )
        buckets = repository.device_summary('one')['speed_bucket_summary']
        self.assertEqual(buckets['city']['average_speed_kmh'], 15.0)
        self.assertEqual(buckets['road']['average_speed_kmh'], 0.0)
        comparison = repository.comparison_summary(speed_bucket='city')
        self.assertEqual(comparison['rows'][0]['average_speed_kmh'], 15.0)
        self.assertEqual(comparison['totals']['average_speed_kmh'], 15.0)
        stopped = repository.comparison_summary(speed_bucket='road')
        self.assertEqual(stopped['rows'][0]['average_speed_kmh'], 0.0)

    def test_empty_and_zero_time_buckets_are_unavailable(self):
        repository = self.repository(
            [
                self.drive(
                    {
                        'city': self.bucket(0, 0),
                        'road': self.bucket(1, 0),
                    }
                )
            ]
        )
        for summary in (
            repository.device_summary('one'),
            repository.grand_total_summary(),
        ):
            for bucket in summary['speed_bucket_summary'].values():
                self.assertIsNone(bucket['average_speed_kmh'])
        comparison = repository.comparison_summary(speed_bucket='road')
        self.assertIsNone(comparison['rows'][0]['average_speed_kmh'])
        self.assertIsNone(comparison['totals']['average_speed_kmh'])
        rows = TableRows(self.render('compare.html', comparison=comparison)).rows
        index = rows[0].index('Average speed')
        self.assertEqual([row[index] for row in rows[1:]], ['—', '—'])

    def test_missing_bucket_data_is_excluded_and_historical_data_works(self):
        repository = self.repository(
            [
                self.drive({'city': self.bucket(5, 600)}),
                self.drive(None),
                self.drive({}),
                self.drive({'city': {'time_ns': 3600 * SECOND_NS}}),
                self.drive({'city': {'distance_km_raw': 500.0}}),
            ]
        )
        for summary in (
            repository.device_summary('one'),
            repository.grand_total_summary(),
        ):
            self.assertEqual(
                summary['speed_bucket_summary']['city']['average_speed_kmh'], 30.0
            )
            self.assertIsNone(
                summary['speed_bucket_summary']['highway']['average_speed_kmh']
            )
        comparison = repository.comparison_summary(speed_bucket='city')
        self.assertEqual(comparison['rows'][0]['average_speed_kmh'], 30.0)
        self.assertEqual(comparison['totals']['average_speed_kmh'], 30.0)
        missing = self.repository([self.drive(None)])
        self.assertIsNone(
            missing.device_summary('one')['speed_bucket_summary']['city'][
                'average_speed_kmh'
            ]
        )
        self.assertEqual(missing.comparison_summary()['speed_bucket_rows'], [])

    def test_combined_devices_use_raw_totals_and_respect_device_and_branch_filters(
        self,
    ):
        repository = self.repository(
            [
                self.drive({'city': self.bucket(0.00049, 1)}, device='one'),
                self.drive({'city': self.bucket(0.00073, 3)}, device='two'),
                self.drive(
                    {'city': self.bucket(100, 3600)}, device='one', branch='other'
                ),
                self.drive({'city': self.bucket(100, 3600)}, device='three'),
            ]
        )
        comparison = repository.comparison_summary(
            device_ids=['one', 'two'],
            branch=['main'],
            speed_bucket='city',
        )
        self.assertAlmostEqual(comparison['totals']['average_speed_kmh'], 1.098)
        self.assertEqual(comparison['totals']['distance_km'], 0.0)
        self.assertEqual(comparison['totals']['time_hours'], 0.0)
        self.assertEqual(len(comparison['rows']), 2)
        one = repository.comparison_summary(
            device_ids=['one'], branch='main', speed_bucket='city'
        )
        self.assertAlmostEqual(one['totals']['average_speed_kmh'], 1.764)
        city = repository.grand_total_summary()['speed_bucket_summary']['city']
        self.assertAlmostEqual(city['average_speed_kmh'], 200.00122 * 3600 / 7204)

    def test_all_bucket_templates_show_one_decimal_units_and_unavailable_marker(self):
        drive = self.drive(
            {
                'city': self.bucket(16.111, 3600),
                'road': self.bucket(70.111, 3600),
                'highway': self.bucket(116.111, 3600),
            }
        )
        drive['drive'] = '2026-09-01--10-00-00'
        drive['steer_intervention_count'] = 0
        repository = self.repository([drive])
        summary = repository.grand_total_summary()
        summary['speed_bucket_summary']['road']['average_speed_kmh'] = 0.0
        summary['speed_bucket_summary']['highway']['average_speed_kmh'] = None
        for name in [
            'total_stats.html',
            *[f'total_stats_style{i}.html' for i in range(1, 5)],
        ]:
            with self.subTest(template=name):
                rows = TableRows(self.render(name, summary=summary)).rows
                header = next(row for row in rows if 'Average speed' in row)
                index = header.index('Average speed')
                bucket_rows = [
                    row
                    for row in rows
                    if row
                    and row[0]
                    in ('City (≤55 km/h)', 'Road (55-90 km/h)', 'Highway (≥90 km/h)')
                ]
                self.assertEqual(
                    [row[index] for row in bucket_rows], ['16.1 km/h', '0.0 km/h', '—']
                )
                self.assertTrue(all(len(row) == len(header) for row in bucket_rows))
        html = self.render(
            'stats_mod.html', devices=[{'summary': summary, 'drives': []}]
        )
        self.assertEqual(html.count('Average speed'), 3)
        self.assertIn('16.1 km/h', html)
        self.assertIn('0.0 km/h', html)
        for selected_bucket in ('all', 'city'):
            comparison = repository.comparison_summary(speed_bucket=selected_bucket)
            html = self.render('compare.html', comparison=comparison)
            self.assertIn('data-label="Average speed"', html)
            self.assertIn('16.1 km/h', html)
            self.assertIn('70.1 km/h', html)
            self.assertIn('116.1 km/h', html)
            rows = TableRows(html).rows
            if selected_bucket == 'city':
                index = rows[0].index('Average speed')
                self.assertEqual(
                    [row[index] for row in rows[1:]], ['16.1 km/h', '16.1 km/h']
                )
            self.assertTrue(all(len(row) == len(rows[0]) for row in rows))


if __name__ == '__main__':
    unittest.main()
