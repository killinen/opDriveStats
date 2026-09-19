import json
from pathlib import Path
import tempfile
import unittest

from starlette.requests import Request

from server.app import templates
from server.data import EngagementRepository


class SpeedBucketSnapshotTests(unittest.TestCase):
    def test_multiple_devices_are_grouped_and_ranked_within_each_speed_bucket(self) -> None:
        def drive(device_id: str, engaged_city: float, branch: str = 'main') -> dict:
            return {
                'device_id': device_id,
                'drive': '2026-09-19--10-00-00',
                'git_branch': branch,
                'odo_distance': 120.0,
                'engaged_distance': 90.0,
                'total_time': 90_000_000_000,
                'drive_time_active': 70_000_000_000,
                'speed_buckets': {
                    'city': {'time_ns': 30_000_000_000, 'engaged_time_ns': 20_000_000_000,
                             'distance_km_raw': 40.0, 'engaged_distance_km_raw': engaged_city,
                             'disengagement_count': 1},
                    'road': {'time_ns': 30_000_000_000, 'engaged_time_ns': 25_000_000_000,
                             'distance_km_raw': 40.0, 'engaged_distance_km_raw': 30.0,
                             'disengagement_count': 2},
                    'highway': {'time_ns': 30_000_000_000, 'engaged_time_ns': 25_000_000_000,
                                'distance_km_raw': 40.0, 'engaged_distance_km_raw': 35.0,
                                'disengagement_count': 3},
                },
            }

        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'engagement_db.json'
            db_path.write_text(json.dumps([
                drive('device-a', 20.0),
                drive('device-b', 36.0),
                drive('device-c', 30.0, 'other'),
            ]), encoding='utf-8')
            repository = EngagementRepository(db_path)
            comparison = repository.comparison_summary(
                device_ids=['device-a', 'device-b', 'device-c'],
                branch='main',
                speed_bucket='city',
            )

            groups = comparison['speed_bucket_groups']
            self.assertEqual([group['key'] for group in groups], ['city', 'road', 'highway'])
            self.assertEqual([[row['device_id'] for row in group['rows']] for group in groups], [
                ['device-b', 'device-a'],
                ['device-a', 'device-b'],
                ['device-a', 'device-b'],
            ])
            self.assertEqual([group['distance_km'] for group in groups], [80.0, 80.0, 80.0])
            self.assertEqual(len(comparison['speed_bucket_rows']), 6)
            self.assertEqual(len(comparison['rows']), 2)

            request = Request({'type': 'http', 'method': 'GET', 'path': '/compare', 'headers': []})
            html = templates.env.get_template('compare.html').render(request=request, comparison=comparison)
            self.assertEqual(html.count('data-rate-scope aria-label'), 3)
            self.assertIn('data-snapshot="speed"', html)


if __name__ == '__main__':
    unittest.main()
