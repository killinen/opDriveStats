import json
from pathlib import Path
import tempfile
import unittest

from server.data import EngagementRepository


class ComparisonSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary_directory.name) / 'engagement_db.json'
        self.db_path.write_text(json.dumps([{
            'device_id': 'device-1',
            'drive': '2026-09-17--10-00-00',
            'git_branch': 'main',
            'odo_distance': 100.0,
            'engaged_distance': 80.0,
            'total_time': 60_000_000_000,
            'drive_time_active': 45_000_000_000,
            'disengagement_count': 6,
            'steer_intervention_count': 3,
            'speed_buckets': {
                'city': {
                    'time_ns': 10_000_000_000,
                    'engaged_time_ns': 8_000_000_000,
                    'distance_km_raw': 40.0,
                    'engaged_distance_km_raw': 32.0,
                    'disengagement_count': 1,
                    'steer_intervention_count': 1,
                },
                'road': {
                    'time_ns': 20_000_000_000,
                    'engaged_time_ns': 10_000_000_000,
                    'distance_km_raw': 30.0,
                    'engaged_distance_km_raw': 15.0,
                    'disengagement_count': 2,
                    'steer_intervention_count': 1,
                },
                'highway': {
                    'time_ns': 30_000_000_000,
                    'engaged_time_ns': 27_000_000_000,
                    'distance_km_raw': 30.0,
                    'engaged_distance_km_raw': 27.0,
                    'disengagement_count': 3,
                    'steer_intervention_count': 1,
                },
            },
        }]), encoding='utf-8')
        self.repository = EngagementRepository(self.db_path)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_single_device_comparison_includes_all_speed_bucket_rows(self) -> None:
        comparison = self.repository.comparison_summary(
            device_ids=['device-1'],
            speed_bucket='city',
        )

        self.assertEqual(comparison['rows'][0]['distance_km'], 40.0)
        self.assertEqual(
            [row['speed_bucket'] for row in comparison['speed_bucket_rows']],
            ['city', 'road', 'highway'],
        )
        self.assertEqual(
            [row['distance_engagement_pct'] for row in comparison['speed_bucket_rows']],
            [80.0, 50.0, 90.0],
        )


if __name__ == '__main__':
    unittest.main()
