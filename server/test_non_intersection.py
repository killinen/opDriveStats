import json
from pathlib import Path
import tempfile
import unittest

from starlette.requests import Request

from server.app import templates
from server.data import EngagementRepository


class NonIntersectionSummaryTests(unittest.TestCase):
    def test_mixed_v3_v4_drives_use_only_covered_distance(self) -> None:
        old = {
            'device_id': 'one', 'drive': '2026-09-18--10-00-00',
            'odo_distance': 900.0, 'drive_time': 3_600_000_000_000,
            'disengagement_count': 3,
            'speed_buckets': {'city': {'time_ns': 10_000_000_000, 'distance_km_raw': 900.0}},
        }
        new = {
            'device_id': 'one', 'drive': '2026-09-19--10-00-00',
            'odo_distance': 100.0, 'drive_time': 3_600_000_000_000,
            'disengagement_count': 2,
            'non_intersection_disengagement_count': 1,
            'raw_non_intersection_disengagement_count': 2,
            'blinker_related_disengagement_count': 1,
            'speed_buckets': {'city': {
                'time_ns': 10_000_000_000, 'distance_km_raw': 50.0,
                'drive_time_ns': 1_800_000_000_000,
                'non_intersection_disengagement_count': 0,
                'raw_non_intersection_disengagement_count': 1,
                'blinker_related_disengagement_count': 1,
            }},
        }
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'engagement_db.json'
            db_path.write_text(json.dumps([old, new]), encoding='utf-8')
            repository = EngagementRepository(db_path)

            device = repository.device_summary('one')
            self.assertEqual(device['non_intersection_disengagement_count'], 1)
            self.assertEqual(device['non_intersection_disengagements_per_100km'], 1.0)
            self.assertEqual(device['non_intersection_disengagements_per_drive_hour'], 1.0)
            self.assertEqual(device['non_intersection_covered_drives'], 1)
            self.assertEqual(device['speed_bucket_summary']['city']['non_intersection_disengagement_count'], 0)
            self.assertEqual(device['speed_bucket_summary']['city']['non_intersection_disengagements_per_100km'], 0.0)

            comparison = repository.comparison_summary(device_ids=['one'], speed_bucket='city')
            self.assertEqual(comparison['totals']['non_intersection_disengagement_count'], 0)
            self.assertEqual(comparison['totals']['non_intersection_distance_km'], 50.0)
            self.assertEqual(repository.grand_total_summary()['non_intersection_disengagements_per_100km'], 1.0)

    def test_legacy_only_data_marks_metric_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'engagement_db.json'
            db_path.write_text(json.dumps([{'device_id': 'one', 'drive': 'old', 'odo_distance': 100.0}]), encoding='utf-8')
            repository = EngagementRepository(db_path)
            self.assertIsNone(repository.device_summary('one')['non_intersection_disengagement_count'])
            self.assertIsNone(repository.comparison_summary()['totals']['non_intersection_disengagements_per_100km'])

    def test_templates_render_with_a_backend_missing_new_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'engagement_db.json'
            db_path.write_text(json.dumps([{
                'device_id': 'one', 'drive': '2026-09-18--10-00-00',
                'odo_distance': 100.0, 'engaged_distance': 80.0,
                'total_time': 3_600_000_000_000, 'drive_time_active': 2_800_000_000_000,
            }]), encoding='utf-8')
            repository = EngagementRepository(db_path)
            comparison = repository.comparison_summary()
            summary = repository.grand_total_summary()

            for row in [*comparison['rows'], comparison['totals'], summary, *summary['speed_bucket_summary'].values()]:
                for key in tuple(row):
                    if key.startswith(('non_intersection', 'raw_non_intersection', 'blinker_related')):
                        del row[key]

            request = Request({'type': 'http', 'method': 'GET', 'path': '/', 'headers': []})
            for template_name, context in [
                ('compare.html', {'comparison': comparison}),
                ('total_stats.html', {'summary': summary}),
            ]:
                html = templates.env.get_template(template_name).render(request=request, **context)
                self.assertIn('Non-intersection', html)


if __name__ == '__main__':
    unittest.main()
