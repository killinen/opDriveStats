from datetime import datetime, timezone
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request

from server.app import app, templates
from server.trends import build_trends
from server.web_auth import AuthConfig, install_auth


class TrendsTests(unittest.TestCase):
    now = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)

    def row(self, date, km=100, engaged=80, device='one', branch='main', **bucket_fields):
        return {
            'device_id': device, 'git_branch': branch, 'drive': date,
            'recorded_at': date, 'speed_buckets': {
                'city': {'distance_km_raw': km, 'engaged_distance_km_raw': engaged,
                         'time_ns': 3600e9, 'engaged_time_ns': 1800e9,
                         **bucket_fields},
            },
        }

    def trends(self, rows, **options):
        return build_trends(rows, now=self.now, **options)

    def city(self, trends):
        return trends['series'][0]

    def test_ratios_use_raw_totals_rather_than_mean_of_percentages(self):
        rows = [self.row('2026-10-17T10:00:00Z', 10, 10),
                self.row('2026-10-18T10:00:00Z', 90, 0)]
        latest = self.city(self.trends(rows))['latest']
        self.assertEqual(latest['value'], 10)
        self.assertEqual(latest['distance_km'], 100)
        self.assertEqual(latest['drive_count'], 2)

    def test_exact_window_boundaries_and_nonoverlapping_comparison(self):
        rows = [self.row('2026-09-20T23:59:59Z', 100, 20),
                self.row('2026-09-21T00:00:00Z', 100, 60),
                self.row('2026-10-18T11:59:59Z', 100, 80),
                self.row('2026-10-18T13:00:00Z', 100, 100)]
        city = self.city(self.trends(rows))
        self.assertEqual(city['latest']['value'], 70)
        self.assertEqual(city['latest']['window_start'], '2026-09-21')
        self.assertEqual(city['previous']['value'], 20)
        self.assertEqual(city['delta'], 50)
        self.assertEqual(city['latest']['weekly_distance_km'], 100)
        self.assertTrue(city['latest']['partial_week'])

    def test_empty_windows_are_gaps_and_weekly_volume_is_not_rolling_volume(self):
        trends = self.trends([self.row('2026-09-21T10:00:00Z')], history_weeks=13)
        points = self.city(trends)['points']
        self.assertEqual(len(points), 13)
        self.assertTrue(all(point['value'] is None for point in points[:8]))
        self.assertEqual(points[-1]['value'], 80)
        self.assertEqual(points[-1]['distance_km'], 100)
        self.assertEqual(points[-1]['weekly_distance_km'], 0)
        self.assertEqual(trends['series'][1]['latest']['value'], None)

    def test_estimates_and_unknown_dates_are_counted_and_excludable(self):
        estimated = self.row('2023-11-22T10:00:00Z')
        estimated.update(git_commit_date='2026-10-01', uploaded_at='2026-10-17T10:00:00Z')
        unknown = self.row('route-without-date')
        rows = [self.row('2026-10-16T10:00:00Z'), estimated, unknown]
        trends = self.trends(rows)
        self.assertEqual(trends['coverage'], {
            'total_drives': 3, 'dated_drives': 2, 'recording_date_drives': 1,
            'estimated_drives': 1, 'undated_drives': 1,
        })
        self.assertEqual(self.city(trends)['latest']['estimated_drive_count'], 1)
        excluded = self.trends(rows, include_estimates=False)
        self.assertEqual(self.city(excluded)['latest']['drive_count'], 1)
        self.assertEqual(self.city(excluded)['latest']['estimated_drive_count'], 0)
        self.assertEqual(excluded['coverage']['estimated_drives'], 1)

    def test_missing_metric_counts_do_not_become_zero_events(self):
        rows = [self.row('2026-10-17T10:00:00Z', disengagement_count=2),
                self.row('2026-10-18T10:00:00Z', km=900, disengagement_count=None)]
        latest = self.city(self.trends(rows, metric='disengagements'))['latest']
        self.assertEqual(latest['value'], 2)
        self.assertEqual(latest['distance_km'], 100)
        self.assertEqual(latest['drive_count'], 1)
        rows[0]['speed_buckets']['city']['disengagement_count'] = 0
        self.assertEqual(self.city(self.trends(rows, metric='disengagements'))['latest']['value'], 0)

    def test_time_engagement_uses_total_time_including_stops(self):
        row = self.row('2026-10-17T10:00:00Z', km=0, engaged=0, drive_time_ns=600e9)
        latest = self.city(self.trends([row], metric='time'))['latest']
        self.assertEqual(latest['value'], 50)
        self.assertEqual(latest['hours'], 1)
        self.assertTrue(latest['low_distance'])

    def test_device_branch_filters_and_windows(self):
        rows = [self.row('2026-09-01T10:00:00Z'),
                self.row('2026-10-17T10:00:00Z', device='other'),
                self.row('2026-10-17T10:00:00Z', branch='other')]
        trends = self.trends(rows, device_ids=['one'], branches=['main'], window_weeks=8)
        self.assertEqual(trends['coverage']['total_drives'], 1)
        self.assertEqual(self.city(trends)['latest']['value'], 80)
        self.assertIsNone(self.city(self.trends(rows[:1]))['latest']['value'])

    def test_default_selects_only_device_with_newest_usable_date(self):
        rows = [self.row('2026-10-18T10:00:00Z', device='newest', engaged=90),
                self.row('2026-10-16T10:00:00Z', device='older', engaged=10),
                self.row('2026-10-19T00:00:00Z', device='future'),
                self.row('undated', device='unknown')]
        trends = self.trends(rows)
        self.assertEqual(trends['selected_devices'], ['newest'])
        self.assertEqual(trends['coverage']['total_drives'], 1)
        self.assertEqual(self.city(trends)['latest']['value'], 90)
        self.assertEqual({series['device_id'] for series in trends['series']}, {'newest'})
        explicit = self.trends(rows, device_ids=['older'])
        self.assertEqual(explicit['selected_devices'], ['older'])
        self.assertEqual(self.city(explicit)['latest']['value'], 10)

    def test_default_device_respects_branches_and_date_policy(self):
        estimated = self.row('undated', device='estimate')
        estimated['uploaded_at'] = '2026-10-18T10:00:00Z'
        rows = [self.row('2026-10-17T10:00:00Z', device='recorded'),
                self.row('2026-10-18T11:00:00Z', device='other-branch', branch='other'),
                estimated]
        self.assertEqual(self.trends(rows)['selected_devices'], ['other-branch'])
        self.assertEqual(self.trends(rows, branches=['main'])['selected_devices'], ['estimate'])
        self.assertEqual(self.trends(rows, branches=['main'], include_estimates=False)['selected_devices'], ['recorded'])
        undated = [self.row('undated', device='z'), self.row('undated', device='a')]
        self.assertEqual(self.trends(undated)['selected_devices'], ['a'])

    def test_multiple_devices_have_independent_series_and_optional_combined_totals(self):
        rows = [self.row('2026-10-10T10:00:00Z', km=10, engaged=10),
                self.row('2026-10-18T10:00:00Z', km=90, engaged=0, device='two')]
        trends = self.trends(rows, device_ids=['one', 'two', 'one'], history_weeks=0)
        self.assertEqual(trends['device_view'], 'individual')
        self.assertEqual(trends['selected_devices'], ['one', 'two'])
        self.assertEqual(len(trends['series']), 6)
        cities = {series['device_id']: series for series in trends['series'] if series['key'] == 'city'}
        self.assertEqual(cities['one']['latest']['value'], 100)
        self.assertEqual(cities['two']['latest']['value'], 0)
        self.assertEqual(cities['one']['latest']['distance_km'], 10)
        self.assertEqual(cities['two']['latest']['distance_km'], 90)
        self.assertNotEqual(cities['one']['color'], cities['two']['color'])
        self.assertEqual({series['color'] for series in trends['series'] if series['device_id'] == 'one'}, {cities['one']['color']})
        filtered = self.trends(rows, device_ids=['two'])
        self.assertEqual(self.city(filtered)['color'], cities['two']['color'])
        self.assertEqual([p['date'] for p in cities['one']['points']],
                         [p['date'] for p in cities['two']['points']])
        combined = self.trends(rows, device_ids=['one', 'two'], device_view='combined')
        self.assertEqual(len(combined['series']), 3)
        self.assertEqual(self.city(combined)['latest']['value'], 10)
        self.assertEqual(self.city(combined)['latest']['drive_count'], 2)
        self.assertIsNone(self.city(combined)['device_id'])
        invalid = self.trends(rows, device_ids=['missing'], device_view='invalid')
        self.assertEqual(invalid['device_view'], 'individual')
        self.assertEqual(invalid['selected_devices'], ['two'])

    def test_individual_template_has_device_labels_and_unique_chart_ids(self):
        rows = [self.row('2026-10-17', device='one / city'),
                self.row('2026-10-18', device='</script><script>alert(1)</script>')]
        trends = self.trends(rows, device_ids=[row['device_id'] for row in rows])
        request = Request({'type': 'http', 'method': 'GET', 'path': '/trends', 'headers': []})
        html = templates.env.get_template('trends.html').render(request=request, trends=trends)
        self.assertEqual(html.count('class="chart"'), 3)
        self.assertEqual(html.count('class="device-summary"'), 6)
        for bucket in ('city', 'road', 'highway'):
            section = re.search(rf'<section class="bucket {bucket}".*?</section>', html, re.S).group()
            self.assertEqual(section.count('class="device-summary"'), 2)
            self.assertIn('one / city', section)
            self.assertIn('&lt;/script&gt;', section)
            self.assertIn(f'id="chart-{bucket}"', section)
            self.assertIn('<th>Device</th>', section)
        self.assertIn('one / city', html)
        self.assertNotIn('</script><script>alert(1)</script>', html)
        ids = re.findall(r'\bid="([^"]+)"', html)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertIn('value="individual" selected', html)

    def test_all_history_retains_true_calendar_spacing(self):
        trends = self.trends([self.row('2025-10-12T10:00:00Z')], history_weeks=0)
        points = self.city(trends)['points']
        self.assertGreater(len(points), 52)
        for a, b in zip(points, points[1:]):
            self.assertEqual((datetime.fromisoformat(b['date']) - datetime.fromisoformat(a['date'])).days, 7)

    def test_tolerated_future_clock_does_not_break_all_history_or_enter_current_window(self):
        trends = self.trends([self.row('2026-10-19T00:00:00Z')], history_weeks=0)
        self.assertEqual(len(self.city(trends)['points']), 1)
        self.assertIsNone(self.city(trends)['latest']['value'])

    def test_invalid_values_do_not_produce_infinity_or_crash(self):
        rows = [self.row('2026-10-17', engaged=float('nan')),
                self.row('2026-10-17', km='invalid'), self.row('2026-10-17', km=0)]
        trends = self.trends(rows, metric='unknown', window_weeks=-100, history_weeks=-1)
        self.assertIsNone(self.city(trends)['latest']['value'])
        self.assertEqual(trends['window_weeks'], 4)
        self.assertEqual(trends['history_weeks'], 26)

    def test_empty_state_and_safe_template_json(self):
        trends = self.trends([], history_weeks=0)
        self.assertEqual(len(self.city(trends)['points']), 1)
        request = Request({'type': 'http', 'method': 'GET', 'path': '/trends', 'headers': []})
        html = templates.env.get_template('trends.html').render(request=request, trends=trends)
        self.assertIn('No dated drives match', html)
        self.assertEqual(html.count('class="chart"'), 3)
        row = self.row('2026-10-17', branch='</script><script>alert(1)</script>')
        html = templates.env.get_template('trends.html').render(request=request, trends=self.trends([row]))
        self.assertNotIn('</script><script>alert(1)</script>', html)

    def test_routes_match_and_are_private(self):
        # Use the real route handlers with an isolated access database.
        from fastapi import FastAPI
        test_app = FastAPI()
        for route in app.routes:
            if route.path in ('/trends', '/api/trends'):
                test_app.router.routes.append(route)
        with tempfile.TemporaryDirectory() as directory:
            config = AuthConfig(False, '', '', '', '', '', Path(directory) / 'auth.sqlite3')
            install_auth(test_app, templates, config)
            rows = [self.row('2026-10-16', device='one'), self.row('2026-10-17', device='two')]
            with TestClient(test_app) as client, patch('server.app.repository.all_entries', return_value=rows):
                response = client.get('/api/trends?window_weeks=8&metric=time&include_estimates=false')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['window_weeks'], 8)
                self.assertFalse(response.json()['include_estimates'])
                self.assertEqual(response.json()['selected_devices'], ['two'])
                self.assertEqual(client.get('/trends').status_code, 200)
                query = '?device=one&device=two&device_view=individual'
                self.assertEqual(len(client.get('/api/trends' + query).json()['series']), 6)
                individual = client.get('/trends' + query).text
                self.assertEqual(individual.count('class="chart"'), 3)
                self.assertEqual(individual.count('class="device-summary"'), 6)
                query = '?device=one&device=two&device_view=combined'
                combined = client.get('/api/trends' + query).json()
                self.assertEqual(combined['device_view'], 'combined')
                self.assertEqual(self.city(combined)['latest']['drive_count'], 2)
                html = client.get('/trends' + query).text
                self.assertEqual(html.count('class="chart"'), 3)
                self.assertEqual(html.count('class="device-summary"'), 3)
                self.assertIn('value="combined" selected', html)
            private = FastAPI()
            install_auth(private, templates, AuthConfig(True, '', '', '', '', '', Path(directory) / 'private.sqlite3'))
            with TestClient(private, follow_redirects=False) as client:
                self.assertEqual(client.get('/trends').status_code, 303)
                self.assertEqual(client.get('/api/trends').status_code, 401)


if __name__ == '__main__':
    unittest.main()
