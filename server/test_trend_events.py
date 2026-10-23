import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from itsdangerous import TimestampSigner

from server.app import app, templates
from server.trend_events import TrendEventStore, visible_events
from server.trends import build_trends
from server.web_auth import AuthConfig, install_auth


class TrendEventStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'events.sqlite3'
        self.store = TrendEventStore(self.path)

    def test_persistent_create_edit_delete_and_missing_ids(self):
        self.assertEqual(self.store.listing(), [])
        event = self.store.save(week='2026-W43', title=' Firmware update ', notes=' v2 ')
        self.assertEqual(event['week_start'], '2026-10-19')
        self.assertEqual(event['title'], 'Firmware update')
        reopened = TrendEventStore(self.path)
        self.assertEqual(reopened.listing(), [event])
        changed = reopened.save(event_id=event['id'], week='2026-W44', title='Hardware change', device_id='one')
        self.assertEqual(self.store.listing(), [changed])
        self.assertTrue(self.store.delete(event['id']))
        self.assertFalse(self.store.delete(event['id']))
        with self.assertRaises(KeyError):
            self.store.save(event_id=event['id'], week='2026-W43', title='Missing')

    def test_week_validation_and_iso_year_boundaries(self):
        for week in ('2026-W00', '2026-W54', '2025-W53', '0000-W01', '2026-43', 'invalid'):
            with self.subTest(week=week), self.assertRaises(ValueError):
                self.store.save(week=week, title='Update')
        event = self.store.save(week='2020-W53', title='Year boundary')
        self.assertEqual(event['week_start'], '2020-12-28')
        for title, notes in (('', ''), (' ' * 10, ''), ('x' * 121, ''), ('Update', 'x' * 1001)):
            with self.assertRaises(ValueError):
                self.store.save(week='2026-W43', title=title, notes=notes)

    def test_visibility_scope_first_week_partial_week_and_same_week_groups(self):
        trends = build_trends([], now=datetime(2026, 10, 23, tzinfo=timezone.utc), history_weeks=13)
        trends['selected_devices'] = ['one']
        first_date = datetime.fromisoformat(trends['first_date']).date()
        year, week, _ = first_date.isocalendar()
        self.store.save(week=f'{year}-W{week:02}', title='First displayed week')
        self.store.save(week='2026-W43', title='Global')
        self.store.save(week='2026-W43', title='Device change', device_id='one')
        self.store.save(week='2026-W43', title='Other device', device_id='two')
        self.store.save(week='2026-W44', title='Future')
        self.store.save(week='2025-W01', title='Old')
        events = visible_events(self.store.listing(), trends)
        self.assertEqual([event['title'] for event in events], ['First displayed week', 'Global', 'Device change'])
        self.assertEqual(events[0]['chart_date'], trends['first_date'])
        self.assertEqual(events[1]['chart_date'], '2026-10-23')
        self.assertEqual([event['marker'] for event in events], [1, 2, 2])
        combined = {**trends, 'device_view': 'combined', 'selected_devices': ['one', 'two']}
        self.assertIn('Other device', [event['title'] for event in visible_events(self.store.listing(), combined)])


class TrendEventRouteTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = TrendEventStore(Path(self.directory.name) / 'events.sqlite3')
        self.config = AuthConfig(True, 'https://stats.example.com', 'owner@example.com',
                                 's' * 48, 'client', 'secret', Path(self.directory.name) / 'auth.sqlite3')
        self.app = FastAPI()
        for route in app.routes:
            if route.path in ('/trends', '/api/trends') or route.path.startswith('/api/trend-events'):
                self.app.router.routes.append(route)
        install_auth(self.app, templates, self.config)
        self.client = TestClient(self.app, base_url=self.config.public_url, follow_redirects=False)
        self.addCleanup(self.client.close)
        self.store_patch = patch('server.app.event_store', self.store)
        self.store_patch.start()
        self.addCleanup(self.store_patch.stop)
        self.rows_patch = patch('server.app.repository.all_entries', return_value=[])
        self.rows_patch.start()
        self.addCleanup(self.rows_patch.stop)
        self.clock_patch = patch('server.app.build_trends', side_effect=lambda *args, **kwargs: build_trends(
            *args, now=datetime(2026, 10, 23, 12, tzinfo=timezone.utc), **kwargs))
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    def login(self, role='owner', **fields):
        user = {'role': role, 'email': self.config.owner_email, 'sub': 'user', 'expires_at': time.time() + 1000, **fields}
        session = base64.b64encode(json.dumps({'user': user, 'csrf': 'test-csrf'}).encode())
        self.client.cookies.clear()
        self.client.cookies.set('engagement_session', TimestampSigner(self.config.secret).sign(session).decode())

    def form(self, **values):
        return {'csrf': 'test-csrf', 'week': '2026-W43', 'title': 'Firmware update', **values}

    def test_owner_crud_and_events_in_html_and_api(self):
        self.login()
        response = self.client.post('/api/trend-events', data=self.form())
        self.assertEqual(response.status_code, 201)
        event_id = response.json()['id']
        events = self.client.get('/api/trends').json()['events']
        self.assertEqual(events[0]['title'], 'Firmware update')
        html = self.client.get('/trends?metric=time').text
        self.assertIn('id="event-form"', html)
        self.assertIn('Firmware update', html)
        response = self.client.post(f'/api/trend-events/{event_id}', data=self.form(title='Hardware update'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(TrendEventStore(self.store.path).listing()[0]['title'], 'Hardware update')
        self.assertEqual(self.client.post(f'/api/trend-events/{event_id}/delete', data=self.form()).status_code, 200)
        self.assertEqual(self.client.get('/api/trends').json()['events'], [])
        self.assertEqual(self.client.post(f'/api/trend-events/{event_id}/delete', data=self.form()).status_code, 404)
        self.assertEqual(self.client.post(f'/api/trend-events/{event_id}', data=self.form()).status_code, 404)

    def test_owner_validation_csrf_and_origin(self):
        self.login()
        for form, headers, status in (
            (self.form(csrf='bad'), {}, 403),
            (self.form(), {'Origin': 'https://other.example'}, 403),
            (self.form(week='2025-W53'), {}, 400),
            (self.form(title=''), {}, 400),
        ):
            self.assertEqual(self.client.post('/api/trend-events', data=form, headers=headers).status_code, status)
        self.assertEqual(self.store.listing(), [])

    def test_members_and_guests_can_view_but_cannot_mutate(self):
        event = self.store.save(week='2026-W43', title='Shared change')
        self.app.state.share_store.add_member('member@example.com')
        share_id, _, expires_at = self.app.state.share_store.create()
        for role, fields in (('member', {'email': 'member@example.com'}),
                             ('guest', {'share_id': share_id, 'expires_at': expires_at})):
            with self.subTest(role=role):
                self.login(role, **fields)
                html = self.client.get('/trends').text
                self.assertIn('Shared change', html)
                self.assertNotIn('id="event-form"', html)
                self.assertEqual(self.client.get('/api/trends').status_code, 200)
                for path in ('/api/trend-events', f'/api/trend-events/{event["id"]}', f'/api/trend-events/{event["id"]}/delete'):
                    self.assertEqual(self.client.post(path, data=self.form()).status_code, 403)
        self.assertEqual(self.store.listing(), [event])

    def test_anonymous_requests_are_rejected(self):
        for path in ('/api/trend-events', '/api/trend-events/1', '/api/trend-events/1/delete'):
            self.assertEqual(self.client.post(path, data=self.form()).status_code, 401)

    def test_titles_and_notes_are_escaped_in_html_and_json_scripts(self):
        self.login()
        title = '</script><script>alert(1)</script>'
        self.client.post('/api/trend-events', data=self.form(title=title, notes='<img src=x onerror=alert(1)>'))
        html = self.client.get('/trends').text
        self.assertNotIn(title, html)
        self.assertIn('&lt;/script&gt;', html)
        self.assertNotIn('<img src=x onerror=alert(1)>', html)
        self.assertEqual(self.client.get('/api/trends').json()['events'][0]['title'], title)
        ids = re.findall(r'\bid="([^"]+)"', html)
        self.assertEqual(len(ids), len(set(ids)))


if __name__ == '__main__':
    unittest.main()
