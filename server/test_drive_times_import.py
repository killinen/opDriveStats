from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from server.drive_dates import resolve_drive_date
from server.drive_stats_sync import DriveStatsSyncConfig, DriveStatsSynchronizer
from server.drive_times_import import apply_drive_times, main

NOW = datetime(2026, 11, 6, 12, tzinfo=timezone.utc)
COMMIT = "'1789000000 2026-09-10 00:26:40 +0000'"


def row(drive, **fields):
    base = {'device_id': 'dev', 'drive': drive, 'recorded_at': '2023-11-21T21:11:06+00:00',
            'git_commit_date': COMMIT}
    return {**base, **fields}


def times(*items):
    return [{'device_id': 'dev', 'route': route, 'value': value, 'source': 'gps', 'estimated': False}
            for route, value in items]


class TrustedStartTests(unittest.TestCase):
    def test_trusted_start_dates_undated_and_replaces_estimate(self):
        entries = [row('00000001--a'), row('00000002--b', uploaded_at='2026-10-20T10:00:00Z')]
        self.assertIsNone(resolve_drive_date(entries[0], now=NOW).timestamp)
        self.assertTrue(resolve_drive_date(entries[1], now=NOW).is_estimated)
        counts = apply_drive_times(entries, times(
            ('00000001--a', '2026-10-01T08:00:00+00:00'),
            ('00000002--b', '2026-10-02T08:00:00+00:00'),
            ('00000003--c', '2026-10-03T08:00:00+00:00')), now=NOW)
        self.assertEqual(counts['dated_previously_undated'], 1)
        self.assertEqual(counts['replaced_upload_estimate'], 1)
        self.assertEqual(counts['not_in_database'], 1)
        for entry, day in zip(entries, (1, 2)):
            resolved = resolve_drive_date(entry, now=NOW)
            self.assertEqual(resolved.source, 'trusted_start')
            self.assertFalse(resolved.is_estimated)
            self.assertEqual(resolved.timestamp.day, day)
        self.assertEqual(entries[0]['trusted_start']['source'], 'opnas_gps')

    def test_implausible_and_estimated_starts_are_rejected(self):
        entries = [row('00000001--a', uploaded_at='2026-10-20T10:00:00Z')]
        counts = apply_drive_times(entries, times(
            ('00000001--a', '2023-11-21T21:11:06+00:00')), now=NOW)  # before its commit
        self.assertEqual(counts['rejected_implausible'], 1)
        counts = apply_drive_times(entries, [{**times(('00000001--a', '2026-10-01T08:00:00Z'))[0], 'estimated': True}], now=NOW)
        self.assertEqual(counts['skipped_estimated'], 1)
        self.assertNotIn('trusted_start', entries[0])
        # A start after the server received the drive is impossible too.
        counts = apply_drive_times(entries, times(('00000001--a', '2026-10-25T08:00:00Z')), now=NOW)
        self.assertEqual(counts['rejected_implausible'], 1)

    def test_reimport_is_unchanged_and_device_filter_applies(self):
        entries = [row('00000001--a'), {**row('00000001--a'), 'device_id': 'other'}]
        data = times(('00000001--a', '2026-10-01T08:00:00Z'))
        data.append({**data[0], 'device_id': 'other'})
        apply_drive_times(entries, data, devices={'dev'}, now=NOW)
        self.assertIn('trusted_start', entries[0])
        self.assertNotIn('trusted_start', entries[1])
        self.assertEqual(apply_drive_times(entries, data, devices={'dev'}, now=NOW)['unchanged'], 1)

    def test_cli_writes_db_and_dry_run_does_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            db, src, lock = Path(tmp, 'db.json'), Path(tmp, 't.json'), Path(tmp, 'lock')
            db.write_text(json.dumps([row('00000001--a')]))
            src.write_text(json.dumps(times(('00000001--a', '2026-10-01T08:00:00Z'))))
            main([str(src), '--db', str(db), '--lock', str(lock), '--dry-run'])
            self.assertNotIn('trusted_start', json.loads(db.read_text())[0])
            main([str(src), '--db', str(db), '--lock', str(lock)])
            self.assertIn('trusted_start', json.loads(db.read_text())[0])

    def test_sync_reupload_keeps_trusted_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = DriveStatsSyncConfig(source_url='http://x', token='t', db_path=Path(tmp, 'db.json'),
                                          cursor_path=Path(tmp, 'c.json'), lock_path=Path(tmp, 'l'))
            existing = [{**row('00000001--a'), 'trusted_start': {'value': '2026-10-01T08:00:00+00:00', 'source': 'opnas_gps'}}]
            page = {'items': [{'cursor': 1, 'received_at': '2026-11-01T00:00:00Z', 'stats': row('00000001--a', odo_distance=5)}]}
            merged, _, imported = DriveStatsSynchronizer(config)._merge_page(existing, page, 0)
            self.assertEqual(imported, 1)
            self.assertEqual(merged[0]['odo_distance'], 5)
            self.assertEqual(merged[0]['trusted_start']['source'], 'opnas_gps')


if __name__ == '__main__':
    unittest.main()
