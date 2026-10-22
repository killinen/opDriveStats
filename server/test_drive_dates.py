from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from server.data import EngagementRepository
from server.drive_dates import parse_timestamp, resolve_drive_date


class DriveDateTests(unittest.TestCase):
    now = datetime(2026, 10, 18, 12, tzinfo=timezone.utc)

    def resolve(self, **fields):
        return resolve_drive_date(fields, now=self.now)

    def test_sane_recording_is_preferred_to_upload(self):
        result = self.resolve(
            recorded_at='2026-10-16T12:00:00+03:00',
            uploaded_at='2026-10-18T10:00:00Z',
        )
        self.assertEqual(result.timestamp.isoformat(), '2026-10-16T09:00:00+00:00')
        self.assertEqual(result.source, 'recorded_at')
        self.assertFalse(result.is_estimated)

    def test_recording_before_software_commit_uses_upload(self):
        for commit in (
            "'1792170385 2026-10-16 20:06:25 +0300'",
            '2026-10-16T17:06:25+00:00',
        ):
            with self.subTest(commit=commit):
                result = self.resolve(
                    recorded_at='2023-11-21T21:49:13+00:00',
                    git_commit_date=commit,
                    uploaded_at='2026-10-18T10:00:00Z',
                )
                self.assertEqual(result.source, 'uploaded_at')
                self.assertTrue(result.is_estimated)
                self.assertEqual(result.timestamp, parse_timestamp('2026-10-18T10:00:00Z'))

    def test_invalid_and_future_recordings_use_upload(self):
        for recorded_at in (None, 'not-a-date', 123, '1970-01-01', '2036-10-18'):
            with self.subTest(recorded_at=recorded_at):
                result = self.resolve(
                    recorded_at=recorded_at, drive='00000130--bba4e80888',
                    uploaded_at='2026-10-18T10:00:00Z',
                )
                self.assertEqual(result.source, 'uploaded_at')

    def test_recording_after_receipt_uses_upload(self):
        result = self.resolve(
            recorded_at='2026-10-17T10:00:00Z',
            uploaded_at='2026-09-01T10:00:00Z',
        )
        self.assertEqual(result.source, 'uploaded_at')

    def test_archived_drives_are_not_rejected_for_being_old(self):
        result = self.resolve(
            recorded_at='2023-11-21T21:49:13+00:00',
            uploaded_at='2026-10-18T10:00:00Z',
        )
        self.assertEqual(result.source, 'recorded_at')

    def test_legacy_drive_name_is_a_recording_date(self):
        result = self.resolve(recorded_at='invalid', drive='2025-04-24--17-36-58')
        self.assertEqual(result.source, 'drive')
        self.assertEqual(result.timestamp.isoformat(), '2025-04-24T17:36:58+00:00')

    def test_small_clock_skew_is_tolerated(self):
        result = self.resolve(
            recorded_at='2026-10-18T10:10:00Z',
            uploaded_at='2026-10-18T10:00:00Z',
        )
        self.assertEqual(result.source, 'recorded_at')

    def test_unknown_date_stays_unknown_and_generated_at_is_not_upload(self):
        result = self.resolve(
            recorded_at='2023-11-21T21:49:13+00:00',
            git_commit_date='2026-10-16T17:06:25+00:00',
            generated_at='2026-10-18T10:00:00Z',
        )
        self.assertIsNone(result.timestamp)
        self.assertIsNone(result.source)
        self.assertFalse(result.is_estimated)

    def test_bad_commit_and_upload_metadata_do_not_hide_valid_recording(self):
        for commit in ('invalid', "''", '9999999999999999999999999999', '2036-01-01'):
            with self.subTest(commit=commit):
                result = self.resolve(
                    recorded_at='2026-10-16T12:00:00Z',
                    git_commit_date=commit, uploaded_at='2036-01-01',
                )
                self.assertEqual(result.source, 'recorded_at')

    def test_api_distinguishes_estimate_from_drive_start_and_sorts_by_timeline(self):
        rows = [
            {'drive': 'old', 'recorded_at': '2025-04-24T17:36:58'},
            {'drive': 'unknown', 'generated_at': '2026-10-18T10:00:00Z'},
            {'drive': 'recent', 'uploaded_at': '2026-10-18T10:00:00Z'},
        ]
        repository = EngagementRepository()
        with patch('server.data.resolve_drive_date', side_effect=lambda row: self.resolve(**row)):
            formatted = repository.format_drive_details(rows)
        self.assertEqual([row['drive'] for row in formatted], ['recent', 'old', 'unknown'])
        self.assertEqual(formatted[0]['timeline_date_source'], 'uploaded_at')
        self.assertTrue(formatted[0]['timeline_date_is_estimated'])
        self.assertIsNone(formatted[0]['drive_started_at'])
        self.assertEqual(formatted[0]['timeline_at'], '2026-10-18T10:00:00+00:00')
        self.assertEqual(formatted[1]['drive_started_at'], '2025-04-24T17:36:58+00:00')
        self.assertIsNone(formatted[2]['timeline_at'])


if __name__ == '__main__':
    unittest.main()
