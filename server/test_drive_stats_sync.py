import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from server.drive_stats_sync import (
    DriveStatsSyncConfig,
    DriveStatsSynchronizer,
    _normalize_stats,
)


def _source_item(cursor: int, drive: str) -> dict:
    return {
        'cursor': cursor,
        'received_at': '2026-09-17T10:00:00+00:00',
        'stats': {
            'schema_version': 3,
            'device_id': 'device-1',
            'drive': drive,
            'disengagement_count': 2,
            'disengagements_per_100km': 4.0,
            'speed_buckets': {
                'city': {
                    'disengagement_count': 1,
                    'disengagements_per_100km': 2.0,
                },
            },
        },
    }


class DriveStatsSynchronizerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        root = Path(self.temporary_directory.name)
        self.config = DriveStatsSyncConfig(
            source_url='https://source.invalid/api/drive-stats/export',
            token='secret',
            db_path=root / 'engagement_db.json',
            cursor_path=root / 'cursor.json',
            lock_path=root / 'sync.lock',
            page_size=1,
        )
        self.synchronizer = DriveStatsSynchronizer(self.config)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_sync_preserves_history_and_imports_cursor_pages(self) -> None:
        historical = {'device_id': 'old-device', 'drive': '2025-01-01--00-00-00'}
        self.config.db_path.write_text(json.dumps([historical]), encoding='utf-8')
        pages = [
            {'items': [_source_item(1, '2026-09-17--10-00-00')], 'has_more': True},
            {'items': [_source_item(2, '2026-09-17--11-00-00')], 'has_more': False},
        ]

        with patch.object(self.synchronizer, '_fetch_page', side_effect=pages):
            imported = self.synchronizer.sync_once()

        self.assertEqual(imported, 2)
        entries = json.loads(self.config.db_path.read_text(encoding='utf-8'))
        self.assertEqual(len(entries), 3)
        self.assertIn(historical, entries)
        imported_entry = next(
            entry for entry in entries if entry.get('drive') == '2026-09-17--10-00-00'
        )
        self.assertEqual(imported_entry['intervention_count'], 2)
        self.assertEqual(
            imported_entry['speed_buckets']['city']['intervention_count'], 1
        )
        cursor = json.loads(self.config.cursor_path.read_text(encoding='utf-8'))
        self.assertEqual(cursor, {'cursor': 2})

    def test_duplicate_key_is_updated_without_duplicate_row(self) -> None:
        first = _normalize_stats(_source_item(1, '2026-09-17--10-00-00')['stats'])
        first['git_branch'] = 'old'
        self.config.db_path.write_text(json.dumps([first]), encoding='utf-8')
        replacement = _source_item(1, '2026-09-17--10-00-00')
        replacement['stats']['git_branch'] = 'new'

        with patch.object(
            self.synchronizer,
            '_fetch_page',
            return_value={'items': [replacement], 'has_more': False},
        ):
            imported = self.synchronizer.sync_once()

        self.assertEqual(imported, 1)
        entries = json.loads(self.config.db_path.read_text(encoding='utf-8'))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['git_branch'], 'new')

    def test_second_sync_resumes_from_persisted_cursor(self) -> None:
        page = {
            'items': [_source_item(7, '2026-09-17--10-00-00')],
            'has_more': False,
        }
        with patch.object(self.synchronizer, '_fetch_page', return_value=page):
            self.synchronizer.sync_once()

        with patch.object(
            self.synchronizer,
            '_fetch_page',
            return_value={'items': [], 'has_more': False},
        ) as fetch:
            self.assertEqual(self.synchronizer.sync_once(), 0)
        fetch.assert_called_once_with(7)


if __name__ == '__main__':
    unittest.main()
