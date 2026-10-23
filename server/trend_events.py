"""Persistent annotations shared by the weekly speed-bucket charts."""

from contextlib import closing
from datetime import date, timedelta
import os
from pathlib import Path
import re
import sqlite3


class TrendEventStore:
    def __init__(self, path=None):
        self.path = Path(path or os.environ.get(
            'TREND_EVENTS_DATABASE',
            str(Path(__file__).resolve().parents[1] / '.trend_events.sqlite3'),
        ))

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('''CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week TEXT NOT NULL,
            week_start TEXT NOT NULL,
            title TEXT NOT NULL,
            notes TEXT NOT NULL,
            device_id TEXT NOT NULL
        )''')
        return db

    def listing(self):
        if not self.path.exists():
            return []
        with closing(self._connect()) as db:
            return [dict(row) for row in db.execute(
                'SELECT * FROM events ORDER BY week_start, id')]

    def save(self, *, week, title, notes='', device_id='', event_id=None):
        if not re.fullmatch(r'[0-9]{4}-W[0-9]{2}', week):
            raise ValueError('Choose a valid week (YYYY-Www).')
        try:
            start = date.fromisocalendar(int(week[:4]), int(week[6:]), 1)
        except ValueError:
            raise ValueError('Choose a valid week (YYYY-Www).') from None
        title, notes, device_id = title.strip(), notes.strip(), device_id.strip()
        if not title or len(title) > 120:
            raise ValueError('Enter an event title of 1–120 characters.')
        if len(notes) > 1000 or len(device_id) > 200:
            raise ValueError('Notes must be at most 1,000 characters; device IDs at most 200.')
        values = (week, start.isoformat(), title, notes, device_id)
        with closing(self._connect()) as db, db:
            if event_id is None:
                event_id = db.execute(
                    'INSERT INTO events (week, week_start, title, notes, device_id) VALUES (?, ?, ?, ?, ?)',
                    values,
                ).lastrowid
            elif not db.execute(
                'UPDATE events SET week=?, week_start=?, title=?, notes=?, device_id=? WHERE id=?',
                (*values, event_id),
            ).rowcount:
                raise KeyError(event_id)
            return dict(db.execute('SELECT * FROM events WHERE id=?', (event_id,)).fetchone())

    def delete(self, event_id):
        with closing(self._connect()) as db, db:
            return bool(db.execute('DELETE FROM events WHERE id=?', (event_id,)).rowcount)


def visible_events(events, trends):
    """Anchor annotations to the same weekly observations in every chart."""
    first = date.fromisoformat(trends['first_date'])
    first_monday = first - timedelta(days=first.weekday())
    last = date.fromisoformat(trends['last_date'])
    visible = []
    weeks = {}
    for event in events:
        start = date.fromisoformat(event['week_start'])
        if event['device_id'] and event['device_id'] not in trends['selected_devices']:
            continue
        if not first_monday <= start <= last:
            continue
        marker = weeks.setdefault(event['week_start'], len(weeks) + 1)
        visible.append({**event, 'marker': marker,
                        'chart_date': min(start + timedelta(days=6), last).isoformat()})
    return visible


event_store = TrendEventStore()
