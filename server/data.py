import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from statistics import mean
from typing import Any, Dict, List, Optional

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / 'engagement_db.json'


def _parse_drive_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d--%H-%M-%S').replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _ns_to_hours(value: Optional[int]) -> float:
    if not value:
        return 0.0
    return float(value) / 3_600_000_000_000


def _safe_sum(values: List[Optional[float]]) -> float:
    total = 0.0
    for item in values:
        if item is None:
            continue
        total += float(item)
    return total


def _safe_mean(values: List[Optional[float]]) -> Optional[float]:
    filtered = [float(v) for v in values if v is not None]
    if not filtered:
        return None
    return mean(filtered)


class EngagementRepository:
    """Load engagement statistics from a JSON file with basic caching."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        env_path = os.environ.get('ENGAGEMENT_DB_PATH')
        path = Path(env_path) if env_path else (db_path or _DEFAULT_DB_PATH)
        self.db_path = path.expanduser().resolve()
        self._lock = Lock()
        self._entries: List[Dict[str, Any]] = []
        self._mtime: Optional[float] = None
        self._last_loaded_at: Optional[datetime] = None

    def _load_from_disk(self) -> List[Dict[str, Any]]:
        if not self.db_path.exists():
            return []
        with self.db_path.open('r', encoding='utf-8') as handle:
            return json.load(handle)

    def _ensure_fresh_cache(self) -> None:
        try:
            mtime = self.db_path.stat().st_mtime
        except FileNotFoundError:
            mtime = None
        with self._lock:
            if mtime != self._mtime:
                self._entries = self._load_from_disk()
                self._mtime = mtime
                self._last_loaded_at = datetime.now(timezone.utc)

    def last_updated(self) -> Optional[str]:
        self._ensure_fresh_cache()
        if self._last_loaded_at is None:
            return None
        return self._last_loaded_at.isoformat()

    def all_entries(self) -> List[Dict[str, Any]]:
        self._ensure_fresh_cache()
        return list(self._entries)

    def drives_for_device(self, device_id: str) -> List[Dict[str, Any]]:
        return [row for row in self.all_entries() if row.get('device_id') == device_id]

    def device_summaries(self) -> List[Dict[str, Any]]:
        entries = self.all_entries()
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for entry in entries:
            device_id = entry.get('device_id') or 'unknown'
            grouped.setdefault(device_id, []).append(entry)

        summaries: List[Dict[str, Any]] = []
        for device_id, rows in grouped.items():
            summaries.append(self._build_device_summary(device_id, rows))

        summaries.sort(key=lambda item: item['device_id'])
        return summaries

    def device_summary(self, device_id: str) -> Optional[Dict[str, Any]]:
        rows = self.drives_for_device(device_id)
        if not rows:
            return None
        return self._build_device_summary(device_id, rows)

    def _build_device_summary(self, device_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        avg_engagement = _safe_mean([row.get('engagement_pct') for row in rows])
        avg_engagement_odo = _safe_mean([row.get('engagement_pct_odo') for row in rows])
        avg_interventions_per_100km = _safe_mean([row.get('interventions_per_100km') for row in rows])

        total_distance = _safe_sum([row.get('odo_distance') for row in rows])
        total_engaged_distance = _safe_sum([row.get('engaged_distance') for row in rows])
        total_drive_time_hours = sum(_ns_to_hours(row.get('drive_time')) for row in rows)
        total_active_time_hours = sum(_ns_to_hours(row.get('drive_time_active')) for row in rows)

        timestamps = [_parse_drive_timestamp(row.get('drive')) for row in rows]
        timestamps = [ts for ts in timestamps if ts is not None]
        first_drive = min(timestamps).isoformat() if timestamps else None
        latest_drive = max(timestamps).isoformat() if timestamps else None

        return {
            'device_id': device_id,
            'drive_count': len(rows),
            'average_engagement_pct': round(avg_engagement, 2) if avg_engagement is not None else None,
            'average_engagement_pct_odo': round(avg_engagement_odo, 2) if avg_engagement_odo is not None else None,
            'average_interventions_per_100km': round(avg_interventions_per_100km, 2) if avg_interventions_per_100km is not None else None,
            'total_distance_km': round(total_distance, 2),
            'total_engaged_distance_km': round(total_engaged_distance, 2),
            'total_drive_time_hours': round(total_drive_time_hours, 2),
            'total_active_time_hours': round(total_active_time_hours, 2),
            'first_drive': first_drive,
            'latest_drive': latest_drive,
        }

    def format_drive_details(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        formatted: List[Dict[str, Any]] = []
        for row in rows:
            drive_start = _parse_drive_timestamp(row.get('drive'))
            formatted.append({
                'drive': row.get('drive'),
                'drive_started_at': drive_start.isoformat() if drive_start else None,
                'engagement_pct': row.get('engagement_pct'),
                'engagement_pct_odo': row.get('engagement_pct_odo'),
                'drive_time_hours': round(_ns_to_hours(row.get('drive_time')), 3),
                'active_time_hours': round(_ns_to_hours(row.get('drive_time_active')), 3),
                'odo_distance_km': row.get('odo_distance'),
                'engaged_distance_km': row.get('engaged_distance'),
                'intervention_count': row.get('intervention_count'),
                'interventions_per_100km': row.get('interventions_per_100km'),
                'steer_intervention_count': row.get('steer_intervention_count'),
                'steer_interventions_per_100km': row.get('steer_interventions_per_100km'),
                'cruise_press_seconds': row.get('cruise_press_seconds'),
                'openpilot_longitudinal': row.get('openpilot_longitudinal'),
                'car_fingerprint': row.get('car_fingerprint'),
                'device_type': row.get('device_type'),
                'version': row.get('version'),
                'git_branch': row.get('git_branch'),
                'git_commit': row.get('git_commit'),
            })

        formatted.sort(key=lambda item: item['drive_started_at'] or '', reverse=True)
        return formatted


repository = EngagementRepository()
