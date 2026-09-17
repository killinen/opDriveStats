import json
import os
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from statistics import mean
from typing import Any, Dict, List, Optional

_DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / 'engagement_db.json'

SPEED_BUCKETS = [
    {
        'key': 'city',
        'label': 'City (≤55 km/h)',
        'min_speed': 0.0,
        'max_speed': 15.3,
    },
    {
        'key': 'road',
        'label': 'Road (55-90 km/h)',
        'min_speed': 15.3,
        'max_speed': 25.0,
    },
    {
        'key': 'highway',
        'label': 'Highway (≥90 km/h)',
        'min_speed': 25.0,
        'max_speed': None,
    },
]


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


def _format_pct(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return 'N/A'
    return f"{value:.{decimals}f}%"


def _round_optional(value: Optional[float], decimals: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(value, decimals)


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

    def comparison_options(self) -> Dict[str, Any]:
        entries = self.all_entries()
        devices = sorted({entry.get('device_id') or 'unknown' for entry in entries})
        branches = sorted({
            entry.get('git_branch') or 'unknown'
            for entry in entries
        })
        return {
            'devices': devices,
            'branches': branches,
            'speed_buckets': [
                {'key': 'all', 'label': 'All speeds'},
                *[
                    {'key': bucket['key'], 'label': bucket['label']}
                    for bucket in SPEED_BUCKETS
                ],
            ],
        }

    def comparison_summary(
        self,
        device_ids: Optional[List[str]] = None,
        branch: Optional[Any] = 'all',
        speed_bucket: str = 'all',
    ) -> Dict[str, Any]:
        options = self.comparison_options()
        valid_buckets = {'all'} | {bucket['key'] for bucket in SPEED_BUCKETS}
        selected_bucket = speed_bucket if speed_bucket in valid_buckets else 'all'
        selected_devices = [
            device_id
            for device_id in (device_ids or [])
            if device_id and device_id in options['devices']
        ]
        if not selected_devices:
            selected_devices = options['devices']

        entries = self.all_entries()
        if selected_devices:
            entries = [
                entry
                for entry in entries
                if (entry.get('device_id') or 'unknown') in selected_devices
            ]

        if isinstance(branch, list):
            requested_branches = [item for item in branch if item]
        elif branch:
            requested_branches = [branch]
        else:
            requested_branches = ['all']

        if not requested_branches or 'all' in requested_branches:
            selected_branches = ['all']
        else:
            selected_branches = [
                item
                for item in requested_branches
                if item in options['branches']
            ]
            if not selected_branches:
                selected_branches = ['all']

        if selected_branches != ['all']:
            entries = [
                entry
                for entry in entries
                if (entry.get('git_branch') or 'unknown') in selected_branches
            ]

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for entry in entries:
            grouped.setdefault(entry.get('device_id') or 'unknown', []).append(entry)

        rows = []
        for device_id in selected_devices:
            device_rows = grouped.get(device_id, [])
            if selected_bucket == 'all':
                rows.append(self._build_comparison_all_row(device_id, device_rows))
            else:
                rows.append(self._build_comparison_bucket_row(device_id, device_rows, selected_bucket))

        rows = [row for row in rows if row is not None]
        totals = self._build_comparison_total_row(rows)
        bucket_label = 'All speeds'
        for bucket in options['speed_buckets']:
            if bucket['key'] == selected_bucket:
                bucket_label = bucket['label']
                break

        return {
            'options': options,
            'selected_devices': selected_devices,
            'selected_branches': selected_branches,
            'selected_branch': 'all' if selected_branches == ['all'] else ', '.join(selected_branches),
            'selected_speed_bucket': selected_bucket,
            'selected_speed_bucket_label': bucket_label,
            'rows': rows,
            'totals': totals,
            'last_loaded': self.last_updated(),
        }

    def _branch_distribution(self, rows: List[Dict[str, Any]]) -> str:
        counts: Dict[str, int] = {}
        for row in rows:
            branch = row.get('git_branch') or 'unknown'
            counts[branch] = counts.get(branch, 0) + 1
        if not counts:
            return '—'
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if len(ordered) == 1:
            return ordered[0][0]
        return ', '.join(f'{branch} ({count})' for branch, count in ordered[:3])

    def _first_latest_drive_values(self, rows: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
        if not rows:
            return {'first_drive': None, 'latest_drive': None}

        parsed = [
            (_parse_drive_timestamp(row.get('drive')), row.get('drive'))
            for row in rows
            if row.get('drive')
        ]
        parsed_timestamps = [(ts, drive) for ts, drive in parsed if ts is not None]
        if parsed_timestamps:
            return {
                'first_drive': min(parsed_timestamps, key=lambda item: item[0])[1],
                'latest_drive': max(parsed_timestamps, key=lambda item: item[0])[1],
            }

        drive_names = sorted(row.get('drive') for row in rows if row.get('drive'))
        return {
            'first_drive': drive_names[0] if drive_names else None,
            'latest_drive': drive_names[-1] if drive_names else None,
        }

    def _speed_mix(
        self,
        bucket_distances: Dict[str, float],
    ) -> Dict[str, Optional[float]]:
        """Return each speed bucket's share of distance with speed data."""
        classified_distance_km = _safe_sum([
            bucket_distances.get(bucket['key'])
            for bucket in SPEED_BUCKETS
        ])
        return {
            bucket['key']: _round_optional(
                float(bucket_distances.get(bucket['key']) or 0.0) / classified_distance_km * 100
                if classified_distance_km > 0 else None
            )
            for bucket in SPEED_BUCKETS
        }

    def _build_comparison_all_row(self, device_id: str, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        summary = self._build_device_summary(device_id, rows)
        drive_values = self._first_latest_drive_values(rows)
        speed_bucket_distance_km = {
            bucket['key']: summary['speed_bucket_summary'][bucket['key']]['distance_km_raw']
            for bucket in SPEED_BUCKETS
        }
        return {
            'device_id': device_id,
            'branch': self._branch_distribution(rows),
            'drive_count': summary['drive_count'],
            'distance_km': summary['total_distance_km'],
            'engaged_distance_km': summary['total_engaged_distance_km'],
            'time_hours': summary['total_time_hours'],
            'engaged_time_hours': summary['total_active_time_hours'],
            'time_engagement_pct': summary['overall_time_engagement_pct'],
            'distance_engagement_pct': summary['overall_engagement_pct'],
            'disengagement_count': summary['total_disengagement_count'],
            'disengagements_per_100km': summary['total_disengagements_per_100km'],
            'disengagement_distance_km': summary['total_distance_km'],
            'steer_intervention_count': summary['total_steer_intervention_count'],
            'steer_interventions_per_100km': summary['total_steer_interventions_per_100km'],
            'steer_distance_km': summary['total_steer_intervention_km'],
            'speed_bucket_distance_km': speed_bucket_distance_km,
            'speed_mix_pct': self._speed_mix(speed_bucket_distance_km),
            'first_drive': drive_values['first_drive'],
            'latest_drive': drive_values['latest_drive'],
        }

    def _build_comparison_bucket_row(
        self,
        device_id: str,
        rows: List[Dict[str, Any]],
        bucket_key: str,
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        STEER_INTERVENTION_START_DATE = datetime(2025, 7, 7, tzinfo=timezone.utc)
        time_ns = 0
        engaged_time_ns = 0
        distance_km = 0.0
        engaged_distance_km = 0.0
        disengagement_count = 0.0
        disengagement_distance_km = 0.0
        steer_intervention_count = 0.0
        steer_distance_km = 0.0
        rows_with_bucket_data = []

        for row in rows:
            bucket = (row.get('speed_buckets') or {}).get(bucket_key)
            if not bucket:
                continue

            bucket_time_ns = bucket.get('time_ns') or 0
            bucket_distance_km = float(bucket.get('distance_km_raw') or 0.0)
            if bucket_time_ns <= 0 and bucket_distance_km <= 0:
                continue

            rows_with_bucket_data.append(row)
            time_ns += bucket_time_ns
            engaged_time_ns += bucket.get('engaged_time_ns') or 0
            distance_km += bucket_distance_km
            engaged_distance_km += float(bucket.get('engaged_distance_km_raw') or 0.0)

            bucket_disengagement_count = bucket.get('disengagement_count')
            if bucket_disengagement_count is not None:
                disengagement_count += float(bucket_disengagement_count or 0.0)
                disengagement_distance_km += bucket_distance_km

            bucket_steer_count = bucket.get('steer_intervention_count')
            drive_date = _parse_drive_timestamp(row.get('drive'))
            if bucket_steer_count is not None and (drive_date is None or drive_date >= STEER_INTERVENTION_START_DATE):
                steer_intervention_count += float(bucket_steer_count or 0.0)
                steer_distance_km += bucket_distance_km

        if not rows_with_bucket_data:
            return None

        time_hours = time_ns / 1e9 / 3600
        engaged_time_hours = engaged_time_ns / 1e9 / 3600
        time_engagement_pct = engaged_time_ns / time_ns * 100 if time_ns > 0 else None
        distance_engagement_pct = engaged_distance_km / distance_km * 100 if distance_km > 0 else None
        disengagements_per_100km = (
            disengagement_count / disengagement_distance_km * 100
            if disengagement_distance_km > 0 else None
        )
        steer_interventions_per_100km = (
            steer_intervention_count / steer_distance_km * 100
            if steer_distance_km > 0 else None
        )
        drive_values = self._first_latest_drive_values(rows_with_bucket_data)

        return {
            'device_id': device_id,
            'branch': self._branch_distribution(rows_with_bucket_data),
            'drive_count': len(rows_with_bucket_data),
            'distance_km': round(distance_km, 2),
            'engaged_distance_km': round(engaged_distance_km, 2),
            'time_hours': round(time_hours, 2),
            'engaged_time_hours': round(engaged_time_hours, 2),
            'time_engagement_pct': _round_optional(time_engagement_pct),
            'distance_engagement_pct': _round_optional(distance_engagement_pct),
            'disengagement_count': int(round(disengagement_count)),
            'disengagements_per_100km': _round_optional(disengagements_per_100km),
            'disengagement_distance_km': round(disengagement_distance_km, 2),
            'steer_intervention_count': int(round(steer_intervention_count)),
            'steer_interventions_per_100km': _round_optional(steer_interventions_per_100km),
            'steer_distance_km': round(steer_distance_km, 2),
            'first_drive': drive_values['first_drive'],
            'latest_drive': drive_values['latest_drive'],
        }

    def _build_comparison_total_row(self, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        distance_km = _safe_sum([row.get('distance_km') for row in rows])
        engaged_distance_km = _safe_sum([row.get('engaged_distance_km') for row in rows])
        time_hours = _safe_sum([row.get('time_hours') for row in rows])
        engaged_time_hours = _safe_sum([row.get('engaged_time_hours') for row in rows])
        disengagement_count = sum(float(row.get('disengagement_count') or 0.0) for row in rows)
        disengagement_distance_km = _safe_sum([row.get('disengagement_distance_km') for row in rows])
        steer_intervention_count = sum(float(row.get('steer_intervention_count') or 0.0) for row in rows)
        steer_distance_km = _safe_sum([row.get('steer_distance_km') for row in rows])
        speed_bucket_distance_km = None
        if any(row.get('speed_bucket_distance_km') is not None for row in rows):
            speed_bucket_distance_km = {
                bucket['key']: _safe_sum([
                    (row.get('speed_bucket_distance_km') or {}).get(bucket['key'])
                    for row in rows
                ])
                for bucket in SPEED_BUCKETS
            }

        return {
            'device_id': 'Total',
            'branch': '—',
            'drive_count': sum(int(row.get('drive_count') or 0) for row in rows),
            'distance_km': round(distance_km, 2),
            'engaged_distance_km': round(engaged_distance_km, 2),
            'time_hours': round(time_hours, 2),
            'engaged_time_hours': round(engaged_time_hours, 2),
            'time_engagement_pct': _round_optional(engaged_time_hours / time_hours * 100 if time_hours > 0 else None),
            'distance_engagement_pct': _round_optional(engaged_distance_km / distance_km * 100 if distance_km > 0 else None),
            'disengagement_count': int(round(disengagement_count)),
            'disengagements_per_100km': _round_optional(disengagement_count / disengagement_distance_km * 100 if disengagement_distance_km > 0 else None),
            'disengagement_distance_km': round(disengagement_distance_km, 2),
            'steer_intervention_count': int(round(steer_intervention_count)),
            'steer_interventions_per_100km': _round_optional(steer_intervention_count / steer_distance_km * 100 if steer_distance_km > 0 else None),
            'steer_distance_km': round(steer_distance_km, 2),
            'speed_bucket_distance_km': speed_bucket_distance_km,
            'speed_mix_pct': self._speed_mix(speed_bucket_distance_km) if speed_bucket_distance_km is not None else None,
            'first_drive': None,
            'latest_drive': None,
        }

    def _build_device_summary(self, device_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        avg_engagement = _safe_mean([row.get('engagement_pct') for row in rows])
        avg_engagement_odo = _safe_mean([row.get('engagement_pct_odo') for row in rows])
        avg_interventions_per_100km = _safe_mean([row.get('interventions_per_100km') for row in rows])

        total_distance = _safe_sum([row.get('odo_distance') for row in rows])
        total_engaged_distance = _safe_sum([row.get('engaged_distance') for row in rows])
        total_drive_time_hours = sum(_ns_to_hours(row.get('drive_time')) for row in rows)
        total_active_time_hours = sum(_ns_to_hours(row.get('drive_time_active')) for row in rows)
        total_time_hours = sum(_ns_to_hours(row.get('total_time')) for row in rows)
        total_interventions = sum(float(row.get('intervention_count') or 0.0) for row in rows)
        total_disengagements = sum(
            float(row.get('disengagement_count'))
            for row in rows
            if row.get('disengagement_count') is not None
        )
        total_steer_interventions = sum(float(row.get('steer_intervention_count') or 0.0) for row in rows)

        overall_engagement_pct = (
            total_engaged_distance / total_distance * 100 if total_distance > 0 else None
        )
        overall_time_engagement_pct = (
            total_active_time_hours / total_time_hours * 100 if total_time_hours > 0 else None
        )
        total_interventions_per_100km = (
            total_interventions / total_distance * 100 if total_distance > 0 else None
        )
        total_disengagements_per_100km = (
            total_disengagements / total_distance * 100 if total_distance > 0 else None
        )

        STEER_INTERVENTION_START_DATE = datetime(2025, 7, 7, tzinfo=timezone.utc)
        total_steer_intervention_km = 0.0
        for row in rows:
            drive_date = _parse_drive_timestamp(row.get('drive'))
            # Newer route-style drive IDs do not contain a timestamp. Their
            # steering metrics are already populated, so include their distance.
            if drive_date is None or drive_date >= STEER_INTERVENTION_START_DATE:
                total_steer_intervention_km += row.get('odo_distance') or 0.0

        total_steer_interventions_per_100km = (
            total_steer_interventions / total_steer_intervention_km * 100 if total_steer_intervention_km > 0 else None
        )

        timestamps = [_parse_drive_timestamp(row.get('drive')) for row in rows]
        timestamps = [ts for ts in timestamps if ts is not None]
        first_drive = min(timestamps).isoformat() if timestamps else None
        latest_drive = max(timestamps).isoformat() if timestamps else None

        bucket_aggregate = {
            bucket['key']: {
                'time_ns': 0,
                'engaged_time_ns': 0,
                'distance_km': 0.0,
                'engaged_distance_km': 0.0,
                'intervention_count': 0.0,
                'steer_intervention_count': 0.0,
                'disengagement_count': 0.0,
                'steer_distance_km': 0.0,
                'disengagement_distance_km': 0.0,
            }
            for bucket in SPEED_BUCKETS
        }

        for row in rows:
            drive_date = _parse_drive_timestamp(row.get('drive'))
            row_buckets = row.get('speed_buckets') or {}
            for key, data in row_buckets.items():
                if key in bucket_aggregate:
                    bucket_data = bucket_aggregate[key]
                    bucket_data['time_ns'] += data.get('time_ns', 0)
                    bucket_data['engaged_time_ns'] += data.get('engaged_time_ns', 0)
                    bucket_data['distance_km'] += data.get('distance_km_raw', 0.0)
                    bucket_data['engaged_distance_km'] += data.get('engaged_distance_km_raw', 0.0)
                    bucket_data['intervention_count'] += float(data.get('intervention_count') or 0.0)

                    steer_count = data.get('steer_intervention_count')
                    if steer_count is not None:
                        if drive_date is None or drive_date >= STEER_INTERVENTION_START_DATE:
                            bucket_data['steer_intervention_count'] += float(steer_count or 0.0)
                            bucket_data['steer_distance_km'] += data.get('distance_km_raw', 0.0)

                    diseng_count = data.get('disengagement_count')
                    if diseng_count is not None:
                        bucket_data['disengagement_count'] += float(diseng_count or 0.0)
                        bucket_data['disengagement_distance_km'] += data.get('distance_km_raw', 0.0)
        
        speed_bucket_summary = {}
        for bucket_cfg in SPEED_BUCKETS:
            key = bucket_cfg['key']
            data = bucket_aggregate[key]
            total_time_min = data['time_ns'] / 1e9 / 60
            engaged_time_min = data['engaged_time_ns'] / 1e9 / 60
            engagement_pct = (data['engaged_time_ns'] / data['time_ns'] * 100) if data['time_ns'] > 0 else None
            dist_engagement_pct = (data['engaged_distance_km'] / data['distance_km'] * 100) if data['distance_km'] > 0 else None
            interventions_per_100km = (
                data['intervention_count'] / data['distance_km'] * 100 if data['distance_km'] > 0 else None
            )
            steer_per_100km = (
                data['steer_intervention_count'] / data['steer_distance_km'] * 100 if data['steer_distance_km'] > 0 else None
            )
            diseng_per_100km = (
                data['disengagement_count'] / data['disengagement_distance_km'] * 100 if data['disengagement_distance_km'] > 0 else None
            )

            speed_bucket_summary[key] = {
                'label': bucket_cfg['label'],
                'time_min': round(total_time_min, 2),
                'engaged_time_min': round(engaged_time_min, 2),
                'distance_km': round(data['distance_km'], 2),
                'engaged_distance_km': round(data['engaged_distance_km'], 2),
                'engagement_pct': round(engagement_pct, 2) if engagement_pct is not None else None,
                'dist_engagement_pct': round(dist_engagement_pct, 2) if dist_engagement_pct is not None else None,
                'time_ns': data['time_ns'],
                'engaged_time_ns': data['engaged_time_ns'],
                'distance_km_raw': data['distance_km'],
                'engaged_distance_km_raw': data['engaged_distance_km'],
                'intervention_count': data['intervention_count'],
                'steer_intervention_count': data['steer_intervention_count'],
                'disengagement_count': data['disengagement_count'],
                'interventions_per_100km': round(interventions_per_100km, 2) if interventions_per_100km is not None else None,
                'steer_interventions_per_100km': round(steer_per_100km, 2) if steer_per_100km is not None else None,
                'disengagements_per_100km': round(diseng_per_100km, 2) if diseng_per_100km is not None else None,
            }

        return {
            'speed_bucket_summary': speed_bucket_summary,
            'device_id': device_id,
            'drive_count': len(rows),
            'average_engagement_pct': round(avg_engagement, 2) if avg_engagement is not None else None,
            'average_engagement_pct_odo': round(avg_engagement_odo, 2) if avg_engagement_odo is not None else None,
            'average_interventions_per_100km': round(avg_interventions_per_100km, 2) if avg_interventions_per_100km is not None else None,
            'total_distance_km': round(total_distance, 2),
            'total_engaged_distance_km': round(total_engaged_distance, 2),
            'total_drive_time_hours': round(total_drive_time_hours, 2),
            'total_active_time_hours': round(total_active_time_hours, 2),
            'total_time_hours': round(total_time_hours, 2),
            'overall_engagement_pct': round(overall_engagement_pct, 2) if overall_engagement_pct is not None else None,
            'overall_time_engagement_pct': round(overall_time_engagement_pct, 2) if overall_time_engagement_pct is not None else None,
            'total_intervention_count': int(round(total_interventions)),
            'total_interventions_per_100km': round(total_interventions_per_100km, 2) if total_interventions_per_100km is not None else None,
            'total_disengagement_count': int(round(total_disengagements)),
            'total_disengagements_per_100km': round(total_disengagements_per_100km, 2) if total_disengagements_per_100km is not None else None,
            'total_steer_intervention_count': int(round(total_steer_interventions)),
            'total_steer_interventions_per_100km': round(total_steer_interventions_per_100km, 2) if total_steer_interventions_per_100km is not None else None,
            'first_drive': first_drive,
            'latest_drive': latest_drive,
            'total_steer_intervention_km': total_steer_intervention_km,
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
                'cruise_press_seconds_per_hour': row.get('cruise_press_seconds_per_hour'),
                'openpilot_longitudinal': row.get('openpilot_longitudinal'),
                'car_fingerprint': row.get('car_fingerprint'),
                'device_type': row.get('device_type'),
                'version': row.get('version'),
                'git_branch': row.get('git_branch'),
                'git_commit': row.get('git_commit'),
            })

        formatted.sort(key=lambda item: item['drive_started_at'] or '', reverse=True)
        return formatted


    def cli_summary(self, include_device_columns: bool = False) -> str:
        base_columns = [
            ('Date/Time', 20, 'left'),
            ('Dur (min)', 9, 'right'),
            ('Drive (min)', 11, 'right'),
            ('Dist (km)', 10, 'right'),
            ('Eng (km)', 10, 'right'),
            ('Time %', 7, 'right'),
            ('Drive %', 7, 'right'),
            ('ODO %', 6, 'right'),
            ('Diseng', 6, 'right'),
            ('Dis/100km', 11, 'right'),
            ('Steer', 5, 'right'),
            ('ST/100km', 11, 'right'),
            ('Press s/h', 12, 'right'),
            ('OPLong', 7, 'left'),
        ]
        device_columns = [
            ('Version', 16, 'left'),
            ('Branch', 18, 'left'),
            ('Car', 24, 'left'),
            ('Device', 10, 'left'),
        ]

        def _fmt_cell(value: Optional[str], width: int, align: str) -> str:
            text = '' if value is None else str(value)
            if len(text) > width:
                text = text[:max(0, width - 1)] + ('…' if width > 1 else '')
            if align == 'left':
                return text.ljust(width)
            if align == 'center':
                return text.center(width)
            return text.rjust(width)

        columns = list(base_columns)
        if include_device_columns:
            columns.extend(device_columns)

        header = '  '.join(
            _fmt_cell(
                label,
                width,
                'center' if align == 'center' else ('right' if align == 'right' else 'left')
            )
            for (label, width, align) in columns
        )

        line_width = max(120, len(header))
        separator = '-' * len(header)

        title = '📊 ENGAGEMENT SUMMARY'
        lines: List[str] = []
        lines.append('=' * line_width)
        if len(title) < line_width:
            lines.append(title.center(line_width))
        else:
            lines.append(title)
        lines.append('=' * line_width)

        all_devices = sorted({entry.get('device_id') or 'unknown' for entry in self.all_entries()})

        if not all_devices:
            lines.append('No engagement data available.')
            lines.append('=' * line_width)
            return '\n'.join(lines)

        for device_id in all_devices:
            drives = self.drives_for_device(device_id)
            if not drives:
                continue

            lines.append(f"\n🚗 Device: {device_id}")
            lines.append(header)
            lines.append(separator)

            total_time = 0.0
            total_active_time = 0.0
            total_drive_time = 0.0
            total_drive_active_time = 0.0
            total_distance = 0.0
            total_engaged_distance = 0.0
            total_interventions = 0
            total_steer_interventions = 0
            total_steer_intervention_km = 0.0
            total_cruise_press_time_ns = 0
            bucket_totals = {
                bucket['key']: {
                    'time': 0,
                    'engaged_time': 0,
                    'distance': 0.0,
                    'engaged_distance': 0.0,
                }
                for bucket in SPEED_BUCKETS
            }

            sorted_drives = sorted(
                drives,
                key=lambda item: _parse_drive_timestamp(item.get('drive')) or datetime.min
            )

            for drive in sorted_drives:
                drive_name = drive.get('drive') or 'unknown'
                total_time_ns = drive.get('total_time') or 0
                active_time_ns = drive.get('active_time') or 0
                drive_time_ns = drive.get('drive_time') or 0
                drive_active_time_ns = drive.get('drive_time_active') or 0
                distance_km = drive.get('odo_distance') or 0.0
                engaged_distance_km = drive.get('engaged_distance') or 0.0
                disengagements = drive.get('intervention_count') or 0
                steer_interventions = drive.get('steer_intervention_count') or 0
                cruise_press_time_ns = drive.get('cruise_press_time_ns') or 0

                if total_time_ns <= 0:
                    continue

                total_time += total_time_ns
                total_active_time += active_time_ns
                total_drive_time += drive_time_ns
                total_drive_active_time += drive_active_time_ns
                total_distance += distance_km
                total_engaged_distance += engaged_distance_km
                total_interventions += disengagements
                total_steer_interventions += steer_interventions
                total_cruise_press_time_ns += cruise_press_time_ns

                STEER_INTERVENTION_START_DATE = datetime(2025, 7, 7, tzinfo=timezone.utc)
                drive_date = _parse_drive_timestamp(drive.get('drive'))
                if drive_date is None or drive_date >= STEER_INTERVENTION_START_DATE:
                    total_steer_intervention_km += distance_km

                duration_minutes = total_time_ns / 1e9 / 60
                drive_duration_minutes = drive_time_ns / 1e9 / 60 if drive_time_ns else 0.0
                time_pct = (active_time_ns / total_time_ns * 100) if total_time_ns else 0.0
                drive_pct = (
                    drive.get('drive_time_engagement_pct')
                    if drive.get('drive_time_engagement_pct') is not None
                    else (drive_active_time_ns / drive_time_ns * 100 if drive_time_ns else None)
                )
                odo_pct = drive.get('engagement_pct_odo')
                diseng_per_100km = drive.get('interventions_per_100km')
                steer_per_100km = drive.get('steer_interventions_per_100km')
                press_seconds_per_hour = drive.get('cruise_press_seconds_per_hour')

                drive_display = drive_name.replace('--', ' ').replace('-', '/')
                duration_cell = f"{duration_minutes:.1f}"
                drive_duration_cell = f"{drive_duration_minutes:.1f}"
                distance_cell = f"{distance_km:.1f}"
                engaged_distance_cell = f"{engaged_distance_km:.1f}"
                time_pct_cell = f"{time_pct:.1f}"
                drive_pct_cell = f"{drive_pct:.1f}" if drive_pct is not None else 'N/A'
                odo_pct_cell = f"{odo_pct:.1f}" if odo_pct is not None else 'N/A'
                diseng_per_100_cell = f"{(diseng_per_100km or 0):.1f}"
                steer_per_100_cell = f"{(steer_per_100km or 0):.1f}"
                press_per_hour_cell = f"{(press_seconds_per_hour or 0):.2f}"

                if include_device_columns:
                    opl = drive.get('openpilot_longitudinal')
                    if opl is True:
                        opl_display = 'ON'
                    elif opl is False:
                        opl_display = 'OFF'
                    else:
                        opl_display = '—'

                    version_value = drive.get('version') or '—'
                    branch_value = drive.get('git_branch') or '—'
                    car_value = drive.get('car_fingerprint') or '—'
                    device_value = drive.get('device_type') or '—'
                else:
                    opl = drive.get('openpilot_longitudinal')
                    if opl is True:
                        opl_display = 'ON'
                    elif opl is False:
                        opl_display = 'OFF'
                    else:
                        opl_display = '—'
                    version_value = branch_value = car_value = device_value = None

                row_cells = [
                    _fmt_cell(drive_display, 20, 'left'),
                    _fmt_cell(duration_cell, 9, 'right'),
                    _fmt_cell(drive_duration_cell, 11, 'right'),
                    _fmt_cell(distance_cell, 10, 'right'),
                    _fmt_cell(engaged_distance_cell, 10, 'right'),
                    _fmt_cell(time_pct_cell, 6, 'right'),
                    _fmt_cell(drive_pct_cell, 6, 'right'),
                    _fmt_cell(odo_pct_cell, 6, 'right'),
                    _fmt_cell(str(disengagements), 6, 'right'),
                    _fmt_cell(diseng_per_100_cell, 10, 'right'),
                    _fmt_cell(str(steer_interventions), 5, 'right'),
                    _fmt_cell(steer_per_100_cell, 10, 'right'),
                    _fmt_cell(press_per_hour_cell, 12, 'right'),
                    _fmt_cell(opl_display, 7, 'right'),
                ]

                if include_device_columns:
                    row_cells.extend([
                        _fmt_cell(version_value, 14, 'left'),
                        _fmt_cell(branch_value, 16, 'left'),
                        _fmt_cell(car_value, 20, 'left'),
                        _fmt_cell(device_value, 12, 'left'),
                    ])

                lines.append('  '.join(row_cells))

                bucket_stats = drive.get('speed_buckets') or {}
                for bucket_cfg in SPEED_BUCKETS:
                    key = bucket_cfg['key']
                    data = bucket_stats.get(key)
                    if not data:
                        continue
                    bucket_totals[key]['time'] += data.get('time_ns', 0)
                    bucket_totals[key]['engaged_time'] += data.get('engaged_time_ns', 0)
                    bucket_totals[key]['distance'] += data.get('distance_km_raw', 0.0)
                    bucket_totals[key]['engaged_distance'] += data.get('engaged_distance_km_raw', 0.0)

            if total_time <= 0:
                lines.append('No valid drive data found for this device.')
                continue

            lines.append(separator)

            total_percentage = (total_active_time / total_time * 100) if total_time else None
            total_drive_percentage = (
                (total_drive_active_time / total_drive_time * 100)
                if total_drive_time else None
            )
            total_interventions_per_100km = (
                (total_interventions / total_distance * 100) if total_distance else 0.0
            )
            total_steer_interventions_per_100km = (
                (total_steer_interventions / total_steer_intervention_km * 100) if total_steer_intervention_km else 0.0
            )
            lines.append('📈 TOTALS:')
            lines.append(f"   • Total Distance: {total_distance:.1f} km")
            lines.append(f"   • Overall Time Engagement: {_format_pct(total_percentage)}")

            if total_distance > 0 and total_engaged_distance > 0:
                odo_pct_total = total_engaged_distance / total_distance * 100
                lines.append(f"   • Overall ODO Engagement: {_format_pct(odo_pct_total)}")

            if total_drive_time > 0:
                total_drive_minutes = total_drive_time / 1e9 / 60
                lines.append(f"   • Total Drive Time: {total_drive_minutes:.1f} min")
                lines.append(f"   • Drive-Time Engagement: {_format_pct(total_drive_percentage)}")

                total_drive_hours = total_drive_time / 1e9 / 3600
                if total_drive_hours > 0 and total_cruise_press_time_ns > 0:
                    press_seconds = total_cruise_press_time_ns / 1e9
                    lines.append(f"   • Total Cruise Button Press Time: {press_seconds:.1f} s")
                    press_seconds_per_hour = press_seconds / total_drive_hours
                    lines.append(
                        f"   • Cruise Press Seconds per Drive Hour: {press_seconds_per_hour:.2f}s"
                    )

            lines.append(
                f"   • Total Disengagements: {total_interventions} ({total_interventions_per_100km:.2f}/100km)"
            )
            lines.append(
                f"   • Total Steering Interventions: {total_steer_interventions} ({total_steer_interventions_per_100km:.2f}/100km)"
            )

            if any(data['time'] > 0 for data in bucket_totals.values()):
                lines.append('   • Speed Bucket Engagement:')
                for bucket_cfg in SPEED_BUCKETS:
                    data = bucket_totals[bucket_cfg['key']]
                    if data['time'] <= 0:
                        continue
                    total_time_min = data['time'] / 1e9 / 60
                    engaged_time_min = data['engaged_time'] / 1e9 / 60
                    total_distance_km = data['distance']
                    engaged_distance_km = data['engaged_distance']
                    engagement_time_pct = (
                        data['engaged_time'] / data['time'] * 100
                        if data['time'] > 0 else None
                    )
                    engagement_dist_pct = (
                        engaged_distance_km / total_distance_km * 100
                        if total_distance_km > 0 else None
                    )
                    lines.append(
                        "     - {label}: {time_pct} / {dist_pct} (time {eng:.1f}/{tot:.1f} min, distance {eng_dist:.1f}/{tot_dist:.1f} km)".format(
                            label=bucket_cfg['label'],
                            time_pct=_format_pct(engagement_time_pct, 2),
                            dist_pct=_format_pct(engagement_dist_pct, 2),
                            eng=engaged_time_min,
                            tot=total_time_min,
                            eng_dist=engaged_distance_km,
                            tot_dist=total_distance_km,
                        )
                    )

            lines.append('=' * line_width)

        return '\n'.join(lines)

    def grand_total_summary(self) -> Dict[str, Any]:
        summaries = self.device_summaries()
        if not summaries:
            return {}

        grand_total_distance_km = sum(s.get('total_distance_km', 0) for s in summaries)
        grand_total_engaged_distance_km = sum(s.get('total_engaged_distance_km', 0) for s in summaries)
        grand_total_drive_time_hours = sum(s.get('total_drive_time_hours', 0) for s in summaries)
        grand_total_active_time_hours = sum(s.get('total_active_time_hours', 0) for s in summaries)
        grand_total_time_hours = sum(s.get('total_time_hours', 0) for s in summaries)
        grand_total_intervention_count = sum(s.get('total_intervention_count', 0) for s in summaries)
        grand_total_disengagement_count = sum(s.get('total_disengagement_count', 0) or 0 for s in summaries)
        grand_total_steer_intervention_count = sum(s.get('total_steer_intervention_count', 0) for s in summaries)
        grand_total_steer_intervention_km = sum(s.get('total_steer_intervention_km', 0) for s in summaries)
        drive_count = sum(s.get('drive_count', 0) for s in summaries)

        overall_engagement_pct = (
            grand_total_engaged_distance_km / grand_total_distance_km * 100 if grand_total_distance_km > 0 else None
        )
        overall_time_engagement_pct = (
            grand_total_active_time_hours / grand_total_time_hours * 100 if grand_total_time_hours > 0 else None
        )
        total_interventions_per_100km = (
            grand_total_intervention_count / grand_total_distance_km * 100 if grand_total_distance_km > 0 else None
        )
        total_disengagements_per_100km = (
            grand_total_disengagement_count / grand_total_distance_km * 100 if grand_total_distance_km > 0 else None
        )
        total_steer_interventions_per_100km = (
            grand_total_steer_intervention_count / grand_total_steer_intervention_km * 100 if grand_total_steer_intervention_km > 0 else None
        )

        grand_total_buckets = {
            bucket['key']: {
                'time_ns': 0,
                'engaged_time_ns': 0,
                'distance_km': 0.0,
                'engaged_distance_km': 0.0,
                'intervention_count': 0.0,
                'steer_intervention_count': 0.0,
                'disengagement_count': 0.0,
                'steer_distance_km': 0.0,
                'disengagement_distance_km': 0.0,
            }
            for bucket in SPEED_BUCKETS
        }

        for s in summaries:
            summary_buckets = s.get('speed_bucket_summary') or {}
            for key, data in summary_buckets.items():
                if key in grand_total_buckets:
                    bucket_data = grand_total_buckets[key]
                    bucket_data['time_ns'] += data.get('time_ns', 0)
                    bucket_data['engaged_time_ns'] += data.get('engaged_time_ns', 0)
                    bucket_data['distance_km'] += data.get('distance_km_raw', 0.0)
                    bucket_data['engaged_distance_km'] += data.get('engaged_distance_km_raw', 0.0)
                    bucket_data['intervention_count'] += data.get('intervention_count', 0.0) or 0.0
                    bucket_data['steer_intervention_count'] += data.get('steer_intervention_count', 0.0) or 0.0
                    bucket_data['disengagement_count'] += data.get('disengagement_count', 0.0) or 0.0
                    bucket_data['steer_distance_km'] += data.get('distance_km_raw', 0.0) if data.get('steer_intervention_count') is not None else 0.0
                    bucket_data['disengagement_distance_km'] += data.get('distance_km_raw', 0.0) if data.get('disengagement_count') is not None else 0.0

        grand_total_bucket_summary = {}
        grand_total_time_ns = sum(b.get('time_ns', 0) for b in grand_total_buckets.values())

        for bucket_cfg in SPEED_BUCKETS:
            key = bucket_cfg['key']
            data = grand_total_buckets[key]
            total_time_min = data['time_ns'] / 1e9 / 60
            engaged_time_min = data['engaged_time_ns'] / 1e9 / 60
            engagement_pct = (data['engaged_time_ns'] / data['time_ns'] * 100) if data['time_ns'] > 0 else None
            dist_engagement_pct = (data['engaged_distance_km'] / data['distance_km'] * 100) if data['distance_km'] > 0 else None
            interventions_per_100km = (
                data['intervention_count'] / data['distance_km'] * 100 if data['distance_km'] > 0 else None
            )
            steer_per_100km = (
                data['steer_intervention_count'] / data['steer_distance_km'] * 100 if data['steer_distance_km'] > 0 else None
            )
            diseng_per_100km = (
                data['disengagement_count'] / data['disengagement_distance_km'] * 100 if data['disengagement_distance_km'] > 0 else None
            )
            
            distance_pct_of_total = (data['distance_km'] / grand_total_distance_km * 100) if grand_total_distance_km > 0 else None
            time_pct_of_total = (data['time_ns'] / grand_total_time_ns * 100) if grand_total_time_ns > 0 else None

            grand_total_bucket_summary[key] = {
                'label': bucket_cfg['label'],
                'time_min': round(total_time_min, 2),
                'engaged_time_min': round(engaged_time_min, 2),
                'distance_km': round(data['distance_km'], 2),
                'engaged_distance_km': round(data['engaged_distance_km'], 2),
                'engagement_pct': round(engagement_pct, 2) if engagement_pct is not None else None,
                'dist_engagement_pct': round(dist_engagement_pct, 2) if dist_engagement_pct is not None else None,
                'intervention_count': data['intervention_count'],
                'steer_intervention_count': data['steer_intervention_count'],
                'disengagement_count': data['disengagement_count'],
                'interventions_per_100km': round(interventions_per_100km, 2) if interventions_per_100km is not None else None,
                'steer_interventions_per_100km': round(steer_per_100km, 2) if steer_per_100km is not None else None,
                'disengagements_per_100km': round(diseng_per_100km, 2) if diseng_per_100km is not None else None,
                'distance_pct_of_total': round(distance_pct_of_total, 2) if distance_pct_of_total is not None else None,
                'time_pct_of_total': round(time_pct_of_total, 2) if time_pct_of_total is not None else None,
            }

        return {
            'speed_bucket_summary': grand_total_bucket_summary,
            'drive_count': drive_count,
            'total_distance_km': round(grand_total_distance_km, 2),
            'total_engaged_distance_km': round(grand_total_engaged_distance_km, 2),
            'total_drive_time_hours': round(grand_total_drive_time_hours, 2),
            'total_active_time_hours': round(grand_total_active_time_hours, 2),
            'total_time_hours': round(grand_total_time_hours, 2),
            'overall_engagement_pct': round(overall_engagement_pct, 2) if overall_engagement_pct is not None else None,
            'overall_time_engagement_pct': round(overall_time_engagement_pct, 2) if overall_time_engagement_pct is not None else None,
            'total_intervention_count': grand_total_intervention_count,
            'total_interventions_per_100km': round(total_interventions_per_100km, 2) if total_interventions_per_100km is not None else None,
            'total_disengagement_count': grand_total_disengagement_count,
            'total_disengagements_per_100km': round(total_disengagements_per_100km, 2) if total_disengagements_per_100km is not None else None,
            'total_steer_intervention_count': grand_total_steer_intervention_count,
            'total_steer_interventions_per_100km': round(total_steer_interventions_per_100km, 2) if total_steer_interventions_per_100km is not None else None,
        }


repository = EngagementRepository()
