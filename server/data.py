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


    def cli_summary(self, include_device_columns: bool = False) -> str:
        header = (
            f"{'Date/Time':<20} {'Duration':<11} {'DriveDur':<11} {'Distance':<9} {'Engaged':<9} "
            f"{'Time%':<7} {'Drive%':<7} {'ODO%':<7} {'Diseng':<6} {'DIS/100km':<9} "
            f"{'Steer':<6} {'ST/100km':<8} {'Press_s/h':<10}"
        )
        if include_device_columns:
            header += f" {'OPLong':<7} {'Version':<14} {'Branch':<16} {'Car':<20} {'Device':<12}"

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

                duration_minutes = total_time_ns / 1e9 / 60
                drive_duration_minutes = drive_time_ns / 1e9 / 60
                time_pct = (active_time_ns / total_time_ns * 100) if total_time_ns else 0
                drive_pct = (
                    drive.get('drive_time_engagement_pct')
                    if drive.get('drive_time_engagement_pct') is not None
                    else (drive_active_time_ns / drive_time_ns * 100 if drive_time_ns else None)
                )
                odo_pct = drive.get('engagement_pct_odo')
                diseng_per_100km = drive.get('interventions_per_100km')
                steer_per_100km = drive.get('steer_interventions_per_100km')
                press_seconds_per_hour = drive.get('cruise_press_seconds_per_hour')

                drive_display = drive_name.replace('--', ' ').replace('-', '/')[:20]
                row = (
                    f"{drive_display:<20} {duration_minutes:>6.1f}min {drive_duration_minutes:>6.1f}min "
                    f"{distance_km:>6.1f}km {engaged_distance_km:>7.1f}km "
                    f"{time_pct:>5.1f}% {(_format_pct(drive_pct, 1) if drive_pct is not None else 'N/A'):>7} "
                    f"{(_format_pct(odo_pct, 1) if odo_pct is not None else 'N/A'):>7} "
                    f"{disengagements:>6} {diseng_per_100km or 0:>9.1f} "
                    f"{steer_interventions:>6} {steer_per_100km or 0:>8.1f} "
                    f"{press_seconds_per_hour or 0:>10.2f}"
                )

                if include_device_columns:
                    opl = drive.get('openpilot_longitudinal')
                    if opl is True:
                        opl_display = 'ON'
                    elif opl is False:
                        opl_display = 'OFF'
                    else:
                        opl_display = '—'

                    version_value = drive.get('version') or '—'
                    if len(version_value) > 14:
                        version_value = version_value[:13] + '…'
                    branch_value = drive.get('git_branch') or '—'
                    if len(branch_value) > 16:
                        branch_value = branch_value[:15] + '…'
                    car_value = drive.get('car_fingerprint') or '—'
                    if len(car_value) > 20:
                        car_value = car_value[:19] + '…'
                    device_value = drive.get('device_type') or '—'
                    if len(device_value) > 12:
                        device_value = device_value[:11] + '…'

                    row += (
                        f" {opl_display:<7} {version_value:<14} {branch_value:<16} {car_value:<20} {device_value:<12}"
                    )

                lines.append(row)

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
                (total_steer_interventions / total_distance * 100) if total_distance else 0.0
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

repository = EngagementRepository()
