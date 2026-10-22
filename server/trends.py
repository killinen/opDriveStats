"""Weekly observations of weighted, trailing-window speed-bucket performance."""

from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Optional

from .data import SPEED_BUCKETS
from .drive_dates import resolve_drive_date


METRICS = {
    'distance': {'label': 'Distance engagement', 'unit': '%', 'higher_is_better': True},
    'time': {'label': 'Time engagement', 'unit': '%', 'higher_is_better': True},
    'disengagements': {'label': 'Disengagements', 'unit': '/100 km', 'higher_is_better': False},
    'steering': {'label': 'Steering interventions', 'unit': '/100 km', 'higher_is_better': False},
}
LOW_DISTANCE_KM = 50


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if isfinite(value) and value >= 0 else None
    except (ValueError, TypeError):
        return None


def _aggregate(dated_rows, key, metric, start, end):
    numerator = denominator = distance = hours = 0.0
    count = estimated = 0
    for timestamp, is_estimated, row in dated_rows:
        if not start <= timestamp < end:
            continue
        bucket = (row.get('speed_buckets') or {}).get(key) or {}
        km = _number(bucket.get('distance_km_raw'))
        time = _number(bucket.get('time_ns'))
        if metric == 'distance':
            top, bottom = _number(bucket.get('engaged_distance_km_raw')), km
        elif metric == 'time':
            top, bottom = _number(bucket.get('engaged_time_ns')), time
        else:
            field = 'disengagement_count' if metric == 'disengagements' else 'steer_intervention_count'
            top, bottom = _number(bucket.get(field)), km
            # Match the existing steering metric's historical start date.
            if metric == 'steering' and not is_estimated and timestamp < datetime(2025, 7, 7, tzinfo=timezone.utc):
                continue
        if top is None or bottom is None or bottom <= 0:
            continue
        numerator += top
        denominator += bottom
        distance += km or 0
        hours += (time or 0) / 3_600_000_000_000
        count += 1
        estimated += int(is_estimated)
    return {
        'value': round(numerator / denominator * 100, 3) if denominator > 0 else None,
        'distance_km': round(distance, 2),
        'hours': round(hours, 2),
        'drive_count': count,
        'estimated_drive_count': estimated,
        'low_distance': count > 0 and distance < LOW_DISTANCE_KM,
    }


def build_trends(
    entries: list[dict[str, Any]], *, device_ids: Optional[list[str]] = None,
    branches: Optional[list[str]] = None, window_weeks: int = 4,
    history_weeks: int = 26, metric: str = 'distance', include_estimates: bool = True,
    device_view: str = 'individual',
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    metric = metric if metric in METRICS else 'distance'
    window_weeks = window_weeks if window_weeks in (4, 8, 12) else 4
    history_weeks = history_weeks if history_weeks in (13, 26, 52, 0) else 26
    device_view = device_view if device_view in ('individual', 'combined') else 'individual'
    devices = sorted({row.get('device_id') or 'unknown' for row in entries})
    branch_options = sorted({row.get('git_branch') or 'unknown' for row in entries})
    selected_devices = list(dict.fromkeys(value for value in (device_ids or []) if value in devices))
    selected_branches = [value for value in (branches or []) if value in branch_options]
    candidates = [(resolve_drive_date(row, now=now), row) for row in entries
                  if not selected_branches or (row.get('git_branch') or 'unknown') in selected_branches]
    if not selected_devices:
        usable = [(date.timestamp, row.get('device_id') or 'unknown') for date, row in candidates
                  if date.timestamp is not None and date.timestamp <= now
                  and (include_estimates or not date.is_estimated)]
        if usable:
            selected_devices = [max(usable)[1]]
        elif candidates:
            selected_devices = [min(row.get('device_id') or 'unknown' for _, row in candidates)]
        elif devices:
            selected_devices = [devices[0]]
    filtered = [(date, row) for date, row in candidates
                if (row.get('device_id') or 'unknown') in selected_devices]
    dated = []
    undated_count = estimated_count = 0
    for resolved, row in filtered:
        if resolved.timestamp is None:
            undated_count += 1
        elif resolved.is_estimated:
            estimated_count += 1
            if include_estimates:
                dated.append((resolved.timestamp, True, row))
        else:
            dated.append((resolved.timestamp, False, row))
    dated.sort(key=lambda item: item[0])

    # Historical points end on Sundays. The final point ends today and is
    # explicitly partial; every observation still covers exactly N trailing days.
    today = now.date()
    monday = today - timedelta(days=today.weekday())
    if history_weeks:
        first_week = monday - timedelta(weeks=history_weeks - 1)
    else:
        first_date = min(dated[0][0].date(), today) if dated else today
        first_week = first_date - timedelta(days=first_date.weekday())
    ends = []
    week = first_week
    while week <= monday:
        ends.append(min(week + timedelta(days=6), today))
        week += timedelta(weeks=1)

    series = []
    groups = selected_devices if device_view == 'individual' and selected_devices else [None]
    for index, device_id in enumerate(groups):
        device_rows = [item for item in dated if (item[2].get('device_id') or 'unknown') == device_id] if device_id is not None else dated
        device_series = _build_series(device_rows, ends, now, window_weeks, metric)
        # Use the full device list so colors stay consistent when filters change.
        color_index = devices.index(device_id) if device_id is not None else 0
        color = f'hsl({(260 + color_index * 137.508) % 360:.1f} 72% 72%)'
        for item in device_series:
            item.update(device_id=device_id, chart_id=f'device-{index}-{item["key"]}', color=color)
        series.extend(device_series)
    return {
        'devices': devices, 'branches': branch_options,
        'selected_devices': selected_devices, 'selected_branches': selected_branches,
        'device_view': device_view,
        'window_weeks': window_weeks, 'history_weeks': history_weeks,
        'metric': metric, 'metric_info': METRICS[metric], 'metrics': METRICS,
        'include_estimates': include_estimates, 'series': series,
        'coverage': {
            'total_drives': len(filtered), 'dated_drives': len(dated),
            'recording_date_drives': len(filtered) - undated_count - estimated_count,
            'estimated_drives': estimated_count, 'undated_drives': undated_count,
        },
        'low_distance_km': LOW_DISTANCE_KM,
        'as_of': now.isoformat(), 'first_date': ends[0].isoformat(),
        'last_date': ends[-1].isoformat(),
    }


def _build_series(dated, ends, now, window_weeks, metric):
    today = now.date()
    series = []
    for bucket in SPEED_BUCKETS:
        points = []
        for day in ends:
            end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            # The current day's interval ends at now, so slightly future clock
            # values admitted by clock tolerance cannot leak into the chart.
            end = min(end, now)
            start = datetime.combine(day - timedelta(days=window_weeks * 7 - 1), datetime.min.time(), tzinfo=timezone.utc)
            week_start = datetime.combine(day - timedelta(days=day.weekday()), datetime.min.time(), tzinfo=timezone.utc)
            value = _aggregate(dated, bucket['key'], metric, start, end)
            volume = _aggregate(dated, bucket['key'], metric, week_start, end)
            points.append({
                'date': day.isoformat(), 'window_start': start.date().isoformat(),
                'window_end': day.isoformat(), 'partial_week': day == today,
                'weekly_distance_km': volume['distance_km'], **value,
            })
        latest = points[-1]
        previous_end = datetime.combine(today - timedelta(days=window_weeks * 7 - 1), datetime.min.time(), tzinfo=timezone.utc)
        previous = _aggregate(dated, bucket['key'], metric, previous_end - timedelta(weeks=window_weeks), previous_end)
        delta = latest['value'] - previous['value'] if latest['value'] is not None and previous['value'] is not None else None
        series.append({
            'key': bucket['key'], 'label': bucket['label'], 'points': points,
            'latest': latest, 'previous': previous,
            'delta': round(delta, 2) if delta is not None else None,
        })
    return series
