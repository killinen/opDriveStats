#!/usr/bin/env python3
"""
Generate engagement trend statistics from engagement_db.json.

The script aggregates per-drive records into daily, monthly, and distance-bucket
views, computes simple linear trends, and prints an at-a-glance summary that
helps answer whether engagement (time/distance) and interventions per 100 km
are improving or degrading over time or kilometres driven.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from math import isnan
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

NS_PER_HOUR = 3_600_000_000_000


@dataclass
class DriveEntry:
    recorded_at: datetime
    total_time_ns: float
    active_time_ns: float
    odo_km: float
    engaged_km: float
    interventions: float


@dataclass
class AggregatedRow:
    label: str
    time_engagement: float
    dist_engagement: float
    interventions_per_100km: float
    total_time_hours: float
    odo_km: float
    start: date
    end: date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute engagement trend statistics.",
    )
    parser.add_argument(
        "--db",
        default="engagement_db.json",
        help="Path to engagement_db.json (default: %(default)s)",
    )
    parser.add_argument(
        "--distance-bin-km",
        type=float,
        default=500.0,
        help="Bucket size in kilometres for distance-based aggregation (default: %(default)s)",
    )
    return parser.parse_args()


def load_entries(path: Path) -> List[DriveEntry]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("Expected a list of drive entries in engagement_db.json")

    entries: List[DriveEntry] = []
    for row in data:
        ts = datetime.strptime(row["recorded_at"], "%Y-%m-%dT%H:%M:%S")
        entries.append(
            DriveEntry(
                recorded_at=ts,
                total_time_ns=float(row.get("total_time", 0.0) or 0.0),
                active_time_ns=float(row.get("active_time", 0.0) or 0.0),
                odo_km=float(row.get("odo_distance", 0.0) or 0.0),
                engaged_km=float(row.get("engaged_distance", 0.0) or 0.0),
                interventions=float(row.get("intervention_count", 0.0) or 0.0),
            )
        )

    entries.sort(key=lambda e: e.recorded_at)
    return entries


def _safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


def aggregate_daily(entries: Sequence[DriveEntry]) -> List[AggregatedRow]:
    buckets: Dict[date, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for entry in entries:
        bucket = buckets[entry.recorded_at.date()]
        bucket["total_time_ns"] += entry.total_time_ns
        bucket["active_time_ns"] += entry.active_time_ns
        bucket["odo_km"] += entry.odo_km
        bucket["engaged_km"] += entry.engaged_km
        bucket["interventions"] += entry.interventions
        bucket.setdefault("start", entry.recorded_at.date())
        bucket["end"] = entry.recorded_at.date()

    rows: List[AggregatedRow] = []
    for day in sorted(buckets.keys()):
        bucket = buckets[day]
        time_ratio = _safe_ratio(bucket["active_time_ns"], bucket["total_time_ns"])
        dist_ratio = _safe_ratio(bucket["engaged_km"], bucket["odo_km"])
        interventions = _safe_ratio(bucket["interventions"], bucket["odo_km"])
        if time_ratio is None or dist_ratio is None or interventions is None:
            continue
        rows.append(
            AggregatedRow(
                label=str(day),
                time_engagement=time_ratio,
                dist_engagement=dist_ratio,
                interventions_per_100km=interventions * 100.0,
                total_time_hours=bucket["total_time_ns"] / NS_PER_HOUR,
                odo_km=bucket["odo_km"],
                start=bucket["start"],
                end=bucket["end"],
            )
        )
    return rows


def aggregate_monthly(entries: Sequence[DriveEntry]) -> List[AggregatedRow]:
    buckets: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for entry in entries:
        key = entry.recorded_at.strftime("%Y-%m")
        bucket = buckets[key]
        bucket["total_time_ns"] += entry.total_time_ns
        bucket["active_time_ns"] += entry.active_time_ns
        bucket["odo_km"] += entry.odo_km
        bucket["engaged_km"] += entry.engaged_km
        bucket["interventions"] += entry.interventions
        bucket.setdefault("start", entry.recorded_at.date())
        bucket["end"] = entry.recorded_at.date()

    rows: List[AggregatedRow] = []
    for key in sorted(buckets.keys()):
        bucket = buckets[key]
        time_ratio = _safe_ratio(bucket["active_time_ns"], bucket["total_time_ns"])
        dist_ratio = _safe_ratio(bucket["engaged_km"], bucket["odo_km"])
        interventions = _safe_ratio(bucket["interventions"], bucket["odo_km"])
        if time_ratio is None or dist_ratio is None or interventions is None:
            continue
        rows.append(
            AggregatedRow(
                label=key,
                time_engagement=time_ratio,
                dist_engagement=dist_ratio,
                interventions_per_100km=interventions * 100.0,
                total_time_hours=bucket["total_time_ns"] / NS_PER_HOUR,
                odo_km=bucket["odo_km"],
                start=bucket["start"],
                end=bucket["end"],
            )
        )
    return rows


def aggregate_by_distance(
    entries: Sequence[DriveEntry],
    bin_km: float,
) -> List[AggregatedRow]:
    if bin_km <= 0:
        raise ValueError("Distance bin size must be positive")

    rows: List[AggregatedRow] = []
    cumulative_km = 0.0
    next_cutoff = bin_km
    bucket: Dict[str, float] = defaultdict(float)
    bucket["start"] = entries[0].recorded_at.date() if entries else None

    for entry in entries:
        remaining_dist = entry.odo_km
        share_active = entry.active_time_ns
        share_total = entry.total_time_ns
        share_engaged = entry.engaged_km
        share_interventions = entry.interventions
        while remaining_dist > 0:
            space = next_cutoff - cumulative_km
            if remaining_dist <= space + 1e-9:
                # Entire remainder fits in the current bucket.
                bucket["odo_km"] += remaining_dist
                bucket["active_time_ns"] += share_active
                bucket["total_time_ns"] += share_total
                bucket["engaged_km"] += share_engaged
                bucket["interventions"] += share_interventions
                bucket["end"] = entry.recorded_at.date()
                cumulative_km += remaining_dist
                remaining_dist = 0.0
            else:
                # Fill up the current bucket with a proportional share.
                if remaining_dist > 0:
                    frac = space / remaining_dist
                else:
                    frac = 0.0
                bucket["odo_km"] += space
                bucket["active_time_ns"] += share_active * frac
                bucket["total_time_ns"] += share_total * frac
                bucket["engaged_km"] += share_engaged * frac
                bucket["interventions"] += share_interventions * frac
                bucket["end"] = entry.recorded_at.date()
                rows.append(_finalise_distance_bucket(bucket, next_cutoff))
                # Reset bucket for the next interval.
                cumulative_km += space
                bucket = defaultdict(float)
                bucket["start"] = entry.recorded_at.date()
                remaining_dist -= space
                share_active *= 1 - frac
                share_total *= 1 - frac
                share_engaged *= 1 - frac
                share_interventions *= 1 - frac
                next_cutoff += bin_km

    if bucket.get("odo_km", 0.0) > 0:
        rows.append(_finalise_distance_bucket(bucket, next_cutoff))

    return rows


def _finalise_distance_bucket(bucket: Dict[str, float], cutoff: float) -> AggregatedRow:
    time_ratio = _safe_ratio(bucket["active_time_ns"], bucket["total_time_ns"])
    dist_ratio = _safe_ratio(bucket["engaged_km"], bucket["odo_km"])
    interventions = _safe_ratio(bucket["interventions"], bucket["odo_km"])
    label = f"≤ {int(round(cutoff))} km"
    return AggregatedRow(
        label=label,
        time_engagement=time_ratio or 0.0,
        dist_engagement=dist_ratio or 0.0,
        interventions_per_100km=(interventions or 0.0) * 100.0,
        total_time_hours=bucket["total_time_ns"] / NS_PER_HOUR,
        odo_km=bucket["odo_km"],
        start=bucket["start"],
        end=bucket.get("end", bucket["start"]),
    )


def linear_trend(
    xs: Sequence[float],
    ys: Sequence[float],
    weights: Optional[Sequence[float]] = None,
) -> Tuple[float, float, float]:
    if len(xs) != len(ys):
        raise ValueError("xs and ys must be the same length")
    if len(xs) < 2:
        return float("nan"), float("nan"), float("nan")

    if weights is None:
        weights = [1.0] * len(xs)

    sum_w = sum(weights)
    if sum_w == 0:
        return float("nan"), float("nan"), float("nan")

    mean_x = sum(w * x for w, x in zip(weights, xs)) / sum_w
    mean_y = sum(w * y for w, y in zip(weights, ys)) / sum_w

    cov = sum(w * (x - mean_x) * (y - mean_y) for w, x, y in zip(weights, xs, ys))
    var = sum(w * (x - mean_x) ** 2 for w, x in zip(weights, xs))
    if abs(var) < 1e-12:
        return float("nan"), float("nan"), float("nan")

    slope = cov / var
    intercept = mean_y - slope * mean_x
    ss_tot = sum(w * (y - mean_y) ** 2 for w, y in zip(weights, ys))
    ss_res = sum(
        w * (y - (slope * x + intercept)) ** 2 for w, x, y in zip(weights, xs, ys)
    )
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return slope, intercept, r2


def summarise_trend(
    rows: Sequence[AggregatedRow],
    metric: str,
    axis: str,
) -> Tuple[float, float, float]:
    if metric not in {"time_engagement", "dist_engagement", "interventions_per_100km"}:
        raise ValueError(f"Unsupported metric: {metric}")

    ys = [getattr(r, metric) for r in rows]
    if axis == "day":
        xs = list(range(len(rows)))
    elif axis == "km":
        cumulative = 0.0
        xs = []
        for row in rows:
            cumulative += row.odo_km
            xs.append(cumulative)
    else:
        raise ValueError(f"Unsupported axis: {axis}")

    weights = None
    if metric in {"time_engagement", "dist_engagement"}:
        weights = [r.total_time_hours for r in rows]
    elif metric == "interventions_per_100km":
        weights = [r.odo_km for r in rows]

    return linear_trend(xs, ys, weights)


def format_percentage(value: float) -> str:
    return f"{value * 100:.1f}%" if not isnan(value) else "—"


def format_rate(value: float) -> str:
    return f"{value:.2f}" if not isnan(value) else "—"


def print_table(
    title: str,
    rows: Sequence[AggregatedRow],
) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    header = (
        f"{'Bucket':<14} {'Time Eng%':>10} {'Dist Eng%':>11} "
        f"{'Intv/100km':>12} {'Hours':>9} {'Km':>9} {'Span':>17}"
    )
    print(header)
    for row in rows:
        span = (
            row.start if row.start == row.end else f"{row.start} → {row.end}"
        )
        print(
            f"{row.label:<14} "
            f"{format_percentage(row.time_engagement):>10} "
            f"{format_percentage(row.dist_engagement):>11} "
            f"{format_rate(row.interventions_per_100km):>12} "
            f"{row.total_time_hours:9.1f} {row.odo_km:9.1f} {span!s:>17}"
        )


def print_trend(name: str, slope: float, intercept: float, r2: float, axis: str) -> None:
    if axis == "day":
        period = "per day"
    elif axis == "km":
        period = "per km"
    else:
        period = f"per {axis}"
    print(
        f"{name:<28} slope={slope:+.4f} {period:<7}  intercept={intercept:.3f}  R²={r2:.3f}"
    )


def main() -> None:
    args = parse_args()
    path = Path(args.db)
    if not path.exists():
        raise FileNotFoundError(path)

    entries = load_entries(path)
    if not entries:
        raise SystemExit("No drive entries found.")

    print(f"Loaded {len(entries)} drives spanning {entries[0].recorded_at} → {entries[-1].recorded_at}")

    daily_rows = aggregate_daily(entries)
    monthly_rows = aggregate_monthly(entries)
    distance_rows = aggregate_by_distance(entries, args.distance_bin_km)

    print_table("Daily Aggregation", daily_rows[:7] + daily_rows[-7:])
    print_table("Monthly Aggregation", monthly_rows)
    print_table("Distance Aggregation", distance_rows)

    print("\nLinear Trends (time axis)")
    for metric in ("time_engagement", "dist_engagement", "interventions_per_100km"):
        slope, intercept, r2 = summarise_trend(daily_rows, metric, axis="day")
        print_trend(metric, slope, intercept, r2, axis="day")

    print("\nLinear Trends (distance axis)")
    for metric in ("time_engagement", "dist_engagement", "interventions_per_100km"):
        slope, intercept, r2 = summarise_trend(distance_rows, metric, axis="km")
        print_trend(metric, slope, intercept, r2, axis="km")

    # Baseline vs recent comparison
    window = min(14, len(daily_rows) // 3 or 1)
    baseline = daily_rows[:window]
    recent = daily_rows[-window:]

    def avg(rows: Sequence[AggregatedRow], attr: str) -> float:
        values = [getattr(r, attr) for r in rows]
        weights = [r.total_time_hours if attr != "interventions_per_100km" else r.odo_km for r in rows]
        total_weight = sum(weights)
        if total_weight == 0:
            return float("nan")
        return sum(v * w for v, w in zip(values, weights)) / total_weight

    print("\nBaseline vs Recent (weighted averages)")
    for metric in ("time_engagement", "dist_engagement", "interventions_per_100km"):
        base_avg = avg(baseline, metric)
        recent_avg = avg(recent, metric)
        delta = recent_avg - base_avg
        if metric == "interventions_per_100km":
            print(
                f"{metric:<28} {base_avg:.2f} → {recent_avg:.2f}  Δ={delta:+.2f}"
            )
        else:
            print(
                f"{metric:<28} {base_avg * 100:.1f}% → {recent_avg * 100:.1f}%  Δ={delta * 100:+.1f} pp"
            )


if __name__ == "__main__":
    main()
