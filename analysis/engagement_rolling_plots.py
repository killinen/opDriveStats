#!/usr/bin/env python3
"""
Generate rolling-average engagement charts from engagement_db.json.

Charts:
  - Rolling overall engagement metrics versus cumulative OpenPilot engaged hours.
  - Rolling overall engagement metrics versus cumulative kilometres driven.
  - Rolling per-speed-bucket engagement ratios versus cumulative engaged hours.
  - Rolling per-speed-bucket engagement ratios versus cumulative kilometres.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt

NS_PER_HOUR = 3_600_000_000_000
DEFAULT_BUCKET_ORDER = ["city", "road", "highway"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot rolling engagement statistics versus usage time and distance.",
    )
    parser.add_argument(
        "--db",
        default="engagement_db.json",
        help="Path to engagement_db.json (default: %(default)s)",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=10.0,
        help="Rolling window size in engaged hours for time-axis plots (default: %(default)s)",
    )
    parser.add_argument(
        "--window-km",
        type=float,
        default=400.0,
        help="Rolling window size in kilometres for distance-axis plots (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        default="analysis/output",
        help="Directory for generated charts (default: %(default)s)",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display plots interactively after saving.",
    )
    return parser.parse_args()


def load_entries(path: Path) -> Tuple[List[Dict], List[str]]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("Expected a list of drive entries.")

    entries: List[Dict] = []
    cum_active_hours = 0.0
    cum_distance_km = 0.0

    bucket_keys = set()
    bucket_cum_hours: Dict[str, float] = defaultdict(float)
    bucket_cum_distance: Dict[str, float] = defaultdict(float)

    for row in sorted(data, key=lambda r: r["recorded_at"]):
        ts = datetime.strptime(row["recorded_at"], "%Y-%m-%dT%H:%M:%S")
        total_time_ns = float(row.get("total_time", 0.0) or 0.0)
        active_time_ns = float(row.get("active_time", 0.0) or 0.0)
        odo_km = float(row.get("odo_distance", 0.0) or 0.0)
        engaged_km = float(row.get("engaged_distance", 0.0) or 0.0)
        interventions = float(row.get("intervention_count", 0.0) or 0.0)
        steer_interventions = float(row.get("steer_intervention_count", 0.0) or 0.0)
        corrections = row.get("disengagement_corrections") or {}
        corrections_version = corrections.get("version")
        is_v2 = corrections_version == 2

        raw_disengagements = row.get("disengagement_count")
        if raw_disengagements is None:
            disengagements: Optional[float] = None
        else:
            disengagements = float(raw_disengagements or 0.0)

        adjusted_disengagements = disengagements
        removed_count = 0.0
        if disengagements is not None and not is_v2 and disengagements > 0.0:
            adjusted_disengagements = max(disengagements - 1.0, 0.0)
            removed_count = disengagements - adjusted_disengagements
        raw_steer_per_100 = row.get("steer_interventions_per_100km")
        if raw_steer_per_100 is None:
            steer_per_100 = (steer_interventions / odo_km * 100.0) if odo_km else math.nan
        else:
            steer_per_100 = float(raw_steer_per_100 or 0.0)
        if adjusted_disengagements is not None and odo_km > 0.0:
            diseng_per_100 = adjusted_disengagements / odo_km * 100.0
        else:
            diseng_per_100 = math.nan

        speed_buckets = row.get("speed_buckets") or {}
        bucket_metrics: Dict[str, Dict[str, float]] = {}
        for bucket_name, bucket in speed_buckets.items():
            bucket_keys.add(bucket_name)
            time_min = float(bucket.get("time_min", 0.0) or 0.0)
            engaged_time_min = float(bucket.get("engaged_time_min", 0.0) or 0.0)
            distance_km = float(bucket.get("distance_km", 0.0) or 0.0)
            engaged_distance_km = float(bucket.get("engaged_distance_km", 0.0) or 0.0)
            raw_bucket_disengagements = bucket.get("disengagement_count")
            if raw_bucket_disengagements is None:
                bucket_disengagements: Optional[float] = None
            else:
                bucket_disengagements = float(raw_bucket_disengagements or 0.0)

            bucket_metrics[bucket_name] = {
                "time_min": time_min,
                "engaged_time_min": engaged_time_min,
                "distance_km": distance_km,
                "engaged_distance_km": engaged_distance_km,
                "intervention_count": float(bucket.get("intervention_count", 0.0) or 0.0),
                "steer_intervention_count": float(bucket.get("steer_intervention_count", 0.0) or 0.0),
                "disengagement_count": bucket_disengagements,
                "interventions_per_100km": float(bucket.get("interventions_per_100km", 0.0) or 0.0) if bucket.get("interventions_per_100km") is not None else float("nan"),
                "steer_interventions_per_100km": float(bucket.get("steer_interventions_per_100km", 0.0) or 0.0) if bucket.get("steer_interventions_per_100km") is not None else float("nan"),
                "disengagements_per_100km": float(bucket.get("disengagements_per_100km", 0.0) or 0.0) if bucket.get("disengagements_per_100km") is not None else float("nan"),
            }

        # Ensure all known buckets are represented, even if missing from the current entry
        zero_template = {
            "time_min": 0.0,
            "engaged_time_min": 0.0,
            "distance_km": 0.0,
            "engaged_distance_km": 0.0,
            "intervention_count": 0.0,
            "steer_intervention_count": 0.0,
            "disengagement_count": None,
            "interventions_per_100km": float("nan"),
            "steer_interventions_per_100km": float("nan"),
            "disengagements_per_100km": float("nan"),
        }
        for bucket_name in bucket_keys:
            bucket_metrics.setdefault(bucket_name, zero_template.copy())

        if removed_count > 0.0 and not is_v2:
            non_null_buckets = [
                (name, metrics)
                for name, metrics in bucket_metrics.items()
                if metrics["disengagement_count"] is not None
            ]
            if non_null_buckets:
                target_bucket, metrics = max(
                    non_null_buckets,
                    key=lambda item: item[1]["disengagement_count"],
                )
                available = metrics["disengagement_count"]
                if available is not None:
                    metrics["disengagement_count"] = max(available - removed_count, 0.0)
        for metrics in bucket_metrics.values():
            distance = metrics["distance_km"]
            count = metrics["disengagement_count"]
            if count is None or distance <= 0.0:
                metrics["disengagements_per_100km"] = math.nan
            else:
                metrics["disengagements_per_100km"] = count / distance * 100.0

        bucket_ranges: Dict[str, Dict[str, float]] = {}
        for bucket_name in bucket_keys:
            metrics = bucket_metrics[bucket_name]
            engaged_hours = metrics["engaged_time_min"] / 60.0
            distance_km = metrics["distance_km"]
            start_hours = bucket_cum_hours[bucket_name]
            start_distance = bucket_cum_distance[bucket_name]
            bucket_cum_hours[bucket_name] += engaged_hours
            bucket_cum_distance[bucket_name] += distance_km
            bucket_ranges[bucket_name] = {
                "engaged_hours_start": start_hours,
                "engaged_hours_end": bucket_cum_hours[bucket_name],
                "distance_start": start_distance,
                "distance_end": bucket_cum_distance[bucket_name],
            }

        active_hours = active_time_ns / NS_PER_HOUR
        total_hours = total_time_ns / NS_PER_HOUR

        entry = {
            "ts": ts,
            "total_time_ns": total_time_ns,
            "active_time_ns": active_time_ns,
            "total_hours": total_hours,
            "active_hours": active_hours,
            "odo_km": odo_km,
            "engaged_km": engaged_km,
            "interventions": interventions,
            "steer_intervention_count": steer_interventions,
            "disengagement_count": adjusted_disengagements,
            "time_engagement": (active_time_ns / total_time_ns) if total_time_ns else math.nan,
            "dist_engagement": (engaged_km / odo_km) if odo_km else math.nan,
            "interventions_per_100km": (interventions / odo_km * 100.0) if odo_km else math.nan,
            "steer_interventions_per_100km": steer_per_100,
            "disengagements_per_100km": diseng_per_100,
            "speed_buckets": bucket_metrics,
            "bucket_ranges": bucket_ranges,
            "cum_active_start": cum_active_hours,
            "cum_distance_start": cum_distance_km,
        }

        cum_active_hours += active_hours
        cum_distance_km += odo_km
        entry["cum_active_end"] = cum_active_hours
        entry["cum_distance_end"] = cum_distance_km

        entries.append(entry)

    def _bucket_sort_key(name: str) -> tuple:
        if name in DEFAULT_BUCKET_ORDER:
            return (0, DEFAULT_BUCKET_ORDER.index(name))
        return (1, name)

    ordered_bucket_keys = sorted(bucket_keys, key=_bucket_sort_key)
    return entries, ordered_bucket_keys


def summarized_metrics(entries: List[Dict]) -> Dict[str, float]:
    total_time_ns = sum(e["total_time_ns"] for e in entries)
    active_time_ns = sum(e["active_time_ns"] for e in entries)
    odo_km = sum(e["odo_km"] for e in entries)
    engaged_km = sum(e["engaged_km"] for e in entries)
    interventions = sum(e["interventions"] for e in entries)

    metrics = {
        "total_drives": len(entries),
        "total_hours": total_time_ns / NS_PER_HOUR,
        "active_hours": active_time_ns / NS_PER_HOUR,
        "total_km": odo_km,
        "engaged_km": engaged_km,
    }
    metrics["time_engagement_pct"] = (
        active_time_ns / total_time_ns * 100.0 if total_time_ns else math.nan
    )
    metrics["dist_engagement_pct"] = (
        engaged_km / odo_km * 100.0 if odo_km else math.nan
    )
    metrics["interventions_per_100km"] = (
        interventions / odo_km * 100.0 if odo_km else math.nan
    )
    return metrics


def rolling_series(
    entries: List[Dict],
    bucket_keys: Iterable[str],
    axis: str,
    window: float,
) -> List[Dict]:
    if axis == "time":
        end_field = "cum_active_end"
        start_field = "cum_active_start"
        denominator_field = "active_hours"
    elif axis == "distance":
        end_field = "cum_distance_end"
        start_field = "cum_distance_start"
        denominator_field = "odo_km"
    else:
        raise ValueError(f"Unsupported axis {axis}")

    bucket_keys = list(bucket_keys)
    results: List[Dict] = []

    for i, entry in enumerate(entries):
        window_end = entry[end_field]
        window_start = max(0.0, window_end - window)
        totals = _empty_window_totals(bucket_keys)
        drive_count = 0

        for j in range(i, -1, -1):
            candidate = entries[j]
            seg_start = candidate[start_field]
            seg_end = candidate[end_field]
            axis_length = seg_end - seg_start
            if axis_length <= 0.0:
                continue

            overlap_start = max(seg_start, window_start)
            overlap_end = min(seg_end, window_end)
            overlap = overlap_end - overlap_start
            if overlap <= 0.0:
                if seg_start < window_start:
                    break
                continue

            denominator = candidate[denominator_field]
            if denominator <= 0.0:
                continue
            fraction = overlap / denominator
            _accumulate_segment(totals, candidate, bucket_keys, fraction)
            drive_count += 1
            if seg_start <= window_start:
                break

        snapshot = _finalise_window_totals(totals, bucket_keys)
        snapshot["axis_value"] = window_end
        snapshot["drive_count"] = drive_count

        bucket_axis: Dict[str, float] = {}
        bucket_ranges = entry.get("bucket_ranges", {})
        for key in bucket_keys:
            ranges = bucket_ranges.get(key)
            if not ranges:
                bucket_axis[key] = math.nan
            else:
                bucket_axis[key] = (
                    ranges["engaged_hours_end"]
                    if axis == "time"
                    else ranges["distance_end"]
                )
        snapshot["bucket_axis"] = bucket_axis
        results.append(snapshot)

    return results


def _empty_window_totals(bucket_keys: Iterable[str]) -> Dict:
    totals = {
        "total_time_ns": 0.0,
        "active_time_ns": 0.0,
        "odo_km": 0.0,
        "engaged_km": 0.0,
        "interventions": 0.0,
        "steer_interventions": 0.0,
        "disengagements": 0.0,
        "disengagement_distance": 0.0,
        "bucket": {},
    }
    for key in bucket_keys:
        totals["bucket"][key] = {
            "time_min": 0.0,
            "engaged_time_min": 0.0,
            "distance_km": 0.0,
            "engaged_distance_km": 0.0,
            "intervention_count": 0.0,
            "steer_intervention_count": 0.0,
            "disengagement_count": 0.0,
            "disengagement_distance": 0.0,
        }
    return totals


def _accumulate_segment(
    totals: Dict,
    entry: Dict,
    bucket_keys: Iterable[str],
    fraction: float,
) -> None:
    totals["total_time_ns"] += entry["total_time_ns"] * fraction
    totals["active_time_ns"] += entry["active_time_ns"] * fraction
    totals["odo_km"] += entry["odo_km"] * fraction
    totals["engaged_km"] += entry["engaged_km"] * fraction
    totals["interventions"] += entry["interventions"] * fraction
    totals["steer_interventions"] += entry.get("steer_intervention_count", 0.0) * fraction

    disengagement_count = entry.get("disengagement_count")
    if disengagement_count is not None:
        totals["disengagements"] += disengagement_count * fraction
        totals["disengagement_distance"] += entry["odo_km"] * fraction

    for key in bucket_keys:
        bucket = entry["speed_buckets"].get(key)
        if not bucket:
            continue
        dest = totals["bucket"][key]
        dest["time_min"] += bucket.get("time_min", 0.0) * fraction
        dest["engaged_time_min"] += bucket.get("engaged_time_min", 0.0) * fraction
        dest["distance_km"] += bucket.get("distance_km", 0.0) * fraction
        dest["engaged_distance_km"] += bucket.get("engaged_distance_km", 0.0) * fraction
        dest["intervention_count"] += bucket.get("intervention_count", 0.0) * fraction
        dest["steer_intervention_count"] += bucket.get("steer_intervention_count", 0.0) * fraction
        bucket_disengagements = bucket.get("disengagement_count")
        if bucket_disengagements is not None:
            dest["disengagement_count"] += bucket_disengagements * fraction
            dest["disengagement_distance"] += bucket.get("distance_km", 0.0) * fraction


def _finalise_window_totals(totals: Dict, bucket_keys: Iterable[str]) -> Dict:
    total_time_ns = max(totals["total_time_ns"], 0.0)
    active_time_ns = max(totals["active_time_ns"], 0.0)
    odo_km = max(totals["odo_km"], 0.0)
    engaged_km = max(totals["engaged_km"], 0.0)
    interventions = max(totals["interventions"], 0.0)
    steer_interventions = max(totals["steer_interventions"], 0.0)
    disengagements = max(totals["disengagements"], 0.0)
    disengagement_distance = max(totals["disengagement_distance"], 0.0)

    interventions_per_100 = (
        interventions / odo_km * 100.0 if odo_km > 0.0 else math.nan
    )
    steer_per_100 = (
        steer_interventions / odo_km * 100.0 if odo_km > 0.0 else math.nan
    )
    diseng_per_100 = (
        disengagements / disengagement_distance * 100.0
        if disengagement_distance > 0.0
        else math.nan
    )

    result = {
        "time_engagement": (active_time_ns / total_time_ns) if total_time_ns > 0.0 else math.nan,
        "dist_engagement": (engaged_km / odo_km) if odo_km > 0.0 else math.nan,
        "interventions_per_100km": interventions_per_100,
        "steer_interventions_per_100km": steer_per_100,
        "disengagements_per_100km": diseng_per_100,
        "intervention_count": interventions,
        "steer_intervention_count": steer_interventions,
        "disengagement_count": disengagements if disengagement_distance > 0.0 else math.nan,
        "bucket": {},
    }

    for key in bucket_keys:
        bucket = totals["bucket"][key]
        time_ratio = (
            bucket["engaged_time_min"] / bucket["time_min"] if bucket["time_min"] > 0.0 else math.nan
        )
        dist_ratio = (
            bucket["engaged_distance_km"] / bucket["distance_km"]
            if bucket["distance_km"] > 0.0
            else math.nan
        )
        distance = bucket["distance_km"]
        interventions_per_100 = (
            bucket["intervention_count"] / distance * 100.0 if distance > 0.0 else math.nan
        )
        steer_per_100 = (
            bucket["steer_intervention_count"] / distance * 100.0 if distance > 0.0 else math.nan
        )
        diseng_distance = bucket["disengagement_distance"]
        diseng_per_100 = (
            bucket["disengagement_count"] / diseng_distance * 100.0
            if diseng_distance > 0.0
            else math.nan
        )
        result["bucket"][key] = {
            "time_engagement": time_ratio,
            "dist_engagement": dist_ratio,
            "intervention_count": bucket["intervention_count"],
            "steer_intervention_count": bucket["steer_intervention_count"],
            "disengagement_count": bucket["disengagement_count"] if diseng_distance > 0.0 else math.nan,
            "interventions_per_100km": interventions_per_100,
            "steer_interventions_per_100km": steer_per_100,
            "disengagements_per_100km": diseng_per_100,
            "distance_km": distance,
        }
    return result


def _series_xy(series: List[Dict], key: str) -> Tuple[List[float], List[float]]:
    xs, ys = [], []
    for row in series:
        value = row.get(key)
        if value is None or math.isnan(value):
            continue
        xs.append(row["axis_value"])
        ys.append(value)
    return xs, ys


def _series_bucket_xy(series: List[Dict], bucket: str, key: str) -> Tuple[List[float], List[float]]:
    xs, ys = [], []
    for row in series:
        bucket_data = row["bucket"].get(bucket)
        if not bucket_data:
            continue
        value = bucket_data.get(key)
        if value is None or math.isnan(value):
            continue
        xs.append(row["axis_value"])
        ys.append(value)
    return xs, ys


def _series_bucket_xy_with_axis(series: List[Dict], bucket: str, key: str) -> Tuple[List[float], List[float]]:
    xs, ys = [], []
    for row in series:
        bucket_data = row["bucket"].get(bucket)
        axis_map = row.get("bucket_axis") or {}
        axis_value = axis_map.get(bucket)
        if not bucket_data or axis_value is None or math.isnan(axis_value):
            continue
        value = bucket_data.get(key)
        if value is None or math.isnan(value):
            continue
        xs.append(axis_value)
        ys.append(value)
    return xs, ys


def plot_overall(series: List[Dict], axis_label: str, output_path: Path, keep_open: bool) -> plt.Figure:
    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
    fig.suptitle(f"Rolling Engagement Metrics vs {axis_label}")

    xs, ys = _series_xy(series, "time_engagement")
    axes[0].plot(xs, [y * 100 for y in ys], color="#1f77b4")
    axes[0].set_ylabel("Time Engaged (%)")
    axes[0].grid(True, alpha=0.3)

    xs, ys = _series_xy(series, "dist_engagement")
    axes[1].plot(xs, [y * 100 for y in ys], color="#2ca02c")
    axes[1].set_ylabel("Distance Engaged (%)")
    axes[1].grid(True, alpha=0.3)

    xs, ys = _series_xy(series, "interventions_per_100km")
    axes[2].plot(xs, ys, color="#d62728", label="Interventions")
    xs, ys = _series_xy(series, "steer_interventions_per_100km")
    if xs:
        axes[2].plot(xs, ys, color="#9467bd", label="Steer Interventions")
    xs, ys = _series_xy(series, "disengagements_per_100km")
    if xs:
        axes[2].plot(xs, ys, color="#8c564b", label="Disengagements")
    axes[2].set_ylabel("Events / 100 km")
    axes[2].grid(True, alpha=0.3)
    axes[2].legend()
    axes[2].set_xlabel(axis_label)

    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.savefig(output_path, dpi=150)
    if not keep_open:
        plt.close(fig)
    return fig


def plot_buckets(
    series: List[Dict],
    bucket_keys: Iterable[str],
    axis_label: str,
    output_path: Path,
    keep_open: bool,
) -> Optional[plt.Figure]:
    bucket_keys = list(bucket_keys)
    if not bucket_keys:
        return None

    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    fig.suptitle(f"Rolling Engagement by Speed Bucket vs {axis_label}")

    for key in bucket_keys:
        xs, ys = _series_bucket_xy(series, key, "time_engagement")
        if xs:
            axes[0].plot(xs, [y * 100 for y in ys], label=key.title())

    axes[0].set_ylabel("Time Engaged (%)")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()

    for key in bucket_keys:
        xs, ys = _series_bucket_xy(series, key, "dist_engagement")
        if xs:
            axes[1].plot(xs, [y * 100 for y in ys], label=key.title())

    axes[1].set_ylabel("Distance Engaged (%)")
    axes[1].grid(True, alpha=0.3)
    axes[1].set_xlabel(axis_label)

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.savefig(output_path, dpi=150)
    if not keep_open:
        plt.close(fig)
    return fig


def plot_bucket_facets(
    series: List[Dict],
    bucket_keys: Iterable[str],
    axis_label: str,
    output_path: Path,
    keep_open: bool,
) -> Optional[plt.Figure]:
    bucket_keys = list(bucket_keys)
    if not bucket_keys:
        return None

    fig, axes = plt.subplots(len(bucket_keys), 1, figsize=(10, 3.0 * len(bucket_keys)), sharex=False)
    if len(bucket_keys) == 1:
        axes = [axes]
    fig.suptitle(f"Rolling Engagement by Speed Bucket vs {axis_label} (Bucket Axis)")

    for ax, key in zip(axes, bucket_keys):
        xs_time, ys_time = _series_bucket_xy_with_axis(series, key, "time_engagement")
        xs_dist, ys_dist = _series_bucket_xy_with_axis(series, key, "dist_engagement")
        label = key.title()
        if xs_time:
            ax.plot(xs_time, [y * 100 for y in ys_time], color="#1f77b4", label="Time %")
        if xs_dist:
            ax.plot(xs_dist, [y * 100 for y in ys_dist], color="#2ca02c", label="Distance %")
        ax.set_ylabel(f"{label}\nEngagement (%)")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="lower right")

    axes[-1].set_xlabel(axis_label)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.savefig(output_path, dpi=150)
    if not keep_open:
        plt.close(fig)
    return fig


def plot_bucket_events(
    series: List[Dict],
    bucket_keys: Iterable[str],
    axis_label: str,
    output_path: Path,
    keep_open: bool,
) -> Optional[plt.Figure]:
    bucket_keys = list(bucket_keys)
    if not bucket_keys:
        return None

    fig, axes = plt.subplots(len(bucket_keys), 1, figsize=(10, 3.0 * len(bucket_keys)), sharex=False)
    if len(bucket_keys) == 1:
        axes = [axes]
    fig.suptitle(f"Rolling Events by Speed Bucket vs {axis_label} (Bucket Axis)")

    for ax, key in zip(axes, bucket_keys):
        xs_int, ys_int = _series_bucket_xy_with_axis(series, key, "interventions_per_100km")
        xs_steer, ys_steer = _series_bucket_xy_with_axis(series, key, "steer_interventions_per_100km")
        xs_dis, ys_dis = _series_bucket_xy_with_axis(series, key, "disengagements_per_100km")
        label = key.title()
        if xs_int:
            ax.plot(xs_int, ys_int, color="#d62728", label="Interventions")
        if xs_steer:
            ax.plot(xs_steer, ys_steer, color="#9467bd", label="Steer Interventions")
        if xs_dis:
            ax.plot(xs_dis, ys_dis, color="#8c564b", label="Disengagements")
        ax.set_ylabel(f"{label}\nEvents / 100 km")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")

    axes[-1].set_xlabel(axis_label)
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.savefig(output_path, dpi=150)
    if not keep_open:
        plt.close(fig)
    return fig


def main() -> None:
    args = parse_args()
    path = Path(args.db)
    if not path.exists():
        raise FileNotFoundError(path)

    entries, bucket_keys = load_entries(path)
    summary = summarized_metrics(entries)
    print(
        f"Loaded {summary['total_drives']} drives covering "
        f"{summary['total_hours']:.1f} h total / {summary['active_hours']:.1f} h engaged "
        f"and {summary['total_km']:.1f} km."
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    time_series = rolling_series(entries, bucket_keys, axis="time", window=args.window_hours)
    distance_series = rolling_series(entries, bucket_keys, axis="distance", window=args.window_km)

    keep_open = args.show
    figs: List[plt.Figure] = []

    time_path = out_dir / "rolling_overall_vs_usage_hours.png"
    figs.append(plot_overall(time_series, "Cumulative engaged hours", time_path, keep_open))

    distance_path = out_dir / "rolling_overall_vs_distance.png"
    figs.append(plot_overall(distance_series, "Cumulative distance (km)", distance_path, keep_open))

    time_bucket_path = out_dir / "rolling_buckets_vs_usage_hours.png"
    fig = plot_buckets(time_series, bucket_keys, "Cumulative engaged hours", time_bucket_path, keep_open)
    if fig:
        figs.append(fig)

    distance_bucket_path = out_dir / "rolling_buckets_vs_distance.png"
    fig = plot_buckets(distance_series, bucket_keys, "Cumulative distance (km)", distance_bucket_path, keep_open)
    if fig:
        figs.append(fig)

    time_bucket_facet_path = out_dir / "rolling_bucket_facets_vs_usage_hours.png"
    fig = plot_bucket_facets(time_series, bucket_keys, "Bucket cumulative engaged hours", time_bucket_facet_path, keep_open)
    if fig:
        figs.append(fig)

    distance_bucket_facet_path = out_dir / "rolling_bucket_facets_vs_distance.png"
    fig = plot_bucket_facets(distance_series, bucket_keys, "Bucket cumulative distance (km)", distance_bucket_facet_path, keep_open)
    if fig:
        figs.append(fig)

    distance_events_path = out_dir / "rolling_bucket_events_vs_distance.png"
    fig = plot_bucket_events(distance_series, bucket_keys, "Bucket cumulative distance (km)", distance_events_path, keep_open)
    if fig:
        figs.append(fig)

    print("Saved charts:")
    print(f"  {time_path}")
    print(f"  {distance_path}")
    print(f"  {time_bucket_path}")
    print(f"  {distance_bucket_path}")
    print(f"  {time_bucket_facet_path}")
    print(f"  {distance_bucket_facet_path}")
    print(f"  {distance_events_path}")

    if args.show and figs:
        for fig in figs:
            fig.canvas.draw_idle()
        plt.show()


if __name__ == "__main__":
    main()
