"""Choose a timeline date without treating a device clock as authoritative."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


EARLIEST_DRIVE_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)
CLOCK_TOLERANCE = timedelta(days=1)


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Accept ISO timestamps and legacy drive names; normalize to UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        timestamp = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    except ValueError:
        try:
            timestamp = datetime.strptime(value, '%Y-%m-%d--%H-%M-%S')
        except ValueError:
            return None
    try:
        if timestamp.tzinfo is None:
            return timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _commit_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip().strip("'\"")
    if not value.strip():
        return None
    timestamp = parse_timestamp(value)
    if timestamp is not None:
        return timestamp
    # On-device metadata uses git's "%ct %ci", sometimes wrapped in quotes.
    try:
        return datetime.fromtimestamp(int(value.split()[0]), tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


@dataclass(frozen=True)
class DriveDate:
    timestamp: Optional[datetime]
    source: Optional[str]

    @property
    def is_estimated(self) -> bool:
        return self.source == 'uploaded_at'


def resolve_drive_date(
    row: Mapping[str, Any], *, now: Optional[datetime] = None
) -> DriveDate:
    """Prefer plausible recording dates, then the earliest retained server receipt.

    Old uploads remain valid: age alone cannot distinguish an archive from a bad
    clock. A recording predating its software commit, or beyond server time, is
    rejected with one day's tolerance. Device-generated processing dates are
    deliberately not used as substitutes for a server receipt.
    """
    now = now or datetime.now(timezone.utc)
    uploaded_at = parse_timestamp(row.get('uploaded_at'))
    if uploaded_at is not None and not (
        EARLIEST_DRIVE_DATE <= uploaded_at <= now + CLOCK_TOLERANCE
    ):
        uploaded_at = None
    latest = min(uploaded_at, now) if uploaded_at is not None else now
    earliest = EARLIEST_DRIVE_DATE
    commit = _commit_timestamp(row.get('git_commit_date'))
    if commit is not None and EARLIEST_DRIVE_DATE <= commit <= latest + CLOCK_TOLERANCE:
        earliest = max(earliest, commit - CLOCK_TOLERANCE)

    for source in ('recorded_at', 'drive'):
        timestamp = parse_timestamp(row.get(source))
        if timestamp is not None and earliest <= timestamp <= latest + CLOCK_TOLERANCE:
            return DriveDate(timestamp, source)
    return DriveDate(uploaded_at, 'uploaded_at' if uploaded_at is not None else None)
