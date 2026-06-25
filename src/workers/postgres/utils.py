"""Datetime helpers for the Postgres worker.

Provides timezone normalisation to Asia/Ho_Chi_Minh and ISO-8601 parsing so
that timestamps consumed from the message stream are stored consistently.
"""

from datetime import datetime, timedelta, timezone

HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


def to_local_time(value: datetime) -> datetime:
    """Return ``value`` expressed in the Asia/Ho_Chi_Minh timezone.

    Naive datetimes are assumed to already be local time and are simply tagged
    with the local timezone; aware datetimes are converted.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 string (accepting a trailing ``Z``) into local time."""
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return to_local_time(timestamp)
