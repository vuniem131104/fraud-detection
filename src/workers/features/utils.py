"""Helper utilities for the Redis features-refresh worker.

Collects the shared building blocks used when maintaining the online feature
store: the Asia/Ho_Chi_Minh timezone and datetime helpers, Redis key builders
for the per-user-card transaction set and feature hash, the Lua
:data:`REFRESH_KEY_SCRIPT` that atomically updates the rolling transaction
window, a Redis value decoder, and a transaction-normalisation helper.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

REFRESH_KEY_SCRIPT = """
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local remaining = redis.call('ZCARD', KEYS[1])
local card_created_at = redis.call('HGET', KEYS[2], 'card_created_at') or false
return {removed, remaining, card_created_at}
"""


def local_now() -> datetime:
    """Return the current time in the Asia/Ho_Chi_Minh timezone."""
    return datetime.now(HO_CHI_MINH_TZ)


def to_local_time(value: datetime) -> datetime:
    """Convert a datetime to the Asia/Ho_Chi_Minh timezone.

    Naive datetimes are assumed to already be in local time and are simply
    tagged with the timezone; aware datetimes are converted.

    Args:
        value: The datetime to localise.

    Returns:
        The datetime expressed in the Asia/Ho_Chi_Minh timezone.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def iso_local(value: datetime) -> str:
    """Return the ISO-8601 string of ``value`` in local (HCM) time."""
    return to_local_time(value).isoformat()


def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 string into a local (HCM) datetime.

    Accepts a trailing ``Z`` UTC designator and returns the result converted to
    the Asia/Ho_Chi_Minh timezone.

    Args:
        value: ISO-8601 datetime string.

    Returns:
        The parsed datetime in the Asia/Ho_Chi_Minh timezone.
    """
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return to_local_time(timestamp)


def days_between(start: datetime, end: datetime) -> float:
    """Return the number of days from ``start`` to ``end``, clamped at zero.

    Args:
        start: The earlier datetime.
        end: The later datetime.

    Returns:
        The elapsed time in fractional days, never negative.
    """
    elapsed_days = (end - start).total_seconds() / (24 * 60 * 60)
    return max(elapsed_days, 0.0)


def build_transactions_key(user_id: str, card_id: str) -> str:
    """Return the Redis key for a user/card's recent-transactions sorted set."""
    return f"user:card:transactions:{user_id}_{card_id}"


def build_features_key(user_id: str, card_id: str) -> str:
    """Return the Redis key for a user/card's derived-feature hash."""
    return f"user:card:features:{user_id}_{card_id}"


def decode_redis_value(value: Any) -> Any:
    """Decode a Redis ``bytes`` value to ``str``, passing other types through.

    Args:
        value: A value returned by Redis.

    Returns:
        The UTF-8 decoded string if ``value`` is ``bytes``, otherwise ``value``
        unchanged.
    """
    if isinstance(value, bytes):
        return value.decode()

    return value


def normalize_email_domains(transaction: dict[str, Any]) -> dict[str, Any]:
    """Return a cleaned copy of a transaction for feature storage.

    Drops request/response metadata fields (``request_id``, ``probability``,
    ``status``, ``latency``) and reduces the purchaser/recipient email fields to
    their lowercased domain part. The input dict is not mutated.

    Args:
        transaction: The raw transaction dict.

    Returns:
        A new dict with metadata removed and email fields normalised to their
        domains.
    """
    normalized_transaction = transaction.copy()
    for field in ("request_id", "probability", "status", "latency"):
        normalized_transaction.pop(field, None)

    for field in ("email_purchaser", "email_recipient"):
        email = normalized_transaction.get(field)
        if isinstance(email, str):
            normalized_transaction[field] = email.strip().lower().split("@")[-1]

    return normalized_transaction
