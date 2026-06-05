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
    return datetime.now(HO_CHI_MINH_TZ)


def to_local_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def iso_local(value: datetime) -> str:
    return to_local_time(value).isoformat()


def parse_datetime(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return to_local_time(timestamp)


def days_between(start: datetime, end: datetime) -> float:
    elapsed_days = (end - start).total_seconds() / (24 * 60 * 60)
    return max(elapsed_days, 0.0)


def build_transactions_key(user_id: str, card_id: str) -> str:
    return f"user:card:transactions:{user_id}_{card_id}"


def build_features_key(user_id: str, card_id: str) -> str:
    return f"user:card:features:{user_id}_{card_id}"


def decode_redis_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()

    return value


def normalize_email_domains(transaction: dict[str, Any]) -> dict[str, Any]:
    normalized_transaction = transaction.copy()
    for field in ("request_id", "probability", "status"):
        normalized_transaction.pop(field, None)

    for field in ("email_purchaser", "email_recipient"):
        email = normalized_transaction.get(field)
        if isinstance(email, str):
            normalized_transaction[field] = email.strip().lower().split("@")[-1]

    return normalized_transaction
