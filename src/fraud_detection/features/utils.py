from datetime import datetime

refresh_key_script = """
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
local remaining = redis.call('ZCARD', KEYS[1])
local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0)[1] or false
local card_created_at = redis.call('HGET', KEYS[2], 'card_created_at') or false
return {removed, remaining, latest, card_created_at}
"""

def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def days_between(start: datetime, end: datetime) -> float:
    elapsed_days = (end - start).total_seconds() / (24 * 60 * 60)
    return max(elapsed_days, 0.0)

def build_transactions_key(user_id: str, card_id: str) -> str:
        return f"user:card:transactions:{user_id}_{card_id}"

def build_features_key(user_id: str, card_id: str) -> str:
    return f"user:card:features:{user_id}_{card_id}"
