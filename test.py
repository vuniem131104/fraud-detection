"""CLI utility to inspect the Redis-cached transactions and features for a user/card pair."""

from redis import asyncio as aioredis
import argparse
import asyncio
import json
from typing import Any


key_transactions = "user:card:transactions:{user_id}_{card_id}"
key_features = "user:card:features:{user_id}_{card_id}"


def decode_redis_value(value: Any) -> Any:
    """Decode a raw Redis value from bytes to str, passing through non-bytes values."""
    if isinstance(value, bytes):
        return value.decode()

    return value


def decode_features(values: dict[Any, Any]) -> dict[str, Any]:
    """Decode a Redis feature hash, coercing known feature keys to their numeric types."""
    feature_types = {
        "no_transactions_30_days": int,
        "card_age_days": float,
        "no_days_since_last_txn": float,
    }
    decoded_features = {}
    for key, value in values.items():
        decoded_key = str(decode_redis_value(key))
        decoded_value = decode_redis_value(value)
        value_type = feature_types.get(decoded_key)
        if value_type is not None:
            decoded_value = value_type(decoded_value)
        decoded_features[decoded_key] = decoded_value
    return decoded_features


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the user/card lookup, including Redis connection options."""
    parser = argparse.ArgumentParser(description="Get transactions and features for a user card.")
    parser.add_argument("user_id")
    parser.add_argument("card_id")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=0)
    return parser.parse_args()


async def get_card_data(
    redis_client: aioredis.Redis,
    user_id: str,
    card_id: str,
) -> dict:
    """Fetch and decode the cached transactions and features for a user/card pair from Redis."""
    transactions_key = key_transactions.format(user_id=user_id, card_id=card_id)
    features_key = key_features.format(user_id=user_id, card_id=card_id)

    pipeline = redis_client.pipeline(transaction=False)
    pipeline.zrevrange(transactions_key, 0, -1)
    pipeline.hgetall(features_key)
    transactions_raw, features_raw = await pipeline.execute()
    features = decode_features(features_raw)

    return {
        "user_id": user_id,
        "card_id": card_id,
        "features": features,
        "transactions": [json.loads(transaction) for transaction in transactions_raw],
    }


async def main() -> None:
    """Connect to Redis, retrieve the card data for the given user/card, and print it as JSON."""
    args = parse_args()
    redis_client = aioredis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        decode_responses=True,
    )
    
    try:
        data = await get_card_data(redis_client, args.user_id, args.card_id)
    finally:
        await redis_client.aclose()

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
