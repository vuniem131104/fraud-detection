#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
from redis import asyncio as aioredis
import asyncio
import json
from typing import Any

# ---- hard-coded values ----
USER_ID = "009f5838ce5b4e53a0d81509b04d4af7"
CARD_ID = "95998c0e999c4a10a8ef86a335d39ea5"
HOST = "10.207.204.179"
PORT = 6379
DB = 0
# ---------------------------

key_transactions = "user:card:transactions:{user_id}_{card_id}"
key_features = "user:card:features:{user_id}_{card_id}"


def decode_redis_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()

    return value


def decode_features(values: dict[Any, Any]) -> dict[str, Any]:
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


async def get_card_data(
    redis_client: aioredis.Redis,
    user_id: str,
    card_id: str,
) -> dict:
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
    redis_client = aioredis.Redis(
        host=HOST,
        port=PORT,
        db=DB,
        decode_responses=True,
    )

    try:
        data = await get_card_data(redis_client, USER_ID, CARD_ID)
    finally:
        await redis_client.aclose()

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
PY
