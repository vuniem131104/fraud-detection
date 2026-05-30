from redis import asyncio as aioredis
import argparse
import asyncio
import json


key_transactions = "user:card:transactions:{user_id}_{card_id}"
key_features = "user:card:features:{user_id}_{card_id}"


def parse_args() -> argparse.Namespace:
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
    transactions_key = key_transactions.format(user_id=user_id, card_id=card_id)
    features_key = key_features.format(user_id=user_id, card_id=card_id)

    pipeline = redis_client.pipeline(transaction=False)
    pipeline.zrevrange(transactions_key, 0, -1)
    pipeline.hgetall(features_key)
    transactions_raw, features = await pipeline.execute()

    return {
        "user_id": user_id,
        "card_id": card_id,
        "features": features,
        "transactions": [json.loads(transaction) for transaction in transactions_raw],
    }


async def main() -> None:
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
