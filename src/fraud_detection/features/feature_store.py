from redis import asyncio as aioredis
from typing import Any
import json
from .utils import (
    build_transactions_key,
    build_features_key,
)
from structlog import get_logger

logger = get_logger(__name__)

class RedisFeatureStore:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis_client = redis_client

    def decode_redis_value(self, value: Any) -> Any:
        if isinstance(value, bytes):
            return value.decode()

        return value

    def decode_transaction(self, value: Any) -> dict[str, Any]:
        value = self.decode_redis_value(value)
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {"raw_value": value}

        if isinstance(decoded, dict):
            return decoded

        return {"value": decoded}

    def decode_features(self, values: dict[Any, Any]) -> dict[str, Any]:
        feature_types = {
            "no_transactions_30_days": int,
            "card_age_days": float,
            "no_days_since_last_txn": float,
        }
        decoded_features = {}
        for key, value in values.items():
            decoded_key = str(self.decode_redis_value(key))
            decoded_value = self.decode_redis_value(value)
            value_type = feature_types.get(decoded_key)
            if value_type is not None:
                decoded_value = value_type(decoded_value)
            decoded_features[decoded_key] = decoded_value
        return decoded_features

    async def get_txs(
        self,
        user_id: str,
        card_id: str,
    ) -> dict[str, Any]:
        transactions_key = build_transactions_key(user_id, card_id)
        features_key = build_features_key(user_id, card_id)

        try:
            logger.info(
                "Starting to fetch transactions and features",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                }
            )
            async with self.redis_client.pipeline(transaction=False) as pipeline:
                pipeline.zrevrange(transactions_key, 0, -1)
                pipeline.hgetall(features_key)
                transaction_values, feature_values = await pipeline.execute()
            features = self.decode_features(feature_values)
            transactions = [
                self.decode_transaction(value) for value in transaction_values
            ]
            logger.info(
                "Fetched transactions and features successfully",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "transaction_count": len(transactions),
                    "feature_count": len(features),
                }
            )

            return {
                "user_id": user_id,
                "card_id": card_id,
                "features": features,
                "transactions": transactions,
            }
        except Exception as e:
            logger.warning(
                "Failed to fetch transactions and features",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "error": str(e),
                }
            )
            return {
                "user_id": user_id,
                "card_id": card_id,
                "features": {},
                "transactions": [],
            }

# if __name__ == "__main__":
#     import os
#     import asyncio
#     async def main():
#         redis_client = aioredis.Redis(
#             host=os.getenv("REDIS_HOST"),
#             port=int(os.getenv("REDIS_PORT")),
#             db=int(os.getenv("REDIS_DB" )),
#             decode_responses=True,
#         )
#         feature_store = RedisFeatureStore(redis_client)
#         result = await feature_store.get_txs(
#             user_id="00e810e4ad7246eea0cc9e9537a19b5c",
#             card_id="de8d32b54ba14e959366cd1d495e78df",
#         )
#         print(json.dumps(result, indent=2))
#         await redis_client.aclose()
        
#     asyncio.run(main())
