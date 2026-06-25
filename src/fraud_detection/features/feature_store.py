"""Redis-backed feature store for online fraud-detection serving.

Provides read access to per-(user, card) transaction history and precomputed
features stored in Redis (a sorted set of transactions and a hash of features),
decoding the raw Redis values into plain Python dictionaries for inference.
"""

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
    """Read access to transactions and features stored in Redis.

    Wraps an async Redis client and exposes helpers to decode raw Redis
    responses and to fetch the combined transaction history and feature
    set for a given user/card pair.
    """

    def __init__(self, redis_client: aioredis.Redis):
        """Store the async Redis client used for all reads.

        Args:
            redis_client: An async Redis client used to query transactions
                and features.
        """
        self.redis_client = redis_client

    def decode_redis_value(self, value: Any) -> Any:
        """Decode a single Redis value to ``str`` if it is ``bytes``.

        Args:
            value: A raw value returned by Redis.

        Returns:
            The decoded ``str`` when the input is ``bytes``, otherwise the
            value unchanged.
        """
        if isinstance(value, bytes):
            return value.decode()

        return value

    def decode_transaction(self, value: Any) -> dict[str, Any]:
        """Decode a stored transaction into a dictionary.

        The value is first decoded from ``bytes`` if needed and then parsed
        as JSON. Non-dict JSON payloads are wrapped under a ``"value"`` key
        and values that fail to parse are wrapped under a ``"raw_value"`` key.

        Args:
            value: A raw transaction entry from the Redis sorted set.

        Returns:
            A dictionary representation of the transaction.
        """
        value = self.decode_redis_value(value)
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {"raw_value": value}

        if isinstance(decoded, dict):
            return decoded

        return {"value": decoded}

    def decode_features(self, values: dict[Any, Any]) -> dict[str, Any]:
        """Decode a Redis feature hash into a typed dictionary.

        Keys and values are decoded from ``bytes`` and known numeric features
        are coerced to their expected Python types (e.g. ``int``/``float``);
        unknown features are kept as decoded strings.

        Args:
            values: The raw field/value mapping returned by ``HGETALL``.

        Returns:
            A dictionary mapping feature names to typed values.
        """
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
        """Fetch transactions and features for a user/card pair.

        Issues a pipelined read for the transaction sorted set (newest first)
        and the feature hash, then decodes both. On any failure the error is
        logged and an empty result is returned instead of raising.

        Args:
            user_id: Identifier of the user.
            card_id: Identifier of the card.

        Returns:
            A dictionary with ``user_id``, ``card_id``, the decoded
            ``features`` dict and the list of decoded ``transactions``. The
            feature and transaction collections are empty if the read fails.
        """
        transactions_key = build_transactions_key(user_id, card_id)
        features_key = build_features_key(user_id, card_id)

        try:
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
