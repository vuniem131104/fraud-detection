from redis import asyncio as aioredis
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
import json
from utils import (
    refresh_key_script,
    parse_datetime,
    days_between,
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

    def decode_features(self, values: Mapping[Any, Any]) -> dict[str, Any]:
        return {
            str(self.decode_redis_value(key)): self.decode_redis_value(value)
            for key, value in values.items()
        }

    def pop_first_value(self, data: dict[str, Any], keys: list[str]) -> Any:
        for key in keys:
            if key in data:
                return data.pop(key)

        return None

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
            transaction_values = await self.redis_client.zrevrange(transactions_key, 0, -1)
            feature_values = await self.redis_client.hgetall(features_key)
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

    async def refresh_features_for_user_card(
        self,
        user_id: str,
        card_id: str,
        cutoff_days: int = 30,
    ) -> None:
        if cutoff_days < 1:
            raise ValueError("cutoff_days must be greater than 0")

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=cutoff_days)
        cutoff_score = int(cutoff.timestamp())
        transactions_key = build_transactions_key(user_id, card_id)
        features_key = build_features_key(user_id, card_id)
        
        try:
            logger.info(
                "Starting to refresh features for user card",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "cutoff_days": cutoff_days,
                }
            )
            refresh_result = await self.redis_client.eval(
                refresh_key_script,
                2,
                transactions_key,
                features_key,
                cutoff_score,
            )
            removed_count, remaining_count, latest_transaction_raw, card_created_at = (
                list(refresh_result) + [None, None]
            )[:4]
            latest_transaction_raw = self.decode_redis_value(latest_transaction_raw)
            card_created_at = self.decode_redis_value(card_created_at)
            feature_updates = {
                "no_transactions_30_days": remaining_count,
            }

            if card_created_at:
                feature_updates["card_ages_days"] = days_between(parse_datetime(card_created_at), now)

            if latest_transaction_raw:
                latest_transaction = json.loads(latest_transaction_raw)
                last_txn_at = latest_transaction["created_at"]
                feature_updates["last_txn_at"] = last_txn_at
                feature_updates["no_days_since_last_txn"] = days_between(parse_datetime(last_txn_at), now)
                await self.redis_client.hset(features_key, mapping=feature_updates)
            else:
                pipeline = self.redis_client.pipeline(transaction=False)
                pipeline.hset(features_key, mapping=feature_updates)
                pipeline.hdel(features_key, "last_txn_at", "no_days_since_last_txn")
                await pipeline.execute()
                
            logger.info(
                "Finished refreshing features for user card",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "removed_count": removed_count,
                    "remaining_count": remaining_count,
                }
            )

        except Exception as e:
            logger.exception(
                "Failed to refresh features for user card",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "error": str(e),
                }
            )
            
if __name__ == "__main__":
    import asyncio

    async def main():
        redis_client = aioredis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        feature_store = RedisFeatureStore(redis_client)
        await feature_store.get_txs("1", "1")

    asyncio.run(main())