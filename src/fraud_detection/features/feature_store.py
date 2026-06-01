from redis import asyncio as aioredis
from datetime import datetime, timedelta, timezone
from typing import Any
import json
from .utils import (
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
        current_transaction: dict[str, Any],
        cutoff_days: int = 30,
    ) -> None:
        if cutoff_days < 1:
            raise ValueError("cutoff_days must be greater than 0")

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=cutoff_days)
        cutoff_score = int(cutoff.timestamp())
        transactions_key = build_transactions_key(user_id, card_id)
        features_key = build_features_key(user_id, card_id)
        transaction_score = int(parse_datetime(current_transaction["event_timestamp"]).timestamp())
        transaction_to_store = dict(current_transaction)
        for feature in ("D4", "D15"):
            value = transaction_to_store.get(feature)
            if value is not None:
                try:
                    transaction_to_store[feature] = float(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"{feature} must be numeric, got {value!r}") from exc
        serialized_transaction = json.dumps(transaction_to_store, separators=(",", ":"))
        
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
                serialized_transaction,
                transaction_score,
            )
            values = list(refresh_result or [])
            values.extend([None] * (3 - len(values)))
            removed_count, remaining_count, card_created_at = values[:3]
            card_created_at = self.decode_redis_value(card_created_at)
            feature_updates = {
                "no_transactions_30_days": remaining_count,
            }

            if card_created_at:
                feature_updates["card_age_days"] = days_between(parse_datetime(card_created_at), now)

            last_txn_at = current_transaction["event_timestamp"]
            feature_updates["last_txn_at"] = last_txn_at
            feature_updates["no_days_since_last_txn"] = days_between(parse_datetime(last_txn_at), now)
            await self.redis_client.hset(features_key, mapping=feature_updates)
                
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
