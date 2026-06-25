"""Unit tests for the Redis key builders and the Redis-backed feature store."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from fraud_detection.features.feature_store import RedisFeatureStore
from fraud_detection.features.utils import build_features_key, build_transactions_key

from conftest import AsyncContextManager


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def test_key_builders():
    """The key helpers produce the agreed, stable Redis key names."""
    assert build_transactions_key("u1", "c1") == "user:card:transactions:u1_c1"
    assert build_features_key("u1", "c1") == "user:card:features:u1_c1"


# ---------------------------------------------------------------------------
# Decoding helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def store() -> RedisFeatureStore:
    return RedisFeatureStore(MagicMock())


def test_decode_redis_value(store):
    """Bytes are decoded to str; other types pass through unchanged."""
    assert store.decode_redis_value(b"hello") == "hello"
    assert store.decode_redis_value("hello") == "hello"
    assert store.decode_redis_value(7) == 7


def test_decode_transaction_variants(store):
    """JSON objects decode to dicts; scalars and invalid JSON are wrapped."""
    assert store.decode_transaction(json.dumps({"a": 1})) == {"a": 1}
    assert store.decode_transaction(b'{"b": 2}') == {"b": 2}
    assert store.decode_transaction("5") == {"value": 5}
    assert store.decode_transaction("not-json") == {"raw_value": "not-json"}


def test_decode_features_typing(store):
    """Known feature names are coerced to their numeric type; others stay strings."""
    decoded = store.decode_features(
        {
            b"no_transactions_30_days": b"3",
            b"card_age_days": b"12.5",
            b"no_days_since_last_txn": b"4",
            b"card_created_at": b"2017-12-01T00:00:00",
        }
    )
    assert decoded["no_transactions_30_days"] == 3
    assert isinstance(decoded["no_transactions_30_days"], int)
    assert decoded["card_age_days"] == 12.5
    assert decoded["no_days_since_last_txn"] == 4.0
    assert decoded["card_created_at"] == "2017-12-01T00:00:00"


# ---------------------------------------------------------------------------
# get_txs
# ---------------------------------------------------------------------------

async def test_get_txs_success():
    """A successful pipelined read returns decoded transactions and features."""
    pipe = MagicMock()
    pipe.zrevrange = MagicMock()
    pipe.hgetall = MagicMock()
    pipe.execute = AsyncMock(
        return_value=(
            [json.dumps({"tx_id": "a"}), json.dumps({"tx_id": "b"})],
            {b"no_transactions_30_days": b"2", b"card_age_days": b"9.0"},
        )
    )
    redis = MagicMock()
    redis.pipeline = MagicMock(return_value=AsyncContextManager(pipe))

    result = await RedisFeatureStore(redis).get_txs("user-1", "card-1")

    assert result["user_id"] == "user-1"
    assert result["card_id"] == "card-1"
    assert result["transactions"] == [{"tx_id": "a"}, {"tx_id": "b"}]
    assert result["features"]["no_transactions_30_days"] == 2
    assert result["features"]["card_age_days"] == 9.0
    pipe.zrevrange.assert_called_once()
    pipe.hgetall.assert_called_once()


async def test_get_txs_failure_returns_empty():
    """A Redis error is swallowed and an empty (but well-formed) result is returned."""
    pipe = MagicMock()
    pipe.zrevrange = MagicMock()
    pipe.hgetall = MagicMock()
    pipe.execute = AsyncMock(side_effect=RuntimeError("redis down"))
    redis = MagicMock()
    redis.pipeline = MagicMock(return_value=AsyncContextManager(pipe))

    result = await RedisFeatureStore(redis).get_txs("user-1", "card-1")

    assert result == {
        "user_id": "user-1",
        "card_id": "card-1",
        "features": {},
        "transactions": [],
    }
