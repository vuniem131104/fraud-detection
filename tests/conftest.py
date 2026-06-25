"""Shared pytest fixtures and test doubles for the fraud-detection unit tests.

Provides:
  * a small synthetic feature schema (so we never load the real 44 MB schema),
  * sample transaction / Redis-state payloads,
  * async-aware mocks for Postgres, Redis and the inference service, and
  * a FastAPI ``TestClient`` wired to those mocks (the app lifespan is bypassed,
    so no real Postgres / Redis / KServe / Kafka connections are opened).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from fraud_detection.core import api
from fraud_detection.core.models import FraudDetectionOutputs
from fraud_detection.core.predict import FraudDetectionService


# ---------------------------------------------------------------------------
# Small async helpers
# ---------------------------------------------------------------------------

class AsyncContextManager:
    """Minimal async context manager yielding a fixed value.

    Stands in for ``redis.pipeline(...)`` / ``database.transaction()`` which the
    route handlers use via ``async with``.
    """

    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def schema() -> dict[str, Any]:
    """A compact, self-contained feature schema exercising the full build pipeline."""
    return {
        "version": "test-1",
        "training_reference_ts": "2017-12-01",
        "email_bin": {"gmail.com": "google", "protonmail.com": "proton"},
        "email_nulls": ["anonymous.com", "mail.com"],
        "uid_columns": ["uid1", "uid2", "uid3", "uid4"],
        "uid_agg_targets": ["amount_usd", "C13", "D15", "D4"],
        "freq_tables": {"card_id": {}, "device_brand": {"windows": 0.25}},
        "categorical_encoders": {
            "channel": {"W": 1, "C": 2, "R": 3, "missing": 0},
            "card_brand": {"visa": 1, "mastercard": 2, "missing": 0},
            "missing_col_example": {"missing": 7},
        },
        "feature_columns": [
            "amount_usd", "amount_log", "amount_cents",
            "hour_of_day", "day_of_week", "is_weekend",
            "C1", "C2", "C13", "D4", "D15",
            "channel", "card_brand",
            "card_id", "card_id_freq", "device_brand_freq",
            "card_tx_count_so_far", "card_amount_sum_so_far",
            "card_amount_mean_so_far", "amount_zscore_card",
            "uid1_amount_usd_mean", "uid1_amount_usd_std",
            "screen_width", "screen_height", "screen_area",
            "os_version", "browser_version",
            "browser_raw",          # stays a string -> coerced to NaN in assemble_vector
            "missing_col_example",  # absent column filled by the categorical encoder
            "V999",                 # never produced -> added as NaN
        ],
        "target": "isFraud",
    }


@pytest.fixture
def transaction_payload() -> dict[str, Any]:
    """A valid scoring request body (uses the C*/D*/M* aliases of the input model)."""
    return {
        "tx_id": "tx-100",
        "event_timestamp": "2017-12-15T13:30:00",
        "amount_usd": 50.0,
        "channel": "W",
        "user_id": "user-1",
        "card_id": "0" * 31 + "1",  # 32-char hex, parsed by to_model_card_id
        "card_country": 840,
        "issuer_code": 84001,
        "card_brand": "visa",
        "bin_code": "411111",
        "card_type": "credit",
        "billing_zone": 1,
        "billing_country": 840,
        "email_purchaser": "buyer@gmail.com",
        "email_recipient": "seller@gmail.com",
        "device_type": "desktop",
        "device_info": "desktop:Windows 11:Chrome",
        "os_raw": "Windows 11",
        "browser_raw": "Chrome 120",
        "screen_resolution": "1920x1080",
        "C1": 5,
        "C2": 2,
        "C13": 10,
        "M1": "T",
        "M2": "T",
        "M6": "F",
        "D4": 3.0,
        "D15": 7.0,
    }


@pytest.fixture
def previous_transactions(transaction_payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Two earlier transactions for the same user/card (drives history aggregates)."""
    older = {**transaction_payload, "tx_id": "old-1",
             "event_timestamp": "2017-12-10T10:00:00", "amount_usd": 40.0, "C13": 3}
    newer = {**transaction_payload, "tx_id": "old-2",
             "event_timestamp": "2017-12-12T11:00:00", "amount_usd": 60.0, "C13": 4}
    return [older, newer]


@pytest.fixture
def redis_state(previous_transactions: list[dict[str, Any]]) -> dict[str, Any]:
    """A cached Redis state mirroring what ``RedisFeatureStore.get_txs`` returns."""
    return {
        "user_id": "user-1",
        "card_id": "0" * 31 + "1",
        "features": {
            "no_transactions_30_days": 2,
            "card_age_days": 3.0,
            "no_days_since_last_txn": 5.0,
        },
        "transactions": previous_transactions,
    }


# ---------------------------------------------------------------------------
# Mock backends
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_service() -> MagicMock:
    """A stand-in ``FraudDetectionService`` whose ``predict`` is an async mock."""
    service = MagicMock(spec=FraudDetectionService)
    service.predict = AsyncMock(
        return_value=FraudDetectionOutputs(tx_id="tx-100", probability=0.83, prediction=1)
    )
    return service


@pytest.fixture
def mock_database() -> tuple[MagicMock, AsyncMock]:
    """A fake Postgres database plus the connection yielded by ``transaction()``."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="INSERT 0 1")

    database = MagicMock()
    database.execute = AsyncMock(return_value="SELECT 1")
    database.transaction = MagicMock(return_value=AsyncContextManager(conn))
    return database, conn


@pytest.fixture
def mock_redis() -> tuple[MagicMock, MagicMock]:
    """A fake async Redis client plus the pipeline object it hands out."""
    pipe = MagicMock()
    pipe.delete = MagicMock()
    pipe.hset = MagicMock()
    pipe.execute = AsyncMock(return_value=[1, 1])

    redis = MagicMock()
    redis.ping = AsyncMock(return_value=True)
    redis.pipeline = MagicMock(return_value=AsyncContextManager(pipe))
    return redis, pipe


@pytest.fixture
def client(
    mock_service: MagicMock,
    mock_database: tuple[MagicMock, AsyncMock],
    mock_redis: tuple[MagicMock, MagicMock],
):
    """A ``TestClient`` for the FastAPI app with all backends mocked.

    The client is *not* used as a context manager, so the real ``lifespan`` never
    runs; we populate ``app.state`` and override the service dependency by hand.
    """
    database, _ = mock_database
    redis, _ = mock_redis

    api.app.state.database = database
    api.app.state.redis_client = redis
    api.app.state.fraud_detection_service = mock_service
    api.app.dependency_overrides[api.get_fraud_detection_service] = lambda: mock_service

    test_client = TestClient(api.app)
    yield test_client

    api.app.dependency_overrides.clear()


@pytest.fixture
def kserve_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the environment variables ``FraudDetectionService.__init__`` reads."""
    monkeypatch.setenv("KSERVE_URL", "http://kserve.local/v2/models/fraud/infer")
    monkeypatch.setenv("PREDICTIONS_TOPIC", "predictions")
    monkeypatch.setenv("MODEL_NAME", "fraud-model")
    monkeypatch.setenv("MODEL_VERSION", "1")
    monkeypatch.setenv("FEATURE_BUILD_WORKERS", "1")
