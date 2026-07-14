"""Shared pytest fixtures and test doubles for the fraud-detection unit tests.

Provides:
  * a compact feature schema mirroring the real ``models/feature_schema.json``
    structure (so tests never depend on the trained artifact),
  * a valid scoring payload for the current ``FraudDetectionInputs`` schema,
  * async-aware mocks for Postgres, Redis, the Feast online store and KServe,
  * a fully-constructed ``FraudDetectionService`` wired to those mocks, and
  * a FastAPI ``TestClient`` with the app lifespan bypassed (no real Postgres /
    Redis / KServe / Kafka connections are ever opened).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient

from fraud_detection.core import api
from fraud_detection.core.models import FraudDetectionOutputs
from fraud_detection.core.predict import FraudDetectionService

# Keep pytest away from the locust file: importing locust applies gevent
# monkey-patching, which breaks the rest of the test run.
collect_ignore = ["locust_load_test.py"]


# ---------------------------------------------------------------------------
# Data fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def schema() -> dict[str, Any]:
    """A compact feature schema with the same shape as the real artifact.

    Keeps the categorical/numeric split and ordering semantics of
    ``models/feature_schema.json`` while staying small enough to reason about
    in assertions.
    """
    return {
        "version": "test-1",
        "target": "is_fraud",
        "feature_columns": [
            "amount_usd",
            "log_amount",
            "hour",
            "weekday",
            "is_night",
            "channel",
            "card_brand",
            "merchant_category",
            "merchant_risk_level",
            "account_age_days",
            "card_age_days",
            "geo_mismatch",
            "foreign_ip",
            "recipient_differs",
            "card_tx_count_1h",
            "card_tx_count_24h",
            "card_amount_sum_24h",
            "card_seconds_since_last_tx",
            "card_amount_zscore",
            "card_tx_seq",
            "card_declines_24h",
            "user_tx_count_24h",
            "user_amount_sum_24h",
            "user_seconds_since_last_tx",
        ],
        "categorical_encoders": {
            "channel": {"web": 0, "mobile": 1, "pos": 2},
            "card_brand": {"visa": 0, "mastercard": 1, "amex": 2},
            "merchant_category": {"electronics": 0, "travel": 1, "grocery": 2},
        },
    }


@pytest.fixture
def transaction_payload() -> dict[str, Any]:
    """A valid request body for ``POST /score`` (current input schema)."""
    return {
        "transaction_id": "tx-100",
        "user_id": "user-1",
        "card_id": "card-1",
        "merchant_category": "electronics",
        "merchant_risk_level": 3,
        "amount_usd": 50.0,
        "timestamp": "2026-07-01T13:30:00Z",
        "channel": "web",
        "billing_country_code": "VN",
        "ip_country_code": "VN",
        "email_purchaser": "buyer@gmail.com",
        "email_recipient": "seller@example.com",
    }


@pytest.fixture
def online_features() -> dict[str, Any]:
    """Online-store values for a (user, card) pair, as Feast returns them."""
    return {
        "user_id": "user-1",
        "card_id": "card-1",
        "card_brand": "visa",
        "card_type": "credit",
        "is_virtual": False,
        "customer_segment": "retail",
        "kyc_level": 2,
        "email_verified": True,
        "account_created_at": "2025-06-01T00:00:00Z",
        "card_created_at": "2026-01-01T00:00:00Z",
        "user_country": "VN",
    }


# ---------------------------------------------------------------------------
# Environment + mock backends
# ---------------------------------------------------------------------------

@pytest.fixture
def service_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set every environment variable ``FraudDetectionService.__init__`` reads."""
    monkeypatch.setenv("KSERVE_URL", "http://kserve.local/v2/models/fraud/infer")
    monkeypatch.setenv("DECISION_THRESHOLD", "0.8")
    monkeypatch.setenv("CARD_TRANSACTIONS_KEY", "card:transactions")
    monkeypatch.setenv("CARD_AGGREGATE_KEY", "card:aggregate")
    monkeypatch.setenv("CARD_DECLINES_KEY", "card:declines")
    monkeypatch.setenv("USER_TRANSACTIONS_KEY", "user:transactions")
    monkeypatch.setenv("USER_AGGREGATE_KEY", "user:aggregate")
    monkeypatch.setenv("PREDICTIONS_TOPIC", "predictions")
    monkeypatch.setenv("MODEL_NAME", "fraud-model")
    monkeypatch.setenv("MODEL_VERSION", "1")
    monkeypatch.delenv("BOOTSTRAP_SERVERS", raising=False)


@pytest.fixture
def mock_database() -> MagicMock:
    """A fake async Postgres database."""
    database = MagicMock()
    database.open = AsyncMock()
    database.close = AsyncMock()
    database.execute = AsyncMock(return_value="SELECT 1")
    return database


@pytest.fixture
def card_velocity_result() -> list[Any]:
    """Raw return of the card velocity Lua script.

    ``[cnt_1h, cnt_24h, sum_24h, declines_24h, n, s, ss, last_txn_at]`` for a
    card with 3 prior transactions of 40/50/60 USD, last seen at epoch
    1782900000 (before the scored transaction).
    """
    return [1, 3, "150.0", 0, 3, "150.0", "7700.0", "1782900000.0"]


@pytest.fixture
def user_velocity_result() -> list[Any]:
    """Raw return of the user velocity Lua script: ``[cnt_24h, sum_24h, last_txn_at]``."""
    return [2, "90.0", "1782900000.0"]


@pytest.fixture
def mock_redis(card_velocity_result, user_velocity_result) -> MagicMock:
    """A fake async Redis client whose registered Lua scripts are async mocks.

    ``register_script`` is called twice by the service constructor — first for
    the card script, then for the user script — so ``side_effect`` hands back
    the matching async callables in that order.
    """
    redis = MagicMock()
    redis.ping = AsyncMock(return_value=True)
    redis.aclose = AsyncMock()
    redis.card_script = AsyncMock(return_value=list(card_velocity_result))
    redis.user_script = AsyncMock(return_value=list(user_velocity_result))
    redis.register_script = MagicMock(side_effect=[redis.card_script, redis.user_script])
    return redis


@pytest.fixture
def mock_feature_store(online_features) -> MagicMock:
    """A fake Feast store returning the canned online feature values."""
    store = MagicMock()
    store.open = AsyncMock()
    store.close = AsyncMock()
    store.get_online_features = AsyncMock(return_value=dict(online_features))
    return store


def make_kserve_transport(probability: float = 0.35) -> tuple[httpx.MockTransport, list[dict]]:
    """Build an httpx transport faking a KServe v2 endpoint.

    Returns the transport plus a list capturing every request payload, so tests
    can assert on the inference request the service actually built.
    """
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={"outputs": [{"name": "output-0", "data": [probability]}]},
        )

    return httpx.MockTransport(handler), seen


@pytest.fixture
def service(service_env, schema, mock_feature_store, mock_database, mock_redis) -> FraudDetectionService:
    """A fully-constructed service with every backend mocked.

    The KServe HTTP client is swapped for one backed by ``httpx.MockTransport``
    returning probability 0.35; the captured request payloads are exposed as
    ``service.kserve_requests`` for assertions.
    """
    svc = FraudDetectionService(
        schema=schema,
        feature_store=mock_feature_store,
        database=mock_database,
        redis_client=mock_redis,
    )
    transport, seen = make_kserve_transport(probability=0.35)
    svc.kserve_client = httpx.AsyncClient(transport=transport)
    svc.kserve_requests = seen
    return svc


# ---------------------------------------------------------------------------
# Web API client
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_service(transaction_payload) -> MagicMock:
    """A stand-in ``FraudDetectionService`` whose ``predict`` is an async mock."""
    service = MagicMock(spec=FraudDetectionService)
    service.predict = AsyncMock(
        return_value=FraudDetectionOutputs(
            transaction_id=transaction_payload["transaction_id"],
            probability=0.83,
            prediction=1,
        )
    )
    return service


@pytest.fixture
def client(mock_service, mock_database, mock_redis):
    """A ``TestClient`` for the FastAPI app with all backends mocked.

    The client is *not* used as a context manager, so the real ``lifespan``
    never runs; ``app.state`` is populated and the service dependency is
    overridden by hand.
    """
    api.app.state.database = mock_database
    api.app.state.redis_client = mock_redis
    api.app.state.fraud_detection_service = mock_service
    api.app.dependency_overrides[api.get_fraud_detection_service] = lambda: mock_service

    test_client = TestClient(api.app)
    yield test_client

    api.app.dependency_overrides.clear()
