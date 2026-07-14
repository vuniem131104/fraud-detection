"""Unit tests for the fraud-detection Web API (FastAPI).

Demonstrates Web-API testing with ``TestClient``, shared fixtures and mocks:
the prediction service is injected via a FastAPI dependency override, while
Postgres and Redis are replaced with async mocks on ``app.state`` — so the
endpoints are exercised end-to-end without any real backend.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI

from fraud_detection.core import api


# ---------------------------------------------------------------------------
# Dependency helper
# ---------------------------------------------------------------------------

def test_get_fraud_detection_service_dependency():
    """The DI helper returns the service stored on app state."""
    sentinel = object()
    request = MagicMock()
    request.app.state.fraud_detection_service = sentinel
    assert api.get_fraud_detection_service(request) is sentinel


# ---------------------------------------------------------------------------
# Health / readiness probes
# ---------------------------------------------------------------------------

def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_ok(client, mock_database, mock_redis):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    mock_database.execute.assert_awaited_once_with("SELECT 1")
    mock_redis.ping.assert_awaited_once()


def test_ready_database_down(client, mock_database):
    mock_database.execute = AsyncMock(side_effect=Exception("db down"))
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json()["detail"] == "Dependencies are not ready"


def test_ready_redis_down(client, mock_redis):
    mock_redis.ping = AsyncMock(side_effect=Exception("redis down"))
    response = client.get("/ready")
    assert response.status_code == 503


# ---------------------------------------------------------------------------
# Scoring endpoint
# ---------------------------------------------------------------------------

def test_score_success(client, mock_service, transaction_payload):
    response = client.post("/score", json=transaction_payload)

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_id"] == "tx-100"
    assert body["probability"] == 0.83
    assert body["prediction"] == 1
    mock_service.predict.assert_awaited_once()
    # The endpoint forwards the *validated* input model to the service.
    (inputs,) = mock_service.predict.await_args.args
    assert inputs.transaction_id == "tx-100"
    assert inputs.amount_usd == 50.0


def test_score_validation_error_never_reaches_service(client, mock_service, transaction_payload):
    invalid = {**transaction_payload, "amount_usd": -5.0}  # must be > 0
    response = client.post("/score", json=invalid)
    assert response.status_code == 422
    mock_service.predict.assert_not_awaited()


def test_score_rejects_unknown_fields(client, transaction_payload):
    """The input model is ``extra=forbid`` — unexpected keys are a 422."""
    response = client.post("/score", json={**transaction_payload, "is_fraud": 1})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Application lifespan (startup / shutdown wiring)
# ---------------------------------------------------------------------------

async def test_lifespan_initializes_and_closes_service(monkeypatch):
    fake_db = MagicMock()
    monkeypatch.setattr(
        api, "PostgresDatabase", MagicMock(from_env=MagicMock(return_value=fake_db))
    )

    fake_redis = MagicMock()
    monkeypatch.setattr(api.aioredis, "BlockingConnectionPool", MagicMock())
    monkeypatch.setattr(api.aioredis, "Redis", MagicMock(return_value=fake_redis))
    monkeypatch.setattr(api, "FeastFeatureStore", MagicMock())

    fake_service = MagicMock()
    fake_service.open = AsyncMock()
    fake_service.close = AsyncMock()
    monkeypatch.setattr(api, "FraudDetectionService", MagicMock(return_value=fake_service))

    monkeypatch.setenv("REDIS_HOST", "localhost")
    monkeypatch.setenv("REDIS_PORT", "6379")
    monkeypatch.setenv("REDIS_DB", "0")

    app = FastAPI()
    async with api.lifespan(app):
        assert app.state.fraud_detection_service is fake_service
        assert app.state.database is fake_db
        assert app.state.redis_client is fake_redis

    fake_service.open.assert_awaited_once()
    fake_service.close.assert_awaited_once()
