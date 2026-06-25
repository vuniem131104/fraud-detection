"""Unit tests for the fraud-detection Web API (FastAPI).

Demonstrates Web-API testing with ``TestClient``, shared fixtures and mocks:
the prediction service is injected via a FastAPI dependency override, while
Postgres and Redis are replaced with async mocks on ``app.state`` — so the
endpoints are exercised end-to-end without any real backend.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from fastapi import FastAPI

from fraud_detection.core import api


# Reusable request bodies -----------------------------------------------------

USER_CARD_PAYLOAD = {
    "user": {"name": "Alice", "email": "alice@example.com", "password": "pw"},
    "card": {
        "issuer_code": "84001",
        "country": 840,
        "brand": "visa",
        "type": "credit",
        "bin_code": "411111",
    },
}


# ---------------------------------------------------------------------------
# Pure datetime / hashing helpers
# ---------------------------------------------------------------------------

def test_local_now():
    now = api.local_now()
    assert now.tzinfo == api.HO_CHI_MINH_TZ
    assert now.microsecond == 0


def test_to_local_datetime_naive():
    result = api.to_local_datetime(datetime(2017, 12, 15, 13, 0, 0))
    assert result.tzinfo == api.HO_CHI_MINH_TZ
    assert result.hour == 13


def test_to_local_datetime_aware():
    result = api.to_local_datetime(datetime(2017, 12, 15, 13, 0, 0, tzinfo=timezone.utc))
    assert result.utcoffset() == timedelta(hours=7)
    assert result.hour == 20  # 13:00 UTC -> 20:00 +07


def test_iso_local():
    assert api.iso_local(datetime(2017, 12, 15, 13, 0, 0)).startswith("2017-12-15T13:00:00")


def test_card_age_days():
    now = api.local_now()
    assert api.card_age_days(now - timedelta(days=5), now) == pytest.approx(5.0)


def test_card_age_days_clamped_to_zero():
    now = api.local_now()
    assert api.card_age_days(now + timedelta(days=5), now) == 0.0


def test_hash_password():
    digest = api.hash_password("secret")
    assert digest == api.hash_password("secret")     # deterministic
    assert digest != api.hash_password("other")
    assert len(digest) == 64                          # sha256 hex digest


# ---------------------------------------------------------------------------
# Health / readiness probes
# ---------------------------------------------------------------------------

def test_get_fraud_detection_service_dependency():
    """The DI helper returns the service stored on app state."""
    sentinel = object()
    request = MagicMock()
    request.app.state.fraud_detection_service = sentinel
    assert api.get_fraud_detection_service(request) is sentinel


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_ok(client):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_ready_unavailable(client, mock_database):
    database, _ = mock_database
    database.execute = AsyncMock(side_effect=Exception("db down"))
    response = client.get("/ready")
    assert response.status_code == 503


# ---------------------------------------------------------------------------
# User + card registration
# ---------------------------------------------------------------------------

def test_create_user_success(client, mock_redis, mock_database):
    _, pipe = mock_redis
    _, conn = mock_database

    response = client.post("/users", json=USER_CARD_PAYLOAD)

    assert response.status_code == 201
    body = response.json()
    assert len(body["user_id"]) == 32
    assert len(body["card_id"]) == 32
    assert "successfully" in body["message"]
    assert conn.execute.await_count == 2       # user insert + card insert
    pipe.hset.assert_called_once()             # initial Redis feature state seeded
    pipe.execute.assert_awaited_once()


def test_create_user_conflict(client, mock_database):
    _, conn = mock_database
    conn.execute = AsyncMock(side_effect=asyncpg.UniqueViolationError("dup"))
    response = client.post("/users", json=USER_CARD_PAYLOAD)
    assert response.status_code == 409


def test_create_user_db_error(client, mock_database):
    _, conn = mock_database
    conn.execute = AsyncMock(side_effect=asyncpg.PostgresError("boom"))
    response = client.post("/users", json=USER_CARD_PAYLOAD)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Scoring endpoint
# ---------------------------------------------------------------------------

def test_score_success(client, mock_service, transaction_payload):
    response = client.post("/score", json=transaction_payload)

    assert response.status_code == 200
    body = response.json()
    assert body["tx_id"] == "tx-100"
    assert body["probability"] == 0.83
    assert body["prediction"] == 1
    mock_service.predict.assert_awaited_once()


def test_score_validation_error(client, transaction_payload):
    invalid = {**transaction_payload, "amount_usd": -5.0}  # must be > 0
    response = client.post("/score", json=invalid)
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Application lifespan (startup / shutdown wiring)
# ---------------------------------------------------------------------------

async def test_lifespan_initializes_and_closes_service(monkeypatch):
    fake_db = MagicMock()
    fake_db.open = AsyncMock()
    fake_db.close = AsyncMock()
    monkeypatch.setattr(api, "PostgresDatabase", MagicMock(from_env=MagicMock(return_value=fake_db)))

    fake_redis = MagicMock()
    monkeypatch.setattr(api.aioredis, "BlockingConnectionPool", MagicMock())
    monkeypatch.setattr(api.aioredis, "Redis", MagicMock(return_value=fake_redis))
    monkeypatch.setattr(api, "RedisFeatureStore", MagicMock())

    fake_service = MagicMock()
    fake_service.open = AsyncMock()
    fake_service.close = AsyncMock()
    monkeypatch.setattr(api, "FraudDetectionService", MagicMock(return_value=fake_service))
    monkeypatch.setattr(api.json, "load", lambda handle: {"feature_columns": []})

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
