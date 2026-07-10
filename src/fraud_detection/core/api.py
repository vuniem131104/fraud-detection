"""FastAPI application exposing the fraud detection service.

Wires together Postgres, the Redis-backed Feast online store and the
``FraudDetectionService`` via the app lifespan, and exposes HTTP endpoints for
health/readiness probes and transaction scoring.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, status
from redis import asyncio as aioredis

from database import PostgresDatabase
from fraud_detection.core.feature_store import FeastFeatureStore
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.core.predict import FraudDetectionService


DEFAULT_MODEL_DIR = Path(__file__).resolve().parents[3] / "models"

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
)

_schema_path = DEFAULT_MODEL_DIR / "feature_schema.json"
with _schema_path.open() as f:
    _FEATURE_SCHEMA = json.load(f)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown.

    Initializes the Postgres database, the Redis connection pool, the feature
    schema and the ``FraudDetectionService``, stores them on ``app.state`` for
    the duration of the app's lifetime, and closes the service on shutdown.
    """
    database = PostgresDatabase.from_env()
    redis_pool = aioredis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "64")),
        timeout=float(os.getenv("REDIS_POOL_TIMEOUT_S", "5")),
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)
    fraud_detection_service: FraudDetectionService | None = None

    try:
        feature_store = FeastFeatureStore()
        app.state.database = database
        app.state.redis_client = redis_client
        fraud_detection_service = FraudDetectionService(
            schema=_FEATURE_SCHEMA,
            feature_store=feature_store,
            database=database,
            redis_client=redis_client,
        )
        await fraud_detection_service.open()
        app.state.fraud_detection_service = fraud_detection_service
        yield
    finally:
        await fraud_detection_service.close()


app = FastAPI(
    title="Fraud Detection API",
    lifespan=lifespan,
)


def get_fraud_detection_service(request: Request) -> FraudDetectionService:
    """FastAPI dependency returning the shared ``FraudDetectionService`` from app state."""
    return request.app.state.fraud_detection_service


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe returning a static ``ok`` status."""
    return {"status": "ok"}


@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    """Readiness probe verifying Postgres and Redis connectivity.

    Raises:
        HTTPException: ``503 Service Unavailable`` if either dependency cannot
            be reached.
    """
    try:
        await request.app.state.database.execute("SELECT 1")
        await request.app.state.redis_client.ping()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dependencies are not ready",
        ) from exc

    return {"status": "ready"}


@app.post("/score", response_model=FraudDetectionOutputs)
async def score(
    inputs: FraudDetectionInputs,
    service: FraudDetectionService = Depends(get_fraud_detection_service),
) -> FraudDetectionOutputs:
    """Score a single transaction for fraud and return the prediction."""
    return await service.predict(inputs)
