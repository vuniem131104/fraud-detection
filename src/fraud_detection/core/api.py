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
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.core.predict import FraudDetectionService
from fraud_detection.features.feature_store import RedisFeatureStore


DEFAULT_MODEL_DIR = Path(__file__).resolve().parents[3] / "models"

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    database = PostgresDatabase.from_env()
    redis_pool = aioredis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "64")),
        timeout=float(os.getenv("REDIS_POOL_TIMEOUT_S", "5")),
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)
    fraud_detection_service: FraudDetectionService | None = None

    schema_path = DEFAULT_MODEL_DIR / "feature_schema.json"
    with schema_path.open() as schema_file:
        schema = json.load(schema_file)

    try:
        await database.open()
        await redis_client.ping()

        feature_store = RedisFeatureStore(redis_client)
        app.state.database = database
        app.state.redis_client = redis_client
        fraud_detection_service = FraudDetectionService(
            schema=schema,
            feature_store=feature_store,
            database=database,
            threshold=0.5,
        )
        app.state.fraud_detection_service = fraud_detection_service
        yield
    finally:
        if fraud_detection_service is not None:
            await fraud_detection_service.close()
        await redis_client.aclose()
        await database.close()


app = FastAPI(
    title="Fraud Detection API",
    lifespan=lifespan,
)


def get_fraud_detection_service(request: Request) -> FraudDetectionService:
    return request.app.state.fraud_detection_service


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready(request: Request) -> dict[str, str]:
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
    return await service.predict(inputs)
