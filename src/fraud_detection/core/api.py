from __future__ import annotations

import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator
from uuid import uuid4

import asyncpg
import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from redis import asyncio as aioredis

from database import PostgresDatabase
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.core.predict import FraudDetectionService
from fraud_detection.features.feature_store import RedisFeatureStore
from fraud_detection.features.utils import build_features_key, build_transactions_key


DEFAULT_MODEL_DIR = Path(__file__).resolve().parents[3] / "models"
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")
PBKDF2_ITERATIONS = 210_000

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
)


class CreateUserPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    email: str = Field(min_length=1)
    password: str = Field(min_length=1)


class CreateCardPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    issuer_code: str = Field(min_length=1)
    country: int = Field(ge=0)
    brand: str = Field(min_length=1)
    type: str = Field(min_length=1)
    bin_code: str = Field(min_length=1)


class CreateUserWithCardPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user: CreateUserPayload
    card: CreateCardPayload


class CreateUserWithCardResponse(BaseModel):
    user_id: str
    card_id: str
    message: str


def local_now() -> datetime:
    return datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0)


def to_local_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def iso_local(value: datetime) -> str:
    return to_local_datetime(value).isoformat()


def card_age_days(card_created_at: datetime, now: datetime) -> float:
    elapsed = now - to_local_datetime(card_created_at)
    return max(elapsed.total_seconds() / 86_400, 0.0)


def hash_password(password: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        b"",
        PBKDF2_ITERATIONS,
    ).hex()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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

    schema_path = DEFAULT_MODEL_DIR / "feature_schema.json"
    with schema_path.open() as schema_file:
        schema = json.load(schema_file)

    try:
        feature_store = RedisFeatureStore(redis_client)
        app.state.database = database
        app.state.redis_client = redis_client
        fraud_detection_service = FraudDetectionService(
            schema=schema,
            feature_store=feature_store,
            database=database,
            threshold=0.5,
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


@app.post(
    "/users",
    response_model=CreateUserWithCardResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_user_with_card(
    payload: CreateUserWithCardPayload,
    request: Request,
) -> CreateUserWithCardResponse:
    user_id = uuid4().hex
    card_id = uuid4().hex
    now = local_now()
    user_created_at = now
    card_created_at = now

    try:
        async with request.app.state.database.transaction() as conn:
            await conn.execute(
                """
                INSERT INTO application.users
                    (id, name, email, password_hash, created_at)
                VALUES ($1, $2, $3, $4, $5)
                """,
                user_id,
                payload.user.name,
                payload.user.email,
                hash_password(payload.user.password),
                user_created_at,
            )
            await conn.execute(
                """
                INSERT INTO application.cards
                    (id, user_id, issuer_code, country, brand, type, bin_code, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                card_id,
                user_id,
                payload.card.issuer_code,
                payload.card.country,
                payload.card.brand,
                payload.card.type,
                payload.card.bin_code,
                card_created_at,
            )
    except asyncpg.UniqueViolationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User, email, or card already exists",
        ) from exc
    except asyncpg.PostgresError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Could not create user and card",
        ) from exc

    features = {
        "no_transactions_30_days": 0,
        "card_age_days": card_age_days(card_created_at, now),
        "card_created_at": iso_local(card_created_at),
    }
    redis_client: aioredis.Redis = request.app.state.redis_client
    async with redis_client.pipeline(transaction=False) as pipeline:
        pipeline.delete(build_transactions_key(user_id, card_id))
        pipeline.hset(build_features_key(user_id, card_id), mapping=features)
        await pipeline.execute()

    return CreateUserWithCardResponse(
        user_id=user_id,
        card_id=card_id,
        message="User and card registered successfully",
    )


@app.post("/score", response_model=FraudDetectionOutputs)
async def score(
    inputs: FraudDetectionInputs,
    service: FraudDetectionService = Depends(get_fraud_detection_service),
) -> FraudDetectionOutputs:
    return await service.predict(inputs)
