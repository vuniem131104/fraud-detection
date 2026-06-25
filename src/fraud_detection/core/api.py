"""FastAPI application exposing the fraud detection service.

Wires together Postgres, Redis and the ``FraudDetectionService`` via the app
lifespan, and exposes HTTP endpoints for health/readiness probes, user/card
registration, and transaction scoring. Also provides small datetime, password
hashing and dependency-injection helpers used by the route handlers.
"""

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
    """Request body fields describing a user to be created."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    email: str = Field(min_length=1)
    password: str = Field(min_length=1)


class CreateCardPayload(BaseModel):
    """Request body fields describing a card to be created."""

    model_config = ConfigDict(extra="forbid")

    issuer_code: str = Field(min_length=1)
    country: int = Field(ge=0)
    brand: str = Field(min_length=1)
    type: str = Field(min_length=1)
    bin_code: str = Field(min_length=1)


class CreateUserWithCardPayload(BaseModel):
    """Request body for registering a user together with their card."""

    model_config = ConfigDict(extra="forbid")

    user: CreateUserPayload
    card: CreateCardPayload


class CreateUserWithCardResponse(BaseModel):
    """Response returned after creating a user and card."""

    user_id: str
    card_id: str
    message: str


def local_now() -> datetime:
    """Return the current time in the Ho Chi Minh timezone, truncated to seconds."""
    return datetime.now(HO_CHI_MINH_TZ).replace(microsecond=0)


def to_local_datetime(value: datetime) -> datetime:
    """Convert a datetime to the Ho Chi Minh timezone.

    Naive datetimes are assumed to already be in the Ho Chi Minh timezone and
    are tagged accordingly; aware datetimes are converted to it.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def iso_local(value: datetime) -> str:
    """Return the ISO 8601 string of ``value`` expressed in the Ho Chi Minh timezone."""
    return to_local_datetime(value).isoformat()


def card_age_days(card_created_at: datetime, now: datetime) -> float:
    """Return the card's age in days, clamped to be non-negative.

    Args:
        card_created_at: When the card was created.
        now: The reference time to measure the age against.

    Returns:
        The elapsed time in days, never less than ``0.0``.
    """
    elapsed = now - to_local_datetime(card_created_at)
    return max(elapsed.total_seconds() / 86_400, 0.0)


def hash_password(password: str) -> str:
    """Hash a password using PBKDF2-HMAC-SHA256 and return the hex digest.

    Note:
        Uses an empty salt and a fixed iteration count (``PBKDF2_ITERATIONS``).
    """
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        b"",
        PBKDF2_ITERATIONS,
    ).hex()


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


@app.post(
    "/users",
    response_model=CreateUserWithCardResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_user_with_card(
    payload: CreateUserWithCardPayload,
    request: Request,
) -> CreateUserWithCardResponse:
    """Register a new user and their card.

    Inserts the user and card in a single Postgres transaction and seeds the
    initial Redis feature state (transaction count, card age and creation time)
    for the new user-card pair.

    Args:
        payload: The user and card details to create.
        request: The incoming request, used to access shared app state.

    Returns:
        The generated user and card identifiers with a success message.

    Raises:
        HTTPException: ``409 Conflict`` if the user, email or card already
            exists, or ``422 Unprocessable Entity`` for other database errors.
    """
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
    """Score a single transaction for fraud and return the prediction."""
    return await service.predict(inputs)
