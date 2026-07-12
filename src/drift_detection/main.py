"""FastAPI application for ``amount_usd`` drift detection.

Wires together the training baseline (parquet) and Postgres via the app lifespan
and exposes HTTP endpoints for health/readiness probes and PSI drift detection.
Mirrors the structure of the ``fraud_detection`` service.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status

from database import PostgresDatabase
from drift_detection.detector import DEFAULT_THRESHOLD
from drift_detection.schemas import DriftResult
from drift_detection.service import DriftDetectionService, InsufficientDataError

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
)

logger = structlog.get_logger(__name__)

# Repo layout: src/drift_detection/main.py → parents[2] is the repo root.
DEFAULT_BASELINE_PATH = Path(__file__).resolve().parents[2] / "datase" / "training_data.parquet"
BASELINE_DATA_PATH = os.getenv("BASELINE_DATA_PATH", str(DEFAULT_BASELINE_PATH))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the :class:`DriftDetectionService` (baseline + Postgres) and manage its lifecycle."""
    database = PostgresDatabase.from_env()
    service = DriftDetectionService(
        baseline_path=BASELINE_DATA_PATH,
        database=database,
        threshold=DEFAULT_THRESHOLD,
    )
    try:
        await service.open()
        app.state.drift_detection_service = service
        logger.info("drift_detection_service_ready", baseline_rows=service.baseline_rows)
        yield
    finally:
        await service.close()


app = FastAPI(
    title="Fraud Detection – Drift Detection API",
    description="Detects data drift on the **amount_usd** column using the Population Stability Index (PSI).",
    version="1.0.0",
    lifespan=lifespan,
)


def get_drift_detection_service(request: Request) -> DriftDetectionService:
    """FastAPI dependency returning the shared ``DriftDetectionService`` from app state."""
    return request.app.state.drift_detection_service


@app.get("/health", tags=["Monitoring"])
async def health() -> dict[str, str]:
    """Liveness probe returning a static ``ok`` status."""
    return {"status": "ok"}


@app.get("/ready", tags=["Monitoring"])
async def ready(
    service: DriftDetectionService = Depends(get_drift_detection_service),
) -> dict:
    """Readiness probe: verifies Postgres connectivity and that the baseline is loaded."""
    if service.baseline_rows == 0:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Baseline not loaded.")
    try:
        await service.database.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unreachable."
        ) from exc
    return {"status": "ok", "baseline_rows": service.baseline_rows}


@app.get("/detect", response_model=DriftResult, tags=["Drift"])
async def detect(
    threshold: float = Query(default=DEFAULT_THRESHOLD, gt=0, le=1),
    service: DriftDetectionService = Depends(get_drift_detection_service),
) -> DriftResult:
    """Query ``amount_usd`` from Postgres (last 30 days) and run the PSI drift test against the training baseline."""
    try:
        result = await service.detect(threshold=threshold)
    except InsufficientDataError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("drift_detection_failed", error=str(exc))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return DriftResult(**result)
