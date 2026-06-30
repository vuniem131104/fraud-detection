"""FastAPI application for TransactionAmt drift detection."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import pandas as pd
import structlog
from fastapi import FastAPI, HTTPException, Query, status

from ..database.postgres import PostgresDatabase
from .detector import DEFAULT_THRESHOLD, DriftDetector
from .repository import fetch_amounts_last_7_days
from .schemas import DriftResult, HealthResponse

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
)

logger = structlog.get_logger(__name__)

_DEFAULT_CSV = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "dataset", "train_transaction.csv")
)
CSV_PATH = os.environ.get("BASELINE_CSV_PATH", _DEFAULT_CSV)

_detector: DriftDetector | None = None
_db: PostgresDatabase | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _detector, _db

    baseline_df = pd.read_csv(CSV_PATH, usecols=["TransactionAmt"])
    _detector = DriftDetector(baseline_df, threshold=DEFAULT_THRESHOLD)
    logger.info("baseline_loaded", rows=_detector.baseline_rows, mean=round(_detector.baseline_mean, 2))

    _db = PostgresDatabase.from_env()
    await _db.open()
    logger.info("postgres_pool_opened", host=_db.host)

    yield

    await _db.close()
    _db = None
    _detector = None


app = FastAPI(
    title="Fraud Detection – Drift Detection API",
    description="Detects data drift on the **TransactionAmt** column using the Kolmogorov-Smirnov 2-sample test.",
    version="1.0.0",
    lifespan=lifespan,
)


def _require_detector() -> DriftDetector:
    if _detector is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Detector not initialised.")
    return _detector


def _require_db() -> PostgresDatabase:
    if _db is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database not initialised.")
    return _db


@app.get("/health", tags=["Monitoring"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready", tags=["Monitoring"])
async def ready() -> dict:
    """Readiness probe: verifies Postgres connectivity and that the baseline is loaded."""
    if _detector is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Baseline not loaded.")
    if _db is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database not initialised.")
    try:
        async with _db.connection() as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database unreachable.") from exc
    return {"status": "ok", "baseline_rows": _detector.baseline_rows}



@app.get("/detect", response_model=DriftResult, tags=["Drift"])
async def detect(
    threshold: float = Query(default=DEFAULT_THRESHOLD, gt=0, le=1),
) -> DriftResult:
    """Query `amount_usd` from Postgres (last 7 days) and run PSI drift test against the training baseline."""
    detector = _require_detector()
    db = _require_db()
    detector._threshold = threshold

    async with db.connection() as conn:
        amounts = await fetch_amounts_last_7_days(conn)

    if len(amounts) < 30:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Only {len(amounts)} rows found in the last 7 days. Need at least 30.",
        )

    try:
        result = detector.detect(amounts)
    except Exception as exc:
        logger.exception("drift_detection_failed", error=str(exc))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return DriftResult(**result)
