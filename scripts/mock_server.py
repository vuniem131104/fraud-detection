"""Lightweight mock server for load testing (no Postgres / Redis / KServe needed).

Starts a minimal FastAPI app that exposes /health and /score with realistic
response structures, so locust can load-test the HTTP layer independently of
external services.

Usage:
    python scripts/mock_server.py
    # Then in another terminal:
    locust -f tests/locust_load_test.py --host http://localhost:8000 ...
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI(title="Mock Fraud Detection API (load test only)")


class MockScoreResponse(BaseModel):
    tx_id: str
    probability: float
    prediction: int


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    return {"status": "ready"}


@app.post("/score", response_model=MockScoreResponse)
async def score(request: Request):
    # Return a deterministic mock response without hitting any real model
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    tx_id = payload.get("tx_id", "mock-tx")
    amount = float(payload.get("amount_usd", 1.0))
    # Simple mock: flag as fraud if amount > 500
    probability = 0.85 if amount > 500 else 0.12
    prediction = 1 if probability >= 0.5 else 0
    return MockScoreResponse(tx_id=tx_id, probability=probability, prediction=prediction)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
