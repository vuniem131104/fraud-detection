"""Unit tests for ``FraudDetectionService`` (the scoring orchestrator).

The KServe inference engine is faked with ``httpx.MockTransport``; Redis,
Postgres, Kafka and the (CPU-bound) feature builder are replaced with mocks, so
no external services are contacted.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import httpx
import pandas as pd
import pytest

from fraud_detection.core import predict
from fraud_detection.core.models import FraudDetectionInputs
from fraud_detection.core.predict import FraudDetectionService


@pytest.fixture
def service(kserve_env, schema) -> FraudDetectionService:
    """A service instance with mocked feature store / database (no open() called)."""
    feature_store = MagicMock()
    feature_store.get_txs = AsyncMock()
    feature_store.redis_client = MagicMock()
    database = MagicMock()
    database.execute = AsyncMock(return_value="INSERT 0 1")
    return FraudDetectionService(
        schema=schema, feature_store=feature_store, database=database, threshold=0.5
    )


# ---------------------------------------------------------------------------
# __init__ / to_float
# ---------------------------------------------------------------------------

def test_init_requires_kserve_url(monkeypatch, schema):
    monkeypatch.delenv("KSERVE_URL", raising=False)
    with pytest.raises(ValueError, match="KSERVE_URL"):
        FraudDetectionService(schema=schema, feature_store=MagicMock(), database=MagicMock())


def test_to_float_static():
    assert FraudDetectionService.to_float(None, "f") == 0.0
    assert FraudDetectionService.to_float("", "f") == 0.0
    assert FraudDetectionService.to_float(float("nan"), "f") == 0.0
    assert FraudDetectionService.to_float(float("inf"), "f", default=2.0) == 2.0
    assert FraudDetectionService.to_float("5", "f") == 5.0
    with pytest.raises(ValueError):
        FraudDetectionService.to_float("abc", "f")


# ---------------------------------------------------------------------------
# predict_with_kserve
# ---------------------------------------------------------------------------

async def test_predict_with_kserve_success(service):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"outputs": [{"name": "output-0", "data": [0.77]}]})

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    probability = await service.predict_with_kserve(pd.DataFrame([{"a": 1.0, "b": 2.0}]))

    assert probability == pytest.approx(0.77)
    body = captured["body"]["inputs"][0]
    assert body["shape"] == [1, 2]
    assert body["datatype"] == "FP32"
    assert body["data"] == [[1.0, 2.0]]
    await service.kserve_client.aclose()


async def test_predict_with_kserve_http_status_error(service):
    service.kserve_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500, text="boom"))
    )
    with pytest.raises(RuntimeError, match="status 500"):
        await service.predict_with_kserve(pd.DataFrame([{"a": 1.0}]))
    await service.kserve_client.aclose()


async def test_predict_with_kserve_invalid_response(service):
    service.kserve_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"nope": 1}))
    )
    with pytest.raises(RuntimeError, match="invalid"):
        await service.predict_with_kserve(pd.DataFrame([{"a": 1.0}]))
    await service.kserve_client.aclose()


async def test_predict_with_kserve_empty_data(service):
    service.kserve_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"outputs": [{"name": "o", "data": []}]})
        )
    )
    with pytest.raises(RuntimeError, match="invalid"):
        await service.predict_with_kserve(pd.DataFrame([{"a": 1.0}]))
    await service.kserve_client.aclose()


async def test_predict_with_kserve_timeout(service):
    def handler(request):
        raise httpx.TimeoutException("slow")

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match="timed out"):
        await service.predict_with_kserve(pd.DataFrame([{"a": 1.0}]))
    await service.kserve_client.aclose()


async def test_predict_with_kserve_request_error(service):
    def handler(request):
        raise httpx.ConnectError("refused")

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match="request failed"):
        await service.predict_with_kserve(pd.DataFrame([{"a": 1.0}]))
    await service.kserve_client.aclose()


# ---------------------------------------------------------------------------
# predict (full pipeline, everything mocked)
# ---------------------------------------------------------------------------

async def test_predict_full_pipeline(service, monkeypatch, transaction_payload):
    service.feature_store.get_txs = AsyncMock(
        return_value={
            "user_id": "user-1", "card_id": "c",
            "features": {"no_transactions_30_days": 2}, "transactions": [{"x": 1}],
        }
    )
    monkeypatch.setattr(
        predict, "build_model_inputs", lambda payload, schema: pd.DataFrame([{"f1": 1.0, "f2": 2.0}])
    )
    service.predict_with_kserve = AsyncMock(return_value=0.9)
    service.producer = AsyncMock()

    result = await service.predict(FraudDetectionInputs(**transaction_payload))

    assert result.tx_id == "tx-100"
    assert result.probability == 0.9
    assert result.prediction == 1                      # 0.9 >= threshold 0.5
    service.feature_store.get_txs.assert_awaited_once()
    service.predict_with_kserve.assert_awaited_once()
    service.database.execute.assert_awaited_once()     # feature snapshot persisted
    service.producer.send_and_wait.assert_awaited_once()


async def test_predict_missing_identifiers_raises(service):
    fake_inputs = MagicMock()
    fake_inputs.model_dump.return_value = {"tx_id": "t", "card_id": "c"}  # no user_id
    with pytest.raises(ValueError, match="user_id and card_id"):
        await service.predict(fake_inputs)


async def test_predict_build_failure_raises(service, monkeypatch, transaction_payload):
    service.feature_store.get_txs = AsyncMock(
        return_value={"features": {}, "transactions": []}  # also exercises the "empty" warning
    )

    def boom(payload, schema):
        raise RuntimeError("bad features")

    monkeypatch.setattr(predict, "build_model_inputs", boom)
    with pytest.raises(RuntimeError, match="Failed to build model inputs"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_tolerates_snapshot_and_kafka_failures(
    service, monkeypatch, transaction_payload
):
    service.feature_store.get_txs = AsyncMock(
        return_value={"features": {"no_transactions_30_days": 1}, "transactions": [{"x": 1}]}
    )
    monkeypatch.setattr(predict, "build_model_inputs", lambda p, s: pd.DataFrame([{"f1": 1.0}]))
    service.predict_with_kserve = AsyncMock(return_value=0.2)        # below threshold
    service.database.execute = AsyncMock(side_effect=Exception("db down"))
    service.producer = AsyncMock()
    service.producer.send_and_wait = AsyncMock(side_effect=Exception("kafka down"))

    result = await service.predict(FraudDetectionInputs(**transaction_payload))

    assert result.prediction == 0
    assert result.probability == 0.2


# ---------------------------------------------------------------------------
# open / close lifecycle
# ---------------------------------------------------------------------------

async def test_open(service, monkeypatch):
    service.database.open = AsyncMock()
    service.feature_store.redis_client.ping = AsyncMock()
    service.kserve_client = AsyncMock()
    monkeypatch.setattr(predict.ssl, "create_default_context", lambda **kw: MagicMock())
    fake_producer = AsyncMock()
    monkeypatch.setattr(predict, "AIOKafkaProducer", MagicMock(return_value=fake_producer))

    await service.open()

    service.database.open.assert_awaited_once()
    service.feature_store.redis_client.ping.assert_awaited_once()
    fake_producer.start.assert_awaited_once()


async def test_close(service):
    service.database.close = AsyncMock()
    service.feature_store.redis_client.aclose = AsyncMock()
    service.kserve_client = AsyncMock()
    service.producer = AsyncMock()

    await service.close()

    service.database.close.assert_awaited_once()
    service.feature_store.redis_client.aclose.assert_awaited_once()
    service.producer.stop.assert_awaited_once()


async def test_close_without_producer(service):
    """close() is safe when the Kafka producer was never started."""
    service.database.close = AsyncMock()
    service.feature_store.redis_client.aclose = AsyncMock()
    service.kserve_client = AsyncMock()
    service.producer = None

    await service.close()  # must not raise

    service.database.close.assert_awaited_once()
