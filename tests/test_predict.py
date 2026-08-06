"""Unit tests for ``FraudDetectionService`` (the scoring orchestrator).

The KServe inference endpoint is faked with ``httpx.MockTransport``; Redis
(including the registered velocity Lua scripts), Postgres, Feast and Kafka are
replaced with mocks — no external services are contacted.
"""

from __future__ import annotations

import json
import math
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from fraud_detection.core import predict
from fraud_detection.core.models import FraudDetectionInputs
from fraud_detection.core.predict import FraudDetectionService


def kserve_response(probability: float = 0.35) -> dict:
    return {"outputs": [{"name": "output-0", "data": [probability]}]}


def set_kserve_response(service: FraudDetectionService, body, status_code: int = 200) -> None:
    """Point the service's KServe client at a canned JSON response."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body)

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))


def vector_by_column(service: FraudDetectionService, payload: dict) -> dict:
    """Map the flat KServe input vector back to named feature columns."""
    data = payload["inputs"][0]["data"][0]
    return dict(zip(service.feature_columns, data))


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------

def test_init_requires_kserve_url(monkeypatch, service_env, schema, mock_redis):
    monkeypatch.delenv("KSERVE_URL", raising=False)
    with pytest.raises(ValueError, match="KSERVE_URL"):
        FraudDetectionService(
            schema=schema,
            feature_store=MagicMock(),
            database=MagicMock(),
            redis_client=mock_redis,
        )


@pytest.mark.parametrize(
    "missing",
    [
        "CARD_TRANSACTIONS_KEY",
        "CARD_AGGREGATE_KEY",
        "CARD_DECLINES_KEY",
        "USER_TRANSACTIONS_KEY",
        "USER_AGGREGATE_KEY",
    ],
)
def test_init_requires_every_redis_key(monkeypatch, service_env, schema, mock_redis, missing):
    monkeypatch.delenv(missing, raising=False)
    with pytest.raises(ValueError, match="Redis key"):
        FraudDetectionService(
            schema=schema,
            feature_store=MagicMock(),
            database=MagicMock(),
            redis_client=mock_redis,
        )


def test_init_reads_threshold_and_registers_scripts(service, mock_redis):
    assert service.threshold == 0.8
    assert mock_redis.register_script.call_count == 2
    assert service._card_velocity_script is mock_redis.card_script
    assert service._user_velocity_script is mock_redis.user_script


# ---------------------------------------------------------------------------
# open / close / Kafka producer lifecycle
# ---------------------------------------------------------------------------

async def test_open_opens_all_backends(service, mock_database, mock_feature_store, mock_redis):
    await service.open()
    mock_database.open.assert_awaited_once()
    mock_feature_store.open.assert_awaited_once()
    mock_redis.ping.assert_awaited_once()
    assert service.producer is None  # BOOTSTRAP_SERVERS unset -> no Kafka


async def test_close_closes_all_backends(service, mock_database, mock_feature_store, mock_redis):
    await service.close()
    mock_database.close.assert_awaited_once()
    mock_feature_store.close.assert_awaited_once()
    mock_redis.aclose.assert_awaited_once()


async def test_close_stops_producer_when_started(service):
    producer = MagicMock()
    producer.stop = AsyncMock()
    service.producer = producer
    await service.close()
    producer.stop.assert_awaited_once()


async def test_start_producer_plaintext(monkeypatch, service):
    fake_producer = MagicMock()
    fake_producer.start = AsyncMock()
    producer_cls = MagicMock(return_value=fake_producer)
    monkeypatch.setattr(predict, "AIOKafkaProducer", producer_cls)
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "kafka:9092")

    await service._start_producer()

    assert service.producer is fake_producer
    fake_producer.start.assert_awaited_once()
    assert producer_cls.call_args.kwargs["security_protocol"] == "PLAINTEXT"
    assert producer_cls.call_args.kwargs["ssl_context"] is None


async def test_start_producer_ssl(monkeypatch, service):
    fake_producer = MagicMock()
    fake_producer.start = AsyncMock()
    monkeypatch.setattr(predict, "AIOKafkaProducer", MagicMock(return_value=fake_producer))

    ssl_context = MagicMock()
    create_ctx = MagicMock(return_value=ssl_context)
    monkeypatch.setattr(predict.ssl, "create_default_context", create_ctx)

    monkeypatch.setenv("BOOTSTRAP_SERVERS", "kafka:9093")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SSL_CAFILE", "/certs/ca.pem")
    monkeypatch.setenv("KAFKA_SSL_CERTFILE", "/certs/cert.pem")
    monkeypatch.setenv("KAFKA_SSL_KEYFILE", "/certs/key.pem")

    await service._start_producer()

    assert service.producer is fake_producer
    create_ctx.assert_called_once_with(cafile="/certs/ca.pem")
    ssl_context.load_cert_chain.assert_called_once_with(
        certfile="/certs/cert.pem", keyfile="/certs/key.pem"
    )


async def test_start_producer_failure_disables_publishing(monkeypatch, service):
    fake_producer = MagicMock()
    fake_producer.start = AsyncMock(side_effect=Exception("broker unreachable"))
    monkeypatch.setattr(predict, "AIOKafkaProducer", MagicMock(return_value=fake_producer))
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "kafka:9092")

    await service._start_producer()  # must not raise

    assert service.producer is None


# ---------------------------------------------------------------------------
# predict_with_kserve
# ---------------------------------------------------------------------------

async def test_predict_with_kserve_success(service):
    probability = await service.predict_with_kserve([1.0, 2.0, 3.0])
    assert probability == 0.35

    payload = service.kserve_requests[0]
    inputs = payload["inputs"][0]
    assert inputs["shape"] == [1, 3]
    assert inputs["datatype"] == "FP32"
    assert inputs["data"] == [[1.0, 2.0, 3.0]]


async def test_predict_with_kserve_nan_becomes_null(service):
    await service.predict_with_kserve([float("nan"), 7.0, float("inf")])
    data = service.kserve_requests[0]["inputs"][0]["data"][0]
    assert data == [None, 7.0, None]


async def test_predict_with_kserve_http_error(service):
    set_kserve_response(service, {"error": "boom"}, status_code=500)
    with pytest.raises(RuntimeError, match="status 500"):
        await service.predict_with_kserve([1.0])


async def test_predict_with_kserve_timeout(service):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match="timed out"):
        await service.predict_with_kserve([1.0])


async def test_predict_with_kserve_connection_error(service):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match="request failed"):
        await service.predict_with_kserve([1.0])


@pytest.mark.parametrize(
    "body",
    [
        {},                                        # no outputs at all
        {"outputs": "not-a-list"},                 # outputs has the wrong type
        {"outputs": []},                           # empty outputs
        {"outputs": [{"name": "o"}]},              # first output has no data
        {"outputs": [{"data": []}]},               # data is empty
        {"outputs": ["scalar"]},                   # output entry is not a dict
        {"outputs": [{"data": ["not-a-float"]}]},  # value not castable to float
    ],
)
async def test_predict_with_kserve_invalid_response(service, body):
    set_kserve_response(service, body)
    with pytest.raises(RuntimeError, match="invalid"):
        await service.predict_with_kserve([1.0])


# ---------------------------------------------------------------------------
# predict — the full scoring pipeline
# ---------------------------------------------------------------------------

async def test_predict_happy_path(service, transaction_payload, mock_redis):
    inputs = FraudDetectionInputs(**transaction_payload)
    outputs = await service.predict(inputs)

    assert outputs.transaction_id == "tx-100"
    assert outputs.probability == 0.35
    assert outputs.prediction == 0  # 0.35 < threshold 0.8

    # Both velocity scripts ran with the shared member "tx_id|amount".
    card_call = mock_redis.card_script.await_args.kwargs
    user_call = mock_redis.user_script.await_args.kwargs
    assert card_call["keys"] == [
        "card:transactions:card-1",
        "card:declines:card-1",
        "card:aggregate:card-1",
    ]
    assert user_call["keys"] == ["user:transactions:user-1", "user:aggregate:user-1"]
    assert card_call["args"][2] == "tx-100|50.0"


async def test_predict_derived_features_reach_the_model(service, transaction_payload):
    """The encoded KServe vector carries every request-time derived feature."""
    inputs = FraudDetectionInputs(**transaction_payload)
    await service.predict(inputs)

    vector = vector_by_column(service, service.kserve_requests[0])

    # Direct transaction attributes.
    assert vector["amount_usd"] == 50.0
    assert vector["log_amount"] == pytest.approx(math.log(51.0))
    # 2026-07-01T13:30Z: 13h UTC, a Wednesday, not night.
    assert vector["hour"] == 13.0
    assert vector["weekday"] == 2.0
    assert vector["is_night"] == 0.0
    # Categoricals encoded via the schema encoders.
    assert vector["channel"] == 0.0            # "web"
    assert vector["card_brand"] == 0.0         # "visa" (from the online store)
    assert vector["merchant_category"] == 0.0  # "electronics"
    assert vector["merchant_risk_level"] == 3.0
    # Cross-field flags.
    assert vector["geo_mismatch"] == 0.0       # billing VN == ip VN
    assert vector["foreign_ip"] == 0.0         # ip VN == user_country VN
    assert vector["recipient_differs"] == 1.0  # gmail.com != example.com
    # Ages relative to the online-store creation timestamps.
    assert vector["account_age_days"] == 395.0
    assert vector["card_age_days"] == 181.0
    # Velocity features derived from the Lua script results (+1 for this tx).
    assert vector["card_tx_count_1h"] == 2.0
    assert vector["card_tx_count_24h"] == 4.0
    assert vector["card_amount_sum_24h"] == 200.0
    assert vector["card_seconds_since_last_tx"] == 12600.0
    assert vector["card_amount_zscore"] == 0.0  # amount 50 == card mean 50
    assert vector["card_tx_seq"] == 4.0
    assert vector["card_declines_24h"] == 0.0
    assert vector["user_tx_count_24h"] == 3.0
    assert vector["user_amount_sum_24h"] == 140.0
    assert vector["user_seconds_since_last_tx"] == 12600.0


async def test_predict_flags_geo_mismatch_and_night(service, transaction_payload):
    transaction_payload["ip_country_code"] = "US"
    transaction_payload["timestamp"] = "2026-07-01T23:30:00Z"
    inputs = FraudDetectionInputs(**transaction_payload)

    await service.predict(inputs)
    vector = vector_by_column(service, service.kserve_requests[0])

    assert vector["geo_mismatch"] == 1.0
    assert vector["foreign_ip"] == 1.0  # ip US != user_country VN
    assert vector["is_night"] == 1.0
    assert vector["hour"] == 23.0


async def test_predict_above_threshold_flags_fraud(service, transaction_payload):
    set_kserve_response(service, kserve_response(probability=0.93))
    outputs = await service.predict(FraudDetectionInputs(**transaction_payload))
    assert outputs.probability == 0.93
    assert outputs.prediction == 1


async def test_predict_at_threshold_flags_fraud(service, transaction_payload):
    """The decision rule is ``probability >= threshold`` (boundary inclusive)."""
    set_kserve_response(service, kserve_response(probability=0.8))
    outputs = await service.predict(FraudDetectionInputs(**transaction_payload))
    assert outputs.prediction == 1


async def test_predict_zscore_nan_with_single_prior_tx(service, transaction_payload, mock_redis):
    # n < 2 -> no variance estimate -> NaN -> null in the KServe payload.
    mock_redis.card_script.return_value = [0, 1, "40.0", 0, 1, "40.0", "1600.0", None]
    await service.predict(FraudDetectionInputs(**transaction_payload))

    vector = vector_by_column(service, service.kserve_requests[0])
    assert vector["card_amount_zscore"] is None
    assert vector["card_seconds_since_last_tx"] is None  # no last_txn_at either


async def test_predict_zscore_nan_with_zero_variance(service, transaction_payload, mock_redis):
    # Three identical 50 USD transactions -> variance 0 -> NaN.
    mock_redis.card_script.return_value = [1, 3, "150.0", 0, 3, "150.0", "7500.0", "1782900000.0"]
    await service.predict(FraudDetectionInputs(**transaction_payload))

    vector = vector_by_column(service, service.kserve_requests[0])
    assert vector["card_amount_zscore"] is None


async def test_predict_warns_when_no_features_materialised(service, transaction_payload, mock_feature_store, online_features):
    """A pair with nothing materialised hits the warning branch, then fails on
    the (None) creation timestamps while deriving features."""
    mock_feature_store.get_online_features = AsyncMock(
        return_value={key: None for key in online_features}
    )
    with pytest.raises(RuntimeError, match="derived features"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_feature_store_failure(service, transaction_payload, mock_feature_store):
    mock_feature_store.get_online_features = AsyncMock(side_effect=Exception("redis down"))
    with pytest.raises(RuntimeError, match="online features"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_bad_timestamp_fails_derived_features(service, transaction_payload):
    transaction_payload["timestamp"] = "not-a-date"
    with pytest.raises(RuntimeError, match="derived features"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_build_inputs_failure(monkeypatch, service, transaction_payload):
    monkeypatch.setattr(
        predict, "build_model_inputs", MagicMock(side_effect=KeyError("boom"))
    )
    with pytest.raises(RuntimeError, match="build model inputs"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_inference_failure(service, transaction_payload):
    set_kserve_response(service, {"error": "boom"}, status_code=503)
    with pytest.raises(RuntimeError, match="inference"):
        await service.predict(FraudDetectionInputs(**transaction_payload))


async def test_predict_publishes_to_kafka(service, transaction_payload):
    producer = MagicMock()
    producer.send_and_wait = AsyncMock()
    service.producer = producer

    outputs = await service.predict(FraudDetectionInputs(**transaction_payload))

    producer.send_and_wait.assert_awaited_once()
    call = producer.send_and_wait.await_args
    assert call.args[0] == "predictions"
    message = json.loads(call.kwargs["value"].decode("utf-8"))
    assert message["transaction_id"] == "tx-100"
    assert message["fraud_score"] == outputs.probability
    assert message["prediction"] == outputs.prediction
    assert message["threshold"] == 0.8
    assert call.kwargs["key"] == message["request_id"].encode()


async def test_predict_kafka_failure_does_not_abort_scoring(service, transaction_payload):
    producer = MagicMock()
    producer.send_and_wait = AsyncMock(side_effect=Exception("kafka down"))
    service.producer = producer

    outputs = await service.predict(FraudDetectionInputs(**transaction_payload))

    assert outputs.transaction_id == "tx-100"
    assert outputs.prediction == 0
