"""Property-based and idempotency tests using Hypothesis.

These tests verify *invariants* — properties that must hold for randomly
generated inputs — rather than fixed example-based assertions, which is far
better at surfacing edge-case bugs than manual test design.

Properties verified:
  1. ``to_number`` is idempotent: applying it twice equals applying it once.
  2. ``to_number`` is total: it never raises and always returns a float that is
     either finite or NaN.
  3. ``encode_categorical`` is deterministic and closed over the encoder: the
     result is either NaN or one of the encoder's own values.
  4. ``build_model_inputs`` always returns exactly the schema's feature columns
     (same order), all floats, and is idempotent/deterministic — the same
     features always encode to the same model vector.
  5. ``build_model_inputs`` ignores irrelevant extra keys.
  6. **Model prediction is idempotent**: scoring the same transaction twice
     against a deterministic model yields the identical probability and
     prediction, and the prediction always equals
     ``int(probability >= threshold)``.
"""

from __future__ import annotations

import asyncio
import math
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from fraud_detection.core import utils
from fraud_detection.core.models import FraudDetectionInputs
from fraud_detection.core.predict import FraudDetectionService

settings.register_profile(
    "ci",
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
settings.load_profile("ci")


def same_float(a: float, b: float) -> bool:
    """Equality that treats NaN == NaN (both encode 'missing')."""
    return (math.isnan(a) and math.isnan(b)) or a == b


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

anything = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(10**12), max_value=10**12),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=30),
    st.lists(st.integers(), max_size=3),
)

feature_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(10**9), max_value=10**9),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=20),
)


# ---------------------------------------------------------------------------
# 1–2. to_number
# ---------------------------------------------------------------------------

@given(value=anything)
def test_to_number_is_idempotent(value):
    once = utils.to_number(value)
    twice = utils.to_number(once)
    assert same_float(once, twice)


@given(value=anything)
def test_to_number_is_total(value):
    result = utils.to_number(value)
    assert isinstance(result, float)
    assert math.isfinite(result) or math.isnan(result)


# ---------------------------------------------------------------------------
# 3. encode_categorical
# ---------------------------------------------------------------------------

@given(
    value=st.one_of(st.none(), st.integers(), st.text(max_size=20)),
    encoder=st.dictionaries(st.text(max_size=10), st.integers(min_value=0, max_value=50), max_size=8),
)
def test_encode_categorical_is_deterministic_and_closed(value, encoder):
    first = utils.encode_categorical(value, encoder)
    second = utils.encode_categorical(value, encoder)
    assert same_float(first, second)
    assert math.isnan(first) or first in {float(v) for v in encoder.values()}


# ---------------------------------------------------------------------------
# 4–5. build_model_inputs
# ---------------------------------------------------------------------------

@given(features=st.dictionaries(st.text(max_size=15), feature_values, max_size=30))
def test_build_model_inputs_schema_is_stable(schema, features):
    encoded = utils.build_model_inputs(features, schema)

    assert list(encoded) == schema["feature_columns"]
    assert all(isinstance(value, float) for value in encoded.values())

    again = utils.build_model_inputs(features, schema)
    assert all(same_float(encoded[c], again[c]) for c in encoded)


@given(
    features=st.dictionaries(st.text(max_size=15), feature_values, max_size=10),
    noise=st.dictionaries(st.text(min_size=16, max_size=20), feature_values, max_size=10),
)
def test_build_model_inputs_ignores_extra_keys(schema, features, noise):
    """Keys outside the schema columns can never influence the vector.

    Noise keys are ≥16 chars while feature keys are ≤15, so noise can't
    accidentally overwrite a feature key; schema columns themselves are
    filtered out of the noise explicitly.
    """
    noise = {k: v for k, v in noise.items() if k not in schema["feature_columns"]}
    baseline = utils.build_model_inputs(features, schema)
    with_noise = utils.build_model_inputs({**features, **noise}, schema)
    assert all(same_float(baseline[c], with_noise[c]) for c in baseline)


# ---------------------------------------------------------------------------
# 6. Model prediction idempotency (end-to-end through the service)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _service_environment(monkeypatch):
    monkeypatch.setenv("KSERVE_URL", "http://kserve.local/v2/models/fraud/infer")
    monkeypatch.setenv("DECISION_THRESHOLD", "0.8")
    monkeypatch.setenv("CARD_TRANSACTIONS_KEY", "card:transactions")
    monkeypatch.setenv("CARD_AGGREGATE_KEY", "card:aggregate")
    monkeypatch.setenv("CARD_DECLINES_KEY", "card:declines")
    monkeypatch.setenv("USER_TRANSACTIONS_KEY", "user:transactions")
    monkeypatch.setenv("USER_AGGREGATE_KEY", "user:aggregate")


def make_deterministic_service(schema: dict[str, Any]) -> FraudDetectionService:
    """A fresh service whose backends are pure functions of the request.

    The fake KServe model returns a probability derived deterministically from
    the feature vector, so identical transactions must produce identical
    predictions — any difference can only come from the service itself.
    """
    redis = MagicMock()
    redis.register_script = MagicMock(
        side_effect=[
            AsyncMock(return_value=[1, 3, "150.0", 0, 3, "150.0", "7700.0", "1782900000.0"]),
            AsyncMock(return_value=[2, "90.0", "1782900000.0"]),
        ]
    )

    feature_store = MagicMock()
    feature_store.get_online_features = AsyncMock(
        return_value={
            "card_brand": "visa",
            "card_type": "credit",
            "is_virtual": False,
            "customer_segment": "retail",
            "kyc_level": 2,
            "email_verified": True,
            "account_created_at": "2025-06-01T00:00:00Z",
            "card_created_at": "2026-01-01T00:00:00Z",
            "user_country": "VN",
        }
    )

    service = FraudDetectionService(
        schema=schema,
        feature_store=feature_store,
        database=MagicMock(),
        redis_client=redis,
    )

    def fake_model(request: httpx.Request) -> httpx.Response:
        import json as _json

        vector = _json.loads(request.content)["inputs"][0]["data"][0]
        checksum = sum(v for v in vector if v is not None)
        probability = round((abs(checksum) % 997) / 997, 6)
        return httpx.Response(200, json={"outputs": [{"data": [probability]}]})

    service.kserve_client = httpx.AsyncClient(transport=httpx.MockTransport(fake_model))
    return service


@given(
    amount=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False, allow_infinity=False),
    risk_level=st.integers(min_value=0, max_value=10),
    channel=st.sampled_from(["web", "mobile", "pos", "unseen-channel"]),
    merchant_category=st.sampled_from(["electronics", "travel", "grocery", "unseen-cat"]),
    hour=st.integers(min_value=0, max_value=23),
    ip_country=st.sampled_from(["VN", "US"]),
)
def test_model_prediction_is_idempotent(
    schema, amount, risk_level, channel, merchant_category, hour, ip_country
):
    inputs = FraudDetectionInputs(
        transaction_id="tx-prop",
        user_id="user-1",
        card_id="card-1",
        merchant_category=merchant_category,
        merchant_risk_level=risk_level,
        amount_usd=amount,
        timestamp=f"2026-07-01T{hour:02d}:15:00Z",
        channel=channel,
        billing_country_code="VN",
        ip_country_code=ip_country,
        email_purchaser="buyer@gmail.com",
        email_recipient="seller@example.com",
    )

    async def score_twice():
        first = await make_deterministic_service(schema).predict(inputs)
        second = await make_deterministic_service(schema).predict(inputs)
        return first, second

    first, second = asyncio.run(score_twice())

    # Idempotency: the same transaction always scores identically.
    assert first.probability == second.probability
    assert first.prediction == second.prediction
    assert first.transaction_id == second.transaction_id == "tx-prop"

    # The decision rule is exactly `probability >= threshold`.
    assert first.prediction == int(first.probability >= 0.8)
    assert 0.0 <= first.probability <= 1.0
