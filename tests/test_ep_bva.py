"""Equivalence Partitioning (EP) and Boundary Value Analysis (BVA) tests.

Each ``pytest.mark.parametrize`` block documents the partition/boundary design
so the intent is traceable:
  - EP  = a representative value from an equivalence class
  - BVA = a value at or immediately adjacent to a boundary between classes

Targets the request validation of the Web API (``FraudDetectionInputs``), the
feature-encoding helpers and the fraud decision rule.
"""

from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from fraud_detection.core import utils
from fraud_detection.core.models import FraudDetectionInputs


# ---------------------------------------------------------------------------
# amount_usd — constraint: amount_usd > 0
# Partitions:  EP1 negative (invalid) | EP2 zero (invalid) | EP3 positive (valid)
# Boundaries:  0 (excluded), smallest positive float above 0, large valid value
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "amount, valid",
    [
        pytest.param(-1_000_000.0, False, id="EP1-large-negative"),
        pytest.param(-10.0, False, id="EP1-negative"),
        pytest.param(-0.01, False, id="BVA-just-below-zero"),
        pytest.param(0.0, False, id="BVA-zero-boundary-excluded"),
        pytest.param(0.001, True, id="BVA-just-above-zero"),
        pytest.param(99.5, True, id="EP3-typical"),
        pytest.param(1_000_000.0, True, id="BVA-very-large-valid"),
    ],
)
def test_amount_usd_partitions(transaction_payload, amount, valid):
    transaction_payload["amount_usd"] = amount
    if valid:
        assert FraudDetectionInputs(**transaction_payload).amount_usd == amount
    else:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**transaction_payload)


# ---------------------------------------------------------------------------
# merchant_risk_level — constraint: 0 <= merchant_risk_level <= 10
# Partitions:  EP1 below range | EP2 in range | EP3 above range
# Boundaries:  -1 / 0 (lower edge), 10 / 11 (upper edge)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "level, valid",
    [
        pytest.param(-100, False, id="EP1-far-below"),
        pytest.param(-1, False, id="BVA-just-below-min"),
        pytest.param(0, True, id="BVA-min-inclusive"),
        pytest.param(1, True, id="BVA-just-above-min"),
        pytest.param(5, True, id="EP2-mid-range"),
        pytest.param(9, True, id="BVA-just-below-max"),
        pytest.param(10, True, id="BVA-max-inclusive"),
        pytest.param(11, False, id="BVA-just-above-max"),
        pytest.param(100, False, id="EP3-far-above"),
    ],
)
def test_merchant_risk_level_partitions(transaction_payload, level, valid):
    transaction_payload["merchant_risk_level"] = level
    if valid:
        assert FraudDetectionInputs(**transaction_payload).merchant_risk_level == level
    else:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**transaction_payload)


# ---------------------------------------------------------------------------
# String identifiers — constraint: min_length = 1
# Partitions:  EP1 empty (invalid) | EP2 non-empty (valid)
# Boundaries:  length 0 / length 1
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", ["transaction_id", "user_id", "card_id", "channel"])
@pytest.mark.parametrize(
    "value, valid",
    [
        pytest.param("", False, id="BVA-length-0"),
        pytest.param("x", True, id="BVA-length-1"),
        pytest.param("x" * 256, True, id="EP2-long-string"),
    ],
)
def test_identifier_length_partitions(transaction_payload, field, value, valid):
    transaction_payload[field] = value
    if valid:
        assert getattr(FraudDetectionInputs(**transaction_payload), field) == value
    else:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**transaction_payload)


# ---------------------------------------------------------------------------
# email_purchaser — EmailStr
# Partitions:  EP1 well-formed address | EP2 structurally broken address
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "email, valid",
    [
        pytest.param("a@b.co", True, id="EP1-minimal-valid"),
        pytest.param("user.name+tag@example.com", True, id="EP1-plus-tag"),
        pytest.param("", False, id="EP2-empty"),
        pytest.param("plainaddress", False, id="EP2-no-at"),
        pytest.param("@no-local-part.com", False, id="EP2-missing-local"),
        pytest.param("user@", False, id="EP2-missing-domain"),
        pytest.param("user@@double.com", False, id="EP2-double-at"),
    ],
)
def test_email_partitions(transaction_payload, email, valid):
    transaction_payload["email_purchaser"] = email
    if valid:
        FraudDetectionInputs(**transaction_payload)
    else:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**transaction_payload)


# ---------------------------------------------------------------------------
# utils.to_number
# Partitions:  EP1 None | EP2 blank string | EP3 unparseable | EP4 valid number
# Boundaries:  0, ±inf (finite/non-finite edge), INT32_MAX
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="EP1-none"),
        pytest.param("", id="EP2-empty"),
        pytest.param("   ", id="EP2-whitespace"),
        pytest.param("abc", id="EP3-letters"),
        pytest.param("12x", id="EP3-mixed"),
        pytest.param(float("inf"), id="BVA-pos-inf-nonfinite"),
        pytest.param(float("-inf"), id="BVA-neg-inf-nonfinite"),
        pytest.param(float("nan"), id="BVA-nan"),
    ],
)
def test_to_number_missing_partitions(value):
    assert math.isnan(utils.to_number(value))


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("0", 0.0, id="BVA-zero-string"),
        pytest.param(0, 0.0, id="BVA-zero-int"),
        pytest.param(-0.5, -0.5, id="EP4-negative"),
        pytest.param("3.5", 3.5, id="EP4-float-string"),
        pytest.param(2_147_483_647, 2_147_483_647.0, id="BVA-int32-max"),
        pytest.param(1e308, 1e308, id="BVA-near-float-max-finite"),
    ],
)
def test_to_number_valid_partitions(value, expected):
    assert utils.to_number(value) == expected


# ---------------------------------------------------------------------------
# utils.encode_categorical
# Partitions:  EP1 None | EP2 known category | EP3 unseen category
# Boundary:    encoder value 0 (falsy encoding must still round-trip, not NaN)
# ---------------------------------------------------------------------------

_ENCODER = {"web": 0, "mobile": 1, "pos": 2}


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, None, id="EP1-none-is-nan"),
        pytest.param("web", 0.0, id="BVA-zero-encoding"),
        pytest.param("mobile", 1.0, id="EP2-known"),
        pytest.param("pos", 2.0, id="BVA-max-encoding"),
        pytest.param("WEB", None, id="EP3-case-sensitive-unseen"),
        pytest.param("carrier-pigeon", None, id="EP3-unseen"),
        pytest.param("", None, id="BVA-empty-string-unseen"),
    ],
)
def test_encode_categorical_partitions(value, expected):
    encoded = utils.encode_categorical(value, _ENCODER)
    if expected is None:
        assert math.isnan(encoded)
    else:
        assert encoded == expected


# ---------------------------------------------------------------------------
# Fraud decision rule — prediction = 1 iff probability >= threshold (0.8)
# Partitions:  EP1 clearly below | EP2 clearly above
# Boundaries:  just below, exactly at, just above the threshold
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "probability, expected_prediction",
    [
        pytest.param(0.0, 0, id="BVA-minimum"),
        pytest.param(0.35, 0, id="EP1-clearly-legit"),
        pytest.param(0.799999, 0, id="BVA-just-below-threshold"),
        pytest.param(0.8, 1, id="BVA-at-threshold-inclusive"),
        pytest.param(0.800001, 1, id="BVA-just-above-threshold"),
        pytest.param(0.95, 1, id="EP2-clearly-fraud"),
        pytest.param(1.0, 1, id="BVA-maximum"),
    ],
)
async def test_decision_threshold_partitions(
    service, transaction_payload, probability, expected_prediction
):
    from test_predict import kserve_response, set_kserve_response

    set_kserve_response(service, kserve_response(probability=probability))
    outputs = await service.predict(FraudDetectionInputs(**transaction_payload))
    assert outputs.prediction == expected_prediction
