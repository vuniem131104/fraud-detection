"""Unit tests for the Pydantic request/response schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs


def test_valid_inputs_parse(transaction_payload):
    inputs = FraudDetectionInputs(**transaction_payload)

    assert inputs.transaction_id == "tx-100"
    assert inputs.user_id == "user-1"
    assert inputs.card_id == "card-1"
    assert inputs.amount_usd == 50.0
    assert inputs.merchant_risk_level == 3
    assert inputs.email_purchaser == "buyer@gmail.com"


def test_extra_fields_are_forbidden(transaction_payload):
    with pytest.raises(ValidationError):
        FraudDetectionInputs(**transaction_payload, is_fraud=1)


@pytest.mark.parametrize("field", list(FraudDetectionInputs.model_fields))
def test_every_field_is_required(transaction_payload, field):
    """The schema declares no defaults — dropping any field fails validation."""
    transaction_payload.pop(field)
    with pytest.raises(ValidationError):
        FraudDetectionInputs(**transaction_payload)


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("transaction_id", ""),            # min_length 1
        ("user_id", ""),
        ("card_id", ""),
        ("merchant_category", ""),
        ("timestamp", ""),
        ("channel", ""),
        ("billing_country_code", ""),
        ("ip_country_code", ""),
        ("amount_usd", 0.0),               # must be > 0
        ("amount_usd", -10.0),
        ("merchant_risk_level", -1),       # must be in [0, 10]
        ("merchant_risk_level", 11),
        ("email_purchaser", "not-an-email"),
        ("email_recipient", "missing-domain@"),
    ],
)
def test_invalid_inputs_raise(transaction_payload, field, bad_value):
    transaction_payload[field] = bad_value
    with pytest.raises(ValidationError):
        FraudDetectionInputs(**transaction_payload)


def test_outputs_roundtrip():
    outputs = FraudDetectionOutputs(transaction_id="tx-1", probability=0.42, prediction=0)
    dumped = outputs.model_dump()
    assert dumped == {"transaction_id": "tx-1", "probability": 0.42, "prediction": 0}
