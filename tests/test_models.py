"""Unit tests for the Pydantic request/response schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs


def test_valid_inputs_parse_aliases(transaction_payload):
    """A valid payload parses and exposes C*/D*/M* fields via their aliases."""
    inputs = FraudDetectionInputs(**transaction_payload)

    assert inputs.c1 == 5
    assert inputs.c13 == 10
    assert inputs.m1 == "T"

    dumped = inputs.model_dump(by_alias=True)
    assert dumped["C1"] == 5
    assert dumped["D4"] == 3.0
    assert dumped["user_id"] == "user-1"


def test_optional_fields_use_defaults(transaction_payload):
    """C13/D4/D15 fall back to their declared defaults when omitted."""
    for field in ("C13", "D4", "D15"):
        transaction_payload.pop(field)

    inputs = FraudDetectionInputs(**transaction_payload)

    assert inputs.c13 == 0
    assert inputs.d4 == 0
    assert inputs.d15 == 0


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("amount_usd", 0.0),            # must be > 0
        ("amount_usd", -10.0),
        ("card_country", -1),           # must be >= 0
        ("screen_resolution", "1920"),  # must match \d+x\d+
        ("m1", "X"),                    # must be T or F  (alias M1)
        ("tx_id", ""),                  # min_length 1
    ],
)
def test_invalid_inputs_raise(transaction_payload, field, bad_value):
    """Out-of-range / malformed values are rejected by validation."""
    key = {"m1": "M1"}.get(field, field)
    transaction_payload[key] = bad_value

    with pytest.raises(ValidationError):
        FraudDetectionInputs(**transaction_payload)


def test_missing_required_field_raises(transaction_payload):
    """Dropping a required field fails validation."""
    transaction_payload.pop("user_id")
    with pytest.raises(ValidationError):
        FraudDetectionInputs(**transaction_payload)


def test_outputs_model():
    """The output schema stores the id, probability and binary prediction."""
    out = FraudDetectionOutputs(tx_id="tx-1", probability=0.42, prediction=0)
    assert out.tx_id == "tx-1"
    assert out.probability == 0.42
    assert out.prediction == 0
