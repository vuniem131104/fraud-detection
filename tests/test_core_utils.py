"""Unit tests for the feature-encoding helpers in ``fraud_detection.core.utils``."""

from __future__ import annotations

import math

from fraud_detection.core import utils


# ---------------------------------------------------------------------------
# encode_categorical
# ---------------------------------------------------------------------------

def test_encode_categorical_known_value():
    assert utils.encode_categorical("visa", {"visa": 3}) == 3.0


def test_encode_categorical_returns_float():
    encoded = utils.encode_categorical("visa", {"visa": 3})
    assert isinstance(encoded, float)


def test_encode_categorical_none_is_nan():
    assert math.isnan(utils.encode_categorical(None, {"visa": 3}))


def test_encode_categorical_unseen_is_nan():
    assert math.isnan(utils.encode_categorical("discover", {"visa": 3}))


def test_encode_categorical_matches_by_string_form():
    """Non-string scalars are looked up by ``str(value)`` (e.g. bools)."""
    assert utils.encode_categorical(True, {"True": 1, "False": 0}) == 1.0
    assert utils.encode_categorical(7, {"7": 5}) == 5.0


def test_encode_categorical_empty_encoder():
    assert math.isnan(utils.encode_categorical("anything", {}))


# ---------------------------------------------------------------------------
# to_number
# ---------------------------------------------------------------------------

def test_to_number_none_is_nan():
    assert math.isnan(utils.to_number(None))


def test_to_number_blank_strings_are_nan():
    assert math.isnan(utils.to_number(""))
    assert math.isnan(utils.to_number("   "))
    assert math.isnan(utils.to_number("\t\n"))


def test_to_number_unparseable_is_nan():
    assert math.isnan(utils.to_number("abc"))
    assert math.isnan(utils.to_number([1, 2]))


def test_to_number_valid_values():
    assert utils.to_number("3.5") == 3.5
    assert utils.to_number(7) == 7.0
    assert utils.to_number(True) == 1.0
    assert utils.to_number(" 42 ") == 42.0


def test_to_number_non_finite_is_nan():
    assert math.isnan(utils.to_number(float("inf")))
    assert math.isnan(utils.to_number(float("-inf")))
    assert math.isnan(utils.to_number(float("nan")))


# ---------------------------------------------------------------------------
# build_model_inputs
# ---------------------------------------------------------------------------

def test_build_model_inputs_orders_and_encodes(schema):
    features = {
        "amount_usd": 50.0,
        "log_amount": 3.93,
        "hour": 13,
        "channel": "web",          # categorical -> 0
        "card_brand": "visa",      # categorical -> 0
        "merchant_category": "travel",  # categorical -> 1
        "card_tx_count_1h": "2",   # numeric string -> 2.0
        "user_id": "user-1",       # extra key: ignored
    }
    encoded = utils.build_model_inputs(features, schema)

    assert list(encoded) == schema["feature_columns"]
    assert encoded["amount_usd"] == 50.0
    assert encoded["channel"] == 0.0
    assert encoded["merchant_category"] == 1.0
    assert encoded["card_tx_count_1h"] == 2.0
    assert "user_id" not in encoded


def test_build_model_inputs_missing_values_are_nan(schema):
    encoded = utils.build_model_inputs({}, schema)
    assert list(encoded) == schema["feature_columns"]
    assert all(math.isnan(value) for value in encoded.values())


def test_build_model_inputs_unseen_category_is_nan(schema):
    encoded = utils.build_model_inputs({"channel": "carrier-pigeon"}, schema)
    assert math.isnan(encoded["channel"])


def test_build_model_inputs_all_values_are_floats(schema):
    features = {"amount_usd": 12, "channel": "pos", "kyc_level": True}
    encoded = utils.build_model_inputs(features, schema)
    assert all(isinstance(value, float) for value in encoded.values())


def test_build_model_inputs_without_encoders_key():
    """``categorical_encoders`` is optional — every column is numeric then."""
    schema = {"feature_columns": ["a", "b"]}
    encoded = utils.build_model_inputs({"a": "1.5", "b": None}, schema)
    assert encoded["a"] == 1.5
    assert math.isnan(encoded["b"])
