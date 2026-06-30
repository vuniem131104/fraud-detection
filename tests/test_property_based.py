"""Property-based and idempotency tests using Hypothesis.

These tests verify *invariants* — properties that must hold for all (or random)
inputs — rather than specific example-based assertions. This is especially
valuable for finding edge-case bugs that manual test design would miss.

Idempotency properties verified:
  1. normalize_email applied twice = applied once (idempotent)
  2. to_number(to_number(x)) = to_number(x) for numeric inputs
  3. hash_password is deterministic: same input always → same digest
  4. build_model_inputs always returns the exact schema feature_columns
  5. card_age_days is always non-negative
  6. browser_family / os_family always return a non-empty string
  7. to_model_card_id result is always in [0, 2_147_483_647)
"""

from __future__ import annotations

import math
import string
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from fraud_detection.core import api, utils
from fraud_detection.core.models import FraudDetectionInputs


# ---------------------------------------------------------------------------
# Hypothesis settings profile: slightly more examples, suppress slow-call warnings
# ---------------------------------------------------------------------------

settings.register_profile(
    "ci",
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=5000,
)
settings.load_profile("ci")


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_hex_chars = string.hexdigits[:16]  # 0-9a-f


def valid_hex32() -> st.SearchStrategy[str]:
    """Generate valid 32-character lowercase hex strings."""
    return st.text(alphabet=_hex_chars, min_size=32, max_size=32)


def any_email_string() -> st.SearchStrategy[str]:
    """Generate arbitrary email-like strings (may or may not contain @)."""
    return st.text(alphabet=string.printable, min_size=0, max_size=100)


def valid_transaction_dict() -> st.SearchStrategy[dict[str, Any]]:
    """Generate minimal valid FraudDetectionInputs-compatible dicts."""
    return st.fixed_dictionaries({
        "tx_id": st.text(min_size=1, max_size=20, alphabet=string.ascii_letters + string.digits),
        "event_timestamp": st.just("2017-12-15T13:00:00"),
        "amount_usd": st.floats(min_value=0.001, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
        "channel": st.sampled_from(["W", "C", "R"]),
        "user_id": st.text(min_size=1, max_size=20, alphabet=string.ascii_lowercase),
        "card_id": st.text(min_size=1, max_size=20, alphabet=string.ascii_lowercase),
        "card_country": st.integers(min_value=0, max_value=999),
        "issuer_code": st.integers(min_value=0, max_value=999999),
        "card_brand": st.sampled_from(["visa", "mastercard", "amex"]),
        "bin_code": st.text(min_size=1, max_size=10, alphabet=string.digits),
        "card_type": st.sampled_from(["credit", "debit"]),
        "billing_zone": st.integers(min_value=0, max_value=9),
        "billing_country": st.integers(min_value=0, max_value=999),
        "email_purchaser": st.just("buyer@example.com"),
        "email_recipient": st.just("seller@example.com"),
        "device_type": st.sampled_from(["desktop", "mobile", "tablet"]),
        "device_info": st.just("desktop:Windows 11:Chrome"),
        "os_raw": st.just("Windows 11"),
        "browser_raw": st.just("Chrome 120"),
        "screen_resolution": st.just("1920x1080"),
        "C1": st.integers(min_value=0, max_value=100),
        "C2": st.integers(min_value=0, max_value=100),
        "M1": st.sampled_from(["T", "F"]),
        "M2": st.sampled_from(["T", "F"]),
        "M6": st.sampled_from(["T", "F"]),
    })


# ---------------------------------------------------------------------------
# 1. normalize_email idempotency
#    Applying normalize_email twice should give the same result as applying once.
# ---------------------------------------------------------------------------

@given(any_email_string())
def test_normalize_email_idempotent(email: str):
    """Applying normalize_email twice == applying once (idempotent)."""
    tx = {"email_purchaser": email, "email_recipient": email}
    once = utils.normalize_email(tx)
    twice = utils.normalize_email(once)
    assert once["email_purchaser"] == twice["email_purchaser"]
    assert once["email_recipient"] == twice["email_recipient"]


# ---------------------------------------------------------------------------
# 2. to_number idempotency for numeric values
#    to_number(to_number(x)) == to_number(x) for any float
# ---------------------------------------------------------------------------

@given(st.floats(allow_nan=True, allow_infinity=True))
def test_to_number_idempotent_float(value: float):
    """to_number applied twice gives same result as once."""
    first = utils.to_number(value)
    second = utils.to_number(first)
    if math.isnan(first):
        assert math.isnan(second)
    else:
        assert first == second


@given(st.text(min_size=0, max_size=50))
def test_to_number_idempotent_string(value: str):
    """to_number on a string, then on the result, is stable."""
    first = utils.to_number(value)
    second = utils.to_number(first)
    if math.isnan(first):
        assert math.isnan(second)
    else:
        assert first == pytest.approx(second)


# ---------------------------------------------------------------------------
# 3. hash_password determinism
#    Same password always → same hex digest, regardless of call order.
# ---------------------------------------------------------------------------

@given(st.text(min_size=1, max_size=200))
@settings(max_examples=10, suppress_health_check=[HealthCheck.too_slow], deadline=30000)
def test_hash_password_deterministic(password: str):
    """hash_password is deterministic: same input → same output always."""
    first = api.hash_password(password)
    second = api.hash_password(password)
    assert first == second
    assert len(first) == 64
    assert all(c in "0123456789abcdef" for c in first)


@given(st.text(min_size=1, max_size=50), st.text(min_size=1, max_size=50))
@settings(max_examples=10, suppress_health_check=[HealthCheck.too_slow], deadline=30000)
def test_hash_password_distinct_inputs_distinct(pw1: str, pw2: str):
    """Different passwords must produce different digests (no trivial collisions)."""
    assume(pw1 != pw2)
    assert api.hash_password(pw1) != api.hash_password(pw2)


# ---------------------------------------------------------------------------
# 4. card_age_days non-negativity
#    For any card_created_at and now, result must be >= 0.
# ---------------------------------------------------------------------------

HCM_TZ = api.HO_CHI_MINH_TZ

@given(
    delta_days=st.floats(min_value=-3650.0, max_value=3650.0, allow_nan=False, allow_infinity=False)
)
def test_card_age_days_always_non_negative(delta_days: float):
    """card_age_days must always be >= 0 regardless of card creation time."""
    now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=HCM_TZ)
    card_created = now - timedelta(days=delta_days)
    result = api.card_age_days(card_created, now)
    assert result >= 0.0


# ---------------------------------------------------------------------------
# 5. browser_family and os_family always return a non-empty string
# ---------------------------------------------------------------------------

@given(st.text(min_size=0, max_size=200))
def test_browser_family_always_returns_string(value: str):
    """browser_family always returns a non-empty string, never raises."""
    result = utils.browser_family(value)
    assert isinstance(result, str)
    assert len(result) > 0


@given(st.text(min_size=0, max_size=200))
def test_os_family_always_returns_string(value: str):
    """os_family always returns a non-empty string, never raises."""
    result = utils.os_family(value)
    assert isinstance(result, str)
    assert len(result) > 0


# ---------------------------------------------------------------------------
# 6. to_model_card_id result is always in [0, 2_147_483_647)
# ---------------------------------------------------------------------------

@given(valid_hex32())
def test_to_model_card_id_hex32_in_range(hex_str: str):
    """Valid 32-char hex always produces value in [0, 2_147_483_647)."""
    result = utils.to_model_card_id(hex_str)
    assert 0 <= result < 2_147_483_647


@given(st.integers(min_value=0, max_value=2_147_483_647))
def test_to_model_card_id_integer_roundtrip(value: int):
    """Integer inputs ≥ 0 pass through to_int_like unchanged."""
    result = utils.to_model_card_id(value)
    assert result == value


# ---------------------------------------------------------------------------
# 7. build_model_inputs column consistency (idempotency of output schema)
#    For any valid payload, calling build_model_inputs twice must return a
#    DataFrame with the same columns in the same order.
# ---------------------------------------------------------------------------

_MINI_SCHEMA = {
    "version": "test",
    "training_reference_ts": "2017-12-01",
    "email_bin": {"gmail.com": "google"},
    "email_nulls": [],
    "uid_columns": ["uid1"],
    "uid_agg_targets": ["amount_usd"],
    "freq_tables": {},
    "categorical_encoders": {
        "channel": {"W": 1, "C": 2, "R": 3, "missing": 0},
        "card_brand": {"visa": 1, "mastercard": 2, "missing": 0},
    },
    "feature_columns": [
        "amount_usd", "amount_log", "amount_cents",
        "hour_of_day", "day_of_week", "is_weekend",
        "channel", "card_brand",
        "screen_width", "screen_height",
        "uid1_amount_usd_mean", "uid1_amount_usd_std",
        "card_tx_count_so_far", "card_amount_sum_so_far",
    ],
    "target": "isFraud",
}


@given(valid_transaction_dict())
@settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow], deadline=10000)
def test_build_model_inputs_column_consistency(tx: dict[str, Any]):
    """build_model_inputs always returns exactly the schema's feature_columns."""
    payload = {"current_transaction": tx, "features": {}, "transactions": []}
    result1 = utils.build_model_inputs(payload, _MINI_SCHEMA)
    result2 = utils.build_model_inputs(payload, _MINI_SCHEMA)
    # Same columns in same order both times
    assert list(result1.columns) == _MINI_SCHEMA["feature_columns"]
    assert list(result2.columns) == _MINI_SCHEMA["feature_columns"]
    # Same shape
    assert result1.shape == result2.shape


@given(valid_transaction_dict())
@settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow], deadline=10000)
def test_build_model_inputs_idempotent_values(tx: dict[str, Any]):
    """build_model_inputs is deterministic: same input → same feature values."""
    payload = {"current_transaction": tx, "features": {}, "transactions": []}
    df1 = utils.build_model_inputs(payload, _MINI_SCHEMA)
    df2 = utils.build_model_inputs(payload, _MINI_SCHEMA)
    # Compare numeric columns (NaN == NaN treated as equal)
    for col in _MINI_SCHEMA["feature_columns"]:
        v1 = df1[col].iloc[0]
        v2 = df2[col].iloc[0]
        # Both NaN or both equal
        if isinstance(v1, float) and math.isnan(v1):
            assert isinstance(v2, float) and math.isnan(v2), f"Column {col}: {v1} != {v2}"
        else:
            assert v1 == pytest.approx(v2, rel=1e-6, abs=1e-9), f"Column {col}: {v1} != {v2}"


# ---------------------------------------------------------------------------
# 8. FraudDetectionInputs model: valid dict always parses without error
# ---------------------------------------------------------------------------

@given(valid_transaction_dict())
def test_fraud_detection_inputs_valid_dict_always_parses(tx: dict[str, Any]):
    """Any dict from valid_transaction_dict() must successfully create a model instance."""
    inputs = FraudDetectionInputs(**tx)
    assert inputs.amount_usd > 0
    assert inputs.card_country >= 0


# ---------------------------------------------------------------------------
# 9. to_local_datetime: result always has HCM tz offset
# ---------------------------------------------------------------------------

@given(
    year=st.integers(min_value=2000, max_value=2030),
    month=st.integers(min_value=1, max_value=12),
    day=st.integers(min_value=1, max_value=28),  # safe day for all months
    hour=st.integers(min_value=0, max_value=23),
)
def test_to_local_datetime_always_hcm_offset(year: int, month: int, day: int, hour: int):
    """to_local_datetime always returns a datetime with UTC+7 offset."""
    dt = datetime(year, month, day, hour, 0, 0)
    result = api.to_local_datetime(dt)
    assert result.utcoffset() == timedelta(hours=7)


# ---------------------------------------------------------------------------
# 10. device_brand always returns a non-empty string
# ---------------------------------------------------------------------------

@given(st.text(min_size=0, max_size=200))
def test_device_brand_always_returns_nonempty_string(value: str):
    """device_brand always returns a non-empty string, never raises."""
    result = utils.device_brand(value)
    assert isinstance(result, str)
    assert len(result) > 0
