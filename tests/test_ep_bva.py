"""Equivalence Partitioning (EP) and Boundary Value Analysis (BVA) tests.

Each parametrize block is annotated with the partition/boundary rationale so the
design intent is traceable:
  - EP  = representative value from an equivalence class
  - BVA = value at or adjacent to a boundary between classes
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from fraud_detection.core import api, utils
from fraud_detection.core.models import FraudDetectionInputs


# ---------------------------------------------------------------------------
# to_number(value, default=nan)
# Equivalence classes:
#   EP1 – None                         → NaN
#   EP2 – blank / whitespace string    → NaN
#   EP3 – non-numeric string           → NaN
#   EP4 – valid numeric string/number  → float(value)
#   BVA5 – integer 0 (zero boundary)   → 0.0
#   BVA6 – positive ∞                  → math.inf
#   BVA7 – negative ∞                  → -math.inf
#   BVA8 – very large integer          → float(2_147_483_647)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        # EP1: None → default (NaN)
        pytest.param(None, float("nan"), id="EP1-none"),
        # EP2: blank strings → default (NaN)
        pytest.param("", float("nan"), id="EP2-empty-string"),
        pytest.param("   ", float("nan"), id="EP2-whitespace"),
        pytest.param("\t\n", float("nan"), id="EP2-tab-newline"),
        # EP3: non-numeric strings → default (NaN)
        pytest.param("abc", float("nan"), id="EP3-letters"),
        pytest.param("12x", float("nan"), id="EP3-mixed"),
        pytest.param("$100", float("nan"), id="EP3-symbol"),
        # EP4: valid numeric inputs → their float value
        pytest.param("3.5", 3.5, id="EP4-float-string"),
        pytest.param("100", 100.0, id="EP4-int-string"),
        pytest.param(7, 7.0, id="EP4-int"),
        pytest.param(3.14, 3.14, id="EP4-float"),
        # BVA5: zero boundary
        pytest.param("0", 0.0, id="BVA5-zero-string"),
        pytest.param(0, 0.0, id="BVA5-zero-int"),
        # BVA6: positive infinity (valid float)
        pytest.param(float("inf"), float("inf"), id="BVA6-pos-inf"),
        # BVA7: negative infinity
        pytest.param(float("-inf"), float("-inf"), id="BVA7-neg-inf"),
        # BVA8: INT32_MAX boundary
        pytest.param(2_147_483_647, 2_147_483_647.0, id="BVA8-int32-max"),
    ],
)
def test_to_number_ep_bva(value, expected):
    result = utils.to_number(value)
    if math.isnan(expected):
        assert math.isnan(result), f"Expected NaN for {value!r}, got {result}"
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "value, default, expected",
    [
        # Custom default replaces NaN output
        pytest.param(None, 0.0, 0.0, id="EP1-none-custom-default"),
        pytest.param("bad", -1.0, -1.0, id="EP3-non-numeric-custom-default"),
        pytest.param("5.5", 0.0, 5.5, id="EP4-valid-ignores-default"),
    ],
)
def test_to_number_custom_default(value, default, expected):
    assert utils.to_number(value, default=default) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# to_float(value, feature, default)
# Equivalence classes:
#   EP1 – None                        → default
#   EP2 – blank string                → default
#   EP3 – NaN float                   → default
#   EP4 – infinite float              → default
#   EP5 – valid finite float string   → parsed value
#   EP6 – non-numeric non-empty str   → raises ValueError
#   BVA7 – exactly 0.0               → 0.0 (valid finite)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, default, expected",
    [
        # EP1: None → default
        pytest.param(None, 1.0, 1.0, id="EP1-none"),
        # EP2: blank string → default
        pytest.param("", 2.0, 2.0, id="EP2-empty"),
        pytest.param("  ", 2.0, 2.0, id="EP2-whitespace"),
        # EP3: NaN → default
        pytest.param(float("nan"), 3.0, 3.0, id="EP3-nan"),
        # EP4: Infinite → default
        pytest.param(float("inf"), 4.0, 4.0, id="EP4-pos-inf"),
        pytest.param(float("-inf"), 4.0, 4.0, id="EP4-neg-inf"),
        # EP5: valid finite string/number → parsed
        pytest.param("5", None, 5.0, id="EP5-int-string"),
        pytest.param(3.14, None, 3.14, id="EP5-float"),
        # BVA7: zero boundary
        pytest.param(0.0, None, 0.0, id="BVA7-zero"),
        pytest.param("0", None, 0.0, id="BVA7-zero-string"),
    ],
)
def test_to_float_ep_bva(value, default, expected):
    result = utils.to_float(value, "feature", default=default)
    assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("abc", id="EP6-letters"),
        pytest.param("1.2.3", id="EP6-multi-dot"),
        pytest.param("$", id="EP6-symbol"),
    ],
)
def test_to_float_invalid_raises(value):
    with pytest.raises(ValueError, match="must be numeric"):
        utils.to_float(value, "feature")


# ---------------------------------------------------------------------------
# to_model_card_id(value)
# Equivalence classes:
#   EP1 – valid 32-char hex string      → int(value, 16) % 2_147_483_647
#   EP2 – 32-char non-hex string        → fallback to_int_like(0)
#   EP3 – plain integer                 → int
#   EP4 – numeric string (non-hex len)  → int(str)
#   BVA5 – 31-char hex string (short)  → fallback (not 32 chars)
#   BVA6 – 33-char hex string (long)   → fallback (not 32 chars)
#   BVA7 – all-zeros 32-char hex        → 0 mod 2_147_483_647 = 0
#   BVA8 – all-f's 32-char hex          → int("f"*32,16) % 2_147_483_647
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        # EP1: valid 32-char hex
        pytest.param("0" * 31 + "1", 1, id="EP1-hex32-one"),
        pytest.param("a" * 32, int("a" * 32, 16) % 2_147_483_647, id="EP1-hex32-a"),
        # EP2: 32-char non-hex → fallback=0
        pytest.param("g" * 32, 0, id="EP2-non-hex-32"),
        pytest.param("z" * 32, 0, id="EP2-z-32"),
        # EP3: plain integers
        pytest.param(42, 42, id="EP3-int"),
        pytest.param(0, 0, id="EP3-zero"),
        # EP4: numeric strings of non-32 length
        pytest.param("12", 12, id="EP4-numeric-string"),
        pytest.param("999", 999, id="EP4-three-digit"),
        # BVA5: 31-char hex → too short, fallback
        pytest.param("a" * 31, 0, id="BVA5-hex31-short"),
        # BVA6: 33-char hex → too long, fallback
        pytest.param("a" * 33, 0, id="BVA6-hex33-long"),
        # BVA7: all-zeros 32-char hex
        pytest.param("0" * 32, 0, id="BVA7-all-zeros"),
        # BVA8: all-f's 32-char hex
        pytest.param("f" * 32, int("f" * 32, 16) % 2_147_483_647, id="BVA8-all-fs"),
    ],
)
def test_to_model_card_id_ep_bva(value, expected):
    assert utils.to_model_card_id(value) == expected


# ---------------------------------------------------------------------------
# api.card_age_days(card_created_at, now)
# Equivalence classes:
#   EP1 – card older than now        → positive float (days elapsed)
#   EP2 – card created in the future → clamped to 0.0
#   BVA3 – card created exactly now  → 0.0 (boundary, zero age)
#   BVA4 – card created 1 second ago → ≈ 1/86400 days (just above zero)
#   BVA5 – card created 1 second ahead → 0.0 (just below zero, clamped)
# ---------------------------------------------------------------------------

HCM_TZ = api.HO_CHI_MINH_TZ

@pytest.mark.parametrize(
    "delta, expected_approx, check_zero",
    [
        # EP1: 30 days old
        pytest.param(timedelta(days=30), 30.0, False, id="EP1-30-days"),
        # EP1: 1 day old
        pytest.param(timedelta(days=1), 1.0, False, id="EP1-1-day"),
        # EP2: 5 days in future → clamped to 0
        pytest.param(timedelta(days=-5), 0.0, True, id="EP2-future-5-days"),
        # BVA3: exactly now (delta=0) → 0.0
        pytest.param(timedelta(0), 0.0, True, id="BVA3-created-now"),
        # BVA4: 1 second ago → very small positive
        pytest.param(timedelta(seconds=1), 1.0 / 86400, False, id="BVA4-1-second-ago"),
        # BVA5: 1 second in future → clamped to 0
        pytest.param(timedelta(seconds=-1), 0.0, True, id="BVA5-1-second-future"),
    ],
)
def test_card_age_days_ep_bva(delta, expected_approx, check_zero):
    now = datetime.now(HCM_TZ).replace(microsecond=0)
    card_created = now - delta
    result = api.card_age_days(card_created, now)
    if check_zero:
        assert result == 0.0
    else:
        assert result == pytest.approx(expected_approx, rel=1e-3)


# ---------------------------------------------------------------------------
# api.hash_password(password)
# Equivalence classes:
#   EP1 – any non-empty string → 64-char hex digest (deterministic)
#   EP2 – single char (min meaningful input) → valid digest
#   EP3 – long password → valid digest (no truncation)
#   BVA4 – two different passwords → different digests (collision check)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "password",
    [
        pytest.param("a", id="EP2-single-char"),
        pytest.param("secret123", id="EP1-typical"),
        pytest.param("P@ssw0rd!#$%", id="EP1-special-chars"),
        pytest.param("x" * 200, id="EP3-long-password"),
    ],
)
def test_hash_password_ep_bva(password):
    digest = api.hash_password(password)
    # Must be a 64-char hex string
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)
    # Must be deterministic
    assert digest == api.hash_password(password)


@pytest.mark.parametrize(
    "pw1, pw2",
    [
        # BVA4: different passwords must differ (no collisions in obvious cases)
        pytest.param("secret", "Secret", id="BVA4-case-differs"),
        pytest.param("password", "password1", id="BVA4-suffix-differs"),
        pytest.param("a", "b", id="BVA4-single-char-differs"),
    ],
)
def test_hash_password_different_inputs_differ(pw1, pw2):
    assert api.hash_password(pw1) != api.hash_password(pw2)


# ---------------------------------------------------------------------------
# FraudDetectionInputs model validation (amount_usd, card_country, screen_resolution)
# Equivalence classes:
#   amount_usd:
#     EP1 – gt=0: value at boundary 0.0        → ValidationError
#     EP2 – gt=0: value just above 0 (0.001)   → valid
#     EP3 – gt=0: very large amount             → valid
#     EP4 – negative                            → ValidationError
#   card_country:
#     EP5 – ge=0: value -1 (just below min)    → ValidationError
#     EP6 – ge=0: value 0 (at boundary)        → valid
#     EP7 – ge=0: value 999 (typical)          → valid
#   screen_resolution (pattern r"\d+x\d+"):
#     EP8 – "0x0"        → valid (min pixels)
#     EP9 – "1920x1080"  → valid (typical HD)
#     EP10 – "9999x9999" → valid (large)
#     EP11 – "1920"      → ValidationError (missing x)
#     EP12 – "1920x"     → ValidationError (no height digits)
#     EP13 – "axb"       → ValidationError (non-numeric)
# ---------------------------------------------------------------------------

@pytest.fixture
def _base_payload():
    return {
        "tx_id": "tx-1",
        "event_timestamp": "2017-12-15T13:00:00",
        "amount_usd": 50.0,
        "channel": "W",
        "user_id": "user-1",
        "card_id": "0" * 31 + "1",
        "card_country": 840,
        "issuer_code": 84001,
        "card_brand": "visa",
        "bin_code": "411111",
        "card_type": "credit",
        "billing_zone": 1,
        "billing_country": 840,
        "email_purchaser": "buyer@example.com",
        "email_recipient": "seller@example.com",
        "device_type": "desktop",
        "device_info": "desktop:Windows:Chrome",
        "os_raw": "Windows 11",
        "browser_raw": "Chrome 120",
        "screen_resolution": "1920x1080",
        "C1": 1,
        "C2": 1,
        "M1": "T",
        "M2": "T",
        "M6": "F",
    }


@pytest.mark.parametrize(
    "field, value, should_raise",
    [
        # amount_usd boundaries
        pytest.param("amount_usd", 0.0, True, id="BVA-amount-at-zero"),
        pytest.param("amount_usd", -10.0, True, id="EP4-amount-negative"),
        pytest.param("amount_usd", 0.001, False, id="BVA-amount-just-above-zero"),
        pytest.param("amount_usd", 1_000_000.0, False, id="EP3-amount-large"),
        # card_country boundaries (ge=0)
        pytest.param("card_country", -1, True, id="BVA-country-below-min"),
        pytest.param("card_country", 0, False, id="BVA-country-at-min"),
        pytest.param("card_country", 999, False, id="EP7-country-typical"),
        # screen_resolution pattern
        pytest.param("screen_resolution", "0x0", False, id="EP8-resolution-min"),
        pytest.param("screen_resolution", "1920x1080", False, id="EP9-resolution-hd"),
        pytest.param("screen_resolution", "9999x9999", False, id="EP10-resolution-large"),
        pytest.param("screen_resolution", "1920", True, id="EP11-resolution-no-x"),
        pytest.param("screen_resolution", "1920x", True, id="EP12-resolution-no-height"),
        pytest.param("screen_resolution", "axb", True, id="EP13-resolution-non-numeric"),
    ],
)
def test_fraud_detection_inputs_ep_bva(_base_payload, field, value, should_raise):
    payload = {**_base_payload, field: value}
    if should_raise:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**payload)
    else:
        inputs = FraudDetectionInputs(**payload)
        assert inputs is not None


# ---------------------------------------------------------------------------
# FraudDetectionInputs — M1/M2/M6 match-feature EP (pattern ^[TF]$)
# EP1 – "T"  → valid
# EP2 – "F"  → valid
# EP3 – "t"  → invalid (case-sensitive)
# EP4 – "TF" → invalid (too long)
# EP5 – ""   → invalid (empty)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "m_value, should_raise",
    [
        pytest.param("T", False, id="EP1-T-valid"),
        pytest.param("F", False, id="EP2-F-valid"),
        pytest.param("t", True, id="EP3-lowercase-invalid"),
        pytest.param("TF", True, id="EP4-too-long"),
        pytest.param("", True, id="EP5-empty"),
        pytest.param("X", True, id="EP6-other-char"),
    ],
)
def test_match_feature_ep_bva(_base_payload, m_value, should_raise):
    payload = {**_base_payload, "M1": m_value}
    if should_raise:
        with pytest.raises(ValidationError):
            FraudDetectionInputs(**payload)
    else:
        assert FraudDetectionInputs(**payload).m1 == m_value


# ---------------------------------------------------------------------------
# to_int_like(value, default)
# EP1 – None         → default (0)
# EP2 – float string → truncated int ("3.9" → 3)
# EP3 – NaN float    → default
# EP4 – integer str  → int
# BVA5 – "0.0"       → 0 (zero boundary)
# BVA6 – "-1.9"      → -1 (negative float truncated)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, default, expected",
    [
        pytest.param(None, 0, 0, id="EP1-none-default"),
        pytest.param(None, 7, 7, id="EP1-none-custom-default"),
        pytest.param("3.9", 0, 3, id="EP2-float-string-truncated"),
        pytest.param(float("nan"), 7, 7, id="EP3-nan-uses-default"),
        pytest.param("5", 0, 5, id="EP4-int-string"),
        pytest.param(42, 0, 42, id="EP4-int"),
        pytest.param("0.0", 0, 0, id="BVA5-zero-float-string"),
        pytest.param("-1.9", 0, -1, id="BVA6-negative-float-truncated"),
    ],
)
def test_to_int_like_ep_bva(value, default, expected):
    assert utils.to_int_like(value, default=default) == expected


# ---------------------------------------------------------------------------
# normalize_email(transaction)
# EP1 – standard email with @ → domain extracted
# EP2 – no @ in email         → value lowercased as-is
# EP3 – non-string value      → passed through unchanged
# EP4 – email with uppercase  → lowercased domain
# BVA5 – email with exactly one @ → domain after @
# BVA6 – email with multiple @    → part after last @ (split()[-1])
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "email, expected_normalized",
    [
        # EP1: standard email
        pytest.param("buyer@gmail.com", "gmail.com", id="EP1-standard-email"),
        # EP2: no @ sign
        pytest.param("nodomain", "nodomain", id="EP2-no-at-sign"),
        # EP4: uppercase → lowercased domain
        pytest.param("User@GMAIL.COM", "gmail.com", id="EP4-uppercase"),
        # BVA5: exactly one @
        pytest.param("a@b.com", "b.com", id="BVA5-one-at"),
        # BVA6: multiple @ → last segment
        pytest.param("a@b@c.com", "c.com", id="BVA6-multiple-at"),
        # Empty string
        pytest.param("", "", id="EP2-empty-string"),
    ],
)
def test_normalize_email_ep_bva(email, expected_normalized):
    result = utils.normalize_email({"email_purchaser": email})
    assert result["email_purchaser"] == expected_normalized


def test_normalize_email_non_string_passthrough():
    """EP3: non-string values are left unchanged."""
    result = utils.normalize_email({"email_purchaser": 12345})
    assert result["email_purchaser"] == 12345


# ---------------------------------------------------------------------------
# api.to_local_datetime(value)
# EP1 – naive datetime   → tagged with HCM tz, hour unchanged
# EP2 – UTC-aware        → converted to +07:00
# BVA3 – midnight UTC    → 07:00 HCM (timezone shift crosses day boundary by 7h)
# BVA4 – 23:59 UTC       → 06:59 +1day HCM
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "input_dt, expected_hour, expected_utcoffset_hours",
    [
        # EP1: naive stays naive-hour but gets HCM tz
        pytest.param(
            datetime(2017, 12, 15, 13, 0, 0),
            13,
            7,
            id="EP1-naive-tagged",
        ),
        # EP2: UTC → +07
        pytest.param(
            datetime(2017, 12, 15, 13, 0, 0, tzinfo=timezone.utc),
            20,
            7,
            id="EP2-utc-to-hcm",
        ),
        # BVA3: midnight UTC → 07:00 HCM
        pytest.param(
            datetime(2017, 12, 15, 0, 0, 0, tzinfo=timezone.utc),
            7,
            7,
            id="BVA3-midnight-utc",
        ),
        # BVA4: 23:59 UTC → 06:59 HCM next day
        pytest.param(
            datetime(2017, 12, 15, 23, 59, 0, tzinfo=timezone.utc),
            6,
            7,
            id="BVA4-late-night-utc",
        ),
    ],
)
def test_to_local_datetime_ep_bva(input_dt, expected_hour, expected_utcoffset_hours):
    result = api.to_local_datetime(input_dt)
    assert result.hour == expected_hour
    assert result.utcoffset() == timedelta(hours=expected_utcoffset_hours)
