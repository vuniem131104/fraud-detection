"""Unit tests for the feature-engineering helpers in ``fraud_detection.core.utils``."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from fraud_detection.core import utils


# ---------------------------------------------------------------------------
# Numeric coercion helpers
# ---------------------------------------------------------------------------

def test_to_number():
    assert math.isnan(utils.to_number(None))
    assert math.isnan(utils.to_number("   "))
    assert math.isnan(utils.to_number("abc"))
    assert utils.to_number("3.5") == 3.5
    assert utils.to_number(None, default=0.0) == 0.0


def test_to_float():
    assert utils.to_float(None, "f", default=1.0) == 1.0
    assert utils.to_float("", "f", default=2.0) == 2.0
    assert utils.to_float(float("nan"), "f", default=3.0) == 3.0
    assert utils.to_float(float("inf"), "f", default=4.0) == 4.0
    assert utils.to_float("5", "f") == 5.0
    with pytest.raises(ValueError):
        utils.to_float("abc", "f")


def test_to_int_like():
    assert utils.to_int_like(None) == 0
    assert utils.to_int_like("3.9") == 3
    assert utils.to_int_like(float("nan"), default=7) == 7
    assert utils.to_int_like("5") == 5


def test_to_model_card_id():
    assert utils.to_model_card_id("0" * 31 + "1") == 1
    assert utils.to_model_card_id("f" * 32) == int("f" * 32, 16) % 2_147_483_647
    assert utils.to_model_card_id("g" * 32) == 0          # not hex -> falls back
    assert utils.to_model_card_id(7) == 7
    assert utils.to_model_card_id("12") == 12


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def test_local_timestamp_naive():
    ts = utils.local_timestamp("2017-12-15T13:00:00")
    assert ts.tzinfo is None
    assert ts.hour == 13


def test_local_timestamp_aware_converted_to_local():
    ts = utils.local_timestamp("2017-12-15T13:00:00+00:00")  # UTC -> +07:00 = 20:00
    assert ts.tzinfo is None
    assert ts.hour == 20


def test_local_timestamp_none_raises():
    with pytest.raises(ValueError):
        utils.local_timestamp(None)


def test_event_offset_seconds():
    schema = {"training_reference_ts": "2017-12-01"}
    assert utils.event_offset_seconds({"event_ts_offset_s": 100}, schema) == 100.0
    offset = utils.event_offset_seconds({"event_timestamp": "2017-12-02"}, schema)
    assert offset == 86_400.0


# ---------------------------------------------------------------------------
# normalize_transaction
# ---------------------------------------------------------------------------

def test_normalize_transaction_types(transaction_payload):
    normalized = utils.normalize_transaction(transaction_payload)
    assert isinstance(normalized["C1"], int)
    assert isinstance(normalized["card_id"], int)
    assert normalized["C13"] == 10
    assert normalized["D4"] == 3.0
    assert normalized["amount_usd"] == 50.0


def test_normalize_transaction_fills_from_snapshot():
    snapshot = {
        "no_transactions_30_days": 4,
        "card_age_days": 10.0,
        "no_days_since_last_txn": 6.0,
    }
    normalized = utils.normalize_transaction({"tx_id": "x"}, feature_snapshot=snapshot)
    assert normalized["C13"] == 5            # previous count (4) + 1
    assert normalized["D4"] == 10.0          # from card_age_days
    assert normalized["D15"] == 6.0          # from days_since_last_tx
    assert normalized["card_age_days"] == 10.0
    assert normalized["M1"] == "T"           # default when missing


# ---------------------------------------------------------------------------
# Categorical label helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        ("", "missing"),
        ("nan", "missing"),
        ("SM-G950F", "samsung"),
        ("Samsung Galaxy", "samsung"),
        ("Huawei P30", "huawei"),
        ("Windows NT", "windows"),
        ("FooBarBazQuxLong/1.0", "foobarbazqux"),
    ],
)
def test_device_brand(value, expected):
    assert utils.device_brand(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Windows 11", "windows"),
        ("iOS 14", "ios"),
        ("Android 10", "android"),
        ("macOS", "mac"),
        ("Linux", "linux"),
        ("PlayStation", "other"),
        ("", "missing"),
        ("nan", "missing"),
    ],
)
def test_os_family(value, expected):
    assert utils.os_family(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Chrome 120", "chrome"),
        ("Safari", "safari"),
        ("Firefox", "firefox"),
        ("ie 11", "ie"),
        ("Lynx", "other"),
        ("", "missing"),
    ],
)
def test_browser_family(value, expected):
    assert utils.browser_family(value) == expected


# ---------------------------------------------------------------------------
# add_identity_features
# ---------------------------------------------------------------------------

def test_add_identity_features():
    df = pd.DataFrame([{
        "device_info": "desktop:Windows 11:Chrome",
        "os_raw": "Windows 11",
        "browser_raw": "Chrome 120",
        "screen_resolution": "1920x1080",
        "device_type": "desktop",
    }])
    utils.add_identity_features(df)
    row = df.iloc[0]
    assert row["device_brand"] == "windows"
    assert row["os_family"] == "windows"
    assert row["os_version"] == 11.0
    assert row["browser_family"] == "chrome"
    assert row["browser_version"] == 120.0
    assert row["screen_width"] == 1920
    assert row["screen_height"] == 1080
    assert row["screen_area"] == 1920 * 1080
    assert row["device_type"] == "desktop"


def test_add_identity_features_single_token_resolution():
    df = pd.DataFrame([{"screen_resolution": "1920"}])
    utils.add_identity_features(df)
    row = df.iloc[0]
    assert row["screen_width"] == 1920
    assert row["screen_height"] == 0
    assert row["screen_area"] == 0


# ---------------------------------------------------------------------------
# build_base_features
# ---------------------------------------------------------------------------

def test_build_base_features(transaction_payload, schema):
    normalized = utils.normalize_transaction(transaction_payload)
    df = utils.build_base_features([normalized], schema)
    row = df.iloc[0]
    assert row["hour_of_day"] == 13
    assert row["day_of_week"] == 4        # 2017-12-15 is a Friday
    assert row["is_weekend"] == 0
    assert row["amount_log"] == pytest.approx(math.log1p(50.0))
    assert row["email_purchaser_provider"] == "other"
    for col in ("uid1", "uid2", "uid3", "uid4"):
        assert col in df.columns


def test_build_base_features_without_card_age_column(schema):
    """When a raw row lacks card_age_days the column defaults to 0.0."""
    row = {
        "event_timestamp": "2017-12-15T13:00:00",
        "amount_usd": 10.0,
        "card_id": "abc",
        "billing_zone": 1,
        "email_purchaser": "a@gmail.com",
        "email_recipient": "b@yahoo.com",
        "device_info": "x",
        "os_raw": "y",
        "browser_raw": "z",
        "screen_resolution": "100x100",
    }
    df = utils.build_base_features([row], schema)
    assert df["card_age_days"].iloc[0] == 0.0


# ---------------------------------------------------------------------------
# History aggregates
# ---------------------------------------------------------------------------

def _current_df(transaction_payload, schema):
    normalized = utils.normalize_transaction(transaction_payload)
    df = utils.build_base_features([normalized], schema)
    utils.init_history_defaults(df, schema)
    return df


def test_init_history_defaults(transaction_payload, schema):
    df = _current_df(transaction_payload, schema)
    assert math.isnan(df["uid1_amount_usd_mean"].iloc[0])
    assert df["card_tx_count_so_far"].iloc[0] == 0
    assert df["card_amount_sum_so_far"].iloc[0] == 0.0
    assert math.isnan(df["amount_zscore_card"].iloc[0])


def test_apply_previous_transactions_with_history(
    transaction_payload, previous_transactions, schema
):
    df = _current_df(transaction_payload, schema)
    utils.apply_previous_transactions(df, previous_transactions, schema, {})
    assert df["card_tx_count_so_far"].iloc[0] == 2
    assert df["uid1_amount_usd_mean"].iloc[0] == pytest.approx(50.0)  # mean(40, 60)
    assert df["amount_zscore_card"].iloc[0] == pytest.approx(0.0)     # (50 - 50) / 50


def test_apply_previous_transactions_no_history(transaction_payload, schema):
    df = _current_df(transaction_payload, schema)
    utils.apply_previous_transactions(df, [], schema, {})
    assert df["card_tx_count_so_far"].iloc[0] == 0


def test_apply_previous_transactions_all_future_history(transaction_payload, schema):
    """History strictly after the current event contributes nothing."""
    df = _current_df(transaction_payload, schema)
    future = {**transaction_payload, "event_timestamp": "2017-12-20T10:00:00"}
    utils.apply_previous_transactions(df, [future], schema, {})
    assert df["card_tx_count_so_far"].iloc[0] == 0


# ---------------------------------------------------------------------------
# apply_schema / assemble_vector
# ---------------------------------------------------------------------------

def test_apply_schema(transaction_payload, schema):
    normalized = utils.normalize_transaction(transaction_payload)
    df = utils.build_base_features([normalized], schema)
    utils.init_history_defaults(df, schema)
    df = utils.apply_schema(df, schema)
    row = df.iloc[0]
    assert row["card_id_freq"] == 1.0          # empty table -> default 1
    assert row["device_brand_freq"] == 0.25    # device_brand "windows" -> 0.25
    assert row["channel"] == 1                  # "W" encoded
    assert row["card_brand"] == 1               # "visa" encoded
    assert row["missing_col_example"] == 7      # absent col filled from encoder default
    for uid in ("uid1", "uid2", "uid3", "uid4"):
        assert uid not in df.columns            # uid columns are dropped


def test_assemble_vector_adds_missing_and_coerces():
    schema = {"feature_columns": ["amount_usd", "browser_raw", "V999"]}
    df = pd.DataFrame([{"amount_usd": 5.0, "browser_raw": "Chrome 120"}])
    result = utils.assemble_vector(df, schema)
    assert list(result.columns) == ["amount_usd", "browser_raw", "V999"]
    assert result["amount_usd"].iloc[0] == 5.0
    assert math.isnan(result["browser_raw"].iloc[0])  # non-numeric string -> NaN
    assert math.isnan(result["V999"].iloc[0])         # absent column -> NaN


# ---------------------------------------------------------------------------
# Payload extraction helpers
# ---------------------------------------------------------------------------

def test_history_from_payload_top_level():
    features, txs = utils.history_from_payload(
        {"features": {"a": 1}, "transactions": [{"x": 1}, 5]}
    )
    assert features == {"a": 1}
    assert txs == [{"x": 1}]  # non-dict row filtered out


def test_history_from_payload_malformed():
    features, txs = utils.history_from_payload({"features": 1, "transactions": "nope"})
    assert features == {}
    assert txs == []


def test_history_from_payload_nested_history():
    features, txs = utils.history_from_payload(
        {
            "features": {"a": 2},
            "history": {
                "feature_snapshot": {"a": 1, "b": 3},
                "previous_transactions": [{"y": 2}],
            },
        }
    )
    assert features == {"a": 2, "b": 3}   # top-level "a" wins over nested
    assert txs == [{"y": 2}]              # nested used (no top-level transactions)


def test_history_from_payload_top_level_transactions_win():
    _, txs = utils.history_from_payload(
        {"transactions": [{"t": 1}], "history": {"previous_transactions": [{"p": 1}]}}
    )
    assert txs == [{"t": 1}]


def test_current_from_payload():
    assert utils.current_from_payload({"current_transaction": {"a": 1}}) == {"a": 1}
    assert utils.current_from_payload({"a": 1}) == {"a": 1}
    with pytest.raises(TypeError):
        utils.current_from_payload({"current_transaction": 5})


# ---------------------------------------------------------------------------
# build_model_inputs (full pipeline)
# ---------------------------------------------------------------------------

def test_build_model_inputs_end_to_end(
    transaction_payload, previous_transactions, schema
):
    payload = {
        "features": {
            "no_transactions_30_days": 2,
            "card_age_days": 3.0,
            "no_days_since_last_txn": 5.0,
        },
        "transactions": previous_transactions,
        "current_transaction": transaction_payload,
    }
    result = utils.build_model_inputs(payload, schema)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert list(result.columns) == schema["feature_columns"]

    row = result.iloc[0]
    assert row["amount_usd"] == 50.0
    assert row["channel"] == 1
    assert row["card_brand"] == 1
    assert row["card_tx_count_so_far"] == 2
    assert row["device_brand_freq"] == 0.25
    assert row["missing_col_example"] == 7
    assert math.isnan(row["V999"])
    assert math.isnan(row["browser_raw"])


# ---------------------------------------------------------------------------
# Redis enrichment + email normalization
# ---------------------------------------------------------------------------

def test_enrich_fills_from_redis():
    tx = {"C13": 0, "D4": 0, "D15": 0}
    state = {
        "features": {
            "no_transactions_30_days": 2,
            "card_age_days": 10.0,
            "no_days_since_last_txn": 6.0,
        },
        "transactions": [],
    }
    result = utils.enrich_current_transaction_with_redis_features(tx, state)
    assert result["C13"] == 3      # previous (2) + 1
    assert result["D4"] == 10.0
    assert result["D15"] == 6.0


def test_enrich_keeps_existing_values():
    tx = {"C13": 9, "D4": 1.0, "D15": 1.0}
    state = {"features": {"card_age_days": 10.0, "no_days_since_last_txn": 6.0},
             "transactions": []}
    result = utils.enrich_current_transaction_with_redis_features(tx, state)
    assert result["C13"] == 9
    assert result["D4"] == 1.0
    assert result["D15"] == 1.0


def test_enrich_handles_malformed_state():
    tx = {"C13": 0, "D4": 0, "D15": 0}
    result = utils.enrich_current_transaction_with_redis_features(
        tx, {"features": 5, "transactions": "nope"}
    )
    assert result["C13"] == 1   # no count available -> 0 + 1
    assert result["D4"] == 0.0
    assert result["D15"] == 0.0


def test_normalize_email():
    result = utils.normalize_email(
        {"email_purchaser": "Buyer@Gmail.com", "email_recipient": "seller", "other": 1}
    )
    assert result["email_purchaser"] == "gmail.com"
    assert result["email_recipient"] == "seller"  # no @ -> lowercased value
    assert result["other"] == 1


def test_normalize_email_non_string():
    result = utils.normalize_email({"email_purchaser": 123})
    assert result["email_purchaser"] == 123
