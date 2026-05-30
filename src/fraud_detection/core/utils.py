from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from structlog import get_logger

logger = get_logger(__name__)

EMAIL_BIN = {
    "gmail.com": "google",
    "googlemail.com": "google",
    "yahoo.com": "yahoo",
    "ymail.com": "yahoo",
    "rocketmail.com": "yahoo",
    "hotmail.com": "microsoft",
    "outlook.com": "microsoft",
    "live.com": "microsoft",
    "msn.com": "microsoft",
    "aol.com": "aol",
    "aim.com": "aol",
    "icloud.com": "apple",
    "me.com": "apple",
    "mac.com": "apple",
}

EMAIL_NULLS = {"anonymous.com", "mail.com"}

MODEL_TO_CHANNEL = {
    "W": "web",
    "C": "mobile_app",
    "R": "pos",
}

def first_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def to_number(value: Any, default: float = np.nan) -> float:
    if value is None:
        return default
    try:
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, feature: str, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        logger.warning(
            "Invalid numeric feature value",
            extra={
                "feature": feature,
                "value": repr(value),
            },
        )
        raise ValueError(f"Feature {feature!r} must be numeric, got {value!r}") from exc
    return number if math.isfinite(number) else default


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_int_like(value: Any, default: int = 0) -> int:
    number = to_number(value, default=float(default))
    if math.isnan(number):
        return default
    return int(number)


def to_model_card_id(value: Any, default: int = 0) -> int:
    if isinstance(value, str):
        identifier = value.strip().lower()
        if len(identifier) == 32 and all(
            character in "0123456789abcdef" for character in identifier
        ):
            return int(identifier, 16) % 2_147_483_647
    return to_int_like(value, default=default)


def utc_timestamp(value: Any) -> pd.Timestamp:
    if value is None:
        raise ValueError("Transaction must include created_at or event_timestamp")
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def event_offset_seconds(row: dict[str, Any], schema: dict[str, Any]) -> float:
    explicit = first_value(row, "event_ts_offset_s", "TransactionDT")
    if explicit is not None:
        return float(explicit)

    event_timestamp = first_value(row, "event_timestamp", "created_at")
    reference = pd.Timestamp(schema["training_reference_ts"])
    return float((utc_timestamp(event_timestamp) - reference).total_seconds())


def group_feature(row: dict[str, Any], group: str, *names: str) -> Any:
    value = row.get(group)
    if not isinstance(value, dict):
        return None
    return first_value(value, *names)


def normalize_transaction(
    transaction: dict[str, Any],
    *,
    feature_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = dict(transaction)
    snapshot = dict(feature_snapshot or {})

    normalized: dict[str, Any] = {
        "tx_id": first_value(row, "tx_id", "transaction_id", "TransactionID"),
        "event_timestamp": first_value(row, "event_timestamp", "created_at"),
        "amount_usd": first_value(row, "amount_usd", "amount", "TransactionAmt"),
        "channel": first_value(row, "channel", "ProductCD") or "missing",
        "user_id": first_value(row, "user_id"),
        "card_id": first_value(row, "card_id", "card1") or 0,
        "issuer_code": first_value(row, "issuer_code", "card2") or 0,
        "card_country": first_value(row, "card_country") or 0,
        "card_brand": first_value(row, "card_brand", "card4") or "missing",
        "bin_code": first_value(row, "bin_code", "card5") or 0,
        "card_type": first_value(row, "card_type", "card6") or "missing",
        "billing_zone": first_value(row, "billing_zone", "addr1") or 0,
        "billing_country": first_value(row, "billing_country", "addr2") or 0,
        "email_purchaser": first_value(row, "email_purchaser", "P_emaildomain") or "missing",
        "email_recipient": first_value(row, "email_recipient", "R_emaildomain") or "missing",
        "device_type": first_value(row, "device_type", "DeviceType") or "missing",
        "device_info": first_value(row, "device_info", "DeviceInfo") or "missing",
        "os_raw": first_value(row, "os_raw", "id_30") or "missing",
        "browser_raw": first_value(row, "browser_raw", "id_31") or "missing",
        "screen_resolution": first_value(row, "screen_resolution", "id_33") or "0x0",
    }

    for model_col, group, names in (
        ("C1", "count_features", ("C1", "c1")),
        ("C2", "count_features", ("C2", "c2")),
        ("C13", "count_features", ("C13", "c13")),
        ("D4", "time_delta_features", ("D4", "d4")),
        ("D15", "time_delta_features", ("D15", "d15")),
        ("M1", "match_features", ("M1", "m1")),
        ("M2", "match_features", ("M2", "m2")),
        ("M6", "match_features", ("M6", "m6")),
    ):
        normalized[model_col] = first_value(row, model_col, *names)
        grouped_value = group_feature(row, group, *names)
        if grouped_value is not None:
            normalized[model_col] = grouped_value

    normalized["card_age_days"] = first_value(row, "card_age_days", "card_age_days", "D4")
    normalized["days_since_last_tx"] = first_value(
        row,
        "days_since_last_tx",
        "no_days_since_last_txn",
        "D15",
    )

    if normalized["card_age_days"] is None:
        normalized["card_age_days"] = first_value(snapshot, "card_age_days", "card_age_days")
    if normalized["days_since_last_tx"] is None:
        normalized["days_since_last_tx"] = first_value(
            snapshot,
            "days_since_last_tx",
            "no_days_since_last_txn",
        )

    if normalized["D4"] is None:
        normalized["D4"] = normalized["card_age_days"]
    if normalized["D15"] is None:
        normalized["D15"] = normalized["days_since_last_tx"]
    if normalized["C13"] is None:
        previous_count = first_value(snapshot, "previous_tx_count", "no_transactions_30_days")
        normalized["C13"] = to_int_like(previous_count, default=0) + 1

    normalized["C1"] = to_int_like(normalized["C1"], default=1)
    normalized["C2"] = to_int_like(normalized["C2"], default=1)
    normalized["C13"] = to_int_like(normalized["C13"], default=1)
    normalized["D4"] = to_number(normalized["D4"], default=0.0)
    normalized["D15"] = to_number(normalized["D15"], default=0.0)
    normalized["card_id"] = to_model_card_id(normalized["card_id"], default=0)
    normalized["issuer_code"] = to_int_like(normalized["issuer_code"], default=0)
    normalized["card_country"] = to_int_like(normalized["card_country"], default=0)
    normalized["bin_code"] = to_int_like(normalized["bin_code"], default=0)
    normalized["billing_zone"] = to_int_like(normalized["billing_zone"], default=0)
    normalized["billing_country"] = to_int_like(normalized["billing_country"], default=0)
    normalized["M1"] = normalized["M1"] or "T"
    normalized["M2"] = normalized["M2"] or "T"
    normalized["M6"] = normalized["M6"] or "F"
    normalized["card_age_days"] = to_number(normalized["card_age_days"], default=normalized["D4"])
    normalized["days_since_last_tx"] = to_number(
        normalized["days_since_last_tx"],
        default=normalized["D15"],
    )
    normalized["amount_usd"] = to_number(normalized["amount_usd"], default=0.0)

    return normalized


def device_brand(value: str) -> str:
    text = value.lower().strip()
    if not text or text == "nan":
        return "missing"
    head = text.split("/")[0].split(" ")[0]
    if head.startswith("sm-") or "samsung" in text:
        return "samsung"
    for needle in (
        "moto",
        "lg",
        "huawei",
        "xiaomi",
        "oppo",
        "vivo",
        "redmi",
        "nokia",
        "lenovo",
        "asus",
        "htc",
        "google",
        "trident",
        "rv:",
        "windows",
        "macos",
        "linux",
        "ios",
        "ipad",
        "iphone",
    ):
        if needle in text:
            return needle
    return head[:12] or "other"


def os_family(value: str) -> str:
    text = value.lower()
    for key in ("windows", "ios", "android", "mac", "linux"):
        if key in text:
            return key
    return "other" if text and text != "nan" else "missing"


def browser_family(value: str) -> str:
    text = value.lower()
    for key in ("chrome", "safari", "firefox", "edge", "opera", "samsung", "ie ", "android"):
        if key in text:
            return key.strip()
    return "other" if text and text != "nan" else "missing"


def add_identity_features(df: pd.DataFrame) -> None:
    device_info = df.get("device_info", pd.Series(["missing"] * len(df), index=df.index))
    device_info = device_info.fillna("missing").astype(str)
    df["device_info"] = device_info
    df["device_brand"] = device_info.map(device_brand)

    os_raw = df.get("os_raw", pd.Series(["missing"] * len(df), index=df.index))
    os_raw = os_raw.fillna("missing").astype(str)
    df["os_raw"] = os_raw
    df["os_family"] = os_raw.map(os_family)
    df["os_version"] = pd.to_numeric(
        os_raw.str.extract(r"(\d+(?:\.\d+)?)", expand=False),
        errors="coerce",
    ).astype(np.float32)

    browser_raw = df.get("browser_raw", pd.Series(["missing"] * len(df), index=df.index))
    browser_raw = browser_raw.fillna("missing").astype(str)
    df["browser_raw"] = browser_raw
    df["browser_family"] = browser_raw.map(browser_family)
    df["browser_version"] = pd.to_numeric(
        browser_raw.str.extract(r"(\d+(?:\.\d+)?)", expand=False),
        errors="coerce",
    ).astype(np.float32)

    screen_resolution = df.get("screen_resolution", pd.Series(["0x0"] * len(df), index=df.index))
    screen_resolution = screen_resolution.fillna("0x0").astype(str)
    width_height = screen_resolution.str.split("x", n=1, expand=True)
    if width_height.shape[1] == 1:
        width_height[1] = 0
    df["screen_resolution"] = screen_resolution
    df["screen_width"] = pd.to_numeric(width_height[0], errors="coerce").astype(np.float32)
    df["screen_height"] = pd.to_numeric(width_height[1], errors="coerce").astype(np.float32)
    df["screen_area"] = (df["screen_width"] * df["screen_height"]).astype(np.float32)

    device_type = df.get("device_type", pd.Series(["missing"] * len(df), index=df.index))
    df["device_type"] = device_type.fillna("missing").astype(str)


def build_base_features(rows: list[dict[str, Any]], schema: dict[str, Any]) -> pd.DataFrame:
    clean_rows = []
    for row in rows:
        clean = dict(row)
        clean["event_ts_offset_s"] = event_offset_seconds(clean, schema)
        clean_rows.append(clean)

    df = pd.DataFrame(clean_rows)
    reference = pd.Timestamp(schema["training_reference_ts"])
    timestamps = reference + pd.to_timedelta(df["event_ts_offset_s"], unit="s")
    df["hour_of_day"] = timestamps.dt.hour.astype(np.int8)
    df["day_of_week"] = timestamps.dt.dayofweek.astype(np.int8)
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(np.int8)
    amount = pd.to_numeric(df["amount_usd"], errors="coerce")
    df["amount_log"] = np.log1p(amount).astype(np.float32)
    df["amount_cents"] = (amount - amount.astype(int)).astype(np.float32)

    add_identity_features(df)

    email_bin = schema.get("email_bin", EMAIL_BIN)
    email_nulls = set(schema.get("email_nulls", EMAIL_NULLS))
    for col in ("email_purchaser", "email_recipient"):
        if col not in df.columns:
            df[col] = "missing"
        df[col] = df[col].where(~df[col].isin(email_nulls), other=np.nan)
        df[f"{col}_provider"] = df[col].map(email_bin).fillna("other")
        df[col] = df[col].fillna("missing").astype(str)

    if "card_age_days" not in df.columns:
        df["card_age_days"] = 0.0
    day = (df["event_ts_offset_s"] // 86400).astype(int)
    first_seen_day = (day - df["card_age_days"].fillna(0).astype(int)).astype(str)

    def as_string(col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(["missing"] * len(df), index=df.index)
        return df[col].fillna("missing").astype(str)

    df["uid1"] = as_string("card_id") + "_" + as_string("billing_zone")
    df["uid2"] = df["uid1"] + "_" + first_seen_day
    df["uid3"] = df["uid2"] + "_" + as_string("email_purchaser")
    df["uid4"] = df["uid3"] + "_" + as_string("device_brand")
    return df


def init_history_defaults(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    for uid in schema.get("uid_columns", ["uid1", "uid2", "uid3", "uid4"]):
        for col in schema.get("uid_agg_targets", ["amount_usd", "C13", "D15", "D4"]):
            df[f"{uid}_{col}_mean"] = np.nan
            df[f"{uid}_{col}_std"] = np.nan

    df["card_tx_count_so_far"] = 0
    df["card_amount_sum_so_far"] = 0.0
    df["card_amount_mean_so_far"] = np.nan
    df["amount_zscore_card"] = np.nan


def apply_previous_transactions(
    df: pd.DataFrame,
    previous_transactions: list[dict[str, Any]],
    schema: dict[str, Any],
    feature_snapshot: dict[str, Any] | None,
) -> dict[str, int]:
    if not previous_transactions:
        return {"history_rows": 0, "matched_uid2_rows": 0, "matched_card_rows": 0}

    history_rows = [
        normalize_transaction(row, feature_snapshot=feature_snapshot)
        for row in previous_transactions
    ]
    history = build_base_features(history_rows, schema)
    history = history[history["event_ts_offset_s"] < float(df["event_ts_offset_s"].iloc[0])].copy()
    if history.empty:
        return {"history_rows": 0, "matched_uid2_rows": 0, "matched_card_rows": 0}

    uid_columns = schema.get("uid_columns", ["uid1", "uid2", "uid3", "uid4"])
    agg_targets = schema.get("uid_agg_targets", ["amount_usd", "C13", "D15", "D4"])
    for uid in uid_columns:
        current_uid = df[uid].iloc[0]
        group = history[history[uid] == current_uid]
        for col in agg_targets:
            if col not in group.columns or group.empty:
                continue
            numeric = pd.to_numeric(group[col], errors="coerce")
            df[f"{uid}_{col}_mean"] = float(numeric.mean())
            df[f"{uid}_{col}_std"] = float(numeric.std())

    same_uid2 = history[history["uid2"] == df["uid2"].iloc[0]]
    same_card = history[history["card_id"].astype(str) == str(df["card_id"].iloc[0])]
    if not same_card.empty:
        amounts = pd.to_numeric(same_card["amount_usd"], errors="coerce").dropna()
        amount_sum = float(amounts.sum())
        amount_mean = float(amounts.mean()) if len(amounts) else np.nan
        df["card_tx_count_so_far"] = int(len(amounts))
        df["card_amount_sum_so_far"] = amount_sum
        df["card_amount_mean_so_far"] = amount_mean
        if amount_mean and not np.isnan(amount_mean):
            df["amount_zscore_card"] = float(
                (float(df["amount_usd"].iloc[0]) - amount_mean) / amount_mean
            )

    return {
        "history_rows": int(len(history)),
        "matched_uid2_rows": int(len(same_uid2)),
        "matched_card_rows": int(len(same_card)),
    }


def apply_schema(df: pd.DataFrame, schema: dict[str, Any]) -> tuple[pd.DataFrame, int]:
    cold_lookups = 0
    for col, table in schema.get("freq_tables", {}).items():
        if col not in df.columns:
            continue
        value = str(df[col].iloc[0])
        df[f"{col}_freq"] = float(table.get(value, 1))
        if value not in table:
            cold_lookups += 1

    uid_columns = schema.get("uid_columns", ["uid1", "uid2", "uid3", "uid4"])
    df.drop(columns=[col for col in uid_columns if col in df.columns], inplace=True, errors="ignore")

    missing_categoricals: dict[str, list[int]] = {}
    for col, dict in schema.get("categorical_encoders", {}).items():
        default = dict.get("missing", 0)
        if col not in df.columns:
            missing_categoricals[col] = [default] * len(df)
            continue
        value = str(df[col].iloc[0]) if pd.notna(df[col].iloc[0]) else "missing"
        df[col] = dict.get(value, default)
        if value not in dict:
            cold_lookups += 1

    if missing_categoricals:
        df = pd.concat([df, pd.DataFrame(missing_categoricals, index=df.index)], axis=1)

    return df, cold_lookups


def assemble_vector(df: pd.DataFrame, schema: dict[str, Any]) -> tuple[pd.DataFrame, list[str]]:
    feature_columns = schema["feature_columns"]
    missing_columns = [col for col in feature_columns if col not in df.columns]
    if missing_columns:
        df = pd.concat(
            [df, pd.DataFrame({col: [np.nan] * len(df) for col in missing_columns}, index=df.index)],
            axis=1,
        )
    model_inputs = df[feature_columns].copy()
    for col in model_inputs.select_dtypes(include=["object"]).columns:
        model_inputs[col] = pd.to_numeric(model_inputs[col], errors="coerce")
    return model_inputs, missing_columns


def history_from_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    features = payload.get("features", {})
    if not isinstance(features, dict):
        features = {}

    transactions = payload.get("transactions", [])
    if not isinstance(transactions, list):
        transactions = []

    history = payload.get("history", {})
    if isinstance(history, dict):
        history_features = history.get("feature_snapshot", {})
        if isinstance(history_features, dict):
            features = {**history_features, **features}
        previous = history.get("previous_transactions", [])
        if isinstance(previous, list) and not transactions:
            transactions = previous

    return dict(features), [dict(row) for row in transactions if isinstance(row, dict)]


def current_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    current = payload.get("current_transaction", payload)
    if not isinstance(current, dict):
        raise TypeError("payload current_transaction must be an object")
    return current

def build_model_inputs(
    payload: dict[str, Any],
    schema: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, int], int, list[str]]:
    features, previous_transactions = history_from_payload(payload)
    current = normalize_transaction(current_from_payload(payload), feature_snapshot=features)
    df = build_base_features([current], schema)
    init_history_defaults(df, schema)
    history_stats = apply_previous_transactions(df, previous_transactions, schema, features)
    df, cold_lookups = apply_schema(df, schema)
    model_inputs, missing_columns = assemble_vector(df, schema)
    return model_inputs, current, history_stats, cold_lookups, missing_columns


def enrich_current_transaction_with_redis_features(
    current_transaction: dict[str, Any],
    redis_state: dict[str, Any],
) -> dict[str, Any]:
    features = redis_state.get("features", {})
    transactions = redis_state.get("transactions", [])
    if not isinstance(features, dict):
        features = {}
    if not isinstance(transactions, list):
        transactions = []

    previous_tx_count = to_int_like(
        features.get("no_transactions_30_days"),
        default=len(transactions),
    )
    card_age_days = features.get("card_age_days")
    days_since_last_tx = features.get("no_days_since_last_txn")

    current_transaction["C13"] = previous_tx_count + 1
    if card_age_days is not None:
        current_transaction["D4"] = card_age_days
    if days_since_last_tx is not None:
        current_transaction["D15"] = days_since_last_tx
    return current_transaction
