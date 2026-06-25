"""Feature engineering helpers for building model inputs from raw transactions.

Provides value-coercion utilities, transaction normalization, device/OS/browser
and email parsing, time-based and aggregate (UID/card history) feature
construction, schema application (frequency tables and categorical encoders) and
final feature-vector assembly. ``build_model_inputs`` ties these together to turn
a transaction payload (current transaction plus Redis history) into the model's
expected feature DataFrame.
"""

from __future__ import annotations

import math
from datetime import timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from structlog import get_logger


logger = get_logger(__name__)
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

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

def to_number(value: Any, default: float = np.nan) -> float:
    """Coerce a value to a float, returning ``default`` when it cannot be parsed.

    ``None`` and blank strings yield ``default``; unlike :func:`to_float` this
    never raises and never rejects non-finite numbers.
    """
    if value is None:
        return default
    try:
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, feature: str, default: float | None = None) -> float | None:
    """Coerce a feature value to a finite float, falling back to ``default``.

    ``None``, NaN, blank strings and non-finite numbers yield ``default``.

    Args:
        value: The raw feature value to convert.
        feature: Feature name, used only for logging/error messages.
        default: Value returned for missing or non-finite inputs.

    Returns:
        The parsed finite float, or ``default``.

    Raises:
        ValueError: If ``value`` is non-empty but cannot be parsed as a number.
    """
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


def to_int_like(value: Any, default: int = 0) -> int:
    """Coerce a value to an int, returning ``default`` for missing/NaN inputs."""
    number = to_number(value, default=float(default))
    if math.isnan(number):
        return default
    return int(number)


def to_model_card_id(value: Any, default: int = 0) -> int:
    """Convert a card identifier to the integer form expected by the model.

    A 32-character lowercase hex string (e.g. a UUID hex) is parsed as base-16
    and reduced modulo ``2_147_483_647`` to fit an int32 range; any other value
    falls back to :func:`to_int_like`.
    """
    if isinstance(value, str):
        identifier = value.strip().lower()
        if len(identifier) == 32 and all(
            character in "0123456789abcdef" for character in identifier
        ):
            return int(identifier, 16) % 2_147_483_647
    return to_int_like(value, default=default)


def local_timestamp(value: Any) -> pd.Timestamp:
    """Parse a value into a naive Ho Chi Minh local ``pd.Timestamp``.

    Timezone-aware inputs are converted to the Ho Chi Minh timezone and then made
    naive (tz dropped) so all timestamps share a common local reference.

    Raises:
        ValueError: If ``value`` is ``None``.
    """
    if value is None:
        raise ValueError("Transaction must include created_at or event_timestamp")
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(HO_CHI_MINH_TZ).tz_localize(None)
    return timestamp


def event_offset_seconds(row: dict[str, Any], schema: dict[str, Any]) -> float:
    """Return the transaction's event time as seconds since the training reference.

    Uses an explicit ``event_ts_offset_s`` if present, otherwise computes the
    offset of ``event_timestamp`` from ``schema["training_reference_ts"]``.
    """
    explicit = row.get("event_ts_offset_s")
    if explicit is not None:
        return float(explicit)

    event_timestamp = row.get("event_timestamp")
    reference = pd.Timestamp(schema["training_reference_ts"])
    return float((local_timestamp(event_timestamp) - reference).total_seconds())


def normalize_transaction(
    transaction: dict[str, Any],
    *,
    feature_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize a raw transaction into a consistent, typed feature dictionary.

    Selects the known fields, fills count/time-delta/match features (``C*``/``D*``/
    ``M*``) from the transaction or the optional feature snapshot, derives missing
    values (e.g. ``C13`` from the prior 30-day count, ``D4``/``D15`` from card age
    and recency), and coerces every field to its expected numeric or categorical
    type with sensible defaults.

    Args:
        transaction: The raw transaction fields.
        feature_snapshot: Optional historical feature snapshot used to fill gaps.

    Returns:
        A dictionary of normalized, type-coerced feature values.
    """
    row = dict(transaction)
    snapshot = dict(feature_snapshot or {})

    normalized: dict[str, Any] = {
        "tx_id": row.get("tx_id", None),
        "event_timestamp": row.get("event_timestamp", None),
        "amount_usd": row.get("amount_usd", None),
        "channel": row.get("channel", None),
        "user_id": row.get("user_id", None),
        "card_id": row.get("card_id", None),
        "issuer_code": row.get("issuer_code", None),
        "card_country": row.get("card_country", None),
        "card_brand": row.get("card_brand", None),
        "bin_code": row.get("bin_code", None),
        "card_type": row.get("card_type", None),
        "billing_zone": row.get("billing_zone", None),
        "billing_country": row.get("billing_country", None),
        "email_purchaser": row.get("email_purchaser", None),
        "email_recipient": row.get("email_recipient", None),
        "device_type": row.get("device_type", None),
        "device_info": row.get("device_info", None),
        "os_raw": row.get("os_raw", None),
        "browser_raw": row.get("browser_raw", None),
        "screen_resolution": row.get("screen_resolution", None),
    }

    for model_col in ("C1", "C2", "C13", "D4", "D15", "M1", "M2", "M6"):
        normalized[model_col] = row.get(model_col)

    normalized["card_age_days"] = row.get("card_age_days")
    normalized["days_since_last_tx"] = row.get("days_since_last_tx")

    if normalized["card_age_days"] is None:
        normalized["card_age_days"] = snapshot.get("card_age_days")
    if normalized["days_since_last_tx"] is None:
        normalized["days_since_last_tx"] = snapshot.get("no_days_since_last_txn")

    if normalized["D4"] is None:
        normalized["D4"] = normalized["card_age_days"]
    if normalized["D15"] is None:
        normalized["D15"] = normalized["days_since_last_tx"]
    if normalized["C13"] is None:
        previous_count = snapshot.get("no_transactions_30_days")
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
    """Derive a coarse device brand/platform label from a device info string.

    Returns ``"missing"`` for empty/``nan`` input, matches a set of known brand
    or platform keywords, and otherwise falls back to the leading token (or
    ``"other"``).
    """
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
    """Map a raw OS string to a coarse family label.

    Returns one of the known families (windows/ios/android/mac/linux),
    ``"other"`` for any other non-empty value, or ``"missing"`` when empty/``nan``.
    """
    text = value.lower()
    for key in ("windows", "ios", "android", "mac", "linux"):
        if key in text:
            return key
    return "other" if text and text != "nan" else "missing"


def browser_family(value: str) -> str:
    """Map a raw browser string to a coarse family label.

    Returns one of the known families (chrome/safari/firefox/edge/opera/samsung/
    ie/android), ``"other"`` for any other non-empty value, or ``"missing"`` when
    empty/``nan``.
    """
    text = value.lower()
    for key in ("chrome", "safari", "firefox", "edge", "opera", "samsung", "ie ", "android"):
        if key in text:
            return key.strip()
    return "other" if text and text != "nan" else "missing"


def add_identity_features(df: pd.DataFrame) -> None:
    """Add device, OS, browser and screen identity features to ``df`` in place.

    Cleans the raw device/OS/browser strings, derives brand/family labels and
    numeric versions, and parses the screen resolution into width, height and
    area columns.
    """
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
    """Build the base (non-aggregate) feature DataFrame for one or more rows.

    Computes each row's event time offset, derives time-of-day/weekend and amount
    features, adds identity features, maps email providers (treating null-domains
    as missing) and constructs the hierarchical ``uid1``-``uid4`` grouping keys.

    Args:
        rows: Normalized transaction dictionaries.
        schema: Feature schema providing the training reference time, email
            mappings and related configuration.

    Returns:
        A DataFrame with one row per input transaction and the base feature set.
    """
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
        """Return ``df[col]`` as filled strings, or a ``"missing"`` series if absent."""
        if col not in df.columns:
            return pd.Series(["missing"] * len(df), index=df.index)
        return df[col].fillna("missing").astype(str)

    df["uid1"] = as_string("card_id") + "_" + as_string("billing_zone")
    df["uid2"] = df["uid1"] + "_" + first_seen_day
    df["uid3"] = df["uid2"] + "_" + as_string("email_purchaser")
    df["uid4"] = df["uid3"] + "_" + as_string("device_brand")
    return df


def init_history_defaults(df: pd.DataFrame, schema: dict[str, Any]) -> None:
    """Initialize history-derived aggregate feature columns to defaults in place.

    Seeds the per-UID mean/std columns to NaN and the per-card running
    count/sum/mean and amount z-score columns to their empty-history defaults, so
    they exist even when no prior transactions are available.
    """
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
) -> None:
    """Populate history-based aggregate features from previous transactions in place.

    Builds base features for the prior transactions, keeps only those strictly
    before the current event, and computes per-UID mean/std of the aggregate
    targets plus per-card running count, sum, mean and amount z-score. No-ops when
    there is no usable history.

    Args:
        df: Single-row DataFrame for the current transaction, updated in place.
        previous_transactions: Prior transactions for the same user/card.
        schema: Feature schema providing UID columns and aggregation targets.
        feature_snapshot: Optional snapshot used when normalizing history rows.
    """
    if not previous_transactions:
        return

    history_rows = [
        normalize_transaction(row, feature_snapshot=feature_snapshot)
        for row in previous_transactions
    ]
    history = build_base_features(history_rows, schema)
    history = history[history["event_ts_offset_s"] < float(df["event_ts_offset_s"].iloc[0])].copy()
    if history.empty:
        return

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


def apply_schema(df: pd.DataFrame, schema: dict[str, Any]) -> pd.DataFrame:
    """Apply frequency encodings and categorical encoders defined by the schema.

    Adds ``<col>_freq`` columns from the schema's frequency tables, drops the
    intermediate UID columns, and replaces categorical columns with their encoded
    integer codes (filling absent columns with the encoder's missing default).

    Returns:
        The transformed DataFrame.
    """
    for col, table in schema.get("freq_tables", {}).items():
        if col not in df.columns:
            continue
        value = str(df[col].iloc[0])
        df[f"{col}_freq"] = float(table.get(value, 1))

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

    if missing_categoricals:
        df = pd.concat([df, pd.DataFrame(missing_categoricals, index=df.index)], axis=1)

    return df


def assemble_vector(df: pd.DataFrame, schema: dict[str, Any]) -> pd.DataFrame:
    """Select and order the final model feature columns from ``df``.

    Adds any feature columns missing from ``df`` as NaN, restricts to the schema's
    ``feature_columns`` in order, and coerces remaining object columns to numeric.

    Returns:
        The model-ready feature DataFrame.
    """
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
    return model_inputs


def history_from_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Extract the feature snapshot and previous transactions from a payload.

    Tolerates missing or malformed sections and supports a nested ``history``
    block (with ``feature_snapshot`` and ``previous_transactions``) as an
    alternative source, merging snapshot features and only using nested
    transactions when no top-level ones are present.

    Returns:
        A tuple of ``(features, previous_transactions)``.
    """
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
    """Extract the current transaction from a payload.

    Uses the ``current_transaction`` key if present, otherwise treats the payload
    itself as the transaction.

    Raises:
        TypeError: If the resolved current transaction is not a mapping.
    """
    current = payload.get("current_transaction", payload)
    if not isinstance(current, dict):
        raise TypeError("payload current_transaction must be an object")
    return current

def build_model_inputs(
    payload: dict[str, Any],
    schema: dict[str, Any],
) -> pd.DataFrame:
    """Build the complete model feature vector from a transaction payload.

    Orchestrates the full pipeline: extracts history and the current transaction,
    normalizes it, builds base features, initializes and fills history aggregates,
    applies schema encodings and assembles the final feature DataFrame.

    Args:
        payload: Payload containing the current transaction and optional history.
        schema: Feature schema driving normalization, encoding and column order.

    Returns:
        The model-ready feature DataFrame.
    """
    features, previous_transactions = history_from_payload(payload)
    current = normalize_transaction(current_from_payload(payload), feature_snapshot=features)
    df = build_base_features([current], schema)
    init_history_defaults(df, schema)
    apply_previous_transactions(df, previous_transactions, schema, features)
    df = apply_schema(df, schema)
    return assemble_vector(df, schema)


def enrich_current_transaction_with_redis_features(
    current_transaction: dict[str, Any],
    redis_state: dict[str, Any],
) -> dict[str, Any]:
    """Fill the current transaction's ``C13``/``D4``/``D15`` from Redis state.

    When these features are absent or zero, derives them from the cached 30-day
    transaction count, card age and days-since-last-transaction so the model sees
    history-aware values.

    Args:
        current_transaction: The current transaction dict, mutated and returned.
        redis_state: Cached ``features`` and ``transactions`` for the user/card.

    Returns:
        The updated current transaction dictionary.
    """
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

    if to_number(current_transaction.get("C13"), default=0.0) == 0:
        current_transaction["C13"] = previous_tx_count + 1
    d4 = to_number(current_transaction.get("D4"), default=0.0)
    d15 = to_number(current_transaction.get("D15"), default=0.0)
    if card_age_days is not None and d4 == 0:
        d4 = to_number(card_age_days, default=0.0)
    if days_since_last_tx is not None and d15 == 0:
        d15 = to_number(days_since_last_tx, default=0.0)
    current_transaction["D4"] = d4
    current_transaction["D15"] = d15
    return current_transaction

def normalize_email(transaction: dict[str, Any]) -> dict[str, Any]:
    """Reduce purchaser/recipient emails to their lowercased domain.

    Returns a copy of the transaction where each email field is trimmed,
    lowercased and replaced by the part after ``@`` (the domain) when present.
    """
    normalized_transaction = transaction.copy()
    for col in ("email_purchaser", "email_recipient"):
        email = normalized_transaction.get(col, "")
        if isinstance(email, str):
            email = email.strip().lower()
            normalized_transaction[col] = email.split("@")[-1] if "@" in email else email
    return normalized_transaction
