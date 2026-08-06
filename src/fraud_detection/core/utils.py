"""Build the model feature vector from precomputed online-store features.

The model consumes the 29 ``feature_columns`` defined in the feature schema,
exactly as produced by the feature ETL and materialised into the Feast online
store. This module maps a raw online-feature dictionary onto that ordered
vector: categorical columns are integer-encoded via the schema's encoders and
every other column is coerced to a float. Missing or unseen values become NaN so
that LightGBM treats them as "missing" — matching how NULLs were handled at
training time (sending ``0.0`` instead would silently skew predictions).
"""

from __future__ import annotations

import math
from typing import Any


NAN = float("nan")


def encode_categorical(value: Any, encoder: dict[str, int]) -> float:
    """Encode a categorical value via ``encoder``, returning NaN when unmapped.

    ``None`` and categories absent from the encoder (unseen at training time)
    yield NaN so the model treats them as missing. Booleans and other scalars
    are matched by their string form (e.g. ``True`` -> ``"True"``).
    """
    if value is None:
        return NAN
    key = str(value)
    if key in encoder:
        return float(encoder[key])
    return NAN


def to_number(value: Any) -> float:
    """Coerce a value to a finite float, returning NaN for missing/invalid input.

    ``None``, blank strings, unparseable values and non-finite numbers all yield
    NaN (the model's "missing" sentinel).
    """
    if value is None:
        return NAN
    if isinstance(value, str) and not value.strip():
        return NAN
    try:
        number = float(value)
    except (TypeError, ValueError):
        return NAN
    return number if math.isfinite(number) else NAN


def build_model_inputs(
    features: dict[str, Any],
    schema: dict[str, Any],
) -> dict[str, float]:
    """Assemble the ordered, encoded feature vector for the model.

    Iterates ``schema["feature_columns"]`` in order, integer-encoding the columns
    listed in ``schema["categorical_encoders"]`` and coercing the rest to floats.

    Args:
        features: Raw feature values keyed by column name, as returned by the
            online store (extra keys such as entity join keys are ignored).
        schema: Feature schema providing ``feature_columns`` and
            ``categorical_encoders``.

    Returns:
        An ordered dict mapping each feature column to its float-encoded value,
        with NaN for anything missing or unseen.
    """
    feature_columns: list[str] = schema["feature_columns"]
    encoders: dict[str, dict[str, int]] = schema.get("categorical_encoders", {})

    encoded: dict[str, float] = {}
    for column in feature_columns:
        raw = features.get(column)
        if column in encoders:
            encoded[column] = encode_categorical(raw, encoders[column])
        else:
            encoded[column] = to_number(raw)
    return encoded
