"""Pydantic request and response schemas for the fraud detection API.

The scoring endpoint identifies a transaction by its ``tx_id`` and the
``(user_id, card_id)`` entity pair; the 29 model features are read from the
Feast online store rather than sent in the request. ``FraudDetectionOutputs``
is the scoring result returned to the caller.
"""

from pydantic import BaseModel, ConfigDict, Field, EmailStr


class FraudDetectionInputs(BaseModel):
    """Validated input schema for a single transaction to be scored.

    Carries only the identifiers needed to look up precomputed features in the
    online store; the feature vector itself is fetched by ``(user_id, card_id)``.
    """

    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
    merchant_category: str = Field(min_length=1)
    merchant_risk_level: int = Field(ge=0, le=10)
    amount_usd: float = Field(gt=0.0)
    timestamp: str = Field(min_length=1)
    channel: str = Field(min_length=1)
    billing_country_code: str = Field(min_length=1)
    ip_country_code: str = Field(min_length=1)
    email_purchaser: EmailStr = Field(...)
    email_recipient: EmailStr = Field(...)


class FraudDetectionOutputs(BaseModel):
    """Scoring result for a single transaction.

    Holds the transaction id, the predicted fraud probability and the binary
    prediction derived from the configured decision threshold.
    """

    transaction_id: str
    probability: float
    prediction: int
