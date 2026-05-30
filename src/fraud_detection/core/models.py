from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from typing import Literal


class Transaction(BaseModel):
    transaction_id: int = Field(alias="tx_id", ge=1)
    event_timestamp: str = Field(min_length=1)
    amount_usd: float = Field(gt=0)
    channel: str = Field(min_length=1)
    card_id: int = Field(ge=1)
    issuer_code: int = Field(ge=0)
    card_brand: str = Field(min_length=1)
    bin_code: int = Field(ge=0)
    card_type: str = Field(min_length=1)
    billing_zone: int = Field(ge=0)
    billing_country: int = Field(ge=0)
    email_purchaser: str = Field(min_length=1)
    email_recipient: str = Field(min_length=1)
    device_type: str = Field(min_length=1)
    device_info: str = Field(min_length=1)
    os_raw: str = Field(min_length=1)
    browser_raw: str = Field(min_length=1)
    screen_resolution: str = Field(pattern=r"^\d+x\d+$")
    # Count features
    c1: int = Field(alias="C1", ge=0)
    c2: int = Field(alias="C2", ge=0)
    c13: int = Field(alias="C13", ge=0)
    # Time delta features
    d4: int = Field(alias="D4", ge=0)
    d15: int = Field(alias="D15", ge=0)
    # Match features
    m1: str = Field(alias="M1", pattern=r"^[TF]$")
    m2: str = Field(alias="M2", pattern=r"^[TF]$")
    m6: str = Field(alias="M6", pattern=r"^[TF]$")


class FraudDetectionInputs(BaseModel):
    current_transaction: Transaction
    history_transactions: list[Transaction]


class FraudDetectionOutputs(BaseModel):
    transaction_id: int
    probability: float
    is_fraud: bool

class TransactionCreate(BaseModel):
    user_id: int = Field(ge=1)
    card_id: int | None = Field(default=None, ge=1)
    status: Literal["approved", "review"]
    amount_usd: Decimal = Field(gt=0, max_digits=14, decimal_places=2)
    channel: str | None = None
    billing_zone: int | None = None
    billing_country: int | None = None
    email_purchaser: str | None = None
    email_recipient: str | None = None
    device_info: str | None = None
    device_type: str | None = None
    os_raw: str | None = None
    browser_raw: str | None = None
    screen_resolution: str | None = Field(default=None, pattern=r"^\d+x\d+$")


class TransactionRecord(TransactionCreate):
    id: int
    created_at: datetime
