from pydantic import BaseModel, Field
from typing import Literal

class FraudDetectionInputs(BaseModel):
    transaction_id: str = Field(min_length=1)
    event_timestamp: str = Field(min_length=1)
    amount: float = Field(gt=0)
    channel: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    card_id: str = Field(min_length=1)
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
    c13: int = Field(alias="C13", ge=0, default=0)
    # Time delta features
    d4: int = Field(alias="D4", ge=0, default=0)
    d15: int = Field(alias="D15", ge=0, default=0)
    # Match features
    m1: str = Field(alias="M1", pattern=r"^[TF]$")
    m2: str = Field(alias="M2", pattern=r"^[TF]$")
    m6: str = Field(alias="M6", pattern=r"^[TF]$")

class FraudDetectionOutputs(BaseModel):
    transaction_id: str
    probability: float
    status: Literal["review", "approved"]
