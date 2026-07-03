from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float64, Int64, String, Bool

from data_sources import transaction_source
from entities import user, card

transaction_features = FeatureView(
    name="transaction_features",
    entities=[user, card],
    ttl=timedelta(days=7),
    source=transaction_source,
    schema=[
        Field(name="amount_usd", dtype=Float64),
        Field(name="log_amount", dtype=Float64),
        Field(name="hour", dtype=Int64),
        Field(name="weekday", dtype=Int64),
        Field(name="is_night", dtype=Int64),

        Field(name="channel", dtype=String),
        Field(name="card_brand", dtype=String),
        Field(name="card_type", dtype=String),
        Field(name="is_virtual", dtype=Bool),

        Field(name="customer_segment", dtype=String),
        Field(name="kyc_level", dtype=Int64),
        Field(name="email_verified", dtype=Bool),

        Field(name="merchant_category", dtype=String),
        Field(name="merchant_risk_level", dtype=Int64),

        Field(name="account_age_days", dtype=Int64),
        Field(name="card_age_days", dtype=Int64),

        Field(name="geo_mismatch", dtype=Int64),
        Field(name="foreign_ip", dtype=Int64),
        Field(name="recipient_differs", dtype=Int64),

        Field(name="card_tx_count_1h", dtype=Int64),
        Field(name="card_tx_count_24h", dtype=Int64),
        Field(name="card_amount_sum_24h", dtype=Float64),
        Field(name="card_seconds_since_last_tx", dtype=Float64),
        Field(name="card_amount_zscore", dtype=Float64),
        Field(name="card_tx_seq", dtype=Int64),
        Field(name="card_declines_24h", dtype=Int64),

        Field(name="user_tx_count_24h", dtype=Int64),
        Field(name="user_amount_sum_24h", dtype=Float64),
        Field(name="user_seconds_since_last_tx", dtype=Float64),
    ],
)
