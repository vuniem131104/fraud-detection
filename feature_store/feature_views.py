from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Int64, String, Bool, UnixTimestamp

from data_sources import transaction_source
from entities import user, card

transaction_features = FeatureView(
    name="transaction_features",
    entities=[user, card],
    ttl=timedelta(0),
    source=transaction_source,
    schema=[
        Field(name="card_brand", dtype=String),
        Field(name="card_type", dtype=String),
        Field(name="is_virtual", dtype=Bool),
        Field(name="card_created_at", dtype=UnixTimestamp),

        Field(name="customer_segment", dtype=String),
        Field(name="kyc_level", dtype=Int64),
        Field(name="email_verified", dtype=Bool),
        Field(name="user_country", dtype=String),
        Field(name="account_created_at", dtype=UnixTimestamp),
    ],
)
