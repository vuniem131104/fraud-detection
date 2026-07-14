from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

transaction_source = PostgreSQLSource(
    name="transaction_source",
    query="""
        SELECT *
        FROM application.transaction_features
    """,
    timestamp_field="created_at",
    created_timestamp_column="computed_at",
)
